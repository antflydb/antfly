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
const inference_audio = @import("inference_audio");
const platform = @import("antfly_platform");
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

const default_spatial_merge_size: usize = @intCast(
    projector_format_mod.gemma4_spatial_merge_size,
);
const default_max_image_tokens: usize = 280;
const default_max_direct_audio_tokens: usize = 16_384;

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
    direct_unified: bool = false,
    image_size: usize,
    patch_size: usize,
    layer_norm_eps: f32,
    rope_theta: f32 = 100.0,
    image_mean: [3]f32,
    image_std: [3]f32,
    spatial_merge_size: usize = default_spatial_merge_size,
    max_image_tokens: usize = default_max_image_tokens,
    position_embeddings_per_axis: usize = 0,

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
    direct_unified: bool = false,
    raw_samples_per_token: usize = 640,
    max_direct_audio_tokens: usize = default_max_direct_audio_tokens,
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

pub const LoadedF32 = struct {
    store: *tensor_store_mod.GgufStore,
    name: []const u8,
    weight: weight_source_mod.LoadedWeight,
    converted: ?Tensor = null,
    data: []const f32,
    shape: []const i64,

    pub fn deinit(self: *LoadedF32) void {
        if (self.converted) |*converted| converted.deinit();
        self.weight.deinit();
        self.store.discardTensorFileCache(self.name);
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
/// This supports the `gemma4v` image projector path and the `gemma4uv`
/// combined image/audio projector metadata used by Gemma 4 12B.
pub fn encodeProjectedImages(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    projector: *ProjectorStore,
    images: []const []const u8,
) !ProjectedImages {
    var weights = ProjectorWeights.init(cb, allocator, projector.gguf, projector);
    defer weights.deinit();
    return encodeProjectedImagesWithWeights(cb, allocator, &weights, images);
}

/// A projector file kept open for a model's loaded lifetime. Parsing the
/// GGUF header (hundreds of tensors) and re-reading the small `a.*` tensors
/// was a visible share of every audio request, so the model manager opens
/// one of these on the first media request and closes it with the model.
/// Reads are lock-free (the store is a read-only mapping); the scalar clamp
/// bounds resolved on first use share a small mutex. Allocations come from
/// the owner's allocator, never from a request.
pub const ProjectorStore = struct {
    allocator: std.mem.Allocator,
    path: []u8,
    gguf: *tensor_store_mod.GgufStore,
    /// Scalar clamp bounds per linear prefix, resolved on first use.
    clamp_specs: std.StringHashMapUnmanaged(ClampSpec) = .empty,
    clamp_mutex: std.atomic.Mutex = .unlocked,

    pub fn open(allocator: std.mem.Allocator, path: []const u8) !*ProjectorStore {
        const self = try allocator.create(ProjectorStore);
        errdefer allocator.destroy(self);
        const owned_path = try allocator.dupe(u8, path);
        errdefer allocator.free(owned_path);
        const gguf = try tensor_store_mod.GgufStore.initAbsolute(allocator, path);
        self.* = .{ .allocator = allocator, .path = owned_path, .gguf = gguf };
        return self;
    }

    pub fn close(self: *ProjectorStore) void {
        const allocator = self.allocator;
        var it = self.clamp_specs.keyIterator();
        while (it.next()) |key| allocator.free(key.*);
        self.clamp_specs.deinit(allocator);
        self.gguf.tensorStore().deinit();
        allocator.free(self.path);
        allocator.destroy(self);
    }

    /// Cached scalar clamp bounds for `prefix`; null when the bounds are not
    /// all scalar (those keep the per-call tensor path).
    fn clampSpec(self: *ProjectorStore, prefix: []const u8) !?ClampSpec {
        platform.sync.lockYielding(&self.clamp_mutex);
        defer self.clamp_mutex.unlock();
        if (self.clamp_specs.get(prefix)) |spec| return spec;
        const spec = (try loadScalarClampSpec(self.allocator, self.gguf, prefix)) orelse return null;
        const key = try self.allocator.dupe(u8, prefix);
        errdefer self.allocator.free(key);
        try self.clamp_specs.put(self.allocator, key, spec);
        return spec;
    }
};

/// Request-scoped projector weights. Each linear / norm / conv weight is
/// fetched once per request (the session's resident copy when it has one,
/// else a load from the projector file) and stays alive until the request
/// ends: Metal caches its dynamic linear slots by tensor identity, so a
/// weight freed and reloaded mid-request could hand a later linear a stale
/// slot. `deinit` releases those slots before freeing the tensors.
const ProjectorWeights = struct {
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    gguf: *tensor_store_mod.GgufStore,
    /// The model-owned store when the request has one (its clamp-bound
    /// cache); null for callers that bring a bare GGUF store.
    owner: ?*ProjectorStore,
    entries: std.StringHashMapUnmanaged(CT) = .empty,

    fn init(cb: *const ComputeBackend, allocator: std.mem.Allocator, gguf: *tensor_store_mod.GgufStore, owner: ?*ProjectorStore) ProjectorWeights {
        return .{ .cb = cb, .allocator = allocator, .gguf = gguf, .owner = owner };
    }

    fn deinit(self: *ProjectorWeights) void {
        const metal_compute_mod = @import("../ops/metal_compute.zig");
        var it = self.entries.iterator();
        while (it.next()) |entry| {
            if (self.cb.kind() == .metal) {
                metal_compute_mod.MetalCompute.releaseDynamicSlotsForTensor(self.cb, entry.value_ptr.*);
            }
            self.cb.free(entry.value_ptr.*);
            self.allocator.free(entry.key_ptr.*);
        }
        self.entries.deinit(self.allocator);
        self.entries = .empty;
    }

    /// The cached tensor for `name`, loading it through `load` on a miss.
    /// The returned tensor is owned by the cache; callers must not free it.
    fn cached(self: *ProjectorWeights, name: []const u8, load: anytype) !CT {
        if (self.entries.get(name)) |tensor| return tensor;
        const tensor = try load.call();
        errdefer self.cb.free(tensor);
        const key = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(key);
        try self.entries.put(self.allocator, key, tensor);
        return tensor;
    }
};

const ClampSpec = struct {
    input_min: ?f32 = null,
    input_max: ?f32 = null,
    output_min: ?f32 = null,
    output_max: ?f32 = null,

    fn clipsInput(self: ClampSpec) bool {
        return self.input_min != null or self.input_max != null;
    }

    fn clipsOutput(self: ClampSpec) bool {
        return self.output_min != null or self.output_max != null;
    }
};

/// `TERMITE_GEMMA4_AUDIO_HOST_OPS=clamp,glu,dwconv,attention,scale,bias,channel_norm,flatten,resident`
/// (or `all`) keeps the named encoder steps on their host path; a debug
/// knob for bisecting a device kernel against the host reference.
const AudioHostOps = struct {
    clamp: bool = false,
    glu: bool = false,
    dwconv: bool = false,
    attention: bool = false,
    scale: bool = false,
    bias: bool = false,
    channel_norm: bool = false,
    flatten: bool = false,
    resident: bool = false,

    fn parse(spec: []const u8) AudioHostOps {
        var ops_mask = AudioHostOps{};
        var it = std.mem.splitScalar(u8, spec, ',');
        while (it.next()) |raw| {
            const item = std.mem.trim(u8, raw, " ");
            if (item.len == 0) continue;
            const all = std.mem.eql(u8, item, "all");
            inline for (@typeInfo(AudioHostOps).@"struct".fields) |field| {
                if (all or std.mem.eql(u8, item, field.name)) @field(ops_mask, field.name) = true;
            }
        }
        return ops_mask;
    }
};

var audio_host_ops_cache: ?AudioHostOps = null;

fn audioHostOps() AudioHostOps {
    if (audio_host_ops_cache) |cached| return cached;
    const parsed = if (platform.env.getenvSlice("TERMITE_GEMMA4_AUDIO_HOST_OPS")) |spec| AudioHostOps.parse(spec) else AudioHostOps{};
    audio_host_ops_cache = parsed;
    return parsed;
}

fn loadScalarClampSpec(allocator: std.mem.Allocator, store: *tensor_store_mod.GgufStore, prefix: []const u8) !?ClampSpec {
    var spec = ClampSpec{};
    const fields = [_]struct { suffix: []const u8, slot: *?f32 }{
        .{ .suffix = "input_min", .slot = &spec.input_min },
        .{ .suffix = "input_max", .slot = &spec.input_max },
        .{ .suffix = "output_min", .slot = &spec.output_min },
        .{ .suffix = "output_max", .slot = &spec.output_max },
    };
    for (fields) |field| {
        const name = try std.fmt.allocPrint(allocator, "{s}.{s}", .{ prefix, field.suffix });
        defer allocator.free(name);
        var tensor = (try loadOptionalTensorF32(store, name)) orelse continue;
        defer tensor.deinit();
        if (tensor.data.len != 1) return null;
        field.slot.* = tensor.data[0];
    }
    return spec;
}

/// One-off encode for callers without a model-owned store (fine-tuning,
/// tools): opens the projector for this call and closes it after.
pub fn encodeProjectedImagesFromPath(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    projector_path: []const u8,
    images: []const []const u8,
) !ProjectedImages {
    const projector = try ProjectorStore.open(allocator, projector_path);
    defer projector.close();
    return encodeProjectedImages(cb, allocator, projector, images);
}

pub fn encodeProjectedImagesFromStore(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    gguf: *tensor_store_mod.GgufStore,
    images: []const []const u8,
) !ProjectedImages {
    var weights = ProjectorWeights.init(cb, allocator, gguf, null);
    defer weights.deinit();
    return encodeProjectedImagesWithWeights(cb, allocator, &weights, images);
}

fn encodeProjectedImagesWithWeights(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    store: *ProjectorWeights,
    images: []const []const u8,
) !ProjectedImages {
    const cfg = try parseConfig(&store.gguf.parsed);

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
    projector: *ProjectorStore,
    audio_clips: []const []const u8,
) !ProjectedAudio {
    var weights = ProjectorWeights.init(cb, allocator, projector.gguf, projector);
    defer weights.deinit();
    return encodeProjectedAudioWithWeights(cb, allocator, &weights, audio_clips);
}

/// One-off encode for callers without a model-owned store.
pub fn encodeProjectedAudioFromPath(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    projector_path: []const u8,
    audio_clips: []const []const u8,
) !ProjectedAudio {
    const projector = try ProjectorStore.open(allocator, projector_path);
    defer projector.close();
    return encodeProjectedAudio(cb, allocator, projector, audio_clips);
}

pub fn encodeProjectedAudioFromStore(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    gguf: *tensor_store_mod.GgufStore,
    audio_clips: []const []const u8,
) !ProjectedAudio {
    var weights = ProjectorWeights.init(cb, allocator, gguf, null);
    defer weights.deinit();
    return encodeProjectedAudioWithWeights(cb, allocator, &weights, audio_clips);
}

fn encodeProjectedAudioWithWeights(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    store: *ProjectorWeights,
    audio_clips: []const []const u8,
) !ProjectedAudio {
    const cfg = try parseAudioConfig(&store.gguf.parsed);

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
    store: *ProjectorWeights,
    cfg: AudioConfig,
    audio_bytes: []const u8,
) !EncodedAudio {
    if (cfg.direct_unified) {
        return encodeUnifiedDirectAudio(cb, allocator, store, cfg, audio_bytes);
    }

    var profile = AudioProfile{ .cb = cb };
    const profile_enabled = gemma4AudioMetalProfileEnabled();
    if (profile_enabled) {
        profile.start();
        active_audio_profile = &profile;
    }
    defer if (profile_enabled) {
        active_audio_profile = null;
    };

    var features = try prepareGemma4AudioFeatures(allocator, audio_bytes, cfg.mel_bins);
    defer features.deinit();
    audioProfileMark(.features);

    // From the conv stack to the output projection every op runs on device
    // over device-resident inputs: keep them in one Metal frame so the
    // runtime submits once instead of committing and waiting after every
    // op. The frame is owned only here; a caller that already composes one
    // keeps it.
    var frame_active = false;
    if (cb.kind() == .metal and gemma4AudioEncoderFrameEnabled() and !cb.decoderRuntimeHasActiveFrame()) {
        frame_active = try cb.decoderRuntimeBeginFrame();
    }
    errdefer if (frame_active) cb.decoderRuntimeCancelFrame() catch {};

    var subsampled = try audioSubsample(cb, allocator, store, cfg, &features);
    defer subsampled.deinit(allocator);
    audioProfileMark(.subsample);

    var hidden = subsampled.hidden;
    errdefer cb.free(hidden);
    // Per-block `per_dim_scale` vectors; the loaded tensors keep their names
    // until they are released, so the names outlive the loop.
    var per_dim_names = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (per_dim_names.items) |name| allocator.free(name);
        per_dim_names.deinit(allocator);
    }
    var per_dim_tensors = std.ArrayListUnmanaged(LoadedF32).empty;
    defer {
        for (per_dim_tensors.items) |*tensor| tensor.deinit();
        per_dim_tensors.deinit(allocator);
    }
    var per_dim_slices = std.ArrayListUnmanaged([]const f32).empty;
    defer per_dim_slices.deinit(allocator);
    for (0..cfg.block_count) |layer| {
        // Each item is owned by its list as soon as the append succeeds, so
        // the local cleanup must not outlive the append.
        const name = try std.fmt.allocPrint(allocator, "a.blk.{d}.per_dim_scale.weight", .{layer});
        {
            errdefer allocator.free(name);
            try per_dim_names.append(allocator, name);
        }
        var per_dim = try loadTensorF32(store.gguf, name);
        {
            errdefer per_dim.deinit();
            try per_dim_tensors.append(allocator, per_dim);
        }
        try per_dim_slices.append(allocator, per_dim_tensors.items[per_dim_tensors.items.len - 1].data);
    }
    var layer_inputs = try AudioLayerInputs.init(cb, allocator, cfg, subsampled.valid_mask, per_dim_slices.items);
    defer layer_inputs.deinit(cb);

    for (0..cfg.block_count) |layer| {
        const next = try audioLayer(cb, allocator, store, cfg, hidden, &layer_inputs, layer);
        cb.free(hidden);
        hidden = next;
    }

    const output = try audioLinearWithBias(cb, allocator, store, hidden, "a.pre_encode.out", subsampled.seq_len, cfg.audio_hidden, cfg.output_hidden);
    cb.free(hidden);
    defer cb.free(output);

    const normed = try cb.rmsNorm(output, layer_inputs.ones, cfg.output_hidden, cfg.layer_norm_eps);
    defer cb.free(normed);
    const projection_w = try projectorLinearWeightCt(cb, allocator, store, "mm.a.input_projection.weight", cfg.output_hidden, cfg.text_hidden);
    const projected = try cb.linearNoBias(normed, projection_w, subsampled.seq_len, cfg.output_hidden, cfg.text_hidden);
    defer cb.free(projected);
    audioProfileMark(.tail);
    if (frame_active) {
        frame_active = false;
        try cb.decoderRuntimeSubmitAndWaitFrame();
    }

    const projected_data = try cb.toFloat32(projected, allocator);
    defer allocator.free(projected_data);
    audioProfileMark(.readback);
    if (profile_enabled) profile.finish(subsampled.seq_len);
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

fn encodeUnifiedDirectAudio(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    store: *ProjectorWeights,
    cfg: AudioConfig,
    audio_bytes: []const u8,
) !EncodedAudio {
    const frames = try prepareGemma4RawAudioFrames(allocator, audio_bytes, cfg.raw_samples_per_token, cfg.max_direct_audio_tokens);
    defer allocator.free(frames);
    const token_count = frames.len / cfg.raw_samples_per_token;
    if (token_count == 0) {
        return .{
            .embeddings = try allocator.alloc(f32, 0),
            .tokens = 0,
        };
    }
    const frame_shape = [_]i32{ @intCast(token_count), @intCast(cfg.raw_samples_per_token) };
    const frame_ct = try cb.fromFloat32Shape(frames, &frame_shape);
    defer cb.free(frame_ct);

    // Direct unified audio projects RMS-normalized raw waveform chunks, matching
    // llama.cpp's Gemma4UnifiedMultimodalEmbedder graph.
    const normed = try rmsNormNoScaleCt(cb, allocator, frame_ct, token_count, cfg.raw_samples_per_token, cfg.layer_norm_eps);
    defer cb.free(normed);
    const projection_w = try loadLinearWeightCt(cb, allocator, store.gguf, "mm.a.input_projection.weight", cfg.raw_samples_per_token, cfg.text_hidden);
    defer cb.free(projection_w);
    const projected = try cb.linearNoBias(normed, projection_w, token_count, cfg.raw_samples_per_token, cfg.text_hidden);
    defer cb.free(projected);

    return .{
        .embeddings = try cb.toFloat32(projected, allocator),
        .tokens = token_count,
    };
}

fn prepareGemma4RawAudioFrames(
    allocator: std.mem.Allocator,
    audio_bytes: []const u8,
    samples_per_token: usize,
    max_tokens: usize,
) ![]f32 {
    if (samples_per_token == 0) return error.InvalidTensorShape;
    const target_rate: u32 = 16_000;
    const max_samples = std.math.mul(usize, samples_per_token, max_tokens) catch return error.InvalidTensorShape;

    var decoded = try audio.decodeBounded(allocator, audio_bytes, .{}, audio.default_decode_working_bytes);
    defer decoded.deinit();
    const source_limit = try sourceSampleLimitForResample(max_samples, decoded.sample_rate, target_rate);
    if (decoded.samples.len > source_limit) return error.AudioInputTooLong;
    const resampled = try audio.copyOrResample(allocator, decoded.samples, decoded.sample_rate, target_rate);
    defer allocator.free(resampled);

    if (resampled.len > max_samples) return error.AudioInputTooLong;
    const real_samples = resampled.len;
    const frames = if (real_samples == 0)
        0
    else
        (real_samples + samples_per_token - 1) / samples_per_token;
    const out_len = std.math.mul(usize, frames, samples_per_token) catch return error.InvalidTensorShape;
    const out = try allocator.alloc(f32, out_len);
    @memset(out, 0.0);
    if (real_samples > 0) @memcpy(out[0..real_samples], resampled[0..real_samples]);
    return out;
}

fn sourceSampleLimitForResample(target_samples: usize, source_rate: u32, target_rate: u32) !usize {
    if (source_rate == 0 or target_rate == 0) return error.UnsupportedAudioFormat;
    const numerator = std.math.mul(u128, @as(u128, target_samples), @as(u128, source_rate)) catch
        return error.AudioInputTooLong;
    const source_samples = numerator / @as(u128, target_rate);
    return std.math.cast(usize, source_samples) orelse error.AudioInputTooLong;
}

test "gemma4 direct audio frames preserve raw waveform samples" {
    const allocator = std.testing.allocator;
    const samples = [_]f32{ 0.0, 0.25, -0.5, 0.75, -0.25 };
    const wav_bytes = try audio.wav.encodeMono(allocator, &samples, .{
        .audio_format = 1,
        .sample_rate = 16_000,
        .bits_per_sample = 16,
    });
    defer allocator.free(wav_bytes);

    const frames = try prepareGemma4RawAudioFrames(allocator, wav_bytes, 4, 4);
    defer allocator.free(frames);

    try std.testing.expectEqual(@as(usize, 8), frames.len);
    for (samples, 0..) |expected, idx| {
        try std.testing.expectApproxEqAbs(expected, frames[idx], 1.0 / 32768.0);
    }
    try std.testing.expectEqual(@as(f32, 0.0), frames[5]);
    try std.testing.expectEqual(@as(f32, 0.0), frames[6]);
    try std.testing.expectEqual(@as(f32, 0.0), frames[7]);
}

test "gemma4 audio source window is bounded before resampling" {
    try std.testing.expectEqual(@as(usize, 30), try sourceSampleLimitForResample(480_000, 1, 16_000));
    try std.testing.expectEqual(@as(usize, 1_440_000), try sourceSampleLimitForResample(480_000, 48_000, 16_000));
    try std.testing.expectError(error.UnsupportedAudioFormat, sourceSampleLimitForResample(480_000, 0, 16_000));
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

    var decoded = try audio.decodeBounded(allocator, audio_bytes, .{}, audio.default_decode_working_bytes);
    defer decoded.deinit();
    const source_limit = try sourceSampleLimitForResample(max_samples, decoded.sample_rate, target_rate);
    const source_window = decoded.samples[0..@min(decoded.samples.len, source_limit)];
    const resampled = try audio.copyOrResample(allocator, source_window, decoded.sample_rate, target_rate);
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
    for (0..frames) |frame| {
        const frame_end = frame * hop_length + frame_size_for_unfold - 1;
        mask[frame] = frame_end >= pad_left and frame_end < pad_left + real_samples;
    }
    try gemma4LogMelFrames(allocator, samples, frames, hop_length, frame_length, fft_length, window, filters, mel_bins, mel_floor, mask, out, audio.blas_available);

    return .{ .allocator = allocator, .data = out, .mask = mask, .frames = frames, .mel_bins = mel_bins };
}

/// Log-mel rows for `frames` windows of `samples`. With BLAS the windowed
/// frames go through one matrix product against the zero-padded DFT basis
/// and the magnitudes through another against the filterbank; without it,
/// each frame takes the radix FFT. Both give the same rows within float
/// rounding. Masked-out frames are written as zero.
fn gemma4LogMelFrames(
    allocator: std.mem.Allocator,
    samples: []const f32,
    frames: usize,
    hop_length: usize,
    frame_length: usize,
    fft_length: usize,
    window: []const f32,
    filters: []const f32,
    mel_bins: usize,
    mel_floor: f32,
    mask: []const bool,
    out: []f32,
    use_blas: bool,
) !void {
    const n_freq = fft_length / 2 + 1;
    if (filters.len != mel_bins * n_freq or out.len != frames * mel_bins or mask.len != frames) return error.InvalidTensorShape;
    if (frames == 0) return;
    if (use_blas and audio.blas_available and frames <= std.math.maxInt(c_int) / 2) {
        // Windowed frames [frames, frame_length].
        const frame_matrix = try allocator.alloc(f32, frames * frame_length);
        defer allocator.free(frame_matrix);
        for (0..frames) |frame| {
            const src = samples[frame * hop_length ..][0..frame_length];
            const row = frame_matrix[frame * frame_length ..][0..frame_length];
            for (row, src, window) |*value, sample, w| value.* = sample * w;
        }
        // DFT basis for the zero-padded transform [frame_length, 2 * n_freq]:
        // cos and -sin per bin, only the first `frame_length` rows matter.
        const basis_cols = 2 * n_freq;
        const basis = try allocator.alloc(f32, frame_length * basis_cols);
        defer allocator.free(basis);
        for (0..frame_length) |i| {
            for (0..n_freq) |k| {
                const angle = 2.0 * std.math.pi * @as(f64, @floatFromInt(i * k)) / @as(f64, @floatFromInt(fft_length));
                basis[i * basis_cols + 2 * k] = @floatCast(@cos(angle));
                basis[i * basis_cols + 2 * k + 1] = @floatCast(-@sin(angle));
            }
        }
        const spectrum = try allocator.alloc(f32, frames * basis_cols);
        defer allocator.free(spectrum);
        audio.blas.cblas_sgemm(audio.blas.row_major, audio.blas.no_trans, audio.blas.no_trans, @intCast(frames), @intCast(basis_cols), @intCast(frame_length), 1.0, frame_matrix.ptr, @intCast(frame_length), basis.ptr, @intCast(basis_cols), 0.0, spectrum.ptr, @intCast(basis_cols));
        // Magnitudes [frames, n_freq] (the front end feeds magnitudes, not power).
        const magnitudes = try allocator.alloc(f32, frames * n_freq);
        defer allocator.free(magnitudes);
        for (0..frames) |frame| {
            const src = spectrum[frame * basis_cols ..][0..basis_cols];
            const dst = magnitudes[frame * n_freq ..][0..n_freq];
            for (0..n_freq) |k| {
                const re = src[2 * k];
                const im = src[2 * k + 1];
                dst[k] = @sqrt(re * re + im * im);
            }
        }
        // Mel energies [frames, mel_bins] = magnitudes x filters^T.
        const mel = try allocator.alloc(f32, frames * mel_bins);
        defer allocator.free(mel);
        audio.blas.cblas_sgemm(audio.blas.row_major, audio.blas.no_trans, audio.blas.trans, @intCast(frames), @intCast(mel_bins), @intCast(n_freq), 1.0, magnitudes.ptr, @intCast(n_freq), filters.ptr, @intCast(n_freq), 0.0, mel.ptr, @intCast(mel_bins));
        for (0..frames) |frame| {
            for (0..mel_bins) |m| {
                out[frame * mel_bins + m] = if (mask[frame]) @log(mel[frame * mel_bins + m] + mel_floor) else 0.0;
            }
        }
        return;
    }

    const magnitudes = try allocator.alloc(f32, n_freq);
    defer allocator.free(magnitudes);
    var fft = try FrameFft.init(allocator, frame_length, fft_length);
    defer fft.deinit(allocator);
    for (0..frames) |frame| {
        try fft.magnitudes(samples[frame * hop_length ..][0..frame_length], window, magnitudes);
        for (0..mel_bins) |m| {
            var sum: f32 = 0.0;
            const filter = filters[m * n_freq ..][0..n_freq];
            for (magnitudes, 0..) |mag, k| sum += mag * filter[k];
            out[frame * mel_bins + m] = if (mask[frame]) @log(sum + mel_floor) else 0.0;
        }
    }
}

test "gemma4 blas log-mel matches the fft log-mel" {
    if (!audio.blas_available) return error.SkipZigTest;
    const allocator = std.testing.allocator;
    const frame_length: usize = 320;
    const hop_length: usize = 160;
    const fft_length: usize = 512;
    const mel_bins: usize = 128;
    const frames: usize = 40;
    const samples = try allocator.alloc(f32, hop_length * (frames - 1) + frame_length + 1);
    defer allocator.free(samples);
    var prng = std.Random.DefaultPrng.init(0x3e1);
    const random = prng.random();
    for (samples, 0..) |*value, i| {
        const t = @as(f32, @floatFromInt(i)) / 16000.0;
        value.* = 0.4 * @sin(2.0 * std.math.pi * 440.0 * t) + 0.2 * @sin(2.0 * std.math.pi * 3100.0 * t) + 0.05 * (random.float(f32) - 0.5);
    }
    const window = try allocator.alloc(f32, frame_length);
    defer allocator.free(window);
    for (window, 0..) |*value, i| {
        const t = @as(f32, @floatFromInt(i)) / @as(f32, @floatFromInt(frame_length));
        value.* = 0.5 - 0.5 * @cos(2.0 * std.math.pi * t);
    }
    const filters = try gemma4MelFilterbank(allocator, mel_bins, fft_length, 16000);
    defer allocator.free(filters);
    const mask = try allocator.alloc(bool, frames);
    defer allocator.free(mask);
    for (mask, 0..) |*flag, i| flag.* = i != 3;
    const via_blas = try allocator.alloc(f32, frames * mel_bins);
    defer allocator.free(via_blas);
    const via_fft = try allocator.alloc(f32, frames * mel_bins);
    defer allocator.free(via_fft);
    try gemma4LogMelFrames(allocator, samples, frames, hop_length, frame_length, fft_length, window, filters, mel_bins, 1e-3, mask, via_blas, true);
    try gemma4LogMelFrames(allocator, samples, frames, hop_length, frame_length, fft_length, window, filters, mel_bins, 1e-3, mask, via_fft, false);
    for (via_blas, via_fft, 0..) |a, b, i| {
        if (i / mel_bins == 3) {
            try std.testing.expectEqual(@as(f32, 0.0), a);
            continue;
        }
        try std.testing.expectApproxEqAbs(b, a, 2e-3);
    }
}

fn audioSubsample(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    store: *ProjectorWeights,
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

    const shape = [_]i32{ 1, 1, @intCast(features.frames), @intCast(features.mel_bins) };
    var hidden = try deviceResidentFromFloat32(cb, masked_features, &shape);
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
        var bias_buf: [128]u8 = undefined;
        const weight = try projectorConv2dWeightCt(cb, allocator, store, try fmt(&weight_buf, "a.conv1d.{d}.weight", .{layer}), 3, 3, channels, out_channels);
        const bias = try projectorZeroVectorCt(cb, allocator, store, try fmt(&bias_buf, "#zero_bias.{d}", .{out_channels}), out_channels);

        const conv = try cb.conv2d(hidden, weight, bias, 1, channels, out_channels, height, width, 3, 3, 2, 2, 1, 1, 1);
        cb.free(hidden);
        hidden = conv;
        const out_h = (height + 2 - 3) / 2 + 1;
        const out_w = (width + 2 - 3) / 2 + 1;

        const norm_name = try fmt(&norm_buf, "a.conv1d.{d}.norm.weight", .{layer});
        const normed = try channelNormRelu(cb, allocator, store, hidden, norm_name, out_channels, out_h, out_w, cfg.layer_norm_eps);
        cb.free(hidden);
        hidden = normed;
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

/// Layer norm over channels + relu for a `[1, channels, height, width]` conv
/// output, on device when the backend has the kernel.
fn channelNormRelu(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    store: *ProjectorWeights,
    input: CT,
    norm_name: []const u8,
    channels: usize,
    height: usize,
    width: usize,
    eps: f32,
) !CT {
    if (cb.vtable.channelLayerNormRelu != null and !audioHostOps().channel_norm) {
        const norm_w = try projectorVectorWeightCt(cb, allocator, store, norm_name, channels);
        if (try cb.channelLayerNormRelu(input, norm_w, channels, height * width, eps)) |normed| return normed;
    }
    const conv_data = try cb.toFloat32(input, allocator);
    defer allocator.free(conv_data);
    var norm = try loadTensorF32(store.gguf, norm_name);
    defer norm.deinit();
    try layerNormChannelsRelu(conv_data, 1, channels, height, width, norm.data, eps);
    const shape = [_]i32{ 1, @intCast(channels), @intCast(height), @intCast(width) };
    return cb.fromFloat32Shape(conv_data, &shape);
}

fn flattenAudioConvOutput(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    hidden: CT,
    time_steps: usize,
    freq_bins: usize,
    channels: usize,
) !CT {
    if (!audioHostOps().flatten) {
        if (try cb.flattenChannelsTimeFreq(hidden, time_steps, freq_bins, channels)) |flattened| return flattened;
    }
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
    store: *ProjectorWeights,
    cfg: Config,
    image_bytes: []const u8,
) !EncodedImage {
    if (cfg.direct_unified) {
        return encodeUnifiedDirectImage(cb, allocator, store, cfg, image_bytes);
    }

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
    try applyOptionalStandardization(allocator, store.gguf, pooled, cfg);

    const pooled_shape = [_]i32{ @intCast(geometry.tokenCount()), @intCast(cfg.vision_hidden) };
    const pooled_ct = try cb.fromFloat32Shape(pooled, &pooled_shape);
    defer cb.free(pooled_ct);
    const normed_pooled = try rmsNormNoScaleCt(cb, allocator, pooled_ct, geometry.tokenCount(), cfg.vision_hidden, cfg.layer_norm_eps);
    defer cb.free(normed_pooled);

    const projection_w = try loadLinearWeightCt(cb, allocator, store.gguf, "mm.input_projection.weight", cfg.vision_hidden, cfg.text_hidden);
    defer cb.free(projection_w);
    const projected = try cb.linearNoBias(normed_pooled, projection_w, geometry.tokenCount(), cfg.vision_hidden, cfg.text_hidden);
    defer cb.free(projected);

    return .{
        .embeddings = try cb.toFloat32(projected, allocator),
        .tokens = geometry.tokenCount(),
    };
}

fn encodeUnifiedDirectImage(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    store: *ProjectorWeights,
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

    const patches = try extractDirectImagePatches(allocator, pixel_values, cfg, geometry);
    defer allocator.free(patches);
    try applyLayerNormFromTensors(allocator, store, patches, geometry.tokenCount(), cfg.patch_size * cfg.patch_size * 3, "v.patch_norm.1", 1e-5);

    const patch_dim = cfg.patch_size * cfg.patch_size * 3;
    const patch_shape = [_]i32{ @intCast(geometry.tokenCount()), @intCast(patch_dim) };
    const patch_ct = try cb.fromFloat32Shape(patches, &patch_shape);
    defer cb.free(patch_ct);
    const patch_w = try loadLinearWeightCt(cb, allocator, store.gguf, "v.patch_embd.weight", patch_dim, cfg.vision_hidden);
    defer cb.free(patch_w);
    const patch_projected = try cb.linearNoBias(patch_ct, patch_w, geometry.tokenCount(), patch_dim, cfg.vision_hidden);
    defer cb.free(patch_projected);

    const hidden = try cb.toFloat32(patch_projected, allocator);
    defer allocator.free(hidden);
    try addBiasFromTensor(allocator, store, hidden, geometry.tokenCount(), cfg.vision_hidden, "v.patch_embd.bias");
    try applyLayerNormFromTensors(allocator, store, hidden, geometry.tokenCount(), cfg.vision_hidden, "v.patch_norm.2", 1e-5);
    try addPositionEmbeddingsInPlace(allocator, store, cfg, hidden, geometry);
    try applyLayerNormFromTensors(allocator, store, hidden, geometry.tokenCount(), cfg.vision_hidden, "v.patch_norm.3", 1e-5);

    const hidden_shape = [_]i32{ @intCast(geometry.tokenCount()), @intCast(cfg.vision_hidden) };
    const hidden_ct = try cb.fromFloat32Shape(hidden, &hidden_shape);
    defer cb.free(hidden_ct);
    const normed = try rmsNormNoScaleCt(cb, allocator, hidden_ct, geometry.tokenCount(), cfg.vision_hidden, cfg.layer_norm_eps);
    defer cb.free(normed);
    const projection_w = try loadLinearWeightCt(cb, allocator, store.gguf, "mm.input_projection.weight", cfg.vision_hidden, cfg.text_hidden);
    defer cb.free(projection_w);
    const projected = try cb.linearNoBias(normed, projection_w, geometry.tokenCount(), cfg.vision_hidden, cfg.text_hidden);
    defer cb.free(projected);

    return .{
        .embeddings = try cb.toFloat32(projected, allocator),
        .tokens = geometry.tokenCount(),
    };
}

fn extractDirectImagePatches(
    allocator: std.mem.Allocator,
    pixel_values: []const f32,
    cfg: Config,
    geometry: Geometry,
) ![]f32 {
    const patch_dim = cfg.patch_size * cfg.patch_size * 3;
    const token_count = geometry.tokenCount();
    if (pixel_values.len != 3 * geometry.height * geometry.width) return error.InvalidPatchEmbeddingShape;
    if (geometry.grid_x != geometry.pooled_x or geometry.grid_y != geometry.pooled_y) return error.InvalidPatchEmbeddingShape;

    const out = try allocator.alloc(f32, token_count * patch_dim);
    for (0..geometry.grid_y) |grid_y| {
        for (0..geometry.grid_x) |grid_x| {
            const token = grid_y * geometry.grid_x + grid_x;
            const dst_base = token * patch_dim;
            var dst: usize = dst_base;
            for (0..3) |channel| {
                for (0..cfg.patch_size) |py| {
                    const src_y = grid_y * cfg.patch_size + py;
                    const src_row = channel * geometry.height * geometry.width + src_y * geometry.width;
                    const src_x = grid_x * cfg.patch_size;
                    @memcpy(out[dst..][0..cfg.patch_size], pixel_values[src_row + src_x ..][0..cfg.patch_size]);
                    dst += cfg.patch_size;
                }
            }
        }
    }
    return out;
}

fn applyLayerNormFromTensors(
    allocator: std.mem.Allocator,
    store: *ProjectorWeights,
    data: []f32,
    rows: usize,
    dim: usize,
    prefix: []const u8,
    eps: f32,
) !void {
    const weight_name = try std.fmt.allocPrint(allocator, "{s}.weight", .{prefix});
    defer allocator.free(weight_name);
    const bias_name = try std.fmt.allocPrint(allocator, "{s}.bias", .{prefix});
    defer allocator.free(bias_name);
    var weight = try loadTensorF32(store.gguf, weight_name);
    defer weight.deinit();
    var bias = try loadTensorF32(store.gguf, bias_name);
    defer bias.deinit();
    try layerNormRowsInPlace(data, rows, dim, weight.data, bias.data, eps);
}

fn layerNormRowsInPlace(
    data: []f32,
    rows: usize,
    dim: usize,
    weight: []const f32,
    bias: []const f32,
    eps: f32,
) !void {
    if (data.len != rows * dim or weight.len != dim or bias.len != dim) return error.InvalidTensorShape;
    for (0..rows) |row| {
        const base = row * dim;
        var mean: f32 = 0.0;
        for (0..dim) |i| mean += data[base + i];
        mean /= @floatFromInt(dim);
        var variance: f32 = 0.0;
        for (0..dim) |i| {
            const delta = data[base + i] - mean;
            variance += delta * delta;
        }
        variance /= @floatFromInt(dim);
        const inv_std = 1.0 / @sqrt(variance + eps);
        for (0..dim) |i| {
            data[base + i] = (data[base + i] - mean) * inv_std * weight[i] + bias[i];
        }
    }
}

fn addBiasFromTensor(
    allocator: std.mem.Allocator,
    store: *ProjectorWeights,
    data: []f32,
    rows: usize,
    dim: usize,
    name: []const u8,
) !void {
    _ = allocator;
    var bias = try loadTensorF32(store.gguf, name);
    defer bias.deinit();
    if (data.len != rows * dim or bias.data.len != dim) return error.InvalidTensorShape;
    for (0..rows) |row| {
        const base = row * dim;
        for (0..dim) |i| data[base + i] += bias.data[i];
    }
}

fn parseConfig(file: *const gguf_format.File) !Config {
    const view = gguf_metadata.View.init(file);
    const arch = view.getString("general.architecture") orelse return error.InvalidGgufProjector;
    if (!std.mem.eql(u8, arch, "clip")) return error.InvalidGgufProjector;
    const projector_type = view.getString("clip.vision.projector_type") orelse return error.InvalidGgufProjector;
    if (!projector_format_mod.isGemma4ImageProjectorType(projector_type)) return error.UnsupportedGgufProjector;
    const block_count: usize = @intCast(view.getU64("clip.vision.block_count") orelse return error.InvalidGgufProjector);
    const direct_unified = std.mem.eql(u8, projector_type, "gemma4uv") and block_count == 0;
    const projection_scale_factor = std.math.cast(
        usize,
        view.getU64("clip.vision.projector_scale_factor") orelse default_spatial_merge_size,
    ) orelse return error.InvalidGgufProjector;
    const metadata_patch_size = std.math.cast(
        usize,
        view.getU64("clip.vision.patch_size") orelse return error.InvalidGgufProjector,
    ) orelse return error.InvalidGgufProjector;
    if (metadata_patch_size == 0 or (direct_unified and projection_scale_factor == 0)) {
        return error.InvalidGgufProjector;
    }
    const patch_size = if (direct_unified)
        std.math.mul(usize, metadata_patch_size, projection_scale_factor) catch
            return error.InvalidGgufProjector
    else
        metadata_patch_size;
    const patch_area = std.math.mul(usize, patch_size, patch_size) catch
        return error.InvalidGgufProjector;
    const patch_input = std.math.mul(usize, patch_area, 3) catch
        return error.InvalidGgufProjector;
    if (patch_input > std.math.maxInt(i32)) return error.InvalidGgufProjector;
    const vision_hidden = std.math.cast(
        usize,
        view.getU64("clip.vision.embedding_length") orelse return error.InvalidGgufProjector,
    ) orelse return error.InvalidGgufProjector;
    const spatial_merge_size: usize = if (direct_unified) 1 else default_spatial_merge_size;
    const position_embeddings_per_axis = std.math.cast(
        usize,
        projector_format_mod.gemma4PositionEmbeddingCapacity(
            file,
            @intCast(vision_hidden),
        ) catch return error.InvalidGgufProjector,
    ) orelse return error.InvalidGgufProjector;
    if (position_embeddings_per_axis < spatial_merge_size) {
        return error.InvalidGgufProjector;
    }

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
        .vision_hidden = vision_hidden,
        .intermediate_size = @intCast(view.getU64("clip.vision.feed_forward_length") orelse return error.InvalidGgufProjector),
        .block_count = block_count,
        .head_count = @intCast(view.getU64("clip.vision.attention.head_count") orelse return error.InvalidGgufProjector),
        .direct_unified = direct_unified,
        .image_size = @intCast(view.getU64("clip.vision.image_size") orelse return error.InvalidGgufProjector),
        .patch_size = patch_size,
        .layer_norm_eps = view.getF32("clip.vision.attention.layer_norm_epsilon") orelse 1e-6,
        .image_mean = image_mean,
        .image_std = image_std,
        .spatial_merge_size = spatial_merge_size,
        .position_embeddings_per_axis = position_embeddings_per_axis,
    };
}

/// Per-request tensors shared by every conformer block: the relative
/// position embeddings (input to each block's `attn_k_rel` projection) and
/// the frame validity mask as a 0/1 float row for the device attention kernel.
const AudioLayerInputs = struct {
    allocator: std.mem.Allocator,
    valid_mask: []const bool,
    rel_in: CT,
    valid_ct: CT,
    /// `[output_hidden]` ones for the final no-scale RMS norm.
    ones: CT,
    /// Per-block query scales (`q_scale * softplus(per_dim_scale)`), host
    /// copies for the reference path and device tensors for the kernel.
    scales_host: [][]f32,
    scales: []CT,

    fn init(cb: *const ComputeBackend, allocator: std.mem.Allocator, cfg: AudioConfig, valid_mask: []const bool, per_dim_scales: []const []const f32) !AudioLayerInputs {
        if (per_dim_scales.len != cfg.block_count) return error.InvalidTensorShape;
        const positions = try audioRelativePositionEmbeddings(allocator, cfg);
        defer allocator.free(positions);
        const rel_shape = [_]i32{ @intCast(cfg.attention_context_left), @intCast(cfg.audio_hidden) };
        const rel_in = try deviceResidentFromFloat32(cb, positions, &rel_shape);
        errdefer cb.free(rel_in);

        const valid = try allocator.alloc(f32, valid_mask.len);
        defer allocator.free(valid);
        for (valid_mask, 0..) |flag, i| valid[i] = if (flag) 1.0 else 0.0;
        const valid_shape = [_]i32{@intCast(valid_mask.len)};
        const valid_ct = try deviceResidentFromFloat32(cb, valid, &valid_shape);
        errdefer cb.free(valid_ct);

        const ones_host = try allocator.alloc(f32, cfg.output_hidden);
        defer allocator.free(ones_host);
        @memset(ones_host, 1.0);
        const ones_shape = [_]i32{@intCast(cfg.output_hidden)};
        const ones = try deviceResidentFromFloat32(cb, ones_host, &ones_shape);
        errdefer cb.free(ones);

        const head_dim = cfg.headDim();
        var scales_host = std.ArrayListUnmanaged([]f32).empty;
        errdefer {
            for (scales_host.items) |item| allocator.free(item);
            scales_host.deinit(allocator);
        }
        var scales = std.ArrayListUnmanaged(CT).empty;
        errdefer {
            for (scales.items) |item| cb.free(item);
            scales.deinit(allocator);
        }
        for (per_dim_scales) |per_dim| {
            if (per_dim.len != head_dim) return error.InvalidTensorShape;
            // Each item is owned by its list as soon as the append succeeds,
            // so the local cleanup must not outlive the append.
            const host = try audioQueryDimScales(allocator, head_dim, per_dim);
            {
                errdefer allocator.free(host);
                try scales_host.append(allocator, host);
            }
            const scales_shape = [_]i32{@intCast(head_dim)};
            const ct = try deviceResidentFromFloat32(cb, host, &scales_shape);
            {
                errdefer cb.free(ct);
                try scales.append(allocator, ct);
            }
        }
        const scales_host_slice = try scales_host.toOwnedSlice(allocator);
        errdefer {
            for (scales_host_slice) |item| allocator.free(item);
            allocator.free(scales_host_slice);
        }
        const scales_slice = try scales.toOwnedSlice(allocator);
        return .{
            .allocator = allocator,
            .valid_mask = valid_mask,
            .rel_in = rel_in,
            .valid_ct = valid_ct,
            .ones = ones,
            .scales_host = scales_host_slice,
            .scales = scales_slice,
        };
    }

    fn deinit(self: *AudioLayerInputs, cb: *const ComputeBackend) void {
        cb.free(self.rel_in);
        cb.free(self.valid_ct);
        cb.free(self.ones);
        for (self.scales_host) |item| self.allocator.free(item);
        self.allocator.free(self.scales_host);
        for (self.scales) |item| cb.free(item);
        self.allocator.free(self.scales);
    }
};

/// A small host tensor pushed to the device now rather than by its first
/// consumer, so nothing uploads inside the encoder frame.
fn deviceResidentFromFloat32(cb: *const ComputeBackend, data: []const f32, shape: []const i32) !CT {
    const host = try cb.fromFloat32Shape(data, shape);
    return cb.ensureDeviceResidentOwned(host);
}

/// Per-stage accounting for one clip, printed with
/// TERMITE_GEMMA4_AUDIO_METAL_PROFILE=1. Each mark flushes the open frame so
/// the elapsed time attributes to the ops since the previous mark; it
/// serializes the encoder and is for diagnosis only.
const AudioProfile = struct {
    const Bucket = enum { features, subsample, ffn, norms, attn_proj, attn_kernel, attn_out, lconv, tail, readback };
    cb: *const ComputeBackend,
    last_ns: u64 = 0,
    totals: [std.enums.values(Bucket).len]u64 = [_]u64{0} ** std.enums.values(Bucket).len,

    fn start(self: *AudioProfile) void {
        self.last_ns = platform.time.monotonicNs();
    }

    fn mark(self: *AudioProfile, bucket: Bucket) void {
        if (self.cb.decoderRuntimeHasActiveFrame()) self.cb.decoderRuntimeFlushActiveFrame() catch {};
        const now = platform.time.monotonicNs();
        self.totals[@intFromEnum(bucket)] += now -| self.last_ns;
        self.last_ns = now;
    }

    fn finish(self: *const AudioProfile, rows: usize) void {
        std.debug.print("gemma4_audio_metal rows={d}", .{rows});
        inline for (std.enums.values(Bucket)) |bucket| {
            std.debug.print(" {s}={d}us", .{ @tagName(bucket), self.totals[@intFromEnum(bucket)] / std.time.ns_per_us });
        }
        std.debug.print("\n", .{});
    }
};

var active_audio_profile: ?*AudioProfile = null;

fn audioProfileMark(bucket: AudioProfile.Bucket) void {
    if (active_audio_profile) |profile| profile.mark(bucket);
}

fn gemma4AudioMetalProfileEnabled() bool {
    return platform.env.getenvBool("TERMITE_GEMMA4_AUDIO_METAL_PROFILE");
}

fn gemma4AudioEncoderFrameEnabled() bool {
    if (@import("builtin").target.cpu.arch.isWasm()) return false;
    return !platform.env.getenvBool("TERMITE_GEMMA4_AUDIO_DISABLE_ENCODER_FRAME");
}

fn audioLayer(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    store: *ProjectorWeights,
    cfg: AudioConfig,
    input: CT,
    inputs: *const AudioLayerInputs,
    layer: usize,
) !CT {
    const rows = inputs.valid_mask.len;
    const ff1 = try audioFeedForward(cb, allocator, store, cfg, input, rows, layer, false);
    defer cb.free(ff1);
    audioProfileMark(.ffn);

    var buf: [128]u8 = undefined;
    const attn_pre = try projectorVectorWeightCt(cb, allocator, store, try fmt(&buf, "a.blk.{d}.attn_pre_norm.weight", .{layer}), cfg.audio_hidden);
    const normed = try cb.rmsNorm(ff1, attn_pre, cfg.audio_hidden, cfg.layer_norm_eps);
    defer cb.free(normed);
    audioProfileMark(.norms);
    const attn = try audioSelfAttention(cb, allocator, store, cfg, normed, inputs, layer);
    defer cb.free(attn);
    audioProfileMark(.attn_out);
    const attn_post = try projectorVectorWeightCt(cb, allocator, store, try fmt(&buf, "a.blk.{d}.attn_post_norm.weight", .{layer}), cfg.audio_hidden);
    const attn_normed = try cb.rmsNorm(attn, attn_post, cfg.audio_hidden, cfg.layer_norm_eps);
    defer cb.free(attn_normed);
    const res_attn = try cb.add(ff1, attn_normed);
    defer cb.free(res_attn);
    audioProfileMark(.norms);

    const lconv = try audioLightConv(cb, allocator, store, cfg, res_attn, rows, layer);
    defer cb.free(lconv);
    audioProfileMark(.lconv);
    const ff2 = try audioFeedForward(cb, allocator, store, cfg, lconv, rows, layer, true);
    defer cb.free(ff2);
    audioProfileMark(.ffn);
    const out_norm = try projectorVectorWeightCt(cb, allocator, store, try fmt(&buf, "a.blk.{d}.ln2.weight", .{layer}), cfg.audio_hidden);
    const out = try cb.rmsNorm(ff2, out_norm, cfg.audio_hidden, cfg.layer_norm_eps);
    audioProfileMark(.norms);
    return out;
}

fn audioFeedForward(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    store: *ProjectorWeights,
    cfg: AudioConfig,
    input: CT,
    rows: usize,
    layer: usize,
    second: bool,
) !CT {
    var buf: [128]u8 = undefined;
    const suffix = if (second) "_1" else "";
    const norm_w = try projectorVectorWeightCt(cb, allocator, store, try fmt(&buf, "a.blk.{d}.ffn_norm{s}.weight", .{ layer, suffix }), cfg.audio_hidden);
    const normed = try cb.rmsNorm(input, norm_w, cfg.audio_hidden, cfg.layer_norm_eps);
    defer cb.free(normed);
    const up = try linearNoBiasMaybeClipped(cb, allocator, store, normed, try fmt(&buf, "a.blk.{d}.ffn_up{s}", .{ layer, suffix }), rows, cfg.audio_hidden, cfg.intermediate_size);
    defer cb.free(up);
    const activated = try cb.silu(up);
    defer cb.free(activated);
    const down = try linearNoBiasMaybeClipped(cb, allocator, store, activated, try fmt(&buf, "a.blk.{d}.ffn_down{s}", .{ layer, suffix }), rows, cfg.intermediate_size, cfg.audio_hidden);
    defer cb.free(down);
    const post_w = try projectorVectorWeightCt(cb, allocator, store, try fmt(&buf, "a.blk.{d}.ffn_post_norm{s}.weight", .{ layer, suffix }), cfg.audio_hidden);
    const post = try cb.rmsNorm(down, post_w, cfg.audio_hidden, cfg.layer_norm_eps);
    defer cb.free(post);
    const scaled = try scaleRows(cb, allocator, post, rows, cfg.audio_hidden, cfg.residual_weight);
    defer cb.free(scaled);
    return cb.add(input, scaled);
}

/// `input * scale`, on device when the backend keeps scalar multiplies there.
fn scaleRows(cb: *const ComputeBackend, allocator: std.mem.Allocator, input: CT, rows: usize, dim: usize, scale: f32) !CT {
    if (!audioHostOps().scale) {
        if (try cb.multiplyScalar(input, scale)) |scaled| return scaled;
    }
    const data = try cb.toFloat32(input, allocator);
    defer allocator.free(data);
    if (data.len != rows * dim) return error.InvalidTensorShape;
    for (data) |*value| value.* *= scale;
    const shape = [_]i32{ @intCast(rows), @intCast(dim) };
    return cb.fromFloat32Shape(data, &shape);
}

fn audioLightConv(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    store: *ProjectorWeights,
    cfg: AudioConfig,
    input: CT,
    rows: usize,
    layer: usize,
) !CT {
    var buf: [128]u8 = undefined;
    const pre_w = try projectorVectorWeightCt(cb, allocator, store, try fmt(&buf, "a.blk.{d}.norm_conv.weight", .{layer}), cfg.audio_hidden);
    const normed = try cb.rmsNorm(input, pre_w, cfg.audio_hidden, cfg.layer_norm_eps);
    defer cb.free(normed);
    const pw1 = try linearNoBiasMaybeClipped(cb, allocator, store, normed, try fmt(&buf, "a.blk.{d}.conv_pw1", .{layer}), rows, cfg.audio_hidden, cfg.audio_hidden * 2);
    defer cb.free(pw1);
    const glu = try applyGlu(cb, allocator, pw1, rows, cfg.audio_hidden);
    defer cb.free(glu);
    const conv = try depthwiseCausalConv1d(cb, allocator, store, glu, rows, cfg.audio_hidden, cfg.conv_kernel_size, try fmt(&buf, "a.blk.{d}.conv_dw.weight", .{layer}));
    defer cb.free(conv);
    const conv_norm_w = try projectorVectorWeightCt(cb, allocator, store, try fmt(&buf, "a.blk.{d}.conv_norm.weight", .{layer}), cfg.audio_hidden);
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
    store: *ProjectorWeights,
    cfg: AudioConfig,
    input: CT,
    inputs: *const AudioLayerInputs,
    layer: usize,
) !CT {
    const valid_mask = inputs.valid_mask;
    const rows = valid_mask.len;
    var buf: [128]u8 = undefined;
    const q = try linearNoBiasMaybeClipped(cb, allocator, store, input, try fmt(&buf, "a.blk.{d}.attn_q", .{layer}), rows, cfg.audio_hidden, cfg.audio_hidden);
    defer cb.free(q);
    const k = try linearNoBiasMaybeClipped(cb, allocator, store, input, try fmt(&buf, "a.blk.{d}.attn_k", .{layer}), rows, cfg.audio_hidden, cfg.audio_hidden);
    defer cb.free(k);
    const v = try linearNoBiasMaybeClipped(cb, allocator, store, input, try fmt(&buf, "a.blk.{d}.attn_v", .{layer}), rows, cfg.audio_hidden, cfg.audio_hidden);
    defer cb.free(v);
    const rel = try linearNoBiasMaybeClipped(cb, allocator, store, inputs.rel_in, try fmt(&buf, "a.blk.{d}.attn_k_rel", .{layer}), cfg.attention_context_left, cfg.audio_hidden, cfg.audio_hidden);
    defer cb.free(rel);
    audioProfileMark(.attn_proj);

    const out_ct = try audioLocalAttention(cb, allocator, cfg, q, k, v, rel, layer, inputs);
    defer cb.free(out_ct);
    audioProfileMark(.attn_kernel);
    return linearNoBiasMaybeClipped(cb, allocator, store, out_ct, try fmt(&buf, "a.blk.{d}.attn_out", .{layer}), rows, cfg.audio_hidden, cfg.audio_hidden);
}

fn audioQueryDimScales(allocator: std.mem.Allocator, head_dim: usize, per_dim_scale: []const f32) ![]f32 {
    const q_scale = @as(f32, @floatFromInt(1)) / @sqrt(@as(f32, @floatFromInt(head_dim))) / @log(@as(f32, 2.0));
    const scales = try allocator.alloc(f32, head_dim);
    for (scales, 0..) |*scale, i| scale.* = q_scale * softplus(per_dim_scale[i]);
    return scales;
}

fn audioKeyScale() f32 {
    return @log(@as(f32, 1.0) + std.math.e) / @log(@as(f32, 2.0));
}

/// Chunked local attention on device when the backend has the kernel, else
/// the host reference.
fn audioLocalAttention(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    cfg: AudioConfig,
    q: CT,
    k: CT,
    v: CT,
    rel: CT,
    layer: usize,
    inputs: *const AudioLayerInputs,
) !CT {
    const rows = inputs.valid_mask.len;
    if (layer >= inputs.scales.len) return error.InvalidTensorShape;
    if (cfg.attention_context_right == 0 and !audioHostOps().attention) {
        if (try cb.gemma4AudioLocalAttention(q, k, v, rel, inputs.scales[layer], inputs.valid_ct, audioLocalAttentionParams(cfg, rows))) |out| return out;
    }

    const q_data = try cb.toFloat32(q, allocator);
    defer allocator.free(q_data);
    const k_data = try cb.toFloat32(k, allocator);
    defer allocator.free(k_data);
    const v_data = try cb.toFloat32(v, allocator);
    defer allocator.free(v_data);
    const rel_data = try cb.toFloat32(rel, allocator);
    defer allocator.free(rel_data);
    if (cfg.attention_context_right != 0) return error.UnsupportedTensorType;
    const out = try audioLocalAttentionReference(allocator, audioLocalAttentionParams(cfg, rows), q_data, k_data, v_data, rel_data, inputs.scales_host[layer], inputs.valid_mask);
    defer allocator.free(out);
    const out_shape = [_]i32{ @intCast(rows), @intCast(cfg.audio_hidden) };
    return cb.fromFloat32Shape(out, &out_shape);
}

/// Windowed frame magnitudes through the shared radix FFT: the frame is
/// zero-padded to `fft_len`, so this equals the direct DFT the front end used
/// to evaluate per bin (`rfftMagnitudeNaive`, kept as the test reference).
const FrameFft = struct {
    plan: inference_audio.FftPlan,
    padded: []f32,
    ones: []f32,
    power: []f32,

    fn init(allocator: std.mem.Allocator, frame_len: usize, fft_len: usize) !FrameFft {
        if (frame_len > fft_len) return error.InvalidTensorShape;
        var plan = try inference_audio.FftPlan.init(allocator, fft_len);
        errdefer plan.deinit(allocator);
        const padded = try allocator.alloc(f32, fft_len);
        errdefer allocator.free(padded);
        const ones = try allocator.alloc(f32, fft_len);
        errdefer allocator.free(ones);
        @memset(ones, 1.0);
        const power = try allocator.alloc(f32, plan.n_freq);
        return .{ .plan = plan, .padded = padded, .ones = ones, .power = power };
    }

    fn deinit(self: *FrameFft, allocator: std.mem.Allocator) void {
        self.plan.deinit(allocator);
        allocator.free(self.padded);
        allocator.free(self.ones);
        allocator.free(self.power);
    }

    fn magnitudes(self: *FrameFft, frame: []const f32, window: []const f32, out: []f32) !void {
        if (frame.len != window.len or frame.len > self.padded.len or out.len != self.plan.n_freq) return error.InvalidTensorShape;
        @memset(self.padded, 0.0);
        for (frame, window, 0..) |sample, w, i| self.padded[i] = sample * w;
        try self.plan.powerSpectrumWindowed(self.padded, self.ones, self.power);
        for (out, self.power[0..out.len]) |*value, p| value.* = @sqrt(p);
    }
};

test "gemma4 frame fft matches the direct dft" {
    const allocator = std.testing.allocator;
    const frame_len: usize = 320;
    const fft_len: usize = 512;
    var frame: [frame_len]f32 = undefined;
    var window: [frame_len]f32 = undefined;
    var prng = std.Random.DefaultPrng.init(0x6e11);
    const random = prng.random();
    for (&frame, 0..) |*s, i| {
        const t = @as(f32, @floatFromInt(i)) / 16000.0;
        s.* = 0.5 * @sin(2.0 * std.math.pi * 440.0 * t) + 0.2 * @sin(2.0 * std.math.pi * 2750.0 * t) + 0.1 * (random.float(f32) - 0.5);
    }
    for (&window, 0..) |*w, i| {
        const t = @as(f32, @floatFromInt(i)) / @as(f32, @floatFromInt(frame_len));
        w.* = 0.5 - 0.5 * @cos(2.0 * std.math.pi * t);
    }
    var expected: [fft_len / 2 + 1]f32 = undefined;
    try rfftMagnitudeNaive(&frame, &window, &expected, fft_len);
    var fft = try FrameFft.init(allocator, frame_len, fft_len);
    defer fft.deinit(allocator);
    var got: [fft_len / 2 + 1]f32 = undefined;
    try fft.magnitudes(&frame, &window, &got);
    var peak: f32 = 0;
    for (expected) |e| peak = @max(peak, e);
    for (expected, got) |e, g| try std.testing.expect(@abs(e - g) <= 1e-3 * peak + 1e-4);
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

/// A conv2d weight held for the request as `[out, in, kh, kw]`, pushed to
/// the device once so the conv joins the encoder frame without an upload of
/// its own.
fn projectorConv2dWeightCt(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    store: *ProjectorWeights,
    name: []const u8,
    kernel_h: usize,
    kernel_w: usize,
    in_channels: usize,
    out_channels: usize,
) !CT {
    const Load = struct {
        cb: *const ComputeBackend,
        allocator: std.mem.Allocator,
        store: *ProjectorWeights,
        name: []const u8,
        kernel_h: usize,
        kernel_w: usize,
        in_channels: usize,
        out_channels: usize,
        fn call(self: @This()) !CT {
            const host = try loadConv2dWeightCt(self.cb, self.allocator, self.store.gguf, self.name, self.kernel_h, self.kernel_w, self.in_channels, self.out_channels);
            return self.cb.ensureDeviceResidentOwned(host);
        }
    };
    return store.cached(name, Load{ .cb = cb, .allocator = allocator, .store = store, .name = name, .kernel_h = kernel_h, .kernel_w = kernel_w, .in_channels = in_channels, .out_channels = out_channels });
}

/// A zero vector of `len` held for the request under a synthetic name (the
/// conv stack's bias-free convolutions still take a bias operand).
fn projectorZeroVectorCt(cb: *const ComputeBackend, allocator: std.mem.Allocator, store: *ProjectorWeights, name: []const u8, len: usize) !CT {
    const Load = struct {
        cb: *const ComputeBackend,
        allocator: std.mem.Allocator,
        len: usize,
        fn call(self: @This()) !CT {
            const zeros = try self.allocator.alloc(f32, self.len);
            defer self.allocator.free(zeros);
            @memset(zeros, 0.0);
            const shape = [_]i32{@intCast(self.len)};
            return deviceResidentFromFloat32(self.cb, zeros, &shape);
        }
    };
    return store.cached(name, Load{ .cb = cb, .allocator = allocator, .len = len });
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
    store: *ProjectorWeights,
    input: CT,
    prefix: []const u8,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !CT {
    const linear = try linearNoBiasMaybeClipped(cb, allocator, store, input, prefix, rows, in_dim, out_dim);
    defer cb.free(linear);

    const bias_name = try std.fmt.allocPrint(allocator, "{s}.bias", .{prefix});
    defer allocator.free(bias_name);
    if (!audioHostOps().bias) {
        const bias = try projectorVectorWeightCt(cb, allocator, store, bias_name, out_dim);
        return cb.add(linear, bias);
    }
    const data = try cb.toFloat32(linear, allocator);
    defer allocator.free(data);
    var bias = try loadTensorF32(store.gguf, bias_name);
    defer bias.deinit();
    if (bias.data.len != out_dim or data.len != rows * out_dim) return error.InvalidTensorShape;
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
    if (!audioHostOps().glu) {
        if (try cb.gluRows(input, rows, hidden)) |glu| return glu;
    }
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
    store: *ProjectorWeights,
    input: CT,
    rows: usize,
    hidden: usize,
    kernel_size: usize,
    weight_name: []const u8,
) !CT {
    if (cb.vtable.depthwiseCausalConv1d != null and !audioHostOps().dwconv) {
        const weight_ct = try depthwiseConvWeightCt(cb, allocator, store, weight_name, kernel_size, hidden);
        if (try cb.depthwiseCausalConv1d(input, weight_ct, rows, hidden, kernel_size)) |conv| return conv;
    }

    const input_data = try cb.toFloat32(input, allocator);
    defer allocator.free(input_data);
    if (input_data.len != rows * hidden) return error.InvalidTensorShape;
    var weight = try loadTensorF32(store.gguf, weight_name);
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

/// The depthwise conv weight as `[kernel_size, hidden]`: the session's
/// resident copy when it has that layout, else loaded (and transposed from
/// `[hidden, kernel_size]` when the file stores it that way).
fn depthwiseConvWeightCt(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    store: *ProjectorWeights,
    name: []const u8,
    kernel_size: usize,
    hidden: usize,
) !CT {
    const Load = struct {
        cb: *const ComputeBackend,
        allocator: std.mem.Allocator,
        store: *ProjectorWeights,
        name: []const u8,
        kernel_size: usize,
        hidden: usize,
        fn call(self: @This()) !CT {
            if (!audioHostOps().resident) {
                if (self.cb.getWeight(self.name)) |resident| {
                    const expected = [_]i64{ @intCast(self.kernel_size), @intCast(self.hidden) };
                    if ((self.cb.tensorShapeMatches(resident, &expected) catch null) orelse false) return resident;
                    self.cb.free(resident);
                } else |_| {}
            }
            var weight = try loadTensorF32(self.store.gguf, self.name);
            defer weight.deinit();
            if (weight.data.len != self.kernel_size * self.hidden or weight.shape.len < 2) return error.InvalidTensorShape;
            const d0: usize = @intCast(weight.shape[0]);
            const d1: usize = @intCast(weight.shape[1]);
            const shape = [_]i32{ @intCast(self.kernel_size), @intCast(self.hidden) };
            if (d0 == self.kernel_size and d1 == self.hidden) return self.cb.fromFloat32Shape(weight.data, &shape);
            if (d0 == self.hidden and d1 == self.kernel_size) {
                const transposed = try transposeMatrix(self.allocator, weight.data, self.hidden, self.kernel_size);
                defer self.allocator.free(transposed);
                return self.cb.fromFloat32Shape(transposed, &shape);
            }
            return error.InvalidTensorShape;
        }
    };
    return store.cached(name, Load{ .cb = cb, .allocator = allocator, .store = store, .name = name, .kernel_size = kernel_size, .hidden = hidden });
}

fn audioLocalAttentionParams(cfg: AudioConfig, rows: usize) ops.Gemma4AudioLocalAttentionParams {
    return .{
        .rows = rows,
        .hidden = cfg.audio_hidden,
        .heads = cfg.head_count,
        .head_dim = cfg.headDim(),
        .chunk = cfg.attention_chunk_size,
        .context_left = cfg.attention_context_left,
        .context = cfg.attentionContextSize(),
        .k_scale = audioKeyScale(),
        .logit_cap = cfg.attention_logit_cap,
        .invalid_value = cfg.attention_invalid_logits_value,
    };
}

/// Host reference for the Gemma 4 audio local attention (the Metal kernel
/// is checked against it).
pub fn audioLocalAttentionReference(
    allocator: std.mem.Allocator,
    params: ops.Gemma4AudioLocalAttentionParams,
    q_data: []const f32,
    k_data: []const f32,
    v_data: []const f32,
    rel_data: []const f32,
    q_dim_scales: []const f32,
    valid_mask: []const bool,
) ![]f32 {
    const rows = valid_mask.len;
    const hidden = params.hidden;
    const head_dim = params.head_dim;
    const heads = params.heads;
    const context = params.context;
    const past = params.context_left - 1;
    const k_scale = params.k_scale;
    if (rows != params.rows or heads * head_dim != hidden) return error.InvalidTensorShape;
    if (q_data.len != rows * hidden or k_data.len != rows * hidden or v_data.len != rows * hidden) return error.InvalidTensorShape;
    if (rel_data.len != params.context_left * hidden or q_dim_scales.len != head_dim) return error.InvalidTensorShape;

    const out = try allocator.alloc(f32, rows * hidden);
    @memset(out, 0.0);
    errdefer allocator.free(out);
    const scores = try allocator.alloc(f32, context);
    defer allocator.free(scores);

    for (0..rows) |q_idx| {
        if (!valid_mask[q_idx]) continue;
        const block_start = (q_idx / params.chunk) * params.chunk;
        const q_off = q_idx - block_start;
        for (0..heads) |head| {
            const head_base = head * head_dim;
            var max_score = -std.math.inf(f32);
            var valid_score_count: usize = 0;
            for (0..context) |c| {
                const rel_idx_signed: isize = @as(isize, @intCast(c)) - @as(isize, @intCast(q_off));
                const k_idx_signed: isize = @as(isize, @intCast(block_start + c)) - @as(isize, @intCast(past));
                if (rel_idx_signed < 0 or rel_idx_signed >= @as(isize, @intCast(params.context_left)) or k_idx_signed < 0 or k_idx_signed >= @as(isize, @intCast(rows))) {
                    scores[c] = params.invalid_value;
                    continue;
                }
                const k_idx: usize = @intCast(k_idx_signed);
                if (!valid_mask[k_idx] or k_idx > q_idx or q_idx - k_idx > past) {
                    scores[c] = params.invalid_value;
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
                score = std.math.tanh(score / params.logit_cap) * params.logit_cap;
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
    if (!projector_format_mod.isGemma4AudioProjectorType(projector_type)) return error.UnsupportedGgufProjector;
    const block_count: usize = @intCast(view.getU64("clip.audio.block_count") orelse return error.InvalidGgufProjector);
    const direct_unified = std.mem.eql(u8, projector_type, "gemma4ua") or
        (std.mem.eql(u8, projector_type, "gemma4uv") and block_count == 0);
    const text_hidden: usize = @intCast(view.getU64("clip.audio.projection_dim") orelse return error.InvalidGgufProjector);
    const output_hidden = if (direct_unified)
        text_hidden
    else
        try audioProjectionInputWidth(file, text_hidden);

    return .{
        .text_hidden = text_hidden,
        .audio_hidden = @intCast(view.getU64("clip.audio.embedding_length") orelse return error.InvalidGgufProjector),
        .output_hidden = output_hidden,
        .intermediate_size = @intCast(view.getU64("clip.audio.feed_forward_length") orelse return error.InvalidGgufProjector),
        .block_count = block_count,
        .head_count = @intCast(view.getU64("clip.audio.attention.head_count") orelse return error.InvalidGgufProjector),
        .direct_unified = direct_unified,
        .raw_samples_per_token = if (direct_unified) @intCast(view.getU64("clip.audio.samples_per_token") orelse view.getU64("clip.audio.embedding_length") orelse 640) else 640,
        .max_direct_audio_tokens = @intCast(view.getU64("clip.audio.max_tokens") orelse default_max_direct_audio_tokens),
        .mel_bins = @intCast(view.getU64("clip.audio.num_mel_bins") orelse 128),
        .layer_norm_eps = view.getF32("clip.audio.attention.layer_norm_epsilon") orelse 1e-5,
    };
}

fn audioProjectionInputWidth(file: *const gguf_format.File, text_hidden: usize) !usize {
    for (file.tensors) |tensor| {
        if (!std.mem.eql(u8, tensor.name, "mm.a.input_projection.weight")) continue;
        if (tensor.dimensions.len != 2) return error.InvalidGgufProjector;
        const first = std.math.cast(usize, tensor.dimensions[0]) orelse return error.InvalidGgufProjector;
        const second = std.math.cast(usize, tensor.dimensions[1]) orelse return error.InvalidGgufProjector;
        const input = if (first == text_hidden)
            second
        else if (second == text_hidden)
            first
        else
            return error.InvalidGgufProjector;
        if (input == 0) return error.InvalidGgufProjector;
        return input;
    }
    return error.InvalidGgufProjector;
}

test "gemma4 unified projector metadata parses image and audio configs" {
    const allocator = std.testing.allocator;
    const metadata = [_]gguf_format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "clip" } },
        .{ .key = "clip.vision.projector_type", .value = .{ .string = "gemma4uv" } },
        .{ .key = "clip.vision.projection_dim", .value = .{ .u32 = 3840 } },
        .{ .key = "clip.vision.embedding_length", .value = .{ .u32 = 3840 } },
        .{ .key = "clip.vision.feed_forward_length", .value = .{ .u32 = 0 } },
        .{ .key = "clip.vision.block_count", .value = .{ .u32 = 0 } },
        .{ .key = "clip.vision.attention.head_count", .value = .{ .u32 = 0 } },
        .{ .key = "clip.vision.image_size", .value = .{ .u32 = 224 } },
        .{ .key = "clip.vision.patch_size", .value = .{ .u32 = 16 } },
        .{ .key = "clip.vision.attention.layer_norm_epsilon", .value = .{ .f32 = 0.000001 } },
        .{ .key = "clip.audio.projector_type", .value = .{ .string = "gemma4ua" } },
        .{ .key = "clip.audio.projection_dim", .value = .{ .u32 = 3840 } },
        .{ .key = "clip.audio.embedding_length", .value = .{ .u32 = 640 } },
        .{ .key = "clip.audio.feed_forward_length", .value = .{ .u32 = 0 } },
        .{ .key = "clip.audio.block_count", .value = .{ .u32 = 0 } },
        .{ .key = "clip.audio.attention.head_count", .value = .{ .u32 = 0 } },
        .{ .key = "clip.audio.num_mel_bins", .value = .{ .u32 = 128 } },
        .{ .key = "clip.audio.attention.layer_norm_epsilon", .value = .{ .f32 = 0.000001 } },
    };
    const position_dims = [_]u64{ 2, 64, 3840 };
    const tensors = [_]@import("../gguf/writer.zig").TensorSpec{
        .{
            .name = "v.position_embd.weight",
            .dimensions = &position_dims,
            .tensor_type = .{ .known = .F32 },
        },
    };
    var layout = try @import("../gguf/writer.zig").buildLayout(
        allocator,
        &metadata,
        &tensors,
    );
    defer layout.deinit(allocator);
    var parsed = try gguf_format.parse(allocator, layout.header_bytes);
    defer parsed.deinit(allocator);

    const image_cfg = try parseConfig(&parsed);
    try std.testing.expectEqual(@as(usize, 3840), image_cfg.text_hidden);
    try std.testing.expectEqual(@as(usize, 3840), image_cfg.vision_hidden);
    try std.testing.expectEqual(@as(usize, 224), image_cfg.image_size);
    try std.testing.expectEqual(@as(usize, 48), image_cfg.patch_size);
    try std.testing.expectEqual(@as(usize, 64), image_cfg.position_embeddings_per_axis);
    try std.testing.expect(image_cfg.direct_unified);

    const audio_cfg = try parseAudioConfig(&parsed);
    try std.testing.expectEqual(@as(usize, 3840), audio_cfg.text_hidden);
    try std.testing.expectEqual(@as(usize, 640), audio_cfg.audio_hidden);
    try std.testing.expectEqual(@as(usize, 640), audio_cfg.raw_samples_per_token);
    try std.testing.expect(audio_cfg.direct_unified);
}

test "regular gemma4 audio config derives its projection input width from GGUF" {
    const allocator = std.testing.allocator;
    const metadata = [_]gguf_format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "clip" } },
        .{ .key = "clip.audio.projector_type", .value = .{ .string = "gemma4a" } },
        .{ .key = "clip.audio.projection_dim", .value = .{ .u32 = 6 } },
        .{ .key = "clip.audio.embedding_length", .value = .{ .u32 = 4 } },
        .{ .key = "clip.audio.feed_forward_length", .value = .{ .u32 = 8 } },
        .{ .key = "clip.audio.block_count", .value = .{ .u32 = 1 } },
        .{ .key = "clip.audio.attention.head_count", .value = .{ .u32 = 2 } },
    };
    const projection_dims = [_]u64{ 6, 5 };
    const tensors = [_]@import("../gguf/writer.zig").TensorSpec{.{
        .name = "mm.a.input_projection.weight",
        .dimensions = &projection_dims,
        .tensor_type = .{ .known = .F32 },
    }};
    var layout = try @import("../gguf/writer.zig").buildLayout(allocator, &metadata, &tensors);
    defer layout.deinit(allocator);
    var parsed = try gguf_format.parse(allocator, layout.header_bytes);
    defer parsed.deinit(allocator);

    const cfg = try parseAudioConfig(&parsed);
    try std.testing.expectEqual(@as(usize, 4), cfg.audio_hidden);
    try std.testing.expectEqual(@as(usize, 5), cfg.output_hidden);
    try std.testing.expectEqual(@as(usize, 6), cfg.text_hidden);
    try std.testing.expect(!cfg.direct_unified);
}

test "gemma4 unified projector rejects invalid effective patch geometry" {
    const allocator = std.testing.allocator;
    const cases = [_]struct {
        patch_size: u32,
        scale_factor: u32,
    }{
        .{ .patch_size = 16, .scale_factor = 0 },
        .{ .patch_size = std.math.maxInt(u32), .scale_factor = std.math.maxInt(u32) },
    };
    for (cases) |case| {
        const metadata = [_]gguf_format.MetadataEntry{
            .{ .key = "general.architecture", .value = .{ .string = "clip" } },
            .{ .key = "clip.vision.projector_type", .value = .{ .string = "gemma4uv" } },
            .{ .key = "clip.vision.projection_dim", .value = .{ .u32 = 4 } },
            .{ .key = "clip.vision.embedding_length", .value = .{ .u32 = 4 } },
            .{ .key = "clip.vision.feed_forward_length", .value = .{ .u32 = 0 } },
            .{ .key = "clip.vision.block_count", .value = .{ .u32 = 0 } },
            .{ .key = "clip.vision.attention.head_count", .value = .{ .u32 = 0 } },
            .{ .key = "clip.vision.image_size", .value = .{ .u32 = 224 } },
            .{ .key = "clip.vision.patch_size", .value = .{ .u32 = case.patch_size } },
            .{ .key = "clip.vision.projector_scale_factor", .value = .{ .u32 = case.scale_factor } },
        };
        var layout = try @import("../gguf/writer.zig").buildLayout(allocator, &metadata, &.{});
        defer layout.deinit(allocator);
        var parsed = try gguf_format.parse(allocator, layout.header_bytes);
        defer parsed.deinit(allocator);

        try std.testing.expectError(error.InvalidGgufProjector, parseConfig(&parsed));
    }
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
    const budget_max_grid =
        (max_patches / (cfg.spatial_merge_size * cfg.spatial_merge_size)) *
        cfg.spatial_merge_size;
    const position_max_grid = if (cfg.position_embeddings_per_axis == 0)
        budget_max_grid
    else
        floorToMultiple(
            @min(cfg.position_embeddings_per_axis, budget_max_grid),
            cfg.spatial_merge_size,
        );
    std.debug.assert(position_max_grid >= cfg.spatial_merge_size);
    const max_side = position_max_grid * cfg.patch_size;
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
    if (target_w > max_side or target_h > max_side) {
        const position_scale = @min(
            @as(f64, @floatFromInt(max_side)) / @as(f64, @floatFromInt(target_w)),
            @as(f64, @floatFromInt(max_side)) / @as(f64, @floatFromInt(target_h)),
        );
        target_w = @min(
            @max(
                floorToMultiple(
                    @intFromFloat(@floor(@as(f64, @floatFromInt(target_w)) * position_scale)),
                    block,
                ),
                block,
            ),
            max_side,
        );
        target_h = @min(
            @max(
                floorToMultiple(
                    @intFromFloat(@floor(@as(f64, @floatFromInt(target_h)) * position_scale)),
                    block,
                ),
                block,
            ),
            max_side,
        );
    }
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

test "gemma4 target geometry respects learned position capacity" {
    const cfg = Config{
        .text_hidden = 4,
        .vision_hidden = 4,
        .intermediate_size = 0,
        .block_count = 0,
        .head_count = 0,
        .direct_unified = true,
        .image_size = 224,
        .patch_size = 2,
        .layer_norm_eps = 1e-6,
        .image_mean = .{ 0.0, 0.0, 0.0 },
        .image_std = .{ 1.0, 1.0, 1.0 },
        .spatial_merge_size = 1,
        .position_embeddings_per_axis = 8,
    };
    const geometry = targetGeometry(cfg, 4096, 64);
    try std.testing.expect(geometry.grid_x <= cfg.position_embeddings_per_axis);
    try std.testing.expect(geometry.grid_y <= cfg.position_embeddings_per_axis);
    try std.testing.expect(geometry.grid_x * geometry.grid_y <= cfg.maxPatchCount());
    try std.testing.expect(geometry.grid_x > 0);
    try std.testing.expect(geometry.grid_y > 0);
}

fn floorToMultiple(value: usize, multiple: usize) usize {
    if (multiple == 0) return value;
    return (value / multiple) * multiple;
}

fn patchEmbed(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    store: *ProjectorWeights,
    cfg: Config,
    pixel_values: []const f32,
    geometry: Geometry,
) ![]f32 {
    const scaled_pixels = try allocator.dupe(f32, pixel_values);
    defer allocator.free(scaled_pixels);
    for (scaled_pixels) |*value| value.* = 2.0 * (value.* - 0.5);

    const patch_w = try loadWeightCt(cb, allocator, store.gguf, "v.patch_embd.weight");
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
    store: *ProjectorWeights,
    cfg: Config,
    patch_embeddings: []const f32,
    geometry: Geometry,
) ![]f32 {
    const patch_count = geometry.grid_x * geometry.grid_y;
    const out = try allocator.alloc(f32, patch_count * cfg.vision_hidden);
    @memcpy(out, patch_embeddings);
    errdefer allocator.free(out);
    try addPositionEmbeddingsInPlace(allocator, store, cfg, out, geometry);
    return out;
}

fn addPositionEmbeddingsInPlace(
    allocator: std.mem.Allocator,
    store: *ProjectorWeights,
    cfg: Config,
    patch_embeddings: []f32,
    geometry: Geometry,
) !void {
    _ = allocator;
    var pos = try loadTensorF32(store.gguf, "v.position_embd.weight");
    defer pos.deinit();
    const hidden_first = pos.shape.len == 3 and
        pos.shape[0] == @as(i64, @intCast(cfg.vision_hidden)) and
        pos.shape[2] == 2;
    const axis_first = pos.shape.len == 3 and
        pos.shape[0] == 2 and
        pos.shape[2] == @as(i64, @intCast(cfg.vision_hidden));
    if (!hidden_first and !axis_first) {
        return error.InvalidPositionEmbeddingShape;
    }
    const positions_per_axis: usize = @intCast(pos.shape[1]);
    if (geometry.grid_x > positions_per_axis or geometry.grid_y > positions_per_axis) {
        return error.InvalidPositionEmbeddingShape;
    }

    const patch_count = geometry.grid_x * geometry.grid_y;
    if (patch_embeddings.len != patch_count * cfg.vision_hidden) return error.InvalidPositionEmbeddingShape;
    for (0..geometry.grid_y) |y| {
        for (0..geometry.grid_x) |x| {
            const patch_idx = y * geometry.grid_x + x;
            const dst = patch_idx * cfg.vision_hidden;
            for (0..cfg.vision_hidden) |h| {
                patch_embeddings[dst + h] += positionEmbeddingValue(pos.data, cfg.vision_hidden, positions_per_axis, 0, x, h, hidden_first) +
                    positionEmbeddingValue(pos.data, cfg.vision_hidden, positions_per_axis, 1, y, h, hidden_first);
            }
        }
    }
}

fn positionEmbeddingValue(
    data: []const f32,
    hidden: usize,
    positions_per_axis: usize,
    axis: usize,
    position: usize,
    h: usize,
    hidden_first: bool,
) f32 {
    return if (hidden_first)
        data[(h * positions_per_axis + position) * 2 + axis]
    else
        data[(axis * positions_per_axis + position) * hidden + h];
}

test "gemma4 position embedding indexes GGUF-normalized layouts" {
    const hidden: usize = 3;
    const positions: usize = 4;
    const axis_hidden_raw = [_]f32{
        100, 101, 102, 110, 111, 112, 120, 121, 122, 130, 131, 132,
        200, 201, 202, 210, 211, 212, 220, 221, 222, 230, 231, 232,
    };
    try std.testing.expectEqual(@as(f32, 111), positionEmbeddingValue(&axis_hidden_raw, hidden, positions, 0, 1, 1, false));
    try std.testing.expectEqual(@as(f32, 221), positionEmbeddingValue(&axis_hidden_raw, hidden, positions, 1, 2, 1, false));

    const hidden_axis_raw = [_]f32{
        100, 200, 110, 210, 120, 220, 130, 230,
        101, 201, 111, 211, 121, 221, 131, 231,
        102, 202, 112, 212, 122, 222, 132, 232,
    };
    try std.testing.expectEqual(@as(f32, 111), positionEmbeddingValue(&hidden_axis_raw, hidden, positions, 0, 1, 1, true));
    try std.testing.expectEqual(@as(f32, 221), positionEmbeddingValue(&hidden_axis_raw, hidden, positions, 1, 2, 1, true));
}

test "gemma4 12b real mmproj optional projector smoke" {
    const native_compute = @import("../ops/native_compute.zig");
    const compat = @import("../io/compat.zig");

    const mmproj_path = platform.env.getenvSlice("ANTFLY_GEMMA4_12B_MMPROJ_PATH") orelse return error.SkipZigTest;
    const image_path = platform.env.getenvSlice("ANTFLY_GEMMA4_12B_IMAGE_PATH");
    const audio_path = platform.env.getenvSlice("ANTFLY_GEMMA4_12B_AUDIO_PATH");
    if (image_path == null and audio_path == null) return error.SkipZigTest;

    const allocator = std.testing.allocator;
    var store = try tensor_store_mod.GgufStore.initAbsolute(allocator, mmproj_path);
    defer store.tensorStore().deinit();

    var weight_store = native_compute.WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    defer native_compute.deinitPrefetchQueue(&weight_store);
    var compute = native_compute.NativeCompute.init(allocator, &weight_store, null);
    defer compute.deinit();
    var cb = compute.computeBackend();

    if (image_path) |path| {
        const image_bytes = try compat.cwd().readFileAlloc(compat.io(), path, allocator, .limited(64 * 1024 * 1024));
        defer allocator.free(image_bytes);
        var projected = try encodeProjectedImagesFromStore(&cb, allocator, store, &.{image_bytes});
        defer projected.deinit();

        try std.testing.expectEqual(@as(usize, 1), projected.tokens_per_image.len);
        try std.testing.expect(projected.tokens_per_image[0] > 0);
        try std.testing.expectEqual(projected.tokens_per_image[0] * projected.hidden_size, projected.embeddings.len);
    }

    if (audio_path) |path| {
        const audio_bytes = try compat.cwd().readFileAlloc(compat.io(), path, allocator, .limited(128 * 1024 * 1024));
        defer allocator.free(audio_bytes);
        var projected = try encodeProjectedAudioFromStore(&cb, allocator, store, &.{audio_bytes});
        defer projected.deinit();

        try std.testing.expectEqual(@as(usize, 1), projected.tokens_per_audio.len);
        try std.testing.expect(projected.tokens_per_audio[0] > 0);
        try std.testing.expectEqual(projected.tokens_per_audio[0] * projected.hidden_size, projected.embeddings.len);
    }
}

fn encoderBlock(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    store: *ProjectorWeights,
    cfg: Config,
    input: CT,
    geometry: Geometry,
    layer: usize,
) !CT {
    const total = geometry.grid_x * geometry.grid_y;
    var buf: [128]u8 = undefined;

    const ln1 = try loadWeightCt(cb, allocator, store.gguf, try fmt(&buf, "v.blk.{d}.ln1.weight", .{layer}));
    defer cb.free(ln1);
    const normed1 = try cb.rmsNorm(input, ln1, cfg.vision_hidden, cfg.layer_norm_eps);
    defer cb.free(normed1);

    const attn = try selfAttention(cb, allocator, store, cfg, normed1, geometry, layer, &buf);
    defer cb.free(attn);
    const attn_post = try loadWeightCt(cb, allocator, store.gguf, try fmt(&buf, "v.blk.{d}.attn_post_norm.weight", .{layer}));
    defer cb.free(attn_post);
    const attn_normed = try cb.rmsNorm(attn, attn_post, cfg.vision_hidden, cfg.layer_norm_eps);
    defer cb.free(attn_normed);
    const res1 = try cb.add(input, attn_normed);
    errdefer cb.free(res1);

    const ln2 = try loadWeightCt(cb, allocator, store.gguf, try fmt(&buf, "v.blk.{d}.ln2.weight", .{layer}));
    defer cb.free(ln2);
    const normed2 = try cb.rmsNorm(res1, ln2, cfg.vision_hidden, cfg.layer_norm_eps);
    defer cb.free(normed2);

    const ffn = try feedForward(cb, allocator, store, cfg, normed2, total, layer, &buf);
    defer cb.free(ffn);
    const ffn_post = try loadWeightCt(cb, allocator, store.gguf, try fmt(&buf, "v.blk.{d}.ffn_post_norm.weight", .{layer}));
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
    store: *ProjectorWeights,
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
        const q_norm_w = try loadWeightCt(cb, allocator, store.gguf, try fmt(buf, "v.blk.{d}.attn_q_norm.weight", .{layer}));
        defer cb.free(q_norm_w);
        const normed = try rmsNormHeadChunksAnd2dRope(cb, allocator, q, q_norm_w, total, cfg.vision_hidden, head_dim, geometry, cfg.layer_norm_eps, cfg.rope_theta, true);
        cb.free(q);
        q = normed;
    }
    defer cb.free(q);

    var k = try linearNoBiasMaybeClipped(cb, allocator, store, input, try fmt(buf, "v.blk.{d}.attn_k", .{layer}), total, cfg.vision_hidden, cfg.vision_hidden);
    errdefer cb.free(k);
    {
        const k_norm_w = try loadWeightCt(cb, allocator, store.gguf, try fmt(buf, "v.blk.{d}.attn_k_norm.weight", .{layer}));
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
    store: *ProjectorWeights,
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

/// A 1-D projector tensor (norm weight, bias, scale) of `len` elements: the
/// session's resident copy when it registered the projector's audio tensors,
/// else a per-call load.
fn projectorVectorWeightCt(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    store: *ProjectorWeights,
    name: []const u8,
    len: usize,
) !CT {
    const Load = struct {
        cb: *const ComputeBackend,
        allocator: std.mem.Allocator,
        store: *ProjectorWeights,
        name: []const u8,
        len: usize,
        fn call(self: @This()) !CT {
            if (!audioHostOps().resident) {
                if (self.cb.getWeight(self.name)) |resident| {
                    const expected = [_]i64{@intCast(self.len)};
                    if ((self.cb.tensorShapeMatches(resident, &expected) catch null) orelse false) return resident;
                    self.cb.free(resident);
                } else |_| {}
            }
            return loadWeightCt(self.cb, self.allocator, self.store.gguf, self.name);
        }
    };
    return store.cached(name, Load{ .cb = cb, .allocator = allocator, .store = store, .name = name, .len = len });
}

pub fn loadWeightCt(
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
    store: *ProjectorWeights,
    input: CT,
    prefix: []const u8,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !CT {
    const weight_name = try std.fmt.allocPrint(allocator, "{s}.weight", .{prefix});
    defer allocator.free(weight_name);
    if (!audioHostOps().clamp) {
        if (if (store.owner) |owner| try owner.clampSpec(prefix) else null) |spec| {
            return linearNoBiasScalarClipped(cb, allocator, store, input, weight_name, spec, rows, in_dim, out_dim);
        }
    }
    const input_min_name = try std.fmt.allocPrint(allocator, "{s}.input_min", .{prefix});
    defer allocator.free(input_min_name);
    const input_max_name = try std.fmt.allocPrint(allocator, "{s}.input_max", .{prefix});
    defer allocator.free(input_max_name);
    const output_min_name = try std.fmt.allocPrint(allocator, "{s}.output_min", .{prefix});
    defer allocator.free(output_min_name);
    const output_max_name = try std.fmt.allocPrint(allocator, "{s}.output_max", .{prefix});
    defer allocator.free(output_max_name);

    var input_min = try loadOptionalTensorF32(store.gguf, input_min_name);
    defer if (input_min) |*tensor| tensor.deinit();
    var input_max = try loadOptionalTensorF32(store.gguf, input_max_name);
    defer if (input_max) |*tensor| tensor.deinit();
    var output_min = try loadOptionalTensorF32(store.gguf, output_min_name);
    defer if (output_min) |*tensor| tensor.deinit();
    var output_max = try loadOptionalTensorF32(store.gguf, output_max_name);
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

    const weight = try projectorLinearWeightCt(cb, allocator, store, weight_name, in_dim, out_dim);
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

/// `linearNoBiasMaybeClipped` for the common case of scalar clamp bounds:
/// the bounds come from the cache and the clamps stay on device when the
/// backend has a scalar clamp.
fn linearNoBiasScalarClipped(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    store: *ProjectorWeights,
    input: CT,
    weight_name: []const u8,
    spec: ClampSpec,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !CT {
    var linear_input = input;
    var free_linear_input = false;
    if (spec.clipsInput()) {
        linear_input = try clampScalarCt(cb, allocator, input, rows, in_dim, spec.input_min, spec.input_max);
        free_linear_input = true;
    }
    defer if (free_linear_input) cb.free(linear_input);

    const weight = try projectorLinearWeightCt(cb, allocator, store, weight_name, in_dim, out_dim);
    const output = try cb.linearNoBias(linear_input, weight, rows, in_dim, out_dim);
    if (!spec.clipsOutput()) return output;
    defer cb.free(output);
    return clampScalarCt(cb, allocator, output, rows, out_dim, spec.output_min, spec.output_max);
}

fn clampScalarCt(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: CT,
    rows: usize,
    dim: usize,
    min_value: ?f32,
    max_value: ?f32,
) !CT {
    if (try cb.clampScalar(input, min_value, max_value)) |clamped| return clamped;
    const data = try cb.toFloat32(input, allocator);
    defer allocator.free(data);
    const min_arr = [_]f32{min_value orelse 0.0};
    const max_arr = [_]f32{max_value orelse 0.0};
    try applyClamp(data, rows, dim, if (min_value != null) min_arr[0..] else null, if (max_value != null) max_arr[0..] else null);
    const shape = [_]i32{ @intCast(rows), @intCast(dim) };
    return cb.fromFloat32Shape(data, &shape);
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

/// A projector linear weight as `[out_dim, in_dim]`: the session's resident
/// copy when the session registered the projector's audio tensors (quantized
/// storage and all), else a per-call load from the projector file.
fn projectorLinearWeightCt(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    store: *ProjectorWeights,
    name: []const u8,
    in_dim: usize,
    out_dim: usize,
) !CT {
    const Load = struct {
        cb: *const ComputeBackend,
        allocator: std.mem.Allocator,
        store: *ProjectorWeights,
        name: []const u8,
        in_dim: usize,
        out_dim: usize,
        fn call(self: @This()) !CT {
            if (!audioHostOps().resident) {
                if (self.cb.getWeight(self.name)) |resident| {
                    const expected = [_]i64{ @intCast(self.out_dim), @intCast(self.in_dim) };
                    if ((self.cb.tensorShapeMatches(resident, &expected) catch null) orelse false) return resident;
                    self.cb.free(resident);
                } else |_| {}
            }
            return loadLinearWeightCt(self.cb, self.allocator, self.store.gguf, self.name, self.in_dim, self.out_dim);
        }
    };
    return store.cached(name, Load{ .cb = cb, .allocator = allocator, .store = store, .name = name, .in_dim = in_dim, .out_dim = out_dim });
}

pub fn loadLinearWeightCt(
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

pub fn loadTensorF32(store: *tensor_store_mod.GgufStore, name: []const u8) !LoadedF32 {
    var tensor_ref = try store.tensorStore().describeTensor(store.allocator, name);
    defer tensor_ref.deinit(store.allocator);
    var loaded = try store.tensorStore().loadTensorRef(&tensor_ref);
    errdefer loaded.deinit();

    if (loaded.tensor.dtype == .f32) {
        return .{
            .store = store,
            .name = name,
            .weight = loaded,
            .data = loaded.tensor.asFloat32(),
            .shape = loaded.tensor.shape,
        };
    }

    if (loaded.tensor.dtype == .f16 or loaded.tensor.dtype == .bf16) {
        const converted = try weight_source_mod.convertToF32(store.allocator, &loaded.tensor);
        errdefer converted.deinit();
        return .{
            .store = store,
            .name = name,
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

test "gemma4 audio device attention and depthwise conv match the host paths" {
    const build_options = @import("build_options");
    if (!build_options.enable_metal) return error.SkipZigTest;
    const gpu_hosted_store = @import("../ops/gpu_hosted_store.zig");
    const metal_compute_mod = @import("../ops/metal_compute.zig");
    const allocator = std.testing.allocator;

    var weight_store = gpu_hosted_store.WeightStore{ .allocator = allocator, .prefix = "", .lazy_weights = .empty };
    defer {
        metal_compute_mod.deinitPrefetchQueue(&weight_store);
        weight_store.lazy_weights.deinit(allocator);
    }
    var metal_compute = try metal_compute_mod.MetalCompute.init(allocator, &weight_store, null);
    defer metal_compute.deinit();
    const cb = metal_compute.computeBackend();
    if (!cb.decoderRuntimeReady()) return error.SkipZigTest;

    const cfg = AudioConfig{
        .text_hidden = 8,
        .audio_hidden = 1536,
        .output_hidden = 8,
        .intermediate_size = 8,
        .block_count = 1,
        .head_count = 12,
        .mel_bins = 128,
        .layer_norm_eps = 1e-6,
    };
    const rows: usize = 63;
    const hidden = cfg.audio_hidden;
    var prng = std.Random.DefaultPrng.init(0x5eed);
    const random = prng.random();
    const q = try allocator.alloc(f32, rows * hidden);
    defer allocator.free(q);
    const k = try allocator.alloc(f32, rows * hidden);
    defer allocator.free(k);
    const v = try allocator.alloc(f32, rows * hidden);
    defer allocator.free(v);
    const rel = try allocator.alloc(f32, cfg.attention_context_left * hidden);
    defer allocator.free(rel);
    for (q) |*value| value.* = random.float(f32) * 2.0 - 1.0;
    for (k) |*value| value.* = random.float(f32) * 2.0 - 1.0;
    for (v) |*value| value.* = random.float(f32) * 2.0 - 1.0;
    for (rel) |*value| value.* = random.float(f32) * 2.0 - 1.0;
    var per_dim: [128]f32 = undefined;
    for (&per_dim, 0..) |*value, i| value.* = -0.5 + 0.01 * @as(f32, @floatFromInt(i));
    const valid_mask = try allocator.alloc(bool, rows);
    defer allocator.free(valid_mask);
    for (valid_mask, 0..) |*flag, i| flag.* = i < rows - 4;

    const per_dim_slices = [_][]const f32{&per_dim};
    var inputs = try AudioLayerInputs.init(&cb, allocator, cfg, valid_mask, &per_dim_slices);
    defer inputs.deinit(&cb);
    const shape = [_]i32{ @intCast(rows), @intCast(hidden) };
    const q_ct = try cb.fromFloat32Shape(q, &shape);
    defer cb.free(q_ct);
    const k_ct = try cb.fromFloat32Shape(k, &shape);
    defer cb.free(k_ct);
    const v_ct = try cb.fromFloat32Shape(v, &shape);
    defer cb.free(v_ct);
    const rel_shape = [_]i32{ @intCast(cfg.attention_context_left), @intCast(hidden) };
    const rel_ct = try cb.fromFloat32Shape(rel, &rel_shape);
    defer cb.free(rel_ct);

    const saved_ops = audio_host_ops_cache;
    defer audio_host_ops_cache = saved_ops;
    audio_host_ops_cache = .{ .attention = true };
    const host_out = try audioLocalAttention(&cb, allocator, cfg, q_ct, k_ct, v_ct, rel_ct, 0, &inputs);
    defer cb.free(host_out);
    audio_host_ops_cache = .{};
    const device_out = try audioLocalAttention(&cb, allocator, cfg, q_ct, k_ct, v_ct, rel_ct, 0, &inputs);
    defer cb.free(device_out);
    const host_data = try cb.toFloat32(host_out, allocator);
    defer allocator.free(host_data);
    const device_data = try cb.toFloat32(device_out, allocator);
    defer allocator.free(device_data);
    try std.testing.expectEqual(host_data.len, device_data.len);
    var max_diff: f32 = 0.0;
    for (host_data, device_data) |a, b| max_diff = @max(max_diff, @abs(a - b));
    try std.testing.expect(max_diff < 1e-3);

    // Depthwise causal conv through the op layer against the host loop.
    const kernel_size = cfg.conv_kernel_size;
    const weight = try allocator.alloc(f32, kernel_size * hidden);
    defer allocator.free(weight);
    for (weight) |*value| value.* = random.float(f32) - 0.5;
    const weight_shape = [_]i32{ @intCast(kernel_size), @intCast(hidden) };
    const weight_ct = try cb.fromFloat32Shape(weight, &weight_shape);
    defer cb.free(weight_ct);
    const conv = (try cb.depthwiseCausalConv1d(q_ct, weight_ct, rows, hidden, kernel_size)) orelse return error.UnexpectedNull;
    defer cb.free(conv);
    const conv_data = try cb.toFloat32(conv, allocator);
    defer allocator.free(conv_data);
    for (0..rows) |t| {
        for (0..hidden) |h| {
            var expected: f32 = 0.0;
            for (0..kernel_size) |kk| {
                if (t + kk < kernel_size - 1) continue;
                expected += q[(t + kk - (kernel_size - 1)) * hidden + h] * weight[kk * hidden + h];
            }
            try std.testing.expectApproxEqAbs(expected, conv_data[t * hidden + h], 1e-4);
        }
    }
}

test "gemma4 real mmproj audio device ops match the host encoder" {
    const build_options = @import("build_options");
    if (!build_options.enable_metal) return error.SkipZigTest;
    const gpu_hosted_store = @import("../ops/gpu_hosted_store.zig");
    const metal_compute_mod = @import("../ops/metal_compute.zig");
    const compat = @import("../io/compat.zig");

    const mmproj_path = platform.env.getenvSlice("ANTFLY_GEMMA4_MMPROJ_PATH") orelse return error.SkipZigTest;
    const audio_path = platform.env.getenvSlice("ANTFLY_GEMMA4_AUDIO_PATH") orelse return error.SkipZigTest;
    const allocator = std.testing.allocator;

    var weight_store = gpu_hosted_store.WeightStore{ .allocator = allocator, .prefix = "", .lazy_weights = .empty };
    defer {
        metal_compute_mod.deinitPrefetchQueue(&weight_store);
        weight_store.lazy_weights.deinit(allocator);
    }
    var metal_compute = try metal_compute_mod.MetalCompute.init(allocator, &weight_store, null);
    defer metal_compute.deinit();
    const cb = metal_compute.computeBackend();
    if (!cb.decoderRuntimeReady()) return error.SkipZigTest;

    const audio_bytes = try compat.cwd().readFileAlloc(compat.io(), audio_path, allocator, .limited(128 * 1024 * 1024));
    defer allocator.free(audio_bytes);
    const projector = try ProjectorStore.open(allocator, mmproj_path);
    defer projector.close();

    const saved_ops = audio_host_ops_cache;
    defer audio_host_ops_cache = saved_ops;
    audio_host_ops_cache = AudioHostOps.parse("all");
    var reference = try encodeProjectedAudio(&cb, allocator, projector, &.{audio_bytes});
    defer reference.deinit();
    try std.testing.expect(reference.embeddings.len > 0);

    const configs = [_][]const u8{
        "glu,dwconv,attention,scale,bias,channel_norm,flatten,resident",
        "clamp,dwconv,attention,scale,bias,channel_norm,flatten,resident",
        "clamp,glu,attention,scale,bias,channel_norm,flatten,resident",
        "clamp,glu,dwconv,scale,bias,channel_norm,flatten,resident",
        "clamp,glu,dwconv,attention,bias,channel_norm,flatten,resident",
        "clamp,glu,dwconv,attention,scale,channel_norm,flatten,resident",
        "clamp,glu,dwconv,attention,scale,bias,flatten,resident",
        "clamp,glu,dwconv,attention,scale,bias,channel_norm,resident",
        "",
    };
    var worst: f32 = 0.0;
    for (configs) |spec| {
        audio_host_ops_cache = AudioHostOps.parse(spec);
        var projected = try encodeProjectedAudio(&cb, allocator, projector, &.{audio_bytes});
        defer projected.deinit();
        try std.testing.expectEqual(reference.embeddings.len, projected.embeddings.len);
        var max_diff: f32 = 0.0;
        var max_ref: f32 = 0.0;
        for (reference.embeddings, projected.embeddings) |a, b| {
            max_diff = @max(max_diff, @abs(a - b));
            max_ref = @max(max_ref, @abs(a));
        }
        std.debug.print("gemma4 audio device-vs-host host_ops='{s}' max_abs_diff={d:.6} max_abs_ref={d:.4}\n", .{ spec, max_diff, max_ref });
        if (spec.len == 0) worst = max_diff;
    }
    try std.testing.expect(worst < 5e-2);
}

test "model-owned projector store serves requests with their own allocators" {
    const allocator = std.testing.allocator;
    var fixture = try tensor_store_mod.writeGemma4AudioProjectorFixture(allocator, "gemma4-projector-store-owned");
    defer fixture.deinit(allocator);

    // Opened by the model owner; requests come and go with scoped allocators.
    const projector = try ProjectorStore.open(allocator, fixture.projector_path);
    defer projector.close();
    try std.testing.expectEqualStrings(fixture.projector_path, projector.path);

    var first_arena = std.heap.ArenaAllocator.init(allocator);
    {
        defer first_arena.deinit();
        var weights = ProjectorWeights.init(undefined, first_arena.allocator(), projector.gguf, projector);
        defer weights.entries.deinit(first_arena.allocator());
        var tensor = try loadTensorF32(weights.gguf, "a.blk.0.ffn_up.weight");
        defer tensor.deinit();
        try std.testing.expectEqual(@as(usize, 2), tensor.data.len);
        const spec = (try weights.owner.?.clampSpec("a.blk.0.ffn_up")) orelse return error.TestUnexpectedResult;
        try std.testing.expect(!spec.clipsInput() and !spec.clipsOutput());
    }

    // The first request's allocator is gone; the store and its clamp cache
    // are untouched by that.
    var second_arena = std.heap.ArenaAllocator.init(allocator);
    defer second_arena.deinit();
    var tensor = try loadTensorF32(projector.gguf, "a.blk.0.ffn_up.weight");
    defer tensor.deinit();
    try std.testing.expectApproxEqAbs(@as(f32, 5.0), tensor.data[0], 0.0);
    try std.testing.expectApproxEqAbs(@as(f32, 6.0), tensor.data[1], 0.0);
    try std.testing.expectEqual(@as(usize, 1), projector.clamp_specs.count());
    const spec = (try projector.clampSpec("a.blk.0.ffn_up")) orelse return error.TestUnexpectedResult;
    try std.testing.expect(!spec.clipsInput() and !spec.clipsOutput());
    try std.testing.expectEqual(@as(usize, 1), projector.clamp_specs.count());
}

fn initAudioLayerInputsUnderAllocationFaults(
    allocator: std.mem.Allocator,
    cfg: AudioConfig,
    valid_mask: []const bool,
    per_dim: []const []const f32,
) !void {
    const native_compute = @import("../ops/native_compute.zig");
    var store = native_compute.WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    defer native_compute.deinitPrefetchQueue(&store);
    var compute = native_compute.NativeCompute.init(allocator, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    var inputs = try AudioLayerInputs.init(&cb, allocator, cfg, valid_mask, per_dim);
    inputs.deinit(&cb);
}

// Every allocation in the layer-input setup fails once; each item must be
// released exactly once whichever step fails (a local cleanup that outlived
// its list append used to free the item twice).
test "audio layer inputs survive allocation failures without leaks or double frees" {
    const cfg = AudioConfig{
        .text_hidden = 8,
        .audio_hidden = 16,
        .output_hidden = 8,
        .intermediate_size = 8,
        .block_count = 3,
        .head_count = 2,
        .mel_bins = 128,
        .layer_norm_eps = 1e-6,
    };
    const valid_mask = [_]bool{ true, true, false };
    const per_dim_row = [_]f32{ 0.1, -0.2, 0.3, 0.4, -0.5, 0.6, 0.7, 0.8 };
    const per_dim = [_][]const f32{ &per_dim_row, &per_dim_row, &per_dim_row };
    try std.testing.checkAllAllocationFailures(
        std.testing.allocator,
        initAudioLayerInputsUnderAllocationFaults,
        .{ cfg, @as([]const bool, &valid_mask), @as([]const []const f32, &per_dim) },
    );
}

const FailingUploadBackend = struct {
    const native_compute = @import("../ops/native_compute.zig");

    fn ensureDeviceResident(_: *anyopaque, _: CT) anyerror!?CT {
        return error.OutOfMemory;
    }

    fn vtable() ComputeBackend.VTable {
        var table = native_compute.vtable_impl;
        table.ensureDeviceResident = ensureDeviceResident;
        return table;
    }
};

// A failed upload must release the host tensor it was handed; the testing
// allocator reports the leak otherwise.
test "device residency helper releases the host tensor when the upload fails" {
    const native_compute = @import("../ops/native_compute.zig");
    const allocator = std.testing.allocator;
    var store = native_compute.WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    defer native_compute.deinitPrefetchQueue(&store);
    var compute = native_compute.NativeCompute.init(allocator, &store, null);
    defer compute.deinit();
    const table = FailingUploadBackend.vtable();
    const cb = ComputeBackend{ .ptr = &compute, .vtable = &table };

    const data = [_]f32{ 1.0, 2.0, 3.0, 4.0 };
    const shape = [_]i32{ 2, 2 };
    try std.testing.expectError(error.OutOfMemory, deviceResidentFromFloat32(&cb, &data, &shape));

    // A backend without device residency hands the host tensor back to the caller.
    const plain = compute.computeBackend();
    const kept = try deviceResidentFromFloat32(&plain, &data, &shape);
    defer plain.free(kept);
    const readback = try plain.toFloat32(kept, allocator);
    defer allocator.free(readback);
    try std.testing.expectEqualSlices(f32, &data, readback);
}
