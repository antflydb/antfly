// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//! Strict Qwen3-VL GGUF vision tower and DeepStack projector.
//!
//! The official projector is not a CLIP pooling head: it contains a temporal
//! split Conv3D patch embed, learned-position interpolation, two-axis vision
//! RoPE, a 2x2 concatenating merger, and multiple DeepStack feature heads.
//! This module keeps those outputs separate so decoder integration cannot
//! accidentally discard DeepStack or treat its concatenated payload as tokens.

const std = @import("std");
const platform = @import("antfly_platform");
const image = @import("../pipelines/image.zig");
const ops = @import("../ops/ops.zig");
const metal_compute_mod = @import("../ops/metal_compute.zig");
const gguf_metadata = @import("../gguf/metadata.zig");
const gguf_format = @import("../gguf/format.zig");
const tensor_store_mod = @import("../models/tensor_store.zig");
const projector_format = @import("projector_format.zig");
const projector_common = @import("gemma4_projector.zig");
const qwen3vl_plan = @import("qwen3vl_plan.zig");
const gpt_arch = @import("gpt.zig");
const gpt_config = @import("../models/gpt.zig");

const ComputeBackend = ops.ComputeBackend;
const CT = ops.CT;

fn qwen3VlProfileEnabled() bool {
    return platform.env.getenvBool("TERMITE_QWEN3VL_PROFILE");
}

fn monotonicNowNs() u64 {
    if (comptime @import("builtin").os.tag == .freestanding) return 0;
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(.MONOTONIC, &ts))) {
        .SUCCESS => return @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec),
        else => return 0,
    }
}

fn profileLap(enabled: bool, started_at: *u64) u64 {
    if (!enabled) return 0;
    const now = monotonicNowNs();
    const elapsed = now -| started_at.*;
    started_at.* = now;
    return elapsed;
}

pub const Limits = struct {
    max_encoded_image_bytes: usize = 64 * 1024 * 1024,
    max_decoded_pixels: usize = 100_000_000,
    max_images: usize = 8,
    /// Official Qwen image preprocessing uses 65,536 minimum pixels. With a
    /// 16x16 patch and 2x2 merger that is exactly 64 merged visual tokens.
    min_merged_tokens: usize = 64,
    /// 576 preserves the official 768x768 training grid with a 2x2 merger.
    max_merged_tokens: usize = 576,
    hard_max_merged_tokens: usize = 1024,

    fn validate(self: Limits) !void {
        if (self.max_encoded_image_bytes == 0 or self.max_decoded_pixels == 0 or self.max_images == 0 or
            self.min_merged_tokens < 4 or self.max_merged_tokens < self.min_merged_tokens or
            self.hard_max_merged_tokens < self.min_merged_tokens or
            self.max_merged_tokens > self.hard_max_merged_tokens or
            self.max_images > 64 or self.hard_max_merged_tokens > 4096)
        {
            return error.InvalidVisionLimits;
        }
    }
};

/// Conservative process-wide reservation for one Qwen3-VL projector request.
/// The projector executes images serially, while final embeddings and
/// DeepStack features accumulate across images, so the estimate combines the
/// largest per-image working set with the aggregate retained outputs.
pub const AdmissionEstimate = struct {
    visual_tokens: usize,
    host_scratch_bytes: usize,
    backend_scratch_bytes: usize,
};

fn admissionMul(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b) catch error.VisionAdmissionExceeded;
}

fn admissionAdd(a: usize, b: usize) !usize {
    return std.math.add(usize, a, b) catch error.VisionAdmissionExceeded;
}

fn admissionF32Bytes(rows: usize, columns: usize) !usize {
    return admissionMul(try admissionMul(rows, columns), @sizeOf(f32));
}

/// Probe untrusted image headers and derive the exact geometry used by the
/// projector before scheduler or process-wide memory admission occurs.
pub fn estimateAdmission(
    images: []const []const u8,
    config: gpt_config.Config,
    limits: Limits,
) !AdmissionEstimate {
    try limits.validate();
    if (images.len == 0 or images.len > limits.max_images) return error.ImageLimitExceeded;
    var dimensions: [64]image.EncodedImageInfo = undefined;
    for (images, 0..) |bytes, index| {
        if (bytes.len == 0 or bytes.len > limits.max_encoded_image_bytes) return error.VisionAdmissionExceeded;
        dimensions[index] = try image.inspectEncodedForInference(bytes, null);
    }
    return estimateAdmissionForImageDimensions(dimensions[0..images.len], config, limits);
}

fn estimateAdmissionForImageDimensions(
    dimensions: []const image.EncodedImageInfo,
    config: gpt_config.Config,
    limits: Limits,
) !AdmissionEstimate {
    try limits.validate();
    if (dimensions.len == 0 or dimensions.len > limits.max_images) return error.ImageLimitExceeded;
    if (config.family != .qwen3_vl or config.vision_patch_size == 0 or
        config.vision_spatial_merge_size == 0 or config.vision_hidden_size == 0 or
        config.vision_intermediate_size == 0 or config.hidden_size == 0 or
        config.vision_num_attention_heads == 0)
    {
        return error.InvalidModelForGeneration;
    }

    var total_visual_tokens: usize = 0;
    var max_source_pixels: usize = 0;
    var max_resized_pixels: usize = 0;
    var max_patch_tokens: usize = 0;
    var max_visual_tokens: usize = 0;
    for (dimensions) |info| {
        const source_pixels = try admissionMul(info.width, info.height);
        if (source_pixels == 0 or source_pixels > limits.max_decoded_pixels) return error.VisionAdmissionExceeded;
        const geometry = try targetGeometryForLayout(
            config.vision_patch_size,
            config.vision_spatial_merge_size,
            info.width,
            info.height,
            limits,
        );
        total_visual_tokens = try admissionAdd(total_visual_tokens, geometry.tokenCount());
        max_source_pixels = @max(max_source_pixels, source_pixels);
        max_resized_pixels = @max(max_resized_pixels, try admissionMul(geometry.width, geometry.height));
        max_patch_tokens = @max(max_patch_tokens, geometry.patchCount());
        max_visual_tokens = @max(max_visual_tokens, geometry.tokenCount());
    }

    const vision_hidden: usize = config.vision_hidden_size;
    const intermediate: usize = config.vision_intermediate_size;
    const text_hidden: usize = config.hidden_size;
    const head_count: usize = config.vision_num_attention_heads;
    const deepstack_count: usize = config.vision_deepstack_visual_indexes_len;

    // Host peak: decoded RGB, normalized and merge-major patch copies, the
    // positioned embedding mirror, and two generations of accumulated final
    // and DeepStack outputs during repack/prompt ownership transfer.
    const decoded_rgb = try admissionMul(max_source_pixels, 3);
    const normalized_and_patches = try admissionMul(try admissionF32Bytes(max_resized_pixels, 3), 2);
    const positioned_embedding = try admissionF32Bytes(max_patch_tokens, vision_hidden);
    const projected_outputs = try admissionMul(try admissionF32Bytes(total_visual_tokens, text_hidden), 2);
    const deepstack_outputs = try admissionMul(
        try admissionF32Bytes(try admissionMul(total_visual_tokens, deepstack_count), text_hidden),
        2,
    );
    var host_scratch = try admissionAdd(decoded_rgb, normalized_and_patches);
    host_scratch = try admissionAdd(host_scratch, positioned_embedding);
    host_scratch = try admissionAdd(host_scratch, projected_outputs);
    host_scratch = try admissionAdd(host_scratch, deepstack_outputs);

    // Backend peak: QKV/RoPE/output tensors plus the larger of an explicit
    // attention score matrix and the two FFN intermediates. This deliberately
    // remains conservative when a backend selects fused SDPA.
    const hidden_rows = try admissionF32Bytes(max_patch_tokens, vision_hidden);
    const qkv_rows = try admissionMul(hidden_rows, 3);
    const rope_and_outputs = try admissionMul(hidden_rows, 4);
    const attention_scores = try admissionF32Bytes(
        try admissionMul(try admissionMul(max_patch_tokens, max_patch_tokens), head_count),
        1,
    );
    const ffn_intermediates = try admissionMul(try admissionF32Bytes(max_patch_tokens, intermediate), 2);
    const merged_hidden = try admissionMul(vision_hidden, try admissionMul(config.vision_spatial_merge_size, config.vision_spatial_merge_size));
    const merge_intermediates = try admissionAdd(
        try admissionMul(try admissionF32Bytes(max_visual_tokens, merged_hidden), 3),
        try admissionF32Bytes(max_visual_tokens, text_hidden),
    );
    var backend_scratch = try admissionAdd(qkv_rows, rope_and_outputs);
    backend_scratch = try admissionAdd(backend_scratch, @max(attention_scores, ffn_intermediates));
    backend_scratch = try admissionAdd(backend_scratch, merge_intermediates);

    return .{
        .visual_tokens = total_visual_tokens,
        .host_scratch_bytes = host_scratch,
        .backend_scratch_bytes = backend_scratch,
    };
}

pub const ProjectedImages = struct {
    allocator: std.mem.Allocator,
    embeddings: []f32,
    deepstack_embeddings: []f32,
    tokens_per_image: []usize,
    grids: []qwen3vl_plan.VisionGrid,
    preprocess_evidence: []PreprocessEvidence,
    preprocess_spatial_patches: []f32,
    hidden_size: usize,
    deepstack_layer_count: usize,

    pub fn deinit(self: *ProjectedImages) void {
        self.allocator.free(self.embeddings);
        self.allocator.free(self.deepstack_embeddings);
        self.allocator.free(self.tokens_per_image);
        self.allocator.free(self.grids);
        self.allocator.free(self.preprocess_evidence);
        self.allocator.free(self.preprocess_spatial_patches);
    }

    pub fn deepstackFeature(self: ProjectedImages, layer: usize, total_tokens: usize) ![]const f32 {
        if (layer >= self.deepstack_layer_count) return error.DeepstackLayerOutOfRange;
        const stride = std.math.mul(usize, total_tokens, self.hidden_size) catch return error.InvalidTensorShape;
        const expected = std.math.mul(usize, stride, self.deepstack_layer_count) catch return error.InvalidTensorShape;
        if (self.deepstack_embeddings.len != expected) return error.InvalidTensorShape;
        return self.deepstack_embeddings[layer * stride ..][0..stride];
    }
};

/// Canonical qualification evidence for the exact float input consumed by the
/// Qwen vision patch embedding. The digest is SHA-256 over little-endian f32
/// rows after resize/normalize/merge-major patchification, before any backend
/// conversion or quantized projector math.
pub const PreprocessEvidence = struct {
    source_width: usize,
    source_height: usize,
    resized_width: usize,
    resized_height: usize,
    grid: qwen3vl_plan.VisionGrid,
    patch_rows: usize,
    patch_columns: usize,
    spatial_patch_f32le_sha256: [64]u8,
    /// Digest after temporal patch projection and learned-position addition,
    /// immediately before the first vision transformer block. This reuses the
    /// host synchronization required by learned-position interpolation.
    positioned_embedding_f32le_sha256: [64]u8,
    vision_trace_layer: ?usize,
    vision_trace_f32le_sha256: ?[64]u8,
};

pub const PreparedPrompt = struct {
    allocator: std.mem.Allocator,
    token_ids: []i64,
    input_embeddings: ?CT,
    plan: qwen3vl_plan.RequestPlan,
    deepstack_embeddings: []f32,
    deepstack_layer_count: usize,

    pub fn deinit(self: *PreparedPrompt, cb: *const ComputeBackend) void {
        self.allocator.free(self.token_ids);
        if (self.input_embeddings) |embeddings| cb.free(embeddings);
        self.plan.deinit();
        self.allocator.free(self.deepstack_embeddings);
    }
};

const Config = struct {
    text_hidden: usize,
    vision_hidden: usize,
    intermediate: usize,
    block_count: usize,
    head_count: usize,
    image_size: usize,
    patch_size: usize,
    spatial_merge: usize,
    layer_norm_eps: f32,
    rope_theta: f32 = 10_000.0,
    image_mean: [3]f32,
    image_std: [3]f32,
    deepstack_layers: [64]usize = [_]usize{0} ** 64,
    deepstack_len: usize = 0,

    fn headDim(self: Config) usize {
        return self.vision_hidden / self.head_count;
    }

    fn mergedHidden(self: Config) usize {
        return self.vision_hidden * self.spatial_merge * self.spatial_merge;
    }
};

/// Request-scoped projector weights with stable tensor identities. Dynamic
/// Metal slots key prepared weights by their backing tensor; freeing each
/// short-lived wrapper after an op allowed allocator address reuse to alias a
/// later vision layer onto stale weights. Retaining and reusing every named
/// tensor for the whole multi-image request makes those identities unique,
/// then explicitly releases their dynamic slots before the wrappers die.
const WeightCache = struct {
    allocator: std.mem.Allocator,
    cb: *const ComputeBackend,
    store: *tensor_store_mod.GgufStore,
    tensors: std.StringHashMapUnmanaged(CT) = .empty,

    fn init(
        allocator: std.mem.Allocator,
        cb: *const ComputeBackend,
        store: *tensor_store_mod.GgufStore,
    ) WeightCache {
        return .{ .allocator = allocator, .cb = cb, .store = store };
    }

    fn deinit(self: *WeightCache) void {
        var value_it = self.tensors.valueIterator();
        while (value_it.next()) |tensor| {
            metal_compute_mod.MetalCompute.releaseDynamicSlotsForTensor(self.cb, tensor.*);
            self.cb.free(tensor.*);
        }
        var key_it = self.tensors.keyIterator();
        while (key_it.next()) |key| self.allocator.free(key.*);
        self.tensors.deinit(self.allocator);
    }

    fn insert(self: *WeightCache, key: []const u8, tensor: CT) !CT {
        const owned_key = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(owned_key);
        try self.tensors.putNoClobber(self.allocator, owned_key, tensor);
        return tensor;
    }

    fn weight(self: *WeightCache, name: []const u8) !CT {
        const key = try std.fmt.allocPrint(self.allocator, "weight:{s}", .{name});
        defer self.allocator.free(key);
        if (self.tensors.get(key)) |tensor| return tensor;
        const tensor = try projector_common.loadWeightCt(self.cb, self.allocator, self.store, name);
        errdefer self.cb.free(tensor);
        return self.insert(key, tensor);
    }

    fn linear(self: *WeightCache, name: []const u8, in_dim: usize, out_dim: usize) !CT {
        const key = try std.fmt.allocPrint(self.allocator, "linear:{d}:{d}:{s}", .{ in_dim, out_dim, name });
        defer self.allocator.free(key);
        if (self.tensors.get(key)) |tensor| return tensor;
        if (self.cb.kind() == .metal) {
            var tensor_ref = try self.store.tensorStore().describeTensor(self.allocator, name);
            defer tensor_ref.deinit(self.allocator);
            if (tensor_ref.quantized) {
                if (try self.store.tensorStore().loadQuantizedStorageRef(&tensor_ref)) |storage_value| {
                    var storage = storage_value;
                    const shape_matches = storage.shape.len == 2 and
                        storage.shape[0] == @as(i64, @intCast(out_dim)) and
                        storage.shape[1] == @as(i64, @intCast(in_dim));
                    if (shape_matches) {
                        // takeQuantizedStorage consumes storage on every
                        // outcome. Unsupported IQ-family formats still have a
                        // valid host-dequantized path below, so only propagate
                        // genuine upload/allocation failures.
                        const maybe_tensor: ?CT = metal_compute_mod.MetalCompute.takeQuantizedStorage(self.cb, storage) catch |err| switch (err) {
                            error.UnsupportedTensorType => null,
                            else => return err,
                        };
                        if (maybe_tensor) |tensor| {
                            errdefer self.cb.free(tensor);
                            return self.insert(key, tensor);
                        }
                    }
                    if (!shape_matches) storage.deinit();
                }
            }
        }
        const tensor = try projector_common.loadLinearWeightCt(
            self.cb,
            self.allocator,
            self.store,
            name,
            in_dim,
            out_dim,
        );
        errdefer self.cb.free(tensor);
        return self.insert(key, tensor);
    }

    fn patch(self: *WeightCache, name: []const u8, cfg: Config) !CT {
        const key = try std.fmt.allocPrint(self.allocator, "patch:{s}", .{name});
        defer self.allocator.free(key);
        if (self.tensors.get(key)) |tensor| return tensor;
        const tensor = try loadPatchWeight(self.cb, self.store, name, cfg);
        errdefer self.cb.free(tensor);
        return self.insert(key, tensor);
    }
};

pub const Geometry = struct {
    width: usize,
    height: usize,
    grid_x: usize,
    grid_y: usize,
    merged_x: usize,
    merged_y: usize,

    pub fn patchCount(self: Geometry) usize {
        return self.grid_x * self.grid_y;
    }

    pub fn tokenCount(self: Geometry) usize {
        return self.merged_x * self.merged_y;
    }
};

pub fn encodeProjectedImages(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    projector_path: []const u8,
    images: []const []const u8,
    limits: Limits,
) !ProjectedImages {
    return encodeProjectedImagesQualified(cb, allocator, projector_path, images, limits, false);
}

pub fn encodeProjectedImagesForQualification(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    projector_path: []const u8,
    images: []const []const u8,
    limits: Limits,
) !ProjectedImages {
    return encodeProjectedImagesQualified(cb, allocator, projector_path, images, limits, true);
}

fn encodeProjectedImagesQualified(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    projector_path: []const u8,
    images: []const []const u8,
    limits: Limits,
    collect_preprocess_evidence: bool,
) !ProjectedImages {
    var store = try tensor_store_mod.GgufStore.initAbsolute(allocator, projector_path);
    defer store.tensorStore().deinit();
    return encodeProjectedImagesFromStoreQualified(cb, allocator, store, images, limits, collect_preprocess_evidence);
}

pub fn encodeProjectedImagesFromStore(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    store: *tensor_store_mod.GgufStore,
    images: []const []const u8,
    limits: Limits,
) !ProjectedImages {
    return encodeProjectedImagesFromStoreQualified(cb, allocator, store, images, limits, false);
}

fn encodeProjectedImagesFromStoreQualified(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    store: *tensor_store_mod.GgufStore,
    images: []const []const u8,
    limits: Limits,
    collect_preprocess_evidence: bool,
) !ProjectedImages {
    try limits.validate();
    if (images.len == 0 or images.len > limits.max_images) return error.ImageLimitExceeded;
    const cfg = try parseConfig(&store.parsed);
    var weights = WeightCache.init(allocator, cb, store);
    defer weights.deinit();
    var embeddings = std.ArrayListUnmanaged(f32).empty;
    errdefer embeddings.deinit(allocator);
    var per_image_deepstack = std.ArrayListUnmanaged([]f32).empty;
    defer {
        for (per_image_deepstack.items) |feature| allocator.free(feature);
        per_image_deepstack.deinit(allocator);
    }
    var tokens_per_image = std.ArrayListUnmanaged(usize).empty;
    errdefer tokens_per_image.deinit(allocator);
    var grids = std.ArrayListUnmanaged(qwen3vl_plan.VisionGrid).empty;
    errdefer grids.deinit(allocator);
    var preprocess_evidence = std.ArrayListUnmanaged(PreprocessEvidence).empty;
    errdefer preprocess_evidence.deinit(allocator);
    var preprocess_spatial_patches = std.ArrayListUnmanaged(f32).empty;
    errdefer preprocess_spatial_patches.deinit(allocator);

    for (images) |bytes| {
        var encoded = try encodeSingleImage(cb, allocator, store, &weights, cfg, bytes, limits, collect_preprocess_evidence);
        defer encoded.deinit(allocator);
        try embeddings.appendSlice(allocator, encoded.embeddings);
        // Reserve the list slot before duplicating the feature payload so a
        // failed list growth cannot orphan a large per-image allocation.
        try per_image_deepstack.ensureUnusedCapacity(allocator, 1);
        const deepstack_copy = try allocator.dupe(f32, encoded.deepstack);
        per_image_deepstack.appendAssumeCapacity(deepstack_copy);
        try tokens_per_image.append(allocator, encoded.tokens);
        try grids.append(allocator, encoded.grid);
        if (encoded.preprocess_evidence) |evidence| try preprocess_evidence.append(allocator, evidence);
        if (encoded.preprocess_spatial_patches) |patches| try preprocess_spatial_patches.appendSlice(allocator, patches);
    }

    // Repack image-major temporary features into [tap][all visual tokens][H],
    // matching decoder layer injection order.
    var total_tokens: usize = 0;
    for (tokens_per_image.items) |count| total_tokens = std.math.add(usize, total_tokens, count) catch return error.VisionAdmissionExceeded;
    const feature_stride = std.math.mul(usize, total_tokens, cfg.text_hidden) catch return error.VisionAdmissionExceeded;
    const deepstack_len = std.math.mul(usize, feature_stride, cfg.deepstack_len) catch return error.VisionAdmissionExceeded;
    const deepstack = try allocator.alloc(f32, deepstack_len);
    errdefer allocator.free(deepstack);
    var image_token_offset: usize = 0;
    for (per_image_deepstack.items, tokens_per_image.items) |features, token_count| {
        const image_stride = std.math.mul(usize, token_count, cfg.text_hidden) catch return error.InvalidTensorShape;
        const expected_image_features = std.math.mul(usize, image_stride, cfg.deepstack_len) catch return error.InvalidTensorShape;
        if (features.len != expected_image_features) return error.InvalidTensorShape;
        for (0..cfg.deepstack_len) |tap| {
            const dst = tap * feature_stride + image_token_offset * cfg.text_hidden;
            const src = tap * image_stride;
            @memcpy(deepstack[dst..][0..image_stride], features[src..][0..image_stride]);
        }
        image_token_offset += token_count;
    }

    const owned_embeddings = try embeddings.toOwnedSlice(allocator);
    errdefer allocator.free(owned_embeddings);
    const owned_tokens_per_image = try tokens_per_image.toOwnedSlice(allocator);
    errdefer allocator.free(owned_tokens_per_image);
    const owned_grids = try grids.toOwnedSlice(allocator);
    errdefer allocator.free(owned_grids);
    const owned_preprocess_evidence = try preprocess_evidence.toOwnedSlice(allocator);
    errdefer allocator.free(owned_preprocess_evidence);
    const owned_preprocess_spatial_patches = try preprocess_spatial_patches.toOwnedSlice(allocator);
    errdefer allocator.free(owned_preprocess_spatial_patches);

    return .{
        .allocator = allocator,
        .embeddings = owned_embeddings,
        .deepstack_embeddings = deepstack,
        .tokens_per_image = owned_tokens_per_image,
        .grids = owned_grids,
        .preprocess_evidence = owned_preprocess_evidence,
        .preprocess_spatial_patches = owned_preprocess_spatial_patches,
        .hidden_size = cfg.text_hidden,
        .deepstack_layer_count = cfg.deepstack_len,
    };
}

/// Expand one image placeholder per projected image, replace exactly those
/// token embeddings, and build the single source of truth for M-RoPE and
/// DeepStack injection.
pub fn prepareExpandedPromptEmbeddings(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: gpt_config.Config,
    placeholder_token_ids: []const i32,
    projected: ProjectedImages,
    max_input_tokens: usize,
) !PreparedPrompt {
    if (config.family != .qwen3_vl or config.image_token_index < 0 or
        projected.tokens_per_image.len == 0 or projected.tokens_per_image.len != projected.grids.len or
        projected.hidden_size != config.hidden_size)
    {
        return error.InvalidMultimodalConfig;
    }
    var ids = std.ArrayListUnmanaged(i64).empty;
    errdefer ids.deinit(allocator);
    var modalities = std.ArrayListUnmanaged(u8).empty;
    defer modalities.deinit(allocator);
    var image_index: usize = 0;
    var visual_tokens: usize = 0;
    for (placeholder_token_ids) |token_id| {
        if (token_id != config.image_token_index) {
            try ids.append(allocator, token_id);
            try modalities.append(allocator, qwen3vl_plan.text_modality);
            continue;
        }
        if (image_index >= projected.tokens_per_image.len) return error.ImagePlaceholderCountMismatch;
        const count = projected.tokens_per_image[image_index];
        visual_tokens = std.math.add(usize, visual_tokens, count) catch return error.RequestTooLarge;
        for (0..count) |_| {
            try ids.append(allocator, config.image_token_index);
            try modalities.append(allocator, qwen3vl_plan.image_modality);
        }
        image_index += 1;
    }
    if (image_index != projected.tokens_per_image.len) return error.ImagePlaceholderCountMismatch;
    if (ids.items.len > max_input_tokens) return error.InputTokenLimitExceeded;
    var plan = try qwen3vl_plan.buildRequestPlan(
        allocator,
        modalities.items,
        projected.grids,
        config.vision_spatial_merge_size,
        .{
            .max_input_tokens = max_input_tokens,
            .max_images = projected.grids.len,
            .max_visual_tokens = visual_tokens,
        },
    );
    errdefer plan.deinit();

    const embedding_weight = try gpt_arch.getEmbeddingWeight(cb, config);
    defer cb.free(embedding_weight);
    const base = try cb.embeddingLookup(embedding_weight, ids.items, ids.items.len, config.hidden_size);
    defer cb.free(base);
    const host = try cb.toFloat32(base, allocator);
    defer allocator.free(host);
    const scale = config.tokenEmbeddingScale();
    if (!std.math.approxEqAbs(f32, scale, 1.0, 1e-6)) {
        for (host) |*value| value.* *= scale;
    }
    const hidden_size: usize = @intCast(config.hidden_size);
    if (projected.embeddings.len != visual_tokens * hidden_size) return error.ImageTokenLengthMismatch;
    var source_row: usize = 0;
    for (plan.visual_token_mask, 0..) |is_visual, row| {
        if (!is_visual) continue;
        @memcpy(host[row * hidden_size ..][0..hidden_size], projected.embeddings[source_row * hidden_size ..][0..hidden_size]);
        source_row += 1;
    }
    if (source_row != visual_tokens) return error.ImageTokenLengthMismatch;
    const shape = [_]i32{ @intCast(ids.items.len), @intCast(hidden_size) };
    const input_embeddings = try cb.fromFloat32Shape(host, &shape);
    errdefer cb.free(input_embeddings);
    const visual_feature_stride = std.math.mul(usize, visual_tokens, hidden_size) catch return error.RequestTooLarge;
    const expected_deepstack = std.math.mul(usize, visual_feature_stride, projected.deepstack_layer_count) catch return error.RequestTooLarge;
    if (projected.deepstack_embeddings.len != expected_deepstack) return error.InvalidTensorShape;
    const token_ids = try ids.toOwnedSlice(allocator);
    errdefer allocator.free(token_ids);
    const deepstack_embeddings = try allocator.dupe(f32, projected.deepstack_embeddings);
    errdefer allocator.free(deepstack_embeddings);
    return .{
        .allocator = allocator,
        .token_ids = token_ids,
        .input_embeddings = input_embeddings,
        .plan = plan,
        .deepstack_embeddings = deepstack_embeddings,
        .deepstack_layer_count = projected.deepstack_layer_count,
    };
}

const EncodedImage = struct {
    embeddings: []f32,
    deepstack: []f32,
    tokens: usize,
    grid: qwen3vl_plan.VisionGrid,
    preprocess_evidence: ?PreprocessEvidence,
    preprocess_spatial_patches: ?[]f32,

    fn deinit(self: *EncodedImage, allocator: std.mem.Allocator) void {
        allocator.free(self.embeddings);
        allocator.free(self.deepstack);
        if (self.preprocess_spatial_patches) |patches| allocator.free(patches);
    }
};

fn encodeSingleImage(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    store: *tensor_store_mod.GgufStore,
    weights: *WeightCache,
    cfg: Config,
    bytes: []const u8,
    limits: Limits,
    collect_preprocess_evidence: bool,
) !EncodedImage {
    const profile = qwen3VlProfileEnabled();
    var stage_started_at = if (profile) monotonicNowNs() else 0;
    if (bytes.len == 0 or bytes.len > limits.max_encoded_image_bytes) return error.VisionAdmissionExceeded;
    const decoded = try image.decode(allocator, bytes);
    defer decoded.deinit(allocator);
    const decode_ns = profileLap(profile, &stage_started_at);
    const decoded_pixels = std.math.mul(usize, decoded.width, decoded.height) catch return error.VisionAdmissionExceeded;
    if (decoded_pixels == 0 or decoded_pixels > limits.max_decoded_pixels) return error.VisionAdmissionExceeded;
    const geometry = try targetGeometry(cfg, decoded.width, decoded.height, limits);
    const pixels = try image.preprocessDecodedRectScaledWithResample(
        allocator,
        decoded,
        @intCast(geometry.width),
        @intCast(geometry.height),
        cfg.image_mean,
        cfg.image_std,
        1.0 / 255.0,
        .pillow_bicubic,
    );
    defer allocator.free(pixels);
    const preprocess_ns = profileLap(profile, &stage_started_at);

    const patch_rows = try extractPatchesMergeMajor(allocator, pixels, cfg, geometry);
    defer allocator.free(patch_rows);
    const patchify_ns = profileLap(profile, &stage_started_at);
    const preprocess_evidence: ?PreprocessEvidence = if (collect_preprocess_evidence) .{
        .source_width = decoded.width,
        .source_height = decoded.height,
        .resized_width = geometry.width,
        .resized_height = geometry.height,
        .grid = .{ .temporal = 1, .height = @intCast(geometry.grid_y), .width = @intCast(geometry.grid_x) },
        .patch_rows = geometry.patchCount(),
        .patch_columns = cfg.patch_size * cfg.patch_size * 3,
        .spatial_patch_f32le_sha256 = sha256F32LeHex(patch_rows),
        .positioned_embedding_f32le_sha256 = [_]u8{0} ** 64,
        .vision_trace_layer = null,
        .vision_trace_f32le_sha256 = null,
    } else null;
    const preprocess_spatial_patches = if (collect_preprocess_evidence)
        try allocator.dupe(f32, patch_rows)
    else
        null;
    errdefer if (preprocess_spatial_patches) |patches| allocator.free(patches);
    const patch_shape = [_]i32{ @intCast(geometry.patchCount()), @intCast(cfg.patch_size * cfg.patch_size * 3) };
    const patch_input = try cb.fromFloat32Shape(patch_rows, &patch_shape);
    defer cb.free(patch_input);
    const patch0 = try weights.patch("v.patch_embd.weight", cfg);
    const patch1 = try weights.patch("v.patch_embd.weight.1", cfg);
    const patch_bias = try weights.weight("v.patch_embd.bias");
    const patch_dim = cfg.patch_size * cfg.patch_size * 3;
    const first_temporal = try cb.linear(patch_input, patch0, patch_bias, geometry.patchCount(), patch_dim, cfg.vision_hidden);
    defer cb.free(first_temporal);
    const second_temporal = try cb.linearNoBias(patch_input, patch1, geometry.patchCount(), patch_dim, cfg.vision_hidden);
    defer cb.free(second_temporal);
    const embedded = try cb.add(first_temporal, second_temporal);
    defer cb.free(embedded);
    const embedded_host = try cb.toFloat32(embedded, allocator);
    defer allocator.free(embedded_host);
    try addInterpolatedPositions(store, embedded_host, cfg, geometry);
    var completed_preprocess_evidence = preprocess_evidence;
    if (completed_preprocess_evidence) |*evidence| {
        evidence.positioned_embedding_f32le_sha256 = sha256F32LeHex(embedded_host);
    }
    const hidden_shape = [_]i32{ @intCast(geometry.patchCount()), @intCast(cfg.vision_hidden) };
    var hidden = try cb.fromFloat32Shape(embedded_host, &hidden_shape);
    // Keep exactly one owner for the current vision tensor. Every successful
    // stage replaces that owner only after freeing its predecessor, so all
    // later failures (including cancellation and backend allocation errors)
    // release the live tensor exactly once.
    defer cb.free(hidden);
    const patch_embed_ns = profileLap(profile, &stage_started_at);

    const positions = try visionPositions(allocator, geometry, cfg.spatial_merge);
    defer allocator.free(positions);
    const positions_ns = profileLap(profile, &stage_started_at);
    var deepstack = std.ArrayListUnmanaged(f32).empty;
    errdefer deepstack.deinit(allocator);
    const vision_trace_layer = if (collect_preprocess_evidence)
        platform.env.getenvUsize("ANTFLY_QWEN3VL_QUALIFICATION_TRACE_LAYER")
    else
        null;
    if (vision_trace_layer) |trace_layer| {
        if (trace_layer >= cfg.block_count) return error.InvalidQwen3VlQualificationTraceLayer;
    }
    var vision_trace_digest: ?[64]u8 = null;
    var next_tap: usize = 0;
    var blocks_ns: u64 = 0;
    var deepstack_ns: u64 = 0;
    for (0..cfg.block_count) |layer| {
        const block_started_at = if (profile) monotonicNowNs() else 0;
        const next = try encoderBlock(cb, allocator, weights, cfg, hidden, positions, geometry.patchCount(), layer);
        if (profile) blocks_ns +|= monotonicNowNs() -| block_started_at;
        cb.free(hidden);
        hidden = next;
        if (vision_trace_layer != null and vision_trace_layer.? == layer) {
            const trace_host = try cb.toFloat32(hidden, allocator);
            defer allocator.free(trace_host);
            vision_trace_digest = sha256F32LeHex(trace_host);
        }
        if (next_tap < cfg.deepstack_len and cfg.deepstack_layers[next_tap] == layer) {
            const tap_started_at = if (profile) monotonicNowNs() else 0;
            const feature = try mergeProjected(cb, allocator, weights, cfg, hidden, geometry, layer);
            defer cb.free(feature);
            const host = try cb.toFloat32(feature, allocator);
            defer allocator.free(host);
            try deepstack.appendSlice(allocator, host);
            if (profile) deepstack_ns +|= monotonicNowNs() -| tap_started_at;
            next_tap += 1;
        }
    }
    stage_started_at = if (profile) monotonicNowNs() else 0;
    if (next_tap != cfg.deepstack_len) return error.InvalidGgufProjector;
    if (completed_preprocess_evidence) |*evidence| {
        evidence.vision_trace_layer = vision_trace_layer;
        evidence.vision_trace_f32le_sha256 = vision_trace_digest;
    }

    const post_norm = try layerNormNamed(cb, allocator, weights, hidden, "v.post_ln", cfg.vision_hidden, cfg.layer_norm_eps);
    cb.free(hidden);
    hidden = post_norm;
    const projected = try mergeProjectedNamed(cb, allocator, weights, cfg, post_norm, geometry, "mm.0", "mm.2");
    cb.free(hidden);
    hidden = projected;
    const embeddings = try cb.toFloat32(projected, allocator);
    const final_merge_ns = profileLap(profile, &stage_started_at);
    errdefer allocator.free(embeddings);
    const owned_deepstack = try deepstack.toOwnedSlice(allocator);
    errdefer allocator.free(owned_deepstack);
    if (profile) {
        std.debug.print(
            "qwen3vl-projector-profile: patches={d} merged_tokens={d} decode_ms={d:.3} preprocess_ms={d:.3} patchify_ms={d:.3} patch_embed_ms={d:.3} positions_ms={d:.3} blocks_ms={d:.3} deepstack_ms={d:.3} final_merge_ms={d:.3}\n",
            .{
                geometry.patchCount(),
                geometry.tokenCount(),
                @as(f64, @floatFromInt(decode_ns)) / std.time.ns_per_ms,
                @as(f64, @floatFromInt(preprocess_ns)) / std.time.ns_per_ms,
                @as(f64, @floatFromInt(patchify_ns)) / std.time.ns_per_ms,
                @as(f64, @floatFromInt(patch_embed_ns)) / std.time.ns_per_ms,
                @as(f64, @floatFromInt(positions_ns)) / std.time.ns_per_ms,
                @as(f64, @floatFromInt(blocks_ns)) / std.time.ns_per_ms,
                @as(f64, @floatFromInt(deepstack_ns)) / std.time.ns_per_ms,
                @as(f64, @floatFromInt(final_merge_ns)) / std.time.ns_per_ms,
            },
        );
    }
    return .{
        .embeddings = embeddings,
        .deepstack = owned_deepstack,
        .tokens = geometry.tokenCount(),
        .grid = .{ .temporal = 1, .height = @intCast(geometry.grid_y), .width = @intCast(geometry.grid_x) },
        .preprocess_evidence = completed_preprocess_evidence,
        .preprocess_spatial_patches = preprocess_spatial_patches,
    };
}

/// Stable qualification digest over the canonical little-endian f32 payload.
/// This is public so the generation acceptance lane can attest the exact
/// projector and DeepStack tensors handed to the decoder without serializing
/// multi-megabyte intermediates into JSON.
pub fn sha256F32LeHex(values: []const f32) [64]u8 {
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    var bytes: [4]u8 = undefined;
    for (values) |value| {
        std.mem.writeInt(u32, &bytes, @bitCast(value), .little);
        hasher.update(&bytes);
    }
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    return std.fmt.bytesToHex(digest, .lower);
}

fn encoderBlock(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    weights: *WeightCache,
    cfg: Config,
    input: CT,
    positions: []const u32,
    token_count: usize,
    layer: usize,
) !CT {
    const profile = qwen3VlProfileEnabled();
    var stage_started_at = if (profile) monotonicNowNs() else 0;
    var prefix_buf: [96]u8 = undefined;
    const ln1_prefix = try std.fmt.bufPrint(&prefix_buf, "v.blk.{d}.ln1", .{layer});
    const normed1 = try layerNormNamed(cb, allocator, weights, input, ln1_prefix, cfg.vision_hidden, cfg.layer_norm_eps);
    defer cb.free(normed1);
    const ln1_ns = profileLap(profile, &stage_started_at);
    const attn = try visionAttention(cb, allocator, weights, cfg, normed1, positions, token_count, layer);
    defer cb.free(attn);
    const attention_ns = profileLap(profile, &stage_started_at);
    const residual1 = try cb.add(input, attn);
    errdefer cb.free(residual1);
    const attention_residual_ns = profileLap(profile, &stage_started_at);

    const ln2_prefix = try std.fmt.bufPrint(&prefix_buf, "v.blk.{d}.ln2", .{layer});
    const normed2 = try layerNormNamed(cb, allocator, weights, residual1, ln2_prefix, cfg.vision_hidden, cfg.layer_norm_eps);
    defer cb.free(normed2);
    const ln2_ns = profileLap(profile, &stage_started_at);
    const fc1_prefix = try std.fmt.bufPrint(&prefix_buf, "v.blk.{d}.ffn_up", .{layer});
    const fc1 = try linearNamed(cb, allocator, weights, normed2, fc1_prefix, token_count, cfg.vision_hidden, cfg.intermediate);
    defer cb.free(fc1);
    const fc1_ns = profileLap(profile, &stage_started_at);
    const activated = try cb.geluExact(fc1) orelse try cb.gelu(fc1);
    defer cb.free(activated);
    const activation_ns = profileLap(profile, &stage_started_at);
    const fc2_prefix = try std.fmt.bufPrint(&prefix_buf, "v.blk.{d}.ffn_down", .{layer});
    const fc2 = try linearNamed(cb, allocator, weights, activated, fc2_prefix, token_count, cfg.intermediate, cfg.vision_hidden);
    defer cb.free(fc2);
    const fc2_ns = profileLap(profile, &stage_started_at);
    const output = try cb.add(residual1, fc2);
    cb.free(residual1);
    const ffn_residual_ns = profileLap(profile, &stage_started_at);
    if (profile) {
        std.debug.print(
            "qwen3vl-vision-block-profile: layer={d} ln1_ms={d:.3} attention_ms={d:.3} attn_add_ms={d:.3} ln2_ms={d:.3} fc1_ms={d:.3} activation_ms={d:.3} fc2_ms={d:.3} ffn_add_ms={d:.3}\n",
            .{
                layer,
                @as(f64, @floatFromInt(ln1_ns)) / std.time.ns_per_ms,
                @as(f64, @floatFromInt(attention_ns)) / std.time.ns_per_ms,
                @as(f64, @floatFromInt(attention_residual_ns)) / std.time.ns_per_ms,
                @as(f64, @floatFromInt(ln2_ns)) / std.time.ns_per_ms,
                @as(f64, @floatFromInt(fc1_ns)) / std.time.ns_per_ms,
                @as(f64, @floatFromInt(activation_ns)) / std.time.ns_per_ms,
                @as(f64, @floatFromInt(fc2_ns)) / std.time.ns_per_ms,
                @as(f64, @floatFromInt(ffn_residual_ns)) / std.time.ns_per_ms,
            },
        );
    }
    return output;
}

fn visionAttention(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    weights: *WeightCache,
    cfg: Config,
    input: CT,
    positions: []const u32,
    token_count: usize,
    layer: usize,
) !CT {
    const profile = qwen3VlProfileEnabled();
    var stage_started_at = if (profile) monotonicNowNs() else 0;
    var buf: [96]u8 = undefined;
    const qkv_prefix = try std.fmt.bufPrint(&buf, "v.blk.{d}.attn_qkv", .{layer});
    const qkv = try linearNamed(cb, allocator, weights, input, qkv_prefix, token_count, cfg.vision_hidden, cfg.vision_hidden * 3);
    defer cb.free(qkv);
    const qkv_ns = profileLap(profile, &stage_started_at);
    const q_unrotated = try cb.sliceLastDim(qkv, 0, cfg.vision_hidden);
    defer cb.free(q_unrotated);
    const k_unrotated = try cb.sliceLastDim(qkv, cfg.vision_hidden, cfg.vision_hidden * 2);
    defer cb.free(k_unrotated);
    const v = try cb.sliceLastDim(qkv, cfg.vision_hidden * 2, cfg.vision_hidden * 3);
    defer cb.free(v);
    const q = (try cb.visionRope(q_unrotated, token_count, cfg.headDim(), cfg.rope_theta, positions)) orelse
        return error.UnsupportedVisionRopeBackend;
    defer cb.free(q);
    const k = (try cb.visionRope(k_unrotated, token_count, cfg.headDim(), cfg.rope_theta, positions)) orelse
        return error.UnsupportedVisionRopeBackend;
    defer cb.free(k);
    const split_rope_ns = profileLap(profile, &stage_started_at);
    // Every patch is active in the single-image vision tower. An explicit
    // all-ones mask only adds a device upload and blocks specialized unmasked
    // SDPA routes without changing the mathematical result.
    const attended = try cb.scaledDotProductAttentionQwen3VlVision(q, k, v, 1, token_count, cfg.head_count, cfg.headDim());
    defer cb.free(attended);
    const sdpa_ns = profileLap(profile, &stage_started_at);
    const out_prefix = try std.fmt.bufPrint(&buf, "v.blk.{d}.attn_out", .{layer});
    const output = try linearNamed(cb, allocator, weights, attended, out_prefix, token_count, cfg.vision_hidden, cfg.vision_hidden);
    const out_ns = profileLap(profile, &stage_started_at);
    if (profile) {
        std.debug.print(
            "qwen3vl-vision-attention-profile: layer={d} qkv_ms={d:.3} split_rope_ms={d:.3} sdpa_ms={d:.3} out_ms={d:.3}\n",
            .{
                layer,
                @as(f64, @floatFromInt(qkv_ns)) / std.time.ns_per_ms,
                @as(f64, @floatFromInt(split_rope_ns)) / std.time.ns_per_ms,
                @as(f64, @floatFromInt(sdpa_ns)) / std.time.ns_per_ms,
                @as(f64, @floatFromInt(out_ns)) / std.time.ns_per_ms,
            },
        );
    }
    return output;
}

fn mergeProjected(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    weights: *WeightCache,
    cfg: Config,
    hidden: CT,
    geometry: Geometry,
    layer: usize,
) !CT {
    var norm_buf: [96]u8 = undefined;
    var fc1_buf: [96]u8 = undefined;
    var fc2_buf: [96]u8 = undefined;
    const norm = try std.fmt.bufPrint(&norm_buf, "v.deepstack.{d}.norm", .{layer});
    const fc1 = try std.fmt.bufPrint(&fc1_buf, "v.deepstack.{d}.fc1", .{layer});
    const fc2 = try std.fmt.bufPrint(&fc2_buf, "v.deepstack.{d}.fc2", .{layer});
    const merged = try cb.reshape2D(
        allocator,
        hidden,
        geometry.patchCount(),
        cfg.vision_hidden,
        geometry.tokenCount(),
        cfg.mergedHidden(),
    );
    defer cb.free(merged);
    const normed = try layerNormNamed(cb, allocator, weights, merged, norm, cfg.mergedHidden(), cfg.layer_norm_eps);
    defer cb.free(normed);
    const first = try linearNamed(cb, allocator, weights, normed, fc1, geometry.tokenCount(), cfg.mergedHidden(), cfg.mergedHidden());
    defer cb.free(first);
    const activated = try cb.geluExact(first) orelse try cb.gelu(first);
    defer cb.free(activated);
    return linearNamed(cb, allocator, weights, activated, fc2, geometry.tokenCount(), cfg.mergedHidden(), cfg.text_hidden);
}

fn mergeProjectedNamed(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    weights: *WeightCache,
    cfg: Config,
    hidden: CT,
    geometry: Geometry,
    fc1_prefix: []const u8,
    fc2_prefix: []const u8,
) !CT {
    const merged = try cb.reshape2D(
        allocator,
        hidden,
        geometry.patchCount(),
        cfg.vision_hidden,
        geometry.tokenCount(),
        cfg.mergedHidden(),
    );
    defer cb.free(merged);
    const fc1 = try linearNamed(cb, allocator, weights, merged, fc1_prefix, geometry.tokenCount(), cfg.mergedHidden(), cfg.mergedHidden());
    defer cb.free(fc1);
    const activated = try cb.geluExact(fc1) orelse try cb.gelu(fc1);
    defer cb.free(activated);
    return linearNamed(cb, allocator, weights, activated, fc2_prefix, geometry.tokenCount(), cfg.mergedHidden(), cfg.text_hidden);
}

fn layerNormNamed(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    weights: *WeightCache,
    input: CT,
    prefix: []const u8,
    dim: usize,
    eps: f32,
) !CT {
    const weight_name = try std.fmt.allocPrint(allocator, "{s}.weight", .{prefix});
    defer allocator.free(weight_name);
    const bias_name = try std.fmt.allocPrint(allocator, "{s}.bias", .{prefix});
    defer allocator.free(bias_name);
    const weight = try weights.weight(weight_name);
    const bias = try weights.weight(bias_name);
    return cb.layerNorm(input, weight, bias, dim, eps);
}

fn linearNamed(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    weights: *WeightCache,
    input: CT,
    prefix: []const u8,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !CT {
    const weight_name = try std.fmt.allocPrint(allocator, "{s}.weight", .{prefix});
    defer allocator.free(weight_name);
    const bias_name = try std.fmt.allocPrint(allocator, "{s}.bias", .{prefix});
    defer allocator.free(bias_name);
    const weight = try weights.linear(weight_name, in_dim, out_dim);
    const bias = try weights.weight(bias_name);
    return cb.linear(input, weight, bias, rows, in_dim, out_dim);
}

fn loadPatchWeight(
    cb: *const ComputeBackend,
    store: *tensor_store_mod.GgufStore,
    name: []const u8,
    cfg: Config,
) !CT {
    var loaded = try projector_common.loadTensorF32(store, name);
    defer loaded.deinit();
    if (loaded.shape.len != 4 or loaded.shape[0] != cfg.vision_hidden or loaded.shape[1] != 3 or
        loaded.shape[2] != cfg.patch_size or loaded.shape[3] != cfg.patch_size)
    {
        return error.InvalidTensorShape;
    }
    const shape = [_]i32{ @intCast(cfg.vision_hidden), @intCast(cfg.patch_size * cfg.patch_size * 3) };
    return cb.fromFloat32Shape(loaded.data, &shape);
}

fn extractPatchesMergeMajor(
    allocator: std.mem.Allocator,
    pixels: []const f32,
    cfg: Config,
    geometry: Geometry,
) ![]f32 {
    if (pixels.len != 3 * geometry.height * geometry.width) return error.InvalidPatchEmbeddingShape;
    const patch_dim = cfg.patch_size * cfg.patch_size * 3;
    const out = try allocator.alloc(f32, geometry.patchCount() * patch_dim);
    var token: usize = 0;
    for (0..geometry.merged_y) |group_y| {
        for (0..geometry.merged_x) |group_x| {
            for (0..cfg.spatial_merge) |inner_y| {
                for (0..cfg.spatial_merge) |inner_x| {
                    const patch_y = group_y * cfg.spatial_merge + inner_y;
                    const patch_x = group_x * cfg.spatial_merge + inner_x;
                    var dst = token * patch_dim;
                    for (0..3) |channel| {
                        for (0..cfg.patch_size) |py| {
                            const src_y = patch_y * cfg.patch_size + py;
                            const src = channel * geometry.height * geometry.width + src_y * geometry.width + patch_x * cfg.patch_size;
                            @memcpy(out[dst..][0..cfg.patch_size], pixels[src..][0..cfg.patch_size]);
                            dst += cfg.patch_size;
                        }
                    }
                    token += 1;
                }
            }
        }
    }
    if (token != geometry.patchCount()) return error.InvalidPatchEmbeddingShape;
    return out;
}

fn visionPositions(allocator: std.mem.Allocator, geometry: Geometry, merge: usize) ![]u32 {
    const count = geometry.patchCount();
    const position_count = std.math.mul(usize, count, 2) catch return error.InvalidPositionEmbeddingShape;
    const positions = try allocator.alloc(u32, position_count);
    var token: usize = 0;
    for (0..geometry.merged_y) |group_y| {
        for (0..geometry.merged_x) |group_x| {
            for (0..merge) |inner_y| {
                for (0..merge) |inner_x| {
                    positions[token] = @intCast(group_y * merge + inner_y);
                    positions[count + token] = @intCast(group_x * merge + inner_x);
                    token += 1;
                }
            }
        }
    }
    return positions;
}

fn addInterpolatedPositions(
    store: *tensor_store_mod.GgufStore,
    hidden: []f32,
    cfg: Config,
    geometry: Geometry,
) !void {
    var table = try projector_common.loadTensorF32(store, "v.position_embd.weight");
    defer table.deinit();
    if (table.shape.len != 2 or table.shape[1] != cfg.vision_hidden or table.shape[0] <= 0) return error.InvalidPositionEmbeddingShape;
    const position_count: usize = @intCast(table.shape[0]);
    const side = exactSquareRoot(position_count) orelse return error.InvalidPositionEmbeddingShape;
    if (hidden.len != geometry.patchCount() * cfg.vision_hidden) return error.InvalidPositionEmbeddingShape;
    var token: usize = 0;
    for (0..geometry.merged_y) |group_y| {
        for (0..geometry.merged_x) |group_x| {
            for (0..cfg.spatial_merge) |inner_y| {
                for (0..cfg.spatial_merge) |inner_x| {
                    const y = group_y * cfg.spatial_merge + inner_y;
                    const x = group_x * cfg.spatial_merge + inner_x;
                    const fy = interpolationCoordinate(y, geometry.grid_y, side);
                    const fx = interpolationCoordinate(x, geometry.grid_x, side);
                    const y0: usize = @intFromFloat(@floor(fy));
                    const x0: usize = @intFromFloat(@floor(fx));
                    const y1 = @min(y0 + 1, side - 1);
                    const x1 = @min(x0 + 1, side - 1);
                    const wy = fy - @as(f32, @floatFromInt(y0));
                    const wx = fx - @as(f32, @floatFromInt(x0));
                    const dst = token * cfg.vision_hidden;
                    for (0..cfg.vision_hidden) |h| {
                        const p00 = table.data[(y0 * side + x0) * cfg.vision_hidden + h];
                        const p01 = table.data[(y0 * side + x1) * cfg.vision_hidden + h];
                        const p10 = table.data[(y1 * side + x0) * cfg.vision_hidden + h];
                        const p11 = table.data[(y1 * side + x1) * cfg.vision_hidden + h];
                        hidden[dst + h] += (p00 * (1.0 - wx) + p01 * wx) * (1.0 - wy) +
                            (p10 * (1.0 - wx) + p11 * wx) * wy;
                    }
                    token += 1;
                }
            }
        }
    }
}

fn interpolationCoordinate(position: usize, target_side: usize, source_side: usize) f32 {
    if (target_side <= 1 or source_side <= 1) return 0;
    return @as(f32, @floatFromInt(position)) * @as(f32, @floatFromInt(source_side - 1)) /
        @as(f32, @floatFromInt(target_side - 1));
}

pub fn targetGeometry(cfg: Config, width: usize, height: usize, limits: Limits) !Geometry {
    return targetGeometryForLayout(cfg.patch_size, cfg.spatial_merge, width, height, limits);
}

fn targetGeometryForLayout(
    patch_size: usize,
    spatial_merge: usize,
    width: usize,
    height: usize,
    limits: Limits,
) !Geometry {
    try limits.validate();
    if (width == 0 or height == 0) return error.InvalidImageDimensions;
    const smaller = @min(width, height);
    const larger = @max(width, height);
    const factor = std.math.mul(usize, patch_size, spatial_merge) catch return error.InvalidImageDimensions;
    const max_aspect_extent = std.math.mul(usize, smaller, 200) catch return error.ImageAspectRatioExceeded;
    if (larger > max_aspect_extent) return error.ImageAspectRatioExceeded;
    if (width < factor or height < factor) return error.InvalidImageDimensions;
    const original_pixels = std.math.mul(usize, width, height) catch return error.InvalidImageDimensions;
    const max_tokens = @min(limits.max_merged_tokens, limits.hard_max_merged_tokens);
    const factor_area = std.math.mul(usize, factor, factor) catch return error.InvalidVisionLimits;
    const max_pixels = std.math.mul(usize, max_tokens, factor_area) catch return error.InvalidVisionLimits;
    const min_pixels = std.math.mul(usize, factor_area, limits.min_merged_tokens) catch return error.InvalidVisionLimits;
    // Match Transformers `smart_resize` exactly: first round each original
    // dimension to the merge factor. Only if that rounded area violates a
    // pixel bound do we scale, using floor for the maximum and ceil for the
    // minimum. Scaling to the clamped area and rounding to nearest is not
    // equivalent for small/non-square inputs (227x149 must become 320x224).
    var target_w = roundIntegerToFactor(width, factor);
    var target_h = roundIntegerToFactor(height, factor);
    const rounded_pixels = std.math.mul(usize, target_w, target_h) catch return error.InvalidImageDimensions;
    if (rounded_pixels > max_pixels) {
        const scale_down = @sqrt(@as(f64, @floatFromInt(original_pixels)) / @as(f64, @floatFromInt(max_pixels)));
        target_w = floorScaledToFactor(@as(f64, @floatFromInt(width)) / scale_down, factor);
        target_h = floorScaledToFactor(@as(f64, @floatFromInt(height)) / scale_down, factor);
    } else if (rounded_pixels < min_pixels) {
        const scale_up = @sqrt(@as(f64, @floatFromInt(min_pixels)) / @as(f64, @floatFromInt(original_pixels)));
        target_w = ceilScaledToFactor(@as(f64, @floatFromInt(width)) * scale_up, factor);
        target_h = ceilScaledToFactor(@as(f64, @floatFromInt(height)) * scale_up, factor);
    }
    const grid_x = target_w / patch_size;
    const grid_y = target_h / patch_size;
    if (grid_x == 0 or grid_y == 0 or grid_x % spatial_merge != 0 or grid_y % spatial_merge != 0) {
        return error.InvalidImageDimensions;
    }
    const geometry = Geometry{
        .width = target_w,
        .height = target_h,
        .grid_x = grid_x,
        .grid_y = grid_y,
        .merged_x = grid_x / spatial_merge,
        .merged_y = grid_y / spatial_merge,
    };
    // The upstream maximum-area branch floors each dimension independently;
    // at a tight cap its final area may consequently fall below min_pixels.
    if (geometry.tokenCount() > max_tokens) return error.VisionAdmissionExceeded;
    return geometry;
}

fn roundIntegerToFactor(value: usize, factor: usize) usize {
    const quotient = value / factor;
    const remainder = value % factor;
    const doubled_remainder = remainder * 2;
    const rounded = if (doubled_remainder < factor)
        quotient
    else if (doubled_remainder > factor)
        quotient + 1
    else if (quotient % 2 == 0)
        quotient
    else
        quotient + 1;
    return @max(@as(usize, 1), rounded) * factor;
}

fn floorScaledToFactor(value: f64, factor: usize) usize {
    const units: usize = @intFromFloat(@floor(value / @as(f64, @floatFromInt(factor))));
    return @max(@as(usize, 1), units) * factor;
}

fn ceilScaledToFactor(value: f64, factor: usize) usize {
    const units: usize = @intFromFloat(@ceil(value / @as(f64, @floatFromInt(factor))));
    return @max(@as(usize, 1), units) * factor;
}

fn parseConfig(file: *const gguf_format.File) !Config {
    const contract = try projector_format.inspectFileContract(file);
    if (contract.kind != .clip_qwen3vl_image) return error.InvalidGgufProjector;
    const view = gguf_metadata.View.init(file);
    var cfg = Config{
        .text_hidden = @intCast(view.getU64("clip.vision.projection_dim") orelse return error.InvalidGgufProjector),
        .vision_hidden = @intCast(view.getU64("clip.vision.embedding_length") orelse return error.InvalidGgufProjector),
        .intermediate = @intCast(view.getU64("clip.vision.feed_forward_length") orelse return error.InvalidGgufProjector),
        .block_count = @intCast(view.getU64("clip.vision.block_count") orelse return error.InvalidGgufProjector),
        .head_count = @intCast(view.getU64("clip.vision.attention.head_count") orelse return error.InvalidGgufProjector),
        .image_size = @intCast(view.getU64("clip.vision.image_size") orelse return error.InvalidGgufProjector),
        .patch_size = @intCast(view.getU64("clip.vision.patch_size") orelse return error.InvalidGgufProjector),
        .spatial_merge = @intCast(view.getU64("clip.vision.spatial_merge_size") orelse return error.InvalidGgufProjector),
        .layer_norm_eps = view.getF32("clip.vision.attention.layer_norm_epsilon") orelse 1e-6,
        .image_mean = try metadataF32x3(view, "clip.vision.image_mean"),
        .image_std = try metadataF32x3(view, "clip.vision.image_std"),
    };
    const deepstack_entry = view.find("clip.vision.is_deepstack_layers") orelse return error.InvalidGgufProjector;
    const values = switch (deepstack_entry.value) {
        .array => |array| array.values,
        else => return error.InvalidGgufProjector,
    };
    if (values.len != cfg.block_count or cfg.block_count > cfg.deepstack_layers.len) return error.InvalidGgufProjector;
    for (values, 0..) |value, layer| {
        const enabled = switch (value) {
            .bool_ => |flag| flag,
            else => return error.InvalidGgufProjector,
        };
        if (enabled) {
            cfg.deepstack_layers[cfg.deepstack_len] = layer;
            cfg.deepstack_len += 1;
        }
    }
    if (cfg.deepstack_len == 0 or cfg.deepstack_len != contract.deepstack_layer_count) return error.InvalidGgufProjector;
    return cfg;
}

fn metadataF32x3(view: gguf_metadata.View, key: []const u8) ![3]f32 {
    const entry = view.find(key) orelse return error.InvalidGgufProjector;
    const array = switch (entry.value) {
        .array => |value| value,
        else => return error.InvalidGgufProjector,
    };
    if (array.values.len != 3) return error.InvalidGgufProjector;
    var out: [3]f32 = undefined;
    for (array.values, 0..) |value, i| {
        out[i] = switch (value) {
            .f32 => |v| v,
            .f64 => |v| @floatCast(v),
            else => return error.InvalidGgufProjector,
        };
    }
    return out;
}

fn exactSquareRoot(value: usize) ?usize {
    const root: usize = @intFromFloat(@sqrt(@as(f64, @floatFromInt(value))));
    return if (root * root == value) root else null;
}

test "Qwen3-VL image geometry is merge aligned and admission bounded" {
    const cfg = Config{
        .text_hidden = 2048,
        .vision_hidden = 1024,
        .intermediate = 4096,
        .block_count = 24,
        .head_count = 16,
        .image_size = 768,
        .patch_size = 16,
        .spatial_merge = 2,
        .layer_norm_eps = 1e-6,
        .image_mean = .{ 0.5, 0.5, 0.5 },
        .image_std = .{ 0.5, 0.5, 0.5 },
    };
    const geometry = try targetGeometry(cfg, 768, 768, .{});
    try std.testing.expectEqual(@as(usize, 768), geometry.width);
    try std.testing.expectEqual(@as(usize, 48 * 48), geometry.patchCount());
    try std.testing.expectEqual(@as(usize, 576), geometry.tokenCount());
    const minimum_scaled_landscape = try targetGeometry(cfg, 227, 149, .{});
    try std.testing.expectEqual(@as(usize, 320), minimum_scaled_landscape.width);
    try std.testing.expectEqual(@as(usize, 224), minimum_scaled_landscape.height);
    try std.testing.expectEqual(@as(usize, 70), minimum_scaled_landscape.tokenCount());
    const bounded = try targetGeometry(cfg, 8000, 4000, .{ .max_merged_tokens = 64 });
    try std.testing.expect(bounded.tokenCount() <= 64);
    try std.testing.expectEqual(@as(usize, 0), bounded.grid_x % 2);
    try std.testing.expectEqual(@as(usize, 0), bounded.grid_y % 2);
    try std.testing.expectError(error.ImageAspectRatioExceeded, targetGeometry(cfg, 100_000, 1, .{}));
}

test "Qwen3-VL admission uses exact projector geometry and reserves transient peaks" {
    const config = gpt_config.Config{
        .family = .qwen3_vl,
        .hidden_size = 2048,
        .vision_hidden_size = 1024,
        .vision_intermediate_size = 4096,
        .vision_num_attention_heads = 16,
        .vision_patch_size = 16,
        .vision_spatial_merge_size = 2,
        .vision_deepstack_visual_indexes_len = 3,
    };
    const estimate = try estimateAdmissionForImageDimensions(
        &.{
            .{ .width = 2048, .height = 1416 },
            .{ .width = 227, .height = 149 },
        },
        config,
        .{},
    );
    const first = try targetGeometryForLayout(16, 2, 2048, 1416, .{});
    const second = try targetGeometryForLayout(16, 2, 227, 149, .{});
    try std.testing.expectEqual(first.tokenCount() + second.tokenCount(), estimate.visual_tokens);
    try std.testing.expect(estimate.host_scratch_bytes > 0);
    try std.testing.expect(estimate.backend_scratch_bytes > estimate.host_scratch_bytes / 4);
}

test "Qwen3-VL admission rejects oversized decoded images before projector execution" {
    const config = gpt_config.Config{
        .family = .qwen3_vl,
        .hidden_size = 2048,
        .vision_hidden_size = 1024,
        .vision_intermediate_size = 4096,
        .vision_num_attention_heads = 16,
        .vision_patch_size = 16,
        .vision_spatial_merge_size = 2,
    };
    try std.testing.expectError(
        error.VisionAdmissionExceeded,
        estimateAdmissionForImageDimensions(
            &.{.{ .width = 10_001, .height = 10_000 }},
            config,
            .{ .max_decoded_pixels = 100_000_000 },
        ),
    );
}

test "Qwen3-VL qualification digest is canonical little-endian f32" {
    const digest = sha256F32LeHex(&.{ 0.0, 1.0, -2.5 });
    try std.testing.expectEqualStrings(
        "4356516ed57de986ba8080c557e8856871336d6a17b170fb946df125605466c9",
        &digest,
    );
}

test "Qwen3-VL vision positions preserve 2x2 merge-major order" {
    const positions = try visionPositions(std.testing.allocator, .{
        .width = 64,
        .height = 32,
        .grid_x = 4,
        .grid_y = 2,
        .merged_x = 2,
        .merged_y = 1,
    }, 2);
    defer std.testing.allocator.free(positions);
    try std.testing.expectEqualSlices(u32, &.{ 0, 0, 1, 1, 0, 0, 1, 1 }, positions[0..8]);
    try std.testing.expectEqualSlices(u32, &.{ 0, 1, 0, 1, 2, 3, 2, 3 }, positions[8..16]);
}

test "Qwen3-VL prompt preparation keeps M-RoPE mask and DeepStack aligned" {
    const native_compute = @import("../ops/native_compute.zig");
    const tensor_mod = @import("../backends/tensor.zig");
    const weight_source = @import("../models/weight_source.zig");
    const allocator = std.testing.allocator;
    var store = native_compute.WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    defer {
        native_compute.deinitPrefetchQueue(&store);
        var iterator = store.resident_weights.iterator();
        while (iterator.next()) |entry| {
            allocator.free(entry.key_ptr.*);
            entry.value_ptr.deinit();
        }
        store.resident_weights.deinit(allocator);
        store.lazy_weights.deinit(allocator);
    }
    const name = try allocator.dupe(u8, "model.embed_tokens.weight");
    var weight_data: [12]f32 = undefined;
    for (&weight_data, 0..) |*value, i| value.* = @floatFromInt(i);
    var tensor = try tensor_mod.Tensor.initFloat32(allocator, name, &.{ 6, 2 }, &weight_data);
    errdefer tensor.deinit();
    try store.resident_weights.put(allocator, name, weight_source.LoadedWeight{ .tensor = tensor });
    var compute = native_compute.NativeCompute.init(allocator, &store, null);
    defer compute.weight_reservations.deinit(allocator);
    var cb = ComputeBackend{ .ptr = &compute, .vtable = &native_compute.vtable_impl };
    var projected = ProjectedImages{
        .allocator = allocator,
        .embeddings = try allocator.dupe(f32, &.{ 100, 101, 200, 201 }),
        .deepstack_embeddings = try allocator.dupe(f32, &.{ 1, 2, 3, 4 }),
        .tokens_per_image = try allocator.dupe(usize, &.{2}),
        .grids = try allocator.dupe(qwen3vl_plan.VisionGrid, &.{.{ .temporal = 1, .height = 2, .width = 4 }}),
        .preprocess_evidence = try allocator.alloc(PreprocessEvidence, 0),
        .preprocess_spatial_patches = try allocator.alloc(f32, 0),
        .hidden_size = 2,
        .deepstack_layer_count = 1,
    };
    defer projected.deinit();
    const config = gpt_config.Config{
        .family = .qwen3_vl,
        .hidden_size = 2,
        .vocab_size = 6,
        .image_token_index = 5,
        .vision_spatial_merge_size = 2,
    };
    var prepared = try prepareExpandedPromptEmbeddings(&cb, allocator, config, &.{ 1, 5, 2 }, projected, 16);
    defer prepared.deinit(&cb);
    try std.testing.expectEqualSlices(i64, &.{ 1, 5, 5, 2 }, prepared.token_ids);
    try std.testing.expectEqualSlices(bool, &.{ false, true, true, false }, prepared.plan.visual_token_mask);
    try std.testing.expectEqual(@as(usize, 1), prepared.deepstack_layer_count);
    const values = try cb.toFloat32(prepared.input_embeddings.?, allocator);
    defer allocator.free(values);
    try std.testing.expectEqualSlices(f32, &.{ 2, 3, 100, 101, 200, 201, 4, 5 }, values);
}
