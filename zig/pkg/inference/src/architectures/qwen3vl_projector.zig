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

//! Strict Qwen3-VL vision tower and DeepStack projector.
//!
//! The official projector is not a CLIP pooling head: it contains a temporal
//! split Conv3D patch embed, learned-position interpolation, two-axis vision
//! RoPE, a 2x2 concatenating merger, and multiple DeepStack feature heads.
//! This module keeps those outputs separate so decoder integration cannot
//! accidentally discard DeepStack or treat its concatenated payload as tokens.

const std = @import("std");
const build_options = @import("build_options");
const platform = @import("antfly_platform");
const image = @import("../pipelines/image.zig");
const image_work_control = @import("antfly_image").work_control;
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

/// Own the frame before releasing tensors retained across vision blocks.
/// The caller keeps the current output; this group owns only predecessors.
const VisionFrameGroup = struct {
    cb: *const ComputeBackend,
    blocks: usize,
    streamed: bool = false,
    owns_frame: bool = false,
    held: [4]?CT = @splat(null),
    count: usize = 0,
    pending: [4]?CT = @splat(null),
    pending_count: usize = 0,
    submitted: bool = false,
    submits: usize = 0,
    waits: usize = 0,
    overlaps: usize = 0,

    fn begin(self: *@This(), enabled: bool) !void {
        if (self.owns_frame) return;
        if (self.streamed and !self.submitted and self.cb.decoderRuntimeHasSubmittedFrame()) return error.PreparedVisionUnavailable;
        if (!enabled or self.cb.decoderRuntimeHasActiveFrame()) {
            if (self.streamed) return error.PreparedVisionUnavailable;
            return;
        }
        self.owns_frame = try self.cb.decoderRuntimeBeginFrame();
        if (self.streamed and !self.owns_frame) return error.PreparedVisionUnavailable;
        if (self.owns_frame and self.submitted) self.overlaps += 1;
    }

    fn release(self: *@This()) void {
        for (self.held[0..self.count]) |tensor| self.cb.free(tensor.?);
        self.count = 0;
    }

    fn releasePending(self: *@This()) void {
        for (self.pending[0..self.pending_count]) |tensor| self.cb.free(tensor.?);
        self.pending_count = 0;
    }

    fn wait(self: *@This()) !void {
        if (!self.submitted) return;
        try self.cb.decoderRuntimeWaitSubmittedFrame();
        self.submitted = false;
        self.waits += 1;
        self.releasePending();
    }

    fn finishBlock(self: *@This(), previous: CT, last: bool) !void {
        if (!self.owns_frame) {
            self.cb.free(previous);
            return;
        }
        self.held[self.count] = previous;
        self.count += 1;
        if (self.count == self.blocks or last) {
            if (self.streamed) {
                // The active group's commands are encoded while its producer
                // executes. Drain that producer before rotating either bank.
                try self.wait();
                try self.cb.decoderRuntimeSubmitFrame();
                self.owns_frame = false;
                self.submitted = true;
                @memcpy(self.pending[0..self.count], self.held[0..self.count]);
                self.pending_count = self.count;
                self.count = 0;
                self.submits += 1;
                if (last) try self.wait();
            } else {
                try self.cb.decoderRuntimeSubmitAndWaitFrame();
                self.owns_frame = false;
                self.submits += 1;
                self.release();
            }
        }
    }

    fn deinit(self: *@This()) void {
        if (self.owns_frame) self.cb.decoderRuntimeCancelFrame() catch {};
        self.owns_frame = false;
        // The active group's input can be an output of the submitted group.
        // Neither set may retire before that producer completes, even on error.
        if (self.submitted) self.cb.decoderRuntimeWaitSubmittedFrame() catch {};
        self.submitted = false;
        self.releasePending();
        self.release();
    }
};

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
    /// Consecutive vision blocks retained by one Metal submission. Admission
    /// charges this many complete block envelopes before execution.
    metal_frame_blocks: u8 = 1,
    /// One submitted plus one active group. Charge both complete envelopes.
    /// Restricted to one/two blocks per group; total retained blocks <= four.
    metal_frame_pipeline: bool = false,

    pub fn retainedFrameBlocks(self: Limits) usize {
        return @as(usize, self.metal_frame_blocks) * (if (self.metal_frame_pipeline) @as(usize, 2) else 1);
    }

    pub fn validate(self: Limits) !void {
        if (self.max_encoded_image_bytes == 0 or self.max_decoded_pixels == 0 or self.max_images == 0 or
            self.min_merged_tokens < 4 or self.max_merged_tokens < self.min_merged_tokens or
            self.hard_max_merged_tokens < self.min_merged_tokens or
            self.max_merged_tokens > self.hard_max_merged_tokens or
            self.max_images > 64 or self.hard_max_merged_tokens > 4096 or
            self.metal_frame_blocks == 0 or self.metal_frame_blocks > 4 or
            (self.metal_frame_pipeline and self.metal_frame_blocks > 2))
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

/// A closed Metal graph with these two qualified vision geometries can use
/// the linear working bound below. Callers must enforce resident execution.
pub fn supportsResidentVisionBudget(config: gpt_config.Config) bool {
    const h = config.vision_hidden_size;
    return config.family == .qwen3_5 and (h == 768 or h == 1024) and
        config.vision_num_attention_heads == h / 64 and
        config.vision_intermediate_size == h * 4 and
        config.vision_spatial_merge_size == 2 and
        config.vision_deepstack_visual_indexes_len == 0;
}

fn estimateAdmissionForImageDimensions(
    dimensions: []const image.EncodedImageInfo,
    config: gpt_config.Config,
    limits: Limits,
) !AdmissionEstimate {
    try limits.validate();
    if (config.qwen35_metal_resident_vision and !supportsResidentVisionBudget(config)) return error.UnsupportedResidentVisionBudget;
    if (dimensions.len == 0 or dimensions.len > limits.max_images) return error.ImageLimitExceeded;
    if ((config.family != .qwen3_vl and config.family != .qwen3_5) or config.vision_patch_size == 0 or
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
    // Resident blocks reject every host fallback for norm, copies, linear,
    // activation, residual and SDPA. No S-by-S score tensor can be allocated.
    // Keep a deliberately loose linear bound even without pool reuse: all
    // block intermediates <=24H, BF16 projection packs <=3.5H, initial upload
    // <=H. Existing 7H + this 20H + merge (>=3H) covers >=30H before the
    // separately charged two SDPA packing banks. Frame grouping still scales
    // the complete bound, including weights/output ownership as before.
    const attention_working = if (config.qwen35_metal_resident_vision)
        try admissionMul(hidden_rows, 20)
    else
        attention_scores;
    backend_scratch = try admissionAdd(backend_scratch, @max(attention_working, ffn_intermediates));
    backend_scratch = try admissionAdd(backend_scratch, merge_intermediates);
    // Packed vision SDPA keeps two half Q/K/V workspaces for overlapping
    // command buffers. They remain live through FFN and epoch reuse, so charge
    // them in addition to the earlier score/FFN maximum, even on older devices.
    // Larger fallback geometries may follow a supported image in the same
    // epoch, so retain the bounded 4096-row packing charge for those too.
    if (((vision_hidden == 768 and head_count == 12) or (vision_hidden == 1024 and head_count == 16)) and
        max_patch_tokens > 256)
    {
        const padded_rows = try admissionMul(try admissionAdd(@min(max_patch_tokens, 4096), 127) / 128, 128);
        const packed_vision_pair = try admissionMul(try admissionF32Bytes(padded_rows, vision_hidden), 3);
        backend_scratch = try admissionAdd(backend_scratch, packed_vision_pair);
    }

    return .{
        .visual_tokens = total_visual_tokens,
        .host_scratch_bytes = try admissionMul(host_scratch, limits.retainedFrameBlocks()),
        .backend_scratch_bytes = try admissionMul(backend_scratch, limits.retainedFrameBlocks()),
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
    is_qwen35: bool = false,
    block_gelu_tanh: bool = false,
    require_metal_resident_vision: bool = false,
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
    deepstack_layers: [64]usize = @as([64]usize, @splat(0)),
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
/// then retires transient slots before their wrappers die. Integrated Metal
/// slots may instead retain their provider-cache-owned backing for the epoch.
const WeightCache = struct {
    const Source = union(enum) {
        gguf: *tensor_store_mod.GgufStore,
        resident: Config,
    };

    allocator: std.mem.Allocator,
    cb: *const ComputeBackend,
    source: Source,
    tensors: std.StringHashMapUnmanaged(CT) = .empty,
    resident_patch_f32: ?[]f32 = null,
    position_f32: ?[]f32 = null,

    fn initGguf(
        allocator: std.mem.Allocator,
        cb: *const ComputeBackend,
        store: *tensor_store_mod.GgufStore,
    ) WeightCache {
        return .{ .allocator = allocator, .cb = cb, .source = .{ .gguf = store } };
    }

    fn initResident(
        allocator: std.mem.Allocator,
        cb: *const ComputeBackend,
        config: Config,
    ) WeightCache {
        return .{ .allocator = allocator, .cb = cb, .source = .{ .resident = config } };
    }

    pub fn deinit(self: *WeightCache) void {
        var value_it = self.tensors.valueIterator();
        while (value_it.next()) |tensor| {
            if (self.cb.kind() == .metal) {
                metal_compute_mod.MetalCompute.releaseProjectorSlotsForTensor(self.cb, tensor.*);
            }
            self.cb.free(tensor.*);
        }
        var key_it = self.tensors.keyIterator();
        while (key_it.next()) |key| self.allocator.free(key.*);
        self.tensors.deinit(self.allocator);
        if (self.resident_patch_f32) |values| self.allocator.free(values);
        if (self.position_f32) |values| self.allocator.free(values);
    }

    fn insert(self: *WeightCache, key: []const u8, tensor: CT) !CT {
        const owned_key = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(owned_key);
        try self.tensors.putNoClobber(self.allocator, owned_key, tensor);
        return tensor;
    }

    fn weight(self: *WeightCache, name: []const u8) !CT {
        try self.cb.checkExecutionControl();
        const key = try std.fmt.allocPrint(self.allocator, "weight:{s}", .{name});
        defer self.allocator.free(key);
        if (self.tensors.get(key)) |tensor| return tensor;
        const tensor = switch (self.source) {
            .gguf => |store| try projector_common.loadWeightCt(self.cb, self.allocator, store, name),
            .resident => |cfg| blk: {
                const resident_name = try residentWeightNameAlloc(self.allocator, cfg, name);
                defer self.allocator.free(resident_name);
                break :blk try self.cb.getWeight(resident_name);
            },
        };
        errdefer self.cb.free(tensor);
        return self.insert(key, tensor);
    }

    fn linear(self: *WeightCache, name: []const u8, in_dim: usize, out_dim: usize) !CT {
        try self.cb.checkExecutionControl();
        const key = try std.fmt.allocPrint(self.allocator, "linear:{d}:{d}:{s}", .{ in_dim, out_dim, name });
        defer self.allocator.free(key);
        if (self.tensors.get(key)) |tensor| return tensor;
        switch (self.source) {
            .resident => |cfg| {
                const resident_name = try residentWeightNameAlloc(self.allocator, cfg, name);
                defer self.allocator.free(resident_name);
                const tensor = try self.cb.getWeight(resident_name);
                errdefer self.cb.free(tensor);
                return self.insert(key, tensor);
            },
            .gguf => {},
        }
        const store = switch (self.source) {
            .gguf => |value| value,
            .resident => unreachable,
        };
        if (self.cb.kind() == .metal) {
            var tensor_ref = try store.tensorStore().describeTensor(self.allocator, name);
            defer tensor_ref.deinit(self.allocator);
            if (tensor_ref.quantized) {
                if (try store.tensorStore().loadQuantizedStorageRef(&tensor_ref)) |storage_value| {
                    var storage = storage_value;
                    const shape_matches = storage.shape.len == 2 and
                        storage.shape[0] == @as(i64, @intCast(out_dim)) and
                        storage.shape[1] == @as(i64, @intCast(in_dim));
                    if (shape_matches) {
                        // takeQuantizedStorage consumes storage on every
                        // outcome. Unsupported IQ-family formats still have a
                        // valid host-dequantized path below, so only propagate
                        // genuine upload/allocation failures.
                        const maybe_tensor: ?CT = metal_compute_mod.MetalCompute.takeQuantizedStorage(self.cb, storage) catch |err| fallback: {
                            if (@as(anyerror, err) == error.UnsupportedTensorType) break :fallback null;
                            return err;
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
            store,
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
        switch (self.source) {
            .resident => {
                const full = if (self.resident_patch_f32) |values|
                    values
                else blk: {
                    const source = try self.cb.getWeight("model.visual.patch_embed.proj.weight");
                    defer self.cb.free(source);
                    const values = try self.cb.toFloat32(source, self.allocator);
                    self.resident_patch_f32 = values;
                    break :blk values;
                };
                const patch_area = try std.math.mul(usize, cfg.patch_size, cfg.patch_size);
                const patch_dim = try std.math.mul(usize, 3, patch_area);
                const expected = try std.math.mul(usize, try std.math.mul(usize, cfg.vision_hidden, patch_dim), 2);
                if (full.len != expected) return error.InvalidTensorShape;
                const temporal_index: usize = if (std.mem.endsWith(u8, name, ".weight.1")) 1 else 0;
                const values = try self.allocator.alloc(f32, cfg.vision_hidden * patch_dim);
                defer self.allocator.free(values);
                for (0..cfg.vision_hidden) |out| {
                    for (0..3) |channel| {
                        for (0..patch_area) |pixel| {
                            const dst = out * patch_dim + channel * patch_area + pixel;
                            const src = ((((out * 3 + channel) * 2 + temporal_index) * patch_area) + pixel);
                            values[dst] = full[src];
                        }
                    }
                }
                const shape = [_]i32{ @intCast(cfg.vision_hidden), @intCast(patch_dim) };
                const tensor = try self.cb.fromFloat32Shape(values, &shape);
                errdefer self.cb.free(tensor);
                return self.insert(key, tensor);
            },
            .gguf => {},
        }
        const store = switch (self.source) {
            .gguf => |value| value,
            .resident => unreachable,
        };
        const tensor = try loadPatchWeight(self.cb, store, name, cfg);
        errdefer self.cb.free(tensor);
        return self.insert(key, tensor);
    }

    /// The integrated safetensors checkpoint stores Conv3D patch weights as
    /// native BF16 [O, C, T=2, H, W].  CUDA can consume that flattened tensor
    /// directly with cuBLASLt when the still-image patch row is duplicated in
    /// the same C,T,H,W order.  Keeping this wrapper resident avoids the old
    /// per-request BF16->host-F32 download, two F32 uploads, and two scalar
    /// dense projections.
    fn fullResidentPatch(self: *WeightCache) !?CT {
        switch (self.source) {
            .gguf => return null,
            .resident => {},
        }
        const key = "patch:resident-full-bf16";
        if (self.tensors.get(key)) |tensor| return tensor;
        const tensor = try self.cb.getWeight("model.visual.patch_embed.proj.weight");
        errdefer self.cb.free(tensor);
        return try self.insert(key, tensor);
    }

    fn positionValues(self: *WeightCache, cfg: Config) ![]const f32 {
        if (self.position_f32) |values| return values;
        const values = switch (self.source) {
            .gguf => |store| blk: {
                var loaded = try projector_common.loadTensorF32(store, "v.position_embd.weight");
                defer loaded.deinit();
                break :blk try self.allocator.dupe(f32, loaded.data);
            },
            .resident => blk: {
                const tensor = try self.weight("v.position_embd.weight");
                break :blk try self.cb.toFloat32(tensor, self.allocator);
            },
        };
        const side = cfg.image_size / cfg.patch_size;
        const expected = try std.math.mul(usize, try std.math.mul(usize, side, side), cfg.vision_hidden);
        if (values.len != expected) {
            self.allocator.free(values);
            return error.InvalidPositionEmbeddingShape;
        }
        self.position_f32 = values;
        return values;
    }
};

fn residentWeightNameAlloc(allocator: std.mem.Allocator, cfg: Config, name: []const u8) ![]u8 {
    if (std.mem.eql(u8, name, "v.patch_embd.bias"))
        return allocator.dupe(u8, "model.visual.patch_embed.proj.bias");
    if (std.mem.eql(u8, name, "v.position_embd.weight"))
        return allocator.dupe(u8, "model.visual.pos_embed.weight");
    if (std.mem.startsWith(u8, name, "v.post_ln."))
        return std.fmt.allocPrint(allocator, "model.visual.merger.norm.{s}", .{name["v.post_ln.".len..]});
    if (std.mem.startsWith(u8, name, "mm.0."))
        return std.fmt.allocPrint(allocator, "model.visual.merger.linear_fc1.{s}", .{name["mm.0.".len..]});
    if (std.mem.startsWith(u8, name, "mm.2."))
        return std.fmt.allocPrint(allocator, "model.visual.merger.linear_fc2.{s}", .{name["mm.2.".len..]});

    const block_prefix = "v.blk.";
    if (std.mem.startsWith(u8, name, block_prefix)) {
        const rest = name[block_prefix.len..];
        const dot = std.mem.indexOfScalar(u8, rest, '.') orelse return error.WeightNotFound;
        const layer = try std.fmt.parseInt(usize, rest[0..dot], 10);
        if (layer >= cfg.block_count) return error.WeightNotFound;
        const component = rest[dot + 1 ..];
        const mappings = [_]struct { logical: []const u8, resident: []const u8 }{
            .{ .logical = "ln1.", .resident = "norm1." },
            .{ .logical = "ln2.", .resident = "norm2." },
            .{ .logical = "attn_qkv.", .resident = "attn.qkv." },
            .{ .logical = "attn_out.", .resident = "attn.proj." },
            .{ .logical = "ffn_up.", .resident = "mlp.linear_fc1." },
            .{ .logical = "ffn_down.", .resident = "mlp.linear_fc2." },
        };
        for (mappings) |mapping| {
            if (std.mem.startsWith(u8, component, mapping.logical)) {
                return std.fmt.allocPrint(
                    allocator,
                    "model.visual.blocks.{d}.{s}{s}",
                    .{ layer, mapping.resident, component[mapping.logical.len..] },
                );
            }
        }
        return error.WeightNotFound;
    }

    const deepstack_prefix = "v.deepstack.";
    if (std.mem.startsWith(u8, name, deepstack_prefix)) {
        const rest = name[deepstack_prefix.len..];
        const dot = std.mem.indexOfScalar(u8, rest, '.') orelse return error.WeightNotFound;
        const layer = try std.fmt.parseInt(usize, rest[0..dot], 10);
        var tap: ?usize = null;
        for (cfg.deepstack_layers[0..cfg.deepstack_len], 0..) |candidate, index| {
            if (candidate == layer) {
                tap = index;
                break;
            }
        }
        const tap_index = tap orelse return error.WeightNotFound;
        const component = rest[dot + 1 ..];
        const mappings = [_]struct { logical: []const u8, resident: []const u8 }{
            .{ .logical = "norm.", .resident = "norm." },
            .{ .logical = "fc1.", .resident = "linear_fc1." },
            .{ .logical = "fc2.", .resident = "linear_fc2." },
        };
        for (mappings) |mapping| {
            if (std.mem.startsWith(u8, component, mapping.logical)) {
                return std.fmt.allocPrint(
                    allocator,
                    "model.visual.deepstack_merger_list.{d}.{s}{s}",
                    .{ tap_index, mapping.resident, component[mapping.logical.len..] },
                );
            }
        }
    }
    return error.WeightNotFound;
}

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
    try cb.checkExecutionControl();
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
    const cfg = try parseConfig(&store.parsed);
    var weights = WeightCache.initGguf(allocator, cb, store);
    defer weights.deinit();
    return encodeProjectedImagesFromWeightsQualified(cb, allocator, &weights, cfg, images, limits, collect_preprocess_evidence);
}

/// Run the upstream integrated safetensors vision tower whose weights already
/// reside on the model compute backend. This is the BF16 counterpart of the
/// split GGUF projector path and deliberately shares the exact preprocessing,
/// M-RoPE, DeepStack, and decoder-injection graph.
fn embedSpatialPatchRows(cb: *const ComputeBackend, allocator: std.mem.Allocator, weights: *WeightCache, cfg: Config, patches: []const f32, geometry: Geometry) !CT {
    const bias = try weights.weight("v.patch_embd.bias");
    const patch_dim = cfg.patch_size * cfg.patch_size * 3;
    const full = if (comptime build_options.enable_cuda)
        if (cb.kind() == .cuda) try weights.fullResidentPatch() else null
    else
        null;
    if (full) |weight| {
        const temporal = try duplicateStillImageTemporalPatches(allocator, patches, geometry.patchCount(), cfg.patch_size);
        defer allocator.free(temporal);
        const input = try cb.fromFloat32Shape(temporal, &.{ @intCast(geometry.patchCount()), @intCast(patch_dim * 2) });
        defer cb.free(input);
        return cb.linear(input, weight, bias, geometry.patchCount(), patch_dim * 2, cfg.vision_hidden);
    }
    const input = try cb.fromFloat32Shape(patches, &.{ @intCast(geometry.patchCount()), @intCast(patch_dim) });
    defer cb.free(input);
    const w0 = try weights.patch("v.patch_embd.weight", cfg);
    const w1 = try weights.patch("v.patch_embd.weight.1", cfg);
    const first = try cb.linear(input, w0, bias, geometry.patchCount(), patch_dim, cfg.vision_hidden);
    defer cb.free(first);
    const second = try cb.linearNoBias(input, w1, geometry.patchCount(), patch_dim, cfg.vision_hidden);
    defer cb.free(second);
    return cb.add(first, second);
}

/// CUDA groups independently admitted pages under the same byte limits. Metal
/// retains its existing qualified four-page submission envelope.
pub const resident_vision_batch_capacity: usize = 16;
pub fn maxResidentVisionBatchItems(backend: ops.BackendKind) usize {
    if (backend != .cuda) return 4;
    return if (platform.env.getenvBool("ANTFLY_LIGHTONOCR3_CUDA_WIDE_MICROBATCHES")) resident_vision_batch_capacity else 8;
}

/// Exact resized geometry for a bounded, independent-image vision batch.
pub fn residentBatchGeometryKey(config: gpt_config.Config, bytes: []const u8, limits: Limits) !u64 {
    try limits.validate();
    if (bytes.len == 0 or bytes.len > limits.max_encoded_image_bytes) return error.VisionAdmissionExceeded;
    const cfg = try configFromResidentModel(config);
    const info = try image.inspectEncodedForInference(bytes, null);
    if (try admissionMul(info.width, info.height) > limits.max_decoded_pixels) return error.VisionAdmissionExceeded;
    const geometry = try targetGeometry(cfg, info.width, info.height, limits);
    if (geometry.patchCount() > 4096) return error.UnsupportedBatchedVision;
    return (@as(u64, @intCast(geometry.width)) << 32) | @as(u64, @intCast(geometry.height));
}

/// Caller admits the sum of the per-image working sets before invoking this
/// route. Every image occupies its own attention batch lane; positions reset
/// per image. The ordinary projector remains the singleton fallback.
/// Batch independently admitted pages. Host admission must remain additive
/// for every image; a serial multi-image host estimate is insufficient.
pub fn encodeProjectedImagesResidentBatched(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: gpt_config.Config,
    images: []const []const u8,
    limits: Limits,
) ![]ProjectedImages {
    try limits.validate();
    if (images.len == 0 or images.len > maxResidentVisionBatchItems(cb.kind()) or images.len > limits.max_images) return error.ImageLimitExceeded;
    const cfg = try configFromResidentModel(config);
    if (!cfg.is_qwen35 or cfg.deepstack_len != 0 or (cb.kind() != .metal and cb.kind() != .cuda)) return error.UnsupportedBatchedVision;
    const key = try residentBatchGeometryKey(config, images[0], limits);
    for (images[1..]) |bytes| if (try residentBatchGeometryKey(config, bytes, limits) != key) return error.VisionBatchGeometryMismatch;
    const Check = struct {
        fn check(raw: ?*const anyopaque) !void {
            const backend: *const ComputeBackend = @ptrCast(@alignCast(raw.?));
            try backend.checkExecutionControl();
        }
    };
    var prepared: [resident_vision_batch_capacity]PreparedImage = undefined;
    var initialized: usize = 0;
    defer for (prepared[0..initialized]) |*value| value.deinit();
    var views: [resident_vision_batch_capacity]*const PreparedImage = undefined;
    for (images, 0..) |bytes, index| {
        prepared[index] = try prepareImageCpu(allocator, cfg, bytes, limits, .{ .context = cb, .check_fn = Check.check }, false);
        initialized += 1;
        views[index] = &prepared[index];
    }
    return encodePreparedProjectedImagesResidentBatched(cb, allocator, config, views[0..images.len], limits);
}

/// Encode borrowed patches from independently admitted pages under one GPU gate.
pub fn encodePreparedProjectedImagesResidentBatched(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: gpt_config.Config,
    prepared_images: []const *const PreparedImage,
    limits: Limits,
) ![]ProjectedImages {
    try limits.validate();
    if (prepared_images.len == 0 or prepared_images.len > maxResidentVisionBatchItems(cb.kind()) or prepared_images.len > limits.max_images) return error.ImageLimitExceeded;
    const cfg = try configFromResidentModel(config);
    if (!cfg.is_qwen35 or cfg.deepstack_len != 0 or (cb.kind() != .metal and cb.kind() != .cuda)) return error.UnsupportedBatchedVision;
    for (prepared_images) |prepared| {
        try prepared.validateFor(cfg, limits);
        if (!std.meta.eql(prepared.geometry, prepared_images[0].geometry)) return error.VisionBatchGeometryMismatch;
    }
    const output = try allocator.alloc(ProjectedImages, prepared_images.len);
    var completed: usize = 0;
    errdefer {
        for (output[0..completed]) |*item| item.deinit();
        allocator.free(output);
    }
    if (prepared_images.len == 1) {
        output[0] = try encodePreparedProjectedImageResident(cb, allocator, config, prepared_images[0], limits);
        return output;
    }
    var weights = WeightCache.initResident(allocator, cb, cfg);
    defer weights.deinit();
    var hidden: ?CT = null;
    defer if (hidden) |value| cb.free(value);
    var geometry: Geometry = undefined;
    for (prepared_images, 0..) |prepared, index| {
        try cb.checkExecutionControl();
        const current = prepared.geometry;
        if (index != 0 and (current.width != geometry.width or current.height != geometry.height)) return error.VisionBatchGeometryMismatch;
        if (index == 0) geometry = current;
        const embedded = try embedSpatialPatchRows(cb, allocator, &weights, cfg, prepared.patch_rows, current);
        defer cb.free(embedded);
        const positioned = try addInterpolatedPositionsTensor(cb, allocator, &weights, embedded, cfg, current);
        defer cb.free(positioned);
        const converted = try cb.ensureDeviceResident(positioned);
        defer if (converted) |value| cb.free(value);
        const resident = converted orelse positioned;
        if (hidden) |old| {
            const joined = try cb.concatRows2D(allocator, old, resident, index * current.patchCount(), current.patchCount(), cfg.vision_hidden);
            cb.free(old);
            hidden = joined;
        } else hidden = try cb.sliceRows2D(allocator, resident, 0, current.patchCount(), cfg.vision_hidden);
    }
    const per_image = geometry.patchCount();
    const rows = try admissionMul(per_image, prepared_images.len);
    const positions_one = try visionPositions(allocator, geometry, cfg.spatial_merge);
    defer allocator.free(positions_one);
    const positions = try allocator.alloc(u32, rows * 2);
    defer allocator.free(positions);
    for (0..2) |axis| for (0..prepared_images.len) |index| {
        @memcpy(positions[axis * rows + index * per_image ..][0..per_image], positions_one[axis * per_image ..][0..per_image]);
    };
    const stream_frames = limits.metal_frame_pipeline and cfg.is_qwen35 and cfg.require_metal_resident_vision and cfg.deepstack_len == 0 and !qwen3VlProfileEnabled() and platform.env.getenv("ANTFLY_QWEN3VL_QUALIFICATION_TRACE_LAYER") == null;
    var frame_group = VisionFrameGroup{ .cb = cb, .blocks = limits.metal_frame_blocks, .streamed = stream_frames };
    defer frame_group.deinit();
    for (0..cfg.block_count) |layer| {
        try cb.checkExecutionControl();
        try frame_group.begin(true);
        const previous = hidden.?;
        hidden = try encoderBlock(cb, allocator, &weights, cfg, previous, positions, rows, layer, prepared_images.len);
        try frame_group.finishBlock(previous, layer + 1 == cfg.block_count);
    }
    if ((limits.metal_frame_blocks > 1 or limits.metal_frame_pipeline) and platform.env.getenvBool("ANTFLY_BENCH_LIGHTONOCR3"))
        std.debug.print("LIGHTONOCR3 vision_frame_blocks={d} vision_frame_submits={d} vision_frame_images={d}\n", .{ limits.metal_frame_blocks, frame_group.submits, prepared_images.len });
    if (stream_frames and platform.env.getenvBool("ANTFLY_BENCH_LIGHTONOCR3"))
        std.debug.print("LIGHTONOCR3 vision_pipeline_enabled=1 vision_pipeline_submits={d} vision_pipeline_waits={d} vision_pipeline_overlaps={d}\n", .{ frame_group.submits, frame_group.waits, frame_group.overlaps });
    const normalized = try layerNormNamed(cb, allocator, &weights, hidden.?, "v.post_ln", cfg.vision_hidden, cfg.layer_norm_eps);
    defer cb.free(normalized);
    for (output, 0..) |*value, index| {
        const slice = try cb.sliceRows2D(allocator, normalized, index * per_image, per_image, cfg.vision_hidden);
        defer cb.free(slice);
        const projected = try mergeProjectedNamed(cb, allocator, &weights, cfg, slice, geometry, "mm.0", "mm.2");
        defer cb.free(projected);
        const embeddings = try cb.toFloat32(projected, allocator);
        errdefer allocator.free(embeddings);
        const counts = try allocator.dupe(usize, &.{geometry.tokenCount()});
        errdefer allocator.free(counts);
        const grids = try allocator.dupe(qwen3vl_plan.VisionGrid, &.{.{ .temporal = 1, .height = @intCast(geometry.grid_y), .width = @intCast(geometry.grid_x) }});
        errdefer allocator.free(grids);
        value.* = .{ .allocator = allocator, .embeddings = embeddings, .tokens_per_image = counts, .grids = grids, .deepstack_embeddings = &.{}, .preprocess_evidence = &.{}, .preprocess_spatial_patches = &.{}, .hidden_size = cfg.text_hidden, .deepstack_layer_count = 0 };
        completed += 1;
    }
    return output;
}

pub fn encodeProjectedImagesResident(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: gpt_config.Config,
    images: []const []const u8,
    limits: Limits,
) !ProjectedImages {
    return encodeProjectedImagesResidentQualified(cb, allocator, config, images, limits, false);
}

pub fn encodeProjectedImagesResidentForQualification(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: gpt_config.Config,
    images: []const []const u8,
    limits: Limits,
) !ProjectedImages {
    return encodeProjectedImagesResidentQualified(cb, allocator, config, images, limits, true);
}

fn encodeProjectedImagesResidentQualified(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: gpt_config.Config,
    images: []const []const u8,
    limits: Limits,
    collect_preprocess_evidence: bool,
) !ProjectedImages {
    const cfg = try configFromResidentModel(config);
    var weights = WeightCache.initResident(allocator, cb, cfg);
    defer weights.deinit();
    return encodeProjectedImagesFromWeightsQualified(cb, allocator, &weights, cfg, images, limits, collect_preprocess_evidence);
}

fn encodeProjectedImagesFromWeightsQualified(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    weights: *WeightCache,
    cfg: Config,
    images: []const []const u8,
    limits: Limits,
    collect_preprocess_evidence: bool,
) !ProjectedImages {
    try limits.validate();
    if (images.len == 0 or images.len > limits.max_images) return error.ImageLimitExceeded;
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
        try cb.checkExecutionControl();
        var encoded = try encodeSingleImage(cb, allocator, weights, cfg, bytes, limits, collect_preprocess_evidence);
        defer encoded.deinit(allocator);
        try cb.checkExecutionControl();
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
    if ((config.family != .qwen3_vl and config.family != .qwen3_5) or config.image_token_index < 0 or
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
    const hidden_size: usize = @intCast(config.hidden_size);
    if (projected.embeddings.len != visual_tokens * hidden_size) return error.ImageTokenLengthMismatch;
    const input_embeddings = if (cb.vtable.concatRows2D != null and cb.vtable.takeRows != null and cb.vtable.multiplyScalar != null)
        try assembleVisualPromptTensor(cb, allocator, base, projected.embeddings, plan.visual_token_mask, hidden_size, config.tokenEmbeddingScale())
    else blk: {
        const host = try cb.toFloat32(base, allocator);
        defer allocator.free(host);
        const scale = config.tokenEmbeddingScale();
        if (!std.math.approxEqAbs(f32, scale, 1.0, 1e-6)) {
            for (host) |*value| value.* *= scale;
        }
        if (projected.embeddings.len != visual_tokens * hidden_size) return error.ImageTokenLengthMismatch;
        var source_row: usize = 0;
        for (plan.visual_token_mask, 0..) |is_visual, row| {
            if (!is_visual) continue;
            @memcpy(host[row * hidden_size ..][0..hidden_size], projected.embeddings[source_row * hidden_size ..][0..hidden_size]);
            source_row += 1;
        }
        if (source_row != visual_tokens) return error.ImageTokenLengthMismatch;
        const shape = [_]i32{ @intCast(ids.items.len), @intCast(hidden_size) };
        break :blk try cb.fromFloat32Shape(host, &shape);
    };
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

    pub fn deinit(self: *EncodedImage, allocator: std.mem.Allocator) void {
        allocator.free(self.embeddings);
        allocator.free(self.deepstack);
        if (self.preprocess_spatial_patches) |patches| allocator.free(patches);
    }
};

pub const PreparedImage = struct {
    allocator: std.mem.Allocator,
    cfg: Config,
    geometry: Geometry,
    source_width: usize,
    source_height: usize,
    encoded_bytes: usize,
    patch_rows: []f32,
    decode_ns: u64,
    preprocess_ns: u64,
    patchify_ns: u64,

    pub fn deinit(self: *PreparedImage) void {
        self.allocator.free(self.patch_rows);
    }

    fn validateFor(self: *const PreparedImage, cfg: Config, limits: Limits) !void {
        try limits.validate();
        if (!std.meta.eql(self.cfg, cfg)) return error.InvalidPreparedImage;
        if (self.encoded_bytes == 0 or self.encoded_bytes > limits.max_encoded_image_bytes) return error.VisionAdmissionExceeded;
        const source_pixels = try admissionMul(self.source_width, self.source_height);
        if (source_pixels == 0 or source_pixels > limits.max_decoded_pixels) return error.VisionAdmissionExceeded;
        const expected = try targetGeometry(cfg, self.source_width, self.source_height, limits);
        const patch_dim = try admissionMul(try admissionMul(cfg.patch_size, cfg.patch_size), 3);
        if (!std.meta.eql(expected, self.geometry) or self.patch_rows.len != try admissionMul(expected.patchCount(), patch_dim)) return error.InvalidPreparedImage;
    }
};

/// Decode, resize and patchify without accessing a shared ComputeBackend.
/// The caller must acquire its page's full host admission before calling.
pub fn prepareProjectedImageResident(
    allocator: std.mem.Allocator,
    config: gpt_config.Config,
    bytes: []const u8,
    limits: Limits,
    control: image_work_control.Control,
) !PreparedImage {
    const cfg = try configFromResidentModel(config);
    return prepareImageCpu(allocator, cfg, bytes, limits, control, true);
}

fn prepareImageCpu(
    allocator: std.mem.Allocator,
    cfg: Config,
    bytes: []const u8,
    limits: Limits,
    control: image_work_control.Control,
    comptime install_scope: bool,
) !PreparedImage {
    try limits.validate();
    const scope: ?image_work_control.Scope = if (install_scope) image_work_control.Scope.enter(control) else null;
    defer if (scope) |active| active.deinit();
    try control.check();
    var zig017_return_error: ?anyerror = null;
    const profile = qwen3VlProfileEnabled();
    var stage_started_at = if (profile) monotonicNowNs() else 0;
    var failed_stage: []const u8 = "admission";
    const failed_layer: ?usize = null;
    errdefer if (zig017_return_error) |err| if (profile) {
        if (failed_layer) |layer|
            std.debug.print("qwen3vl-projector-error: stage={s} layer={d} error={s}\n", .{ failed_stage, layer, @errorName(err) })
        else
            std.debug.print("qwen3vl-projector-error: stage={s} error={s}\n", .{ failed_stage, @errorName(err) });
    };
    if (bytes.len == 0 or bytes.len > limits.max_encoded_image_bytes) return zig017_failure: {
        zig017_return_error = error.VisionAdmissionExceeded;
        break :zig017_failure error.VisionAdmissionExceeded;
    };
    failed_stage = "decode";
    const decoded = (image.decode(allocator, bytes) catch |zig017_err| {
        zig017_return_error = zig017_err;
        return zig017_err;
    });
    defer decoded.deinit(allocator);
    (control.check() catch |zig017_err| {
        zig017_return_error = zig017_err;
        return zig017_err;
    });
    const decode_ns = profileLap(profile, &stage_started_at);
    const decoded_pixels = std.math.mul(usize, decoded.width, decoded.height) catch return zig017_failure: {
        zig017_return_error = error.VisionAdmissionExceeded;
        break :zig017_failure error.VisionAdmissionExceeded;
    };
    if (decoded_pixels == 0 or decoded_pixels > limits.max_decoded_pixels) return zig017_failure: {
        zig017_return_error = error.VisionAdmissionExceeded;
        break :zig017_failure error.VisionAdmissionExceeded;
    };
    failed_stage = "preprocess";
    const geometry = (targetGeometry(cfg, decoded.width, decoded.height, limits) catch |zig017_err| {
        zig017_return_error = zig017_err;
        return zig017_err;
    });
    const pixels = (image.preprocessDecodedRectScaledWithResample(
        allocator,
        decoded,
        @intCast(geometry.width),
        @intCast(geometry.height),
        cfg.image_mean,
        cfg.image_std,
        1.0 / 255.0,
        .pillow_bicubic,
    ) catch |zig017_err| {
        zig017_return_error = zig017_err;
        return zig017_err;
    });
    defer allocator.free(pixels);
    const preprocess_ns = profileLap(profile, &stage_started_at);

    failed_stage = "patchify";
    const patch_rows = (extractPatchesMergeMajor(allocator, pixels, cfg, geometry) catch |zig017_err| {
        zig017_return_error = zig017_err;
        return zig017_err;
    });
    errdefer allocator.free(patch_rows);
    const patchify_ns = profileLap(profile, &stage_started_at);
    try control.check();
    return .{
        .allocator = allocator,
        .cfg = cfg,
        .geometry = geometry,
        .source_width = decoded.width,
        .source_height = decoded.height,
        .encoded_bytes = bytes.len,
        .patch_rows = patch_rows,
        .decode_ns = decode_ns,
        .preprocess_ns = preprocess_ns,
        .patchify_ns = patchify_ns,
    };
}

/// Encode borrowed host patches while the caller owns the shared GPU gate.
pub fn encodePreparedProjectedImageResident(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: gpt_config.Config,
    prepared: *const PreparedImage,
    limits: Limits,
) !ProjectedImages {
    const cfg = try configFromResidentModel(config);
    try prepared.validateFor(cfg, limits);
    try cb.checkExecutionControl();
    var weights = WeightCache.initResident(allocator, cb, cfg);
    defer weights.deinit();
    var encoded = try encodePreparedSingleImage(cb, allocator, &weights, cfg, prepared, limits, false);
    errdefer encoded.deinit(allocator);
    const counts = try allocator.alloc(usize, 1);
    errdefer allocator.free(counts);
    const grids = try allocator.alloc(qwen3vl_plan.VisionGrid, 1);
    counts[0] = encoded.tokens;
    grids[0] = encoded.grid;
    return .{
        .allocator = allocator,
        .embeddings = encoded.embeddings,
        .deepstack_embeddings = encoded.deepstack,
        .tokens_per_image = counts,
        .grids = grids,
        .preprocess_evidence = &.{},
        .preprocess_spatial_patches = &.{},
        .hidden_size = cfg.text_hidden,
        .deepstack_layer_count = cfg.deepstack_len,
    };
}

fn encodeSingleImage(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    weights: *WeightCache,
    cfg: Config,
    bytes: []const u8,
    limits: Limits,
    collect_preprocess_evidence: bool,
) !EncodedImage {
    const Check = struct {
        fn check(raw: ?*const anyopaque) !void {
            const backend: *const ComputeBackend = @ptrCast(@alignCast(raw.?));
            try backend.checkExecutionControl();
        }
    };
    var prepared = try prepareImageCpu(allocator, cfg, bytes, limits, .{ .context = cb, .check_fn = Check.check }, false);
    defer prepared.deinit();
    return encodePreparedSingleImage(cb, allocator, weights, cfg, &prepared, limits, collect_preprocess_evidence);
}

fn encodePreparedSingleImage(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    weights: *WeightCache,
    cfg: Config,
    prepared: *const PreparedImage,
    limits: Limits,
    collect_preprocess_evidence: bool,
) !EncodedImage {
    var zig017_return_error: ?anyerror = null;
    const profile = qwen3VlProfileEnabled();
    var stage_started_at = if (profile) monotonicNowNs() else 0;
    var failed_stage: []const u8 = "admission";
    var failed_layer: ?usize = null;
    errdefer if (zig017_return_error) |err| if (profile) {
        if (failed_layer) |layer|
            std.debug.print("qwen3vl-projector-error: stage={s} layer={d} error={s}\n", .{ failed_stage, layer, @errorName(err) })
        else
            std.debug.print("qwen3vl-projector-error: stage={s} error={s}\n", .{ failed_stage, @errorName(err) });
    };
    try prepared.validateFor(cfg, limits);
    const geometry = prepared.geometry;
    const patch_rows = prepared.patch_rows;
    const decode_ns = prepared.decode_ns;
    const preprocess_ns = prepared.preprocess_ns;
    const patchify_ns = prepared.patchify_ns;
    const preprocess_evidence: ?PreprocessEvidence = if (collect_preprocess_evidence) .{
        .source_width = prepared.source_width,
        .source_height = prepared.source_height,
        .resized_width = geometry.width,
        .resized_height = geometry.height,
        .grid = .{ .temporal = 1, .height = @intCast(geometry.grid_y), .width = @intCast(geometry.grid_x) },
        .patch_rows = geometry.patchCount(),
        .patch_columns = cfg.patch_size * cfg.patch_size * 3,
        .spatial_patch_f32le_sha256 = sha256F32LeHex(patch_rows),
        .positioned_embedding_f32le_sha256 = @as([64]u8, @splat(0)),
        .vision_trace_layer = null,
        .vision_trace_f32le_sha256 = null,
    } else null;
    const preprocess_spatial_patches = if (collect_preprocess_evidence)
        (allocator.dupe(f32, patch_rows) catch |zig017_err| {
            zig017_return_error = zig017_err;
            return zig017_err;
        })
    else
        null;
    errdefer if (preprocess_spatial_patches) |patches| allocator.free(patches);
    failed_stage = "patch_embedding";
    const embedded = embedSpatialPatchRows(cb, allocator, weights, cfg, patch_rows, geometry) catch |err| {
        zig017_return_error = err;
        return err;
    };
    defer cb.free(embedded);
    failed_stage = "position_embedding";
    var hidden = addInterpolatedPositionsTensor(cb, allocator, weights, embedded, cfg, geometry) catch |err| {
        zig017_return_error = err;
        return err;
    };
    defer cb.free(hidden);
    var completed_preprocess_evidence = preprocess_evidence;
    if (completed_preprocess_evidence) |*evidence| {
        const positioned_host = cb.toFloat32(hidden, allocator) catch |err| {
            zig017_return_error = err;
            return err;
        };
        defer allocator.free(positioned_host);
        evidence.positioned_embedding_f32le_sha256 = sha256F32LeHex(positioned_host);
    }
    // Keep exactly one owner for the current vision tensor. Every successful
    // stage replaces that owner only after freeing its predecessor, so all
    // later failures (including cancellation and backend allocation errors)
    // release the live tensor exactly once.
    if (cfg.is_qwen35 and cb.kind() == .metal) {
        if (try cb.ensureDeviceResident(hidden)) |resident| {
            cb.free(hidden);
            hidden = resident;
        }
    }
    const patch_embed_ns = profileLap(profile, &stage_started_at);

    failed_stage = "rope_positions";
    const positions = (visionPositions(allocator, geometry, cfg.spatial_merge) catch |zig017_err| {
        zig017_return_error = zig017_err;
        return zig017_err;
    });
    defer allocator.free(positions);
    const positions_ns = profileLap(profile, &stage_started_at);
    var deepstack = std.ArrayListUnmanaged(f32).empty;
    errdefer deepstack.deinit(allocator);
    const vision_trace_layer = if (collect_preprocess_evidence)
        platform.env.getenvUsize("ANTFLY_QWEN3VL_QUALIFICATION_TRACE_LAYER")
    else
        null;
    if (vision_trace_layer) |trace_layer| {
        if (trace_layer >= cfg.block_count) return zig017_failure: {
            zig017_return_error = error.InvalidQwen3VlQualificationTraceLayer;
            break :zig017_failure error.InvalidQwen3VlQualificationTraceLayer;
        };
    }
    var vision_trace_digest: ?[64]u8 = null;
    var next_tap: usize = 0;
    var blocks_ns: u64 = 0;
    var deepstack_ns: u64 = 0;
    const stream_frames = !collect_preprocess_evidence and limits.metal_frame_pipeline and cfg.is_qwen35 and cfg.require_metal_resident_vision and cfg.deepstack_len == 0 and !qwen3VlProfileEnabled() and platform.env.getenv("ANTFLY_QWEN3VL_QUALIFICATION_TRACE_LAYER") == null;
    var frame_group = VisionFrameGroup{ .cb = cb, .blocks = limits.metal_frame_blocks, .streamed = stream_frames };
    defer frame_group.deinit();
    for (0..cfg.block_count) |layer| {
        // GGUF-backed projector weights bypass ComputeBackend.getWeight, so
        // retain explicit bounded checkpoints even when those weights cache.
        (cb.checkExecutionControl() catch |zig017_err| {
            zig017_return_error = zig017_err;
            return zig017_err;
        });
        failed_stage = "vision_block";
        failed_layer = layer;
        // Bound retained blocks by the larger admitted frame envelope.
        try frame_group.begin(cfg.is_qwen35 and cb.kind() == .metal and
            !platform.env.getenvBool("TERMITE_METAL_DISABLE_QWEN35_VISION_FRAME"));
        const block_started_at = if (profile) monotonicNowNs() else 0;
        const next = (encoderBlock(cb, allocator, weights, cfg, hidden, positions, geometry.patchCount(), layer, 1) catch |zig017_err| {
            zig017_return_error = zig017_err;
            return zig017_err;
        });
        const previous = hidden;
        hidden = next;
        try frame_group.finishBlock(previous, layer + 1 == cfg.block_count);
        if (profile) blocks_ns +|= monotonicNowNs() -| block_started_at;
        if (vision_trace_layer != null and vision_trace_layer.? == layer) {
            const trace_host = (cb.toFloat32(hidden, allocator) catch |zig017_err| {
                zig017_return_error = zig017_err;
                return zig017_err;
            });
            defer allocator.free(trace_host);
            vision_trace_digest = sha256F32LeHex(trace_host);
        }
        if (next_tap < cfg.deepstack_len and cfg.deepstack_layers[next_tap] == layer) {
            const tap_started_at = if (profile) monotonicNowNs() else 0;
            const feature = (mergeProjected(cb, allocator, weights, cfg, hidden, geometry, layer) catch |zig017_err| {
                zig017_return_error = zig017_err;
                return zig017_err;
            });
            defer cb.free(feature);
            const host = (cb.toFloat32(feature, allocator) catch |zig017_err| {
                zig017_return_error = zig017_err;
                return zig017_err;
            });
            defer allocator.free(host);
            (deepstack.appendSlice(allocator, host) catch |zig017_err| {
                zig017_return_error = zig017_err;
                return zig017_err;
            });
            if (profile) deepstack_ns +|= monotonicNowNs() -| tap_started_at;
            next_tap += 1;
        }
    }
    stage_started_at = if (profile) monotonicNowNs() else 0;
    failed_layer = null;
    if (next_tap != cfg.deepstack_len) return zig017_failure: {
        zig017_return_error = error.InvalidGgufProjector;
        break :zig017_failure error.InvalidGgufProjector;
    };
    if (completed_preprocess_evidence) |*evidence| {
        evidence.vision_trace_layer = vision_trace_layer;
        evidence.vision_trace_f32le_sha256 = vision_trace_digest;
    }

    failed_stage = "post_norm";
    (cb.checkExecutionControl() catch |zig017_err| {
        zig017_return_error = zig017_err;
        return zig017_err;
    });
    if ((limits.metal_frame_blocks > 1 or limits.metal_frame_pipeline) and platform.env.getenvBool("ANTFLY_BENCH_LIGHTONOCR3"))
        std.debug.print("LIGHTONOCR3 vision_frame_blocks={d} vision_frame_submits={d} vision_frame_images=1\n", .{ limits.metal_frame_blocks, frame_group.submits });
    if (stream_frames and platform.env.getenvBool("ANTFLY_BENCH_LIGHTONOCR3"))
        std.debug.print("LIGHTONOCR3 vision_pipeline_enabled=1 vision_pipeline_submits={d} vision_pipeline_waits={d} vision_pipeline_overlaps={d}\n", .{ frame_group.submits, frame_group.waits, frame_group.overlaps });
    const post_norm = (layerNormNamed(cb, allocator, weights, hidden, "v.post_ln", cfg.vision_hidden, cfg.layer_norm_eps) catch |zig017_err| {
        zig017_return_error = zig017_err;
        return zig017_err;
    });
    cb.free(hidden);
    hidden = post_norm;
    failed_stage = "final_merge";
    const projected = (mergeProjectedNamed(cb, allocator, weights, cfg, post_norm, geometry, "mm.0", "mm.2") catch |zig017_err| {
        zig017_return_error = zig017_err;
        return zig017_err;
    });
    cb.free(hidden);
    hidden = projected;
    failed_stage = "final_download";
    const embeddings = (cb.toFloat32(projected, allocator) catch |zig017_err| {
        zig017_return_error = zig017_err;
        return zig017_err;
    });
    const final_merge_ns = profileLap(profile, &stage_started_at);
    errdefer allocator.free(embeddings);
    const owned_deepstack = (deepstack.toOwnedSlice(allocator) catch |zig017_err| {
        zig017_return_error = zig017_err;
        return zig017_err;
    });
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

const ResidentVisionScope = struct {
    cb: *const ComputeBackend,
    input: CT,
    owned: bool = false,
    enabled: bool = false,
    previous: bool = false,

    fn begin(cb: *const ComputeBackend, cfg: Config, input: CT) !ResidentVisionScope {
        var scope = ResidentVisionScope{ .cb = cb, .input = input };
        if (!cfg.require_metal_resident_vision) return scope;
        if (cb.kind() != .metal) return error.PreparedVisionUnavailable;
        if (comptime @import("build_options").enable_metal) {
            const M = metal_compute_mod.MetalCompute;
            if (!M.debugHasDeviceTensor(cb, input)) {
                scope.input = (try M.makeDeviceResident(cb, input)) orelse return error.PreparedVisionUnavailable;
                scope.owned = true;
            }
            scope.previous = M.requireQwenVisionResident(cb, true);
            scope.enabled = true;
            return scope;
        }
        return error.PreparedVisionUnavailable;
    }

    fn deinit(scope: *ResidentVisionScope) void {
        if (scope.enabled) _ = metal_compute_mod.MetalCompute.requireQwenVisionResident(scope.cb, scope.previous);
        if (scope.owned) scope.cb.free(scope.input);
    }
};

// Hugging Face uses config.hidden_act in vision blocks. Patch mergers
// deliberately retain their separate nn.GELU() (exact-erf) operation.
fn tryLightOnVisionMlp(
    cb: *const ComputeBackend,
    weights: *WeightCache,
    cfg: Config,
    input: CT,
    rows: usize,
    layer: usize,
) !?CT {
    if (comptime !@import("build_options").enable_cuda) return null;
    if (cb.kind() != .cuda or !cfg.is_qwen35 or !cfg.block_gelu_tanh or
        !platform.env.getenvBool("ANTFLY_CUDA_ENABLE_LIGHTON_FUSED_VISION_MLP")) return null;
    var name: [128]u8 = undefined;
    const norm_w = try weights.weight(try std.fmt.bufPrint(&name, "v.blk.{d}.ln2.weight", .{layer}));
    const norm_b = try weights.weight(try std.fmt.bufPrint(&name, "v.blk.{d}.ln2.bias", .{layer}));
    const up_w = try weights.linear(try std.fmt.bufPrint(&name, "v.blk.{d}.ffn_up.weight", .{layer}), cfg.vision_hidden, cfg.intermediate);
    const up_b = try weights.weight(try std.fmt.bufPrint(&name, "v.blk.{d}.ffn_up.bias", .{layer}));
    const down_w = try weights.linear(try std.fmt.bufPrint(&name, "v.blk.{d}.ffn_down.weight", .{layer}), cfg.intermediate, cfg.vision_hidden);
    const down_b = try weights.weight(try std.fmt.bufPrint(&name, "v.blk.{d}.ffn_down.bias", .{layer}));
    return @import("../ops/cuda/cuda_compute.zig").CudaCompute.lightOnVisionMlp(cb, input, .{ norm_w, norm_b, up_w, up_b, down_w, down_b }, rows, cfg.vision_hidden, cfg.intermediate, cfg.layer_norm_eps);
}

fn visionBlockActivation(cb: *const ComputeBackend, cfg: Config, input: CT) !CT {
    if (cfg.block_gelu_tanh) return cb.gelu(input);
    return (try cb.geluExact(input)) orelse try cb.gelu(input);
}

fn encoderBlock(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    weights: *WeightCache,
    cfg: Config,
    input_ct: CT,
    positions: []const u32,
    token_count: usize,
    layer: usize,
    image_batch: usize,
) !CT {
    var resident_scope = try ResidentVisionScope.begin(cb, cfg, input_ct);
    defer resident_scope.deinit();
    const input = resident_scope.input;
    const owns_encoder_scope = if (cfg.is_qwen35)
        try metal_compute_mod.MetalCompute.beginOcrEncoderScope(cb, .vision)
    else
        false;
    defer if (owns_encoder_scope) cb.decoderRuntimeEndPlannedComputeScope();
    var zig017_return_error: ?anyerror = null;
    const profile = qwen3VlProfileEnabled();
    var stage_started_at = if (profile) monotonicNowNs() else 0;
    var failed_stage: []const u8 = "ln1";
    errdefer if (zig017_return_error) |err| if (profile) std.debug.print(
        "qwen3vl-vision-block-error: layer={d} stage={s} error={s}\n",
        .{ layer, failed_stage, @errorName(err) },
    );
    var prefix_buf: [96]u8 = undefined;
    const ln1_prefix = (std.fmt.bufPrint(&prefix_buf, "v.blk.{d}.ln1", .{layer}) catch |zig017_err| {
        zig017_return_error = zig017_err;
        return zig017_err;
    });
    const normed1 = (layerNormNamed(cb, allocator, weights, input, ln1_prefix, cfg.vision_hidden, cfg.layer_norm_eps) catch |zig017_err| {
        zig017_return_error = zig017_err;
        return zig017_err;
    });
    defer cb.free(normed1);
    const ln1_ns = profileLap(profile, &stage_started_at);
    failed_stage = "attention";
    const attn = (visionAttention(cb, allocator, weights, cfg, normed1, positions, token_count, layer, image_batch) catch |zig017_err| {
        zig017_return_error = zig017_err;
        return zig017_err;
    });
    defer cb.free(attn);
    const attention_ns = profileLap(profile, &stage_started_at);
    failed_stage = "attention_residual";
    const residual1 = (cb.add(input, attn) catch |zig017_err| {
        zig017_return_error = zig017_err;
        return zig017_err;
    });
    errdefer cb.free(residual1);
    const attention_residual_ns = profileLap(profile, &stage_started_at);

    const ln2_prefix = (std.fmt.bufPrint(&prefix_buf, "v.blk.{d}.ln2", .{layer}) catch |zig017_err| {
        zig017_return_error = zig017_err;
        return zig017_err;
    });
    if (try tryLightOnVisionMlp(cb, weights, cfg, residual1, token_count, layer)) |fused| {
        cb.free(residual1);
        return fused;
    }
    failed_stage = "ln2";
    const normed2 = (layerNormNamed(cb, allocator, weights, residual1, ln2_prefix, cfg.vision_hidden, cfg.layer_norm_eps) catch |zig017_err| {
        zig017_return_error = zig017_err;
        return zig017_err;
    });
    defer cb.free(normed2);
    const ln2_ns = profileLap(profile, &stage_started_at);
    const fc1_prefix = (std.fmt.bufPrint(&prefix_buf, "v.blk.{d}.ffn_up", .{layer}) catch |zig017_err| {
        zig017_return_error = zig017_err;
        return zig017_err;
    });
    failed_stage = "fc1";
    const fc1 = (linearNamed(cb, allocator, weights, normed2, fc1_prefix, token_count, cfg.vision_hidden, cfg.intermediate) catch |zig017_err| {
        zig017_return_error = zig017_err;
        return zig017_err;
    });
    defer cb.free(fc1);
    const fc1_ns = profileLap(profile, &stage_started_at);
    failed_stage = "gelu";
    const activated = (visionBlockActivation(cb, cfg, fc1) catch |zig017_err| {
        zig017_return_error = zig017_err;
        return zig017_err;
    });
    defer cb.free(activated);
    const activation_ns = profileLap(profile, &stage_started_at);
    const fc2_prefix = (std.fmt.bufPrint(&prefix_buf, "v.blk.{d}.ffn_down", .{layer}) catch |zig017_err| {
        zig017_return_error = zig017_err;
        return zig017_err;
    });
    failed_stage = "fc2";
    const fc2 = (linearNamed(cb, allocator, weights, activated, fc2_prefix, token_count, cfg.intermediate, cfg.vision_hidden) catch |zig017_err| {
        zig017_return_error = zig017_err;
        return zig017_err;
    });
    defer cb.free(fc2);
    const fc2_ns = profileLap(profile, &stage_started_at);
    failed_stage = "ffn_residual";
    const output = (cb.add(residual1, fc2) catch |zig017_err| {
        zig017_return_error = zig017_err;
        return zig017_err;
    });
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
    image_batch: usize,
) !CT {
    var zig017_return_error: ?anyerror = null;
    const profile = qwen3VlProfileEnabled();
    var stage_started_at = if (profile) monotonicNowNs() else 0;
    var failed_stage: []const u8 = "qkv";
    errdefer if (zig017_return_error) |err| if (profile) std.debug.print(
        "qwen3vl-vision-attention-error: layer={d} stage={s} error={s}\n",
        .{ layer, failed_stage, @errorName(err) },
    );
    var buf: [96]u8 = undefined;
    const qkv_prefix = (std.fmt.bufPrint(&buf, "v.blk.{d}.attn_qkv", .{layer}) catch |zig017_err| {
        zig017_return_error = zig017_err;
        return zig017_err;
    });
    const qkv = (linearNamed(cb, allocator, weights, input, qkv_prefix, token_count, cfg.vision_hidden, cfg.vision_hidden * 3) catch |zig017_err| {
        zig017_return_error = zig017_err;
        return zig017_err;
    });
    defer cb.free(qkv);
    const qkv_ns = profileLap(profile, &stage_started_at);
    if (comptime build_options.enable_cuda) {
        if (try @import("../ops/cuda/cuda_compute.zig").CudaCompute.lightOnVisionAttention(cb, qkv, positions, image_batch, token_count, cfg.head_count, cfg.headDim(), cfg.rope_theta)) |attended| {
            defer cb.free(attended);
            const attention_ns = profileLap(profile, &stage_started_at);
            const out_prefix = try std.fmt.bufPrint(&buf, "v.blk.{d}.attn_out", .{layer});
            const output = try linearNamed(cb, allocator, weights, attended, out_prefix, token_count, cfg.vision_hidden, cfg.vision_hidden);
            if (profile) std.debug.print("qwen3vl-vision-fused-attention-profile: layer={d} qkv_ms={d:.3} attention_ms={d:.3} out_ms={d:.3}\n", .{ layer, @as(f64, @floatFromInt(qkv_ns)) / std.time.ns_per_ms, @as(f64, @floatFromInt(attention_ns)) / std.time.ns_per_ms, @as(f64, @floatFromInt(profileLap(profile, &stage_started_at))) / std.time.ns_per_ms });
            return output;
        }
    }

    failed_stage = "split_q";
    const q_unrotated = (cb.sliceLastDim(qkv, 0, cfg.vision_hidden) catch |zig017_err| {
        zig017_return_error = zig017_err;
        return zig017_err;
    });
    defer cb.free(q_unrotated);
    failed_stage = "split_k";
    const k_unrotated = (cb.sliceLastDim(qkv, cfg.vision_hidden, cfg.vision_hidden * 2) catch |zig017_err| {
        zig017_return_error = zig017_err;
        return zig017_err;
    });
    defer cb.free(k_unrotated);
    failed_stage = "split_v";
    const v = (cb.sliceLastDim(qkv, cfg.vision_hidden * 2, cfg.vision_hidden * 3) catch |zig017_err| {
        zig017_return_error = zig017_err;
        return zig017_err;
    });
    defer cb.free(v);
    failed_stage = "rope_q";
    const q = ((cb.visionRope(q_unrotated, token_count, cfg.headDim(), cfg.rope_theta, positions) catch |zig017_err| {
        zig017_return_error = zig017_err;
        return zig017_err;
    })) orelse
        return zig017_failure: {
            zig017_return_error = error.UnsupportedVisionRopeBackend;
            break :zig017_failure error.UnsupportedVisionRopeBackend;
        };
    defer cb.free(q);
    failed_stage = "rope_k";
    const k = ((cb.visionRope(k_unrotated, token_count, cfg.headDim(), cfg.rope_theta, positions) catch |zig017_err| {
        zig017_return_error = zig017_err;
        return zig017_err;
    })) orelse
        return zig017_failure: {
            zig017_return_error = error.UnsupportedVisionRopeBackend;
            break :zig017_failure error.UnsupportedVisionRopeBackend;
        };
    defer cb.free(k);
    const split_rope_ns = profileLap(profile, &stage_started_at);
    // Every patch is active in the single-image vision tower. An explicit
    // all-ones mask only adds a device upload and blocks specialized unmasked
    // SDPA routes without changing the mathematical result.
    failed_stage = "sdpa";
    const attended = (cb.scaledDotProductAttentionQwen3VlVision(q, k, v, image_batch, token_count / image_batch, cfg.head_count, cfg.headDim()) catch |zig017_err| {
        zig017_return_error = zig017_err;
        return zig017_err;
    });
    defer cb.free(attended);
    const sdpa_ns = profileLap(profile, &stage_started_at);
    const out_prefix = (std.fmt.bufPrint(&buf, "v.blk.{d}.attn_out", .{layer}) catch |zig017_err| {
        zig017_return_error = zig017_err;
        return zig017_err;
    });
    failed_stage = "output_projection";
    const output = (linearNamed(cb, allocator, weights, attended, out_prefix, token_count, cfg.vision_hidden, cfg.vision_hidden) catch |zig017_err| {
        zig017_return_error = zig017_err;
        return zig017_err;
    });
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
    hidden_ct: CT,
    geometry: Geometry,
    fc1_prefix: []const u8,
    fc2_prefix: []const u8,
) !CT {
    var resident_scope = try ResidentVisionScope.begin(cb, cfg, hidden_ct);
    defer resident_scope.deinit();
    const hidden = resident_scope.input;
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
    var zig017_return_error: ?anyerror = null;
    const profile = qwen3VlProfileEnabled();
    var failed_stage: []const u8 = "weight";
    errdefer if (zig017_return_error) |err| if (profile) std.debug.print(
        "qwen3vl-linear-error: prefix={s} stage={s} error={s}\n",
        .{ prefix, failed_stage, @errorName(err) },
    );
    const weight_name = (std.fmt.allocPrint(allocator, "{s}.weight", .{prefix}) catch |zig017_err| {
        zig017_return_error = zig017_err;
        return zig017_err;
    });
    defer allocator.free(weight_name);
    const bias_name = (std.fmt.allocPrint(allocator, "{s}.bias", .{prefix}) catch |zig017_err| {
        zig017_return_error = zig017_err;
        return zig017_err;
    });
    defer allocator.free(bias_name);
    const weight = (weights.linear(weight_name, in_dim, out_dim) catch |zig017_err| {
        zig017_return_error = zig017_err;
        return zig017_err;
    });
    failed_stage = "bias";
    const bias = (weights.weight(bias_name) catch |zig017_err| {
        zig017_return_error = zig017_err;
        return zig017_err;
    });
    failed_stage = "linear";
    return cb.linear(input, weight, bias, rows, in_dim, out_dim) catch |zig017_err| {
        zig017_return_error = zig017_err;
        return zig017_err;
    };
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

fn duplicateStillImageTemporalPatches(
    allocator: std.mem.Allocator,
    patches: []const f32,
    patch_count: usize,
    patch_size: usize,
) ![]f32 {
    const patch_area = try std.math.mul(usize, patch_size, patch_size);
    const spatial_dim = try std.math.mul(usize, 3, patch_area);
    if (patches.len != try std.math.mul(usize, patch_count, spatial_dim)) return error.InvalidPatchEmbeddingShape;
    const temporal_dim = try std.math.mul(usize, spatial_dim, 2);
    const out = try allocator.alloc(f32, try std.math.mul(usize, patch_count, temporal_dim));
    for (0..patch_count) |token| {
        const src_row = patches[token * spatial_dim ..][0..spatial_dim];
        const dst_row = out[token * temporal_dim ..][0..temporal_dim];
        for (0..3) |channel| {
            const source = src_row[channel * patch_area ..][0..patch_area];
            const channel_base = channel * 2 * patch_area;
            @memcpy(dst_row[channel_base..][0..patch_area], source);
            @memcpy(dst_row[channel_base + patch_area ..][0..patch_area], source);
        }
    }
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

fn assembleVisualPromptTensor(cb: *const ComputeBackend, allocator: std.mem.Allocator, base: CT, visual: []const f32, mask: []const bool, dim: usize, scale: f32) !CT {
    if (dim == 0 or visual.len % dim != 0) return error.InvalidTensorShape;
    const scaled = try cb.multiplyScalar(base, scale) orelse return error.UnsupportedResidentQwenVision;
    defer cb.free(scaled);
    const images = try cb.ensureDeviceResidentOwned(try cb.fromFloat32Shape(visual, &.{ @intCast(visual.len / dim), @intCast(dim) }));
    defer cb.free(images);
    const source = try cb.concatRows2D(allocator, scaled, images, mask.len, visual.len / dim, dim);
    defer cb.free(source);
    const ids = try allocator.alloc(u32, mask.len);
    defer allocator.free(ids);
    var visual_row: usize = 0;
    for (mask, 0..) |is_visual, row| {
        ids[row] = @intCast(if (is_visual) mask.len + visual_row else row);
        if (is_visual) visual_row += 1;
    }
    if (visual_row != visual.len / dim) return error.ImageTokenLengthMismatch;
    return try cb.takeRows(source, ids, mask.len, dim) orelse error.UnsupportedResidentQwenVision;
}

fn addInterpolatedPositionsTensor(cb: *const ComputeBackend, allocator: std.mem.Allocator, weights: *WeightCache, input: CT, cfg: Config, geometry: Geometry) !CT {
    if (comptime build_options.enable_cuda) {
        if (cb.kind() == .cuda and cfg.is_qwen35 and weights.source == .resident) {
            const table = try weights.weight("v.position_embd.weight");
            if (try @import("../ops/cuda/cuda_compute.zig").CudaCompute.lightOnVisionPositions(cb, input, table, geometry.grid_x, geometry.grid_y, cfg.spatial_merge, cfg.image_size / cfg.patch_size, cfg.vision_hidden)) |positioned| return positioned;
        }
    }
    const positions = try allocator.alloc(f32, geometry.patchCount() * cfg.vision_hidden);
    defer allocator.free(positions);
    @memset(positions, 0);
    try addInterpolatedPositions(weights, positions, cfg, geometry);
    const offsets = try cb.ensureDeviceResidentOwned(try cb.fromFloat32Shape(positions, &.{ @intCast(geometry.patchCount()), @intCast(cfg.vision_hidden) }));
    defer cb.free(offsets);
    return cb.add(input, offsets);
}

fn addInterpolatedPositions(
    weights: *WeightCache,
    hidden: []f32,
    cfg: Config,
    geometry: Geometry,
) !void {
    const table = try weights.positionValues(cfg);
    const position_count = table.len / cfg.vision_hidden;
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
                        const p00 = table[(y0 * side + x0) * cfg.vision_hidden + h];
                        const p01 = table[(y0 * side + x1) * cfg.vision_hidden + h];
                        const p10 = table[(y1 * side + x0) * cfg.vision_hidden + h];
                        const p11 = table[(y1 * side + x1) * cfg.vision_hidden + h];
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

fn configFromResidentModel(config: gpt_config.Config) !Config {
    if ((!config.hasQwen3VlArchitectureContract() and !config.hasQwen35VisionArchitectureContract()) or
        config.vision_temporal_patch_size != 2 or config.vision_num_position_embeddings == 0)
    {
        return error.InvalidMultimodalConfig;
    }
    const position_count: usize = @intCast(config.vision_num_position_embeddings);
    const position_side = exactSquareRoot(position_count) orelse
        return error.InvalidPositionEmbeddingShape;
    const patch_size: usize = @intCast(config.vision_patch_size);
    var cfg = Config{
        .is_qwen35 = config.family == .qwen3_5,
        .block_gelu_tanh = config.vision_use_gelu_tanh,
        .require_metal_resident_vision = config.qwen35_metal_resident_vision,
        .text_hidden = @intCast(config.hidden_size),
        .vision_hidden = @intCast(config.vision_hidden_size),
        .intermediate = @intCast(config.vision_intermediate_size),
        .block_count = @intCast(config.vision_num_hidden_layers),
        .head_count = @intCast(config.vision_num_attention_heads),
        .image_size = try std.math.mul(usize, position_side, patch_size),
        .patch_size = patch_size,
        .spatial_merge = @intCast(config.vision_spatial_merge_size),
        .layer_norm_eps = 1e-6,
        // Qwen3-VL's vision tower retains the Qwen2-VL 10K rotary base; the
        // multi-million text RoPE base belongs only to the language model.
        .rope_theta = 10_000.0,
        .image_mean = .{ 0.5, 0.5, 0.5 },
        .image_std = .{ 0.5, 0.5, 0.5 },
    };
    const deepstack = config.visionDeepstackVisualIndexes();
    if (deepstack.len > cfg.deepstack_layers.len) return error.InvalidMultimodalConfig;
    for (deepstack, 0..) |layer, index| {
        const layer_index: usize = @intCast(layer);
        if (layer_index >= cfg.block_count) return error.InvalidMultimodalConfig;
        cfg.deepstack_layers[index] = layer_index;
    }
    cfg.deepstack_len = deepstack.len;
    if (cfg.text_hidden == 0 or cfg.vision_hidden == 0 or cfg.intermediate == 0 or
        cfg.block_count == 0 or cfg.head_count == 0 or cfg.vision_hidden % cfg.head_count != 0 or
        cfg.patch_size == 0 or cfg.spatial_merge == 0)
    {
        return error.InvalidMultimodalConfig;
    }
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

test "Qwen3-VL resident projector maps integrated safetensors weights" {
    var cfg = Config{
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
        .deepstack_len = 3,
    };
    cfg.deepstack_layers[0] = 5;
    cfg.deepstack_layers[1] = 11;
    cfg.deepstack_layers[2] = 17;

    const cases = [_]struct { logical: []const u8, resident: []const u8 }{
        .{ .logical = "v.patch_embd.bias", .resident = "model.visual.patch_embed.proj.bias" },
        .{ .logical = "v.position_embd.weight", .resident = "model.visual.pos_embed.weight" },
        .{ .logical = "v.blk.7.attn_qkv.weight", .resident = "model.visual.blocks.7.attn.qkv.weight" },
        .{ .logical = "v.blk.23.ffn_down.bias", .resident = "model.visual.blocks.23.mlp.linear_fc2.bias" },
        .{ .logical = "v.post_ln.weight", .resident = "model.visual.merger.norm.weight" },
        .{ .logical = "mm.0.bias", .resident = "model.visual.merger.linear_fc1.bias" },
        .{ .logical = "v.deepstack.11.fc2.weight", .resident = "model.visual.deepstack_merger_list.1.linear_fc2.weight" },
    };
    for (cases) |case| {
        const actual = try residentWeightNameAlloc(std.testing.allocator, cfg, case.logical);
        defer std.testing.allocator.free(actual);
        try std.testing.expectEqualStrings(case.resident, actual);
    }
    try std.testing.expectError(
        error.WeightNotFound,
        residentWeightNameAlloc(std.testing.allocator, cfg, "v.deepstack.6.fc1.weight"),
    );
}

test "Qwen3-VL resident projector derives the official BF16 vision contract" {
    var model = gpt_config.Config{
        .family = .qwen3_vl,
        .hidden_size = 2048,
        .num_attention_heads = 16,
        .image_token_index = 151655,
        .video_token_index = 151656,
        .boi_token_index = 151652,
        .eoi_token_index = 151653,
        .vision_hidden_size = 1024,
        .vision_num_hidden_layers = 24,
        .vision_num_attention_heads = 16,
        .vision_intermediate_size = 4096,
        .vision_num_position_embeddings = 2304,
        .vision_out_hidden_size = 2048,
        .vision_patch_size = 16,
        .vision_spatial_merge_size = 2,
        .vision_temporal_patch_size = 2,
        .vision_deepstack_visual_indexes_len = 3,
        .mrope_interleaved = true,
        .mrope_section = .{ 24, 20, 20 },
    };
    model.vision_deepstack_visual_indexes[0] = 5;
    model.vision_deepstack_visual_indexes[1] = 11;
    model.vision_deepstack_visual_indexes[2] = 17;

    const cfg = try configFromResidentModel(model);
    try std.testing.expectEqual(@as(usize, 768), cfg.image_size);
    try std.testing.expectEqual(@as(usize, 24), cfg.block_count);
    try std.testing.expectEqual(@as(usize, 3), cfg.deepstack_len);
    try std.testing.expectEqualSlices(usize, &.{ 5, 11, 17 }, cfg.deepstack_layers[0..cfg.deepstack_len]);
    try std.testing.expectEqual(@as(f32, 10_000.0), cfg.rope_theta);
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

test "LightOnOCR3 admission charges packed vision storage beyond FFN and score peaks" {
    for ([_]usize{ 768, 1024 }) |vision_hidden| {
        const text_hidden: usize = if (vision_hidden == 768) 1024 else 2560;
        const config = gpt_config.Config{
            .family = .qwen3_5,
            .hidden_size = @intCast(text_hidden),
            .vision_hidden_size = @intCast(vision_hidden),
            .vision_intermediate_size = @intCast(vision_hidden * 4),
            .vision_num_attention_heads = @intCast(vision_hidden / 64),
            .vision_patch_size = 16,
            .vision_spatial_merge_size = 2,
        };
        const limits = Limits{ .max_merged_tokens = 4096, .hard_max_merged_tokens = 4096 };
        for ([_]image.EncodedImageInfo{ .{ .width = 1000, .height = 768 }, .{ .width = 2048, .height = 2048 } }) |image_dimensions| {
            const geometry = try targetGeometryForLayout(16, 2, image_dimensions.width, image_dimensions.height, limits);
            const patches = geometry.patchCount();
            const estimate = try estimateAdmissionForImageDimensions(&.{image_dimensions}, config, limits);
            const hidden = patches * vision_hidden * @sizeOf(f32);
            const scores = patches * patches * (vision_hidden / 64) * @sizeOf(f32);
            const ffn = patches * (vision_hidden * 4) * @sizeOf(f32) * 2;
            const merge = geometry.tokenCount() * (vision_hidden * 4 * 3 + text_hidden) * @sizeOf(f32);
            const packed_pair = std.mem.alignForward(usize, @min(patches, 4096), 128) * vision_hidden * 3 * 2 * @sizeOf(u16);
            try std.testing.expectEqual(7 * hidden + @max(scores, ffn) + merge + packed_pair, estimate.backend_scratch_bytes);
            var grouped_limits = limits;
            grouped_limits.metal_frame_blocks = 4;
            const grouped = try estimateAdmissionForImageDimensions(&.{image_dimensions}, config, grouped_limits);
            try std.testing.expectEqual(estimate.visual_tokens, grouped.visual_tokens);
            try std.testing.expectEqual(estimate.host_scratch_bytes * 4, grouped.host_scratch_bytes);
            try std.testing.expectEqual(estimate.backend_scratch_bytes * 4, grouped.backend_scratch_bytes);
            grouped_limits.metal_frame_blocks = 1;
            grouped_limits.metal_frame_pipeline = true;
            const streamed_one = try estimateAdmissionForImageDimensions(&.{image_dimensions}, config, grouped_limits);
            try std.testing.expectEqual(estimate.host_scratch_bytes * 2, streamed_one.host_scratch_bytes);
            try std.testing.expectEqual(estimate.backend_scratch_bytes * 2, streamed_one.backend_scratch_bytes);
            grouped_limits.metal_frame_blocks = 2;
            const streamed_two = try estimateAdmissionForImageDimensions(&.{image_dimensions}, config, grouped_limits);
            try std.testing.expectEqual(grouped.host_scratch_bytes, streamed_two.host_scratch_bytes);
            try std.testing.expectEqual(grouped.backend_scratch_bytes, streamed_two.backend_scratch_bytes);
            grouped_limits.metal_frame_blocks = 3;
            try std.testing.expectError(error.InvalidVisionLimits, grouped_limits.validate());
            grouped_limits.metal_frame_pipeline = false;
            grouped_limits.metal_frame_blocks = 0;
            try std.testing.expectError(error.InvalidVisionLimits, grouped_limits.validate());
            grouped_limits.metal_frame_blocks = 5;
            try std.testing.expectError(error.InvalidVisionLimits, grouped_limits.validate());
        }
    }
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

test "Qwen3-VL still-image temporal patch expansion matches Conv3D CTHW order" {
    const spatial = [_]f32{ 1, 2, 3, 4, 5, 6 };
    const actual = try duplicateStillImageTemporalPatches(std.testing.allocator, &spatial, 2, 1);
    defer std.testing.allocator.free(actual);
    try std.testing.expectEqualSlices(f32, &.{ 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6 }, actual);
}

test "Qwen3-VL projector cancellation unwinds live vision intermediates with cached weights" {
    const native_compute = @import("../ops/native_compute.zig");
    const allocator = std.testing.allocator;
    var store = native_compute.WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    defer native_compute.deinitPrefetchQueue(&store);
    var compute = native_compute.NativeCompute.init(allocator, &store, null);
    defer compute.deinit();
    var cb = ComputeBackend{ .ptr = &compute, .vtable = &native_compute.vtable_impl };
    // The first layer norm really executes. Cancellation at its following
    // projection must release that intermediate even when weights are cached.
    var weights = WeightCache.initGguf(allocator, &cb, undefined);
    defer weights.deinit();
    _ = try weights.insert("weight:v.blk.0.ln1.weight", try cb.fromFloat32(&.{ 1, 1, 1, 1 }));
    _ = try weights.insert("weight:v.blk.0.ln1.bias", try cb.fromFloat32(&.{ 0, 0, 0, 0 }));
    const input = try cb.fromFloat32Shape(&.{ 1, 2, 3, 4 }, &.{ 1, 4 });
    defer cb.free(input);
    const Probe = struct {
        checks: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.checks += 1;
            if (self.checks >= 3) return error.Cancelled;
        }
    };
    var probe = Probe{};
    cb.execution_control = .{ .ptr = &probe, .check_fn = Probe.check };
    try std.testing.expectError(error.Cancelled, encoderBlock(&cb, allocator, &weights, .{
        .text_hidden = 4,
        .vision_hidden = 4,
        .intermediate = 8,
        .block_count = 1,
        .head_count = 1,
        .image_size = 2,
        .patch_size = 1,
        .spatial_merge = 2,
        .layer_norm_eps = 1e-5,
        .image_mean = .{ 0, 0, 0 },
        .image_std = .{ 1, 1, 1 },
    }, input, &.{ 0, 0 }, 1, 0, 1));
    try std.testing.expectEqual(@as(usize, 3), probe.checks);
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
    defer compute.deinit();
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

test "LightOnOCR3 resident vision uses learned positions without DeepStack" {
    const model = try gpt_config.parseConfig(std.testing.allocator, @embedFile("../models/testdata/lightonocr3/config.json"));
    const cfg = try configFromResidentModel(model);
    try std.testing.expectEqual(@as(usize, 768), cfg.vision_hidden);
    try std.testing.expectEqual(@as(usize, 1024), cfg.text_hidden);
    try std.testing.expectEqual(@as(usize, 12), cfg.block_count);
    try std.testing.expectEqual(@as(usize, 0), cfg.deepstack_len);
    try std.testing.expectEqual(@as(usize, 768), cfg.image_size);
    const name = try residentWeightNameAlloc(std.testing.allocator, cfg, "v.position_embd.weight");
    defer std.testing.allocator.free(name);
    try std.testing.expectEqualStrings("model.visual.pos_embed.weight", name);
}

fn checkQwen3Residency(cb: ComputeBackend, reference: ComputeBackend) !void {
    const allocator = std.testing.allocator;
    const fixture = @import("../ops/media_residency_test.zig");
    const cfg = Config{ .text_hidden = 4, .vision_hidden = 4, .intermediate = 8, .block_count = 1, .head_count = 1, .image_size = 4, .patch_size = 1, .spatial_merge = 2, .layer_norm_eps = 1e-5, .image_mean = .{ 0, 0, 0 }, .image_std = .{ 1, 1, 1 } };
    const geometry = Geometry{ .width = 6, .height = 4, .grid_x = 6, .grid_y = 4, .merged_x = 3, .merged_y = 2 };
    var weights = WeightCache.initGguf(allocator, &cb, undefined);
    defer weights.deinit();
    weights.position_f32 = try allocator.alloc(f32, 16 * 4);
    for (weights.position_f32.?, 0..) |*v, i| v.* = @as(f32, @floatFromInt(i)) * 0.01;
    var data: [24 * 4]f32 = undefined;
    for (&data, 0..) |*v, i| v.* = @as(f32, @floatFromInt(i % 23)) * 0.03;
    const input = try cb.ensureDeviceResidentOwned(try cb.fromFloat32Shape(&data, &.{ 24, 4 }));
    defer cb.free(input);
    var expected = data;
    try addInterpolatedPositions(&weights, &expected, cfg, geometry);
    const before = fixture.Transfers.snapshot(cb);
    const positioned = try addInterpolatedPositionsTensor(&cb, allocator, &weights, input, cfg, geometry);
    defer cb.free(positioned);
    try before.expectUnchanged(cb);
    const actual = try cb.toFloat32(positioned, allocator);
    defer allocator.free(actual);
    for (expected, actual) |e, a| try std.testing.expectApproxEqAbs(e, a, 1e-6);
    const base_data = [_]f32{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 };
    const base = try cb.ensureDeviceResidentOwned(try cb.fromFloat32Shape(&base_data, &.{ 4, 4 }));
    defer cb.free(base);
    const visual = [_]f32{ 100, 101, 102, 103, 200, 201, 202, 203 };
    const before_prompt = fixture.Transfers.snapshot(cb);
    const prompt = try assembleVisualPromptTensor(&cb, allocator, base, &visual, &.{ false, true, true, false }, 4, 2);
    defer cb.free(prompt);
    try before_prompt.expectUnchanged(cb);
    const prompt_data = try cb.toFloat32(prompt, allocator);
    defer allocator.free(prompt_data);
    _ = reference;
    try std.testing.expectEqualSlices(f32, &.{ 2, 4, 6, 8, 100, 101, 102, 103, 200, 201, 202, 203, 26, 28, 30, 32 }, prompt_data);
}

test "media residency Qwen3 VL positions and prompt native parity" {
    const fixture = @import("../ops/media_residency_test.zig");
    try fixture.onNative(fixture.noWeights, checkQwen3Residency);
}

test "media residency Qwen3 VL positions and prompt Metal parity" {
    const fixture = @import("../ops/media_residency_test.zig");
    try fixture.onMetal(fixture.noWeights, checkQwen3Residency);
}

test "media residency Qwen3 VL positions and prompt CUDA parity" {
    const fixture = @import("../ops/media_residency_test.zig");
    try fixture.onCuda(fixture.noWeights, checkQwen3Residency);
}

test "LightOnOCR3 CPU image preparation owns patches and binds model geometry" {
    const a = std.testing.allocator;
    const model = try gpt_config.parseConfig(a, @embedFile("../models/testdata/lightonocr3/config.json"));
    const bytes = @embedFile("../models/testdata/lightonocr3/invoice.png");
    var prepared = try prepareProjectedImageResident(a, model, bytes, .{}, .{});
    defer prepared.deinit();
    try prepared.validateFor(try configFromResidentModel(model), .{});
    try std.testing.expect(prepared.patch_rows.len > 0 and prepared.geometry.tokenCount() > 0);
    var other = prepared.cfg;
    other.text_hidden += 1;
    try std.testing.expectError(error.InvalidPreparedImage, prepared.validateFor(other, .{}));
    try std.testing.expectError(error.VisionAdmissionExceeded, prepared.validateFor(prepared.cfg, .{ .max_decoded_pixels = 1 }));
    try std.testing.expectError(error.VisionAdmissionExceeded, prepared.validateFor(prepared.cfg, .{ .max_encoded_image_bytes = 1 }));
    const full = prepared.patch_rows;
    prepared.patch_rows = full[0 .. full.len - 1];
    defer prepared.patch_rows = full;
    try std.testing.expectError(error.InvalidPreparedImage, prepared.validateFor(prepared.cfg, .{}));
}

test "LightOnOCR3 CPU image preparation cancels in kernels and restores its caller scope" {
    const Counter = struct {
        calls: usize = 0,
        cancel_at: usize,
        fn check(raw: ?*const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw.?)));
            self.calls += 1;
            if (self.calls >= self.cancel_at) return error.Cancelled;
        }
    };
    var outer = Counter{ .cancel_at = std.math.maxInt(usize) };
    const scope = image_work_control.Scope.enter(.{ .context = &outer, .check_fn = Counter.check });
    defer scope.deinit();
    var cancelled = Counter{ .cancel_at = 4 };
    const a = std.testing.allocator;
    const model = try gpt_config.parseConfig(a, @embedFile("../models/testdata/lightonocr3/config.json"));
    try std.testing.expectError(error.Cancelled, prepareProjectedImageResident(a, model, @embedFile("../models/testdata/lightonocr3/invoice.png"), .{}, .{ .context = &cancelled, .check_fn = Counter.check }));
    try std.testing.expect(cancelled.calls >= 4);
    try image_work_control.check();
    try std.testing.expectEqual(@as(usize, 1), outer.calls);
}

test "LightOnOCR3 resident vision budget preserves host banks and complete grouped working bounds" {
    for ([_]usize{ 768, 1024 }) |h| {
        var cfg = gpt_config.Config{ .family = .qwen3_5, .hidden_size = if (h == 768) 1024 else 2560, .vision_hidden_size = @intCast(h), .vision_intermediate_size = @intCast(h * 4), .vision_num_attention_heads = @intCast(h / 64), .vision_patch_size = 16, .vision_spatial_merge_size = 2 };
        const dims = image.EncodedImageInfo{ .width = 2048, .height = 2048 };
        var limits = Limits{ .max_merged_tokens = 4096, .hard_max_merged_tokens = 4096 };
        const old = try estimateAdmissionForImageDimensions(&.{dims}, cfg, limits);
        cfg.qwen35_metal_resident_vision = true;
        const current = try estimateAdmissionForImageDimensions(&.{dims}, cfg, limits);
        const geometry = try targetGeometryForLayout(16, 2, dims.width, dims.height, limits);
        const hidden = geometry.patchCount() * h * @sizeOf(f32);
        const merge = geometry.tokenCount() * (12 * h + cfg.hidden_size) * @sizeOf(f32);
        const packed_pair = std.mem.alignForward(usize, @min(geometry.patchCount(), 4096), 128) * h * 3 * @sizeOf(f32);
        try std.testing.expectEqual(27 * hidden + merge + packed_pair, current.backend_scratch_bytes);
        try std.testing.expect(current.backend_scratch_bytes >= 30 * hidden + packed_pair);
        try std.testing.expect(current.backend_scratch_bytes < old.backend_scratch_bytes);
        try std.testing.expectEqual(old.host_scratch_bytes, current.host_scratch_bytes);
        try std.testing.expectEqual(old.visual_tokens, current.visual_tokens);
        limits.metal_frame_blocks = 4;
        const grouped = try estimateAdmissionForImageDimensions(&.{dims}, cfg, limits);
        try std.testing.expectEqual(current.backend_scratch_bytes * 4, grouped.backend_scratch_bytes);
        try std.testing.expectEqual(current.host_scratch_bytes * 4, grouped.host_scratch_bytes);
        cfg.family = .qwen3_vl;
        try std.testing.expectError(error.UnsupportedResidentVisionBudget, estimateAdmissionForImageDimensions(&.{dims}, cfg, limits));
    }
}

test "Qwen3-VL vision pipeline drains both tensor banks on completion cancellation and failures" {
    const native_compute = @import("../ops/native_compute.zig");
    const Fake = struct {
        mode: usize,
        active: bool = false,
        submitted: bool = false,
        active_mask: u16 = 0,
        submitted_mask: u16 = 0,
        freed_mask: u16 = 0,
        premature_frees: u16 = 0,
        duplicate_frees: usize = 0,
        invalid_frames: usize = 0,
        submit_calls: usize = 0,
        wait_calls: usize = 0,
        failed_wait: bool = false,
        fn get(raw: *anyopaque) *@This() {
            return @ptrCast(@alignCast(raw));
        }
        fn begin(raw: *anyopaque) !bool {
            const f = get(raw);
            if (f.mode == 4) return false;
            if (f.active) f.invalid_frames += 1;
            f.active = true;
            return true;
        }
        fn hasActive(raw: *anyopaque) bool {
            return get(raw).active;
        }
        fn hasSubmitted(raw: *anyopaque) bool {
            return get(raw).submitted;
        }
        fn submit(raw: *anyopaque) !void {
            const f = get(raw);
            f.submit_calls += 1;
            if (f.mode == 2 and f.submit_calls == 2) return error.InjectedSubmitFailure;
            if (!f.active or f.submitted) f.invalid_frames += 1;
            f.submitted = true;
            f.active = false;
            f.submitted_mask = f.active_mask;
            f.active_mask = 0;
        }
        fn wait(raw: *anyopaque) !void {
            const f = get(raw);
            if (!f.submitted) return;
            f.wait_calls += 1;
            if (f.mode == 3 and !f.failed_wait) {
                f.failed_wait = true;
                return error.InjectedWaitFailure;
            }
            f.submitted = false;
            f.submitted_mask = 0;
        }
        fn submitWait(raw: *anyopaque) !void {
            try submit(raw);
            try wait(raw);
        }
        fn cancel(raw: *anyopaque) !void {
            const f = get(raw);
            f.active = false;
            f.active_mask = 0;
        }
        fn free(raw: *anyopaque, tensor: CT) void {
            const f = get(raw);
            const bit = @as(u16, 1) << @as(u4, @intCast(@intFromPtr(tensor)));
            // The previous group's output is also the active group's input.
            // Cancel alone cannot make that tensor safe to free.
            if ((f.active_mask | f.submitted_mask) & bit != 0) f.premature_frees |= bit;
            if (f.freed_mask & bit != 0) f.duplicate_frees += 1;
            f.freed_mask |= bit;
        }
    };
    for (0..6) |mode| {
        var fake = Fake{ .mode = mode };
        var table = native_compute.vtable_impl;
        table.freeTensor = Fake.free;
        table.decoderRuntimeBeginFrame = Fake.begin;
        table.decoderRuntimeHasActiveFrame = Fake.hasActive;
        table.decoderRuntimeHasSubmittedFrame = Fake.hasSubmitted;
        table.decoderRuntimeSubmitFrame = Fake.submit;
        table.decoderRuntimeWaitSubmittedFrame = Fake.wait;
        table.decoderRuntimeSubmitAndWaitFrame = Fake.submitWait;
        table.decoderRuntimeCancelFrame = Fake.cancel;
        const cb = ComputeBackend{ .ptr = &fake, .vtable = &table };
        var group = VisionFrameGroup{ .cb = &cb, .blocks = 2, .streamed = mode != 5 };
        var encoded: usize = 0;
        var failure: ?anyerror = null;
        for (0..6) |index| {
            group.begin(true) catch |err| {
                failure = err;
                break;
            };
            // Track the producer output as well as the input; the next group
            // borrows that output while the first producer is still in flight.
            fake.active_mask |= (@as(u16, 1) << @as(u4, @intCast(index + 1))) |
                (@as(u16, 1) << @as(u4, @intCast(index + 2)));
            encoded += 1;
            group.finishBlock(@ptrFromInt(index + 1), index == 5) catch |err| {
                failure = err;
                break;
            };
            if (mode == 1 and encoded == 3) break;
        }
        group.deinit();
        try std.testing.expect(!fake.active and !fake.submitted);
        try std.testing.expectEqual(@as(u16, 0), fake.active_mask | fake.submitted_mask);
        const expected = (@as(u16, 1) << @as(u4, @intCast(encoded + 1))) - 2;
        try std.testing.expectEqual(expected, fake.freed_mask);
        // The final/current output remains the caller's property.
        Fake.free(&fake, @ptrFromInt(encoded + 1));
        try std.testing.expectEqual(@as(u16, 0), fake.premature_frees);
        try std.testing.expectEqual(@as(usize, 0), fake.duplicate_frees);
        try std.testing.expectEqual(@as(usize, 0), fake.invalid_frames);
        switch (mode) {
            0 => {
                try std.testing.expect(failure == null);
                try std.testing.expectEqual(@as(usize, 3), group.submits);
                try std.testing.expectEqual(group.submits, group.waits);
                try std.testing.expectEqual(@as(usize, 2), group.overlaps);
            },
            1 => try std.testing.expect(failure == null),
            2 => try std.testing.expectEqual(error.InjectedSubmitFailure, failure.?),
            3 => try std.testing.expectEqual(error.InjectedWaitFailure, failure.?),
            4 => try std.testing.expectEqual(error.PreparedVisionUnavailable, failure.?),
            5 => {
                try std.testing.expect(failure == null);
                try std.testing.expectEqual(@as(usize, 3), group.submits);
                try std.testing.expectEqual(@as(usize, 0), group.overlaps);
            },
            else => unreachable,
        }
    }
}

test "LightOnOCR3 CUDA resident vision batch width preserves other backend ceilings" {
    try std.testing.expectEqual(@as(usize, if (platform.env.getenvBool("ANTFLY_LIGHTONOCR3_CUDA_WIDE_MICROBATCHES")) 16 else 8), maxResidentVisionBatchItems(.cuda));
    try std.testing.expectEqual(@as(usize, 4), maxResidentVisionBatchItems(.metal));
    try std.testing.expectEqual(@as(usize, 4), maxResidentVisionBatchItems(.native));
}

test "LightOnOCR3 CUDA vision block activation follows checkpoint tanh GELU independently of exact merger GELU" {
    const a = std.testing.allocator;
    const model_config = try gpt_config.parseConfig(a, @embedFile("../models/testdata/lightonocr3/config.json"));
    var cfg = try configFromResidentModel(model_config);
    try std.testing.expect(cfg.block_gelu_tanh);
    const Oracle = struct {
        fn check(cb: *const ComputeBackend, config: Config) !void {
            const values = [_]f32{ -4, -2.5, -1, -0.001, 0, 0.001, 1, 2.5, 4 };
            const input = try cb.fromFloat32(&values);
            defer cb.free(input);
            const output = try visionBlockActivation(cb, config, input);
            defer cb.free(output);
            const result = try cb.toFloat32(output, std.testing.allocator);
            defer std.testing.allocator.free(result);
            for (values, result) |value, actual| {
                const x: f64 = value;
                const reference: f32 = @floatCast(0.5 * x * (1 + std.math.tanh(@sqrt(@as(f64, 2.0 / std.math.pi)) * (x + 0.044715 * x * x * x))));
                try std.testing.expectApproxEqAbs(reference, actual, 2e-5);
            }
            // The exact merger operation remains available, and differs at
            // x=2.5 enough to detect accidentally routing both through it.
            const merger = (try cb.geluExact(input)).?;
            defer cb.free(merger);
            const exact = try cb.toFloat32(merger, std.testing.allocator);
            defer std.testing.allocator.free(exact);
            try std.testing.expect(@abs(result[7] - exact[7]) > 1e-4);
            const unchanged = try cb.toFloat32(input, std.testing.allocator);
            defer std.testing.allocator.free(unchanged);
            try std.testing.expectEqualSlices(f32, &values, unchanged);
        }
    };
    const native_compute = @import("../ops/native_compute.zig");
    var store = native_compute.WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
    var native = native_compute.NativeCompute.init(a, &store, null);
    defer native.deinit();
    const cb = ComputeBackend{ .ptr = &native, .vtable = &native_compute.vtable_impl };
    try Oracle.check(&cb, cfg);
    if (comptime build_options.enable_cuda) {
        const CudaCompute = @import("../ops/cuda/cuda_compute.zig").CudaCompute;
        var cuda = try CudaCompute.init(a);
        defer cuda.deinit();
        const cuda_cb = cuda.computeBackend();
        try Oracle.check(&cuda_cb, cfg);
    }
    cfg.block_gelu_tanh = false;
    const input = try cb.fromFloat32(&.{2.5});
    defer cb.free(input);
    const fallback = try visionBlockActivation(&cb, cfg, input);
    defer cb.free(fallback);
    const exact = (try cb.geluExact(input)).?;
    defer cb.free(exact);
    const result = try cb.toFloat32(fallback, a);
    defer a.free(result);
    const expected = try cb.toFloat32(exact, a);
    defer a.free(expected);
    try std.testing.expectEqualSlices(f32, expected, result);
}

test "LightOnOCR3 CUDA learned positions preserve host interpolation merge order and resident transfers" {
    if (comptime !build_options.enable_cuda) return error.SkipZigTest;
    const CudaCompute = @import("../ops/cuda/cuda_compute.zig").CudaCompute;
    const tensor_mod = @import("../backends/tensor.zig");
    const a = std.testing.allocator;
    for ([_]usize{ 768, 1024 }) |dim| for ([_]bool{ false, true }) |bf16| {
        var compute = try CudaCompute.init(a);
        defer compute.deinit();
        if (compute.ctx.info.compute_major != 12 or compute.ctx.info.compute_minor != 1) return error.SkipZigTest;
        compute.capability_profile = .lightonocr3;
        const cb = compute.computeBackend();
        const values = try a.alloc(f32, 48 * 48 * dim);
        defer a.free(values);
        for (values, 0..) |*value, i| value.* = @as(f32, @floatFromInt(@as(i32, @intCast((i * 7) % 41)) - 20)) / 32;
        const shape = [_]i64{ 48 * 48, @intCast(dim) };
        const name = "model.visual.pos_embed.weight";
        if (bf16) {
            const bits = try a.alloc(u16, values.len);
            defer a.free(bits);
            for (values, bits) |value, *b| b.* = @intCast(@as(u32, @bitCast(value)) >> 16);
            const tensor = tensor_mod.Tensor{ .data = std.mem.sliceAsBytes(bits), .dtype = .bf16, .shape = &shape, .name = name, .allocator = a, .owns_data = false, .owns_shape = false };
            try compute.insertBf16WeightFromTensor(try a.dupe(u8, name), &tensor);
        } else {
            const tensor = tensor_mod.Tensor{ .data = std.mem.sliceAsBytes(values), .dtype = .f32, .shape = &shape, .name = name, .allocator = a, .owns_data = false, .owns_shape = false };
            try compute.insertWeightFromTensor(try a.dupe(u8, name), &tensor);
        }
        const cfg = Config{ .text_hidden = 1024, .vision_hidden = dim, .intermediate = dim * 4, .block_count = 1, .head_count = dim / 64, .image_size = 48, .patch_size = 1, .spatial_merge = 2, .layer_norm_eps = 1e-5, .image_mean = .{ 0, 0, 0 }, .image_std = .{ 1, 1, 1 }, .is_qwen35 = true };
        var weights = WeightCache.initResident(a, &cb, cfg);
        defer weights.deinit();
        // Independent host reference uses exactly the existing projector.
        weights.position_f32 = try a.dupe(f32, values);
        for ([_][2]usize{ .{ 2, 2 }, .{ 34, 50 }, .{ 128, 128 } }) |grid| {
            const geometry = Geometry{ .width = grid[0], .height = grid[1], .grid_x = grid[0], .grid_y = grid[1], .merged_x = grid[0] / 2, .merged_y = grid[1] / 2 };
            const input_values = try a.alloc(f32, geometry.patchCount() * dim);
            defer a.free(input_values);
            for (input_values, 0..) |*value, i| value.* = @as(f32, @floatFromInt(@as(i32, @intCast((i * 13) % 37)) - 18)) / 128;
            const input = try cb.fromFloat32Shape(input_values, &.{ @intCast(geometry.patchCount()), @intCast(dim) });
            defer cb.free(input);
            const expected = try a.dupe(f32, input_values);
            defer a.free(expected);
            try addInterpolatedPositions(&weights, expected, cfg, geometry);
            const before = compute.stats;
            const positioned = try addInterpolatedPositionsTensor(&cb, a, &weights, input, cfg, geometry);
            defer cb.free(positioned);
            try std.testing.expectEqual(before.h2d_bytes, compute.stats.h2d_bytes);
            try std.testing.expectEqual(before.d2h_bytes, compute.stats.d2h_bytes);
            const actual = try cb.toFloat32(positioned, a);
            defer a.free(actual);
            for (expected, actual) |e, value| try std.testing.expectApproxEqAbs(e, value, 1e-6);
            const unchanged = try cb.toFloat32(input, a);
            defer a.free(unchanged);
            try std.testing.expectEqualSlices(f32, input_values, unchanged);
            const table = try weights.weight("v.position_embd.weight");
            const table_values = try cb.toFloat32(table, a);
            defer a.free(table_values);
            try std.testing.expectEqualSlices(f32, values, table_values);
            const launches = compute.ctx.stats.kernel_launches;
            try std.testing.expect(try CudaCompute.lightOnVisionPositions(&cb, input, table, 1, 2, 2, 48, dim) == null);
            compute.resident_training_cache = true;
            try std.testing.expect(try CudaCompute.lightOnVisionPositions(&cb, input, table, grid[0], grid[1], 2, 48, dim) == null);
            compute.resident_training_cache = false;
            try std.testing.expectEqual(launches, compute.ctx.stats.kernel_launches);
        }
        try std.testing.expectEqual(@as(usize, 3), compute.lighton_vision_fused.position_calls);
    };
}
