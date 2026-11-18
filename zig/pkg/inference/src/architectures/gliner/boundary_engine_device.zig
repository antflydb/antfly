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

//! Strict FP32 Metal DeBERTa and marker routing for GLiNER2.5. All activation
//! math and gathers use device-only contracts; final decoding is separate.
const std = @import("std");
const ops = @import("../../ops/ops.zig");
const model = @import("../../models/gliner_boundary.zig");
const native_engine = @import("boundary_engine.zig");
const math_mod = @import("boundary_device_math.zig");
const artifact = @import("../../models/gliner_boundary_artifact.zig");
const head = @import("boundary_device.zig");
const processor = @import("../../pipelines/gliner_boundary_processor.zig");
const Control = @import("../../execution_control.zig").InferenceExecutionControl;
const CT = ops.CT;

pub const Options = struct {
    backend: ops.BackendKind = .metal,
    /// Offline qualification only: the session must own BF16 encoder matrices.
    experimental_bf16_encoder: bool = false,
    /// Must come from the authenticated artifact identity, never from a model
    /// name or an assumption about a loader's preferred compute dtype.
    precision: artifact.Precision = .fp32,
    execution_policy: math_mod.ExecutionPolicy = .reference_v1,
    admission: native_engine.Limits = .{},
    max_device_bytes: usize = 2 * 1024 * 1024 * 1024,
    max_result_download_bytes: usize = 128 * 1024 * 1024,
    control: ?Control = null,
};

pub const Plan = struct {
    native: native_engine.Plan,
    encoder_weight_bytes: usize,
    encoder_slot_bias_bytes: usize,
    activation_upper_bound_bytes: usize,
    metadata_upper_bound_bytes: usize,
    max_weight_upload_scratch_bytes: usize,
    late_weight_upload_scratch_bytes: usize,
    embedding_phase_upper_bound_bytes: usize,
    /// Conservative device allocation/admission charge, including temporary
    /// private-buffer uploads. This is not process or unified-memory RSS.
    device_upper_bound_bytes: usize,
};

fn mul(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b) catch error.ResourceLimitExceeded;
}
fn add(a: usize, b: usize) !usize {
    return std.math.add(usize, a, b) catch error.ResourceLimitExceeded;
}

pub fn plan(config: *const model.Config, prepared: *const processor.PreparedBatch, options: Options) !Plan {
    try artifact.validatePrecision(config.backbone, options.precision);
    if (options.execution_policy == .optimized_v2 and options.precision != .fp32)
        return error.UnsupportedGlinerBoundaryPrecision;
    const admitted = try native_engine.plan(config, prepared, .{ .limits = options.admission, .control = options.control });
    const e = config.encoder;
    const h: usize = e.hidden_size;
    const i: usize = e.intermediate_size;
    if (h / e.num_attention_heads > 128 or prepared.word_width == 0) return error.UnsupportedGlinerBoundaryConfiguration;
    const precision = physicalPrecision(options.precision);
    const matrix_layer_bytes = try add(try mul(4, try precision.byteLen(h, h)), try add(try precision.byteLen(i, h), try precision.byteLen(h, i)));
    const protected_values = try add(try add(try mul(e.max_position_embeddings, h), try mul(4, h)), try mul(e.num_hidden_layers, try add(try mul(9, h), i)));
    const embedding_bytes = try precision.byteLen(e.vocab_size, h);
    const weight_bytes = try add(try add(embedding_bytes, try mul(e.num_hidden_layers, matrix_layer_bytes)), try mul(protected_values, 4));
    const slot_bias_bytes = if (precision == .f32) 0 else try mul(try mul(e.num_hidden_layers, try add(try mul(5, h), i)), 4);
    const tokens = try mul(admitted.batch, prepared.sequence_length);
    const transient = try mul(tokens, @max(try mul(12, h), try mul(3, i)));
    const relative = try mul(4, try mul(e.max_position_embeddings, h));
    const attention_workspace: usize = if (options.backend == .cuda) (if (options.experimental_bf16_encoder) @import("../../ops/cuda/gliner25_bf16_attention.zig").workspace_limit else @import("../../ops/cuda/gliner25_fp32_attention.zig").workspace_limit) else 0;
    const reduced_staging = if (options.experimental_bf16_encoder) try mul(try mul(tokens, @max(h, i)), 2) else 0;
    const activation_bytes = try add(try add(try add(try mul(try add(transient, relative), 4), admitted.routed_bytes), attention_workspace), reduced_staging);
    // One metadata upload may overlap its staging buffer. Mask and relative
    // IDs persist while each routing index buffer is temporary.
    const route_width = @max(@max(prepared.word_width, prepared.query_width), @max(@max(prepared.classification_width, prepared.group_width), admitted.relation_width));
    // Compact CUDA preparation may temporarily own a padded table-index
    // upload and its staging copy before the persistent relative map exists.
    const compact_metadata = if (options.backend == .cuda) try mul(e.max_position_embeddings, 8) else 0;
    const metadata_bytes = try add(try mul(try add(try add(tokens, try mul(prepared.sequence_length, 2)), try mul(try mul(admitted.batch, route_width), 2)), 4), compact_metadata);
    const largest_linear = @max(try precision.byteLen(h, h), @max(try precision.byteLen(i, h), try precision.byteLen(h, i)));
    const late_scratch = @max(try add(largest_linear, if (precision == .f32) 0 else try mul(@max(h, i), 4)), try mul(try mul(e.max_position_embeddings, h), 4));
    // The vocabulary upload happens before other encoder weights are loaded.
    // Keeping this separate admits useful long contexts without charging a
    // second full vocabulary table throughout all twelve encoder layers.
    const embedding_peak = try add(try mul(embedding_bytes, 2), try add(try mul(try mul(tokens, h), 4), try mul(tokens, 8)));
    const late_peak = try add(try add(try add(weight_bytes, slot_bias_bytes), activation_bytes), try add(metadata_bytes, late_scratch));
    const upper = @max(embedding_peak, late_peak);
    if (options.execution_policy == .optimized_v2) {
        // Immutable weights/relative projections have a separate model lease.
        // Frame-retained intermediates are admitted dynamically before each
        // dispatch, so the enforced request cap is their conservative bound.
        const minimum_live = try add(try mul(try mul(tokens, h), 4), metadata_bytes);
        if (minimum_live > options.max_device_bytes) return error.ResourceLimitExceeded;
        return .{
            .native = admitted,
            .encoder_weight_bytes = 0,
            .encoder_slot_bias_bytes = 0,
            .activation_upper_bound_bytes = options.max_device_bytes,
            .metadata_upper_bound_bytes = metadata_bytes,
            .max_weight_upload_scratch_bytes = 0,
            .late_weight_upload_scratch_bytes = 0,
            .embedding_phase_upper_bound_bytes = minimum_live,
            .device_upper_bound_bytes = options.max_device_bytes,
        };
    }
    if (upper > options.max_device_bytes) return error.ResourceLimitExceeded;
    return .{ .native = admitted, .encoder_weight_bytes = weight_bytes, .encoder_slot_bias_bytes = slot_bias_bytes, .activation_upper_bound_bytes = activation_bytes, .metadata_upper_bound_bytes = metadata_bytes, .max_weight_upload_scratch_bytes = @max(embedding_bytes, late_scratch), .late_weight_upload_scratch_bytes = late_scratch, .embedding_phase_upper_bound_bytes = embedding_peak, .device_upper_bound_bytes = upper };
}

fn physicalPrecision(precision: artifact.Precision) ops.gliner_boundary_device.WeightPrecision {
    return switch (precision) {
        .fp32 => .f32,
        .fp16_encoder => .f16,
        .q8_0 => .q8_0,
        .q4_0 => .q4_0,
        .q4_k => .q4_k,
    };
}

pub const RelationRoute = struct { group_index: usize, schema_index: usize, head_query: usize, tail_query: usize };

/// The request's PreparedBatch remains borrowed for schema/offset metadata.
/// Every returned CT and the per-sample lengths/relations are owned here.
pub const Result = struct {
    math: *math_mod.Context,
    prepared: *const processor.PreparedBatch,
    hidden_states: CT,
    text_states: CT,
    query_states: ?CT,
    classification_states: ?CT,
    parent_states: ?CT,
    relation_query_states: ?CT,
    text_lengths: []usize,
    relation_routes: []?RelationRoute,
    relation_width: usize,
    relation_query_dim: usize,

    pub fn deinit(self: *Result) void {
        const a = self.math.allocator;
        a.free(self.text_lengths);
        a.free(self.relation_routes);
        self.math.destroy();
        self.* = undefined;
    }

    pub fn asHeadInput(self: *const Result, control: ?Control) !head.Input {
        return .{ .batch = self.prepared.samples.len, .text_length = self.prepared.word_width, .queries = self.prepared.query_width, .experimental_bf16_linears = self.math.experimental_bf16_heads, .text_states = self.text_states, .query_states = self.query_states orelse return error.NoBoundaryQueries, .text_lengths = self.text_lengths, .query_mask = self.prepared.query_marker_mask, .execution_policy = if (self.math.resident_weights) .optimized_v2 else .reference_v1, .control = control };
    }

    pub fn download(self: *Result, tensor: CT, elements: usize) ![]f32 {
        return self.math.download(tensor, elements, false);
    }

    pub fn stats(self: *const Result) head.Stats {
        return self.math.stats;
    }
};

fn uploadIds(math: *math_mod.Context, ids: []const i64) !CT {
    const values = try math.allocator.alloc(i32, ids.len);
    defer math.allocator.free(values);
    for (ids, values) |value, *out| out.* = std.math.cast(i32, value) orelse return error.InvalidBoundaryRouting;
    return math.uploadIntegers(values);
}

fn route(math: *math_mod.Context, hidden: CT, prepared: *const processor.PreparedBatch, indices: []const i64, valid: []const bool, width: usize, hidden_size: usize) !?CT {
    if (width == 0) return null;
    const values = try math.allocator.alloc(i32, indices.len);
    defer math.allocator.free(values);
    for (indices, valid, values, 0..) |index, keep, *out, i| {
        out.* = if (keep) std.math.cast(i32, try add(try mul(i / width, prepared.sequence_length), @intCast(index))) orelse return error.InvalidBoundaryRouting else -1;
    }
    const tensor = try math.uploadIntegers(values);
    defer math.drop(tensor);
    return math.kernel(.gather_i32, &.{ prepared.input_ids.len, hidden_size, indices.len }, &.{ hidden, tensor }, 0);
}

const CompactRelativeLayout = struct {
    buckets: [512]i32 = undefined,
    offsets: [1023]i32 = undefined,
    rows: usize = 0,
};

fn compactRelativeLayout(sequence: usize, buckets: u32, maximum: usize) ?CompactRelativeLayout {
    if (sequence == 0 or sequence > 512 or buckets != 256 or maximum != 512) return null;
    var layout = CompactRelativeLayout{};
    var lookup: [512]i32 = @splat(-1);
    for (layout.offsets[0 .. 2 * sequence - 1], 0..) |*offset, i| {
        const bucket = @import("../../models/deberta.zig").relativePositionBucket(@as(i64, @intCast(i)) - @as(i64, @intCast(sequence - 1)), buckets, @intCast(maximum));
        if (lookup[bucket] < 0) {
            lookup[bucket] = @intCast(layout.rows);
            layout.buckets[layout.rows] = @intCast(bucket);
            layout.rows += 1;
        }
        offset.* = lookup[bucket];
    }
    // Keep the GEMM leading dimension aligned. Padded duplicates are never
    // referenced by the offset map and introduce no new relative positions.
    const padded = std.mem.alignForward(usize, layout.rows, 32);
    @memset(layout.buckets[layout.rows..padded], layout.buckets[layout.rows - 1]);
    layout.rows = padded;
    return layout;
}

test "GLiNER FP32 attention boundary compact rows preserve every bucket" {
    for ([_]usize{ 1, 31, 128, 129, 511, 512 }) |sequence| {
        const compact = compactRelativeLayout(sequence, 256, 512).?;
        try std.testing.expect(compact.rows > 0 and compact.rows <= 512 and compact.rows % 32 == 0);
        for (compact.offsets[0 .. 2 * sequence - 1], 0..) |offset, i| {
            const expected = @import("../../models/deberta.zig").relativePositionBucket(@as(i64, @intCast(i)) - @as(i64, @intCast(sequence - 1)), 256, 512);
            try std.testing.expect(offset >= 0 and offset < compact.rows);
            try std.testing.expectEqual(expected, @as(u32, @intCast(compact.buckets[@intCast(offset)])));
        }
    }
    try std.testing.expectEqual(@as(usize, 256), compactRelativeLayout(128, 256, 512).?.rows);
    try std.testing.expect(compactRelativeLayout(513, 256, 512) == null);
    try std.testing.expect(compactRelativeLayout(128, 128, 512) == null);
}

fn encoderLayer(math: *math_mod.Context, hidden: CT, relative: CT, relative_ids: CT, mask: CT, config: *const model.Config, batch: usize, sequence: usize, relative_rows: usize, layer: usize) !CT {
    const e = config.encoder;
    const h = e.hidden_size;
    const rows = batch * sequence;
    var name: [192]u8 = undefined;
    var stage_started_ns = math.traceNow();
    var stage_started_dispatches = math.stats.device_dispatches;
    const direct_qkv = try math.bf16AttentionProjections(hidden, rows, h, layer, batch, sequence, 3, null);
    const packed_qkv = if (direct_qkv) |p| @as(?ops.gliner_boundary_device.PackedQkvResult, .{ .query = p[0].?, .key = p[1].?, .value = p[2].? }) else try math.linearQkv(hidden, rows, h, layer);
    const q = if (packed_qkv) |result| result.query else try math.linear(hidden, rows, h, h, try std.fmt.bufPrint(&name, "encoder.layer.{d}.attention.self.query_proj", .{layer}));
    const k = if (packed_qkv) |result| result.key else try math.linear(hidden, rows, h, h, try std.fmt.bufPrint(&name, "encoder.layer.{d}.attention.self.key_proj", .{layer}));
    const v = if (packed_qkv) |result| result.value else try math.linear(hidden, rows, h, h, try std.fmt.bufPrint(&name, "encoder.layer.{d}.attention.self.value_proj", .{layer}));
    const direct_relative = try math.bf16AttentionProjections(relative, relative_rows, h, layer, batch, sequence, 2, .{ .layout = .boundary_padded_v1, .normalization_eps = e.layer_norm_eps, .position_buckets = e.position_buckets, .max_position = e.max_position_embeddings });
    const qr = if (direct_relative) |p| p[0].? else if (math.resident_weights)
        try math.derived(.{ .relative_query = @intCast(layer) }, &.{ @intCast(relative_rows), h })
    else
        try math.linear(relative, relative_rows, h, h, try std.fmt.bufPrint(&name, "encoder.layer.{d}.attention.self.query_proj", .{layer}));
    const kr = if (direct_relative) |p| p[1].? else if (math.resident_weights)
        try math.derived(.{ .relative_key = @intCast(layer) }, &.{ @intCast(relative_rows), h })
    else
        try math.linear(relative, relative_rows, h, h, try std.fmt.bufPrint(&name, "encoder.layer.{d}.attention.self.key_proj", .{layer}));
    try math.traceLayerStage(layer, .qkv, stage_started_ns, stage_started_dispatches);
    stage_started_ns = math.traceNow();
    stage_started_dispatches = math.stats.device_dispatches;
    const attended = try math.kernel(.deberta_attention, &.{ batch, sequence, e.num_attention_heads, h / e.num_attention_heads, relative_rows }, &.{ q, k, v, qr, kr, relative_ids, mask }, 0);
    try math.traceLayerStage(layer, .attention, stage_started_ns, stage_started_dispatches);
    stage_started_ns = math.traceNow();
    stage_started_dispatches = math.stats.device_dispatches;
    math.drop(q);
    math.drop(k);
    math.drop(v);
    math.drop(qr);
    math.drop(kr);
    const projected = try math.linear(attended, rows, h, h, try std.fmt.bufPrint(&name, "encoder.layer.{d}.attention.output.dense", .{layer}));
    math.drop(attended);
    const attention_norm = try math.addNormEps(hidden, projected, rows, h, try std.fmt.bufPrint(&name, "encoder.layer.{d}.attention.output.LayerNorm", .{layer}), e.layer_norm_eps);
    math.drop(projected);
    try math.traceLayerStage(layer, .output, stage_started_ns, stage_started_dispatches);
    stage_started_ns = math.traceNow();
    stage_started_dispatches = math.stats.device_dispatches;
    if (try math.fusedFfn(attention_norm, attention_norm, rows, h, e.intermediate_size, layer, e.layer_norm_eps)) |normalized| {
        math.drop(attention_norm);
        try math.traceLayerStage(layer, .ffn, stage_started_ns, stage_started_dispatches);
        return normalized;
    }
    const intermediate = try math.linear(attention_norm, rows, h, e.intermediate_size, try std.fmt.bufPrint(&name, "encoder.layer.{d}.intermediate.dense", .{layer}));
    const activated = try math.kernel(.gelu, &.{rows * e.intermediate_size}, &.{intermediate}, 0);
    math.drop(intermediate);
    const output = try math.linear(activated, rows, e.intermediate_size, h, try std.fmt.bufPrint(&name, "encoder.layer.{d}.output.dense", .{layer}));
    math.drop(activated);
    const ffn_residual = try math.kernel(.add, &.{rows * h}, &.{ attention_norm, output }, 0);
    math.drop(attention_norm);
    math.drop(output);
    const normalized = try math.normEps(ffn_residual, rows, h, try std.fmt.bufPrint(&name, "encoder.layer.{d}.output.LayerNorm", .{layer}), e.layer_norm_eps);
    math.drop(ffn_residual);
    try math.traceLayerStage(layer, .ffn, stage_started_ns, stage_started_dispatches);
    return normalized;
}

/// Prepare constants using the exact inference arithmetic and the model
/// allocator. The factory grants preparation access and publishes the complete
/// immutable owner only after this function and backend teardown succeed.
pub fn prepareResidentConstants(cb: *const ops.ComputeBackend, allocator: std.mem.Allocator, config: *const model.Config, control: ?Control) !void {
    const math = try math_mod.Context.create(allocator, cb, .{ .max_device_bytes = 2 * 1024 * 1024 * 1024 }, control);
    defer math.destroy();
    math.resident_weights = true;
    const e = config.encoder;
    const h = e.hidden_size;
    const shape: []const i64 = &.{ e.max_position_embeddings, h };
    const weights = try math.weight("encoder.rel_embeddings.weight", shape);
    const relative = try math.normEps(weights, e.max_position_embeddings, h, "encoder.LayerNorm", e.layer_norm_eps);
    defer math.drop(relative);
    try math.publishDerived(.relative_normalized, shape, relative);
    for (0..e.num_hidden_layers) |layer| {
        var name: [192]u8 = undefined;
        const qr = try math.linear(relative, e.max_position_embeddings, h, h, try std.fmt.bufPrint(&name, "encoder.layer.{d}.attention.self.query_proj", .{layer}));
        defer math.drop(qr);
        try math.publishDerived(.{ .relative_query = @intCast(layer) }, shape, qr);
        const kr = try math.linear(relative, e.max_position_embeddings, h, h, try std.fmt.bufPrint(&name, "encoder.layer.{d}.attention.self.key_proj", .{layer}));
        defer math.drop(kr);
        try math.publishDerived(.{ .relative_key = @intCast(layer) }, shape, kr);
    }
}

pub fn encodeDevice(cb: *const ops.ComputeBackend, allocator: std.mem.Allocator, config: *const model.Config, prepared: *const processor.PreparedBatch, options: Options) !Result {
    if ((cb.kind() != .metal and cb.kind() != .cuda) or cb.vtable.glinerBoundaryDevice == null) return error.UnsupportedGlinerBoundaryDevice;
    if (config.encoder.family != .deberta) return error.UnsupportedGlinerBoundaryEncoder;
    if (cb.decoderRuntimeHasActiveFrame()) return error.GlinerBoundaryExternalFrame;
    try cb.checkExecutionControl();
    if (options.control) |control| try control.check();
    var planning_options = options;
    planning_options.backend = cb.kind();
    const resource_plan = try plan(config, prepared, planning_options);
    const math = try math_mod.Context.create(allocator, cb, .{ .max_device_bytes = options.max_device_bytes, .max_result_download_bytes = options.max_result_download_bytes }, options.control);
    errdefer math.destroy();
    if (options.experimental_bf16_encoder and (cb.kind() != .cuda or options.precision != .fp32 or options.execution_policy != .reference_v1)) return error.UnsupportedGlinerBoundaryPrecision;
    math.encoder_precision = if (options.experimental_bf16_encoder) .bf16 else physicalPrecision(options.precision);
    math.experimental_bf16_heads = options.experimental_bf16_encoder;
    math.configure(options.execution_policy);
    math.setTraceRole(.encoder);
    const e = config.encoder;
    const h = e.hidden_size;
    const batch = prepared.samples.len;
    const sequence = prepared.sequence_length;
    const rows = prepared.input_ids.len;
    const embedding_started_ns = math.traceNow();
    const mask = try uploadIds(math, prepared.attention_mask);
    defer math.drop(mask);
    const words = if (options.precision == .fp32) blk: {
        const ids = try uploadIds(math, prepared.input_ids);
        defer math.drop(ids);
        const embedding = try math.weight("embeddings.word_embeddings.weight", &.{ e.vocab_size, h });
        break :blk try math.kernel(.gather_i32, &.{ e.vocab_size, h, rows }, &.{ embedding, ids }, 0);
    } else try math.embedding(prepared.input_ids, e.vocab_size, h);
    const normed = try math.normEps(words, rows, h, "embeddings.LayerNorm", e.layer_norm_eps);
    math.drop(words);
    var hidden = try math.kernel(.mask_rows, &.{ rows, h }, &.{ normed, mask }, 0);
    math.drop(normed);
    math.traceStage(.embedding, embedding_started_ns);
    var relative = if (math.resident_weights)
        try math.derived(.relative_normalized, &.{ e.max_position_embeddings, h })
    else blk: {
        const relative_weight = try math.weight("encoder.rel_embeddings.weight", &.{ e.max_position_embeddings, h });
        break :blk try math.normEps(relative_weight, e.max_position_embeddings, h, "encoder.LayerNorm", e.layer_norm_eps);
    };
    defer math.drop(relative);
    const relative_values = try allocator.alloc(i32, sequence * 2 - 1);
    defer allocator.free(relative_values);
    var relative_rows: usize = e.max_position_embeddings;
    const compact = if (!math.resident_weights and cb.kind() == .cuda and cb.compactDebertaRelativeProjectionEnabled(sequence)) compactRelativeLayout(sequence, e.position_buckets, e.max_position_embeddings) else null;
    if (compact != null and compact.?.rows < e.max_position_embeddings) {
        const layout = compact.?;
        @memcpy(relative_values, layout.offsets[0..relative_values.len]);
        const indices = try math.uploadIntegers(layout.buckets[0..layout.rows]);
        defer math.drop(indices);
        const gathered = try math.kernel(.gather_i32, &.{ e.max_position_embeddings, h, layout.rows }, &.{ relative, indices }, 0);
        math.drop(relative);
        relative = gathered;
        relative_rows = layout.rows;
    } else for (relative_values, 0..) |*value, i| value.* = @intCast(@import("../../models/deberta.zig").relativePositionBucket(@as(i64, @intCast(i)) - @as(i64, @intCast(sequence - 1)), e.position_buckets, e.max_position_embeddings));
    const relative_ids = try math.uploadIntegers(relative_values);
    defer math.drop(relative_ids);
    const encoder_started_ns = math.traceNow();
    for (0..e.num_hidden_layers) |layer| {
        try math.check();
        const output = try encoderLayer(math, hidden, relative, relative_ids, mask, config, batch, sequence, relative_rows, layer);
        math.drop(hidden);
        hidden = output;
        if ((layer + 1) % 2 == 0) try math.finishSegment();
    }
    math.traceStage(.encoder_layers, encoder_started_ns);
    const routing_started_ns = math.traceNow();
    const text = (try route(math, hidden, prepared, prepared.text_word_indices, prepared.text_word_mask, prepared.word_width, h)).?;
    const queries = try route(math, hidden, prepared, prepared.query_marker_indices, prepared.query_marker_mask, prepared.query_width, h);
    const classification = try route(math, hidden, prepared, prepared.cls_marker_indices, prepared.cls_marker_mask, prepared.classification_width, h);
    const parents = try route(math, hidden, prepared, prepared.parent_marker_indices, prepared.parent_marker_mask, prepared.group_width, h);
    const lengths = try allocator.alloc(usize, batch);
    errdefer allocator.free(lengths);
    for (prepared.samples, lengths) |sample, *length| length.* = sample.words.len;
    const relation_width = resource_plan.native.relation_width;
    const relation_routes = try allocator.alloc(?RelationRoute, batch * relation_width);
    errdefer allocator.free(relation_routes);
    @memset(relation_routes, null);
    const relation_states = if (relation_width > 0) blk: {
        const head_ids = try allocator.alloc(i64, batch * relation_width);
        defer allocator.free(head_ids);
        const tail_ids = try allocator.alloc(i64, batch * relation_width);
        defer allocator.free(tail_ids);
        const valid = try allocator.alloc(bool, batch * relation_width);
        defer allocator.free(valid);
        @memset(head_ids, 0);
        @memset(tail_ids, 0);
        @memset(valid, false);
        for (prepared.samples, 0..) |sample, b| {
            var relation_index: usize = 0;
            for (sample.groups, 0..) |group, gi| {
                if (group.kind != .relation) continue;
                var head_query: ?usize = null;
                var tail_query: ?usize = null;
                for (sample.queries, 0..) |query, qi| {
                    if (query.group_index != gi) continue;
                    if (query.kind == .relation_head) head_query = qi;
                    if (query.kind == .relation_tail) tail_query = qi;
                }
                const hi = head_query orelse return error.InvalidBoundaryRouting;
                const ti = tail_query orelse return error.InvalidBoundaryRouting;
                const index = b * relation_width + relation_index;
                relation_routes[index] = .{ .group_index = gi, .schema_index = group.schema_index, .head_query = hi, .tail_query = ti };
                head_ids[index] = @intCast(sample.queries[hi].marker_index);
                tail_ids[index] = @intCast(sample.queries[ti].marker_index);
                valid[index] = true;
                relation_index += 1;
            }
        }
        const heads = (try route(math, hidden, prepared, head_ids, valid, relation_width, h)).?;
        defer math.drop(heads);
        const tails = (try route(math, hidden, prepared, tail_ids, valid, relation_width, h)).?;
        defer math.drop(tails);
        if (!config.head.directional_relation_states) return error.UnsupportedGlinerBoundaryConfiguration;
        break :blk try math.kernel(.concat, &.{ batch * relation_width, h, h }, &.{ heads, tails }, 0);
    } else null;
    try math.finishSegment();
    math.traceStage(.routing, routing_started_ns);
    return .{ .math = math, .prepared = prepared, .hidden_states = hidden, .text_states = text, .query_states = queries, .classification_states = classification, .parent_states = parents, .relation_query_states = relation_states, .text_lengths = lengths, .relation_routes = relation_routes, .relation_width = relation_width, .relation_query_dim = resource_plan.native.relation_query_dim };
}
