// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Native encoder and structural state routing for GLiNER2.5. This module is
//! an internal inference building block; runtime advertisement remains gated
//! independently until the complete task pipeline and backends are qualified.
//! Model weights use the existing GLiNER loader convention: strip one leading
//! "encoder." from encoder.embeddings.* and encoder.encoder.*, retaining all
//! boundary_head.*, classifier.*, relation_scorer.*, and record_decoder.* keys.
const std = @import("std");
const boundary = @import("../models/gliner_boundary.zig");
const processor = @import("../pipelines/gliner_boundary_processor.zig");
const compute = @import("../ops/ops.zig");
const deberta = @import("deberta.zig");
const tiled_attention = @import("../ops/deberta_tiled_attention.zig");
const head = @import("gliner_boundary_head.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Allocator = std.mem.Allocator;

pub const Limits = struct {
    max_batch: usize = 8,
    /// max_len in the artifact counts body words, not subword tokens. Schema
    /// tokens and enum prefixes therefore have separate explicit admission.
    max_sequence_tokens: usize = 16384,
    max_batch_tokens: usize = 65536,
    max_text_words: usize = 8192,
    max_queries: usize = 256,
    max_classification_labels: usize = 512,
    max_groups: usize = 64,
    max_relations: usize = 64,
    max_encoder_output_bytes: usize = 128 * 1024 * 1024,
    max_routed_bytes: usize = 128 * 1024 * 1024,
    /// Attention remains quadratic in work. This cap admits one base/multi
    /// request at the full encoded-token limit while bounding aggregate batch
    /// work; deadlines provide the independent wall-clock limit.
    max_attention_work_items: u64 = 4 * 1024 * 1024 * 1024,
    /// Exact tile workspace is separately checked and exported by plan().
    /// Serving admission must also include other encoder tensors and weights.
    max_attention_scratch_bytes: usize = 8 * 1024 * 1024,
};

pub const Options = struct { limits: Limits = .{}, control: ?Control = null };

pub const RelationRoute = struct {
    group_index: usize,
    schema_index: usize,
    head_query_id: usize,
    tail_query_id: usize,
};

pub const Result = struct {
    allocator: Allocator,
    arena: *std.heap.ArenaAllocator,
    batch: usize,
    hidden_size: usize,
    text_length: usize,
    queries: usize,
    classifications: usize,
    groups: usize,
    relations: usize,
    relation_query_dim: usize,
    text_states: []f32, // [B,W,H], includes synthetic enum-prefix words
    query_states: []f32, // [B,Q,H]
    classification_states: []f32, // [B,C,H]
    parent_states: []f32, // [B,G,H]
    relation_query_states: []f32, // [B,R,2H] directional, otherwise [B,R,H]
    text_mask: []bool, // [B,W]
    query_mask: []bool, // [B,Q]
    classification_mask: []bool, // [B,C]
    parent_mask: []bool, // [B,G]
    relation_mask: []bool, // [B,R]
    text_lengths: []usize, // [B]
    query_counts: []usize,
    classification_counts: []usize,
    group_counts: []usize,
    relation_counts: []usize,
    prefix_word_counts: []usize,
    query_group_indices: []i64, // [B,Q]
    classification_group_indices: []i64, // [B,C]
    relation_routes: []?RelationRoute, // [B,R], padded entries are null

    pub fn deinit(self: *Result) void {
        self.arena.deinit();
        self.allocator.destroy(self.arena);
        self.* = undefined;
    }

    /// Borrows only this result's storage; no processor arena is retained.
    /// Classification-only batches have zero queries and skip the boundary
    /// head while using classification_states and classification_mask.
    pub fn asHeadInput(self: *const Result, control: ?Control) head.Input {
        return .{
            .batch = self.batch,
            .text_length = self.text_length,
            .queries = self.queries,
            .text_states = self.text_states,
            .query_states = self.query_states,
            .text_lengths = self.text_lengths,
            .query_mask = self.query_mask,
            .control = control,
        };
    }
};

pub const Plan = struct {
    batch: usize,
    hidden_elements: usize,
    relation_width: usize,
    relation_query_dim: usize,
    routed_bytes: usize,
    attention_work_items: u64,
    attention_scratch_bytes: usize,
};

fn product(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b) catch error.ResourceLimitExceeded;
}

fn sum(a: usize, b: usize) !usize {
    return std.math.add(usize, a, b) catch error.ResourceLimitExceeded;
}

fn check(control: ?Control) !void {
    if (control) |c| try c.check();
}

fn checkedIndex(value: i64, length: usize) !usize {
    if (value < 0 or @as(u64, @intCast(value)) >= length) return error.InvalidBoundaryRouting;
    return @intCast(value);
}

fn validateRoutes(indices: []const i64, mask: []const bool, batch: usize, width: usize) !void {
    const count = try product(batch, width);
    if (indices.len != count or mask.len != count) return error.InvalidInputShape;
}

fn countRelations(sample: processor.Sample) usize {
    var count: usize = 0;
    for (sample.groups) |group| if (group.kind == .relation) {
        count += 1;
    };
    return count;
}

fn relationRoute(sample: processor.Sample, group_index: usize) !RelationRoute {
    const group = sample.groups[group_index];
    if (group.kind != .relation or group.child_markers.len != 2) return error.InvalidBoundaryRouting;
    var head_id: ?usize = null;
    var tail_id: ?usize = null;
    for (sample.queries, 0..) |query, query_id| {
        if (query.group_index != group_index) continue;
        switch (query.kind) {
            .relation_head => {
                if (head_id != null or query.role_index != 0) return error.InvalidBoundaryRouting;
                head_id = query_id;
            },
            .relation_tail => {
                if (tail_id != null or query.role_index != 1) return error.InvalidBoundaryRouting;
                tail_id = query_id;
            },
            else => return error.InvalidBoundaryRouting,
        }
        if (query.schema_index != group.schema_index) return error.InvalidBoundaryRouting;
    }
    return .{
        .group_index = group_index,
        .schema_index = group.schema_index,
        .head_query_id = head_id orelse return error.InvalidBoundaryRouting,
        .tail_query_id = tail_id orelse return error.InvalidBoundaryRouting,
    };
}

/// Validates all typed routes and computes allocation/attention admission
/// before encoder execution. A hand-built malformed PreparedBatch cannot turn
/// padding, signed IDs, or a misclassified marker into an unchecked gather.
pub fn plan(config: *const boundary.Config, prepared: *const processor.PreparedBatch, options: Options) !Plan {
    try check(options.control);
    if (config.version != boundary.config_version or config.architecture_version != boundary.architecture_version)
        return error.UnsupportedGlinerBoundaryVersion;
    const encoder = config.encoder;
    if (encoder.hidden_size == 0 or encoder.hidden_size > std.math.maxInt(i32) or
        encoder.num_attention_heads == 0 or encoder.hidden_size % encoder.num_attention_heads != 0 or
        encoder.intermediate_size == 0 or encoder.intermediate_size > std.math.maxInt(i32) or
        encoder.num_hidden_layers == 0 or encoder.vocab_size == 0 or
        encoder.max_position_embeddings == 0 or encoder.position_buckets == 0 or
        !std.math.isFinite(encoder.layer_norm_eps) or encoder.layer_norm_eps <= 0 or config.max_len == 0)
        return error.InvalidGlinerBoundaryConfig;
    const b = prepared.samples.len;
    const s = prepared.sequence_length;
    const limits = options.limits;
    if (b == 0 or s == 0) return error.InvalidInputShape;
    if (b > limits.max_batch or b > std.math.maxInt(i32) or
        s > limits.max_sequence_tokens or s > std.math.maxInt(i32) or
        prepared.word_width > limits.max_text_words or prepared.query_width > limits.max_queries or
        prepared.classification_width > limits.max_classification_labels or prepared.group_width > limits.max_groups)
        return error.ResourceLimitExceeded;
    const tokens = try product(b, s);
    if (tokens > limits.max_batch_tokens or tokens > std.math.maxInt(i32)) return error.ResourceLimitExceeded;
    if (prepared.input_ids.len != tokens or prepared.attention_mask.len != tokens) return error.InvalidInputShape;
    try validateRoutes(prepared.text_word_indices, prepared.text_word_mask, b, prepared.word_width);
    try validateRoutes(prepared.query_marker_indices, prepared.query_marker_mask, b, prepared.query_width);
    try validateRoutes(prepared.cls_marker_indices, prepared.cls_marker_mask, b, prepared.classification_width);
    try validateRoutes(prepared.parent_marker_indices, prepared.parent_marker_mask, b, prepared.group_width);
    if (prepared.query_group_index.len != prepared.query_marker_indices.len or
        prepared.cls_group_index.len != prepared.cls_marker_indices.len) return error.InvalidInputShape;
    var relation_width: usize = 0;
    for (prepared.samples, 0..) |sample, row| {
        try check(options.control);
        if (sample.input_ids.len == 0 or sample.input_ids.len > s or sample.words.len > prepared.word_width or
            sample.queries.len > prepared.query_width or sample.classification_labels.len > prepared.classification_width or
            sample.groups.len > prepared.group_width or sample.prefix_word_count > sample.words.len or
            sample.body_word_count != sample.words.len - sample.prefix_word_count) return error.InvalidInputShape;
        // Upstream max_len is applied before synthetic enum words are added.
        if (sample.body_word_count > config.max_len) return error.ResourceLimitExceeded;
        for (0..s) |i| {
            const index = row * s + i;
            const valid = i < sample.input_ids.len;
            if (prepared.attention_mask[index] != @as(i64, if (valid) 1 else 0)) return error.InvalidBoundaryRouting;
            const id = prepared.input_ids[index];
            if (id < 0 or @as(u64, @intCast(id)) >= encoder.vocab_size) return error.InvalidGlinerBoundaryTokenId;
            if (valid and id != sample.input_ids[i]) return error.InvalidBoundaryRouting;
            if (!valid and id != 0) return error.InvalidBoundaryRouting;
        }
        for (0..prepared.word_width) |i| {
            const index = row * prepared.word_width + i;
            const valid = i < sample.words.len;
            if (prepared.text_word_mask[index] != valid) return error.InvalidBoundaryRouting;
            if (!valid) continue;
            const token_index = try checkedIndex(prepared.text_word_indices[index], sample.input_ids.len);
            const word = sample.words[i];
            if (token_index != word.input_start or word.input_end <= word.input_start or word.input_end > sample.input_ids.len)
                return error.InvalidBoundaryRouting;
            if (i > 0 and word.input_start < sample.words[i - 1].input_end) return error.InvalidBoundaryRouting;
        }
        for (0..prepared.query_width) |i| {
            const index = row * prepared.query_width + i;
            const valid = i < sample.queries.len;
            if (prepared.query_marker_mask[index] != valid) return error.InvalidBoundaryRouting;
            if (!valid) continue;
            const token_index = try checkedIndex(prepared.query_marker_indices[index], sample.input_ids.len);
            const group_index = try checkedIndex(prepared.query_group_index[index], sample.groups.len);
            const query = sample.queries[i];
            if (token_index != query.marker_index or group_index != query.group_index) return error.InvalidBoundaryRouting;
            const group = sample.groups[group_index];
            if (group.kind == .classification or query.role_index >= group.child_markers.len or
                group.child_markers[query.role_index] != token_index) return error.InvalidBoundaryRouting;
            const compatible = switch (query.kind) {
                .field => group.kind == .structure and query.schema_index == group.schema_index,
                .entity, .attribute => group.kind == .entities,
                .relation_head, .relation_tail => group.kind == .relation,
            };
            if (!compatible) return error.InvalidBoundaryRouting;
        }
        for (0..prepared.classification_width) |i| {
            const index = row * prepared.classification_width + i;
            const valid = i < sample.classification_labels.len;
            if (prepared.cls_marker_mask[index] != valid) return error.InvalidBoundaryRouting;
            if (!valid) continue;
            const token_index = try checkedIndex(prepared.cls_marker_indices[index], sample.input_ids.len);
            const group_index = try checkedIndex(prepared.cls_group_index[index], sample.groups.len);
            const label = sample.classification_labels[i];
            const group = sample.groups[group_index];
            if (token_index != label.marker_index or group_index != label.group_index or group.kind != .classification or
                label.schema_index != group.schema_index) return error.InvalidBoundaryRouting;
            var matches: usize = 0;
            for (group.child_markers) |position| if (position == token_index) {
                matches += 1;
            };
            if (matches != 1) return error.InvalidBoundaryRouting;
        }
        for (0..prepared.group_width) |i| {
            const index = row * prepared.group_width + i;
            const valid = i < sample.groups.len;
            if (prepared.parent_marker_mask[index] != valid) return error.InvalidBoundaryRouting;
            if (!valid) continue;
            const token_index = try checkedIndex(prepared.parent_marker_indices[index], sample.input_ids.len);
            const group = sample.groups[i];
            if (token_index != group.parent_marker) return error.InvalidBoundaryRouting;
            if (group.child_markers.len > (if (group.kind == .classification) sample.classification_labels.len else sample.queries.len))
                return error.InvalidBoundaryRouting;
            for (group.child_markers, 0..) |position, child_index| {
                if (position >= sample.input_ids.len or position == group.parent_marker) return error.InvalidBoundaryRouting;
                for (group.child_markers[0..child_index]) |other| if (other == position) return error.InvalidBoundaryRouting;
                var matches: usize = 0;
                if (group.kind == .classification) {
                    for (sample.classification_labels) |label| if (label.group_index == i and label.marker_index == position) {
                        matches += 1;
                    };
                } else {
                    for (sample.queries) |query| if (query.group_index == i and query.marker_index == position) {
                        matches += 1;
                    };
                }
                if (matches != 1) return error.InvalidBoundaryRouting;
            }
            if (group.kind == .relation) _ = try relationRoute(sample, i);
        }
        relation_width = @max(relation_width, countRelations(sample));
    }
    if (relation_width > limits.max_relations) return error.ResourceLimitExceeded;
    const hidden_elements = try product(tokens, encoder.hidden_size);
    if (try product(hidden_elements, @sizeOf(f32)) > limits.max_encoder_output_bytes) return error.ResourceLimitExceeded;
    const attention_pairs = std.math.mul(u64, @intCast(tokens), @intCast(s)) catch return error.ResourceLimitExceeded;
    const attention_work_items = std.math.mul(u64, attention_pairs, encoder.num_attention_heads) catch return error.ResourceLimitExceeded;
    if (attention_work_items > limits.max_attention_work_items) return error.ResourceLimitExceeded;
    const attention_workspace = tiled_attention.plan(.{
        .batch = b,
        .sequence = s,
        .heads = encoder.num_attention_heads,
        .head_dim = encoder.hidden_size / encoder.num_attention_heads,
    }, .{
        .max_output_bytes = limits.max_encoder_output_bytes,
        .max_scratch_bytes = limits.max_attention_scratch_bytes,
        .control = options.control,
    }) catch |err| switch (err) {
        error.AttentionOutputLimitExceeded, error.AttentionScratchLimitExceeded => return error.ResourceLimitExceeded,
        else => return err,
    };
    const relation_dim = try product(encoder.hidden_size, if (config.head.directional_relation_states) @as(usize, 2) else 1);
    const rows = try sum(try sum(prepared.word_width, prepared.query_width), try sum(prepared.classification_width, prepared.group_width));
    const float_elements = try sum(try product(try product(b, rows), encoder.hidden_size), try product(try product(b, relation_width), relation_dim));
    var routed_bytes = try product(float_elements, @sizeOf(f32));
    routed_bytes = try sum(routed_bytes, try product(try product(b, try sum(rows, relation_width)), @sizeOf(bool)));
    routed_bytes = try sum(routed_bytes, try product(try product(b, 6), @sizeOf(usize)));
    routed_bytes = try sum(routed_bytes, try product(try product(b, try sum(prepared.query_width, prepared.classification_width)), @sizeOf(i64)));
    routed_bytes = try sum(routed_bytes, try product(try product(b, relation_width), @sizeOf(?RelationRoute)));
    if (routed_bytes > limits.max_routed_bytes) return error.ResourceLimitExceeded;
    return .{ .batch = b, .hidden_elements = hidden_elements, .relation_width = relation_width, .relation_query_dim = relation_dim, .routed_bytes = routed_bytes, .attention_work_items = attention_work_items, .attention_scratch_bytes = attention_workspace.scratch_bytes };
}

const CombinedControl = struct {
    first: ?Control,
    second: ?Control,

    fn checkBoth(raw: ?*anyopaque) !void {
        const self: *const CombinedControl = @ptrCast(@alignCast(raw.?));
        try check(self.first);
        try check(self.second);
    }

    fn view(self: *CombinedControl) Control {
        // Preserve native interruption/progress capabilities while adding both
        // sources' cancellation and deadline checks to every encoder boundary.
        var result = self.second orelse self.first orelse Control{};
        if (result.hard_cancellation == null) if (self.first) |first| {
            result.hard_cancellation = first.hard_cancellation;
        };
        result.ptr = self;
        result.check_fn = checkBoth;
        return result;
    }
};

/// Executes the existing DeBERTa kernels and routes the complete mixed batch.
/// The caller owns beginRequest/endRequest and backend workspace admission.
/// All returned states are owned; the full subword tensor is released here.
pub fn encodeNative(cb: *const compute.ComputeBackend, allocator: Allocator, config: *const boundary.Config, prepared: *const processor.PreparedBatch, options: Options) !Result {
    if (cb.kind() != .native) return error.UnsupportedGlinerBoundaryBackend;
    try cb.checkExecutionControl();
    const checked = try plan(config, prepared, options);
    var combined = CombinedControl{ .first = cb.execution_control, .second = options.control };
    var request_backend = cb.*;
    request_backend.execution_control = combined.view();
    const states = try deberta.forward(&request_backend, allocator, config.encoder.toDeberta(), prepared.input_ids, prepared.attention_mask, checked.batch, prepared.sequence_length);
    defer allocator.free(states);
    var result = try routePlanned(allocator, config, prepared, states, checked, .{ .limits = options.limits, .control = request_backend.execution_control });
    errdefer result.deinit();
    try request_backend.checkExecutionControl();
    return result;
}

/// Routes externally captured encoder states with the same strict contract.
/// This makes first-subtoken gathers and mixed-task ordering independently
/// testable before comparing encoder kernels and the complete task pipeline.
pub fn routeEncoded(allocator: Allocator, config: *const boundary.Config, prepared: *const processor.PreparedBatch, states: []const f32, options: Options) !Result {
    const checked = try plan(config, prepared, options);
    return routePlanned(allocator, config, prepared, states, checked, options);
}

fn gather(allocator: Allocator, states: []const f32, indices: []const i64, mask: []const bool, batch: usize, sequence: usize, width: usize, hidden: usize, control: ?Control) ![]f32 {
    const output = try allocator.alloc(f32, try product(indices.len, hidden));
    @memset(output, 0);
    for (0..batch) |b| for (0..width) |i| {
        try check(control);
        const row = b * width + i;
        if (!mask[row]) continue;
        // plan validated every active signed index before any allocation.
        const position: usize = @intCast(indices[row]);
        @memcpy(output[row * hidden ..][0..hidden], states[(b * sequence + position) * hidden ..][0..hidden]);
    };
    return output;
}

fn routePlanned(allocator: Allocator, config: *const boundary.Config, prepared: *const processor.PreparedBatch, states: []const f32, checked: Plan, options: Options) !Result {
    if (states.len != checked.hidden_elements) return error.InvalidInputShape;
    for (states, 0..) |value, i| {
        if (i % 16384 == 0) try check(options.control);
        if (!std.math.isFinite(value)) return error.NonFiniteBoundaryScore;
    }
    const arena = try allocator.create(std.heap.ArenaAllocator);
    errdefer allocator.destroy(arena);
    arena.* = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const a = arena.allocator();
    const b = checked.batch;
    const h: usize = config.encoder.hidden_size;
    const relation_rows = try product(b, checked.relation_width);
    const result = Result{
        .allocator = allocator,
        .arena = arena,
        .batch = b,
        .hidden_size = h,
        .text_length = prepared.word_width,
        .queries = prepared.query_width,
        .classifications = prepared.classification_width,
        .groups = prepared.group_width,
        .relations = checked.relation_width,
        .relation_query_dim = checked.relation_query_dim,
        .text_states = try gather(a, states, prepared.text_word_indices, prepared.text_word_mask, b, prepared.sequence_length, prepared.word_width, h, options.control),
        .query_states = try gather(a, states, prepared.query_marker_indices, prepared.query_marker_mask, b, prepared.sequence_length, prepared.query_width, h, options.control),
        .classification_states = try gather(a, states, prepared.cls_marker_indices, prepared.cls_marker_mask, b, prepared.sequence_length, prepared.classification_width, h, options.control),
        .parent_states = try gather(a, states, prepared.parent_marker_indices, prepared.parent_marker_mask, b, prepared.sequence_length, prepared.group_width, h, options.control),
        .relation_query_states = try a.alloc(f32, try product(relation_rows, checked.relation_query_dim)),
        .text_mask = try a.dupe(bool, prepared.text_word_mask),
        .query_mask = try a.dupe(bool, prepared.query_marker_mask),
        .classification_mask = try a.dupe(bool, prepared.cls_marker_mask),
        .parent_mask = try a.dupe(bool, prepared.parent_marker_mask),
        .relation_mask = try a.alloc(bool, relation_rows),
        .text_lengths = try a.alloc(usize, b),
        .query_counts = try a.alloc(usize, b),
        .classification_counts = try a.alloc(usize, b),
        .group_counts = try a.alloc(usize, b),
        .relation_counts = try a.alloc(usize, b),
        .prefix_word_counts = try a.alloc(usize, b),
        .query_group_indices = try a.dupe(i64, prepared.query_group_index),
        .classification_group_indices = try a.dupe(i64, prepared.cls_group_index),
        .relation_routes = try a.alloc(?RelationRoute, relation_rows),
    };
    @memset(result.relation_query_states, 0);
    @memset(result.relation_mask, false);
    @memset(result.relation_routes, null);
    for (prepared.samples, 0..) |sample, row| {
        try check(options.control);
        result.text_lengths[row] = sample.words.len;
        result.query_counts[row] = sample.queries.len;
        result.classification_counts[row] = sample.classification_labels.len;
        result.group_counts[row] = sample.groups.len;
        result.relation_counts[row] = countRelations(sample);
        result.prefix_word_counts[row] = sample.prefix_word_count;
        var relation_index: usize = 0;
        for (sample.groups, 0..) |group, group_index| {
            if (group.kind != .relation) continue;
            const route = try relationRoute(sample, group_index);
            const index = row * checked.relation_width + relation_index;
            relation_index += 1;
            result.relation_routes[index] = route;
            result.relation_mask[index] = true;
            const head_state = result.query_states[(row * prepared.query_width + route.head_query_id) * h ..][0..h];
            const tail_state = result.query_states[(row * prepared.query_width + route.tail_query_id) * h ..][0..h];
            const output = result.relation_query_states[index * checked.relation_query_dim ..][0..checked.relation_query_dim];
            if (config.head.directional_relation_states) {
                @memcpy(output[0..h], head_state);
                @memcpy(output[h..], tail_state);
            } else {
                for (output, head_state, tail_state) |*value, left, right| {
                    value.* = (left + right) * 0.5;
                    if (!std.math.isFinite(value.*)) return error.NonFiniteBoundaryScore;
                }
            }
        }
    }
    try check(options.control);
    return result;
}

/// Shared adversarial routing fixture for the CPU and device contract tests.
pub const TestBatch = struct {
    ids: [24]i64 = .{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 21, 22, 23, 24, 25, 26, 27, 0, 0, 0, 0, 0 },
    attention: [24]i64 = .{ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0 },
    word_indices: [4]i64 = .{ 10, 11, 6, -99 },
    word_mask: [4]bool = .{ true, true, true, false },
    query_indices: [6]i64 = .{ 1, 3, 4, 1, -99, 999 },
    query_mask: [6]bool = .{ true, true, true, true, false, false },
    query_groups: [6]i64 = .{ 0, 1, 1, 0, 0, 0 },
    cls_indices: [4]i64 = .{ 6, 7, 3, -99 },
    cls_mask: [4]bool = .{ true, true, true, false },
    cls_groups: [4]i64 = .{ 2, 2, 1, 0 },
    parent_indices: [6]i64 = .{ 0, 2, 5, 0, 2, -99 },
    parent_mask: [6]bool = .{ true, true, true, true, true, false },
    groups: [3]processor.Group = .{
        .{ .kind = .entities, .schema_index = 0, .name = "entities", .model_name = "entities", .fragments = &.{}, .parent_marker = 0, .child_markers = &.{1} },
        .{ .kind = .relation, .schema_index = 0, .name = "works_for", .model_name = "works_for", .fragments = &.{}, .parent_marker = 2, .child_markers = &.{ 3, 4 } },
        .{ .kind = .classification, .schema_index = 0, .name = "sentiment", .model_name = "sentiment", .fragments = &.{}, .parent_marker = 5, .child_markers = &.{ 6, 7 } },
    },
    other_groups: [2]processor.Group = .{
        .{ .kind = .entities, .schema_index = 0, .name = "entities", .model_name = "entities", .fragments = &.{}, .parent_marker = 0, .child_markers = &.{1} },
        .{ .kind = .classification, .schema_index = 0, .name = "sentiment", .model_name = "sentiment", .fragments = &.{}, .parent_marker = 2, .child_markers = &.{3} },
    },
    queries: [3]processor.Query = .{
        .{ .kind = .entity, .group_index = 0, .schema_index = 0, .role_index = 0, .label_index = 0, .name = "person", .marker_index = 1 },
        .{ .kind = .relation_head, .group_index = 1, .schema_index = 0, .role_index = 0, .label_index = 0, .name = "head", .marker_index = 3 },
        .{ .kind = .relation_tail, .group_index = 1, .schema_index = 0, .role_index = 1, .label_index = 1, .name = "tail", .marker_index = 4 },
    },
    words: [2]processor.Word = .{
        .{ .text = "choice", .source = null, .input_start = 10, .input_end = 11 },
        .{ .text = "a", .source = .{ .start = 0, .end = 1 }, .input_start = 11, .input_end = 12 },
    },
    other_words: [1]processor.Word = .{.{ .text = "b", .source = .{ .start = 0, .end = 1 }, .input_start = 6, .input_end = 7 }},
    classifications: [2]processor.ClassificationLabel = .{
        .{ .group_index = 2, .schema_index = 0, .label_index = 0, .name = "positive", .marker_index = 6 },
        .{ .group_index = 2, .schema_index = 0, .label_index = 1, .name = "negative", .marker_index = 7 },
    },
    other_classifications: [1]processor.ClassificationLabel = .{.{ .group_index = 1, .schema_index = 0, .label_index = 0, .name = "neutral", .marker_index = 3 }},
    samples: [2]processor.Sample = undefined,

    pub fn prepared(self: *TestBatch) processor.PreparedBatch {
        self.samples = .{
            .{ .original_text = "a", .schema_fingerprint = .{0} ** 32, .input_ids = self.ids[0..12], .words = &self.words, .groups = &self.groups, .queries = &self.queries, .classification_labels = &self.classifications, .enum_choices = &.{}, .prefix_word_count = 1, .body_word_count = 1, .terminal_period_added = false, .is_joint_ie = false },
            .{ .original_text = "b", .schema_fingerprint = .{0} ** 32, .input_ids = self.ids[12..19], .words = &self.other_words, .groups = &self.other_groups, .queries = self.queries[0..1], .classification_labels = &self.other_classifications, .enum_choices = &.{}, .prefix_word_count = 0, .body_word_count = 1, .terminal_period_added = false, .is_joint_ie = false },
        };
        return .{
            .arena = std.heap.ArenaAllocator.init(std.testing.allocator),
            .samples = &self.samples,
            .sequence_length = 12,
            .word_width = 2,
            .query_width = 3,
            .classification_width = 2,
            .group_width = 3,
            .input_ids = &self.ids,
            .attention_mask = &self.attention,
            .text_word_indices = &self.word_indices,
            .text_word_mask = &self.word_mask,
            .query_marker_indices = &self.query_indices,
            .query_marker_mask = &self.query_mask,
            .query_group_index = &self.query_groups,
            .cls_marker_indices = &self.cls_indices,
            .cls_marker_mask = &self.cls_mask,
            .cls_group_index = &self.cls_groups,
            .parent_marker_indices = &self.parent_indices,
            .parent_marker_mask = &self.parent_mask,
        };
    }

    pub fn config() boundary.Config {
        return .{
            .version = boundary.config_version,
            .architecture_version = boundary.architecture_version,
            .max_len = 1,
            .backbone = .small,
            .head = .{ .directional_relation_states = true },
            .encoder = .{ .hidden_size = 2, .intermediate_size = 4, .num_hidden_layers = 1, .num_attention_heads = 1, .vocab_size = 100, .max_position_embeddings = 512, .position_buckets = 256, .layer_norm_eps = 1e-7, .hidden_dropout_prob = 0, .attention_probs_dropout_prob = 0, .pad_token_id = 0 },
        };
    }

    fn states() [48]f32 {
        var result: [48]f32 = undefined;
        for (&result, 0..) |*value, i| value.* = @floatFromInt(i + 1);
        return result;
    }
};

test "gliner boundary engine mixed structural routing preserves masks roles and prefixes" {
    var fixture = TestBatch{};
    var prepared = fixture.prepared();
    defer prepared.deinit();
    const states = TestBatch.states();
    var config = TestBatch.config();
    var result = try routeEncoded(std.testing.allocator, &config, &prepared, &states, .{});
    defer result.deinit();
    try std.testing.expectEqualSlices(f32, &.{ 21, 22, 23, 24, 37, 38, 0, 0 }, result.text_states);
    try std.testing.expectEqualSlices(f32, &.{ 3, 4, 7, 8, 9, 10, 27, 28, 0, 0, 0, 0 }, result.query_states);
    try std.testing.expectEqualSlices(f32, &.{ 13, 14, 15, 16, 31, 32, 0, 0 }, result.classification_states);
    try std.testing.expectEqualSlices(f32, &.{ 1, 2, 5, 6, 11, 12, 25, 26, 29, 30, 0, 0 }, result.parent_states);
    try std.testing.expectEqualSlices(f32, &.{ 7, 8, 9, 10, 0, 0, 0, 0 }, result.relation_query_states);
    try std.testing.expectEqualSlices(usize, &.{ 2, 1 }, result.text_lengths);
    try std.testing.expectEqualSlices(usize, &.{ 1, 0 }, result.prefix_word_counts);
    try std.testing.expectEqualSlices(bool, &.{ true, false }, result.relation_mask);
    try std.testing.expectEqualDeep(@as(?RelationRoute, .{ .group_index = 1, .schema_index = 0, .head_query_id = 1, .tail_query_id = 2 }), result.relation_routes[0]);
    try std.testing.expectEqual(@as(?RelationRoute, null), result.relation_routes[1]);
    try std.testing.expectEqualSlices(f32, result.text_states, result.asHeadInput(null).text_states);
    // Results own every numeric route and mask independently of preparation.
    fixture.word_mask[0] = false;
    fixture.query_groups[0] = 2;
    try std.testing.expect(result.text_mask[0]);
    try std.testing.expectEqual(@as(i64, 0), result.query_group_indices[0]);
    fixture.word_mask[0] = true;
    fixture.query_groups[0] = 0;
    config.head.directional_relation_states = false;
    var mean = try routeEncoded(std.testing.allocator, &config, &prepared, &states, .{});
    defer mean.deinit();
    try std.testing.expectEqualSlices(f32, &.{ 8, 9, 0, 0 }, mean.relation_query_states);
}

test "gliner boundary engine admission rejects malformed routes and distinct body token budgets" {
    const a = std.testing.allocator;
    var fixture = TestBatch{};
    var prepared = fixture.prepared();
    defer prepared.deinit();
    const config = TestBatch.config();
    var states = TestBatch.states();
    _ = try plan(&config, &prepared, .{}); // max_len1 admits one body word + prefix.
    try std.testing.expectError(error.ResourceLimitExceeded, plan(&config, &prepared, .{ .limits = .{ .max_sequence_tokens = 11 } }));
    try std.testing.expectError(error.ResourceLimitExceeded, plan(&config, &prepared, .{ .limits = .{ .max_text_words = 1 } }));
    try std.testing.expectError(error.ResourceLimitExceeded, plan(&config, &prepared, .{ .limits = .{ .max_attention_work_items = 1 } }));
    try std.testing.expectError(error.ResourceLimitExceeded, plan(&config, &prepared, .{ .limits = .{ .max_attention_scratch_bytes = 1 } }));
    try std.testing.expectError(error.ResourceLimitExceeded, plan(&config, &prepared, .{ .limits = .{ .max_routed_bytes = 1 } }));
    try std.testing.expectError(error.ResourceLimitExceeded, plan(&config, &prepared, .{ .limits = .{ .max_encoder_output_bytes = 1 } }));
    fixture.samples[0].prefix_word_count = 0;
    fixture.samples[0].body_word_count = 2;
    try std.testing.expectError(error.ResourceLimitExceeded, plan(&config, &prepared, .{}));
    fixture.samples[0].prefix_word_count = 1;
    fixture.samples[0].body_word_count = 1;
    fixture.query_indices[0] = -1;
    try std.testing.expectError(error.InvalidBoundaryRouting, plan(&config, &prepared, .{}));
    fixture.query_indices[0] = 1;
    fixture.attention[13] = 0;
    try std.testing.expectError(error.InvalidBoundaryRouting, plan(&config, &prepared, .{}));
    fixture.attention[13] = 1;
    fixture.ids[0] = -1;
    try std.testing.expectError(error.InvalidGlinerBoundaryTokenId, plan(&config, &prepared, .{}));
    fixture.ids[0] = 1;
    fixture.queries[2].kind = .relation_head;
    try std.testing.expectError(error.InvalidBoundaryRouting, plan(&config, &prepared, .{}));
    fixture.queries[2].kind = .relation_tail;
    fixture.query_mask[5] = true;
    try std.testing.expectError(error.InvalidBoundaryRouting, plan(&config, &prepared, .{}));
    fixture.query_mask[5] = false;
    try std.testing.expectError(error.InvalidInputShape, routeEncoded(a, &config, &prepared, states[0..1], .{}));
    states[0] = std.math.nan(f32);
    try std.testing.expectError(error.NonFiniteBoundaryScore, routeEncoded(a, &config, &prepared, &states, .{}));
    const Cancel = struct {
        fn call(_: ?*anyopaque) !void {
            return error.Cancelled;
        }
    };
    try std.testing.expectError(error.Cancelled, plan(&config, &prepared, .{ .control = .{ .check_fn = Cancel.call } }));
}

test "gliner boundary engine routed states clean up every allocation failure" {
    var fixture = TestBatch{};
    var prepared = fixture.prepared();
    defer prepared.deinit();
    const config = TestBatch.config();
    const states = TestBatch.states();
    const Check = struct {
        fn run(a: Allocator, config_: *const boundary.Config, prepared_: *const processor.PreparedBatch, states_: []const f32) !void {
            var result = try routeEncoded(a, config_, prepared_, states_, .{});
            defer result.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{ &config, &prepared, &states });
}

test "gliner boundary engine eager encoder host allocation cleanup on padded batch" {
    const native = @import("../ops/native_compute.zig");
    const Tensor = @import("../backends/tensor.zig").Tensor;
    const a = std.testing.allocator;
    var fixture = TestBatch{};
    var prepared = fixture.prepared();
    defer prepared.deinit();
    const config = TestBatch.config();
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    const definitions = [_]struct { name: []const u8, shape: []const i64 }{
        .{ .name = "embeddings.word_embeddings.weight", .shape = &.{ 100, 2 } },
        .{ .name = "embeddings.LayerNorm.weight", .shape = &.{2} },
        .{ .name = "embeddings.LayerNorm.bias", .shape = &.{2} },
        .{ .name = "encoder.rel_embeddings.weight", .shape = &.{ 512, 2 } },
        .{ .name = "encoder.LayerNorm.weight", .shape = &.{2} },
        .{ .name = "encoder.LayerNorm.bias", .shape = &.{2} },
        .{ .name = "encoder.layer.0.attention.self.query_proj.weight", .shape = &.{ 2, 2 } },
        .{ .name = "encoder.layer.0.attention.self.query_proj.bias", .shape = &.{2} },
        .{ .name = "encoder.layer.0.attention.self.key_proj.weight", .shape = &.{ 2, 2 } },
        .{ .name = "encoder.layer.0.attention.self.key_proj.bias", .shape = &.{2} },
        .{ .name = "encoder.layer.0.attention.self.value_proj.weight", .shape = &.{ 2, 2 } },
        .{ .name = "encoder.layer.0.attention.self.value_proj.bias", .shape = &.{2} },
        .{ .name = "encoder.layer.0.attention.output.dense.weight", .shape = &.{ 2, 2 } },
        .{ .name = "encoder.layer.0.attention.output.dense.bias", .shape = &.{2} },
        .{ .name = "encoder.layer.0.attention.output.LayerNorm.weight", .shape = &.{2} },
        .{ .name = "encoder.layer.0.attention.output.LayerNorm.bias", .shape = &.{2} },
        .{ .name = "encoder.layer.0.intermediate.dense.weight", .shape = &.{ 4, 2 } },
        .{ .name = "encoder.layer.0.intermediate.dense.bias", .shape = &.{4} },
        .{ .name = "encoder.layer.0.output.dense.weight", .shape = &.{ 2, 4 } },
        .{ .name = "encoder.layer.0.output.dense.bias", .shape = &.{2} },
        .{ .name = "encoder.layer.0.output.LayerNorm.weight", .shape = &.{2} },
        .{ .name = "encoder.layer.0.output.LayerNorm.bias", .shape = &.{2} },
    };
    for (definitions) |definition| {
        const name = try a.dupe(u8, definition.name);
        errdefer a.free(name);
        var count: usize = 1;
        for (definition.shape) |dimension| count *= @intCast(dimension);
        const values = try a.alloc(f32, count);
        defer a.free(values);
        @memset(values, if (std.mem.endsWith(u8, name, "LayerNorm.weight")) 1 else 0);
        var tensor = try Tensor.initFloat32(a, name, definition.shape, values);
        errdefer tensor.deinit();
        try store.resident_weights.put(a, name, .{ .tensor = tensor });
    }
    var backend = native.NativeCompute.init(a, &store, null);
    defer backend.deinit();
    const cb = backend.computeBackend();
    const Check = struct {
        fn run(allocator: Allocator, backend_: *const compute.ComputeBackend, config_: *const boundary.Config, prepared_: *const processor.PreparedBatch) !void {
            var result = try encodeNative(backend_, allocator, config_, prepared_, .{});
            defer result.deinit();
            for (result.text_states) |value| try std.testing.expectEqual(@as(f32, 0), value);
        }
    };
    // Backend tensors use their stable allocator; inject each architecture/
    // routing host failure, including the padding-mask and relative-ID paths.
    try std.testing.checkAllAllocationFailures(a, Check.run, .{ &cb, &config, &prepared });
}

test "gliner boundary engine Python parity pinned small checkpoint CPU encoder and routing" {
    const directory = @import("antfly_platform").env.getenv("ANTFLY_GLINER25_SMALL_MODEL_DIR") orelse return error.SkipZigTest;
    const fixtures = @import("gliner_boundary_parity_test.zig");
    const safetensors = @import("../models/safetensors.zig");
    const native = @import("../ops/native_compute.zig");
    const ir = @import("../pipelines/extraction_schema.zig");
    const a = std.testing.allocator;
    const weight_path = try std.fs.path.join(a, &.{ directory, "model.safetensors" });
    defer a.free(weight_path);
    var weights = fixtures.TensorFixture{ .allocator = a, .reader = try safetensors.MMapReader.openFileAbsolute(a, weight_path) };
    defer weights.deinit();
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(weights.reader.file_bytes, &digest, .{});
    var actual_hash = std.fmt.bytesToHex(digest, .lower);
    try std.testing.expectEqualStrings("4ee982787ace270d4bf15dbcb28ced38e0aa201372347114ceedd6336055de2b", &actual_hash);
    const tokenizer_path = try std.fs.path.join(a, &.{ directory, "tokenizer.json" });
    defer a.free(tokenizer_path);
    const tokenizer_bytes = try @import("../util/c_file.zig").readFile(a, tokenizer_path);
    defer a.free(tokenizer_bytes);
    std.crypto.hash.sha2.Sha256.hash(tokenizer_bytes, &digest, .{});
    actual_hash = std.fmt.bytesToHex(digest, .lower);
    try std.testing.expectEqualStrings("cbc8ae6037812709c9c26f2a160f8dc48b0440bcb79c8141804259ae2d6adac3", &actual_hash);
    const tokenizer = try @import("inference_hf_tokenizer").HfTokenizer.loadFromBytes(a, tokenizer_bytes);
    defer tokenizer.tokenizer().deinitTokenizer();
    const config_bytes = try fixtures.fixtureBytes(a, "models/small/config.json");
    defer a.free(config_bytes);
    const encoder_bytes = try fixtures.fixtureBytes(a, "models/small/encoder_config.json");
    defer a.free(encoder_bytes);
    const config = try boundary.parseConfig(a, config_bytes, encoder_bytes);
    var store = try weights.loadWeights();
    defer store.deinitOwned();
    var backend = native.NativeCompute.init(a, &store, null);
    defer backend.deinit();
    const cb = backend.computeBackend();
    const cases = [_]struct { id: []const u8, text: []const u8, schema: []const u8 }{
        .{
            .id = "mixed_tasks",
            .text = "John works at Apple. Alice works at Google.",
            .schema =
            \\{"entities":["person","organization"],"relations":[{"type":"works_for"}],"classifications":[{"name":"sentiment","labels":["positive","negative"]}]}
            ,
        },
        .{
            .id = "unicode_offsets",
            .text = "İpek works at Apple in 東京. 🙂 Alice visits café é.",
            .schema =
            \\{"entities":["person","location"],"entity_definitions":{"person":{"description":"A named person"},"location":{"description":"A named place"}}}
            ,
        },
        .{
            .id = "enum_field",
            .text = "The iPhone camera is good.",
            .schema =
            \\{"structures":{"review":{"fields":{"product":{"type":"str"},"sentiment":{"type":"str","choices":["positive","negative"]}}}}}
            ,
        },
    };
    for (cases) |case| {
        errdefer std.debug.print("small checkpoint encoder fixture: {s}\n", .{case.id});
        const fixture_name = try std.fmt.allocPrint(a, "small_reference/{s}.safetensors", .{case.id});
        defer a.free(fixture_name);
        var reference = try fixtures.TensorFixture.init(a, fixture_name);
        defer reference.deinit();
        var schema = try ir.compile(a, case.schema, .{});
        defer schema.deinit();
        var prepared = try processor.prepare(a, tokenizer.tokenizer(), &.{.{ .text = case.text, .schema = &schema }}, .{});
        defer prepared.deinit();
        const ids = try reference.tensor("input.ids");
        const attention = try reference.tensor("input.attention_mask");
        try std.testing.expectEqual(@as(usize, @intCast(ids.shape[1])), prepared.sequence_length);
        try std.testing.expectEqual(ids.data.len, prepared.input_ids.len * 8);
        try std.testing.expectEqual(attention.data.len, prepared.attention_mask.len * 8);
        for (prepared.input_ids, prepared.attention_mask, 0..) |id, mask, i| {
            try std.testing.expectEqual(std.mem.readInt(i64, ids.data[i * 8 ..][0..8], .little), id);
            try std.testing.expectEqual(std.mem.readInt(i64, attention.data[i * 8 ..][0..8], .little), mask);
        }
        const Probe = struct {
            calls: usize = 0,
            fn check(raw: ?*anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw.?));
                self.calls += 1;
            }
        };
        var probe = Probe{};
        // An explicit request control forces cooperative tiled attention even
        // at these short lengths, using the real learned relative projections.
        var result = try encodeNative(&cb, a, &config, &prepared, .{ .control = .{ .ptr = &probe, .check_fn = Probe.check } });
        defer result.deinit();
        try std.testing.expect(probe.calls > config.encoder.num_hidden_layers);
        try fixtures.expectFloats(try reference.floats("encoded.text"), result.text_states, 5e-4, 5e-5);
        try fixtures.expectFloats(try reference.floats("encoded.query"), result.query_states, 5e-4, 5e-5);
        const expected_text_mask = try reference.booleans(a, "encoded.text_mask");
        defer a.free(expected_text_mask);
        const expected_query_mask = try reference.booleans(a, "encoded.query_mask");
        defer a.free(expected_query_mask);
        try std.testing.expectEqualSlices(bool, expected_text_mask, result.text_mask);
        try std.testing.expectEqualSlices(bool, expected_query_mask, result.query_mask);
    }
}
