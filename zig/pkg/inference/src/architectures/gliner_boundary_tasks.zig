// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Learned GLiNER2.5 classification, relation and record scoring on CPU.
//! Routing and final selection remain explicit caller responsibilities. In
//! particular, record inference preserves (field,candidate) seed identity;
//! it must not be replaced by the different dense training representation.

const std = @import("std");
const compute = @import("../ops/ops.zig");
const boundary = @import("../models/gliner_boundary.zig");
const head = @import("gliner_boundary_head.zig");
const Span = @import("gliner_boundary_ops.zig").Span;
const Control = @import("../execution_control.zig").InferenceExecutionControl;

pub const Limits = struct {
    math: head.Limits = .{},
    max_classification_choices: usize = 4096,
    max_relation_pairs: usize = 65536,
    max_record_fields: usize = 256,
    max_candidates_per_field: usize = 4096,
    max_record_instances: usize = 4096,
};

const Storage = struct {
    allocator: std.mem.Allocator,
    arena: *std.heap.ArenaAllocator,

    fn init(allocator: std.mem.Allocator) !Storage {
        const arena = try allocator.create(std.heap.ArenaAllocator);
        arena.* = std.heap.ArenaAllocator.init(allocator);
        return .{ .allocator = allocator, .arena = arena };
    }

    fn deinit(self: *Storage) void {
        self.arena.deinit();
        self.allocator.destroy(self.arena);
        self.* = undefined;
    }

    fn math(self: Storage, cb: *const compute.ComputeBackend, limits: Limits, control: ?Control) head.NativeMath {
        return .{ .cb = cb, .allocator = self.arena.allocator(), .limits = limits.math, .control = control };
    }
};

fn validateConfig(cb: *const compute.ComputeBackend, config: *const boundary.Config) !void {
    if (cb.kind() != .native) return error.UnsupportedGlinerBoundaryBackend;
    if (config.version != boundary.config_version or config.architecture_version != boundary.architecture_version)
        return error.UnsupportedGlinerBoundaryVersion;
    if (config.encoder.hidden_size == 0) return error.InvalidInputShape;
    try config.head.validate();
    try cb.checkExecutionControl();
}

fn product(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b);
}

fn validateFloats(values: []const f32, expected: usize) !void {
    if (values.len != expected) return error.InvalidInputShape;
    for (values) |value| if (!std.math.isFinite(value)) return error.NonFiniteBoundaryScore;
}

fn signed(value: usize) !i64 {
    return std.math.cast(i64, value) orelse error.InvalidInputShape;
}

fn dot(a: []const f32, b: []const f32) f32 {
    var value: f32 = 0;
    for (a, b) |left, right| value += left * right;
    return value;
}

fn finite(value: f32) !f32 {
    if (!std.math.isFinite(value)) return error.NonFiniteBoundaryScore;
    return value;
}

pub const ClassificationInput = struct {
    choices: usize,
    choice_states: []const f32, // [choices,H], exact choice-marker states
    control: ?Control = null,
};

pub const ClassificationResult = struct {
    storage: Storage,
    hidden: []f32, // [choices,2H], after ReLU
    logits: []f32,

    pub fn deinit(self: *ClassificationResult) void {
        self.storage.deinit();
        self.* = undefined;
    }
};

/// The trained scalar choice MLP, prior to task-specific temperature/selection.
pub fn classifyNative(cb: *const compute.ComputeBackend, allocator: std.mem.Allocator, config: *const boundary.Config, input: ClassificationInput, limits: Limits) !ClassificationResult {
    try validateConfig(cb, config);
    if (input.control) |control| try control.check();
    const h: usize = config.encoder.hidden_size;
    if (input.choices > limits.max_classification_choices) return error.ResourceLimitExceeded;
    try validateFloats(input.choice_states, try product(input.choices, h));
    var storage = try Storage.init(allocator);
    errdefer storage.deinit();
    var math = storage.math(cb, limits, input.control);
    const hidden = try math.linear(input.choice_states, input.choices, h, 2 * h, "classifier.0");
    for (hidden) |*value| value.* = @max(value.*, 0);
    // create_mlp omits its Dropout module entirely when dropout == 0.
    const output_name = if (config.head.dropout > 0) "classifier.3" else "classifier.2";
    const logits = try math.linear(hidden, input.choices, 2 * h, 1, output_name);
    try math.check();
    return .{ .storage = storage, .hidden = hidden, .logits = logits };
}

pub const RelationPair = struct {
    batch_index: i64,
    relation_index: i64,
    head_span: head.SignedSpan,
    tail_span: head.SignedSpan,
    valid: bool = true,
};

pub const RelationInput = struct {
    batch: usize,
    sequence_length: usize,
    relations: usize,
    text_states: []const f32, // [B,L,H], original pooled text states
    /// [B,R,2H] when directional states are enabled; otherwise [B,R,H].
    /// Directional states concatenate the head and tail relation query vectors.
    relation_query_states: []const f32,
    pairs: []const RelationPair,
    control: ?Control = null,
};

pub const RelationResult = struct {
    storage: Storage,
    features: []f32, // [P,4H+query_dim+2], includes signed order/distance
    hidden: []f32, // [P,H], after exact GELU
    mlp_logits: []f32, // [P], before biaffine content terms
    head_content: ?[]f32, // [P,H], after learned projection
    tail_content: ?[]f32,
    logits: []f32, // [P], invalid padded pairs are zero in upstream
    valid: []bool,

    pub fn deinit(self: *RelationResult) void {
        self.storage.deinit();
        self.* = undefined;
    }
};

fn clampedIndex(value: i128, limit: usize) usize {
    return @intCast(@min(@max(value, 0), @as(i128, limit)));
}

/// Score already selected typed pairs. The bounded typed proposal generator
/// must preserve orientation; swapping head and tail changes learned features.
pub fn scoreRelationsNative(cb: *const compute.ComputeBackend, allocator: std.mem.Allocator, config: *const boundary.Config, input: RelationInput, limits: Limits) !RelationResult {
    try validateConfig(cb, config);
    if (input.control) |control| try control.check();
    if (!config.head.enable_relations) return error.UnsupportedGlinerBoundaryTask;
    const h: usize = config.encoder.hidden_size;
    const query_dim = if (config.head.directional_relation_states) 2 * h else h;
    const p = input.pairs.len;
    if (input.batch > limits.math.max_batch or input.relations > limits.math.max_queries or
        input.sequence_length > limits.math.max_text_words or p > limits.max_relation_pairs)
        return error.ResourceLimitExceeded;
    if (p > 0 and (input.batch == 0 or input.sequence_length == 0 or input.relations == 0)) return error.InvalidInputShape;
    try validateFloats(input.text_states, try product(try product(input.batch, input.sequence_length), h));
    try validateFloats(input.relation_query_states, try product(try product(input.batch, input.relations), query_dim));
    var storage = try Storage.init(allocator);
    errdefer storage.deinit();
    var math = storage.math(cb, limits, input.control);
    const feature_dim = try std.math.add(usize, try std.math.add(usize, try product(4, h), query_dim), 2);
    const features = try math.alloc(f32, try product(p, feature_dim));
    const relations = try math.alloc(f32, try product(p, query_dim));
    const valid = try math.alloc(bool, p);
    const batch_indices = try math.alloc(usize, p);
    for (input.pairs, 0..) |pair, i| {
        try math.check();
        const valid_batch = pair.batch_index >= 0 and pair.batch_index < try signed(input.batch);
        const valid_relation = pair.relation_index >= 0 and pair.relation_index < try signed(input.relations);
        valid[i] = pair.valid and valid_batch and valid_relation;
        const b = clampedIndex(pair.batch_index, input.batch - 1);
        const r = clampedIndex(pair.relation_index, input.relations - 1);
        batch_indices[i] = b;
        const positions = [4]usize{
            clampedIndex(pair.head_span.start, input.sequence_length - 1),
            clampedIndex(@as(i128, pair.head_span.end) - 1, input.sequence_length - 1),
            clampedIndex(pair.tail_span.start, input.sequence_length - 1),
            clampedIndex(@as(i128, pair.tail_span.end) - 1, input.sequence_length - 1),
        };
        const row = features[i * feature_dim ..][0..feature_dim];
        for (positions, 0..) |position, endpoint| @memcpy(row[endpoint * h ..][0..h], input.text_states[(b * input.sequence_length + position) * h ..][0..h]);
        const rel = input.relation_query_states[(b * input.relations + r) * query_dim ..][0..query_dim];
        @memcpy(row[4 * h ..][0..query_dim], rel);
        @memcpy(relations[i * query_dim ..][0..query_dim], rel);
        const delta: f32 = @floatFromInt(@as(i128, pair.tail_span.start) - pair.head_span.start);
        row[feature_dim - 2] = if (delta > 0) 1 else if (delta < 0) -1 else 0;
        row[feature_dim - 1] = @abs(delta) / @as(f32, @floatFromInt(@max(input.sequence_length, 1)));
    }
    const hidden = try math.linear(features, p, feature_dim, h, "relation_scorer.mlp.0");
    const activated = if (p > 0) try math.geluExact(hidden, h) else hidden;
    const mlp_logits = try math.linear(activated, p, h, 1, "relation_scorer.mlp.3");
    const logits = try math.alloc(f32, p);
    @memcpy(logits, mlp_logits);
    var head_content: ?[]f32 = null;
    var tail_content: ?[]f32 = null;
    if (config.head.relation_biaffine_content and p > 0) {
        const n = try std.math.add(usize, input.sequence_length, 1);
        const prefix = try math.alloc(f32, try product(try product(input.batch, n), h));
        for (0..input.batch) |b| {
            @memset(prefix[b * n * h ..][0..h], 0);
            for (0..input.sequence_length) |t| for (0..h) |col| {
                prefix[(b * n + t + 1) * h + col] = prefix[(b * n + t) * h + col] + input.text_states[(b * input.sequence_length + t) * h + col];
            };
        }
        const heads = try math.alloc(f32, try product(p, h));
        const tails = try math.alloc(f32, heads.len);
        for (input.pairs, 0..) |pair, i| {
            const b = batch_indices[i];
            for ([_]head.SignedSpan{ pair.head_span, pair.tail_span }, 0..) |span, endpoint| {
                const start = clampedIndex(span.start, input.sequence_length);
                const end = clampedIndex(span.end, input.sequence_length);
                const width: f32 = @floatFromInt(@max(@as(i128, span.end) - span.start, 1));
                const output = if (endpoint == 0) heads else tails;
                for (0..h) |col| output[i * h + col] = (prefix[(b * n + end) * h + col] - prefix[(b * n + start) * h + col]) / width;
            }
        }
        head_content = try math.linear(heads, p, h, h, "relation_scorer.head_content_projection");
        tail_content = try math.linear(tails, p, h, h, "relation_scorer.tail_content_projection");
        const gate = try math.linear(relations, p, query_dim, h, "relation_scorer.relation_content_gate");
        const joined_dim = 2 * h + query_dim;
        const joined = try math.alloc(f32, try product(p, joined_dim));
        const scale = 1.0 / @sqrt(@as(f32, @floatFromInt(h)));
        for (0..p) |i| {
            var biaffine: f32 = 0;
            for (0..h) |col| biaffine += head_content.?[i * h + col] * (1.0 / (1.0 + @exp(-gate[i * h + col]))) * tail_content.?[i * h + col];
            logits[i] += biaffine * scale;
            @memcpy(joined[i * joined_dim ..][0..h], head_content.?[i * h ..][0..h]);
            @memcpy(joined[i * joined_dim + h ..][0..h], tail_content.?[i * h ..][0..h]);
            @memcpy(joined[i * joined_dim + 2 * h ..][0..query_dim], relations[i * query_dim ..][0..query_dim]);
        }
        const linear = try math.linear(joined, p, joined_dim, 1, "relation_scorer.content_linear");
        for (logits, linear) |*value, update| value.* += update;
    }
    for (logits, valid) |*value, keep| value.* = if (keep) try finite(value.*) else 0;
    try math.check();
    return .{ .storage = storage, .features = features, .hidden = activated, .mlp_logits = mlp_logits, .head_content = head_content, .tail_content = tail_content, .logits = logits, .valid = valid };
}

pub const RecordMode = enum { natural, latent, anchorless };
pub const RecordSeed = struct { field: usize, candidate: usize };

pub const RecordFieldInput = struct {
    query_state: []const f32, // [H]
    candidate_states: []const f32, // [Cf,H], upstream retained order
    candidate_spans: []const Span, // [Cf]
    candidate_logits: []const f32, // [Cf]
};

pub const RecordInput = struct {
    mode: RecordMode,
    fields: []const RecordFieldInput,
    anchor_field: ?usize = null,
    control: ?Control = null,
};

pub const RecordFieldResult = struct {
    candidate_spans: []Span,
    candidate_logits: []f32,
    assign_logits: []f32, // [instances,1+Cf], ABSENT is column zero
};

pub const RecordResult = struct {
    storage: Storage,
    mode: RecordMode,
    anchor_field: ?usize,
    instance_states: []f32, // [I,H], before inst_proj
    object_logits: []f32,
    instance_seeds: []?RecordSeed,
    fields: []RecordFieldResult,

    pub fn deinit(self: *RecordResult) void {
        self.storage.deinit();
        self.* = undefined;
    }
};

/// Inference-exact RecordHead.forward_group on one prepared sample/group.
/// A latent instance is a (field,candidate) seed, including repeated spans.
pub fn scoreRecordNative(cb: *const compute.ComputeBackend, allocator: std.mem.Allocator, config: *const boundary.Config, input: RecordInput, limits: Limits) !RecordResult {
    try validateConfig(cb, config);
    if (input.control) |control| try control.check();
    if (!config.head.enable_records) return error.UnsupportedGlinerBoundaryTask;
    const h: usize = config.encoder.hidden_size;
    const d: usize = config.head.record_dim;
    if (input.fields.len > limits.max_record_fields) return error.ResourceLimitExceeded;
    if (input.mode == .natural and (input.anchor_field == null or input.anchor_field.? >= input.fields.len)) return error.InvalidGlinerRecordRouting;
    var total_candidates: usize = 0;
    for (input.fields) |field| {
        const count = field.candidate_spans.len;
        if (count > limits.max_candidates_per_field) return error.ResourceLimitExceeded;
        try validateFloats(field.query_state, h);
        try validateFloats(field.candidate_states, try product(count, h));
        try validateFloats(field.candidate_logits, count);
        for (field.candidate_spans) |span| if (span.start >= span.end or span.end > limits.math.max_text_words) return error.InvalidGlinerRecordRouting;
        total_candidates = try std.math.add(usize, total_candidates, count);
    }
    const instances = switch (input.mode) {
        .natural => input.fields[input.anchor_field.?].candidate_spans.len,
        .latent => total_candidates,
        .anchorless => config.head.record_instance_queries,
    };
    if (instances > limits.max_record_instances) return error.ResourceLimitExceeded;
    var storage = try Storage.init(allocator);
    errdefer storage.deinit();
    var math = storage.math(cb, limits, input.control);
    try math.check();
    const seeds = try math.alloc(?RecordSeed, instances);
    @memset(seeds, null);
    const all_candidates = try math.alloc(f32, try product(total_candidates, h));
    var cursor: usize = 0;
    for (input.fields) |field| {
        @memcpy(all_candidates[cursor..][0..field.candidate_states.len], field.candidate_states);
        cursor += field.candidate_states.len;
    }
    const instance_states = switch (input.mode) {
        .natural => blk: {
            const values = try math.alloc(f32, try product(instances, h));
            @memcpy(values, input.fields[input.anchor_field.?].candidate_states);
            for (seeds, 0..) |*seed, i| seed.* = .{ .field = input.anchor_field.?, .candidate = i };
            break :blk values;
        },
        .latent => blk: {
            var offset: usize = 0;
            for (input.fields, 0..) |field, f| for (0..field.candidate_spans.len) |c| {
                seeds[offset] = .{ .field = f, .candidate = c };
                offset += 1;
            };
            break :blk all_candidates;
        },
        .anchorless => blk: {
            const learned = try math.weight("record_decoder.instance_embed", &.{ try signed(instances), try signed(h) });
            defer cb.free(learned);
            const values = try math.host(learned, try product(instances, h));
            if (total_candidates == 0) break :blk values;
            const q = try math.linear(values, instances, h, d, "record_decoder.q_proj");
            const k = try math.linear(all_candidates, total_candidates, h, d, "record_decoder.k_proj");
            const v = try math.linear(all_candidates, total_candidates, h, h, "record_decoder.v_proj");
            const weights = try math.alloc(f32, total_candidates);
            const scale = 1.0 / @sqrt(@as(f32, @floatFromInt(d)));
            for (0..instances) |i| {
                try math.check();
                var maximum: f32 = -std.math.inf(f32);
                for (0..total_candidates) |c| {
                    weights[c] = try finite(dot(q[i * d ..][0..d], k[c * d ..][0..d]) * scale);
                    maximum = @max(maximum, weights[c]);
                }
                var denominator: f32 = 0;
                for (weights) |*value| {
                    value.* = @exp(value.* - maximum);
                    denominator += value.*;
                }
                for (0..h) |col| {
                    var update: f32 = 0;
                    for (0..total_candidates) |c| update += (weights[c] / denominator) * v[c * h + col];
                    values[i * h + col] += update;
                }
            }
            break :blk values;
        },
    };
    const objects = switch (input.mode) {
        .natural => blk: {
            const values = try math.alloc(f32, instances);
            @memcpy(values, input.fields[input.anchor_field.?].candidate_logits);
            break :blk values;
        },
        .latent => try math.linear(instance_states, instances, h, 1, "record_decoder.latent_seed_head"),
        .anchorless => try math.linear(instance_states, instances, h, 1, "record_decoder.object_head"),
    };
    const instance_query = try math.linear(instance_states, instances, h, d, "record_decoder.inst_proj");
    const field_states = try math.alloc(f32, try product(input.fields.len, h));
    for (input.fields, 0..) |field, f| @memcpy(field_states[f * h ..][0..h], field.query_state);
    const field_queries = try math.linear(field_states, input.fields.len, h, d, "record_decoder.field_proj");
    const null_embed = try math.vector("record_decoder.null_embed", d);
    const fields = try math.alloc(RecordFieldResult, input.fields.len);
    const query = try math.alloc(f32, d);
    for (input.fields, 0..) |field, f| {
        try math.check();
        const cf = field.candidate_spans.len;
        const candidates = try math.linear(field.candidate_states, cf, h, d, "record_decoder.cand_proj");
        const span_copy = try math.alloc(Span, cf);
        @memcpy(span_copy, field.candidate_spans);
        const logit_copy = try math.alloc(f32, cf);
        @memcpy(logit_copy, field.candidate_logits);
        const columns = try std.math.add(usize, cf, 1);
        const logits = try math.alloc(f32, try product(instances, columns));
        for (0..instances) |i| {
            for (0..d) |col| query[col] = instance_query[i * d + col] + field_queries[f * d + col];
            logits[i * columns] = try finite(dot(query, null_embed));
            for (0..cf) |c| logits[i * columns + c + 1] = try finite(dot(query, candidates[c * d ..][0..d]));
        }
        fields[f] = .{ .candidate_spans = span_copy, .candidate_logits = logit_copy, .assign_logits = logits };
    }
    try math.check();
    return .{ .storage = storage, .mode = input.mode, .anchor_field = input.anchor_field, .instance_states = instance_states, .object_logits = objects, .instance_seeds = seeds, .fields = fields };
}

test "gliner boundary task scoring validates bounds before acquiring weights" {
    const a = std.testing.allocator;
    const native = @import("../ops/native_compute.zig");
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var backend = native.NativeCompute.init(a, &store, null);
    defer backend.deinit();
    const cb = backend.computeBackend();
    const config = boundary.Config{
        .version = boundary.config_version,
        .architecture_version = boundary.architecture_version,
        .max_len = 32,
        .backbone = .small,
        .head = .{},
        .encoder = .{
            .hidden_size = 4,
            .intermediate_size = 16,
            .num_hidden_layers = 1,
            .num_attention_heads = 1,
            .vocab_size = 4,
            .max_position_embeddings = 32,
            .position_buckets = 16,
            .layer_norm_eps = 1e-7,
            .hidden_dropout_prob = 0,
            .attention_probs_dropout_prob = 0,
            .pad_token_id = 0,
        },
    };
    const states = [_]f32{ 0, 1, 2, 3, 4, 5, 6, 7 };
    try std.testing.expectError(error.ResourceLimitExceeded, classifyNative(&cb, a, &config, .{
        .choices = 1,
        .choice_states = states[0..4],
    }, .{ .math = .{ .max_intermediate_bytes = 1 } }));
    try std.testing.expectError(error.InvalidInputShape, classifyNative(&cb, a, &config, .{
        .choices = 2,
        .choice_states = states[0..4],
    }, .{}));
    try std.testing.expectError(error.NonFiniteBoundaryScore, classifyNative(&cb, a, &config, .{
        .choices = 1,
        .choice_states = &.{ 0, 1, std.math.nan(f32), 2 },
    }, .{}));
    const relation = RelationInput{
        .batch = 1,
        .sequence_length = 2,
        .relations = 1,
        .text_states = &states,
        .relation_query_states = states[0..4],
        .pairs = &.{.{ .batch_index = 0, .relation_index = 0, .head_span = .{ .start = 0, .end = 1 }, .tail_span = .{ .start = 1, .end = 2 } }},
    };
    try std.testing.expectError(error.ResourceLimitExceeded, scoreRelationsNative(&cb, a, &config, relation, .{ .max_relation_pairs = 0 }));
    try std.testing.expectError(error.ResourceLimitExceeded, scoreRelationsNative(&cb, a, &config, relation, .{ .math = .{ .max_intermediate_bytes = 1 } }));
    const fields = [_]RecordFieldInput{.{
        .query_state = states[0..4],
        .candidate_states = &states,
        .candidate_spans = &.{ .{ .start = 0, .end = 1 }, .{ .start = 1, .end = 2 } },
        .candidate_logits = &.{ 1, 2 },
    }};
    try std.testing.expectError(error.InvalidGlinerRecordRouting, scoreRecordNative(&cb, a, &config, .{ .mode = .natural, .fields = &fields }, .{}));
    try std.testing.expectError(error.ResourceLimitExceeded, scoreRecordNative(&cb, a, &config, .{ .mode = .latent, .fields = &fields }, .{ .max_record_instances = 1 }));
    try std.testing.expectError(error.ResourceLimitExceeded, scoreRecordNative(&cb, a, &config, .{ .mode = .natural, .fields = &fields, .anchor_field = 0 }, .{ .math = .{ .max_intermediate_bytes = 1 } }));
}
