// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Backend-independent extraction score boundary. CPU and resident-device
//! implementations expose only proposal metadata and final scalar scores.
//! Encoder, boundary and candidate-state tensors stay behind callbacks.
const std = @import("std");
const Allocator = std.mem.Allocator;
const compute = @import("../ops/ops.zig");
const model = @import("../models/gliner_boundary.zig");
const head = @import("../architectures/gliner_boundary_head.zig");
const tasks = @import("../architectures/gliner_boundary_tasks.zig");
const ops = @import("../architectures/gliner_boundary_ops.zig");
const processor = @import("gliner_boundary_processor.zig");
pub const Control = @import("../execution_control.zig").InferenceExecutionControl;

pub const CoreView = struct {
    text_states: []const f32,
    query_states: []const f32,
    classification_states: []const f32,
    text_lengths: []const usize,
};
pub const CandidateScoreView = struct {
    batch: usize,
    text_length: usize,
    queries: usize,
    pool: *const ops.SharedPool,
    pair_logits: []const f32,
    null_logits: ?[]const f32,
    count_log_rates: ?[]const f32,

    pub fn fromNative(result: *const head.Result) CandidateScoreView {
        return .{ .batch = result.batch, .text_length = result.text_length, .queries = result.queries, .pool = &result.pool, .pair_logits = result.pair_logits, .null_logits = result.null_logits, .count_log_rates = result.count_log_rates };
    }
};
pub const Storage = struct {
    allocator: Allocator,
    arena: *std.heap.ArenaAllocator,
    pub fn init(allocator: Allocator) !Storage {
        const arena = try allocator.create(std.heap.ArenaAllocator);
        arena.* = std.heap.ArenaAllocator.init(allocator);
        return .{ .allocator = allocator, .arena = arena };
    }
    pub fn alloc(self: Storage) Allocator {
        return self.arena.allocator();
    }
    pub fn deinit(self: *Storage) void {
        self.arena.deinit();
        self.allocator.destroy(self.arena);
        self.* = undefined;
    }
};
pub const ClassificationScores = struct {
    storage: Storage,
    logits: []const f32, // [B, classification_width]
    pub fn deinit(self: *ClassificationScores) void {
        self.storage.deinit();
        self.* = undefined;
    }
};
pub const ExplicitRequest = struct {
    sample_index: usize,
    query_ids: []const usize,
    capacity: usize,
    indices: []const head.SignedSpan, // [selected_queries, capacity]
    valid_mask: ?[]const bool = null,
};
pub const ExplicitScores = struct {
    storage: Storage,
    logits: []const f32,
    valid: []const bool,
    pub fn deinit(self: *ExplicitScores) void {
        self.storage.deinit();
        self.* = undefined;
    }
};
pub const RelationRequest = struct {
    sample_index: usize,
    query_pairs: []const [2]usize,
    /// Per-sample coordinates: batch_index must be zero for a valid pair.
    /// Invalid/padded values retain the existing scorer's masked semantics.
    pairs: []const tasks.RelationPair,
};
pub const RelationScores = struct {
    storage: Storage,
    logits: []const f32,
    valid: []const bool,
    pub fn deinit(self: *RelationScores) void {
        self.storage.deinit();
        self.* = undefined;
    }
};
pub const RecordFieldRoute = struct {
    query_id: usize,
    /// Ordered indices into this sample's shared pool. Preserve duplicates:
    /// latent seed identity is a (field, candidate-position) pair.
    pool_indices: []const usize,
};
pub const RecordRequest = struct {
    sample_index: usize,
    mode: tasks.RecordMode,
    anchor_field: ?usize = null,
    fields: []const RecordFieldRoute,
};
pub const RecordScores = struct {
    storage: Storage,
    object_logits: []const f32,
    instance_seeds: []const ?tasks.RecordSeed,
    fields: []const tasks.RecordFieldResult,
    pub fn deinit(self: *RecordScores) void {
        self.storage.deinit();
        self.* = undefined;
    }
};

pub const Scorer = struct {
    context: *anyopaque,
    check_fn: *const fn (*anyopaque) anyerror!void,
    classify_fn: *const fn (*anyopaque, Allocator, tasks.Limits, ?Control) anyerror!ClassificationScores,
    explicit_fn: *const fn (*anyopaque, Allocator, ExplicitRequest, head.Limits, ?Control) anyerror!ExplicitScores,
    relations_fn: *const fn (*anyopaque, Allocator, RelationRequest, tasks.Limits, ?Control) anyerror!RelationScores,
    record_fn: *const fn (*anyopaque, Allocator, RecordRequest, tasks.Limits, ?Control) anyerror!RecordScores,
    pub fn check(self: Scorer) !void {
        try self.check_fn(self.context);
    }
    pub fn classify(self: Scorer, allocator: Allocator, limits: tasks.Limits, control: ?Control) !ClassificationScores {
        return self.classify_fn(self.context, allocator, limits, control);
    }
    pub fn explicit(self: Scorer, allocator: Allocator, request: ExplicitRequest, limits: head.Limits, control: ?Control) !ExplicitScores {
        return self.explicit_fn(self.context, allocator, request, limits, control);
    }
    pub fn relations(self: Scorer, allocator: Allocator, request: RelationRequest, limits: tasks.Limits, control: ?Control) !RelationScores {
        return self.relations_fn(self.context, allocator, request, limits, control);
    }
    pub fn record(self: Scorer, allocator: Allocator, request: RecordRequest, limits: tasks.Limits, control: ?Control) !RecordScores {
        return self.record_fn(self.context, allocator, request, limits, control);
    }
};

/// Borrowed, synchronous CPU callback owner. It and its tensor inputs must
/// outlive every callback; returned scalar results own their storage. Existing
/// eager arenas transfer directly, preserving FP32 math and cleanup behavior.
pub const NativeContext = struct {
    cb: *const compute.ComputeBackend,
    config: *const model.Config,
    prepared: *const processor.PreparedBatch,
    core: CoreView,
    scores: ?*const head.Result,

    pub fn scorer(self: *NativeContext) Scorer {
        return .{ .context = self, .check_fn = checkNative, .classify_fn = classify, .explicit_fn = explicit, .relations_fn = relationScores, .record_fn = recordScores };
    }
    fn get(raw: *anyopaque) *NativeContext {
        return @ptrCast(@alignCast(raw));
    }
    fn checkNative(raw: *anyopaque) !void {
        const self = get(raw);
        if (self.cb.kind() != .native) return error.UnsupportedGlinerBoundaryBackend;
        try self.cb.checkExecutionControl();
    }
    fn checkSample(self: *NativeContext, sample: usize, control: ?Control) !void {
        try checkNative(self);
        if (control) |value| try value.check();
        if (sample >= self.prepared.samples.len or self.core.text_lengths.len != self.prepared.samples.len) return error.InvalidBoundaryScorerRouting;
    }
    fn classify(raw: *anyopaque, allocator: Allocator, limits: tasks.Limits, control: ?Control) !ClassificationScores {
        const self = get(raw);
        try checkNative(raw);
        if (control) |value| try value.check();
        const result = try tasks.classifyNative(self.cb, allocator, self.config, .{ .choices = try std.math.mul(usize, self.prepared.samples.len, self.prepared.classification_width), .choice_states = self.core.classification_states, .control = control }, limits);
        return .{ .storage = .{ .allocator = result.storage.allocator, .arena = result.storage.arena }, .logits = result.logits };
    }
    fn explicit(raw: *anyopaque, allocator: Allocator, request: ExplicitRequest, limits: head.Limits, control: ?Control) !ExplicitScores {
        const self = get(raw);
        try self.checkSample(request.sample_index, control);
        const b = request.sample_index;
        const h: usize = self.config.encoder.hidden_size;
        const q = self.prepared.query_width;
        const l = self.prepared.word_width;
        if (request.query_ids.len == 0 or request.query_ids.len > limits.max_queries or request.capacity == 0 or request.capacity > limits.max_explicit_candidates or
            request.indices.len != try std.math.mul(usize, request.query_ids.len, request.capacity)) return error.InvalidBoundaryScorerRouting;
        if (request.valid_mask) |mask| if (mask.len != request.indices.len) return error.InvalidBoundaryScorerRouting;
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const a = arena.allocator();
        const states = try a.alloc(f32, try std.math.mul(usize, request.query_ids.len, h));
        const mask = try a.alloc(bool, request.query_ids.len);
        for (request.query_ids, mask, 0..) |query, *valid, row| {
            if (query >= q) return error.InvalidBoundaryScorerRouting;
            @memcpy(states[row * h ..][0..h], self.core.query_states[(b * q + query) * h ..][0..h]);
            valid.* = self.prepared.query_marker_mask[b * q + query];
        }
        const result = try head.scoreExplicitSpansNative(self.cb, allocator, self.config, .{ .batch = 1, .text_length = l, .queries = request.query_ids.len, .text_states = self.core.text_states[b * l * h ..][0 .. l * h], .query_states = states, .text_lengths = self.core.text_lengths[b .. b + 1], .query_mask = mask, .control = control }, .{ .capacity = request.capacity, .indices = request.indices, .valid_mask = request.valid_mask }, limits);
        return .{ .storage = .{ .allocator = result.allocator, .arena = result.arena }, .logits = result.logits, .valid = result.valid };
    }
    fn relationScores(raw: *anyopaque, allocator: Allocator, request: RelationRequest, limits: tasks.Limits, control: ?Control) !RelationScores {
        const self = get(raw);
        try self.checkSample(request.sample_index, control);
        const b = request.sample_index;
        const h: usize = self.config.encoder.hidden_size;
        const q = self.prepared.query_width;
        const l = self.prepared.word_width;
        if (request.query_pairs.len > limits.math.max_queries or request.pairs.len > limits.max_relation_pairs) return error.InvalidBoundaryScorerRouting;
        const query_dim = if (self.config.head.directional_relation_states) 2 * h else h;
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const states = try arena.allocator().alloc(f32, try std.math.mul(usize, request.query_pairs.len, query_dim));
        for (request.query_pairs, 0..) |pair, row| {
            if (pair[0] >= q or pair[1] >= q) return error.InvalidBoundaryScorerRouting;
            const hs = self.core.query_states[(b * q + pair[0]) * h ..][0..h];
            const ts = self.core.query_states[(b * q + pair[1]) * h ..][0..h];
            const output = states[row * query_dim ..][0..query_dim];
            if (self.config.head.directional_relation_states) {
                @memcpy(output[0..h], hs);
                @memcpy(output[h..], ts);
            } else for (output, hs, ts) |*value, left, right| value.* = (left + right) / 2;
        }
        const result = try tasks.scoreRelationsNative(self.cb, allocator, self.config, .{ .batch = 1, .sequence_length = l, .relations = request.query_pairs.len, .text_states = self.core.text_states[b * l * h ..][0 .. l * h], .relation_query_states = states, .pairs = request.pairs, .control = control }, limits);
        return .{ .storage = .{ .allocator = result.storage.allocator, .arena = result.storage.arena }, .logits = result.logits, .valid = result.valid };
    }
    fn recordScores(raw: *anyopaque, allocator: Allocator, request: RecordRequest, limits: tasks.Limits, control: ?Control) !RecordScores {
        const self = get(raw);
        try self.checkSample(request.sample_index, control);
        const b = request.sample_index;
        const h: usize = self.config.encoder.hidden_size;
        const q = self.prepared.query_width;
        const scores = self.scores orelse return error.MissingBoundaryScores;
        const candidate_states = scores.candidate_states orelse return error.MissingBoundaryCandidateStates;
        const capacity = scores.pool.capacity;
        if (request.fields.len == 0 or request.fields.len > limits.max_record_fields) return error.InvalidBoundaryScorerRouting;
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const a = arena.allocator();
        const fields = try a.alloc(tasks.RecordFieldInput, request.fields.len);
        for (request.fields, fields) |route, *field| {
            if (route.query_id >= q or route.pool_indices.len > limits.max_candidates_per_field) return error.InvalidBoundaryScorerRouting;
            const states = try a.alloc(f32, try std.math.mul(usize, route.pool_indices.len, h));
            const spans = try a.alloc(ops.Span, route.pool_indices.len);
            const logits = try a.alloc(f32, route.pool_indices.len);
            for (route.pool_indices, spans, logits, 0..) |candidate, *span, *logit, row| {
                if (control) |value| try value.check();
                if (candidate >= capacity or !scores.pool.valid[b * capacity + candidate]) return error.InvalidBoundaryScorerRouting;
                @memcpy(states[row * h ..][0..h], candidate_states[(b * capacity + candidate) * h ..][0..h]);
                span.* = scores.pool.indices[b * capacity + candidate];
                logit.* = scores.pair_logits[(b * q + route.query_id) * capacity + candidate];
            }
            field.* = .{ .query_state = self.core.query_states[(b * q + route.query_id) * h ..][0..h], .candidate_states = states, .candidate_spans = spans, .candidate_logits = logits };
        }
        const result = try tasks.scoreRecordNative(self.cb, allocator, self.config, .{ .mode = request.mode, .fields = fields, .anchor_field = request.anchor_field, .control = control }, limits);
        return .{ .storage = .{ .allocator = result.storage.allocator, .arena = result.storage.arena }, .object_logits = result.object_logits, .instance_seeds = result.instance_seeds, .fields = result.fields };
    }
};
