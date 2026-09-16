// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Resident-device implementation of the shared extraction scorer interface.
//! Only final scalar tensors cross this boundary. The encoder/head owners are
//! borrowed, and each child scorer unwinds its device temporaries separately.
const std = @import("std");
const ops = @import("../ops/ops.zig");
const model = @import("../models/gliner_boundary.zig");
const scoring = @import("../pipelines/gliner_boundary_scoring.zig");
const engine = @import("gliner_boundary_engine_device.zig");
const head = @import("gliner_boundary_device.zig");
const math_mod = @import("gliner_boundary_device_math.zig");
const tasks = @import("gliner_boundary_tasks_device.zig");
const cpu_tasks = @import("gliner_boundary_tasks.zig");
const cpu_head = @import("gliner_boundary_head.zig");
const Span = @import("gliner_boundary_ops.zig").Span;
const Allocator = std.mem.Allocator;
const CT = ops.CT;

pub const Limits = struct {
    tasks: tasks.Limits = .{},
    /// Cumulative across encoder, shared head and every callback, including
    /// diagnostic downloads made through either owner before this adapter.
    max_result_download_bytes: usize = 128 * 1024 * 1024,
};
pub const Stats = struct {
    encoder: math_mod.Stats,
    head: ?math_mod.Stats,
    result_download_bytes: usize,
    proposal_download_bytes: usize,
    /// Sum of per-owner admission peaks, an upper bound on live device bytes.
    peak_device_upper_bound_bytes: usize,
};

fn mul(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b) catch error.ResourceLimitExceeded;
}
fn add(a: usize, b: usize) !usize {
    return std.math.add(usize, a, b) catch error.ResourceLimitExceeded;
}
fn id(value: usize) !i32 {
    return std.math.cast(i32, value) orelse error.InvalidBoundaryScorerRouting;
}

pub const Context = struct {
    config: *const model.Config,
    encoded: *engine.Result,
    head_result: ?*head.Result,
    storage: scoring.Storage,
    scores: ?scoring.CandidateScoreView,
    limits: Limits,

    pub fn init(allocator: Allocator, config: *const model.Config, encoded: *engine.Result, head_result: ?*head.Result, limits: Limits) !Context {
        var self = Context{ .config = config, .encoded = encoded, .head_result = head_result, .storage = try scoring.Storage.init(allocator), .scores = null, .limits = limits };
        errdefer self.storage.deinit();
        try self.check(null);
        if (head_result) |result| {
            const b = encoded.prepared.samples.len;
            const q = encoded.prepared.query_width;
            const pool = result.pool();
            if (pool.batch != b) return error.InvalidBoundaryScorerRouting;
            const allocator_ = self.storage.alloc();
            const pairs = try self.download(allocator_, result.context(), result.pair_logits, try mul(try mul(b, q), pool.capacity));
            const nulls = if (result.null_logits) |tensor| try self.download(allocator_, result.context(), tensor, try mul(b, q)) else null;
            const counts = if (result.count_log_rates) |tensor| try self.download(allocator_, result.context(), tensor, try mul(b, q)) else null;
            self.scores = .{ .batch = b, .text_length = encoded.prepared.word_width, .queries = q, .pool = pool, .pair_logits = pairs, .null_logits = nulls, .count_log_rates = counts };
        } else if (encoded.prepared.query_width > 0) return error.MissingBoundaryScores;
        return self;
    }

    pub fn deinit(self: *Context) void {
        self.storage.deinit();
        self.* = undefined;
    }
    pub fn scorer(self: *Context) scoring.Scorer {
        return .{ .context = self, .check_fn = checkCallback, .classify_fn = classify, .explicit_fn = explicit, .relations_fn = relations, .record_fn = record };
    }
    pub fn stats(self: *const Context) !Stats {
        const encoder = self.encoded.stats();
        const headed: ?math_mod.Stats = if (self.head_result) |result| result.stats() else null;
        return .{ .encoder = encoder, .head = headed, .result_download_bytes = try add(encoder.result_download_bytes, if (headed) |value| value.result_download_bytes else 0), .proposal_download_bytes = try add(encoder.proposal_download_bytes, if (headed) |value| value.proposal_download_bytes else 0), .peak_device_upper_bound_bytes = try add(encoder.peak_device_bytes, if (headed) |value| value.peak_device_bytes else 0) };
    }
    fn check(self: *const Context, control: ?scoring.Control) !void {
        try self.encoded.math.check();
        if (self.head_result) |result| try result.context().check();
        if (control) |active| try active.check();
    }
    fn get(raw: *anyopaque) *Context {
        return @ptrCast(@alignCast(raw));
    }
    fn checkCallback(raw: *anyopaque) !void {
        return get(raw).check(null);
    }
    fn sample(self: *Context, index: usize, control: ?scoring.Control) !void {
        try self.check(control);
        if (index >= self.encoded.prepared.samples.len) return error.InvalidBoundaryScorerRouting;
    }
    fn transferInto(self: *Context, math: *math_mod.Context, tensor: CT, output: []f32) !void {
        const used = (try self.stats()).result_download_bytes;
        if (try add(used, try mul(output.len, 4)) > self.limits.max_result_download_bytes) return error.ResourceLimitExceeded;
        try math.downloadInto(tensor, output, false);
    }
    fn download(self: *Context, allocator: Allocator, math: *math_mod.Context, tensor: CT, count: usize) ![]f32 {
        const used = (try self.stats()).result_download_bytes;
        if (try add(used, try mul(count, 4)) > self.limits.max_result_download_bytes) return error.ResourceLimitExceeded;
        return math.downloadTo(allocator, tensor, count, false);
    }
    fn taskLimits(self: *const Context, requested: cpu_tasks.Limits) tasks.Limits {
        var result = self.limits.tasks;
        result.max_classification_choices = @min(result.max_classification_choices, requested.max_classification_choices);
        result.max_relation_pairs = @min(result.max_relation_pairs, requested.max_relation_pairs);
        result.max_record_fields = @min(result.max_record_fields, requested.max_record_fields);
        result.max_candidates_per_field = @min(result.max_candidates_per_field, requested.max_candidates_per_field);
        result.max_record_instances = @min(result.max_record_instances, requested.max_record_instances);
        result.max_record_assignment_elements = @min(result.max_record_assignment_elements, requested.math.max_intermediate_bytes / 4);
        return result;
    }
    fn validateMath(self: *const Context, limits: cpu_head.Limits) !void {
        const prepared = self.encoded.prepared;
        if (prepared.samples.len > limits.max_batch or prepared.word_width > limits.max_text_words or prepared.query_width > limits.max_queries) return error.ResourceLimitExceeded;
    }

    fn classify(raw: *anyopaque, allocator: Allocator, limits: cpu_tasks.Limits, control: ?scoring.Control) !scoring.ClassificationScores {
        const self = get(raw);
        try self.check(control);
        const count = try mul(self.encoded.prepared.samples.len, self.encoded.prepared.classification_width);
        if (count > limits.max_classification_choices) return error.ResourceLimitExceeded;
        var storage = try scoring.Storage.init(allocator);
        errdefer storage.deinit();
        if (count == 0) return .{ .storage = storage, .logits = &.{} };
        var result = try tasks.classify(self.encoded.math, self.config, .{ .choices = count, .choice_states = self.encoded.classification_states orelse return error.InvalidBoundaryScorerRouting, .control = control }, self.taskLimits(limits));
        defer result.deinit();
        const logits = try self.download(storage.alloc(), self.encoded.math, result.logits.?, count);
        try self.check(control);
        return .{ .storage = storage, .logits = logits };
    }

    fn explicit(raw: *anyopaque, allocator: Allocator, request: scoring.ExplicitRequest, limits: cpu_head.Limits, control: ?scoring.Control) !scoring.ExplicitScores {
        const self = get(raw);
        try self.sample(request.sample_index, control);
        if (request.query_ids.len > limits.max_queries or request.capacity > limits.max_explicit_candidates or try mul(request.indices.len, 4) > limits.max_intermediate_bytes) return error.ResourceLimitExceeded;
        const headed = self.head_result orelse return error.MissingBoundaryScores;
        var result = try headed.scoreExplicitSubset(request.sample_index, request.query_ids, .{ .capacity = request.capacity, .indices = request.indices, .valid_mask = request.valid_mask });
        defer result.deinit();
        var storage = try scoring.Storage.init(allocator);
        errdefer storage.deinit();
        const output = try storage.alloc().alloc(f32, result.valid.len);
        for (result.chunks) |chunk| try self.transferInto(headed.context(), chunk.logits, output[chunk.offset..][0..chunk.elements]);
        const valid = try storage.alloc().dupe(bool, result.valid);
        try self.check(control);
        return .{ .storage = storage, .logits = output, .valid = valid };
    }

    fn gather(math: *math_mod.Context, tensor: CT, rows: usize, width: usize, indices: []const i32) !CT {
        const ids = try math.uploadIntegers(indices);
        defer math.drop(ids);
        return math.kernel(.gather_i32, &.{ rows, width, indices.len }, &.{ tensor, ids }, 0);
    }

    fn relations(raw: *anyopaque, allocator: Allocator, request: scoring.RelationRequest, limits: cpu_tasks.Limits, control: ?scoring.Control) !scoring.RelationScores {
        const self = get(raw);
        try self.sample(request.sample_index, control);
        try self.validateMath(limits.math);
        if (!self.config.head.directional_relation_states) return error.UnsupportedGlinerBoundaryConfiguration;
        if (request.query_pairs.len > limits.math.max_queries or request.pairs.len > limits.max_relation_pairs) return error.ResourceLimitExceeded;
        var storage = try scoring.Storage.init(allocator);
        errdefer storage.deinit();
        if (request.pairs.len == 0) return .{ .storage = storage, .logits = &.{}, .valid = &.{} };
        if (request.query_pairs.len == 0) return error.InvalidBoundaryScorerRouting;
        const prepared = self.encoded.prepared;
        const h = self.config.encoder.hidden_size;
        const b = request.sample_index;
        const q = prepared.query_width;
        const l = prepared.word_width;
        const headed = self.head_result orelse return error.MissingBoundaryScores;
        const math = headed.context();
        const query_ids = try storage.alloc().alloc(i32, try mul(request.query_pairs.len, 2));
        for (request.query_pairs, 0..) |pair, i| for (pair, 0..) |query, role| {
            if (query >= q) return error.InvalidBoundaryScorerRouting;
            query_ids[i * 2 + role] = try id(try add(try mul(b, q), query));
        };
        const queries = try gather(math, headed.query_states, try mul(prepared.samples.len, q), h, query_ids);
        defer math.drop(queries);
        const word_ids = try storage.alloc().alloc(i32, l);
        for (word_ids, 0..) |*value, word| value.* = try id(try add(try mul(b, l), word));
        const text = try gather(math, headed.text_states, try mul(prepared.samples.len, l), h, word_ids);
        defer math.drop(text);
        var result = try tasks.scoreRelations(math, self.config, .{ .batch = 1, .text_length = l, .relations = request.query_pairs.len, .text_states = text, .relation_query_states = queries, .pairs = request.pairs, .control = control }, self.taskLimits(limits));
        defer result.deinit();
        const logits = try self.download(storage.alloc(), math, result.logits.?, result.valid.len);
        const valid = try storage.alloc().dupe(bool, result.valid);
        try self.check(control);
        return .{ .storage = storage, .logits = logits, .valid = valid };
    }

    fn record(raw: *anyopaque, allocator: Allocator, request: scoring.RecordRequest, limits: cpu_tasks.Limits, control: ?scoring.Control) !scoring.RecordScores {
        const self = get(raw);
        try self.sample(request.sample_index, control);
        try self.validateMath(limits.math);
        if (request.fields.len == 0 or request.fields.len > limits.max_record_fields) return error.InvalidBoundaryScorerRouting;
        const headed = self.head_result orelse return error.MissingBoundaryScores;
        const candidates = headed.candidate_states orelse return error.MissingBoundaryCandidateStates;
        const scores = self.scores orelse return error.MissingBoundaryScores;
        const prepared = self.encoded.prepared;
        const b = request.sample_index;
        const q = prepared.query_width;
        const c = scores.pool.capacity;
        var storage = try scoring.Storage.init(allocator);
        errdefer storage.deinit();
        const a = storage.alloc();
        const inputs = try a.alloc(tasks.RecordField, request.fields.len);
        for (request.fields, inputs) |route, *out| {
            if (route.query_id >= q or route.pool_indices.len > limits.max_candidates_per_field) return error.InvalidBoundaryScorerRouting;
            const indices = try a.alloc(usize, route.pool_indices.len);
            const spans = try a.alloc(Span, route.pool_indices.len);
            const logits = try a.alloc(f32, route.pool_indices.len);
            for (route.pool_indices, indices, spans, logits) |candidate, *index, *span, *logit| {
                if (candidate >= c or !scores.pool.valid[b * c + candidate]) return error.InvalidBoundaryScorerRouting;
                index.* = b * c + candidate;
                span.* = scores.pool.indices[index.*];
                logit.* = scores.pair_logits[(b * q + route.query_id) * c + candidate];
            }
            out.* = .{ .query_index = b * q + route.query_id, .candidate_indices = indices, .candidate_spans = spans, .candidate_logits = logits };
        }
        const math = headed.context();
        var result = try tasks.scoreRecord(math, self.config, .{ .mode = request.mode, .fields = inputs, .anchor_field = request.anchor_field, .query_states = headed.query_states, .query_rows = prepared.samples.len * q, .candidate_states = candidates, .candidate_rows = prepared.samples.len * c, .control = control }, self.taskLimits(limits));
        defer result.deinit();
        const objects: []const f32 = if (result.object_logits) |tensor| try self.download(a, math, tensor, result.instances) else &.{};
        const assignments: []const f32 = if (result.assignment_logits) |tensor| try self.download(a, math, tensor, result.assignment_elements) else &.{};
        const fields = try a.alloc(cpu_tasks.RecordFieldResult, result.fields.len);
        for (result.fields, fields, inputs) |field, *out, input| out.* = .{ .candidate_spans = @constCast(input.candidate_spans), .candidate_logits = @constCast(input.candidate_logits), .assign_logits = @constCast(assignments[field.assignment_offset..][0 .. result.instances * field.columns]) };
        const seeds = try a.dupe(?cpu_tasks.RecordSeed, result.instance_seeds);
        try self.check(control);
        return .{ .storage = storage, .object_logits = objects, .instance_seeds = seeds, .fields = fields };
    }
};
