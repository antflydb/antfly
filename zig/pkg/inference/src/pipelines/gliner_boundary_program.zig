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

//! Request-owned GLiNER task-stage transport. Semantic head graphs and their
//! immutable parameters are cached by the session; only bounded score/routing
//! arrays cross into the schema decoder.
const std = @import("std");
const Session = @import("../backends/session.zig").Session;
const Tensor = @import("../backends/tensor.zig").Tensor;
const Config = @import("../models/gliner_boundary.zig").Config;
const engine = @import("../architectures/gliner/boundary_engine.zig");
const head = @import("../architectures/gliner/boundary_head.zig");
const math = @import("../architectures/gliner/boundary_ops.zig");
const tasks = @import("../architectures/gliner/boundary_tasks.zig");
const processor = @import("gliner_boundary_processor.zig");
const scoring = @import("gliner_boundary_scoring.zig");
const pipeline = @import("gliner_boundary_pipeline.zig");
const schema = @import("extraction_schema.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const A = std.mem.Allocator;

const Inputs = struct {
    a: A,
    tensors: std.ArrayList(Tensor) = .empty,
    fn floats(self: *Inputs, name: []const u8, shape: []const i64, values: []const f32) !void {
        const dimensions = try self.a.dupe(i64, shape);
        try self.tensors.append(self.a, .{ .name = name, .data = @constCast(std.mem.sliceAsBytes(values)), .shape = dimensions, .dtype = .f32, .allocator = self.a, .owns_data = false, .owns_shape = true });
    }
    fn ints(self: *Inputs, name: []const u8, shape: []const i64, values: []const i32) !void {
        try self.tensors.append(self.a, try Tensor.initInt32(self.a, name, shape, values));
    }
    fn booleans(self: *Inputs, name: []const u8, shape: []const i64, values: []const bool) !void {
        const converted = try self.a.alloc(f32, values.len);
        for (converted, values) |*value, bit| value.* = @floatFromInt(@intFromBool(bit));
        try self.floats(name, shape, converted);
    }
    fn execute(self: *Inputs, session: Session, control: ?Control) ![]Tensor {
        const outputs = try session.runWithControl(self.tensors.items, self.a, control);
        errdefer {
            for (outputs) |*output| output.deinit();
            self.a.free(outputs);
        }
        for (outputs) |output| if (output.dtype != .f32) return error.InvalidBoundaryScorerOutput;
        return outputs;
    }
};

fn tensor(outputs: []const Tensor, name: []const u8) !Tensor {
    for (outputs) |output| if (std.mem.eql(u8, output.name, name)) return output;
    return error.InvalidBoundaryScorerOutput;
}
fn outputValues(outputs: []const Tensor, name: []const u8) ![]f32 {
    const output = try tensor(outputs, name);
    return @alignCast(std.mem.bytesAsSlice(f32, output.data));
}
fn optionalValues(outputs: []const Tensor, name: []const u8) ?[]f32 {
    return outputValues(outputs, name) catch null;
}

/// Full public extraction uses this stage owner after schema preparation and
/// admission. Routing/selection retain their existing CPU reference semantics.
pub fn run(session: Session, allocator: A, config: *const Config, prepared: *const processor.PreparedBatch, schemas: []const *const schema.CompiledSchema, options: pipeline.Options, encoder_options: engine.Options) !pipeline.Result {
    return execute(false, session, allocator, allocator, config, prepared, schemas, options, encoder_options);
}

pub fn runWindows(session: Session, allocator: A, output_allocator: A, config: *const Config, prepared: *const processor.PreparedBatch, schemas: []const *const schema.CompiledSchema, options: pipeline.Options, encoder_options: engine.Options) !pipeline.WindowResult {
    return execute(true, session, allocator, output_allocator, config, prepared, schemas, options, encoder_options);
}

fn execute(comptime windows: bool, session: Session, allocator: A, output_allocator: A, config: *const Config, prepared: *const processor.PreparedBatch, schemas: []const *const schema.CompiledSchema, options: pipeline.Options, encoder_options: engine.Options) !(if (windows) pipeline.WindowResult else pipeline.Result) {
    if (session.backend() != .pjrt) return error.UnsupportedGlinerBoundaryBackend;
    const live_config = try @import("../architectures/session_factory.zig").getGlinerBoundaryConfig(session);
    if (!std.meta.eql(config.*, live_config)) return error.GlinerBoundaryArtifactMismatch;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var ids = try Tensor.initInt64(a, "input_ids", &.{ @intCast(prepared.samples.len), @intCast(prepared.sequence_length) }, prepared.input_ids);
    defer ids.deinit();
    var mask = try Tensor.initInt64(a, "attention_mask", ids.shape, prepared.attention_mask);
    defer mask.deinit();
    const encoded = try session.runWithControl(&.{ ids, mask }, a, options.control);
    defer a.free(encoded);
    defer for (encoded) |*output| output.deinit();
    const hidden = try tensor(encoded, "last_hidden_state");
    var routed = try engine.routeEncoded(a, config, prepared, hidden.asFloat32(), encoder_options);
    defer routed.deinit();
    var owner = Owner{ .session = session, .config = config, .prepared = prepared, .core = &routed, .a = a, .control = options.control };
    defer owner.deinit();
    if (prepared.query_width != 0) try owner.prepareHeads(options.head_limits);
    return if (windows)
        pipeline.runScoredWindows(output_allocator, config, prepared, schemas, owner.score_view, owner.scorer(), options, &.{})
    else
        pipeline.runScored(output_allocator, config, prepared, schemas, owner.score_view, owner.scorer(), options);
}

const Owner = struct {
    session: Session,
    config: *const Config,
    prepared: *const processor.PreparedBatch,
    core: *const engine.Result,
    a: A,
    control: ?Control,
    proposals: []Tensor = &.{},
    candidates: []Tensor = &.{},
    score_view: ?scoring.CandidateScoreView = null,
    pool: ?math.SharedPool = null,
    means: []f32 = &.{},

    fn deinit(self: *Owner) void {
        for (self.proposals) |*output| output.deinit();
        for (self.candidates) |*output| output.deinit();
    }

    fn coreInputs(self: *const Owner) !Inputs {
        var inputs = Inputs{ .a = self.a };
        const b: i64 = @intCast(self.prepared.samples.len);
        const w: i64 = @intCast(self.prepared.word_width);
        const q: i64 = @intCast(self.prepared.query_width);
        const h: i64 = self.config.encoder.hidden_size;
        try inputs.floats("head.text_states", &.{ b, w, h }, self.core.text_states);
        try inputs.floats("head.query_states", &.{ b, q, h }, self.core.query_states);
        try inputs.booleans("head.text_mask", &.{ b, w }, self.core.text_mask);
        try inputs.booleans("head.query_mask", &.{ b, q }, self.core.query_mask);
        return inputs;
    }

    fn prepareHeads(self: *Owner, limits: head.Limits) !void {
        _ = try head.planShape(self.config, .{ .batch = self.prepared.samples.len, .text_length = self.prepared.word_width, .queries = self.prepared.query_width }, limits);
        var initial = try self.coreInputs();
        self.proposals = try initial.execute(self.session, self.control);
        const b = self.prepared.samples.len;
        const w = self.prepared.word_width;
        const q = self.prepared.query_width;
        const n = w + 1;
        self.pool = try math.buildSharedPool(self.a, .{ .batch = b, .boundaries = n, .queries = q, .dim = self.config.head.boundary_dim, .lengths = self.core.text_lengths, .query_mask = self.core.query_mask, .start_logits = try outputValues(self.proposals, "start_logits"), .end_logits = try outputValues(self.proposals, "end_logits"), .projected_starts = try outputValues(self.proposals, "pool_start"), .projected_ends = try outputValues(self.proposals, "pool_end"), .control = self.control }, .{ .boundary_top_k = self.config.head.pool_boundary_top_k, .capacity = self.config.head.pool_size, .min_per_query = self.config.head.min_pool_per_query, .max_pair_elements = limits.max_pool_pair_elements });
        const pool = &self.pool.?;
        const count = pool.indices.len;
        const starts = try self.a.alloc(i32, count);
        const ends = try self.a.alloc(i32, count);
        const lengths = try self.a.alloc(f32, count);
        const features = try self.a.alloc(f32, count * 3);
        for (pool.indices, starts, ends, lengths, 0..) |span, *start, *end, *length, index| {
            const sample = index / pool.capacity;
            start.* = @intCast(sample * n + span.start);
            end.* = @intCast(sample * n + span.end);
            length.* = @floatFromInt(@max(span.end - span.start, 1));
            features[index * 3] = @log(1 + length.*);
            features[index * 3 + 1] = length.* / @as(f32, @floatFromInt(@max(self.core.text_lengths[sample], 1)));
            features[index * 3 + 2] = 1 / @sqrt(length.*);
        }
        const inside_mask = try self.a.alloc(bool, b * q * w);
        for (0..b) |sample| for (0..q) |query| @memcpy(inside_mask[(sample * q + query) * w ..][0..w], self.core.text_mask[sample * w ..][0..w]);
        self.means = try self.a.alloc(f32, b * q);
        const prefix = try self.a.alloc(f32, b * q * n);
        try math.centeredInsidePrefix(try outputValues(self.proposals, "inside_logits"), inside_mask, b * q, w, prefix, self.means);
        var selected = try self.coreInputs();
        for (self.proposals) |output| {
            if (std.mem.eql(u8, output.name, "null_logits") or std.mem.eql(u8, output.name, "count_log_rates")) continue;
            try selected.floats(try std.fmt.allocPrint(self.a, "head.{s}", .{output.name}), output.shape, output.asFloat32());
        }
        try selected.ints("head.pool.starts", &.{@intCast(count)}, starts);
        try selected.ints("head.pool.ends", &.{@intCast(count)}, ends);
        try selected.booleans("head.pool.valid", &.{ @intCast(b), @intCast(pool.capacity) }, pool.valid);
        try selected.floats("head.pool.lengths", &.{ @intCast(count), 1 }, lengths);
        try selected.floats("head.pool.length_features", &.{ @intCast(count), 3 }, features);
        if (self.config.head.use_inside_evidence) try selected.floats("head.pool.inside_mean", &.{ @intCast(b), @intCast(q), 1 }, self.means);
        self.candidates = try selected.execute(self.session, self.control);
        self.score_view = .{ .batch = b, .text_length = w, .queries = q, .pool = pool, .pair_logits = try outputValues(self.candidates, "pair_logits"), .null_logits = optionalValues(self.proposals, "null_logits"), .count_log_rates = optionalValues(self.proposals, "count_log_rates") };
    }

    fn scorer(self: *Owner) scoring.Scorer {
        return .{ .context = self, .check_fn = check, .classify_fn = classify, .explicit_fn = explicit, .relations_fn = relations, .record_fn = record };
    }
    fn get(raw: *anyopaque) *Owner {
        return @ptrCast(@alignCast(raw));
    }
    fn classify(raw: *anyopaque, a: A, limits: tasks.Limits, control: ?Control) !scoring.ClassificationScores {
        const self = get(raw);
        try check(raw);
        const b = self.prepared.samples.len;
        const c = self.prepared.classification_width;
        if (b * c > limits.max_classification_choices) return error.ResourceLimitExceeded;
        var storage = try scoring.Storage.init(a);
        errdefer storage.deinit();
        var inputs = Inputs{ .a = storage.alloc() };
        try inputs.floats("head.classification_states", &.{ @intCast(b), @intCast(c), self.config.encoder.hidden_size }, self.core.classification_states);
        const outputs = try inputs.execute(self.session, control);
        storage.outputs = outputs;
        return .{ .storage = storage, .logits = try outputValues(outputs, "classification_logits") };
    }
    fn selfOf(raw: *anyopaque) *Owner {
        return @ptrCast(@alignCast(raw));
    }
    fn check(raw: *anyopaque) !void {
        if (selfOf(raw).control) |control| try control.check();
    }
    fn sampleInputs(self: *const Owner, a: A, sample: usize, selected: ?[]const usize, width: usize) !Inputs {
        if (sample >= self.prepared.samples.len or width == 0 or width > self.prepared.word_width) return error.InvalidBoundaryScorerRouting;
        var inputs = Inputs{ .a = a };
        const h = self.config.encoder.hidden_size;
        const w = self.prepared.word_width;
        const q = self.prepared.query_width;
        const count = if (selected) |queries| queries.len else q;
        const queries = try a.alloc(f32, count * h);
        const query_mask = try a.alloc(bool, count);
        for (0..count) |i| {
            const source = if (selected) |indices| indices[i] else i;
            if (source >= q) return error.InvalidBoundaryScorerRouting;
            @memcpy(queries[i * h ..][0..h], self.core.query_states[(sample * q + source) * h ..][0..h]);
            query_mask[i] = self.core.query_mask[sample * q + source];
        }
        try inputs.floats("head.text_states", &.{ 1, @intCast(width), h }, self.core.text_states[sample * w * h ..][0 .. width * h]);
        try inputs.floats("head.query_states", &.{ 1, @intCast(count), h }, queries);
        try inputs.booleans("head.text_mask", &.{ 1, @intCast(width) }, self.core.text_mask[sample * w ..][0..width]);
        try inputs.booleans("head.query_mask", &.{ 1, @intCast(count) }, query_mask);
        return inputs;
    }
    fn explicit(raw: *anyopaque, allocator: A, request: scoring.ExplicitRequest, limits: head.Limits, control: ?Control) !scoring.ExplicitScores {
        const self = selfOf(raw);
        try check(raw);
        const q = request.query_ids.len;
        const c = request.capacity;
        const count = try std.math.mul(usize, q, c);
        if (q == 0 or c == 0 or request.indices.len != count or self.pool == null) return error.InvalidBoundaryScorerRouting;
        if (request.valid_mask) |valid| if (valid.len != count) return error.InvalidBoundaryScorerRouting;
        try request.validate(limits);
        var storage = try scoring.Storage.init(allocator);
        errdefer storage.deinit();
        const a = storage.alloc();
        const b = request.sample_index;
        const w = self.prepared.word_width;
        var inputs = try self.sampleInputs(a, b, request.query_ids, w);
        const starts = try a.alloc(i32, count);
        const ends = try a.alloc(i32, count);
        const marginal_starts = try a.alloc(i32, count);
        const marginal_ends = try a.alloc(i32, count);
        const valid = try a.alloc(bool, count);
        const lengths = try a.alloc(f32, count);
        const features = try a.alloc(f32, count * 3);
        for (request.indices, 0..) |span, i| {
            const query = request.query_ids[i / c];
            valid[i] = (if (request.valid_mask) |mask| mask[i] else true) and self.core.query_mask[b * self.prepared.query_width + query] and span.start >= 0 and span.end > span.start and span.end <= self.core.text_lengths[b];
            starts[i] = if (valid[i]) @intCast(span.start) else 0;
            ends[i] = if (valid[i]) @intCast(span.end) else 0;
            marginal_starts[i] = @intCast((i / c) * (w + 1) + @as(usize, @intCast(starts[i])));
            marginal_ends[i] = @intCast((i / c) * (w + 1) + @as(usize, @intCast(ends[i])));
            lengths[i] = @floatFromInt(@max(@as(i128, span.end) - span.start, 1));
            features[i * 3] = @log(1 + lengths[i]);
            features[i * 3 + 1] = lengths[i] / @as(f32, @floatFromInt(@max(self.core.text_lengths[b], 1)));
            features[i * 3 + 2] = 1 / @sqrt(lengths[i]);
        }
        try inputs.ints("head.explicit.starts", &.{@intCast(count)}, starts);
        try inputs.ints("head.explicit.ends", &.{@intCast(count)}, ends);
        try inputs.ints("head.explicit.marginal_starts", &.{@intCast(count)}, marginal_starts);
        try inputs.ints("head.explicit.marginal_ends", &.{@intCast(count)}, marginal_ends);
        try inputs.booleans("head.explicit.valid", &.{ 1, @intCast(q), @intCast(c) }, valid);
        try inputs.floats("head.explicit.lengths", &.{ @intCast(count), 1 }, lengths);
        try inputs.floats("head.explicit.length_features", &.{ @intCast(count), 3 }, features);
        if (self.config.head.use_inside_evidence) {
            const means = try a.alloc(f32, q);
            for (means, request.query_ids) |*mean, query| mean.* = self.means[b * self.prepared.query_width + query];
            try inputs.floats("head.explicit.inside_mean", &.{ 1, @intCast(q), 1 }, means);
        }
        const outputs = try inputs.execute(self.session, control);
        storage.outputs = outputs;
        return .{ .storage = storage, .logits = try outputValues(outputs, "explicit_logits"), .valid = valid };
    }
    fn clamp(value: i128, limit: usize) usize {
        return @intCast(@min(@max(value, 0), @as(i128, limit)));
    }
    fn relations(raw: *anyopaque, allocator: A, request: scoring.RelationRequest, limits: tasks.Limits, control: ?Control) !scoring.RelationScores {
        const self = selfOf(raw);
        try check(raw);
        const b = request.sample_index;
        if (b >= self.prepared.samples.len or request.pairs.len > limits.max_relation_pairs or request.query_pairs.len > limits.math.max_queries) return error.InvalidBoundaryScorerRouting;
        const w = self.prepared.samples[b].words.len;
        const h = self.config.encoder.hidden_size;
        const q = self.prepared.query_width;
        const r = request.query_pairs.len;
        const p = request.pairs.len;
        const dim: usize = if (self.config.head.directional_relation_states) 2 * h else h;
        var storage = try scoring.Storage.init(allocator);
        errdefer storage.deinit();
        const a = storage.alloc();
        const valid = try a.alloc(bool, p);
        if (p == 0) return .{ .storage = storage, .logits = try a.alloc(f32, 0), .valid = valid };
        if (w == 0 or r == 0) return error.InvalidBoundaryScorerRouting;
        var inputs = try self.sampleInputs(a, b, null, w);
        const queries = try a.alloc(f32, r * dim);
        for (request.query_pairs, 0..) |pair, i| {
            if (pair[0] >= q or pair[1] >= q) return error.InvalidBoundaryScorerRouting;
            const first = self.core.query_states[(b * q + pair[0]) * h ..][0..h];
            const second = self.core.query_states[(b * q + pair[1]) * h ..][0..h];
            if (self.config.head.directional_relation_states) {
                @memcpy(queries[i * dim ..][0..h], first);
                @memcpy(queries[i * dim + h ..][0..h], second);
            } else {
                for (first, second, queries[i * dim ..][0..h]) |left, right, *value| value.* = (left + right) * 0.5;
            }
        }
        try inputs.floats("head.relation.queries", &.{ 1, @intCast(r), @intCast(dim) }, queries);
        const query_indices = try a.alloc(i32, p);
        var endpoints: [4][]i32 = undefined;
        for (&endpoints) |*indices| indices.* = try a.alloc(i32, p);
        var prefix: [4][]i32 = undefined;
        for (&prefix) |*indices| indices.* = try a.alloc(i32, p);
        const head_lengths = try a.alloc(f32, p);
        const tail_lengths = try a.alloc(f32, p);
        const geometry = try a.alloc(f32, p * 2);
        for (request.pairs, 0..) |pair, i| {
            valid[i] = pair.valid and pair.batch_index == 0 and pair.relation_index >= 0 and pair.relation_index < r;
            query_indices[i] = @intCast(clamp(pair.relation_index, r - 1));
            endpoints[0][i] = @intCast(clamp(pair.head_span.start, w - 1));
            endpoints[1][i] = @intCast(clamp(@as(i128, pair.head_span.end) - 1, w - 1));
            endpoints[2][i] = @intCast(clamp(pair.tail_span.start, w - 1));
            endpoints[3][i] = @intCast(clamp(@as(i128, pair.tail_span.end) - 1, w - 1));
            prefix[0][i] = @intCast(clamp(pair.head_span.start, w));
            prefix[1][i] = @intCast(clamp(pair.head_span.end, w));
            prefix[2][i] = @intCast(clamp(pair.tail_span.start, w));
            prefix[3][i] = @intCast(clamp(pair.tail_span.end, w));
            head_lengths[i] = @floatFromInt(@max(@as(i128, pair.head_span.end) - pair.head_span.start, 1));
            tail_lengths[i] = @floatFromInt(@max(@as(i128, pair.tail_span.end) - pair.tail_span.start, 1));
            const delta: f32 = @floatFromInt(@as(i128, pair.tail_span.start) - pair.head_span.start);
            geometry[i * 2] = if (delta > 0) 1 else if (delta < 0) -1 else 0;
            geometry[i * 2 + 1] = @abs(delta) / @as(f32, @floatFromInt(w));
        }
        try inputs.ints("head.relation.query_indices", &.{@intCast(p)}, query_indices);
        const endpoint_names = [_][]const u8{ "head_first", "head_last", "tail_first", "tail_last" };
        const prefix_names = [_][]const u8{ "head_start", "head_end", "tail_start", "tail_end" };
        for (endpoints, endpoint_names) |indices, name| try inputs.ints(try std.fmt.allocPrint(a, "head.relation.{s}", .{name}), &.{@intCast(p)}, indices);
        for (prefix, prefix_names) |indices, name| try inputs.ints(try std.fmt.allocPrint(a, "head.relation.{s}", .{name}), &.{@intCast(p)}, indices);
        try inputs.floats("head.relation.head_length", &.{ @intCast(p), 1 }, head_lengths);
        try inputs.floats("head.relation.tail_length", &.{ @intCast(p), 1 }, tail_lengths);
        try inputs.floats("head.relation.geometry", &.{ @intCast(p), 2 }, geometry);
        try inputs.booleans("head.relation.valid", &.{@intCast(p)}, valid);
        const outputs = try inputs.execute(self.session, control);
        storage.outputs = outputs;
        return .{ .storage = storage, .logits = try outputValues(outputs, "relation_logits"), .valid = valid };
    }
    fn record(raw: *anyopaque, allocator: A, request: scoring.RecordRequest, limits: tasks.Limits, control: ?Control) !scoring.RecordScores {
        const self = selfOf(raw);
        try check(raw);
        try request.validate(self.config.head.record_instance_queries, limits);
        const b = request.sample_index;
        if (b >= self.prepared.samples.len) return error.InvalidBoundaryScorerRouting;
        const view = self.score_view orelse return error.MissingBoundaryScores;
        const source_states = optionalValues(self.candidates, "candidate_states") orelse return error.MissingBoundaryCandidateStates;
        const h = self.config.encoder.hidden_size;
        const q = self.prepared.query_width;
        const capacity = view.pool.capacity;
        const f = request.fields.len;
        var total: usize = 0;
        for (request.fields) |field| total = try std.math.add(usize, total, field.pool_indices.len);
        const count = @max(total, 1);
        const instances: usize = switch (request.mode) {
            .natural => request.fields[request.anchor_field.?].pool_indices.len,
            .latent => total,
            .anchorless => self.config.head.record_instance_queries,
        };
        const padded_instances = @max(count, self.config.head.record_instance_queries);
        var storage = try scoring.Storage.init(allocator);
        errdefer storage.deinit();
        const a = storage.alloc();
        const fields = try a.alloc(tasks.RecordFieldResult, f);
        const offsets = try a.alloc(usize, f);
        const states = try a.alloc(f32, count * h);
        @memset(states, 0);
        const queries = try a.alloc(f32, f * h);
        const membership = try a.alloc(f32, f * count);
        @memset(membership, 0);
        const object_source = try a.alloc(f32, count);
        @memset(object_source, 0);
        var cursor: usize = 0;
        for (request.fields, fields, offsets, 0..) |route, *field, *offset, field_index| {
            if (route.query_id >= q) return error.InvalidBoundaryScorerRouting;
            offset.* = cursor;
            @memcpy(queries[field_index * h ..][0..h], self.core.query_states[(b * q + route.query_id) * h ..][0..h]);
            field.* = .{ .candidate_spans = try a.alloc(math.Span, route.pool_indices.len), .candidate_logits = try a.alloc(f32, route.pool_indices.len), .assign_logits = try a.alloc(f32, instances * (1 + route.pool_indices.len)) };
            for (route.pool_indices, field.candidate_spans, field.candidate_logits) |candidate, *span, *logit| {
                if (candidate >= capacity or !view.pool.valid[b * capacity + candidate]) return error.InvalidBoundaryScorerRouting;
                @memcpy(states[cursor * h ..][0..h], source_states[(b * capacity + candidate) * h ..][0..h]);
                membership[field_index * count + cursor] = 1;
                span.* = view.pool.indices[b * capacity + candidate];
                logit.* = view.pair_logits[(b * q + route.query_id) * capacity + candidate];
                object_source[cursor] = logit.*;
                cursor += 1;
            }
        }
        const objects = try a.alloc(f32, instances);
        const seeds = try a.alloc(?tasks.RecordSeed, instances);
        @memset(seeds, null);
        if (instances == 0) return .{ .storage = storage, .object_logits = objects, .instance_seeds = seeds, .fields = fields };
        var inputs = try self.sampleInputs(a, b, null, self.prepared.word_width);
        const valid_candidates = try a.alloc(bool, count);
        for (valid_candidates, 0..) |*valid, index| valid.* = index < total;
        const valid_instances = try a.alloc(bool, padded_instances);
        @memset(valid_instances, false);
        const rows = try a.alloc(usize, instances);
        for (rows, 0..) |*row, index| {
            row.* = if (request.mode == .natural) offsets[request.anchor_field.?] + index else index;
            valid_instances[row.*] = true;
            if (request.mode == .natural) seeds[index] = .{ .field = request.anchor_field.?, .candidate = index };
            if (request.mode == .latent) for (offsets, request.fields, 0..) |offset, field, field_index| {
                if (index >= offset and index - offset < field.pool_indices.len) {
                    seeds[index] = .{ .field = field_index, .candidate = index - offset };
                    break;
                }
            };
        }
        try inputs.floats("head.record.candidate_states", &.{ @intCast(count), h }, states);
        try inputs.floats("head.record.field_queries", &.{ @intCast(f), h }, queries);
        try inputs.booleans("head.record.candidate_mask", &.{@intCast(count)}, valid_candidates);
        try inputs.floats("head.record.field_membership", &.{ @intCast(f), @intCast(count) }, membership);
        try inputs.booleans("head.record.instance_mask", &.{@intCast(padded_instances)}, valid_instances);
        if (request.mode == .natural) try inputs.floats("head.record.object_logits", &.{@intCast(count)}, object_source);
        try inputs.tensors.append(a, try Tensor.initInt64(a, "head.record.mode", &.{1}, &.{@backingInt(request.mode)}));
        const outputs = try inputs.execute(self.session, control);
        storage.outputs = outputs;
        const compiled_objects = try outputValues(outputs, "record_objects");
        const assignments = try outputValues(outputs, "record_assignments");
        if (compiled_objects.len != padded_instances or assignments.len != padded_instances * f * (count + 1)) return error.InvalidBoundaryScorerOutput;
        for (rows, objects, 0..) |row, *object, instance| {
            object.* = compiled_objects[row];
            for (fields, offsets, 0..) |*field, offset, field_index| {
                const source = assignments[(row * f + field_index) * (count + 1) ..][0 .. count + 1];
                const width = field.candidate_spans.len + 1;
                field.assign_logits[instance * width] = source[0];
                @memcpy(field.assign_logits[instance * width + 1 ..][0 .. width - 1], source[offset + 1 ..][0 .. width - 1]);
            }
        }
        return .{ .storage = storage, .object_logits = objects, .instance_seeds = seeds, .fields = fields };
    }
};
