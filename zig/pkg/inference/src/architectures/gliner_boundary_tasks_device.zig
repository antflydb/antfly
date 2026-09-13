// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Strict resident learned task scoring. A child owns only its own tensors;
//! immutable weights and transfer accounting remain in the request Context.
const std = @import("std");
const ops = @import("../ops/ops.zig");
const model = @import("../models/gliner_boundary.zig");
const math_mod = @import("gliner_boundary_device_math.zig");
const native = @import("gliner_boundary_tasks.zig");
const Span = @import("gliner_boundary_ops.zig").Span;
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const CT = ops.CT;

pub const Limits = struct {
    max_classification_choices: usize = 4096,
    max_relation_pairs: usize = 65536,
    max_record_fields: usize = 256,
    max_candidates_per_field: usize = 4096,
    max_record_candidates: usize = 65536,
    max_record_instances: usize = 4096,
    max_record_assignment_elements: usize = 16 * 1024 * 1024,
    max_record_attention_work: usize = 16 * 1024 * 1024,
};

fn mul(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b) catch error.ResourceLimitExceeded;
}
fn add(a: usize, b: usize) !usize {
    return std.math.add(usize, a, b) catch error.ResourceLimitExceeded;
}
fn int(value: usize) !i32 {
    return std.math.cast(i32, value) orelse error.ResourceLimitExceeded;
}

fn validate(math: *math_mod.Context, config: *const model.Config, control: ?Control) !void {
    try math.check();
    if (control) |active| try active.check();
    if (math.cb.kind() != .metal or math.cb.vtable.glinerBoundaryDevice == null) return error.UnsupportedGlinerBoundaryDevice;
    if (config.version != model.config_version or config.architecture_version != model.architecture_version) return error.UnsupportedGlinerBoundaryVersion;
    if (config.encoder.hidden_size == 0) return error.InvalidGlinerBoundaryConfig;
    try config.head.validate();
}

const Owner = struct {
    math: *math_mod.Context,
    arena: std.heap.ArenaAllocator,
    tensors: std.ArrayList(CT) = .empty,
    control: ?Control,

    fn create(math: *math_mod.Context, control: ?Control) !*Owner {
        const self = try math.allocator.create(Owner);
        self.* = .{ .math = math, .arena = std.heap.ArenaAllocator.init(math.allocator), .control = control };
        return self;
    }
    fn destroy(self: *Owner) void {
        const a = self.math.allocator;
        for (self.tensors.items) |tensor| self.math.drop(tensor);
        self.tensors.deinit(a);
        self.arena.deinit();
        a.destroy(self);
    }
    fn check(self: *const Owner) !void {
        try self.math.check();
        if (self.control) |active| try active.check();
    }
    fn track(self: *Owner, tensor: CT) !CT {
        errdefer self.math.drop(tensor);
        try self.check();
        try self.tensors.append(self.math.allocator, tensor);
        return tensor;
    }
    fn drop(self: *Owner, tensor: CT) void {
        for (self.tensors.items, 0..) |value, i| if (value == tensor) {
            _ = self.tensors.swapRemove(i);
            self.math.drop(tensor);
            return;
        };
        unreachable;
    }
    fn kernel(self: *Owner, kind: ops.gliner_boundary_device.Kind, dims: []const usize, inputs: []const CT, scalar: f32) !CT {
        try self.check();
        return self.track(try self.math.kernel(kind, dims, inputs, scalar));
    }
    fn linear(self: *Owner, input: CT, rows: usize, in_dim: usize, out_dim: usize, name: []const u8) !CT {
        try self.check();
        return self.track(try self.math.linear(input, rows, in_dim, out_dim, name));
    }
    fn integers(self: *Owner, values: []const i32) !CT {
        try self.check();
        return self.track(try self.math.uploadIntegers(values));
    }
    fn metadata(self: *Owner, values: []const f32) !CT {
        try self.check();
        for (values) |value| if (!std.math.isFinite(value)) return error.NonFiniteBoundaryScore;
        const tensor = try self.track(try self.math.execute(.{ .upload_f32 = .{ .values = values, .shape = &.{try int(values.len)} } }, values.len));
        self.math.stats.metadata_upload_bytes = try add(self.math.stats.metadata_upload_bytes, try mul(values.len, 4));
        return tensor;
    }
    fn gather(self: *Owner, input: CT, rows: usize, width: usize, indices: []const i32) !CT {
        const ids = try self.integers(indices);
        defer self.drop(ids);
        return self.kernel(.gather_i32, &.{ rows, width, indices.len }, &.{ input, ids }, 0);
    }
};

pub const ClassificationInput = struct { choices: usize, choice_states: CT, control: ?Control = null };
pub const ClassificationResult = struct {
    owner: *Owner,
    choices: usize,
    hidden: ?CT,
    logits: ?CT,
    pub fn deinit(self: *ClassificationResult) void {
        self.owner.destroy();
        self.* = undefined;
    }
    pub fn download(self: *ClassificationResult) ![]f32 {
        return if (self.logits) |tensor| self.owner.math.download(tensor, self.choices, false) else self.owner.math.allocator.alloc(f32, 0);
    }
};

pub fn classify(math: *math_mod.Context, config: *const model.Config, input: ClassificationInput, limits: Limits) !ClassificationResult {
    try validate(math, config, input.control);
    if (input.choices > limits.max_classification_choices) return error.ResourceLimitExceeded;
    const h: usize = config.encoder.hidden_size;
    const o = try Owner.create(math, input.control);
    errdefer o.destroy();
    if (input.choices == 0) return .{ .owner = o, .choices = 0, .hidden = null, .logits = null };
    const projected = try o.linear(input.choice_states, input.choices, h, 2 * h, "classifier.0");
    const hidden = try o.kernel(.relu, &.{try mul(input.choices, 2 * h)}, &.{projected}, 0);
    o.drop(projected);
    const logits = try o.linear(hidden, input.choices, 2 * h, 1, if (config.head.dropout > 0) "classifier.3" else "classifier.2");
    return .{ .owner = o, .choices = input.choices, .hidden = hidden, .logits = logits };
}

pub const RelationInput = struct {
    batch: usize,
    text_length: usize,
    relations: usize,
    text_states: CT,
    relation_query_states: CT,
    pairs: []const native.RelationPair,
    control: ?Control = null,
};
pub const RelationResult = struct {
    owner: *Owner,
    features: ?CT,
    hidden: ?CT,
    mlp_logits: ?CT,
    head_content: ?CT,
    tail_content: ?CT,
    logits: ?CT,
    valid: []bool,
    pub fn deinit(self: *RelationResult) void {
        self.owner.destroy();
        self.* = undefined;
    }
    pub fn download(self: *RelationResult) ![]f32 {
        return if (self.logits) |tensor| self.owner.math.download(tensor, self.valid.len, false) else self.owner.math.allocator.alloc(f32, 0);
    }
};

fn clamp(value: i128, maximum: usize) usize {
    return @intCast(@min(@max(value, 0), @as(i128, maximum)));
}

pub fn scoreRelations(math: *math_mod.Context, config: *const model.Config, input: RelationInput, limits: Limits) !RelationResult {
    try validate(math, config, input.control);
    if (!config.head.enable_relations) return error.UnsupportedGlinerBoundaryTask;
    const p = input.pairs.len;
    if (p > limits.max_relation_pairs or input.batch > math.limits.max_batch or input.text_length > math.limits.max_text_words or input.relations > math.limits.max_queries) return error.ResourceLimitExceeded;
    if (p > 0 and (input.batch == 0 or input.text_length == 0 or input.relations == 0)) return error.InvalidInputShape;
    const h: usize = config.encoder.hidden_size;
    const qd = if (config.head.directional_relation_states) 2 * h else h;
    const fd = 4 * h + qd + 2;
    const o = try Owner.create(math, input.control);
    errdefer o.destroy();
    const a = o.arena.allocator();
    const valid = try a.alloc(bool, p);
    if (p == 0) return .{ .owner = o, .features = null, .hidden = null, .mlp_logits = null, .head_content = null, .tail_content = null, .logits = null, .valid = valid };
    const endpoint_ids = try a.alloc(i32, try mul(p, 4));
    const relation_ids = try a.alloc(i32, p);
    const positions = try a.alloc(i32, try mul(p, 4));
    const widths = try a.alloc(f32, try mul(p, 2));
    const offsets = try a.alloc(f32, try mul(p, 2));
    const mask = try a.alloc(i32, p);
    for (input.pairs, 0..) |pair, i| {
        try o.check();
        valid[i] = pair.valid and pair.batch_index >= 0 and @as(i128, pair.batch_index) < input.batch and pair.relation_index >= 0 and @as(i128, pair.relation_index) < input.relations;
        mask[i] = @intFromBool(valid[i]);
        const b = clamp(pair.batch_index, input.batch - 1);
        const r = clamp(pair.relation_index, input.relations - 1);
        relation_ids[i] = try int(try add(try mul(b, input.relations), r));
        for ([_]@import("gliner_boundary_head.zig").SignedSpan{ pair.head_span, pair.tail_span }, 0..) |span, end| {
            endpoint_ids[i * 4 + end * 2] = try int(try add(try mul(b, input.text_length), clamp(span.start, input.text_length - 1)));
            endpoint_ids[i * 4 + end * 2 + 1] = try int(try add(try mul(b, input.text_length), clamp(@as(i128, span.end) - 1, input.text_length - 1)));
            positions[i * 4 + end * 2] = try int(try add(try mul(b, input.text_length + 1), clamp(span.start, input.text_length)));
            positions[i * 4 + end * 2 + 1] = try int(try add(try mul(b, input.text_length + 1), clamp(span.end, input.text_length)));
            widths[i * 2 + end] = @floatFromInt(@max(@as(i128, span.end) - span.start, 1));
        }
        const delta: f32 = @floatFromInt(@as(i128, pair.tail_span.start) - pair.head_span.start);
        offsets[i * 2] = if (delta > 0) 1 else if (delta < 0) -1 else 0;
        offsets[i * 2 + 1] = @abs(delta) / @as(f32, @floatFromInt(input.text_length));
    }
    const endpoint_states = try o.gather(input.text_states, try mul(input.batch, input.text_length), h, endpoint_ids);
    const query_states = try o.gather(input.relation_query_states, try mul(input.batch, input.relations), qd, relation_ids);
    const joined = try o.kernel(.concat, &.{ p, 4 * h, qd }, &.{ endpoint_states, query_states }, 0);
    o.drop(endpoint_states);
    const relative_offsets = try o.metadata(offsets);
    const features = try o.kernel(.concat, &.{ p, 4 * h + qd, 2 }, &.{ joined, relative_offsets }, 0);
    o.drop(joined);
    o.drop(relative_offsets);
    const projected = try o.linear(features, p, fd, h, "relation_scorer.mlp.0");
    const hidden = try o.kernel(.gelu, &.{try mul(p, h)}, &.{projected}, 0);
    o.drop(projected);
    const mlp_logits = try o.linear(hidden, p, h, 1, "relation_scorer.mlp.3");
    const pair_mask = try o.integers(mask);
    var head_content: ?CT = null;
    var tail_content: ?CT = null;
    const logits = if (config.head.relation_biaffine_content) blk: {
        const lengths = try a.alloc(i32, input.batch);
        @memset(lengths, try int(input.text_length));
        const lengths_ct = try o.integers(lengths);
        const prefix = try o.kernel(.content_prefix, &.{ input.batch, input.text_length, h }, &.{ input.text_states, lengths_ct }, 0);
        o.drop(lengths_ct);
        const ends = try o.gather(prefix, try mul(input.batch, input.text_length + 1), h, positions);
        o.drop(prefix);
        const denominators = try o.metadata(widths);
        const means = try o.kernel(.interval_means, &.{ p * 2, h }, &.{ ends, denominators }, 0);
        o.drop(ends);
        o.drop(denominators);
        const heads = try a.alloc(i32, p);
        const tails = try a.alloc(i32, p);
        for (heads, tails, 0..) |*hv, *tv, i| {
            hv.* = try int(i * 2);
            tv.* = try int(i * 2 + 1);
        }
        const hs = try o.gather(means, p * 2, h, heads);
        const ts = try o.gather(means, p * 2, h, tails);
        o.drop(means);
        head_content = try o.linear(hs, p, h, h, "relation_scorer.head_content_projection");
        tail_content = try o.linear(ts, p, h, h, "relation_scorer.tail_content_projection");
        o.drop(hs);
        o.drop(ts);
        const gate = try o.linear(query_states, p, qd, h, "relation_scorer.relation_content_gate");
        const contents = try o.kernel(.concat, &.{ p, h, h }, &.{ head_content.?, tail_content.? }, 0);
        const all = try o.kernel(.concat, &.{ p, 2 * h, qd }, &.{ contents, query_states }, 0);
        o.drop(contents);
        const update = try o.linear(all, p, 2 * h + qd, 1, "relation_scorer.content_linear");
        o.drop(all);
        const output = try o.kernel(.relation_gated_score, &.{ p, h }, &.{ mlp_logits, head_content.?, tail_content.?, gate, update, pair_mask }, 0);
        o.drop(gate);
        o.drop(update);
        break :blk output;
    } else try o.kernel(.mask_rows, &.{ p, 1 }, &.{ mlp_logits, pair_mask }, 0);
    o.drop(query_states);
    o.drop(pair_mask);
    return .{ .owner = o, .features = features, .hidden = hidden, .mlp_logits = mlp_logits, .head_content = head_content, .tail_content = tail_content, .logits = logits, .valid = valid };
}

pub const RecordField = struct {
    query_index: usize,
    candidate_indices: []const usize,
    candidate_spans: []const Span,
    candidate_logits: []const f32,
};
pub const RecordInput = struct {
    mode: native.RecordMode,
    fields: []const RecordField,
    anchor_field: ?usize = null,
    query_states: CT,
    query_rows: usize,
    candidate_states: ?CT,
    candidate_rows: usize,
    control: ?Control = null,
};
pub const RecordFieldResult = struct { candidate_spans: []const Span, candidate_logits: []const f32, assignment_offset: usize, columns: usize };
pub const RecordResult = struct {
    owner: *Owner,
    instances: usize,
    instance_states: ?CT,
    object_logits: ?CT,
    assignment_logits: ?CT,
    assignment_elements: usize,
    instance_seeds: []?native.RecordSeed,
    fields: []RecordFieldResult,
    pub fn deinit(self: *RecordResult) void {
        self.owner.destroy();
        self.* = undefined;
    }
    pub fn downloadObjects(self: *RecordResult) ![]f32 {
        return if (self.object_logits) |value| self.owner.math.download(value, self.instances, false) else self.owner.math.allocator.alloc(f32, 0);
    }
    pub fn downloadAssignments(self: *RecordResult) ![]f32 {
        return if (self.assignment_logits) |value| self.owner.math.download(value, self.assignment_elements, false) else self.owner.math.allocator.alloc(f32, 0);
    }
};

pub fn scoreRecord(math: *math_mod.Context, config: *const model.Config, input: RecordInput, limits: Limits) !RecordResult {
    try validate(math, config, input.control);
    if (!config.head.enable_records) return error.UnsupportedGlinerBoundaryTask;
    if (input.fields.len > limits.max_record_fields) return error.ResourceLimitExceeded;
    if (input.mode == .natural and (input.anchor_field == null or input.anchor_field.? >= input.fields.len)) return error.InvalidGlinerRecordRouting;
    const h: usize = config.encoder.hidden_size;
    const d: usize = config.head.record_dim;
    var total: usize = 0;
    for (input.fields) |field| {
        const count = field.candidate_indices.len;
        if (count > limits.max_candidates_per_field) return error.ResourceLimitExceeded;
        if (field.query_index >= input.query_rows or field.candidate_spans.len != count or field.candidate_logits.len != count) return error.InvalidGlinerRecordRouting;
        for (field.candidate_indices, field.candidate_spans, field.candidate_logits) |index, span, logit| {
            if (index >= input.candidate_rows or span.start >= span.end or span.end > math.limits.max_text_words) return error.InvalidGlinerRecordRouting;
            if (!std.math.isFinite(logit)) return error.NonFiniteBoundaryScore;
        }
        total = try add(total, count);
    }
    if (total > limits.max_record_candidates) return error.ResourceLimitExceeded;
    if (total > 0 and input.candidate_states == null) return error.InvalidGlinerRecordRouting;
    const instances = switch (input.mode) {
        .natural => input.fields[input.anchor_field.?].candidate_indices.len,
        .latent => total,
        .anchorless => config.head.record_instance_queries,
    };
    if (instances > limits.max_record_instances) return error.ResourceLimitExceeded;
    const assignments = try mul(instances, try add(total, input.fields.len));
    if (assignments > limits.max_record_assignment_elements or (input.mode == .anchorless and try mul(instances, total) > limits.max_record_attention_work)) return error.ResourceLimitExceeded;
    const o = try Owner.create(math, input.control);
    errdefer o.destroy();
    const a = o.arena.allocator();
    const fields = try a.alloc(RecordFieldResult, input.fields.len);
    const seeds = try a.alloc(?native.RecordSeed, instances);
    @memset(seeds, null);
    const offsets = try a.alloc(i32, input.fields.len + 1);
    offsets[0] = 0;
    const query_ids = try a.alloc(i32, input.fields.len);
    const candidate_ids = try a.alloc(i32, total);
    var cursor: usize = 0;
    for (input.fields, fields, 0..) |field, *out, f| {
        query_ids[f] = try int(field.query_index);
        out.* = .{ .candidate_spans = try a.dupe(Span, field.candidate_spans), .candidate_logits = try a.dupe(f32, field.candidate_logits), .assignment_offset = try mul(instances, try add(cursor, f)), .columns = field.candidate_indices.len + 1 };
        for (field.candidate_indices, 0..) |index, c| {
            candidate_ids[cursor + c] = try int(index);
            if (input.mode == .latent) seeds[cursor + c] = .{ .field = f, .candidate = c };
        }
        cursor += field.candidate_indices.len;
        offsets[f + 1] = try int(cursor);
    }
    if (instances == 0) return .{ .owner = o, .instances = 0, .instance_states = null, .object_logits = null, .assignment_logits = null, .assignment_elements = 0, .instance_seeds = seeds, .fields = fields };
    const all_candidates = if (total > 0) try o.gather(input.candidate_states.?, input.candidate_rows, h, candidate_ids) else null;
    const instance_states = switch (input.mode) {
        .natural => blk: {
            const anchor = input.anchor_field.?;
            const ids = try a.alloc(i32, instances);
            for (ids, seeds, 0..) |*id, *seed, c| {
                id.* = try int(@as(usize, @intCast(offsets[anchor])) + c);
                seed.* = .{ .field = anchor, .candidate = c };
            }
            break :blk try o.gather(all_candidates.?, total, h, ids);
        },
        .latent => all_candidates.?,
        .anchorless => blk: {
            const learned = try math.weight("record_decoder.instance_embed", &.{ @intCast(instances), @intCast(h) });
            if (total == 0) break :blk try o.track(try math.execute(.{ .resident_f32 = .{ .input = learned } }, try mul(instances, h)));
            const q = try o.linear(learned, instances, h, d, "record_decoder.q_proj");
            const k = try o.linear(all_candidates.?, total, h, d, "record_decoder.k_proj");
            const v = try o.linear(all_candidates.?, total, h, h, "record_decoder.v_proj");
            const states = try o.kernel(.record_attention, &.{ instances, total, d, h }, &.{ q, k, v, learned }, 0);
            o.drop(q);
            o.drop(k);
            o.drop(v);
            break :blk states;
        },
    };
    const objects = switch (input.mode) {
        .natural => try o.metadata(input.fields[input.anchor_field.?].candidate_logits),
        .latent => try o.linear(instance_states, instances, h, 1, "record_decoder.latent_seed_head"),
        .anchorless => try o.linear(instance_states, instances, h, 1, "record_decoder.object_head"),
    };
    const assignment_logits = if (input.fields.len > 0) blk: {
        const iq = try o.linear(instance_states, instances, h, d, "record_decoder.inst_proj");
        const field_states = try o.gather(input.query_states, input.query_rows, h, query_ids);
        const fq = try o.linear(field_states, input.fields.len, h, d, "record_decoder.field_proj");
        o.drop(field_states);
        const null_state = try math.weight("record_decoder.null_embed", &.{@intCast(d)});
        const candidates = if (all_candidates) |values| try o.linear(values, total, h, d, "record_decoder.cand_proj") else null_state;
        const field_offsets = try o.integers(offsets);
        const scores = try o.kernel(.record_assign, &.{ instances, d, input.fields.len, total }, &.{ iq, fq, candidates, null_state, field_offsets }, 0);
        o.drop(iq);
        o.drop(fq);
        o.drop(field_offsets);
        if (all_candidates != null) o.drop(candidates);
        break :blk scores;
    } else null;
    if (all_candidates) |values| if (input.mode != .latent) o.drop(values);
    return .{ .owner = o, .instances = instances, .instance_states = instance_states, .object_logits = objects, .assignment_logits = assignment_logits, .assignment_elements = assignments, .instance_seeds = seeds, .fields = fields };
}
