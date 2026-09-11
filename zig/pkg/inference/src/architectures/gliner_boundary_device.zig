// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Two-phase GLiNER2.5 device head. Learned tensors remain resident; the
//! deterministic shared-pool selector is an explicit, budgeted CPU cut.
//! This internal path does not advertise model/runtime qualification.
const std = @import("std");
const compute = @import("../ops/ops.zig");
const device = compute.gliner_boundary_device;
const boundary = @import("../models/gliner_boundary.zig");
const primitives = @import("gliner_boundary_ops.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const CT = compute.CT;

pub const Input = struct {
    batch: usize,
    text_length: usize,
    queries: usize,
    text_states: CT,
    query_states: CT,
    text_lengths: []const usize,
    query_mask: []const bool,
    precision: enum { f32, f16 } = .f32,
    execution_policy: device_math.ExecutionPolicy = .reference_v1,
    required_outputs: RequiredHeadOutputs = .{},
    control: ?Control = null,
};

pub const RequiredHeadOutputs = struct {
    /// Shared candidate features remain required independently of this branch.
    record_candidates: bool = true,
};

const device_math = @import("gliner_boundary_device_math.zig");
pub const Limits = device_math.Limits;
pub const Stats = device_math.Stats;

pub const Plan = struct {
    proposal_download_bytes: usize,
    film_chunk_elements: usize,
    boundary_elements: usize,
};

/// Geometry-only admission does not require allocating encoder tensors.
pub const Shape = struct {
    batch: usize,
    text_length: usize,
    queries: usize,
    text_lengths: []const usize,
    query_mask: []const bool,
};

fn count(values: []const usize) !usize {
    var n: usize = 1;
    for (values) |v| n = try std.math.mul(usize, n, v);
    if (n > std.math.maxInt(i32)) return error.ResourceLimitExceeded;
    return n;
}

fn bytes(elements: usize) !usize {
    return std.math.mul(usize, elements, 4);
}

fn dim(n: usize) !u32 {
    return std.math.cast(u32, n) orelse error.InvalidInputShape;
}

pub fn plan(config: *const boundary.Config, input: Input, limits: Limits) !Plan {
    if (input.execution_policy == .optimized_v2 and input.precision != .f32) return error.UnsupportedTensorType;
    return planShape(config, .{ .batch = input.batch, .text_length = input.text_length, .queries = input.queries, .text_lengths = input.text_lengths, .query_mask = input.query_mask }, limits);
}

pub fn planShape(config: *const boundary.Config, input: Shape, limits: Limits) !Plan {
    try config.head.validate();
    if (config.version != boundary.config_version or config.architecture_version != boundary.architecture_version)
        return error.UnsupportedGlinerBoundaryVersion;
    if (config.head.candidate_pool != .shared or config.head.content_soft_max_pool or
        config.head.candidate_attention_layers != 0 or config.head.query_attention_layers != 0 or
        config.head.boundary_dim / config.head.boundary_attention_heads > 32)
        return error.UnsupportedGlinerBoundaryConfiguration;
    if (input.batch == 0 or input.text_length == 0 or input.queries == 0 or config.encoder.hidden_size == 0 or limits.query_chunk == 0)
        return error.InvalidInputShape;
    if (input.batch > limits.max_batch or input.text_length > limits.max_text_words or input.queries > limits.max_queries)
        return error.ResourceLimitExceeded;
    if (input.text_lengths.len != input.batch or input.query_mask.len != try count(&.{ input.batch, input.queries }))
        return error.InvalidInputShape;
    for (input.text_lengths) |length| if (length > input.text_length) return error.InvalidInputShape;
    const n = try std.math.add(usize, input.text_length, 1);
    const boundary_elements = try count(&.{ input.batch, n, config.head.boundary_dim });
    const marginal_elements = try count(&.{ input.batch, input.queries, n });
    const download_bytes = try bytes(try std.math.mul(usize, 2, try std.math.add(usize, boundary_elements, marginal_elements)));
    if (download_bytes > limits.max_proposal_download_bytes) return error.ResourceLimitExceeded;
    const film_elements = try count(&.{ input.batch, config.head.pool_size, @min(input.queries, limits.query_chunk), config.head.pair_dim });
    if (try bytes(film_elements) > limits.max_device_bytes) return error.ResourceLimitExceeded;
    _ = try count(&.{ input.batch, input.text_length, config.encoder.hidden_size });
    _ = try count(&.{ input.batch, input.queries, config.encoder.hidden_size });
    return .{ .proposal_download_bytes = download_bytes, .film_chunk_elements = film_elements, .boundary_elements = boundary_elements };
}

const Owner = struct {
    allocator: std.mem.Allocator,
    cb: compute.ComputeBackend,
    config: boundary.Config,
    input: Input,
    limits: Limits,
    math: *device_math.Context,
    text_lengths: []usize,
    query_mask: []bool,
    pool: ?primitives.SharedPool = null,

    fn destroy(self: *Owner) void {
        if (self.pool) |*pool| pool.deinit();
        self.math.destroy();
        self.allocator.free(self.text_lengths);
        self.allocator.free(self.query_mask);
        self.allocator.destroy(self);
    }
};

pub const Marginals = struct {
    start_logits: CT,
    end_logits: CT,
    inside_logits: CT,
    /// [B,Q,L+2]: zero, L centered prefix values, detached mean.
    inside_prefix_and_mean: CT,
};

pub const Prepared = struct {
    owner: ?*Owner,
    text_states: CT,
    query_states: CT,
    lengths: CT,
    query_mask: CT,
    boundary_states: CT,
    marginals: Marginals,
    projected_starts: ?CT,
    projected_ends: ?CT,

    pub fn deinit(self: *Prepared) void {
        if (self.owner) |owner| owner.destroy();
        self.* = undefined;
    }

    pub fn stats(self: *const Prepared) Stats {
        return self.owner.?.math.stats;
    }

    /// Exactly four activation downloads. CPU selection is stable, bounded,
    /// and shared with the verified native reference implementation.
    pub fn propose(self: *Prepared) !*const primitives.SharedPool {
        const o = self.owner orelse return error.InvalidBoundaryDeviceState;
        try o.math.check();
        if (o.pool) |*pool| return pool;
        const b = o.input.batch;
        const q = o.input.queries;
        const n = o.input.text_length + 1;
        const d = o.config.head.boundary_dim;
        const start = try o.math.download(self.marginals.start_logits, try count(&.{ b, q, n }), true);
        defer o.allocator.free(start);
        const end = try o.math.download(self.marginals.end_logits, start.len, true);
        defer o.allocator.free(end);
        const projected_start = try o.math.download(self.projected_starts.?, try count(&.{ b, n, d }), true);
        defer o.allocator.free(projected_start);
        const projected_end = try o.math.download(self.projected_ends.?, projected_start.len, true);
        defer o.allocator.free(projected_end);
        o.pool = try primitives.buildSharedPool(o.allocator, .{
            .batch = b,
            .boundaries = n,
            .queries = q,
            .dim = d,
            .lengths = o.text_lengths,
            .query_mask = o.query_mask,
            .start_logits = start,
            .end_logits = end,
            .projected_starts = projected_start,
            .projected_ends = projected_end,
            .control = o.input.control orelse o.cb.execution_control,
        }, .{ .boundary_top_k = o.config.head.pool_boundary_top_k, .capacity = o.config.head.pool_size, .min_per_query = o.config.head.min_pool_per_query, .max_pair_elements = o.limits.max_pool_pair_elements });
        o.math.drop(self.projected_starts.?);
        o.math.drop(self.projected_ends.?);
        self.projected_starts = null;
        self.projected_ends = null;
        return &o.pool.?;
    }
};

pub const Result = struct {
    owner: *Owner,
    text_states: CT,
    query_states: CT,
    lengths: CT,
    boundary_states: CT,
    marginals: Marginals,
    candidate_features: CT,
    pair_logits: CT,
    candidate_states: ?CT,
    null_logits: ?CT,
    count_log_rates: ?CT,

    pub fn deinit(self: *Result) void {
        self.owner.destroy();
        self.* = undefined;
    }

    pub fn pool(self: *const Result) *const primitives.SharedPool {
        return &self.owner.pool.?;
    }

    pub fn stats(self: *const Result) Stats {
        return self.owner.math.stats;
    }

    /// An explicit final-output/debug transfer, separately charged from the
    /// proposal cut. Caller frees the returned slice with the request allocator.
    pub fn download(self: *Result, tensor: CT, elements: usize) ![]f32 {
        return self.owner.math.download(tensor, elements, false);
    }

    pub fn scoreExplicit(self: *Result, input: @import("gliner_boundary_head.zig").ExplicitInput) !ExplicitResult {
        return scoreExplicitSpans(self, input);
    }

    /// Learned task adapters borrow this request's weights, budgets and
    /// counters. They must release their child tensors before this result.
    pub fn context(self: *Result) *device_math.Context {
        return self.owner.math;
    }

    pub fn scoreExplicitSubset(self: *Result, sample: usize, queries: []const usize, input: @import("gliner_boundary_head.zig").ExplicitInput) !ExplicitResult {
        return explicitSubset(self, sample, queries, input);
    }
};

pub const ExplicitChunk = struct { offset: usize, elements: usize, logits: CT };

/// Borrowed parent lifetime: release this result before its parent Result.
/// Chunks stay resident and avoid a dense B*Q*C*P activation allocation.
pub const ExplicitResult = struct {
    owner: *Owner,
    chunks: []ExplicitChunk,
    valid: []bool,

    pub fn deinit(self: *ExplicitResult) void {
        for (self.chunks) |chunk| self.owner.math.drop(chunk.logits);
        self.owner.allocator.free(self.chunks);
        self.owner.allocator.free(self.valid);
        self.* = undefined;
    }

    pub fn download(self: *ExplicitResult) ![]f32 {
        const output = try self.owner.allocator.alloc(f32, self.valid.len);
        errdefer self.owner.allocator.free(output);
        for (self.chunks) |chunk| {
            const values = try self.owner.math.download(chunk.logits, chunk.elements, false);
            defer self.owner.allocator.free(values);
            @memcpy(output[chunk.offset..][0..chunk.elements], values);
        }
        return output;
    }
};

fn validateInputTensor(cb: *const compute.ComputeBackend, allocator: std.mem.Allocator, tensor: CT, elements: usize) !void {
    const shape = try cb.tensorShape(tensor, allocator);
    defer allocator.free(shape);
    var actual: usize = 1;
    for (shape) |d| {
        if (d <= 0) return error.InvalidInputShape;
        actual = try std.math.mul(usize, actual, @intCast(d));
    }
    if (actual != elements) return error.InvalidInputShape;
}

pub fn prepare(cb: *const compute.ComputeBackend, allocator: std.mem.Allocator, config: *const boundary.Config, input: Input, limits: Limits) !Prepared {
    if (cb.kind() != .metal or cb.vtable.glinerBoundaryDevice == null) return error.UnsupportedGlinerBoundaryDevice;
    try cb.checkExecutionControl();
    if (input.control) |control| try control.check();
    _ = try plan(config, input, limits);
    const text_count = try count(&.{ input.batch, input.text_length, config.encoder.hidden_size });
    const query_count = try count(&.{ input.batch, input.queries, config.encoder.hidden_size });
    try validateInputTensor(cb, allocator, input.text_states, text_count);
    try validateInputTensor(cb, allocator, input.query_states, query_count);
    const o = try createOwner(cb, allocator, config, input, limits);
    errdefer o.destroy();
    return prepareOwned(o, text_count, query_count);
}

fn createOwner(cb: *const compute.ComputeBackend, allocator: std.mem.Allocator, config: *const boundary.Config, input: Input, limits: Limits) !*Owner {
    const math = try device_math.Context.create(allocator, cb, limits, input.control);
    errdefer math.destroy();
    math.configure(input.execution_policy);
    const o = try allocator.create(Owner);
    errdefer allocator.destroy(o);
    const lengths = try allocator.dupe(usize, input.text_lengths);
    errdefer allocator.free(lengths);
    const mask = try allocator.dupe(bool, input.query_mask);
    o.* = .{ .allocator = allocator, .cb = cb.*, .config = config.*, .input = input, .limits = limits, .math = math, .text_lengths = lengths, .query_mask = mask };
    return o;
}

fn prepareOwned(o: *Owner, text_count: usize, query_count: usize) !Prepared {
    const input = o.input;
    const h = o.config.encoder.hidden_size;
    const d = o.config.head.boundary_dim;
    const n = input.text_length + 1;
    const rows = try count(&.{ input.batch, n });
    const text = try o.math.execute(.{ .resident_f32 = .{ .input = input.text_states, .source_precision = if (input.precision == .f16) .f16 else .f32 } }, text_count);
    const queries = try o.math.execute(.{ .resident_f32 = .{ .input = input.query_states, .source_precision = if (input.precision == .f16) .f16 else .f32 } }, query_count);
    const ints = try o.allocator.alloc(i32, input.batch);
    defer o.allocator.free(ints);
    for (o.text_lengths, ints) |v, *target| target.* = @intCast(v);
    const lengths = try o.math.uploadIntegers(ints);
    for (ints) |*v| v.* += 1;
    const boundary_lengths = try o.math.uploadIntegers(ints);
    defer o.math.drop(boundary_lengths);
    const query_mask = try o.math.uploadMask(o.query_mask);
    const valid = try o.allocator.alloc(bool, rows);
    defer o.allocator.free(valid);
    for (0..input.batch) |b| for (0..n) |pos| {
        valid[b * n + pos] = pos <= o.text_lengths[b];
    };
    const boundary_mask = try o.math.uploadMask(valid);
    defer o.math.drop(boundary_mask);
    const bos = try o.math.weight("boundary_head.boundary_encoder.bos_state", &.{h});
    const eos = try o.math.weight("boundary_head.boundary_encoder.eos_state", &.{h});
    const left = try o.math.kernel(.boundary_side, &.{ input.batch, input.text_length, h, 0 }, &.{ text, bos, eos, lengths }, 0);
    const right = try o.math.kernel(.boundary_side, &.{ input.batch, input.text_length, h, 1 }, &.{ text, bos, eos, lengths }, 0);
    const lp = try o.math.linear(left, rows, h, d, "boundary_head.boundary_encoder.left_projection");
    o.math.drop(left);
    const rp = try o.math.linear(right, rows, h, d, "boundary_head.boundary_encoder.right_projection");
    o.math.drop(right);
    const joined = try o.math.kernel(.concat, &.{ rows, d, d }, &.{ lp, rp }, 0);
    o.math.drop(lp);
    o.math.drop(rp);
    const projected = try o.math.linear(joined, rows, 2 * d, d, "boundary_head.boundary_encoder.output_projection");
    o.math.drop(joined);
    var states = try o.math.norm(projected, rows, d, "boundary_head.boundary_encoder.layer_norm");
    o.math.drop(projected);
    for (0..o.config.head.boundary_attention_layers) |layer| {
        var name: [192]u8 = undefined;
        const normalized = try o.math.norm(states, rows, d, try std.fmt.bufPrint(&name, "boundary_head.boundary_encoder.attention_blocks.{d}.norm", .{layer}));
        const qkv = try o.math.linear(normalized, rows, d, 3 * d, try std.fmt.bufPrint(&name, "boundary_head.boundary_encoder.attention_blocks.{d}.qkv_projection", .{layer}));
        o.math.drop(normalized);
        const attention = try o.math.kernel(.banded_attention, &.{ input.batch, n, o.config.head.boundary_attention_heads, d / o.config.head.boundary_attention_heads, o.config.head.boundary_attention_window }, &.{ qkv, boundary_lengths }, 0);
        o.math.drop(qkv);
        const update = try o.math.linear(attention, rows, d, d, try std.fmt.bufPrint(&name, "boundary_head.boundary_encoder.attention_blocks.{d}.output_projection", .{layer}));
        o.math.drop(attention);
        const combined = try o.math.kernel(.add, &.{try count(&.{ rows, d })}, &.{ states, update }, 0);
        o.math.drop(states);
        o.math.drop(update);
        states = try o.math.kernel(.mask_rows, &.{ rows, d }, &.{ combined, boundary_mask }, 0);
        o.math.drop(combined);
    }
    const ffn: usize = @intFromFloat(@as(f64, @floatFromInt(d)) * o.config.head.boundary_ffn_multiplier);
    for (0..o.config.head.boundary_refinement_layers) |layer| {
        var name: [192]u8 = undefined;
        const normalized = try o.math.norm(states, rows, d, try std.fmt.bufPrint(&name, "boundary_head.boundary_encoder.refinement_blocks.{d}.norm", .{layer}));
        const pair = try o.math.linear(normalized, rows, d, 2 * ffn, try std.fmt.bufPrint(&name, "boundary_head.boundary_encoder.refinement_blocks.{d}.input_projection", .{layer}));
        o.math.drop(normalized);
        const activated = try o.math.kernel(.swiglu, &.{ rows, ffn }, &.{pair}, 0);
        o.math.drop(pair);
        const update = try o.math.linear(activated, rows, ffn, d, try std.fmt.bufPrint(&name, "boundary_head.boundary_encoder.refinement_blocks.{d}.output_projection", .{layer}));
        o.math.drop(activated);
        const combined = try o.math.kernel(.add, &.{try count(&.{ rows, d })}, &.{ states, update }, 0);
        o.math.drop(states);
        o.math.drop(update);
        states = combined;
    }
    const masked = try o.math.kernel(.mask_rows, &.{ rows, d }, &.{ states, boundary_mask }, 0);
    o.math.drop(states);
    states = masked;
    const marginals = try computeMarginals(o, states, text, queries, lengths, query_mask);
    const starts = try o.math.linear(states, rows, d, d, "boundary_head.shared_pool_builder.start_projection");
    const ends = try o.math.linear(states, rows, d, d, "boundary_head.shared_pool_builder.end_projection");
    return .{ .owner = o, .text_states = text, .query_states = queries, .lengths = lengths, .query_mask = query_mask, .boundary_states = states, .marginals = marginals, .projected_starts = starts, .projected_ends = ends };
}

fn computeMarginals(o: *Owner, states: CT, text: CT, queries: CT, lengths: CT, mask: CT) !Marginals {
    const b = o.input.batch;
    const q = o.input.queries;
    const l = o.input.text_length;
    const n = l + 1;
    const d = o.config.head.boundary_dim;
    const h = o.config.encoder.hidden_size;
    var logits: [3]CT = undefined;
    const state_names = [_][]const u8{ "start_boundary_projection", "end_boundary_projection", "inside_text_projection" };
    const query_names = [_][]const u8{ "start_query_projection", "end_query_projection", "inside_query_projection" };
    for (0..3) |i| {
        const positions = if (i == 2) l else n;
        var name: [192]u8 = undefined;
        const bp = try o.math.linear(if (i == 2) text else states, b * positions, if (i == 2) h else d, d, try std.fmt.bufPrint(&name, "boundary_head.boundary_query_head.{s}", .{state_names[i]}));
        const qp = try o.math.linear(queries, b * q, h, d, try std.fmt.bufPrint(&name, "boundary_head.boundary_query_head.{s}", .{query_names[i]}));
        logits[i] = try o.math.kernel(.marginals, &.{ b, positions, q, d, @intFromBool(i != 2) }, &.{ bp, qp, lengths, mask }, 0);
        o.math.drop(bp);
        o.math.drop(qp);
    }
    const prefix = try o.math.kernel(.inside_prefix, &.{ b, l, q }, &.{ logits[2], lengths, mask }, 0);
    return .{ .start_logits = logits[0], .end_logits = logits[1], .inside_logits = logits[2], .inside_prefix_and_mean = prefix };
}

/// Consumes a prepared request only on success. The caller can always defer
/// Prepared.deinit(); after success the returned Result owns the tensors.
pub fn score(prepared: *Prepared) !Result {
    const o = prepared.owner orelse return error.InvalidBoundaryDeviceState;
    const pool = o.pool orelse return error.BoundaryDevicePoolNotPrepared;
    try o.math.check();
    const b = o.input.batch;
    const q = o.input.queries;
    const n = o.input.text_length + 1;
    const h = o.config.encoder.hidden_size;
    const d = o.config.head.boundary_dim;
    const p = o.config.head.pair_dim;
    const c = pool.capacity;
    const bc = b * c;
    const ints = try o.allocator.alloc(i32, bc * 2);
    defer o.allocator.free(ints);
    for (pool.indices, 0..) |span, i| {
        ints[2 * i] = @intCast(span.start);
        ints[2 * i + 1] = @intCast(span.end);
    }
    const spans = try o.math.uploadIntegers(ints);
    const mask = try o.math.uploadMask(pool.valid);
    const starts = try o.math.linear(prepared.boundary_states, b * n, d, p, "boundary_head.shared_pool_scorer.start_projection");
    const ends = try o.math.linear(prepared.boundary_states, b * n, d, p, "boundary_head.shared_pool_scorer.end_projection");
    const lengths = try o.math.kernel(.length_features, &.{ b, c }, &.{ spans, prepared.lengths }, 0);
    const length_projection = try o.math.linear(lengths, bc, 3, p, "boundary_head.shared_pool_scorer.length_projection");
    o.math.drop(lengths);
    const compat = try o.math.execute(.{ .upload_f32 = .{ .values = pool.compat_logits, .shape = &.{@intCast(bc)} } }, bc);
    o.math.stats.metadata_upload_bytes += try bytes(bc);
    const prior_projection = try o.math.linear(compat, bc, 1, p, "boundary_head.shared_pool_scorer.prior_projection");
    o.math.drop(compat);
    var candidates = try o.math.kernel(.shared_sum, &.{ b, n, c, p }, &.{ starts, ends, length_projection, prior_projection, spans }, 0);
    o.math.drop(starts);
    o.math.drop(ends);
    o.math.drop(length_projection);
    o.math.drop(prior_projection);
    if (o.config.head.enable_span_content) {
        const cd = o.config.head.content_dim;
        const projected = try o.math.linear(prepared.text_states, b * o.input.text_length, h, cd, "boundary_head.shared_pool_scorer.content_pooler.value_projection");
        const prefix = try o.math.kernel(.content_prefix, &.{ b, o.input.text_length, cd }, &.{ projected, prepared.lengths }, 0);
        o.math.drop(projected);
        const mean = try o.math.kernel(.range_mean, &.{ b, n, c, cd }, &.{ prefix, spans }, 0);
        o.math.drop(prefix);
        const normed = try o.math.norm(mean, bc, cd, "boundary_head.shared_pool_scorer.content_pooler.layer_norm");
        o.math.drop(mean);
        const content = try o.math.linear(normed, bc, cd, p, "boundary_head.shared_pool_scorer.content_projection");
        o.math.drop(normed);
        const combined = try o.math.kernel(.add, &.{bc * p}, &.{ candidates, content }, 0);
        o.math.drop(candidates);
        o.math.drop(content);
        candidates = combined;
    }
    const normed = try o.math.norm(candidates, bc, p, "boundary_head.shared_pool_scorer.candidate_norm");
    o.math.drop(candidates);
    const features = try o.math.kernel(.mask_rows, &.{ bc, p }, &.{ normed, mask }, 0);
    o.math.drop(normed);
    const queries = try o.math.linear(prepared.query_states, b * q, h, p, "boundary_head.shared_pool_scorer.query_projection");
    const film = try o.math.linear(queries, b * q, p, 2 * p, "boundary_head.shared_pool_scorer.film");
    var logits: ?CT = null;
    var first_query: usize = 0;
    while (first_query < q) {
        const nq = @min(q - first_query, o.limits.query_chunk);
        const conditioned = try o.math.kernel(.film, &.{ b, c, q, p, first_query, nq }, &.{ features, film }, 0);
        const hidden = try o.math.linear(conditioned, bc * nq, p, 64, "boundary_head.shared_pool_scorer.film_output.0");
        o.math.drop(conditioned);
        const activated = try o.math.kernel(.gelu, &.{bc * nq * 64}, &.{hidden}, 0);
        o.math.drop(hidden);
        const film_score = try o.math.linear(activated, bc * nq, 64, 1, "boundary_head.shared_pool_scorer.film_output.3");
        o.math.drop(activated);
        const chunk = try o.math.kernel(.shared_score, &.{ b, n, c, q, p, first_query, nq, @intFromBool(o.config.head.use_inside_evidence) }, &.{ features, queries, film_score, prepared.marginals.start_logits, prepared.marginals.end_logits, prepared.marginals.inside_prefix_and_mean, spans, mask, prepared.query_mask }, 0);
        o.math.drop(film_score);
        if (logits) |previous| {
            logits = try o.math.kernel(.concat_queries, &.{ b, first_query, nq, c }, &.{ previous, chunk }, 0);
            o.math.drop(previous);
            o.math.drop(chunk);
        } else logits = chunk;
        first_query += nq;
    }
    o.math.drop(film);
    o.math.drop(queries);
    const candidate_states = if (o.config.head.enable_records and o.input.required_outputs.record_candidates) blk: {
        const endpoints = try o.math.kernel(.endpoints, &.{ b, n, c, d }, &.{ prepared.boundary_states, spans }, 0);
        const states = try o.math.linear(endpoints, bc, 2 * d, h, "boundary_head.candidate_encoder");
        o.math.drop(endpoints);
        const masked = try o.math.kernel(.mask_rows, &.{ bc, h }, &.{ states, mask }, 0);
        o.math.drop(states);
        break :blk masked;
    } else null;
    const null_logits = if (o.config.head.enable_abstention) try o.math.linear(prepared.query_states, b * q, h, 1, "boundary_head.null_projection") else null;
    const counts = if (o.config.head.enable_count_head) try o.math.linear(prepared.query_states, b * q, h, 1, "boundary_head.count_head") else null;
    try o.math.check();
    prepared.owner = null;
    return .{ .owner = o, .text_states = prepared.text_states, .query_states = prepared.query_states, .lengths = prepared.lengths, .boundary_states = prepared.boundary_states, .marginals = prepared.marginals, .candidate_features = features, .pair_logits = logits.?, .candidate_states = candidate_states, .null_logits = null_logits, .count_log_rates = counts };
}

fn rotatedProjection(o: *Owner, values: CT, batch: usize, boundaries: usize, in_dim: usize, out_dim: usize, name: []const u8) !CT {
    const rows = try count(&.{ batch, boundaries });
    const projected = try o.math.linear(values, rows, in_dim, out_dim, name);
    defer o.math.drop(projected);
    return o.math.kernel(.rotary, &.{ batch, boundaries, out_dim }, &.{projected}, o.config.head.rotary_base);
}

fn sigmoidProjection(o: *Owner, values: CT, rows: usize, in_dim: usize, out_dim: usize, name: []const u8) !CT {
    const projected = try o.math.linear(values, rows, in_dim, out_dim, name);
    defer o.math.drop(projected);
    return o.math.kernel(.sigmoid, &.{rows * out_dim}, &.{projected}, 0);
}

fn scoreExplicitSpans(result: *Result, explicit: @import("gliner_boundary_head.zig").ExplicitInput) !ExplicitResult {
    return scoreExplicitView(.{ .owner = result.owner, .batch = result.owner.input.batch, .queries = result.owner.input.queries, .text_lengths = result.owner.text_lengths, .query_mask = result.owner.query_mask, .text_states = result.text_states, .query_states = result.query_states, .boundary_states = result.boundary_states, .lengths = result.lengths, .start_logits = result.marginals.start_logits, .end_logits = result.marginals.end_logits, .inside_prefix_and_mean = result.marginals.inside_prefix_and_mean }, explicit);
}

const ExplicitView = struct {
    owner: *Owner,
    batch: usize,
    queries: usize,
    text_lengths: []const usize,
    query_mask: []const bool,
    text_states: CT,
    query_states: CT,
    boundary_states: CT,
    lengths: CT,
    start_logits: CT,
    end_logits: CT,
    inside_prefix_and_mean: CT,
};

fn scoreExplicitView(view: ExplicitView, explicit: @import("gliner_boundary_head.zig").ExplicitInput) !ExplicitResult {
    const o = view.owner;
    try o.math.check();
    const settings = o.config.head;
    // All three immutable release artifacts and the oracle fixture use this
    // branch. Future configurations require a separately qualified formula.
    if (!settings.enable_rotary_endpoints or !settings.endpoint_difference_features or !settings.reranker_endpoint_compat or
        !settings.enable_span_content or !settings.query_conditioned_inside_weight or !settings.use_inside_evidence)
        return error.UnsupportedGlinerBoundaryConfiguration;
    const b = view.batch;
    const q = view.queries;
    const n = o.input.text_length + 1;
    const d = settings.boundary_dim;
    const p = settings.pair_dim;
    const h = o.config.encoder.hidden_size;
    const cd = settings.content_dim;
    const c = explicit.capacity;
    if (c == 0 or o.limits.explicit_chunk == 0) return error.InvalidInputShape;
    if (c > o.limits.max_explicit_candidates) return error.ResourceLimitExceeded;
    const slots = try count(&.{ b, q, c });
    if (slots > o.limits.max_explicit_elements) return error.ResourceLimitExceeded;
    if (explicit.indices.len != slots) return error.InvalidInputShape;
    if (explicit.valid_mask) |mask| if (mask.len != slots) return error.InvalidInputShape;
    const chunk_count = @min(slots, o.limits.explicit_chunk);
    // Admit the largest per-candidate activation before creating projections.
    try o.math.reserve(try bytes(try count(&.{ chunk_count, p, 2 })));
    const valid = try o.allocator.alloc(bool, slots);
    errdefer o.allocator.free(valid);
    const signed_spans = try o.allocator.alloc(i32, try count(&.{ slots, 2 }));
    defer o.allocator.free(signed_spans);
    for (explicit.indices, 0..) |span, i| {
        const qi = i / c;
        const bi = qi / q;
        valid[i] = span.start >= 0 and span.end > span.start and span.end <= @as(i64, @intCast(view.text_lengths[bi])) and
            view.query_mask[qi] and (explicit.valid_mask == null or explicit.valid_mask.?[i]);
        signed_spans[i * 2] = if (valid[i]) @intCast(span.start) else 0;
        signed_spans[i * 2 + 1] = if (valid[i]) @intCast(span.end) else 0;
    }
    const spans = try o.math.uploadIntegers(signed_spans);
    defer o.math.drop(spans);
    const mask = try o.math.uploadMask(valid);
    defer o.math.drop(mask);
    const proposer_start = try rotatedProjection(o, view.boundary_states, b, n, d, d, "boundary_head.boundary_proposer.start_pair_projection");
    defer o.math.drop(proposer_start);
    const proposer_end = try rotatedProjection(o, view.boundary_states, b, n, d, d, "boundary_head.boundary_proposer.end_key_projection");
    defer o.math.drop(proposer_end);
    const proposer_gate = try sigmoidProjection(o, view.query_states, b * q, h, d / 2, "boundary_head.boundary_proposer.start_query_projection");
    defer o.math.drop(proposer_gate);
    const scorer_start = try rotatedProjection(o, view.boundary_states, b, n, d, p, "boundary_head.pair_scorer.start_endpoint_projection");
    defer o.math.drop(scorer_start);
    const scorer_end = try rotatedProjection(o, view.boundary_states, b, n, d, p, "boundary_head.pair_scorer.end_endpoint_projection");
    defer o.math.drop(scorer_end);
    const scorer_gate = try sigmoidProjection(o, view.query_states, b * q, h, p / 2, "boundary_head.pair_scorer.query_gate");
    defer o.math.drop(scorer_gate);
    const content_values = try o.math.linear(view.text_states, b * o.input.text_length, h, cd, "boundary_head.pair_scorer.content_pooler.value_projection");
    defer o.math.drop(content_values);
    const content_prefix = try o.math.kernel(.content_prefix, &.{ b, o.input.text_length, cd }, &.{ content_values, view.lengths }, 0);
    defer o.math.drop(content_prefix);
    const content_queries = try o.math.linear(view.query_states, b * q, h, cd, "boundary_head.pair_scorer.content_query_projection");
    defer o.math.drop(content_queries);
    const inside_weights = try o.math.linear(view.query_states, b * q, h, 1, "boundary_head.pair_scorer.inside_weight");
    defer o.math.drop(inside_weights);
    const length_coeffs = try o.math.linear(view.query_states, b * q, h, 3, "boundary_head.pair_scorer.length_query_projection");
    defer o.math.drop(length_coeffs);
    var chunks: std.ArrayList(ExplicitChunk) = .empty;
    errdefer for (chunks.items) |chunk| o.math.drop(chunk.logits);
    defer chunks.deinit(o.allocator);
    var offset: usize = 0;
    while (offset < slots) {
        const size = @min(slots - offset, o.limits.explicit_chunk);
        const prior = try o.math.kernel(.explicit_prior, &.{ b, n, q, c, d, offset, size }, &.{ proposer_start, proposer_end, proposer_gate, spans, mask }, 0);
        defer o.math.drop(prior);
        const per_head = try o.math.kernel(.explicit_compat, &.{ b, n, q, c, p, offset, size, settings.multihead_pair_compat_heads }, &.{ scorer_start, scorer_end, scorer_gate, spans, mask }, 0);
        defer o.math.drop(per_head);
        const compat = try o.math.linear(per_head, size, settings.multihead_pair_compat_heads, 1, "boundary_head.pair_scorer.compat_mix");
        defer o.math.drop(compat);
        const differences = try o.math.kernel(.explicit_difference, &.{ b, n, q, c, p, offset, size }, &.{ scorer_start, scorer_end, spans }, 0);
        defer o.math.drop(differences);
        const delta = try o.math.linear(differences, size, 2 * p, 1, "boundary_head.pair_scorer.endpoint_difference_projection");
        defer o.math.drop(delta);
        const base = try o.math.kernel(.explicit_base, &.{ b, n, q, c, p, offset, size }, &.{ compat, delta, prior, view.start_logits, view.end_logits, spans }, 0);
        defer o.math.drop(base);
        const mean = try o.math.kernel(.explicit_range_mean, &.{ b, n, q, c, cd, offset, size }, &.{ content_prefix, spans }, 0);
        defer o.math.drop(mean);
        const content = try o.math.norm(mean, size, cd, "boundary_head.pair_scorer.content_pooler.layer_norm");
        defer o.math.drop(content);
        const bias = try o.math.linear(content, size, cd, 1, "boundary_head.pair_scorer.content_bias");
        defer o.math.drop(bias);
        const with_content = try o.math.kernel(.explicit_content, &.{ b, n, q, c, cd, offset, size }, &.{ base, content, content_queries, bias }, 0);
        defer o.math.drop(with_content);
        const logits = try o.math.kernel(.explicit_finish, &.{ b, n, q, c, 0, offset, size }, &.{ with_content, view.inside_prefix_and_mean, inside_weights, length_coeffs, spans, mask, view.lengths }, 0);
        errdefer o.math.drop(logits);
        try chunks.append(o.allocator, .{ .offset = offset, .elements = size, .logits = logits });
        offset += size;
    }
    return .{ .owner = o, .chunks = try chunks.toOwnedSlice(o.allocator), .valid = valid };
}

fn gatherRows(math: *device_math.Context, source: CT, source_rows: usize, width: usize, ids: []const i32) !CT {
    const metadata = try math.uploadIntegers(ids);
    defer math.drop(metadata);
    return math.kernel(.gather_i32, &.{ source_rows, width, ids.len }, &.{ source, metadata }, 0);
}

fn explicitSubset(result: *Result, sample: usize, queries: []const usize, explicit: @import("gliner_boundary_head.zig").ExplicitInput) !ExplicitResult {
    const o = result.owner;
    try o.math.check();
    if (sample >= o.input.batch or queries.len == 0 or queries.len > o.limits.max_queries) return error.InvalidBoundaryScorerRouting;
    if (explicit.capacity == 0 or explicit.indices.len != try count(&.{ queries.len, explicit.capacity })) return error.InvalidBoundaryScorerRouting;
    if (explicit.capacity > o.limits.max_explicit_candidates or explicit.indices.len > o.limits.max_explicit_elements) return error.ResourceLimitExceeded;
    if (explicit.valid_mask) |mask| if (mask.len != explicit.indices.len) return error.InvalidBoundaryScorerRouting;
    for (queries) |query| if (query >= o.input.queries) return error.InvalidBoundaryScorerRouting;
    const n = o.input.text_length + 1;
    const h = o.config.encoder.hidden_size;
    const d = o.config.head.boundary_dim;
    const ids = try o.allocator.alloc(i32, n);
    defer o.allocator.free(ids);
    for (ids, 0..) |*id, i| id.* = @intCast(sample * n + i);
    const boundary_states = try gatherRows(o.math, result.boundary_states, o.input.batch * n, d, ids);
    defer o.math.drop(boundary_states);
    for (ids[0..o.input.text_length], 0..) |*id, i| id.* = @intCast(sample * o.input.text_length + i);
    const text = try gatherRows(o.math, result.text_states, o.input.batch * o.input.text_length, h, ids[0..o.input.text_length]);
    defer o.math.drop(text);
    const query_ids = try o.allocator.alloc(i32, queries.len);
    defer o.allocator.free(query_ids);
    const mask = try o.allocator.alloc(bool, queries.len);
    defer o.allocator.free(mask);
    for (queries, query_ids, mask) |query, *id, *valid| {
        if (query >= o.input.queries) return error.InvalidBoundaryScorerRouting;
        id.* = @intCast(sample * o.input.queries + query);
        valid.* = o.query_mask[@intCast(id.*)];
    }
    const rows = o.input.batch * o.input.queries;
    const query_states = try gatherRows(o.math, result.query_states, rows, h, query_ids);
    defer o.math.drop(query_states);
    const start = try gatherRows(o.math, result.marginals.start_logits, rows, n, query_ids);
    defer o.math.drop(start);
    const end = try gatherRows(o.math, result.marginals.end_logits, rows, n, query_ids);
    defer o.math.drop(end);
    const inside = try gatherRows(o.math, result.marginals.inside_prefix_and_mean, rows, n + 1, query_ids);
    defer o.math.drop(inside);
    const lengths = try o.math.uploadIntegers(&.{@intCast(o.text_lengths[sample])});
    defer o.math.drop(lengths);
    return scoreExplicitView(.{ .owner = o, .batch = 1, .queries = queries.len, .text_lengths = o.text_lengths[sample..][0..1], .query_mask = mask, .text_states = text, .query_states = query_states, .boundary_states = boundary_states, .lengths = lengths, .start_logits = start, .end_logits = end, .inside_prefix_and_mean = inside }, explicit);
}

pub fn forwardDevice(cb: *const compute.ComputeBackend, allocator: std.mem.Allocator, config: *const boundary.Config, input: Input, limits: Limits) !Result {
    var prepared = try prepare(cb, allocator, config, input, limits);
    defer prepared.deinit();
    _ = try prepared.propose();
    var result = try score(&prepared);
    errdefer result.deinit();
    try result.context().finishSegment();
    return result;
}
