// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! FP32 CPU reference execution for the published GLiNER2.5 boundary head.
//! Inputs are already pooled text/query states, with prefix-valid text lengths.
//! This internal parity path is deliberately separate from runtime admission:
//! device-resident execution and the task decoders require their own gates.

const std = @import("std");
const compute = @import("../ops/ops.zig");
const boundary = @import("../models/gliner_boundary.zig");
const primitives = @import("gliner_boundary_ops.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;

pub const Input = struct {
    batch: usize,
    text_length: usize,
    queries: usize,
    text_states: []const f32, // [B,L,H]
    query_states: []const f32, // [B,Q,H]
    text_lengths: []const usize, // [B], valid text is a contiguous prefix
    query_mask: []const bool, // [B,Q]
    control: ?Control = null,
};

pub const Limits = struct {
    max_batch: usize = 8,
    /// Total routed words, including synthetic enum choices. The engine
    /// independently applies config.max_len to body words before that prefix.
    max_text_words: usize = 8192,
    max_queries: usize = 256,
    max_explicit_candidates: usize = 4096,
    /// Bounds retained host intermediates. Caller admission must independently
    /// reserve backend compute workspace and model-weight residency.
    max_intermediate_bytes: usize = 256 * 1024 * 1024,
    max_pool_pair_elements: usize = 1024 * 1024,
};

pub const Marginals = struct {
    start_logits: []f32, // [B,Q,L+1]
    end_logits: []f32,
    inside_logits: []f32, // [B,Q,L]
    inside_prefix: []f32, // [B,Q,L+1]
    inside_mean: []f32, // [B,Q]
};

pub const Result = struct {
    allocator: std.mem.Allocator,
    arena: *std.heap.ArenaAllocator,
    batch: usize,
    text_length: usize,
    queries: usize,
    boundary_dim: usize,
    pair_dim: usize,
    boundary_states: []f32, // [B,L+1,D]
    marginals: Marginals,
    pool: primitives.SharedPool,
    candidate_features: []f32, // [B,C,P], shared scorer features
    pair_logits: []f32, // [B,Q,C], public upstream order
    candidate_states: ?[]f32, // [B,C,H], record decoder input
    null_logits: ?[]f32, // [B,Q], unmasked as in upstream
    count_log_rates: ?[]f32, // [B,Q], unmasked as in upstream

    pub fn deinit(self: *Result) void {
        self.arena.deinit();
        self.allocator.destroy(self.arena);
        self.* = undefined;
    }
};

pub const SignedSpan = struct { start: i64, end: i64 };

pub const ExplicitInput = struct {
    capacity: usize,
    indices: []const SignedSpan, // [B,Q,C]
    valid_mask: ?[]const bool = null,
};

pub const ExplicitResult = struct {
    allocator: std.mem.Allocator,
    arena: *std.heap.ArenaAllocator,
    logits: []f32, // [B,Q,C]
    valid: []bool,

    pub fn deinit(self: *ExplicitResult) void {
        self.arena.deinit();
        self.allocator.destroy(self.arena);
        self.* = undefined;
    }
};

/// Checked FP32 CPU tensor operations shared by boundary and task scorers.
pub const NativeMath = struct {
    cb: *const compute.ComputeBackend,
    allocator: std.mem.Allocator,
    control: ?Control = null,
    limits: Limits,
    allocated_bytes: usize = 0,

    pub fn check(self: *const NativeMath) !void {
        try self.cb.checkExecutionControl();
        if (self.control) |control| try control.check();
    }

    pub fn charge(self: *NativeMath, comptime T: type, count: usize) !void {
        try self.preflight(T, count);
        const bytes = try std.math.mul(usize, @sizeOf(T), count);
        self.allocated_bytes = try std.math.add(usize, self.allocated_bytes, bytes);
    }

    fn preflight(self: *const NativeMath, comptime T: type, count: usize) !void {
        const bytes = try std.math.mul(usize, @sizeOf(T), count);
        const next = try std.math.add(usize, self.allocated_bytes, bytes);
        if (next > self.limits.max_intermediate_bytes) return error.ResourceLimitExceeded;
    }

    pub fn alloc(self: *NativeMath, comptime T: type, count: usize) ![]T {
        try self.charge(T, count);
        return self.allocator.alloc(T, count);
    }

    pub fn weight(self: *NativeMath, name: []const u8, shape: []const i64) !compute.CT {
        const tensor = try self.cb.getWeight(name);
        errdefer self.cb.free(tensor);
        const actual = try self.cb.tensorShape(tensor, self.allocator);
        defer self.allocator.free(actual);
        if (!std.mem.eql(i64, actual, shape)) return error.InvalidGlinerBoundaryWeightShape;
        if (try self.cb.tensorDType(tensor) != .f32) return error.UnsupportedGlinerBoundaryHeadPrecision;
        return tensor;
    }

    pub fn host(self: *NativeMath, tensor: compute.CT, count: usize) ![]f32 {
        try self.charge(f32, count);
        const values = try self.cb.toFloat32(tensor, self.allocator);
        errdefer self.allocator.free(values);
        if (values.len != count) return error.InvalidGlinerBoundaryWeightShape;
        for (values) |value| if (!std.math.isFinite(value)) return error.NonFiniteBoundaryScore;
        return values;
    }

    pub fn vector(self: *NativeMath, name: []const u8, width: usize) ![]f32 {
        const tensor = try self.weight(name, &.{try signed(width)});
        defer self.cb.free(tensor);
        return self.host(tensor, width);
    }

    pub fn linear(self: *NativeMath, values: []const f32, rows: usize, in_dim: usize, out_dim: usize, prefix: []const u8) ![]f32 {
        try self.check();
        if (in_dim == 0 or out_dim == 0) return error.InvalidInputShape;
        if (values.len != try product(rows, in_dim)) return error.InvalidInputShape;
        const output_len = try product(rows, out_dim);
        try self.preflight(f32, output_len);
        if (rows == 0) return self.alloc(f32, output_len);
        _ = try dimension(rows);
        _ = try dimension(in_dim);
        _ = try dimension(out_dim);
        var name_buffer: [256]u8 = undefined;
        const weight_name = try std.fmt.bufPrint(&name_buffer, "{s}.weight", .{prefix});
        const weights = try self.weight(weight_name, &.{ try signed(out_dim), try signed(in_dim) });
        defer self.cb.free(weights);
        const bias_name = try std.fmt.bufPrint(&name_buffer, "{s}.bias", .{prefix});
        const bias = try self.weight(bias_name, &.{try signed(out_dim)});
        defer self.cb.free(bias);
        const input_tensor = try self.cb.fromFloat32Shape(values, &.{ try dimension(rows), try dimension(in_dim) });
        defer self.cb.free(input_tensor);
        const output = try self.cb.linear(input_tensor, weights, bias, rows, in_dim, out_dim);
        defer self.cb.free(output);
        return self.host(output, output_len);
    }

    pub fn norm(self: *NativeMath, values: []const f32, dim: usize, prefix: []const u8) ![]f32 {
        try self.check();
        if (dim == 0 or values.len % dim != 0) return error.InvalidInputShape;
        try self.preflight(f32, values.len);
        if (values.len == 0) return self.alloc(f32, 0);
        var name_buffer: [256]u8 = undefined;
        const weight_name = try std.fmt.bufPrint(&name_buffer, "{s}.weight", .{prefix});
        const gamma = try self.weight(weight_name, &.{try signed(dim)});
        defer self.cb.free(gamma);
        const bias_name = try std.fmt.bufPrint(&name_buffer, "{s}.bias", .{prefix});
        const beta = try self.weight(bias_name, &.{try signed(dim)});
        defer self.cb.free(beta);
        const tensor = try self.cb.fromFloat32Shape(values, &.{ try dimension(values.len / dim), try dimension(dim) });
        defer self.cb.free(tensor);
        // Task-head LayerNorm uses PyTorch's default epsilon, independently of
        // the DeBERTa encoder's 1e-7 epsilon.
        const normalized = try self.cb.layerNorm(tensor, gamma, beta, dim, 1e-5);
        defer self.cb.free(normalized);
        return self.host(normalized, values.len);
    }

    pub fn geluExact(self: *NativeMath, values: []const f32, dim: usize) ![]f32 {
        try self.check();
        if (dim == 0 or values.len % dim != 0) return error.InvalidInputShape;
        try self.preflight(f32, values.len);
        if (values.len == 0) return self.alloc(f32, 0);
        const tensor = try self.cb.fromFloat32Shape(values, &.{ try dimension(values.len / dim), try dimension(dim) });
        defer self.cb.free(tensor);
        const result = try self.cb.geluExact(tensor) orelse return error.UnsupportedGlinerBoundaryActivation;
        defer self.cb.free(result);
        return self.host(result, values.len);
    }
};

const Context = struct {
    math: NativeMath,
    config: *const boundary.Config,
    input: Input,
};

fn product(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b);
}

fn signed(value: usize) !i64 {
    return std.math.cast(i64, value) orelse error.InvalidInputShape;
}

fn dimension(value: usize) !i32 {
    return std.math.cast(i32, value) orelse error.InvalidInputShape;
}

fn validate(config: *const boundary.Config, input: Input, limits: Limits) !void {
    try config.head.validate();
    if (config.version != boundary.config_version or config.architecture_version != boundary.architecture_version)
        return error.UnsupportedGlinerBoundaryVersion;
    if (config.head.candidate_pool != .shared or config.head.content_soft_max_pool or
        config.head.candidate_attention_layers != 0 or config.head.query_attention_layers != 0)
        return error.UnsupportedGlinerBoundaryConfiguration;
    if (input.batch == 0 or input.queries == 0 or config.encoder.hidden_size == 0) return error.InvalidInputShape;
    if (input.batch > limits.max_batch or input.queries > limits.max_queries or input.text_length > limits.max_text_words)
        return error.ResourceLimitExceeded;
    const words = try product(input.batch, input.text_length);
    const queries = try product(input.batch, input.queries);
    if (input.text_states.len != try product(words, config.encoder.hidden_size) or
        input.query_states.len != try product(queries, config.encoder.hidden_size) or
        input.text_lengths.len != input.batch or input.query_mask.len != queries)
        return error.InvalidInputShape;
    for (input.text_lengths) |length| if (length > input.text_length) return error.InvalidInputShape;
    for (input.text_states) |value| if (!std.math.isFinite(value)) return error.NonFiniteBoundaryScore;
    for (input.query_states) |value| if (!std.math.isFinite(value)) return error.NonFiniteBoundaryScore;
}

/// Complete shared-pool boundary inference math for the published configs.
/// Returns intermediates for exact oracle comparison and later task decoding.
pub fn forwardNative(cb: *const compute.ComputeBackend, allocator: std.mem.Allocator, config: *const boundary.Config, input: Input, limits: Limits) !Result {
    if (cb.kind() != .native) return error.UnsupportedGlinerBoundaryBackend;
    try cb.checkExecutionControl();
    if (input.control) |control| try control.check();
    try validate(config, input, limits);
    const arena = try allocator.create(std.heap.ArenaAllocator);
    errdefer allocator.destroy(arena);
    arena.* = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    var ctx = Context{ .math = .{ .cb = cb, .allocator = arena.allocator(), .limits = limits, .control = input.control }, .config = config, .input = input };
    try ctx.math.check();
    const states = try encodeBoundaries(&ctx);
    const marginals = try computeMarginals(&ctx, states);
    const boundary_count = try std.math.add(usize, input.text_length, 1);
    const boundary_rows = try product(input.batch, boundary_count);
    const query_rows = try product(input.batch, input.queries);
    const d: usize = config.head.boundary_dim;
    const start = try ctx.math.linear(states, boundary_rows, d, d, "boundary_head.shared_pool_builder.start_projection");
    const end = try ctx.math.linear(states, boundary_rows, d, d, "boundary_head.shared_pool_builder.end_projection");
    const pool_count = try product(input.batch, config.head.pool_size);
    try ctx.math.charge(primitives.Span, pool_count);
    try ctx.math.charge(bool, pool_count);
    try ctx.math.charge(f32, try product(pool_count, 2));
    const pool = try primitives.buildSharedPool(ctx.math.allocator, .{
        .batch = input.batch,
        .boundaries = boundary_count,
        .queries = input.queries,
        .dim = d,
        .lengths = input.text_lengths,
        .query_mask = input.query_mask,
        .start_logits = marginals.start_logits,
        .end_logits = marginals.end_logits,
        .projected_starts = start,
        .projected_ends = end,
        .control = input.control orelse cb.execution_control,
    }, .{
        .boundary_top_k = config.head.pool_boundary_top_k,
        .capacity = config.head.pool_size,
        .min_per_query = config.head.min_pool_per_query,
        .max_pair_elements = limits.max_pool_pair_elements,
    });
    // The pool primitive independently bounds its scratch by pair elements.
    const scored = try scoreSharedPool(&ctx, states, marginals, pool);
    const candidate_states = if (config.head.enable_records) blk: {
        const endpoints = try gatherEndpoints(&ctx, states, pool.indices, pool.capacity, d);
        const value = try ctx.math.linear(endpoints, pool.indices.len, 2 * d, config.encoder.hidden_size, "boundary_head.candidate_encoder");
        maskRows(value, config.encoder.hidden_size, pool.valid);
        break :blk value;
    } else null;
    const null_logits = if (config.head.enable_abstention)
        try ctx.math.linear(input.query_states, query_rows, config.encoder.hidden_size, 1, "boundary_head.null_projection")
    else
        null;
    const count_log_rates = if (config.head.enable_count_head)
        try ctx.math.linear(input.query_states, query_rows, config.encoder.hidden_size, 1, "boundary_head.count_head")
    else
        null;
    try ctx.math.check();
    return .{
        .allocator = allocator,
        .arena = arena,
        .batch = input.batch,
        .text_length = input.text_length,
        .queries = input.queries,
        .boundary_dim = d,
        .pair_dim = config.head.pair_dim,
        .boundary_states = states,
        .marginals = marginals,
        .pool = pool,
        .candidate_features = scored.features,
        .pair_logits = scored.logits,
        .candidate_states = candidate_states,
        .null_logits = null_logits,
        .count_log_rates = count_log_rates,
    };
}

fn maskRows(values: []f32, dim: usize, mask: []const bool) void {
    for (mask, 0..) |valid, row| if (!valid) @memset(values[row * dim ..][0..dim], 0);
}

fn encodeBoundaries(ctx: *Context) ![]f32 {
    const input = ctx.input;
    const settings = ctx.config.head;
    const h: usize = ctx.config.encoder.hidden_size;
    const d: usize = settings.boundary_dim;
    const n = input.text_length + 1;
    const rows = try product(input.batch, n);
    const bos = try ctx.math.vector("boundary_head.boundary_encoder.bos_state", h);
    const eos = try ctx.math.vector("boundary_head.boundary_encoder.eos_state", h);
    const left = try ctx.math.alloc(f32, try product(rows, h));
    const right = try ctx.math.alloc(f32, left.len);
    const valid = try ctx.math.alloc(bool, rows);
    const valid_lengths = try ctx.math.alloc(usize, input.batch);
    for (0..input.batch) |b| {
        valid_lengths[b] = input.text_lengths[b] + 1;
        for (0..n) |position| {
            const target = (b * n + position) * h;
            const left_source = if (position == 0) bos else input.text_states[(b * input.text_length + position - 1) * h ..][0..h];
            const right_source = if (position == input.text_length or position == input.text_lengths[b]) eos else input.text_states[(b * input.text_length + position) * h ..][0..h];
            @memcpy(left[target..][0..h], left_source);
            @memcpy(right[target..][0..h], right_source);
            valid[b * n + position] = position <= input.text_lengths[b];
        }
    }
    const left_p = try ctx.math.linear(left, rows, h, d, "boundary_head.boundary_encoder.left_projection");
    const right_p = try ctx.math.linear(right, rows, h, d, "boundary_head.boundary_encoder.right_projection");
    const combined = try ctx.math.alloc(f32, try product(rows, 2 * d));
    for (0..rows) |row| {
        @memcpy(combined[row * 2 * d ..][0..d], left_p[row * d ..][0..d]);
        @memcpy(combined[row * 2 * d + d ..][0..d], right_p[row * d ..][0..d]);
    }
    const projected = try ctx.math.linear(combined, rows, 2 * d, d, "boundary_head.boundary_encoder.output_projection");
    const states = try ctx.math.norm(projected, d, "boundary_head.boundary_encoder.layer_norm");
    for (0..settings.boundary_attention_layers) |layer| {
        try ctx.math.check();
        var name: [192]u8 = undefined;
        const normed = try ctx.math.norm(states, d, try std.fmt.bufPrint(&name, "boundary_head.boundary_encoder.attention_blocks.{d}.norm", .{layer}));
        const qkv = try ctx.math.linear(normed, rows, d, 3 * d, try std.fmt.bufPrint(&name, "boundary_head.boundary_encoder.attention_blocks.{d}.qkv_projection", .{layer}));
        const q = try ctx.math.alloc(f32, states.len);
        const k = try ctx.math.alloc(f32, states.len);
        const v = try ctx.math.alloc(f32, states.len);
        const attended = try ctx.math.alloc(f32, states.len);
        for (0..rows) |row| {
            @memcpy(q[row * d ..][0..d], qkv[row * 3 * d ..][0..d]);
            @memcpy(k[row * d ..][0..d], qkv[row * 3 * d + d ..][0..d]);
            @memcpy(v[row * d ..][0..d], qkv[row * 3 * d + 2 * d ..][0..d]);
        }
        try primitives.bandedAttention(.{
            .batch = input.batch,
            .positions = n,
            .heads = settings.boundary_attention_heads,
            .head_dim = d / settings.boundary_attention_heads,
            .window = settings.boundary_attention_window,
            .lengths = valid_lengths,
            .control = input.control orelse ctx.math.cb.execution_control,
        }, q, k, v, attended);
        const update = try ctx.math.linear(attended, rows, d, d, try std.fmt.bufPrint(&name, "boundary_head.boundary_encoder.attention_blocks.{d}.output_projection", .{layer}));
        for (states, update) |*state, value| state.* += value;
        maskRows(states, d, valid);
    }
    const ffn: usize = @intFromFloat(@as(f64, @floatFromInt(d)) * settings.boundary_ffn_multiplier);
    for (0..settings.boundary_refinement_layers) |layer| {
        try ctx.math.check();
        var name: [192]u8 = undefined;
        const normed = try ctx.math.norm(states, d, try std.fmt.bufPrint(&name, "boundary_head.boundary_encoder.refinement_blocks.{d}.norm", .{layer}));
        const pair = try ctx.math.linear(normed, rows, d, 2 * ffn, try std.fmt.bufPrint(&name, "boundary_head.boundary_encoder.refinement_blocks.{d}.input_projection", .{layer}));
        const activated = try ctx.math.alloc(f32, try product(rows, ffn));
        for (0..rows) |row| for (0..ffn) |col| {
            const value = pair[row * 2 * ffn + col];
            const gate = pair[row * 2 * ffn + ffn + col];
            activated[row * ffn + col] = value * gate / (1.0 + @exp(-gate));
        };
        const update = try ctx.math.linear(activated, rows, ffn, d, try std.fmt.bufPrint(&name, "boundary_head.boundary_encoder.refinement_blocks.{d}.output_projection", .{layer}));
        for (states, update) |*state, value| state.* += value;
    }
    maskRows(states, d, valid);
    return states;
}

fn computeMarginals(ctx: *Context, states: []const f32) !Marginals {
    const input = ctx.input;
    const d: usize = ctx.config.head.boundary_dim;
    const h: usize = ctx.config.encoder.hidden_size;
    const n = input.text_length + 1;
    const bq = try product(input.batch, input.queries);
    const rows = try product(input.batch, n);
    const start_b = try ctx.math.linear(states, rows, d, d, "boundary_head.boundary_query_head.start_boundary_projection");
    const start_q = try ctx.math.linear(input.query_states, bq, h, d, "boundary_head.boundary_query_head.start_query_projection");
    const end_b = try ctx.math.linear(states, rows, d, d, "boundary_head.boundary_query_head.end_boundary_projection");
    const end_q = try ctx.math.linear(input.query_states, bq, h, d, "boundary_head.boundary_query_head.end_query_projection");
    const inside_t = try ctx.math.linear(input.text_states, try product(input.batch, input.text_length), h, d, "boundary_head.boundary_query_head.inside_text_projection");
    const inside_q = try ctx.math.linear(input.query_states, bq, h, d, "boundary_head.boundary_query_head.inside_query_projection");
    const start_logits = try ctx.math.alloc(f32, try product(bq, n));
    const end_logits = try ctx.math.alloc(f32, start_logits.len);
    const inside_logits = try ctx.math.alloc(f32, try product(bq, input.text_length));
    const inside_mask = try ctx.math.alloc(bool, inside_logits.len);
    const prefix = try ctx.math.alloc(f32, start_logits.len);
    const means = try ctx.math.alloc(f32, bq);
    const scale = 1.0 / @sqrt(@as(f32, @floatFromInt(d)));
    for (0..input.batch) |b| for (0..input.queries) |q| {
        try ctx.math.check();
        const qi = b * input.queries + q;
        for (0..n) |position| {
            const offset = qi * n + position;
            const valid = input.query_mask[qi] and position <= input.text_lengths[b];
            start_logits[offset] = if (valid) dot(start_b[(b * n + position) * d ..][0..d], start_q[qi * d ..][0..d]) * scale else primitives.mask_logit;
            end_logits[offset] = if (valid) dot(end_b[(b * n + position) * d ..][0..d], end_q[qi * d ..][0..d]) * scale else primitives.mask_logit;
        }
        for (0..input.text_length) |position| {
            const offset = qi * input.text_length + position;
            const valid = input.query_mask[qi] and position < input.text_lengths[b];
            inside_mask[offset] = valid;
            inside_logits[offset] = if (valid) dot(inside_t[(b * input.text_length + position) * d ..][0..d], inside_q[qi * d ..][0..d]) * scale else primitives.mask_logit;
        }
    };
    try primitives.centeredInsidePrefix(inside_logits, inside_mask, bq, input.text_length, prefix, means);
    return .{ .start_logits = start_logits, .end_logits = end_logits, .inside_logits = inside_logits, .inside_prefix = prefix, .inside_mean = means };
}

fn dot(a: []const f32, b: []const f32) f32 {
    var result: f32 = 0;
    for (a, b) |av, bv| result += av * bv;
    return result;
}

fn gatherEndpoints(ctx: *Context, states: []const f32, indices: []const primitives.Span, per_batch: usize, dim: usize) ![]f32 {
    const n = ctx.input.text_length + 1;
    const output = try ctx.math.alloc(f32, try product(indices.len, 2 * dim));
    for (indices, 0..) |span, i| {
        const b = i / per_batch;
        @memcpy(output[i * 2 * dim ..][0..dim], states[(b * n + span.start) * dim ..][0..dim]);
        @memcpy(output[i * 2 * dim + dim ..][0..dim], states[(b * n + span.end) * dim ..][0..dim]);
    }
    return output;
}

fn poolContent(ctx: *Context, indices: []const primitives.Span, per_batch: usize, prefix_name: []const u8) ![]f32 {
    const input = ctx.input;
    const dim: usize = ctx.config.head.content_dim;
    const n = input.text_length + 1;
    var name: [192]u8 = undefined;
    const projected = try ctx.math.linear(input.text_states, try product(input.batch, input.text_length), ctx.config.encoder.hidden_size, dim, try std.fmt.bufPrint(&name, "{s}.value_projection", .{prefix_name}));
    const prefix = try ctx.math.alloc(f32, try product(try product(input.batch, n), dim));
    for (0..input.batch) |b| {
        @memset(prefix[b * n * dim ..][0..dim], 0);
        for (0..input.text_length) |t| for (0..dim) |col| {
            const value = if (t < input.text_lengths[b]) projected[(b * input.text_length + t) * dim + col] else 0;
            prefix[(b * n + t + 1) * dim + col] = prefix[(b * n + t) * dim + col] + value;
        };
    }
    const pooled = try ctx.math.alloc(f32, try product(indices.len, dim));
    for (indices, 0..) |span, i| {
        const b = i / per_batch;
        const length: f32 = @floatFromInt(@max(span.end - span.start, 1));
        for (0..dim) |col| {
            pooled[i * dim + col] = (prefix[(b * n + span.end) * dim + col] - prefix[(b * n + span.start) * dim + col]) / length;
        }
    }
    return ctx.math.norm(pooled, dim, try std.fmt.bufPrint(&name, "{s}.layer_norm", .{prefix_name}));
}

const SharedScores = struct { features: []f32, logits: []f32 };

fn scoreSharedPool(ctx: *Context, states: []const f32, marginals: Marginals, pool: primitives.SharedPool) !SharedScores {
    const input = ctx.input;
    const d: usize = ctx.config.head.boundary_dim;
    const p: usize = ctx.config.head.pair_dim;
    const h: usize = ctx.config.encoder.hidden_size;
    const n = input.text_length + 1;
    const rows = try product(input.batch, n);
    const bq = try product(input.batch, input.queries);
    const c = pool.capacity;
    const bc = pool.indices.len;
    const starts = try ctx.math.linear(states, rows, d, p, "boundary_head.shared_pool_scorer.start_projection");
    const ends = try ctx.math.linear(states, rows, d, p, "boundary_head.shared_pool_scorer.end_projection");
    const lengths = try ctx.math.alloc(f32, try product(bc, 3));
    for (pool.indices, 0..) |span, index| {
        const b = index / c;
        const length: f32 = @floatFromInt(@max(span.end - span.start, 1));
        lengths[index * 3] = @log(1.0 + length);
        lengths[index * 3 + 1] = length / @as(f32, @floatFromInt(@max(input.text_lengths[b], 1)));
        lengths[index * 3 + 2] = 1.0 / @sqrt(length);
    }
    const length_p = try ctx.math.linear(lengths, bc, 3, p, "boundary_head.shared_pool_scorer.length_projection");
    const prior_p = try ctx.math.linear(pool.compat_logits, bc, 1, p, "boundary_head.shared_pool_scorer.prior_projection");
    const candidates = try ctx.math.alloc(f32, try product(bc, p));
    for (pool.indices, 0..) |span, index| {
        const b = index / c;
        for (0..p) |col| candidates[index * p + col] = starts[(b * n + span.start) * p + col] + ends[(b * n + span.end) * p + col] + length_p[index * p + col] + prior_p[index * p + col];
    }
    if (ctx.config.head.enable_span_content) {
        const content = try poolContent(ctx, pool.indices, c, "boundary_head.shared_pool_scorer.content_pooler");
        const projected = try ctx.math.linear(content, bc, ctx.config.head.content_dim, p, "boundary_head.shared_pool_scorer.content_projection");
        for (candidates, projected) |*candidate, value| candidate.* += value;
    }
    const features = try ctx.math.norm(candidates, p, "boundary_head.shared_pool_scorer.candidate_norm");
    maskRows(features, p, pool.valid);
    const queries = try ctx.math.linear(input.query_states, bq, h, p, "boundary_head.shared_pool_scorer.query_projection");
    const film = try ctx.math.linear(queries, bq, p, 2 * p, "boundary_head.shared_pool_scorer.film");
    const score_count = try product(bc, input.queries);
    const conditioned = try ctx.math.alloc(f32, try product(score_count, p));
    for (0..input.batch) |b| for (0..c) |candidate| for (0..input.queries) |query| {
        const output = ((b * c + candidate) * input.queries + query) * p;
        const qi = (b * input.queries + query) * 2 * p;
        for (0..p) |col| conditioned[output + col] = features[(b * c + candidate) * p + col] * (1.0 + film[qi + col]) + film[qi + p + col];
    };
    const first = try ctx.math.linear(conditioned, score_count, p, 64, "boundary_head.shared_pool_scorer.film_output.0");
    const activated = try ctx.math.geluExact(first, 64);
    const film_scores = try ctx.math.linear(activated, score_count, 64, 1, "boundary_head.shared_pool_scorer.film_output.3");
    const logits = try ctx.math.alloc(f32, score_count);
    const scale = 1.0 / @sqrt(@as(f32, @floatFromInt(p)));
    for (0..input.batch) |b| for (0..input.queries) |query| {
        try ctx.math.check();
        const qi = b * input.queries + query;
        for (0..c) |candidate| {
            const ci = b * c + candidate;
            const output = qi * c + candidate;
            if (!input.query_mask[qi] or !pool.valid[ci]) {
                logits[output] = primitives.mask_logit;
                continue;
            }
            const span = pool.indices[ci];
            var score = dot(features[ci * p ..][0..p], queries[qi * p ..][0..p]) * scale + film_scores[ci * input.queries + query];
            score += marginals.start_logits[qi * n + span.start];
            score += marginals.end_logits[qi * n + span.end];
            if (ctx.config.head.use_inside_evidence) {
                const length: f32 = @floatFromInt(span.end - span.start);
                const interval = marginals.inside_prefix[qi * n + span.end] - marginals.inside_prefix[qi * n + span.start] + marginals.inside_mean[qi] * length;
                score += interval / @sqrt(length);
            }
            if (!std.math.isFinite(score)) return error.NonFiniteBoundaryScore;
            logits[output] = score;
        }
    };
    return .{ .features = features, .logits = logits };
}

/// Exact separate proposal-prior and PairScorer path used by attributes and
/// constrained fields. Invalid signed coordinates are masked before gathering.
pub fn scoreExplicitSpansNative(
    cb: *const compute.ComputeBackend,
    allocator: std.mem.Allocator,
    config: *const boundary.Config,
    input: Input,
    explicit: ExplicitInput,
    limits: Limits,
) !ExplicitResult {
    if (cb.kind() != .native) return error.UnsupportedGlinerBoundaryBackend;
    try cb.checkExecutionControl();
    if (input.control) |control| try control.check();
    try validate(config, input, limits);
    if (explicit.capacity == 0) return error.InvalidInputShape;
    if (explicit.capacity > limits.max_explicit_candidates) return error.ResourceLimitExceeded;
    const bq = try product(input.batch, input.queries);
    const count = try product(bq, explicit.capacity);
    if (explicit.indices.len != count) return error.InvalidInputShape;
    if (explicit.valid_mask) |mask| if (mask.len != count) return error.InvalidInputShape;
    const arena = try allocator.create(std.heap.ArenaAllocator);
    errdefer allocator.destroy(arena);
    arena.* = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    var ctx = Context{ .math = .{ .cb = cb, .allocator = arena.allocator(), .limits = limits, .control = input.control }, .config = config, .input = input };
    const valid = try ctx.math.alloc(bool, count);
    const spans = try ctx.math.alloc(primitives.Span, count);
    for (explicit.indices, 0..) |span, i| {
        const qi = i / explicit.capacity;
        const b = qi / input.queries;
        const legal = span.start >= 0 and span.end > span.start and
            span.end <= try signed(input.text_lengths[b]) and input.query_mask[qi] and
            (explicit.valid_mask == null or explicit.valid_mask.?[i]);
        valid[i] = legal;
        spans[i] = if (legal) .{ .start = @intCast(span.start), .end = @intCast(span.end) } else .{ .start = 0, .end = 0 };
    }
    const states = try encodeBoundaries(&ctx);
    const marginals = try computeMarginals(&ctx, states);
    const d: usize = config.head.boundary_dim;
    const p: usize = config.head.pair_dim;
    const h: usize = config.encoder.hidden_size;
    const n = input.text_length + 1;
    const boundary_rows = try product(input.batch, n);
    const per_batch = try product(input.queries, explicit.capacity);
    const rotary = config.head.enable_rotary_endpoints;
    const proposer_start = try ctx.math.linear(states, boundary_rows, d, d, "boundary_head.boundary_proposer.start_pair_projection");
    const proposer_end = try ctx.math.linear(states, boundary_rows, d, d, "boundary_head.boundary_proposer.end_key_projection");
    if (rotary) {
        rotateEndpoints(proposer_start, input.batch, n, d, config.head.rotary_base);
        rotateEndpoints(proposer_end, input.batch, n, d, config.head.rotary_base);
    }
    const proposer_gate_dim = if (rotary) d / 2 else d;
    const proposer_gate = try ctx.math.linear(input.query_states, bq, h, proposer_gate_dim, "boundary_head.boundary_proposer.start_query_projection");
    for (proposer_gate) |*value| value.* = sigmoid(value.*);
    const scorer_start = try ctx.math.linear(states, boundary_rows, d, p, "boundary_head.pair_scorer.start_endpoint_projection");
    const scorer_end = try ctx.math.linear(states, boundary_rows, d, p, "boundary_head.pair_scorer.end_endpoint_projection");
    if (rotary) {
        rotateEndpoints(scorer_start, input.batch, n, p, config.head.rotary_base);
        rotateEndpoints(scorer_end, input.batch, n, p, config.head.rotary_base);
    }
    const scorer_gate_dim = if (rotary) p / 2 else p;
    const scorer_gate = try ctx.math.linear(input.query_states, bq, h, scorer_gate_dim, "boundary_head.pair_scorer.query_gate");
    for (scorer_gate) |*value| value.* = sigmoid(value.*);
    const heads: usize = config.head.multihead_pair_compat_heads;
    const per_head = try ctx.math.alloc(f32, try product(count, heads));
    @memset(per_head, 0);
    const differences = if (config.head.endpoint_difference_features) try ctx.math.alloc(f32, try product(count, 2 * p)) else null;
    const priors = try ctx.math.alloc(f32, count);
    for (spans, 0..) |span, i| {
        const qi = i / explicit.capacity;
        const b = qi / input.queries;
        var prior: f32 = 0;
        for (0..d) |col| {
            const gate = proposer_gate[qi * proposer_gate_dim + (if (rotary) col / 2 else col)];
            prior += proposer_start[(b * n + span.start) * d + col] * gate * proposer_end[(b * n + span.end) * d + col];
        }
        priors[i] = if (valid[i]) prior / @sqrt(@as(f32, @floatFromInt(d))) else 0;
        for (0..p) |col| {
            const start = scorer_start[(b * n + span.start) * p + col];
            const end = scorer_end[(b * n + span.end) * p + col];
            const gate = scorer_gate[qi * scorer_gate_dim + (if (rotary) col / 2 else col)];
            per_head[i * heads + col / (p / heads)] += start * gate * end;
            if (differences) |difference| {
                difference[i * 2 * p + col] = start - end;
                difference[i * 2 * p + p + col] = @abs(start - end);
            }
        }
    }
    const compat = if (config.head.reranker_endpoint_compat)
        try ctx.math.linear(per_head, count, heads, 1, "boundary_head.pair_scorer.compat_mix")
    else blk: {
        const zeros = try ctx.math.alloc(f32, count);
        @memset(zeros, 0);
        break :blk zeros;
    };
    const scale = 1.0 / @sqrt(@as(f32, @floatFromInt(p)));
    for (compat) |*value| value.* *= scale;
    if (differences) |difference| {
        const values = try ctx.math.linear(difference, count, 2 * p, 1, "boundary_head.pair_scorer.endpoint_difference_projection");
        for (compat, values) |*value, update| value.* += update;
    }
    const logits = try ctx.math.alloc(f32, count);
    for (spans, 0..) |span, i| {
        const qi = i / explicit.capacity;
        logits[i] = compat[i] + marginals.start_logits[qi * n + span.start] + marginals.end_logits[qi * n + span.end] + priors[i];
    }
    if (config.head.enable_span_content) {
        const content = try poolContent(&ctx, spans, per_batch, "boundary_head.pair_scorer.content_pooler");
        const content_dim: usize = config.head.content_dim;
        const coefficient = try ctx.math.linear(input.query_states, bq, h, content_dim, "boundary_head.pair_scorer.content_query_projection");
        const bias = try ctx.math.linear(content, count, content_dim, 1, "boundary_head.pair_scorer.content_bias");
        const content_scale = 1.0 / @sqrt(@as(f32, @floatFromInt(content_dim)));
        for (logits, 0..) |*value, i| {
            const qi = i / explicit.capacity;
            value.* += dot(content[i * content_dim ..][0..content_dim], coefficient[qi * content_dim ..][0..content_dim]) * content_scale;
            value.* += bias[i];
        }
    }
    if (config.head.use_inside_evidence) {
        const weights = if (config.head.query_conditioned_inside_weight)
            try ctx.math.linear(input.query_states, bq, h, 1, "boundary_head.pair_scorer.inside_weight")
        else blk: {
            const scalar = try ctx.math.weight("boundary_head.pair_scorer.inside_weight", &.{});
            defer cb.free(scalar);
            break :blk try ctx.math.host(scalar, 1);
        };
        for (spans, 0..) |span, i| {
            const qi = i / explicit.capacity;
            const length: f32 = @floatFromInt(span.end - span.start);
            const interval = marginals.inside_prefix[qi * n + span.end] - marginals.inside_prefix[qi * n + span.start] + marginals.inside_mean[qi] * length;
            logits[i] += weights[if (config.head.query_conditioned_inside_weight) qi else 0] * (interval / @sqrt(@max(length, 1)));
        }
    }
    const length_coeff = try ctx.math.linear(input.query_states, bq, h, 3, "boundary_head.pair_scorer.length_query_projection");
    for (spans, 0..) |span, i| {
        if (!valid[i]) {
            logits[i] = primitives.mask_logit;
            continue;
        }
        const qi = i / explicit.capacity;
        const b = qi / input.queries;
        const length: f32 = @floatFromInt(span.end - span.start);
        const features = [3]f32{ @log(1.0 + length), length / @as(f32, @floatFromInt(@max(input.text_lengths[b], 1))), 1.0 / @sqrt(length) };
        logits[i] += dot(&features, length_coeff[qi * 3 ..][0..3]);
        if (!std.math.isFinite(logits[i])) return error.NonFiniteBoundaryScore;
    }
    try ctx.math.check();
    return .{ .allocator = allocator, .arena = arena, .logits = logits, .valid = valid };
}

fn sigmoid(value: f32) f32 {
    return 1.0 / (1.0 + @exp(-value));
}

fn rotateEndpoints(values: []f32, batch: usize, positions: usize, dim: usize, theta: f32) void {
    for (0..positions) |position| for (0..dim / 2) |pair| {
        const exponent = @as(f32, @floatFromInt(2 * pair)) / @as(f32, @floatFromInt(dim));
        const inverse_frequency = 1.0 / std.math.pow(f32, theta, exponent);
        const angle = @as(f32, @floatFromInt(position)) * inverse_frequency;
        const cosine = @cos(angle);
        const sine = @sin(angle);
        for (0..batch) |b| {
            const index = (b * positions + position) * dim + 2 * pair;
            const even = values[index];
            const odd = values[index + 1];
            values[index] = even * cosine - odd * sine;
            values[index + 1] = even * sine + odd * cosine;
        }
    };
}
