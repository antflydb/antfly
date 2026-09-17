// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Differentiable GLiNER2.5 task graphs. The caller owns the Graph and every
//! runtime tensor; this builder owns binding descriptors only. Discard the
//! caller's graph after any construction error. No parameter value is copied
//! into a host constant and no encoder activation is detached here.
const std = @import("std");
const ml = @import("ml");
const model = @import("../models/gliner_boundary.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Allocator = std.mem.Allocator;
const Builder = ml.graph.Builder;
const NodeId = ml.graph.NodeId;
const Shape = ml.graph.Shape;
const nil = ml.graph.null_node;

pub const Mode = enum { training, evaluation };
pub const Stage = enum { proposals, candidates };
pub const BindingKind = enum { values, binary_mask, inverted_dropout, indices };
pub const Binding = struct {
    node: NodeId,
    name: []const u8,
    shape: Shape,
    kind: BindingKind,
    stage: Stage,
    probability: f32 = 0,
    /// Exclusive upper bound for integer routing inputs.
    index_bound: usize = 0,
};
pub const Layout = struct { batch: u32, words: u32, queries: u32, classifications: u32 = 0 };
pub const Limits = struct {
    max_batch: u32 = 128,
    max_words: u32 = 16384,
    max_queries: u32 = 1024,
    max_classifications: u32 = 4096,
    max_hidden: u32 = 8192,
    max_nodes: usize = 100000,
    max_tensor_elements: usize = 64 * 1024 * 1024,
    max_graph_elements: usize = 512 * 1024 * 1024,
    max_constant_bytes: usize = 64 * 1024 * 1024,
    max_attention_elements: usize = 16 * 1024 * 1024,
    control: ?Control = null,
};
pub const Input = struct {
    text: NodeId, // [B*W,H]
    queries: NodeId, // [B*Q,H]
    text_mask: NodeId, // [B,W], binary prefix mask
    query_mask: NodeId, // [B,Q]
};
pub const Proposals = struct {
    input: Input,
    boundary_states: NodeId, // [B*(W+1),D]
    boundary_mask: NodeId, // [B,W+1]
    start_logits: NodeId, // [B,Q,W+1]
    end_logits: NodeId,
    inside_logits: NodeId, // [B,Q,W]
    pool_start: NodeId, // [B*(W+1),D]
    pool_end: NodeId,
    null_logits: ?NodeId, // [B,Q]
    count_logits: ?NodeId,
};

pub const GraphBuilder = struct {
    allocator: Allocator,
    builder: *Builder,
    config: model.Config,
    layout: Layout,
    mode: Mode,
    limits: Limits,
    bindings: std.ArrayListUnmanaged(Binding) = .empty,
    checked_nodes: usize,
    graph_elements: usize = 0,

    pub fn init(a: Allocator, b: *Builder, config: model.Config, layout: Layout, mode: Mode, limits: Limits) !GraphBuilder {
        if (limits.control) |control| try control.check();
        try config.head.validate();
        if (config.version != model.config_version or config.architecture_version != model.architecture_version) return error.UnsupportedGlinerBoundaryVersion;
        if (layout.batch == 0 or layout.words == 0 or config.encoder.hidden_size == 0) return error.InvalidBoundaryTrainingGraphLayout;
        if (layout.batch > limits.max_batch or layout.words > limits.max_words or layout.queries > limits.max_queries or
            layout.classifications > limits.max_classifications or config.encoder.hidden_size > limits.max_hidden or
            config.head.boundary_dim > limits.max_hidden or config.head.pair_dim > limits.max_hidden or config.head.record_dim > limits.max_hidden)
            return error.BoundaryTrainingGraphLimitExceeded;
        _ = try multiply(layout.batch, layout.words);
        _ = try multiply(layout.batch, try std.math.add(u32, layout.words, 1));
        _ = try multiply(layout.batch, layout.queries);
        if (b.graph.nodes.items.len >= limits.max_nodes) return error.BoundaryTrainingGraphLimitExceeded;
        return .{ .allocator = a, .builder = b, .config = config, .layout = layout, .mode = mode, .limits = limits, .checked_nodes = b.graph.nodes.items.len };
    }
    pub fn deinit(self: *GraphBuilder) void {
        for (self.bindings.items) |binding| self.allocator.free(binding.name);
        self.bindings.deinit(self.allocator);
        self.* = undefined;
    }
    pub fn check(self: *GraphBuilder) !void {
        if (self.limits.control) |control| try control.check();
        const graph = self.builder.graph;
        if (graph.nodes.items.len > self.limits.max_nodes or graph.constant_pool.items.len > self.limits.max_constant_bytes) return error.BoundaryTrainingGraphLimitExceeded;
        for (graph.nodes.items[self.checked_nodes..]) |node| {
            const count = try self.elements(node.output_shape);
            self.graph_elements = std.math.add(usize, self.graph_elements, count) catch return error.BoundaryTrainingGraphLimitExceeded;
            if (self.graph_elements > self.limits.max_graph_elements) return error.BoundaryTrainingGraphLimitExceeded;
        }
        self.checked_nodes = graph.nodes.items.len;
    }
    fn constantBytes(self: *GraphBuilder, additional: usize) !void {
        try self.check();
        const total = std.math.add(usize, self.builder.graph.constant_pool.items.len, additional) catch return error.BoundaryTrainingGraphLimitExceeded;
        if (total > self.limits.max_constant_bytes) return error.BoundaryTrainingGraphLimitExceeded;
    }
    fn elements(self: *const GraphBuilder, shape: Shape) !usize {
        const n = shape.numElements() orelse return error.InvalidBoundaryTrainingGraphShape;
        const count = std.math.cast(usize, n) orelse return error.InvalidBoundaryTrainingGraphShape;
        if (count > self.limits.max_tensor_elements) return error.BoundaryTrainingGraphLimitExceeded;
        return count;
    }
    pub fn require(self: *GraphBuilder, node: NodeId, shape: Shape) !void {
        if (node >= self.builder.graph.nodeCount() or !self.builder.graph.node(node).output_shape.eq(shape)) return error.InvalidBoundaryTrainingGraphShape;
        _ = try self.elements(shape);
        try self.check();
    }
    fn valueShape(self: *GraphBuilder, node: NodeId) !Shape {
        if (node >= self.builder.graph.nodeCount()) return error.InvalidBoundaryTrainingGraphShape;
        const shape = self.builder.graph.node(node).output_shape;
        if (shape.dtype != .f32) return error.UnsupportedBoundaryTrainingGraphPrecision;
        _ = try self.elements(shape);
        return shape;
    }
    pub fn weight(self: *GraphBuilder, name: []const u8, dims: []const i64) !NodeId {
        const shape = Shape.init(.f32, dims);
        _ = try self.elements(shape);
        try self.check();
        for (self.builder.graph.parameters.items) |node| {
            const parameter = self.builder.graph.node(node);
            if (!std.mem.eql(u8, self.builder.graph.parameterName(parameter), name)) continue;
            if (!parameter.output_shape.eq(shape)) return error.BoundaryTrainingParameterShapeMismatch;
            return node;
        }
        return self.builder.parameter(name, shape);
    }
    pub fn input(self: *GraphBuilder, name: []const u8, shape: Shape, kind: BindingKind, stage: Stage, index_bound: usize) !NodeId {
        _ = try self.elements(shape);
        try self.check();
        if (!std.mem.startsWith(u8, name, "__gliner25.")) return error.InvalidBoundaryTrainingBinding;
        if ((kind == .indices and (shape.dtype != .i32 or index_bound == 0)) or (kind != .indices and shape.dtype != .f32)) return error.InvalidBoundaryTrainingBinding;
        for (self.builder.graph.parameters.items) |node| if (std.mem.eql(u8, self.builder.graph.parameterName(self.builder.graph.node(node)), name)) return error.DuplicateBoundaryTrainingBinding;
        const owned_name = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(owned_name);
        const node = try self.builder.parameter(name, shape);
        try self.bindings.append(self.allocator, .{ .node = node, .name = owned_name, .shape = shape, .kind = kind, .stage = stage, .index_bound = index_bound });
        return node;
    }
    pub fn dropout(self: *GraphBuilder, value: NodeId, site: []const u8, stage: Stage) !NodeId {
        if (self.mode == .evaluation or self.config.head.dropout == 0) return value;
        const shape = try self.valueShape(value);
        var name: [512]u8 = undefined;
        const mask = try self.input(try std.fmt.bufPrint(&name, "__gliner25.dropout.{s}", .{site}), shape, .inverted_dropout, stage, 0);
        self.bindings.items[self.bindings.items.len - 1].probability = self.config.head.dropout;
        return self.builder.mul(value, mask);
    }
    pub fn reshape(self: *GraphBuilder, value: NodeId, dims: []const i64) !NodeId {
        const before = try self.valueShape(value);
        const after = Shape.init(.f32, dims);
        if (try self.elements(after) != try self.elements(before)) return error.InvalidBoundaryTrainingGraphShape;
        try self.check();
        return self.builder.reshape(value, after);
    }
    pub fn expand(self: *GraphBuilder, value: NodeId, dims: []const i64, axes: []const u8) !NodeId {
        const before = try self.valueShape(value);
        const after = Shape.init(.f32, dims);
        _ = try self.elements(after);
        if (axes.len != before.rank()) return error.InvalidBoundaryTrainingGraphShape;
        for (axes, 0..) |axis, i| {
            if (axis >= after.rank() or (i > 0 and axes[i - 1] >= axis)) return error.InvalidBoundaryTrainingGraphShape;
            if (before.dim(@intCast(i)) != 1 and before.dim(@intCast(i)) != after.dim(axis)) return error.InvalidBoundaryTrainingGraphShape;
        }
        try self.check();
        var attrs = ml.graph.node.BroadcastAttrs{ .target_shape = after, .num_axes = @intCast(axes.len) };
        @memcpy(attrs.broadcast_axes[0..axes.len], axes);
        return self.builder.graph.addNode(.{ .op = .{ .broadcast_in_dim = attrs }, .output_shape = after, .inputs = .{ value, nil, nil, nil }, .num_inputs = 1 });
    }
    pub fn fill(self: *GraphBuilder, dims: []const i64, value: f32) !NodeId {
        return self.expand(try self.builder.scalarConst(.f32, value), dims, &.{});
    }
    pub fn maskFill(self: *GraphBuilder, value: NodeId, mask: NodeId, other: f32) !NodeId {
        const shape = try self.valueShape(value);
        try self.require(mask, shape);
        const fallback = try self.builder.scalarConst(.f32, other);
        return self.builder.graph.addNode(.{ .op = .{ .where_select = {} }, .output_shape = shape, .inputs = .{ mask, value, fallback, nil }, .num_inputs = 3 });
    }
    pub fn scale(self: *GraphBuilder, value: NodeId, factor: f32) !NodeId {
        _ = try self.valueShape(value);
        try self.check();
        return self.builder.mul(value, try self.builder.scalarConst(.f32, factor));
    }
    pub fn linear(self: *GraphBuilder, value: NodeId, in_dim: u32, out_dim: u32, prefix: []const u8) !NodeId {
        const shape = try self.valueShape(value);
        if (shape.rank() != 2 or shape.dim(1) != in_dim or in_dim == 0 or out_dim == 0) return error.InvalidBoundaryTrainingGraphShape;
        const rows = std.math.cast(u32, shape.dim(0)) orelse return error.BoundaryTrainingGraphLimitExceeded;
        _ = try self.elements(Shape.init(.f32, &.{ rows, out_dim }));
        var name: [512]u8 = undefined;
        const weights = try self.weight(try std.fmt.bufPrint(&name, "{s}.weight", .{prefix}), &.{ out_dim, in_dim });
        const bias = try self.weight(try std.fmt.bufPrint(&name, "{s}.bias", .{prefix}), &.{out_dim});
        return self.builder.linear(value, weights, bias, rows, in_dim, out_dim);
    }
    pub fn norm(self: *GraphBuilder, value: NodeId, dim: u32, prefix: []const u8) !NodeId {
        const shape = try self.valueShape(value);
        if (shape.rank() != 2 or shape.dim(1) != dim or dim == 0) return error.InvalidBoundaryTrainingGraphShape;
        var name: [512]u8 = undefined;
        const gamma = try self.weight(try std.fmt.bufPrint(&name, "{s}.weight", .{prefix}), &.{dim});
        const beta = try self.weight(try std.fmt.bufPrint(&name, "{s}.bias", .{prefix}), &.{dim});
        return self.builder.layerNorm(value, gamma, beta, dim, 1e-5);
    }
    pub fn gather(self: *GraphBuilder, value: NodeId, indices: NodeId, rows: u32, dim: u32) !NodeId {
        const shape = try self.valueShape(value);
        if (shape.rank() != 2 or shape.dim(1) != dim) return error.InvalidBoundaryTrainingGraphShape;
        try self.require(indices, Shape.init(.i32, &.{rows}));
        _ = try self.elements(Shape.init(.f32, &.{ rows, dim }));
        return self.builder.gather(value, indices, Shape.init(.f32, &.{ rows, dim }));
    }
    pub fn rowMask(self: *GraphBuilder, value: NodeId, mask: NodeId, rows: u32, dim: u32) !NodeId {
        try self.require(value, Shape.init(.f32, &.{ rows, dim }));
        const flat_mask = try self.reshape(mask, &.{rows});
        return self.builder.mul(value, try self.expand(flat_mask, &.{ rows, dim }, &.{0}));
    }

    /// Inclusive prefix scan of [B*W,D], represented by log2(W) sparse gather
    /// and add stages. Static scan indices are constants, never learned values.
    pub fn prefixSum(self: *GraphBuilder, value: NodeId, batch: u32, width: u32, dim: u32) !NodeId {
        if (batch == 0 or width == 0 or dim == 0) return error.InvalidBoundaryTrainingGraphShape;
        const rows = try multiply(batch, width);
        try self.require(value, Shape.init(.f32, &.{ rows, dim }));
        _ = try self.elements(Shape.init(.i32, &.{rows}));
        try self.constantBytes(try std.math.mul(usize, rows, 8));
        const indices = try self.allocator.alloc(i32, rows);
        defer self.allocator.free(indices);
        const mask = try self.allocator.alloc(f32, rows);
        defer self.allocator.free(mask);
        var prefix = value;
        var shift: u32 = 1;
        while (shift < width) : (shift = std.math.mul(u32, shift, 2) catch return error.BoundaryTrainingGraphLimitExceeded) {
            try self.constantBytes(try std.math.mul(usize, rows, 8));
            for (indices, mask, 0..) |*index, *keep, row| {
                if (row % 4096 == 0) try self.check();
                const position = row % width;
                index.* = @intCast(if (position >= shift) row - shift else row);
                keep.* = if (position >= shift) 1 else 0;
            }
            const route = try self.builder.tensorConstBytes(std.mem.sliceAsBytes(indices), Shape.init(.i32, &.{rows}));
            const keep = try self.builder.tensorConst(mask, Shape.init(.f32, &.{rows}));
            const previous = try self.rowMask(try self.gather(prefix, route, rows, dim), keep, rows, dim);
            prefix = try self.builder.add(prefix, previous);
        }
        const zero = try self.fill(&.{ batch, 1, dim }, 0);
        const padded = try self.builder.concat(zero, try self.reshape(prefix, &.{ batch, width, dim }), 1);
        return self.reshape(padded, &.{ try multiply(batch, try std.math.add(u32, width, 1)), dim });
    }

    pub fn buildClassification(self: *GraphBuilder, choices: NodeId, count: u32) !NodeId {
        const h = self.config.encoder.hidden_size;
        if (count == 0 or count > self.limits.max_classifications) return error.InvalidBoundaryTrainingGraphLayout;
        try self.require(choices, Shape.init(.f32, &.{ count, h }));
        var hidden = try self.linear(choices, h, try multiply(2, h), "classifier.0");
        hidden = try self.builder.relu(hidden);
        hidden = try self.dropout(hidden, "classifier", .proposals);
        const output = try self.linear(hidden, 2 * h, 1, if (self.config.head.dropout > 0) "classifier.3" else "classifier.2");
        try self.check();
        return self.reshape(output, &.{count});
    }

    /// Requires at least one query. Classification-only graphs call
    /// buildClassification directly and do not create a dummy boundary task.
    pub fn buildProposals(self: *GraphBuilder, inputs: Input) !Proposals {
        const b = self.layout.batch;
        const w = self.layout.words;
        const q = self.layout.queries;
        const n = try std.math.add(u32, w, 1);
        const h = self.config.encoder.hidden_size;
        const d = self.config.head.boundary_dim;
        if (q == 0) return error.InvalidBoundaryTrainingGraphLayout;
        try self.require(inputs.text, Shape.init(.f32, &.{ try multiply(b, w), h }));
        try self.require(inputs.queries, Shape.init(.f32, &.{ try multiply(b, q), h }));
        try self.require(inputs.text_mask, Shape.init(.f32, &.{ b, w }));
        try self.require(inputs.query_mask, Shape.init(.f32, &.{ b, q }));
        const boundary_mask = try self.builder.concat(try self.fill(&.{ b, 1 }, 1), inputs.text_mask, 1);
        const word_right_mask = try self.builder.concat(inputs.text_mask, try self.fill(&.{ b, 1 }, 0), 1);
        const eos_mask = try self.builder.sub(boundary_mask, word_right_mask);
        const bos = try self.expand(try self.weight("boundary_head.boundary_encoder.bos_state", &.{h}), &.{ b, 1, h }, &.{2});
        const eos = try self.expand(try self.weight("boundary_head.boundary_encoder.eos_state", &.{h}), &.{ b, 1, h }, &.{2});
        const text = try self.reshape(inputs.text, &.{ b, w, h });
        const left = try self.reshape(try self.builder.concat(bos, text, 1), &.{ try multiply(b, n), h });
        const right_initial = try self.builder.concat(text, eos, 1);
        const eos_all = try self.expand(try self.reshape(eos, &.{ b, h }), &.{ b, n, h }, &.{ 0, 2 });
        const right_mask = try self.expand(eos_mask, &.{ b, n, h }, &.{ 0, 1 });
        const right = try self.reshape(try self.builder.graph.addNode(.{ .op = .{ .where_select = {} }, .output_shape = Shape.init(.f32, &.{ b, n, h }), .inputs = .{ right_mask, eos_all, right_initial, nil }, .num_inputs = 3 }), &.{ try multiply(b, n), h });
        const left_p = try self.linear(left, h, d, "boundary_head.boundary_encoder.left_projection");
        const right_p = try self.linear(right, h, d, "boundary_head.boundary_encoder.right_projection");
        const merged = try self.builder.concat(left_p, right_p, 1);
        var states = try self.linear(merged, try multiply(2, d), d, "boundary_head.boundary_encoder.output_projection");
        states = try self.norm(states, d, "boundary_head.boundary_encoder.layer_norm");
        states = try self.dropout(states, "boundary_encoder.output", .proposals);
        for (0..self.config.head.boundary_attention_layers) |layer| states = try self.boundaryAttention(states, boundary_mask, @intCast(layer));
        const ffn_float = @as(f64, @floatFromInt(d)) * self.config.head.boundary_ffn_multiplier;
        if (ffn_float > self.limits.max_hidden) return error.BoundaryTrainingGraphLimitExceeded;
        const ffn: u32 = @max(1, @as(u32, @intFromFloat(ffn_float)));
        for (0..self.config.head.boundary_refinement_layers) |layer| {
            var name: [256]u8 = undefined;
            const normalized = try self.norm(states, d, try std.fmt.bufPrint(&name, "boundary_head.boundary_encoder.refinement_blocks.{d}.norm", .{layer}));
            const pair = try self.linear(normalized, d, try multiply(2, ffn), try std.fmt.bufPrint(&name, "boundary_head.boundary_encoder.refinement_blocks.{d}.input_projection", .{layer}));
            const value = try self.builder.sliceLastDim(pair, 0, ffn);
            const gate = try self.builder.sliceLastDim(pair, ffn, 2 * ffn);
            var update = try self.builder.mul(value, try self.builder.silu(gate));
            update = try self.dropout(update, try std.fmt.bufPrint(&name, "boundary_encoder.refinement.{d}.hidden", .{layer}), .proposals);
            update = try self.linear(update, ffn, d, try std.fmt.bufPrint(&name, "boundary_head.boundary_encoder.refinement_blocks.{d}.output_projection", .{layer}));
            update = try self.dropout(update, try std.fmt.bufPrint(&name, "boundary_encoder.refinement.{d}.output", .{layer}), .proposals);
            states = try self.builder.add(states, update);
        }
        states = try self.rowMask(states, boundary_mask, try multiply(b, n), d);
        const start_b = try self.dropout(try self.linear(states, d, d, "boundary_head.boundary_query_head.start_boundary_projection"), "marginals.start", .proposals);
        const start_q = try self.linear(inputs.queries, h, d, "boundary_head.boundary_query_head.start_query_projection");
        const end_b = try self.dropout(try self.linear(states, d, d, "boundary_head.boundary_query_head.end_boundary_projection"), "marginals.end", .proposals);
        const end_q = try self.linear(inputs.queries, h, d, "boundary_head.boundary_query_head.end_query_projection");
        const inside_t = try self.dropout(try self.linear(inputs.text, h, d, "boundary_head.boundary_query_head.inside_text_projection"), "marginals.inside", .proposals);
        const inside_q = try self.linear(inputs.queries, h, d, "boundary_head.boundary_query_head.inside_query_projection");
        const boundary_keep = try self.builder.mul(try self.expand(boundary_mask, &.{ b, q, n }, &.{ 0, 2 }), try self.expand(inputs.query_mask, &.{ b, q, n }, &.{ 0, 1 }));
        const inside_keep = try self.builder.mul(try self.expand(inputs.text_mask, &.{ b, q, w }, &.{ 0, 2 }), try self.expand(inputs.query_mask, &.{ b, q, w }, &.{ 0, 1 }));
        const out = Proposals{
            .input = inputs,
            .boundary_states = states,
            .boundary_mask = boundary_mask,
            .start_logits = try self.maskFill(try self.marginals(start_b, start_q, n), boundary_keep, -10000),
            .end_logits = try self.maskFill(try self.marginals(end_b, end_q, n), boundary_keep, -10000),
            .inside_logits = try self.maskFill(try self.marginals(inside_t, inside_q, w), inside_keep, -10000),
            .pool_start = try self.linear(states, d, d, "boundary_head.shared_pool_builder.start_projection"),
            .pool_end = try self.linear(states, d, d, "boundary_head.shared_pool_builder.end_projection"),
            .null_logits = if (self.config.head.enable_abstention) try self.reshape(try self.linear(inputs.queries, h, 1, "boundary_head.null_projection"), &.{ b, q }) else null,
            .count_logits = if (self.config.head.enable_count_head) try self.reshape(try self.linear(inputs.queries, h, 1, "boundary_head.count_head"), &.{ b, q }) else null,
        };
        try self.check();
        return out;
    }
    fn marginals(self: *GraphBuilder, positions: NodeId, queries: NodeId, width: u32) !NodeId {
        const b = self.layout.batch;
        const q = self.layout.queries;
        const d = self.config.head.boundary_dim;
        const lhs = try self.reshape(queries, &.{ b, q, d });
        const rhs = try self.builder.transpose(try self.reshape(positions, &.{ b, width, d }), &.{ 0, 2, 1 });
        _ = try self.elements(Shape.init(.f32, &.{ b, q, width }));
        return self.scale(try self.builder.matmul3D(lhs, rhs), 1 / @sqrt(@as(f32, @floatFromInt(d))));
    }
    fn boundaryAttention(self: *GraphBuilder, states: NodeId, mask: NodeId, layer: u32) !NodeId {
        const b = self.layout.batch;
        const n = self.layout.words + 1;
        const d = self.config.head.boundary_dim;
        const heads = self.config.head.boundary_attention_heads;
        const hd = d / heads;
        const bh = try multiply(b, heads);
        const rows = try multiply(b, n);
        const attention_shape = Shape.init(.f32, &.{ bh, n, n });
        if (try self.elements(attention_shape) > self.limits.max_attention_elements) return error.BoundaryTrainingGraphLimitExceeded;
        var name: [256]u8 = undefined;
        const normalized = try self.norm(states, d, try std.fmt.bufPrint(&name, "boundary_head.boundary_encoder.attention_blocks.{d}.norm", .{layer}));
        const projected = try self.linear(normalized, d, try multiply(3, d), try std.fmt.bufPrint(&name, "boundary_head.boundary_encoder.attention_blocks.{d}.qkv_projection", .{layer}));
        var qkv: [3]NodeId = undefined;
        for (&qkv, 0..) |*part, i| {
            const sliced = try self.builder.sliceLastDim(projected, @intCast(i * d), @intCast((i + 1) * d));
            const separated = try self.reshape(sliced, &.{ b, n, heads, hd });
            const permuted = try self.builder.transpose(separated, &.{ 0, 2, 1, 3 });
            part.* = try self.reshape(permuted, &.{ bh, n, hd });
        }
        const keys = try self.builder.transpose(qkv[1], &.{ 0, 2, 1 });
        const raw = try self.scale(try self.builder.matmul3D(qkv[0], keys), 1 / @sqrt(@as(f32, @floatFromInt(hd))));
        const count = try multiply(n, n);
        try self.constantBytes(try std.math.mul(usize, count, 8));
        const local = try self.allocator.alloc(f32, count);
        defer self.allocator.free(local);
        const diagonal = try self.allocator.alloc(f32, count);
        defer self.allocator.free(diagonal);
        for (local, diagonal, 0..) |*near, *same, i| {
            if (i % 4096 == 0) try self.check();
            const row = i / n;
            const col = i % n;
            const distance = if (row >= col) row - col else col - row;
            near.* = if (self.config.head.boundary_attention_window == 0 or distance <= self.config.head.boundary_attention_window) 1 else 0;
            same.* = if (row == col) 1 else 0;
        }
        const local_node = try self.builder.tensorConst(local, Shape.init(.f32, &.{ n, n }));
        const diagonal_node = try self.builder.tensorConst(diagonal, Shape.init(.f32, &.{ n, n }));
        const shape4 = [_]i64{ b, heads, n, n };
        const key_mask = try self.expand(mask, &shape4, &.{ 0, 3 });
        const local_mask = try self.expand(local_node, &shape4, &.{ 2, 3 });
        const diagonal_mask = try self.expand(diagonal_node, &shape4, &.{ 2, 3 });
        const valid_keys = try self.builder.mul(key_mask, local_mask);
        const allowed = try self.builder.sub(try self.builder.add(valid_keys, diagonal_mask), try self.builder.mul(valid_keys, diagonal_mask));
        const masked = try self.maskFill(raw, try self.reshape(allowed, &.{ bh, n, n }), -std.math.inf(f32));
        var probabilities = try self.builder.softmax(masked);
        probabilities = try self.dropout(probabilities, try std.fmt.bufPrint(&name, "boundary_encoder.attention.{d}.probabilities", .{layer}), .proposals);
        var attended = try self.builder.matmul3D(probabilities, qkv[2]);
        attended = try self.reshape(attended, &.{ b, heads, n, hd });
        attended = try self.reshape(try self.builder.transpose(attended, &.{ 0, 2, 1, 3 }), &.{ rows, d });
        var update = try self.linear(attended, d, d, try std.fmt.bufPrint(&name, "boundary_head.boundary_encoder.attention_blocks.{d}.output_projection", .{layer}));
        update = try self.dropout(update, try std.fmt.bufPrint(&name, "boundary_encoder.attention.{d}.output", .{layer}), .proposals);
        return self.rowMask(try self.builder.add(states, update), mask, rows, d);
    }
};

fn multiply(a: u32, b: u32) !u32 {
    return std.math.mul(u32, a, b) catch error.BoundaryTrainingGraphLimitExceeded;
}

/// Tensor binding validation is mandatory before graph execution. A caller
/// must additionally prove masks/routes came from the admitted schema/sample.
pub fn validateFloatBinding(binding: Binding, values: []const f32) !void {
    if (binding.kind == .indices or binding.shape.dtype != .f32) return error.InvalidBoundaryTrainingBinding;
    const count = binding.shape.numElements() orelse return error.InvalidBoundaryTrainingBinding;
    if (count < 0 or @as(u64, @intCast(count)) != values.len) return error.InvalidBoundaryTrainingBinding;
    if (!std.math.isFinite(binding.probability) or binding.probability < 0 or binding.probability >= 1) return error.InvalidBoundaryTrainingBinding;
    const scale = 1 / (1 - binding.probability);
    for (values) |value| {
        if (!std.math.isFinite(value)) return error.InvalidBoundaryTrainingBinding;
        switch (binding.kind) {
            .binary_mask => if (value != 0 and value != 1) return error.InvalidBoundaryTrainingBinding,
            .inverted_dropout => if (value != 0 and value != scale) return error.InvalidBoundaryTrainingBinding,
            .values => {},
            .indices => unreachable,
        }
    }
}
pub fn validateIndexBinding(binding: Binding, values: []const i32) !void {
    if (binding.kind != .indices or binding.shape.dtype != .i32) return error.InvalidBoundaryTrainingBinding;
    const count = binding.shape.numElements() orelse return error.InvalidBoundaryTrainingBinding;
    if (count < 0 or @as(u64, @intCast(count)) != values.len) return error.InvalidBoundaryTrainingBinding;
    for (values) |value| if (value < 0 or value >= binding.index_bound) return error.InvalidBoundaryTrainingBinding;
}
