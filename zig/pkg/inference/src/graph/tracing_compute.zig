// Copyright 2026 Antfly, Inc.
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

// Tracing compute backend: records ComputeBackend VTable calls into a
// Graph instead of executing them. Model architectures call cb.linear(),
// cb.rmsNorm(), etc. as usual — but when `cb` is a TracingCompute, those
// calls build a computation graph that can later be optimized and
// replayed through any real backend.
//
// CT handles wrap TracingHandle pointers that hold a NodeId.

const std = @import("std");
const ml = @import("ml");
const ops = @import("../ops/ops.zig");
const contracts = @import("backend_contracts.zig");

const Graph = ml.graph.Graph;
const Builder = ml.graph.Builder;
const Node = ml.graph.Node;
const NodeId = ml.graph.NodeId;
const null_node = ml.graph.null_node;
const Shape = ml.graph.Shape;
const DType = ml.graph.DType;
const OpCode = ml.graph.OpCode;

const CT = contracts.CT;
const ComputeBackend = ops.ComputeBackend;
const BackendKind = contracts.BackendKind;
const AttentionContext = contracts.AttentionContext;
const LinearNoBiasPairResult = ops.LinearNoBiasPairResult;
const MoeLinearNoBiasRequest = ops.MoeLinearNoBiasRequest;
const MoeLinearNoBiasPairResult = ops.MoeLinearNoBiasPairResult;
const MoeScatterAddRequest = ops.MoeScatterAddRequest;
const MoeRouteSelection = ops.MoeRouteSelection;
const TakeRowsRequest = ops.TakeRowsRequest;

/// Opaque handle stored inside CT (*anyopaque). Each traced tensor is
/// represented by one of these, allocated from a pool.
const TracingHandle = struct {
    node_id: NodeId,
};

/// Weight shape manifest entry: maps weight names to their shapes so the
/// tracer can annotate Parameter nodes without loading actual data.
pub const WeightShape = struct {
    name: []const u8,
    shape: Shape,
};

pub const WeightShapeResolver = struct {
    context: ?*anyopaque = null,
    resolve: *const fn (context: ?*anyopaque, allocator: std.mem.Allocator, name: []const u8) anyerror!?Shape,
};

pub const TracingCompute = struct {
    allocator: std.mem.Allocator,
    graph: Graph,

    /// Pool of TracingHandle allocations. Freed in bulk on deinit.
    handles: std.ArrayListUnmanaged(*TracingHandle),

    /// Weight name → shape lookup for parameter nodes.
    weight_shapes: std.StringHashMapUnmanaged(Shape),

    weight_shape_resolver: ?WeightShapeResolver = null,

    /// Default shape used when a weight is not in the manifest.
    /// Dynamic dims (-1) signal "resolve at execution time".
    default_weight_shape: Shape = Shape.init(.f32, &.{-1}),

    /// Node ID of the most recent fused_moe_select_routes node.
    /// Threaded as a dependency input to subsequent MoE ops so the
    /// interpreter's reachability walk includes the select_routes node.
    last_moe_select_routes: NodeId = null_node,

    /// Node ID of the per-expert output scale parameter for the current
    /// MoE layer. Set via setMoeExpertScale, consumed by moeScatterAdd.
    last_moe_expert_scale: NodeId = null_node,

    runtime_embedding_ids_ptr: ?[*]const i64 = null,
    runtime_embedding_ids_len: usize = 0,

    pub fn init(allocator: std.mem.Allocator) TracingCompute {
        return .{
            .allocator = allocator,
            .graph = Graph.init(allocator),
            .handles = .empty,
            .weight_shapes = .empty,
        };
    }

    pub fn initWithWeights(allocator: std.mem.Allocator, weights: []const WeightShape) !TracingCompute {
        var tc = init(allocator);
        for (weights) |w| {
            try tc.putWeightShape(w.name, w.shape);
        }
        return tc;
    }

    pub fn initWithWeightResolver(allocator: std.mem.Allocator, resolver: WeightShapeResolver) TracingCompute {
        var tc = init(allocator);
        tc.weight_shape_resolver = resolver;
        return tc;
    }

    /// Get a Builder that operates on this TracingCompute's graph.
    /// The returned Builder borrows the graph pointer and must not
    /// outlive the TracingCompute.
    fn getBuilder(self: *TracingCompute) Builder {
        return Builder.init(&self.graph);
    }

    fn putWeightShape(self: *TracingCompute, name: []const u8, shape: Shape) !void {
        const owned_name = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(owned_name);
        try self.weight_shapes.put(self.allocator, owned_name, shape);
    }

    pub fn deinit(self: *TracingCompute) void {
        for (self.handles.items) |h| {
            self.allocator.destroy(h);
        }
        self.handles.deinit(self.allocator);
        var it = self.weight_shapes.iterator();
        while (it.next()) |entry| self.allocator.free(entry.key_ptr.*);
        self.weight_shapes.deinit(self.allocator);
        self.graph.deinit();
    }

    /// Get the built graph. Valid until deinit.
    pub fn getGraph(self: *const TracingCompute) *const Graph {
        return &self.graph;
    }

    /// Extract a mutable reference to the graph (e.g. to mark outputs).
    pub fn getGraphMut(self: *TracingCompute) *Graph {
        return &self.graph;
    }

    pub fn setRuntimeEmbeddingIds(self: *TracingCompute, ids: []const i64) void {
        self.runtime_embedding_ids_ptr = ids.ptr;
        self.runtime_embedding_ids_len = ids.len;
    }

    /// Transfer ownership of the traced graph to the caller.
    /// Replaces the internal graph with a fresh empty one so that
    /// `deinit()` will not free the returned graph.
    pub fn extractGraph(self: *TracingCompute) Graph {
        const g = self.graph;
        self.graph = Graph.init(self.allocator);
        return g;
    }

    /// Return the ComputeBackend interface wrapping this tracer.
    pub fn backend(self: *TracingCompute) ComputeBackend {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &vtable,
        };
    }

    // ── Handle management ─────────────────────────────────────────────

    fn makeHandle(self: *TracingCompute, node_id: NodeId) !CT {
        const h = try self.allocator.create(TracingHandle);
        h.* = .{ .node_id = node_id };
        try self.handles.append(self.allocator, h);
        return @ptrCast(h);
    }

    fn nodeIdFromCT(ct: CT) NodeId {
        const h: *TracingHandle = @ptrCast(@alignCast(ct));
        return h.node_id;
    }

    // ── VTable implementation ─────────────────────────────────────────

    const vtable = ComputeBackend.VTable{
        .backendKind = &backendKind,
        .deinitBackend = &deinitBackend,
        .freeTensor = &freeTensor,
        .getWeight = &getWeight,
        .prefetchWeightHint = &prefetchWeightHint,
        .drainPrefetchBudget = &drainPrefetchBudget,
        .embeddingLookup = &embeddingLookup,
        .linear = &linearOp,
        .linearNoBias = &linearNoBiasOp,
        .layerNorm = &layerNormOp,
        .rmsNorm = &rmsNormOp,
        .gelu = &geluOp,
        .relu = &reluOp,
        .silu = &siluOp,
        .quickGelu = &quickGeluOp,
        .sigmoid = &sigmoidOp,
        .tanh_act = &tanhActOp,
        .concat = &concatOp,
        .add = &addOp,
        .scaledDotProductAttention = &sdpaOp,
        .causalSelfAttention = &causalSelfAttentionOp,
        .crossAttention = &crossAttentionOp,
        .relativePositionBias = &relativePositionBiasOp,
        .disentangledRelativeAttention = &debertaOp,
        .windowedSelfAttention = &windowedSelfAttentionOp,
        .channelSelfAttention = &channelSelfAttentionOp,
        .tokenGridConv2d = &tokenGridConv2dOp,
        .multiply = &multiplyOp,
        .conv1d = &conv1dOp,
        .conv2d = &conv2dOp,
        .rope = &ropeOp,
        .ropePerItem = &ropePerItemOp,
        .gqaCausalAttention = &gqaCausalAttentionOp,
        .gqaPagedAttention = &gqaPagedAttentionOp,
        .fromFloat32 = &fromFloat32Op,
        .fromFloat32Shape = &fromFloat32ShapeOp,
        .toFloat32 = &toFloat32Op,
        .takeRows = &takeRowsOp,
        .zeroTensor = &zeroTensorOp,
        .reshape2d = &reshape2dOp,
        .sliceLastDim = &sliceLastDimOp,
        .linearNoBiasPair = &linearNoBiasPairOp,
        .mulMatId = &moeLinearNoBiasOp,
        .moeLinearNoBias = &moeLinearNoBiasOp,
        .moeLinearNoBiasPair = &moeLinearNoBiasPairOp,
        .moeScatterAdd = &moeScatterAddOp,
        .moeSelectRoutes = &moeSelectRoutesOp,
        .setMoeExpertScale = &setMoeExpertScaleOp,
        .evalTensor = &evalTensorOp,
        .argmaxLastRow = &argmaxLastRowOp,
    };

    fn fromCtx(ctx: *anyopaque) *TracingCompute {
        return @ptrCast(@alignCast(ctx));
    }

    fn backendKind(_: *anyopaque) BackendKind {
        return .graph;
    }

    fn deinitBackend(_: *anyopaque) void {
        // Caller owns the TracingCompute struct; nothing to do here.
    }

    fn freeTensor(_: *anyopaque, _: CT) void {
        // No-op during tracing. Handles are freed in bulk on deinit.
    }

    fn prefetchWeightHint(_: *anyopaque, _: []const u8, _: u32) void {}
    fn drainPrefetchBudget(_: *anyopaque, _: usize) void {}

    // ── Weight / parameter ops ────────────────────────────────────────

    fn getWeight(ctx: *anyopaque, name: []const u8) anyerror!CT {
        const tc = fromCtx(ctx);
        if (tc.weight_shapes.get(name) == null) {
            if (tc.weight_shape_resolver) |resolver| {
                if (try resolver.resolve(resolver.context, tc.allocator, name)) |shape| {
                    try tc.putWeightShape(name, shape);
                } else {
                    return error.MissingWeight;
                }
            }
        }
        const shape = tc.weight_shapes.get(name) orelse tc.default_weight_shape;
        var b = Builder.init(tc.getGraphMut());
        const id = try b.parameter(name, shape);
        return tc.makeHandle(id);
    }

    fn embeddingLookup(ctx: *anyopaque, weight: CT, ids: []const i64, total: usize, dim: usize) anyerror!CT {
        const tc = fromCtx(ctx);
        const w_id = nodeIdFromCT(weight);
        if (ids.len < total) return error.UnsupportedShape;
        const idx_id = if (tc.isRuntimeEmbeddingIds(ids[0..total])) blk: {
            const idx_shape = Shape.init(.i64, &.{@intCast(total)});
            break :blk try tc.graph.addNode(.{
                .op = .{ .fused_from_float32 = {} },
                .output_shape = idx_shape,
            });
        } else blk: {
            const idx_data = try tc.allocator.alloc(f32, total);
            defer tc.allocator.free(idx_data);
            for (idx_data, ids[0..total]) |*dst, id| dst.* = @floatFromInt(id);
            const loc = try tc.graph.internConstant(idx_data);
            break :blk try tc.graph.addNode(.{
                .op = .{ .constant = .{ .data_offset = loc.offset, .data_len = loc.len } },
                .output_shape = Shape.init(.f32, &.{@intCast(total)}),
            });
        };
        var b = Builder.init(tc.getGraphMut());
        const id = try b.embeddingLookup(w_id, idx_id, @intCast(total), @intCast(dim));
        return tc.makeHandle(id);
    }

    fn isRuntimeEmbeddingIds(self: *const TracingCompute, ids: []const i64) bool {
        const ptr = self.runtime_embedding_ids_ptr orelse return false;
        return ptr == ids.ptr and self.runtime_embedding_ids_len == ids.len;
    }

    // ── Linear ops ────────────────────────────────────────────────────

    fn linearOp(ctx: *anyopaque, input: CT, weight: CT, bias: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!CT {
        const tc = fromCtx(ctx);
        const in_id = nodeIdFromCT(input);
        const w_id = nodeIdFromCT(weight);
        const b_id = nodeIdFromCT(bias);
        const in_shape = tc.graph.node(in_id).output_shape;
        const w_shape = tc.graph.node(w_id).output_shape;
        const bias_shape = tc.graph.node(b_id).output_shape;

        const id = if ((in_shape.rank() == 1 or in_shape.rank() == 2) and w_shape.rank() == 2 and bias_shape.rank() == 1) blk: {
            var tc_mut = fromCtx(ctx);
            var b = Builder.init(tc_mut.getGraphMut());
            break :blk try b.linear(
                in_id,
                w_id,
                b_id,
                @intCast(rows),
                @intCast(in_dim),
                @intCast(out_dim),
            );
        } else try tc.graph.addNode(.{
            .op = .{ .fused_linear = .{ .rows = @intCast(rows), .in_dim = @intCast(in_dim), .out_dim = @intCast(out_dim) } },
            .output_shape = Shape.init(in_shape.dtype, &.{ @intCast(rows), @intCast(out_dim) }),
            .inputs = .{ in_id, w_id, b_id, null_node },
            .num_inputs = 3,
        });
        return tc.makeHandle(id);
    }

    fn linearNoBiasOp(ctx: *anyopaque, input: CT, weight: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!CT {
        const tc = fromCtx(ctx);
        const in_id = nodeIdFromCT(input);
        const w_id = nodeIdFromCT(weight);
        const in_shape = tc.graph.node(in_id).output_shape;
        const w_shape = tc.graph.node(w_id).output_shape;

        const id = if ((in_shape.rank() == 1 or in_shape.rank() == 2) and w_shape.rank() == 2) blk: {
            var tc_mut = fromCtx(ctx);
            var b = Builder.init(tc_mut.getGraphMut());
            break :blk try b.linearNoBias(
                in_id,
                w_id,
                @intCast(rows),
                @intCast(in_dim),
                @intCast(out_dim),
            );
        } else try tc.graph.addNode(.{
            .op = .{ .fused_linear_no_bias = .{ .rows = @intCast(rows), .in_dim = @intCast(in_dim), .out_dim = @intCast(out_dim) } },
            .output_shape = Shape.init(in_shape.dtype, &.{ @intCast(rows), @intCast(out_dim) }),
            .inputs = .{ in_id, w_id, null_node, null_node },
            .num_inputs = 2,
        });
        return tc.makeHandle(id);
    }

    // ── Normalization ─────────────────────────────────────────────────

    fn layerNormOp(ctx: *anyopaque, input: CT, gamma: CT, beta: CT, dim: usize, eps: f32) anyerror!CT {
        const tc = fromCtx(ctx);
        const in_id = nodeIdFromCT(input);
        const in_shape = tc.graph.node(in_id).output_shape;
        const id = try tc.graph.addNode(.{
            .op = .{ .fused_layer_norm = .{ .dim = @intCast(dim), .eps = eps } },
            .output_shape = in_shape,
            .inputs = .{ in_id, nodeIdFromCT(gamma), nodeIdFromCT(beta), null_node },
            .num_inputs = 3,
        });
        return tc.makeHandle(id);
    }

    fn rmsNormOp(ctx: *anyopaque, input: CT, weight: CT, dim: usize, eps: f32) anyerror!CT {
        const tc = fromCtx(ctx);
        var b = Builder.init(tc.getGraphMut());
        const id = try b.rmsNorm(
            nodeIdFromCT(input),
            nodeIdFromCT(weight),
            @intCast(dim),
            eps,
        );
        return tc.makeHandle(id);
    }

    // ── Activations ───────────────────────────────────────────────────

    fn geluOp(ctx: *anyopaque, input: CT) anyerror!CT {
        var tc = fromCtx(ctx);
        var b = Builder.init(tc.getGraphMut());
        const id = try b.gelu(nodeIdFromCT(input));
        return tc.makeHandle(id);
    }

    fn reluOp(ctx: *anyopaque, input: CT) anyerror!CT {
        var tc = fromCtx(ctx);
        var b = Builder.init(tc.getGraphMut());
        const id = try b.relu(nodeIdFromCT(input));
        return tc.makeHandle(id);
    }

    fn siluOp(ctx: *anyopaque, input: CT) anyerror!CT {
        var tc = fromCtx(ctx);
        var b = Builder.init(tc.getGraphMut());
        const id = try b.silu(nodeIdFromCT(input));
        return tc.makeHandle(id);
    }

    fn quickGeluOp(ctx: *anyopaque, input: CT) anyerror!CT {
        const tc = fromCtx(ctx);
        const in_id = nodeIdFromCT(input);
        const in_shape = tc.graph.node(in_id).output_shape;
        const id = try tc.graph.addNode(.{
            .op = .{ .fused_quick_gelu = {} },
            .output_shape = in_shape,
            .inputs = .{ in_id, null_node, null_node, null_node },
            .num_inputs = 1,
        });
        return tc.makeHandle(id);
    }

    fn sigmoidOp(ctx: *anyopaque, input: CT) anyerror!CT {
        const tc = fromCtx(ctx);
        const in_id = nodeIdFromCT(input);
        const in_shape = tc.graph.node(in_id).output_shape;
        const id = try tc.graph.addNode(.{
            .op = .{ .fused_sigmoid = {} },
            .output_shape = in_shape,
            .inputs = .{ in_id, null_node, null_node, null_node },
            .num_inputs = 1,
        });
        return tc.makeHandle(id);
    }

    fn tanhActOp(ctx: *anyopaque, input: CT) anyerror!CT {
        const tc = fromCtx(ctx);
        const in_id = nodeIdFromCT(input);
        const in_shape = tc.graph.node(in_id).output_shape;
        const id = try tc.graph.addNode(.{
            .op = .{ .fused_tanh_act = {} },
            .output_shape = in_shape,
            .inputs = .{ in_id, null_node, null_node, null_node },
            .num_inputs = 1,
        });
        return tc.makeHandle(id);
    }

    // ── Element-wise binary ───────────────────────────────────────────

    fn addOp(ctx: *anyopaque, a: CT, b: CT) anyerror!CT {
        var tc = fromCtx(ctx);
        var bld = Builder.init(tc.getGraphMut());
        const id = try bld.elemAdd(nodeIdFromCT(a), nodeIdFromCT(b));
        return tc.makeHandle(id);
    }

    fn multiplyOp(ctx: *anyopaque, a: CT, b: CT) anyerror!CT {
        var tc = fromCtx(ctx);
        var bld = Builder.init(tc.getGraphMut());
        const id = try bld.elemMultiply(nodeIdFromCT(a), nodeIdFromCT(b));
        return tc.makeHandle(id);
    }

    fn concatOp(ctx: *anyopaque, a: CT, b: CT, total: usize, dim_a: usize, dim_b: usize) anyerror!CT {
        const tc = fromCtx(ctx);
        const a_id = nodeIdFromCT(a);
        const out_shape = Shape.init(
            tc.graph.node(a_id).output_shape.dtype,
            &.{ @intCast(total), @intCast(dim_a + dim_b) },
        );
        const id = try tc.graph.addNode(.{
            .op = .{ .fused_concat = .{
                .total = @intCast(total),
                .dim_a = @intCast(dim_a),
                .dim_b = @intCast(dim_b),
            } },
            .output_shape = out_shape,
            .inputs = .{ a_id, nodeIdFromCT(b), null_node, null_node },
            .num_inputs = 2,
        });
        return tc.makeHandle(id);
    }

    // ── Attention ops ─────────────────────────────────────────────────

    fn sdpaOp(ctx: *anyopaque, Q: CT, K: CT, V: CT, _: []const i64, attn_bias: ?CT, batch: usize, seq_len: usize, num_heads: usize, head_dim: usize) anyerror!CT {
        const tc = fromCtx(ctx);
        const q_id = nodeIdFromCT(Q);
        const out_shape = tc.graph.node(q_id).output_shape;
        var inputs: [4]NodeId = .{ q_id, nodeIdFromCT(K), nodeIdFromCT(V), null_node };
        var num_inputs: u8 = 3;
        if (attn_bias) |bias| {
            inputs[3] = nodeIdFromCT(bias);
            num_inputs = 4;
        }
        const id = try tc.graph.addNode(.{
            .op = .{ .fused_sdpa = .{
                .batch = @intCast(batch),
                .seq_len = @intCast(seq_len),
                .num_heads = @intCast(num_heads),
                .head_dim = @intCast(head_dim),
            } },
            .output_shape = out_shape,
            .inputs = inputs,
            .num_inputs = num_inputs,
        });
        return tc.makeHandle(id);
    }

    fn causalSelfAttentionOp(ctx: *anyopaque, Q: CT, K: CT, V: CT, attn_bias: ?CT, batch: usize, seq_len: usize, num_heads: usize, head_dim: usize) anyerror!CT {
        const tc = fromCtx(ctx);
        const q_id = nodeIdFromCT(Q);
        const out_shape = tc.graph.node(q_id).output_shape;
        var inputs: [4]NodeId = .{ q_id, nodeIdFromCT(K), nodeIdFromCT(V), null_node };
        var num_inputs: u8 = 3;
        if (attn_bias) |bias| {
            inputs[3] = nodeIdFromCT(bias);
            num_inputs = 4;
        }
        const id = try tc.graph.addNode(.{
            .op = .{ .fused_causal_self_attention = .{
                .batch = @intCast(batch),
                .seq_len = @intCast(seq_len),
                .num_heads = @intCast(num_heads),
                .head_dim = @intCast(head_dim),
            } },
            .output_shape = out_shape,
            .inputs = inputs,
            .num_inputs = num_inputs,
        });
        return tc.makeHandle(id);
    }

    fn crossAttentionOp(ctx: *anyopaque, Q: CT, K: CT, V: CT, _: []const i64, batch: usize, dec_seq: usize, enc_seq: usize, num_heads: usize, head_dim: usize) anyerror!CT {
        const tc = fromCtx(ctx);
        const q_id = nodeIdFromCT(Q);
        const out_shape = tc.graph.node(q_id).output_shape;
        const id = try tc.graph.addNode(.{
            .op = .{ .fused_cross_attention = .{
                .batch = @intCast(batch),
                .dec_seq = @intCast(dec_seq),
                .enc_seq = @intCast(enc_seq),
                .num_heads = @intCast(num_heads),
                .head_dim = @intCast(head_dim),
            } },
            .output_shape = out_shape,
            .inputs = .{ q_id, nodeIdFromCT(K), nodeIdFromCT(V), null_node },
            .num_inputs = 3,
        });
        return tc.makeHandle(id);
    }

    fn gqaCausalAttentionOp(ctx: *anyopaque, Q: CT, K: CT, V: CT, attn_bias: ?CT, batch: usize, seq_len: usize, num_heads: usize, num_kv_heads: usize, head_dim: usize) anyerror!CT {
        const tc = fromCtx(ctx);
        const q_id = nodeIdFromCT(Q);
        const out_shape = tc.graph.node(q_id).output_shape;
        var inputs: [4]NodeId = .{ q_id, nodeIdFromCT(K), nodeIdFromCT(V), null_node };
        var num_inputs: u8 = 3;
        if (attn_bias) |bias| {
            inputs[3] = nodeIdFromCT(bias);
            num_inputs = 4;
        }
        const id = try tc.graph.addNode(.{
            .op = .{ .fused_gqa_causal_attention = .{
                .batch = @intCast(batch),
                .seq_len = @intCast(seq_len),
                .kv_seq_len = @intCast(seq_len),
                .num_heads = @intCast(num_heads),
                .num_kv_heads = @intCast(num_kv_heads),
                .head_dim = @intCast(head_dim),
            } },
            .output_shape = out_shape,
            .inputs = inputs,
            .num_inputs = num_inputs,
        });
        return tc.makeHandle(id);
    }

    fn gqaPagedAttentionOp(ctx: *anyopaque, Q: CT, K: CT, V: CT, attn_bias: ?CT, attention: AttentionContext, batch: usize, num_heads: usize, num_kv_heads: usize, head_dim: usize) anyerror!CT {
        // Paged attention records the same as GQA causal with the query
        // sequence length from the attention context.
        const tc = fromCtx(ctx);
        const q_id = nodeIdFromCT(Q);
        const out_shape = tc.graph.node(q_id).output_shape;
        var inputs: [4]NodeId = .{ q_id, nodeIdFromCT(K), nodeIdFromCT(V), null_node };
        var num_inputs: u8 = 3;
        if (attn_bias) |bias| {
            inputs[3] = nodeIdFromCT(bias);
            num_inputs = 4;
        }
        const id = try tc.graph.addNode(.{
            .op = .{ .fused_gqa_causal_attention = .{
                .batch = @intCast(batch),
                .seq_len = @intCast(attention.query_sequence_len),
                .kv_seq_len = @intCast(attention.kv_sequence_len),
                .num_heads = @intCast(num_heads),
                .num_kv_heads = @intCast(num_kv_heads),
                .head_dim = @intCast(head_dim),
                .layer_index = @intCast(attention.layer_index),
                .skip_kv_write = attention.skip_kv_write,
            } },
            .output_shape = out_shape,
            .inputs = inputs,
            .num_inputs = num_inputs,
        });
        return tc.makeHandle(id);
    }

    fn relativePositionBiasOp(ctx: *anyopaque, weight: CT, q_len: usize, k_len: usize, num_heads: usize, num_buckets: usize, max_distance: usize, bidirectional: bool) anyerror!CT {
        const tc = fromCtx(ctx);
        const w_id = nodeIdFromCT(weight);
        const out_shape = Shape.init(
            tc.graph.node(w_id).output_shape.dtype,
            &.{ @intCast(num_heads), @intCast(q_len), @intCast(k_len) },
        );
        const id = try tc.graph.addNode(.{
            .op = .{ .fused_relative_position_bias = .{
                .q_len = @intCast(q_len),
                .k_len = @intCast(k_len),
                .num_heads = @intCast(num_heads),
                .num_buckets = @intCast(num_buckets),
                .max_distance = @intCast(max_distance),
                .bidirectional = bidirectional,
            } },
            .output_shape = out_shape,
            .inputs = .{ w_id, null_node, null_node, null_node },
            .num_inputs = 1,
        });
        return tc.makeHandle(id);
    }

    fn debertaOp(ctx: *anyopaque, Q: CT, K: CT, V: CT, Q_r: CT, K_r: CT, _: []const i64, batch: usize, seq_len: usize, num_heads: usize, head_dim: usize) anyerror!CT {
        const tc = fromCtx(ctx);
        const q_id = nodeIdFromCT(Q);
        const out_shape = tc.graph.node(q_id).output_shape;
        // 5 inputs: Q, K, V, Q_r, K_r — but we only have 4 inline slots.
        // Store the first 4 inline; K_r will be an extra node reference
        // stored via vjp_alternate as a temporary workaround.
        // TODO: support overflow inputs when needed.
        const id = try tc.graph.addNode(.{
            .op = .{ .fused_causal_self_attention = .{
                .batch = @intCast(batch),
                .seq_len = @intCast(seq_len),
                .num_heads = @intCast(num_heads),
                .head_dim = @intCast(head_dim),
            } },
            .output_shape = out_shape,
            .inputs = .{ q_id, nodeIdFromCT(K), nodeIdFromCT(V), nodeIdFromCT(Q_r) },
            .num_inputs = 4,
            .vjp_alternate = nodeIdFromCT(K_r),
        });
        return tc.makeHandle(id);
    }

    fn windowedSelfAttentionOp(
        ctx: *anyopaque,
        input: CT,
        norm_weight: CT,
        norm_bias: CT,
        qkv_weight: CT,
        _: CT, // qkv_bias
        _: CT, // proj_weight
        _: CT, // proj_bias
        batch: usize,
        height: usize,
        width: usize,
        dim: usize,
        num_heads: usize,
        window_size: usize,
    ) anyerror!CT {
        const tc = fromCtx(ctx);
        const in_id = nodeIdFromCT(input);
        const out_shape = tc.graph.node(in_id).output_shape;
        const id = try tc.graph.addNode(.{
            .op = .{ .fused_windowed_self_attention = .{
                .batch = @intCast(batch),
                .height = @intCast(height),
                .width = @intCast(width),
                .dim = @intCast(dim),
                .num_heads = @intCast(num_heads),
                .window_size = @intCast(window_size),
            } },
            .output_shape = out_shape,
            .inputs = .{ in_id, nodeIdFromCT(norm_weight), nodeIdFromCT(norm_bias), nodeIdFromCT(qkv_weight) },
            .num_inputs = 4,
        });
        return tc.makeHandle(id);
    }

    fn channelSelfAttentionOp(
        ctx: *anyopaque,
        input: CT,
        norm_weight: CT,
        norm_bias: CT,
        qkv_weight: CT,
        _: CT, // qkv_bias
        _: CT, // proj_weight
        _: CT, // proj_bias
        batch: usize,
        seq_len: usize,
        dim: usize,
        groups: usize,
    ) anyerror!CT {
        const tc = fromCtx(ctx);
        const in_id = nodeIdFromCT(input);
        const out_shape = tc.graph.node(in_id).output_shape;
        const id = try tc.graph.addNode(.{
            .op = .{ .fused_channel_self_attention = .{
                .batch = @intCast(batch),
                .seq_len = @intCast(seq_len),
                .dim = @intCast(dim),
                .groups = @intCast(groups),
            } },
            .output_shape = out_shape,
            .inputs = .{ in_id, nodeIdFromCT(norm_weight), nodeIdFromCT(norm_bias), nodeIdFromCT(qkv_weight) },
            .num_inputs = 4,
        });
        return tc.makeHandle(id);
    }

    // ── Reshape ───────────────────────────────────────────────────────

    fn reshape2dOp(ctx: *anyopaque, input: CT, rows: usize, cols: usize) anyerror!CT {
        const tc = fromCtx(ctx);
        const in_id = nodeIdFromCT(input);
        const in_dtype = tc.graph.node(in_id).output_shape.dtype;
        var b = Builder.init(tc.getGraphMut());
        const id = try b.reshape(in_id, Shape.init(in_dtype, &.{ @intCast(rows), @intCast(cols) }));
        return tc.makeHandle(id);
    }

    fn sliceLastDimOp(ctx: *anyopaque, input: CT, start: usize, stop: usize) anyerror!CT {
        const tc = fromCtx(ctx);
        const in_id = nodeIdFromCT(input);
        const in_shape = tc.graph.node(in_id).output_shape;
        if (in_shape.rank() != 2 or stop < start) return error.InvalidSlice;

        var attrs = ml.graph.node.SliceAttrs{};
        attrs.num_axes = 2;
        attrs.starts[0] = 0;
        attrs.starts[1] = @intCast(start);
        attrs.limits[0] = in_shape.dim(0);
        attrs.limits[1] = @intCast(stop);
        attrs.strides[0] = 1;
        attrs.strides[1] = 1;

        const out_shape = Shape.init(in_shape.dtype, &.{ in_shape.dim(0), @intCast(stop - start) });
        const id = try tc.graph.addNode(.{
            .op = .{ .slice = attrs },
            .output_shape = out_shape,
            .inputs = .{ in_id, null_node, null_node, null_node },
            .num_inputs = 1,
        });
        return tc.makeHandle(id);
    }

    // ── RoPE ──────────────────────────────────────────────────────────

    fn ropeOp(ctx: *anyopaque, input: CT, seq_len: usize, head_dim: usize, rope_dim: usize, theta: f32, freq_scale: f32, position_offset: usize, consecutive_pairs: bool) anyerror!CT {
        const tc = fromCtx(ctx);
        const in_id = nodeIdFromCT(input);
        const out_shape = tc.graph.node(in_id).output_shape;
        const id = try tc.graph.addNode(.{
            .op = .{ .fused_rope = .{
                .seq_len = @intCast(seq_len),
                .head_dim = @intCast(head_dim),
                .rope_dim = @intCast(rope_dim),
                .theta = theta,
                .freq_scale = freq_scale,
                .position_offset = @intCast(position_offset),
                .consecutive_pairs = consecutive_pairs,
            } },
            .output_shape = out_shape,
            .inputs = .{ in_id, null_node, null_node, null_node },
            .num_inputs = 1,
        });
        return tc.makeHandle(id);
    }

    fn ropePerItemOp(ctx: *anyopaque, input: CT, _: usize, _: usize, head_dim: usize, rope_dim: usize, theta: f32, freq_scale: f32, _: []const usize, _: []const usize, consecutive_pairs: bool) anyerror!CT {
        // Record as a basic rope node — per-item details are runtime state
        const tc = fromCtx(ctx);
        const in_id = nodeIdFromCT(input);
        const out_shape = tc.graph.node(in_id).output_shape;
        const id = try tc.graph.addNode(.{
            .op = .{
                .fused_rope = .{
                    .seq_len = 0, // dynamic
                    .head_dim = @intCast(head_dim),
                    .rope_dim = @intCast(rope_dim),
                    .theta = theta,
                    .freq_scale = freq_scale,
                    .consecutive_pairs = consecutive_pairs,
                },
            },
            .output_shape = out_shape,
            .inputs = .{ in_id, null_node, null_node, null_node },
            .num_inputs = 1,
        });
        return tc.makeHandle(id);
    }

    // ── Convolution ───────────────────────────────────────────────────

    fn conv1dOp(ctx: *anyopaque, input: CT, weight: CT, bias: CT, batch: usize, in_channels: usize, out_channels: usize, time_steps: usize, kernel_size: usize, stride: usize, padding: usize) anyerror!CT {
        const tc = fromCtx(ctx);
        const out_time = (time_steps + 2 * padding - kernel_size) / stride + 1;
        const out_shape = Shape.init(.f32, &.{
            @intCast(batch),
            @intCast(out_channels),
            @intCast(out_time),
        });
        const id = try tc.graph.addNode(.{
            .op = .{ .fused_conv1d = .{
                .batch = @intCast(batch),
                .in_channels = @intCast(in_channels),
                .out_channels = @intCast(out_channels),
                .time_steps = @intCast(time_steps),
                .kernel_size = @intCast(kernel_size),
                .stride = @intCast(stride),
                .padding = @intCast(padding),
            } },
            .output_shape = out_shape,
            .inputs = .{ nodeIdFromCT(input), nodeIdFromCT(weight), nodeIdFromCT(bias), null_node },
            .num_inputs = 3,
        });
        return tc.makeHandle(id);
    }

    fn conv2dOp(ctx: *anyopaque, input: CT, weight: CT, bias: CT, batch: usize, in_channels: usize, out_channels: usize, height: usize, width: usize, kernel_h: usize, kernel_w: usize, stride_h: usize, stride_w: usize, padding_h: usize, padding_w: usize, groups: usize) anyerror!CT {
        const tc = fromCtx(ctx);
        const out_h = (height + 2 * padding_h - kernel_h) / stride_h + 1;
        const out_w = (width + 2 * padding_w - kernel_w) / stride_w + 1;
        const out_shape = Shape.init(.f32, &.{
            @intCast(batch),
            @intCast(out_channels),
            @intCast(out_h),
            @intCast(out_w),
        });
        const id = try tc.graph.addNode(.{
            .op = .{ .fused_conv2d = .{
                .batch = @intCast(batch),
                .in_channels = @intCast(in_channels),
                .out_channels = @intCast(out_channels),
                .height = @intCast(height),
                .width = @intCast(width),
                .kernel_h = @intCast(kernel_h),
                .kernel_w = @intCast(kernel_w),
                .stride_h = @intCast(stride_h),
                .stride_w = @intCast(stride_w),
                .padding_h = @intCast(padding_h),
                .padding_w = @intCast(padding_w),
                .groups = @intCast(groups),
            } },
            .output_shape = out_shape,
            .inputs = .{ nodeIdFromCT(input), nodeIdFromCT(weight), nodeIdFromCT(bias), null_node },
            .num_inputs = 3,
        });
        return tc.makeHandle(id);
    }

    fn tokenGridConv2dOp(
        ctx: *anyopaque,
        input: CT,
        weight: CT,
        bias: CT,
        batch: usize,
        _: usize, // in_channels
        out_channels: usize,
        height: usize,
        width: usize,
        kernel_h: usize,
        kernel_w: usize,
        stride_h: usize,
        stride_w: usize,
        padding_h: usize,
        padding_w: usize,
        _: usize, // groups
    ) anyerror!CT {
        const tc = fromCtx(ctx);
        const out_h = (height + 2 * padding_h - kernel_h) / stride_h + 1;
        const out_w = (width + 2 * padding_w - kernel_w) / stride_w + 1;
        const out_shape = Shape.init(.f32, &.{
            @intCast(batch * out_h * out_w),
            @intCast(out_channels),
        });
        const id = try tc.graph.addNode(.{
            .op = .{
                .fused_conv2d = .{
                    .batch = @intCast(batch),
                    .in_channels = 0, // token grid layout
                    .out_channels = @intCast(out_channels),
                    .height = @intCast(height),
                    .width = @intCast(width),
                    .kernel_h = @intCast(kernel_h),
                    .kernel_w = @intCast(kernel_w),
                    .stride_h = @intCast(stride_h),
                    .stride_w = @intCast(stride_w),
                    .padding_h = @intCast(padding_h),
                    .padding_w = @intCast(padding_w),
                    .groups = 0, // token grid marker
                },
            },
            .output_shape = out_shape,
            .inputs = .{ nodeIdFromCT(input), nodeIdFromCT(weight), nodeIdFromCT(bias), null_node },
            .num_inputs = 3,
        });
        return tc.makeHandle(id);
    }

    // ── Linear pair / MoE / misc ────────────────────────────────────────

    fn linearNoBiasPairOp(ctx: *anyopaque, input: CT, weight_a: CT, weight_b: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!LinearNoBiasPairResult {
        const tc = fromCtx(ctx);
        const in_id = nodeIdFromCT(input);
        const wa_id = nodeIdFromCT(weight_a);
        const wb_id = nodeIdFromCT(weight_b);
        const out_shape = Shape.init(.f32, &.{ @intCast(rows), @intCast(out_dim) });
        const first_id = try tc.graph.addNode(.{
            .op = .{ .fused_linear_no_bias_pair = .{
                .rows = @intCast(rows),
                .in_dim = @intCast(in_dim),
                .out_dim = @intCast(out_dim),
            } },
            .output_shape = out_shape,
            .inputs = .{ in_id, wa_id, wb_id, null_node },
            .num_inputs = 3,
        });
        // Second output is modelled as a separate toFloat32 marker that the
        // interpreter resolves into the second result.  For graph purposes
        // the two outputs share the same fused node.
        const second_id = try tc.graph.addNode(.{
            .op = .{ .fused_to_float32 = {} },
            .output_shape = out_shape,
            .inputs = .{ first_id, null_node, null_node, null_node },
            .num_inputs = 1,
        });
        return .{
            .first = try tc.makeHandle(first_id),
            .second = try tc.makeHandle(second_id),
        };
    }

    fn takeRowsOp(ctx: *anyopaque, request: *const TakeRowsRequest) anyerror!?CT {
        const tc = fromCtx(ctx);
        const in_id = nodeIdFromCT(request.input);
        const out_shape = Shape.init(.f32, &.{ @intCast(request.rows), @intCast(request.dim) });
        const id = try tc.graph.addNode(.{
            .op = .{ .fused_take_rows = .{
                .rows = @intCast(request.rows),
                .dim = @intCast(request.dim),
            } },
            .output_shape = out_shape,
            .inputs = .{ in_id, tc.last_moe_select_routes, null_node, null_node },
            .num_inputs = 2,
        });
        return tc.makeHandle(id);
    }

    fn zeroTensorOp(ctx: *anyopaque, rows: usize, dim: usize) anyerror!?CT {
        const tc = fromCtx(ctx);
        const out_shape = Shape.init(.f32, &.{ @intCast(rows), @intCast(dim) });
        const id = try tc.graph.addNode(.{
            .op = .{ .fused_zero_tensor = .{
                .rows = @intCast(rows),
                .in_dim = 0,
                .out_dim = @intCast(dim),
            } },
            .output_shape = out_shape,
        });
        return tc.makeHandle(id);
    }

    fn moeLinearNoBiasOp(ctx: *anyopaque, request: *const MoeLinearNoBiasRequest) anyerror!?CT {
        const tc = fromCtx(ctx);
        const in_id = nodeIdFromCT(request.input);
        const w_id = nodeIdFromCT(request.weight);
        const out_shape = Shape.init(.f32, &.{ @intCast(request.rows), @intCast(request.out_dim) });
        const id = try tc.graph.addNode(.{
            .op = .{ .fused_moe_linear_no_bias = .{
                .rows = @intCast(request.rows),
                .in_dim = @intCast(request.in_dim),
                .out_dim = @intCast(request.out_dim),
            } },
            .output_shape = out_shape,
            .inputs = .{ in_id, w_id, tc.last_moe_select_routes, null_node },
            .num_inputs = 3,
        });
        return tc.makeHandle(id);
    }

    fn moeLinearNoBiasPairOp(ctx: *anyopaque, input: CT, _: []const u32, weight_a: CT, weight_b: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!?MoeLinearNoBiasPairResult {
        const tc = fromCtx(ctx);
        const in_id = nodeIdFromCT(input);
        const wa_id = nodeIdFromCT(weight_a);
        const wb_id = nodeIdFromCT(weight_b);
        const out_shape = Shape.init(.f32, &.{ @intCast(rows), @intCast(out_dim) });
        const first_id = try tc.graph.addNode(.{
            .op = .{ .fused_moe_linear_no_bias_pair = .{
                .rows = @intCast(rows),
                .in_dim = @intCast(in_dim),
                .out_dim = @intCast(out_dim),
            } },
            .output_shape = out_shape,
            .inputs = .{ in_id, wa_id, wb_id, tc.last_moe_select_routes },
            .num_inputs = 4,
        });
        const second_id = try tc.graph.addNode(.{
            .op = .{ .fused_to_float32 = {} },
            .output_shape = out_shape,
            .inputs = .{ first_id, null_node, null_node, null_node },
            .num_inputs = 1,
        });
        return .{
            .first = try tc.makeHandle(first_id),
            .second = try tc.makeHandle(second_id),
        };
    }

    fn moeScatterAddOp(ctx: *anyopaque, request: *const MoeScatterAddRequest) anyerror!?CT {
        const tc = fromCtx(ctx);
        const base_id = nodeIdFromCT(request.base);
        const updates_id = nodeIdFromCT(request.updates);
        const scale_id = tc.last_moe_expert_scale;
        const out_shape = Shape.init(.f32, &.{ @intCast(request.rows), @intCast(request.dim) });
        const id = try tc.graph.addNode(.{
            .op = .{ .fused_moe_scatter_add = .{
                .rows = @intCast(request.rows),
                .dim = @intCast(request.dim),
            } },
            .output_shape = out_shape,
            .inputs = .{ base_id, updates_id, tc.last_moe_select_routes, scale_id },
            .num_inputs = if (scale_id != null_node) 4 else 3,
        });
        // Reset per-layer scale so it doesn't leak to subsequent layers.
        tc.last_moe_expert_scale = null_node;
        return tc.makeHandle(id);
    }

    fn moeSelectRoutesOp(ctx: *anyopaque, logits: CT, rows: usize, num_experts: usize, top_k: usize, allocator: std.mem.Allocator) anyerror!?MoeRouteSelection {
        const tc = fromCtx(ctx);
        const in_id = nodeIdFromCT(logits);
        // Record the select_routes node so the graph captures MoE structure.
        // Save its ID so downstream MoE ops can reference it as a dependency.
        tc.last_moe_select_routes = try tc.graph.addNode(.{
            .op = .{ .fused_moe_select_routes = .{
                .rows = @intCast(rows),
                .num_experts = @intCast(num_experts),
                .top_k = @intCast(top_k),
            } },
            .output_shape = Shape.init(.f32, &.{ @intCast(rows), @intCast(num_experts) }),
            .inputs = .{ in_id, null_node, null_node, null_node },
            .num_inputs = 1,
        });
        // Return dummy routing: all tokens -> expert 0, weight = 1/top_k.
        // This lets the caller proceed through the grouped MoE path
        // (runGroupedExpertBatchTensor), tracing fused_moe_linear_no_bias
        // and fused_moe_scatter_add nodes. At interpretation time, the
        // interpreter calls the real backend's moeSelectRoutes for actual
        // routing decisions.
        const total = rows * top_k;
        const expert_ids = try allocator.alloc(u32, total);
        @memset(expert_ids, 0);
        const route_weights = try allocator.alloc(f32, total);
        const w: f32 = 1.0 / @as(f32, @floatFromInt(top_k));
        @memset(route_weights, w);
        return MoeRouteSelection{
            .expert_ids = expert_ids,
            .route_weights = route_weights,
            .rows = rows,
            .top_k = top_k,
        };
    }

    fn setMoeExpertScaleOp(ctx: *anyopaque, scale: CT) void {
        const tc = fromCtx(ctx);
        tc.last_moe_expert_scale = nodeIdFromCT(scale);
    }

    fn evalTensorOp(_: *anyopaque, _: CT) anyerror!void {
        // No-op during tracing — scheduling barrier, not computation.
    }

    fn argmaxLastRowOp(ctx: *anyopaque, tensor: CT, rows: usize, dim: usize) anyerror!?u32 {
        const tc = fromCtx(ctx);
        const in_id = nodeIdFromCT(tensor);
        // Record the op in the graph for completeness, but return null
        // to signal the caller should sample outside the graph (the
        // result is a scalar index, not a tensor).
        _ = try tc.graph.addNode(.{
            .op = .{ .fused_argmax_last_row = .{
                .rows = @intCast(rows),
                .dim = @intCast(dim),
            } },
            .output_shape = Shape.init(.i32, &.{1}),
            .inputs = .{ in_id, null_node, null_node, null_node },
            .num_inputs = 1,
        });
        return null;
    }

    // ── Data conversion ───────────────────────────────────────────────

    fn fromFloat32Op(ctx: *anyopaque, data: []const f32) anyerror!CT {
        const tc = fromCtx(ctx);
        const loc = try tc.graph.internConstant(data);
        const id = try tc.graph.addNode(.{
            .op = .{ .constant = .{ .data_offset = loc.offset, .data_len = loc.len } },
            .output_shape = Shape.init(.f32, &.{@intCast(data.len)}),
        });
        return tc.makeHandle(id);
    }

    fn fromFloat32ShapeOp(ctx: *anyopaque, data: []const f32, shape: []const i32) anyerror!CT {
        const tc = fromCtx(ctx);
        const loc = try tc.graph.internConstant(data);
        var dims: [8]i64 = @splat(0);
        for (shape, 0..) |d, i| {
            dims[i] = @intCast(d);
        }
        const s = Shape{
            .dtype = .f32,
            .dims = dims,
            .rank_ = @intCast(shape.len),
        };
        const id = try tc.graph.addNode(.{
            .op = .{ .constant = .{ .data_offset = loc.offset, .data_len = loc.len } },
            .output_shape = s,
        });
        return tc.makeHandle(id);
    }

    fn toFloat32Op(ctx: *anyopaque, tensor: CT, allocator: std.mem.Allocator) anyerror![]f32 {
        const tc = fromCtx(ctx);
        const node_id = nodeIdFromCT(tensor);
        // Mark as graph output
        try tc.graph.markOutput(node_id);
        // Return dummy zeros — tracing doesn't produce real values
        const n = tc.graph.node(node_id).output_shape.numElements() orelse 1;
        const result = try allocator.alloc(f32, @intCast(n));
        @memset(result, 0.0);
        return result;
    }
};

// ── Tests ──────────────────────────────────────────────────────────────

test "TracingCompute basic tracing" {
    const allocator = std.testing.allocator;
    var tc = try TracingCompute.initWithWeights(allocator, &.{
        .{ .name = "layer.weight", .shape = Shape.init(.f32, &.{ 3, 4 }) },
        .{ .name = "layer.bias", .shape = Shape.init(.f32, &.{3}) },
        .{ .name = "input", .shape = Shape.init(.f32, &.{ 2, 4 }) },
        .{ .name = "norm.weight", .shape = Shape.init(.f32, &.{3}) },
    });
    defer tc.deinit();

    var cb = tc.backend();

    // Trace: getWeight -> linear -> rmsNorm -> gelu -> add -> toFloat32
    const w = try cb.getWeight("layer.weight");
    const b = try cb.getWeight("layer.bias");
    const x = try cb.getWeight("input");
    const norm_w = try cb.getWeight("norm.weight");

    const y = try cb.linear(x, w, b, 2, 4, 3);
    const normed = try cb.rmsNorm(y, norm_w, 3, 1e-5);
    const activated = try cb.gelu(normed);
    const residual = try cb.add(x, activated);
    const result = try cb.toFloat32(residual, allocator);
    defer allocator.free(result);

    // Graph should have nodes
    const graph = tc.getGraph();
    try std.testing.expect(graph.nodeCount() > 0);

    // Should have outputs
    try std.testing.expectEqual(@as(usize, 1), graph.outputs.items.len);

    // Should have parameters
    try std.testing.expect(graph.parameters.items.len >= 4);
}

test "TracingCompute backend kind" {
    const allocator = std.testing.allocator;
    var tc = TracingCompute.init(allocator);
    defer tc.deinit();

    const cb = tc.backend();
    try std.testing.expectEqual(BackendKind.graph, cb.kind());
}

test "TracingCompute weight shapes" {
    const allocator = std.testing.allocator;
    var tc = try TracingCompute.initWithWeights(allocator, &.{
        .{ .name = "w", .shape = Shape.init(.f32, &.{ 3, 4 }) },
    });
    defer tc.deinit();

    var cb = tc.backend();
    const w = try cb.getWeight("w");
    const node_id = TracingCompute.nodeIdFromCT(w);
    const shape = tc.getGraph().node(node_id).output_shape;
    try std.testing.expect(shape.eq(Shape.init(.f32, &.{ 3, 4 })));
}

test "TracingCompute embeds non-runtime embedding ids as constants" {
    const allocator = std.testing.allocator;
    var tc = try TracingCompute.initWithWeights(allocator, &.{
        .{ .name = "tok", .shape = Shape.init(.f32, &.{ 8, 4 }) },
        .{ .name = "pos", .shape = Shape.init(.f32, &.{ 8, 4 }) },
    });
    defer tc.deinit();

    var cb = tc.backend();
    const token_ids = [_]i64{3};
    tc.setRuntimeEmbeddingIds(&token_ids);

    const tok_w = try cb.getWeight("tok");
    const tok = try cb.embeddingLookup(tok_w, &token_ids, 1, 4);
    const pos_w = try cb.getWeight("pos");
    const pos_ids = [_]i64{0};
    const pos = try cb.embeddingLookup(pos_w, &pos_ids, 1, 4);
    const sum = try cb.add(tok, pos);
    const result = try cb.toFloat32(sum, allocator);
    defer allocator.free(result);

    var runtime_placeholders: usize = 0;
    var constants: usize = 0;
    for (tc.getGraph().nodes.items) |node| {
        switch (node.op) {
            .fused_from_float32 => runtime_placeholders += 1,
            .constant => constants += 1,
            else => {},
        }
    }
    try std.testing.expectEqual(@as(usize, 1), runtime_placeholders);
    try std.testing.expectEqual(@as(usize, 1), constants);
}

test "TracingCompute weight resolver supplies traced dtype and shape" {
    const allocator = std.testing.allocator;
    const Resolver = struct {
        fn resolve(_: ?*anyopaque, _: std.mem.Allocator, name: []const u8) anyerror!?Shape {
            if (std.mem.eql(u8, name, "w")) return Shape.init(.f16, &.{ 2, 5 });
            return null;
        }
    };

    var tc = TracingCompute.initWithWeightResolver(allocator, .{
        .resolve = &Resolver.resolve,
    });
    defer tc.deinit();

    var cb = tc.backend();
    const w = try cb.getWeight("w");
    const node_id = TracingCompute.nodeIdFromCT(w);
    const shape = tc.getGraph().node(node_id).output_shape;
    try std.testing.expect(shape.eq(Shape.init(.f16, &.{ 2, 5 })));
}

test "TracingCompute weight resolver propagates missing weights" {
    const allocator = std.testing.allocator;
    const Resolver = struct {
        fn resolve(_: ?*anyopaque, _: std.mem.Allocator, _: []const u8) anyerror!?Shape {
            return null;
        }
    };

    var tc = TracingCompute.initWithWeightResolver(allocator, .{
        .resolve = &Resolver.resolve,
    });
    defer tc.deinit();

    var cb = tc.backend();
    try std.testing.expectError(error.MissingWeight, cb.getWeight("optional.weight"));
}

test "TracingCompute extractGraph transfers ownership" {
    const allocator = std.testing.allocator;
    var tc = TracingCompute.init(allocator);

    // Trace some ops so the graph is non-empty.
    var cb = tc.backend();
    const x = try cb.fromFloat32(&[_]f32{ 1.0, 2.0 });
    const y = try cb.gelu(x);
    const dummy = try cb.toFloat32(y, allocator);
    allocator.free(dummy);

    // Extract: caller now owns the graph.
    var extracted = tc.extractGraph();
    defer extracted.deinit();

    // The internal graph was replaced with an empty one, so deinit is safe.
    tc.deinit();

    // Extracted graph should have nodes and outputs.
    try std.testing.expect(extracted.nodeCount() > 0);
    try std.testing.expect(extracted.outputs.items.len > 0);
}
