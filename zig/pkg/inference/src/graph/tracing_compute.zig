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
    /// Cold immutable checkpoint tables only; runtime operands never use this.
    values: ?*const fn (?*anyopaque, std.mem.Allocator, []const u8) anyerror!?[]f32 = null,
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
    runtime_inputs: std.ArrayListUnmanaged(RuntimeInput) = .empty,
    /// Shared program capture cannot observe provisional host values.
    require_resident_outputs: bool = false,
    precision: @import("../execution/precision.zig").Policy = .f32,

    const RuntimeInput = struct {
        bytes: []const u8,
        shape: Shape,
        node: NodeId,
    };

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
        self.runtime_inputs.deinit(self.allocator);
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

    /// Declare a request tensor before invoking an ordinary model forward.
    /// The source bytes are borrowed only during capture. Repeated executions
    /// supply the parameter's current value rather than a captured constant.
    pub fn bindRuntimeInput(self: *TracingCompute, name: []const u8, shape: Shape, bytes: []const u8) !NodeId {
        const count = shape.numElements() orelse return error.UnsupportedShape;
        if (bytes.len != try std.math.mul(usize, @intCast(count), shape.dtype.byteSize())) return error.InvalidInputShape;
        for (self.runtime_inputs.items) |input| {
            if (std.mem.eql(u8, self.graph.parameterName(self.graph.node(input.node)), name)) return error.DuplicateProgramInput;
            if (bytes.len != 0 and input.bytes.ptr == bytes.ptr and input.bytes.len == bytes.len and input.shape.dtype == shape.dtype) return error.AmbiguousProgramInput;
        }
        try self.runtime_inputs.ensureUnusedCapacity(self.allocator, 1);
        var builder = Builder.init(&self.graph);
        const node = try builder.parameter(name, shape);
        self.runtime_inputs.appendAssumeCapacity(.{ .bytes = bytes, .shape = shape, .node = node });
        return node;
    }

    fn runtimeInput(self: *TracingCompute, bytes: []const u8, dtype: DType, shape: Shape) !?NodeId {
        if (bytes.len == 0) return null;
        for (self.runtime_inputs.items) |input| {
            if (input.shape.dtype != dtype or input.bytes.ptr != bytes.ptr or input.bytes.len != bytes.len) continue;
            if (input.shape.eq(shape)) return input.node;
            if (input.shape.numElements() != shape.numElements()) return error.InvalidInputShape;
            var builder = Builder.init(&self.graph);
            return try builder.reshape(input.node, shape);
        }
        return null;
    }

    /// Keep declared mask bytes as a typed operand; only undeclared metadata
    /// becomes a constant. Expanded attention layouts stay in the graph.
    fn maskVisibility(self: *TracingCompute, mask: []const i64, batch: usize, sequence: usize) !struct { node: NodeId, dynamic: bool } {
        if (mask.len != try std.math.mul(usize, batch, sequence)) return error.InvalidAttentionMask;
        for (mask) |bit| if (bit != 0 and bit != 1) return error.InvalidAttentionMask;
        const shape = Shape.init(.i64, &.{ @intCast(batch), @intCast(sequence) });
        const runtime = try self.runtimeInput(std.mem.sliceAsBytes(mask), .i64, shape);
        const value = runtime orelse try self.typedConstant(std.mem.sliceAsBytes(mask), shape);
        var builder = Builder.init(&self.graph);
        const zero = try self.broadcastNode(try builder.scalarConst(.i64, 0), &.{}, shape);
        return .{ .node = try self.lessNode(zero, value, shape), .dynamic = runtime != null };
    }

    pub fn markOutput(self: *TracingCompute, value: CT) !NodeId {
        var owned = false;
        for (self.handles.items) |handle| if (@as(CT, @ptrCast(handle)) == value) {
            owned = true;
            break;
        };
        if (!owned) return error.InvalidProgramBinding;
        const node = nodeIdFromCT(value);
        if (node >= self.graph.nodeCount()) return error.InvalidProgramBinding;
        try self.graph.markOutput(node);
        return node;
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
            .arithmetic_publication_dtype = if (self.precision == .bf16) .bf16 else .f32,
        };
    }

    // ── Handle management ─────────────────────────────────────────────

    fn makeHandle(self: *TracingCompute, node_id: NodeId) !CT {
        var value = node_id;
        const dtype = self.graph.node(value).output_shape.dtype;
        if (self.precision == .bf16 and (dtype == .f32 or dtype == .bf16)) {
            var builder = Builder.init(&self.graph);
            if (dtype == .f32) value = try builder.convertDtype(value, .bf16);
            // F32 arithmetic views fuse into their consumer; the explicit BF16
            // edge fixes storage/rounding without reducing sensitive arithmetic.
            value = try builder.convertDtype(value, .f32);
        }
        return self.makeHandleExact(value);
    }

    fn makeHandleExact(self: *TracingCompute, node_id: NodeId) !CT {
        const h = try self.allocator.create(TracingHandle);
        errdefer self.allocator.destroy(h);
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
        .acquireWeight = &getWeight,
        .prefetchWeightHint = &prefetchWeightHint,
        .drainPrefetchBudget = &drainPrefetchBudget,
        .embeddingLookup = &embeddingLookup,
        .linear = &linearOp,
        .linearNoBias = &linearNoBiasOp,
        .layerNorm = &layerNormOp,
        .rmsNorm = &rmsNormOp,
        .gelu = &geluOp,
        .geluExact = &geluExactOp,
        .relu = &reluOp,
        .silu = &siluOp,
        .quickGelu = &quickGeluOp,
        .sigmoid = &sigmoidOp,
        .tanh_act = &tanhActOp,
        .concat = &concatOp,
        .add = &addOp,
        .addMultiplyScalarTensor = &addMultiplyScalarTensorOp,
        .scaledDotProductAttention = &sdpaOp,
        .causalSelfAttention = &causalSelfAttentionOp,
        .crossAttention = &crossAttentionOp,
        .relativePositionBias = &relativePositionBiasOp,
        .disentangledRelativeAttention = &debertaOp,
        .disentangledRelativeAttentionBackward = &debertaBackwardOp,
        .windowedSelfAttention = &windowedSelfAttentionOp,
        .channelSelfAttention = &channelSelfAttentionOp,
        .segmentAttention = &segmentAttentionOp,
        .segmentAttentionGrouped = &segmentAttentionGroupedOp,
        .tokenGridConv2d = &tokenGridConv2dOp,
        .multiply = &multiplyOp,
        .conv1d = &conv1dOp,
        .conv2d = &conv2dOp,
        .channelLayerNormRelu = &channelNormReluOp,
        .flattenChannelsTimeFreq = &flattenChannelsOp,
        .gluRows = &gluRowsOp,
        .clampScalar = &clampScalarOp,
        .depthwiseCausalConv1d = &depthwiseCausalOp,
        .depthwiseGatedDelta = &depthwiseGatedDeltaOp,
        .regions = .{ .audio_attention = .{ .localRelative = &localRelativeOp } },
        .rope = &ropeOp,
        .ropePerItem = &ropePerItemOp,
        .visionRope = &visionRopeOp,
        .mrope = &mropeOp,
        .gqaCausalAttention = &gqaCausalAttentionOp,
        .gqaPagedAttention = &gqaPagedAttentionOp,
        .fromFloat32 = &fromFloat32Op,
        .fromFloat32Shape = &fromFloat32ShapeOp,
        .toFloat32 = &toFloat32Op,
        .multiplyScalar = &multiplyScalarOp,
        .maskedMeanBatch = &maskedMeanBatchOp,
        .tensorShape = &tensorShapeOp,
        .tensorShapeMatches = &tensorShapeMatchesOp,
        .tensorDType = &tensorDTypeOp,
        .convertDType = &convertDTypeOp,
        .takeRows = &takeRowsOp,
        .zeroTensor = &zeroTensorOp,
        .reshape2d = &reshape2dOp,
        .cloneTensorShape = &cloneTensorShapeOp,
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
        .negate = &primitiveNegate,
        .sqrtOp = &primitiveSqrtOp,
        .rsqrtOp = &primitiveRsqrtOp,
        .expOp = &primitiveExpOp,
        .logOp = &primitiveLogOp,
        .sinOp = &primitiveSinOp,
        .cosOp = &primitiveCosOp,
        .tanhOp = &primitiveTanhOp,
        .erfOp = &primitiveErfOp,
        .absOp = &primitiveAbsOp,
        .subtract = &primitiveSubtract,
        .divide = &primitiveDivide,
        .reduceSumOp = &primitiveReduceSumOp,
        .reduceMaxOp = &primitiveReduceMaxOp,
        .reduceMeanOp = &primitiveReduceMeanOp,
        .sliceRows2D = &sliceRows2DOp,
        .fromInt32Shape = &fromInt32ShapeOp,
        .fromConstantBytes = &fromConstantBytesOp,
        .gatherOp = &primitiveGather,
        .lessThan = &primitiveLessThan,
        .whereSelect = &primitiveWhereSelect,
        .reshapeOp = &primitiveReshape,
        .transposeOp = &primitiveTranspose,
        .broadcastInDimOp = &primitiveBroadcast,
        .concatPrimOp = &primitiveConcat,
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

    fn primitiveConcat(ctx: *anyopaque, a: CT, b: CT, axis: u8, a_dims: []const i64, b_dims: []const i64) anyerror!CT {
        const tc = fromCtx(ctx);
        const left = nodeIdFromCT(a);
        const right = nodeIdFromCT(b);
        const a_shape = tc.graph.node(left).output_shape;
        const b_shape = tc.graph.node(right).output_shape;
        if (!std.mem.eql(i64, a_shape.dims[0..a_shape.rank()], a_dims) or !std.mem.eql(i64, b_shape.dims[0..b_shape.rank()], b_dims)) return error.InvalidInputShape;
        var builder = Builder.init(&tc.graph);
        return tc.makeHandleExact(try builder.concat(left, right, axis));
    }

    fn primitiveNegate(ctx: *anyopaque, input: CT) anyerror!CT {
        const tc = fromCtx(ctx);
        var builder = Builder.init(&tc.graph);
        return tc.makeHandle(try builder.neg(nodeIdFromCT(input)));
    }

    fn primitiveSqrtOp(ctx: *anyopaque, input: CT) anyerror!CT {
        const tc = fromCtx(ctx);
        var builder = Builder.init(&tc.graph);
        return tc.makeHandle(try builder.sqrt(nodeIdFromCT(input)));
    }

    fn primitiveRsqrtOp(ctx: *anyopaque, input: CT) anyerror!CT {
        const tc = fromCtx(ctx);
        var builder = Builder.init(&tc.graph);
        return tc.makeHandle(try builder.rsqrt(nodeIdFromCT(input)));
    }

    fn primitiveExpOp(ctx: *anyopaque, input: CT) anyerror!CT {
        const tc = fromCtx(ctx);
        var builder = Builder.init(&tc.graph);
        return tc.makeHandle(try builder.expOp(nodeIdFromCT(input)));
    }

    fn primitiveLogOp(ctx: *anyopaque, input: CT) anyerror!CT {
        const tc = fromCtx(ctx);
        var builder = Builder.init(&tc.graph);
        return tc.makeHandle(try builder.logOp(nodeIdFromCT(input)));
    }

    fn primitiveSinOp(ctx: *anyopaque, input: CT) anyerror!CT {
        const tc = fromCtx(ctx);
        var builder = Builder.init(&tc.graph);
        return tc.makeHandle(try builder.sinOp(nodeIdFromCT(input)));
    }

    fn primitiveCosOp(ctx: *anyopaque, input: CT) anyerror!CT {
        const tc = fromCtx(ctx);
        var builder = Builder.init(&tc.graph);
        return tc.makeHandle(try builder.cosOp(nodeIdFromCT(input)));
    }

    fn primitiveTanhOp(ctx: *anyopaque, input: CT) anyerror!CT {
        const tc = fromCtx(ctx);
        var builder = Builder.init(&tc.graph);
        return tc.makeHandle(try builder.tanhOp(nodeIdFromCT(input)));
    }

    fn primitiveErfOp(ctx: *anyopaque, input: CT) anyerror!CT {
        const tc = fromCtx(ctx);
        var builder = Builder.init(&tc.graph);
        return tc.makeHandle(try builder.erfOp(nodeIdFromCT(input)));
    }

    fn primitiveAbsOp(ctx: *anyopaque, input: CT) anyerror!CT {
        const tc = fromCtx(ctx);
        var builder = Builder.init(&tc.graph);
        return tc.makeHandle(try builder.absOp(nodeIdFromCT(input)));
    }

    fn primitiveSubtract(ctx: *anyopaque, a: CT, b: CT) anyerror!CT {
        const tc = fromCtx(ctx);
        var builder = Builder.init(&tc.graph);
        return tc.makeHandle(try builder.sub(nodeIdFromCT(a), nodeIdFromCT(b)));
    }

    fn primitiveDivide(ctx: *anyopaque, a: CT, b: CT) anyerror!CT {
        const tc = fromCtx(ctx);
        var builder = Builder.init(&tc.graph);
        return tc.makeHandle(try builder.div(nodeIdFromCT(a), nodeIdFromCT(b)));
    }

    fn primitiveReduceSumOp(ctx: *anyopaque, input: CT, axes: []const u8, input_shape: []const i64) anyerror!CT {
        const tc = fromCtx(ctx);
        const shape = try tc.checkedInputShape(input, input_shape);
        var seen: [8]bool = @splat(false);
        for (axes) |axis| {
            if (axis >= shape.rank() or seen[axis]) return error.InvalidInputShape;
            seen[axis] = true;
        }
        var builder = Builder.init(&tc.graph);
        return tc.makeHandle(try builder.reduceSum(nodeIdFromCT(input), axes));
    }

    fn primitiveReduceMaxOp(ctx: *anyopaque, input: CT, axes: []const u8, input_shape: []const i64) anyerror!CT {
        const tc = fromCtx(ctx);
        const shape = try tc.checkedInputShape(input, input_shape);
        var seen: [8]bool = @splat(false);
        for (axes) |axis| {
            if (axis >= shape.rank() or seen[axis]) return error.InvalidInputShape;
            seen[axis] = true;
        }
        var builder = Builder.init(&tc.graph);
        return tc.makeHandle(try builder.reduceMax(nodeIdFromCT(input), axes));
    }

    fn primitiveReduceMeanOp(ctx: *anyopaque, input: CT, axes: []const u8, input_shape: []const i64) anyerror!CT {
        const tc = fromCtx(ctx);
        const shape = try tc.checkedInputShape(input, input_shape);
        var seen: [8]bool = @splat(false);
        for (axes) |axis| {
            if (axis >= shape.rank() or seen[axis]) return error.InvalidInputShape;
            seen[axis] = true;
        }
        var builder = Builder.init(&tc.graph);
        return tc.makeHandle(try builder.reduceMean(nodeIdFromCT(input), axes));
    }

    fn sliceRows2DOp(ctx: *anyopaque, input: CT, start: usize, rows: usize, cols: usize) anyerror!CT {
        const tc = fromCtx(ctx);
        const source = nodeIdFromCT(input);
        const shape = tc.graph.node(source).output_shape;
        if (shape.rank() != 2) return error.InvalidInputShape;
        const available = std.math.cast(usize, shape.dim(0)) orelse return error.InvalidInputShape;
        if (shape.dim(1) != cols or start > available or rows > available - start) return error.InvalidInputShape;
        var attrs = ml.graph.node.SliceAttrs{};
        attrs.num_axes = 2;
        attrs.starts[0] = @intCast(start);
        attrs.limits[0] = @intCast(start + rows);
        attrs.limits[1] = @intCast(cols);
        return tc.makeHandle(try tc.graph.addNode(.{ .op = .{ .slice = attrs }, .output_shape = Shape.init(shape.dtype, &.{ @intCast(rows), @intCast(cols) }), .inputs = .{ source, null_node, null_node, null_node }, .num_inputs = 1 }));
    }

    fn fromConstantBytesOp(ctx: *anyopaque, data: []const u8, dtype: DType, dims: []const i64) anyerror!?CT {
        const tc = fromCtx(ctx);
        if (dims.len > 8) return error.UnsupportedShape;
        const shape = Shape.init(dtype, dims);
        const count = shape.numElements() orelse return error.InvalidInputShape;
        if (data.len != try std.math.mul(usize, @intCast(count), dtype.byteSize())) return error.InvalidInputShape;
        if (try tc.runtimeInput(data, dtype, shape)) |node| return try tc.makeHandle(node);
        const loc = try tc.graph.internConstantBytes(data, dtype);
        return try tc.makeHandle(try tc.graph.addNode(.{ .op = .{ .constant = .{ .data_offset = loc.offset, .data_len = loc.len } }, .output_shape = shape }));
    }

    fn primitiveGather(ctx: *anyopaque, input: CT, indices: CT, axis: u8, dims: []const i64) anyerror!CT {
        const tc = fromCtx(ctx);
        const shape = try tc.checkedInputShape(input, dims);
        const index_shape = tc.graph.node(nodeIdFromCT(indices)).output_shape;
        if (axis != 0 or shape.rank() == 0 or (index_shape.dtype != .i64 and index_shape.dtype != .i32) or index_shape.rank() + shape.rank() - 1 > 8) return error.UnsupportedShape;
        var result: [8]i64 = undefined;
        @memcpy(result[0..index_shape.rank()], index_shape.dims[0..index_shape.rank()]);
        @memcpy(result[index_shape.rank()..][0 .. shape.rank() - 1], shape.dims[1..shape.rank()]);
        var builder = Builder.init(tc.getGraphMut());
        return tc.makeHandle(try builder.gather(nodeIdFromCT(input), nodeIdFromCT(indices), Shape.init(shape.dtype, result[0 .. index_shape.rank() + shape.rank() - 1])));
    }

    fn primitiveLessThan(ctx: *anyopaque, a: CT, b: CT) anyerror!CT {
        const tc = fromCtx(ctx);
        const lhs = tc.graph.node(nodeIdFromCT(a)).output_shape;
        const rhs = tc.graph.node(nodeIdFromCT(b)).output_shape;
        if (!lhs.eq(rhs)) return error.UnsupportedComparisonBroadcast;
        return tc.makeHandle(try tc.graph.addNode(.{ .op = .{ .less_than = {} }, .output_shape = Shape.init(.bool_, lhs.dims[0..lhs.rank()]), .inputs = .{ nodeIdFromCT(a), nodeIdFromCT(b), null_node, null_node }, .num_inputs = 2 }));
    }

    fn primitiveWhereSelect(ctx: *anyopaque, condition: CT, yes: CT, no: CT) anyerror!CT {
        const tc = fromCtx(ctx);
        const shape = tc.graph.node(nodeIdFromCT(yes)).output_shape;
        const no_shape = tc.graph.node(nodeIdFromCT(no)).output_shape;
        const predicate = tc.graph.node(nodeIdFromCT(condition)).output_shape;
        if (shape.dtype != no_shape.dtype) return error.UnsupportedSelectionBroadcast;
        const rank = @max(@max(shape.rank(), no_shape.rank()), predicate.rank());
        var dimensions: [8]i64 = @splat(1);
        for ([_]Shape{ shape, no_shape, predicate }) |source| {
            for (0..source.rank()) |axis| {
                const size = source.dim(@intCast(axis));
                const output_axis = rank - source.rank() + axis;
                if (size < 0 or (size != dimensions[output_axis] and size != 1 and dimensions[output_axis] != 1)) return error.UnsupportedSelectionBroadcast;
                if (dimensions[output_axis] == 1) dimensions[output_axis] = size;
            }
        }
        const result_shape = Shape.init(shape.dtype, dimensions[0..rank]);
        return tc.makeHandle(try tc.graph.addNode(.{ .op = .{ .where_select = {} }, .output_shape = result_shape, .inputs = .{ nodeIdFromCT(condition), nodeIdFromCT(yes), nodeIdFromCT(no), null_node }, .num_inputs = 3 }));
    }

    fn fromInt32ShapeOp(ctx: *anyopaque, data: []const i32, dims: []const i32) anyerror!?CT {
        const tc = fromCtx(ctx);
        if (dims.len > 8) return error.UnsupportedShape;
        var shape_dims: [8]i64 = undefined;
        for (dims, 0..) |dim, axis| shape_dims[axis] = dim;
        const shape = Shape.init(.i32, shape_dims[0..dims.len]);
        const count = shape.numElements() orelse return error.InvalidInputShape;
        if (std.math.cast(usize, count) != data.len) return error.InvalidInputShape;
        if (try tc.runtimeInput(std.mem.sliceAsBytes(data), .i32, shape)) |node| return try tc.makeHandle(node);
        const loc = try tc.graph.internConstantBytes(std.mem.sliceAsBytes(data), .i32);
        return try tc.makeHandle(try tc.graph.addNode(.{ .op = .{ .constant = .{ .data_offset = loc.offset, .data_len = loc.len } }, .output_shape = shape }));
    }

    fn checkedInputShape(tc: *TracingCompute, input: CT, dims: []const i64) !Shape {
        const shape = tc.graph.node(nodeIdFromCT(input)).output_shape;
        if (!std.mem.eql(i64, shape.dims[0..shape.rank()], dims)) return error.InvalidInputShape;
        return shape;
    }

    fn primitiveReshape(ctx: *anyopaque, input: CT, dims: []const i64) anyerror!CT {
        const tc = fromCtx(ctx);
        if (dims.len > 8) return error.UnsupportedShape;
        const original = tc.graph.node(nodeIdFromCT(input)).output_shape;
        const shape = Shape.init(original.dtype, dims);
        const count = shape.numElements() orelse return error.UnsupportedShape;
        if (count != (original.numElements() orelse return error.UnsupportedShape)) return error.InvalidInputShape;
        var builder = Builder.init(&tc.graph);
        return tc.makeHandle(try builder.reshape(nodeIdFromCT(input), shape));
    }

    fn primitiveTranspose(ctx: *anyopaque, input: CT, perm: []const u8, dims: []const i64) anyerror!CT {
        const tc = fromCtx(ctx);
        const shape = try tc.checkedInputShape(input, dims);
        if (perm.len != shape.rank()) return error.InvalidInputShape;
        var seen: [8]bool = @splat(false);
        for (perm) |axis| {
            if (axis >= shape.rank() or seen[axis]) return error.InvalidInputShape;
            seen[axis] = true;
        }
        var builder = Builder.init(&tc.graph);
        return tc.makeHandle(try builder.transpose(nodeIdFromCT(input), perm));
    }

    fn primitiveBroadcast(ctx: *anyopaque, input: CT, target: []const i64, axes: []const u8, dims: []const i64) anyerror!CT {
        const tc = fromCtx(ctx);
        const original = try tc.checkedInputShape(input, dims);
        if (target.len > 8 or axes.len != original.rank()) return error.InvalidInputShape;
        const shape = Shape.init(original.dtype, target);
        _ = shape.numElements() orelse return error.UnsupportedShape;
        var seen: [8]bool = @splat(false);
        for (axes, 0..) |axis, i| {
            if (axis >= target.len or seen[axis]) return error.InvalidInputShape;
            if (dims[i] != 1 and dims[i] != target[axis]) return error.InvalidInputShape;
            seen[axis] = true;
        }
        return tc.makeHandle(try tc.broadcastNode(nodeIdFromCT(input), axes, shape));
    }

    fn broadcastNode(tc: *TracingCompute, value: NodeId, axes: []const u8, shape: Shape) !NodeId {
        var attrs = ml.graph.node.BroadcastAttrs{ .target_shape = shape, .num_axes = @intCast(axes.len) };
        @memcpy(attrs.broadcast_axes[0..axes.len], axes);
        return tc.graph.addNode(.{ .op = .{ .broadcast_in_dim = attrs }, .output_shape = shape, .inputs = .{ value, null_node, null_node, null_node }, .num_inputs = 1 });
    }

    fn maskedMeanBatchOp(ctx: *anyopaque, input: CT, mask: ?[]const i64, batch: usize, sequence: usize, width: usize, dimensions: usize, normalize: bool) anyerror!?CT {
        const tc = fromCtx(ctx);
        if (batch == 0 or sequence == 0 or width == 0 or dimensions == 0 or dimensions > width) return error.InvalidInputShape;
        const rows = try std.math.mul(usize, batch, sequence);
        if (mask) |values| if (values.len != rows) return error.InvalidInputShape;
        if (mask) |values| for (0..batch) |row| {
            var active: usize = 0;
            for (values[row * sequence ..][0..sequence]) |bit| {
                if (bit != 0 and bit != 1) return error.InvalidAttentionMask;
                active += @intFromBool(bit != 0);
            }
            if (active == 0) return error.EmptyEmbeddingInput;
        };
        var builder = Builder.init(&tc.graph);
        const mean_shape = Shape.init(.f32, &.{ @intCast(batch), @intCast(width) });
        // A single already-pooled vector needs no mask, sum, count or division
        // by one. Keep normalization's F32 max/sum boundaries unchanged.
        var result = if (mask == null and sequence == 1)
            try builder.reshape(nodeIdFromCT(input), mean_shape)
        else pool: {
            const shape3 = Shape.init(.f32, &.{ @intCast(batch), @intCast(sequence), @intCast(width) });
            const shaped = try builder.reshape(nodeIdFromCT(input), shape3);
            const flags_shape = Shape.init(.f32, &.{ @intCast(batch), @intCast(sequence) });
            const comparison = if (mask) |values| (try tc.maskVisibility(values, batch, sequence)).node else try tc.broadcastNode(try tc.typedConstant(&.{1}, Shape.init(.bool_, &.{})), &.{}, Shape.init(.bool_, flags_shape.dims[0..flags_shape.rank()]));
            const flags_node = try builder.convertDtype(comparison, .f32);
            const zero = try builder.scalarConst(.f32, 0);
            const selected = try tc.graph.addNode(.{ .op = .{ .where_select = {} }, .output_shape = shape3, .inputs = .{ try tc.broadcastNode(comparison, &.{ 0, 1 }, Shape.init(.bool_, shape3.dims[0..shape3.rank()])), shaped, try tc.broadcastNode(zero, &.{}, shape3), null_node }, .num_inputs = 3 });
            const sums = try builder.reshape(try builder.reduceSum(selected, &.{1}), Shape.init(.f32, &.{ @intCast(batch), @intCast(width) }));
            const count_node = try builder.reshape(try builder.reduceSum(flags_node, &.{1}), Shape.init(.f32, &.{@intCast(batch)}));
            break :pool try builder.div(sums, try tc.broadcastNode(count_node, &.{0}, mean_shape));
        };
        if (dimensions != width) result = try builder.sliceLastDim(result, 0, @intCast(dimensions));
        if (normalize) {
            const shape = Shape.init(.f32, &.{ @intCast(batch), @intCast(dimensions) });
            const absolute = try builder.absOp(result);
            const maximum = try builder.reshape(try builder.reduceMax(absolute, &.{1}), Shape.init(.f32, &.{@intCast(batch)}));
            result = try builder.div(result, try tc.broadcastNode(maximum, &.{0}, shape));
            const norms = try builder.reshape(try builder.sqrt(try builder.reduceSum(try builder.mul(result, result), &.{1})), Shape.init(.f32, &.{@intCast(batch)}));
            result = try builder.div(result, try tc.broadcastNode(norms, &.{0}, shape));
        }
        return try tc.makeHandleExact(result);
    }

    fn tensorShapeOp(ctx: *anyopaque, value: CT, allocator: std.mem.Allocator) anyerror![]i64 {
        const shape = fromCtx(ctx).graph.node(nodeIdFromCT(value)).output_shape;
        return allocator.dupe(i64, shape.dims[0..shape.rank()]);
    }

    fn tensorDTypeOp(ctx: *anyopaque, value: CT) anyerror!@import("../backends/tensor.zig").DType {
        const dtype = fromCtx(ctx).graph.node(nodeIdFromCT(value)).output_shape.dtype;
        return switch (dtype) {
            inline else => |tag| @field(@import("../backends/tensor.zig").DType, @tagName(tag)),
        };
    }

    fn tensorShapeMatchesOp(ctx: *anyopaque, value: CT, dims: []const i64) anyerror!?bool {
        const shape = fromCtx(ctx).graph.node(nodeIdFromCT(value)).output_shape;
        return std.mem.eql(i64, shape.dims[0..shape.rank()], dims);
    }

    fn convertDTypeOp(ctx: *anyopaque, value: CT, target: ops.GraphDType) anyerror!?CT {
        const tc = fromCtx(ctx);
        var builder = Builder.init(&tc.graph);
        return try tc.makeHandleExact(try builder.convertDtype(nodeIdFromCT(value), target));
    }

    fn multiplyScalarOp(ctx: *anyopaque, input: CT, scale: f32) anyerror!?CT {
        const tc = fromCtx(ctx);
        var builder = Builder.init(&tc.graph);
        const scalar = try builder.scalarConst(.f32, scale);
        return try tc.makeHandle(try builder.mul(nodeIdFromCT(input), scalar));
    }

    // ── Weight / parameter ops ────────────────────────────────────────

    fn getWeight(ctx: *anyopaque, name: []const u8) anyerror!CT {
        const tc = fromCtx(ctx);
        for (tc.runtime_inputs.items) |input| {
            if (std.mem.eql(u8, tc.graph.parameterName(tc.graph.node(input.node)), name)) return error.ProgramBindingNameCollision;
        }
        if (tc.weight_shapes.get(name) == null) {
            if (tc.weight_shape_resolver) |resolver| {
                if (try resolver.resolve(resolver.context, tc.allocator, name)) |shape| {
                    try tc.putWeightShape(name, shape);
                } else {
                    return error.MissingWeight;
                }
            }
        }
        var shape = tc.weight_shapes.get(name) orelse tc.default_weight_shape;
        if (shape.dtype == .f32 or shape.dtype == .bf16 or shape.dtype == .f16) shape.dtype = if (tc.precision == .bf16) .bf16 else .f32;
        var b = Builder.init(tc.getGraphMut());
        const id = try b.parameter(name, shape);
        return tc.makeHandle(id);
    }

    fn embeddingLookup(ctx: *anyopaque, weight: CT, ids: []const i64, total: usize, dim: usize) anyerror!CT {
        const tc = fromCtx(ctx);
        var w_id = nodeIdFromCT(weight);
        if (ids.len < total) return error.UnsupportedShape;
        const idx_shape = Shape.init(.i64, &.{@intCast(total)});
        const idx_id = if (try tc.runtimeInput(std.mem.sliceAsBytes(ids[0..total]), .i64, idx_shape)) |node| node else if (tc.isRuntimeEmbeddingIds(ids[0..total])) blk: {
            break :blk try tc.graph.addNode(.{
                .op = .{ .fused_from_float32 = {} },
                .output_shape = idx_shape,
            });
        } else blk: {
            const loc = try tc.graph.internConstantBytes(std.mem.sliceAsBytes(ids[0..total]), .i64);
            break :blk try tc.graph.addNode(.{
                .op = .{ .constant = .{ .data_offset = loc.offset, .data_len = loc.len } },
                .output_shape = idx_shape,
            });
        };
        var b = Builder.init(tc.getGraphMut());
        if (tc.require_resident_outputs) {
            // Eager lookup treats checkpoint storage as a contiguous row table.
            // Axis/position tables may carry extra leading dimensions.
            const shape = tc.graph.node(w_id).output_shape;
            const elements = shape.numElements() orelse return error.UnsupportedShape;
            if (dim == 0 or elements <= 0 or @mod(elements, @as(i64, @intCast(dim))) != 0) return error.InvalidInputShape;
            if (shape.rank() != 2 or shape.dim(1) != dim) w_id = try b.reshape(w_id, Shape.init(shape.dtype, &.{ @divExact(elements, @as(i64, @intCast(dim))), @intCast(dim) }));
        }
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

    fn geluExactOp(ctx: *anyopaque, input: CT) anyerror!CT {
        var tc = fromCtx(ctx);
        var b = Builder.init(tc.getGraphMut());
        const id = try b.geluExact(nodeIdFromCT(input));
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

    fn addMultiplyScalarTensorOp(ctx: *anyopaque, a: CT, b: CT, scalar: CT) anyerror!?CT {
        var tc = fromCtx(ctx);
        var bld = Builder.init(tc.getGraphMut());
        const id = try bld.addMulScalar(nodeIdFromCT(a), nodeIdFromCT(b), nodeIdFromCT(scalar));
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

    fn typedConstant(tc: *TracingCompute, bytes: []const u8, shape: Shape) !NodeId {
        const count = shape.numElements() orelse return error.UnsupportedShape;
        if (bytes.len != try std.math.mul(usize, @intCast(count), shape.dtype.byteSize())) return error.InvalidInputShape;
        const loc = try tc.graph.internConstantBytes(bytes, shape.dtype);
        return tc.graph.addNode(.{ .op = .{ .constant = .{ .data_offset = loc.offset, .data_len = loc.len } }, .output_shape = shape });
    }
    fn selectNode(tc: *TracingCompute, condition: NodeId, yes: NodeId, no: NodeId, shape: Shape) !NodeId {
        return tc.graph.addNode(.{ .op = .{ .where_select = {} }, .output_shape = shape, .inputs = .{ condition, yes, no, null_node }, .num_inputs = 3 });
    }
    fn lessNode(tc: *TracingCompute, a: NodeId, b: NodeId, shape: Shape) !NodeId {
        return tc.graph.addNode(.{ .op = .{ .less_than = {} }, .output_shape = Shape.init(.bool_, shape.dims[0..shape.rank()]), .inputs = .{ a, b, null_node, null_node }, .num_inputs = 2 });
    }
    fn attentionHeads(tc: *TracingCompute, input: CT, batch: usize, sequence: usize, heads: usize, dim: usize, token_major: bool) !NodeId {
        var builder = Builder.init(&tc.graph);
        const source = nodeIdFromCT(input);
        const shape = tc.graph.node(source).output_shape;
        const count = try std.math.mul(usize, try std.math.mul(usize, batch, sequence), try std.math.mul(usize, heads, dim));
        if (shape.numElements() != std.math.cast(i64, count)) return error.InvalidAttentionShape;
        const layout = if (token_major) blk: {
            const tokens = try builder.reshape(source, Shape.init(.f32, &.{ @intCast(batch), @intCast(sequence), @intCast(heads), @intCast(dim) }));
            break :blk try builder.transpose(tokens, &.{ 0, 2, 1, 3 });
        } else source;
        return builder.reshape(layout, Shape.init(.f32, &.{ @intCast(try std.math.mul(usize, batch, heads)), @intCast(sequence), @intCast(dim) }));
    }
    /// Capture the ordinary dense attention definition without dropping its
    /// mask or bias. Mask metadata is linear in tokens, never a host Q*K array.
    fn denseAttention(tc: *TracingCompute, Q: CT, K: CT, V: CT, mask: []const i64, bias: ?CT, batch: usize, queries: usize, keys: usize, heads: usize, dim: usize, causal: bool, token_major: bool) !CT {
        if (batch == 0 or queries == 0 or keys == 0 or heads == 0 or dim == 0) return error.InvalidAttentionShape;
        const bh = try std.math.mul(usize, batch, heads);
        const mask_count = try std.math.mul(usize, batch, keys);
        if (mask.len != 0 and mask.len != mask_count) return error.InvalidAttentionMask;
        var builder = Builder.init(&tc.graph);
        const query = try tc.attentionHeads(Q, batch, queries, heads, dim, token_major);
        const key = try tc.attentionHeads(K, batch, keys, heads, dim, token_major);
        var value = try tc.attentionHeads(V, batch, keys, heads, dim, token_major);
        const score_shape = Shape.init(.f32, &.{ @intCast(bh), @intCast(queries), @intCast(keys) });
        var score = try builder.matmul3D(query, try builder.transpose(key, &.{ 0, 2, 1 }));
        score = try builder.mul(score, try builder.scalarConst(.f32, 1 / @sqrt(@as(f32, @floatFromInt(dim)))));
        if (bias) |tensor| {
            const source = nodeIdFromCT(tensor);
            const count = tc.graph.node(source).output_shape.numElements() orelse return error.UnsupportedAttentionBias;
            const per_head = try std.math.mul(usize, queries, keys);
            const per_batch = try std.math.mul(usize, heads, per_head);
            const normalized = if (count == try std.math.mul(usize, bh, per_head)) try builder.reshape(source, score_shape) else if (count == per_batch) shared: {
                const shaped = try builder.reshape(source, Shape.init(.f32, &.{ @intCast(heads), @intCast(queries), @intCast(keys) }));
                const expanded = try tc.broadcastNode(shaped, &.{ 1, 2, 3 }, Shape.init(.f32, &.{ @intCast(batch), @intCast(heads), @intCast(queries), @intCast(keys) }));
                break :shared try builder.reshape(expanded, score_shape);
            } else if (count == per_head) shared: {
                const shaped = try builder.reshape(source, Shape.init(.f32, &.{ @intCast(queries), @intCast(keys) }));
                break :shared try tc.broadcastNode(shaped, &.{ 1, 2 }, score_shape);
            } else return error.UnsupportedAttentionBias;
            score = try builder.add(score, normalized);
        }
        const negative = if (mask.len != 0 or causal) try tc.broadcastNode(try builder.scalarConst(.f32, -std.math.inf(f32)), &.{}, score_shape) else null_node;
        var empty_row = false;
        if (mask.len != 0) {
            const visibility = try tc.maskVisibility(mask, batch, keys);
            for (0..batch) |item| {
                var active: usize = 0;
                for (mask[item * keys ..][0..keys]) |valid| active += @intFromBool(valid != 0);
                empty_row = empty_row or active == 0;
            }
            // Dynamic masks can introduce an empty row after capture.
            empty_row = empty_row or visibility.dynamic;
            const expanded = try tc.broadcastNode(visibility.node, &.{ 0, 2 }, Shape.init(.bool_, &.{ @intCast(batch), @intCast(heads), @intCast(keys) }));
            const visible = try builder.reshape(expanded, Shape.init(.bool_, &.{ @intCast(bh), @intCast(keys) }));
            score = try tc.selectNode(try tc.broadcastNode(visible, &.{ 0, 2 }, Shape.init(.bool_, score_shape.dims[0..score_shape.rank()])), score, negative, score_shape);
            const value_shape = tc.graph.node(value).output_shape;
            const value_zero = try tc.broadcastNode(try builder.scalarConst(.f32, 0), &.{}, value_shape);
            value = try tc.selectNode(try tc.broadcastNode(visible, &.{ 0, 1 }, Shape.init(.bool_, value_shape.dims[0..value_shape.rank()])), value, value_zero, value_shape);
        }
        if (causal) {
            const columns = try tc.allocator.alloc(i32, keys);
            defer tc.allocator.free(columns);
            const upper = try tc.allocator.alloc(i32, queries);
            defer tc.allocator.free(upper);
            for (columns, 0..) |*column, index| column.* = std.math.cast(i32, index) orelse return error.UnsupportedShape;
            for (upper, 0..) |*limit, index| limit.* = std.math.cast(i32, index + 1) orelse return error.UnsupportedShape;
            const integer_shape = Shape.init(.i32, score_shape.dims[0..score_shape.rank()]);
            const column = try tc.broadcastNode(try tc.typedConstant(std.mem.sliceAsBytes(columns), Shape.init(.i32, &.{@intCast(keys)})), &.{2}, integer_shape);
            const limit = try tc.broadcastNode(try tc.typedConstant(std.mem.sliceAsBytes(upper), Shape.init(.i32, &.{@intCast(queries)})), &.{1}, integer_shape);
            score = try tc.selectNode(try tc.lessNode(column, limit, integer_shape), score, negative, score_shape);
        }
        const probabilities = if (!empty_row and bias == null) try builder.softmax(score) else safe: {
            const reduced_shape = Shape.init(.f32, &.{ @intCast(bh), @intCast(queries), 1 });
            const maximum = try builder.reduceMax(score, &.{2});
            const minimum = try tc.broadcastNode(try builder.scalarConst(.f32, -std.math.floatMax(f32)), &.{}, reduced_shape);
            const safe_maximum = try tc.selectNode(try tc.lessNode(maximum, minimum, reduced_shape), minimum, maximum, reduced_shape);
            const exponentials = try builder.expOp(try builder.sub(score, try tc.broadcastNode(safe_maximum, &.{ 0, 1, 2 }, score_shape)));
            const sums = try builder.reduceSum(exponentials, &.{2});
            const one = try tc.broadcastNode(try builder.scalarConst(.f32, 1), &.{}, reduced_shape);
            const denominator = try tc.selectNode(try tc.lessNode(sums, one, reduced_shape), one, sums, reduced_shape);
            break :safe try builder.div(exponentials, try tc.broadcastNode(denominator, &.{ 0, 1, 2 }, score_shape));
        };
        const stored_probabilities = if (tc.precision == .bf16) try builder.convertDtype(try builder.convertDtype(probabilities, .bf16), .f32) else probabilities;
        var result = try builder.matmul3D(stored_probabilities, value);
        if (token_major) {
            result = try builder.reshape(result, Shape.init(.f32, &.{ @intCast(batch), @intCast(heads), @intCast(queries), @intCast(dim) }));
            result = try builder.transpose(result, &.{ 0, 2, 1, 3 });
        }
        return tc.makeHandle(try builder.reshape(result, tc.graph.node(nodeIdFromCT(Q)).output_shape));
    }

    fn sdpaOp(ctx: *anyopaque, Q: CT, K: CT, V: CT, mask: []const i64, attn_bias: ?CT, batch: usize, seq_len: usize, num_heads: usize, head_dim: usize) anyerror!CT {
        const tc = fromCtx(ctx);
        if (tc.require_resident_outputs) {
            const shape = tc.graph.node(nodeIdFromCT(Q)).output_shape;
            const token_major = @import("../ops/kernel_geometry.zig").DenseAttentionLayout.fromShape(i64, shape.dims[0..shape.rank()], batch, seq_len, num_heads, head_dim) == .token_major;
            if (num_heads > 1) for ([_]CT{ K, V }) |tensor| {
                const other = tc.graph.node(nodeIdFromCT(tensor)).output_shape;
                const other_tokens = @import("../ops/kernel_geometry.zig").DenseAttentionLayout.fromShape(i64, other.dims[0..other.rank()], batch, seq_len, num_heads, head_dim) == .token_major;
                if (token_major != other_tokens) return error.InvalidAttentionShape;
            };
            return tc.denseAttention(Q, K, V, mask, attn_bias, batch, seq_len, seq_len, num_heads, head_dim, false, token_major);
        }
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
        if (tc.require_resident_outputs) return tc.denseAttention(Q, K, V, &.{}, attn_bias, batch, seq_len, seq_len, num_heads, head_dim, true, true);
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

    fn crossAttentionOp(ctx: *anyopaque, Q: CT, K: CT, V: CT, mask: []const i64, batch: usize, dec_seq: usize, enc_seq: usize, num_heads: usize, head_dim: usize) anyerror!CT {
        const tc = fromCtx(ctx);
        if (tc.require_resident_outputs) return tc.denseAttention(Q, K, V, mask, null, batch, dec_seq, enc_seq, num_heads, head_dim, false, true);
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
                .sliding_window = 0,
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
        if (attention.attn_or_mask != null) return tc.makeHandle(try @import("masked_grouped_attention.zig").emit(&tc.graph, nodeIdFromCT(Q), nodeIdFromCT(K), nodeIdFromCT(V), if (attn_bias) |bias| nodeIdFromCT(bias) else null, attention, batch, num_heads, num_kv_heads, head_dim));
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
                .sliding_window = std.math.cast(u32, attention.sliding_window) orelse return error.UnsupportedShape,
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

    fn debertaOp(ctx: *anyopaque, Q: CT, K: CT, V: CT, Q_r: CT, K_r: CT, mask: []const i64, batch: usize, seq_len: usize, num_heads: usize, head_dim: usize) anyerror!CT {
        const tc = fromCtx(ctx);
        if (batch == 0 or seq_len == 0 or num_heads == 0 or head_dim == 0) return error.InvalidAttentionShape;
        const rows = try std.math.mul(usize, batch, seq_len);
        const width = try std.math.mul(usize, num_heads, head_dim);
        const relatives = try std.math.sub(usize, try std.math.mul(usize, 2, seq_len), 1);
        if (mask.len != rows) return error.InvalidAttentionShape;
        var builder = Builder.init(tc.getGraphMut());
        var content: [3]NodeId = undefined;
        for ([_]CT{ Q, K, V }, &content) |value, *node| {
            const source = nodeIdFromCT(value);
            if (tc.graph.node(source).output_shape.numElements() != (std.math.cast(i64, try std.math.mul(usize, rows, width)) orelse return error.InvalidAttentionShape)) return error.InvalidAttentionShape;
            node.* = try builder.reshape(source, Shape.init(.f32, &.{ @intCast(rows), @intCast(width) }));
        }
        var relative: [2]NodeId = undefined;
        for ([_]CT{ Q_r, K_r }, &relative) |value, *node| {
            const source = nodeIdFromCT(value);
            if (tc.graph.node(source).output_shape.numElements() != (std.math.cast(i64, try std.math.mul(usize, relatives, width)) orelse return error.InvalidAttentionShape)) return error.InvalidAttentionShape;
            node.* = try builder.reshape(source, Shape.init(.f32, &.{ @intCast(relatives), @intCast(width) }));
        }
        const qkv = try builder.concat(try builder.concat(content[0], content[1], 0), content[2], 0);
        const qrkr = try builder.concat(relative[0], relative[1], 0);
        const visibility = try tc.maskVisibility(mask, batch, seq_len);
        const key_shape = Shape.init(.f32, &.{ @intCast(batch), @intCast(seq_len) });
        const zero = try tc.broadcastNode(try builder.scalarConst(.f32, 0), &.{}, key_shape);
        const negative = try tc.broadcastNode(try builder.scalarConst(.f32, -std.math.inf(f32)), &.{}, key_shape);
        const keys = try tc.selectNode(visibility.node, zero, negative, key_shape);
        const expanded = try tc.broadcastNode(keys, &.{ 0, 3 }, Shape.init(.f32, &.{ @intCast(batch), @intCast(num_heads), @intCast(seq_len), @intCast(seq_len) }));
        const bias = try builder.reshape(expanded, Shape.init(.f32, &.{ @intCast(batch * num_heads), @intCast(seq_len), @intCast(seq_len) }));
        const output = try tc.graph.addNode(.{
            .op = .{ .fused_disentangled_attention = .{ .batch = @intCast(batch), .seq_len = @intCast(seq_len), .num_heads = @intCast(num_heads), .head_dim = @intCast(head_dim) } },
            .output_shape = Shape.init(.f32, &.{ @intCast(rows), @intCast(width) }),
            .inputs = .{ qkv, qrkr, bias, null_node },
            .num_inputs = 3,
        });
        return tc.makeHandle(output);
    }

    fn debertaBackwardOp(_: *anyopaque, _: CT, _: CT, _: CT, _: CT, _: CT, _: []const i64, _: CT, _: usize, _: usize, _: usize, _: usize) anyerror!CT {
        // Tracing captures forward inference graphs only; the disentangled
        // attention backward never appears in a traced graph.
        return error.UnsupportedOperation;
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

    // A trace owns immutable symbolic values. Cloning dense storage changes
    // only shape/ownership; preserve the input dependency instead of emitting
    // a host readback and a disconnected dummy upload.
    fn cloneTensorShapeOp(ctx: *anyopaque, input: CT, dims: []const i32) anyerror!?CT {
        if (dims.len > 8) return error.InvalidShape;
        const tc = fromCtx(ctx);
        const source = nodeIdFromCT(input);
        const shape = tc.graph.node(source).output_shape;
        var logical: [8]i64 = undefined;
        for (dims, 0..) |dim, i| {
            if (dim <= 0) return error.InvalidShape;
            logical[i] = dim;
        }
        const target = Shape.init(shape.dtype, logical[0..dims.len]);
        if (target.numElements() == null or target.numElements() != shape.numElements()) return error.InvalidShape;
        var builder = Builder.init(tc.getGraphMut());
        return tc.makeHandle(try builder.reshape(source, target));
    }

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

    fn clampScalarOp(ctx: *anyopaque, input: CT, low: ?f32, high: ?f32) anyerror!?CT {
        const tc = fromCtx(ctx);
        var builder = Builder.init(&tc.graph);
        var node = nodeIdFromCT(input);
        const shape = tc.graph.node(node).output_shape;
        for ([_]?f32{ low, high }, 0..) |bound, side| if (bound) |value| {
            const scalar = try tc.broadcastNode(try builder.scalarConst(.f32, value), &.{}, shape);
            const predicate = try tc.graph.addNode(.{ .op = .{ .less_than = {} }, .output_shape = Shape.init(.bool_, shape.dims[0..shape.rank()]), .inputs = .{ if (side == 0) node else scalar, if (side == 0) scalar else node, null_node, null_node }, .num_inputs = 2 });
            node = try tc.graph.addNode(.{ .op = .{ .where_select = {} }, .output_shape = shape, .inputs = .{ predicate, scalar, node, null_node }, .num_inputs = 3 });
        };
        return tc.makeHandle(node);
    }

    fn channelNormReluOp(ctx: *anyopaque, input: CT, weight: CT, channels: usize, positions: usize, eps: f32) anyerror!?CT {
        const tc = fromCtx(ctx);
        return tc.makeHandle(try @import("temporal_program.zig").channelNormRelu(&tc.graph, nodeIdFromCT(input), nodeIdFromCT(weight), channels, positions, eps));
    }
    fn flattenChannelsOp(ctx: *anyopaque, input: CT, time: usize, freq: usize, channels: usize) anyerror!?CT {
        const tc = fromCtx(ctx);
        return tc.makeHandleExact(try @import("temporal_program.zig").flattenChannels(&tc.graph, nodeIdFromCT(input), time, freq, channels));
    }
    fn gluRowsOp(ctx: *anyopaque, input: CT, rows: usize, dim: usize) anyerror!?CT {
        const tc = fromCtx(ctx);
        return tc.makeHandle(try @import("temporal_program.zig").glu(&tc.graph, nodeIdFromCT(input), rows, dim));
    }
    fn depthwiseCausalOp(ctx: *anyopaque, input: CT, weight: CT, rows: usize, dim: usize, kernel: usize) anyerror!?CT {
        const tc = fromCtx(ctx);
        return tc.makeHandle(try @import("temporal_program.zig").depthwiseCausal(&tc.graph, nodeIdFromCT(input), nodeIdFromCT(weight), rows, dim, kernel));
    }
    fn depthwiseGatedDeltaOp(ctx: *anyopaque, request: *const ops.DepthwiseGatedDeltaRequest) anyerror!CT {
        const tc = fromCtx(ctx);
        var nodes: [8]NodeId = undefined;
        inline for (.{ "mixed", "z", "beta", "a", "conv_weight", "a_log", "dt_bias", "norm_weight" }, 0..) |field, i| nodes[i] = nodeIdFromCT(@field(request, field));
        return tc.makeHandleExact(try @import("gated_delta_program.zig").emit(&tc.graph, nodes, request.geometry, request.kernel, request.eps));
    }
    fn localRelativeOp(ctx: *anyopaque, q: CT, k: CT, v: CT, rel: CT, scales: CT, valid: CT, params: ops.Gemma4AudioLocalAttentionParams) anyerror!?CT {
        const tc = fromCtx(ctx);
        return tc.makeHandle(try @import("temporal_program.zig").localRelative(&tc.graph, nodeIdFromCT(q), nodeIdFromCT(k), nodeIdFromCT(v), nodeIdFromCT(rel), nodeIdFromCT(scales), nodeIdFromCT(valid), params));
    }

    fn mropeOp(ctx: *anyopaque, input: CT, rows: usize, dim: usize, theta: f32, scale: f32, positions: []const u32, sections: [3]u32) anyerror!?CT {
        const tc = fromCtx(ctx);
        return tc.makeHandleExact(try @import("rotary_program.zig").emitMrope(&tc.graph, nodeIdFromCT(input), rows, dim, theta, scale, positions, sections));
    }

    fn visionRopeOp(ctx: *anyopaque, input: CT, rows: usize, dim: usize, theta: f32, positions: []const u32) anyerror!?CT {
        const tc = fromCtx(ctx);
        return tc.makeHandleExact(try @import("rotary_program.zig").emitVision(&tc.graph, nodeIdFromCT(input), rows, dim, theta, positions));
    }

    fn ropeOp(ctx: *anyopaque, input: CT, seq_len: usize, head_dim: usize, rope_dim: usize, theta: f32, freq_scale: f32, position_offset: usize, consecutive_pairs: bool) anyerror!CT {
        const tc = fromCtx(ctx);
        const in_id = nodeIdFromCT(input);
        const out_shape = tc.graph.node(in_id).output_shape;
        if (tc.precision == .bf16) return tc.makeHandleExact(try @import("rotary_program.zig").emitBf16(&tc.graph, in_id, .{ .seq_len = @intCast(seq_len), .head_dim = @intCast(head_dim), .rope_dim = @intCast(rope_dim), .theta = theta, .freq_scale = freq_scale, .position_offset = @intCast(position_offset), .consecutive_pairs = consecutive_pairs }));
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

    fn ropePerItemOp(ctx: *anyopaque, input: CT, batch: usize, sequence: usize, head_dim: usize, rope_dim: usize, theta: f32, freq_scale: f32, lengths: []const usize, offsets: []const usize, consecutive_pairs: bool) anyerror!CT {
        const tc = fromCtx(ctx);
        if (tc.runtime_inputs.items.len != 0) return tc.declaredRopePerItem(input, batch, sequence, head_dim, rope_dim, theta, freq_scale, lengths, offsets, consecutive_pairs);
        // Legacy decoder placeholders resolve positions from execution context.
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

    fn declaredRopePerItem(tc: *TracingCompute, input: CT, batch: usize, sequence: usize, head_dim: usize, rope_dim: usize, theta: f32, freq_scale: f32, lengths: []const usize, offsets: []const usize, consecutive_pairs: bool) !CT {
        if (batch == 0 or sequence == 0 or head_dim == 0 or lengths.len != batch or offsets.len != batch) return error.InvalidRoPEInput;
        const original = tc.graph.node(nodeIdFromCT(input)).output_shape;
        const elements = original.numElements() orelse return error.InvalidRoPEInput;
        const rows = try std.math.mul(usize, batch, sequence);
        if (@as(usize, @intCast(elements)) % rows != 0) return error.InvalidRoPEInput;
        const width = @as(usize, @intCast(elements)) / rows;
        if (width % head_dim != 0) return error.InvalidRoPEInput;
        var builder = Builder.init(&tc.graph);
        const flat = try builder.reshape(nodeIdFromCT(input), Shape.init(original.dtype, &.{ @intCast(rows), @intCast(width) }));
        var result: ?NodeId = null;
        for (lengths, offsets, 0..) |length, offset, item| {
            if (length > sequence) return error.InvalidRoPEInput;
            for ([_]bool{ true, false }) |active| {
                const count = if (active) length else sequence - length;
                if (count == 0) continue;
                const start = item * sequence + (if (active) @as(usize, 0) else length);
                var attrs = ml.graph.node.SliceAttrs{};
                attrs.num_axes = 2;
                attrs.starts[0] = @intCast(start);
                attrs.limits[0] = @intCast(start + count);
                attrs.limits[1] = @intCast(width);
                const shape = Shape.init(original.dtype, &.{ @intCast(count), @intCast(width) });
                const slice = try tc.graph.addNode(.{ .op = .{ .slice = attrs }, .output_shape = shape, .inputs = .{ flat, null_node, null_node, null_node }, .num_inputs = 1 });
                const rotary_attrs = ml.graph.node.RopeAttrs{ .seq_len = @intCast(count), .head_dim = @intCast(head_dim), .rope_dim = @intCast(rope_dim), .theta = theta, .freq_scale = freq_scale, .position_offset = std.math.cast(u32, offset) orelse return error.InvalidRoPEInput, .consecutive_pairs = consecutive_pairs };
                const part = if (!active) slice else if (tc.precision == .bf16)
                    try @import("rotary_program.zig").emitBf16(&tc.graph, slice, rotary_attrs)
                else
                    try tc.graph.addNode(.{ .op = .{ .fused_rope = rotary_attrs }, .output_shape = shape, .inputs = .{ slice, null_node, null_node, null_node }, .num_inputs = 1 });
                result = if (result) |previous| try builder.concat(previous, part, 0) else part;
            }
        }
        return tc.makeHandle(try builder.reshape(result orelse return error.InvalidRoPEInput, original));
    }

    fn segmentAttentionOp(ctx: *anyopaque, q: CT, k: CT, v: CT, request: *const ops.SegmentAttention) anyerror!?CT {
        return recordSegmentAttention(fromCtx(ctx), q, k, v, request, 0, null, false);
    }

    fn segmentAttentionGroupedOp(ctx: *anyopaque, q: CT, k: CT, v: CT, request: *const ops.SegmentAttentionGrouped) anyerror!?CT {
        const tc = fromCtx(ctx);
        if (!tc.require_resident_outputs) return null;
        if (request.num_kv_heads == 0 or request.visibility.num_heads % request.num_kv_heads != 0 or !std.math.isFinite(request.score_scale)) return error.InvalidAttentionShape;
        return recordSegmentAttention(tc, q, k, v, &request.visibility, request.num_kv_heads, request.score_scale, request.bf16_probability or tc.precision == .bf16);
    }

    fn recordSegmentAttention(tc: *TracingCompute, q: CT, k: CT, v: CT, request: *const ops.SegmentAttention, kv_heads: usize, score_scale: ?f32, bf16_probability: bool) !CT {
        if (request.ranges.len != try std.math.mul(usize, request.queries, 6) or request.query_positions.len != request.queries or request.key_positions.len != request.keys or request.num_heads == 0 or request.head_dim == 0) return error.InvalidAttentionShape;
        const metadata_len = try std.math.add(usize, request.ranges.len, try std.math.add(usize, request.queries, request.keys));
        const metadata = try tc.allocator.alloc(i32, metadata_len);
        defer tc.allocator.free(metadata);
        for (request.ranges, metadata[0..request.ranges.len]) |range, *dest| dest.* = std.math.cast(i32, range) orelse return error.InvalidAttentionShape;
        @memcpy(metadata[request.ranges.len..][0..request.queries], request.query_positions);
        @memcpy(metadata[request.ranges.len + request.queries ..], request.key_positions);
        const loc = try tc.graph.internConstantBytes(std.mem.sliceAsBytes(metadata), .i32);
        const visibility = try tc.graph.addNode(.{ .op = .{ .constant = .{ .data_offset = loc.offset, .data_len = loc.len } }, .output_shape = Shape.init(.i32, &.{@intCast(metadata_len)}) });
        const node = try tc.graph.addNode(.{
            .op = .{ .fused_segment_attention = .{ .queries = @intCast(request.queries), .keys = @intCast(request.keys), .num_heads = @intCast(request.num_heads), .head_dim = @intCast(request.head_dim), .window = request.window, .num_kv_heads = @intCast(kv_heads), .bf16_probability = bf16_probability, .score_scale = score_scale } },
            .output_shape = Shape.init(.f32, &.{ @intCast(request.queries), @intCast(try std.math.mul(usize, request.num_heads, request.head_dim)) }),
            .inputs = .{ nodeIdFromCT(q), nodeIdFromCT(k), nodeIdFromCT(v), visibility },
            .num_inputs = 4,
        });
        return try tc.makeHandle(node);
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
        in_channels: usize,
        out_channels: usize,
        height: usize,
        width: usize,
        kernel_h: usize,
        kernel_w: usize,
        stride_h: usize,
        stride_w: usize,
        padding_h: usize,
        padding_w: usize,
        groups: usize,
    ) anyerror!CT {
        const tc = fromCtx(ctx);
        if (stride_h == 0 or stride_w == 0 or groups == 0 or in_channels % groups != 0 or out_channels % groups != 0) return error.InvalidInputShape;
        const padded_h = try std.math.add(usize, height, try std.math.mul(usize, 2, padding_h));
        const padded_w = try std.math.add(usize, width, try std.math.mul(usize, 2, padding_w));
        if (kernel_h == 0 or kernel_w == 0 or padded_h < kernel_h or padded_w < kernel_w) return error.InvalidInputShape;
        const out_h = (padded_h - kernel_h) / stride_h + 1;
        const out_w = (padded_w - kernel_w) / stride_w + 1;
        const grid = try primitiveReshape(ctx, input, &.{ @intCast(batch), @intCast(height), @intCast(width), @intCast(in_channels) });
        const channels = try primitiveTranspose(ctx, grid, &.{ 0, 3, 1, 2 }, &.{ @intCast(batch), @intCast(height), @intCast(width), @intCast(in_channels) });
        const shape = Shape.init(.f32, &.{ @intCast(batch), @intCast(out_channels), @intCast(out_h), @intCast(out_w) });
        var attrs = ml.graph.node.ConvAttrs{ .num_spatial = 2, .groups = @intCast(groups) };
        attrs.strides[0..2].* = .{ @intCast(stride_h), @intCast(stride_w) };
        attrs.padding[0..2].* = .{ .{ @intCast(padding_h), @intCast(padding_h) }, .{ @intCast(padding_w), @intCast(padding_w) } };
        const convolution = try tc.graph.addNode(.{ .op = .{ .conv_general = attrs }, .output_shape = shape, .inputs = .{ nodeIdFromCT(channels), nodeIdFromCT(weight), null_node, null_node }, .num_inputs = 2 });
        var builder = Builder.init(&tc.graph);
        const bias_node = try builder.reshape(nodeIdFromCT(bias), Shape.init(.f32, &.{@intCast(out_channels)}));
        const biased = try builder.add(convolution, try tc.broadcastNode(bias_node, &.{1}, shape));
        const result = try tc.makeHandle(biased);
        const tokens = try primitiveTranspose(ctx, result, &.{ 0, 2, 3, 1 }, shape.dims[0..shape.rank()]);
        const rows = try std.math.mul(usize, batch, try std.math.mul(usize, out_h, out_w));
        return primitiveReshape(ctx, tokens, &.{ @intCast(rows), @intCast(out_channels) });
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
        if (tc.last_moe_select_routes == null_node) {
            // Ordinary indexed rows are not a routed-expert placeholder.
            // Retain their explicit indices as a canonical gather, including
            // duplicates and permutations used by position/pooling adapters.
            const shape = tc.graph.node(in_id).output_shape;
            if (shape.rank() != 2 or shape.dim(0) <= 0 or shape.dim(1) != request.dim or request.row_ids.len != request.rows) return error.InvalidInputShape;
            const indices = try tc.allocator.alloc(i64, request.rows);
            defer tc.allocator.free(indices);
            for (indices, request.row_ids) |*index, row| {
                if (row >= shape.dim(0)) return error.InvalidRowIndex;
                index.* = row;
            }
            var builder = Builder.init(&tc.graph);
            const selected = try builder.tensorConstBytes(std.mem.sliceAsBytes(indices), Shape.init(.i64, &.{@intCast(request.rows)}));
            return tc.makeHandle(try builder.gather(in_id, selected, out_shape));
        }
        // Historical MoE routing carries runtime route ownership through this
        // marker; preserve that separate contract until its typed migration.
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

    fn moeSelectRoutesOp(ctx: *anyopaque, layer_index: usize, logits: CT, rows: usize, num_experts: usize, top_k: usize, logit_scale: f32, allocator: std.mem.Allocator) anyerror!?MoeRouteSelection {
        _ = logit_scale;
        const tc = fromCtx(ctx);
        const in_id = nodeIdFromCT(logits);
        // Record the select_routes node so the graph captures MoE structure.
        // Save its ID so downstream MoE ops can reference it as a dependency.
        tc.last_moe_select_routes = try tc.graph.addNode(.{
            .op = .{ .fused_moe_select_routes = .{
                .layer_index = @intCast(layer_index),
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
        const shape = Shape.init(.f32, &.{@intCast(data.len)});
        if (try tc.runtimeInput(std.mem.sliceAsBytes(data), .f32, shape)) |node| return tc.makeHandle(node);
        const loc = try tc.graph.internConstant(data);
        const id = try tc.graph.addNode(.{
            .op = .{ .constant = .{ .data_offset = loc.offset, .data_len = loc.len } },
            .output_shape = Shape.init(.f32, &.{@intCast(data.len)}),
        });
        return tc.makeHandle(id);
    }

    fn fromFloat32ShapeOp(ctx: *anyopaque, data: []const f32, shape: []const i32) anyerror!CT {
        const tc = fromCtx(ctx);
        if (shape.len > 8) return error.UnsupportedShape;
        var dims: [8]i64 = @splat(0);
        for (shape, 0..) |d, i| {
            dims[i] = @intCast(d);
        }
        const s = Shape{
            .dtype = .f32,
            .dims = dims,
            .rank_ = @intCast(shape.len),
        };
        if (try tc.runtimeInput(std.mem.sliceAsBytes(data), .f32, s)) |node| return tc.makeHandle(node);
        const loc = try tc.graph.internConstant(data);
        const id = try tc.graph.addNode(.{
            .op = .{ .constant = .{ .data_offset = loc.offset, .data_len = loc.len } },
            .output_shape = s,
        });
        return tc.makeHandle(id);
    }

    fn toFloat32Op(ctx: *anyopaque, tensor: CT, allocator: std.mem.Allocator) anyerror![]f32 {
        const tc = fromCtx(ctx);
        const node_id = nodeIdFromCT(tensor);
        if (tc.require_resident_outputs) {
            var source = node_id;
            while (tc.graph.node(source).op == .convert_dtype) source = tc.graph.node(source).inputs[0];
            const n = tc.graph.node(source);
            if (n.op != .parameter) return error.HostReadbackDuringCapture;
            for (tc.runtime_inputs.items) |input| if (input.node == source) return error.HostReadbackDuringCapture;
            const count = n.output_shape.numElements() orelse return error.HostReadbackDuringCapture;
            if (count < 0 or count > 4096) return error.HostReadbackDuringCapture;
            const resolver = tc.weight_shape_resolver orelse return error.HostReadbackDuringCapture;
            const read = resolver.values orelse return error.HostReadbackDuringCapture;
            const values = (try read(resolver.context, allocator, tc.graph.parameterName(n))) orelse return error.MissingWeight;
            errdefer allocator.free(values);
            if (values.len != count) return error.InvalidInputShape;
            if (n.output_shape.dtype == .bf16) for (values) |*value| {
                value.* = @import("antfly_tile").fexpr.roundBf16Value(value.*);
            };
            return values;
        }
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

test "capture reads only bounded immutable checkpoint metadata" {
    const a = std.testing.allocator;
    const Resolver = struct {
        fn shape(_: ?*anyopaque, _: std.mem.Allocator, name: []const u8) anyerror!?Shape {
            return Shape.init(.f32, &.{if (std.mem.eql(u8, name, "large")) @as(i64, 4097) else 2});
        }
        fn values(_: ?*anyopaque, allocator: std.mem.Allocator, _: []const u8) anyerror!?[]f32 {
            return try allocator.dupe(f32, &.{ 0.25, 0.5 });
        }
    };
    var tracer = TracingCompute.initWithWeightResolver(a, .{ .resolve = Resolver.shape, .values = Resolver.values });
    defer tracer.deinit();
    tracer.require_resident_outputs = true;
    const cb = tracer.backend();
    const weight = try cb.getWeight("small");
    defer cb.free(weight);
    const metadata = try cb.toFloat32(weight, a);
    defer a.free(metadata);
    try std.testing.expectEqualSlices(f32, &.{ 0.25, 0.5 }, metadata);
    const large = try cb.getWeight("large");
    defer cb.free(large);
    try std.testing.expectError(error.HostReadbackDuringCapture, cb.toFloat32(large, a));
    const values = [_]f32{ 0.25, 0.5 };
    _ = try tracer.bindRuntimeInput("activation", Shape.init(.f32, &.{2}), std.mem.sliceAsBytes(&values));
    const runtime = try cb.fromFloat32Shape(&values, &.{2});
    defer cb.free(runtime);
    try std.testing.expectError(error.HostReadbackDuringCapture, cb.toFloat32(runtime, a));
    const transformed = try cb.multiply(weight, weight);
    defer cb.free(transformed);
    try std.testing.expectError(error.HostReadbackDuringCapture, cb.toFloat32(transformed, a));
}
