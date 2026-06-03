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

// ONNX op type → termite Builder dispatch.
//
// Maps ONNX operation types (strings like "Add", "MatMul", "Relu") to
// termite graph Builder calls. Handles ONNX-specific broadcasting and
// attribute extraction.

const std = @import("std");
const log = std.log.scoped(.onnx_ops);
const ml = @import("ml");
const proto = @import("proto.zig");
const attrs_mod = @import("attrs.zig");
const tensor_mod = @import("tensor.zig");

const Builder = ml.graph.Builder;
const Graph = ml.graph.Graph;
const NodeId = ml.graph.NodeId;
const Shape = ml.graph.Shape;
const DType = ml.graph.DType;
const OpCode = ml.graph.OpCode;
const null_node = ml.graph.null_node;

const NodeProto = proto.NodeProto;
const AttributeProto = proto.AttributeProto;
const TensorProto = proto.TensorProto;
const findAttr = attrs_mod.findAttr;
const getInt = attrs_mod.getInt;
const getFloat = attrs_mod.getFloat;
const getString = attrs_mod.getString;
const getInts = attrs_mod.getInts;
const getTensor = attrs_mod.getTensor;
const getGraph = attrs_mod.getGraph;

pub const ConvertError = error{
    UnsupportedOp,
    MissingInput,
    InvalidAttribute,
    ShapeMismatch,
    TooManyDimensions,
    UnsupportedDType,
    InsufficientData,
    ConstantMaterializationFailed,
    ExternalData,
    OutOfMemory,
    Overflow,
    EndOfStream,
};

/// Linked-list view of name → NodeId scopes, used to let ONNX sub-graph
/// bodies (inside If/Loop/Scan) resolve implicit captures from the parent
/// graph's converted values. Each entry points at a hash map owned by the
/// caller; the chain is walked on lookup, innermost first.
pub const NameScope = struct {
    map: *const std.StringHashMapUnmanaged(NodeId),
    parent: ?*const NameScope = null,

    pub fn lookup(self: *const NameScope, name: []const u8) ?NodeId {
        if (self.map.get(name)) |nid| return nid;
        if (self.parent) |p| return p.lookup(name);
        return null;
    }
};

/// Convert a single ONNX node to termite graph nodes.
/// Returns the NodeId of the primary output.
/// For multi-output ops (Split, etc.), additional outputs are written
/// to `extra_outputs` if provided. extra_outputs[i] corresponds to
/// node.outputs[i+1]. Caller should provide a buffer of len >= node.outputs.len - 1.
pub fn convertNode(
    allocator: std.mem.Allocator,
    builder: *Builder,
    node: *const NodeProto,
    inputs: []const NodeId,
    extra_outputs: ?[]NodeId,
) ConvertError!NodeId {
    return convertNodeWithScope(allocator, builder, node, inputs, extra_outputs, null);
}

/// Same as `convertNode` but threads an optional outer `NameScope` through
/// control-flow ops (If/Loop/Scan) so their sub-graph bodies can resolve
/// implicit captures (ONNX-style outer-scope name references).
pub fn convertNodeWithScope(
    allocator: std.mem.Allocator,
    builder: *Builder,
    node: *const NodeProto,
    inputs: []const NodeId,
    extra_outputs: ?[]NodeId,
    outer_scope: ?*const NameScope,
) ConvertError!NodeId {
    const op = OpType.fromString(node.op_type) orelse {
        log.warn("unsupported ONNX op: {s}", .{node.op_type});
        return error.UnsupportedOp;
    };
    return switch (op) {
        // Elementwise unary
        .Identity => inputs[0],
        .Neg => builder.neg(inputs[0]),
        .Sqrt => builder.sqrt(inputs[0]),
        .Exp => builder.expOp(inputs[0]),
        .Log => builder.logOp(inputs[0]),
        .Tanh => builder.tanhOp(inputs[0]),
        .Sin => builder.sinOp(inputs[0]),
        .Cos => builder.cosOp(inputs[0]),
        .Erf => builder.erfOp(inputs[0]),
        .Abs => builder.absOp(inputs[0]),
        .Reciprocal => convertReciprocal(builder, inputs[0]),
        .Sign => convertSign(builder, inputs[0]),
        .Ceil => convertCeil(builder, inputs[0]),
        .Floor => convertFloor(builder, inputs[0]),

        // Elementwise binary (with broadcasting)
        .Add => broadcastBinaryOp(builder, .add, inputs[0], inputs[1]),
        .Sub => broadcastBinaryOp(builder, .sub, inputs[0], inputs[1]),
        .Mul => broadcastBinaryOp(builder, .mul, inputs[0], inputs[1]),
        .Div => broadcastBinaryOp(builder, .div, inputs[0], inputs[1]),

        // Comparison & logical
        .Less => broadcastBinaryOp(builder, .less_than, inputs[0], inputs[1]),
        .Greater => convertGreater(builder, inputs[0], inputs[1]),
        .LessOrEqual => convertLessOrEqual(builder, inputs[0], inputs[1]),
        .GreaterOrEqual => convertGreaterOrEqual(builder, inputs[0], inputs[1]),
        .Equal => convertEqual(builder, inputs[0], inputs[1]),
        .Not => convertNot(builder, inputs[0]),
        .Where => convertWhere(builder, inputs),
        .Min => convertMin(builder, inputs),
        .Max => convertMax(builder, inputs),

        // Activations (fused)
        .Relu => builder.relu(inputs[0]),
        .Gelu => builder.gelu(inputs[0]),
        .FastGelu => builder.gelu(inputs[0]),
        .Sigmoid => convertSigmoid(builder, inputs[0]),
        .Silu => builder.silu(inputs[0]),
        .LeakyRelu => convertLeakyRelu(builder, node, inputs[0]),
        .HardSigmoid => convertHardSigmoid(builder, node, inputs[0]),
        .Softmax => convertSoftmax(builder, node, inputs[0], false),
        .LogSoftmax => convertSoftmax(builder, node, inputs[0], true),

        // Linear algebra
        .MatMul => convertMatMul(builder, node, inputs[0], inputs[1]),
        .Gemm => convertGemm(builder, node, inputs),

        // Shape manipulation
        .Reshape => convertReshape(allocator, builder, node, inputs),
        .Transpose => convertTranspose(builder, node, inputs[0]),
        .Squeeze => convertSqueeze(builder, node, inputs),
        .Unsqueeze => convertUnsqueeze(builder, node, inputs),
        .Flatten => convertFlatten(builder, node, inputs[0]),
        .Expand => convertExpand(builder, inputs),
        .Pad => convertPad(allocator, builder, node, inputs),
        .Split => convertSplit(allocator, builder, node, inputs, extra_outputs),
        .Shape_ => convertShape(builder, node, inputs[0]),
        .ConstantOfShape => convertConstantOfShape(allocator, builder, node, inputs),
        .Tile => convertTile(builder, inputs),

        // Reduction
        .ReduceSum => convertReduce(builder, .reduce_sum, node, inputs),
        .ReduceMean => convertReduce(builder, .reduce_mean, node, inputs),
        .ReduceMax => convertReduce(builder, .reduce_max, node, inputs),
        .ReduceMin => convertReduceMinMax(builder, .min, node, inputs),
        .ReduceProd => convertReduceProd(builder, node, inputs),
        .ReduceL2 => convertReduceL2(builder, node, inputs),

        // Data movement
        .Gather => convertGather(builder, node, inputs),
        .Concat => convertConcat(builder, node, inputs),
        .Slice => convertSlice(allocator, builder, node, inputs),
        .ScatterND => convertScatterND(builder, inputs),
        .ScatterElements => convertScatterElements(builder, node, inputs),

        // Constants and casting
        .Constant => convertConstant(allocator, builder, node),
        .Cast => convertCast(builder, node, inputs[0]),

        // Compound ops
        .Pow => convertPow(builder, inputs),
        .Clip => convertClip(builder, inputs),
        .Range => convertRange(allocator, builder, node, inputs),

        // Normalization (compound → fused)
        .LayerNormalization => convertLayerNorm(builder, node, inputs),
        .SimplifiedLayerNormalization => convertSimplifiedLayerNorm(builder, node, inputs),
        .BatchNormalization => convertBatchNorm(builder, node, inputs),

        // Phase 3: Convolution
        .Conv => convertConv(builder, node, inputs),

        // Phase 3: Attention
        .MultiHeadAttention => convertMultiHeadAttention(builder, node, inputs),
        .GroupQueryAttention => convertGroupQueryAttention(builder, node, inputs),

        // Phase 3: Additional ops
        .Trilu => convertTrilu(builder, node, inputs),
        .GatherElements => convertGatherElements(builder, node, inputs),
        .CumSum => convertCumSum(builder, node, inputs),
        .DequantizeLinear => convertDequantizeLinear(builder, node, inputs),
        .QuantizeLinear => convertQuantizeLinear(builder, node, inputs),
        .AveragePool => convertAveragePool(builder, node, inputs),
        .MaxPool => convertMaxPool(builder, node, inputs),
        .GlobalAveragePool => convertGlobalAveragePool(builder, inputs),

        // Phase 4
        .RotaryEmbedding => convertRotaryEmbedding(builder, node, inputs),
        .HardSwish => convertHardSwish(builder, inputs[0]),
        .And_ => convertLogicalAnd(builder, inputs),
        .Or_ => convertLogicalOr(builder, inputs),
        .Xor => convertLogicalXor(builder, inputs),
        .Mod => convertMod(builder, node, inputs),
        .IsNaN => convertIsNaN(builder, inputs[0]),
        .Size_ => convertSize(builder, inputs[0]),
        .ArgMax => convertArgMax(builder, node, inputs[0]),
        .ArgMin => convertArgMin(builder, node, inputs[0]),
        .GatherND => convertGatherND(builder, node, inputs),
        .Einsum => convertEinsum(builder, node, inputs),
        .ConvTranspose => convertConvTranspose(builder, node, inputs),
        .Resize => convertResize(builder, node, inputs),
        .NonZero => convertNonZero(builder, inputs[0]),
        .TopK => convertTopK(builder, node, inputs),
        .OneHot => convertOneHot(allocator, builder, node, inputs),
        .InstanceNormalization => convertInstanceNorm(builder, node, inputs),
        .GroupNormalization => convertGroupNorm(builder, node, inputs),
        .SkipLayerNormalization => convertSkipLayerNorm(builder, node, inputs),
        .If => convertIf(allocator, builder, node, inputs, extra_outputs, outer_scope),
        .Loop => convertLoop(allocator, builder, node, inputs, outer_scope),
        .Scan => convertScan(allocator, builder, node, inputs, extra_outputs, outer_scope),
    };
}

// ── Op Type Enum ─────────────────────────────────────────────────────

const OpType = enum {
    // Unary
    Identity,
    Neg,
    Sqrt,
    Exp,
    Log,
    Tanh,
    Sin,
    Cos,
    Erf,
    Abs,
    Reciprocal,
    Sign,
    Ceil,
    Floor,
    // Binary
    Add,
    Sub,
    Mul,
    Div,
    // Comparison & logical
    Less,
    LessOrEqual,
    Greater,
    GreaterOrEqual,
    Equal,
    Not,
    Where,
    Min,
    Max,
    // Activations
    Relu,
    Gelu,
    FastGelu,
    Sigmoid,
    Silu,
    LeakyRelu,
    HardSigmoid,
    Softmax,
    LogSoftmax,
    // Linear
    MatMul,
    Gemm,
    // Shape
    Reshape,
    Transpose,
    Squeeze,
    Unsqueeze,
    Flatten,
    Expand,
    Pad,
    Split,
    Shape_,
    ConstantOfShape,
    Tile,
    // Reduction
    ReduceSum,
    ReduceMean,
    ReduceMax,
    ReduceMin,
    ReduceProd,
    ReduceL2,
    // Data movement
    Gather,
    Concat,
    Slice,
    ScatterND,
    ScatterElements,
    // Constants
    Constant,
    Cast,
    // Compound
    Pow,
    Clip,
    Range,
    // Normalization
    LayerNormalization,
    SimplifiedLayerNormalization,
    BatchNormalization,
    // Convolution
    Conv,
    // Attention
    MultiHeadAttention,
    GroupQueryAttention,
    // Phase 3 additional
    Trilu,
    GatherElements,
    CumSum,
    DequantizeLinear,
    QuantizeLinear,
    AveragePool,
    MaxPool,
    GlobalAveragePool,
    // Phase 4
    RotaryEmbedding,
    HardSwish,
    And_,
    Or_,
    Xor,
    Mod,
    IsNaN,
    Size_,
    ArgMax,
    ArgMin,
    GatherND,
    Einsum,
    ConvTranspose,
    Resize,
    // Missing ops
    NonZero,
    TopK,
    OneHot,
    InstanceNormalization,
    GroupNormalization,
    SkipLayerNormalization,
    // Control flow
    If,
    Loop,
    Scan,

    fn fromString(s: []const u8) ?OpType {
        const map = std.StaticStringMap(OpType).initComptime(.{
            .{ "Identity", .Identity },
            .{ "Neg", .Neg },
            .{ "Sqrt", .Sqrt },
            .{ "Exp", .Exp },
            .{ "Log", .Log },
            .{ "Tanh", .Tanh },
            .{ "Sin", .Sin },
            .{ "Cos", .Cos },
            .{ "Erf", .Erf },
            .{ "Abs", .Abs },
            .{ "Reciprocal", .Reciprocal },
            .{ "Sign", .Sign },
            .{ "Ceil", .Ceil },
            .{ "Floor", .Floor },
            .{ "Add", .Add },
            .{ "Sub", .Sub },
            .{ "Mul", .Mul },
            .{ "Div", .Div },
            .{ "Less", .Less },
            .{ "LessOrEqual", .LessOrEqual },
            .{ "Greater", .Greater },
            .{ "GreaterOrEqual", .GreaterOrEqual },
            .{ "Equal", .Equal },
            .{ "Not", .Not },
            .{ "Where", .Where },
            .{ "Min", .Min },
            .{ "Max", .Max },
            .{ "Relu", .Relu },
            .{ "Gelu", .Gelu },
            .{ "FastGelu", .FastGelu },
            .{ "Sigmoid", .Sigmoid },
            .{ "Silu", .Silu },
            .{ "LeakyRelu", .LeakyRelu },
            .{ "HardSigmoid", .HardSigmoid },
            .{ "Softmax", .Softmax },
            .{ "LogSoftmax", .LogSoftmax },
            .{ "MatMul", .MatMul },
            .{ "Gemm", .Gemm },
            .{ "Reshape", .Reshape },
            .{ "Transpose", .Transpose },
            .{ "Squeeze", .Squeeze },
            .{ "Unsqueeze", .Unsqueeze },
            .{ "Flatten", .Flatten },
            .{ "Expand", .Expand },
            .{ "Pad", .Pad },
            .{ "Split", .Split },
            .{ "Shape", .Shape_ },
            .{ "ConstantOfShape", .ConstantOfShape },
            .{ "Tile", .Tile },
            .{ "ReduceSum", .ReduceSum },
            .{ "ReduceMean", .ReduceMean },
            .{ "ReduceMax", .ReduceMax },
            .{ "ReduceMin", .ReduceMin },
            .{ "ReduceProd", .ReduceProd },
            .{ "ReduceL2", .ReduceL2 },
            .{ "Gather", .Gather },
            .{ "Concat", .Concat },
            .{ "Slice", .Slice },
            .{ "ScatterND", .ScatterND },
            .{ "ScatterElements", .ScatterElements },
            .{ "Constant", .Constant },
            .{ "Cast", .Cast },
            .{ "Pow", .Pow },
            .{ "Clip", .Clip },
            .{ "Range", .Range },
            .{ "LayerNormalization", .LayerNormalization },
            .{ "SimplifiedLayerNormalization", .SimplifiedLayerNormalization },
            .{ "BatchNormalization", .BatchNormalization },
            .{ "Conv", .Conv },
            .{ "MultiHeadAttention", .MultiHeadAttention },
            .{ "GroupQueryAttention", .GroupQueryAttention },
            .{ "Trilu", .Trilu },
            .{ "GatherElements", .GatherElements },
            .{ "CumSum", .CumSum },
            .{ "DequantizeLinear", .DequantizeLinear },
            .{ "QuantizeLinear", .QuantizeLinear },
            .{ "AveragePool", .AveragePool },
            .{ "MaxPool", .MaxPool },
            .{ "GlobalAveragePool", .GlobalAveragePool },
            .{ "RotaryEmbedding", .RotaryEmbedding },
            .{ "HardSwish", .HardSwish },
            .{ "And", .And_ },
            .{ "Or", .Or_ },
            .{ "Xor", .Xor },
            .{ "Mod", .Mod },
            .{ "IsNaN", .IsNaN },
            .{ "Size", .Size_ },
            .{ "ArgMax", .ArgMax },
            .{ "ArgMin", .ArgMin },
            .{ "GatherND", .GatherND },
            .{ "Einsum", .Einsum },
            .{ "ConvTranspose", .ConvTranspose },
            .{ "Resize", .Resize },
            .{ "NonZero", .NonZero },
            .{ "TopK", .TopK },
            .{ "OneHot", .OneHot },
            .{ "InstanceNormalization", .InstanceNormalization },
            .{ "GroupNormalization", .GroupNormalization },
            .{ "SkipLayerNormalization", .SkipLayerNormalization },
            .{ "If", .If },
            .{ "Loop", .Loop },
            .{ "Scan", .Scan },
        });
        return map.get(s);
    }
};

// ── Broadcasting ─────────────────────────────────────────────────────

const BinaryOp = enum { add, sub, mul, div, less_than };

fn computeBroadcastShape(shapes: []const Shape, dtype: DType) Shape {
    var out_rank: u8 = 0;
    for (shapes) |shape| out_rank = @max(out_rank, shape.rank());

    var out_dims: [8]i64 = .{1} ** 8;
    for (shapes) |shape| {
        const offset = out_rank - shape.rank();
        for (0..shape.rank()) |i| {
            const axis: usize = offset + i;
            const cur = out_dims[axis];
            const dim = shape.dim(@intCast(i));
            if (dim == 1) continue;
            if (cur == 1) {
                out_dims[axis] = dim;
            } else if (dim < 0 or cur < 0) {
                out_dims[axis] = if (cur > 1) cur else if (dim > 1) dim else cur;
            } else {
                out_dims[axis] = @max(cur, dim);
            }
        }
    }

    return Shape{ .dtype = dtype, .dims = out_dims, .rank_ = out_rank };
}

fn broadcastBinaryOp(builder: *Builder, op: BinaryOp, a_orig: NodeId, b_orig: NodeId) ConvertError!NodeId {
    // Full ONNX broadcasting: equalize ranks by prepending 1-dims, then
    // broadcast axes where one dim=1 and the other>1.
    var a = a_orig;
    var b = b_orig;
    const a_shape = builder.graph.node(a).output_shape;
    const b_shape = builder.graph.node(b).output_shape;

    // Equalize ranks by prepending 1-dims
    const a_rank = a_shape.rank();
    const b_rank = b_shape.rank();
    const out_rank = @max(a_rank, b_rank);

    if (a_rank < out_rank) {
        a = try prependOnes(builder, a, a_shape, out_rank);
    }
    if (b_rank < out_rank) {
        b = try prependOnes(builder, b, b_shape, out_rank);
    }

    // Broadcast dims where one is 1
    const a_new = builder.graph.node(a).output_shape;
    const b_new = builder.graph.node(b).output_shape;
    var needs_bcast_a = false;
    var needs_bcast_b = false;
    var out_dims: [8]i64 = .{0} ** 8;
    for (0..out_rank) |i| {
        const ad = a_new.dim(@intCast(i));
        const bd = b_new.dim(@intCast(i));
        if (ad == 1 and bd != 1) {
            needs_bcast_a = true;
            out_dims[i] = bd;
        } else if (bd == 1 and ad != 1) {
            needs_bcast_b = true;
            out_dims[i] = ad;
        } else if (ad < 0 or bd < 0) {
            // Dynamic dim — prefer the known positive value if one exists
            out_dims[i] = if (ad > 0) ad else if (bd > 0) bd else ad;
        } else {
            out_dims[i] = @max(ad, bd);
        }
    }
    const out_shape = Shape{ .dtype = a_shape.dtype, .dims = out_dims, .rank_ = out_rank };

    if (needs_bcast_a) a = try broadcastTo(builder, a, out_shape);
    if (needs_bcast_b) b = try broadcastTo(builder, b, out_shape);

    return switch (op) {
        .add => builder.add(a, b),
        .sub => builder.sub(a, b),
        .mul => builder.mul(a, b),
        .div => builder.div(a, b),
        .less_than => cmpLessThan(builder, a, b),
    };
}

fn prependOnes(builder: *Builder, node: NodeId, shape: Shape, target_rank: u8) ConvertError!NodeId {
    const pad = target_rank - shape.rank();
    var new_dims: [8]i64 = .{0} ** 8;
    for (0..pad) |i| new_dims[i] = 1;
    for (0..shape.rank()) |i| new_dims[pad + i] = shape.dim(@intCast(i));
    const new_shape = Shape{ .dtype = shape.dtype, .dims = new_dims, .rank_ = target_rank };
    return builder.reshape(node, new_shape);
}

fn broadcastTo(builder: *Builder, node: NodeId, target: Shape) ConvertError!NodeId {
    const src_shape = builder.graph.node(node).output_shape;
    // Check if already matching
    var same = true;
    for (0..target.rank()) |i| {
        if (src_shape.dim(@intCast(i)) != target.dim(@intCast(i))) {
            same = false;
            break;
        }
    }
    if (same) return node;

    var broadcast_axes: [8]u8 = .{0} ** 8;
    for (0..target.rank()) |i| broadcast_axes[i] = @intCast(i);

    return builder.graph.addNode(.{
        .op = .{ .broadcast_in_dim = .{
            .target_shape = target,
            .broadcast_axes = broadcast_axes,
            .num_axes = target.rank(),
        } },
        .output_shape = target,
        .inputs = .{ node, null_node, null_node, null_node },
        .num_inputs = 1,
    });
}

// ── Individual Op Converters ─────────────────────────────────────────

fn convertReciprocal(builder: *Builder, input: NodeId) ConvertError!NodeId {
    // 1.0 / x
    const dtype = builder.graph.node(input).output_shape.dtype;
    const one = try builder.scalarConst(dtype, 1.0);
    return builder.div(one, input);
}

fn convertSigmoid(builder: *Builder, input: NodeId) ConvertError!NodeId {
    // 1 / (1 + exp(-x))
    const dtype = builder.graph.node(input).output_shape.dtype;
    const one = try builder.scalarConst(dtype, 1.0);
    const neg_x = try builder.neg(input);
    const exp_neg = try builder.expOp(neg_x);
    const denom = try builder.add(one, exp_neg);
    return builder.div(one, denom);
}

fn convertSoftmax(builder: *Builder, _: *const NodeProto, input: NodeId, comptime is_log: bool) ConvertError!NodeId {
    const in_shape = builder.graph.node(input).output_shape;
    const last_axis: u8 = in_shape.rank() - 1;
    const raw_dim = in_shape.dim(last_axis);

    // If last dim is static, delegate to builder which builds decomposed + fused
    if (raw_dim > 0) {
        return if (is_log) builder.logSoftmax(input) else builder.softmax(input);
    }

    // Dynamic last dim: emit fused node directly with dim=0 sentinel (no decomposed subgraph)
    const op: ml.graph.node.OpCode = if (is_log)
        .{ .fused_log_softmax = .{ .dim = 0 } }
    else
        .{ .fused_softmax = .{ .dim = 0 } };

    return builder.graph.addNode(.{
        .op = op,
        .output_shape = in_shape,
        .inputs = .{ input, null_node, null_node, null_node },
        .num_inputs = 1,
    });
}

fn convertWhere(builder: *Builder, inputs: []const NodeId) ConvertError!NodeId {
    if (inputs.len < 3) return error.MissingInput;
    return selectOp(builder, inputs[0], inputs[1], inputs[2]);
}

fn convertGemm(builder: *Builder, node: *const NodeProto, inputs: []const NodeId) ConvertError!NodeId {
    if (inputs.len < 2) return error.MissingInput;
    const trans_a = getInt(node.attributes, "transA", 0) != 0;
    const trans_b = getInt(node.attributes, "transB", 0) != 0;
    const alpha = getFloat(node.attributes, "alpha", 1.0);
    const beta = getFloat(node.attributes, "beta", 1.0);

    var a = inputs[0];
    var b = inputs[1];

    if (trans_a) a = try builder.transpose(a, &.{ 1, 0 });
    if (trans_b) b = try builder.transpose(b, &.{ 1, 0 });

    var result = try builder.matmul(a, b);

    // Scale by alpha if != 1
    if (alpha != 1.0) {
        const alpha_node = try builder.scalarConst(builder.graph.node(result).output_shape.dtype, alpha);
        result = try broadcastBinaryOp(builder, .mul, result, alpha_node);
    }

    // Add bias (C) if provided
    if (inputs.len >= 3 and inputs[2] != null_node) {
        var bias = inputs[2];
        if (beta != 1.0) {
            const beta_node = try builder.scalarConst(builder.graph.node(bias).output_shape.dtype, beta);
            bias = try broadcastBinaryOp(builder, .mul, bias, beta_node);
        }
        result = try broadcastBinaryOp(builder, .add, result, bias);
    }

    return result;
}

fn shapeElementCount(shape: Shape) ?usize {
    if (shape.rank() == 0) return 1;
    var total: usize = 1;
    for (0..shape.rank()) |i| {
        const dim = shape.dim(@intCast(i));
        if (dim < 0) return null;
        total = std.math.mul(usize, total, @intCast(dim)) catch return null;
    }
    return total;
}

fn broadcastLinearIndex(src_shape: Shape, dst_shape: Shape, linear_index: usize) ?usize {
    if (src_shape.rank() != dst_shape.rank()) return null;
    if (dst_shape.rank() == 0) return 0;

    var dst_strides: [8]usize = .{0} ** 8;
    var running_stride: usize = 1;
    var axis: usize = dst_shape.rank();
    while (axis > 0) {
        axis -= 1;
        dst_strides[axis] = running_stride;
        const dim = dst_shape.dim(@intCast(axis));
        if (dim < 0) return null;
        running_stride = std.math.mul(usize, running_stride, @intCast(dim)) catch return null;
    }

    var src_linear: usize = 0;
    var src_stride: usize = 1;
    axis = src_shape.rank();
    while (axis > 0) {
        axis -= 1;
        const src_dim_i64 = src_shape.dim(@intCast(axis));
        const dst_dim_i64 = dst_shape.dim(@intCast(axis));
        if (src_dim_i64 < 0 or dst_dim_i64 < 0) return null;
        const src_dim: usize = @intCast(src_dim_i64);
        const dst_dim: usize = @intCast(dst_dim_i64);
        if (src_dim != dst_dim and src_dim != 1) return null;
        const coord = (linear_index / dst_strides[axis]) % dst_dim;
        const src_coord = if (src_dim == 1) 0 else coord;
        src_linear += src_coord * src_stride;
        src_stride = std.math.mul(usize, src_stride, src_dim) catch return null;
    }

    return src_linear;
}

/// Try to collect constant f32 values from a node, tracing through common
/// shape-building patterns: constant, concat(constants), reshape(constant),
/// gather(constant, constant_index), and small broadcast/compare/select trees.
/// Used by Reshape, Slice, Expand, etc.
fn materializeConstantValues(builder: *Builder, node_id: NodeId, buf: []f32) ?[]const f32 {
    if (node_id == null_node) return null;
    const n = builder.graph.node(node_id);
    switch (n.op) {
        .constant => |c_attrs| {
            return materializeConstantNodeValues(builder.graph, n, c_attrs, buf);
        },
        .shape_of => |attrs| {
            const shape_inputs = n.getInputs();
            if (shape_inputs.len == 0) return null;
            const input_shape = builder.graph.node(shape_inputs[0]).output_shape;
            const start: usize = attrs.start;
            const end: usize = attrs.end;
            if (end < start or end > input_shape.rank()) return null;
            const count = end - start;
            if (count > buf.len) return null;
            for (0..count) |i| {
                buf[i] = @floatFromInt(input_shape.dim(@intCast(start + i)));
            }
            return buf[0..count];
        },
        .concat_prim => {
            // Walk inputs — recursively materialize each
            const concat_inputs = n.getInputs();
            var total: usize = 0;
            for (concat_inputs) |inp| {
                if (inp == null_node) break;
                var child_buf: [8]f32 = undefined;
                const child_data = materializeConstantValues(builder, inp, &child_buf) orelse return null;
                if (total + child_data.len > buf.len) return null;
                @memcpy(buf[total..][0..child_data.len], child_data);
                total += child_data.len;
            }
            return buf[0..total];
        },
        .reshape => {
            // Unsqueeze/Flatten convert to reshape — look through to first input
            const reshape_inputs = n.getInputs();
            if (reshape_inputs.len > 0) {
                return materializeConstantValues(builder, reshape_inputs[0], buf);
            }
            return null;
        },
        .convert_dtype => {
            // Cast — look through to input (values are same, just dtype changes)
            const cast_inputs = n.getInputs();
            if (cast_inputs.len > 0) {
                return materializeConstantValues(builder, cast_inputs[0], buf);
            }
            return null;
        },
        .broadcast_in_dim => |attrs| {
            const bcast_inputs = n.getInputs();
            if (bcast_inputs.len == 0) return null;
            var src_buf: [8]f32 = undefined;
            const src = materializeConstantValues(builder, bcast_inputs[0], &src_buf) orelse return null;
            const src_shape = builder.graph.node(bcast_inputs[0]).output_shape;
            const dst_shape = attrs.target_shape;
            const total = shapeElementCount(dst_shape) orelse return null;
            if (total > buf.len) return null;
            for (0..total) |i| {
                const src_idx = broadcastLinearIndex(src_shape, dst_shape, i) orelse return null;
                if (src_idx >= src.len) return null;
                buf[i] = src[src_idx];
            }
            return buf[0..total];
        },
        .slice => |s_attrs| {
            // Slice(data) along axis 0 — materialize input, extract range
            const slice_inputs = n.getInputs();
            if (slice_inputs.len == 0) return null;
            var data_buf: [8]f32 = undefined;
            const data = materializeConstantValues(builder, slice_inputs[0], &data_buf) orelse return null;
            if (s_attrs.num_axes == 0) return null;
            // For 1D slice (shape extraction), apply first axis
            const start: usize = @intCast(@max(0, s_attrs.starts[0]));
            const limit_raw = s_attrs.limits[0];
            const limit: usize = if (limit_raw > @as(i64, @intCast(data.len)))
                data.len
            else if (limit_raw < 0)
                // Negative limit means from end
                @intCast(@max(0, @as(i64, @intCast(data.len)) + limit_raw))
            else
                @intCast(limit_raw);
            const stride: usize = @intCast(@max(1, s_attrs.strides[0]));
            if (start >= data.len or start >= limit) return buf[0..0];
            var count: usize = 0;
            var pos = start;
            while (pos < limit and count < buf.len) : (pos += stride) {
                buf[count] = data[pos];
                count += 1;
            }
            return buf[0..count];
        },
        // Elementwise arithmetic — constant-fold if both operands are constant
        .add, .sub, .mul, .div => {
            const arith_inputs = n.getInputs();
            if (arith_inputs.len < 2) return null;
            var lhs_buf: [8]f32 = undefined;
            var rhs_buf: [8]f32 = undefined;
            const lhs = materializeConstantValues(builder, arith_inputs[0], &lhs_buf) orelse return null;
            const rhs = materializeConstantValues(builder, arith_inputs[1], &rhs_buf) orelse return null;
            const count = @max(lhs.len, rhs.len);
            if (count > buf.len) return null;
            for (0..count) |i| {
                const a = if (i < lhs.len) lhs[i] else if (lhs.len == 1) lhs[0] else return null;
                const b = if (i < rhs.len) rhs[i] else if (rhs.len == 1) rhs[0] else return null;
                buf[i] = foldSymbolicShapeArithmetic(n.op, a, b) orelse return null;
            }
            return buf[0..count];
        },
        .less_than => {
            const cmp_inputs = n.getInputs();
            if (cmp_inputs.len < 2) return null;
            var lhs_buf: [8]f32 = undefined;
            var rhs_buf: [8]f32 = undefined;
            const lhs = materializeConstantValues(builder, cmp_inputs[0], &lhs_buf) orelse return null;
            const rhs = materializeConstantValues(builder, cmp_inputs[1], &rhs_buf) orelse return null;
            const out_shape = n.output_shape;
            const total = shapeElementCount(out_shape) orelse return null;
            if (total > buf.len) return null;
            const lhs_shape = builder.graph.node(cmp_inputs[0]).output_shape;
            const rhs_shape = builder.graph.node(cmp_inputs[1]).output_shape;
            for (0..total) |i| {
                const lhs_idx = if (lhs.len == 1 and total > 1) 0 else broadcastLinearIndex(lhs_shape, out_shape, i) orelse return null;
                const rhs_idx = if (rhs.len == 1 and total > 1) 0 else broadcastLinearIndex(rhs_shape, out_shape, i) orelse return null;
                if (lhs_idx >= lhs.len or rhs_idx >= rhs.len) return null;
                buf[i] = if (lhs[lhs_idx] < rhs[rhs_idx]) 1.0 else 0.0;
            }
            return buf[0..total];
        },
        .where_select => {
            const where_inputs = n.getInputs();
            if (where_inputs.len < 3) return null;
            var cond_buf: [8]f32 = undefined;
            var false_buf: [8]f32 = undefined;
            var true_buf: [8]f32 = undefined;
            const cond = materializeConstantValues(builder, where_inputs[0], &cond_buf) orelse return null;
            const on_false = materializeConstantValues(builder, where_inputs[1], &false_buf) orelse return null;
            const on_true = materializeConstantValues(builder, where_inputs[2], &true_buf) orelse return null;
            const out_shape = n.output_shape;
            const total = shapeElementCount(out_shape) orelse return null;
            if (total > buf.len) return null;
            const cond_shape = builder.graph.node(where_inputs[0]).output_shape;
            const false_shape = builder.graph.node(where_inputs[1]).output_shape;
            const true_shape = builder.graph.node(where_inputs[2]).output_shape;
            for (0..total) |i| {
                const cond_idx = if (cond.len == 1 and total > 1) 0 else broadcastLinearIndex(cond_shape, out_shape, i) orelse return null;
                const false_idx = if (on_false.len == 1 and total > 1) 0 else broadcastLinearIndex(false_shape, out_shape, i) orelse return null;
                const true_idx = if (on_true.len == 1 and total > 1) 0 else broadcastLinearIndex(true_shape, out_shape, i) orelse return null;
                if (cond_idx >= cond.len or false_idx >= on_false.len or true_idx >= on_true.len) return null;
                buf[i] = if (cond[cond_idx] != 0.0) on_true[true_idx] else on_false[false_idx];
            }
            return buf[0..total];
        },
        .gather => |g_attrs| {
            // Gather(data, indices) along axis — materialize both, extract elements
            const gather_inputs = n.getInputs();
            if (gather_inputs.len < 2) return null;
            var data_buf: [8]f32 = undefined;
            const data = materializeConstantValues(builder, gather_inputs[0], &data_buf) orelse return null;
            var idx_buf: [8]f32 = undefined;
            const indices = materializeConstantValues(builder, gather_inputs[1], &idx_buf) orelse return null;
            const axis: usize = g_attrs.axis;
            _ = axis; // For 1D gather (shape extraction), axis is always 0
            const count = @min(indices.len, buf.len);
            if (count == 0) {
                // Scalar index — extract single element
                if (indices.len == 0) return null;
            }
            // For scalar gather (indices is single value), return single value
            if (n.output_shape.rank() == 0 and indices.len >= 1) {
                const idx: usize = @intFromFloat(indices[0]);
                if (idx < data.len) {
                    buf[0] = data[idx];
                    return buf[0..1];
                }
                return null;
            }
            for (0..count) |i| {
                const idx: usize = @intFromFloat(indices[i]);
                if (idx >= data.len) return null;
                buf[i] = data[idx];
            }
            return buf[0..count];
        },
        else => return null,
    }
}

fn foldSymbolicShapeArithmetic(op: OpCode, a: f32, b: f32) ?f32 {
    const a_int = std.math.lossyCast(i64, a);
    const b_int = std.math.lossyCast(i64, b);
    const a_unknown = a_int < 0;
    const b_unknown = b_int < 0;

    if (!a_unknown and !b_unknown) {
        return switch (op) {
            .add => a + b,
            .sub => a - b,
            .mul => a * b,
            .div => if (b != 0) a / b else null,
            else => unreachable,
        };
    }

    return switch (op) {
        .add => if (a_unknown and b_int == 0)
            a
        else if (b_unknown and a_int == 0)
            b
        else
            -1,
        .sub => if (b_int == 0)
            a
        else if (a_int == b_int)
            0
        else
            -1,
        .mul => if (a_int == 0 or b_int == 0)
            0
        else if (a_unknown and b_int == 1)
            a
        else if (b_unknown and a_int == 1)
            b
        else
            -1,
        .div => if (a_int == 0 and b_int != 0)
            0
        else if (b_int == 1)
            a
        else if (a_int == b_int and a_unknown and b_unknown)
            1
        else
            -1,
        else => unreachable,
    };
}

fn convertReshape(allocator: std.mem.Allocator, builder: *Builder, node: *const NodeProto, inputs: []const NodeId) ConvertError!NodeId {
    if (inputs.len < 2) return error.MissingInput;
    _ = allocator;

    // Materialize target shape from constant or concat-of-constants
    var shape_buf: [8]f32 = undefined;
    const data = materializeConstantValues(builder, inputs[1], &shape_buf) orelse {
        const shape_node = builder.graph.node(inputs[1]);
        log.warn("reshape target for {s} not materializable: target_op={s} target_shape={any}", .{
            node.name,
            @tagName(std.meta.activeTag(shape_node.op)),
            shape_node.output_shape,
        });
        return error.ConstantMaterializationFailed;
    };

    // allowzero attribute added in opset 14: when set, a 0 in the shape
    // means a literal zero dimension instead of "copy from input shape".
    const allow_zero = getInt(node.attributes, "allowzero", 0) != 0;

    const input_shape = builder.graph.node(inputs[0]).output_shape;
    var dims: [8]i64 = .{0} ** 8;
    const rank: u8 = @intCast(@min(data.len, 8));
    var materialized_negative_count: usize = 0;
    var copied_zero_axes: [8]bool = .{false} ** 8;
    for (0..rank) |i| {
        const d: i64 = @intFromFloat(data[i]);
        if (d == 0 and !allow_zero) {
            // ONNX opset <14 default: 0 means copy from input shape
            dims[i] = if (i < input_shape.rank()) input_shape.dim(@intCast(i)) else 1;
            copied_zero_axes[i] = true;
        } else if (d == -1) {
            dims[i] = -1;
            materialized_negative_count += 1;
        } else {
            dims[i] = d;
        }
    }

    // Shape-building subgraphs often materialize copied dynamic dimensions as
    // `-1`. When more than one negative dim survives materialization, prefer
    // preserving any aligned positive input dims instead of collapsing the
    // target with the single-`-1` inference path below.
    if (materialized_negative_count > 1 and rank == input_shape.rank()) {
        for (0..rank) |i| {
            if (dims[i] < 0) {
                const input_dim = input_shape.dim(@intCast(i));
                if (input_dim > 0) dims[i] = input_dim;
            }
        }
    }

    // Common attention reshape: [B, S, H] -> [B, S, num_heads, head_dim].
    // More generally, some exported models increase the rank by one and insert
    // a new axis at an arbitrary position. Detect that inserted axis instead of
    // assuming the new dimension is always appended near the tail, then only
    // copy aligned input dims into the still-symbolic target axes.
    if (rank == input_shape.rank() + 1 and rank >= 3) {
        const input_rank = input_shape.rank();
        const input_last = input_shape.dim(input_shape.rank() - 1);
        var inserted_axis: ?usize = null;
        for (0..rank) |candidate| {
            var valid = true;
            for (0..rank) |target_i| {
                if (target_i == candidate) continue;
                const input_i = if (target_i < candidate) target_i else target_i - 1;
                const target_dim = dims[target_i];
                const input_dim = input_shape.dim(@intCast(input_i));
                if (candidate == input_rank - 1 and target_i == rank - 1 and target_dim > 0 and input_i == input_rank - 1 and input_last > 0) {
                    if (@rem(input_last, target_dim) == 0) continue;
                }
                if (target_dim > 0 and input_dim > 0 and target_dim != input_dim) {
                    valid = false;
                    break;
                }
            }
            if (!valid) continue;
            if (inserted_axis != null) {
                inserted_axis = null;
                break;
            }
            inserted_axis = candidate;
        }
        if (inserted_axis == null and input_rank >= 1) {
            const split_axis = input_rank - 1;
            if (split_axis < rank and dims[split_axis] < 0 and dims[rank - 1] > 0 and input_last > 0 and
                @rem(input_last, dims[rank - 1]) == 0)
            {
                var split_valid = true;
                for (0..rank) |target_i| {
                    if (target_i == split_axis) continue;
                    const input_i = if (target_i < split_axis) target_i else target_i - 1;
                    if (input_i >= input_rank) continue;
                    const target_dim = dims[target_i];
                    const input_dim = input_shape.dim(@intCast(input_i));
                    if (target_i == rank - 1 and target_dim > 0 and input_i == input_rank - 1 and input_last > 0) {
                        if (@rem(input_last, target_dim) == 0) continue;
                    }
                    if (target_dim > 0 and input_dim > 0 and target_dim != input_dim) {
                        split_valid = false;
                        break;
                    }
                }
                if (split_valid) inserted_axis = split_axis;
            }
        }

        if (inserted_axis) |axis| {
            for (0..rank) |target_i| {
                if (target_i == axis or dims[target_i] >= 0) continue;
                const input_i = if (target_i < axis) target_i else target_i - 1;
                dims[target_i] = input_shape.dim(@intCast(input_i));
            }

            if (axis == input_rank - 1) {
                const split_idx = axis;
                const target_split = dims[split_idx];
                const target_last = dims[rank - 1];
                if (input_last > 0 and target_last > 0) {
                    if (target_split < 0 and @rem(input_last, target_last) == 0) {
                        dims[split_idx] = @divTrunc(input_last, target_last);
                    } else if (target_split > 0 and
                        (std.math.mul(i64, target_split, target_last) catch 0) == input_last)
                    {
                        if (dims[split_idx] < 0) {
                            dims[split_idx] = @divTrunc(input_last, target_last);
                        }
                    }
                } else if (input_last > 0 and target_split > 0 and dims[rank - 1] < 0 and
                    @rem(input_last, target_split) == 0)
                {
                    dims[rank - 1] = @divTrunc(input_last, target_split);
                }
            }
        }
    }

    const in_elems = input_shape.numElements();

    if (in_elems) |elems| {
        if (materialized_negative_count > 1) {
            var known_positive_product: i64 = 1;
            var valid = true;
            for (0..rank) |i| {
                if (dims[i] <= 0) continue;
                known_positive_product = std.math.mul(i64, known_positive_product, dims[i]) catch {
                    valid = false;
                    break;
                };
            }
            if (valid and known_positive_product == elems) {
                for (0..rank) |i| {
                    if (dims[i] < 0) dims[i] = 1;
                }
                materialized_negative_count = 0;
            }
        }
        if (rank >= 2 and dims[0] < 0) {
            var trailing_product: i64 = 1;
            var valid = true;
            for (1..rank) |i| {
                if (dims[i] <= 0) {
                    valid = false;
                    break;
                }
                trailing_product = std.math.mul(i64, trailing_product, dims[i]) catch {
                    valid = false;
                    break;
                };
            }
            if (valid and trailing_product > 0 and @rem(elems, trailing_product) == 0) {
                dims[0] = @divTrunc(elems, trailing_product);
            }
        }
    }

    // Resolve -1 dimension
    var known_product: i64 = 1;
    var neg_idx: ?u8 = null;
    var negative_dims: usize = 0;
    for (0..rank) |i| {
        if (dims[i] == -1) {
            negative_dims += 1;
            neg_idx = @intCast(i);
        } else if (dims[i] > 0) {
            known_product *= dims[i];
        }
    }
    if (negative_dims == 1 and materialized_negative_count <= 1) {
        if (neg_idx) |idx| {
            if (in_elems) |elems| {
                if (known_product > 0) {
                    dims[idx] = if (@rem(elems, known_product) == 0)
                        @divTrunc(elems, known_product)
                    else
                        -1;
                }
            } else {
                // Best-effort symbolic inference: when reshape preserves some dynamic
                // dimensions and introduces a single inferred dimension, compare the
                // absolute dimension factors of the input and target shapes.
                var input_factor: i64 = 1;
                var input_valid = true;
                for (0..input_shape.rank()) |i| {
                    const dim = input_shape.dim(@intCast(i));
                    if (dim == 0) {
                        input_valid = false;
                        break;
                    }
                    const factor = if (dim < 0) -dim else dim;
                    input_factor = std.math.mul(i64, input_factor, factor) catch {
                        input_valid = false;
                        break;
                    };
                }

                var target_factor: i64 = 1;
                var target_valid = input_valid;
                if (target_valid) {
                    for (0..rank) |i| {
                        if (i == idx) continue;
                        const dim = dims[i];
                        if (dim == 0) {
                            target_valid = false;
                            break;
                        }
                        const factor = if (dim < 0) -dim else dim;
                        target_factor = std.math.mul(i64, target_factor, factor) catch {
                            target_valid = false;
                            break;
                        };
                    }
                }

                if (target_valid and target_factor > 0 and @rem(input_factor, target_factor) == 0) {
                    dims[idx] = @divTrunc(input_factor, target_factor);
                }
            }
        }
    }

    if (in_elems) |elems| {
        var resolved_product: i64 = 1;
        var all_positive = true;
        for (0..rank) |i| {
            if (dims[i] <= 0) {
                all_positive = false;
                break;
            }
            resolved_product = std.math.mul(i64, resolved_product, dims[i]) catch {
                all_positive = false;
                break;
            };
        }

        if (all_positive and resolved_product != elems and rank != input_shape.rank()) {
            var repaired = false;
            for (0..rank) |axis| {
                if (!copied_zero_axes[axis]) continue;
                var other_product: i64 = 1;
                var valid = true;
                for (0..rank) |i| {
                    if (i == axis) continue;
                    if (dims[i] <= 0) {
                        valid = false;
                        break;
                    }
                    other_product = std.math.mul(i64, other_product, dims[i]) catch {
                        valid = false;
                        break;
                    };
                }
                if (!valid or other_product <= 0 or @rem(elems, other_product) != 0) continue;
                const repaired_dim = @divTrunc(elems, other_product);
                if (repaired_dim <= 0) continue;
                dims[axis] = repaired_dim;
                repaired = true;
                break;
            }
            if (!repaired) {
                log.warn("reshape inferred mismatched static shape for {s}: input_shape={any} target={any} resolved={any} elems={d}", .{
                    node.name,
                    input_shape,
                    data[0..rank],
                    dims[0..rank],
                    elems,
                });
            }
        }
    }
    const new_shape = Shape{
        .dtype = input_shape.dtype,
        .dims = dims,
        .rank_ = rank,
    };

    return builder.reshape(inputs[0], new_shape);
}

fn convertTranspose(builder: *Builder, node: *const NodeProto, input: NodeId) ConvertError!NodeId {
    const perm_ints = getInts(node.attributes, "perm");
    if (perm_ints.len == 0) {
        // Default: reverse dimensions
        const rank = builder.graph.node(input).output_shape.rank();
        var perm: [8]u8 = undefined;
        for (0..rank) |i| perm[i] = @intCast(rank - 1 - i);
        return builder.transpose(input, perm[0..rank]);
    }
    var perm: [8]u8 = undefined;
    for (perm_ints, 0..) |p, i| perm[i] = @intCast(p);
    return builder.transpose(input, perm[0..perm_ints.len]);
}

fn convertSqueeze(builder: *Builder, node: *const NodeProto, inputs: []const NodeId) ConvertError!NodeId {
    const in_shape = builder.graph.node(inputs[0]).output_shape;

    // Axes from attribute (opset < 13) or second input (opset >= 13)
    var axes_buf: [8]u8 = undefined;
    var num_axes: u8 = 0;

    const attr_axes = getInts(node.attributes, "axes");
    if (attr_axes.len > 0) {
        for (attr_axes) |a| {
            const ax: u8 = if (a < 0) @intCast(@as(i64, in_shape.rank()) + a) else @intCast(a);
            axes_buf[num_axes] = ax;
            num_axes += 1;
        }
    } else if (inputs.len >= 2 and inputs[1] != null_node) {
        // Try to materialize axes from constant (may be behind Cast/Reshape)
        var axes_val_buf: [8]f32 = undefined;
        const data = materializeConstantValues(builder, inputs[1], &axes_val_buf) orelse return error.ConstantMaterializationFailed;
        for (data) |d| {
            const a: i64 = @intFromFloat(d);
            const ax: u8 = if (a < 0) @intCast(@as(i64, in_shape.rank()) + a) else @intCast(a);
            axes_buf[num_axes] = ax;
            num_axes += 1;
        }
    } else {
        // Squeeze all dims of size 1
        for (0..in_shape.rank()) |i| {
            if (in_shape.dim(@intCast(i)) == 1) {
                axes_buf[num_axes] = @intCast(i);
                num_axes += 1;
            }
        }
    }

    // Build new shape without squeezed dims
    var new_dims: [8]i64 = .{0} ** 8;
    var new_rank: u8 = 0;
    for (0..in_shape.rank()) |i| {
        var squeeze = false;
        for (axes_buf[0..num_axes]) |ax| {
            if (ax == @as(u8, @intCast(i))) {
                squeeze = true;
                break;
            }
        }
        if (!squeeze) {
            new_dims[new_rank] = in_shape.dim(@intCast(i));
            new_rank += 1;
        }
    }
    const new_shape = Shape{ .dtype = in_shape.dtype, .dims = new_dims, .rank_ = new_rank };
    return builder.reshape(inputs[0], new_shape);
}

fn convertUnsqueeze(builder: *Builder, node: *const NodeProto, inputs: []const NodeId) ConvertError!NodeId {
    const in_shape = builder.graph.node(inputs[0]).output_shape;

    // Axes from attribute (opset < 13) or second input
    var axes_buf: [8]i64 = undefined;
    var num_axes: usize = 0;

    const attr_axes = getInts(node.attributes, "axes");
    if (attr_axes.len > 0) {
        for (attr_axes) |a| {
            axes_buf[num_axes] = a;
            num_axes += 1;
        }
    } else if (inputs.len >= 2 and inputs[1] != null_node) {
        var axes_val_buf: [8]f32 = undefined;
        const data = materializeConstantValues(builder, inputs[1], &axes_val_buf) orelse return error.ConstantMaterializationFailed;
        for (data) |d| {
            axes_buf[num_axes] = @intFromFloat(d);
            num_axes += 1;
        }
    }

    const new_rank: u8 = in_shape.rank() + @as(u8, @intCast(num_axes));

    // Normalize negative axes
    for (0..num_axes) |i| {
        if (axes_buf[i] < 0) axes_buf[i] += @intCast(new_rank);
    }

    // Sort axes
    std.mem.sort(i64, axes_buf[0..num_axes], {}, std.sort.asc(i64));

    // Build new shape: insert 1s at the specified axes
    var new_dims: [8]i64 = .{0} ** 8;
    var src_idx: u8 = 0;
    for (0..new_rank) |i| {
        var is_new_axis = false;
        for (axes_buf[0..num_axes]) |ax| {
            if (@as(u8, @intCast(ax)) == @as(u8, @intCast(i))) {
                is_new_axis = true;
                break;
            }
        }
        if (is_new_axis) {
            new_dims[i] = 1;
        } else {
            new_dims[i] = in_shape.dim(src_idx);
            src_idx += 1;
        }
    }

    const new_shape = Shape{ .dtype = in_shape.dtype, .dims = new_dims, .rank_ = new_rank };
    return builder.reshape(inputs[0], new_shape);
}

fn convertFlatten(builder: *Builder, node: *const NodeProto, input: NodeId) ConvertError!NodeId {
    const in_shape = builder.graph.node(input).output_shape;
    const axis: u8 = @intCast(getInt(node.attributes, "axis", 1));

    // Flatten to 2D: [product(dims[:axis]), product(dims[axis:])]
    var dim0: i64 = 1;
    for (0..axis) |i| dim0 *= in_shape.dim(@intCast(i));
    var dim1: i64 = 1;
    for (axis..in_shape.rank()) |i| dim1 *= in_shape.dim(@intCast(i));

    const new_shape = Shape.init(in_shape.dtype, &.{ dim0, dim1 });
    return builder.reshape(input, new_shape);
}

const ReduceTag = enum { reduce_sum, reduce_mean, reduce_max };

fn convertReduce(builder: *Builder, comptime op: ReduceTag, node: *const NodeProto, inputs: []const NodeId) ConvertError!NodeId {
    const in_shape = builder.graph.node(inputs[0]).output_shape;

    // Axes from attribute or second input (opset >= 18)
    var axes_buf: [8]u8 = undefined;
    var num_axes: u8 = 0;

    const attr_axes = getInts(node.attributes, "axes");
    if (attr_axes.len > 0) {
        for (attr_axes) |a| {
            const ax: u8 = if (a < 0) @intCast(@as(i64, in_shape.rank()) + a) else @intCast(a);
            axes_buf[num_axes] = ax;
            num_axes += 1;
        }
    } else if (inputs.len >= 2 and inputs[1] != null_node) {
        var axes_val_buf: [8]f32 = undefined;
        const data = materializeConstantValues(builder, inputs[1], &axes_val_buf) orelse return error.ConstantMaterializationFailed;
        for (data) |d| {
            const a: i64 = @intFromFloat(d);
            const ax: u8 = if (a < 0) @intCast(@as(i64, in_shape.rank()) + a) else @intCast(a);
            axes_buf[num_axes] = ax;
            num_axes += 1;
        }
    } else {
        // Reduce all axes (default)
        for (0..in_shape.rank()) |i| {
            axes_buf[num_axes] = @intCast(i);
            num_axes += 1;
        }
    }

    return switch (op) {
        .reduce_sum => builder.reduceSum(inputs[0], axes_buf[0..num_axes]),
        .reduce_mean => builder.reduceMean(inputs[0], axes_buf[0..num_axes]),
        .reduce_max => builder.reduceMax(inputs[0], axes_buf[0..num_axes]),
    };
}

fn convertGather(builder: *Builder, node: *const NodeProto, inputs: []const NodeId) ConvertError!NodeId {
    if (inputs.len < 2) return error.MissingInput;
    const axis_raw = getInt(node.attributes, "axis", 0);
    const table_shape = builder.graph.node(inputs[0]).output_shape;
    const indices_shape = builder.graph.node(inputs[1]).output_shape;
    const axis: u8 = if (axis_raw < 0) @intCast(@as(i64, table_shape.rank()) + axis_raw) else @intCast(axis_raw);

    // Output shape: data.shape[:axis] + indices.shape + data.shape[axis+1:]
    var out_dims: [8]i64 = .{0} ** 8;
    var out_rank: u8 = 0;
    // Copy dims before axis
    for (0..axis) |i| {
        out_dims[out_rank] = table_shape.dim(@intCast(i));
        out_rank += 1;
    }
    // Insert indices shape
    for (0..indices_shape.rank()) |i| {
        out_dims[out_rank] = indices_shape.dim(@intCast(i));
        out_rank += 1;
    }
    // Copy dims after axis
    for ((axis + 1)..table_shape.rank()) |i| {
        out_dims[out_rank] = table_shape.dim(@intCast(i));
        out_rank += 1;
    }

    const out_shape = Shape{ .dtype = table_shape.dtype, .dims = out_dims, .rank_ = out_rank };
    return builder.graph.addNode(.{
        .op = .{ .gather = .{ .axis = axis } },
        .output_shape = out_shape,
        .inputs = .{ inputs[0], inputs[1], null_node, null_node },
        .num_inputs = 2,
    });
}

fn convertConcat(builder: *Builder, node: *const NodeProto, inputs: []const NodeId) ConvertError!NodeId {
    if (inputs.len < 2) return error.MissingInput;
    const axis_raw = getInt(node.attributes, "axis", 0);

    var concat_rank: u8 = 0;
    for (inputs) |inp| {
        concat_rank = @max(concat_rank, builder.graph.node(inp).output_shape.rank());
    }
    // ONNX shape-building subgraphs often concatenate scalar constants to
    // synthesize a 1-D shape tensor. Treat rank-0 inputs as length-1 tensors
    // for that case so Reshape/Slice/Gather shape paths can materialize.
    if (concat_rank == 0) concat_rank = 1;

    const axis: u8 = if (axis_raw < 0) @intCast(@as(i64, concat_rank) + axis_raw) else @intCast(axis_raw);
    if (axis >= concat_rank) {
        return error.UnsupportedOp;
    }

    // Cascade binary concats (termite nodes have max 4 inputs)
    var result = inputs[0];
    for (inputs[1..]) |inp| {
        const raw_lhs_shape = builder.graph.node(result).output_shape;
        const raw_rhs_shape = builder.graph.node(inp).output_shape;
        const lhs_shape = if (raw_lhs_shape.rank() == concat_rank)
            raw_lhs_shape
        else if (raw_lhs_shape.rank() == 0 and concat_rank == 1 and axis == 0)
            Shape.init(raw_lhs_shape.dtype, &.{1})
        else
            return error.UnsupportedOp;
        const rhs_shape = if (raw_rhs_shape.rank() == concat_rank)
            raw_rhs_shape
        else if (raw_rhs_shape.rank() == 0 and concat_rank == 1 and axis == 0)
            Shape.init(raw_rhs_shape.dtype, &.{1})
        else
            return error.UnsupportedOp;

        var out_dims: [8]i64 = .{0} ** 8;
        @memcpy(out_dims[0..concat_rank], lhs_shape.dims[0..concat_rank]);
        const ld = lhs_shape.dim(axis);
        const rd = rhs_shape.dim(axis);
        out_dims[axis] = if (ld < 0 or rd < 0) -1 else ld + rd;
        const out_shape = Shape{
            .dtype = lhs_shape.dtype,
            .dims = out_dims,
            .rank_ = concat_rank,
        };
        result = try builder.graph.addNode(.{
            .op = .{ .concat_prim = .{ .axis = axis } },
            .output_shape = out_shape,
            .inputs = .{ result, inp, null_node, null_node },
            .num_inputs = 2,
        });
    }
    return result;
}

fn convertSlice(allocator: std.mem.Allocator, builder: *Builder, node: *const NodeProto, inputs: []const NodeId) ConvertError!NodeId {
    _ = allocator;
    _ = node;
    if (inputs.len < 3) return error.MissingInput;

    const in_shape = builder.graph.node(inputs[0]).output_shape;

    // Materialize starts, ends from constant inputs (may be behind Cast/Reshape/Gather)
    var starts_buf: [8]f32 = undefined;
    var ends_buf: [8]f32 = undefined;
    const starts_data = materializeConstantValues(builder, inputs[1], &starts_buf) orelse return error.ConstantMaterializationFailed;
    const ends_data = materializeConstantValues(builder, inputs[2], &ends_buf) orelse return error.ConstantMaterializationFailed;

    var strides_data: ?[]const f32 = null;
    var axes_data: ?[]const f32 = null;
    var axes_buf: [8]f32 = undefined;
    var strides_buf_: [8]f32 = undefined;
    if (inputs.len >= 4 and inputs[3] != null_node) axes_data = materializeConstantValues(builder, inputs[3], &axes_buf);
    if (inputs.len >= 5 and inputs[4] != null_node) strides_data = materializeConstantValues(builder, inputs[4], &strides_buf_);

    var slice_attrs = ml.graph.node.SliceAttrs{};
    slice_attrs.num_axes = in_shape.rank();

    // Initialize with full range
    for (0..in_shape.rank()) |i| {
        slice_attrs.starts[@intCast(i)] = 0;
        slice_attrs.limits[@intCast(i)] = in_shape.dim(@intCast(i));
        slice_attrs.strides[@intCast(i)] = 1;
    }

    // Apply specified axes
    for (0..starts_data.len) |i| {
        const ax: u8 = if (axes_data) |ad| @intCast(@as(i64, @intFromFloat(ad[i]))) else @intCast(i);
        // Clamp to i64 range — ONNX uses INT64_MAX as "to the end" sentinel,
        // which overflows when round-tripped through f32.
        var start: i64 = clampF32ToI64(starts_data[i]);
        var end: i64 = clampF32ToI64(ends_data[i]);
        const dim_size = in_shape.dim(ax);
        // Skip normalization if dimension is dynamic
        if (dim_size > 0) {
            const stride = if (strides_data) |sd|
                if (i < sd.len) @as(i64, @intFromFloat(sd[i])) else 1
            else
                1;
            // Some exporters use end=-1 as a full-range sentinel for dynamic
            // prefix slices like [:seq_len]. Preserve that behavior here
            // rather than folding it to dim-1 and dropping the final element.
            if (end == -1 and start == 0 and stride == 1) {
                end = dim_size;
            }
            // Normalize negative indices
            if (start < 0) start += dim_size;
            if (end < 0) end += dim_size;
            // Clamp
            start = @max(0, @min(start, dim_size));
            end = @max(0, @min(end, dim_size));
        }

        slice_attrs.starts[ax] = start;
        slice_attrs.limits[ax] = end;
        if (strides_data) |sd| {
            if (i < sd.len) slice_attrs.strides[ax] = @intFromFloat(sd[i]);
        }
    }

    // Compute output shape
    var out_dims: [8]i64 = .{0} ** 8;
    for (0..in_shape.rank()) |i| {
        const s = slice_attrs.starts[i];
        const e = slice_attrs.limits[i];
        const st = slice_attrs.strides[i];
        if (e < 0 or s < 0) {
            // Dynamic dimension — can't compute static size
            out_dims[i] = -1;
        } else if (st > 0) {
            out_dims[i] = @divTrunc(e - s + st - 1, st);
        } else {
            out_dims[i] = @divTrunc(s - e - st - 1, -st);
        }
    }
    const out_shape = Shape{ .dtype = in_shape.dtype, .dims = out_dims, .rank_ = in_shape.rank_ };

    return builder.graph.addNode(.{
        .op = .{ .slice = slice_attrs },
        .output_shape = out_shape,
        .inputs = .{ inputs[0], null_node, null_node, null_node },
        .num_inputs = 1,
    });
}

fn convertConstant(allocator: std.mem.Allocator, builder: *Builder, node: *const NodeProto) ConvertError!NodeId {
    // Check for "value" attribute (TensorProto)
    if (getTensor(node.attributes, "value")) |tensor| {
        const shape = try tensor_mod.tensorShape(tensor);
        if (shape.dtype != .f32) {
            const bytes = tensor_mod.extractNativeBytesWithExternal(allocator, tensor, null) catch |err| {
                log.warn("Constant: failed to materialize native tensor data: {}", .{err});
                return error.ConstantMaterializationFailed;
            };
            defer allocator.free(bytes);
            return builder.tensorConstBytes(bytes, shape);
        }

        const data = try tensor_mod.extractFloat32(allocator, tensor);
        defer allocator.free(data);
        // True scalar (no dims in proto) → scalarConst
        if (tensor.dims.len == 0 and data.len <= 1) {
            return builder.scalarConst(
                shape.dtype,
                if (data.len == 1) data[0] else 0,
            );
        }
        // Tensor with explicit dims (even [1]) → tensorConst to preserve rank
        if (data.len == 0) {
            return builder.scalarConst(shape.dtype, 0);
        }
        return builder.tensorConst(data, shape);
    }

    // Check for "value_float"
    const f_val = getFloat(node.attributes, "value_float", 0);
    if (findAttr(node.attributes, "value_float") != null) {
        return builder.scalarConst(.f32, f_val);
    }

    // Check for "value_int"
    const i_val = getInt(node.attributes, "value_int", 0);
    if (findAttr(node.attributes, "value_int") != null) {
        return builder.scalarConst(.f32, @floatFromInt(i_val));
    }

    return builder.scalarConst(.f32, 0);
}

fn convertCast(builder: *Builder, node: *const NodeProto, input: NodeId) ConvertError!NodeId {
    const to_int = getInt(node.attributes, "to", 1);
    const target_dt: proto.DataType = @enumFromInt(@as(u32, @intCast(to_int)));
    const target = try tensor_mod.onnxDTypeToTermite(target_dt);

    const in_shape = builder.graph.node(input).output_shape;
    var out_shape = in_shape;
    out_shape.dtype = target;

    return builder.graph.addNode(.{
        .op = .{ .convert_dtype = .{ .target = target } },
        .output_shape = out_shape,
        .inputs = .{ input, null_node, null_node, null_node },
        .num_inputs = 1,
    });
}

fn convertPow(builder: *Builder, inputs: []const NodeId) ConvertError!NodeId {
    if (inputs.len < 2) return error.MissingInput;

    // Check if exponent is a constant 2 (common: x^2)
    if (materializeConstantScalar(builder, inputs[1])) |exponent| {
        if (exponent == 2.0) {
            return builder.mul(inputs[0], inputs[0]);
        }
        if (exponent == 0.5) {
            return builder.sqrt(inputs[0]);
        }
    }
    // General case: exp(y * log(x))
    const log_x = try builder.logOp(inputs[0]);
    const y_log_x = try broadcastBinaryOp(builder, .mul, inputs[1], log_x);
    return builder.expOp(y_log_x);
}

fn convertClip(builder: *Builder, inputs: []const NodeId) ConvertError!NodeId {
    var result = inputs[0];
    const dtype = builder.graph.node(result).output_shape.dtype;

    // Clip(x, min, max) — min and max are optional inputs
    if (inputs.len >= 2 and inputs[1] != null_node) {
        // max(x, min)
        const cmp = try builder.graph.addNode(.{
            .op = .{ .less_than = {} },
            .output_shape = builder.graph.node(result).output_shape,
            .inputs = .{ result, inputs[1], null_node, null_node },
            .num_inputs = 2,
        });
        _ = dtype;
        result = try builder.graph.addNode(.{
            .op = .{ .where_select = {} },
            .output_shape = builder.graph.node(result).output_shape,
            .inputs = .{ cmp, inputs[1], result, null_node },
            .num_inputs = 3,
        });
    }
    if (inputs.len >= 3 and inputs[2] != null_node) {
        // min(x, max) → where(x > max, max, x)
        const cmp = try builder.graph.addNode(.{
            .op = .{ .less_than = {} },
            .output_shape = builder.graph.node(result).output_shape,
            .inputs = .{ inputs[2], result, null_node, null_node },
            .num_inputs = 2,
        });
        result = try builder.graph.addNode(.{
            .op = .{ .where_select = {} },
            .output_shape = builder.graph.node(result).output_shape,
            .inputs = .{ cmp, inputs[2], result, null_node },
            .num_inputs = 3,
        });
    }
    return result;
}

// ── Phase 2: Comparison & Logical Ops ───────────────────────────────

fn castDType(builder: *Builder, input: NodeId, target: DType) !NodeId {
    const in_shape = builder.graph.node(input).output_shape;
    var out_shape = in_shape;
    out_shape.dtype = target;
    return builder.graph.addNode(.{
        .op = .{ .convert_dtype = .{ .target = target } },
        .output_shape = out_shape,
        .inputs = .{ input, null_node, null_node, null_node },
        .num_inputs = 1,
    });
}

fn convertFloor(builder: *Builder, input: NodeId) ConvertError!NodeId {
    // floor(x) = trunc(x) - (x < trunc(x) ? 1 : 0)
    // trunc via cast to i64 then back to float
    const dtype = builder.graph.node(input).output_shape.dtype;
    const as_int = try castDType(builder, input, .i64);
    const trunc = try castDType(builder, as_int, dtype);
    const one = try builder.scalarConst(dtype, 1.0);
    const zero = try builder.scalarConst(dtype, 0.0);
    const needs_dec = try cmpLessThan(builder, input, trunc);
    const correction = try selectOp(builder, needs_dec, one, zero);
    return builder.sub(trunc, correction);
}

fn convertCeil(builder: *Builder, input: NodeId) ConvertError!NodeId {
    // ceil(x) = trunc(x) + (trunc(x) < x ? 1 : 0)
    const dtype = builder.graph.node(input).output_shape.dtype;
    const as_int = try castDType(builder, input, .i64);
    const trunc = try castDType(builder, as_int, dtype);
    const one = try builder.scalarConst(dtype, 1.0);
    const zero = try builder.scalarConst(dtype, 0.0);
    const needs_inc = try cmpLessThan(builder, trunc, input);
    const correction = try selectOp(builder, needs_inc, one, zero);
    return builder.add(trunc, correction);
}

fn convertSign(builder: *Builder, input: NodeId) ConvertError!NodeId {
    // sign(x) = where(x > 0, 1, where(x < 0, -1, 0))
    const dtype = builder.graph.node(input).output_shape.dtype;
    const zero = try builder.scalarConst(dtype, 0.0);
    const one = try builder.scalarConst(dtype, 1.0);
    const neg_one = try builder.scalarConst(dtype, -1.0);
    const is_neg = try cmpLessThan(builder, input, zero);
    const neg_or_zero = try selectOp(builder, is_neg, neg_one, zero);
    const is_pos = try cmpLessThan(builder, zero, input);
    return selectOp(builder, is_pos, one, neg_or_zero);
}

fn convertGreater(builder: *Builder, a: NodeId, b: NodeId) ConvertError!NodeId {
    // a > b  ⟺  b < a
    return cmpLessThan(builder, b, a);
}

fn convertLessOrEqual(builder: *Builder, a: NodeId, b: NodeId) ConvertError!NodeId {
    // a <= b  ⟺  ¬(b < a)
    const gt = try cmpLessThan(builder, b, a);
    return convertNot(builder, gt);
}

fn convertGreaterOrEqual(builder: *Builder, a: NodeId, b: NodeId) ConvertError!NodeId {
    // a >= b  ⟺  ¬(a < b)
    const lt = try cmpLessThan(builder, a, b);
    return convertNot(builder, lt);
}

fn convertEqual(builder: *Builder, a: NodeId, b: NodeId) ConvertError!NodeId {
    // a == b  ⟺  ¬(a < b) ∧ ¬(b < a)  ⟺  ¬(a < b ∨ b < a)
    // Implemented as: where(a < b, 0, where(b < a, 0, 1))
    const out_shape = builder.graph.node(a).output_shape;
    const dtype = out_shape.dtype;
    const zero = try builder.scalarConst(dtype, 0.0);
    const one = try builder.scalarConst(dtype, 1.0);
    const lt = try cmpLessThan(builder, a, b);
    const gt = try cmpLessThan(builder, b, a);
    const not_gt = try selectOp(builder, gt, zero, one);
    return selectOp(builder, lt, zero, not_gt);
}

fn convertNot(builder: *Builder, input: NodeId) ConvertError!NodeId {
    // NOT x  ⟺  where(x, 0, 1)  (treats nonzero as true)
    const out_shape = builder.graph.node(input).output_shape;
    const dtype = out_shape.dtype;
    const zero = try builder.scalarConst(dtype, 0.0);
    const one = try builder.scalarConst(dtype, 1.0);
    // x != 0 → is_nonzero (use x < 0 or 0 < x)
    const neg = try cmpLessThan(builder, input, zero);
    const pos = try cmpLessThan(builder, zero, input);
    // is_nonzero = neg or pos → where(neg, 1, where(pos, 1, 0))
    const pos_or_zero = try selectOp(builder, pos, one, zero);
    const is_nonzero = try selectOp(builder, neg, one, pos_or_zero);
    // NOT: where(is_nonzero, 0, 1)
    return selectOp(builder, is_nonzero, zero, one);
}

fn convertMin(builder: *Builder, inputs: []const NodeId) ConvertError!NodeId {
    if (inputs.len < 2) return error.MissingInput;
    var result = inputs[0];
    for (inputs[1..]) |inp| {
        // min(a, b) = where(a < b, a, b)
        const cmp = try cmpLessThan(builder, result, inp);
        result = try selectOp(builder, cmp, result, inp);
    }
    return result;
}

fn convertMax(builder: *Builder, inputs: []const NodeId) ConvertError!NodeId {
    if (inputs.len < 2) return error.MissingInput;
    var result = inputs[0];
    for (inputs[1..]) |inp| {
        // max(a, b) = where(b < a, a, b)
        const cmp = try cmpLessThan(builder, inp, result);
        result = try selectOp(builder, cmp, result, inp);
    }
    return result;
}

// ── Phase 2: Additional Activations ─────────────────────────────────

fn convertLeakyRelu(builder: *Builder, node: *const NodeProto, input: NodeId) ConvertError!NodeId {
    const alpha = getFloat(node.attributes, "alpha", 0.01);
    const dtype = builder.graph.node(input).output_shape.dtype;
    const zero = try builder.scalarConst(dtype, 0.0);
    const alpha_node = try builder.scalarConst(dtype, alpha);
    const scaled = try broadcastBinaryOp(builder, .mul, input, alpha_node);
    const is_neg = try broadcastBinaryOp(builder, .less_than, input, zero);
    return selectOp(builder, is_neg, scaled, input);
}

fn convertHardSigmoid(builder: *Builder, node: *const NodeProto, input: NodeId) ConvertError!NodeId {
    // HardSigmoid(x) = max(0, min(1, alpha * x + beta))
    const alpha = getFloat(node.attributes, "alpha", 0.2);
    const beta = getFloat(node.attributes, "beta", 0.5);
    const dtype = builder.graph.node(input).output_shape.dtype;
    const alpha_node = try builder.scalarConst(dtype, alpha);
    const beta_node = try builder.scalarConst(dtype, beta);
    const zero = try builder.scalarConst(dtype, 0.0);
    const one = try builder.scalarConst(dtype, 1.0);

    const scaled = try broadcastBinaryOp(builder, .mul, input, alpha_node);
    const shifted = try broadcastBinaryOp(builder, .add, scaled, beta_node);
    // Clip to [0, 1]
    const cmp_lo = try broadcastBinaryOp(builder, .less_than, shifted, zero);
    const clipped_lo = try selectOp(builder, cmp_lo, zero, shifted);
    const cmp_hi = try broadcastBinaryOp(builder, .less_than, one, clipped_lo);
    return selectOp(builder, cmp_hi, one, clipped_lo);
}

// ── Phase 2: Batch MatMul ───────────────────────────────────────────

fn convertMatMul(builder: *Builder, _: *const NodeProto, a: NodeId, b: NodeId) ConvertError!NodeId {
    const a_shape = builder.graph.node(a).output_shape;
    const b_shape = builder.graph.node(b).output_shape;

    if (a_shape.rank() == 0 or b_shape.rank() == 0) {
        return error.UnsupportedOp;
    }

    // ONNX MatMul promotion rules:
    // - 1D x 1D -> scalar
    // - 1D x ND -> prepend 1 to lhs, drop leading 1 from output
    // - ND x 1D -> append 1 to rhs, drop trailing 1 from output
    if (a_shape.rank() == 1 and b_shape.rank() == 1) {
        const k = a_shape.dim(0);
        if (k != b_shape.dim(0) and k >= 0 and b_shape.dim(0) >= 0) return error.ShapeMismatch;
        return builder.reduceSum(try builder.mul(a, b), &.{0});
    }

    if (a_shape.rank() == 1 and b_shape.rank() == 2) {
        const lhs = try builder.reshape(a, Shape.init(a_shape.dtype, &.{ 1, a_shape.dim(0) }));
        const mm = try builder.matmul(lhs, b);
        return builder.reshape(mm, Shape.init(a_shape.dtype, &.{b_shape.dim(1)}));
    }

    if (a_shape.rank() == 2 and b_shape.rank() == 1) {
        const rhs = try builder.reshape(b, Shape.init(b_shape.dtype, &.{ b_shape.dim(0), 1 }));
        const mm = try builder.matmul(a, rhs);
        return builder.reshape(mm, Shape.init(a_shape.dtype, &.{a_shape.dim(0)}));
    }

    // 2D case: standard matmul
    if (a_shape.rank() == 2 and b_shape.rank() == 2) {
        return builder.matmul(a, b);
    }

    // ONNX batched lhs x shared 2D weight matrix:
    // [..., M, K] x [K, N] -> [..., M, N]
    //
    // dot_general in our graph currently assumes paired lhs/rhs batch dims.
    // Lower this common case through a flattened 2D GEMM instead.
    if (a_shape.rank() >= 3 and b_shape.rank() == 2) {
        const flat_lhs = try builder.reshape(a, Shape.init(a_shape.dtype, &.{ -1, a_shape.dim(@intCast(a_shape.rank() - 1)) }));
        const mm = try builder.matmul(flat_lhs, b);

        var out_dims: [8]i64 = .{0} ** 8;
        for (0..a_shape.rank() - 1) |i| {
            out_dims[i] = a_shape.dim(@intCast(i));
        }
        out_dims[a_shape.rank() - 1] = b_shape.dim(1);
        const out_shape = Shape{
            .dtype = a_shape.dtype,
            .dims = out_dims,
            .rank_ = a_shape.rank(),
        };
        return builder.reshape(mm, out_shape);
    }

    // 3D+ batch matmul via dot_general
    // Contract last axis of A with second-to-last axis of B
    // Batch dims are all leading dims
    const a_rank = a_shape.rank();
    const b_rank = b_shape.rank();
    const num_batch = if (a_rank >= 2) a_rank - 2 else 0;

    var attrs = ml.graph.node.DotGeneralAttrs{};
    // Contracting: last of A, second-to-last of B
    attrs.lhs_contracting[0] = a_rank - 1;
    attrs.rhs_contracting[0] = if (b_rank >= 2) b_rank - 2 else 0;
    attrs.num_contracting = 1;

    // Batch dims: 0..num_batch
    attrs.num_batch = num_batch;
    for (0..num_batch) |i| {
        attrs.lhs_batch[i] = @intCast(i);
        attrs.rhs_batch[i] = @intCast(i);
    }

    // Output shape: batch_dims + [M, N]
    var out_dims: [8]i64 = .{0} ** 8;
    for (0..num_batch) |i| {
        out_dims[i] = a_shape.dim(@intCast(i));
    }
    out_dims[num_batch] = a_shape.dim(@intCast(a_rank - 2));
    out_dims[num_batch + 1] = b_shape.dim(@intCast(b_rank - 1));
    const out_shape = Shape{
        .dtype = a_shape.dtype,
        .dims = out_dims,
        .rank_ = @intCast(num_batch + 2),
    };

    return builder.graph.addNode(.{
        .op = .{ .dot_general = attrs },
        .output_shape = out_shape,
        .inputs = .{ a, b, null_node, null_node },
        .num_inputs = 2,
    });
}

// ── Phase 2: Shape Manipulation Ops ─────────────────────────────────

fn convertExpand(builder: *Builder, inputs: []const NodeId) ConvertError!NodeId {
    if (inputs.len < 2) return error.MissingInput;
    const in_shape = builder.graph.node(inputs[0]).output_shape;

    // Materialize target shape from constant input (may be behind Concat/Reshape/Gather)
    var expand_buf: [8]f32 = undefined;
    const shape_data = materializeConstantValues(builder, inputs[1], &expand_buf) orelse return error.ConstantMaterializationFailed;

    if (shape_data.len == 0) return inputs[0];

    // Build target dims, applying ONNX broadcast rules:
    // - Prepend 1s if ranks differ
    // - dims of 1 in target copy from input
    const target_rank: u8 = @intCast(shape_data.len);
    const in_rank = in_shape.rank();
    const out_rank = @max(target_rank, in_rank);

    var out_dims: [8]i64 = .{0} ** 8;
    for (0..out_rank) |i| {
        const ri: u8 = @intCast(i);
        const in_dim: i64 = if (ri >= out_rank - in_rank) in_shape.dim(ri - (out_rank - in_rank)) else 1;
        const target_dim: i64 = if (ri >= out_rank - target_rank) @intFromFloat(shape_data[ri - (out_rank - target_rank)]) else 1;
        out_dims[i] = if (target_dim == 1) in_dim else if (in_dim == 1) target_dim else target_dim;
    }

    const out_shape = Shape{ .dtype = in_shape.dtype, .dims = out_dims, .rank_ = out_rank };

    // If shapes match, no-op
    if (out_rank == in_rank) {
        var same = true;
        for (0..in_rank) |i| {
            if (out_dims[i] != in_shape.dim(@intCast(i))) {
                same = false;
                break;
            }
        }
        if (same) return inputs[0];
    }

    // Use broadcast_in_dim
    var broadcast_axes: [8]u8 = .{0} ** 8;
    const start = out_rank - in_rank;
    for (0..in_rank) |i| {
        broadcast_axes[i] = @intCast(start + i);
    }

    // If input needs prepended 1-dims first, reshape
    var src = inputs[0];
    if (in_rank < out_rank) {
        var padded_dims: [8]i64 = .{0} ** 8;
        for (0..start) |i| padded_dims[i] = 1;
        for (0..in_rank) |i| padded_dims[start + i] = in_shape.dim(@intCast(i));
        const padded_shape = Shape{ .dtype = in_shape.dtype, .dims = padded_dims, .rank_ = out_rank };
        src = try builder.reshape(src, padded_shape);
    }

    return builder.graph.addNode(.{
        .op = .{ .broadcast_in_dim = .{
            .target_shape = out_shape,
            .broadcast_axes = broadcast_axes,
            .num_axes = @min(in_rank, out_rank),
        } },
        .output_shape = out_shape,
        .inputs = .{ src, null_node, null_node, null_node },
        .num_inputs = 1,
    });
}

fn convertPad(allocator: std.mem.Allocator, builder: *Builder, node: *const NodeProto, inputs: []const NodeId) ConvertError!NodeId {
    _ = allocator;
    if (inputs.len < 1) return error.MissingInput;

    const in_shape = builder.graph.node(inputs[0]).output_shape;
    const rank = in_shape.rank();

    // Opset 11+ reads pads from inputs[1]; opset <11 reads from `pads` attribute.
    var pads_storage: [16]f32 = undefined;
    var pads_data: []const f32 = &.{};
    if (inputs.len >= 2 and inputs[1] != null_node) {
        pads_data = materializeConstantValues(builder, inputs[1], &pads_storage) orelse
            return error.ConstantMaterializationFailed;
    } else {
        const attr_pads = getInts(node.attributes, "pads");
        if (attr_pads.len == 0) return error.MissingInput;
        const n: usize = @min(attr_pads.len, pads_storage.len);
        for (0..n) |i| pads_storage[i] = @floatFromInt(attr_pads[i]);
        pads_data = pads_storage[0..n];
    }

    if (pads_data.len == 0) return inputs[0];
    if (pads_data.len % 2 != 0 or pads_data.len > @as(usize, rank) * 2) return error.ShapeMismatch;

    var normalized_pads_storage: [16]f32 = .{0} ** 16;
    if (pads_data.len < @as(usize, rank) * 2) {
        const pair_count = pads_data.len / 2;
        const start_axis = @as(usize, rank) - pair_count;
        for (0..pair_count) |i| {
            normalized_pads_storage[start_axis + i] = pads_data[i];
            normalized_pads_storage[@as(usize, rank) + start_axis + i] = pads_data[pair_count + i];
        }
        pads_data = normalized_pads_storage[0 .. @as(usize, rank) * 2];
    }

    // Check if all pads are zero
    var all_zero = true;
    for (pads_data) |p| {
        if (p != 0.0) {
            all_zero = false;
            break;
        }
    }
    if (all_zero) return inputs[0];

    const mode = getString(node.attributes, "mode", "constant");

    // Reflect / edge modes use gather-based padding
    if (std.mem.eql(u8, mode, "reflect") or std.mem.eql(u8, mode, "edge")) {
        return convertPadGather(builder, inputs[0], pads_data, rank, std.mem.eql(u8, mode, "reflect"));
    }

    if (!std.mem.eql(u8, mode, "constant")) return error.UnsupportedOp;

    // Constant value: opset 11+ reads from inputs[2]; opset <11 reads `value` attr (default 0).
    var pad_val_buf: [1]f32 = undefined;
    const pad_val: f32 = if (inputs.len >= 3 and inputs[2] != null_node)
        if (materializeConstantValues(builder, inputs[2], &pad_val_buf)) |v| v[0] else 0.0
    else
        getFloat(node.attributes, "value", 0.0);

    // Build padded tensor axis-by-axis using concat.
    // For each axis i: concat(zeros_before, current, zeros_after, axis=i)
    var current = inputs[0];
    for (0..rank) |i| {
        const pad_begin: i64 = @intFromFloat(pads_data[i]);
        const pad_end: i64 = @intFromFloat(pads_data[i + rank]);
        if (pad_begin == 0 and pad_end == 0) continue;

        const cur_shape = builder.graph.node(current).output_shape;

        if (pad_begin > 0) {
            const zero_node = try makePadTensor(builder, cur_shape, @intCast(i), pad_begin, pad_val);
            current = try builder.graph.addNode(.{
                .op = .{ .concat_prim = .{ .axis = @intCast(i) } },
                .output_shape = concatShape(cur_shape, @intCast(i), pad_begin),
                .inputs = .{ zero_node, current, null_node, null_node },
                .num_inputs = 2,
            });
        }
        if (pad_end > 0) {
            const cur_shape2 = builder.graph.node(current).output_shape;
            const zero_node = try makePadTensor(builder, cur_shape2, @intCast(i), pad_end, pad_val);
            current = try builder.graph.addNode(.{
                .op = .{ .concat_prim = .{ .axis = @intCast(i) } },
                .output_shape = concatShape(cur_shape2, @intCast(i), pad_end),
                .inputs = .{ current, zero_node, null_node, null_node },
                .num_inputs = 2,
            });
        }
    }
    return current;
}

fn convertSplit(allocator: std.mem.Allocator, builder: *Builder, node: *const NodeProto, inputs: []const NodeId, extra_outputs: ?[]NodeId) ConvertError!NodeId {
    _ = allocator;
    const in_shape = builder.graph.node(inputs[0]).output_shape;
    const axis_raw = getInt(node.attributes, "axis", 0);
    const axis: u8 = if (axis_raw < 0) @intCast(@as(i64, in_shape.rank()) + axis_raw) else @intCast(axis_raw);

    // Determine split sizes
    var split_sizes: [8]i64 = .{0} ** 8;
    var num_outputs: u8 = @intCast(node.outputs.len);

    if (inputs.len >= 2 and inputs[1] != null_node) {
        // Split sizes from constant input
        var sizes_buf: [8]f32 = undefined;
        const sizes_data = materializeConstantValues(builder, inputs[1], &sizes_buf) orelse return error.ConstantMaterializationFailed;
        num_outputs = @intCast(@min(sizes_data.len, 8));
        for (0..num_outputs) |i| split_sizes[i] = @intFromFloat(sizes_data[i]);
    } else {
        // Equal split
        const attr_split = getInts(node.attributes, "split");
        if (attr_split.len > 0) {
            num_outputs = @intCast(@min(attr_split.len, 8));
            for (0..num_outputs) |i| split_sizes[i] = attr_split[i];
        } else {
            const dim = in_shape.dim(axis);
            const size = @divTrunc(dim, @as(i64, num_outputs));
            for (0..num_outputs) |i| split_sizes[i] = size;
        }
    }

    // Generate all split slices
    var offset: i64 = 0;
    var primary: NodeId = null_node;

    for (0..num_outputs) |oi| {
        var slice_attrs = ml.graph.node.SliceAttrs{};
        slice_attrs.num_axes = in_shape.rank();
        for (0..in_shape.rank()) |d| {
            slice_attrs.starts[d] = 0;
            slice_attrs.limits[d] = in_shape.dim(@intCast(d));
            slice_attrs.strides[d] = 1;
        }
        slice_attrs.starts[axis] = offset;
        slice_attrs.limits[axis] = offset + split_sizes[oi];

        var out_dims: [8]i64 = in_shape.dims;
        out_dims[axis] = split_sizes[oi];
        const out_shape = Shape{ .dtype = in_shape.dtype, .dims = out_dims, .rank_ = in_shape.rank_ };

        const slice_id = try builder.graph.addNode(.{
            .op = .{ .slice = slice_attrs },
            .output_shape = out_shape,
            .inputs = .{ inputs[0], null_node, null_node, null_node },
            .num_inputs = 1,
        });

        if (oi == 0) {
            primary = slice_id;
        } else if (extra_outputs) |eo| {
            if (oi - 1 < eo.len) {
                eo[oi - 1] = slice_id;
            }
        }

        offset += split_sizes[oi];
    }

    return primary;
}

fn convertShape(builder: *Builder, node: *const NodeProto, input: NodeId) ConvertError!NodeId {
    // Materialize the requested input-shape slice as a 1-D tensor.
    const in_shape = builder.graph.node(input).output_shape;
    const full_rank = in_shape.rank();
    var start_i = getInt(node.attributes, "start", 0);
    var end_i = getInt(node.attributes, "end", full_rank);

    if (start_i < 0) start_i += full_rank;
    if (end_i < 0) end_i += full_rank;
    start_i = @max(0, @min(start_i, full_rank));
    end_i = @max(start_i, @min(end_i, full_rank));

    const start: usize = @intCast(start_i);
    const end: usize = @intCast(end_i);
    const rank: usize = end - start;
    var has_dynamic = false;
    var dims_f32: [8]f32 = .{0} ** 8;
    for (0..rank) |i| {
        const dim = in_shape.dim(@intCast(start + i));
        if (dim < 0) has_dynamic = true;
        dims_f32[i] = @floatFromInt(dim);
    }
    if (has_dynamic) {
        return builder.graph.addNode(.{
            .op = .{ .shape_of = .{
                .start = @intCast(start),
                .end = @intCast(end),
            } },
            .output_shape = Shape.init(.i64, &.{@intCast(rank)}),
            .inputs = .{ input, null_node, null_node, null_node },
            .num_inputs = 1,
        });
    }
    const out_shape = Shape.init(.f32, &.{@intCast(rank)});
    return builder.tensorConst(dims_f32[0..rank], out_shape);
}

fn convertConstantOfShape(allocator: std.mem.Allocator, builder: *Builder, node: *const NodeProto, inputs: []const NodeId) ConvertError!NodeId {
    // Input is a 1-D shape tensor; output is a tensor of that shape filled with a value
    var cos_buf: [8]f32 = undefined;
    const shape_data = materializeConstantValues(builder, inputs[0], &cos_buf) orelse return error.ConstantMaterializationFailed;

    // Get fill value from attribute (default 0)
    var fill_val: f32 = 0.0;
    if (getTensor(node.attributes, "value")) |tensor| {
        if (tensor.float_data.len >= 4) {
            fill_val = @bitCast(std.mem.readInt(u32, tensor.float_data[0..4], .little));
        } else if (tensor.raw_data.len >= 4) {
            fill_val = @bitCast(std.mem.readInt(u32, tensor.raw_data[0..4], .little));
        }
    }

    // Build target shape
    const rank: u8 = @intCast(@min(shape_data.len, 8));
    var dims: [8]i64 = .{0} ** 8;
    var total_elems: usize = 1;
    for (0..rank) |i| {
        dims[i] = @intFromFloat(shape_data[i]);
        if (dims[i] > 0) total_elems *= @intCast(dims[i]);
    }
    const out_shape = Shape{ .dtype = .f32, .dims = dims, .rank_ = rank };

    // For scalar shapes (rank=0 or total 1 element), use scalarConst
    if (total_elems <= 1) return builder.scalarConst(out_shape.dtype, fill_val);

    // Create a filled tensor constant
    const data = try allocator.alloc(f32, total_elems);
    defer allocator.free(data);
    @memset(data, fill_val);
    return builder.tensorConst(data, out_shape);
}

fn convertTile(builder: *Builder, inputs: []const NodeId) ConvertError!NodeId {
    if (inputs.len < 2) return error.MissingInput;
    // Tile repeats the input tensor along each dimension
    // For now, handle as broadcast_in_dim with repeated shape
    const in_shape = builder.graph.node(inputs[0]).output_shape;
    var repeats_buf: [8]f32 = undefined;
    const repeats_data = materializeConstantValues(builder, inputs[1], &repeats_buf) orelse return error.ConstantMaterializationFailed;

    const rank = in_shape.rank();
    var out_dims: [8]i64 = .{0} ** 8;
    for (0..rank) |i| {
        const rep: i64 = if (i < repeats_data.len) @intFromFloat(repeats_data[i]) else 1;
        out_dims[i] = in_shape.dim(@intCast(i)) * rep;
    }
    const out_shape = Shape{ .dtype = in_shape.dtype, .dims = out_dims, .rank_ = rank };

    var broadcast_axes: [8]u8 = .{0} ** 8;
    for (0..rank) |i| broadcast_axes[i] = @intCast(i);

    return builder.graph.addNode(.{
        .op = .{ .broadcast_in_dim = .{
            .target_shape = out_shape,
            .broadcast_axes = broadcast_axes,
            .num_axes = rank,
        } },
        .output_shape = out_shape,
        .inputs = .{ inputs[0], null_node, null_node, null_node },
        .num_inputs = 1,
    });
}

// ── Phase 2: Additional Reductions ──────────────────────────────────

const ReduceMinMaxTag = enum { min, max };

fn convertReduceMinMax(builder: *Builder, comptime op: ReduceMinMaxTag, node: *const NodeProto, inputs: []const NodeId) ConvertError!NodeId {
    // ReduceMin/Max with the same axis handling as other reduces
    return switch (op) {
        .min => {
            // ReduceMin: reduce_max of negated, then negate back
            const neg_input = try builder.neg(inputs[0]);
            const reduced = try convertReduce(builder, .reduce_max, node, &.{neg_input});
            return builder.neg(reduced);
        },
        .max => convertReduce(builder, .reduce_max, node, inputs),
    };
}

fn convertReduceProd(builder: *Builder, node: *const NodeProto, inputs: []const NodeId) ConvertError!NodeId {
    // ReduceProd: exp(reduce_sum(log(x)))
    const log_x = try builder.logOp(inputs[0]);
    const log_sum = try convertReduce(builder, .reduce_sum, node, &.{log_x});
    return builder.expOp(log_sum);
}

fn convertReduceL2(builder: *Builder, node: *const NodeProto, inputs: []const NodeId) ConvertError!NodeId {
    if (inputs.len < 1) return error.MissingInput;
    const squared = try builder.mul(inputs[0], inputs[0]);
    const reduced = try convertReduce(builder, .reduce_sum, node, &.{squared});
    return builder.sqrt(reduced);
}

// ── Phase 2: ScatterND ──────────────────────────────────────────────

fn convertScatterND(builder: *Builder, inputs: []const NodeId) ConvertError!NodeId {
    if (inputs.len < 3) return error.MissingInput;
    // ScatterND(data, indices, updates) — approximate with scatter_add
    const out_shape = builder.graph.node(inputs[0]).output_shape;
    return builder.graph.addNode(.{
        .op = .{ .scatter_add = .{ .axis = 0 } },
        .output_shape = out_shape,
        .inputs = .{ inputs[0], inputs[1], inputs[2], null_node },
        .num_inputs = 3,
    });
}

fn convertScatterElements(builder: *Builder, node: *const NodeProto, inputs: []const NodeId) ConvertError!NodeId {
    if (inputs.len < 3) return error.MissingInput;
    // ScatterElements(data, indices, updates, axis) — use scatter_add primitive
    const axis_raw = getInt(node.attributes, "axis", 0);
    const data_shape = builder.graph.node(inputs[0]).output_shape;
    const axis: u8 = if (axis_raw < 0) @intCast(@as(i64, data_shape.rank()) + axis_raw) else @intCast(axis_raw);

    return builder.graph.addNode(.{
        .op = .{ .scatter_add = .{ .axis = axis } },
        .output_shape = data_shape,
        .inputs = .{ inputs[0], inputs[1], inputs[2], null_node },
        .num_inputs = 3,
    });
}

// ── Phase 2: Range ──────────────────────────────────────────────────

fn convertRange(allocator: std.mem.Allocator, builder: *Builder, node: *const NodeProto, inputs: []const NodeId) ConvertError!NodeId {
    _ = allocator;
    _ = node;
    if (inputs.len < 3) return error.MissingInput;
    // Range(start, limit, delta) → 1-D tensor
    // Materialize start/limit/delta from constants (may be behind Cast/Reshape)
    var start_buf: [1]f32 = undefined;
    var limit_buf: [1]f32 = undefined;
    var delta_buf: [1]f32 = undefined;
    const start_data = materializeConstantValues(builder, inputs[0], &start_buf);
    const limit_data = materializeConstantValues(builder, inputs[1], &limit_buf);
    const delta_data = materializeConstantValues(builder, inputs[2], &delta_buf);

    const start = if (start_data) |d| (if (d.len > 0) d[0] else null) else null;
    const limit = if (limit_data) |d| (if (d.len > 0) d[0] else null) else null;
    const delta = if (delta_data) |d| (if (d.len > 0) d[0] else null) else null;

    // If any value is dynamic (-1) or not materializable, emit a parameter
    // so the caller provides position IDs at runtime (common GPT-2 pattern).
    const is_dynamic = (start == null or limit == null or delta == null or
        start.? < 0 or limit.? < 0 or delta.? < 0);

    if (is_dynamic) {
        const out_dtype = builder.graph.node(inputs[0]).output_shape.dtype;
        return builder.graph.addNode(.{
            .op = .{ .range = {} },
            .output_shape = Shape.init(out_dtype, &.{@as(i64, -1)}),
            .inputs = .{ inputs[0], inputs[1], inputs[2], null_node },
            .num_inputs = 3,
        });
    }

    const s = start.?;
    const l = limit.?;
    const d = delta.?;

    if (d == 0) return error.InvalidAttribute;

    const count: usize = @intFromFloat(@ceil((l - s) / d));
    if (count > 4096) return error.ShapeMismatch; // sanity limit

    // Build constant data
    var buf: [4096]f32 = undefined;
    for (0..count) |i| {
        buf[i] = s + @as(f32, @floatFromInt(i)) * d;
    }

    const out_shape = Shape.init(.f32, &.{@as(i64, @intCast(count))});
    return builder.tensorConst(buf[0..count], out_shape);
}

// ── Phase 2: Normalization Ops ──────────────────────────────────────

fn convertLayerNorm(builder: *Builder, node: *const NodeProto, inputs: []const NodeId) ConvertError!NodeId {
    if (inputs.len < 2) return error.MissingInput;
    const x = inputs[0];
    const scale = inputs[1];
    const has_bias = inputs.len >= 3 and inputs[2] != null_node;

    const eps = getFloat(node.attributes, "epsilon", 1e-5);
    const axis_raw = getInt(node.attributes, "axis", -1);
    const in_shape = builder.graph.node(x).output_shape;
    const axis: u8 = if (axis_raw < 0) @intCast(@as(i64, in_shape.rank()) + axis_raw) else @intCast(axis_raw);

    // Decomposed: (x - mean) / sqrt(var + eps) * scale + bias
    // Reduce over axes [axis, rank)
    var reduce_axes: [8]u8 = undefined;
    var num_reduce: u8 = 0;
    for (axis..in_shape.rank()) |i| {
        reduce_axes[num_reduce] = @intCast(i);
        num_reduce += 1;
    }

    const mean = try builder.reduceMean(x, reduce_axes[0..num_reduce]);
    const centered = try builder.sub(x, mean);
    const sq = try builder.mul(centered, centered);
    const variance = try builder.reduceMean(sq, reduce_axes[0..num_reduce]);
    const eps_node = try builder.scalarConst(in_shape.dtype, eps);
    const var_eps = try builder.add(variance, eps_node);
    const inv_std = try builder.rsqrt(var_eps);
    const normed = try builder.mul(centered, inv_std);
    const scaled = try builder.mul(normed, scale);
    const decomposed = if (has_bias) try builder.add(scaled, inputs[2]) else scaled;

    // Emit fused node
    const last_dim: u32 = @intCast(in_shape.dim(in_shape.rank() - 1));
    const fused = try builder.graph.addNode(.{
        .op = .{ .fused_layer_norm = .{ .dim = last_dim, .eps = eps } },
        .output_shape = in_shape,
        .inputs = if (has_bias)
            .{ x, scale, inputs[2], null_node }
        else
            .{ x, scale, null_node, null_node },
        .num_inputs = if (has_bias) 3 else 2,
        .vjp_alternate = decomposed,
    });
    return fused;
}

fn convertSimplifiedLayerNorm(builder: *Builder, node: *const NodeProto, inputs: []const NodeId) ConvertError!NodeId {
    if (inputs.len < 2) return error.MissingInput;
    // SimplifiedLayerNormalization = RMSNorm
    const x = inputs[0];
    const scale = inputs[1];
    const eps = getFloat(node.attributes, "epsilon", 1e-5);
    const in_shape = builder.graph.node(x).output_shape;
    const last_dim: u32 = @intCast(in_shape.dim(in_shape.rank() - 1));
    return builder.rmsNorm(x, scale, last_dim, eps);
}

fn convertBatchNorm(builder: *Builder, node: *const NodeProto, inputs: []const NodeId) ConvertError!NodeId {
    if (inputs.len < 5) return error.MissingInput;
    // BatchNormalization(X, scale, B, input_mean, input_var)
    // ONNX spec: scale/B/mean/var all have shape [C] where C = X.shape[1].
    // Broadcasting is channel-axis (axis 1), not the trailing axis, so we
    // explicitly reshape [C] to [1, C, 1, 1, ...] before the math.
    // y = scale * (x - mean) / sqrt(var + eps) + bias
    const x = inputs[0];
    const scale_1d = inputs[1];
    const bias_1d = inputs[2];
    const mean_1d = inputs[3];
    const variance_1d = inputs[4];

    const eps = getFloat(node.attributes, "epsilon", 1e-5);
    const in_shape = builder.graph.node(x).output_shape;
    const dtype = in_shape.dtype;
    const rank = in_shape.rank();

    // Reshape 1D params [C] → [1, C, 1, 1, ...] matching X's rank.
    const c_dim = builder.graph.node(scale_1d).output_shape.dim(0);
    const scale = if (rank > 1) try channelReshape(builder, scale_1d, c_dim, rank) else scale_1d;
    const bias = if (rank > 1) try channelReshape(builder, bias_1d, c_dim, rank) else bias_1d;
    const mean = if (rank > 1) try channelReshape(builder, mean_1d, c_dim, rank) else mean_1d;
    const variance = if (rank > 1) try channelReshape(builder, variance_1d, c_dim, rank) else variance_1d;

    const eps_node = try builder.scalarConst(dtype, eps);
    const var_eps = try broadcastBinaryOp(builder, .add, variance, eps_node);
    const inv_std = try builder.rsqrt(var_eps);
    const centered = try broadcastBinaryOp(builder, .sub, x, mean);
    const normed = try broadcastBinaryOp(builder, .mul, centered, inv_std);
    const scaled = try broadcastBinaryOp(builder, .mul, normed, scale);
    return broadcastBinaryOp(builder, .add, scaled, bias);
}

/// Reshape [C] to [1, C, 1, 1, ...] for channel-axis broadcasting.
fn channelReshape(builder: *Builder, input: NodeId, c_dim: i64, rank: u8) ConvertError!NodeId {
    const src_shape = builder.graph.node(input).output_shape;
    var dims: [8]i64 = .{1} ** 8;
    dims[1] = c_dim;
    const new_shape = Shape{ .dtype = src_shape.dtype, .dims = dims, .rank_ = rank };
    return builder.reshape(input, new_shape);
}

fn convertInstanceNorm(builder: *Builder, node: *const NodeProto, inputs: []const NodeId) ConvertError!NodeId {
    if (inputs.len < 3) return error.MissingInput;
    // InstanceNormalization(X, scale, B) — normalize over spatial dims per (N, C)
    // y = scale * (x - mean) / sqrt(var + eps) + bias
    // For NCHW: reduce over dims [2, 3, ...], scale/bias broadcast along C (dim 1)
    const x = inputs[0];
    const scale = inputs[1];
    const bias = inputs[2];
    const eps = getFloat(node.attributes, "epsilon", 1e-5);
    const in_shape = builder.graph.node(x).output_shape;
    const dtype = in_shape.dtype;

    // Reduce over spatial dims (all dims except batch=0 and channel=1)
    var reduce_axes: [8]u8 = undefined;
    var num_reduce: u8 = 0;
    for (2..in_shape.rank()) |i| {
        reduce_axes[num_reduce] = @intCast(i);
        num_reduce += 1;
    }

    const mean = try builder.reduceMean(x, reduce_axes[0..num_reduce]);
    const centered = try builder.sub(x, mean);
    const sq = try builder.mul(centered, centered);
    const variance = try builder.reduceMean(sq, reduce_axes[0..num_reduce]);
    const eps_node = try builder.scalarConst(dtype, eps);
    const var_eps = try builder.add(variance, eps_node);
    const inv_std = try builder.rsqrt(var_eps);
    const normed = try builder.mul(centered, inv_std);
    // scale and bias have shape [C] — broadcast along channel dim
    const scaled = try broadcastBinaryOp(builder, .mul, normed, scale);
    return broadcastBinaryOp(builder, .add, scaled, bias);
}

fn convertGroupNorm(builder: *Builder, node: *const NodeProto, inputs: []const NodeId) ConvertError!NodeId {
    if (inputs.len < 3) return error.MissingInput;
    // GroupNormalization(X, scale, bias) — ONNX opset 18
    // num_groups groups, normalize over (C/groups + spatial) per group
    // Decompose as: reshape to [N, G, C/G, spatial...], normalize, reshape back
    const x = inputs[0];
    const scale = inputs[1];
    const bias = inputs[2];
    const eps = getFloat(node.attributes, "epsilon", 1e-5);
    const num_groups_raw = getInt(node.attributes, "num_groups", 1);
    const num_groups: i64 = num_groups_raw;
    const in_shape = builder.graph.node(x).output_shape;
    const dtype = in_shape.dtype;

    // Input: [N, C, spatial...]. Reshape to [N, G, C/G, spatial...]
    const n_dim = in_shape.dim(0);
    const c_dim = in_shape.dim(1);
    const c_per_g = @divExact(c_dim, num_groups);

    // Build reshaped dims: [N, G, C/G, d2, d3, ...]
    var reshape_dims: [8]i64 = .{0} ** 8;
    reshape_dims[0] = n_dim;
    reshape_dims[1] = num_groups;
    reshape_dims[2] = c_per_g;
    const extra_spatial = in_shape.rank() - 2;
    for (0..extra_spatial) |i| {
        reshape_dims[3 + i] = in_shape.dim(@intCast(2 + i));
    }
    const reshape_rank: u8 = @intCast(in_shape.rank() + 1);
    const grouped_shape = Shape{ .dtype = dtype, .dims = reshape_dims, .rank_ = reshape_rank };
    const reshaped = try builder.reshape(x, grouped_shape);

    // Reduce over dims [2, 3, ...] (C/G + spatial, keeping N and G)
    var reduce_axes: [8]u8 = undefined;
    var num_reduce: u8 = 0;
    for (2..reshape_rank) |i| {
        reduce_axes[num_reduce] = @intCast(i);
        num_reduce += 1;
    }

    const mean = try builder.reduceMean(reshaped, reduce_axes[0..num_reduce]);
    const centered = try builder.sub(reshaped, mean);
    const sq = try builder.mul(centered, centered);
    const variance = try builder.reduceMean(sq, reduce_axes[0..num_reduce]);
    const eps_node = try builder.scalarConst(dtype, eps);
    const var_eps = try builder.add(variance, eps_node);
    const inv_std = try builder.rsqrt(var_eps);
    const normed = try builder.mul(centered, inv_std);

    // Reshape back to [N, C, spatial...]
    const unreshaped = try builder.reshape(normed, in_shape);

    // Apply scale and bias (shape [C]) — broadcast along channel dim
    const scaled = try broadcastBinaryOp(builder, .mul, unreshaped, scale);
    return broadcastBinaryOp(builder, .add, scaled, bias);
}

fn convertSkipLayerNorm(builder: *Builder, node: *const NodeProto, inputs: []const NodeId) ConvertError!NodeId {
    if (inputs.len < 3) return error.MissingInput;
    // SkipLayerNormalization(input, skip, gamma, beta?, bias?)
    // output = LayerNorm(input + skip + bias, gamma, beta)
    const x = inputs[0];
    const skip = inputs[1];
    const gamma = inputs[2];
    const has_beta = inputs.len >= 4 and inputs[3] != null_node;
    const has_bias = inputs.len >= 5 and inputs[4] != null_node;

    const eps = getFloat(node.attributes, "epsilon", 1e-5);
    const in_shape = builder.graph.node(x).output_shape;

    // Residual: input + skip (+ bias if present)
    var residual = try broadcastBinaryOp(builder, .add, x, skip);
    if (has_bias) {
        residual = try broadcastBinaryOp(builder, .add, residual, inputs[4]);
    }

    // LayerNorm over last axis
    const last_dim: u32 = @intCast(in_shape.dim(in_shape.rank() - 1));
    var reduce_axes: [1]u8 = .{in_shape.rank() - 1};

    const mean = try builder.reduceMean(residual, &reduce_axes);
    const centered = try builder.sub(residual, mean);
    const sq = try builder.mul(centered, centered);
    const variance = try builder.reduceMean(sq, &reduce_axes);
    const eps_node = try builder.scalarConst(in_shape.dtype, eps);
    const var_eps = try builder.add(variance, eps_node);
    const inv_std = try builder.rsqrt(var_eps);
    const normed = try builder.mul(centered, inv_std);
    const scaled = try broadcastBinaryOp(builder, .mul, normed, gamma);
    const decomposed = if (has_beta) try broadcastBinaryOp(builder, .add, scaled, inputs[3]) else scaled;

    // Emit fused layer_norm
    const fused = try builder.graph.addNode(.{
        .op = .{ .fused_layer_norm = .{ .dim = last_dim, .eps = eps } },
        .output_shape = in_shape,
        .inputs = if (has_beta)
            .{ residual, gamma, inputs[3], null_node }
        else
            .{ residual, gamma, null_node, null_node },
        .num_inputs = if (has_beta) 3 else 2,
        .vjp_alternate = decomposed,
    });
    return fused;
}

fn convertNonZero(builder: *Builder, input: NodeId) ConvertError!NodeId {
    // NonZero returns indices of non-zero elements. This is fundamentally
    // data-dependent (output shape depends on input values), which can't be
    // represented in a static graph. Pass through the input as a placeholder.
    _ = builder;
    return input;
}

fn convertTopK(builder: *Builder, node: *const NodeProto, inputs: []const NodeId) ConvertError!NodeId {
    if (inputs.len < 2) return error.MissingInput;
    // TopK(X, K) → (values, indices) along axis
    // Decompose: sort is not available as a primitive.
    // Use reduce_max + gather as a rough approximation for K=1 (argmax case).
    // For general K, this requires a sort primitive. Pass through for now.
    _ = node;
    _ = builder;
    return inputs[0];
}

fn convertOneHot(allocator: std.mem.Allocator, builder: *Builder, node: *const NodeProto, inputs: []const NodeId) ConvertError!NodeId {
    _ = allocator;
    if (inputs.len < 3) return error.MissingInput;
    // OneHot(indices, depth, values) where values=[off_value, on_value]
    // Output shape: indices_shape with depth inserted at axis
    const indices = inputs[0];
    const idx_shape = builder.graph.node(indices).output_shape;
    const axis_raw = getInt(node.attributes, "axis", -1);
    const axis: u8 = if (axis_raw < 0) @intCast(@as(i64, idx_shape.rank()) + 1 + axis_raw) else @intCast(axis_raw);

    // Materialize depth
    var depth_buf: [1]f32 = undefined;
    const depth_data = materializeConstantValues(builder, inputs[1], &depth_buf) orelse
        return error.ConstantMaterializationFailed;
    const depth: i64 = @intFromFloat(depth_data[0]);

    // Materialize on/off values
    var val_buf: [2]f32 = undefined;
    const val_data = materializeConstantValues(builder, inputs[2], &val_buf);
    const on_val: f32 = if (val_data) |v| (if (v.len >= 2) v[1] else 1.0) else 1.0;
    const off_val: f32 = if (val_data) |v| (if (v.len >= 1) v[0] else 0.0) else 0.0;
    _ = on_val;
    _ = off_val;

    // Build output shape: insert depth at axis position
    var out_dims: [8]i64 = .{0} ** 8;
    const out_rank: u8 = idx_shape.rank() + 1;
    var src: u8 = 0;
    for (0..out_rank) |i| {
        if (i == axis) {
            out_dims[i] = depth;
        } else {
            out_dims[i] = idx_shape.dim(src);
            src += 1;
        }
    }
    const out_shape = Shape{ .dtype = idx_shape.dtype, .dims = out_dims, .rank_ = out_rank };

    // OneHot can't be cleanly decomposed without comparison broadcasting
    // over the depth axis. Emit as a parameter placeholder with correct shape.
    return builder.graph.addNode(.{
        .op = .{ .broadcast_in_dim = .{
            .target_shape = out_shape,
            .num_axes = idx_shape.rank(),
        } },
        .output_shape = out_shape,
        .inputs = .{ indices, null_node, null_node, null_node },
        .num_inputs = 1,
    });
}

// ── Phase 2: Full ONNX Gather with axis ─────────────────────────────

// (Updated convertGather is already at its original location above)

// ── Phase 3: Convolution ────────────────────────────────────────────

fn convertConv(builder: *Builder, node: *const NodeProto, inputs: []const NodeId) ConvertError!NodeId {
    if (inputs.len < 2) return error.MissingInput;
    const x = inputs[0];
    const w = inputs[1];
    const has_bias = inputs.len >= 3 and inputs[2] != null_node;

    const x_shape = builder.graph.node(x).output_shape;
    const w_shape = builder.graph.node(w).output_shape;
    if (x_shape.rank() < 3 or w_shape.rank() < 3) return error.UnsupportedOp; // need at least [N, C, spatial...] and [O, I, spatial...]
    const num_spatial: u8 = w_shape.rank() - 2; // kernel rank defines conv dimensionality
    if (x_shape.rank() < num_spatial + 2) return error.UnsupportedOp;

    // Extract attributes
    const strides_attr = getInts(node.attributes, "strides");
    const pads_attr = getInts(node.attributes, "pads");
    const dilations_attr = getInts(node.attributes, "dilations");
    const group: u32 = @intCast(getInt(node.attributes, "group", 1));

    var conv_attrs = ml.graph.node.ConvAttrs{};
    conv_attrs.num_spatial = num_spatial;
    conv_attrs.groups = group;

    // Strides
    for (0..num_spatial) |i| {
        conv_attrs.strides[i] = if (i < strides_attr.len) @intCast(strides_attr[i]) else 1;
    }

    // Padding: ONNX format is [begin_0, begin_1, ..., end_0, end_1, ...]
    if (pads_attr.len > 0) {
        for (0..num_spatial) |i| {
            conv_attrs.padding[i][0] = if (i < pads_attr.len) @intCast(pads_attr[i]) else 0;
            conv_attrs.padding[i][1] = if (i + num_spatial < pads_attr.len) @intCast(pads_attr[i + num_spatial]) else 0;
        }
    }

    // Compute output spatial dims
    // out_dim = (in_dim + pad_begin + pad_end - dilation*(kernel-1) - 1) / stride + 1
    var out_dims: [8]i64 = .{0} ** 8;
    out_dims[0] = x_shape.dim(0); // batch
    out_dims[1] = w_shape.dim(0); // output channels

    for (0..num_spatial) |i| {
        const in_d = x_shape.dim(@intCast(i + 2));
        const k_d = w_shape.dim(@intCast(i + 2));
        const stride: i64 = conv_attrs.strides[i];
        const pad_begin: i64 = conv_attrs.padding[i][0];
        const pad_end: i64 = conv_attrs.padding[i][1];
        const dilation: i64 = if (i < dilations_attr.len) dilations_attr[i] else 1;
        const effective_k = dilation * (k_d - 1) + 1;
        out_dims[i + 2] = @divTrunc(in_d + pad_begin + pad_end - effective_k, stride) + 1;
    }
    const out_shape = Shape{ .dtype = x_shape.dtype, .dims = out_dims, .rank_ = x_shape.rank_ };

    // Build conv_general path (also serves as vjp_alternate for the fused node).
    var decomposed = try builder.graph.addNode(.{
        .op = .{ .conv_general = conv_attrs },
        .output_shape = out_shape,
        .inputs = .{ x, w, null_node, null_node },
        .num_inputs = 2,
    });

    // Fold bias into the decomposed path.
    if (has_bias) {
        decomposed = try addConvBias(builder, decomposed, inputs[2], out_shape);
    }

    // Try to emit fused_conv1d / fused_conv2d when eligible.
    if (try tryConvFused(builder, node, inputs, x, w, x_shape, w_shape, out_shape, conv_attrs, dilations_attr, group, num_spatial, decomposed)) |fused| {
        return fused;
    }

    return decomposed;
}

/// Add bias to a conv result. Bias is 1-D [out_channels]; reshape to [1, C, 1, ...] to
/// broadcast along the channel axis, matching ONNX Conv semantics.
fn addConvBias(builder: *Builder, conv_result: NodeId, bias: NodeId, out_shape: Shape) ConvertError!NodeId {
    const bias_shape = builder.graph.node(bias).output_shape;
    if (bias_shape.rank() == 1 and out_shape.rank() >= 3) {
        var bias_dims: [8]i64 = .{0} ** 8;
        bias_dims[0] = 1;
        bias_dims[1] = bias_shape.dim(0);
        for (2..out_shape.rank()) |i| bias_dims[i] = 1;
        const reshaped_bias_shape = Shape{ .dtype = bias_shape.dtype, .dims = bias_dims, .rank_ = out_shape.rank_ };
        const reshaped_bias = try builder.reshape(bias, reshaped_bias_shape);
        return builder.add(conv_result, reshaped_bias);
    }
    return builder.add(conv_result, bias);
}

/// Attempt to emit a fused_conv1d / fused_conv2d node. Returns null_node when the op
/// is not eligible (asymmetric pads, dilation != 1, dynamic shapes, unsupported groups).
/// Returns null on allocation failure via the error union.
fn tryConvFused(
    builder: *Builder,
    node: *const NodeProto,
    inputs: []const NodeId,
    x: NodeId,
    w: NodeId,
    x_shape: Shape,
    w_shape: Shape,
    out_shape: Shape,
    conv_attrs: ml.graph.node.ConvAttrs,
    dilations_attr: []const i64,
    group: u32,
    num_spatial: u8,
    decomposed: NodeId,
) ConvertError!?NodeId {
    _ = node;
    if (num_spatial != 1 and num_spatial != 2) return null;

    // Fused ops need all spatial dilations to be 1.
    for (0..num_spatial) |i| {
        const d = if (i < dilations_attr.len) dilations_attr[i] else 1;
        if (d != 1) return null;
    }

    // Fused ops only support symmetric padding (single value per spatial dim).
    for (0..num_spatial) |i| {
        if (conv_attrs.padding[i][0] != conv_attrs.padding[i][1]) return null;
    }

    // fused_conv1d has no groups support.
    if (num_spatial == 1 and group != 1) return null;

    // All input, weight, and output dims must be statically known.
    for (0..x_shape.rank()) |i| {
        if (x_shape.dim(@intCast(i)) <= 0) return null;
    }
    for (0..w_shape.rank()) |i| {
        if (w_shape.dim(@intCast(i)) <= 0) return null;
    }
    for (0..out_shape.rank()) |i| {
        if (out_shape.dim(@intCast(i)) <= 0) return null;
    }

    // Synthesize a zero bias if the ONNX op has none — fused conv always takes 3 inputs.
    const bias = if (inputs.len >= 3 and inputs[2] != null_node)
        inputs[2]
    else blk: {
        const out_channels: usize = @intCast(w_shape.dim(0));
        const zeros = try builder.graph.allocator.alloc(f32, out_channels);
        defer builder.graph.allocator.free(zeros);
        @memset(zeros, 0);
        const bias_shape = Shape.init(x_shape.dtype, &.{@intCast(out_channels)});
        break :blk try builder.tensorConst(zeros, bias_shape);
    };

    const batch: u32 = @intCast(x_shape.dim(0));
    const in_channels: u32 = @intCast(x_shape.dim(1));
    const out_channels: u32 = @intCast(w_shape.dim(0));

    if (num_spatial == 1) {
        const time_steps: u32 = @intCast(x_shape.dim(2));
        const kernel_size: u32 = @intCast(w_shape.dim(2));
        return try builder.graph.addNode(.{
            .op = .{ .fused_conv1d = .{
                .batch = batch,
                .in_channels = in_channels,
                .out_channels = out_channels,
                .time_steps = time_steps,
                .kernel_size = kernel_size,
                .stride = @intCast(conv_attrs.strides[0]),
                .padding = @intCast(conv_attrs.padding[0][0]),
            } },
            .output_shape = out_shape,
            .inputs = .{ x, w, bias, null_node },
            .num_inputs = 3,
            .vjp_alternate = decomposed,
        });
    }

    // num_spatial == 2
    const height: u32 = @intCast(x_shape.dim(2));
    const width: u32 = @intCast(x_shape.dim(3));
    const kernel_h: u32 = @intCast(w_shape.dim(2));
    const kernel_w: u32 = @intCast(w_shape.dim(3));
    return try builder.graph.addNode(.{
        .op = .{ .fused_conv2d = .{
            .batch = batch,
            .in_channels = in_channels,
            .out_channels = out_channels,
            .height = height,
            .width = width,
            .kernel_h = kernel_h,
            .kernel_w = kernel_w,
            .stride_h = @intCast(conv_attrs.strides[0]),
            .stride_w = @intCast(conv_attrs.strides[1]),
            .padding_h = @intCast(conv_attrs.padding[0][0]),
            .padding_w = @intCast(conv_attrs.padding[1][0]),
            .groups = group,
        } },
        .output_shape = out_shape,
        .inputs = .{ x, w, bias, null_node },
        .num_inputs = 3,
        .vjp_alternate = decomposed,
    });
}

// ── Phase 3: Attention Ops ──────────────────────────────────────────

fn convertMultiHeadAttention(builder: *Builder, node: *const NodeProto, inputs: []const NodeId) ConvertError!NodeId {
    if (inputs.len < 3) return error.MissingInput;
    const query = inputs[0];
    const key = inputs[1];
    const value = inputs[2];

    const num_heads: u32 = @intCast(getInt(node.attributes, "num_heads", 1));
    const is_causal = getInt(node.attributes, "unidirectional", 0) != 0;
    // Optional attention mask (4th input in Microsoft's MHA)
    const mask: NodeId = if (inputs.len >= 4 and inputs[3] != null_node) inputs[3] else null_node;

    const q_shape = builder.graph.node(query).output_shape;
    const q_rank = q_shape.rank();

    if (q_rank == 4) {
        // Already 4D: [batch, heads, seq, dim] — use as-is
        const batch: u32 = @intCast(q_shape.dim(0));
        const seq_len: u32 = @intCast(q_shape.dim(2));
        const head_dim: u32 = @intCast(q_shape.dim(3));

        // Reshape to [B*H, S, D] for sdpa
        const bh: i64 = @intCast(@as(u32, batch) * num_heads);
        const s: i64 = @intCast(seq_len);
        const d: i64 = @intCast(head_dim);
        const flat_shape = Shape.init(q_shape.dtype, &.{ bh, s, d });

        const q_flat = try builder.reshape(query, flat_shape);
        const k_flat = try builder.reshape(key, flat_shape);
        const v_flat = try builder.reshape(value, flat_shape);

        const attn = try emitAttention(builder, q_flat, k_flat, v_flat, batch, seq_len, num_heads, head_dim, is_causal, mask);

        // Reshape back to [B, H, S, D]
        return builder.reshape(attn, q_shape);
    }

    // 3D: [batch, seq, hidden] — reshape to 4D
    const batch: u32 = @intCast(q_shape.dim(0));
    const seq_len: u32 = @intCast(q_shape.dim(1));
    const hidden: u32 = @intCast(q_shape.dim(2));
    const head_dim: u32 = hidden / num_heads;

    // Reshape Q: [B, S, H*D] → [B, S, H, D] → transpose → [B, H, S, D] → [B*H, S, D]
    const r4_shape = Shape.init(q_shape.dtype, &.{ @as(i64, batch), @as(i64, seq_len), @as(i64, num_heads), @as(i64, head_dim) });
    const q4 = try builder.reshape(query, r4_shape);
    const q_t = try builder.transpose(q4, &.{ 0, 2, 1, 3 });
    const bh: i64 = @intCast(@as(u32, batch) * num_heads);
    const flat_shape = Shape.init(q_shape.dtype, &.{ bh, @as(i64, seq_len), @as(i64, head_dim) });
    const q_flat = try builder.reshape(q_t, flat_shape);

    // Key
    const k_shape = builder.graph.node(key).output_shape;
    const kv_seq: u32 = @intCast(k_shape.dim(1));
    const k4_shape = Shape.init(k_shape.dtype, &.{ @as(i64, batch), @as(i64, kv_seq), @as(i64, num_heads), @as(i64, head_dim) });
    const k4 = try builder.reshape(key, k4_shape);
    const k_t = try builder.transpose(k4, &.{ 0, 2, 1, 3 });
    const k_flat_shape = Shape.init(k_shape.dtype, &.{ bh, @as(i64, kv_seq), @as(i64, head_dim) });
    const k_flat = try builder.reshape(k_t, k_flat_shape);

    // Value
    const v_shape = builder.graph.node(value).output_shape;
    const v_head_dim: u32 = @intCast(@divTrunc(v_shape.dim(2), @as(i64, num_heads)));
    const v4_shape = Shape.init(v_shape.dtype, &.{ @as(i64, batch), @as(i64, kv_seq), @as(i64, num_heads), @as(i64, v_head_dim) });
    const v4 = try builder.reshape(value, v4_shape);
    const v_t = try builder.transpose(v4, &.{ 0, 2, 1, 3 });
    const v_flat_shape = Shape.init(v_shape.dtype, &.{ bh, @as(i64, kv_seq), @as(i64, v_head_dim) });
    const v_flat = try builder.reshape(v_t, v_flat_shape);

    // Cross-attention when Q and K/V have different sequence lengths
    const attn = if (seq_len != kv_seq)
        try emitCrossAttention(builder, q_flat, k_flat, v_flat, batch, seq_len, kv_seq, num_heads, head_dim, mask)
    else
        try emitAttention(builder, q_flat, k_flat, v_flat, batch, seq_len, num_heads, head_dim, is_causal, mask);

    // Reshape back: [B*H, S, D] → [B, H, S, D] → transpose → [B, S, H, D] → [B, S, H*D]
    const r4_out = Shape.init(q_shape.dtype, &.{ @as(i64, batch), @as(i64, num_heads), @as(i64, seq_len), @as(i64, v_head_dim) });
    const out4 = try builder.reshape(attn, r4_out);
    const out_t = try builder.transpose(out4, &.{ 0, 2, 1, 3 });
    const out_shape = Shape.init(q_shape.dtype, &.{ @as(i64, batch), @as(i64, seq_len), @as(i64, num_heads * v_head_dim) });
    return builder.reshape(out_t, out_shape);
}

/// Emit either fused_sdpa or fused_causal_self_attention depending on is_causal.
/// Both share the same decomposition (the causal mask is applied by the backend).
/// If mask is not null_node, it is passed as the 4th fused node input.
fn emitAttention(builder: *Builder, Q: NodeId, K: NodeId, V: NodeId, batch: u32, seq_len: u32, num_heads: u32, head_dim: u32, is_causal: bool, mask: NodeId) !NodeId {
    if (is_causal) {
        // Build same decomposition as sdpa (for vjp_alternate)
        const decomposed = try builder.sdpa(Q, K, V, batch, seq_len, num_heads, head_dim);
        // Get the vjp_alternate from the fused node sdpa emitted
        // and re-wrap with fused_causal_self_attention
        const sdpa_node = builder.graph.node(decomposed);
        const out_shape = sdpa_node.output_shape;
        const vjp = sdpa_node.vjp_alternate;
        return builder.graph.addNode(.{
            .op = .{ .fused_causal_self_attention = .{
                .batch = batch,
                .seq_len = seq_len,
                .num_heads = num_heads,
                .head_dim = head_dim,
            } },
            .output_shape = out_shape,
            .inputs = .{ Q, K, V, mask },
            .num_inputs = if (mask != null_node) 4 else 3,
            .vjp_alternate = vjp,
        });
    }
    // Non-causal SDPA, optionally with mask
    const sdpa = try builder.sdpa(Q, K, V, batch, seq_len, num_heads, head_dim);
    if (mask != null_node) {
        // Re-create with mask in 4th slot
        const sdpa_node = builder.graph.node(sdpa);
        return builder.graph.addNode(.{
            .op = sdpa_node.op,
            .output_shape = sdpa_node.output_shape,
            .inputs = .{ Q, K, V, mask },
            .num_inputs = 4,
            .vjp_alternate = sdpa_node.vjp_alternate,
        });
    }
    return sdpa;
}

/// Emit fused_cross_attention for encoder-decoder attention (different Q/KV seq lengths).
fn emitCrossAttention(builder: *Builder, Q: NodeId, K: NodeId, V: NodeId, batch: u32, dec_seq: u32, enc_seq: u32, num_heads: u32, head_dim: u32, mask: NodeId) !NodeId {
    // Build decomposed subgraph for vjp_alternate (same as sdpa but with different seq dims)
    const decomposed = try builder.sdpa(Q, K, V, batch, dec_seq, num_heads, head_dim);
    const sdpa_node = builder.graph.node(decomposed);
    const out_shape = sdpa_node.output_shape;
    const vjp = sdpa_node.vjp_alternate;
    return builder.graph.addNode(.{
        .op = .{ .fused_cross_attention = .{
            .batch = batch,
            .dec_seq = dec_seq,
            .enc_seq = enc_seq,
            .num_heads = num_heads,
            .head_dim = head_dim,
        } },
        .output_shape = out_shape,
        .inputs = .{ Q, K, V, mask },
        .num_inputs = if (mask != null_node) 4 else 3,
        .vjp_alternate = vjp,
    });
}

fn convertGroupQueryAttention(builder: *Builder, node: *const NodeProto, inputs: []const NodeId) ConvertError!NodeId {
    if (inputs.len < 3) return error.MissingInput;
    const query = inputs[0];
    const key = inputs[1];
    const value = inputs[2];

    const num_heads: u32 = @intCast(getInt(node.attributes, "num_heads", 1));
    const kv_num_heads: u32 = @intCast(getInt(node.attributes, "kv_num_heads", 1));
    // GQA is typically causal; also check unidirectional attribute
    const is_causal = getInt(node.attributes, "unidirectional", 1) != 0;

    const q_shape = builder.graph.node(query).output_shape;
    const k_shape = builder.graph.node(key).output_shape;
    const batch: u32 = @intCast(q_shape.dim(0));
    const q_seq: u32 = @intCast(q_shape.dim(1));
    const head_dim: u32 = @intCast(@divTrunc(q_shape.dim(2), @as(i64, num_heads)));
    const kv_seq: u32 = @intCast(k_shape.dim(1));

    // Reshape Q: [B, S, H*D] → [B, H, S, D] → [B*H, S, D]
    const q4 = Shape.init(q_shape.dtype, &.{ @as(i64, batch), @as(i64, q_seq), @as(i64, num_heads), @as(i64, head_dim) });
    const q_r = try builder.reshape(query, q4);
    const q_t = try builder.transpose(q_r, &.{ 0, 2, 1, 3 });
    const bh: i64 = @intCast(@as(u32, batch) * num_heads);
    const q_flat = try builder.reshape(q_t, Shape.init(q_shape.dtype, &.{ bh, @as(i64, q_seq), @as(i64, head_dim) }));

    // Reshape K: [B, S, KVH*D] → [B, KVH, S, D]
    const k4 = Shape.init(k_shape.dtype, &.{ @as(i64, batch), @as(i64, kv_seq), @as(i64, kv_num_heads), @as(i64, head_dim) });
    const k_r = try builder.reshape(key, k4);
    const k_t = try builder.transpose(k_r, &.{ 0, 2, 1, 3 });

    // Reshape V similarly
    const v4 = Shape.init(k_shape.dtype, &.{ @as(i64, batch), @as(i64, kv_seq), @as(i64, kv_num_heads), @as(i64, head_dim) });
    const v_r = try builder.reshape(value, v4);
    const v_t = try builder.transpose(v_r, &.{ 0, 2, 1, 3 });

    // If num_heads != kv_num_heads, repeat KV heads
    var k_expanded = k_t;
    var v_expanded = v_t;
    if (kv_num_heads < num_heads) {
        const repeat_factor = num_heads / kv_num_heads;
        // [B, KVH, S, D] → broadcast to [B, H, S, D] via repeat
        // Reshape: [B, KVH, 1, S, D] → broadcast → [B, KVH, R, S, D] → reshape → [B, H, S, D]
        const expanded = Shape.init(k_shape.dtype, &.{
            @as(i64, batch),
            @as(i64, kv_num_heads),
            1,
            @as(i64, kv_seq),
            @as(i64, head_dim),
        });
        const k_5d = try builder.reshape(k_t, expanded);
        const v_5d = try builder.reshape(v_t, expanded);

        var target_dims: [8]i64 = .{0} ** 8;
        target_dims[0] = @intCast(batch);
        target_dims[1] = @intCast(kv_num_heads);
        target_dims[2] = @intCast(repeat_factor);
        target_dims[3] = @intCast(kv_seq);
        target_dims[4] = @intCast(head_dim);
        const target_shape = Shape{ .dtype = k_shape.dtype, .dims = target_dims, .rank_ = 5 };

        var bcast_axes: [8]u8 = .{0} ** 8;
        for (0..5) |i| bcast_axes[i] = @intCast(i);

        k_expanded = try builder.graph.addNode(.{
            .op = .{ .broadcast_in_dim = .{ .target_shape = target_shape, .broadcast_axes = bcast_axes, .num_axes = 5 } },
            .output_shape = target_shape,
            .inputs = .{ k_5d, null_node, null_node, null_node },
            .num_inputs = 1,
        });
        v_expanded = try builder.graph.addNode(.{
            .op = .{ .broadcast_in_dim = .{ .target_shape = target_shape, .broadcast_axes = bcast_axes, .num_axes = 5 } },
            .output_shape = target_shape,
            .inputs = .{ v_5d, null_node, null_node, null_node },
            .num_inputs = 1,
        });

        const h_shape = Shape.init(k_shape.dtype, &.{ @as(i64, batch), @as(i64, num_heads), @as(i64, kv_seq), @as(i64, head_dim) });
        k_expanded = try builder.reshape(k_expanded, h_shape);
        v_expanded = try builder.reshape(v_expanded, h_shape);
    }

    // Flatten to [B*H, S, D]
    const k_flat = try builder.reshape(k_expanded, Shape.init(k_shape.dtype, &.{ bh, @as(i64, kv_seq), @as(i64, head_dim) }));
    const v_flat = try builder.reshape(v_expanded, Shape.init(k_shape.dtype, &.{ bh, @as(i64, kv_seq), @as(i64, head_dim) }));

    // SDPA (or causal) — GQA doesn't use a separate mask input
    const attn = try emitAttention(builder, q_flat, k_flat, v_flat, batch, q_seq, num_heads, head_dim, is_causal, null_node);

    // Reshape back: [B*H, S, D] → [B, H, S, D] → [B, S, H, D] → [B, S, H*D]
    const r4_out = Shape.init(q_shape.dtype, &.{ @as(i64, batch), @as(i64, num_heads), @as(i64, q_seq), @as(i64, head_dim) });
    const out4 = try builder.reshape(attn, r4_out);
    const out_t = try builder.transpose(out4, &.{ 0, 2, 1, 3 });
    const out_shape = Shape.init(q_shape.dtype, &.{ @as(i64, batch), @as(i64, q_seq), @as(i64, num_heads * head_dim) });
    return builder.reshape(out_t, out_shape);
}

// ── Phase 3: Trilu ──────────────────────────────────────────────────

fn convertTrilu(builder: *Builder, node: *const NodeProto, inputs: []const NodeId) ConvertError!NodeId {
    const upper = getInt(node.attributes, "upper", 1) != 0;
    const in_shape = builder.graph.node(inputs[0]).output_shape;
    const dtype = in_shape.dtype;

    // Get offset k (default 0); unused in stub but parsed for correctness.
    if (inputs.len >= 2 and inputs[1] != null_node) {
        _ = materializeConstantScalar(builder, inputs[1]);
    }

    // Trilu needs iota-like primitives to create row/col index tensors.
    // Since termite doesn't have iota, return identity for now.
    // upper: mask[i,j] = 1 if j >= i + k, else 0
    // lower: mask[i,j] = 1 if j <= i + k, else 0
    _ = upper;
    _ = dtype;
    return inputs[0];
}

// ── Phase 3: GatherElements ─────────────────────────────────────────

fn convertGatherElements(builder: *Builder, node: *const NodeProto, inputs: []const NodeId) ConvertError!NodeId {
    if (inputs.len < 2) return error.MissingInput;
    const axis_raw = getInt(node.attributes, "axis", 0);
    const data_shape = builder.graph.node(inputs[0]).output_shape;
    const indices_shape = builder.graph.node(inputs[1]).output_shape;
    const axis: u8 = if (axis_raw < 0) @intCast(@as(i64, data_shape.rank()) + axis_raw) else @intCast(axis_raw);

    // GatherElements output shape = indices shape
    return builder.graph.addNode(.{
        .op = .{ .gather = .{ .axis = axis } },
        .output_shape = Shape{ .dtype = data_shape.dtype, .dims = indices_shape.dims, .rank_ = indices_shape.rank_ },
        .inputs = .{ inputs[0], inputs[1], null_node, null_node },
        .num_inputs = 2,
    });
}

// ── Phase 3: CumSum ─────────────────────────────────────────────────

fn convertCumSum(builder: *Builder, node: *const NodeProto, inputs: []const NodeId) ConvertError!NodeId {
    _ = node;
    _ = builder;

    // CumSum can't be efficiently decomposed without scan primitive.
    // Return input as-is for now — models that need cumsum will need
    // a scan primitive added to inference.
    return inputs[0];
}

// ── Phase 3: DequantizeLinear ───────────────────────────────────────

fn convertDequantizeLinear(builder: *Builder, node: *const NodeProto, inputs: []const NodeId) ConvertError!NodeId {
    if (inputs.len < 2) return error.MissingInput;
    // DequantizeLinear: y = (x - zero_point) * scale
    // Per-tensor: scale is scalar, zero_point is scalar
    // Per-axis:   scale/zero_point have shape matching x along `axis` dim
    // Block:      scale/zero_point have shape [..., ceil(dim/block_size), ...] (opset 21)
    const x = inputs[0];
    var scale = inputs[1];
    const axis_attr = getInt(node.attributes, "axis", 1);
    const block_size = getInt(node.attributes, "block_size", 0);

    const x_shape = builder.graph.node(x).output_shape;

    // Block quantization (opset 21): reshape x into blocks, apply per-block scale
    if (block_size > 0) {
        return dequantizeBlock(builder, x, scale, if (inputs.len >= 3 and inputs[2] != null_node) inputs[2] else null, axis_attr, block_size);
    }

    var dequant = x;
    // Subtract zero point if provided
    if (inputs.len >= 3 and inputs[2] != null_node) {
        var zp = inputs[2];
        // For per-axis, reshape zero_point to broadcast along the quantization axis
        zp = try broadcastPerAxis(builder, zp, x_shape, axis_attr);
        dequant = try builder.sub(dequant, zp);
    }
    // Cast to float if needed (quantized input is typically int8/uint8)
    const scale_shape = builder.graph.node(scale).output_shape;
    const dq_shape = builder.graph.node(dequant).output_shape;
    if (dq_shape.dtype != scale_shape.dtype) {
        const out_shape = Shape{ .dtype = scale_shape.dtype, .dims = dq_shape.dims, .rank_ = dq_shape.rank_ };
        dequant = try builder.graph.addNode(.{
            .op = .{ .convert_dtype = .{ .target = scale_shape.dtype } },
            .output_shape = out_shape,
            .inputs = .{ dequant, null_node, null_node, null_node },
            .num_inputs = 1,
        });
    }
    // For per-axis, reshape scale to broadcast along the quantization axis
    scale = try broadcastPerAxis(builder, scale, builder.graph.node(dequant).output_shape, axis_attr);
    // Multiply by scale
    return builder.mul(dequant, scale);
}

/// Block dequantization: reshape input so the quantization axis is split into
/// [num_blocks, block_size], apply per-block scale/zero_point via broadcast,
/// then reshape back to original shape.
fn dequantizeBlock(builder: *Builder, x: NodeId, scale: NodeId, zp: ?NodeId, axis_raw: i64, block_size: i64) ConvertError!NodeId {
    const x_shape = builder.graph.node(x).output_shape;
    const rank = x_shape.rank();
    const axis: u8 = if (axis_raw < 0) @intCast(@as(i64, rank) + axis_raw) else @intCast(axis_raw);
    const dim = x_shape.dim(axis);
    const num_blocks = @divTrunc(dim + block_size - 1, block_size); // ceil division
    const actual_block = @divExact(dim, num_blocks); // must divide evenly for reshape

    // Reshape x: insert block dimension → [..., num_blocks, block_size, ...]
    var block_dims: [8]i64 = .{0} ** 8;
    var bi: u8 = 0;
    for (0..axis) |i| {
        block_dims[bi] = x_shape.dim(@intCast(i));
        bi += 1;
    }
    block_dims[bi] = num_blocks;
    bi += 1;
    block_dims[bi] = actual_block;
    bi += 1;
    for ((axis + 1)..rank) |i| {
        block_dims[bi] = x_shape.dim(@intCast(i));
        bi += 1;
    }
    const block_shape = Shape{ .dtype = x_shape.dtype, .dims = block_dims, .rank_ = bi };
    var blocked = try builder.reshape(x, block_shape);

    // Subtract zero point if provided
    if (zp) |zp_id| {
        var zp_bc = try broadcastPerAxis(builder, zp_id, block_shape, @intCast(axis));
        const zp_shape = builder.graph.node(zp_bc).output_shape;
        if (zp_shape.dtype != x_shape.dtype) {
            zp_bc = try castDType(builder, zp_bc, x_shape.dtype);
        }
        blocked = try builder.sub(blocked, zp_bc);
    }

    // Cast to float
    const scale_dtype = builder.graph.node(scale).output_shape.dtype;
    const blk_dtype = builder.graph.node(blocked).output_shape.dtype;
    if (blk_dtype != scale_dtype) {
        const cast_shape = Shape{ .dtype = scale_dtype, .dims = builder.graph.node(blocked).output_shape.dims, .rank_ = bi };
        blocked = try builder.graph.addNode(.{
            .op = .{ .convert_dtype = .{ .target = scale_dtype } },
            .output_shape = cast_shape,
            .inputs = .{ blocked, null_node, null_node, null_node },
            .num_inputs = 1,
        });
    }

    // Broadcast scale along the block axis (scale has shape [..., num_blocks, ...])
    const scale_bc = try broadcastPerAxis(builder, scale, builder.graph.node(blocked).output_shape, axis);
    const result = try builder.mul(blocked, scale_bc);

    // Reshape back to original shape
    const out_shape = Shape{ .dtype = scale_dtype, .dims = x_shape.dims, .rank_ = rank };
    return builder.reshape(result, out_shape);
}

fn convertQuantizeLinear(builder: *Builder, node: *const NodeProto, inputs: []const NodeId) ConvertError!NodeId {
    if (inputs.len < 2) return error.MissingInput;
    // QuantizeLinear: y = clamp(round(x / scale) + zero_point, qmin, qmax)
    const x = inputs[0];
    var scale = inputs[1];
    const axis_attr = getInt(node.attributes, "axis", 1);
    const x_shape = builder.graph.node(x).output_shape;

    // Determine output dtype from zero_point if present, else default to uint8
    const out_dtype: DType = if (inputs.len >= 3 and inputs[2] != null_node)
        builder.graph.node(inputs[2]).output_shape.dtype
    else
        .u8;

    // Broadcast scale for per-axis quantization
    scale = try broadcastPerAxis(builder, scale, x_shape, axis_attr);

    // y = x / scale
    var result = try builder.div(x, scale);

    // Round to nearest (half to even is ONNX default, we approximate with floor(x + 0.5))
    const half = try builder.scalarConst(x_shape.dtype, 0.5);
    result = try builder.add(result, half);
    result = try convertFloor(builder, result);

    // Add zero_point if provided
    if (inputs.len >= 3 and inputs[2] != null_node) {
        var zp = inputs[2];
        // Cast zero_point to float for arithmetic
        const zp_shape = builder.graph.node(zp).output_shape;
        if (zp_shape.dtype != x_shape.dtype) {
            zp = try castDType(builder, zp, x_shape.dtype);
        }
        zp = try broadcastPerAxis(builder, zp, x_shape, axis_attr);
        result = try builder.add(result, zp);
    }

    // Clamp to output type range
    const qmin: f32 = switch (out_dtype) {
        .u8 => 0.0,
        .i32 => -2147483648.0,
        else => 0.0,
    };
    const qmax: f32 = switch (out_dtype) {
        .u8 => 255.0,
        .i32 => 2147483647.0,
        else => 255.0,
    };
    const min_node = try builder.scalarConst(x_shape.dtype, qmin);
    const max_node = try builder.scalarConst(x_shape.dtype, qmax);
    // clamp: max(min, min(x, max))
    const clamped_hi = try selectOp(builder, try cmpLessThan(builder, result, max_node), result, max_node);
    const clamped = try selectOp(builder, try cmpLessThan(builder, min_node, clamped_hi), clamped_hi, min_node);

    // Cast to output dtype
    return castDType(builder, clamped, out_dtype);
}

/// Reshape a 1D per-axis tensor to broadcast along a specific axis of the target shape.
/// E.g., scale [C] with axis=1 and target [N,C,H,W] → reshape to [1,C,1,1].
fn broadcastPerAxis(builder: *Builder, input: NodeId, target_shape: Shape, axis_raw: i64) ConvertError!NodeId {
    const in_shape = builder.graph.node(input).output_shape;
    // Only reshape if input is 1D and target is higher rank
    if (in_shape.rank() != 1 or target_shape.rank() <= 1) return input;

    const axis: u8 = if (axis_raw < 0) @intCast(@as(i64, target_shape.rank()) + axis_raw) else @intCast(axis_raw);

    // Build shape: all 1s except at `axis`
    var new_dims: [8]i64 = .{1} ** 8;
    new_dims[axis] = in_shape.dim(0);
    const new_shape = Shape{
        .dtype = in_shape.dtype,
        .dims = new_dims,
        .rank_ = target_shape.rank(),
    };
    return builder.reshape(input, new_shape);
}

// ── Phase 3: Pooling Ops ────────────────────────────────────────────

fn convertAveragePool(builder: *Builder, node: *const NodeProto, inputs: []const NodeId) ConvertError!NodeId {
    // AveragePool: reduce_mean over spatial window
    // For global average pool variant or simple cases
    const in_shape = builder.graph.node(inputs[0]).output_shape;
    const kernel = getInts(node.attributes, "kernel_shape");
    const strides = getInts(node.attributes, "strides");

    if (kernel.len == 0) return error.InvalidAttribute;

    // Simple case: kernel covers entire spatial dim and stride=1
    // → reduce_mean over spatial axes
    const num_spatial = in_shape.rank() - 2;
    var is_global = true;
    for (0..num_spatial) |i| {
        if (i < kernel.len and kernel[i] != in_shape.dim(@intCast(i + 2))) {
            is_global = false;
            break;
        }
    }

    if (is_global) {
        return convertGlobalAveragePool(builder, inputs);
    }

    // Non-global: compute output shape and use reduce_mean windowed
    // This is an approximation — proper sliding window needs conv_general
    var out_dims: [8]i64 = .{0} ** 8;
    out_dims[0] = in_shape.dim(0); // batch
    out_dims[1] = in_shape.dim(1); // channels
    for (0..num_spatial) |i| {
        const in_d = in_shape.dim(@intCast(i + 2));
        const k: i64 = if (i < kernel.len) kernel[i] else 1;
        const s: i64 = if (i < strides.len) strides[i] else 1;
        out_dims[i + 2] = @divTrunc(in_d - k, s) + 1;
    }
    const out_shape = Shape{ .dtype = in_shape.dtype, .dims = out_dims, .rank_ = in_shape.rank_ };

    // Use conv_general with uniform weights as average pooling
    // For simplicity, use reduce_mean on spatial axes as approximation
    var spatial_axes: [8]u8 = undefined;
    for (0..num_spatial) |i| spatial_axes[i] = @intCast(i + 2);
    const reduced = try builder.reduceMean(inputs[0], spatial_axes[0..num_spatial]);

    // If output shape differs from reduced, reshape
    const red_shape = builder.graph.node(reduced).output_shape;
    if (red_shape.rank_ != out_shape.rank_) {
        return builder.reshape(reduced, out_shape);
    }
    return reduced;
}

fn convertMaxPool(builder: *Builder, node: *const NodeProto, inputs: []const NodeId) ConvertError!NodeId {
    // MaxPool: reduce_max over spatial window
    const in_shape = builder.graph.node(inputs[0]).output_shape;
    const kernel = getInts(node.attributes, "kernel_shape");
    const strides = getInts(node.attributes, "strides");

    if (kernel.len == 0) return error.InvalidAttribute;

    const num_spatial = in_shape.rank() - 2;

    // Compute output shape
    var out_dims: [8]i64 = .{0} ** 8;
    out_dims[0] = in_shape.dim(0);
    out_dims[1] = in_shape.dim(1);
    for (0..num_spatial) |i| {
        const in_d = in_shape.dim(@intCast(i + 2));
        const k: i64 = if (i < kernel.len) kernel[i] else 1;
        const s: i64 = if (i < strides.len) strides[i] else 1;
        out_dims[i + 2] = @divTrunc(in_d - k, s) + 1;
    }
    const out_shape = Shape{ .dtype = in_shape.dtype, .dims = out_dims, .rank_ = in_shape.rank_ };

    // Approximate: reduce_max over spatial axes
    var spatial_axes: [8]u8 = undefined;
    for (0..num_spatial) |i| spatial_axes[i] = @intCast(i + 2);
    const reduced = try builder.reduceMax(inputs[0], spatial_axes[0..num_spatial]);

    const red_shape = builder.graph.node(reduced).output_shape;
    if (red_shape.rank_ != out_shape.rank_) {
        return builder.reshape(reduced, out_shape);
    }
    return reduced;
}

fn convertGlobalAveragePool(builder: *Builder, inputs: []const NodeId) ConvertError!NodeId {
    // GlobalAveragePool: reduce_mean over all spatial axes (keep batch + channel)
    const in_shape = builder.graph.node(inputs[0]).output_shape;
    const num_spatial = in_shape.rank() - 2;
    var spatial_axes: [8]u8 = undefined;
    for (0..num_spatial) |i| spatial_axes[i] = @intCast(i + 2);
    return builder.reduceMean(inputs[0], spatial_axes[0..num_spatial]);
}

// ── Phase 4: RotaryEmbedding ───────────────────────────────────────

fn convertRotaryEmbedding(builder: *Builder, node: *const NodeProto, inputs: []const NodeId) ConvertError!NodeId {
    if (inputs.len < 4) return error.MissingInput;
    const x = inputs[0];
    const cos = inputs[2];
    const sin = inputs[3];
    _ = node;

    // NOTE on fused_rope: termite's fused_rope recomputes sin/cos from (theta, freq_scale)
    // at runtime, whereas ONNX RotaryEmbedding provides *precomputed* cos/sin caches as
    // inputs. The mapping isn't faithful unless we can prove the caches came from a known
    // theta, which the converter can't verify at conversion time. We keep the primitive
    // decomposition here (slice → mul/add/sub → concat) so results stay correct regardless
    // of how the caches were constructed; optimization passes can still match and fuse
    // this subgraph when theta is known.
    const x_shape = builder.graph.node(x).output_shape;
    const rank = x_shape.rank();
    const d = x_shape.dim(rank - 1);
    const half_d = @divTrunc(d, @as(i64, 2));

    // Split input into two halves along last dim
    var slice1 = ml.graph.node.SliceAttrs{};
    var slice2 = ml.graph.node.SliceAttrs{};
    slice1.num_axes = rank;
    slice2.num_axes = rank;
    for (0..rank) |i| {
        slice1.starts[i] = 0;
        slice1.limits[i] = x_shape.dim(@intCast(i));
        slice1.strides[i] = 1;
        slice2.starts[i] = 0;
        slice2.limits[i] = x_shape.dim(@intCast(i));
        slice2.strides[i] = 1;
    }
    slice1.limits[rank - 1] = half_d;
    slice2.starts[rank - 1] = half_d;

    var half_dims = x_shape.dims;
    half_dims[rank - 1] = half_d;
    const half_shape = Shape{ .dtype = x_shape.dtype, .dims = half_dims, .rank_ = rank };

    const x1 = try builder.graph.addNode(.{
        .op = .{ .slice = slice1 },
        .output_shape = half_shape,
        .inputs = .{ x, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const x2 = try builder.graph.addNode(.{
        .op = .{ .slice = slice2 },
        .output_shape = half_shape,
        .inputs = .{ x, null_node, null_node, null_node },
        .num_inputs = 1,
    });

    // RoPE: [cos*x1 - sin*x2, sin*x1 + cos*x2]
    const cos_x1 = try broadcastBinaryOp(builder, .mul, cos, x1);
    const sin_x2 = try broadcastBinaryOp(builder, .mul, sin, x2);
    const real = try builder.sub(cos_x1, sin_x2);

    const sin_x1 = try broadcastBinaryOp(builder, .mul, sin, x1);
    const cos_x2 = try broadcastBinaryOp(builder, .mul, cos, x2);
    const imag = try builder.add(sin_x1, cos_x2);

    return builder.graph.addNode(.{
        .op = .{ .concat_prim = .{ .axis = rank - 1 } },
        .output_shape = x_shape,
        .inputs = .{ real, imag, null_node, null_node },
        .num_inputs = 2,
    });
}

// ── Phase 4: HardSwish ─────────────────────────────────────────────

fn convertHardSwish(builder: *Builder, input: NodeId) ConvertError!NodeId {
    // HardSwish(x) = x * clip(x + 3, 0, 6) / 6
    const dtype = builder.graph.node(input).output_shape.dtype;
    const three = try builder.scalarConst(dtype, 3.0);
    const zero = try builder.scalarConst(dtype, 0.0);
    const six = try builder.scalarConst(dtype, 6.0);

    const x_plus_3 = try broadcastBinaryOp(builder, .add, input, three);
    const cmp_lo = try broadcastBinaryOp(builder, .less_than, x_plus_3, zero);
    const clipped_lo = try selectOp(builder, cmp_lo, zero, x_plus_3);
    const cmp_hi = try broadcastBinaryOp(builder, .less_than, six, clipped_lo);
    const clipped = try selectOp(builder, cmp_hi, six, clipped_lo);
    const scaled = try broadcastBinaryOp(builder, .div, clipped, six);
    return broadcastBinaryOp(builder, .mul, input, scaled);
}

// ── Phase 4: Logical Ops ───────────────────────────────────────────

fn convertLogicalAnd(builder: *Builder, inputs: []const NodeId) ConvertError!NodeId {
    if (inputs.len < 2) return error.MissingInput;
    // ONNX And operates on boolean (0/1) tensors
    return builder.mul(inputs[0], inputs[1]);
}

fn convertLogicalOr(builder: *Builder, inputs: []const NodeId) ConvertError!NodeId {
    if (inputs.len < 2) return error.MissingInput;
    // OR on boolean (0/1): a + b - a*b
    const sum = try builder.add(inputs[0], inputs[1]);
    const product = try builder.mul(inputs[0], inputs[1]);
    return builder.sub(sum, product);
}

fn convertLogicalXor(builder: *Builder, inputs: []const NodeId) ConvertError!NodeId {
    if (inputs.len < 2) return error.MissingInput;
    // XOR on boolean (0/1): a + b - 2*a*b
    const dtype = builder.graph.node(inputs[0]).output_shape.dtype;
    const two = try builder.scalarConst(dtype, 2.0);
    const sum = try builder.add(inputs[0], inputs[1]);
    const product = try builder.mul(inputs[0], inputs[1]);
    const double_prod = try builder.mul(two, product);
    return builder.sub(sum, double_prod);
}

// ── Phase 4: Mod, IsNaN, Size ──────────────────────────────────────

fn convertMod(builder: *Builder, node: *const NodeProto, inputs: []const NodeId) ConvertError!NodeId {
    if (inputs.len < 2) return error.MissingInput;
    const fmod_attr = getInt(node.attributes, "fmod", 0);

    // fmod=1: C-style fmod: a - trunc(a/b) * b
    // fmod=0: Python-style mod: a - floor(a/b) * b
    // We implement trunc via cast to i32 and back (works for values in i32 range).
    // For floor: trunc(x) - (trunc(x) > x ? 1 : 0), i.e. adjust negative remainders.
    const a = inputs[0];
    const b = inputs[1];
    const quotient = try builder.div(a, b);

    // trunc(quotient) = convert(convert(quotient, i32), f32)
    const in_shape = builder.graph.node(quotient).output_shape;
    var i32_shape = in_shape;
    i32_shape.dtype = .i32;
    const as_i32 = try builder.graph.addNode(.{
        .op = .{ .convert_dtype = .{ .target = .i32 } },
        .output_shape = i32_shape,
        .inputs = .{ quotient, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const truncated = try builder.graph.addNode(.{
        .op = .{ .convert_dtype = .{ .target = in_shape.dtype } },
        .output_shape = in_shape,
        .inputs = .{ as_i32, null_node, null_node, null_node },
        .num_inputs = 1,
    });

    if (fmod_attr == 1) {
        // fmod: a - trunc(a/b) * b
        const prod = try builder.mul(truncated, b);
        return builder.sub(a, prod);
    }

    // Python-style floor mod: floor(x) = trunc(x) - (trunc(x) > x ? 1 : 0)
    // trunc(x) > x is equivalent to x < trunc(x)
    const x_lt_trunc = try cmpLessThan(builder, quotient, truncated);
    const one = try builder.scalarConst(in_shape.dtype, 1.0);
    const zero = try builder.scalarConst(in_shape.dtype, 0.0);
    const adjustment = try selectOp(builder, x_lt_trunc, one, zero);
    const floored = try builder.sub(truncated, adjustment);
    const prod = try builder.mul(floored, b);
    return builder.sub(a, prod);
}

fn convertIsNaN(builder: *Builder, input: NodeId) ConvertError!NodeId {
    // Decompose IsNaN using only arithmetic + less_than + where_select.
    //
    // Key IEEE 754 properties:
    //   x * 0 = 0 for finite, NaN for NaN, NaN for Inf (Inf*0 is NaN)
    //   less_than(a, NaN) = false for any a
    //   abs(Inf) = Inf, abs(NaN) = NaN
    //
    // Step 1: isFinite = less_than(0, x*0 + 1)
    //   finite → less_than(0, 1) = true
    //   NaN    → less_than(0, NaN) = false
    //   Inf    → less_than(0, NaN) = false
    //
    // Step 2: isInf = less_than(FLT_MAX, abs(x))
    //   finite → false, NaN → false, Inf → true
    //
    // Step 3: isNaN = NOT(isFinite) AND NOT(isInf)
    //   Encode NOT as where_select(cond, 0, 1) and AND as mul.
    const in_shape = builder.graph.node(input).output_shape;
    const zero = try builder.scalarConst(in_shape.dtype, 0.0);
    const one = try builder.scalarConst(in_shape.dtype, 1.0);

    // x * 0 + 1: finite→1, NaN/Inf→NaN
    const t = try builder.add(try builder.mul(input, zero), one);

    // isFinite: true for finite, false for NaN and Inf
    const is_finite = try builder.graph.addNode(.{
        .op = .{ .less_than = {} },
        .output_shape = in_shape,
        .inputs = .{ zero, t, null_node, null_node },
        .num_inputs = 2,
    });

    // isInf: true only for ±Inf (less_than(maxfloat, NaN) = false)
    const flt_max = try builder.scalarConst(in_shape.dtype, std.math.floatMax(f32));
    const abs_x = try builder.absOp(input);
    const is_inf = try builder.graph.addNode(.{
        .op = .{ .less_than = {} },
        .output_shape = in_shape,
        .inputs = .{ flt_max, abs_x, null_node, null_node },
        .num_inputs = 2,
    });

    // NOT(isFinite): where(isFinite, 0, 1)
    const not_finite = try builder.graph.addNode(.{
        .op = .{ .where_select = {} },
        .output_shape = in_shape,
        .inputs = .{ is_finite, zero, one, null_node },
        .num_inputs = 3,
    });

    // NOT(isInf): where(isInf, 0, 1)
    const not_inf = try builder.graph.addNode(.{
        .op = .{ .where_select = {} },
        .output_shape = in_shape,
        .inputs = .{ is_inf, zero, one, null_node },
        .num_inputs = 3,
    });

    // isNaN = NOT(isFinite) AND NOT(isInf) = mul(not_finite, not_inf)
    return builder.mul(not_finite, not_inf);
}

fn convertSize(builder: *Builder, input: NodeId) ConvertError!NodeId {
    const in_shape = builder.graph.node(input).output_shape;
    const count = in_shape.numElements() orelse std.math.maxInt(i32);
    const out_shape = Shape.init(.f32, &.{1});
    return builder.tensorConst(&.{@as(f32, @floatFromInt(count))}, out_shape);
}

// ── Phase 4: ArgMax, ArgMin ────────────────────────────────────────

fn convertArgMax(builder: *Builder, node: *const NodeProto, input: NodeId) ConvertError!NodeId {
    const axis_raw = getInt(node.attributes, "axis", 0);
    const keepdims = getInt(node.attributes, "keepdims", 1) != 0;
    const in_shape = builder.graph.node(input).output_shape;
    const axis: u8 = if (axis_raw < 0) @intCast(@as(i64, in_shape.rank()) + axis_raw) else @intCast(axis_raw);
    return builder.argMax(input, axis, keepdims);
}

fn convertArgMin(builder: *Builder, node: *const NodeProto, input: NodeId) ConvertError!NodeId {
    const axis_raw = getInt(node.attributes, "axis", 0);
    const keepdims = getInt(node.attributes, "keepdims", 1) != 0;
    const in_shape = builder.graph.node(input).output_shape;
    const axis: u8 = if (axis_raw < 0) @intCast(@as(i64, in_shape.rank()) + axis_raw) else @intCast(axis_raw);
    const neg = try builder.neg(input);
    return builder.argMax(neg, axis, keepdims);
}

// ── Phase 4: GatherND ──────────────────────────────────────────────

fn convertGatherND(builder: *Builder, node: *const NodeProto, inputs: []const NodeId) ConvertError!NodeId {
    if (inputs.len < 2) return error.MissingInput;
    _ = node;
    const data_shape = builder.graph.node(inputs[0]).output_shape;
    return builder.graph.addNode(.{
        .op = .{ .gather = .{ .axis = 0 } },
        .output_shape = data_shape,
        .inputs = .{ inputs[0], inputs[1], null_node, null_node },
        .num_inputs = 2,
    });
}

// ── Phase 4: Einsum ────────────────────────────────────────────────

fn convertEinsum(builder: *Builder, node: *const NodeProto, inputs: []const NodeId) ConvertError!NodeId {
    if (inputs.len < 1) return error.MissingInput;

    const equation = attrs_mod.getString(node.attributes, "equation", "");
    if (equation.len == 0) return error.InvalidAttribute;

    // Single operand: pass through
    if (inputs.len == 1) return inputs[0];

    // Parse "abc,bcd->acd"
    var comma_pos: ?usize = null;
    var arrow_pos: ?usize = null;
    for (equation, 0..) |c, i| {
        if (c == ',' and comma_pos == null) comma_pos = i;
        if (c == '-' and i + 1 < equation.len and equation[i + 1] == '>') arrow_pos = i;
    }
    if (comma_pos == null or arrow_pos == null) return error.InvalidAttribute;

    const lhs_idx = equation[0..comma_pos.?];
    const rhs_idx = equation[comma_pos.? + 1 .. arrow_pos.?];
    const out_idx = equation[arrow_pos.? + 2 ..];

    var attrs = ml.graph.node.DotGeneralAttrs{};

    for (lhs_idx, 0..) |lc, li| {
        var rhs_pos: ?usize = null;
        for (rhs_idx, 0..) |rc, ri| {
            if (lc == rc) {
                rhs_pos = ri;
                break;
            }
        }
        if (rhs_pos) |ri| {
            var in_output = false;
            for (out_idx) |oc| {
                if (lc == oc) {
                    in_output = true;
                    break;
                }
            }
            if (in_output) {
                attrs.lhs_batch[attrs.num_batch] = @intCast(li);
                attrs.rhs_batch[attrs.num_batch] = @intCast(ri);
                attrs.num_batch += 1;
            } else {
                attrs.lhs_contracting[attrs.num_contracting] = @intCast(li);
                attrs.rhs_contracting[attrs.num_contracting] = @intCast(ri);
                attrs.num_contracting += 1;
            }
        }
    }

    const a_shape = builder.graph.node(inputs[0]).output_shape;
    const b_shape = builder.graph.node(inputs[1]).output_shape;
    var out_dims: [8]i64 = .{0} ** 8;
    var out_rank: u8 = 0;

    for (out_idx) |oc| {
        var found = false;
        for (lhs_idx, 0..) |lc, li| {
            if (lc == oc) {
                out_dims[out_rank] = a_shape.dim(@intCast(li));
                out_rank += 1;
                found = true;
                break;
            }
        }
        if (!found) {
            for (rhs_idx, 0..) |rc, ri| {
                if (rc == oc) {
                    out_dims[out_rank] = b_shape.dim(@intCast(ri));
                    out_rank += 1;
                    break;
                }
            }
        }
    }

    const out_shape = Shape{ .dtype = a_shape.dtype, .dims = out_dims, .rank_ = out_rank };

    return builder.graph.addNode(.{
        .op = .{ .dot_general = attrs },
        .output_shape = out_shape,
        .inputs = .{ inputs[0], inputs[1], null_node, null_node },
        .num_inputs = 2,
    });
}

// ── Phase 4: ConvTranspose, Resize ─────────────────────────────────

fn convertConvTranspose(builder: *Builder, node: *const NodeProto, inputs: []const NodeId) ConvertError!NodeId {
    if (inputs.len < 2) return error.MissingInput;
    const x = inputs[0];
    const w = inputs[1];
    const has_bias = inputs.len >= 3 and inputs[2] != null_node;

    const x_shape = builder.graph.node(x).output_shape;
    const w_shape = builder.graph.node(w).output_shape;
    if (x_shape.rank() < 3) return error.UnsupportedOp;
    const num_spatial: u8 = x_shape.rank() - 2;

    const strides_attr = getInts(node.attributes, "strides");
    const pads_attr = getInts(node.attributes, "pads");
    const dilations_attr = getInts(node.attributes, "dilations");
    const output_padding_attr = getInts(node.attributes, "output_padding");
    const group: u32 = @intCast(getInt(node.attributes, "group", 1));

    // ConvTranspose: decompose as conv_general with adjusted padding.
    // For stride=1, ConvTranspose is conv with kernel spatially flipped and
    // padding = kernel_size - 1 - original_padding (full convolution).
    // For stride>1, we insert zeros between input elements (fractional striding),
    // which we approximate by using conv_general with stride=1 and expanded padding.

    var conv_attrs = ml.graph.node.ConvAttrs{};
    conv_attrs.num_spatial = num_spatial;
    conv_attrs.groups = group;

    // ConvTranspose always runs the inner conv with stride=1.
    // The "stride" in ConvTranspose means input dilation (fractional striding).
    for (0..num_spatial) |i| {
        conv_attrs.strides[i] = 1;
    }

    // Compute output spatial dims and effective padding.
    // out = (in - 1) * stride - 2*pad + dilation*(kernel-1) + output_padding + 1
    var out_dims: [8]i64 = .{0} ** 8;
    out_dims[0] = x_shape.dim(0); // batch
    // For ConvTranspose, weight layout is [C_in, C_out/groups, kH, kW]
    out_dims[1] = w_shape.dim(1) * @as(i64, group); // output channels

    for (0..num_spatial) |i| {
        const in_d = x_shape.dim(@intCast(i + 2));
        const k_d = w_shape.dim(@intCast(i + 2));
        const stride: i64 = if (i < strides_attr.len) strides_attr[i] else 1;
        const dilation: i64 = if (i < dilations_attr.len) dilations_attr[i] else 1;
        const pad_begin: i64 = if (i < pads_attr.len) pads_attr[i] else 0;
        const pad_end: i64 = if (i + num_spatial < pads_attr.len) pads_attr[i + num_spatial] else 0;
        const out_pad: i64 = if (i < output_padding_attr.len) output_padding_attr[i] else 0;
        const effective_k = dilation * (k_d - 1) + 1;

        out_dims[i + 2] = (in_d - 1) * stride - pad_begin - pad_end + effective_k + out_pad;

        // Effective padding for the transpose conv (full convolution padding)
        conv_attrs.padding[i][0] = @intCast(effective_k - 1 - pad_begin);
        conv_attrs.padding[i][1] = @intCast(effective_k - 1 - pad_end + out_pad);
    }

    const out_shape = Shape{ .dtype = x_shape.dtype, .dims = out_dims, .rank_ = x_shape.rank_ };

    var result = try builder.graph.addNode(.{
        .op = .{ .conv_general = conv_attrs },
        .output_shape = out_shape,
        .inputs = .{ x, w, null_node, null_node },
        .num_inputs = 2,
    });

    if (has_bias) {
        const bias = inputs[2];
        const bias_shape = builder.graph.node(bias).output_shape;
        if (bias_shape.rank() == 1 and out_shape.rank() >= 3) {
            var bias_dims: [8]i64 = .{0} ** 8;
            bias_dims[0] = 1;
            bias_dims[1] = bias_shape.dim(0);
            for (2..out_shape.rank()) |i| bias_dims[i] = 1;
            const reshaped_bias_shape = Shape{ .dtype = bias_shape.dtype, .dims = bias_dims, .rank_ = out_shape.rank_ };
            const reshaped_bias = try builder.reshape(bias, reshaped_bias_shape);
            result = try builder.add(result, reshaped_bias);
        } else {
            result = try builder.add(result, bias);
        }
    }

    return result;
}

fn convertResize(builder: *Builder, node: *const NodeProto, inputs: []const NodeId) ConvertError!NodeId {
    // ONNX Resize (opset 11+):
    //   inputs: X, roi, scales, sizes
    //   scales or sizes must be provided (one can be empty)
    //
    // Two strategies:
    //   1. Integer scale factors → reshape + broadcast (fast, exact)
    //   2. Fractional scales / output sizes → per-axis gather with
    //      nearest-neighbor coordinate mapping
    //
    // Nearest-neighbor and cubic modes are supported. Cubic lowering is
    // currently the ONNX/CLAP case: align_corners, no antialiasing.
    const in_shape = builder.graph.node(inputs[0]).output_shape;
    const rank = in_shape.rank();
    if (rank == 0 or rank > 8) return inputs[0];

    const mode = getString(node.attributes, "mode", "nearest");
    const is_nearest = std.mem.eql(u8, mode, "nearest");
    const is_cubic = std.mem.eql(u8, mode, "cubic");
    if (!is_nearest and !is_cubic) return error.UnsupportedOp;

    // Compute output dimensions from scales or sizes inputs.
    var out_dims: [8]i64 = undefined;
    var has_out_dims = false;
    var all_integer_scales = true;
    var scale_factors: [8]i64 = .{1} ** 8;

    // Try scales input (index 2)
    var scales_buf: [8]f32 = undefined;
    if (inputs.len >= 3 and inputs[2] != null_node) {
        if (materializeConstantValues(builder, inputs[2], &scales_buf)) |scales_data| {
            if (scales_data.len >= rank) {
                has_out_dims = true;
                for (0..rank) |i| {
                    const s = scales_data[i];
                    const in_d = in_shape.dim(@intCast(i));
                    out_dims[i] = @intFromFloat(@as(f32, @floatFromInt(in_d)) * s);
                    if (out_dims[i] < 1) out_dims[i] = 1;
                    // Check if integer
                    const si: i64 = @intFromFloat(s);
                    if (s != @as(f32, @floatFromInt(si)) or si < 1) {
                        all_integer_scales = false;
                    } else {
                        scale_factors[i] = si;
                    }
                }
            }
        }
    }

    // Try sizes input (index 3) — overrides scales
    var sizes_buf2: [8]f32 = undefined;
    if (!has_out_dims and inputs.len >= 4 and inputs[3] != null_node) {
        if (materializeConstantValues(builder, inputs[3], &sizes_buf2)) |sizes_data| {
            if (sizes_data.len >= rank) {
                has_out_dims = true;
                all_integer_scales = true;
                for (0..rank) |i| {
                    out_dims[i] = @intFromFloat(sizes_data[i]);
                    if (out_dims[i] == 0) out_dims[i] = 1;
                    const in_d = in_shape.dim(@intCast(i));
                    if (in_d > 0 and out_dims[i] > 0 and @mod(out_dims[i], in_d) == 0) {
                        scale_factors[i] = @divExact(out_dims[i], in_d);
                    } else {
                        all_integer_scales = false;
                    }
                }
            }
        }
    }

    if (!has_out_dims) return inputs[0];

    // Check if all dims unchanged (no-op)
    var all_same = true;
    for (0..rank) |i| {
        if (out_dims[i] != in_shape.dim(@intCast(i))) {
            all_same = false;
            break;
        }
    }
    if (all_same) return inputs[0];

    if (is_cubic) {
        const coordinate_mode = getString(node.attributes, "coordinate_transformation_mode", "half_pixel");
        const antialias = getInt(node.attributes, "antialias", 0);
        const exclude_outside = getInt(node.attributes, "exclude_outside", 0);
        if (!std.mem.eql(u8, coordinate_mode, "align_corners") or antialias != 0 or exclude_outside != 0) {
            return error.UnsupportedOp;
        }
        const cubic_coeff_a = getFloat(node.attributes, "cubic_coeff_a", -0.75);
        return resizeCubicGather(builder, inputs[0], rank, &out_dims, @floatCast(cubic_coeff_a));
    }

    // Fast path: integer scale factors → reshape + broadcast + reshape.
    if (all_integer_scales) {
        return resizeIntegerBroadcast(builder, inputs[0], in_shape, rank, &scale_factors);
    }

    // General path: per-axis gather with nearest-neighbor index mapping.
    // For each axis where out_dim != in_dim, gather along that axis
    // with indices = floor(arange(out_dim) * in_dim / out_dim).
    return resizeNearestGather(builder, inputs[0], in_shape, rank, &out_dims);
}

fn addElementwiseWithShape(builder: *Builder, comptime op: std.meta.Tag(OpCode), a: NodeId, b: NodeId, out_shape: Shape) ConvertError!NodeId {
    return builder.graph.addNode(.{
        .op = @unionInit(OpCode, @tagName(op), {}),
        .output_shape = out_shape,
        .inputs = .{ a, b, null_node, null_node },
        .num_inputs = 2,
    });
}

fn broadcastRank1ToAxis(builder: *Builder, input: NodeId, axis: usize, target_shape: Shape) ConvertError!NodeId {
    var attrs = ml.graph.node.BroadcastAttrs{ .target_shape = target_shape };
    attrs.broadcast_axes[0] = @intCast(axis);
    attrs.num_axes = 1;
    return builder.graph.addNode(.{
        .op = .{ .broadcast_in_dim = attrs },
        .output_shape = target_shape,
        .inputs = .{ input, null_node, null_node, null_node },
        .num_inputs = 1,
    });
}

/// Resize via reshape + broadcast for integer scale factors (fast path).
fn resizeIntegerBroadcast(builder: *Builder, input: NodeId, in_shape: Shape, rank: u8, scale_factors: *const [8]i64) ConvertError!NodeId {
    // [d0, d1, ..., dn] → [d0, 1, d1, 1, ...] → [d0, s0, d1, s1, ...] → [d0*s0, ...]
    var interleaved_dims: [8]i64 = .{0} ** 8;
    const interleaved_rank: u8 = rank * 2;
    for (0..rank) |i| {
        interleaved_dims[i * 2] = in_shape.dim(@intCast(i));
        interleaved_dims[i * 2 + 1] = 1;
    }
    const interleaved_shape = Shape{
        .dtype = in_shape.dtype,
        .dims = interleaved_dims,
        .rank_ = interleaved_rank,
    };
    const reshaped = try builder.reshape(input, interleaved_shape);

    var broadcast_dims: [8]i64 = .{0} ** 8;
    for (0..rank) |i| {
        broadcast_dims[i * 2] = in_shape.dim(@intCast(i));
        broadcast_dims[i * 2 + 1] = scale_factors[i];
    }
    var broadcast_axes: [8]u8 = .{0} ** 8;
    for (0..interleaved_rank) |i| broadcast_axes[i] = @intCast(i);
    const broadcast_shape = Shape{
        .dtype = in_shape.dtype,
        .dims = broadcast_dims,
        .rank_ = interleaved_rank,
    };
    const broadcasted = try builder.graph.addNode(.{
        .op = .{ .broadcast_in_dim = .{
            .target_shape = broadcast_shape,
            .broadcast_axes = broadcast_axes,
            .num_axes = interleaved_rank,
        } },
        .output_shape = broadcast_shape,
        .inputs = .{ reshaped, null_node, null_node, null_node },
        .num_inputs = 1,
    });

    var out_dims: [8]i64 = .{0} ** 8;
    for (0..rank) |i| {
        out_dims[i] = in_shape.dim(@intCast(i)) * scale_factors[i];
    }
    const out_shape = Shape{
        .dtype = in_shape.dtype,
        .dims = out_dims,
        .rank_ = rank,
    };
    return builder.reshape(broadcasted, out_shape);
}

/// Resize via per-axis gather with nearest-neighbor coordinate mapping.
/// For each axis where the dimension changes, build index tensor
/// indices[j] = floor(j * in_dim / out_dim) and gather along that axis.
fn resizeNearestGather(builder: *Builder, input: NodeId, in_shape: Shape, rank: u8, out_dims: *const [8]i64) ConvertError!NodeId {
    var current = input;

    for (0..rank) |axis| {
        const in_d = in_shape.dim(@intCast(axis));
        const out_d = out_dims[axis];
        if (in_d == out_d) continue;
        if (in_d <= 0 or out_d <= 0) continue;

        // Build index tensor: indices[j] = floor(j * in_d / out_d)
        // Compute as integer: j * in_d / out_d (integer division truncates = floor for positive)
        const n: usize = @intCast(out_d);
        var idx_data: [4096]f32 = undefined;
        if (n > 4096) return error.TooManyDimensions;
        for (0..n) |j| {
            const coord: i64 = @divTrunc(@as(i64, @intCast(j)) * in_d, out_d);
            idx_data[j] = @floatFromInt(coord);
        }

        // Build i32 index tensor via raw constant data.
        const idx_shape = Shape.init(.i32, &.{out_d});
        var i32_data: [4096]i32 = undefined;
        for (0..n) |j| {
            i32_data[j] = @intCast(@divTrunc(@as(i64, @intCast(j)) * in_d, out_d));
        }
        const indices = try builder.tensorConstBytes(std.mem.sliceAsBytes(i32_data[0..n]), idx_shape);

        // Gather along this axis — compute output shape
        const cur_shape = builder.graph.node(current).output_shape;
        var gathered_dims: [8]i64 = cur_shape.dims;
        gathered_dims[axis] = out_d;
        const gathered_shape = Shape{
            .dtype = cur_shape.dtype,
            .dims = gathered_dims,
            .rank_ = cur_shape.rank(),
        };
        current = try builder.graph.addNode(.{
            .op = .{ .gather = .{ .axis = @intCast(axis) } },
            .output_shape = gathered_shape,
            .inputs = .{ current, indices, null_node, null_node },
            .num_inputs = 2,
        });
    }

    return current;
}

/// Cubic Resize via separable weighted gathers. For each resized axis, gather
/// the four cubic-neighborhood samples and combine them with precomputed
/// align_corners weights.
fn resizeCubicGather(builder: *Builder, input: NodeId, rank: u8, out_dims: *const [8]i64, cubic_coeff_a: f32) ConvertError!NodeId {
    var current = input;

    for (0..rank) |axis| {
        const cur_shape = builder.graph.node(current).output_shape;
        const in_d = cur_shape.dim(@intCast(axis));
        const out_d = out_dims[axis];
        if (in_d == out_d) continue;
        if (in_d <= 0 or out_d <= 0) return error.ShapeMismatch;

        const n: usize = @intCast(out_d);
        const allocator = builder.graph.allocator;
        const offsets = [_]i64{ -1, 0, 1, 2 };
        var weighted_terms: [4]NodeId = undefined;

        for (offsets, 0..) |offset, term_idx| {
            const indices = try allocator.alloc(i32, n);
            defer allocator.free(indices);
            const weights = try allocator.alloc(f32, n);
            defer allocator.free(weights);

            for (0..n) |j| {
                const src = resizeAlignCornersSourceCoord(j, n, @intCast(in_d));
                const center: i64 = @intFromFloat(@floor(src));
                const unclamped = center + offset;
                indices[j] = @intCast(std.math.clamp(unclamped, 0, in_d - 1));
                weights[j] = cubicResizeWeight(src - @as(f32, @floatFromInt(unclamped)), cubic_coeff_a);
            }

            const idx_shape = Shape.init(.i32, &.{out_d});
            const idx_node = try builder.tensorConstBytes(std.mem.sliceAsBytes(indices), idx_shape);

            var gathered_dims: [8]i64 = cur_shape.dims;
            gathered_dims[axis] = out_d;
            const gathered_shape = Shape{
                .dtype = cur_shape.dtype,
                .dims = gathered_dims,
                .rank_ = cur_shape.rank(),
                .bounds = cur_shape.bounds,
            };
            const gathered = try builder.graph.addNode(.{
                .op = .{ .gather = .{ .axis = @intCast(axis) } },
                .output_shape = gathered_shape,
                .inputs = .{ current, idx_node, null_node, null_node },
                .num_inputs = 2,
            });

            const weight_node = try builder.tensorConst(weights, Shape.init(.f32, &.{out_d}));
            const weight_bc = try broadcastRank1ToAxis(builder, weight_node, axis, gathered_shape);
            weighted_terms[term_idx] = try addElementwiseWithShape(builder, .mul, gathered, weight_bc, gathered_shape);
        }

        const sum01 = try addElementwiseWithShape(builder, .add, weighted_terms[0], weighted_terms[1], builder.graph.node(weighted_terms[0]).output_shape);
        const sum23 = try addElementwiseWithShape(builder, .add, weighted_terms[2], weighted_terms[3], builder.graph.node(weighted_terms[2]).output_shape);
        current = try addElementwiseWithShape(builder, .add, sum01, sum23, builder.graph.node(sum01).output_shape);
    }

    return current;
}

fn resizeAlignCornersSourceCoord(dst_index: usize, out_len: usize, in_len: usize) f32 {
    if (out_len <= 1 or in_len <= 1) return 0.0;
    return (@as(f32, @floatFromInt(dst_index)) * @as(f32, @floatFromInt(in_len - 1))) /
        @as(f32, @floatFromInt(out_len - 1));
}

fn cubicResizeWeight(x: f32, a: f32) f32 {
    const ax = @abs(x);
    if (ax <= 1.0) {
        return ((a + 2.0) * ax * ax * ax) - ((a + 3.0) * ax * ax) + 1.0;
    }
    if (ax < 2.0) {
        return (a * ax * ax * ax) - (5.0 * a * ax * ax) + (8.0 * a * ax) - (4.0 * a);
    }
    return 0.0;
}

// ── Control Flow ────────────────────────────────────────────────────

/// Unroll an ONNX Loop op when the trip count is a compile-time constant.
///
/// ONNX Loop(M, cond, v_initial...):
///   body inputs:  [i, cond_in, v_1_in, v_2_in, ...]
///   body outputs: [cond_out, v_1_out, v_2_out, ..., scan_1, scan_2, ...]
///
/// We unroll by inlining the body sub-graph N times, chaining
/// loop-carried deps from one iteration to the next. Scan outputs
/// are concatenated across iterations.
fn convertLoop(
    allocator: std.mem.Allocator,
    builder: *Builder,
    node: *const NodeProto,
    inputs: []const NodeId,
    outer_scope: ?*const NameScope,
) ConvertError!NodeId {
    if (inputs.len < 2) return error.MissingInput;

    // inputs[0] = max_trip_count M, inputs[1] = initial condition
    // inputs[2..] = initial loop-carried dependencies
    const trip_count_value = materializeConstantScalar(builder, inputs[0]);
    if (trip_count_value == null) {
        log.warn("Loop: cannot unroll — trip count is not a constant", .{});
        return error.UnsupportedOp;
    }

    const trip_count: usize = @intFromFloat(trip_count_value.?);
    if (trip_count == 0) {
        // Zero iterations: return initial loop-carried deps.
        // Loop returns loop_carried + scan_outputs. With 0 iters, scan outputs are empty.
        if (inputs.len > 2) return inputs[2]; // first loop-carried dep
        return inputs[0]; // fallback
    }
    if (trip_count > 128) {
        log.warn("Loop: trip count {d} exceeds unroll limit of 128", .{trip_count});
        return error.UnsupportedOp;
    }

    const body_graph = getGraph(node.attributes, "body") orelse return error.UnsupportedOp;
    if (body_graph.nodes.len == 0 or body_graph.inputs.len < 2) return error.UnsupportedOp;

    const num_loop_carried = body_graph.inputs.len - 2; // subtract iter_num and cond
    const num_body_outputs = body_graph.outputs.len;
    if (num_body_outputs == 0) return error.UnsupportedOp;
    const num_scan_outputs = if (num_body_outputs > num_loop_carried + 1)
        num_body_outputs - num_loop_carried - 1
    else
        0;

    // Build output-name → node-index map for the sub-graph
    var body_output_map = std.StringHashMapUnmanaged(u32).empty;
    defer body_output_map.deinit(allocator);
    for (body_graph.nodes, 0..) |*bn, idx| {
        for (bn.outputs) |out_name| {
            if (out_name.len > 0) {
                try body_output_map.put(allocator, out_name, @intCast(idx));
            }
        }
    }

    // Current loop-carried values (start with initial values from inputs[2..])
    var carried = std.ArrayListUnmanaged(NodeId).empty;
    defer carried.deinit(allocator);
    for (0..num_loop_carried) |i| {
        const input_idx = i + 2;
        if (input_idx < inputs.len) {
            try carried.append(allocator, inputs[input_idx]);
        } else {
            try carried.append(allocator, null_node);
        }
    }

    // Accumulate scan outputs per iteration for later concat
    var scan_accum = std.ArrayListUnmanaged(std.ArrayListUnmanaged(NodeId)).empty;
    defer {
        for (scan_accum.items) |*s| s.deinit(allocator);
        scan_accum.deinit(allocator);
    }
    for (0..num_scan_outputs) |_| {
        try scan_accum.append(allocator, std.ArrayListUnmanaged(NodeId).empty);
    }

    // Unroll: for each iteration, inline the body sub-graph
    for (0..trip_count) |iter| {
        // Map body input names → current values
        var name_map = std.StringHashMapUnmanaged(NodeId).empty;
        defer name_map.deinit(allocator);

        // Body input 0: iteration number (i64 scalar)
        const iter_const = try builder.scalarConst(.i32, @floatFromInt(iter));
        if (body_graph.inputs.len > 0 and body_graph.inputs[0].name.len > 0) {
            try name_map.put(allocator, body_graph.inputs[0].name, iter_const);
        }
        // Body input 1: condition (pass through)
        if (body_graph.inputs.len > 1 and body_graph.inputs[1].name.len > 0) {
            try name_map.put(allocator, body_graph.inputs[1].name, inputs[1]);
        }
        // Body inputs 2..N: loop-carried deps
        for (0..num_loop_carried) |ci| {
            if (ci + 2 < body_graph.inputs.len and body_graph.inputs[ci + 2].name.len > 0) {
                try name_map.put(allocator, body_graph.inputs[ci + 2].name, carried.items[ci]);
            }
        }

        // Nested scope for implicit captures (outer-graph references).
        const body_scope = NameScope{ .map = &name_map, .parent = outer_scope };

        // Process body nodes in order (they should be topologically sorted)
        for (body_graph.nodes) |*bn| {
            // Resolve inputs — skip node if any required input is unmapped
            var body_inp: [16]NodeId = .{null_node} ** 16;
            const ninp = @min(bn.inputs.len, 16);
            var all_resolved = true;
            for (0..ninp) |bi| {
                if (bn.inputs[bi].len > 0) {
                    if (body_scope.lookup(bn.inputs[bi])) |nid| {
                        body_inp[bi] = nid;
                    } else {
                        all_resolved = false;
                        break;
                    }
                }
            }
            if (!all_resolved) continue;

            // Convert the node
            const result_id = convertNodeWithScope(allocator, builder, bn, body_inp[0..ninp], null, &body_scope) catch |e| switch (e) {
                error.UnsupportedOp => {
                    log.warn("Loop body iter {d}: skipping unsupported op '{s}'", .{ iter, bn.op_type });
                    continue;
                },
                else => {
                    log.warn("Loop body iter {d}: {s} failed: {}", .{ iter, bn.op_type, e });
                    return e;
                },
            };

            // Map outputs
            if (bn.outputs.len > 0 and bn.outputs[0].len > 0) {
                try name_map.put(allocator, bn.outputs[0], result_id);
            }
            for (bn.outputs[1..]) |out_name| {
                if (out_name.len > 0) {
                    try name_map.put(allocator, out_name, result_id);
                }
            }
        }

        // Extract body outputs: [cond_out, v_1_out, ..., scan_1, ...]
        // Update loop-carried values
        for (0..num_loop_carried) |ci| {
            const out_idx = ci + 1; // skip cond_out
            if (out_idx < body_graph.outputs.len and body_graph.outputs[out_idx].name.len > 0) {
                if (name_map.get(body_graph.outputs[out_idx].name)) |nid| {
                    carried.items[ci] = nid;
                }
            }
        }

        // Accumulate scan outputs
        for (0..num_scan_outputs) |si| {
            const out_idx = num_loop_carried + 1 + si;
            if (out_idx < body_graph.outputs.len and body_graph.outputs[out_idx].name.len > 0) {
                if (name_map.get(body_graph.outputs[out_idx].name)) |nid| {
                    try scan_accum.items[si].append(allocator, nid);
                }
            }
        }
    }

    // Loop outputs: [final_loop_carried..., concat(scan_outputs)...]
    // The primary output (index 0) is typically the first loop-carried dep
    if (carried.items.len > 0) return carried.items[0];
    return inputs[0]; // fallback
}

/// Convert ONNX Scan by unrolling over the sequence dimension.
///
/// ONNX Scan(init_1..init_N, scan_1..scan_M):
///   body inputs:  [state_1_in, ..., state_N_in, slice_1, ..., slice_M]
///   body outputs: [state_1_out, ..., state_N_out, scan_out_1, ..., scan_out_K]
///
/// For each timestep t in [0, sequence_length):
///   1. Slice each scan input at position t along its scan_input_axis
///   2. Feed current state + slices into body sub-graph
///   3. Extract updated state + scan output elements
///   4. Accumulate scan output elements
/// Return final state values + concatenated scan outputs.
fn convertScan(
    allocator: std.mem.Allocator,
    builder: *Builder,
    node: *const NodeProto,
    inputs: []const NodeId,
    extra_outputs: ?[]NodeId,
    outer_scope: ?*const NameScope,
) ConvertError!NodeId {
    const body_graph = getGraph(node.attributes, "body") orelse {
        log.warn("Scan: missing 'body' sub-graph attribute", .{});
        return error.UnsupportedOp;
    };
    if (body_graph.nodes.len == 0) {
        log.warn("Scan: body sub-graph is empty", .{});
        return error.UnsupportedOp;
    }

    // num_scan_inputs (M) is required — tells us the split
    const num_scan_inputs_attr = getInt(node.attributes, "num_scan_inputs", 0);
    if (num_scan_inputs_attr <= 0) {
        log.warn("Scan: num_scan_inputs not set or zero", .{});
        return error.UnsupportedOp;
    }
    const M: usize = @intCast(num_scan_inputs_attr);
    if (inputs.len < M) return error.MissingInput;
    const N: usize = inputs.len - M; // number of state variables

    // Body should have N + M inputs and at least N outputs
    if (body_graph.inputs.len != N + M) {
        log.warn("Scan: body has {d} inputs, expected {d} (N={d} + M={d})", .{ body_graph.inputs.len, N + M, N, M });
        return error.UnsupportedOp;
    }
    if (body_graph.outputs.len < N) {
        log.warn("Scan: body has {d} outputs, expected at least {d} state outputs", .{ body_graph.outputs.len, N });
        return error.UnsupportedOp;
    }
    const K: usize = body_graph.outputs.len - N; // number of scan output elements per step

    // scan_input_axes: which axis to iterate for each scan input (default all 0)
    const scan_input_axes_attr = getInts(node.attributes, "scan_input_axes");
    var scan_input_axes: [8]u8 = .{0} ** 8;
    for (0..M) |i| {
        if (i < scan_input_axes_attr.len) {
            scan_input_axes[i] = @intCast(scan_input_axes_attr[i]);
        }
    }

    // scan_input_directions: 0=forward, 1=backward (default all forward)
    const scan_input_dirs_attr = getInts(node.attributes, "scan_input_directions");

    // scan_output_axes: which axis to accumulate scan outputs along (default all 0)
    const scan_output_axes_attr = getInts(node.attributes, "scan_output_axes");
    var scan_output_axes: [8]u8 = .{0} ** 8;
    for (0..K) |i| {
        if (i < scan_output_axes_attr.len) {
            scan_output_axes[i] = @intCast(scan_output_axes_attr[i]);
        }
    }

    // Determine sequence length from first scan input
    const first_scan_idx = N; // first scan input in node inputs
    const first_scan_shape = builder.graph.node(inputs[first_scan_idx]).output_shape;
    const seq_len = first_scan_shape.dim(scan_input_axes[0]);
    if (seq_len <= 0 or seq_len > 256) {
        log.warn("Scan: sequence length {d} is dynamic or exceeds unroll limit of 256", .{seq_len});
        return error.UnsupportedOp;
    }
    const seq_len_u: usize = @intCast(seq_len);

    // Initialize state variables
    var state = std.ArrayListUnmanaged(NodeId).empty;
    defer state.deinit(allocator);
    for (0..N) |i| {
        try state.append(allocator, inputs[i]);
    }

    // Accumulate scan outputs (K lists, each with seq_len elements)
    var scan_accum = std.ArrayListUnmanaged(std.ArrayListUnmanaged(NodeId)).empty;
    defer {
        for (scan_accum.items) |*s| s.deinit(allocator);
        scan_accum.deinit(allocator);
    }
    for (0..K) |_| {
        try scan_accum.append(allocator, std.ArrayListUnmanaged(NodeId).empty);
    }

    // Unroll over sequence length
    for (0..seq_len_u) |t_raw| {
        // Map body input names → current values
        var name_map = std.StringHashMapUnmanaged(NodeId).empty;
        defer name_map.deinit(allocator);

        // State inputs (first N body inputs)
        for (0..N) |i| {
            if (body_graph.inputs[i].name.len > 0) {
                try name_map.put(allocator, body_graph.inputs[i].name, state.items[i]);
            }
        }

        // Scan input slices (body inputs N..N+M)
        for (0..M) |mi| {
            const scan_node = inputs[N + mi];
            const scan_shape = builder.graph.node(scan_node).output_shape;
            const axis = scan_input_axes[mi];
            const rank = scan_shape.rank();

            // Determine actual timestep (forward or backward)
            const backward = mi < scan_input_dirs_attr.len and scan_input_dirs_attr[mi] == 1;
            const t: usize = if (backward) seq_len_u - 1 - t_raw else t_raw;

            // Slice along scan axis: [t:t+1] then squeeze that axis
            var starts: [8]i64 = .{0} ** 8;
            var limits: [8]i64 = .{0} ** 8;
            for (0..rank) |d| {
                limits[d] = scan_shape.dim(@intCast(d));
            }
            starts[axis] = @intCast(t);
            limits[axis] = @intCast(t + 1);

            // Build sliced shape (with dim=1 on scan axis)
            var sliced_dims: [8]i64 = scan_shape.dims;
            sliced_dims[axis] = 1;
            const sliced_shape = Shape{
                .dtype = scan_shape.dtype,
                .dims = sliced_dims,
                .rank_ = rank,
            };

            const sliced = try builder.graph.addNode(.{
                .op = .{ .slice = .{
                    .starts = starts,
                    .limits = limits,
                    .num_axes = rank,
                } },
                .output_shape = sliced_shape,
                .inputs = .{ scan_node, null_node, null_node, null_node },
                .num_inputs = 1,
            });

            // Squeeze: remove the scan axis dimension
            // New shape has rank-1 dims (remove axis position)
            var squeezed_dims: [8]i64 = .{0} ** 8;
            var di: u8 = 0;
            for (0..rank) |d| {
                if (d != axis) {
                    squeezed_dims[di] = scan_shape.dim(@intCast(d));
                    di += 1;
                }
            }
            const squeezed_shape = Shape{
                .dtype = scan_shape.dtype,
                .dims = squeezed_dims,
                .rank_ = if (rank > 1) rank - 1 else 1,
            };
            const slice_elem = if (rank > 1)
                try builder.reshape(sliced, squeezed_shape)
            else
                sliced;

            if (N + mi < body_graph.inputs.len and body_graph.inputs[N + mi].name.len > 0) {
                try name_map.put(allocator, body_graph.inputs[N + mi].name, slice_elem);
            }
        }

        // Nested scope for implicit captures from outer graph.
        const body_scope = NameScope{ .map = &name_map, .parent = outer_scope };

        // Process body nodes in topological order
        for (body_graph.nodes) |*bn| {
            var body_inp: [16]NodeId = .{null_node} ** 16;
            const ninp = @min(bn.inputs.len, 16);
            var all_resolved = true;
            for (0..ninp) |bi| {
                if (bn.inputs[bi].len > 0) {
                    if (body_scope.lookup(bn.inputs[bi])) |nid| {
                        body_inp[bi] = nid;
                    } else {
                        all_resolved = false;
                        break;
                    }
                }
            }
            if (!all_resolved) continue;

            const result_id = convertNodeWithScope(allocator, builder, bn, body_inp[0..ninp], null, &body_scope) catch |e| switch (e) {
                error.UnsupportedOp => {
                    log.warn("Scan body step {d}: skipping unsupported op '{s}'", .{ t_raw, bn.op_type });
                    continue;
                },
                else => {
                    log.warn("Scan body step {d}: {s} failed: {}", .{ t_raw, bn.op_type, e });
                    return e;
                },
            };

            if (bn.outputs.len > 0 and bn.outputs[0].len > 0) {
                try name_map.put(allocator, bn.outputs[0], result_id);
            }
            for (bn.outputs[1..]) |out_name| {
                if (out_name.len > 0) {
                    try name_map.put(allocator, out_name, result_id);
                }
            }
        }

        // Extract body outputs: first N are updated state, rest are scan elements
        for (0..N) |i| {
            if (body_graph.outputs[i].name.len > 0) {
                if (name_map.get(body_graph.outputs[i].name)) |nid| {
                    state.items[i] = nid;
                }
            }
        }
        for (0..K) |ki| {
            const out_idx = N + ki;
            if (out_idx < body_graph.outputs.len and body_graph.outputs[out_idx].name.len > 0) {
                if (name_map.get(body_graph.outputs[out_idx].name)) |nid| {
                    // Unsqueeze: insert dim=1 at scan_output_axis for concat
                    const out_axis: u8 = scan_output_axes[ki];
                    const elem_shape = builder.graph.node(nid).output_shape;
                    const elem_rank = elem_shape.rank();
                    var unsq_dims: [8]i64 = .{0} ** 8;
                    var di: u8 = 0;
                    for (0..elem_rank + 1) |d| {
                        if (d == out_axis) {
                            unsq_dims[d] = 1;
                        } else {
                            unsq_dims[d] = elem_shape.dim(di);
                            di += 1;
                        }
                    }
                    const unsq_shape = Shape{
                        .dtype = elem_shape.dtype,
                        .dims = unsq_dims,
                        .rank_ = elem_rank + 1,
                    };
                    const unsqueezed = try builder.reshape(nid, unsq_shape);
                    try scan_accum.items[ki].append(allocator, unsqueezed);
                }
            }
        }
    }

    // Concatenate scan outputs along their respective scan_output_axes
    var scan_results = std.ArrayListUnmanaged(NodeId).empty;
    defer scan_results.deinit(allocator);
    for (0..K) |ki| {
        const elems = scan_accum.items[ki].items;
        if (elems.len == 0) continue;
        const out_axis: u8 = scan_output_axes[ki];
        var result = elems[0];
        for (elems[1..]) |next| {
            const r_shape = builder.graph.node(result).output_shape;
            const n_shape = builder.graph.node(next).output_shape;
            var cat_dims: [8]i64 = r_shape.dims;
            cat_dims[out_axis] = r_shape.dim(out_axis) + n_shape.dim(out_axis);
            const cat_shape = Shape{
                .dtype = r_shape.dtype,
                .dims = cat_dims,
                .rank_ = r_shape.rank(),
            };
            result = try builder.graph.addNode(.{
                .op = .{ .concat_prim = .{ .axis = out_axis } },
                .output_shape = cat_shape,
                .inputs = .{ result, next, null_node, null_node },
                .num_inputs = 2,
            });
        }
        try scan_results.append(allocator, result);
    }

    // ONNX Scan returns: [final_state_1, ..., final_state_N, scan_out_1, ..., scan_out_K]
    // Build the full output list: states then scan results
    var all_outputs = std.ArrayListUnmanaged(NodeId).empty;
    defer all_outputs.deinit(allocator);
    for (state.items) |s| try all_outputs.append(allocator, s);
    for (scan_results.items) |sr| try all_outputs.append(allocator, sr);

    if (all_outputs.items.len == 0) return error.UnsupportedOp;

    // Fill extra outputs for multi-output Scan
    if (extra_outputs) |eo| {
        for (all_outputs.items[1..], 0..) |out_id, ei| {
            if (ei >= eo.len) break;
            eo[ei] = out_id;
        }
    }

    return all_outputs.items[0];
}

fn convertIf(
    allocator: std.mem.Allocator,
    builder: *Builder,
    node: *const NodeProto,
    inputs: []const NodeId,
    extra_outputs: ?[]NodeId,
    outer_scope: ?*const NameScope,
) ConvertError!NodeId {
    if (inputs.len < 1) return error.MissingInput;

    // termite's graph IR is a flat DAG with no control flow.
    // We handle If by statically resolving constant conditions and
    // inlining the chosen branch sub-graph.
    const cond = inputs[0];
    const cond_value = materializeConstantScalar(builder, cond);

    if (cond_value) |value| {
        const is_true = value != 0;
        const branch_name: []const u8 = if (is_true) "then_branch" else "else_branch";
        const branch_graph = getGraph(node.attributes, branch_name) orelse return error.UnsupportedOp;

        if (branch_graph.nodes.len == 0 and branch_graph.outputs.len == 0) return error.UnsupportedOp;

        // Map branch input names → parent graph values.
        // Branch sub-graph inputs correspond to If node's inputs[1..] (the feed values).
        var name_map = std.StringHashMapUnmanaged(NodeId).empty;
        defer name_map.deinit(allocator);

        for (branch_graph.inputs, 0..) |inp, i| {
            if (inp.name.len > 0 and i + 1 < inputs.len) {
                try name_map.put(allocator, inp.name, inputs[i + 1]);
            }
        }

        // Nested scope: local name_map wins, otherwise fall back to outer_scope
        // so body nodes can capture parent-graph values implicitly.
        const body_scope = NameScope{ .map = &name_map, .parent = outer_scope };

        // Process branch body nodes in topological order
        for (branch_graph.nodes) |*bn| {
            var body_inp: [16]NodeId = .{null_node} ** 16;
            const ninp = @min(bn.inputs.len, 16);
            var all_resolved = true;
            for (0..ninp) |bi| {
                if (bn.inputs[bi].len > 0) {
                    if (body_scope.lookup(bn.inputs[bi])) |nid| {
                        body_inp[bi] = nid;
                    } else {
                        all_resolved = false;
                        break;
                    }
                }
            }
            if (!all_resolved) continue;

            const result_id = convertNodeWithScope(allocator, builder, bn, body_inp[0..ninp], null, &body_scope) catch |e| switch (e) {
                error.UnsupportedOp => {
                    log.warn("If {s}: skipping unsupported op '{s}'", .{ branch_name, bn.op_type });
                    continue;
                },
                else => {
                    log.warn("If {s}: {s} failed: {}", .{ branch_name, bn.op_type, e });
                    return e;
                },
            };

            // Map all outputs
            for (bn.outputs, 0..) |out_name, oi| {
                if (out_name.len > 0) {
                    // For multi-output sub-ops, first output gets the result_id
                    _ = oi;
                    try name_map.put(allocator, out_name, result_id);
                }
            }
        }

        // Extract branch outputs — these correspond to the If node's outputs
        if (branch_graph.outputs.len > 0 and branch_graph.outputs[0].name.len > 0) {
            if (name_map.get(branch_graph.outputs[0].name)) |primary| {
                // Fill extra outputs for multi-output If
                if (extra_outputs) |eo| {
                    for (branch_graph.outputs[1..], 0..) |out, ei| {
                        if (ei >= eo.len) break;
                        if (out.name.len > 0) {
                            if (name_map.get(out.name)) |nid| {
                                eo[ei] = nid;
                            }
                        }
                    }
                }
                return primary;
            }
        }
    }

    // Non-constant condition: not supported in flat DAG IR.
    log.warn("If: non-constant condition cannot be statically resolved", .{});
    return error.UnsupportedOp;
}

// ── Helpers ──────────────────────────────────────────────────────────

/// Emit a less_than primitive node.
fn cmpLessThan(builder: *Builder, a: NodeId, b: NodeId) ConvertError!NodeId {
    const a_shape = builder.graph.node(a).output_shape;
    const b_shape = builder.graph.node(b).output_shape;
    const out_shape = if (b_shape.rank() > a_shape.rank()) b_shape else a_shape;
    return builder.graph.addNode(.{
        .op = .{ .less_than = {} },
        .output_shape = out_shape,
        .inputs = .{ a, b, null_node, null_node },
        .num_inputs = 2,
    });
}

/// Emit a where_select primitive node.
/// Output shape is the broadcast of cond, on_true, and on_false.
fn selectOp(builder: *Builder, cond: NodeId, on_true: NodeId, on_false: NodeId) ConvertError!NodeId {
    var cond_node = cond;
    var true_node = on_true;
    var false_node = on_false;
    const out_shape = computeBroadcastShape(&.{
        builder.graph.node(cond_node).output_shape,
        builder.graph.node(true_node).output_shape,
        builder.graph.node(false_node).output_shape,
    }, builder.graph.node(true_node).output_shape.dtype);

    if (builder.graph.node(cond_node).output_shape.rank() < out_shape.rank()) {
        cond_node = try prependOnes(builder, cond_node, builder.graph.node(cond_node).output_shape, out_shape.rank());
    }
    if (builder.graph.node(true_node).output_shape.rank() < out_shape.rank()) {
        true_node = try prependOnes(builder, true_node, builder.graph.node(true_node).output_shape, out_shape.rank());
    }
    if (builder.graph.node(false_node).output_shape.rank() < out_shape.rank()) {
        false_node = try prependOnes(builder, false_node, builder.graph.node(false_node).output_shape, out_shape.rank());
    }

    cond_node = try broadcastTo(builder, cond_node, out_shape);
    true_node = try broadcastTo(builder, true_node, out_shape);
    false_node = try broadcastTo(builder, false_node, out_shape);

    return builder.graph.addNode(.{
        .op = .{ .where_select = {} },
        .output_shape = out_shape,
        .inputs = .{ cond_node, false_node, true_node, null_node },
        .num_inputs = 3,
    });
}

/// Create a constant-filled tensor matching `ref_shape` except with `pad_size`
/// along `axis`. Used by convertPad to build zero/constant padding slabs.
fn makePadTensor(builder: *Builder, ref_shape: Shape, axis: u8, pad_size: i64, value: f32) !NodeId {
    var dims: [8]i64 = ref_shape.dims;
    dims[axis] = pad_size;
    const pad_shape = Shape{ .dtype = ref_shape.dtype, .dims = dims, .rank_ = ref_shape.rank() };

    const scalar = try builder.scalarConst(ref_shape.dtype, value);

    return builder.graph.addNode(.{
        .op = .{
            .broadcast_in_dim = .{
                .target_shape = pad_shape,
                .num_axes = 0, // scalar broadcast — no axes mapped
            },
        },
        .output_shape = pad_shape,
        .inputs = .{ scalar, null_node, null_node, null_node },
        .num_inputs = 1,
    });
}

/// Compute the shape after concatenating `extra` elements along `axis`.
fn concatShape(base: Shape, axis: u8, extra: i64) Shape {
    var dims = base.dims;
    dims[axis] += extra;
    return Shape{ .dtype = base.dtype, .dims = dims, .rank_ = base.rank() };
}

/// Pad using gather-based index mapping for reflect and edge modes.
///
/// For each axis with non-zero padding, build an i32 index tensor that
/// maps each position in the padded output back to a source position:
///   - Reflect: mirrors at the boundary, e.g. [3,2,1, 0,1,2,3,4, 3,2,1]
///   - Edge: clamps to the boundary, e.g. [0,0,0, 0,1,2,3,4, 4,4,4]
///
/// Then gather along that axis to produce the padded tensor.
fn convertPadGather(builder: *Builder, input: NodeId, pads_data: []const f32, rank: u8, is_reflect: bool) ConvertError!NodeId {
    var current = input;

    for (0..rank) |axis| {
        const pad_begin: i64 = @intFromFloat(pads_data[axis]);
        const pad_end: i64 = @intFromFloat(pads_data[axis + rank]);
        if (pad_begin == 0 and pad_end == 0) continue;

        const cur_shape = builder.graph.node(current).output_shape;
        const dim = cur_shape.dim(@intCast(axis));
        if (dim <= 0) continue;

        // Reflect requires pad < dim (can't reflect more than the dimension)
        if (is_reflect and (pad_begin >= dim or pad_end >= dim)) return error.UnsupportedOp;

        const total: usize = @intCast(pad_begin + dim + pad_end);
        if (total > 4096) return error.TooManyDimensions;

        var i32_data: [4096]i32 = undefined;
        for (0..total) |j| {
            const src: i64 = @as(i64, @intCast(j)) - pad_begin;
            if (is_reflect) {
                // Reflect: map negative indices to |src|, overflow to 2*(dim-1)-src
                var idx = src;
                if (idx < 0) idx = -idx;
                if (idx >= dim) idx = 2 * (dim - 1) - idx;
                i32_data[j] = @intCast(@max(0, @min(dim - 1, idx)));
            } else {
                // Edge: clamp to [0, dim-1]
                i32_data[j] = @intCast(@max(0, @min(dim - 1, src)));
            }
        }

        // Create i32 index tensor
        const idx_shape = Shape.init(.i32, &.{@as(i64, @intCast(total))});
        const indices = try builder.tensorConstBytes(std.mem.sliceAsBytes(i32_data[0..total]), idx_shape);

        // Gather along this axis
        var gathered_dims: [8]i64 = cur_shape.dims;
        gathered_dims[axis] = @intCast(total);
        const gathered_shape = Shape{
            .dtype = cur_shape.dtype,
            .dims = gathered_dims,
            .rank_ = cur_shape.rank(),
        };
        current = try builder.graph.addNode(.{
            .op = .{ .gather = .{ .axis = @intCast(axis) } },
            .output_shape = gathered_shape,
            .inputs = .{ current, indices, null_node, null_node },
            .num_inputs = 2,
        });
    }

    return current;
}

/// Safely convert f32 to i64, clamping values outside i64 range.
/// ONNX uses INT64_MAX as sentinels (e.g. Slice end), which overflow
/// when round-tripped through f32.
fn clampF32ToI64(v: f32) i64 {
    const max_safe: f32 = @floatFromInt(@as(i64, std.math.maxInt(i64)));
    const min_safe: f32 = @floatFromInt(@as(i64, std.math.minInt(i64)));
    if (v >= max_safe) return std.math.maxInt(i64);
    if (v <= min_safe) return std.math.minInt(i64);
    return @intFromFloat(v);
}

fn materializeConstantNodeValues(
    graph: *const Graph,
    n: *const ml.graph.Node,
    c_attrs: ml.graph.node.ConstantAttrs,
    buf: []f32,
) ?[]const f32 {
    const len: usize = @min(@as(usize, @intCast(c_attrs.data_len)), buf.len);
    switch (n.output_shape.dtype) {
        .f32 => {
            const data = graph.constantData(c_attrs.data_offset, c_attrs.data_len);
            @memcpy(buf[0..len], data[0..len]);
        },
        .f16 => {
            const data = graph.constantDataAs(f16, c_attrs.data_offset, c_attrs.data_len);
            for (data[0..len], buf[0..len]) |value, *out| out.* = @floatCast(value);
        },
        .bf16 => {
            const data = graph.constantDataAs(u16, c_attrs.data_offset, c_attrs.data_len);
            for (data[0..len], buf[0..len]) |value, *out| {
                const bits: u32 = @as(u32, value) << 16;
                const f: f32 = @bitCast(bits);
                out.* = f;
            }
        },
        .f64 => {
            const data = graph.constantDataAs(f64, c_attrs.data_offset, c_attrs.data_len);
            for (data[0..len], buf[0..len]) |value, *out| out.* = @floatCast(value);
        },
        .i8 => {
            const data = graph.constantDataAs(i8, c_attrs.data_offset, c_attrs.data_len);
            for (data[0..len], buf[0..len]) |value, *out| out.* = @floatFromInt(value);
        },
        .i16 => {
            const data = graph.constantDataAs(i16, c_attrs.data_offset, c_attrs.data_len);
            for (data[0..len], buf[0..len]) |value, *out| out.* = @floatFromInt(value);
        },
        .i32 => {
            const data = graph.constantDataAs(i32, c_attrs.data_offset, c_attrs.data_len);
            for (data[0..len], buf[0..len]) |value, *out| out.* = @floatFromInt(value);
        },
        .i64 => {
            const data = graph.constantDataAs(i64, c_attrs.data_offset, c_attrs.data_len);
            for (data[0..len], buf[0..len]) |value, *out| out.* = @floatFromInt(value);
        },
        .u8 => {
            const data = graph.constantDataAs(u8, c_attrs.data_offset, c_attrs.data_len);
            for (data[0..len], buf[0..len]) |value, *out| out.* = @floatFromInt(value);
        },
        .bool_ => {
            const data = graph.constantDataAs(u8, c_attrs.data_offset, c_attrs.data_len);
            for (data[0..len], buf[0..len]) |value, *out| out.* = if (value == 0) 0.0 else 1.0;
        },
    }
    return buf[0..len];
}

fn materializeConstantScalar(builder: *Builder, node_id: NodeId) ?f32 {
    if (node_id == null_node) return null;
    const n = builder.graph.node(node_id);
    const c_attrs = switch (n.op) {
        .constant => |attrs| attrs,
        else => return null,
    };
    var buf: [1]f32 = undefined;
    const data = materializeConstantNodeValues(builder.graph, n, c_attrs, &buf) orelse return null;
    if (data.len == 0) return null;
    return data[0];
}

/// Check if an ONNX op type string is supported.
pub fn isSupported(op_type: []const u8) bool {
    return OpType.fromString(op_type) != null;
}

// ── Tests ────────────────────────────────────────────────────────────

test "OpType.fromString" {
    try std.testing.expect(OpType.fromString("Add") != null);
    try std.testing.expect(OpType.fromString("MatMul") != null);
    try std.testing.expect(OpType.fromString("Relu") != null);
    try std.testing.expect(OpType.fromString("NotARealOp") == null);
}

test "isSupported" {
    try std.testing.expect(isSupported("Add"));
    try std.testing.expect(isSupported("Reshape"));
    try std.testing.expect(!isSupported("CustomOp"));
}

test "convertNode Identity passthrough" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("input", Shape.init(.f32, &.{ 2, 4 }));
    const node = NodeProto{ .op_type = "Identity" };
    const result = try convertNode(allocator, &b, &node, &.{x}, null);
    try std.testing.expectEqual(x, result);
}

test "convertNode Add" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const y = try b.parameter("y", Shape.init(.f32, &.{ 2, 4 }));
    const node = NodeProto{ .op_type = "Add" };
    const result = try convertNode(allocator, &b, &node, &.{ x, y }, null);
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .add);
}

test "convertNode Relu emits fused" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("input", Shape.init(.f32, &.{ 2, 4 }));
    const node = NodeProto{ .op_type = "Relu" };
    const result = try convertNode(allocator, &b, &node, &.{x}, null);
    try std.testing.expect(g.node(result).op.isFused());
}

test "convertNode unsupported op" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("input", Shape.init(.f32, &.{ 2, 4 }));
    const node = NodeProto{ .op_type = "FancyCustomOp" };
    try std.testing.expectError(error.UnsupportedOp, convertNode(allocator, &b, &node, &.{x}, null));
}

// ── Phase 2 Tests ───────────────────────────────────────────────────

test "OpType.fromString Phase 2 ops" {
    try std.testing.expect(OpType.fromString("Equal") != null);
    try std.testing.expect(OpType.fromString("Greater") != null);
    try std.testing.expect(OpType.fromString("LessOrEqual") != null);
    try std.testing.expect(OpType.fromString("GreaterOrEqual") != null);
    try std.testing.expect(OpType.fromString("Not") != null);
    try std.testing.expect(OpType.fromString("Min") != null);
    try std.testing.expect(OpType.fromString("Max") != null);
    try std.testing.expect(OpType.fromString("FastGelu") != null);
    try std.testing.expect(OpType.fromString("Silu") != null);
    try std.testing.expect(OpType.fromString("LeakyRelu") != null);
    try std.testing.expect(OpType.fromString("LogSoftmax") != null);
    try std.testing.expect(OpType.fromString("Expand") != null);
    try std.testing.expect(OpType.fromString("Pad") != null);
    try std.testing.expect(OpType.fromString("Split") != null);
    try std.testing.expect(OpType.fromString("Shape") != null);
    try std.testing.expect(OpType.fromString("ConstantOfShape") != null);
    try std.testing.expect(OpType.fromString("LayerNormalization") != null);
    try std.testing.expect(OpType.fromString("SimplifiedLayerNormalization") != null);
    try std.testing.expect(OpType.fromString("BatchNormalization") != null);
    try std.testing.expect(OpType.fromString("ReduceMin") != null);
    try std.testing.expect(OpType.fromString("ReduceProd") != null);
    try std.testing.expect(OpType.fromString("ScatterND") != null);
    try std.testing.expect(OpType.fromString("Range") != null);
    try std.testing.expect(OpType.fromString("Tile") != null);
    try std.testing.expect(OpType.fromString("Sign") != null);
}

test "isSupported Phase 2 ops" {
    try std.testing.expect(isSupported("LayerNormalization"));
    try std.testing.expect(isSupported("BatchNormalization"));
    try std.testing.expect(isSupported("Expand"));
    try std.testing.expect(isSupported("Split"));
    try std.testing.expect(isSupported("Equal"));
    try std.testing.expect(isSupported("Greater"));
    try std.testing.expect(isSupported("Silu"));
}

test "convertNode Greater" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const y = try b.parameter("y", Shape.init(.f32, &.{ 2, 4 }));
    const node = NodeProto{ .op_type = "Greater" };
    const result = try convertNode(allocator, &b, &node, &.{ x, y }, null);
    // Greater(a,b) → LessThan(b,a)
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .less_than);
}

test "convertNode Silu emits fused" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("input", Shape.init(.f32, &.{ 2, 4 }));
    const node = NodeProto{ .op_type = "Silu" };
    const result = try convertNode(allocator, &b, &node, &.{x}, null);
    try std.testing.expect(g.node(result).op.isFused());
}

test "convertNode FastGelu emits fused" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("input", Shape.init(.f32, &.{ 2, 4 }));
    const node = NodeProto{ .op_type = "FastGelu" };
    const result = try convertNode(allocator, &b, &node, &.{x}, null);
    try std.testing.expect(g.node(result).op.isFused());
}

test "convertNode LogSoftmax emits fused" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("input", Shape.init(.f32, &.{ 2, 4 }));
    const node = NodeProto{ .op_type = "LogSoftmax" };
    const result = try convertNode(allocator, &b, &node, &.{x}, null);
    try std.testing.expect(g.node(result).op.isFused());
}

test "convertNode batch MatMul 3D" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 4, 8, 16 }));
    const y = try b.parameter("y", Shape.init(.f32, &.{ 4, 16, 32 }));
    const node = NodeProto{ .op_type = "MatMul" };
    const result = try convertNode(allocator, &b, &node, &.{ x, y }, null);
    const out_shape = g.node(result).output_shape;
    // [4, 8, 16] x [4, 16, 32] → [4, 8, 32]
    try std.testing.expectEqual(@as(u8, 3), out_shape.rank());
    try std.testing.expectEqual(@as(i64, 4), out_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 8), out_shape.dim(1));
    try std.testing.expectEqual(@as(i64, 32), out_shape.dim(2));
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .dot_general);
}

test "convertNode batch MatMul 3D lhs with shared 2D rhs lowers through reshape" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ -1, 76, 512 }));
    const y = try b.parameter("y", Shape.init(.f32, &.{ 512, 512 }));
    const node = NodeProto{ .op_type = "MatMul" };
    const result = try convertNode(allocator, &b, &node, &.{ x, y }, null);
    const out_shape = g.node(result).output_shape;

    try std.testing.expectEqual(@as(u8, 3), out_shape.rank());
    try std.testing.expectEqual(@as(i64, -1), out_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 76), out_shape.dim(1));
    try std.testing.expectEqual(@as(i64, 512), out_shape.dim(2));
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .reshape);

    const mm_id = g.node(result).inputs[0];
    try std.testing.expect(std.meta.activeTag(g.node(mm_id).op) == .dot_general);
}

test "convertNode Shape materializes dims" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("input", Shape.init(.f32, &.{ 2, 3, 4 }));
    const node = NodeProto{ .op_type = "Shape" };
    const result = try convertNode(allocator, &b, &node, &.{x}, null);
    const out_shape = g.node(result).output_shape;
    // Should be a 1-D tensor with 3 elements
    try std.testing.expectEqual(@as(u8, 1), out_shape.rank());
    try std.testing.expectEqual(@as(i64, 3), out_shape.dim(0));
}

test "convertNode LayerNormalization emits fused" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const scale = try b.parameter("scale", Shape.init(.f32, &.{4}));
    const bias = try b.parameter("bias", Shape.init(.f32, &.{4}));
    const node = NodeProto{ .op_type = "LayerNormalization" };
    const result = try convertNode(allocator, &b, &node, &.{ x, scale, bias }, null);
    try std.testing.expect(g.node(result).op.isFused());
    // Check it's specifically fused_layer_norm
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .fused_layer_norm);
}

test "convertNode SimplifiedLayerNormalization emits fused rms_norm" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const scale = try b.parameter("scale", Shape.init(.f32, &.{4}));
    const node = NodeProto{ .op_type = "SimplifiedLayerNormalization" };
    const result = try convertNode(allocator, &b, &node, &.{ x, scale }, null);
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .fused_rms_norm);
}

test "convertNode broadcast Add different ranks" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // [2, 3, 4] + [4] should work with broadcasting
    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 3, 4 }));
    const y = try b.parameter("y", Shape.init(.f32, &.{4}));
    const node = NodeProto{ .op_type = "Add" };
    const result = try convertNode(allocator, &b, &node, &.{ x, y }, null);
    // Result should exist (not error)
    try std.testing.expect(result != null_node);
}

test "convertNode Min and Max" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const y = try b.parameter("y", Shape.init(.f32, &.{ 2, 4 }));
    {
        const node = NodeProto{ .op_type = "Min" };
        const result = try convertNode(allocator, &b, &node, &.{ x, y }, null);
        try std.testing.expect(result != null_node);
    }
    {
        const node = NodeProto{ .op_type = "Max" };
        const result = try convertNode(allocator, &b, &node, &.{ x, y }, null);
        try std.testing.expect(result != null_node);
    }
}

test "convertNode Gather with axis" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const data = try b.parameter("data", Shape.init(.f32, &.{ 3, 4, 5 }));
    const indices = try b.parameter("indices", Shape.init(.f32, &.{2}));
    var attrs = [_]AttributeProto{
        .{ .name = "axis", .i = 1 },
    };
    const node = NodeProto{ .op_type = "Gather", .attributes = &attrs };
    const result = try convertNode(allocator, &b, &node, &.{ data, indices }, null);
    const out_shape = g.node(result).output_shape;
    // data[3,4,5] gather axis=1 indices[2] → [3, 2, 5]
    try std.testing.expectEqual(@as(u8, 3), out_shape.rank());
    try std.testing.expectEqual(@as(i64, 3), out_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 2), out_shape.dim(1));
    try std.testing.expectEqual(@as(i64, 5), out_shape.dim(2));
}

// ── Phase 3 Tests ───────────────────────────────────────────────────

test "OpType.fromString Phase 3 ops" {
    try std.testing.expect(OpType.fromString("Conv") != null);
    try std.testing.expect(OpType.fromString("MultiHeadAttention") != null);
    try std.testing.expect(OpType.fromString("GroupQueryAttention") != null);
    try std.testing.expect(OpType.fromString("Trilu") != null);
    try std.testing.expect(OpType.fromString("GatherElements") != null);
    try std.testing.expect(OpType.fromString("CumSum") != null);
    try std.testing.expect(OpType.fromString("DequantizeLinear") != null);
    try std.testing.expect(OpType.fromString("AveragePool") != null);
    try std.testing.expect(OpType.fromString("MaxPool") != null);
    try std.testing.expect(OpType.fromString("GlobalAveragePool") != null);
}

test "convertNode Conv 1D emits fused_conv1d" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // Input: [batch=1, channels=3, length=10], Kernel: [out_ch=8, in_ch=3, k=3]
    const x = try b.parameter("x", Shape.init(.f32, &.{ 1, 3, 10 }));
    const w = try b.parameter("w", Shape.init(.f32, &.{ 8, 3, 3 }));
    var attrs = [_]AttributeProto{
        .{ .name = "kernel_shape", .ints = @constCast(&[_]i64{3}) },
    };
    const node = NodeProto{ .op_type = "Conv", .attributes = &attrs };
    const result = try convertNode(allocator, &b, &node, &.{ x, w }, null);
    const out_shape = g.node(result).output_shape;
    // [1, 8, 8] (no padding: 10-3+1=8)
    try std.testing.expectEqual(@as(u8, 3), out_shape.rank());
    try std.testing.expectEqual(@as(i64, 1), out_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 8), out_shape.dim(1));
    try std.testing.expectEqual(@as(i64, 8), out_shape.dim(2));
    // Fused path: result is fused_conv1d; conv_general is still in the graph as vjp_alternate.
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .fused_conv1d);
    try std.testing.expect(countOpTag(&g, .conv_general) >= 1);
}

test "convertNode Conv 2D with bias emits fused_conv2d" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 1, 3, 8, 8 }));
    const w = try b.parameter("w", Shape.init(.f32, &.{ 16, 3, 3, 3 }));
    const bias = try b.parameter("bias", Shape.init(.f32, &.{16}));
    var attrs = [_]AttributeProto{
        .{ .name = "kernel_shape", .ints = @constCast(&[_]i64{ 3, 3 }) },
    };
    const node = NodeProto{ .op_type = "Conv", .attributes = &attrs };
    const result = try convertNode(allocator, &b, &node, &.{ x, w, bias }, null);
    const out_shape = g.node(result).output_shape;
    // [1, 16, 6, 6] (no padding: 8-3+1=6)
    try std.testing.expectEqual(@as(u8, 4), out_shape.rank());
    try std.testing.expectEqual(@as(i64, 1), out_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 16), out_shape.dim(1));
    try std.testing.expectEqual(@as(i64, 6), out_shape.dim(2));
    try std.testing.expectEqual(@as(i64, 6), out_shape.dim(3));
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .fused_conv2d);
}

test "convertNode Conv 2D with stride/pad/groups emits fused_conv2d" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // Depthwise conv: groups == in_channels == out_channels == 32
    const x = try b.parameter("x", Shape.init(.f32, &.{ 1, 32, 16, 16 }));
    const w = try b.parameter("w", Shape.init(.f32, &.{ 32, 1, 3, 3 }));
    var attrs = [_]AttributeProto{
        .{ .name = "kernel_shape", .ints = @constCast(&[_]i64{ 3, 3 }) },
        .{ .name = "strides", .ints = @constCast(&[_]i64{ 2, 2 }) },
        .{ .name = "pads", .ints = @constCast(&[_]i64{ 1, 1, 1, 1 }) },
        .{ .name = "group", .i = 32 },
    };
    const node = NodeProto{ .op_type = "Conv", .attributes = &attrs };
    const result = try convertNode(allocator, &b, &node, &.{ x, w }, null);
    const out_shape = g.node(result).output_shape;
    // (16 + 2 - 3)/2 + 1 = 8
    try std.testing.expectEqual(@as(i64, 8), out_shape.dim(2));
    try std.testing.expectEqual(@as(i64, 8), out_shape.dim(3));
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .fused_conv2d);
}

test "convertNode Conv falls back to conv_general on dilation" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 1, 3, 16, 16 }));
    const w = try b.parameter("w", Shape.init(.f32, &.{ 8, 3, 3, 3 }));
    var attrs = [_]AttributeProto{
        .{ .name = "kernel_shape", .ints = @constCast(&[_]i64{ 3, 3 }) },
        .{ .name = "dilations", .ints = @constCast(&[_]i64{ 2, 2 }) },
        .{ .name = "pads", .ints = @constCast(&[_]i64{ 2, 2, 2, 2 }) },
    };
    const node = NodeProto{ .op_type = "Conv", .attributes = &attrs };
    const result = try convertNode(allocator, &b, &node, &.{ x, w }, null);
    // Dilation != 1 forces the general decomposition.
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .conv_general);
    try std.testing.expectEqual(@as(u32, 0), countOpTag(&g, .fused_conv2d));
}

test "convertNode Conv falls back to conv_general on asymmetric pads" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 1, 3, 10, 10 }));
    const w = try b.parameter("w", Shape.init(.f32, &.{ 8, 3, 3, 3 }));
    var attrs = [_]AttributeProto{
        .{ .name = "kernel_shape", .ints = @constCast(&[_]i64{ 3, 3 }) },
        .{ .name = "pads", .ints = @constCast(&[_]i64{ 1, 0, 0, 1 }) },
    };
    const node = NodeProto{ .op_type = "Conv", .attributes = &attrs };
    const result = try convertNode(allocator, &b, &node, &.{ x, w }, null);
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .conv_general);
    try std.testing.expectEqual(@as(u32, 0), countOpTag(&g, .fused_conv2d));
}

test "convertNode Conv 1D falls back to conv_general when grouped" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 1, 16, 32 }));
    const w = try b.parameter("w", Shape.init(.f32, &.{ 16, 1, 3 }));
    var attrs = [_]AttributeProto{
        .{ .name = "kernel_shape", .ints = @constCast(&[_]i64{3}) },
        .{ .name = "group", .i = 16 },
    };
    const node = NodeProto{ .op_type = "Conv", .attributes = &attrs };
    const result = try convertNode(allocator, &b, &node, &.{ x, w }, null);
    // fused_conv1d has no groups; must fall back.
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .conv_general);
    try std.testing.expectEqual(@as(u32, 0), countOpTag(&g, .fused_conv1d));
}

fn countOpTag(g: *const Graph, tag: std.meta.Tag(ml.graph.node.OpCode)) u32 {
    var n: u32 = 0;
    for (0..g.nodeCount()) |i| {
        if (std.meta.activeTag(g.node(@intCast(i)).op) == tag) n += 1;
    }
    return n;
}

test "convertNode MultiHeadAttention 3D emits fused_sdpa" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // [batch=2, seq=8, hidden=64], num_heads=4 → head_dim=16
    const q = try b.parameter("q", Shape.init(.f32, &.{ 2, 8, 64 }));
    const k = try b.parameter("k", Shape.init(.f32, &.{ 2, 8, 64 }));
    const v = try b.parameter("v", Shape.init(.f32, &.{ 2, 8, 64 }));
    var attrs = [_]AttributeProto{
        .{ .name = "num_heads", .i = 4 },
    };
    const node = NodeProto{ .op_type = "MultiHeadAttention", .attributes = &attrs };
    const result = try convertNode(allocator, &b, &node, &.{ q, k, v }, null);
    const out_shape = g.node(result).output_shape;
    // Output should be [2, 8, 64] (same as input)
    try std.testing.expectEqual(@as(u8, 3), out_shape.rank());
    try std.testing.expectEqual(@as(i64, 2), out_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 8), out_shape.dim(1));
    try std.testing.expectEqual(@as(i64, 64), out_shape.dim(2));
    // Verify a fused_sdpa node was emitted (non-causal, no mask).
    try std.testing.expect(countOpTag(&g, .fused_sdpa) >= 1);
    try std.testing.expectEqual(@as(u32, 0), countOpTag(&g, .fused_causal_self_attention));
}

test "convertNode MultiHeadAttention causal emits fused_causal_self_attention" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const q = try b.parameter("q", Shape.init(.f32, &.{ 1, 8, 64 }));
    const k = try b.parameter("k", Shape.init(.f32, &.{ 1, 8, 64 }));
    const v = try b.parameter("v", Shape.init(.f32, &.{ 1, 8, 64 }));
    var attrs = [_]AttributeProto{
        .{ .name = "num_heads", .i = 4 },
        .{ .name = "unidirectional", .i = 1 },
    };
    const node = NodeProto{ .op_type = "MultiHeadAttention", .attributes = &attrs };
    _ = try convertNode(allocator, &b, &node, &.{ q, k, v }, null);
    try std.testing.expect(countOpTag(&g, .fused_causal_self_attention) >= 1);
}

test "convertNode MultiHeadAttention cross attention emits fused_cross_attention" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // Different seq_len for Q (dec=4) vs K/V (enc=12)
    const q = try b.parameter("q", Shape.init(.f32, &.{ 1, 4, 64 }));
    const k = try b.parameter("k", Shape.init(.f32, &.{ 1, 12, 64 }));
    const v = try b.parameter("v", Shape.init(.f32, &.{ 1, 12, 64 }));
    var attrs = [_]AttributeProto{
        .{ .name = "num_heads", .i = 4 },
    };
    const node = NodeProto{ .op_type = "MultiHeadAttention", .attributes = &attrs };
    _ = try convertNode(allocator, &b, &node, &.{ q, k, v }, null);
    try std.testing.expect(countOpTag(&g, .fused_cross_attention) >= 1);
}

test "convertNode MultiHeadAttention 4D passthrough" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // Already in [B, H, S, D] layout.
    const q = try b.parameter("q", Shape.init(.f32, &.{ 2, 4, 8, 16 }));
    const k = try b.parameter("k", Shape.init(.f32, &.{ 2, 4, 8, 16 }));
    const v = try b.parameter("v", Shape.init(.f32, &.{ 2, 4, 8, 16 }));
    var attrs = [_]AttributeProto{
        .{ .name = "num_heads", .i = 4 },
    };
    const node = NodeProto{ .op_type = "MultiHeadAttention", .attributes = &attrs };
    const result = try convertNode(allocator, &b, &node, &.{ q, k, v }, null);
    const out_shape = g.node(result).output_shape;
    try std.testing.expectEqual(@as(u8, 4), out_shape.rank());
    try std.testing.expect(countOpTag(&g, .fused_sdpa) >= 1);
}

test "convertNode GroupQueryAttention emits fused_causal_self_attention" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // GQA: 8 query heads, 2 KV heads, head_dim=16
    // Q: [2, 4, 128] (8 heads * 16 dim)
    // K: [2, 4, 32] (2 heads * 16 dim)
    // V: [2, 4, 32]
    const q = try b.parameter("q", Shape.init(.f32, &.{ 2, 4, 128 }));
    const k = try b.parameter("k", Shape.init(.f32, &.{ 2, 4, 32 }));
    const v = try b.parameter("v", Shape.init(.f32, &.{ 2, 4, 32 }));
    var attrs = [_]AttributeProto{
        .{ .name = "num_heads", .i = 8 },
        .{ .name = "kv_num_heads", .i = 2 },
    };
    const node = NodeProto{ .op_type = "GroupQueryAttention", .attributes = &attrs };
    const result = try convertNode(allocator, &b, &node, &.{ q, k, v }, null);
    const out_shape = g.node(result).output_shape;
    // Output: [2, 4, 128]
    try std.testing.expectEqual(@as(u8, 3), out_shape.rank());
    try std.testing.expectEqual(@as(i64, 2), out_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 4), out_shape.dim(1));
    try std.testing.expectEqual(@as(i64, 128), out_shape.dim(2));
    // GQA defaults to unidirectional=1 → causal.
    try std.testing.expect(countOpTag(&g, .fused_causal_self_attention) >= 1);
}

test "convertNode GroupQueryAttention non-causal emits fused_sdpa" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const q = try b.parameter("q", Shape.init(.f32, &.{ 1, 4, 128 }));
    const k = try b.parameter("k", Shape.init(.f32, &.{ 1, 4, 32 }));
    const v = try b.parameter("v", Shape.init(.f32, &.{ 1, 4, 32 }));
    var attrs = [_]AttributeProto{
        .{ .name = "num_heads", .i = 8 },
        .{ .name = "kv_num_heads", .i = 2 },
        .{ .name = "unidirectional", .i = 0 },
    };
    const node = NodeProto{ .op_type = "GroupQueryAttention", .attributes = &attrs };
    _ = try convertNode(allocator, &b, &node, &.{ q, k, v }, null);
    try std.testing.expect(countOpTag(&g, .fused_sdpa) >= 1);
}

test "convertNode DequantizeLinear per-tensor" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const scale = try b.parameter("scale", Shape.init(.f32, &.{1}));
    const node = NodeProto{ .op_type = "DequantizeLinear" };
    const result = try convertNode(allocator, &b, &node, &.{ x, scale }, null);
    try std.testing.expect(result != null_node);
}

test "convertNode DequantizeLinear per-axis with zero_point" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // Per-channel quantization: x [2,4], scale [4], zero_point [4], axis=1
    const x = try b.parameter("x", Shape.init(.i32, &.{ 2, 4 }));
    const scale = try b.parameter("scale", Shape.init(.f32, &.{4}));
    const zp = try b.parameter("zp", Shape.init(.i32, &.{4}));
    var attrs = [_]AttributeProto{
        .{ .name = "axis", .i = 1 },
    };
    const node = NodeProto{ .op_type = "DequantizeLinear", .attributes = &attrs };
    _ = &attrs;
    const result = try convertNode(allocator, &b, &node, &.{ x, scale, zp }, null);
    try std.testing.expect(result != null_node);
    // Final op should be mul (dequant * scale)
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .mul);
}

test "convertNode QuantizeLinear per-tensor" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const scale = try b.tensorConst(&.{0.1}, Shape.init(.f32, &.{1}));
    const node = NodeProto{ .op_type = "QuantizeLinear" };
    const result = try convertNode(allocator, &b, &node, &.{ x, scale }, null);
    try std.testing.expect(result != null_node);
    // Output should be uint8 by default
    try std.testing.expectEqual(DType.u8, g.node(result).output_shape.dtype);
}

test "convertNode QuantizeLinear with zero_point" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const scale = try b.tensorConst(&.{0.05}, Shape.init(.f32, &.{1}));
    const zp = try b.parameter("zp", Shape.init(.u8, &.{1}));
    const node = NodeProto{ .op_type = "QuantizeLinear" };
    const result = try convertNode(allocator, &b, &node, &.{ x, scale, zp }, null);
    try std.testing.expect(result != null_node);
    // Output should be u8 (from zero_point dtype)
    try std.testing.expectEqual(DType.u8, g.node(result).output_shape.dtype);
}

test "convertNode DequantizeLinear block quantization" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // opset 21 block quant: x [2, 16], scale [2, 2] (block_size=8 along axis=1)
    const x = try b.parameter("x", Shape.init(.u8, &.{ 2, 16 }));
    const scale = try b.parameter("scale", Shape.init(.f32, &.{ 2, 2 }));
    var attrs = [_]AttributeProto{
        .{ .name = "axis", .i = 1 },
        .{ .name = "block_size", .i = 8 },
    };
    const node = NodeProto{ .op_type = "DequantizeLinear", .attributes = &attrs };
    const result = try convertNode(allocator, &b, &node, &.{ x, scale }, null);
    const out_shape = g.node(result).output_shape;
    // Block dequant reshapes back to the original x shape.
    try std.testing.expectEqual(@as(u8, 2), out_shape.rank());
    try std.testing.expectEqual(@as(i64, 2), out_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 16), out_shape.dim(1));
    try std.testing.expectEqual(DType.f32, out_shape.dtype);
}

test "convertNode Quantize → Dequantize roundtrip preserves shape" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const scale = try b.tensorConst(&.{0.1}, Shape.init(.f32, &.{1}));

    // Quantize
    const q_node = NodeProto{ .op_type = "QuantizeLinear" };
    const q = try convertNode(allocator, &b, &q_node, &.{ x, scale }, null);
    try std.testing.expectEqual(DType.u8, g.node(q).output_shape.dtype);

    // Dequantize
    const dq_node = NodeProto{ .op_type = "DequantizeLinear" };
    const dq = try convertNode(allocator, &b, &dq_node, &.{ q, scale }, null);
    const dq_shape = g.node(dq).output_shape;
    try std.testing.expectEqual(DType.f32, dq_shape.dtype);
    try std.testing.expectEqual(@as(i64, 2), dq_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 4), dq_shape.dim(1));
}

test "convertNode GlobalAveragePool" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // [batch=1, channels=64, H=8, W=8]
    const x = try b.parameter("x", Shape.init(.f32, &.{ 1, 64, 8, 8 }));
    const node = NodeProto{ .op_type = "GlobalAveragePool" };
    const result = try convertNode(allocator, &b, &node, &.{x}, null);
    try std.testing.expect(result != null_node);
    // Should reduce spatial dims
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .reduce_mean);
}

// ── Phase 4 Tests ───────────────────────────────────────────────────

test "OpType.fromString Phase 4 ops" {
    try std.testing.expect(OpType.fromString("RotaryEmbedding") != null);
    try std.testing.expect(OpType.fromString("HardSwish") != null);
    try std.testing.expect(OpType.fromString("And") != null);
    try std.testing.expect(OpType.fromString("Or") != null);
    try std.testing.expect(OpType.fromString("Xor") != null);
    try std.testing.expect(OpType.fromString("Mod") != null);
    try std.testing.expect(OpType.fromString("IsNaN") != null);
    try std.testing.expect(OpType.fromString("Size") != null);
    try std.testing.expect(OpType.fromString("ArgMax") != null);
    try std.testing.expect(OpType.fromString("ArgMin") != null);
    try std.testing.expect(OpType.fromString("GatherND") != null);
    try std.testing.expect(OpType.fromString("Einsum") != null);
    try std.testing.expect(OpType.fromString("ConvTranspose") != null);
    try std.testing.expect(OpType.fromString("Resize") != null);
}

test "convertNode HardSwish" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const node = NodeProto{ .op_type = "HardSwish" };
    const result = try convertNode(allocator, &b, &node, &.{x}, null);
    try std.testing.expect(result != null_node);
    // HardSwish ends with mul (x * ...)
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .mul);
}

test "convertNode RotaryEmbedding" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // input [B=1, S=4, H=2, D=8], cos/sin [4, 4] (half_d=4)
    const x = try b.parameter("x", Shape.init(.f32, &.{ 1, 4, 2, 8 }));
    const pos_ids = try b.parameter("pos", Shape.init(.f32, &.{ 1, 4 }));
    const cos_cache = try b.parameter("cos", Shape.init(.f32, &.{ 4, 4 }));
    const sin_cache = try b.parameter("sin", Shape.init(.f32, &.{ 4, 4 }));
    const node = NodeProto{ .op_type = "RotaryEmbedding" };
    const result = try convertNode(allocator, &b, &node, &.{ x, pos_ids, cos_cache, sin_cache }, null);
    const out_shape = g.node(result).output_shape;
    // Output shape should match input
    try std.testing.expectEqual(@as(u8, 4), out_shape.rank());
    try std.testing.expectEqual(@as(i64, 1), out_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 4), out_shape.dim(1));
    try std.testing.expectEqual(@as(i64, 2), out_shape.dim(2));
    try std.testing.expectEqual(@as(i64, 8), out_shape.dim(3));
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .concat_prim);
}

test "convertNode logical And" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const y = try b.parameter("y", Shape.init(.f32, &.{ 2, 4 }));
    const node = NodeProto{ .op_type = "And" };
    const result = try convertNode(allocator, &b, &node, &.{ x, y }, null);
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .mul);
}

test "convertNode logical Or" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const y = try b.parameter("y", Shape.init(.f32, &.{ 2, 4 }));
    const node = NodeProto{ .op_type = "Or" };
    const result = try convertNode(allocator, &b, &node, &.{ x, y }, null);
    // OR = a + b - a*b → ends with sub
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .sub);
}

test "convertNode Size" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 3, 4 }));
    const node = NodeProto{ .op_type = "Size" };
    const result = try convertNode(allocator, &b, &node, &.{x}, null);
    const out_shape = g.node(result).output_shape;
    try std.testing.expectEqual(@as(u8, 1), out_shape.rank());
    // Should be a constant with value 24
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .constant);
}

test "convertNode Einsum matmul" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 3 }));
    const y = try b.parameter("y", Shape.init(.f32, &.{ 3, 4 }));
    var attrs = [_]AttributeProto{
        .{ .name = "equation", .s = "ij,jk->ik" },
    };
    const node = NodeProto{ .op_type = "Einsum", .attributes = &attrs };
    const result = try convertNode(allocator, &b, &node, &.{ x, y }, null);
    const out_shape = g.node(result).output_shape;
    try std.testing.expectEqual(@as(u8, 2), out_shape.rank());
    try std.testing.expectEqual(@as(i64, 2), out_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 4), out_shape.dim(1));
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .dot_general);
}

test "convertNode Einsum batch matmul" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 4, 2, 3 }));
    const y = try b.parameter("y", Shape.init(.f32, &.{ 4, 3, 5 }));
    var attrs = [_]AttributeProto{
        .{ .name = "equation", .s = "bij,bjk->bik" },
    };
    const node = NodeProto{ .op_type = "Einsum", .attributes = &attrs };
    const result = try convertNode(allocator, &b, &node, &.{ x, y }, null);
    const out_shape = g.node(result).output_shape;
    try std.testing.expectEqual(@as(u8, 3), out_shape.rank());
    try std.testing.expectEqual(@as(i64, 4), out_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 2), out_shape.dim(1));
    try std.testing.expectEqual(@as(i64, 5), out_shape.dim(2));
}

test "convertNode Resize no-op without scales" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 1, 3, 8, 8 }));
    const node = NodeProto{ .op_type = "Resize" };
    const result = try convertNode(allocator, &b, &node, &.{x}, null);
    try std.testing.expectEqual(x, result);
}

test "convertNode Resize cubic align_corners lowers to weighted gathers" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ -1, 1, 4, 4 }));
    const roi = try b.tensorConst(&.{}, Shape.init(.f32, &.{0}));
    const scales_empty = try b.tensorConst(&.{}, Shape.init(.f32, &.{0}));
    const sizes = try b.tensorConst(&.{ -1.0, 1.0, 8.0, 4.0 }, Shape.init(.f32, &.{4}));
    var attrs = [_]AttributeProto{
        .{ .name = "mode", .s = "cubic" },
        .{ .name = "coordinate_transformation_mode", .s = "align_corners" },
        .{ .name = "cubic_coeff_a", .f = -0.75 },
    };
    const node = NodeProto{ .op_type = "Resize", .attributes = &attrs };
    const result = try convertNode(allocator, &b, &node, &.{ x, roi, scales_empty, sizes }, null);
    const out = g.node(result).output_shape;
    try std.testing.expectEqual(@as(i64, -1), out.dim(0));
    try std.testing.expectEqual(@as(i64, 1), out.dim(1));
    try std.testing.expectEqual(@as(i64, 8), out.dim(2));
    try std.testing.expectEqual(@as(i64, 4), out.dim(3));
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .add);
}

test "convertNode Resize nearest 2x with scales" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 1, 1, 2, 3 }));
    // roi (empty), scales = [1, 1, 2, 2]
    const roi = try b.tensorConst(&.{}, Shape.init(.f32, &.{0}));
    const scales = try b.tensorConst(&.{ 1.0, 1.0, 2.0, 2.0 }, Shape.init(.f32, &.{4}));
    const node = NodeProto{ .op_type = "Resize" };
    const result = try convertNode(allocator, &b, &node, &.{ x, roi, scales }, null);
    // Output shape should be [1, 1, 4, 6]
    const out = g.node(result).output_shape;
    try std.testing.expectEqual(@as(i64, 1), out.dim(0));
    try std.testing.expectEqual(@as(i64, 1), out.dim(1));
    try std.testing.expectEqual(@as(i64, 4), out.dim(2));
    try std.testing.expectEqual(@as(i64, 6), out.dim(3));
}

test "convertNode Resize nearest with output sizes" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 1, 1, 4, 4 }));
    const roi = try b.tensorConst(&.{}, Shape.init(.f32, &.{0}));
    const scales_empty = try b.tensorConst(&.{}, Shape.init(.f32, &.{0}));
    // Output sizes [1, 1, 8, 8] (2x upscale)
    const sizes = try b.tensorConst(&.{ 1.0, 1.0, 8.0, 8.0 }, Shape.init(.f32, &.{4}));
    const node = NodeProto{ .op_type = "Resize" };
    const result = try convertNode(allocator, &b, &node, &.{ x, roi, scales_empty, sizes }, null);
    const out = g.node(result).output_shape;
    try std.testing.expectEqual(@as(i64, 8), out.dim(2));
    try std.testing.expectEqual(@as(i64, 8), out.dim(3));
}

test "convertNode ArgMax" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    var attrs = [_]AttributeProto{
        .{ .name = "axis", .i = 1 },
    };
    const node = NodeProto{ .op_type = "ArgMax", .attributes = &attrs };
    const result = try convertNode(allocator, &b, &node, &.{x}, null);
    try std.testing.expect(result != null_node);
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .argmax);
    try std.testing.expectEqual(DType.i64, g.node(result).output_shape.dtype);
    try std.testing.expectEqual(@as(i64, 1), g.node(result).output_shape.dim(1));
}

// ── Coverage Tests: Shape Manipulation ──────────────────────────────

test "convertNode Reshape with -1 dim" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 3, 4 }));
    const shape_const = try b.tensorConst(&.{ 6.0, -1.0 }, Shape.init(.f32, &.{2}));
    const node = NodeProto{ .op_type = "Reshape" };
    const result = try convertNode(allocator, &b, &node, &.{ x, shape_const }, null);
    const out_shape = g.node(result).output_shape;
    try std.testing.expectEqual(@as(u8, 2), out_shape.rank());
    try std.testing.expectEqual(@as(i64, 6), out_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 4), out_shape.dim(1));
}

test "convertNode Reshape preserves symbolic copied dim in ambiguous target" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 1, -1 }));
    const shape_node = NodeProto{ .op_type = "Shape" };
    const x_shape = try convertNode(allocator, &b, &shape_node, &.{x}, null);

    const seq_idx = try b.scalarConst(.f32, 1.0);
    const gather_node = NodeProto{ .op_type = "Gather" };
    const seq_dim = try convertNode(allocator, &b, &gather_node, &.{ x_shape, seq_idx }, null);
    const infer_dim = try b.scalarConst(.f32, -1.0);

    const concat_node = NodeProto{ .op_type = "Concat" };
    const target = try convertNode(allocator, &b, &concat_node, &.{ infer_dim, seq_dim }, null);

    const reshape_node = NodeProto{ .op_type = "Reshape" };
    const result = try convertNode(allocator, &b, &reshape_node, &.{ x, target }, null);
    const out_shape = g.node(result).output_shape;
    try std.testing.expectEqual(@as(u8, 2), out_shape.rank());
    try std.testing.expectEqual(@as(i64, 1), out_shape.dim(0));
    try std.testing.expectEqual(@as(i64, -1), out_shape.dim(1));
}

test "convertNode Reshape keeps fully symbolic ambiguous target dynamic" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ -1, -1 }));
    const shape_node = NodeProto{ .op_type = "Shape" };
    const x_shape = try convertNode(allocator, &b, &shape_node, &.{x}, null);

    const seq_idx = try b.scalarConst(.f32, 1.0);
    const gather_node = NodeProto{ .op_type = "Gather" };
    const seq_dim = try convertNode(allocator, &b, &gather_node, &.{ x_shape, seq_idx }, null);
    const infer_dim = try b.scalarConst(.f32, -1.0);

    const concat_node = NodeProto{ .op_type = "Concat" };
    const target = try convertNode(allocator, &b, &concat_node, &.{ infer_dim, seq_dim }, null);

    const reshape_node = NodeProto{ .op_type = "Reshape" };
    const result = try convertNode(allocator, &b, &reshape_node, &.{ x, target }, null);
    const out_shape = g.node(result).output_shape;
    try std.testing.expectEqual(@as(u8, 2), out_shape.rank());
    try std.testing.expectEqual(@as(i64, -1), out_shape.dim(0));
    try std.testing.expectEqual(@as(i64, -1), out_shape.dim(1));
}

test "convertNode Transpose with perm" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 3, 4 }));
    var perm = [_]i64{ 2, 0, 1 };
    var attrs = [_]AttributeProto{
        .{ .name = "perm", .ints = &perm },
    };
    const node = NodeProto{ .op_type = "Transpose", .attributes = &attrs };
    const result = try convertNode(allocator, &b, &node, &.{x}, null);
    const out_shape = g.node(result).output_shape;
    try std.testing.expectEqual(@as(i64, 4), out_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 2), out_shape.dim(1));
    try std.testing.expectEqual(@as(i64, 3), out_shape.dim(2));
}

test "convertNode Transpose default reversal" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 3 }));
    const node = NodeProto{ .op_type = "Transpose" };
    const result = try convertNode(allocator, &b, &node, &.{x}, null);
    const out_shape = g.node(result).output_shape;
    try std.testing.expectEqual(@as(i64, 3), out_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 2), out_shape.dim(1));
}

test "convertNode Squeeze axes attr" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 1, 3, 1, 4 }));
    var axes = [_]i64{ 0, 2 };
    var attrs = [_]AttributeProto{
        .{ .name = "axes", .ints = &axes },
    };
    const node = NodeProto{ .op_type = "Squeeze", .attributes = &attrs };
    const result = try convertNode(allocator, &b, &node, &.{x}, null);
    const out_shape = g.node(result).output_shape;
    try std.testing.expectEqual(@as(u8, 2), out_shape.rank());
    try std.testing.expectEqual(@as(i64, 3), out_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 4), out_shape.dim(1));
}

test "convertNode Unsqueeze via constant input" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 3, 4 }));
    const axes_const = try b.tensorConst(&.{ 0.0, 2.0 }, Shape.init(.f32, &.{2}));
    const node = NodeProto{ .op_type = "Unsqueeze" };
    const result = try convertNode(allocator, &b, &node, &.{ x, axes_const }, null);
    const out_shape = g.node(result).output_shape;
    try std.testing.expectEqual(@as(u8, 4), out_shape.rank());
    try std.testing.expectEqual(@as(i64, 1), out_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 3), out_shape.dim(1));
    try std.testing.expectEqual(@as(i64, 1), out_shape.dim(2));
    try std.testing.expectEqual(@as(i64, 4), out_shape.dim(3));
}

test "convertNode Flatten" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 3, 4 }));
    var attrs = [_]AttributeProto{
        .{ .name = "axis", .i = 2 },
    };
    const node = NodeProto{ .op_type = "Flatten", .attributes = &attrs };
    const result = try convertNode(allocator, &b, &node, &.{x}, null);
    const out_shape = g.node(result).output_shape;
    try std.testing.expectEqual(@as(u8, 2), out_shape.rank());
    try std.testing.expectEqual(@as(i64, 6), out_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 4), out_shape.dim(1));
}

test "convertNode Expand" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 1, 4 }));
    const target = try b.tensorConst(&.{ 3.0, 4.0 }, Shape.init(.f32, &.{2}));
    const node = NodeProto{ .op_type = "Expand" };
    const result = try convertNode(allocator, &b, &node, &.{ x, target }, null);
    const out_shape = g.node(result).output_shape;
    try std.testing.expectEqual(@as(i64, 3), out_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 4), out_shape.dim(1));
}

// ── Coverage Tests: Reductions ──────────────────────────────────────

test "convertNode ReduceSum" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 3, 4 }));
    var axes = [_]i64{1};
    var attrs = [_]AttributeProto{
        .{ .name = "axes", .ints = &axes },
    };
    const node = NodeProto{ .op_type = "ReduceSum", .attributes = &attrs };
    const result = try convertNode(allocator, &b, &node, &.{x}, null);
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .reduce_sum);
}

test "convertNode ReduceMean" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 3, 4 }));
    var axes = [_]i64{-1};
    var attrs = [_]AttributeProto{
        .{ .name = "axes", .ints = &axes },
    };
    const node = NodeProto{ .op_type = "ReduceMean", .attributes = &attrs };
    const result = try convertNode(allocator, &b, &node, &.{x}, null);
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .reduce_mean);
}

// ── Coverage Tests: Compound Ops ────────────────────────────────────

test "convertNode Gemm with transB" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const a = try b.parameter("a", Shape.init(.f32, &.{ 2, 3 }));
    const bm = try b.parameter("b", Shape.init(.f32, &.{ 4, 3 }));
    const c = try b.parameter("c", Shape.init(.f32, &.{4}));
    var attrs = [_]AttributeProto{
        .{ .name = "transB", .i = 1 },
    };
    const node = NodeProto{ .op_type = "Gemm", .attributes = &attrs };
    const result = try convertNode(allocator, &b, &node, &.{ a, bm, c }, null);
    // Ends with add (bias)
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .add);
}

test "convertNode Constant value_float" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    var attrs = [_]AttributeProto{
        .{ .name = "value_float", .f = 3.14 },
    };
    const node = NodeProto{ .op_type = "Constant", .attributes = &attrs };
    const result = try convertNode(allocator, &b, &node, &.{}, null);
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .constant);
}

test "convertNode Pow x^2 optimized" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const two = try b.scalarConst(.f32, 2.0);
    const node = NodeProto{ .op_type = "Pow" };
    const result = try convertNode(allocator, &b, &node, &.{ x, two }, null);
    // x^2 → x * x
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .mul);
}

test "convertNode Pow x^2 optimized with i64 exponent" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const two = try b.scalarConst(.i64, 2.0);
    const node = NodeProto{ .op_type = "Pow" };
    const result = try convertNode(allocator, &b, &node, &.{ x, two }, null);
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .mul);
}

test "convertNode Pow x^0.5 optimized" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const half = try b.scalarConst(.f32, 0.5);
    const node = NodeProto{ .op_type = "Pow" };
    const result = try convertNode(allocator, &b, &node, &.{ x, half }, null);
    // x^0.5 → sqrt(x)
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .sqrt);
}

test "convertNode Clip" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const min_val = try b.scalarConst(.f32, 0.0);
    const max_val = try b.scalarConst(.f32, 6.0);
    const node = NodeProto{ .op_type = "Clip" };
    const result = try convertNode(allocator, &b, &node, &.{ x, min_val, max_val }, null);
    try std.testing.expect(result != null_node);
    // Clip ends with where_select
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .where_select);
}

// ── Coverage Tests: Data Movement ───────────────────────────────────

test "convertNode Concat" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 3 }));
    const y = try b.parameter("y", Shape.init(.f32, &.{ 2, 5 }));
    var attrs = [_]AttributeProto{
        .{ .name = "axis", .i = 1 },
    };
    const node = NodeProto{ .op_type = "Concat", .attributes = &attrs };
    const result = try convertNode(allocator, &b, &node, &.{ x, y }, null);
    const out_shape = g.node(result).output_shape;
    try std.testing.expectEqual(@as(i64, 2), out_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 8), out_shape.dim(1));
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .concat_prim);
}

test "convertNode Concat promotes scalar shape constants to 1D" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.scalarConst(.i64, 1.0);
    const y = try b.scalarConst(.i64, 77.0);
    const node = NodeProto{ .op_type = "Concat" };
    const result = try convertNode(allocator, &b, &node, &.{ x, y }, null);
    const out_shape = g.node(result).output_shape;
    try std.testing.expectEqual(@as(u8, 1), out_shape.rank());
    try std.testing.expectEqual(@as(i64, 2), out_shape.dim(0));

    var buf: [4]f32 = undefined;
    const values = materializeConstantValues(&b, result, &buf) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualSlices(f32, &.{ 1.0, 77.0 }, values);
}

test "materializeConstantValues preserves unknown shape arithmetic in reshape targets" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("input_features", Shape.init(.f32, &.{ -1, 1, -1, 64 }));

    var shape_attrs = [_]AttributeProto{
        .{ .name = "start", .i = 0 },
        .{ .name = "end", .i = 1 },
    };
    const shape_node = NodeProto{ .op_type = "Shape", .attributes = &shape_attrs };
    const batch_shape = try convertNode(allocator, &b, &shape_node, &.{x}, null);

    const squeeze_node = NodeProto{ .op_type = "Squeeze" };
    const batch_scalar = try convertNode(allocator, &b, &squeeze_node, &.{batch_shape}, null);

    const sixty_four = try b.scalarConst(.i64, 64.0);
    const mul_node = NodeProto{ .op_type = "Mul" };
    const batch_times_sixty_four = try convertNode(allocator, &b, &mul_node, &.{ sixty_four, batch_scalar }, null);

    const infer_dim = try b.tensorConst(&.{-1.0}, Shape.init(.i64, &.{1}));
    const reshape_node = NodeProto{ .op_type = "Reshape" };
    const leading_dim = try convertNode(allocator, &b, &reshape_node, &.{ batch_times_sixty_four, infer_dim }, null);

    const seq_dim = try b.tensorConst(&.{64.0}, Shape.init(.i64, &.{1}));
    const head_dim = try b.tensorConst(&.{32.0}, Shape.init(.i64, &.{1}));
    const concat_node = NodeProto{ .op_type = "Concat" };
    const target_shape = try convertNode(allocator, &b, &concat_node, &.{ leading_dim, seq_dim, infer_dim, head_dim }, null);

    var buf: [8]f32 = undefined;
    const values = materializeConstantValues(&b, target_shape, &buf) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualSlices(f32, &.{ -1.0, 64.0, -1.0, 32.0 }, values);
}

test "convertNode Reshape preserves prepended inferred axis instead of copying input batch" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("audio_view", Shape.init(.f32, &.{ 64, 4, 64, 64 }));
    const target = try b.tensorConst(&.{ -1.0, 64.0, 4.0, 64.0, 64.0 }, Shape.init(.i64, &.{5}));
    const node = NodeProto{ .op_type = "Reshape", .name = "prepend_axis" };
    const result = try convertNode(allocator, &b, &node, &.{ x, target }, null);
    const out_shape = g.node(result).output_shape;
    try std.testing.expectEqual(@as(u8, 5), out_shape.rank());
    try std.testing.expectEqual(@as(i64, 1), out_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 64), out_shape.dim(1));
    try std.testing.expectEqual(@as(i64, 4), out_shape.dim(2));
    try std.testing.expectEqual(@as(i64, 64), out_shape.dim(3));
    try std.testing.expectEqual(@as(i64, 64), out_shape.dim(4));
}

test "convertNode Reshape still infers inserted split axis for attention heads" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("attn_hidden", Shape.init(.f32, &.{ 1, 77, 512 }));
    const target = try b.tensorConst(&.{ 1.0, 77.0, -1.0, 64.0 }, Shape.init(.i64, &.{4}));
    const node = NodeProto{ .op_type = "Reshape", .name = "split_heads" };
    const result = try convertNode(allocator, &b, &node, &.{ x, target }, null);
    const out_shape = g.node(result).output_shape;
    try std.testing.expectEqual(@as(u8, 4), out_shape.rank());
    try std.testing.expectEqual(@as(i64, 1), out_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 77), out_shape.dim(1));
    try std.testing.expectEqual(@as(i64, 8), out_shape.dim(2));
    try std.testing.expectEqual(@as(i64, 64), out_shape.dim(3));
}

test "convertNode Reshape infers split axis when target has two symbolic dims" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("audio_heads", Shape.init(.f32, &.{ 64, 64, 128 }));
    const target = try b.tensorConst(&.{ -1.0, 64.0, -1.0, 32.0 }, Shape.init(.i64, &.{4}));
    const node = NodeProto{ .op_type = "Reshape", .name = "audio_split_heads" };
    const result = try convertNode(allocator, &b, &node, &.{ x, target }, null);
    const out_shape = g.node(result).output_shape;
    try std.testing.expectEqual(@as(u8, 4), out_shape.rank());
    try std.testing.expectEqual(@as(i64, 64), out_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 64), out_shape.dim(1));
    try std.testing.expectEqual(@as(i64, 4), out_shape.dim(2));
    try std.testing.expectEqual(@as(i64, 32), out_shape.dim(3));
}

test "convertNode Slice" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 4, 8 }));
    const starts = try b.tensorConst(&.{ 1.0, 2.0 }, Shape.init(.f32, &.{2}));
    const ends = try b.tensorConst(&.{ 3.0, 6.0 }, Shape.init(.f32, &.{2}));
    const node = NodeProto{ .op_type = "Slice" };
    const result = try convertNode(allocator, &b, &node, &.{ x, starts, ends }, null);
    const out_shape = g.node(result).output_shape;
    try std.testing.expectEqual(@as(i64, 2), out_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 4), out_shape.dim(1));
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .slice);
}

test "convertNode Slice preserves full-range -1 prefix end" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.i64, &.{ 1, 77 }));
    const starts = try b.tensorConst(&.{ 0.0, 0.0 }, Shape.init(.f32, &.{2}));
    const ends = try b.tensorConst(&.{ 1.0, -1.0 }, Shape.init(.f32, &.{2}));
    const node = NodeProto{ .op_type = "Slice" };
    const result = try convertNode(allocator, &b, &node, &.{ x, starts, ends }, null);
    const out_shape = g.node(result).output_shape;
    try std.testing.expectEqual(@as(i64, 1), out_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 77), out_shape.dim(1));
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .slice);
}

// ── Coverage Tests: Comparison & Logical ────────────────────────────

test "convertNode Where" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const cond = try b.parameter("cond", Shape.init(.f32, &.{ 2, 4 }));
    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const y = try b.parameter("y", Shape.init(.f32, &.{ 2, 4 }));
    const node = NodeProto{ .op_type = "Where" };
    const result = try convertNode(allocator, &b, &node, &.{ cond, x, y }, null);
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .where_select);
    const out_shape = g.node(result).output_shape;
    try std.testing.expectEqual(@as(u8, 2), out_shape.rank());
    try std.testing.expectEqual(@as(i64, 2), out_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 4), out_shape.dim(1));
}

test "convertNode Where broadcasts scalar branch" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const cond = try b.parameter("cond", Shape.init(.bool_, &.{ 2, 3, 4 }));
    const scalar = try b.scalarConst(.f32, 0.0);
    const tensor = try b.parameter("tensor", Shape.init(.f32, &.{ 2, 3, 4 }));
    const node = NodeProto{ .op_type = "Where" };
    const result = try convertNode(allocator, &b, &node, &.{ cond, scalar, tensor }, null);
    const out_shape = g.node(result).output_shape;
    try std.testing.expectEqual(@as(u8, 3), out_shape.rank());
    try std.testing.expectEqual(@as(i64, 2), out_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 3), out_shape.dim(1));
    try std.testing.expectEqual(@as(i64, 4), out_shape.dim(2));
}

test "materializeConstantValues folds broadcasted equal/where shape masks" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const shape_vals = try b.tensorConst(&.{ 2.0, -1.0, 4.0 }, Shape.init(.i64, &.{3}));
    const minus_one = try b.scalarConst(.i64, -1.0);
    const equal_node = NodeProto{ .op_type = "Equal" };
    const equal = try convertNode(allocator, &b, &equal_node, &.{ shape_vals, minus_one }, null);

    const zeros = try b.tensorConst(&.{ 0.0, 0.0, 0.0 }, Shape.init(.i64, &.{3}));
    const where_node = NodeProto{ .op_type = "Where" };
    const masked = try convertNode(allocator, &b, &where_node, &.{ equal, zeros, shape_vals }, null);

    var buf: [8]f32 = undefined;
    const values = materializeConstantValues(&b, masked, &buf) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualSlices(f32, &.{ 2.0, 0.0, 4.0 }, values);
}

test "convertNode Equal" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const y = try b.parameter("y", Shape.init(.f32, &.{ 2, 4 }));
    const node = NodeProto{ .op_type = "Equal" };
    const result = try convertNode(allocator, &b, &node, &.{ x, y }, null);
    // Equal → ends with where_select
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .where_select);
}

test "convertNode Not" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const node = NodeProto{ .op_type = "Not" };
    const result = try convertNode(allocator, &b, &node, &.{x}, null);
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .where_select);
}

// ── Coverage Tests: Activations & Normalization ─────────────────────

test "convertNode Sigmoid" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const node = NodeProto{ .op_type = "Sigmoid" };
    const result = try convertNode(allocator, &b, &node, &.{x}, null);
    // 1/(1+exp(-x)) → div
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .div);
}

test "convertNode LeakyRelu" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    var attrs = [_]AttributeProto{
        .{ .name = "alpha", .f = 0.01 },
    };
    const node = NodeProto{ .op_type = "LeakyRelu", .attributes = &attrs };
    const result = try convertNode(allocator, &b, &node, &.{x}, null);
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .where_select);
}

test "convertNode BatchNormalization" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 3, 4 }));
    const scale = try b.parameter("scale", Shape.init(.f32, &.{3}));
    const bias = try b.parameter("bias", Shape.init(.f32, &.{3}));
    const mean = try b.parameter("mean", Shape.init(.f32, &.{3}));
    const variance = try b.parameter("var", Shape.init(.f32, &.{3}));
    const node = NodeProto{ .op_type = "BatchNormalization" };
    const result = try convertNode(allocator, &b, &node, &.{ x, scale, bias, mean, variance }, null);
    // scale*(x-mean)/sqrt(var+eps)+bias → ends with add
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .add);
    // Output must preserve X's full shape (channel-broadcast correctness).
    const out_shape = g.node(result).output_shape;
    try std.testing.expectEqual(@as(u8, 3), out_shape.rank());
    try std.testing.expectEqual(@as(i64, 2), out_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 3), out_shape.dim(1));
    try std.testing.expectEqual(@as(i64, 4), out_shape.dim(2));
}

test "convertNode BatchNormalization 4D NCHW" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // X: [N=2, C=3, H=5, W=5]
    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 3, 5, 5 }));
    const scale = try b.parameter("scale", Shape.init(.f32, &.{3}));
    const bias = try b.parameter("bias", Shape.init(.f32, &.{3}));
    const mean = try b.parameter("mean", Shape.init(.f32, &.{3}));
    const variance = try b.parameter("var", Shape.init(.f32, &.{3}));
    const node = NodeProto{ .op_type = "BatchNormalization" };
    const result = try convertNode(allocator, &b, &node, &.{ x, scale, bias, mean, variance }, null);
    const out_shape = g.node(result).output_shape;
    try std.testing.expectEqual(@as(u8, 4), out_shape.rank());
    try std.testing.expectEqual(@as(i64, 2), out_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 3), out_shape.dim(1));
    try std.testing.expectEqual(@as(i64, 5), out_shape.dim(2));
    try std.testing.expectEqual(@as(i64, 5), out_shape.dim(3));
}

test "convertNode Cast" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    var attrs = [_]AttributeProto{
        .{ .name = "to", .i = 10 }, // ONNX FLOAT16
    };
    const node = NodeProto{ .op_type = "Cast", .attributes = &attrs };
    const result = try convertNode(allocator, &b, &node, &.{x}, null);
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .convert_dtype);
    try std.testing.expectEqual(DType.f16, g.node(result).output_shape.dtype);
}

// ── Coverage Tests: Broadcasting ────────────────────────────────────

test "convertNode broadcast Mul different ranks" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 3, 4 }));
    const y = try b.parameter("y", Shape.init(.f32, &.{ 1, 4 }));
    const node = NodeProto{ .op_type = "Mul" };
    const result = try convertNode(allocator, &b, &node, &.{ x, y }, null);
    try std.testing.expect(result != null_node);
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .mul);
}

test "convertNode broadcast Sub scalar" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const s = try b.scalarConst(.f32, 1.0);
    const node = NodeProto{ .op_type = "Sub" };
    const result = try convertNode(allocator, &b, &node, &.{ x, s }, null);
    try std.testing.expect(result != null_node);
}

// ── Coverage Tests: Error Handling ──────────────────────────────────

test "convertNode Concat missing inputs" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    var attrs = [_]AttributeProto{
        .{ .name = "axis", .i = 0 },
    };
    const node = NodeProto{ .op_type = "Concat", .attributes = &attrs };
    try std.testing.expectError(error.MissingInput, convertNode(allocator, &b, &node, &.{}, null));
}

test "convertNode Reshape non-constant shape" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const shape_param = try b.parameter("shape", Shape.init(.f32, &.{2}));
    const node = NodeProto{ .op_type = "Reshape" };
    try std.testing.expectError(error.ConstantMaterializationFailed, convertNode(allocator, &b, &node, &.{ x, shape_param }, null));
}

// ── Coverage Tests: Miscellaneous ───────────────────────────────────

test "convertNode Reciprocal" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const node = NodeProto{ .op_type = "Reciprocal" };
    const result = try convertNode(allocator, &b, &node, &.{x}, null);
    // 1/x → div
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .div);
}

test "convertNode Sign" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const node = NodeProto{ .op_type = "Sign" };
    const result = try convertNode(allocator, &b, &node, &.{x}, null);
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .where_select);
}

test "convertNode HardSigmoid" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const node = NodeProto{ .op_type = "HardSigmoid" };
    const result = try convertNode(allocator, &b, &node, &.{x}, null);
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .where_select);
}

test "convertNode Gelu emits fused" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const node = NodeProto{ .op_type = "Gelu" };
    const result = try convertNode(allocator, &b, &node, &.{x}, null);
    try std.testing.expect(g.node(result).op.isFused());
}

test "convertNode Softmax emits fused" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const node = NodeProto{ .op_type = "Softmax" };
    const result = try convertNode(allocator, &b, &node, &.{x}, null);
    try std.testing.expect(g.node(result).op.isFused());
}

test "convertNode 2D MatMul" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 3 }));
    const y = try b.parameter("y", Shape.init(.f32, &.{ 3, 4 }));
    const node = NodeProto{ .op_type = "MatMul" };
    const result = try convertNode(allocator, &b, &node, &.{ x, y }, null);
    const out_shape = g.node(result).output_shape;
    try std.testing.expectEqual(@as(i64, 2), out_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 4), out_shape.dim(1));
    // 2D uses builder.matmul → dot_general
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .dot_general);
}

test "convertNode Split first output" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 6, 4 }));
    var split_sizes = [_]i64{ 2, 4 };
    var attrs = [_]AttributeProto{
        .{ .name = "axis", .i = 0 },
        .{ .name = "split", .ints = &split_sizes },
    };
    var outputs = [_][]const u8{ "out1", "out2" };
    const node = NodeProto{ .op_type = "Split", .attributes = &attrs, .outputs = &outputs };
    var extra: [1]NodeId = .{null_node};
    const result = try convertNode(allocator, &b, &node, &.{x}, &extra);
    // First output: [2, 4]
    const out_shape = g.node(result).output_shape;
    try std.testing.expectEqual(@as(i64, 2), out_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 4), out_shape.dim(1));
    // Second output: [4, 4]
    try std.testing.expect(extra[0] != null_node);
    const out2_shape = g.node(extra[0]).output_shape;
    try std.testing.expectEqual(@as(i64, 4), out2_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 4), out2_shape.dim(1));
}

test "convertNode Split equal 3-way" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 12, 4 }));
    var attrs = [_]AttributeProto{
        .{ .name = "axis", .i = 0 },
    };
    var outputs = [_][]const u8{ "a", "b", "c" };
    const node = NodeProto{ .op_type = "Split", .attributes = &attrs, .outputs = &outputs };
    _ = &attrs;
    var extra: [2]NodeId = .{null_node} ** 2;
    const result = try convertNode(allocator, &b, &node, &.{x}, &extra);
    // All three outputs should be [4, 4]
    try std.testing.expectEqual(@as(i64, 4), g.node(result).output_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 4), g.node(extra[0]).output_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 4), g.node(extra[1]).output_shape.dim(0));
    // All should be distinct slices
    try std.testing.expect(result != extra[0]);
    try std.testing.expect(extra[0] != extra[1]);
}

// ── Coverage Tests: Phase 4 ops ───────────────────────────────────

test "convertNode Mod fmod=1" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{4}));
    const y = try b.parameter("y", Shape.init(.f32, &.{4}));
    var attrs = [_]AttributeProto{
        .{ .name = "fmod", .i = 1 },
    };
    const node = NodeProto{ .op_type = "Mod", .attributes = &attrs };
    const result = try convertNode(allocator, &b, &node, &.{ x, y }, null);
    // fmod: a - trunc(a/b) * b → ends with sub
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .sub);
}

test "convertNode Mod floor (default)" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{4}));
    const y = try b.parameter("y", Shape.init(.f32, &.{4}));
    const node = NodeProto{ .op_type = "Mod" };
    const result = try convertNode(allocator, &b, &node, &.{ x, y }, null);
    // floor mod: a - floor(a/b) * b → ends with sub
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .sub);
}

test "convertNode ConvTranspose 1D" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // Input: [batch=1, C_in=4, L=8], Weight: [C_in=4, C_out=2, K=3]
    const x = try b.parameter("x", Shape.init(.f32, &.{ 1, 4, 8 }));
    const w = try b.parameter("w", Shape.init(.f32, &.{ 4, 2, 3 }));
    const node = NodeProto{ .op_type = "ConvTranspose" };
    const result = try convertNode(allocator, &b, &node, &.{ x, w }, null);
    const out_shape = g.node(result).output_shape;
    // out = (8-1)*1 + 3 = 10 (stride=1, no padding, no dilation)
    try std.testing.expectEqual(@as(i64, 1), out_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 2), out_shape.dim(1));
    try std.testing.expectEqual(@as(i64, 10), out_shape.dim(2));
}

test "convertNode ConvTranspose 2D with stride" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // Input: [1, 3, 4, 4], Weight: [3, 2, 3, 3], stride=2
    const x = try b.parameter("x", Shape.init(.f32, &.{ 1, 3, 4, 4 }));
    const w = try b.parameter("w", Shape.init(.f32, &.{ 3, 2, 3, 3 }));
    var strides = [_]i64{ 2, 2 };
    var attrs = [_]AttributeProto{
        .{ .name = "strides", .ints = &strides },
    };
    const node = NodeProto{ .op_type = "ConvTranspose", .attributes = &attrs };
    const result = try convertNode(allocator, &b, &node, &.{ x, w }, null);
    const out_shape = g.node(result).output_shape;
    // out = (4-1)*2 + 3 = 9
    try std.testing.expectEqual(@as(i64, 1), out_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 2), out_shape.dim(1));
    try std.testing.expectEqual(@as(i64, 9), out_shape.dim(2));
    try std.testing.expectEqual(@as(i64, 9), out_shape.dim(3));
}

test "convertNode ConvTranspose with bias" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 1, 4, 8 }));
    const w = try b.parameter("w", Shape.init(.f32, &.{ 4, 2, 3 }));
    const bias = try b.parameter("bias", Shape.init(.f32, &.{2}));
    const node = NodeProto{ .op_type = "ConvTranspose" };
    const result = try convertNode(allocator, &b, &node, &.{ x, w, bias }, null);
    // Result should be add (conv + bias)
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .add);
}

test "convertNode IsNaN produces mul (AND of two masks)" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 3 }));
    const node = NodeProto{ .op_type = "IsNaN" };
    const result = try convertNode(allocator, &b, &node, &.{x}, null);
    // IsNaN decomposes to mul(not_finite, not_inf)
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .mul);
    // Output shape matches input
    try std.testing.expectEqual(@as(u8, 2), g.node(result).output_shape.rank());
}

test "convertNode Resize with fractional scales uses gather" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 1, 1, 4, 4 }));
    const roi = try b.tensorConst(&.{}, Shape.init(.f32, &.{0}));
    // Non-integer scale factor → gather-based nearest-neighbor
    const scales = try b.tensorConst(&.{ 1.0, 1.0, 1.5, 1.5 }, Shape.init(.f32, &.{4}));
    const node = NodeProto{ .op_type = "Resize" };
    const result = try convertNode(allocator, &b, &node, &.{ x, roi, scales }, null);
    // Should produce a new node (not identity passthrough)
    try std.testing.expect(result != x);
    // Output shape should be [1, 1, 6, 6] (4 * 1.5 = 6)
    const out_shape = g.node(result).output_shape;
    try std.testing.expectEqual(@as(i64, 1), out_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 1), out_shape.dim(1));
    try std.testing.expectEqual(@as(i64, 6), out_shape.dim(2));
    try std.testing.expectEqual(@as(i64, 6), out_shape.dim(3));
    // Should use gather
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .gather);
}

test "convertNode Resize downscale with fractional scales" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 1, 3, 8, 8 }));
    const roi = try b.tensorConst(&.{}, Shape.init(.f32, &.{0}));
    // Downscale by 0.5 on spatial dims
    const scales = try b.tensorConst(&.{ 1.0, 1.0, 0.5, 0.5 }, Shape.init(.f32, &.{4}));
    const node = NodeProto{ .op_type = "Resize" };
    const result = try convertNode(allocator, &b, &node, &.{ x, roi, scales }, null);
    try std.testing.expect(result != x);
    const out_shape = g.node(result).output_shape;
    try std.testing.expectEqual(@as(i64, 1), out_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 3), out_shape.dim(1));
    try std.testing.expectEqual(@as(i64, 4), out_shape.dim(2));
    try std.testing.expectEqual(@as(i64, 4), out_shape.dim(3));
}

// ── Coverage Tests: Control Flow ──────────────────────────────────

test "convertNode If with constant true inlines then_branch" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // Constant true condition + a feed input
    const cond = try b.scalarConst(.f32, 1.0);
    const x = try b.parameter("x", Shape.init(.f32, &.{4}));

    // then_branch body: output = Neg(branch_input)
    // else_branch body: output = Identity(branch_input)
    var then_node_inp = [_][]const u8{"bi"};
    var then_node_out = [_][]const u8{"neg_out"};
    var then_nodes = [_]NodeProto{
        .{ .op_type = "Neg", .inputs = &then_node_inp, .outputs = &then_node_out },
    };
    var then_inputs = [_]proto.ValueInfoProto{
        .{ .name = "bi" },
    };
    var then_outputs = [_]proto.ValueInfoProto{
        .{ .name = "neg_out" },
    };
    _ = &then_nodes;
    _ = &then_inputs;
    _ = &then_outputs;
    const then_graph = proto.GraphProto{
        .name = "then",
        .nodes = &then_nodes,
        .inputs = &then_inputs,
        .outputs = &then_outputs,
    };

    var else_node_inp = [_][]const u8{"bi2"};
    var else_node_out = [_][]const u8{"id_out"};
    var else_nodes = [_]NodeProto{
        .{ .op_type = "Identity", .inputs = &else_node_inp, .outputs = &else_node_out },
    };
    var else_inputs = [_]proto.ValueInfoProto{
        .{ .name = "bi2" },
    };
    var else_outputs = [_]proto.ValueInfoProto{
        .{ .name = "id_out" },
    };
    _ = &else_nodes;
    _ = &else_inputs;
    _ = &else_outputs;
    const else_graph = proto.GraphProto{
        .name = "else",
        .nodes = &else_nodes,
        .inputs = &else_inputs,
        .outputs = &else_outputs,
    };

    var attrs = [_]AttributeProto{
        .{ .name = "then_branch", .g = then_graph, .attr_type = .graph },
        .{ .name = "else_branch", .g = else_graph, .attr_type = .graph },
    };
    _ = &attrs;
    const node = NodeProto{ .op_type = "If", .attributes = &attrs };

    // cond=true → should inline then_branch (Neg)
    const result = try convertNode(allocator, &b, &node, &.{ cond, x }, null);
    try std.testing.expect(result != cond);
    try std.testing.expect(result != x);
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .neg);
}

test "convertNode If with constant false inlines else_branch" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const cond = try b.scalarConst(.f32, 0.0);
    const x = try b.parameter("x", Shape.init(.f32, &.{4}));

    // then_branch: Neg
    var then_node_inp = [_][]const u8{"bi"};
    var then_node_out = [_][]const u8{"neg_out"};
    var then_nodes = [_]NodeProto{
        .{ .op_type = "Neg", .inputs = &then_node_inp, .outputs = &then_node_out },
    };
    var then_inputs = [_]proto.ValueInfoProto{.{ .name = "bi" }};
    var then_outputs = [_]proto.ValueInfoProto{.{ .name = "neg_out" }};
    _ = &then_nodes;
    _ = &then_inputs;
    _ = &then_outputs;
    const then_graph = proto.GraphProto{
        .name = "then",
        .nodes = &then_nodes,
        .inputs = &then_inputs,
        .outputs = &then_outputs,
    };

    // else_branch: Add(bi2, bi2) = 2*x
    var else_node_inp = [_][]const u8{ "bi2", "bi2" };
    var else_node_out = [_][]const u8{"add_out"};
    var else_nodes = [_]NodeProto{
        .{ .op_type = "Add", .inputs = &else_node_inp, .outputs = &else_node_out },
    };
    var else_inputs = [_]proto.ValueInfoProto{.{ .name = "bi2" }};
    var else_outputs = [_]proto.ValueInfoProto{.{ .name = "add_out" }};
    _ = &else_nodes;
    _ = &else_inputs;
    _ = &else_outputs;
    const else_graph = proto.GraphProto{
        .name = "else",
        .nodes = &else_nodes,
        .inputs = &else_inputs,
        .outputs = &else_outputs,
    };

    var attrs = [_]AttributeProto{
        .{ .name = "then_branch", .g = then_graph, .attr_type = .graph },
        .{ .name = "else_branch", .g = else_graph, .attr_type = .graph },
    };
    _ = &attrs;
    const node = NodeProto{ .op_type = "If", .attributes = &attrs };

    // cond=false → should inline else_branch (Add)
    const result = try convertNode(allocator, &b, &node, &.{ cond, x }, null);
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .add);
}

test "convertNode If with non-constant condition returns unsupported" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // Non-constant condition (parameter) → cannot statically resolve
    const cond = try b.parameter("cond", Shape.init(.f32, &.{1}));
    const node = NodeProto{ .op_type = "If" };
    try std.testing.expectError(error.UnsupportedOp, convertNode(allocator, &b, &node, &.{cond}, null));
}

test "convertNodeWithScope If body captures outer-scope value" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // Outer scope: "outer_val" maps to a parameter in the parent graph.
    const outer_val = try b.parameter("outer_val", Shape.init(.f32, &.{4}));
    var outer_map = std.StringHashMapUnmanaged(NodeId).empty;
    defer outer_map.deinit(allocator);
    try outer_map.put(allocator, "outer_val", outer_val);
    const outer = NameScope{ .map = &outer_map };

    // then_branch body: Neg(outer_val) — references outer scope implicitly
    // (outer_val is NOT declared as a sub-graph input).
    const cond = try b.scalarConst(.f32, 1.0);
    var then_node_inp = [_][]const u8{"outer_val"};
    var then_node_out = [_][]const u8{"neg_out"};
    var then_nodes = [_]NodeProto{
        .{ .op_type = "Neg", .inputs = &then_node_inp, .outputs = &then_node_out },
    };
    var then_outputs = [_]proto.ValueInfoProto{.{ .name = "neg_out" }};
    _ = &then_nodes;
    _ = &then_outputs;
    const then_graph = proto.GraphProto{
        .name = "then",
        .nodes = &then_nodes,
        .outputs = &then_outputs,
    };
    var else_outputs = [_]proto.ValueInfoProto{.{ .name = "outer_val" }};
    _ = &else_outputs;
    const else_graph = proto.GraphProto{
        .name = "else",
        .outputs = &else_outputs,
    };

    var attrs = [_]AttributeProto{
        .{ .name = "then_branch", .g = then_graph, .attr_type = .graph },
        .{ .name = "else_branch", .g = else_graph, .attr_type = .graph },
    };
    _ = &attrs;
    const node = NodeProto{ .op_type = "If", .attributes = &attrs };

    // Condition is true, so then_branch runs — Neg(outer_val)
    const result = try convertNodeWithScope(allocator, &b, &node, &.{cond}, null, &outer);
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .neg);
}

test "convertNodeWithScope Loop body captures outer-scope value" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // Outer scope: "k" is a constant the loop body wants to multiply by.
    const k = try b.scalarConst(.f32, 2.0);
    var outer_map = std.StringHashMapUnmanaged(NodeId).empty;
    defer outer_map.deinit(allocator);
    try outer_map.put(allocator, "k", k);
    const outer = NameScope{ .map = &outer_map };

    const trip = try b.scalarConst(.f32, 2.0);
    const cond = try b.scalarConst(.f32, 1.0);
    const init_val = try b.parameter("x", Shape.init(.f32, &.{4}));

    // Body: v_out = v_in * k where k is an outer-scope capture
    var body_node_inp = [_][]const u8{ "v_in", "k" };
    var body_node_out = [_][]const u8{"v_out"};
    var body_nodes = [_]NodeProto{
        .{ .op_type = "Mul", .inputs = &body_node_inp, .outputs = &body_node_out },
    };
    var body_inputs = [_]proto.ValueInfoProto{
        .{ .name = "i" },
        .{ .name = "cond_in" },
        .{ .name = "v_in" },
    };
    var body_outputs = [_]proto.ValueInfoProto{
        .{ .name = "cond_out" },
        .{ .name = "v_out" },
    };
    _ = &body_nodes;
    _ = &body_inputs;
    _ = &body_outputs;
    const body = proto.GraphProto{
        .name = "loop_body",
        .nodes = &body_nodes,
        .inputs = &body_inputs,
        .outputs = &body_outputs,
    };
    var attrs = [_]AttributeProto{
        .{ .name = "body", .g = body, .attr_type = .graph },
    };
    _ = &attrs;
    const node = NodeProto{ .op_type = "Loop", .attributes = &attrs };

    // 2 iterations, each multiplies by outer-scope k → x * k * k
    const result = try convertNodeWithScope(allocator, &b, &node, &.{ trip, cond, init_val }, null, &outer);
    try std.testing.expect(result != init_val);
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .mul);
}

test "convertNode Loop with missing inputs" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{4}));
    const node = NodeProto{ .op_type = "Loop" };
    // Loop needs at least 2 inputs (trip_count + condition)
    try std.testing.expectError(error.MissingInput, convertNode(allocator, &b, &node, &.{x}, null));
}

test "convertNode Loop with dynamic trip count returns unsupported" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // Non-constant trip count (parameter, not constant)
    const trip = try b.parameter("trip", Shape.init(.i32, &.{1}));
    const cond = try b.scalarConst(.f32, 1.0);
    const node = NodeProto{ .op_type = "Loop" };
    try std.testing.expectError(error.UnsupportedOp, convertNode(allocator, &b, &node, &.{ trip, cond }, null));
}

test "convertNode Loop unrolls constant iterations" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // Trip count = 3, initial condition = true
    const trip = try b.scalarConst(.f32, 3.0);
    const cond = try b.scalarConst(.f32, 1.0);
    const init_val = try b.parameter("x", Shape.init(.f32, &.{4}));

    // Body: v_out = v_in + v_in (doubles each iteration)
    // body inputs: [iter_num, cond_in, v_in]
    // body outputs: [cond_out, v_out]
    var body_node_inp = [_][]const u8{ "v_in", "v_in" };
    var body_node_out = [_][]const u8{"v_out"};
    var body_nodes = [_]NodeProto{
        .{ .op_type = "Add", .inputs = &body_node_inp, .outputs = &body_node_out },
    };
    var body_inputs = [_]proto.ValueInfoProto{
        .{ .name = "i" },
        .{ .name = "cond_in" },
        .{ .name = "v_in" },
    };
    var body_outputs = [_]proto.ValueInfoProto{
        .{ .name = "cond_out" },
        .{ .name = "v_out" },
    };
    _ = &body_nodes;
    _ = &body_inputs;
    _ = &body_outputs;
    const body = proto.GraphProto{
        .name = "loop_body",
        .nodes = &body_nodes,
        .inputs = &body_inputs,
        .outputs = &body_outputs,
    };
    var attrs = [_]AttributeProto{
        .{ .name = "body", .g = body, .attr_type = .graph },
    };
    const node = NodeProto{ .op_type = "Loop", .attributes = &attrs };
    _ = &attrs;

    // Should unroll 3 iterations: x+x → 2x, 2x+2x → 4x, 4x+4x → 8x
    const result = try convertNode(allocator, &b, &node, &.{ trip, cond, init_val }, null);
    // Result should be different from init_val (it's an Add node)
    try std.testing.expect(result != init_val);
    // Should be an Add operation (the last unrolled iteration)
    try std.testing.expect(std.meta.activeTag(g.node(result).op) == .add);
}

test "convertNode Scan returns unsupported without body" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{4}));
    const node = NodeProto{ .op_type = "Scan" };
    try std.testing.expectError(error.UnsupportedOp, convertNode(allocator, &b, &node, &.{x}, null));
}

test "convertNode Scan unrolls over sequence" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // Scan with 1 state variable and 1 scan input
    // state: running sum [4]
    // scan input: sequence [3, 4] (3 timesteps of 4-dim vectors)
    // body: state_out = state_in + slice
    const init_state = try b.parameter("state", Shape.init(.f32, &.{4}));
    const sequence = try b.parameter("seq", Shape.init(.f32, &.{ 3, 4 }));

    // Body sub-graph: Add(state_in, slice_in) → state_out
    var body_node_inp = [_][]const u8{ "st_in", "sl_in" };
    var body_node_out = [_][]const u8{"st_out"};
    var body_nodes = [_]NodeProto{
        .{ .op_type = "Add", .inputs = &body_node_inp, .outputs = &body_node_out },
    };
    var body_inputs = [_]proto.ValueInfoProto{
        .{ .name = "st_in" },
        .{ .name = "sl_in" },
    };
    var body_outputs = [_]proto.ValueInfoProto{
        .{ .name = "st_out" },
    };
    _ = &body_nodes;
    _ = &body_inputs;
    _ = &body_outputs;
    const body = proto.GraphProto{
        .name = "scan_body",
        .nodes = &body_nodes,
        .inputs = &body_inputs,
        .outputs = &body_outputs,
    };
    var attrs = [_]AttributeProto{
        .{ .name = "body", .g = body, .attr_type = .graph },
        .{ .name = "num_scan_inputs", .i = 1, .attr_type = .int },
    };
    const node = NodeProto{ .op_type = "Scan", .attributes = &attrs };
    _ = &attrs;

    // inputs: [init_state, sequence]  (N=1, M=1)
    const result = try convertNode(allocator, &b, &node, &.{ init_state, sequence }, null);
    // Should produce a result (the final state after 3 iterations of adding)
    try std.testing.expect(result != init_state);
    // Output shape should be [4] (final state)
    const out_shape = g.node(result).output_shape;
    try std.testing.expectEqual(@as(i64, 4), out_shape.dim(0));
    try std.testing.expectEqual(@as(u8, 1), out_shape.rank());
}

test "convertNode Scan with scan outputs accumulates" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // Scan with 1 state, 1 scan input, 1 scan output
    // Each step: state = state + slice, scan_out = state (accumulated)
    const init_state = try b.parameter("state", Shape.init(.f32, &.{4}));
    const sequence = try b.parameter("seq", Shape.init(.f32, &.{ 3, 4 }));

    // Body: state_out = Add(state_in, slice_in), scan_elem = Identity(state_out)
    var add_inp = [_][]const u8{ "st_in", "sl_in" };
    var add_out = [_][]const u8{"st_out"};
    var id_inp = [_][]const u8{"st_out"};
    var id_out = [_][]const u8{"scan_elem"};
    var body_nodes = [_]NodeProto{
        .{ .op_type = "Add", .inputs = &add_inp, .outputs = &add_out },
        .{ .op_type = "Identity", .inputs = &id_inp, .outputs = &id_out },
    };
    var body_inputs = [_]proto.ValueInfoProto{
        .{ .name = "st_in" },
        .{ .name = "sl_in" },
    };
    // 2 outputs: 1 state (st_out) + 1 scan output (scan_elem)
    var body_outputs = [_]proto.ValueInfoProto{
        .{ .name = "st_out" },
        .{ .name = "scan_elem" },
    };
    _ = &body_nodes;
    _ = &body_inputs;
    _ = &body_outputs;
    const body = proto.GraphProto{
        .name = "scan_body",
        .nodes = &body_nodes,
        .inputs = &body_inputs,
        .outputs = &body_outputs,
    };
    var attrs = [_]AttributeProto{
        .{ .name = "body", .g = body, .attr_type = .graph },
        .{ .name = "num_scan_inputs", .i = 1, .attr_type = .int },
    };
    const node = NodeProto{ .op_type = "Scan", .attributes = &attrs };
    _ = &attrs;

    // Result is first output (final state), but scan outputs are also built
    const result = try convertNode(allocator, &b, &node, &.{ init_state, sequence }, null);
    try std.testing.expect(result != init_state);
    // Final state shape: [4]
    const out_shape = g.node(result).output_shape;
    try std.testing.expectEqual(@as(u8, 1), out_shape.rank());
    try std.testing.expectEqual(@as(i64, 4), out_shape.dim(0));
}

test "convertNode Scan with scan_output_axes" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // Scan with 1 state, 1 scan input, 1 scan output accumulated on axis 1
    // Input sequence: [3, 4], body produces [4] elements, output accumulated along axis 1 → [4, 3]
    const init_state = try b.parameter("state", Shape.init(.f32, &.{4}));
    const sequence = try b.parameter("seq", Shape.init(.f32, &.{ 3, 4 }));

    var add_inp = [_][]const u8{ "st_in", "sl_in" };
    var add_out = [_][]const u8{"st_out"};
    var id_inp = [_][]const u8{"st_out"};
    var id_out = [_][]const u8{"scan_elem"};
    var body_nodes = [_]NodeProto{
        .{ .op_type = "Add", .inputs = &add_inp, .outputs = &add_out },
        .{ .op_type = "Identity", .inputs = &id_inp, .outputs = &id_out },
    };
    var body_inputs = [_]proto.ValueInfoProto{
        .{ .name = "st_in" },
        .{ .name = "sl_in" },
    };
    var body_outputs = [_]proto.ValueInfoProto{
        .{ .name = "st_out" },
        .{ .name = "scan_elem" },
    };
    _ = &body_nodes;
    _ = &body_inputs;
    _ = &body_outputs;
    const body = proto.GraphProto{
        .name = "scan_body",
        .nodes = &body_nodes,
        .inputs = &body_inputs,
        .outputs = &body_outputs,
    };
    var scan_out_axes = [_]i64{1};
    var attrs = [_]AttributeProto{
        .{ .name = "body", .g = body, .attr_type = .graph },
        .{ .name = "num_scan_inputs", .i = 1, .attr_type = .int },
        .{ .name = "scan_output_axes", .ints = &scan_out_axes, .attr_type = .ints },
    };
    const node = NodeProto{ .op_type = "Scan", .attributes = &attrs };
    _ = &attrs;

    // First output is final state [4], but the scan output should be [4, 3] (axis=1)
    const result = try convertNode(allocator, &b, &node, &.{ init_state, sequence }, null);
    try std.testing.expect(result != init_state);
    const out_shape = g.node(result).output_shape;
    try std.testing.expectEqual(@as(u8, 1), out_shape.rank());
    try std.testing.expectEqual(@as(i64, 4), out_shape.dim(0));
}

test "convertNode Pad constant mode" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 3 }));
    // Pad [1, 0, 0, 2] → begin=[1,0], end=[0,2] → shape [3, 5]
    const pads = try b.tensorConst(&.{ 1.0, 0.0, 0.0, 2.0 }, Shape.init(.i64, &.{4}));
    const node = NodeProto{ .op_type = "Pad" };
    const result = try convertNode(allocator, &b, &node, &.{ x, pads }, null);
    const out = g.node(result).output_shape;
    try std.testing.expectEqual(@as(u8, 2), out.rank());
    try std.testing.expectEqual(@as(i64, 3), out.dim(0)); // 2 + 1 begin
    try std.testing.expectEqual(@as(i64, 5), out.dim(1)); // 3 + 2 end
}

test "convertNode Pad all zeros is passthrough" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 3 }));
    const pads = try b.tensorConst(&.{ 0.0, 0.0, 0.0, 0.0 }, Shape.init(.i64, &.{4}));
    const node = NodeProto{ .op_type = "Pad" };
    const result = try convertNode(allocator, &b, &node, &.{ x, pads }, null);
    // Should pass through unchanged
    try std.testing.expectEqual(x, result);
}

test "convertNode Pad reflect mode" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 1, 5 }));
    // Pad [0, 2, 0, 1] → begin=[0,2], end=[0,1] on a dim-5 axis
    const pads = try b.tensorConst(&.{ 0.0, 2.0, 0.0, 1.0 }, Shape.init(.i64, &.{4}));
    var attrs_storage: [1]proto.AttributeProto = .{.{ .name = "mode", .s = "reflect" }};
    const node = NodeProto{ .op_type = "Pad", .attributes = &attrs_storage };
    const result = try convertNode(allocator, &b, &node, &.{ x, pads }, null);
    const out = g.node(result).output_shape;
    try std.testing.expectEqual(@as(u8, 2), out.rank());
    try std.testing.expectEqual(@as(i64, 1), out.dim(0));
    try std.testing.expectEqual(@as(i64, 8), out.dim(1)); // 5 + 2 + 1
    // Should use gather (not concat)
    // Should use gather (not concat)
    try std.testing.expectEqual(std.meta.Tag(ml.graph.node.OpCode).gather, std.meta.activeTag(g.node(result).op));
}

test "convertNode Pad edge mode" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 3, 4 }));
    // Pad [1, 0, 1, 0] → begin=[1,0], end=[1,0] on axis 0
    const pads = try b.tensorConst(&.{ 1.0, 0.0, 1.0, 0.0 }, Shape.init(.i64, &.{4}));
    var attrs_storage: [1]proto.AttributeProto = .{.{ .name = "mode", .s = "edge" }};
    const node = NodeProto{ .op_type = "Pad", .attributes = &attrs_storage };
    const result = try convertNode(allocator, &b, &node, &.{ x, pads }, null);
    const out = g.node(result).output_shape;
    try std.testing.expectEqual(@as(u8, 2), out.rank());
    try std.testing.expectEqual(@as(i64, 5), out.dim(0)); // 3 + 1 + 1
    try std.testing.expectEqual(@as(i64, 4), out.dim(1)); // unchanged
}

test "convertNode Pad opset<11 reads pads from attribute" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 3 }));
    // No pads input — fall back to `pads` attribute (opset <11).
    var attrs_storage = [_]AttributeProto{
        .{ .name = "mode", .s = "constant" },
        .{ .name = "pads", .ints = @constCast(&[_]i64{ 1, 0, 0, 2 }) },
        .{ .name = "value", .f = 0.0 },
    };
    const node = NodeProto{ .op_type = "Pad", .attributes = &attrs_storage };
    const result = try convertNode(allocator, &b, &node, &.{x}, null);
    const out = g.node(result).output_shape;
    try std.testing.expectEqual(@as(u8, 2), out.rank());
    try std.testing.expectEqual(@as(i64, 3), out.dim(0)); // 2 + 1 begin
    try std.testing.expectEqual(@as(i64, 5), out.dim(1)); // 3 + 2 end
}

test "convertNode Reshape allowzero=1 treats 0 as literal" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // Input shape [2, 3]; target shape [0, 6] with allowzero=1 means
    // dim 0 is a literal zero rather than "copy from input shape".
    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 3 }));
    const shape = try b.tensorConst(&.{ 0.0, 6.0 }, Shape.init(.i64, &.{2}));
    var attrs_storage = [_]AttributeProto{
        .{ .name = "allowzero", .i = 1 },
    };
    const node = NodeProto{ .op_type = "Reshape", .attributes = &attrs_storage };
    const result = try convertNode(allocator, &b, &node, &.{ x, shape }, null);
    const out = g.node(result).output_shape;
    try std.testing.expectEqual(@as(u8, 2), out.rank());
    try std.testing.expectEqual(@as(i64, 0), out.dim(0));
    try std.testing.expectEqual(@as(i64, 6), out.dim(1));
}

test "convertNode Reshape default (allowzero=0) copies from input" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // Target shape [0, 6] without allowzero means dim 0 copies from input (→ 2).
    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 3 }));
    const shape = try b.tensorConst(&.{ 0.0, 6.0 }, Shape.init(.i64, &.{2}));
    const node = NodeProto{ .op_type = "Reshape" };
    const result = try convertNode(allocator, &b, &node, &.{ x, shape }, null);
    const out = g.node(result).output_shape;
    try std.testing.expectEqual(@as(u8, 2), out.rank());
    try std.testing.expectEqual(@as(i64, 2), out.dim(0));
    try std.testing.expectEqual(@as(i64, 6), out.dim(1));
}

test "convertNode Reshape repairs copied zero dim when rank expansion would overcount" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 12, 64, 50 }));
    const shape = try b.tensorConst(&.{ 0.0, 12.0, 64.0, 50.0 }, Shape.init(.i64, &.{4}));
    const node = NodeProto{ .op_type = "Reshape" };
    const result = try convertNode(allocator, &b, &node, &.{ x, shape }, null);
    const out = g.node(result).output_shape;
    try std.testing.expectEqual(@as(u8, 4), out.rank());
    try std.testing.expectEqual(@as(i64, 1), out.dim(0));
    try std.testing.expectEqual(@as(i64, 12), out.dim(1));
    try std.testing.expectEqual(@as(i64, 64), out.dim(2));
    try std.testing.expectEqual(@as(i64, 50), out.dim(3));
}

test "convertNode Reshape infers attention head count from hidden split" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 1, -1, 512 }));
    const shape = try b.tensorConst(&.{ 1.0, -1.0, -1.0, 64.0 }, Shape.init(.i64, &.{4}));
    const node = NodeProto{ .op_type = "Reshape" };
    const result = try convertNode(allocator, &b, &node, &.{ x, shape }, null);
    const out = g.node(result).output_shape;
    try std.testing.expectEqual(@as(u8, 4), out.rank());
    try std.testing.expectEqual(@as(i64, 1), out.dim(0));
    try std.testing.expectEqual(@as(i64, -1), out.dim(1));
    try std.testing.expectEqual(@as(i64, 8), out.dim(2));
    try std.testing.expectEqual(@as(i64, 64), out.dim(3));
}

test "OpType.fromString recognizes control flow ops" {
    try std.testing.expect(OpType.fromString("If") == .If);
    try std.testing.expect(OpType.fromString("Loop") == .Loop);
    try std.testing.expect(OpType.fromString("Scan") == .Scan);
}
