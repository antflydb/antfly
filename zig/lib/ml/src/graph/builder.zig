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

const std = @import("std");
const graph_mod = @import("graph.zig");
const node_mod = @import("node.zig");
const shape_mod = @import("shape.zig");

const Graph = graph_mod.Graph;
const Node = node_mod.Node;
const NodeId = node_mod.NodeId;
const null_node = node_mod.null_node;
const OpCode = node_mod.OpCode;
const Shape = shape_mod.Shape;
const DType = shape_mod.DType;

/// High-level graph construction API. Provides both primitive ops (add, mul,
/// dot_general, ...) and fused ops (linear, rmsNorm, gelu, ...).
///
/// Fused builder methods emit both the fused node and a decomposed primitive
/// subgraph, storing the decomposed root in vjp_alternate. This enables
/// autograd without hand-written VJPs for fused ops (GoMLX pattern).
pub const Builder = struct {
    graph: *Graph,

    pub fn init(graph: *Graph) Builder {
        return .{ .graph = graph };
    }

    // ── Parameters & Constants ─────────────────────────────────────────

    pub fn parameter(self: *Builder, name: []const u8, s: Shape) !NodeId {
        const str = try self.graph.internString(name);
        const id = try self.graph.addNode(.{
            .op = .{ .parameter = .{ .name_offset = str.offset, .name_len = str.len } },
            .output_shape = s,
        });
        try self.graph.parameters.append(self.graph.allocator, id);
        return id;
    }

    /// Create or reuse a cached scalar constant node. The value is
    /// stored using the dtype's native byte layout (f16 stored as 2
    /// bytes, i32 as 4 bytes, bf16 as the top 16 bits of the f32
    /// representation, etc.) — readers via `constantDataAs(T, ...)`
    /// see the right thing for any T matching `dtype`.
    pub fn scalarConst(self: *Builder, dtype: DType, value: f32) !NodeId {
        // Check cache
        if (self.graph.constant_cache.getScalar(dtype, @floatCast(value))) |cached| {
            return cached;
        }
        const loc = try self.graph.internScalarConst(@floatCast(value), dtype);
        const id = try self.graph.addNode(.{
            .op = .{ .constant = .{ .data_offset = loc.offset, .data_len = loc.len } },
            .output_shape = Shape.scalar(dtype),
        });
        try self.graph.constant_cache.putScalar(self.graph.allocator, dtype, @floatCast(value), id);
        return id;
    }

    pub fn tensorConst(self: *Builder, data: []const f32, s: Shape) !NodeId {
        const loc = if (s.dtype == .f32) blk: {
            break :blk try self.graph.internConstant(data);
        } else blk: {
            const elem_size = s.dtype.byteSize();
            const bytes = try self.graph.allocator.alloc(u8, data.len * elem_size);
            defer self.graph.allocator.free(bytes);
            var tmp: [8]u8 = undefined;
            for (data, 0..) |value, i| {
                const encoded = graph_mod.encodeScalar(value, s.dtype, &tmp);
                @memcpy(bytes[i * elem_size ..][0..elem_size], encoded);
            }
            break :blk try self.graph.internConstantBytes(bytes, s.dtype);
        };
        return self.graph.addNode(.{
            .op = .{ .constant = .{ .data_offset = loc.offset, .data_len = loc.len } },
            .output_shape = s,
        });
    }

    /// Typed tensor-const that takes raw bytes plus a shape and uses
    /// `s.dtype` to interpret them. Useful for non-f32 constants —
    /// e.g. integer index tables — where the f32-only `tensorConst`
    /// would otherwise force the caller to manually call
    /// `internConstantBytes` and add the node by hand.
    pub fn tensorConstBytes(self: *Builder, bytes: []const u8, s: Shape) !NodeId {
        const loc = try self.graph.internConstantBytes(bytes, s.dtype);
        return self.graph.addNode(.{
            .op = .{ .constant = .{ .data_offset = loc.offset, .data_len = loc.len } },
            .output_shape = s,
        });
    }

    // ── Primitive Elementwise Unary ────────────────────────────────────

    fn unaryOp(self: *Builder, comptime op: std.meta.Tag(OpCode), input: NodeId) !NodeId {
        const s = self.graph.node(input).output_shape;
        return self.graph.addNode(.{
            .op = @unionInit(OpCode, @tagName(op), {}),
            .output_shape = s,
            .inputs = .{ input, null_node, null_node, null_node },
            .num_inputs = 1,
        });
    }

    pub fn neg(self: *Builder, input: NodeId) !NodeId {
        return self.unaryOp(.neg, input);
    }

    pub fn sqrt(self: *Builder, input: NodeId) !NodeId {
        return self.unaryOp(.sqrt, input);
    }

    pub fn rsqrt(self: *Builder, input: NodeId) !NodeId {
        return self.unaryOp(.rsqrt, input);
    }

    pub fn expOp(self: *Builder, input: NodeId) !NodeId {
        return self.unaryOp(.exp, input);
    }

    pub fn logOp(self: *Builder, input: NodeId) !NodeId {
        return self.unaryOp(.log, input);
    }

    pub fn sinOp(self: *Builder, input: NodeId) !NodeId {
        return self.unaryOp(.sin, input);
    }

    pub fn cosOp(self: *Builder, input: NodeId) !NodeId {
        return self.unaryOp(.cos, input);
    }

    pub fn tanhOp(self: *Builder, input: NodeId) !NodeId {
        return self.unaryOp(.tanh, input);
    }

    pub fn erfOp(self: *Builder, input: NodeId) !NodeId {
        return self.unaryOp(.erf, input);
    }

    pub fn absOp(self: *Builder, input: NodeId) !NodeId {
        return self.unaryOp(.abs, input);
    }

    // ── Primitive Elementwise Binary ───────────────────────────────────

    fn binaryOp(self: *Builder, comptime op: std.meta.Tag(OpCode), a: NodeId, b: NodeId) !NodeId {
        // Use the shape with more elements (handles scalar broadcasting).
        const a_shape = self.graph.node(a).output_shape;
        const b_shape = self.graph.node(b).output_shape;
        const a_elems = a_shape.numElements() orelse 1;
        const b_elems = b_shape.numElements() orelse 1;
        const s = if (a_elems >= b_elems) a_shape else b_shape;
        return self.graph.addNode(.{
            .op = @unionInit(OpCode, @tagName(op), {}),
            .output_shape = s,
            .inputs = .{ a, b, null_node, null_node },
            .num_inputs = 2,
        });
    }

    pub fn add(self: *Builder, a: NodeId, b: NodeId) !NodeId {
        return self.binaryOp(.add, a, b);
    }

    pub fn mul(self: *Builder, a: NodeId, b: NodeId) !NodeId {
        return self.binaryOp(.mul, a, b);
    }

    pub fn sub(self: *Builder, a: NodeId, b: NodeId) !NodeId {
        return self.binaryOp(.sub, a, b);
    }

    pub fn div(self: *Builder, a: NodeId, b: NodeId) !NodeId {
        return self.binaryOp(.div, a, b);
    }

    // ── Primitive Reduction ────────────────────────────────────────────

    pub fn reduceSum(self: *Builder, input: NodeId, axes: []const u8) !NodeId {
        return self.reduceOp(.reduce_sum, input, axes);
    }

    pub fn reduceMax(self: *Builder, input: NodeId, axes: []const u8) !NodeId {
        return self.reduceOp(.reduce_max, input, axes);
    }

    pub fn reduceMean(self: *Builder, input: NodeId, axes: []const u8) !NodeId {
        return self.reduceOp(.reduce_mean, input, axes);
    }

    pub fn argMax(self: *Builder, input: NodeId, axis: u8, keepdims: bool) !NodeId {
        const in_shape = self.graph.node(input).output_shape;
        if (axis >= in_shape.rank()) return error.ShapeMismatch;

        var out_dims: [shape_mod.max_rank]i64 = @splat(0);
        var out_rank: usize = 0;
        if (keepdims) {
            out_rank = in_shape.rank();
            out_dims = in_shape.dims;
            out_dims[axis] = 1;
        } else {
            for (0..in_shape.rank()) |dim_idx| {
                if (dim_idx == axis) continue;
                out_dims[out_rank] = in_shape.dims[dim_idx];
                out_rank += 1;
            }
        }
        const out_shape = Shape{
            .dtype = .i64,
            .dims = out_dims,
            .rank_ = @intCast(out_rank),
        };

        return self.graph.addNode(.{
            .op = .{ .argmax = .{ .axis = axis, .keepdims = keepdims } },
            .output_shape = out_shape,
            .inputs = .{ input, null_node, null_node, null_node },
            .num_inputs = 1,
        });
    }

    fn reduceOp(self: *Builder, comptime op: std.meta.Tag(OpCode), input: NodeId, axes: []const u8) !NodeId {
        var attrs = node_mod.ReduceAttrs{};
        attrs.num_axes = @intCast(axes.len);
        @memcpy(attrs.axes[0..axes.len], axes);

        // Compute output shape: remove reduced axes (simplified — keeps rank, sets reduced dims to 1)
        const in_shape = self.graph.node(input).output_shape;
        var out_dims: [shape_mod.max_rank]i64 = in_shape.dims;
        for (axes) |ax| {
            out_dims[ax] = 1;
        }
        const out_shape = Shape{
            .dtype = in_shape.dtype,
            .dims = out_dims,
            .rank_ = in_shape.rank_,
        };

        return self.graph.addNode(.{
            .op = @unionInit(OpCode, @tagName(op), attrs),
            .output_shape = out_shape,
            .inputs = .{ input, null_node, null_node, null_node },
            .num_inputs = 1,
        });
    }

    // ── Primitive Shape Manipulation ───────────────────────────────────

    pub fn reshape(self: *Builder, input: NodeId, new_shape: Shape) !NodeId {
        return self.graph.addNode(.{
            .op = .{ .reshape = .{ .new_shape = new_shape } },
            .output_shape = new_shape,
            .inputs = .{ input, null_node, null_node, null_node },
            .num_inputs = 1,
        });
    }

    pub fn transpose(self: *Builder, input: NodeId, perm: []const u8) !NodeId {
        var attrs = node_mod.TransposeAttrs{};
        attrs.num_axes = @intCast(perm.len);
        @memcpy(attrs.perm[0..perm.len], perm);

        const in_shape = self.graph.node(input).output_shape;
        var out_dims: [shape_mod.max_rank]i64 = @splat(0);
        for (perm, 0..) |p, i| {
            out_dims[i] = in_shape.dims[p];
        }
        const out_shape = Shape{
            .dtype = in_shape.dtype,
            .dims = out_dims,
            .rank_ = in_shape.rank_,
        };

        return self.graph.addNode(.{
            .op = .{ .transpose = attrs },
            .output_shape = out_shape,
            .inputs = .{ input, null_node, null_node, null_node },
            .num_inputs = 1,
        });
    }

    // ── Primitive Contraction ──────────────────────────────────────────

    /// Standard 2D matmul: [M, K] x [K, N] -> [M, N]
    pub fn matmul(self: *Builder, a: NodeId, b: NodeId) !NodeId {
        const a_shape = self.graph.node(a).output_shape;
        const b_shape = self.graph.node(b).output_shape;
        const out_shape = Shape.init(a_shape.dtype, &.{
            a_shape.dim(0),
            b_shape.dim(1),
        });
        return self.graph.addNode(.{
            .op = .{ .dot_general = .{
                .lhs_contracting = .{ 1, 0, 0, 0, 0, 0, 0, 0 },
                .rhs_contracting = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
                .num_contracting = 1,
            } },
            .output_shape = out_shape,
            .inputs = .{ a, b, null_node, null_node },
            .num_inputs = 2,
        });
    }

    /// Batched matmul with a single leading batch dimension:
    ///   [B, M, K] x [B, K, N] -> [B, M, N].
    /// Both inputs must be 3-D and share the same batch dim. Used by
    /// decomposed multi-head attention (Q @ K^T, probs @ V).
    pub fn matmul3D(self: *Builder, a: NodeId, b: NodeId) !NodeId {
        const a_shape = self.graph.node(a).output_shape;
        const b_shape = self.graph.node(b).output_shape;
        const out_shape = Shape.init(a_shape.dtype, &.{
            a_shape.dim(0),
            a_shape.dim(1),
            b_shape.dim(2),
        });
        return self.graph.addNode(.{
            .op = .{ .dot_general = .{
                .lhs_contracting = .{ 2, 0, 0, 0, 0, 0, 0, 0 },
                .rhs_contracting = .{ 1, 0, 0, 0, 0, 0, 0, 0 },
                .lhs_batch = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
                .rhs_batch = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
                .num_contracting = 1,
                .num_batch = 1,
            } },
            .output_shape = out_shape,
            .inputs = .{ a, b, null_node, null_node },
            .num_inputs = 2,
        });
    }

    // ── Type Conversion ────────────────────────────────────────────────

    /// Insert a dtype conversion node.  The output tensor has the same
    /// shape as `input` but with dtype changed to `target_dtype`.
    pub fn convertDtype(self: *Builder, input: NodeId, target_dtype: DType) !NodeId {
        const in_shape = self.graph.node(input).output_shape;
        const out_shape = Shape{
            .dtype = target_dtype,
            .dims = in_shape.dims,
            .rank_ = in_shape.rank_,
        };
        return self.graph.addNode(.{
            .op = .{ .convert_dtype = .{ .target = target_dtype } },
            .output_shape = out_shape,
            .inputs = .{ input, null_node, null_node, null_node },
            .num_inputs = 1,
        });
    }

    // ── Broadcast helper for decomposed ops ────────────────────────────

    /// After a reduce removes an axis, expand the result back to the
    /// original shape for correct element-wise binary ops. Without this,
    /// the flat `i % small.len` broadcast in native_compute produces wrong
    /// indices when the reduced axis is not the outermost dimension.
    fn broadcastReduced(self: *Builder, reduced: NodeId, target_shape: Shape) !NodeId {
        const reduced_shape = self.graph.node(reduced).output_shape;
        if (reduced_shape.numElements() == target_shape.numElements()) return reduced;

        var attrs = node_mod.BroadcastAttrs{ .target_shape = target_shape };
        var ax: u8 = 0;
        const reduced_rank = reduced_shape.rank();
        for (0..reduced_rank) |i| {
            attrs.broadcast_axes[ax] = @intCast(i);
            ax += 1;
        }
        attrs.num_axes = ax;

        return self.graph.addNode(.{
            .op = .{ .broadcast_in_dim = attrs },
            .output_shape = target_shape,
            .inputs = .{ reduced, null_node, null_node, null_node },
            .num_inputs = 1,
        });
    }

    // ── Primitive Gather ───────────────────────────────────────────────

    pub fn gather(self: *Builder, table: NodeId, indices: NodeId, out_shape: Shape) !NodeId {
        return self.graph.addNode(.{
            .op = .{ .gather = .{ .axis = 0 } },
            .output_shape = out_shape,
            .inputs = .{ table, indices, null_node, null_node },
            .num_inputs = 2,
        });
    }

    // ── Fused Ops (with decomposed vjp_alternate) ──────────────────────

    /// Fused linear: Y = X @ W^T + bias.
    /// Also emits primitive decomposition as vjp_alternate.
    pub fn linear(self: *Builder, input: NodeId, weight: NodeId, bias: NodeId, rows: u32, in_dim: u32, out_dim: u32) !NodeId {
        const input_shape = self.graph.node(input).output_shape;
        const matmul_input = if (input_shape.rank() == 1)
            try self.reshape(input, Shape.init(input_shape.dtype, &.{ @intCast(rows), @intCast(in_dim) }))
        else
            input;
        // Build decomposed subgraph: transpose(W) -> matmul -> add bias
        const wt = try self.transpose(weight, &.{ 1, 0 });
        const mm = try self.matmul(matmul_input, wt);
        const decomposed = try self.add(mm, bias);

        // Emit fused node
        const out_shape = Shape.init(
            input_shape.dtype,
            &.{ @intCast(rows), @intCast(out_dim) },
        );
        const fused = try self.graph.addNode(.{
            .op = .{ .fused_linear = .{ .rows = rows, .in_dim = in_dim, .out_dim = out_dim } },
            .output_shape = out_shape,
            .inputs = .{ input, weight, bias, null_node },
            .num_inputs = 3,
            .vjp_alternate = decomposed,
        });
        return fused;
    }

    /// Fused linear without bias: Y = X @ W^T.
    pub fn linearNoBias(self: *Builder, input: NodeId, weight: NodeId, rows: u32, in_dim: u32, out_dim: u32) !NodeId {
        const input_shape = self.graph.node(input).output_shape;
        const matmul_input = if (input_shape.rank() == 1)
            try self.reshape(input, Shape.init(input_shape.dtype, &.{ @intCast(rows), @intCast(in_dim) }))
        else
            input;
        // Decomposed
        const wt = try self.transpose(weight, &.{ 1, 0 });
        const decomposed = try self.matmul(matmul_input, wt);

        const out_shape = Shape.init(
            input_shape.dtype,
            &.{ @intCast(rows), @intCast(out_dim) },
        );
        const fused = try self.graph.addNode(.{
            .op = .{ .fused_linear_no_bias = .{ .rows = rows, .in_dim = in_dim, .out_dim = out_dim } },
            .output_shape = out_shape,
            .inputs = .{ input, weight, null_node, null_node },
            .num_inputs = 2,
            .vjp_alternate = decomposed,
        });
        return fused;
    }

    /// Fused RMS normalization: x * rsqrt(mean(x^2) + eps) * weight.
    pub fn rmsNorm(self: *Builder, input: NodeId, weight: NodeId, dim: u32, eps: f32) !NodeId {
        // Decomposed: x_sq -> mean -> + eps -> rsqrt -> * x -> * weight
        const in_shape = self.graph.node(input).output_shape;
        const last_axis: u8 = in_shape.rank() - 1;
        const x_sq = try self.mul(input, input);
        const mean_sq = try self.reduceMean(x_sq, &.{last_axis});
        const eps_node = try self.scalarConst(in_shape.dtype, eps);
        const mean_plus_eps = try self.add(mean_sq, eps_node);
        const inv_rms = try self.rsqrt(mean_plus_eps);
        const inv_rms_bc = try self.broadcastReduced(inv_rms, in_shape);
        const normed = try self.mul(input, inv_rms_bc);
        const decomposed = try self.mul(normed, weight);

        const fused = try self.graph.addNode(.{
            .op = .{ .fused_rms_norm = .{ .dim = dim, .eps = eps } },
            .output_shape = self.graph.node(input).output_shape,
            .inputs = .{ input, weight, null_node, null_node },
            .num_inputs = 2,
            .vjp_alternate = decomposed,
        });
        return fused;
    }

    /// Fused GELU activation.
    pub fn gelu(self: *Builder, input: NodeId) !NodeId {
        // Decomposed: x * 0.5 * (1 + tanh(sqrt(2/pi) * (x + 0.044715 * x^3)))
        const dtype = self.graph.node(input).output_shape.dtype;
        const half = try self.scalarConst(dtype, 0.5);
        const one = try self.scalarConst(dtype, 1.0);
        const coeff = try self.scalarConst(dtype, 0.044715);
        const sqrt_2_over_pi = try self.scalarConst(dtype, 0.7978845608); // sqrt(2/pi)

        const x_sq = try self.mul(input, input);
        const x_cubed = try self.mul(x_sq, input);
        const inner = try self.add(input, try self.mul(coeff, x_cubed));
        const scaled = try self.mul(sqrt_2_over_pi, inner);
        const tanh_val = try self.tanhOp(scaled);
        const one_plus_tanh = try self.add(one, tanh_val);
        const x_half = try self.mul(input, half);
        const decomposed = try self.mul(x_half, one_plus_tanh);

        const fused = try self.graph.addNode(.{
            .op = .{ .fused_gelu = {} },
            .output_shape = self.graph.node(input).output_shape,
            .inputs = .{ input, null_node, null_node, null_node },
            .num_inputs = 1,
            .vjp_alternate = decomposed,
        });
        return fused;
    }

    /// Fused SiLU/Swish activation: x * sigmoid(x).
    pub fn silu(self: *Builder, input: NodeId) !NodeId {
        // Decomposed: x / (1 + exp(-x))
        const dtype = self.graph.node(input).output_shape.dtype;
        const one = try self.scalarConst(dtype, 1.0);
        const neg_x = try self.neg(input);
        const exp_neg = try self.expOp(neg_x);
        const denom = try self.add(one, exp_neg);
        const sigmoid_x = try self.div(one, denom);
        const decomposed = try self.mul(input, sigmoid_x);

        const fused = try self.graph.addNode(.{
            .op = .{ .fused_silu = {} },
            .output_shape = self.graph.node(input).output_shape,
            .inputs = .{ input, null_node, null_node, null_node },
            .num_inputs = 1,
            .vjp_alternate = decomposed,
        });
        return fused;
    }

    /// Fused ReLU: max(0, x).
    pub fn relu(self: *Builder, input: NodeId) !NodeId {
        // Decomposed: where(x < 0, 0, x)
        const dtype = self.graph.node(input).output_shape.dtype;
        const zero = try self.scalarConst(dtype, 0.0);
        const cmp = try self.binaryOp(.less_than, input, zero);
        const decomposed = try self.graph.addNode(.{
            .op = .{ .where_select = {} },
            .output_shape = self.graph.node(input).output_shape,
            .inputs = .{ cmp, zero, input, null_node },
            .num_inputs = 3,
        });

        const fused = try self.graph.addNode(.{
            .op = .{ .fused_relu = {} },
            .output_shape = self.graph.node(input).output_shape,
            .inputs = .{ input, null_node, null_node, null_node },
            .num_inputs = 1,
            .vjp_alternate = decomposed,
        });
        return fused;
    }

    /// Fused element-wise add (matches VTable's add).
    pub fn elemAdd(self: *Builder, a: NodeId, b: NodeId) !NodeId {
        const decomposed = try self.add(a, b);

        const fused = try self.graph.addNode(.{
            .op = .{ .fused_elem_add = {} },
            .output_shape = self.graph.node(decomposed).output_shape,
            .inputs = .{ a, b, null_node, null_node },
            .num_inputs = 2,
            .vjp_alternate = decomposed,
        });
        return fused;
    }

    /// Fused element-wise multiply (matches VTable's multiply).
    pub fn elemMultiply(self: *Builder, a: NodeId, b: NodeId) !NodeId {
        const decomposed = try self.mul(a, b);

        const fused = try self.graph.addNode(.{
            .op = .{ .fused_elem_multiply = {} },
            .output_shape = self.graph.node(decomposed).output_shape,
            .inputs = .{ a, b, null_node, null_node },
            .num_inputs = 2,
            .vjp_alternate = decomposed,
        });
        return fused;
    }

    /// Fused softmax: exp(x - max(x)) / sum(exp(x - max(x))) along last axis.
    pub fn softmax(self: *Builder, input: NodeId) !NodeId {
        const in_shape = self.graph.node(input).output_shape;
        const last_axis: u8 = in_shape.rank() - 1;
        const last_dim_i64 = in_shape.dim(last_axis);
        // Dynamic last dim → 0 sentinel (mirrors lib/onnx/src/ops.zig:614).
        const dim: u32 = if (last_dim_i64 > 0) @intCast(last_dim_i64) else 0;

        // Decomposed: numerically stable softmax with explicit broadcast
        const max_val = try self.reduceMax(input, &.{last_axis});
        const max_bc = try self.broadcastReduced(max_val, in_shape);
        const shifted = try self.sub(input, max_bc);
        const exp_shifted = try self.expOp(shifted);
        const sum_exp = try self.reduceSum(exp_shifted, &.{last_axis});
        const sum_bc = try self.broadcastReduced(sum_exp, in_shape);
        const decomposed = try self.div(exp_shifted, sum_bc);

        const fused = try self.graph.addNode(.{
            .op = .{ .fused_softmax = .{ .dim = dim } },
            .output_shape = in_shape,
            .inputs = .{ input, null_node, null_node, null_node },
            .num_inputs = 1,
            .vjp_alternate = decomposed,
        });
        return fused;
    }

    /// Fused log-softmax: x - max(x) - log(sum(exp(x - max(x)))) along last axis.
    pub fn logSoftmax(self: *Builder, input: NodeId) !NodeId {
        const in_shape = self.graph.node(input).output_shape;
        const last_axis: u8 = in_shape.rank() - 1;
        const last_dim_i64 = in_shape.dim(last_axis);
        const dim: u32 = if (last_dim_i64 > 0) @intCast(last_dim_i64) else 0;

        // Decomposed: numerically stable log-softmax with explicit broadcast
        const max_val = try self.reduceMax(input, &.{last_axis});
        const max_bc = try self.broadcastReduced(max_val, in_shape);
        const shifted = try self.sub(input, max_bc);
        const exp_shifted = try self.expOp(shifted);
        const sum_exp = try self.reduceSum(exp_shifted, &.{last_axis});
        const log_sum_exp = try self.logOp(sum_exp);
        const lse_bc = try self.broadcastReduced(log_sum_exp, in_shape);
        const decomposed = try self.sub(shifted, lse_bc);

        const fused = try self.graph.addNode(.{
            .op = .{ .fused_log_softmax = .{ .dim = dim } },
            .output_shape = in_shape,
            .inputs = .{ input, null_node, null_node, null_node },
            .num_inputs = 1,
            .vjp_alternate = decomposed,
        });
        return fused;
    }

    /// Cross-entropy loss: -mean(sum(target * log_softmax(logits), axis=-1)).
    /// Returns a scalar. No fused op — composes from existing ops.
    pub fn crossEntropyLoss(self: *Builder, logits: NodeId, targets: NodeId) !NodeId {
        const in_shape = self.graph.node(logits).output_shape;
        const last_axis: u8 = in_shape.rank() - 1;

        const log_probs = try self.logSoftmax(logits);
        const weighted = try self.mul(targets, log_probs);
        // Sum over classes (last axis), then mean over batch
        const class_sum = try self.reduceSum(weighted, &.{last_axis}); // [batch, 1]
        // Reduce all remaining dims to scalar
        const cs_shape = self.graph.node(class_sum).output_shape;
        const cs_rank = cs_shape.rank();
        var all_axes: [shape_mod.max_rank]u8 = undefined;
        for (0..cs_rank) |i| all_axes[i] = @intCast(i);
        const mean_loss = try self.reduceMean(class_sum, all_axes[0..cs_rank]);
        return self.neg(mean_loss);
    }

    /// MSE loss: mean((predictions - targets)^2). Returns a scalar.
    /// No fused op — composes from existing ops.
    pub fn mseLoss(self: *Builder, predictions: NodeId, targets: NodeId) !NodeId {
        const diff = try self.sub(predictions, targets);
        const sq = try self.mul(diff, diff);
        // Reduce all dims to scalar
        const in_shape = self.graph.node(sq).output_shape;
        const rank = in_shape.rank();
        var all_axes: [shape_mod.max_rank]u8 = undefined;
        for (0..rank) |i| all_axes[i] = @intCast(i);
        return self.reduceMean(sq, all_axes[0..rank]);
    }

    /// Fused scaled dot-product attention with primitive decomposition.
    /// Q, K, V should be [B*H, S, D] shaped. Output is same shape as Q.
    pub fn sdpa(self: *Builder, Q: NodeId, K: NodeId, V: NodeId, batch: u32, seq_len: u32, num_heads: u32, head_dim: u32) !NodeId {
        const dtype = self.graph.node(Q).output_shape.dtype;
        const bh: i64 = @intCast(@as(u32, batch) * num_heads);
        const s: i64 = @intCast(seq_len);
        const d: i64 = @intCast(head_dim);

        // Decomposed: scores = Q @ K^T / sqrt(D), probs = softmax(scores), output = probs @ V

        // 1. K^T via transpose [0, 2, 1]: [B*H, S, D] → [B*H, D, S]
        const k_t = try self.transpose(K, &.{ 0, 2, 1 });

        // 2. scores = batched matmul(Q, K^T): [B*H, S, S]
        const scores_shape = Shape.init(dtype, &.{ bh, s, s });
        const scores = try self.graph.addNode(.{
            .op = .{ .dot_general = .{
                .lhs_contracting = .{ 2, 0, 0, 0, 0, 0, 0, 0 },
                .rhs_contracting = .{ 1, 0, 0, 0, 0, 0, 0, 0 },
                .lhs_batch = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
                .rhs_batch = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
                .num_contracting = 1,
                .num_batch = 1,
            } },
            .output_shape = scores_shape,
            .inputs = .{ Q, k_t, null_node, null_node },
            .num_inputs = 2,
        });

        // 3. Scale by 1/sqrt(head_dim)
        const scale = try self.scalarConst(dtype, 1.0 / @sqrt(@as(f32, @floatFromInt(head_dim))));
        const scaled_scores = try self.mul(scores, scale);

        // 4. Softmax along last axis (seq dim)
        const probs = try self.softmax(scaled_scores);

        // 5. output = batched matmul(probs, V): [B*H, S, D]
        const out_shape = Shape.init(dtype, &.{ bh, s, d });
        const decomposed = try self.graph.addNode(.{
            .op = .{ .dot_general = .{
                .lhs_contracting = .{ 2, 0, 0, 0, 0, 0, 0, 0 },
                .rhs_contracting = .{ 1, 0, 0, 0, 0, 0, 0, 0 },
                .lhs_batch = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
                .rhs_batch = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
                .num_contracting = 1,
                .num_batch = 1,
            } },
            .output_shape = out_shape,
            .inputs = .{ probs, V, null_node, null_node },
            .num_inputs = 2,
        });

        // Fused node
        const fused = try self.graph.addNode(.{
            .op = .{ .fused_sdpa = .{
                .batch = batch,
                .seq_len = seq_len,
                .num_heads = num_heads,
                .head_dim = head_dim,
            } },
            .output_shape = out_shape,
            .inputs = .{ Q, K, V, null_node },
            .num_inputs = 3,
            .vjp_alternate = decomposed,
        });
        return fused;
    }

    /// Fused layer normalization: gamma * (x - mean) / sqrt(var + eps) + beta.
    /// Used by BERT, DeBERTa, LayoutLMv3, ModernBERT. Distinct from rmsNorm.
    pub fn layerNorm(self: *Builder, input: NodeId, gamma: NodeId, beta: NodeId, dim: u32, eps: f32) !NodeId {
        const dtype = self.graph.node(input).output_shape.dtype;
        const last_axis: u8 = self.graph.node(input).output_shape.rank() - 1;

        // Decomposed: (x - mean) / sqrt(var + eps) * gamma + beta
        const in_shape = self.graph.node(input).output_shape;
        const mean = try self.reduceMean(input, &.{last_axis});
        const mean_bc = try self.broadcastReduced(mean, in_shape);
        const centered = try self.sub(input, mean_bc);
        const centered_sq = try self.mul(centered, centered);
        const variance = try self.reduceMean(centered_sq, &.{last_axis});
        const eps_node = try self.scalarConst(dtype, eps);
        const var_plus_eps = try self.add(variance, eps_node);
        const inv_std = try self.rsqrt(var_plus_eps);
        const inv_std_bc = try self.broadcastReduced(inv_std, in_shape);
        const normalized = try self.mul(centered, inv_std_bc);
        const scaled = try self.mul(normalized, gamma);
        const decomposed = try self.add(scaled, beta);

        const fused = try self.graph.addNode(.{
            .op = .{ .fused_layer_norm = .{ .dim = dim, .eps = eps } },
            .output_shape = self.graph.node(input).output_shape,
            .inputs = .{ input, gamma, beta, null_node },
            .num_inputs = 3,
            .vjp_alternate = decomposed,
        });
        return fused;
    }

    /// Fused rotary position embedding. Input [B*H, seq, head_dim] returns
    /// same shape with `rope_dim` leading dimensions rotated per-position.
    /// Uses precomputed cos/sin tables passed as `cos` and `sin` parameters,
    /// each shaped [seq, rope_dim/2].
    ///
    /// The half-split convention (not interleaved): for dim pair i in
    /// [0, rope_dim/2), the rotation reads x0 = input[..., i],
    /// x1 = input[..., i + rope_dim/2] and writes
    ///   out[..., i]                = x0 * cos[..., i] - x1 * sin[..., i]
    ///   out[..., i + rope_dim/2]   = x0 * sin[..., i] + x1 * cos[..., i]
    ///
    /// Callers precompute cos/sin as graph constants for their sequence
    /// layout and pass them in as tensor-const nodes.
    pub fn rope(
        self: *Builder,
        input: NodeId,
        cos: NodeId,
        sin: NodeId,
        seq_len: u32,
        head_dim: u32,
        rope_dim: u32,
        theta: f32,
    ) !NodeId {
        // RoPE is a per-pair rotation in the (x0, x1) plane. Because the
        // half-swap permutation cannot be expressed with the current set of
        // primitive builder wrappers (no slice/concat helper), we do NOT
        // emit a `vjp_alternate` decomposition here. Instead, `fused_rope`
        // is left intact through lowering and handled by a dedicated VJP
        // rule in `autodiff.zig` (see `.fused_rope` case in `applyVjp`).
        //
        // The backward of a rotation by angle θ is the rotation by −θ, so
        // the VJP simply invokes another `fused_rope` with `sin` negated.
        // This keeps the gradient mathematically exact without requiring
        // any scatter/gather or primitive-slice decomposition.
        const out_shape = self.graph.node(input).output_shape;

        const fused = try self.graph.addNode(.{
            .op = .{ .fused_rope = .{
                .seq_len = seq_len,
                .head_dim = head_dim,
                .rope_dim = rope_dim,
                .theta = theta,
                .freq_scale = 1.0,
                .position_offset = 0,
                .consecutive_pairs = false,
            } },
            .output_shape = out_shape,
            .inputs = .{ input, cos, sin, null_node },
            .num_inputs = 3,
            // No vjp_alternate: autodiff handles `fused_rope` with a
            // hand-written rule that reuses the fused op itself with a
            // negated-angle sin table.
            .vjp_alternate = null_node,
        });
        return fused;
    }

    /// SwiGLU MLP composition: down(silu(gate(x)) * up(x)).
    /// Takes pre-computed gate/up linear outputs and composes the activation
    /// + elementwise multiply. The caller handles the down-projection after.
    pub fn swigluActivation(self: *Builder, gate_linear: NodeId, up_linear: NodeId) !NodeId {
        const gated = try self.silu(gate_linear);
        return self.elemMultiply(gated, up_linear);
    }

    /// Slice the last dimension of a 2-D tensor: input[0:rows, start:end].
    /// input must be shape [rows, cols]; output is [rows, end-start].
    pub fn sliceLastDim(self: *Builder, input: NodeId, start: i64, end: i64) !NodeId {
        const in_shape = self.graph.node(input).output_shape;
        const rows = in_shape.dim(0);
        const out_shape = Shape.init(in_shape.dtype, &.{ rows, end - start });
        var attrs = node_mod.SliceAttrs{};
        attrs.num_axes = 2;
        attrs.starts[0] = 0;
        attrs.starts[1] = start;
        attrs.limits[0] = rows;
        attrs.limits[1] = end;
        attrs.strides[0] = 1;
        attrs.strides[1] = 1;
        return self.graph.addNode(.{
            .op = .{ .slice = attrs },
            .output_shape = out_shape,
            .inputs = .{ input, null_node, null_node, null_node },
            .num_inputs = 1,
        });
    }

    /// Fused embedding lookup.
    ///
    /// If `indices` is not `.i64`, a `convert_dtype` node is auto-inserted
    /// so that the decomposed gather path (used by autodiff) and the fused
    /// dispatch path both receive properly-typed index data.
    pub fn embeddingLookup(self: *Builder, weight: NodeId, indices: NodeId, total: u32, dim: u32) !NodeId {
        // Ensure indices are i64 for the gather op.
        const idx_dtype = self.graph.node(indices).output_shape.dtype;
        const actual_indices = if (idx_dtype != .i64)
            try self.convertDtype(indices, .i64)
        else
            indices;

        const dtype = self.graph.node(weight).output_shape.dtype;
        const out_shape = Shape.init(dtype, &.{ @intCast(total), @intCast(dim) });
        const decomposed = try self.gather(weight, actual_indices, out_shape);

        const fused = try self.graph.addNode(.{
            .op = .{ .fused_embedding_lookup = .{ .total = total, .dim = dim } },
            .output_shape = out_shape,
            .inputs = .{ weight, actual_indices, null_node, null_node },
            .num_inputs = 2,
            .vjp_alternate = decomposed,
        });
        return fused;
    }

    // ── Concat (binary, axis-aware) ────────────────────────────────────
    //
    // The IR's concat is binary; chain calls to concatenate three or
    // more tensors.  Output shape is the input shape with `axis`'s
    // dimension summed across both inputs.

    pub fn concat(self: *Builder, a: NodeId, b: NodeId, axis: u8) !NodeId {
        const a_shape = self.graph.node(a).output_shape;
        const b_shape = self.graph.node(b).output_shape;
        std.debug.assert(a_shape.rank() == b_shape.rank());
        std.debug.assert(axis < a_shape.rank());
        std.debug.assert(a_shape.dtype == b_shape.dtype);
        var out_dims: [shape_mod.max_rank]i64 = undefined;
        for (0..a_shape.rank()) |i| {
            const ai: u8 = @intCast(i);
            if (ai == axis) {
                out_dims[i] = a_shape.dim(ai) + b_shape.dim(ai);
            } else {
                std.debug.assert(a_shape.dim(ai) == b_shape.dim(ai));
                out_dims[i] = a_shape.dim(ai);
            }
        }
        return self.graph.addNode(.{
            .op = .{ .concat_prim = .{ .axis = axis } },
            .output_shape = Shape.init(a_shape.dtype, out_dims[0..a_shape.rank()]),
            .inputs = .{ a, b, null_node, null_node },
            .num_inputs = 2,
        });
    }

    // ── Sigmoid (composed) ─────────────────────────────────────────────
    //
    // Same pattern silu uses internally: sigmoid(x) = 1 / (1 + exp(-x)).
    // No fused IR node, so we just emit the decomposition; CSE in the
    // pipeline can fuse later if it's worth it.

    pub fn sigmoid(self: *Builder, x: NodeId) !NodeId {
        const dtype = self.graph.node(x).output_shape.dtype;
        const one = try self.scalarConst(dtype, 1.0);
        const neg_x = try self.neg(x);
        const exp_neg = try self.expOp(neg_x);
        const denom = try self.add(one, exp_neg);
        return self.div(one, denom);
    }

    // ── Scatter-add ────────────────────────────────────────────────────
    //
    // Inverse of gather along `axis` -- adds `values[i, :]` into
    // `dest[indices[i], :]`.  `dest` provides the output shape;
    // `values` is what we accumulate; `indices` selects the
    // destination rows.

    pub fn scatterAdd(self: *Builder, dest: NodeId, values: NodeId, indices: NodeId, axis: u8) !NodeId {
        const dest_shape = self.graph.node(dest).output_shape;
        return self.graph.addNode(.{
            .op = .{ .scatter_add = .{ .axis = axis } },
            .output_shape = dest_shape,
            .inputs = .{ dest, values, indices, null_node },
            .num_inputs = 3,
        });
    }
};

// ── Tests ──────────────────────────────────────────────────────────────

test "Builder.parameter" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const p = try b.parameter("input", Shape.init(.f32, &.{ 4, 8 }));
    try std.testing.expectEqual(@as(NodeId, 0), p);
    try std.testing.expectEqual(@as(usize, 1), g.parameters.items.len);
    try std.testing.expectEqualStrings("input", g.parameterName(g.node(p)));
}

test "Builder.scalarConst caching" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const c1 = try b.scalarConst(.f32, 1.0);
    const c2 = try b.scalarConst(.f32, 1.0);
    const c3 = try b.scalarConst(.f32, 2.0);
    try std.testing.expectEqual(c1, c2); // cached
    try std.testing.expect(c1 != c3); // different value
}

test "Builder.linear emits fused + decomposed" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("input", Shape.init(.f32, &.{ 2, 4 }));
    const w = try b.parameter("weight", Shape.init(.f32, &.{ 3, 4 }));
    const bias = try b.parameter("bias", Shape.init(.f32, &.{3}));

    const result = try b.linear(x, w, bias, 2, 4, 3);
    const result_node = g.node(result);

    // Fused node
    try std.testing.expect(result_node.op.isFused());

    // Has vjp_alternate pointing to decomposed subgraph
    try std.testing.expect(result_node.vjp_alternate != null_node);

    // Decomposed subgraph root should be a primitive add
    const decomposed = g.node(result_node.vjp_alternate);
    try std.testing.expect(decomposed.op.isPrimitive());
}

test "Builder.linearNoBias reshapes rank-1 input in decomposition" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("input", Shape.init(.f32, &.{4}));
    const w = try b.parameter("weight", Shape.init(.f32, &.{ 3, 4 }));

    const result = try b.linearNoBias(x, w, 1, 4, 3);
    const result_node = g.node(result);
    try std.testing.expect(result_node.op.isFused());
    try std.testing.expect(result_node.vjp_alternate != null_node);

    const matmul = g.node(result_node.vjp_alternate);
    try std.testing.expectEqual(OpCode.dot_general, std.meta.activeTag(matmul.op));
    const reshape = g.node(matmul.inputs[0]);
    try std.testing.expectEqual(OpCode.reshape, std.meta.activeTag(reshape.op));
    try std.testing.expectEqual(@as(i64, 1), reshape.output_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 4), reshape.output_shape.dim(1));
}

test "Builder.rmsNorm emits fused + decomposed" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("hidden", Shape.init(.f32, &.{ 4, 8 }));
    const w = try b.parameter("weight", Shape.init(.f32, &.{8}));

    const result = try b.rmsNorm(x, w, 8, 1e-5);
    const result_node = g.node(result);

    try std.testing.expect(result_node.op.isFused());
    try std.testing.expect(result_node.vjp_alternate != null_node);
}

test "Builder.gelu emits fused + decomposed" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("input", Shape.init(.f32, &.{ 4, 8 }));
    const result = try b.gelu(x);
    const result_node = g.node(result);

    try std.testing.expect(result_node.op.isFused());
    try std.testing.expect(result_node.vjp_alternate != null_node);
}

test "Builder.softmax emits fused + decomposed" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("input", Shape.init(.f32, &.{ 2, 4 }));
    const result = try b.softmax(x);
    const result_node = g.node(result);

    try std.testing.expect(result_node.op.isFused());
    try std.testing.expect(result_node.vjp_alternate != null_node);
    try std.testing.expectEqual(@as(i64, 2), result_node.output_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 4), result_node.output_shape.dim(1));
}

test "Builder fused elementwise ops preserve broadcasted shape" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const scalar = try b.parameter("scalar", Shape.init(.f32, &.{1}));
    const vector = try b.parameter("vector", Shape.init(.f32, &.{ 1, 6144 }));

    const product = try b.elemMultiply(scalar, vector);
    const sum = try b.elemAdd(scalar, vector);

    try std.testing.expectEqual(@as(i64, 1), g.node(product).output_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 6144), g.node(product).output_shape.dim(1));
    try std.testing.expectEqual(@as(i64, 1), g.node(sum).output_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 6144), g.node(sum).output_shape.dim(1));
}

test "Builder binary ops tolerate overflowed static element counts" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const huge = try b.parameter("huge", Shape.init(.f32, &.{ std.math.maxInt(i64), 2 }));
    const scalar = try b.parameter("scalar", Shape.init(.f32, &.{}));

    const result = try b.div(huge, scalar);
    try std.testing.expectEqual(@as(?i64, null), g.node(result).output_shape.numElements());
    try std.testing.expectEqual(@as(i64, std.math.maxInt(i64)), g.node(result).output_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 2), g.node(result).output_shape.dim(1));
}

test "Builder.crossEntropyLoss produces scalar" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const logits = try b.parameter("logits", Shape.init(.f32, &.{ 2, 3 }));
    const targets = try b.parameter("targets", Shape.init(.f32, &.{ 2, 3 }));
    const loss = try b.crossEntropyLoss(logits, targets);
    const loss_node = g.node(loss);

    // Cross-entropy loss should produce a scalar
    try std.testing.expectEqual(@as(i64, 1), loss_node.output_shape.numElements() orelse 0);
}

test "Builder.mseLoss produces scalar" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const pred = try b.parameter("pred", Shape.init(.f32, &.{ 4, 3 }));
    const target = try b.parameter("target", Shape.init(.f32, &.{ 4, 3 }));
    const loss = try b.mseLoss(pred, target);
    const loss_node = g.node(loss);

    // MSE loss should produce a scalar
    try std.testing.expectEqual(@as(i64, 1), loss_node.output_shape.numElements() orelse 0);
}

test "Builder.sdpa emits fused + decomposed" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // batch=1, heads=2, seq=4, head_dim=4 → B*H=2
    const q = try b.parameter("Q", Shape.init(.f32, &.{ 2, 4, 4 }));
    const k = try b.parameter("K", Shape.init(.f32, &.{ 2, 4, 4 }));
    const v = try b.parameter("V", Shape.init(.f32, &.{ 2, 4, 4 }));
    const result = try b.sdpa(q, k, v, 1, 4, 2, 4);
    const result_node = g.node(result);

    try std.testing.expect(result_node.op.isFused());
    try std.testing.expect(result_node.vjp_alternate != null_node);
    try std.testing.expectEqual(@as(i64, 2), result_node.output_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 4), result_node.output_shape.dim(1));
    try std.testing.expectEqual(@as(i64, 4), result_node.output_shape.dim(2));
}

test "Builder.tensorConst encodes requested dtype" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const i64_node = try b.tensorConst(&.{ -1.0, 64.0 }, Shape.init(.i64, &.{2}));
    const i64_attrs = g.node(i64_node).op.constant;
    try std.testing.expectEqualSlices(i64, &.{ -1, 64 }, g.constantDataAs(i64, i64_attrs.data_offset, i64_attrs.data_len));

    const i32_node = try b.tensorConst(&.{ 2.0, 7.0 }, Shape.init(.i32, &.{2}));
    const i32_attrs = g.node(i32_node).op.constant;
    try std.testing.expectEqualSlices(i32, &.{ 2, 7 }, g.constantDataAs(i32, i32_attrs.data_offset, i32_attrs.data_len));
}

test "Builder.layerNorm emits fused + decomposed" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("hidden", Shape.init(.f32, &.{ 4, 8 }));
    const gamma = try b.parameter("gamma", Shape.init(.f32, &.{8}));
    const beta = try b.parameter("beta", Shape.init(.f32, &.{8}));
    const result = try b.layerNorm(x, gamma, beta, 8, 1e-5);
    const result_node = g.node(result);

    try std.testing.expect(result_node.op.isFused());
    try std.testing.expect(result_node.vjp_alternate != null_node);
    try std.testing.expectEqual(@as(i64, 4), result_node.output_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 8), result_node.output_shape.dim(1));
}

test "Builder.rope emits fused op without vjp_alternate" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // [B*H=2, seq=4, head_dim=8], cos/sin [seq=4, head_dim=8]
    const x = try b.parameter("input", Shape.init(.f32, &.{ 2, 4, 8 }));
    const cos = try b.parameter("cos", Shape.init(.f32, &.{ 4, 8 }));
    const sin = try b.parameter("sin", Shape.init(.f32, &.{ 4, 8 }));
    const result = try b.rope(x, cos, sin, 4, 8, 8, 10000.0);
    const result_node = g.node(result);

    try std.testing.expect(result_node.op.isFused());
    // `fused_rope` intentionally has no decomposition; autodiff handles it
    // directly via a hand-written VJP that reuses fused_rope with −sin.
    try std.testing.expect(result_node.vjp_alternate == null_node);
    try std.testing.expectEqual(@as(i64, 8), result_node.output_shape.dim(2));
}

test "Builder.swigluActivation emits silu * up" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const gate = try b.parameter("gate", Shape.init(.f32, &.{ 4, 16 }));
    const up = try b.parameter("up", Shape.init(.f32, &.{ 4, 16 }));
    const result = try b.swigluActivation(gate, up);
    const result_node = g.node(result);

    // Result is elemMultiply (fused) of silu(gate) * up.
    try std.testing.expect(result_node.op.isFused());
    try std.testing.expectEqual(@as(i64, 4), result_node.output_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 16), result_node.output_shape.dim(1));
}

test "Builder.concat sums the chosen axis dim" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const a = try b.parameter("a", Shape.init(.f32, &.{ 4, 8 }));
    const c = try b.parameter("c", Shape.init(.f32, &.{ 4, 16 }));
    const result = try b.concat(a, c, 1);
    const result_node = g.node(result);

    try std.testing.expectEqual(@as(i64, 4), result_node.output_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 24), result_node.output_shape.dim(1)); // 8 + 16
}

test "Builder.sigmoid emits the 1/(1+exp(-x)) decomposition" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 4, 8 }));
    const result = try b.sigmoid(x);
    const result_node = g.node(result);

    // Output shape matches input -- the final node is the div.
    try std.testing.expectEqual(@as(i64, 4), result_node.output_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 8), result_node.output_shape.dim(1));
}

test "Builder.scatterAdd preserves dest shape" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const dest = try b.parameter("dest", Shape.init(.f32, &.{ 6, 8 }));
    const values = try b.parameter("values", Shape.init(.f32, &.{ 4, 8 }));
    const indices = try b.parameter("indices", Shape.init(.i64, &.{4}));
    const result = try b.scatterAdd(dest, values, indices, 0);
    const result_node = g.node(result);

    try std.testing.expectEqual(@as(i64, 6), result_node.output_shape.dim(0));
    try std.testing.expectEqual(@as(i64, 8), result_node.output_shape.dim(1));
}
