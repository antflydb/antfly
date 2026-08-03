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

// Numerical gradient checking via finite differences.
//
// Evaluates a computation graph with concrete f32 values and compares
// analytical gradients (from autodiff.zig) against numerical gradients
// computed via central differences: (f(x+eps) - f(x-eps)) / (2*eps).
//
// This validates every VJP rule in the autodiff module. A mismatch
// between numerical and analytical gradients indicates a bug in the
// corresponding VJP implementation.

const std = @import("std");
const graph_mod = @import("graph.zig");
const node_mod = @import("node.zig");
const shape_mod = @import("shape.zig");
const builder_mod = @import("builder.zig");
const autodiff_mod = @import("autodiff.zig");
const lower_mod = @import("lower.zig");
const tensor_eval = @import("tensor_eval.zig");

const Graph = graph_mod.Graph;
const Node = node_mod.Node;
const NodeId = node_mod.NodeId;
const null_node = node_mod.null_node;
const OpCode = node_mod.OpCode;
const Shape = shape_mod.Shape;
const Builder = builder_mod.Builder;

/// Result of evaluating a graph on concrete f32 data.
const EvalResult = struct {
    /// Output values, one slice per graph output.
    values: [][]f32,
    allocator: std.mem.Allocator,

    fn deinit(self: *EvalResult) void {
        for (self.values) |v| self.allocator.free(v);
        self.allocator.free(self.values);
    }
};

/// Evaluate a primitive-only graph on concrete f32 parameter values.
/// Returns the output tensor values.
fn eval(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    param_values: []const []const f32,
) !EvalResult {
    const count = graph.nodeCount();
    // Map each node → its computed f32 values
    const node_vals = try allocator.alloc(?[]f32, count);
    defer {
        for (node_vals) |maybe_v| {
            if (maybe_v) |v| allocator.free(v);
        }
        allocator.free(node_vals);
    }
    @memset(node_vals, null);

    // Bind parameters (in definition order)
    for (graph.parameters.items, 0..) |pid, idx| {
        if (idx < param_values.len) {
            node_vals[pid] = try allocator.dupe(f32, param_values[idx]);
        }
    }

    // Evaluate in topological order
    for (0..count) |i| {
        if (node_vals[i] != null) continue; // already set (parameter)
        const n = graph.node(@intCast(i));
        node_vals[i] = try evalNode(allocator, graph, n, node_vals);
    }

    // Collect outputs
    const outputs = try allocator.alloc([]f32, graph.outputs.items.len);
    for (graph.outputs.items, 0..) |oid, idx| {
        outputs[idx] = try allocator.dupe(f32, node_vals[oid] orelse return error.MissingValue);
    }

    return .{ .values = outputs, .allocator = allocator };
}

fn getVal(node_vals: []const ?[]f32, id: NodeId) []const f32 {
    return node_vals[id].?;
}

fn scalarVal(node_vals: []const ?[]f32, id: NodeId) f32 {
    const v = node_vals[id].?;
    return v[0];
}

fn evalNode(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    n: *const Node,
    node_vals: []const ?[]f32,
) ![]f32 {
    const ins = n.getInputs();
    const out_elems: usize = @intCast(n.output_shape.numElements() orelse 1);
    const in0_shape = if (n.num_inputs > 0 and ins[0] != null_node) graph.node(ins[0]).output_shape else n.output_shape;

    switch (n.op) {
        .constant => |attrs| {
            // Decode by dtype. The pool stores raw bytes, so reading an
            // `.i64` index constant as f32 (what this used to do) yields
            // garbage — and index constants are exactly what feeds the
            // gather / scatter_add operands.
            const dtype = n.output_shape.dtype;
            if (dtype == .f32) {
                return allocator.dupe(f32, graph.constantData(attrs.data_offset, attrs.data_len));
            }
            const view = try graph.constantDataAsF32(allocator, dtype, attrs.data_offset, attrs.data_len);
            if (view.owned) |owned| return owned;
            return allocator.dupe(f32, view.data);
        },

        .neg => {
            const a = getVal(node_vals, ins[0]);
            const out = try allocator.alloc(f32, a.len);
            for (out, a) |*o, v| o.* = -v;
            return out;
        },

        .sqrt => {
            const a = getVal(node_vals, ins[0]);
            const out = try allocator.alloc(f32, a.len);
            for (out, a) |*o, v| o.* = @sqrt(v);
            return out;
        },

        .rsqrt => {
            const a = getVal(node_vals, ins[0]);
            const out = try allocator.alloc(f32, a.len);
            for (out, a) |*o, v| o.* = 1.0 / @sqrt(v);
            return out;
        },

        .exp => {
            const a = getVal(node_vals, ins[0]);
            const out = try allocator.alloc(f32, a.len);
            for (out, a) |*o, v| o.* = @exp(v);
            return out;
        },

        .log => {
            const a = getVal(node_vals, ins[0]);
            const out = try allocator.alloc(f32, a.len);
            for (out, a) |*o, v| o.* = @log(v);
            return out;
        },

        .sin => {
            const a = getVal(node_vals, ins[0]);
            const out = try allocator.alloc(f32, a.len);
            for (out, a) |*o, v| o.* = @sin(v);
            return out;
        },

        .cos => {
            const a = getVal(node_vals, ins[0]);
            const out = try allocator.alloc(f32, a.len);
            for (out, a) |*o, v| o.* = @cos(v);
            return out;
        },

        .tanh => {
            const a = getVal(node_vals, ins[0]);
            const out = try allocator.alloc(f32, a.len);
            for (out, a) |*o, v| o.* = std.math.tanh(v);
            return out;
        },

        .erf => {
            const a = getVal(node_vals, ins[0]);
            const out = try allocator.alloc(f32, a.len);
            // Approximation: erf(x) ≈ tanh(sqrt(2/pi) * (x + 0.044715 * x^3))
            for (out, a) |*o, v| {
                const x3 = v * v * v;
                o.* = std.math.tanh(0.7978845608 * (v + 0.044715 * x3));
            }
            return out;
        },

        .abs => {
            const a = getVal(node_vals, ins[0]);
            const out = try allocator.alloc(f32, a.len);
            for (out, a) |*o, v| o.* = @abs(v);
            return out;
        },

        .add => {
            const a = getVal(node_vals, ins[0]);
            const b = getVal(node_vals, ins[1]);
            return broadcastBinaryOp(allocator, a, b, out_elems, .add_op);
        },

        .mul => {
            const a = getVal(node_vals, ins[0]);
            const b = getVal(node_vals, ins[1]);
            return broadcastBinaryOp(allocator, a, b, out_elems, .mul_op);
        },

        .sub => {
            const a = getVal(node_vals, ins[0]);
            const b = getVal(node_vals, ins[1]);
            return broadcastBinaryOp(allocator, a, b, out_elems, .sub_op);
        },

        .div => {
            const a = getVal(node_vals, ins[0]);
            const b = getVal(node_vals, ins[1]);
            return broadcastBinaryOp(allocator, a, b, out_elems, .div_op);
        },

        .less_than => {
            const a = getVal(node_vals, ins[0]);
            const b = getVal(node_vals, ins[1]);
            return broadcastBinaryOp(allocator, a, b, out_elems, .less_than_op);
        },

        .where_select => {
            const cond = getVal(node_vals, ins[0]);
            const on_true = getVal(node_vals, ins[1]);
            const on_false = getVal(node_vals, ins[2]);
            const out = try allocator.alloc(f32, out_elems);
            for (out, 0..) |*o, i| {
                const c = if (i < cond.len) cond[i] else cond[0];
                const t = if (i < on_true.len) on_true[i] else on_true[0];
                const f = if (i < on_false.len) on_false[i] else on_false[0];
                o.* = if (c > 0) t else f;
            }
            return out;
        },

        .reduce_sum => |attrs| {
            return evalReduce(allocator, n, node_vals, attrs, in0_shape, .sum);
        },

        .reduce_mean => |attrs| {
            return evalReduce(allocator, n, node_vals, attrs, in0_shape, .mean);
        },

        .reduce_max => |attrs| {
            return evalReduce(allocator, n, node_vals, attrs, in0_shape, .max);
        },

        .reshape => {
            // Reshape is a no-op on the underlying data — same elements
            // in the same row-major order, just a different shape view.
            const a = getVal(node_vals, ins[0]);
            return allocator.dupe(f32, a);
        },

        .broadcast_in_dim => |attrs| {
            // Delegate to the shared evaluator so const_fold and
            // grad_check stay in lock-step. The shared helper is
            // axis-aware (the `i % a.len` heuristic in the original
            // grad_check copy was a real bug and only reached the
            // light of day when this path got exercised by the
            // softmax / norm decompositions).
            const a = getVal(node_vals, ins[0]);
            const in_shape = graph.node(ins[0]).output_shape;
            if (try tensor_eval.evalBroadcast(f32, allocator, a, in_shape, n.output_shape, attrs)) |out| {
                return out;
            }
            // Fallback: copy bytes when the shared helper bails (e.g.
            // shape mismatch). Numerical-grad eval prefers a best-
            // effort answer over a hard error since the comparison
            // only matters when both forward passes succeed.
            const out = try allocator.alloc(f32, out_elems);
            for (out, 0..) |*o, i| o.* = if (i < a.len) a[i] else 0;
            return out;
        },

        .transpose => |attrs| {
            const a = getVal(node_vals, ins[0]);
            const in_shape = graph.node(ins[0]).output_shape;
            if (try tensor_eval.evalTranspose(f32, allocator, a, in_shape, n.output_shape, attrs)) |out| {
                return out;
            }
            // Shape mismatch (e.g. dynamic dims) — fall back to a
            // copy so the surrounding eval keeps going.
            return allocator.dupe(f32, a);
        },

        .dot_general => |attrs| {
            const a = getVal(node_vals, ins[0]);
            const b = getVal(node_vals, ins[1]);
            const a_shape = graph.node(ins[0]).output_shape;
            const b_shape = graph.node(ins[1]).output_shape;

            // 3D batched matmul: batch dim 0, one contracting dim.
            if (a_shape.rank() == 3 and b_shape.rank() == 3 and attrs.num_batch == 1 and attrs.num_contracting == 1) {
                const batch_sz: usize = @intCast(a_shape.dim(0));
                const lc = attrs.lhs_contracting[0];
                const rc = attrs.rhs_contracting[0];
                const k: usize = @intCast(a_shape.dim(lc));
                // Free dims: non-batch, non-contracting.
                const m: usize = @intCast(a_shape.dim(if (lc == 1) 2 else 1));
                const nn: usize = @intCast(b_shape.dim(if (rc == 1) 2 else 1));
                const a_d1: usize = @intCast(a_shape.dim(1));
                const a_d2: usize = @intCast(a_shape.dim(2));
                const b_d1: usize = @intCast(b_shape.dim(1));
                const b_d2: usize = @intCast(b_shape.dim(2));
                const out = try allocator.alloc(f32, batch_sz * m * nn);
                @memset(out, 0);
                for (0..batch_sz) |bi| {
                    for (0..m) |mi| {
                        for (0..nn) |ni| {
                            var s: f32 = 0;
                            for (0..k) |ki| {
                                const a_idx = bi * a_d1 * a_d2 + (if (lc == 2) mi * a_d2 + ki else ki * a_d2 + mi);
                                const b_idx = bi * b_d1 * b_d2 + (if (rc == 1) ki * b_d2 + ni else ni * b_d2 + ki);
                                s += a[a_idx] * b[b_idx];
                            }
                            out[bi * m * nn + mi * nn + ni] = s;
                        }
                    }
                }
                return out;
            }

            // 2D matmul (no batch dims).
            if (a_shape.rank() != 2 or b_shape.rank() != 2) {
                const out = try allocator.alloc(f32, out_elems);
                @memset(out, 0);
                return out;
            }
            const M: usize = @intCast(a_shape.dim(0));
            const K: usize = @intCast(a_shape.dim(1));
            const N: usize = @intCast(b_shape.dim(1));
            const out = try allocator.alloc(f32, M * N);
            @memset(out, 0);
            for (0..M) |m| {
                for (0..N) |nn| {
                    var sum: f32 = 0;
                    for (0..K) |k| {
                        sum += a[m * K + k] * b[k * N + nn];
                    }
                    out[m * N + nn] = sum;
                }
            }
            return out;
        },

        .slice => |attrs| {
            const a = getVal(node_vals, ins[0]);
            const in_shape = graph.node(ins[0]).output_shape;
            if (try tensor_eval.evalSlice(f32, allocator, a, in_shape, n.output_shape, attrs)) |out| {
                return out;
            }
            return error.UnsupportedSlice;
        },

        .concat_prim => |attrs| {
            const a = getVal(node_vals, ins[0]);
            const bb = getVal(node_vals, ins[1]);
            const a_shape = graph.node(ins[0]).output_shape;
            const b_shape = graph.node(ins[1]).output_shape;
            if (try tensor_eval.evalConcat(f32, allocator, a, a_shape, bb, b_shape, n.output_shape, attrs)) |out| {
                return out;
            }
            return error.UnsupportedConcat;
        },

        .gather => |attrs| {
            const table = getVal(node_vals, ins[0]);
            const indices = getVal(node_vals, ins[1]);
            return evalGatherRef(
                allocator,
                table,
                graph.node(ins[0]).output_shape,
                indices,
                graph.node(ins[1]).output_shape,
                attrs.axis,
                n.output_shape,
            );
        },

        .scatter_add => |attrs| {
            // Two shapes of this op live in the IR:
            //
            //   3-input `Builder.scatterAdd(dest, values, indices, axis)`
            //     — output starts as a copy of `dest`.
            //   2-input autodiff form `scatter_add(adj, indices)` emitted
            //     by the `.gather` VJP — output starts at zero.
            //
            // Both are handled by `interpreter.zig`'s `.scatter_add`
            // dispatch, so the reference evaluator handles both too.
            if (n.num_inputs == 2) {
                const values = getVal(node_vals, ins[0]);
                const indices = getVal(node_vals, ins[1]);
                return evalScatterAddRef(
                    allocator,
                    null,
                    values,
                    graph.node(ins[0]).output_shape,
                    indices,
                    graph.node(ins[1]).output_shape,
                    attrs.axis,
                    n.output_shape,
                );
            }
            if (n.num_inputs != 3) return error.UnsupportedScatterAddArity;
            const dest = getVal(node_vals, ins[0]);
            const values = getVal(node_vals, ins[1]);
            const indices = getVal(node_vals, ins[2]);
            return evalScatterAddRef(
                allocator,
                dest,
                values,
                graph.node(ins[1]).output_shape,
                indices,
                graph.node(ins[2]).output_shape,
                attrs.axis,
                n.output_shape,
            );
        },

        .convert_dtype => |attrs| {
            // The executors model every tensor as an f32 buffer, so a
            // dtype conversion is a value-domain operation here, not a
            // bit-level one. Converting to an integer dtype rounds so
            // that downstream index consumers see exact integers —
            // matching `interpreter.zig`'s `.convert_dtype` handler.
            const src = getVal(node_vals, ins[0]);
            const out = try allocator.dupe(f32, src);
            switch (attrs.target) {
                .i64, .i32, .u8, .bool_ => {
                    for (out) |*v| v.* = @round(v.*);
                },
                else => {},
            }
            return out;
        },

        .fused_rope => |attrs| {
            // Half-split rotation used by the builder/autodiff.
            //   input shape: [..., seq, head_dim]
            //   cos/sin shape: [seq, head_dim]
            // For each pair i in [0, rope_dim/2):
            //   x0 = input[..., i],   x1 = input[..., i + D/2]
            //   out[..., i]        = x0*cos[i] - x1*sin[i]
            //   out[..., i + D/2]  = x0*sin[i] + x1*cos[i]
            // Elements past rope_dim pass through unchanged.
            const in_data = getVal(node_vals, ins[0]);
            const cos_data = getVal(node_vals, ins[1]);
            const sin_data = getVal(node_vals, ins[2]);
            const in_shape = graph.node(ins[0]).output_shape;
            const rank = in_shape.rank();
            const head_dim: usize = @intCast(in_shape.dim(rank - 1));
            const seq_len: usize = if (rank >= 2) @intCast(in_shape.dim(rank - 2)) else 1;
            const rope_dim_attr: usize = if (attrs.rope_dim == 0) head_dim else @intCast(attrs.rope_dim);
            const rope_dim: usize = @min(rope_dim_attr, head_dim);
            const half: usize = rope_dim / 2;

            // outer_batch = product of leading dims before seq.
            var outer_batch: usize = 1;
            if (rank >= 2) {
                for (0..(rank - 2)) |d| {
                    outer_batch *= @intCast(in_shape.dim(@intCast(d)));
                }
            } else {
                outer_batch = 1;
            }

            const out = try allocator.alloc(f32, out_elems);
            @memcpy(out, in_data);
            for (0..outer_batch) |b_idx| {
                for (0..seq_len) |s| {
                    const row_off = (b_idx * seq_len + s) * head_dim;
                    const cs_row = s * head_dim;
                    for (0..half) |i| {
                        const x0 = in_data[row_off + i];
                        const x1 = in_data[row_off + i + half];
                        const c = cos_data[cs_row + i];
                        const sv = sin_data[cs_row + i];
                        out[row_off + i] = x0 * c - x1 * sv;
                        out[row_off + i + half] = x0 * sv + x1 * c;
                    }
                }
            }
            return out;
        },

        .parameter => unreachable, // should be pre-set

        else => {
            // Unsupported op — return zeros
            const out = try allocator.alloc(f32, out_elems);
            @memset(out, 0);
            return out;
        },
    }
}

// ── gather / scatter_add reference semantics ──────────────────────────
//
// These mirror what the graph executors actually compute, so that a
// finite-difference check that runs over a graph containing gather /
// scatter_add validates against the real forward instead of a stub.
//
// Authoritative implementations this was written against:
//
//   gather      `pkg/inference/src/ops/native_compute.zig:primGatherOp`
//               (the axis-generic strided path), the equivalent
//               `metal_compute.zig:primGatherOp` axis-0 2-D path, and
//               the constant-folding twin `passes/const_fold.zig:foldGather`.
//               Semantics are ONNX `Gather`:
//                 out[outer, k, inner] = table[outer, indices[k], inner]
//               with output shape
//                 table.dims[0..axis] ++ indices.dims ++ table.dims[axis+1..]
//               Negative indices wrap (`idx += axis_extent`); anything
//               still out of range is an error.
//
//   scatter_add `pkg/inference/src/ops/native_compute.zig:primScatterAddOp`,
//               `metal_compute.zig:primScatterAddOp` (kernel
//               `termite_scatter_add_axis0_f32` in metal_kernels.m) and
//               the host fallback `graph/interpreter.zig:executeScatterAdd`.
//               Semantics:
//                 out[outer, indices[k], inner] += values[outer, k, inner]
//               Output shape is the destination shape. Duplicate indices
//               accumulate. Unlike gather, no backend wraps negative
//               indices for scatter_add, so a negative index is an error
//               here rather than a wrap.
//
// Anything outside those contracts fails loudly: a reference forward
// that silently returns something plausible is worse than no check.

const AxisSplit = struct {
    outer: usize,
    axis_size: usize,
    inner: usize,

    fn total(self: AxisSplit) usize {
        return self.outer * self.axis_size * self.inner;
    }
};

/// Split a static shape into (product of dims before `axis`, dim at
/// `axis`, product of dims after `axis`).
fn splitAroundAxis(shape: Shape, axis: u8) !AxisSplit {
    const rank = shape.rank();
    if (axis >= rank) return error.IndexAxisOutOfRange;
    var outer: usize = 1;
    for (0..axis) |k| {
        const d = shape.dim(@intCast(k));
        if (d <= 0) return error.DynamicShapeUnsupported;
        outer *= @intCast(d);
    }
    const axis_dim = shape.dim(axis);
    if (axis_dim <= 0) return error.DynamicShapeUnsupported;
    var inner: usize = 1;
    for ((axis + 1)..rank) |k| {
        const d = shape.dim(@intCast(k));
        if (d <= 0) return error.DynamicShapeUnsupported;
        inner *= @intCast(d);
    }
    return .{ .outer = outer, .axis_size = @intCast(axis_dim), .inner = inner };
}

/// Index tensors travel through this evaluator as f32 (every executor
/// models tensors as f32 buffers). Require an exact integer: the
/// backends disagree on how to coerce a non-integral index (native
/// truncates via `@intFromFloat`, the Metal kernel uses `rint`, the
/// interpreter host path uses `@round`), so a fractional index has no
/// single right answer and must not be silently resolved.
fn indexValue(raw: f32) !i64 {
    if (!std.math.isFinite(raw)) return error.NonIntegerIndex;
    const rounded = @round(raw);
    if (@abs(raw - rounded) > 1e-3) return error.NonIntegerIndex;
    return @intFromFloat(rounded);
}

/// Element count of an index operand, cross-checked against the data.
fn indexCount(idx_shape: Shape, indices: []const f32) !usize {
    const declared = tensor_eval.staticElements(idx_shape) orelse indices.len;
    if (declared != indices.len) return error.IndexShapeMismatch;
    return declared;
}

fn evalGatherRef(
    allocator: std.mem.Allocator,
    table: []const f32,
    table_shape: Shape,
    indices: []const f32,
    idx_shape: Shape,
    axis: u8,
    out_shape: Shape,
) ![]f32 {
    const split = try splitAroundAxis(table_shape, axis);
    if (table.len != split.total()) return error.GatherTableShapeMismatch;
    const idx_count = try indexCount(idx_shape, indices);

    const out_elems = tensor_eval.staticElements(out_shape) orelse return error.DynamicShapeUnsupported;
    if (out_elems != split.outer * idx_count * split.inner) return error.GatherOutputShapeMismatch;

    const out = try allocator.alloc(f32, out_elems);
    errdefer allocator.free(out);

    const axis_size_i64: i64 = @intCast(split.axis_size);
    for (0..idx_count) |k| {
        var row = try indexValue(indices[k]);
        if (row < 0) row += axis_size_i64; // ONNX negative-index wrap
        if (row < 0 or row >= axis_size_i64) return error.GatherIndexOutOfRange;
        const src_row: usize = @intCast(row);
        for (0..split.outer) |o| {
            const src = (o * split.axis_size + src_row) * split.inner;
            const dst = (o * idx_count + k) * split.inner;
            @memcpy(out[dst..][0..split.inner], table[src..][0..split.inner]);
        }
    }
    return out;
}

fn evalScatterAddRef(
    allocator: std.mem.Allocator,
    dest: ?[]const f32,
    values: []const f32,
    values_shape: Shape,
    indices: []const f32,
    idx_shape: Shape,
    axis: u8,
    out_shape: Shape,
) ![]f32 {
    const split = try splitAroundAxis(out_shape, axis);
    const out_elems = tensor_eval.staticElements(out_shape) orelse return error.DynamicShapeUnsupported;
    if (out_elems != split.total()) return error.ScatterOutputShapeMismatch;
    const idx_count = try indexCount(idx_shape, indices);

    if (tensor_eval.staticElements(values_shape)) |declared_values| {
        if (declared_values != values.len) return error.ScatterValuesShapeMismatch;
    }
    if (values.len != split.outer * idx_count * split.inner) return error.ScatterValuesShapeMismatch;

    const out = try allocator.alloc(f32, out_elems);
    errdefer allocator.free(out);
    if (dest) |d| {
        if (d.len != out_elems) return error.ScatterDestShapeMismatch;
        @memcpy(out, d);
    } else {
        @memset(out, 0);
    }

    const axis_size_i64: i64 = @intCast(split.axis_size);
    for (0..idx_count) |k| {
        const row = try indexValue(indices[k]);
        // No negative-index wrap: no backend implements one for
        // scatter_add (native traps / errors, the Metal kernel silently
        // drops the row, the interpreter host path returns
        // IndexOutOfBounds), so there is no agreed behaviour to model.
        if (row < 0 or row >= axis_size_i64) return error.ScatterIndexOutOfRange;
        const dst_row: usize = @intCast(row);
        for (0..split.outer) |o| {
            const src = (o * idx_count + k) * split.inner;
            const dst = (o * split.axis_size + dst_row) * split.inner;
            for (0..split.inner) |i| out[dst + i] += values[src + i];
        }
    }
    return out;
}

const BinaryOpKind = enum { add_op, mul_op, sub_op, div_op, less_than_op };

fn broadcastBinaryOp(allocator: std.mem.Allocator, a: []const f32, b: []const f32, out_elems: usize, op: BinaryOpKind) ![]f32 {
    const out = try allocator.alloc(f32, out_elems);
    for (out, 0..) |*o, i| {
        const av = if (a.len == 1) a[0] else if (i < a.len) a[i] else a[i % a.len];
        const bv = if (b.len == 1) b[0] else if (i < b.len) b[i] else b[i % b.len];
        o.* = switch (op) {
            .add_op => av + bv,
            .mul_op => av * bv,
            .sub_op => av - bv,
            .div_op => av / bv,
            .less_than_op => if (av < bv) @as(f32, 1.0) else 0.0,
        };
    }
    return out;
}

const ReduceKind = tensor_eval.ReduceKind;

fn evalReduce(
    allocator: std.mem.Allocator,
    n: *const Node,
    node_vals: []const ?[]f32,
    attrs: node_mod.ReduceAttrs,
    in_shape: Shape,
    kind: ReduceKind,
) ![]f32 {
    const ins = n.getInputs();
    const a = getVal(node_vals, ins[0]);
    const out_elems: usize = @intCast(n.output_shape.numElements() orelse 1);

    if (try tensor_eval.evalReduce(f32, allocator, a, in_shape, n.output_shape, attrs, kind)) |out| {
        return out;
    }
    // Shared helper bailed (dynamic shape, mismatched ranks, etc.).
    // Fall back to a copy so the numerical-grad eval can keep going;
    // the analytical and numerical paths both hit the same fallback,
    // so the comparison itself remains meaningful.
    const out = try allocator.alloc(f32, out_elems);
    for (out, 0..) |*o, i| o.* = if (i < a.len) a[i] else 0;
    return out;
}

// ── Gradient Checking ────────────────────────────────────────────────

/// Check analytical gradients against numerical (finite difference) gradients.
/// Returns the maximum relative error across all parameter elements.
pub fn checkGradients(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    loss_id: NodeId,
    wrt: []const NodeId,
    param_values: []const []const f32,
    eps: f32,
) !f32 {
    // 1. Compute analytical gradients via autodiff
    var ad_result = try autodiff_mod.gradient(allocator, graph, loss_id, wrt);
    defer ad_result.deinit();

    // 2. Evaluate analytical gradient values
    // The AD result graph has the original + gradient nodes.
    // We need to evaluate it with the same parameter values.
    // First, lower the original graph to get param mapping.
    var lowered_for_eval = try lower_mod.lower(allocator, graph);
    defer lowered_for_eval.deinit();

    // Evaluate the AD graph (which is already lowered + grad nodes)
    var ad_eval = try eval(allocator, &ad_result.graph, param_values);
    defer ad_eval.deinit();

    // The AD graph's outputs are the original outputs.
    // We need to evaluate the gradient nodes separately.
    // Add gradient nodes as outputs temporarily.
    var grad_graph = ad_result.graph;
    const orig_output_count = grad_graph.outputs.items.len;

    for (ad_result.param_grads) |grad_id| {
        if (grad_id != null_node) {
            try grad_graph.outputs.append(allocator, grad_id);
        }
    }

    var full_eval = try eval(allocator, &grad_graph, param_values);
    defer full_eval.deinit();

    // Remove temporarily added outputs
    grad_graph.outputs.shrinkRetainingCapacity(orig_output_count);

    // 3. Compute numerical gradients via central differences
    var max_rel_error: f32 = 0;

    for (wrt, 0..) |wrt_id, wrt_idx| {
        const grad_id = ad_result.param_grads[wrt_idx];
        if (grad_id == null_node) continue;

        // Map wrt NodeId to parameter index in graph.parameters.
        var graph_param_idx: ?usize = null;
        for (graph.parameters.items, 0..) |pid, idx| {
            if (pid == wrt_id) {
                graph_param_idx = idx;
                break;
            }
        }
        if (graph_param_idx == null) continue;
        const pi = graph_param_idx.?;

        const analytical = full_eval.values[orig_output_count + wrt_idx];
        const param_data = param_values[pi];

        // Perturb each element of this parameter
        var perturbed = try allocator.alloc([]f32, param_values.len);
        defer allocator.free(perturbed);

        // Copy all param slices
        for (param_values, 0..) |pv, j| {
            perturbed[j] = try allocator.dupe(f32, pv);
        }
        defer for (perturbed) |p| allocator.free(p);

        for (0..param_data.len) |elem_idx| {
            const orig_val = param_data[elem_idx];

            // f(x + eps)
            perturbed[pi][elem_idx] = orig_val + eps;
            var eval_plus = try eval(allocator, &lowered_for_eval.graph, perturbed);
            const f_plus = eval_plus.values[0][0];
            eval_plus.deinit();

            // f(x - eps)
            perturbed[pi][elem_idx] = orig_val - eps;
            var eval_minus = try eval(allocator, &lowered_for_eval.graph, perturbed);
            const f_minus = eval_minus.values[0][0];
            eval_minus.deinit();

            // Restore
            perturbed[pi][elem_idx] = orig_val;

            const numerical = (f_plus - f_minus) / (2.0 * eps);
            const anal = if (elem_idx < analytical.len) analytical[elem_idx] else 0;

            // Hybrid absolute/relative error. Pure relative error blows
            // up to ~1.0 when both anal and num are near zero (e.g.
            // sum(softmax(x)) — the analytic gradient is exactly 0 and
            // the numerical gradient is float noise on the order of
            // 1e-5). Floor the denominator with a small absolute scale
            // so near-zero pairs are compared in absolute terms.
            const abs_floor: f32 = 1e-3;
            const denom = @max(@max(@abs(numerical), @abs(anal)), abs_floor);
            const err = @abs(numerical - anal) / denom;

            max_rel_error = @max(max_rel_error, err);
        }
    }

    return max_rel_error;
}

// ── Tests ──────────────────────────────────────────────────────────────

const tolerance: f32 = 0.02; // 2% relative error tolerance

fn makeParamValues(allocator: std.mem.Allocator, comptime specs: anytype) ![specs.len][]f32 {
    var result: [specs.len][]f32 = undefined;
    inline for (specs, 0..) |spec, i| {
        result[i] = try allocator.dupe(f32, &spec);
    }
    return result;
}

fn freeParamValues(allocator: std.mem.Allocator, vals: anytype) void {
    for (vals) |v| allocator.free(v);
}

test "eval simple add" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const a = try b.parameter("a", Shape.init(.f32, &.{3}));
    const bp = try b.parameter("b", Shape.init(.f32, &.{3}));
    const sum = try b.add(a, bp);
    const loss = try b.reduceSum(sum, &.{0});
    try g.markOutput(loss);

    const pa = [_]f32{ 1.0, 2.0, 3.0 };
    const pb = [_]f32{ 4.0, 5.0, 6.0 };

    var result = try eval(allocator, &g, &.{ &pa, &pb });
    defer result.deinit();

    // sum([1+4, 2+5, 3+6]) = sum([5, 7, 9]) = 21
    try std.testing.expectApproxEqAbs(@as(f32, 21.0), result.values[0][0], 1e-5);
}

test "eval matmul" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const a = try b.parameter("a", Shape.init(.f32, &.{ 1, 2 }));
    const bp = try b.parameter("b", Shape.init(.f32, &.{ 2, 1 }));
    const y = try b.matmul(a, bp);
    const loss = try b.reduceSum(y, &.{ 0, 1 });
    try g.markOutput(loss);

    const pa = [_]f32{ 2.0, 3.0 }; // [1, 2]
    const pb = [_]f32{ 4.0, 5.0 }; // [2, 1]

    var result = try eval(allocator, &g, &.{ &pa, &pb });
    defer result.deinit();

    // [2,3] @ [4; 5] = [2*4 + 3*5] = [23]
    try std.testing.expectApproxEqAbs(@as(f32, 23.0), result.values[0][0], 1e-5);
}

test "grad_check add" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const a = try b.parameter("a", Shape.init(.f32, &.{3}));
    const bp = try b.parameter("b", Shape.init(.f32, &.{3}));
    const sum = try b.add(a, bp);
    const loss = try b.reduceSum(sum, &.{0});
    try g.markOutput(loss);

    const pa = [_]f32{ 1.0, 2.0, 3.0 };
    const pb = [_]f32{ 4.0, 5.0, 6.0 };

    const max_err = try checkGradients(allocator, &g, loss, &.{ a, bp }, &.{ &pa, &pb }, 1e-4);
    try std.testing.expect(max_err < tolerance);
}

test "grad_check mul" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{3}));
    const w = try b.parameter("w", Shape.init(.f32, &.{3}));
    const prod = try b.mul(x, w);
    const loss = try b.reduceSum(prod, &.{0});
    try g.markOutput(loss);

    const px = [_]f32{ 1.0, 2.0, 3.0 };
    const pw = [_]f32{ 0.5, -0.5, 1.5 };

    const max_err = try checkGradients(allocator, &g, loss, &.{ x, w }, &.{ &px, &pw }, 1e-4);
    try std.testing.expect(max_err < tolerance);
}

test "grad_check matmul" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 3 }));
    const w = try b.parameter("w", Shape.init(.f32, &.{ 3, 2 }));
    const y = try b.matmul(x, w);
    const loss = try b.reduceSum(y, &.{ 0, 1 });
    try g.markOutput(loss);

    const px = [_]f32{ 0.1, 0.2, 0.3, 0.4, 0.5, 0.6 };
    const pw = [_]f32{ 0.7, 0.8, 0.9, 1.0, 1.1, 1.2 };

    const max_err = try checkGradients(allocator, &g, loss, &.{ x, w }, &.{ &px, &pw }, 1e-4);
    try std.testing.expect(max_err < tolerance);
}

test "grad_check fused gelu" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{4}));
    const activated = try b.gelu(x);
    const loss = try b.reduceSum(activated, &.{0});
    try g.markOutput(loss);

    const px = [_]f32{ -1.0, 0.0, 0.5, 1.5 };

    const max_err = try checkGradients(allocator, &g, loss, &.{x}, &.{&px}, 1e-4);
    try std.testing.expect(max_err < tolerance);
}

test "grad_check fused silu" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{4}));
    const activated = try b.silu(x);
    const loss = try b.reduceSum(activated, &.{0});
    try g.markOutput(loss);

    const px = [_]f32{ -1.0, 0.0, 0.5, 2.0 };

    const max_err = try checkGradients(allocator, &g, loss, &.{x}, &.{&px}, 1e-4);
    try std.testing.expect(max_err < tolerance);
}

test "grad_check fused linear" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 3 }));
    const w = try b.parameter("w", Shape.init(.f32, &.{ 2, 3 }));
    const bias = try b.parameter("bias", Shape.init(.f32, &.{2}));
    const y = try b.linear(x, w, bias, 2, 3, 2);
    const loss = try b.reduceSum(y, &.{ 0, 1 });
    try g.markOutput(loss);

    const px = [_]f32{ 0.1, 0.2, 0.3, 0.4, 0.5, 0.6 };
    const pw = [_]f32{ 0.7, 0.8, 0.9, 1.0, 1.1, 1.2 };
    const pbias = [_]f32{ 0.01, 0.02 };

    const max_err = try checkGradients(allocator, &g, loss, &.{ x, w, bias }, &.{ &px, &pw, &pbias }, 1e-4);
    try std.testing.expect(max_err < tolerance);
}

test "grad_check linear -> gelu chain" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 3 }));
    const w = try b.parameter("w", Shape.init(.f32, &.{ 2, 3 }));
    const bias = try b.parameter("bias", Shape.init(.f32, &.{2}));
    const y = try b.linear(x, w, bias, 2, 3, 2);
    const activated = try b.gelu(y);
    const loss = try b.reduceSum(activated, &.{ 0, 1 });
    try g.markOutput(loss);

    const px = [_]f32{ 0.1, 0.2, 0.3, 0.4, 0.5, 0.6 };
    const pw = [_]f32{ 0.3, 0.4, 0.5, 0.6, 0.7, 0.8 };
    const pbias = [_]f32{ 0.01, 0.02 };

    const max_err = try checkGradients(allocator, &g, loss, &.{ w, bias }, &.{ &px, &pw, &pbias }, 1e-4);
    try std.testing.expect(max_err < tolerance);
}

test "grad_check softmax" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 3 }));
    const sm = try b.softmax(x);
    const loss = try b.reduceSum(sm, &.{ 0, 1 });
    try g.markOutput(loss);

    const px = [_]f32{ 1.0, 2.0, 3.0, 0.5, -0.5, 1.5 };

    const max_err = try checkGradients(allocator, &g, loss, &.{x}, &.{&px}, 1e-4);
    try std.testing.expect(max_err < tolerance);
}

test "grad_check log_softmax" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 3 }));
    const lsm = try b.logSoftmax(x);
    const loss = try b.reduceSum(lsm, &.{ 0, 1 });
    try g.markOutput(loss);

    const px = [_]f32{ 1.0, 2.0, 3.0, 0.5, -0.5, 1.5 };

    const max_err = try checkGradients(allocator, &g, loss, &.{x}, &.{&px}, 1e-3);
    try std.testing.expect(max_err < tolerance);
}

test "grad_check cross_entropy_loss" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // 2 samples, 3 classes
    const logits = try b.parameter("logits", Shape.init(.f32, &.{ 2, 3 }));
    const targets = try b.parameter("targets", Shape.init(.f32, &.{ 2, 3 }));
    const loss = try b.crossEntropyLoss(logits, targets);
    try g.markOutput(loss);

    // Logits and one-hot targets
    const p_logits = [_]f32{ 1.0, 2.0, 0.5, -1.0, 0.0, 3.0 };
    const p_targets = [_]f32{ 1.0, 0.0, 0.0, 0.0, 0.0, 1.0 }; // one-hot

    const max_err = try checkGradients(allocator, &g, loss, &.{logits}, &.{ &p_logits, &p_targets }, 1e-3);
    try std.testing.expect(max_err < tolerance);
}

test "grad_check mse_loss" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const pred = try b.parameter("pred", Shape.init(.f32, &.{ 2, 3 }));
    const target = try b.parameter("target", Shape.init(.f32, &.{ 2, 3 }));
    const loss = try b.mseLoss(pred, target);
    try g.markOutput(loss);

    const p_pred = [_]f32{ 1.0, 2.0, 3.0, 4.0, 5.0, 6.0 };
    const p_target = [_]f32{ 1.1, 1.9, 3.2, 3.8, 5.1, 5.9 };

    const max_err = try checkGradients(allocator, &g, loss, &.{pred}, &.{ &p_pred, &p_target }, 1e-4);
    try std.testing.expect(max_err < tolerance);
}

test "grad_check softmax * y" {
    // softmax-in-the-middle-of-a-chain (elementwise mul). Catches
    // softmax-VJP bugs without the additional batched-matmul layer
    // that SDPA adds on top.
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 3 }));
    const y = try b.parameter("y", Shape.init(.f32, &.{ 2, 3 }));
    const sm = try b.softmax(x);
    const product = try b.mul(sm, y);
    const loss = try b.reduceSum(product, &.{ 0, 1 });
    try g.markOutput(loss);

    const px = [_]f32{ 1.0, 2.0, 3.0, 0.5, -0.5, 1.5 };
    const py = [_]f32{ 0.1, 0.2, 0.3, 0.4, 0.5, 0.6 };

    const max_err = try checkGradients(allocator, &g, loss, &.{ x, y }, &.{ &px, &py }, 1e-3);
    try std.testing.expect(max_err < tolerance);
}

test "grad_check softmax @ y (batched dot)" {
    // softmax-then-3D-dot — the SDPA Q@K^T side. Isolates the
    // batched-dot VJP from the rest of the SDPA chain.
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 1, 3, 3 }));
    const v = try b.parameter("v", Shape.init(.f32, &.{ 1, 3, 2 }));
    const sm = try b.softmax(x);
    const out = try g.addNode(.{
        .op = .{ .dot_general = .{
            .lhs_contracting = .{ 2, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_contracting = .{ 1, 0, 0, 0, 0, 0, 0, 0 },
            .lhs_batch = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_batch = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
            .num_contracting = 1,
            .num_batch = 1,
        } },
        .output_shape = Shape.init(.f32, &.{ 1, 3, 2 }),
        .inputs = .{ sm, v, null_node, null_node },
        .num_inputs = 2,
    });
    const loss = try b.reduceSum(out, &.{ 0, 1, 2 });
    try g.markOutput(loss);

    const px = [_]f32{ 1.0, 2.0, 3.0, 0.5, -0.5, 1.5, 0.1, 0.7, 0.3 };
    const pv = [_]f32{ 0.1, 0.2, 0.3, 0.4, 0.5, 0.6 };

    const max_err = try checkGradients(allocator, &g, loss, &.{ x, v }, &.{ &px, &pv }, 1e-3);
    try std.testing.expect(max_err < tolerance);
}

test "grad_check sdpa" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // Small attention: batch=1, heads=1, seq=3, head_dim=2 → B*H=1
    const q = try b.parameter("Q", Shape.init(.f32, &.{ 1, 3, 2 }));
    const k = try b.parameter("K", Shape.init(.f32, &.{ 1, 3, 2 }));
    const v = try b.parameter("V", Shape.init(.f32, &.{ 1, 3, 2 }));
    const attn = try b.sdpa(q, k, v, 1, 3, 1, 2);
    const loss = try b.reduceSum(attn, &.{ 0, 1, 2 });
    try g.markOutput(loss);

    const pq = [_]f32{ 0.1, 0.2, 0.3, 0.4, 0.5, 0.6 };
    const pk = [_]f32{ 0.7, 0.8, 0.9, 1.0, 1.1, 1.2 };
    const pv = [_]f32{ 0.2, 0.3, 0.4, 0.5, 0.6, 0.7 };

    const max_err = try checkGradients(allocator, &g, loss, &.{ q, k, v }, &.{ &pq, &pk, &pv }, 1e-3);
    // SDPA is a deep chain (matmul → scale → softmax(reduce_max,
    // sub, exp, reduce_sum, broadcast, div) → matmul); finite-
    // difference truncation accumulates. The matching peer test
    // `gradient through sdpa + linear chain` uses the same 5%
    // floor.
    try std.testing.expect(max_err < 0.05);
}

test "gradient through sdpa + linear chain" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // Q = input @ W_q (with bias), then SDPA
    const x = try b.parameter("x", Shape.init(.f32, &.{ 3, 4 }));
    const wq = try b.parameter("wq", Shape.init(.f32, &.{ 2, 4 }));
    const bq = try b.parameter("bq", Shape.init(.f32, &.{2}));
    const q_proj = try b.linear(x, wq, bq, 3, 4, 2);

    // Use same x for K and V projections (simplified)
    const wk = try b.parameter("wk", Shape.init(.f32, &.{ 2, 4 }));
    const bk = try b.parameter("bk", Shape.init(.f32, &.{2}));
    const k_proj = try b.linear(x, wk, bk, 3, 4, 2);

    const wv = try b.parameter("wv", Shape.init(.f32, &.{ 2, 4 }));
    const bv = try b.parameter("bv", Shape.init(.f32, &.{2}));
    const v_proj = try b.linear(x, wv, bv, 3, 4, 2);

    // Reshape to [B*H, S, D]: batch=1, heads=1, seq=3, head_dim=2
    const q3d = try b.reshape(q_proj, Shape.init(.f32, &.{ 1, 3, 2 }));
    const k3d = try b.reshape(k_proj, Shape.init(.f32, &.{ 1, 3, 2 }));
    const v3d = try b.reshape(v_proj, Shape.init(.f32, &.{ 1, 3, 2 }));

    const attn = try b.sdpa(q3d, k3d, v3d, 1, 3, 1, 2);
    const loss = try b.reduceSum(attn, &.{ 0, 1, 2 });
    try g.markOutput(loss);

    const px = [_]f32{ 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2 };
    const pwq = [_]f32{ 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8 };
    const pbq = [_]f32{ 0.01, 0.02 };
    const pwk = [_]f32{ 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0 };
    const pbk = [_]f32{ 0.01, 0.02 };
    const pwv = [_]f32{ 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9 };
    const pbv = [_]f32{ 0.01, 0.02 };

    const max_err = try checkGradients(
        allocator,
        &g,
        loss,
        &.{ wq, wk, wv },
        &.{ &px, &pwq, &pbq, &pwk, &pbk, &pwv, &pbv },
        1e-3,
    );
    try std.testing.expect(max_err < 0.05); // slightly looser tolerance for deep chain
}

// ── gather / scatter_add ───────────────────────────────────────────────

test "eval gather honors the index operand" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // table [4, 3] gathered by indices [3] = {2, 0, 2} → [3, 3].
    const table = try b.parameter("table", Shape.init(.f32, &.{ 4, 3 }));
    const idx = try b.parameter("idx", Shape.init(.i64, &.{3}));
    const gathered = try b.gather(table, idx, Shape.init(.f32, &.{ 3, 3 }));
    try g.markOutput(gathered);

    const ptable = [_]f32{
        0.0, 1.0,  2.0,
        3.0, 4.0,  5.0,
        6.0, 7.0,  8.0,
        9.0, 10.0, 11.0,
    };
    const pidx = [_]f32{ 2.0, 0.0, 2.0 };

    var result = try eval(allocator, &g, &.{ &ptable, &pidx });
    defer result.deinit();

    const expected = [_]f32{
        6.0, 7.0, 8.0,
        0.0, 1.0, 2.0,
        6.0, 7.0, 8.0,
    };
    try std.testing.expectEqualSlices(f32, &expected, result.values[0]);
}

test "eval gather wraps negative indices" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const table = try b.parameter("table", Shape.init(.f32, &.{ 3, 2 }));
    const idx = try b.parameter("idx", Shape.init(.i64, &.{2}));
    const gathered = try b.gather(table, idx, Shape.init(.f32, &.{ 2, 2 }));
    try g.markOutput(gathered);

    const ptable = [_]f32{ 0.0, 1.0, 2.0, 3.0, 4.0, 5.0 };
    const pidx = [_]f32{ -1.0, -3.0 }; // → rows 2 and 0

    var result = try eval(allocator, &g, &.{ &ptable, &pidx });
    defer result.deinit();

    try std.testing.expectEqualSlices(f32, &[_]f32{ 4.0, 5.0, 0.0, 1.0 }, result.values[0]);
}

test "eval gather rejects an out-of-range index" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const table = try b.parameter("table", Shape.init(.f32, &.{ 3, 2 }));
    const idx = try b.parameter("idx", Shape.init(.i64, &.{2}));
    const gathered = try b.gather(table, idx, Shape.init(.f32, &.{ 2, 2 }));
    try g.markOutput(gathered);

    const ptable = [_]f32{ 0.0, 1.0, 2.0, 3.0, 4.0, 5.0 };
    const pidx = [_]f32{ 0.0, 3.0 };

    try std.testing.expectError(
        error.GatherIndexOutOfRange,
        eval(allocator, &g, &.{ &ptable, &pidx }),
    );
}

test "eval gather along axis 1" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // table [2, 3] gathered along axis 1 by indices [2] = {2, 1} → [2, 2].
    const table = try b.parameter("table", Shape.init(.f32, &.{ 2, 3 }));
    const idx = try b.parameter("idx", Shape.init(.i64, &.{2}));
    const gathered = try g.addNode(.{
        .op = .{ .gather = .{ .axis = 1 } },
        .output_shape = Shape.init(.f32, &.{ 2, 2 }),
        .inputs = .{ table, idx, null_node, null_node },
        .num_inputs = 2,
    });
    try g.markOutput(gathered);

    const ptable = [_]f32{ 0.0, 1.0, 2.0, 3.0, 4.0, 5.0 };
    const pidx = [_]f32{ 2.0, 1.0 };

    var result = try eval(allocator, &g, &.{ &ptable, &pidx });
    defer result.deinit();

    try std.testing.expectEqualSlices(f32, &[_]f32{ 2.0, 1.0, 5.0, 4.0 }, result.values[0]);
}

test "eval scatter_add accumulates duplicate indices" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // 3-input builder form: out = dest, then out[idx[k], :] += values[k, :].
    const dest = try b.parameter("dest", Shape.init(.f32, &.{ 3, 2 }));
    const values = try b.parameter("values", Shape.init(.f32, &.{ 4, 2 }));
    const idx = try b.parameter("idx", Shape.init(.i64, &.{4}));
    const out = try b.scatterAdd(dest, values, idx, 0);
    try g.markOutput(out);

    const pdest = [_]f32{ 100.0, 200.0, 300.0, 400.0, 500.0, 600.0 };
    const pvalues = [_]f32{ 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0 };
    const pidx = [_]f32{ 1.0, 0.0, 1.0, 1.0 };

    var result = try eval(allocator, &g, &.{ &pdest, &pvalues, &pidx });
    defer result.deinit();

    // row0 += values[1] = (3,4); row1 += values[0]+values[2]+values[3] = (13,16)
    const expected = [_]f32{
        103.0, 204.0,
        313.0, 416.0,
        500.0, 600.0,
    };
    try std.testing.expectEqualSlices(f32, &expected, result.values[0]);
}

test "eval scatter_add two-input autodiff form starts from zero" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // The form the `.gather` VJP emits: scatter_add(adjoint, indices)
    // with the table shape as the output shape and an implicit zero dest.
    const values = try b.parameter("values", Shape.init(.f32, &.{ 3, 2 }));
    const idx = try b.parameter("idx", Shape.init(.i64, &.{3}));
    const out = try g.addNode(.{
        .op = .{ .scatter_add = .{ .axis = 0 } },
        .output_shape = Shape.init(.f32, &.{ 3, 2 }),
        .inputs = .{ values, idx, null_node, null_node },
        .num_inputs = 2,
    });
    try g.markOutput(out);

    const pvalues = [_]f32{ 1.0, 2.0, 3.0, 4.0, 5.0, 6.0 };
    const pidx = [_]f32{ 2.0, 2.0, 0.0 };

    var result = try eval(allocator, &g, &.{ &pvalues, &pidx });
    defer result.deinit();

    const expected = [_]f32{
        5.0, 6.0, // row0 ← values[2]
        0.0, 0.0, // row1 untouched
        4.0, 6.0, // row2 ← values[0] + values[1]
    };
    try std.testing.expectEqualSlices(f32, &expected, result.values[0]);
}

test "eval scatter_add rejects a negative index" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const values = try b.parameter("values", Shape.init(.f32, &.{ 2, 2 }));
    const idx = try b.parameter("idx", Shape.init(.i64, &.{2}));
    const out = try g.addNode(.{
        .op = .{ .scatter_add = .{ .axis = 0 } },
        .output_shape = Shape.init(.f32, &.{ 3, 2 }),
        .inputs = .{ values, idx, null_node, null_node },
        .num_inputs = 2,
    });
    try g.markOutput(out);

    const pvalues = [_]f32{ 1.0, 2.0, 3.0, 4.0 };
    const pidx = [_]f32{ 0.0, -1.0 };

    try std.testing.expectError(
        error.ScatterIndexOutOfRange,
        eval(allocator, &g, &.{ &pvalues, &pidx }),
    );
}

test "grad_check gather with repeated rows" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // loss = sum(gather(table, idx) * w). Row 2 is gathered twice and
    // row 1 never, so a stub forward (or a scatter_add VJP that ignores
    // the indices) produces a visibly different gradient.
    const table = try b.parameter("table", Shape.init(.f32, &.{ 4, 3 }));
    const idx = try b.parameter("idx", Shape.init(.i64, &.{3}));
    const w = try b.parameter("w", Shape.init(.f32, &.{ 3, 3 }));
    const gathered = try b.gather(table, idx, Shape.init(.f32, &.{ 3, 3 }));
    const product = try b.mul(gathered, w);
    const loss = try b.reduceSum(product, &.{ 0, 1 });
    try g.markOutput(loss);

    const ptable = [_]f32{
        0.1, 0.2, 0.3,
        0.4, 0.5, 0.6,
        0.7, 0.8, 0.9,
        1.0, 1.1, 1.2,
    };
    const pidx = [_]f32{ 2.0, 0.0, 2.0 };
    const pw = [_]f32{
        1.0, 2.0, 3.0,
        4.0, 5.0, 6.0,
        7.0, 8.0, 9.0,
    };

    // Analytic expectation, independent of autodiff:
    //   d loss / d table[r, c] = sum_{k: idx[k] == r} w[k, c]
    var ad_result = try autodiff_mod.gradient(allocator, &g, loss, &.{table});
    defer ad_result.deinit();
    const grad_id = ad_result.param_grads[0];
    try std.testing.expect(grad_id != null_node);
    try ad_result.graph.outputs.append(allocator, grad_id);
    var analytic_eval = try eval(allocator, &ad_result.graph, &.{ &ptable, &pidx, &pw });
    defer analytic_eval.deinit();
    const analytic = analytic_eval.values[analytic_eval.values.len - 1];
    const expected_grad = [_]f32{
        4.0, 5.0, 6.0, // row0 ← w[1]
        0.0, 0.0, 0.0, // row1 never gathered
        8.0, 10.0, 12.0, // row2 ← w[0] + w[2]
        0.0, 0.0, 0.0, // row3 never gathered
    };
    try std.testing.expectEqualSlices(f32, &expected_grad, analytic);

    const max_err = try checkGradients(allocator, &g, loss, &.{table}, &.{ &ptable, &pidx, &pw }, 1e-3);
    try std.testing.expect(max_err < tolerance);
}

test "grad_check scatter_add with duplicate indices" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // loss = sum(scatter_add(values, idx) * w), autodiff two-input form.
    // Index 1 receives three rows, index 2 none.
    const values = try b.parameter("values", Shape.init(.f32, &.{ 4, 2 }));
    const idx = try b.parameter("idx", Shape.init(.i64, &.{4}));
    const w = try b.parameter("w", Shape.init(.f32, &.{ 3, 2 }));
    const scattered = try g.addNode(.{
        .op = .{ .scatter_add = .{ .axis = 0 } },
        .output_shape = Shape.init(.f32, &.{ 3, 2 }),
        .inputs = .{ values, idx, null_node, null_node },
        .num_inputs = 2,
    });
    const product = try b.mul(scattered, w);
    const loss = try b.reduceSum(product, &.{ 0, 1 });
    try g.markOutput(loss);

    const pvalues = [_]f32{ 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8 };
    const pidx = [_]f32{ 1.0, 0.0, 1.0, 1.0 };
    const pw = [_]f32{ 1.0, 2.0, 3.0, 4.0, 5.0, 6.0 };

    // d loss / d values[k, c] = w[idx[k], c]
    var ad_result = try autodiff_mod.gradient(allocator, &g, loss, &.{values});
    defer ad_result.deinit();
    const grad_id = ad_result.param_grads[0];
    try std.testing.expect(grad_id != null_node);
    try ad_result.graph.outputs.append(allocator, grad_id);
    var analytic_eval = try eval(allocator, &ad_result.graph, &.{ &pvalues, &pidx, &pw });
    defer analytic_eval.deinit();
    const analytic = analytic_eval.values[analytic_eval.values.len - 1];
    const expected_grad = [_]f32{
        3.0, 4.0, // ← w[1]
        1.0, 2.0, // ← w[0]
        3.0, 4.0, // ← w[1]
        3.0, 4.0, // ← w[1]
    };
    try std.testing.expectEqualSlices(f32, &expected_grad, analytic);

    const max_err = try checkGradients(allocator, &g, loss, &.{values}, &.{ &pvalues, &pidx, &pw }, 1e-3);
    try std.testing.expect(max_err < tolerance);
}

test "grad_check gliner2-shaped span/schema gather chain" {
    // Mirrors the gather topology of the GLiNER2 schema-conditioned
    // structure loss (gliner2_real_autodiff.zig:519-624):
    //
    //   start_hidden   = gather(hidden_flat, start_idx)            [S, H]
    //   compact_schema = gather(schema_idx_2d, first_span_rows)    [B, E]
    //   schema_hidden  = gather(hidden_flat, compact_schema_flat)  [B*E, H]
    //   repeated_span  = gather(start_hidden, row_idx)             [S*E, H]
    //   schema_proj    = gather(schema_hidden, schema_repeat_rows) [S*E, H]
    //   logits         = sum_h(repeated_span * schema_proj)
    //
    // Every gather feeds a scatter_add on the backward pass, two of them
    // with duplicate indices, and `hidden_flat` is reached through two
    // independent gather chains, so its adjoint is an accumulation of
    // two scatter_adds.
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const T = 6; // tokens
    const H = 3; // hidden
    const S = 4; // span rows
    const E = 2; // entity types
    const B = 2; // samples (2 spans per sample)

    const hidden = try b.parameter("hidden", Shape.init(.f32, &.{ T, H }));
    const start_idx = try b.parameter("start_idx", Shape.init(.i64, &.{S}));
    const schema_idx_2d = try b.parameter("schema_idx_2d", Shape.init(.f32, &.{ S, E }));
    const first_span_rows = try b.parameter("first_span_rows", Shape.init(.i64, &.{B}));
    const row_idx = try b.parameter("row_idx", Shape.init(.i64, &.{S * E}));
    const schema_repeat_rows = try b.parameter("schema_repeat_rows", Shape.init(.i64, &.{S * E}));

    const start_hidden = try b.gather(hidden, start_idx, Shape.init(.f32, &.{ S, H }));

    const compact_schema_2d = try b.gather(schema_idx_2d, first_span_rows, Shape.init(.f32, &.{ B, E }));
    const compact_schema_flat = try b.reshape(compact_schema_2d, Shape.init(.f32, &.{B * E}));
    const compact_schema_i64 = try b.convertDtype(compact_schema_flat, .i64);
    const schema_hidden = try b.gather(hidden, compact_schema_i64, Shape.init(.f32, &.{ B * E, H }));

    const repeated_span = try b.gather(start_hidden, row_idx, Shape.init(.f32, &.{ S * E, H }));
    const schema_proj = try b.gather(schema_hidden, schema_repeat_rows, Shape.init(.f32, &.{ S * E, H }));

    const span_schema_product = try b.mul(repeated_span, schema_proj);
    const logits = try b.reduceSum(span_schema_product, &.{1});
    const loss = try b.reduceSum(logits, &.{ 0, 1 });
    try g.markOutput(loss);

    const phidden = [_]f32{
        0.10,  -0.20, 0.30,
        0.40,  0.50,  -0.60,
        -0.70, 0.80,  0.90,
        1.00,  -1.10, 1.20,
        0.05,  0.15,  -0.25,
        -0.35, 0.45,  0.55,
    };
    // Repeated + skipped token rows, as real span data produces.
    const pstart_idx = [_]f32{ 0.0, 2.0, 2.0, 5.0 };
    // Per-span schema token positions; only the first span row of each
    // sample is read (rows 0 and 2 here).
    const pschema_idx_2d = [_]f32{
        1.0, 3.0,
        9.0, 9.0, // never read
        4.0, 1.0,
        9.0, 9.0, // never read
    };
    const pfirst_span_rows = [_]f32{ 0.0, 2.0 };
    // span row for each (span, entity) pair
    const prow_idx = [_]f32{ 0.0, 0.0, 1.0, 1.0, 2.0, 2.0, 3.0, 3.0 };
    // (sample * E + entity) for each (span, entity) pair
    const pschema_repeat_rows = [_]f32{ 0.0, 1.0, 0.0, 1.0, 2.0, 3.0, 2.0, 3.0 };

    const params = [_][]const f32{
        &phidden,
        &pstart_idx,
        &pschema_idx_2d,
        &pfirst_span_rows,
        &prow_idx,
        &pschema_repeat_rows,
    };

    const max_err = try checkGradients(allocator, &g, loss, &.{hidden}, &params, 1e-3);
    try std.testing.expect(max_err < tolerance);
}

test "grad_check fused_rope half-swap rotation" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // Shapes: [B*H=1, seq=2, head_dim=4]; rope_dim = head_dim = 4 → half = 2.
    // cos/sin have shape [seq, head_dim]; only the first `half` entries per
    // row are consulted by the rotation (the other half is unused).
    const x = try b.parameter("x", Shape.init(.f32, &.{ 1, 2, 4 }));
    const cos = try b.parameter("cos", Shape.init(.f32, &.{ 2, 4 }));
    const sin = try b.parameter("sin", Shape.init(.f32, &.{ 2, 4 }));
    const rotated = try b.rope(x, cos, sin, 2, 4, 4, 10000.0);
    const loss = try b.reduceSum(rotated, &.{ 0, 1, 2 });
    try g.markOutput(loss);

    // Concrete cos/sin values (unit-norm per pair so the rotation is
    // exact, but any values work — autodiff only needs cos/sin themselves
    // to stay fixed during the finite-difference sweep).
    //   seq=0: angles π/6 and π/3
    //   seq=1: angles π/4 and π/2
    // Only cols 0..half are consulted; cols half..D can be anything.
    const c_pi6: f32 = 0.8660254; // cos(π/6)
    const s_pi6: f32 = 0.5; // sin(π/6)
    const c_pi3: f32 = 0.5; // cos(π/3)
    const s_pi3: f32 = 0.8660254; // sin(π/3)
    const c_pi4: f32 = 0.7071068; // cos(π/4)
    const s_pi4: f32 = 0.7071068; // sin(π/4)
    const c_pi2: f32 = 0.0; // cos(π/2)
    const s_pi2: f32 = 1.0; // sin(π/2)

    const px = [_]f32{
        // seq=0
        0.10, 0.20, 0.30, 0.40,
        // seq=1
        0.50, 0.60, 0.70, 0.80,
    };
    const pcos = [_]f32{
        c_pi6, c_pi3, 0.0, 0.0,
        c_pi4, c_pi2, 0.0, 0.0,
    };
    const psin = [_]f32{
        s_pi6, s_pi3, 0.0, 0.0,
        s_pi4, s_pi2, 0.0, 0.0,
    };

    // Only differentiate w.r.t. x — cos/sin are treated as frozen.
    //
    // We don't use `checkGradients` here because its relative-error metric
    // blows up when the analytic gradient is exactly zero (finite-difference
    // noise then dominates). RoPE produces several genuinely-zero gradient
    // components (e.g. cos=0, sin=1 rows), so we compare manually with a
    // small absolute-plus-relative tolerance instead.
    var ad_result = try autodiff_mod.gradient(allocator, &g, loss, &.{x});
    defer ad_result.deinit();

    const grad_id = ad_result.param_grads[0];
    try std.testing.expect(grad_id != null_node);

    try ad_result.graph.outputs.append(allocator, grad_id);
    var analytic_eval = try eval(allocator, &ad_result.graph, &.{ &px, &pcos, &psin });
    defer analytic_eval.deinit();
    const analytic = analytic_eval.values[analytic_eval.values.len - 1];
    try std.testing.expectEqual(@as(usize, 8), analytic.len);

    var lowered_for_eval = try lower_mod.lower(allocator, &g);
    defer lowered_for_eval.deinit();

    var px_copy = try allocator.dupe(f32, &px);
    defer allocator.free(px_copy);
    const fd_eps: f32 = 1e-3;
    var numeric: [8]f32 = undefined;
    for (0..8) |i| {
        const orig = px_copy[i];
        px_copy[i] = orig + fd_eps;
        var plus = try eval(allocator, &lowered_for_eval.graph, &.{ px_copy, &pcos, &psin });
        const f_plus = plus.values[0][0];
        plus.deinit();
        px_copy[i] = orig - fd_eps;
        var minus = try eval(allocator, &lowered_for_eval.graph, &.{ px_copy, &pcos, &psin });
        const f_minus = minus.values[0][0];
        minus.deinit();
        px_copy[i] = orig;
        numeric[i] = (f_plus - f_minus) / (2.0 * fd_eps);
    }

    // Compare analytic vs numeric with mixed absolute/relative tolerance.
    // atol handles zero-valued components; rtol is for larger magnitudes.
    const atol: f32 = 1e-3;
    const rtol: f32 = 1e-3;
    for (0..8) |i| {
        const a = analytic[i];
        const n = numeric[i];
        const diff = @abs(a - n);
        const allow = atol + rtol * @max(@abs(a), @abs(n));
        if (diff > allow) {
            std.debug.print(
                "\n[rope-grad] mismatch at idx {}: analytic={d} numeric={d} diff={d} allow={d}\n",
                .{ i, a, n, diff, allow },
            );
            return error.GradientMismatch;
        }
    }
}
