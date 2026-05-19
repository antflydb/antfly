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

// Constant folding pass.
//
// Walks the graph in topological order. For each node whose inputs are
// all constants, evaluates the node on the CPU and replaces it with a
// constant node containing the result. This catches scaling factors
// (1/sqrt(head_dim)), shape constants, and literal computations that are
// invariant across batches.
//
// Only folds pure constant subexpressions — parameter-dependent folding
// requires weight data and is deferred to a future lazy first-execution
// pass.
//
// Uses the same redirect-map + rebuild pattern as fuse.zig.

const std = @import("std");
const graph_mod = @import("../graph.zig");
const node_mod = @import("../node.zig");
const shape_mod = @import("../shape.zig");
const tensor_eval = @import("../tensor_eval.zig");

const Graph = graph_mod.Graph;
const Node = node_mod.Node;
const NodeId = node_mod.NodeId;
const null_node = node_mod.null_node;
const OpCode = node_mod.OpCode;
const Shape = shape_mod.Shape;

pub const ConstFoldResult = struct {
    graph: Graph,
    /// old_id -> new_id mapping. Caller must free.
    id_map: []NodeId,
    /// Number of nodes folded into constants.
    num_folded: u32,

    pub fn deinit(self: *ConstFoldResult) void {
        const allocator = self.graph.allocator;
        self.graph.deinit();
        allocator.free(self.id_map);
    }
};

/// Apply constant folding to the graph.
/// Returns a new graph with constant subexpressions evaluated.
pub fn fold(allocator: std.mem.Allocator, graph: *const Graph) !ConstFoldResult {
    const count = graph.nodeCount();
    if (count == 0) return .{
        .graph = Graph.init(allocator),
        .id_map = try allocator.alloc(NodeId, 0),
        .num_folded = 0,
    };

    // redirect[i] = i means "keep original node".
    // redirect[i] = j (j != i) means "node i was folded to constant j".
    const redirect = try allocator.alloc(NodeId, count);
    defer allocator.free(redirect);
    for (0..count) |i| redirect[i] = @intCast(i);

    // Track which nodes are constant (original constants + folded nodes).
    const is_const = try allocator.alloc(bool, count);
    defer allocator.free(is_const);
    @memset(is_const, false);

    // Mark original constant nodes.
    for (0..count) |i| {
        switch (graph.node(@intCast(i)).op) {
            .constant => is_const[i] = true,
            else => {},
        }
    }

    // Work graph — we append new constant nodes here when folding.
    var work = try cloneGraph(allocator, graph);
    errdefer work.deinit();

    var num_folded: u32 = 0;

    // Walk in topological order (append-only graph guarantees this).
    for (0..count) |i| {
        const id: NodeId = @intCast(i);
        const n = graph.node(id);

        // Skip constants and parameters — nothing to fold.
        switch (n.op) {
            .constant, .parameter => continue,
            else => {},
        }

        // Check if all inputs are constants (after following redirects).
        const inputs = n.getInputs();
        if (inputs.len == 0) continue;

        // Check is_const at the ORIGINAL input id, not the resolved one
        // — after folding a predecessor we mark its original slot as
        // constant, so downstream nodes whose inputs were just folded
        // can also fold in the same pass (this lets `neg(sqrt(c))`
        // collapse to a single constant in one fold() call).
        var all_const = true;
        for (inputs) |inp| {
            if (inp == null_node) {
                all_const = false;
                break;
            }
            if (inp >= is_const.len or !is_const[inp]) {
                all_const = false;
                break;
            }
        }
        if (!all_const) continue;

        // Special paths for ops whose natural output dtype is something
        // other than f32 (so the f32-only `evalNode` path can't
        // represent the result). These read the inputs in their native
        // dtypes via `constantDataAs` and write output bytes through
        // `internConstantBytes`.
        switch (std.meta.activeTag(n.op)) {
            .convert_dtype => {
                const target = n.op.convert_dtype.target;
                const src_id = resolve(redirect, n.inputs[0]);
                const src_shape = work.node(src_id).output_shape;
                if (target != src_shape.dtype) {
                    if (try foldConvertDtype(allocator, &work, n, src_id, src_shape, target)) |new_id| {
                        redirect[i] = new_id;
                        is_const[i] = true;
                        num_folded += 1;
                        continue;
                    }
                }
            },
            .gather => {
                if (try foldGather(allocator, &work, n, redirect)) |new_id| {
                    redirect[i] = new_id;
                    is_const[i] = true;
                    num_folded += 1;
                    continue;
                }
            },
            .argmax => {
                if (try foldArgmax(allocator, &work, n, redirect)) |new_id| {
                    redirect[i] = new_id;
                    is_const[i] = true;
                    num_folded += 1;
                    continue;
                }
            },
            else => {},
        }

        // Try to evaluate this node.
        if (try evalNode(allocator, &work, n, redirect)) |result| {
            defer allocator.free(result);

            // Add a new constant node to the work graph.
            const loc = try work.internConstant(result);
            const new_id = try work.addNode(.{
                .op = .{ .constant = .{ .data_offset = loc.offset, .data_len = loc.len } },
                .output_shape = n.output_shape,
            });

            redirect[i] = new_id;
            is_const[i] = true;

            num_folded += 1;
        }
    }

    // Rebuild graph following redirects (also performs implicit DCE).
    const result = try rebuild(allocator, &work, redirect, count, num_folded);
    work.deinit();
    return result;
}

// ── Evaluation ────────────────────────────────────────────────────────

/// Try to evaluate a node on constant inputs. Returns the result data
/// if the op is supported for constant folding, or null otherwise.
fn evalNode(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    n: *const Node,
    redirect: []const NodeId,
) !?[]f32 {
    // Only fold ops with static output shapes.
    const num_elements_i64 = n.output_shape.numElements() orelse return null;
    if (num_elements_i64 <= 0) return null;
    const num_elements: usize = @intCast(num_elements_i64);

    // Load input constant data.
    const in0_id = resolve(redirect, n.inputs[0]);
    const in0_data = getConstData(graph, in0_id) orelse return null;
    const in0_shape = graph.node(in0_id).output_shape;

    switch (n.op) {
        // ── Elementwise unary ──────────────────────────────────
        .neg => return try evalUnary(allocator, in0_data, num_elements, struct {
            fn f(x: f32) f32 {
                return -x;
            }
        }.f),
        .sqrt => return try evalUnary(allocator, in0_data, num_elements, struct {
            fn f(x: f32) f32 {
                return @sqrt(x);
            }
        }.f),
        .rsqrt => return try evalUnary(allocator, in0_data, num_elements, struct {
            fn f(x: f32) f32 {
                return 1.0 / @sqrt(x);
            }
        }.f),
        .exp => return try evalUnary(allocator, in0_data, num_elements, struct {
            fn f(x: f32) f32 {
                return @exp(x);
            }
        }.f),
        .log => return try evalUnary(allocator, in0_data, num_elements, struct {
            fn f(x: f32) f32 {
                return @log(x);
            }
        }.f),
        .abs => return try evalUnary(allocator, in0_data, num_elements, struct {
            fn f(x: f32) f32 {
                return @abs(x);
            }
        }.f),

        .tanh => return try evalUnary(allocator, in0_data, num_elements, struct {
            fn f(x: f32) f32 {
                return std.math.tanh(x);
            }
        }.f),
        .sin => return try evalUnary(allocator, in0_data, num_elements, struct {
            fn f(x: f32) f32 {
                return @sin(x);
            }
        }.f),
        .cos => return try evalUnary(allocator, in0_data, num_elements, struct {
            fn f(x: f32) f32 {
                return @cos(x);
            }
        }.f),

        // ── Elementwise binary ─────────────────────────────────
        .add, .sub, .mul, .div => {
            if (n.num_inputs < 2) return null;
            const in1_id = resolve(redirect, n.inputs[1]);
            const in1_data = getConstData(graph, in1_id) orelse return null;
            const in1_shape = graph.node(in1_id).output_shape;

            // Equal-shape fast path.
            if (shapesEqual(in0_shape, in1_shape) and in0_data.len == in1_data.len) {
                const result = try allocator.alloc(f32, num_elements);
                errdefer allocator.free(result);
                for (0..num_elements) |j| {
                    result[j] = switch (n.op) {
                        .add => in0_data[j] + in1_data[j],
                        .sub => in0_data[j] - in1_data[j],
                        .mul => in0_data[j] * in1_data[j],
                        .div => if (in1_data[j] != 0) in0_data[j] / in1_data[j] else return null,
                        else => unreachable,
                    };
                }
                return result;
            }

            // Scalar-broadcast path. Only fold when one input is a single
            // element — covers the common pattern `mul(x, scalar_const)`
            // after const-fold collapses the scalar's broadcast wrapper.
            // General N-D broadcasting is left out: it requires agreement
            // on broadcast semantics that the binary builder does not
            // currently fix (binaryOp picks the larger shape verbatim).
            if (in0_data.len == 1) {
                const scalar = in0_data[0];
                if (in1_data.len != num_elements) return null;
                const result = try allocator.alloc(f32, num_elements);
                errdefer allocator.free(result);
                for (0..num_elements) |j| {
                    result[j] = switch (n.op) {
                        .add => scalar + in1_data[j],
                        .sub => scalar - in1_data[j],
                        .mul => scalar * in1_data[j],
                        .div => if (in1_data[j] != 0) scalar / in1_data[j] else return null,
                        else => unreachable,
                    };
                }
                return result;
            }
            if (in1_data.len == 1) {
                const scalar = in1_data[0];
                if (in0_data.len != num_elements) return null;
                const result = try allocator.alloc(f32, num_elements);
                errdefer allocator.free(result);
                for (0..num_elements) |j| {
                    result[j] = switch (n.op) {
                        .add => in0_data[j] + scalar,
                        .sub => in0_data[j] - scalar,
                        .mul => in0_data[j] * scalar,
                        .div => if (scalar != 0) in0_data[j] / scalar else return null,
                        else => unreachable,
                    };
                }
                return result;
            }
            return null;
        },

        // ── Reshape (data unchanged) ───────────────────────────
        .reshape => {
            if (in0_data.len != num_elements) return null;
            return try allocator.dupe(f32, in0_data);
        },

        // ── Transpose (permute axes) ───────────────────────────
        .transpose => |attrs| {
            return try evalTranspose(allocator, in0_data, in0_shape, n.output_shape, attrs);
        },

        // ── Broadcast (replicate along broadcast axes) ────────
        .broadcast_in_dim => |attrs| {
            return try evalBroadcast(allocator, in0_data, in0_shape, n.output_shape, attrs);
        },

        // ── Slice (extract sub-tensor) ─────────────────────────
        .slice => |attrs| {
            return try evalSlice(allocator, in0_data, in0_shape, n.output_shape, attrs);
        },

        // ── Reductions ─────────────────────────────────────────
        .reduce_sum => |attrs| {
            return try evalReduce(allocator, in0_data, in0_shape, n.output_shape, attrs, .sum);
        },
        .reduce_max => |attrs| {
            return try evalReduce(allocator, in0_data, in0_shape, n.output_shape, attrs, .max);
        },
        .reduce_mean => |attrs| {
            return try evalReduce(allocator, in0_data, in0_shape, n.output_shape, attrs, .mean);
        },

        // ── Concat (binary) ────────────────────────────────────
        .concat_prim => |attrs| {
            if (n.num_inputs < 2) return null;
            const in1_id = resolve(redirect, n.inputs[1]);
            const in1_data = getConstData(graph, in1_id) orelse return null;
            const in1_shape = graph.node(in1_id).output_shape;
            return try evalConcat(allocator, in0_data, in0_shape, in1_data, in1_shape, n.output_shape, attrs);
        },

        // ── where_select (constant condition) ─────────────────
        .where_select => {
            if (n.num_inputs < 3) return null;
            const cond_id = resolve(redirect, n.inputs[0]);
            const true_id = resolve(redirect, n.inputs[1]);
            const false_id = resolve(redirect, n.inputs[2]);
            // The "in0" already loaded above is the condition. We need the
            // values too.
            const true_data = getConstData(graph, true_id) orelse return null;
            const false_data = getConstData(graph, false_id) orelse return null;
            // Condition can be bool_ stored in f32 by const-fold (0.0/1.0)
            // or any other constant the importer left as f32 sentinels.
            const cond_data = getConstData(graph, cond_id) orelse return null;

            // For correctness, only fold when condition is a literal
            // single-element constant — that decisively picks one branch.
            if (cond_data.len == 1) {
                const pick = if (cond_data[0] != 0.0) true_data else false_data;
                return try allocator.dupe(f32, pick);
            }
            // Per-element fold when all three are full-shaped tensors.
            if (cond_data.len == num_elements and true_data.len == num_elements and false_data.len == num_elements) {
                const result = try allocator.alloc(f32, num_elements);
                errdefer allocator.free(result);
                for (0..num_elements) |j| {
                    result[j] = if (cond_data[j] != 0.0) true_data[j] else false_data[j];
                }
                return result;
            }
            return null;
        },

        // ── Type conversion (f32 → f32 identity) ──────────────
        .convert_dtype => |attrs| {
            // Only fold f32→f32 (identity). Other conversions need to
            // store data with a different element size, which the f32-only
            // constant pool does not yet support.
            if (attrs.target == .f32 and in0_shape.dtype == .f32) {
                return try allocator.dupe(f32, in0_data);
            }
            return null;
        },

        else => return null,
    }
}

fn evalConcat(
    allocator: std.mem.Allocator,
    a_data: []const f32,
    a_shape: Shape,
    b_data: []const f32,
    b_shape: Shape,
    out_shape: Shape,
    attrs: node_mod.ConcatAttrs,
) !?[]f32 {
    if (a_shape.dtype != .f32 or b_shape.dtype != .f32 or out_shape.dtype != .f32) return null;
    return tensor_eval.evalConcat(f32, allocator, a_data, a_shape, b_data, b_shape, out_shape, attrs);
}

// ── convert_dtype folding ─────────────────────────────────────────────

/// Fold a `convert_dtype` node whose target dtype differs from the
/// source. Reads the source as its native type, converts each element,
/// and stores the result through the byte-typed constant pool API.
/// Returns the new constant node's id, or null if the dtype combination
/// is unsupported for compile-time folding.
fn foldConvertDtype(
    allocator: std.mem.Allocator,
    graph: *Graph,
    n: *const Node,
    src_id: NodeId,
    src_shape: Shape,
    target: shape_mod.DType,
) !?NodeId {
    if (src_id == null_node or src_id >= graph.nodeCount()) return null;
    const src_node = graph.node(src_id);
    const src_attrs = switch (src_node.op) {
        .constant => |a| a,
        else => return null,
    };
    const num_elements = src_shape.numElements() orelse return null;
    if (num_elements <= 0) return null;
    const n_elems: usize = @intCast(num_elements);

    const src_dtype = src_shape.dtype;
    const src_bytes_len: u32 = @intCast(@as(usize, src_attrs.data_len) * src_dtype.byteSize());
    const src_bytes = graph.constantBytes(src_attrs.data_offset, src_bytes_len);
    const out_bytes = try allocator.alloc(u8, n_elems * target.byteSize());
    defer allocator.free(out_bytes);

    if (!try convertDtypeBytes(allocator, src_bytes, src_dtype, out_bytes, target, n_elems)) return null;

    const loc = try graph.internConstantBytes(out_bytes, target);
    return try graph.addNode(.{
        .op = .{ .constant = .{ .data_offset = loc.offset, .data_len = loc.len } },
        .output_shape = n.output_shape,
    });
}

/// Element-by-element dtype conversion. Routes bf16 through an f32
/// staging buffer (bf16 = top 16 bits of the f32 representation; no
/// native Zig type for it, so we manipulate u16 values explicitly).
/// On success `out_bytes` is filled with `count` converted elements.
fn convertDtypeBytes(
    allocator: std.mem.Allocator,
    src_bytes: []const u8,
    src_dtype: shape_mod.DType,
    out_bytes: []u8,
    target: shape_mod.DType,
    count: usize,
) !bool {
    // Identity bf16↔bf16: just memcpy.
    if (src_dtype == .bf16 and target == .bf16) {
        @memcpy(out_bytes, src_bytes);
        return true;
    }

    // bf16 → other: decode to f32, then dispatch via the existing
    // typed path with src=f32.
    if (src_dtype == .bf16) {
        const staging = try allocator.alloc(f32, count);
        defer allocator.free(staging);
        decodeBf16ToF32(src_bytes, staging);
        return convertFromTyped(f32, std.mem.sliceAsBytes(staging), out_bytes, target, count);
    }

    // other → bf16: decode src to f32 staging, then truncate-round
    // each f32 to its top 16 bits.
    if (target == .bf16) {
        const staging = try allocator.alloc(f32, count);
        defer allocator.free(staging);
        const staging_bytes = std.mem.sliceAsBytes(staging);
        const ok = try convertDtypeBytes(allocator, src_bytes, src_dtype, staging_bytes, .f32, count);
        if (!ok) return false;
        encodeF32ToBf16(staging, out_bytes);
        return true;
    }

    return switch (src_dtype) {
        .f32 => convertFromTyped(f32, src_bytes, out_bytes, target, count),
        .f64 => convertFromTyped(f64, src_bytes, out_bytes, target, count),
        .f16 => convertFromTyped(f16, src_bytes, out_bytes, target, count),
        .i8 => convertFromTyped(i8, src_bytes, out_bytes, target, count),
        .i16 => convertFromTyped(i16, src_bytes, out_bytes, target, count),
        .i32 => convertFromTyped(i32, src_bytes, out_bytes, target, count),
        .i64 => convertFromTyped(i64, src_bytes, out_bytes, target, count),
        .u8 => convertFromTyped(u8, src_bytes, out_bytes, target, count),
        .bool_ => convertFromTyped(u8, src_bytes, out_bytes, target, count),
        .bf16 => unreachable, // already handled above
    };
}

/// bf16 → f32: shift the 16 stored bits into the upper half of an
/// f32 word.
fn decodeBf16ToF32(src_bytes: []const u8, out: []f32) void {
    const src_aligned: [*]align(@alignOf(u16)) const u8 = @alignCast(src_bytes.ptr);
    const src = @as([*]const u16, @ptrCast(src_aligned))[0..out.len];
    for (src, out) |w, *o| {
        const bits: u32 = @as(u32, w) << 16;
        o.* = @bitCast(bits);
    }
}

/// f32 → bf16: round-to-nearest-even by adding 0x7FFF + LSB before
/// truncating to the upper 16 bits.
fn encodeF32ToBf16(src: []const f32, out_bytes: []u8) void {
    const dst_aligned: [*]align(@alignOf(u16)) u8 = @alignCast(out_bytes.ptr);
    const dst = @as([*]u16, @ptrCast(dst_aligned))[0..src.len];
    for (src, dst) |v, *o| {
        const bits: u32 = @bitCast(v);
        const lsb: u32 = (bits >> 16) & 1;
        const rounded = bits +% 0x7FFF +% lsb;
        o.* = @truncate(rounded >> 16);
    }
}

fn convertFromTyped(
    comptime Src: type,
    src_bytes: []const u8,
    out_bytes: []u8,
    target: shape_mod.DType,
    count: usize,
) bool {
    // Align-cast back to the natural alignment of `Src` — the constant
    // pool guarantees `constant_pool_alignment` (8 bytes) at every
    // interned offset, so this is sound for any dtype the IR knows.
    const src_aligned: [*]align(@alignOf(Src)) const u8 = @alignCast(src_bytes.ptr);
    const src = @as([*]const Src, @ptrCast(src_aligned))[0..count];
    return switch (target) {
        .f32 => writeConverted(Src, f32, src, out_bytes),
        .f64 => writeConverted(Src, f64, src, out_bytes),
        .f16 => writeConverted(Src, f16, src, out_bytes),
        .i8 => writeConverted(Src, i8, src, out_bytes),
        .i16 => writeConverted(Src, i16, src, out_bytes),
        .i32 => writeConverted(Src, i32, src, out_bytes),
        .i64 => writeConverted(Src, i64, src, out_bytes),
        .u8 => writeConverted(Src, u8, src, out_bytes),
        .bool_ => writeBool(Src, src, out_bytes),
        .bf16 => false,
    };
}

fn writeConverted(comptime Src: type, comptime Dst: type, src: []const Src, out_bytes: []u8) bool {
    const dst_aligned: [*]align(@alignOf(Dst)) u8 = @alignCast(out_bytes.ptr);
    const dst = @as([*]Dst, @ptrCast(dst_aligned))[0..src.len];
    const src_is_float = @typeInfo(Src) == .float;
    const dst_is_float = @typeInfo(Dst) == .float;
    for (src, dst) |v, *o| {
        if (src_is_float and dst_is_float) {
            o.* = @floatCast(v);
        } else if (src_is_float and !dst_is_float) {
            // Float → integer: truncation toward zero (matches @intFromFloat).
            o.* = @intFromFloat(v);
        } else if (!src_is_float and dst_is_float) {
            o.* = @floatFromInt(v);
        } else {
            o.* = @intCast(v);
        }
    }
    return true;
}

fn writeBool(comptime Src: type, src: []const Src, out_bytes: []u8) bool {
    // bool_ is stored as a u8 (one byte per element, 0 or 1).
    if (out_bytes.len != src.len) return false;
    const src_is_float = @typeInfo(Src) == .float;
    for (src, 0..) |v, i| {
        const truthy: bool = if (src_is_float)
            v != 0.0
        else
            v != 0;
        out_bytes[i] = if (truthy) 1 else 0;
    }
    return true;
}

// ── gather / argmax folding ───────────────────────────────────────────

/// Fold `gather(table, indices)` along `axis`. Both inputs must be
/// constants. Output's dtype = table's dtype, output's shape comes
/// from `n.output_shape`. Handles the standard case of rank-1
/// `indices` along `axis`; mismatching ranks fall through.
fn foldGather(
    allocator: std.mem.Allocator,
    graph: *Graph,
    n: *const Node,
    redirect: []const NodeId,
) !?NodeId {
    if (n.num_inputs < 2) return null;
    const table_id = resolve(redirect, n.inputs[0]);
    const idx_id = resolve(redirect, n.inputs[1]);
    if (table_id == null_node or idx_id == null_node) return null;
    if (table_id >= graph.nodeCount() or idx_id >= graph.nodeCount()) return null;

    const table_node = graph.node(table_id);
    const idx_node = graph.node(idx_id);
    const table_attrs = switch (table_node.op) {
        .constant => |a| a,
        else => return null,
    };
    const idx_attrs = switch (idx_node.op) {
        .constant => |a| a,
        else => return null,
    };
    const table_shape = table_node.output_shape;
    const idx_shape = idx_node.output_shape;
    const out_shape = n.output_shape;
    if (table_shape.dtype != out_shape.dtype) return null;
    const attrs = n.op.gather;
    const axis = attrs.axis;
    const rank = table_shape.rank();
    if (axis >= rank) return null;
    // Only handle rank-1 indices for now — that's the canonical
    // embedding-lookup / shape-arithmetic shape and avoids tricky
    // multi-dim index broadcasting.
    if (idx_shape.rank() != 1) return null;
    const idx_count: usize = blk: {
        const ie = idx_shape.numElements() orelse return null;
        if (ie < 0) return null;
        break :blk @intCast(ie);
    };
    const out_elements: usize = blk: {
        const oe = out_shape.numElements() orelse return null;
        if (oe < 0) return null;
        break :blk @intCast(oe);
    };

    // Read indices (i32 or i64).
    const indices_buf = try allocator.alloc(i64, idx_count);
    defer allocator.free(indices_buf);
    switch (idx_shape.dtype) {
        .i32 => {
            const src = graph.constantDataAs(i32, idx_attrs.data_offset, idx_attrs.data_len);
            if (src.len != idx_count) return null;
            for (src, indices_buf) |v, *o| o.* = v;
        },
        .i64 => {
            const src = graph.constantDataAs(i64, idx_attrs.data_offset, idx_attrs.data_len);
            if (src.len != idx_count) return null;
            @memcpy(indices_buf, src);
        },
        .i8 => {
            const src = graph.constantDataAs(i8, idx_attrs.data_offset, idx_attrs.data_len);
            if (src.len != idx_count) return null;
            for (src, indices_buf) |v, *o| o.* = v;
        },
        .i16 => {
            const src = graph.constantDataAs(i16, idx_attrs.data_offset, idx_attrs.data_len);
            if (src.len != idx_count) return null;
            for (src, indices_buf) |v, *o| o.* = v;
        },
        .u8 => {
            const src = graph.constantDataAs(u8, idx_attrs.data_offset, idx_attrs.data_len);
            if (src.len != idx_count) return null;
            for (src, indices_buf) |v, *o| o.* = v;
        },
        else => return null,
    }

    // Validate index range.
    const axis_dim = table_shape.dim(axis);
    if (axis_dim <= 0) return null;
    for (indices_buf) |idx| {
        if (idx < 0 or idx >= axis_dim) return null;
    }

    // Per-element copy through stride math.
    const table_elem_bytes = table_shape.dtype.byteSize();
    const table_bytes_len: u32 = @intCast((table_shape.numElements() orelse return null) * @as(i64, @intCast(table_elem_bytes)));
    const table_bytes = graph.constantBytes(table_attrs.data_offset, table_bytes_len);

    const out_bytes = try allocator.alloc(u8, out_elements * table_elem_bytes);
    defer allocator.free(out_bytes);

    // Split shape around the gathered axis. inner_size = product of
    // dims after axis; outer_size = product of dims before axis.
    var outer_size: usize = 1;
    for (0..axis) |k| outer_size *= @intCast(table_shape.dim(@intCast(k)));
    var inner_size: usize = 1;
    for ((axis + 1)..rank) |k| inner_size *= @intCast(table_shape.dim(@intCast(k)));
    const axis_size: usize = @intCast(axis_dim);

    const block_bytes = inner_size * table_elem_bytes;
    for (0..outer_size) |o| {
        for (0..idx_count) |k| {
            const src_axis_idx: usize = @intCast(indices_buf[k]);
            const src_off = ((o * axis_size) + src_axis_idx) * block_bytes;
            const dst_off = ((o * idx_count) + k) * block_bytes;
            @memcpy(out_bytes[dst_off..][0..block_bytes], table_bytes[src_off..][0..block_bytes]);
        }
    }

    const loc = try graph.internConstantBytes(out_bytes, table_shape.dtype);
    return try graph.addNode(.{
        .op = .{ .constant = .{ .data_offset = loc.offset, .data_len = loc.len } },
        .output_shape = out_shape,
    });
}

/// Fold `argmax(input, axis, keepdims)`. Output dtype is i64. Only
/// the input's natural dtype matters for the comparison, so this
/// path reads the input typed and keeps the comparison in the
/// largest f64-compatible type.
fn foldArgmax(
    allocator: std.mem.Allocator,
    graph: *Graph,
    n: *const Node,
    redirect: []const NodeId,
) !?NodeId {
    if (n.num_inputs < 1) return null;
    const src_id = resolve(redirect, n.inputs[0]);
    if (src_id == null_node or src_id >= graph.nodeCount()) return null;
    const src_node = graph.node(src_id);
    const src_attrs = switch (src_node.op) {
        .constant => |a| a,
        else => return null,
    };
    const in_shape = src_node.output_shape;
    const out_shape = n.output_shape;
    if (out_shape.dtype != .i64) return null;
    const attrs = n.op.argmax;
    const axis = attrs.axis;
    const rank = in_shape.rank();
    if (axis >= rank) return null;
    const axis_dim = in_shape.dim(axis);
    if (axis_dim <= 0) return null;

    const num_in_elements: usize = blk: {
        const ie = in_shape.numElements() orelse return null;
        if (ie < 0) return null;
        break :blk @intCast(ie);
    };
    const num_out_elements: usize = blk: {
        const oe = out_shape.numElements() orelse return null;
        if (oe < 0) return null;
        break :blk @intCast(oe);
    };

    // Read input values into an f64 buffer so the comparison is
    // dtype-agnostic.
    const values = try allocator.alloc(f64, num_in_elements);
    defer allocator.free(values);
    if (!try readAsF64(graph, src_attrs, in_shape.dtype, values)) return null;

    // Compute strides on the input (row-major).
    const in_strides = computeStrides(in_shape.dims, rank);

    // Iterate every output position; for each, scan the `axis`-aligned
    // run and pick the index of the first maximum.
    const result = try allocator.alloc(i64, num_out_elements);
    defer allocator.free(result);

    // Build "outer/inner" iteration: every output index corresponds
    // to a (outer, inner) pair where outer ranges over dims before
    // the axis and inner ranges over dims after.
    var outer_size: usize = 1;
    for (0..axis) |k| outer_size *= @intCast(in_shape.dim(@intCast(k)));
    var inner_size: usize = 1;
    for ((axis + 1)..rank) |k| inner_size *= @intCast(in_shape.dim(@intCast(k)));
    const axis_stride: usize = @intCast(in_strides[axis]);
    const axis_size: usize = @intCast(axis_dim);

    var out_idx: usize = 0;
    for (0..outer_size) |o| {
        for (0..inner_size) |inner| {
            const base = (o * axis_size * inner_size) + inner;
            var best_val: f64 = values[base];
            var best_k: i64 = 0;
            var k: usize = 1;
            while (k < axis_size) : (k += 1) {
                const v = values[base + k * axis_stride];
                if (v > best_val) {
                    best_val = v;
                    best_k = @intCast(k);
                }
            }
            result[out_idx] = best_k;
            out_idx += 1;
        }
    }

    const loc = try graph.internConstantBytes(std.mem.sliceAsBytes(result), .i64);
    return try graph.addNode(.{
        .op = .{ .constant = .{ .data_offset = loc.offset, .data_len = loc.len } },
        .output_shape = out_shape,
    });
}

/// Decode an interned constant of any numeric dtype into an f64 buffer.
/// Used by `foldArgmax` so the comparison loop is dtype-agnostic.
fn readAsF64(
    graph: *const Graph,
    attrs: node_mod.ConstantAttrs,
    dtype: shape_mod.DType,
    out: []f64,
) !bool {
    switch (dtype) {
        .f32 => {
            const src = graph.constantDataAs(f32, attrs.data_offset, attrs.data_len);
            if (src.len != out.len) return false;
            for (src, out) |v, *o| o.* = v;
        },
        .f64 => {
            const src = graph.constantDataAs(f64, attrs.data_offset, attrs.data_len);
            if (src.len != out.len) return false;
            @memcpy(out, src);
        },
        .f16 => {
            const src = graph.constantDataAs(f16, attrs.data_offset, attrs.data_len);
            if (src.len != out.len) return false;
            for (src, out) |v, *o| o.* = v;
        },
        .bf16 => {
            const len_bytes: u32 = @intCast(@as(usize, attrs.data_len) * 2);
            const bytes = graph.constantBytes(attrs.data_offset, len_bytes);
            const aligned: [*]align(@alignOf(u16)) const u8 = @alignCast(bytes.ptr);
            const src = @as([*]const u16, @ptrCast(aligned))[0..attrs.data_len];
            if (src.len != out.len) return false;
            for (src, out) |w, *o| {
                const bits: u32 = @as(u32, w) << 16;
                const f: f32 = @bitCast(bits);
                o.* = f;
            }
        },
        .i8 => {
            const src = graph.constantDataAs(i8, attrs.data_offset, attrs.data_len);
            if (src.len != out.len) return false;
            for (src, out) |v, *o| o.* = @floatFromInt(v);
        },
        .i16 => {
            const src = graph.constantDataAs(i16, attrs.data_offset, attrs.data_len);
            if (src.len != out.len) return false;
            for (src, out) |v, *o| o.* = @floatFromInt(v);
        },
        .i32 => {
            const src = graph.constantDataAs(i32, attrs.data_offset, attrs.data_len);
            if (src.len != out.len) return false;
            for (src, out) |v, *o| o.* = @floatFromInt(v);
        },
        .i64 => {
            const src = graph.constantDataAs(i64, attrs.data_offset, attrs.data_len);
            if (src.len != out.len) return false;
            for (src, out) |v, *o| o.* = @floatFromInt(v);
        },
        .u8 => {
            const src = graph.constantDataAs(u8, attrs.data_offset, attrs.data_len);
            if (src.len != out.len) return false;
            for (src, out) |v, *o| o.* = @floatFromInt(v);
        },
        .bool_ => {
            const src = graph.constantDataAs(u8, attrs.data_offset, attrs.data_len);
            if (src.len != out.len) return false;
            for (src, out) |v, *o| o.* = if (v != 0) 1.0 else 0.0;
        },
    }
    return true;
}

// ── Shape helpers ─────────────────────────────────────────────────────
//
// Shared with grad_check.zig; live in `../tensor_eval.zig`. Local
// aliases to keep the call sites short.

const computeStrides = tensor_eval.computeStrides;
const unravelIdx = tensor_eval.unravelIdx;
const shapeStaticElements = tensor_eval.staticElements;

// ── Op evaluators ─────────────────────────────────────────────────────

fn evalTranspose(
    allocator: std.mem.Allocator,
    input: []const f32,
    in_shape: Shape,
    out_shape: Shape,
    attrs: node_mod.TransposeAttrs,
) !?[]f32 {
    if (in_shape.dtype != .f32 or out_shape.dtype != .f32) return null;
    return tensor_eval.evalTranspose(f32, allocator, input, in_shape, out_shape, attrs);
}

fn evalBroadcast(
    allocator: std.mem.Allocator,
    input: []const f32,
    in_shape: Shape,
    out_shape: Shape,
    attrs: node_mod.BroadcastAttrs,
) !?[]f32 {
    if (in_shape.dtype != .f32 or out_shape.dtype != .f32) return null;
    return tensor_eval.evalBroadcast(f32, allocator, input, in_shape, out_shape, attrs);
}

fn evalSlice(
    allocator: std.mem.Allocator,
    input: []const f32,
    in_shape: Shape,
    out_shape: Shape,
    attrs: node_mod.SliceAttrs,
) !?[]f32 {
    if (in_shape.dtype != .f32 or out_shape.dtype != .f32) return null;
    return tensor_eval.evalSlice(f32, allocator, input, in_shape, out_shape, attrs);
}

const ReduceKind = tensor_eval.ReduceKind;

fn evalReduce(
    allocator: std.mem.Allocator,
    input: []const f32,
    in_shape: Shape,
    out_shape: Shape,
    attrs: node_mod.ReduceAttrs,
    kind: ReduceKind,
) !?[]f32 {
    if (in_shape.dtype != .f32 or out_shape.dtype != .f32) return null;
    return tensor_eval.evalReduce(f32, allocator, input, in_shape, out_shape, attrs, kind);
}

fn evalUnary(allocator: std.mem.Allocator, input: []const f32, num_elements: usize, comptime f: fn (f32) f32) ![]f32 {
    if (input.len != num_elements) return error.ConstFoldShapeMismatch;
    const result = try allocator.alloc(f32, num_elements);
    for (0..num_elements) |i| {
        result[i] = f(input[i]);
    }
    return result;
}

fn getConstData(graph: *const Graph, id: NodeId) ?[]const f32 {
    if (id == null_node or id >= graph.nodeCount()) return null;
    const n = graph.node(id);
    if (n.output_shape.dtype != .f32) return null;
    switch (n.op) {
        .constant => |attrs| return graph.constantData(attrs.data_offset, attrs.data_len),
        else => return null,
    }
}

// ── Helpers ───────────────────────────────────────────────────────────

fn resolve(redirect: []const NodeId, id: NodeId) NodeId {
    if (id == null_node) return id;
    if (id >= redirect.len) return id;
    var cur = id;
    var depth: u32 = 0;
    while (cur < redirect.len and redirect[cur] != cur and depth < 100) : (depth += 1) {
        cur = redirect[cur];
    }
    return cur;
}

fn shapesEqual(a: Shape, b: Shape) bool {
    if (a.dtype != b.dtype) return false;
    if (a.rank() != b.rank()) return false;
    for (0..a.rank()) |i| {
        if (a.dim(@intCast(i)) != b.dim(@intCast(i))) return false;
    }
    return true;
}

// ── Graph Cloning ─────────────────────────────────────────────────────

fn cloneGraph(allocator: std.mem.Allocator, src: *const Graph) !Graph {
    var dst = Graph.init(allocator);
    errdefer dst.deinit();

    try dst.nodes.appendSlice(allocator, src.nodes.items);
    try dst.constant_pool.appendSlice(allocator, src.constant_pool.items);
    try dst.string_table.appendSlice(allocator, src.string_table.items);
    try dst.outputs.appendSlice(allocator, src.outputs.items);
    try dst.parameters.appendSlice(allocator, src.parameters.items);
    return dst;
}

// ── Rebuild ───────────────────────────────────────────────────────────

fn rebuild(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    redirect: []const NodeId,
    orig_count: u32,
    num_folded: u32,
) !ConstFoldResult {
    const total_count = graph.nodeCount();

    // Mark reachable from outputs.
    const reachable = try allocator.alloc(bool, total_count);
    defer allocator.free(reachable);
    @memset(reachable, false);

    for (graph.outputs.items) |out_id| {
        markReachable(graph, reachable, redirect, resolve(redirect, out_id));
    }

    // Build old->new ID mapping. Synthetic nodes appended by constant folding
    // are always constants with no inputs, so emit them first to ensure any
    // original users that redirect to them see a populated id_map entry.
    const id_map = try allocator.alloc(NodeId, total_count);
    @memset(id_map, null_node);

    var next_new_id: NodeId = 0;
    for (orig_count..total_count) |i| {
        if (!reachable[i]) continue;
        id_map[i] = next_new_id;
        next_new_id += 1;
    }
    for (0..orig_count) |i| {
        if (!reachable[i]) continue;
        id_map[i] = next_new_id;
        next_new_id += 1;
    }

    var new_graph = Graph.init(allocator);
    errdefer {
        new_graph.deinit();
        allocator.free(id_map);
    }

    try new_graph.string_table.appendSlice(allocator, graph.string_table.items);
    try new_graph.constant_pool.appendSlice(allocator, graph.constant_pool.items);

    for (orig_count..total_count) |i| {
        if (!reachable[i]) continue;
        const old_node = graph.node(@intCast(i));
        const new_node = old_node.*;

        _ = try new_graph.addNode(new_node);
    }

    // Emit original nodes in original topological order. Any redirected
    // synthetic constants already have id_map entries from the first pass.
    for (0..orig_count) |i| {
        if (!reachable[i]) continue;
        const old_node = graph.node(@intCast(i));
        var new_node = old_node.*;

        // Remap inputs through redirect then id_map.
        for (0..new_node.num_inputs) |j| {
            const old_input = new_node.inputs[j];
            if (old_input != null_node) {
                const redir = resolve(redirect, old_input);
                if (redir < total_count) {
                    new_node.inputs[j] = id_map[redir];
                }
            }
        }

        if (new_node.vjp_alternate != null_node) {
            const redir = resolve(redirect, new_node.vjp_alternate);
            if (redir < total_count and reachable[redir]) {
                new_node.vjp_alternate = id_map[redir];
            } else {
                new_node.vjp_alternate = null_node;
            }
        }

        _ = try new_graph.addNode(new_node);
    }

    // Remap outputs.
    for (graph.outputs.items) |old_out| {
        const redir = resolve(redirect, old_out);
        try new_graph.outputs.append(allocator, id_map[redir]);
    }

    // Remap parameters.
    for (graph.parameters.items) |old_param| {
        const redir = resolve(redirect, old_param);
        if (redir < total_count and id_map[redir] != null_node) {
            try new_graph.parameters.append(allocator, id_map[redir]);
        }
    }

    // Return id_map trimmed to original count for caller use.
    const caller_map = try allocator.alloc(NodeId, orig_count);
    @memcpy(caller_map, id_map[0..orig_count]);
    allocator.free(id_map);

    return .{ .graph = new_graph, .id_map = caller_map, .num_folded = num_folded };
}

fn markReachable(graph: *const Graph, reachable: []bool, redirect: []const NodeId, id: NodeId) void {
    if (id == null_node or id >= reachable.len) return;
    if (reachable[id]) return;

    reachable[id] = true;

    const n = graph.node(id);
    for (n.getInputs()) |input_id| {
        if (input_id != null_node) {
            markReachable(graph, reachable, redirect, resolve(redirect, input_id));
        }
    }

    if (n.vjp_alternate != null_node) {
        markReachable(graph, reachable, redirect, resolve(redirect, n.vjp_alternate));
    }
}

// ── Tests ─────────────────────────────────────────────────────────────

const Builder = @import("../builder.zig").Builder;

test "fold constant unary chain" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    var b = Builder.init(&g);

    // Build: neg(sqrt(const(4.0))) = neg(2.0) = -2.0
    const c = try b.scalarConst(.f32, 4.0);
    const s = try b.sqrt(c);
    const n = try b.neg(s);
    try g.markOutput(n);

    var result = try fold(allocator, &g);
    defer result.deinit();
    g.deinit();

    // Two folds in one pass: sqrt(4)=2 then neg(2)=-2. The fold loop
    // marks an original-slot as constant after a successful eval so
    // downstream nodes can pick up the synthesized constant in the
    // same iteration.
    try std.testing.expectEqual(@as(u32, 2), result.num_folded);
    try std.testing.expectEqual(@as(u32, 1), @as(u32, @intCast(result.graph.outputs.items.len)));
    const out_id = result.graph.outputs.items[0];
    const out_node = result.graph.node(out_id);
    switch (out_node.op) {
        .constant => |attrs| {
            const data = result.graph.constantData(attrs.data_offset, attrs.data_len);
            try std.testing.expectEqual(@as(usize, 1), data.len);
            try std.testing.expectApproxEqAbs(@as(f32, -2.0), data[0], 1e-6);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "folded synthetic constant is emitted before original users" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.scalar(.f32));
    const c = try b.scalarConst(.f32, 4.0);
    const scale = try b.rsqrt(c);
    const out = try b.mul(x, scale);
    try g.markOutput(out);

    var result = try fold(allocator, &g);
    defer result.deinit();
    g.deinit();

    const out_id = result.graph.outputs.items[0];
    const out_node = result.graph.node(out_id);
    try std.testing.expectEqual(@as(u8, 2), out_node.num_inputs);
    try std.testing.expect(out_node.inputs[0] != null_node);
    try std.testing.expect(out_node.inputs[1] != null_node);
    try std.testing.expectEqual(.constant, std.meta.activeTag(result.graph.node(out_node.inputs[1]).op));
}

test "fold constant binary ops" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    var b = Builder.init(&g);

    // Build: mul(const(3.0), const(7.0)) = 21.0
    const a = try b.scalarConst(.f32, 3.0);
    const c = try b.scalarConst(.f32, 7.0);
    const m = try b.mul(a, c);
    try g.markOutput(m);

    var result = try fold(allocator, &g);
    defer result.deinit();
    g.deinit();

    try std.testing.expect(result.num_folded >= 1);
    const out_id = result.graph.outputs.items[0];
    const out_node = result.graph.node(out_id);
    switch (out_node.op) {
        .constant => |attrs| {
            const data = result.graph.constantData(attrs.data_offset, attrs.data_len);
            try std.testing.expectApproxEqAbs(@as(f32, 21.0), data[0], 1e-6);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "does not fold parameter-dependent expressions" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    var b = Builder.init(&g);

    // Build: add(param, const(1.0)) — should NOT be folded.
    const p = try b.parameter("x", Shape.init(.f32, &.{4}));
    const c = try b.scalarConst(.f32, 1.0);
    // Need a broadcast for the add since shapes differ (scalar vs [4]).
    // Instead, just use the scalar directly.
    const added = try b.add(p, c);
    try g.markOutput(added);

    var result = try fold(allocator, &g);
    defer result.deinit();
    g.deinit();

    // Nothing should be folded — the add depends on a parameter.
    try std.testing.expectEqual(@as(u32, 0), result.num_folded);
}

test "fold rsqrt scaling factor" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    var b = Builder.init(&g);

    // Typical attention scaling: rsqrt(64.0) = 1/8 = 0.125
    const head_dim = try b.scalarConst(.f32, 64.0);
    const scale = try b.rsqrt(head_dim);
    try g.markOutput(scale);

    var result = try fold(allocator, &g);
    defer result.deinit();
    g.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.num_folded);
    const out_id = result.graph.outputs.items[0];
    switch (result.graph.node(out_id).op) {
        .constant => |attrs| {
            const data = result.graph.constantData(attrs.data_offset, attrs.data_len);
            try std.testing.expectApproxEqAbs(@as(f32, 0.125), data[0], 1e-6);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "fold chain: div(const, const) then mul with param preserved" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    var b = Builder.init(&g);

    // Build: mul(param, div(6.0, 3.0))
    // div(6,3)=2.0 should be folded, but mul(param, 2.0) should not.
    const p = try b.parameter("x", Shape.scalar(.f32));
    const six = try b.scalarConst(.f32, 6.0);
    const three = try b.scalarConst(.f32, 3.0);
    const ratio = try b.div(six, three);
    const scaled = try b.mul(p, ratio);
    try g.markOutput(scaled);

    var result = try fold(allocator, &g);
    defer result.deinit();
    g.deinit();

    // div should be folded, mul should remain.
    try std.testing.expectEqual(@as(u32, 1), result.num_folded);
    // Graph should still have the parameter and the mul.
    try std.testing.expect(result.graph.nodeCount() >= 3); // param, const(2.0), mul
}

test "fold tensor constants" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    var b = Builder.init(&g);

    // Build: add([1,2,3], [4,5,6]) = [5,7,9]
    const shape = Shape.init(.f32, &.{3});
    const a = try b.tensorConst(&.{ 1.0, 2.0, 3.0 }, shape);
    const c = try b.tensorConst(&.{ 4.0, 5.0, 6.0 }, shape);
    const sum = try b.add(a, c);
    try g.markOutput(sum);

    var result = try fold(allocator, &g);
    defer result.deinit();
    g.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.num_folded);
    const out_id = result.graph.outputs.items[0];
    switch (result.graph.node(out_id).op) {
        .constant => |attrs| {
            const data = result.graph.constantData(attrs.data_offset, attrs.data_len);
            try std.testing.expectEqual(@as(usize, 3), data.len);
            try std.testing.expectApproxEqAbs(@as(f32, 5.0), data[0], 1e-6);
            try std.testing.expectApproxEqAbs(@as(f32, 7.0), data[1], 1e-6);
            try std.testing.expectApproxEqAbs(@as(f32, 9.0), data[2], 1e-6);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "fold empty graph" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);

    var result = try fold(allocator, &g);
    defer result.deinit();
    g.deinit();

    try std.testing.expectEqual(@as(u32, 0), result.num_folded);
    try std.testing.expectEqual(@as(u32, 0), result.graph.nodeCount());
}

test "fold transpose of constant" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    var b = Builder.init(&g);

    // [[1, 2, 3], [4, 5, 6]] transposed to [[1, 4], [2, 5], [3, 6]].
    const c = try b.tensorConst(&.{ 1, 2, 3, 4, 5, 6 }, Shape.init(.f32, &.{ 2, 3 }));
    const t = try b.transpose(c, &.{ 1, 0 });
    try g.markOutput(t);

    var result = try fold(allocator, &g);
    defer result.deinit();
    g.deinit();

    try std.testing.expect(result.num_folded >= 1);
    const out_id = result.graph.outputs.items[0];
    const data = switch (result.graph.node(out_id).op) {
        .constant => |attrs| result.graph.constantData(attrs.data_offset, attrs.data_len),
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqual(@as(usize, 6), data.len);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), data[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 4.0), data[1], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 2.0), data[2], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 5.0), data[3], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 3.0), data[4], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 6.0), data[5], 1e-6);
}

test "fold reduce_sum of constant" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    var b = Builder.init(&g);

    // sum([[1, 2, 3], [4, 5, 6]], axis=1) -> [[6], [15]]
    const c = try b.tensorConst(&.{ 1, 2, 3, 4, 5, 6 }, Shape.init(.f32, &.{ 2, 3 }));
    const r = try b.reduceSum(c, &.{1});
    try g.markOutput(r);

    var result = try fold(allocator, &g);
    defer result.deinit();
    g.deinit();

    try std.testing.expect(result.num_folded >= 1);
    const out_id = result.graph.outputs.items[0];
    const data = switch (result.graph.node(out_id).op) {
        .constant => |attrs| result.graph.constantData(attrs.data_offset, attrs.data_len),
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqual(@as(usize, 2), data.len);
    try std.testing.expectApproxEqAbs(@as(f32, 6.0), data[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 15.0), data[1], 1e-6);
}

test "fold reduce_max of constant" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    var b = Builder.init(&g);

    const c = try b.tensorConst(&.{ -1, 5, 2, 3, 0, 4 }, Shape.init(.f32, &.{ 2, 3 }));
    const r = try b.reduceMax(c, &.{1});
    try g.markOutput(r);

    var result = try fold(allocator, &g);
    defer result.deinit();
    g.deinit();

    try std.testing.expect(result.num_folded >= 1);
    const out_id = result.graph.outputs.items[0];
    const data = switch (result.graph.node(out_id).op) {
        .constant => |attrs| result.graph.constantData(attrs.data_offset, attrs.data_len),
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectApproxEqAbs(@as(f32, 5.0), data[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 4.0), data[1], 1e-6);
}

test "fold slice of constant" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    var b = Builder.init(&g);

    const c = try b.tensorConst(&.{ 1, 2, 3, 4, 5, 6 }, Shape.init(.f32, &.{ 2, 3 }));
    // sliceLastDim selects columns [1..3) → [[2, 3], [5, 6]].
    const s = try b.sliceLastDim(c, 1, 3);
    try g.markOutput(s);

    var result = try fold(allocator, &g);
    defer result.deinit();
    g.deinit();

    try std.testing.expect(result.num_folded >= 1);
    const out_id = result.graph.outputs.items[0];
    const data = switch (result.graph.node(out_id).op) {
        .constant => |attrs| result.graph.constantData(attrs.data_offset, attrs.data_len),
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqual(@as(usize, 4), data.len);
    try std.testing.expectApproxEqAbs(@as(f32, 2.0), data[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 3.0), data[1], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 5.0), data[2], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 6.0), data[3], 1e-6);
}

test "fold scalar broadcast binary" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    var b = Builder.init(&g);

    // mul([1, 2, 3], 2.0) -> [2, 4, 6]
    const v = try b.tensorConst(&.{ 1, 2, 3 }, Shape.init(.f32, &.{3}));
    const s = try b.scalarConst(.f32, 2.0);
    const out = try b.mul(v, s);
    try g.markOutput(out);

    var result = try fold(allocator, &g);
    defer result.deinit();
    g.deinit();

    try std.testing.expect(result.num_folded >= 1);
    const out_id = result.graph.outputs.items[0];
    const data = switch (result.graph.node(out_id).op) {
        .constant => |attrs| result.graph.constantData(attrs.data_offset, attrs.data_len),
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqual(@as(usize, 3), data.len);
    try std.testing.expectApproxEqAbs(@as(f32, 2.0), data[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 4.0), data[1], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 6.0), data[2], 1e-6);
}

test "fold concat_prim of constants" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    var b = Builder.init(&g);

    const a = try b.tensorConst(&.{ 1, 2, 3, 4 }, Shape.init(.f32, &.{ 2, 2 }));
    const c = try b.tensorConst(&.{ 5, 6, 7, 8, 9, 10 }, Shape.init(.f32, &.{ 2, 3 }));
    const out = try g.addNode(.{
        .op = .{ .concat_prim = .{ .axis = 1 } },
        .output_shape = Shape.init(.f32, &.{ 2, 5 }),
        .inputs = .{ a, c, null_node, null_node },
        .num_inputs = 2,
    });
    try g.markOutput(out);

    var result = try fold(allocator, &g);
    defer result.deinit();
    g.deinit();

    try std.testing.expect(result.num_folded >= 1);
    const out_id = result.graph.outputs.items[0];
    const data = switch (result.graph.node(out_id).op) {
        .constant => |attrs| result.graph.constantData(attrs.data_offset, attrs.data_len),
        else => return error.TestUnexpectedResult,
    };
    // Row 0: [1, 2, 5, 6, 7]; row 1: [3, 4, 8, 9, 10].
    try std.testing.expectEqualSlices(f32, &.{ 1, 2, 5, 6, 7, 3, 4, 8, 9, 10 }, data);
}

test "fold where_select with scalar constant condition" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    var b = Builder.init(&g);

    const cond = try b.scalarConst(.f32, 1.0); // true
    const t = try b.tensorConst(&.{ 1, 2, 3 }, Shape.init(.f32, &.{3}));
    const f = try b.tensorConst(&.{ 4, 5, 6 }, Shape.init(.f32, &.{3}));
    const out = try g.addNode(.{
        .op = .{ .where_select = {} },
        .output_shape = Shape.init(.f32, &.{3}),
        .inputs = .{ cond, t, f, null_node },
        .num_inputs = 3,
    });
    try g.markOutput(out);

    var result = try fold(allocator, &g);
    defer result.deinit();
    g.deinit();

    try std.testing.expect(result.num_folded >= 1);
    const out_id = result.graph.outputs.items[0];
    const data = switch (result.graph.node(out_id).op) {
        .constant => |attrs| result.graph.constantData(attrs.data_offset, attrs.data_len),
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3 }, data);
}

test "fold convert_dtype f32 → i32" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    var b = Builder.init(&g);

    const c = try b.tensorConst(&.{ 0.5, 1.7, -2.3, 9.0 }, Shape.init(.f32, &.{4}));
    const cast = try g.addNode(.{
        .op = .{ .convert_dtype = .{ .target = .i32 } },
        .output_shape = Shape.init(.i32, &.{4}),
        .inputs = .{ c, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    try g.markOutput(cast);

    var result = try fold(allocator, &g);
    defer result.deinit();
    g.deinit();

    try std.testing.expect(result.num_folded >= 1);
    const out_id = result.graph.outputs.items[0];
    const out_node = result.graph.node(out_id);
    const attrs = switch (out_node.op) {
        .constant => |a| a,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqual(shape_mod.DType.i32, out_node.output_shape.dtype);
    const data = result.graph.constantDataAs(i32, attrs.data_offset, attrs.data_len);
    try std.testing.expectEqualSlices(i32, &.{ 0, 1, -2, 9 }, data);
}

test "generic const-fold evaluator does not reinterpret typed constants as f32" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    var b = Builder.init(&g);

    const a_values = [_]i64{ 1, 2 };
    const b_values = [_]i64{ 3, 4 };
    const shape = Shape.init(.i64, &.{2});
    const a = try b.tensorConstBytes(std.mem.sliceAsBytes(&a_values), shape);
    const c = try b.tensorConstBytes(std.mem.sliceAsBytes(&b_values), shape);
    const sum = try b.add(a, c);
    try g.markOutput(sum);

    var result = try fold(allocator, &g);
    defer result.deinit();
    g.deinit();

    try std.testing.expectEqual(@as(u32, 0), result.num_folded);
    const out_id = result.graph.outputs.items[0];
    try std.testing.expectEqual(.add, std.meta.activeTag(result.graph.node(out_id).op));
}

test "fold convert_dtype f32 → f16" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    var b = Builder.init(&g);

    const c = try b.tensorConst(&.{ 1.5, 2.25, -3.0 }, Shape.init(.f32, &.{3}));
    const cast = try g.addNode(.{
        .op = .{ .convert_dtype = .{ .target = .f16 } },
        .output_shape = Shape.init(.f16, &.{3}),
        .inputs = .{ c, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    try g.markOutput(cast);

    var result = try fold(allocator, &g);
    defer result.deinit();
    g.deinit();

    try std.testing.expect(result.num_folded >= 1);
    const out_id = result.graph.outputs.items[0];
    const out_node = result.graph.node(out_id);
    const attrs = switch (out_node.op) {
        .constant => |a| a,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqual(shape_mod.DType.f16, out_node.output_shape.dtype);
    const data = result.graph.constantDataAs(f16, attrs.data_offset, attrs.data_len);
    try std.testing.expectApproxEqAbs(@as(f16, 1.5), data[0], 1e-3);
    try std.testing.expectApproxEqAbs(@as(f16, 2.25), data[1], 1e-3);
    try std.testing.expectApproxEqAbs(@as(f16, -3.0), data[2], 1e-3);
}

test "fold convert_dtype f32 → bool_" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    var b = Builder.init(&g);

    const c = try b.tensorConst(&.{ 0.0, 1.0, -2.0, 0.0 }, Shape.init(.f32, &.{4}));
    const cast = try g.addNode(.{
        .op = .{ .convert_dtype = .{ .target = .bool_ } },
        .output_shape = Shape.init(.bool_, &.{4}),
        .inputs = .{ c, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    try g.markOutput(cast);

    var result = try fold(allocator, &g);
    defer result.deinit();
    g.deinit();

    try std.testing.expect(result.num_folded >= 1);
    const out_id = result.graph.outputs.items[0];
    const out_node = result.graph.node(out_id);
    const attrs = switch (out_node.op) {
        .constant => |a| a,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqual(shape_mod.DType.bool_, out_node.output_shape.dtype);
    const data = result.graph.constantDataAs(u8, attrs.data_offset, attrs.data_len);
    try std.testing.expectEqualSlices(u8, &.{ 0, 1, 1, 0 }, data);
}

test "fold convert_dtype f32 ↔ bf16 round-trip" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    var b = Builder.init(&g);

    // Powers-of-two are exactly representable in bf16, so the round
    // trip f32 → bf16 → f32 should be lossless for them.
    const c = try b.tensorConst(&.{ 1.0, 2.0, -4.0, 0.5 }, Shape.init(.f32, &.{4}));
    const to_bf16 = try g.addNode(.{
        .op = .{ .convert_dtype = .{ .target = .bf16 } },
        .output_shape = Shape.init(.bf16, &.{4}),
        .inputs = .{ c, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const back_to_f32 = try g.addNode(.{
        .op = .{ .convert_dtype = .{ .target = .f32 } },
        .output_shape = Shape.init(.f32, &.{4}),
        .inputs = .{ to_bf16, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    try g.markOutput(back_to_f32);

    var result = try fold(allocator, &g);
    defer result.deinit();
    g.deinit();

    try std.testing.expect(result.num_folded >= 2);
    const out_id = result.graph.outputs.items[0];
    const out_node = result.graph.node(out_id);
    const attrs = switch (out_node.op) {
        .constant => |a| a,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqual(shape_mod.DType.f32, out_node.output_shape.dtype);
    const data = result.graph.constantDataAs(f32, attrs.data_offset, attrs.data_len);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), data[0], 1e-3);
    try std.testing.expectApproxEqAbs(@as(f32, 2.0), data[1], 1e-3);
    try std.testing.expectApproxEqAbs(@as(f32, -4.0), data[2], 1e-3);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), data[3], 1e-3);
}

test "fold convert_dtype bf16 → i32 (LLM-style cast chain)" {
    // LLM exports often go bf16 weights → cast to f32 for compute;
    // the f32 → i32 path was already covered. This walks the harder
    // bf16 → i32 direction (via the f32 staging buffer).
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    var b = Builder.init(&g);

    const c = try b.tensorConst(&.{ 0.0, 2.0, -3.0, 7.0 }, Shape.init(.f32, &.{4}));
    const to_bf16 = try g.addNode(.{
        .op = .{ .convert_dtype = .{ .target = .bf16 } },
        .output_shape = Shape.init(.bf16, &.{4}),
        .inputs = .{ c, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const to_i32 = try g.addNode(.{
        .op = .{ .convert_dtype = .{ .target = .i32 } },
        .output_shape = Shape.init(.i32, &.{4}),
        .inputs = .{ to_bf16, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    try g.markOutput(to_i32);

    var result = try fold(allocator, &g);
    defer result.deinit();
    g.deinit();

    try std.testing.expect(result.num_folded >= 2);
    const out_id = result.graph.outputs.items[0];
    const out_node = result.graph.node(out_id);
    const attrs = switch (out_node.op) {
        .constant => |a| a,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqual(shape_mod.DType.i32, out_node.output_shape.dtype);
    const data = result.graph.constantDataAs(i32, attrs.data_offset, attrs.data_len);
    try std.testing.expectEqualSlices(i32, &.{ 0, 2, -3, 7 }, data);
}

test "fold gather along axis 0 (embedding-lookup shape)" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    var b = Builder.init(&g);

    // table[4][3] = row r filled with r*10 + c.
    const table = try b.tensorConst(&.{
        0,  1,  2,
        10, 11, 12,
        20, 21, 22,
        30, 31, 32,
    }, Shape.init(.f32, &.{ 4, 3 }));

    // Indices [2, 0, 3] as i32 — `tensorConstBytes` takes the dtype
    // from the shape so we can build typed constants without going
    // through `internConstantBytes` + `addNode` by hand.
    const idx_i32 = [_]i32{ 2, 0, 3 };
    const idx_node = try b.tensorConstBytes(std.mem.sliceAsBytes(&idx_i32), Shape.init(.i32, &.{3}));

    const gather = try g.addNode(.{
        .op = .{ .gather = .{ .axis = 0 } },
        .output_shape = Shape.init(.f32, &.{ 3, 3 }),
        .inputs = .{ table, idx_node, null_node, null_node },
        .num_inputs = 2,
    });
    try g.markOutput(gather);

    var result = try fold(allocator, &g);
    defer result.deinit();
    g.deinit();

    try std.testing.expect(result.num_folded >= 1);
    const out_id = result.graph.outputs.items[0];
    const out_node = result.graph.node(out_id);
    const attrs = switch (out_node.op) {
        .constant => |a| a,
        else => return error.TestUnexpectedResult,
    };
    const data = result.graph.constantDataAs(f32, attrs.data_offset, attrs.data_len);
    // Rows in order [2, 0, 3] of the table.
    try std.testing.expectEqualSlices(f32, &.{
        20, 21, 22,
        0,  1,  2,
        30, 31, 32,
    }, data);
}

test "fold argmax along last axis (i64 output)" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    var b = Builder.init(&g);

    // [[0.1, 0.7, 0.2], [0.5, 0.3, 0.8]] — argmax along axis=1 with
    // keepdims=true → [[1], [2]].
    const x = try b.tensorConst(&.{
        0.1, 0.7, 0.2,
        0.5, 0.3, 0.8,
    }, Shape.init(.f32, &.{ 2, 3 }));

    const am = try g.addNode(.{
        .op = .{ .argmax = .{ .axis = 1, .keepdims = true } },
        .output_shape = Shape.init(.i64, &.{ 2, 1 }),
        .inputs = .{ x, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    try g.markOutput(am);

    var result = try fold(allocator, &g);
    defer result.deinit();
    g.deinit();

    try std.testing.expect(result.num_folded >= 1);
    const out_id = result.graph.outputs.items[0];
    const out_node = result.graph.node(out_id);
    const attrs = switch (out_node.op) {
        .constant => |a| a,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqual(shape_mod.DType.i64, out_node.output_shape.dtype);
    const data = result.graph.constantDataAs(i64, attrs.data_offset, attrs.data_len);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2 }, data);
}
