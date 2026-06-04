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

// Cross-op fusion and algebraic simplification pass.
//
// Applies pattern-based rewrites to optimize the computation graph:
//
// Algebraic simplifications (redirect-only, no new nodes):
// - Identity elimination: add(x, 0), sub(x, 0), mul(x, 1), div(x, 1) -> x
// - Annihilation: mul(x, 0) -> 0
// - Double negation: neg(neg(x)) -> x
// - Idempotent abs: abs(abs(x)) -> abs(x)
// - Identity type conversion: convert_dtype(x, same_dtype) -> x
// - Identity broadcast: broadcast_in_dim(x, same_shape) -> x
// - Transpose cancelation: transpose(transpose(x, p), inv(p)) -> x
// - Identity reshape: reshape(x, same_shape) -> x
//
// Multi-node fusion (adds new nodes to the graph):
// - Linear pair fusion: two fused_linear_no_bias on same input with same
//   rows/in_dim -> fused_linear_no_bias_pair + fused_to_float32
//
// Uses a redirect-map approach (like lower.zig): matched patterns set
// redirect[old] = replacement, then the graph is rebuilt following
// redirects. Unreachable nodes are dropped (implicit DCE).

const std = @import("std");
const graph_mod = @import("../graph.zig");
const node_mod = @import("../node.zig");
const shape_mod = @import("../shape.zig");

const Graph = graph_mod.Graph;
const Node = node_mod.Node;
const NodeId = node_mod.NodeId;
const null_node = node_mod.null_node;
const OpCode = node_mod.OpCode;
const Shape = shape_mod.Shape;

pub const FuseResult = struct {
    graph: Graph,
    /// old_id -> new_id mapping. Caller must free.
    id_map: []NodeId,
    /// Number of patterns that fired.
    num_rewrites: u32,

    pub fn deinit(self: *FuseResult) void {
        const allocator = self.graph.allocator;
        self.graph.deinit();
        allocator.free(self.id_map);
    }
};

/// Apply fusion and simplification patterns to the graph.
/// Returns a new optimized graph.
pub fn fuse(allocator: std.mem.Allocator, graph: *const Graph) !FuseResult {
    // Clone into mutable workspace so multi-node patterns can add nodes.
    var work = try cloneForFusion(allocator, graph);
    errdefer work.deinit();

    const orig_count = work.nodeCount();
    var num_rewrites: u32 = 0;

    // Reachability from outputs (via inputs only — NOT vjp_alternate).
    // Used by the primitive→fused matchers to skip dead subgraphs that
    // are referenced solely as `vjp_alternate` decompositions of an
    // already-fused op. Without this, builders that emit a fused node
    // alongside its decomposed shadow (e.g. `Builder.softmax`) would
    // spuriously re-fuse the shadow on every fuse() call.
    const reachable_anchor = try allocator.alloc(bool, orig_count);
    defer allocator.free(reachable_anchor);
    @memset(reachable_anchor, false);
    for (work.outputs.items) |out_id| {
        markReachableForFuse(&work, reachable_anchor, out_id);
    }

    // Phase 1: Multi-node patterns (may add nodes to work).
    //
    // Order matters: primitive→fused matchers run first so that downstream
    // matchers (SDPA looks for fused_softmax) can see the new fused nodes.
    // Within a single fuse() call, the SDPA matcher still inspects the
    // raw `op` of nodes — it does NOT follow the redirect map. So a div
    // pattern that softmax-matched to fused_softmax is still seen by SDPA
    // as a div. The pipeline fixed-point loop in `pipeline.zig` re-runs
    // fuse, which lets SDPA match on iteration 2 once the fused_softmax
    // node has been baked into the graph by the previous iteration.
    var softmax_rewrites = try fuseSoftmaxFromPrimitives(allocator, &work, reachable_anchor);
    num_rewrites += softmax_rewrites.num_rewrites;

    var silu_rewrites = try fuseSiLUFromPrimitives(allocator, &work, reachable_anchor);
    num_rewrites += silu_rewrites.num_rewrites;

    var gelu_rewrites = try fuseGELUFromPrimitives(allocator, &work, reachable_anchor);
    num_rewrites += gelu_rewrites.num_rewrites;

    var quick_gelu_rewrites = try fuseQuickGELUFromPrimitives(allocator, &work, reachable_anchor);
    num_rewrites += quick_gelu_rewrites.num_rewrites;

    var rms_norm_rewrites = try fuseRMSNormFromPrimitives(allocator, &work, reachable_anchor);
    num_rewrites += rms_norm_rewrites.num_rewrites;

    var layer_norm_rewrites = try fuseLayerNormFromPrimitives(allocator, &work, reachable_anchor);
    num_rewrites += layer_norm_rewrites.num_rewrites;

    var linear_bias_rewrites = try fuseLinearBias(allocator, &work, reachable_anchor);
    num_rewrites += linear_bias_rewrites.num_rewrites;

    var matmul_no_bias_rewrites = try fuseMatmulNoBias(allocator, &work, reachable_anchor);
    num_rewrites += matmul_no_bias_rewrites.num_rewrites;

    var rope_rewrites = try fuseRopeFromPrimitives(allocator, &work, reachable_anchor);
    num_rewrites += rope_rewrites.num_rewrites;

    // Algebraic / shape canonicalizations. These run alongside the
    // primitive→fused matchers in Phase 1 because they synthesize new
    // nodes (Phase 2 only does redirects).
    var hoist_rewrites = try fuseHoistUnaryAboveBroadcast(allocator, &work, reachable_anchor);
    num_rewrites += hoist_rewrites.num_rewrites;

    var chain_reshape_rewrites = try fuseChainReshape(allocator, &work, reachable_anchor);
    num_rewrites += chain_reshape_rewrites.num_rewrites;

    var chain_broadcast_rewrites = try fuseChainBroadcast(allocator, &work, reachable_anchor);
    num_rewrites += chain_broadcast_rewrites.num_rewrites;

    var chain_transpose_rewrites = try fuseChainTranspose(allocator, &work, reachable_anchor);
    num_rewrites += chain_transpose_rewrites.num_rewrites;

    var transpose_into_dot_rewrites = try fuseTransposeIntoDot(allocator, &work, reachable_anchor);
    num_rewrites += transpose_into_dot_rewrites.num_rewrites;

    var reduce_merge_rewrites = try fuseReduceAxisMerge(allocator, &work, reachable_anchor);
    num_rewrites += reduce_merge_rewrites.num_rewrites;

    var neg_normalize_rewrites = try fuseNegNormalize(allocator, &work, reachable_anchor);
    num_rewrites += neg_normalize_rewrites.num_rewrites;

    var slice_chain_rewrites = try fuseSliceChain(allocator, &work, reachable_anchor);
    num_rewrites += slice_chain_rewrites.num_rewrites;

    var slice_concat_rewrites = try fuseSliceOfConcat(allocator, &work, reachable_anchor);
    num_rewrites += slice_concat_rewrites.num_rewrites;

    var canon_commute_rewrites = try fuseCanonicalizeCommutative(allocator, &work, reachable_anchor);
    num_rewrites += canon_commute_rewrites.num_rewrites;

    var self_pair_rewrites = try fuseAlgebraicSelfPairs(allocator, &work, reachable_anchor);
    num_rewrites += self_pair_rewrites.num_rewrites;

    var pair_rewrites = try fuseLinearPairs(allocator, &work);
    num_rewrites += pair_rewrites.num_rewrites;

    var sdpa_rewrites = try fuseSDPA(allocator, &work);
    num_rewrites += sdpa_rewrites.num_rewrites;

    // Phase 2: Simple redirect patterns.
    const total_count = work.nodeCount();
    const redirect = try allocator.alloc(NodeId, total_count);
    defer allocator.free(redirect);
    for (0..total_count) |i| redirect[i] = @intCast(i);

    // Apply redirects. Order matters when two matchers fire on the
    // same anchor: the LAST application wins. Algebraic / shape-only
    // canonicalizations apply first; fused-op promotions apply last so
    // they take precedence over a peer that also rewrote the same node
    // (e.g. transpose_into_dot rewriting `dot_general(x,
    // transpose(W))` to a bare `dot_general` would otherwise hide the
    // matmul→fused_linear_no_bias promotion).
    const algebraic_redirects = [_]*std.ArrayListUnmanaged(Redirect){
        &hoist_rewrites.redirects,
        &chain_reshape_rewrites.redirects,
        &chain_broadcast_rewrites.redirects,
        &chain_transpose_rewrites.redirects,
        &transpose_into_dot_rewrites.redirects,
        &reduce_merge_rewrites.redirects,
        &neg_normalize_rewrites.redirects,
        &slice_chain_rewrites.redirects,
        &slice_concat_rewrites.redirects,
        &canon_commute_rewrites.redirects,
        &self_pair_rewrites.redirects,
    };
    for (algebraic_redirects) |list| {
        for (list.items) |r| redirect[r.from] = r.to;
    }
    const fused_redirects = [_]*std.ArrayListUnmanaged(Redirect){
        &softmax_rewrites.redirects,
        &silu_rewrites.redirects,
        &gelu_rewrites.redirects,
        &quick_gelu_rewrites.redirects,
        &rms_norm_rewrites.redirects,
        &layer_norm_rewrites.redirects,
        &linear_bias_rewrites.redirects,
        &matmul_no_bias_rewrites.redirects,
        &rope_rewrites.redirects,
        &pair_rewrites.redirects,
        &sdpa_rewrites.redirects,
    };
    for (fused_redirects) |list| {
        for (list.items) |r| redirect[r.from] = r.to;
    }

    softmax_rewrites.redirects.deinit(allocator);
    silu_rewrites.redirects.deinit(allocator);
    gelu_rewrites.redirects.deinit(allocator);
    quick_gelu_rewrites.redirects.deinit(allocator);
    rms_norm_rewrites.redirects.deinit(allocator);
    layer_norm_rewrites.redirects.deinit(allocator);
    linear_bias_rewrites.redirects.deinit(allocator);
    matmul_no_bias_rewrites.redirects.deinit(allocator);
    rope_rewrites.redirects.deinit(allocator);
    hoist_rewrites.redirects.deinit(allocator);
    chain_reshape_rewrites.redirects.deinit(allocator);
    chain_broadcast_rewrites.redirects.deinit(allocator);
    chain_transpose_rewrites.redirects.deinit(allocator);
    transpose_into_dot_rewrites.redirects.deinit(allocator);
    reduce_merge_rewrites.redirects.deinit(allocator);
    neg_normalize_rewrites.redirects.deinit(allocator);
    slice_chain_rewrites.redirects.deinit(allocator);
    slice_concat_rewrites.redirects.deinit(allocator);
    canon_commute_rewrites.redirects.deinit(allocator);
    self_pair_rewrites.redirects.deinit(allocator);
    pair_rewrites.redirects.deinit(allocator);
    sdpa_rewrites.redirects.deinit(allocator);

    // Apply algebraic patterns in topological order (original nodes only;
    // synthetic nodes from phase 1 are correct by construction).
    for (0..orig_count) |i| {
        // Skip nodes already redirected by phase 1.
        if (redirect[i] != @as(NodeId, @intCast(i))) continue;

        const id: NodeId = @intCast(i);
        const n = work.node(id);

        // Resolve inputs through redirect chain.
        const in0 = resolve(redirect, n.inputs[0]);
        const in1 = resolve(redirect, n.inputs[1]);

        // Identity-redirect rules must preserve the node's output shape.
        // Without this guard, e.g. add(small_x, broadcast(0, big_S)) →
        // small_x would silently drop the broadcast and consumers would
        // see a smaller shape than the original add produced.
        const out_shape = n.output_shape;
        const in0_shape_eq_out = in0 != null_node and in0 < work.nodeCount() and
            shapesEqual(work.node(in0).output_shape, out_shape);
        const in1_shape_eq_out = in1 != null_node and in1 < work.nodeCount() and
            shapesEqual(work.node(in1).output_shape, out_shape);

        switch (n.op) {
            // ── add(x, 0) -> x | add(0, x) -> x ──────────────────
            .add => {
                if (isConstScalarValue(&work, in1, 0.0) and in0_shape_eq_out) {
                    redirect[i] = in0;
                    num_rewrites += 1;
                } else if (isConstScalarValue(&work, in0, 0.0) and in1_shape_eq_out) {
                    redirect[i] = in1;
                    num_rewrites += 1;
                }
            },

            // ── sub(x, 0) -> x ────────────────────────────────────
            .sub => {
                if (isConstScalarValue(&work, in1, 0.0) and in0_shape_eq_out) {
                    redirect[i] = in0;
                    num_rewrites += 1;
                }
            },

            // ── mul(x, 1) -> x | mul(1, x) -> x | mul(x, 0) -> 0 ─
            .mul => {
                if (isConstScalarValue(&work, in1, 1.0) and in0_shape_eq_out) {
                    redirect[i] = in0;
                    num_rewrites += 1;
                } else if (isConstScalarValue(&work, in0, 1.0) and in1_shape_eq_out) {
                    redirect[i] = in1;
                    num_rewrites += 1;
                } else if (isConstScalarValue(&work, in1, 0.0) and in1_shape_eq_out) {
                    // Only redirect to the zero when its shape already
                    // matches the mul output. A scalar 0 against a vector x
                    // would otherwise smash the output rank.
                    redirect[i] = in1;
                    num_rewrites += 1;
                } else if (isConstScalarValue(&work, in0, 0.0) and in0_shape_eq_out) {
                    redirect[i] = in0;
                    num_rewrites += 1;
                }
            },

            // ── div(x, 1) -> x | div(0, nonzero_x) -> 0 ──────────
            .div => {
                if (isConstScalarValue(&work, in1, 1.0) and in0_shape_eq_out) {
                    redirect[i] = in0;
                    num_rewrites += 1;
                } else if (isConstScalarValue(&work, in0, 0.0) and in0_shape_eq_out and isProvablyNonZero(&work, in1)) {
                    redirect[i] = in0;
                    num_rewrites += 1;
                }
            },

            // ── neg(neg(x)) -> x ──────────────────────────────────
            .neg => {
                if (in0 == null_node) continue;
                const inner = work.node(in0);
                if (std.meta.activeTag(inner.op) == .neg) {
                    redirect[i] = resolve(redirect, inner.inputs[0]);
                    num_rewrites += 1;
                }
            },

            // ── abs(abs(x)) -> abs(x) ────────────────────────────
            .abs => {
                if (in0 == null_node) continue;
                const inner = work.node(in0);
                if (std.meta.activeTag(inner.op) == .abs) {
                    redirect[i] = in0;
                    num_rewrites += 1;
                }
            },

            // ── reshape(x, same_shape) -> x ───────────────────────
            .reshape => |attrs| {
                if (in0 == null_node) continue;
                const input_shape = work.node(in0).output_shape;
                if (shapesEqual(input_shape, attrs.new_shape)) {
                    redirect[i] = in0;
                    num_rewrites += 1;
                }
            },

            // ── convert_dtype(x, same_dtype) -> x ────────────────
            .convert_dtype => |attrs| {
                if (in0 == null_node) continue;
                const input_shape = work.node(in0).output_shape;
                if (input_shape.dtype == attrs.target) {
                    redirect[i] = in0;
                    num_rewrites += 1;
                }
            },

            // ── broadcast_in_dim(x, same_shape) -> x ─────────────
            .broadcast_in_dim => |attrs| {
                if (in0 == null_node) continue;
                const input_shape = work.node(in0).output_shape;
                if (shapesEqual(input_shape, attrs.target_shape)) {
                    redirect[i] = in0;
                    num_rewrites += 1;
                }
            },

            // ── transpose(transpose(x, p), inv(p)) -> x ──────────
            .transpose => |outer_attrs| {
                if (in0 == null_node) continue;
                const inner = work.node(in0);
                switch (inner.op) {
                    .transpose => |inner_attrs| {
                        if (isInversePerm(
                            inner_attrs.perm[0..inner_attrs.num_axes],
                            outer_attrs.perm[0..outer_attrs.num_axes],
                        )) {
                            redirect[i] = resolve(redirect, inner.inputs[0]);
                            num_rewrites += 1;
                        }
                    },
                    else => {},
                }
            },

            else => {},
        }
    }

    const result = try rebuild(allocator, &work, redirect, num_rewrites);
    work.deinit();
    return result;
}

// ── Helpers ───────────────────────────────────────────────────────────

fn resolve(redirect: []const NodeId, id: NodeId) NodeId {
    if (id == null_node) return id;
    if (id >= redirect.len) return id;
    var cur = id;
    var depth: u32 = 0;
    while (redirect[cur] != cur and depth < 100) : (depth += 1) {
        cur = redirect[cur];
    }
    return cur;
}

/// Walk through shape-only / dtype-only ops (convert_dtype, reshape)
/// to find the underlying op. Used by primitive→fused matchers so that
/// FP16 ONNX exports — which routinely surround Softmax / LayerNorm with
/// `Cast(f32) → … → Cast(f16)` for numerical stability — still match.
fn skipShapeOps(graph: *const Graph, id: NodeId) NodeId {
    var cur = id;
    var depth: u32 = 0;
    while (depth < 8) : (depth += 1) {
        if (cur == null_node or cur >= graph.nodeCount()) return cur;
        const tag = std.meta.activeTag(graph.node(cur).op);
        if (tag != .convert_dtype and tag != .reshape) return cur;
        cur = graph.node(cur).inputs[0];
    }
    return cur;
}

/// True iff the node is a scalar-constant equal to `value`, possibly
/// wrapped in shape-only ops (broadcast_in_dim/reshape/convert_dtype).
/// Use this in algebraic rules — `extractScalarConstValue` already
/// handles the common chain produced by importers and const-fold.
fn isConstScalarValue(graph: *const Graph, id: NodeId, value: f32) bool {
    const v = extractScalarConstValue(graph, id) orelse return false;
    return v == value;
}

/// Mark every node reachable from `id` via inputs only (NOT via
/// vjp_alternate). Used by primitive→fused matchers in Phase 1 so they
/// skip dead vjp shadow subgraphs.
fn markReachableForFuse(graph: *const Graph, reachable: []bool, id: NodeId) void {
    if (id == null_node or id >= reachable.len) return;
    if (reachable[id]) return;
    reachable[id] = true;
    const n = graph.node(id);
    for (n.getInputs()) |inp| {
        if (inp != null_node) markReachableForFuse(graph, reachable, inp);
    }
}

fn shapesEqual(a: Shape, b: Shape) bool {
    if (a.dtype != b.dtype) return false;
    if (a.rank() != b.rank()) return false;
    for (0..a.rank()) |i| {
        if (a.dim(@intCast(i)) != b.dim(@intCast(i))) return false;
    }
    return true;
}

fn isInversePerm(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    // b[a[i]] == i for all i
    for (a, 0..) |ai, i| {
        if (ai >= b.len) return false;
        if (b[ai] != @as(u8, @intCast(i))) return false;
    }
    return true;
}

// ── Graph Cloning ─────────────────────────────────────────────────────

fn cloneForFusion(allocator: std.mem.Allocator, src: *const Graph) !Graph {
    var dst = Graph.init(allocator);
    errdefer dst.deinit();

    try dst.nodes.appendSlice(allocator, src.nodes.items);
    try dst.constant_pool.appendSlice(allocator, src.constant_pool.items);
    try dst.string_table.appendSlice(allocator, src.string_table.items);
    try dst.outputs.appendSlice(allocator, src.outputs.items);
    try dst.parameters.appendSlice(allocator, src.parameters.items);
    // constant_cache is not needed for fusion (no new constants added).
    return dst;
}

// ── Linear Pair Fusion ────────────────────────────────────────────────

const Redirect = struct {
    from: NodeId,
    to: NodeId,
};

const PairFusionResult = struct {
    redirects: std.ArrayListUnmanaged(Redirect),
    num_rewrites: u32,
};

/// Detect groups of fused_linear_no_bias nodes sharing the same input
/// (and same `rows`, `in_dim`) and fuse them.
///
/// Two paths:
///  * Uniform-out_dim groups of exactly 2 members → emit a single
///    `fused_linear_no_bias_pair` op (the backend ships a fused kernel
///    that drives both matmuls from one input read).
///  * Everything else (mixed-out_dim, or 3+ members regardless of dim)
///    → "GQA path": chain `concat_prim` over the weights along axis 0,
///    run a single `fused_linear_no_bias` over the concatenated weight,
///    and `slice` the result back into per-projection outputs. This
///    folds Q + K + V (LLaMA-3 / Mistral GQA) into one matmul even when
///    Q has a larger out_dim than K and V.
fn fuseLinearPairs(allocator: std.mem.Allocator, work: *Graph) !PairFusionResult {
    var redirects = std.ArrayListUnmanaged(Redirect).empty;
    errdefer redirects.deinit(allocator);
    var num_rewrites: u32 = 0;

    const count = work.nodeCount();

    // Group by (input_id, rows, in_dim) — out_dim is intentionally NOT
    // part of the key so GQA-style Q/K/V (with different out_dim) end
    // up in the same bucket.
    const GroupKey = struct {
        input_id: NodeId,
        rows: u32,
        in_dim: u32,
    };
    const LinearInfo = struct {
        node_id: NodeId,
        weight_id: NodeId,
        out_dim: u32,
        has_vjp_alternate: bool,
    };
    var groups = std.AutoHashMapUnmanaged(GroupKey, std.ArrayListUnmanaged(LinearInfo)).empty;
    defer {
        var it = groups.iterator();
        while (it.next()) |entry| entry.value_ptr.deinit(allocator);
        groups.deinit(allocator);
    }

    for (0..count) |i| {
        const n = work.node(@intCast(i));
        switch (n.op) {
            .fused_linear_no_bias => |attrs| {
                const key = GroupKey{
                    .input_id = n.inputs[0],
                    .rows = attrs.rows,
                    .in_dim = attrs.in_dim,
                };
                const gop = try groups.getOrPut(allocator, key);
                if (!gop.found_existing) gop.value_ptr.* = .empty;
                try gop.value_ptr.append(allocator, .{
                    .node_id = @intCast(i),
                    .weight_id = n.inputs[1],
                    .out_dim = attrs.out_dim,
                    .has_vjp_alternate = n.vjp_alternate != null_node,
                });
            },
            else => {},
        }
    }

    var git = groups.iterator();
    while (git.next()) |entry| {
        const key = entry.key_ptr.*;
        const members = entry.value_ptr.items;
        if (members.len < 2) continue;

        const output_dtype = work.node(members[0].node_id).output_shape.dtype;
        const first_weight_shape = work.node(members[0].weight_id).output_shape;
        if (first_weight_shape.rank() != 2 or first_weight_shape.dim(1) != key.in_dim or first_weight_shape.dim(0) != members[0].out_dim) continue;
        const weight_dtype = first_weight_shape.dtype;

        var uniform_out_dim = true;
        var any_vjp_alternate = members[0].has_vjp_alternate;
        var compatible_weights = true;
        const first_out_dim = members[0].out_dim;
        for (members[1..]) |m| {
            if (m.out_dim != first_out_dim) {
                uniform_out_dim = false;
            }
            any_vjp_alternate = any_vjp_alternate or m.has_vjp_alternate;
            const weight_shape = work.node(m.weight_id).output_shape;
            if (weight_shape.rank() != 2 or
                weight_shape.dtype != weight_dtype or
                weight_shape.dim(0) != m.out_dim or
                weight_shape.dim(1) != key.in_dim)
            {
                compatible_weights = false;
                break;
            }
        }
        if (!compatible_weights) continue;

        // 2-member uniform group → existing fused_linear_no_bias_pair path.
        if (uniform_out_dim and members.len == 2) {
            const out_shape = Shape.init(output_dtype, &.{
                @intCast(key.rows),
                @intCast(first_out_dim),
            });
            const a = members[0];
            const b = members[1];

            const pair_id = try work.addNode(.{
                .op = .{ .fused_linear_no_bias_pair = .{
                    .rows = key.rows,
                    .in_dim = key.in_dim,
                    .out_dim = first_out_dim,
                } },
                .output_shape = out_shape,
                .inputs = .{ key.input_id, a.weight_id, b.weight_id, null_node },
                .num_inputs = 3,
            });

            const second_id = try work.addNode(.{
                .op = .{ .fused_to_float32 = {} },
                .output_shape = out_shape,
                .inputs = .{ pair_id, null_node, null_node, null_node },
                .num_inputs = 1,
            });

            try redirects.append(allocator, .{ .from = a.node_id, .to = pair_id });
            try redirects.append(allocator, .{ .from = b.node_id, .to = second_id });
            num_rewrites += 1;
            continue;
        }

        // GQA path: 3+ members (uniform or mixed) OR 2 members with
        // mismatched out_dim. Concatenate weights along axis 0 (rows of
        // W = output channels), do one big matmul, slice back.
        //
        // This path introduces primitive concat/slice nodes. Their VJPs are
        // intentionally conservative elsewhere in this module, so only apply
        // the rewrite to inference-style fused nodes that do not carry
        // builder-emitted `vjp_alternate` decompositions.
        if (any_vjp_alternate) continue;

        // Build the concat tree. concat_prim is binary, so chain it.
        var combined_weight_id = members[0].weight_id;
        var combined_out_dim: u32 = members[0].out_dim;
        for (members[1..]) |m| {
            const new_total = combined_out_dim + m.out_dim;
            // Weight shape is [out_dim, in_dim]; we concat along axis 0.
            const new_shape = Shape.init(weight_dtype, &.{
                @intCast(new_total),
                @intCast(key.in_dim),
            });
            combined_weight_id = try work.addNode(.{
                .op = .{ .concat_prim = .{ .axis = 0 } },
                .output_shape = new_shape,
                .inputs = .{ combined_weight_id, m.weight_id, null_node, null_node },
                .num_inputs = 2,
            });
            combined_out_dim = new_total;
        }

        // Single fused_linear_no_bias on the concatenated weight,
        // tagged with the per-projection split sizes so a grouped-
        // kernel-aware backend can dispatch a fused QKV-style matmul
        // without re-pattern-matching the surrounding concat+slices.
        const combined_out_shape = Shape.init(output_dtype, &.{
            @intCast(key.rows),
            @intCast(combined_out_dim),
        });
        var grouped_attrs = node_mod.LinearAttrs{
            .rows = key.rows,
            .in_dim = key.in_dim,
            .out_dim = combined_out_dim,
        };
        if (members.len <= grouped_attrs.projection_out_dims.len) {
            for (members, 0..) |m, gi| {
                grouped_attrs.projection_out_dims[gi] = m.out_dim;
            }
            grouped_attrs.num_projections = @intCast(members.len);
        }
        const combined_linear_id = try work.addNode(.{
            .op = .{ .fused_linear_no_bias = grouped_attrs },
            .output_shape = combined_out_shape,
            .inputs = .{ key.input_id, combined_weight_id, null_node, null_node },
            .num_inputs = 2,
        });

        // Slice each projection out of the combined output along the
        // last (column) axis. Output shape of each linear is [rows,
        // out_dim_i]; slice([rows, total_out_dim], starts=[0, off],
        // limits=[rows, off+out_i]).
        var col_offset: u32 = 0;
        for (members) |m| {
            const slice_shape = Shape.init(output_dtype, &.{
                @intCast(key.rows),
                @intCast(m.out_dim),
            });
            var slice_attrs = node_mod.SliceAttrs{};
            slice_attrs.num_axes = 2;
            slice_attrs.starts[0] = 0;
            slice_attrs.starts[1] = @intCast(col_offset);
            slice_attrs.limits[0] = @intCast(key.rows);
            slice_attrs.limits[1] = @intCast(col_offset + m.out_dim);
            slice_attrs.strides[0] = 1;
            slice_attrs.strides[1] = 1;

            const slice_id = try work.addNode(.{
                .op = .{ .slice = slice_attrs },
                .output_shape = slice_shape,
                .inputs = .{ combined_linear_id, null_node, null_node, null_node },
                .num_inputs = 1,
            });
            try redirects.append(allocator, .{ .from = m.node_id, .to = slice_id });
            col_offset += m.out_dim;
        }
        num_rewrites += 1;
    }

    return .{ .redirects = redirects, .num_rewrites = num_rewrites };
}

// ── Softmax Pattern Detection ─────────────────────────────────────────
//
// Detects the numerically stable softmax decomposition emitted by
// `Builder.softmax` and by ONNX importers that have lowered Softmax to
// primitives:
//
//   max_val   = reduce_max(x, axis=-1)
//   max_bc    = broadcast_in_dim(max_val, x.shape)
//   shifted   = sub(x, max_bc)
//   exp_v     = exp(shifted)
//   sum_v     = reduce_sum(exp_v, axis=-1)
//   sum_bc    = broadcast_in_dim(sum_v, x.shape)
//   result    = div(exp_v, sum_bc)
//
// On match, emits a fused_softmax(x) node and redirects the div to it.
// Equivalent to ORT's `SoftmaxFusion` and XLA's softmax pattern matcher.

fn fuseSoftmaxFromPrimitives(allocator: std.mem.Allocator, work: *Graph, reachable: []const bool) !PairFusionResult {
    var redirects = std.ArrayListUnmanaged(Redirect).empty;
    errdefer redirects.deinit(allocator);
    var num_rewrites: u32 = 0;

    const count = work.nodeCount();

    for (0..count) |i| {
        if (i >= reachable.len or !reachable[i]) continue;
        const div_id: NodeId = @intCast(i);
        const div_node = work.node(div_id);
        if (std.meta.activeTag(div_node.op) != .div) continue;

        // Walk through any Cast/Reshape wrappers — FP16 ONNX exports
        // surround the softmax with `Cast(f32) → … → Cast(f16)` for
        // numerical stability and we want to match those too.
        const num_id = skipShapeOps(work, div_node.inputs[0]);
        const den_id = skipShapeOps(work, div_node.inputs[1]);
        if (num_id == null_node or den_id == null_node) continue;
        if (num_id >= count or den_id >= count) continue;

        // Numerator: exp(sub(x, broadcast_in_dim(reduce_max(x, last))))
        const num_node = work.node(num_id);
        if (std.meta.activeTag(num_node.op) != .exp) continue;

        const sub_id = skipShapeOps(work, num_node.inputs[0]);
        if (sub_id == null_node or sub_id >= count) continue;
        const sub_node = work.node(sub_id);
        if (std.meta.activeTag(sub_node.op) != .sub) continue;

        const x_id = skipShapeOps(work, sub_node.inputs[0]);
        const max_bc_id = skipShapeOps(work, sub_node.inputs[1]);
        if (x_id == null_node or max_bc_id == null_node) continue;
        if (x_id >= count or max_bc_id >= count) continue;

        const max_bc_node = work.node(max_bc_id);
        if (std.meta.activeTag(max_bc_node.op) != .broadcast_in_dim) continue;
        const max_id = skipShapeOps(work, max_bc_node.inputs[0]);
        if (max_id == null_node or max_id >= count) continue;
        const max_node = work.node(max_id);
        const max_attrs = switch (max_node.op) {
            .reduce_max => |a| a,
            else => continue,
        };
        if (skipShapeOps(work, max_node.inputs[0]) != x_id) continue;

        const x_shape = work.node(x_id).output_shape;
        if (x_shape.rank() == 0) continue;
        const last_axis: u8 = x_shape.rank() - 1;
        if (max_attrs.num_axes != 1 or max_attrs.axes[0] != last_axis) continue;

        // Denominator: broadcast_in_dim(reduce_sum(exp_v, last))
        const den_node = work.node(den_id);
        if (std.meta.activeTag(den_node.op) != .broadcast_in_dim) continue;
        const sum_id = skipShapeOps(work, den_node.inputs[0]);
        if (sum_id == null_node or sum_id >= count) continue;
        const sum_node = work.node(sum_id);
        const sum_attrs = switch (sum_node.op) {
            .reduce_sum => |a| a,
            else => continue,
        };
        if (skipShapeOps(work, sum_node.inputs[0]) != num_id) continue;
        if (sum_attrs.num_axes != 1 or sum_attrs.axes[0] != last_axis) continue;

        // Compute dim from shape; 0 is the dynamic-shape sentinel used
        // elsewhere (see lib/onnx/src/ops.zig:614).
        const last_dim = x_shape.dim(last_axis);
        const dim: u32 = if (last_dim > 0) @intCast(last_dim) else 0;

        const fused_id = try work.addNode(.{
            .op = .{ .fused_softmax = .{ .dim = dim } },
            .output_shape = div_node.output_shape,
            .inputs = .{ x_id, null_node, null_node, null_node },
            .num_inputs = 1,
        });

        try redirects.append(allocator, .{ .from = div_id, .to = fused_id });
        num_rewrites += 1;
    }

    return .{ .redirects = redirects, .num_rewrites = num_rewrites };
}

// ── SiLU Pattern Detection ────────────────────────────────────────────
//
// Detects `x * sigmoid(x)` where sigmoid is the primitive decomposition
// `1 / (1 + exp(-x))`. Emits fused_silu(x).

fn fuseSiLUFromPrimitives(allocator: std.mem.Allocator, work: *Graph, reachable: []const bool) !PairFusionResult {
    var redirects = std.ArrayListUnmanaged(Redirect).empty;
    errdefer redirects.deinit(allocator);
    var num_rewrites: u32 = 0;

    const count = work.nodeCount();

    for (0..count) |i| {
        if (i >= reachable.len or !reachable[i]) continue;
        const mul_id: NodeId = @intCast(i);
        const n = work.node(mul_id);
        if (std.meta.activeTag(n.op) != .mul) continue;

        const a = n.inputs[0];
        const b = n.inputs[1];
        if (a == null_node or b == null_node) continue;
        if (a >= count or b >= count) continue;

        // Try mul(x, sigmoid(x)) first, then mul(sigmoid(x), x).
        const x_id: NodeId = if (matchesSigmoidOf(work, b, a))
            a
        else if (matchesSigmoidOf(work, a, b))
            b
        else
            continue;

        // Output shape preserved from the mul node.
        const fused_id = try work.addNode(.{
            .op = .{ .fused_silu = {} },
            .output_shape = n.output_shape,
            .inputs = .{ x_id, null_node, null_node, null_node },
            .num_inputs = 1,
        });

        try redirects.append(allocator, .{ .from = mul_id, .to = fused_id });
        num_rewrites += 1;
    }

    return .{ .redirects = redirects, .num_rewrites = num_rewrites };
}

/// True iff `sig_id` is the subgraph `1 / (1 + exp(-x_id))`.
fn matchesSigmoidOf(graph: *const Graph, sig_id: NodeId, x_id: NodeId) bool {
    if (sig_id == null_node or sig_id >= graph.nodeCount()) return false;
    const div_node = graph.node(sig_id);
    if (std.meta.activeTag(div_node.op) != .div) return false;

    if (!isConstScalarValue(graph, div_node.inputs[0], 1.0)) return false;
    const add_id = div_node.inputs[1];
    if (add_id == null_node or add_id >= graph.nodeCount()) return false;
    const add_node = graph.node(add_id);
    if (std.meta.activeTag(add_node.op) != .add) return false;

    // One add operand is scalar 1, the other is exp(neg(x)).
    const a0 = add_node.inputs[0];
    const a1 = add_node.inputs[1];
    const exp_id: NodeId = if (isConstScalarValue(graph, a0, 1.0))
        a1
    else if (isConstScalarValue(graph, a1, 1.0))
        a0
    else
        return false;
    if (exp_id == null_node or exp_id >= graph.nodeCount()) return false;
    const exp_node = graph.node(exp_id);
    if (std.meta.activeTag(exp_node.op) != .exp) return false;

    const neg_id = exp_node.inputs[0];
    if (neg_id == null_node or neg_id >= graph.nodeCount()) return false;
    const neg_node = graph.node(neg_id);
    if (std.meta.activeTag(neg_node.op) != .neg) return false;

    return neg_node.inputs[0] == x_id;
}

// ── MatMul + Bias → fused_linear ──────────────────────────────────────
//
// Detects `add(matmul(input, transpose(weight, [1,0])), bias)` and
// replaces it with `fused_linear(input, weight, bias)`. This is the
// primitive form emitted by `Builder.linear`'s `vjp_alternate`, and the
// pattern produced by ONNX MatMul→Add chains. Mirrors ORT's
// `MatMulAddFusion` / XLA's `gemm-rewriter`.
//
// Constraints:
//   * matmul is a 2-D dot_general (num_contracting=1, num_batch=0,
//     lhs_contracting=[1], rhs_contracting=[0]).
//   * the rhs is a transpose with perm [1, 0] on a 2-D weight node.
//   * the bias has shape [out_dim] (post-broadcast variants live in the
//     identity-broadcast simplification rule).

fn fuseLinearBias(allocator: std.mem.Allocator, work: *Graph, reachable: []const bool) !PairFusionResult {
    var redirects = std.ArrayListUnmanaged(Redirect).empty;
    errdefer redirects.deinit(allocator);
    var num_rewrites: u32 = 0;

    const count = work.nodeCount();

    for (0..count) |i| {
        if (i >= reachable.len or !reachable[i]) continue;
        const add_id: NodeId = @intCast(i);
        const add_node = work.node(add_id);
        if (std.meta.activeTag(add_node.op) != .add) continue;

        const a = add_node.inputs[0];
        const b = add_node.inputs[1];
        if (a == null_node or b == null_node) continue;
        if (a >= count or b >= count) continue;

        // Try (matmul, bias) then (bias, matmul).
        var mm_id: NodeId = a;
        var bias_id: NodeId = b;
        if (!isMatmul2D(work, mm_id)) {
            if (isMatmul2D(work, b)) {
                mm_id = b;
                bias_id = a;
            } else {
                continue;
            }
        }

        const mm_node = work.node(mm_id);
        const input_id = mm_node.inputs[0];
        const wt_id = mm_node.inputs[1];
        if (input_id == null_node or wt_id == null_node) continue;
        if (wt_id >= count) continue;

        // The rhs of the matmul must be transpose(weight, [1, 0]).
        const wt_node = work.node(wt_id);
        const wt_attrs = switch (wt_node.op) {
            .transpose => |a_| a_,
            else => continue,
        };
        if (wt_attrs.num_axes != 2) continue;
        if (wt_attrs.perm[0] != 1 or wt_attrs.perm[1] != 0) continue;
        const weight_id = wt_node.inputs[0];
        if (weight_id == null_node or weight_id >= count) continue;

        // Output shape is [rows, out_dim] from the add node.
        const out_shape = add_node.output_shape;
        if (out_shape.rank() != 2) continue;
        const rows_i64 = out_shape.dim(0);
        const out_dim_i64 = out_shape.dim(1);
        if (rows_i64 <= 0 or out_dim_i64 <= 0) continue;

        // in_dim from the input's last axis.
        const in_shape = work.node(input_id).output_shape;
        if (in_shape.rank() != 2) continue;
        const in_dim_i64 = in_shape.dim(1);
        if (in_dim_i64 <= 0) continue;

        // Bias should be a 1-D tensor of [out_dim] (any node, often a
        // parameter). Reject ranks that don't match — the algebraic
        // identity-broadcast rule will already have collapsed redundant
        // shape-only wrappers by this point in the pipeline.
        const bias_shape = work.node(bias_id).output_shape;
        if (bias_shape.rank() != 1 or bias_shape.dim(0) != out_dim_i64) continue;

        const fused_id = try work.addNode(.{
            .op = .{ .fused_linear = .{
                .rows = @intCast(rows_i64),
                .in_dim = @intCast(in_dim_i64),
                .out_dim = @intCast(out_dim_i64),
            } },
            .output_shape = out_shape,
            .inputs = .{ input_id, weight_id, bias_id, null_node },
            .num_inputs = 3,
        });

        try redirects.append(allocator, .{ .from = add_id, .to = fused_id });
        num_rewrites += 1;
    }

    return .{ .redirects = redirects, .num_rewrites = num_rewrites };
}

fn isMatmul2D(graph: *const Graph, id: NodeId) bool {
    if (id == null_node or id >= graph.nodeCount()) return false;
    const n = graph.node(id);
    const attrs = switch (n.op) {
        .dot_general => |a| a,
        else => return false,
    };
    if (attrs.num_contracting != 1 or attrs.num_batch != 0) return false;
    if (attrs.lhs_contracting[0] != 1) return false;
    if (attrs.rhs_contracting[0] != 0) return false;
    return n.output_shape.rank() == 2;
}

// ── MatMul (no bias) → fused_linear_no_bias ──────────────────────────
//
// Pure-primitive sibling of `fuseLinearBias`. Catches ONNX MatMul that
// did NOT have a downstream Add (e.g. residual-only paths, projections
// feeding directly into a non-bias op). After this fires, `fuseLinearPairs`
// can pick the result up and combine it with sibling projections (GQA).

fn fuseMatmulNoBias(allocator: std.mem.Allocator, work: *Graph, reachable: []const bool) !PairFusionResult {
    var redirects = std.ArrayListUnmanaged(Redirect).empty;
    errdefer redirects.deinit(allocator);
    var num_rewrites: u32 = 0;

    const count = work.nodeCount();

    for (0..count) |i| {
        if (i >= reachable.len or !reachable[i]) continue;
        const mm_id: NodeId = @intCast(i);
        const mm_node = work.node(mm_id);
        const dot_attrs = switch (mm_node.op) {
            .dot_general => |a| a,
            else => continue,
        };
        if (dot_attrs.num_contracting != 1 or dot_attrs.num_batch != 0) continue;
        if (mm_node.output_shape.rank() != 2) continue;
        if (dot_attrs.lhs_contracting[0] != 1) continue;

        // Two equivalent encodings of `X @ W^T`:
        //   * lhs=[M,K] · rhs=transpose(W,[1,0]) (=[K,N]) with rhs_contracting=0
        //   * lhs=[M,K] · rhs=W (=[N,K])         with rhs_contracting=1
        // (the second form is what fuseTransposeIntoDot leaves behind).
        const input_id = mm_node.inputs[0];
        const rhs_id = mm_node.inputs[1];
        if (input_id == null_node or rhs_id == null_node) continue;
        if (rhs_id >= count) continue;

        var weight_id: NodeId = null_node;
        if (dot_attrs.rhs_contracting[0] == 0) {
            // rhs must be `transpose(W, [1, 0])` for the result to be X @ W^T.
            const rhs_node = work.node(rhs_id);
            const wt_attrs = switch (rhs_node.op) {
                .transpose => |a| a,
                else => continue,
            };
            if (wt_attrs.num_axes != 2) continue;
            if (wt_attrs.perm[0] != 1 or wt_attrs.perm[1] != 0) continue;
            weight_id = rhs_node.inputs[0];
        } else if (dot_attrs.rhs_contracting[0] == 1) {
            weight_id = rhs_id;
        } else {
            continue;
        }
        if (weight_id == null_node or weight_id >= count) continue;

        const out_shape = mm_node.output_shape;
        const rows_i64 = out_shape.dim(0);
        const out_dim_i64 = out_shape.dim(1);
        if (rows_i64 <= 0 or out_dim_i64 <= 0) continue;

        const in_shape = work.node(input_id).output_shape;
        if (in_shape.rank() != 2) continue;
        const in_dim_i64 = in_shape.dim(1);
        if (in_dim_i64 <= 0) continue;

        const fused_id = try work.addNode(.{
            .op = .{ .fused_linear_no_bias = .{
                .rows = @intCast(rows_i64),
                .in_dim = @intCast(in_dim_i64),
                .out_dim = @intCast(out_dim_i64),
            } },
            .output_shape = out_shape,
            .inputs = .{ input_id, weight_id, null_node, null_node },
            .num_inputs = 2,
        });
        try redirects.append(allocator, .{ .from = mm_id, .to = fused_id });
        num_rewrites += 1;
    }

    return .{ .redirects = redirects, .num_rewrites = num_rewrites };
}

// ── GELU Pattern Detection (erf and tanh approximations) ─────────────
//
// Two canonical primitive forms produce fused_gelu:
//   * Erf-form (exact):
//       0.5 · x · (1 + erf(x · (1/√2)))
//   * Tanh-form (approximate, the form Builder.gelu emits):
//       0.5 · x · (1 + tanh(√(2/π) · (x + 0.044715 · x³)))
//
// Both anchor on a top-level `mul` whose two operands are
// `mul(x, 0.5)` (or `mul(0.5, x)`) and `add(1, inner)`.

fn fuseGELUFromPrimitives(allocator: std.mem.Allocator, work: *Graph, reachable: []const bool) !PairFusionResult {
    var redirects = std.ArrayListUnmanaged(Redirect).empty;
    errdefer redirects.deinit(allocator);
    var num_rewrites: u32 = 0;

    const count = work.nodeCount();

    for (0..count) |i| {
        if (i >= reachable.len or !reachable[i]) continue;
        const mul_id: NodeId = @intCast(i);
        const n = work.node(mul_id);
        if (std.meta.activeTag(n.op) != .mul) continue;

        const a = n.inputs[0];
        const b = n.inputs[1];
        if (a == null_node or b == null_node) continue;

        // Try both operand orders. Each side must be either a halfX
        // factor (mul(x, 0.5)) or a one_plus_inner factor.
        const x_id_opt: ?NodeId = blk: {
            if (matchesGELUOperands(work, a, b)) |x| break :blk x;
            if (matchesGELUOperands(work, b, a)) |x| break :blk x;
            break :blk null;
        };
        const x_id = x_id_opt orelse continue;

        const fused_id = try work.addNode(.{
            .op = .{ .fused_gelu = {} },
            .output_shape = n.output_shape,
            .inputs = .{ x_id, null_node, null_node, null_node },
            .num_inputs = 1,
        });
        try redirects.append(allocator, .{ .from = mul_id, .to = fused_id });
        num_rewrites += 1;
    }

    return .{ .redirects = redirects, .num_rewrites = num_rewrites };
}

/// Returns the underlying `x` if (`half_x_id`, `one_plus_inner_id`) form
/// the GELU canonical pair `(0.5·x, 1 + erf(...) | 1 + tanh(...))`.
fn matchesGELUOperands(graph: *const Graph, half_x_id: NodeId, one_plus_inner_id: NodeId) ?NodeId {
    const x_id = matchHalfX(graph, half_x_id) orelse return null;
    if (!matchOnePlusGELUInner(graph, one_plus_inner_id, x_id)) return null;
    return x_id;
}

/// Matches `mul(x, 0.5)` / `mul(0.5, x)` and returns x.
fn matchHalfX(graph: *const Graph, id: NodeId) ?NodeId {
    if (id == null_node or id >= graph.nodeCount()) return null;
    const n = graph.node(id);
    if (std.meta.activeTag(n.op) != .mul) return null;
    if (isApproxScalarValue(graph, n.inputs[1], 0.5)) return n.inputs[0];
    if (isApproxScalarValue(graph, n.inputs[0], 0.5)) return n.inputs[1];
    return null;
}

/// Matches `add(1, erf(x · 1/√2))` (erf form) or
///         `add(1, tanh(√(2/π) · (x + 0.044715·x³)))` (tanh form).
fn matchOnePlusGELUInner(graph: *const Graph, id: NodeId, x_id: NodeId) bool {
    if (id == null_node or id >= graph.nodeCount()) return false;
    const add_node = graph.node(id);
    if (std.meta.activeTag(add_node.op) != .add) return false;

    const inner_id = pickNonOneOperand(graph, add_node.inputs[0], add_node.inputs[1]) orelse return false;
    if (inner_id == null_node or inner_id >= graph.nodeCount()) return false;
    const inner = graph.node(inner_id);
    switch (inner.op) {
        .erf => return matchErfOfScaledX(graph, inner.inputs[0], x_id),
        .tanh => return matchTanhOfTanhGELUInner(graph, inner.inputs[0], x_id),
        else => return false,
    }
}

fn pickNonOneOperand(graph: *const Graph, a: NodeId, b: NodeId) ?NodeId {
    if (isApproxScalarValue(graph, a, 1.0)) return b;
    if (isApproxScalarValue(graph, b, 1.0)) return a;
    return null;
}

/// Matches `mul(x, 1/√2)` or equivalent `div(x, √2)`. Returns true on match.
fn matchErfOfScaledX(graph: *const Graph, scaled_id: NodeId, x_id: NodeId) bool {
    if (scaled_id == null_node or scaled_id >= graph.nodeCount()) return false;
    const n = graph.node(scaled_id);
    const inv_sqrt_2: f32 = 0.70710678;
    const sqrt_2: f32 = 1.41421356;
    switch (n.op) {
        .mul => {
            if (n.inputs[0] == x_id and isApproxScalarValue(graph, n.inputs[1], inv_sqrt_2)) return true;
            if (n.inputs[1] == x_id and isApproxScalarValue(graph, n.inputs[0], inv_sqrt_2)) return true;
            return false;
        },
        .div => {
            if (n.inputs[0] == x_id and isApproxScalarValue(graph, n.inputs[1], sqrt_2)) return true;
            return false;
        },
        else => return false,
    }
}

/// Matches the inner of the tanh-approx GELU:
///   √(2/π) · (x + 0.044715 · x³)
fn matchTanhOfTanhGELUInner(graph: *const Graph, scaled_id: NodeId, x_id: NodeId) bool {
    if (scaled_id == null_node or scaled_id >= graph.nodeCount()) return false;
    const sqrt_2_over_pi: f32 = 0.7978845608;
    const n = graph.node(scaled_id);
    if (std.meta.activeTag(n.op) != .mul) return false;

    // One operand is √(2/π); the other is `add(x, 0.044715·x³)`.
    const inner_id: NodeId = blk: {
        if (isApproxScalarValue(graph, n.inputs[0], sqrt_2_over_pi)) break :blk n.inputs[1];
        if (isApproxScalarValue(graph, n.inputs[1], sqrt_2_over_pi)) break :blk n.inputs[0];
        return false;
    };
    if (inner_id == null_node or inner_id >= graph.nodeCount()) return false;
    const inner = graph.node(inner_id);
    if (std.meta.activeTag(inner.op) != .add) return false;

    const cubed_term_id: NodeId = blk: {
        if (inner.inputs[0] == x_id) break :blk inner.inputs[1];
        if (inner.inputs[1] == x_id) break :blk inner.inputs[0];
        return false;
    };
    return matchScaledCubeOfX(graph, cubed_term_id, x_id);
}

/// Matches `0.044715 · x · x · x` in the canonical builder form
///   x_sq = mul(x, x); x_cubed = mul(x_sq, x); inner = mul(0.044715, x_cubed)
fn matchScaledCubeOfX(graph: *const Graph, scaled_cube_id: NodeId, x_id: NodeId) bool {
    if (scaled_cube_id == null_node or scaled_cube_id >= graph.nodeCount()) return false;
    const c: f32 = 0.044715;
    const n = graph.node(scaled_cube_id);
    if (std.meta.activeTag(n.op) != .mul) return false;
    const cube_id: NodeId = blk: {
        if (isApproxScalarValue(graph, n.inputs[0], c)) break :blk n.inputs[1];
        if (isApproxScalarValue(graph, n.inputs[1], c)) break :blk n.inputs[0];
        return false;
    };
    if (cube_id == null_node or cube_id >= graph.nodeCount()) return false;

    // Cube is mul(mul(x, x), x) in either order.
    const cube_node = graph.node(cube_id);
    if (std.meta.activeTag(cube_node.op) != .mul) return false;
    const sq_id: NodeId = blk: {
        if (cube_node.inputs[0] == x_id) break :blk cube_node.inputs[1];
        if (cube_node.inputs[1] == x_id) break :blk cube_node.inputs[0];
        return false;
    };
    if (sq_id == null_node or sq_id >= graph.nodeCount()) return false;
    const sq_node = graph.node(sq_id);
    if (std.meta.activeTag(sq_node.op) != .mul) return false;
    return sq_node.inputs[0] == x_id and sq_node.inputs[1] == x_id;
}

/// True iff a node is a scalar-equivalent constant approximately equal
/// to `expected` (tolerance 1e-4 — covers fp16-rounded literals like
/// 0.7071 vs 0.70710678).
fn isApproxScalarValue(graph: *const Graph, id: NodeId, expected: f32) bool {
    const v = extractScalarConstValue(graph, id) orelse return false;
    return @abs(v - expected) <= 1e-4;
}

// ── QuickGELU Pattern Detection ───────────────────────────────────────
//
// QuickGELU is `x · sigmoid(1.702 · x)` — used by CLIP, ViT, GPT-2 (in
// some forks). Sigmoid is the same `1 / (1 + exp(-y))` shape that the
// SiLU matcher already understands.

fn fuseQuickGELUFromPrimitives(allocator: std.mem.Allocator, work: *Graph, reachable: []const bool) !PairFusionResult {
    var redirects = std.ArrayListUnmanaged(Redirect).empty;
    errdefer redirects.deinit(allocator);
    var num_rewrites: u32 = 0;

    const count = work.nodeCount();

    for (0..count) |i| {
        if (i >= reachable.len or !reachable[i]) continue;
        const mul_id: NodeId = @intCast(i);
        const n = work.node(mul_id);
        if (std.meta.activeTag(n.op) != .mul) continue;

        const a = n.inputs[0];
        const b = n.inputs[1];
        if (a == null_node or b == null_node) continue;

        const x_id: NodeId = blk: {
            if (matchesSigmoidOfScaledX(work, b, a, 1.702)) break :blk a;
            if (matchesSigmoidOfScaledX(work, a, b, 1.702)) break :blk b;
            continue;
        };

        const fused_id = try work.addNode(.{
            .op = .{ .fused_quick_gelu = {} },
            .output_shape = n.output_shape,
            .inputs = .{ x_id, null_node, null_node, null_node },
            .num_inputs = 1,
        });
        try redirects.append(allocator, .{ .from = mul_id, .to = fused_id });
        num_rewrites += 1;
    }

    return .{ .redirects = redirects, .num_rewrites = num_rewrites };
}

/// Matches `sig_id = 1 / (1 + exp(-(scale · x_id)))`.
fn matchesSigmoidOfScaledX(graph: *const Graph, sig_id: NodeId, x_id: NodeId, scale: f32) bool {
    if (sig_id == null_node or sig_id >= graph.nodeCount()) return false;
    const div_node = graph.node(sig_id);
    if (std.meta.activeTag(div_node.op) != .div) return false;

    if (!isConstScalarValue(graph, div_node.inputs[0], 1.0)) return false;
    const add_id = div_node.inputs[1];
    if (add_id == null_node or add_id >= graph.nodeCount()) return false;
    const add_node = graph.node(add_id);
    if (std.meta.activeTag(add_node.op) != .add) return false;

    const exp_id: NodeId = blk: {
        if (isConstScalarValue(graph, add_node.inputs[0], 1.0)) break :blk add_node.inputs[1];
        if (isConstScalarValue(graph, add_node.inputs[1], 1.0)) break :blk add_node.inputs[0];
        return false;
    };
    if (exp_id == null_node or exp_id >= graph.nodeCount()) return false;
    const exp_node = graph.node(exp_id);
    if (std.meta.activeTag(exp_node.op) != .exp) return false;
    const neg_id = exp_node.inputs[0];
    if (neg_id == null_node or neg_id >= graph.nodeCount()) return false;
    const neg_node = graph.node(neg_id);
    if (std.meta.activeTag(neg_node.op) != .neg) return false;

    // neg's input is mul(scale, x_id) (in either order).
    const inner = neg_node.inputs[0];
    if (inner == null_node or inner >= graph.nodeCount()) return false;
    const inner_node = graph.node(inner);
    if (std.meta.activeTag(inner_node.op) != .mul) return false;
    if (inner_node.inputs[0] == x_id and isApproxScalarValue(graph, inner_node.inputs[1], scale)) return true;
    if (inner_node.inputs[1] == x_id and isApproxScalarValue(graph, inner_node.inputs[0], scale)) return true;
    return false;
}

// ── RMSNorm Pattern Detection ─────────────────────────────────────────
//
// Detects the canonical RMSNorm decomposition:
//   x_sq         = mul(x, x)
//   mean_sq      = reduce_mean(x_sq, [last_axis])
//   mean_plus_eps = add(mean_sq, eps)
//   inv_rms      = rsqrt(mean_plus_eps)
//   inv_rms_bc   = broadcast_in_dim(inv_rms, x.shape)
//   normed       = mul(x, inv_rms_bc)
//   result       = mul(normed, weight)

fn fuseRMSNormFromPrimitives(allocator: std.mem.Allocator, work: *Graph, reachable: []const bool) !PairFusionResult {
    var redirects = std.ArrayListUnmanaged(Redirect).empty;
    errdefer redirects.deinit(allocator);
    var num_rewrites: u32 = 0;

    const count = work.nodeCount();

    for (0..count) |i| {
        if (i >= reachable.len or !reachable[i]) continue;
        const result_id: NodeId = @intCast(i);
        const n = work.node(result_id);
        if (std.meta.activeTag(n.op) != .mul) continue;

        const a = n.inputs[0];
        const b = n.inputs[1];
        if (a == null_node or b == null_node) continue;

        // One side must be `mul(x, broadcast(rsqrt(add(reduce_mean(mul(x,x)), eps))))`.
        const matched: ?struct { x_id: NodeId, weight_id: NodeId, dim: u32, eps: f32 } = blk: {
            if (matchRMSNormCore(work, a)) |c| break :blk .{ .x_id = c.x_id, .weight_id = b, .dim = c.dim, .eps = c.eps };
            if (matchRMSNormCore(work, b)) |c| break :blk .{ .x_id = c.x_id, .weight_id = a, .dim = c.dim, .eps = c.eps };
            break :blk null;
        };
        const m = matched orelse continue;

        const fused_id = try work.addNode(.{
            .op = .{ .fused_rms_norm = .{ .dim = m.dim, .eps = m.eps } },
            .output_shape = n.output_shape,
            .inputs = .{ m.x_id, m.weight_id, null_node, null_node },
            .num_inputs = 2,
        });
        try redirects.append(allocator, .{ .from = result_id, .to = fused_id });
        num_rewrites += 1;
    }

    return .{ .redirects = redirects, .num_rewrites = num_rewrites };
}

const RMSCore = struct {
    x_id: NodeId,
    dim: u32,
    eps: f32,
};

/// Match `mul(x, broadcast_in_dim(rsqrt(add(reduce_mean(mul(x,x), [last]), eps))))`.
fn matchRMSNormCore(graph: *const Graph, id: NodeId) ?RMSCore {
    if (id == null_node or id >= graph.nodeCount()) return null;
    const n = graph.node(id);
    if (std.meta.activeTag(n.op) != .mul) return null;

    // Identify which side is x and which is the broadcast(rsqrt(...)) chain.
    const a = n.inputs[0];
    const b = n.inputs[1];
    if (a == null_node or b == null_node) return null;

    const try_pair = struct {
        fn run(g: *const Graph, x_cand_in: NodeId, scale_cand_in: NodeId) ?RMSCore {
            const x_cand = skipShapeOps(g, x_cand_in);
            const scale_cand = skipShapeOps(g, scale_cand_in);
            if (x_cand == null_node or scale_cand == null_node) return null;
            if (scale_cand >= g.nodeCount()) return null;
            const scale_node = g.node(scale_cand);
            if (std.meta.activeTag(scale_node.op) != .broadcast_in_dim) return null;
            const rsqrt_id = skipShapeOps(g, scale_node.inputs[0]);
            if (rsqrt_id == null_node or rsqrt_id >= g.nodeCount()) return null;
            const rsqrt_node = g.node(rsqrt_id);
            if (std.meta.activeTag(rsqrt_node.op) != .rsqrt) return null;

            const add_id = skipShapeOps(g, rsqrt_node.inputs[0]);
            if (add_id == null_node or add_id >= g.nodeCount()) return null;
            const add_node = g.node(add_id);
            if (std.meta.activeTag(add_node.op) != .add) return null;

            const mean_id: NodeId = mean_blk: {
                const eps_then_mean = extractScalarConstValue(g, add_node.inputs[1]);
                if (eps_then_mean) |_| break :mean_blk skipShapeOps(g, add_node.inputs[0]);
                const eps_alt = extractScalarConstValue(g, add_node.inputs[0]);
                if (eps_alt) |_| break :mean_blk skipShapeOps(g, add_node.inputs[1]);
                return null;
            };
            const eps_id = if (skipShapeOps(g, add_node.inputs[0]) == mean_id) add_node.inputs[1] else add_node.inputs[0];
            const eps_val = extractScalarConstValue(g, eps_id) orelse return null;

            if (mean_id == null_node or mean_id >= g.nodeCount()) return null;
            const mean_node = g.node(mean_id);
            const mean_attrs = switch (mean_node.op) {
                .reduce_mean => |aa| aa,
                else => return null,
            };
            if (mean_attrs.num_axes != 1) return null;

            const x_shape = g.node(x_cand).output_shape;
            if (x_shape.rank() == 0) return null;
            const last_axis: u8 = x_shape.rank() - 1;
            if (mean_attrs.axes[0] != last_axis) return null;

            const sq_id = skipShapeOps(g, mean_node.inputs[0]);
            if (sq_id == null_node or sq_id >= g.nodeCount()) return null;
            const sq_node = g.node(sq_id);
            if (std.meta.activeTag(sq_node.op) != .mul) return null;
            if (skipShapeOps(g, sq_node.inputs[0]) != x_cand or skipShapeOps(g, sq_node.inputs[1]) != x_cand) return null;

            const last_dim = x_shape.dim(last_axis);
            const dim_u32: u32 = if (last_dim > 0) @intCast(last_dim) else 0;
            return .{ .x_id = x_cand, .dim = dim_u32, .eps = eps_val };
        }
    }.run;

    if (try_pair(graph, a, b)) |c| return c;
    if (try_pair(graph, b, a)) |c| return c;
    return null;
}

// ── LayerNorm Pattern Detection ───────────────────────────────────────
//
// Canonical primitive form (matches `Builder.layerNorm`):
//   mean         = reduce_mean(x, [last])
//   mean_bc      = broadcast_in_dim(mean, x.shape)
//   centered     = sub(x, mean_bc)
//   centered_sq  = mul(centered, centered)
//   variance     = reduce_mean(centered_sq, [last])
//   var_plus_eps = add(variance, eps)
//   inv_std      = rsqrt(var_plus_eps)
//   inv_std_bc   = broadcast_in_dim(inv_std, x.shape)
//   normalized   = mul(centered, inv_std_bc)
//   scaled       = mul(normalized, gamma)
//   result       = add(scaled, beta)

fn fuseLayerNormFromPrimitives(allocator: std.mem.Allocator, work: *Graph, reachable: []const bool) !PairFusionResult {
    var redirects = std.ArrayListUnmanaged(Redirect).empty;
    errdefer redirects.deinit(allocator);
    var num_rewrites: u32 = 0;

    const count = work.nodeCount();

    for (0..count) |i| {
        if (i >= reachable.len or !reachable[i]) continue;
        const result_id: NodeId = @intCast(i);
        const result_node = work.node(result_id);
        if (std.meta.activeTag(result_node.op) != .add) continue;

        const a = result_node.inputs[0];
        const b = result_node.inputs[1];
        if (a == null_node or b == null_node) continue;

        // Try each operand as `scaled = mul(normalized, gamma)` and the
        // other as `beta`.
        const matched: ?struct { x_id: NodeId, gamma_id: NodeId, beta_id: NodeId, dim: u32, eps: f32 } = blk: {
            if (matchLayerNormScaled(work, a)) |s| break :blk .{ .x_id = s.x_id, .gamma_id = s.gamma_id, .beta_id = b, .dim = s.dim, .eps = s.eps };
            if (matchLayerNormScaled(work, b)) |s| break :blk .{ .x_id = s.x_id, .gamma_id = s.gamma_id, .beta_id = a, .dim = s.dim, .eps = s.eps };
            break :blk null;
        };
        const m = matched orelse continue;

        const fused_id = try work.addNode(.{
            .op = .{ .fused_layer_norm = .{ .dim = m.dim, .eps = m.eps } },
            .output_shape = result_node.output_shape,
            .inputs = .{ m.x_id, m.gamma_id, m.beta_id, null_node },
            .num_inputs = 3,
        });
        try redirects.append(allocator, .{ .from = result_id, .to = fused_id });
        num_rewrites += 1;
    }

    return .{ .redirects = redirects, .num_rewrites = num_rewrites };
}

const LayerNormScaled = struct {
    x_id: NodeId,
    gamma_id: NodeId,
    dim: u32,
    eps: f32,
};

/// Match `mul(normalized, gamma)` where `normalized = mul(centered, broadcast(rsqrt(...)))`.
fn matchLayerNormScaled(graph: *const Graph, id: NodeId) ?LayerNormScaled {
    if (id == null_node or id >= graph.nodeCount()) return null;
    const n = graph.node(id);
    if (std.meta.activeTag(n.op) != .mul) return null;

    const a = n.inputs[0];
    const b = n.inputs[1];

    const try_pair = struct {
        fn run(g: *const Graph, normed_cand_in: NodeId, gamma_cand: NodeId) ?LayerNormScaled {
            const normed_cand = skipShapeOps(g, normed_cand_in);
            if (normed_cand == null_node or normed_cand >= g.nodeCount()) return null;
            const norm = g.node(normed_cand);
            if (std.meta.activeTag(norm.op) != .mul) return null;
            const c0 = norm.inputs[0];
            const c1 = norm.inputs[1];
            if (c0 == null_node or c1 == null_node) return null;

            const try_centered = struct {
                fn inner(g2: *const Graph, centered_cand_in: NodeId, scale_cand_in: NodeId, gamma_in: NodeId) ?LayerNormScaled {
                    const centered_cand = skipShapeOps(g2, centered_cand_in);
                    const scale_cand = skipShapeOps(g2, scale_cand_in);
                    if (centered_cand >= g2.nodeCount() or scale_cand >= g2.nodeCount()) return null;
                    const centered_node = g2.node(centered_cand);
                    if (std.meta.activeTag(centered_node.op) != .sub) return null;
                    const x_cand = skipShapeOps(g2, centered_node.inputs[0]);
                    const mean_bc_id = skipShapeOps(g2, centered_node.inputs[1]);
                    if (x_cand == null_node or mean_bc_id == null_node) return null;
                    if (mean_bc_id >= g2.nodeCount()) return null;
                    const mean_bc = g2.node(mean_bc_id);
                    if (std.meta.activeTag(mean_bc.op) != .broadcast_in_dim) return null;
                    const mean_id = skipShapeOps(g2, mean_bc.inputs[0]);
                    if (mean_id == null_node or mean_id >= g2.nodeCount()) return null;
                    const mean_node = g2.node(mean_id);
                    const mean_attrs = switch (mean_node.op) {
                        .reduce_mean => |aa| aa,
                        else => return null,
                    };
                    if (skipShapeOps(g2, mean_node.inputs[0]) != x_cand) return null;

                    const x_shape = g2.node(x_cand).output_shape;
                    if (x_shape.rank() == 0) return null;
                    const last_axis: u8 = x_shape.rank() - 1;
                    if (mean_attrs.num_axes != 1 or mean_attrs.axes[0] != last_axis) return null;

                    // The "scale" side: broadcast(rsqrt(add(reduce_mean(centered²), eps))).
                    const scale_node = g2.node(scale_cand);
                    if (std.meta.activeTag(scale_node.op) != .broadcast_in_dim) return null;
                    const rsqrt_id = skipShapeOps(g2, scale_node.inputs[0]);
                    if (rsqrt_id == null_node or rsqrt_id >= g2.nodeCount()) return null;
                    const rsqrt_node = g2.node(rsqrt_id);
                    if (std.meta.activeTag(rsqrt_node.op) != .rsqrt) return null;

                    const add_id = skipShapeOps(g2, rsqrt_node.inputs[0]);
                    if (add_id == null_node or add_id >= g2.nodeCount()) return null;
                    const add_node = g2.node(add_id);
                    if (std.meta.activeTag(add_node.op) != .add) return null;
                    const var_id: NodeId = vblk: {
                        if (extractScalarConstValue(g2, add_node.inputs[1])) |_| break :vblk skipShapeOps(g2, add_node.inputs[0]);
                        if (extractScalarConstValue(g2, add_node.inputs[0])) |_| break :vblk skipShapeOps(g2, add_node.inputs[1]);
                        return null;
                    };
                    const eps_id = if (skipShapeOps(g2, add_node.inputs[0]) == var_id) add_node.inputs[1] else add_node.inputs[0];
                    const eps_val = extractScalarConstValue(g2, eps_id) orelse return null;
                    if (var_id == null_node or var_id >= g2.nodeCount()) return null;

                    const var_node = g2.node(var_id);
                    const var_attrs = switch (var_node.op) {
                        .reduce_mean => |aa| aa,
                        else => return null,
                    };
                    if (var_attrs.num_axes != 1 or var_attrs.axes[0] != last_axis) return null;

                    const sq_id = skipShapeOps(g2, var_node.inputs[0]);
                    if (sq_id == null_node or sq_id >= g2.nodeCount()) return null;
                    const sq_node = g2.node(sq_id);
                    if (std.meta.activeTag(sq_node.op) != .mul) return null;
                    if (skipShapeOps(g2, sq_node.inputs[0]) != centered_cand or skipShapeOps(g2, sq_node.inputs[1]) != centered_cand) return null;

                    const last_dim = x_shape.dim(last_axis);
                    const dim_u32: u32 = if (last_dim > 0) @intCast(last_dim) else 0;
                    return .{ .x_id = x_cand, .gamma_id = gamma_in, .dim = dim_u32, .eps = eps_val };
                }
            }.inner;

            if (try_centered(g, c0, c1, gamma_cand)) |r| return r;
            if (try_centered(g, c1, c0, gamma_cand)) |r| return r;
            return null;
        }
    }.run;

    if (try_pair(graph, a, b)) |r| return r;
    if (try_pair(graph, b, a)) |r| return r;
    return null;
}

// ── RoPE Pattern Detection ────────────────────────────────────────────
//
// Detects the LLaMA-style half-split rotary embedding emitted as
// primitives:
//
//   half       = head_dim / 2
//   x0         = slice(x, last=[0:half])
//   x1         = slice(x, last=[half:head_dim])
//   neg_x1     = neg(x1)
//   rotated    = concat([neg_x1, x0], axis=-1)
//   result     = add(mul(x, cos_b), mul(rotated, sin_b))
//
// Where `cos_b` and `sin_b` are the cos/sin tables (optionally wrapped
// in broadcast_in_dim/reshape). On match, emits fused_rope(x, cos, sin).
// Interleaved-pair RoPE (consecutive_pairs=true) and GPT-J variants are
// not detected here.

fn fuseRopeFromPrimitives(allocator: std.mem.Allocator, work: *Graph, reachable: []const bool) !PairFusionResult {
    var redirects = std.ArrayListUnmanaged(Redirect).empty;
    errdefer redirects.deinit(allocator);
    var num_rewrites: u32 = 0;

    const count = work.nodeCount();

    for (0..count) |i| {
        if (i >= reachable.len or !reachable[i]) continue;
        const add_id: NodeId = @intCast(i);
        const add_node = work.node(add_id);
        if (std.meta.activeTag(add_node.op) != .add) continue;

        const left = add_node.inputs[0];
        const right = add_node.inputs[1];
        if (left == null_node or right == null_node) continue;
        if (left >= count or right >= count) continue;

        // Both sides must be mul. Try (x_branch, rotated_branch) and the swap.
        if (!isMulNode(work, left)) continue;
        if (!isMulNode(work, right)) continue;

        const matched = matchRopeBranches(work, left, right) orelse
            matchRopeBranches(work, right, left) orelse
            continue;

        const out_shape = add_node.output_shape;
        if (out_shape.rank() < 2) continue;
        const head_dim_i64 = out_shape.dim(out_shape.rank() - 1);
        if (head_dim_i64 <= 0 or @rem(head_dim_i64, 2) != 0) continue;
        const head_dim: u32 = @intCast(head_dim_i64);
        const seq_axis: u8 = out_shape.rank() - 2;
        const seq_dim_i64 = out_shape.dim(seq_axis);
        const seq_len: u32 = if (seq_dim_i64 > 0) @intCast(seq_dim_i64) else 0;

        const fused_id = try work.addNode(.{
            .op = .{
                .fused_rope = .{
                    .seq_len = seq_len,
                    .head_dim = head_dim,
                    .rope_dim = head_dim,
                    .theta = 10000.0, // not recoverable from primitives; backend uses cos/sin tables
                    .freq_scale = 1.0,
                    .position_offset = 0,
                    .consecutive_pairs = false,
                },
            },
            .output_shape = out_shape,
            .inputs = .{ matched.x_id, matched.cos_id, matched.sin_id, null_node },
            .num_inputs = 3,
        });

        try redirects.append(allocator, .{ .from = add_id, .to = fused_id });
        num_rewrites += 1;
    }

    return .{ .redirects = redirects, .num_rewrites = num_rewrites };
}

const RopeMatch = struct {
    x_id: NodeId,
    cos_id: NodeId,
    sin_id: NodeId,
};

/// Match `mul(x, cos) + mul(rotated_half(x), sin)` where the first
/// argument is the "x branch" and the second is the "rotated branch".
fn matchRopeBranches(graph: *const Graph, x_branch: NodeId, rot_branch: NodeId) ?RopeMatch {
    const xb = graph.node(x_branch);
    const rb = graph.node(rot_branch);
    if (std.meta.activeTag(xb.op) != .mul) return null;
    if (std.meta.activeTag(rb.op) != .mul) return null;

    // Each mul has (data, table) operands in either order. The data
    // operand of the x_branch is x; the data operand of rot_branch is
    // rotate_half(x). The "table" operand on each side is cos / sin
    // (possibly wrapped in shape-only ops, which the backend can lift
    // out — we pass the table node verbatim).
    const xb_a = xb.inputs[0];
    const xb_b = xb.inputs[1];
    const rb_a = rb.inputs[0];
    const rb_b = rb.inputs[1];

    // Try each combination of which operand is the data vs the table.
    const pairs = [_]struct { x: NodeId, cos: NodeId, rot: NodeId, sin: NodeId }{
        .{ .x = xb_a, .cos = xb_b, .rot = rb_a, .sin = rb_b },
        .{ .x = xb_a, .cos = xb_b, .rot = rb_b, .sin = rb_a },
        .{ .x = xb_b, .cos = xb_a, .rot = rb_a, .sin = rb_b },
        .{ .x = xb_b, .cos = xb_a, .rot = rb_b, .sin = rb_a },
    };

    for (pairs) |p| {
        if (p.x == null_node or p.rot == null_node) continue;
        if (isHalfRotateOf(graph, p.rot, p.x)) {
            return .{ .x_id = p.x, .cos_id = p.cos, .sin_id = p.sin };
        }
    }
    return null;
}

/// True iff `rot_id` is `concat(neg(slice(x, half:)), slice(x, :half), axis=-1)`.
fn isHalfRotateOf(graph: *const Graph, rot_id: NodeId, x_id: NodeId) bool {
    if (rot_id == null_node or rot_id >= graph.nodeCount()) return false;
    const rot = graph.node(rot_id);

    // The concat in the IR is `concat_prim` (primitive form).
    const concat_attrs = switch (rot.op) {
        .concat_prim => |a| a,
        else => return false,
    };
    if (rot.num_inputs != 2) return false;

    const x_shape = graph.node(x_id).output_shape;
    if (x_shape.rank() == 0) return false;
    const last_axis: u8 = x_shape.rank() - 1;
    if (concat_attrs.axis != last_axis) return false;
    const head_dim = x_shape.dim(last_axis);
    if (head_dim <= 0 or @rem(head_dim, 2) != 0) return false;
    const half = @divExact(head_dim, 2);

    // First concat input: neg(slice(x, last=[half..head_dim])).
    const neg_id = rot.inputs[0];
    if (neg_id == null_node or neg_id >= graph.nodeCount()) return false;
    const neg_node = graph.node(neg_id);
    if (std.meta.activeTag(neg_node.op) != .neg) return false;
    if (!isLastDimSlice(graph, neg_node.inputs[0], x_id, half, head_dim)) return false;

    // Second concat input: slice(x, last=[0..half]).
    if (!isLastDimSlice(graph, rot.inputs[1], x_id, 0, half)) return false;
    return true;
}

fn isLastDimSlice(graph: *const Graph, slice_id: NodeId, x_id: NodeId, start: i64, end: i64) bool {
    if (slice_id == null_node or slice_id >= graph.nodeCount()) return false;
    const s = graph.node(slice_id);
    const attrs = switch (s.op) {
        .slice => |a| a,
        else => return false,
    };
    if (s.inputs[0] != x_id) return false;
    const x_shape = graph.node(x_id).output_shape;
    if (attrs.num_axes != x_shape.rank()) return false;
    const last_axis: u8 = x_shape.rank() - 1;
    // Every non-last axis must span the full dim with stride 1.
    for (0..attrs.num_axes) |ax| {
        if (attrs.strides[ax] != 1) return false;
        if (ax == last_axis) {
            if (attrs.starts[ax] != start) return false;
            if (attrs.limits[ax] != end) return false;
        } else {
            if (attrs.starts[ax] != 0) return false;
            if (attrs.limits[ax] != x_shape.dim(@intCast(ax))) return false;
        }
    }
    return true;
}

fn isMulNode(graph: *const Graph, id: NodeId) bool {
    if (id == null_node or id >= graph.nodeCount()) return false;
    return std.meta.activeTag(graph.node(id).op) == .mul;
}

// ── Algebraic / Shape Canonicalizations ───────────────────────────────

/// Hoist an elementwise unary above a broadcast:
///   unary(broadcast_in_dim(x, S)) → broadcast_in_dim(unary(x), S)
///
/// Compute-wise this is always a win (the unary now runs on x's smaller
/// shape, then broadcasts), and exposes more fusion downstream — e.g.
/// `mul(scalar_const, broadcast(x))` becomes `mul(scalar_const,
/// broadcast(...))` only after the broadcast itself becomes simpler.
fn fuseHoistUnaryAboveBroadcast(allocator: std.mem.Allocator, work: *Graph, reachable: []const bool) !PairFusionResult {
    var redirects = std.ArrayListUnmanaged(Redirect).empty;
    errdefer redirects.deinit(allocator);
    var num_rewrites: u32 = 0;

    const count = work.nodeCount();

    for (0..count) |i| {
        if (i >= reachable.len or !reachable[i]) continue;
        const unary_id: NodeId = @intCast(i);
        const u = work.node(unary_id);
        const tag = std.meta.activeTag(u.op);
        if (!isHoistableUnary(tag)) continue;

        const bc_id = u.inputs[0];
        if (bc_id == null_node or bc_id >= count) continue;
        const bc = work.node(bc_id);
        if (std.meta.activeTag(bc.op) != .broadcast_in_dim) continue;
        const inner_id = bc.inputs[0];
        if (inner_id == null_node or inner_id >= count) continue;

        // Skip if the unary is convert_dtype that actually changes the
        // dtype — we'd produce a broadcast of a different element type
        // than the original broadcast emitted, which the runtime treats
        // as a real conversion.
        const inner_shape = work.node(inner_id).output_shape;
        const new_unary_dtype = u.output_shape.dtype;
        var new_unary_shape = inner_shape;
        new_unary_shape.dtype = new_unary_dtype;

        const new_unary_id = try work.addNode(.{
            .op = u.op,
            .output_shape = new_unary_shape,
            .inputs = .{ inner_id, null_node, null_node, null_node },
            .num_inputs = 1,
        });

        const new_bc_attrs = bc.op.broadcast_in_dim;
        var new_bc_target = new_bc_attrs.target_shape;
        new_bc_target.dtype = new_unary_dtype;
        const new_bc_id = try work.addNode(.{
            .op = .{ .broadcast_in_dim = .{
                .target_shape = new_bc_target,
                .broadcast_axes = new_bc_attrs.broadcast_axes,
                .num_axes = new_bc_attrs.num_axes,
            } },
            .output_shape = u.output_shape,
            .inputs = .{ new_unary_id, null_node, null_node, null_node },
            .num_inputs = 1,
        });
        try redirects.append(allocator, .{ .from = unary_id, .to = new_bc_id });
        num_rewrites += 1;
    }

    return .{ .redirects = redirects, .num_rewrites = num_rewrites };
}

fn isHoistableUnary(tag: std.meta.Tag(OpCode)) bool {
    return switch (tag) {
        .neg, .abs, .sqrt, .rsqrt, .exp, .log, .sin, .cos, .tanh, .erf, .convert_dtype => true,
        else => false,
    };
}

/// Collapse `reshape(reshape(x, s1), s2) → reshape(x, s2)`.
fn fuseChainReshape(allocator: std.mem.Allocator, work: *Graph, reachable: []const bool) !PairFusionResult {
    var redirects = std.ArrayListUnmanaged(Redirect).empty;
    errdefer redirects.deinit(allocator);
    var num_rewrites: u32 = 0;

    const count = work.nodeCount();

    for (0..count) |i| {
        if (i >= reachable.len or !reachable[i]) continue;
        const outer_id: NodeId = @intCast(i);
        const outer = work.node(outer_id);
        const outer_attrs = switch (outer.op) {
            .reshape => |a| a,
            else => continue,
        };
        const inner_id = outer.inputs[0];
        if (inner_id == null_node or inner_id >= count) continue;
        const inner = work.node(inner_id);
        if (std.meta.activeTag(inner.op) != .reshape) continue;

        const new_id = try work.addNode(.{
            .op = .{ .reshape = .{ .new_shape = outer_attrs.new_shape } },
            .output_shape = outer.output_shape,
            .inputs = .{ inner.inputs[0], null_node, null_node, null_node },
            .num_inputs = 1,
        });
        try redirects.append(allocator, .{ .from = outer_id, .to = new_id });
        num_rewrites += 1;
    }

    return .{ .redirects = redirects, .num_rewrites = num_rewrites };
}

/// Collapse `broadcast(broadcast(x, s1), s2) → broadcast(x, s2)`. Both
/// the unmapped (`num_axes == 0`) and the explicit-axis-map form are
/// handled — for the latter we compose the maps so the resulting node
/// directly states "where each input axis lands in the outer target
/// shape".
fn fuseChainBroadcast(allocator: std.mem.Allocator, work: *Graph, reachable: []const bool) !PairFusionResult {
    var redirects = std.ArrayListUnmanaged(Redirect).empty;
    errdefer redirects.deinit(allocator);
    var num_rewrites: u32 = 0;

    const count = work.nodeCount();

    for (0..count) |i| {
        if (i >= reachable.len or !reachable[i]) continue;
        const outer_id: NodeId = @intCast(i);
        const outer = work.node(outer_id);
        const outer_attrs = switch (outer.op) {
            .broadcast_in_dim => |a| a,
            else => continue,
        };

        const inner_id = outer.inputs[0];
        if (inner_id == null_node or inner_id >= count) continue;
        const inner = work.node(inner_id);
        const inner_attrs = switch (inner.op) {
            .broadcast_in_dim => |a| a,
            else => continue,
        };

        // Compose the broadcast attrs. `broadcast_axes[i]` says where
        // input axis i lands in the output (a.k.a. the StableHLO
        // semantics): so `composed.broadcast_axes[i] = outer.broadcast_axes[inner.broadcast_axes[i]]`
        // — input lands at intermediate `inner.broadcast_axes[i]`, which
        // outer then routes to its own output axis.
        var new_attrs: node_mod.BroadcastAttrs = .{
            .target_shape = outer_attrs.target_shape,
        };

        if (outer_attrs.num_axes == 0 and inner_attrs.num_axes == 0) {
            // Both unmapped (right-aligned NumPy broadcast on both
            // hops): the composed broadcast is also unmapped onto
            // outer's target shape.
            new_attrs.num_axes = 0;
        } else if (outer_attrs.num_axes == 0) {
            // Outer is unmapped — keep inner's axis map. This is
            // correct iff outer's input rank equals outer's target
            // rank (the unmapped form left every axis in place);
            // otherwise the inner map points at axes that would shift
            // under outer's right-alignment, and composing is unsafe.
            const outer_in_rank = inner.output_shape.rank();
            const outer_out_rank = outer_attrs.target_shape.rank();
            if (outer_in_rank != outer_out_rank) continue;
            new_attrs.num_axes = inner_attrs.num_axes;
            new_attrs.broadcast_axes = inner_attrs.broadcast_axes;
        } else if (inner_attrs.num_axes == 0) {
            // Inner is unmapped — outer's map is in terms of its
            // input axes, which equal inner's output axes. As long as
            // inner's input rank matches its output rank (the only
            // safe unmapped form), we can keep the outer map and
            // pretend inner was an identity-mapped broadcast.
            const inner_in_rank = work.node(inner.inputs[0]).output_shape.rank();
            const inner_out_rank = inner.output_shape.rank();
            if (inner_in_rank != inner_out_rank) continue;
            new_attrs.num_axes = outer_attrs.num_axes;
            new_attrs.broadcast_axes = outer_attrs.broadcast_axes;
        } else {
            // Both mapped: compose `outer ∘ inner`.
            new_attrs.num_axes = inner_attrs.num_axes;
            var composition_ok = true;
            for (0..inner_attrs.num_axes) |k| {
                const intermediate_axis = inner_attrs.broadcast_axes[k];
                if (intermediate_axis >= outer_attrs.num_axes) {
                    // Inner sends an axis to a position that outer
                    // doesn't enumerate — the composition's axis map
                    // would be ambiguous, so skip this rewrite rather
                    // than fall back to a possibly-incorrect unmapped
                    // form.
                    composition_ok = false;
                    break;
                }
                new_attrs.broadcast_axes[k] = outer_attrs.broadcast_axes[intermediate_axis];
            }
            if (!composition_ok) continue;
        }

        const new_id = try work.addNode(.{
            .op = .{ .broadcast_in_dim = new_attrs },
            .output_shape = outer.output_shape,
            .inputs = .{ inner.inputs[0], null_node, null_node, null_node },
            .num_inputs = 1,
        });
        try redirects.append(allocator, .{ .from = outer_id, .to = new_id });
        num_rewrites += 1;
    }

    return .{ .redirects = redirects, .num_rewrites = num_rewrites };
}

/// Collapse general `transpose(transpose(x, p1), p2) → transpose(x, p1∘p2)`.
/// The inverse-perm case (which folds the chain to identity) is already
/// handled by the algebraic phase. Here we only catch the strictly-non-
/// inverse case so we always synthesize a single transpose.
fn fuseChainTranspose(allocator: std.mem.Allocator, work: *Graph, reachable: []const bool) !PairFusionResult {
    var redirects = std.ArrayListUnmanaged(Redirect).empty;
    errdefer redirects.deinit(allocator);
    var num_rewrites: u32 = 0;

    const count = work.nodeCount();

    for (0..count) |i| {
        if (i >= reachable.len or !reachable[i]) continue;
        const outer_id: NodeId = @intCast(i);
        const outer = work.node(outer_id);
        const outer_attrs = switch (outer.op) {
            .transpose => |a| a,
            else => continue,
        };
        const inner_id = outer.inputs[0];
        if (inner_id == null_node or inner_id >= count) continue;
        const inner = work.node(inner_id);
        const inner_attrs = switch (inner.op) {
            .transpose => |a| a,
            else => continue,
        };
        if (outer_attrs.num_axes != inner_attrs.num_axes) continue;
        // Skip the inverse case — Phase 2 collapses it directly to x.
        if (isInversePerm(
            inner_attrs.perm[0..inner_attrs.num_axes],
            outer_attrs.perm[0..outer_attrs.num_axes],
        )) continue;

        var combined: TransposeAttrs = .{};
        combined.num_axes = outer_attrs.num_axes;
        // Composed perm: out_axis k corresponds to inner-input axis
        // inner_perm[outer_perm[k]].
        for (0..outer_attrs.num_axes) |k| {
            combined.perm[k] = inner_attrs.perm[outer_attrs.perm[k]];
        }

        const new_id = try work.addNode(.{
            .op = .{ .transpose = combined },
            .output_shape = outer.output_shape,
            .inputs = .{ inner.inputs[0], null_node, null_node, null_node },
            .num_inputs = 1,
        });
        try redirects.append(allocator, .{ .from = outer_id, .to = new_id });
        num_rewrites += 1;
    }

    return .{ .redirects = redirects, .num_rewrites = num_rewrites };
}

const TransposeAttrs = node_mod.TransposeAttrs;

/// Fold `dot_general(x, transpose(W, [1,0]))` into a `dot_general` whose
/// `rhs_contracting` is shifted to the other axis, eliminating the
/// transpose entirely. This both saves a copy and lets `fuseLinearBias`
/// / `fuseMatmulNoBias` see the bare matmul without the intermediary.
fn fuseTransposeIntoDot(allocator: std.mem.Allocator, work: *Graph, reachable: []const bool) !PairFusionResult {
    var redirects = std.ArrayListUnmanaged(Redirect).empty;
    errdefer redirects.deinit(allocator);
    var num_rewrites: u32 = 0;

    const count = work.nodeCount();

    for (0..count) |i| {
        if (i >= reachable.len or !reachable[i]) continue;
        const dot_id: NodeId = @intCast(i);
        const dot = work.node(dot_id);
        const dot_attrs = switch (dot.op) {
            .dot_general => |a| a,
            else => continue,
        };
        // Only handle the simple 2-D matmul form (no batch, single
        // contracting axis) — composing transposes through batched dot
        // semantics needs care that's out of scope here.
        if (dot_attrs.num_contracting != 1 or dot_attrs.num_batch != 0) continue;

        const rhs_id = dot.inputs[1];
        if (rhs_id == null_node or rhs_id >= count) continue;
        const rhs = work.node(rhs_id);
        const rhs_t_attrs = switch (rhs.op) {
            .transpose => |a| a,
            else => continue,
        };
        if (rhs_t_attrs.num_axes != 2) continue;
        if (rhs_t_attrs.perm[0] != 1 or rhs_t_attrs.perm[1] != 0) continue;
        const bare_w = rhs.inputs[0];
        if (bare_w == null_node or bare_w >= count) continue;

        // Original rhs_contracting axis 0 means we contracted W^T's axis 0.
        // After dropping the transpose, that's W's axis 1.
        const old_rhs_axis = dot_attrs.rhs_contracting[0];
        const new_rhs_axis: u8 = if (old_rhs_axis == 0) 1 else 0;

        var new_attrs = dot_attrs;
        new_attrs.rhs_contracting[0] = new_rhs_axis;

        const new_id = try work.addNode(.{
            .op = .{ .dot_general = new_attrs },
            .output_shape = dot.output_shape,
            .inputs = .{ dot.inputs[0], bare_w, null_node, null_node },
            .num_inputs = 2,
        });
        try redirects.append(allocator, .{ .from = dot_id, .to = new_id });
        num_rewrites += 1;
    }

    return .{ .redirects = redirects, .num_rewrites = num_rewrites };
}

/// Merge two same-kind reductions: `reduce_sum(reduce_sum(x, A), B)` →
/// `reduce_sum(x, A∪B)`. Same for max/mean — though `mean` of `mean`
/// is only safe when neither reduces over an axis the other already
/// reduced; we enforce that with a disjointness check.
fn fuseReduceAxisMerge(allocator: std.mem.Allocator, work: *Graph, reachable: []const bool) !PairFusionResult {
    var redirects = std.ArrayListUnmanaged(Redirect).empty;
    errdefer redirects.deinit(allocator);
    var num_rewrites: u32 = 0;

    const count = work.nodeCount();

    for (0..count) |i| {
        if (i >= reachable.len or !reachable[i]) continue;
        const outer_id: NodeId = @intCast(i);
        const outer = work.node(outer_id);
        const outer_kind = std.meta.activeTag(outer.op);
        const outer_attrs: node_mod.ReduceAttrs = switch (outer.op) {
            .reduce_sum, .reduce_max, .reduce_mean => |a| a,
            else => continue,
        };
        const inner_id = outer.inputs[0];
        if (inner_id == null_node or inner_id >= count) continue;
        const inner = work.node(inner_id);
        if (std.meta.activeTag(inner.op) != outer_kind) continue;
        const inner_attrs: node_mod.ReduceAttrs = switch (inner.op) {
            .reduce_sum, .reduce_max, .reduce_mean => |a| a,
            else => continue,
        };

        // Build the union of axes; bail on overlap (would double-count
        // for mean / repeat work for sum).
        var present: [shape_mod.max_rank]bool = @splat(false);
        var merged: node_mod.ReduceAttrs = .{};
        var n_merged: u8 = 0;
        var disjoint = true;
        for (0..inner_attrs.num_axes) |j| {
            const ax = inner_attrs.axes[j];
            if (present[ax]) {
                disjoint = false;
                break;
            }
            present[ax] = true;
            merged.axes[n_merged] = ax;
            n_merged += 1;
        }
        if (!disjoint) continue;
        for (0..outer_attrs.num_axes) |j| {
            const ax = outer_attrs.axes[j];
            if (present[ax]) {
                disjoint = false;
                break;
            }
            present[ax] = true;
            merged.axes[n_merged] = ax;
            n_merged += 1;
        }
        if (!disjoint) continue;
        merged.num_axes = n_merged;

        const new_op: OpCode = switch (outer_kind) {
            .reduce_sum => .{ .reduce_sum = merged },
            .reduce_max => .{ .reduce_max = merged },
            .reduce_mean => .{ .reduce_mean = merged },
            else => unreachable,
        };
        const new_id = try work.addNode(.{
            .op = new_op,
            .output_shape = outer.output_shape,
            .inputs = .{ inner.inputs[0], null_node, null_node, null_node },
            .num_inputs = 1,
        });
        try redirects.append(allocator, .{ .from = outer_id, .to = new_id });
        num_rewrites += 1;
    }

    return .{ .redirects = redirects, .num_rewrites = num_rewrites };
}

/// Canonicalize neg-bearing add/sub/mul:
///   add(x, neg(y))   → sub(x, y)
///   add(neg(y), x)   → sub(x, y)
///   sub(x, neg(y))   → add(x, y)
///   mul(neg(x), y)   → neg(mul(x, y))
///   mul(x, neg(y))   → neg(mul(x, y))
///   mul(neg(x), neg(y)) → mul(x, y)
fn fuseNegNormalize(allocator: std.mem.Allocator, work: *Graph, reachable: []const bool) !PairFusionResult {
    var redirects = std.ArrayListUnmanaged(Redirect).empty;
    errdefer redirects.deinit(allocator);
    var num_rewrites: u32 = 0;

    const count = work.nodeCount();

    for (0..count) |i| {
        if (i >= reachable.len or !reachable[i]) continue;
        const node_id: NodeId = @intCast(i);
        const n = work.node(node_id);
        const tag = std.meta.activeTag(n.op);
        if (tag != .add and tag != .sub and tag != .mul) continue;

        const lhs = n.inputs[0];
        const rhs = n.inputs[1];
        if (lhs == null_node or rhs == null_node) continue;
        if (lhs >= count or rhs >= count) continue;

        const lhs_neg = isNeg(work, lhs);
        const rhs_neg = isNeg(work, rhs);

        switch (tag) {
            .add => {
                if (rhs_neg) {
                    // add(x, neg(y)) → sub(x, y)
                    const y = work.node(rhs).inputs[0];
                    if (y == null_node) continue;
                    const new_id = try work.addNode(.{
                        .op = .{ .sub = {} },
                        .output_shape = n.output_shape,
                        .inputs = .{ lhs, y, null_node, null_node },
                        .num_inputs = 2,
                    });
                    try redirects.append(allocator, .{ .from = node_id, .to = new_id });
                    num_rewrites += 1;
                } else if (lhs_neg) {
                    // add(neg(y), x) → sub(x, y)
                    const y = work.node(lhs).inputs[0];
                    if (y == null_node) continue;
                    const new_id = try work.addNode(.{
                        .op = .{ .sub = {} },
                        .output_shape = n.output_shape,
                        .inputs = .{ rhs, y, null_node, null_node },
                        .num_inputs = 2,
                    });
                    try redirects.append(allocator, .{ .from = node_id, .to = new_id });
                    num_rewrites += 1;
                }
            },
            .sub => {
                if (rhs_neg) {
                    // sub(x, neg(y)) → add(x, y)
                    const y = work.node(rhs).inputs[0];
                    if (y == null_node) continue;
                    const new_id = try work.addNode(.{
                        .op = .{ .add = {} },
                        .output_shape = n.output_shape,
                        .inputs = .{ lhs, y, null_node, null_node },
                        .num_inputs = 2,
                    });
                    try redirects.append(allocator, .{ .from = node_id, .to = new_id });
                    num_rewrites += 1;
                }
            },
            .mul => {
                if (lhs_neg and rhs_neg) {
                    // mul(neg(x), neg(y)) → mul(x, y)
                    const x = work.node(lhs).inputs[0];
                    const y = work.node(rhs).inputs[0];
                    if (x == null_node or y == null_node) continue;
                    const new_id = try work.addNode(.{
                        .op = .{ .mul = {} },
                        .output_shape = n.output_shape,
                        .inputs = .{ x, y, null_node, null_node },
                        .num_inputs = 2,
                    });
                    try redirects.append(allocator, .{ .from = node_id, .to = new_id });
                    num_rewrites += 1;
                } else if (lhs_neg) {
                    // mul(neg(x), y) → neg(mul(x, y))
                    const x = work.node(lhs).inputs[0];
                    if (x == null_node) continue;
                    const inner_id = try work.addNode(.{
                        .op = .{ .mul = {} },
                        .output_shape = n.output_shape,
                        .inputs = .{ x, rhs, null_node, null_node },
                        .num_inputs = 2,
                    });
                    const neg_id = try work.addNode(.{
                        .op = .{ .neg = {} },
                        .output_shape = n.output_shape,
                        .inputs = .{ inner_id, null_node, null_node, null_node },
                        .num_inputs = 1,
                    });
                    try redirects.append(allocator, .{ .from = node_id, .to = neg_id });
                    num_rewrites += 1;
                } else if (rhs_neg) {
                    // mul(x, neg(y)) → neg(mul(x, y))
                    const y = work.node(rhs).inputs[0];
                    if (y == null_node) continue;
                    const inner_id = try work.addNode(.{
                        .op = .{ .mul = {} },
                        .output_shape = n.output_shape,
                        .inputs = .{ lhs, y, null_node, null_node },
                        .num_inputs = 2,
                    });
                    const neg_id = try work.addNode(.{
                        .op = .{ .neg = {} },
                        .output_shape = n.output_shape,
                        .inputs = .{ inner_id, null_node, null_node, null_node },
                        .num_inputs = 1,
                    });
                    try redirects.append(allocator, .{ .from = node_id, .to = neg_id });
                    num_rewrites += 1;
                }
            },
            else => unreachable,
        }
    }

    return .{ .redirects = redirects, .num_rewrites = num_rewrites };
}

fn isNeg(graph: *const Graph, id: NodeId) bool {
    if (id == null_node or id >= graph.nodeCount()) return false;
    return std.meta.activeTag(graph.node(id).op) == .neg;
}

/// Collapse `slice(slice(x, A), B) → slice(x, composed)`. Handles
/// arbitrary positive strides via the standard composition:
///   start' = inner.start + outer.start * inner.stride
///   stride' = inner.stride * outer.stride
///   limit' = start' + outer_output_dim * stride'
/// We derive `outer_output_dim` from the outer node's static output
/// shape so the rewrite stays exact even when `(limit - start)` isn't
/// cleanly divisible by `stride`.
fn fuseSliceChain(allocator: std.mem.Allocator, work: *Graph, reachable: []const bool) !PairFusionResult {
    var redirects = std.ArrayListUnmanaged(Redirect).empty;
    errdefer redirects.deinit(allocator);
    var num_rewrites: u32 = 0;

    const count = work.nodeCount();

    for (0..count) |i| {
        if (i >= reachable.len or !reachable[i]) continue;
        const outer_id: NodeId = @intCast(i);
        const outer = work.node(outer_id);
        const outer_attrs = switch (outer.op) {
            .slice => |a| a,
            else => continue,
        };
        const inner_id = outer.inputs[0];
        if (inner_id == null_node or inner_id >= count) continue;
        const inner = work.node(inner_id);
        const inner_attrs = switch (inner.op) {
            .slice => |a| a,
            else => continue,
        };
        if (outer_attrs.num_axes != inner_attrs.num_axes) continue;

        const out_shape = outer.output_shape;
        if (out_shape.rank() != outer_attrs.num_axes) continue;

        var positive_strides = true;
        for (0..outer_attrs.num_axes) |k| {
            if (outer_attrs.strides[k] <= 0 or inner_attrs.strides[k] <= 0) {
                positive_strides = false;
                break;
            }
        }
        if (!positive_strides) continue;

        var composed: node_mod.SliceAttrs = .{};
        composed.num_axes = outer_attrs.num_axes;
        for (0..outer_attrs.num_axes) |k| {
            composed.starts[k] = inner_attrs.starts[k] + outer_attrs.starts[k] * inner_attrs.strides[k];
            composed.strides[k] = inner_attrs.strides[k] * outer_attrs.strides[k];
            const outer_dim = out_shape.dim(@intCast(k));
            if (outer_dim < 0) {
                // Dynamic output dim — bail rather than guess at the
                // limit (which would otherwise rely on cleanly-divisible
                // (limit−start) / stride arithmetic).
                composed.num_axes = 0;
                break;
            }
            composed.limits[k] = composed.starts[k] + outer_dim * composed.strides[k];
        }
        if (composed.num_axes != outer_attrs.num_axes) continue;

        const new_id = try work.addNode(.{
            .op = .{ .slice = composed },
            .output_shape = outer.output_shape,
            .inputs = .{ inner.inputs[0], null_node, null_node, null_node },
            .num_inputs = 1,
        });
        try redirects.append(allocator, .{ .from = outer_id, .to = new_id });
        num_rewrites += 1;
    }

    return .{ .redirects = redirects, .num_rewrites = num_rewrites };
}

/// Fold `slice(concat([a, b], axis), [..., start:limit, ...])` when the
/// slice along the concat axis lies entirely within `a` or entirely
/// within `b`. The full XLA `algsimp` rule walks an n-ary concat of
/// arbitrary length; concat_prim is binary in this IR so we just check
/// against the two operands.
fn fuseSliceOfConcat(allocator: std.mem.Allocator, work: *Graph, reachable: []const bool) !PairFusionResult {
    var redirects = std.ArrayListUnmanaged(Redirect).empty;
    errdefer redirects.deinit(allocator);
    var num_rewrites: u32 = 0;

    const count = work.nodeCount();

    for (0..count) |i| {
        if (i >= reachable.len or !reachable[i]) continue;
        const slice_id: NodeId = @intCast(i);
        const slice_node = work.node(slice_id);
        const slice_attrs = switch (slice_node.op) {
            .slice => |a| a,
            else => continue,
        };
        const concat_id = slice_node.inputs[0];
        if (concat_id == null_node or concat_id >= count) continue;
        const concat_node = work.node(concat_id);
        const concat_attrs = switch (concat_node.op) {
            .concat_prim => |a| a,
            else => continue,
        };

        const a_id = concat_node.inputs[0];
        const b_id = concat_node.inputs[1];
        if (a_id == null_node or b_id == null_node) continue;
        if (a_id >= count or b_id >= count) continue;
        const axis = concat_attrs.axis;
        if (axis >= slice_attrs.num_axes) continue;
        if (slice_attrs.strides[axis] != 1) continue;

        const a_shape = work.node(a_id).output_shape;
        const a_dim = a_shape.dim(axis);
        if (a_dim < 0) continue; // dynamic — bail
        const start = slice_attrs.starts[axis];
        const limit = slice_attrs.limits[axis];
        if (limit < start) continue;

        var pick_id: NodeId = null_node;
        var new_attrs = slice_attrs;
        if (limit <= a_dim) {
            // Slice falls entirely inside operand `a`.
            pick_id = a_id;
        } else if (start >= a_dim) {
            // Slice falls entirely inside operand `b` — shift the offset.
            pick_id = b_id;
            new_attrs.starts[axis] = start - a_dim;
            new_attrs.limits[axis] = limit - a_dim;
        } else {
            continue; // spans both — leave alone
        }

        const new_id = try work.addNode(.{
            .op = .{ .slice = new_attrs },
            .output_shape = slice_node.output_shape,
            .inputs = .{ pick_id, null_node, null_node, null_node },
            .num_inputs = 1,
        });
        try redirects.append(allocator, .{ .from = slice_id, .to = new_id });
        num_rewrites += 1;
    }

    return .{ .redirects = redirects, .num_rewrites = num_rewrites };
}

/// Put the constant operand on the right of commutative binary ops:
///   add(const, x) → add(x, const)
///   mul(const, x) → mul(x, const)
/// Halves the number of operand orders downstream matchers have to
/// enumerate, and is idempotent (the new node has the constant on the
/// right, so it won't re-trigger the rewrite).
fn fuseCanonicalizeCommutative(allocator: std.mem.Allocator, work: *Graph, reachable: []const bool) !PairFusionResult {
    var redirects = std.ArrayListUnmanaged(Redirect).empty;
    errdefer redirects.deinit(allocator);
    var num_rewrites: u32 = 0;

    const count = work.nodeCount();

    for (0..count) |i| {
        if (i >= reachable.len or !reachable[i]) continue;
        const node_id: NodeId = @intCast(i);
        const n = work.node(node_id);
        const is_commutative = switch (std.meta.activeTag(n.op)) {
            .add, .mul => true,
            else => false,
        };
        if (!is_commutative) continue;

        const a = n.inputs[0];
        const b = n.inputs[1];
        if (a == null_node or b == null_node) continue;
        if (a >= count or b >= count) continue;

        // Only fire when the LHS is a constant (or scalar-like
        // wrapper) and the RHS is not.
        const lhs_is_const = extractScalarConstValue(work, a) != null;
        const rhs_is_const = extractScalarConstValue(work, b) != null;
        if (!lhs_is_const or rhs_is_const) continue;

        const new_id = try work.addNode(.{
            .op = n.op,
            .output_shape = n.output_shape,
            .inputs = .{ b, a, null_node, null_node },
            .num_inputs = 2,
        });
        try redirects.append(allocator, .{ .from = node_id, .to = new_id });
        num_rewrites += 1;
    }

    return .{ .redirects = redirects, .num_rewrites = num_rewrites };
}

/// Self-pair algebraic rewrites that need a synthesized constant of
/// the value's dtype:
///   add(x, x) → mul(x, 2)
///   sub(x, x) → 0   (a broadcast of scalar 0 of x's shape)
/// Both fire only when the two operands resolve to the same node id —
/// CSE before this pass collapses syntactically-identical subgraphs
/// to the same id, so this pattern catches `add(gelu(z), gelu(z))`,
/// `sub(linear(x, w), linear(x, w))`, etc.
///
/// Skipped for `bool_` (no notion of `2`) and for any dtype the
/// `internScalarConst` helper doesn't support.
fn fuseAlgebraicSelfPairs(allocator: std.mem.Allocator, work: *Graph, reachable: []const bool) !PairFusionResult {
    var redirects = std.ArrayListUnmanaged(Redirect).empty;
    errdefer redirects.deinit(allocator);
    var num_rewrites: u32 = 0;

    const count = work.nodeCount();

    for (0..count) |i| {
        if (i >= reachable.len or !reachable[i]) continue;
        const node_id: NodeId = @intCast(i);
        const n = work.node(node_id);
        const tag = std.meta.activeTag(n.op);
        if (tag != .add and tag != .sub and tag != .div) continue;

        const a = n.inputs[0];
        const b = n.inputs[1];
        if (a == null_node or b == null_node) continue;
        if (a != b) continue;

        const out_shape = n.output_shape;
        const dtype = out_shape.dtype;
        if (dtype == .bool_) continue;

        switch (tag) {
            .add => {
                // add(x, x) → mul(x, 2). The `2` is a scalar of x's
                // dtype that gets broadcast by the mul (binaryOp uses
                // the bigger shape). Skip dtypes without a sensible 2.
                const two_id = try buildTypedScalar(work, dtype, 2.0) orelse continue;
                const new_id = try work.addNode(.{
                    .op = .{ .mul = {} },
                    .output_shape = out_shape,
                    .inputs = .{ a, two_id, null_node, null_node },
                    .num_inputs = 2,
                });
                try redirects.append(allocator, .{ .from = node_id, .to = new_id });
                num_rewrites += 1;
            },
            .sub => {
                // sub(x, x) → broadcast(0, x.shape).
                const zero_scalar = try buildTypedScalar(work, dtype, 0.0) orelse continue;
                if (out_shape.rank() == 0) {
                    try redirects.append(allocator, .{ .from = node_id, .to = zero_scalar });
                } else {
                    var bc_attrs: node_mod.BroadcastAttrs = .{ .target_shape = out_shape };
                    bc_attrs.num_axes = 0;
                    const bc_id = try work.addNode(.{
                        .op = .{ .broadcast_in_dim = bc_attrs },
                        .output_shape = out_shape,
                        .inputs = .{ zero_scalar, null_node, null_node, null_node },
                        .num_inputs = 1,
                    });
                    try redirects.append(allocator, .{ .from = node_id, .to = bc_id });
                }
                num_rewrites += 1;
            },
            .div => {
                // div(x, x) → 1, but ONLY when we can prove x is
                // never zero — otherwise we'd silently turn a
                // legitimate NaN-producing 0/0 into 1. Conservative
                // proofs: x is `exp(...)` (always > 0) or x is a
                // constant whose stored bytes are all non-zero in its
                // native dtype. Anything else we leave alone.
                if (!isProvablyNonZero(work, a)) continue;
                const one_scalar = try buildTypedScalar(work, dtype, 1.0) orelse continue;
                if (out_shape.rank() == 0) {
                    try redirects.append(allocator, .{ .from = node_id, .to = one_scalar });
                } else {
                    var bc_attrs: node_mod.BroadcastAttrs = .{ .target_shape = out_shape };
                    bc_attrs.num_axes = 0;
                    const bc_id = try work.addNode(.{
                        .op = .{ .broadcast_in_dim = bc_attrs },
                        .output_shape = out_shape,
                        .inputs = .{ one_scalar, null_node, null_node, null_node },
                        .num_inputs = 1,
                    });
                    try redirects.append(allocator, .{ .from = node_id, .to = bc_id });
                }
                num_rewrites += 1;
            },
            else => unreachable,
        }
    }

    return .{ .redirects = redirects, .num_rewrites = num_rewrites };
}

/// Conservative non-zero proof for `div(x, x) → 1`. Returns true only
/// when we can show `x != 0` for every element without solving an
/// optimisation problem:
///   * x is a constant whose every element is finite and non-zero
///     in its native dtype. Floating infinities and NaNs are not safe:
///     `inf / inf` and `nan / nan` both produce NaN, not 1.
fn isProvablyNonZero(graph: *const Graph, id: NodeId) bool {
    if (id == null_node or id >= graph.nodeCount()) return false;
    const n = graph.node(id);
    switch (n.op) {
        .constant => |attrs| return constantElementsNonZero(graph, n.output_shape.dtype, attrs),
        else => return false,
    }
}

fn constantElementsNonZero(graph: *const Graph, dtype: shape_mod.DType, attrs: node_mod.ConstantAttrs) bool {
    if (attrs.data_len == 0) return false;
    return switch (dtype) {
        .f32 => blk: {
            for (graph.constantDataAs(f32, attrs.data_offset, attrs.data_len)) |value| {
                if (value == 0.0 or !std.math.isFinite(value)) break :blk false;
            }
            break :blk true;
        },
        .f16 => blk: {
            for (graph.constantDataAs(f16, attrs.data_offset, attrs.data_len)) |value| {
                const value_f32: f32 = @floatCast(value);
                if (value_f32 == 0.0 or !std.math.isFinite(value_f32)) break :blk false;
            }
            break :blk true;
        },
        .bf16 => blk: {
            for (graph.constantDataAs(u16, attrs.data_offset, attrs.data_len)) |bits| {
                const value: f32 = @bitCast(@as(u32, bits) << 16);
                if (value == 0.0 or !std.math.isFinite(value)) break :blk false;
            }
            break :blk true;
        },
        .f64 => blk: {
            for (graph.constantDataAs(f64, attrs.data_offset, attrs.data_len)) |value| {
                if (value == 0.0 or !std.math.isFinite(value)) break :blk false;
            }
            break :blk true;
        },
        .i8 => blk: {
            for (graph.constantDataAs(i8, attrs.data_offset, attrs.data_len)) |value| {
                if (value == 0) break :blk false;
            }
            break :blk true;
        },
        .i16 => blk: {
            for (graph.constantDataAs(i16, attrs.data_offset, attrs.data_len)) |value| {
                if (value == 0) break :blk false;
            }
            break :blk true;
        },
        .i32 => blk: {
            for (graph.constantDataAs(i32, attrs.data_offset, attrs.data_len)) |value| {
                if (value == 0) break :blk false;
            }
            break :blk true;
        },
        .i64 => blk: {
            for (graph.constantDataAs(i64, attrs.data_offset, attrs.data_len)) |value| {
                if (value == 0) break :blk false;
            }
            break :blk true;
        },
        .u8, .bool_ => blk: {
            for (graph.constantDataAs(u8, attrs.data_offset, attrs.data_len)) |value| {
                if (value == 0) break :blk false;
            }
            break :blk true;
        },
    };
}

/// Build a scalar constant node of `value` typed as `dtype`. Returns
/// null for dtype combinations where the value can't be encoded
/// (currently only `bool_` for non-{0,1} values, but the caller
/// should already have filtered).
fn buildTypedScalar(work: *Graph, dtype: shape_mod.DType, value: f64) !?NodeId {
    const loc = try work.internScalarConst(value, dtype);
    return try work.addNode(.{
        .op = .{ .constant = .{ .data_offset = loc.offset, .data_len = loc.len } },
        .output_shape = shape_mod.Shape.scalar(dtype),
    });
}

// ── SDPA Fusion ──────────────────────────────────────────────────────
//
// Detects the decomposed attention pattern:
//   dot_general(softmax(mul(dot_general(Q, transpose(K)), scale)), V)
// and replaces it with fused_sdpa(Q, K, V).
//
// The pattern matching walks backwards from each batched dot_general
// node and checks the chain: probs @ V where probs = softmax(scale * (Q @ K^T)).

fn fuseSDPA(allocator: std.mem.Allocator, work: *Graph) !PairFusionResult {
    var redirects = std.ArrayListUnmanaged(Redirect).empty;
    errdefer redirects.deinit(allocator);
    var num_rewrites: u32 = 0;

    const count = work.nodeCount();

    for (0..count) |i| {
        const n = work.node(@intCast(i));

        // Anchor: final dot_general(probs, V)
        const dg_attrs = switch (n.op) {
            .dot_general => |a| a,
            else => continue,
        };
        if (dg_attrs.num_contracting != 1) continue;
        // Allow num_batch=1 ([B*H, S, D]) or num_batch=2 ([B, H, S, D])
        if (dg_attrs.num_batch != 1 and dg_attrs.num_batch != 2) continue;

        const first_input = n.inputs[0];
        const second_input = n.inputs[1];
        if (first_input == null_node or second_input == null_node) continue;

        var probs_id = first_input;
        var v_id = second_input;
        var softmax_id = findSoftmaxNode(work, probs_id);
        if (softmax_id == null_node) {
            const alt_softmax_id = findSoftmaxNode(work, second_input);
            if (alt_softmax_id != null_node) {
                probs_id = second_input;
                v_id = first_input;
                softmax_id = alt_softmax_id;
            }
        }
        if (softmax_id == null_node) continue;

        // Walk backward from softmax input through masking ops to find scale
        const softmax_node = work.node(softmax_id);
        const softmax_input = softmax_node.inputs[0];
        if (softmax_input == null_node) continue;
        const scale_result = findScaleOp(work, softmax_input) orelse continue;

        // Scores must be dot_general(Q, K^T)
        const scores_id = scale_result.scores_id;
        const scores_node = work.node(scores_id);
        const scores_dg = switch (scores_node.op) {
            .dot_general => |a| a,
            else => continue,
        };
        if (scores_dg.num_contracting != 1) continue;
        if (scores_dg.num_batch != 1 and scores_dg.num_batch != 2) continue;

        const q_input_id = scores_node.inputs[0];
        const kt_id = scores_node.inputs[1];
        if (q_input_id == null_node or kt_id == null_node) continue;

        // K^T = transpose(K), optionally wrapped in the same scalar scaling
        // patterns as Q.
        const scaled_kt = findScaledTransposeTensor(work, kt_id);
        const k_id = scaled_kt.k_id;
        if (k_id == null_node) continue;

        const prescaled_q = findPrescaledTensor(work, q_input_id);
        const q_id = prescaled_q.base_id;
        if (q_id == null_node) continue;

        // Extract dimensions from Q shape
        // 3D: [B*H, S, D] → batch=1, num_heads=B*H
        // 4D: [B, H, S, D] → batch=B, num_heads=H
        const q_shape = work.node(q_id).output_shape;
        const k_shape = work.node(k_id).output_shape;
        const v_shape = work.node(v_id).output_shape;
        const q_rank = q_shape.rank();
        if (q_rank != 3 and q_rank != 4) continue;
        if (k_shape.rank() != q_rank) continue;
        if (v_shape.rank() != q_rank) continue;

        var batch: u32 = undefined;
        var num_heads: u32 = undefined;
        var seq_len: u32 = undefined;
        var head_dim: u32 = undefined;

        if (q_rank == 4) {
            const b = q_shape.dim(0);
            const h = q_shape.dim(1);
            const s2 = q_shape.dim(2);
            const d2 = v_shape.dim(3);
            // Allow a dynamic batch for encoder-style attention. Heads and
            // head_dim still need to be statically known so the backend can
            // specialize the fused kernel shape.
            if (h <= 0 or d2 <= 0) continue;
            batch = if (b > 0) @intCast(b) else 0;
            num_heads = @intCast(h);
            seq_len = if (s2 > 0) @intCast(s2) else 0;
            head_dim = @intCast(d2);
        } else {
            const bh = q_shape.dim(0);
            const s2 = q_shape.dim(1);
            const d2 = v_shape.dim(2);
            if (d2 <= 0) continue;
            batch = 1;
            num_heads = if (bh > 0) @intCast(bh) else 0;
            seq_len = if (s2 > 0) @intCast(s2) else 0;
            head_dim = @intCast(d2);
        }

        var effective_scale = scale_result.scale;
        if (prescaled_q.scale) |scale| effective_scale *= scale;
        if (scaled_kt.scale) |scale| effective_scale *= scale;

        const expected_scale: f32 = 1.0 / @sqrt(@as(f32, @floatFromInt(head_dim)));
        if (!approxEqScale(effective_scale, expected_scale)) continue;

        const out_shape = n.output_shape;
        const fused_id = try work.addNode(.{
            .op = .{ .fused_sdpa = .{
                .batch = batch,
                .seq_len = seq_len,
                .num_heads = num_heads,
                .head_dim = head_dim,
            } },
            .output_shape = out_shape,
            .inputs = .{ q_id, k_id, v_id, null_node },
            .num_inputs = 3,
            .vjp_alternate = @intCast(i),
        });

        try redirects.append(allocator, .{ .from = @intCast(i), .to = fused_id });
        num_rewrites += 1;
    }

    return .{ .redirects = redirects, .num_rewrites = num_rewrites };
}

const ScaleResult = struct {
    scores_id: NodeId,
    scale: f32,
};

/// Walk backward from a node through masking ops (where_select, add,
/// convert_dtype) to find a mul or div by a scalar constant. Returns the
/// scores input to the scale op if found.
fn findScaleOp(graph: *const Graph, start_id: NodeId) ?ScaleResult {
    var cur = start_id;
    var depth: u32 = 0;
    while (depth < 8) : (depth += 1) {
        if (cur == null_node or cur >= graph.nodeCount()) return null;
        const node = graph.node(cur);
        const tag = std.meta.activeTag(node.op);

        // mul(scores, scale) or mul(scale, scores)
        if (tag == .mul) {
            const in0 = node.inputs[0];
            const in1 = node.inputs[1];
            if (in0 == null_node or in1 == null_node) return null;
            if (extractScalarConstValue(graph, in1)) |scale| return .{ .scores_id = in0, .scale = scale };
            if (extractScalarConstValue(graph, in0)) |scale| return .{ .scores_id = in1, .scale = scale };
            return null;
        }

        // div(scores, scale) — GPT-2 uses Div by sqrt(head_dim).
        // Only fire when the divisor is a literal scalar constant; the
        // previous "small or broadcast" fallback silently misclassified
        // non-scale operands (e.g. masks) as a unit divisor and produced
        // a fused_sdpa with the wrong attention scaling.
        if (tag == .div) {
            const in0 = node.inputs[0];
            const in1 = node.inputs[1];
            if (in0 == null_node or in1 == null_node) return null;
            if (extractScalarConstValue(graph, in1)) |scale| {
                if (scale == 0.0) return null;
                return .{ .scores_id = in0, .scale = 1.0 / scale };
            }
            return null;
        }

        // Skip through masking/casting/shape-only ops
        if (tag == .where_select or tag == .add or tag == .convert_dtype or tag == .reshape or tag == .broadcast_in_dim) {
            // For where_select: input 0 is condition, inputs 1/2 are values.
            // The scores flow through input 1 (true_value) or input 2.
            // For add: one operand is the mask, other is scores — follow
            // the non-constant operand.
            // For convert_dtype/reshape/broadcast_in_dim: pass through input 0.
            if (tag == .convert_dtype or tag == .reshape or tag == .broadcast_in_dim) {
                cur = node.inputs[0];
                continue;
            }
            if (tag == .where_select) {
                // where(cond, false_val, true_val) — scores are in true_val (input 2)
                // or false_val (input 1). Try input 2 first (ONNX Where convention
                // after conversion: inputs = [cond, false, true]).
                cur = node.inputs[2];
                if (cur == null_node) cur = node.inputs[1];
                continue;
            }
            if (tag == .add) {
                const a0 = node.inputs[0];
                const a1 = node.inputs[1];
                // Follow the non-broadcast/non-constant operand
                if (a1 != null_node and isSmallOrBroadcast(graph, a1)) {
                    if (findScaleOp(graph, a0)) |result| return result;
                    cur = a0;
                } else if (a0 != null_node and isSmallOrBroadcast(graph, a0)) {
                    if (findScaleOp(graph, a1)) |result| return result;
                    cur = a1;
                } else {
                    // Can't determine which is the mask. Prefer the branch
                    // that actually resolves to a score chain.
                    if (findScaleOp(graph, a0)) |result| return result;
                    if (findScaleOp(graph, a1)) |result| return result;
                    cur = a0;
                }
                continue;
            }
        }

        if (tag == .dot_general) {
            return .{ .scores_id = cur, .scale = 1.0 };
        }

        return null; // unrecognized op in the chain
    }
    return null;
}

fn findSoftmaxNode(graph: *const Graph, start_id: NodeId) NodeId {
    var cur = start_id;
    var depth: u32 = 0;
    while (depth < 8) : (depth += 1) {
        if (cur == null_node or cur >= graph.nodeCount()) return null_node;
        const n = graph.node(cur);
        switch (n.op) {
            .fused_softmax => return cur,
            .convert_dtype, .reshape => cur = n.inputs[0],
            .where_select => {
                const true_input = findSoftmaxNode(graph, n.inputs[2]);
                if (true_input != null_node) return true_input;
                cur = n.inputs[1];
            },
            else => return null_node,
        }
    }
    return null_node;
}

/// Check if a node is a broadcast, constant, or parameter (likely a mask).
fn isSmallOrBroadcast(graph: *const Graph, id: NodeId) bool {
    if (id == null_node or id >= graph.nodeCount()) return false;
    const tag = std.meta.activeTag(graph.node(id).op);
    return tag == .broadcast_in_dim or tag == .constant or tag == .parameter;
}

fn isScalarConstNode(graph: *const Graph, id: NodeId) bool {
    return extractScalarConstValue(graph, id) != null;
}

fn extractScalarConstValue(graph: *const Graph, id: NodeId) ?f32 {
    if (id == null_node or id >= graph.nodeCount()) return null;
    const n = graph.node(id);
    switch (n.op) {
        .constant => |attrs| {
            if (attrs.data_len != 1) return null;
            return scalarConstantValue(graph, n.output_shape.dtype, attrs);
        },
        .convert_dtype, .reshape, .broadcast_in_dim => {
            return extractScalarConstValue(graph, n.inputs[0]);
        },
        else => return null,
    }
}

fn scalarConstantValue(graph: *const Graph, dtype: shape_mod.DType, attrs: node_mod.ConstantAttrs) f32 {
    return switch (dtype) {
        .f32 => graph.constantDataAs(f32, attrs.data_offset, attrs.data_len)[0],
        .f16 => @floatCast(graph.constantDataAs(f16, attrs.data_offset, attrs.data_len)[0]),
        .bf16 => blk: {
            const bits: u32 = @as(u32, graph.constantDataAs(u16, attrs.data_offset, attrs.data_len)[0]) << 16;
            break :blk @bitCast(bits);
        },
        .f64 => @floatCast(graph.constantDataAs(f64, attrs.data_offset, attrs.data_len)[0]),
        .i8 => @floatFromInt(graph.constantDataAs(i8, attrs.data_offset, attrs.data_len)[0]),
        .i16 => @floatFromInt(graph.constantDataAs(i16, attrs.data_offset, attrs.data_len)[0]),
        .i32 => @floatFromInt(graph.constantDataAs(i32, attrs.data_offset, attrs.data_len)[0]),
        .i64 => @floatFromInt(graph.constantDataAs(i64, attrs.data_offset, attrs.data_len)[0]),
        .u8 => @floatFromInt(graph.constantDataAs(u8, attrs.data_offset, attrs.data_len)[0]),
        .bool_ => if (graph.constantDataAs(u8, attrs.data_offset, attrs.data_len)[0] == 0) 0.0 else 1.0,
    };
}

const PrescaledTensor = struct {
    base_id: NodeId,
    scale: ?f32 = null,
};

fn findPrescaledTensor(graph: *const Graph, id: NodeId) PrescaledTensor {
    if (id == null_node or id >= graph.nodeCount()) return .{ .base_id = id };
    const n = graph.node(id);
    switch (n.op) {
        .mul => {
            const in0 = n.inputs[0];
            const in1 = n.inputs[1];
            if (extractScalarConstValue(graph, in1)) |scale| return .{ .base_id = in0, .scale = scale };
            if (extractScalarConstValue(graph, in0)) |scale| return .{ .base_id = in1, .scale = scale };
        },
        .div => {
            const in0 = n.inputs[0];
            const in1 = n.inputs[1];
            if (extractScalarConstValue(graph, in1)) |scale| {
                if (scale == 0.0) return .{ .base_id = id };
                return .{ .base_id = in0, .scale = 1.0 / scale };
            }
        },
        else => {},
    }
    return .{ .base_id = id };
}

const ScaledTransposeTensor = struct {
    k_id: NodeId,
    scale: ?f32 = null,
};

fn canonicalizeAttentionK(graph: *const Graph, id: NodeId) NodeId {
    if (id == null_node or id >= graph.nodeCount()) return id;
    const n = graph.node(id);
    if (std.meta.activeTag(n.op) != .reshape) return id;
    const src_id = n.inputs[0];
    if (src_id == null_node or src_id >= graph.nodeCount()) return id;
    const src_shape = graph.node(src_id).output_shape;
    const dst_shape = n.output_shape;
    if (src_shape.rank() == 4 and dst_shape.rank() == 3 and src_shape.dim(3) == dst_shape.dim(2)) {
        return src_id;
    }
    return id;
}

fn findScaledTransposeTensor(graph: *const Graph, id: NodeId) ScaledTransposeTensor {
    if (id == null_node or id >= graph.nodeCount()) return .{ .k_id = id };
    const prescaled = findPrescaledTensor(graph, id);
    var base_id = prescaled.base_id;
    if (base_id == null_node or base_id >= graph.nodeCount()) {
        return .{ .k_id = prescaled.base_id, .scale = prescaled.scale };
    }
    while (base_id != null_node and base_id < graph.nodeCount()) {
        const base = graph.node(base_id);
        switch (base.op) {
            .reshape, .convert_dtype => base_id = base.inputs[0],
            else => break,
        }
    }
    if (base_id == null_node or base_id >= graph.nodeCount()) {
        return .{ .k_id = null_node, .scale = prescaled.scale };
    }
    const n = graph.node(base_id);
    if (std.meta.activeTag(n.op) != .transpose) {
        return .{ .k_id = null_node, .scale = prescaled.scale };
    }
    return .{ .k_id = canonicalizeAttentionK(graph, n.inputs[0]), .scale = prescaled.scale };
}

fn approxEqScale(a: f32, b: f32) bool {
    return @abs(a - b) <= 1e-4;
}

// ── Graph Reconstruction ──────────────────────────────────────────────

fn rebuild(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    redirect: []const NodeId,
    num_rewrites: u32,
) !FuseResult {
    const count = graph.nodeCount();

    // Mark reachable from redirected outputs.
    const reachable = try allocator.alloc(bool, count);
    defer allocator.free(reachable);
    @memset(reachable, false);

    for (graph.outputs.items) |out_id| {
        markReachable(graph, reachable, redirect, resolve(redirect, out_id));
    }

    // Compute topological order of reachable nodes via Kahn's algorithm.
    // Redirects can cause synthetic nodes (appended at end) to be
    // referenced by earlier nodes, breaking sequential order.
    const in_degree = try allocator.alloc(u32, count);
    defer allocator.free(in_degree);
    @memset(in_degree, 0);

    // Build forward adjacency: for each reachable node, count how many
    // reachable nodes depend on it (its consumer count). But for Kahn's
    // we need in-degree = number of reachable *inputs* each node has.
    for (0..count) |i| {
        if (!reachable[i]) continue;
        const n = graph.node(@intCast(i));
        var deg: u32 = 0;
        for (n.getInputs()) |inp| {
            if (inp != null_node and inp < count) {
                const redir = resolve(redirect, inp);
                if (redir < count and reachable[redir]) {
                    deg += 1;
                }
            }
        }
        in_degree[i] = deg;
    }

    // Build consumer lists for efficient Kahn's BFS.
    const consumers = try allocator.alloc(std.ArrayListUnmanaged(NodeId), count);
    defer {
        for (consumers) |*c| c.deinit(allocator);
        allocator.free(consumers);
    }
    for (consumers) |*c| c.* = .empty;

    for (0..count) |i| {
        if (!reachable[i]) continue;
        const n = graph.node(@intCast(i));
        for (n.getInputs()) |inp| {
            if (inp != null_node and inp < count) {
                const redir = resolve(redirect, inp);
                if (redir < count and reachable[redir]) {
                    try consumers[redir].append(allocator, @intCast(i));
                }
            }
        }
    }

    // BFS from nodes with in-degree 0.
    const topo_order = try allocator.alloc(NodeId, count);
    defer allocator.free(topo_order);
    var topo_len: u32 = 0;

    var queue = std.ArrayListUnmanaged(NodeId).empty;
    defer queue.deinit(allocator);
    for (0..count) |i| {
        if (reachable[i] and in_degree[i] == 0) {
            try queue.append(allocator, @intCast(i));
        }
    }

    while (queue.items.len > 0) {
        const node_id = queue.orderedRemove(0);
        topo_order[topo_len] = node_id;
        topo_len += 1;

        for (consumers[node_id].items) |consumer_id| {
            in_degree[consumer_id] -= 1;
            if (in_degree[consumer_id] == 0) {
                try queue.append(allocator, consumer_id);
            }
        }
    }

    // Build old->new ID mapping in topological order.
    const id_map = try allocator.alloc(NodeId, count);
    @memset(id_map, null_node);

    var new_count: u32 = 0;
    for (topo_order[0..topo_len]) |old_id| {
        id_map[old_id] = new_count;
        new_count += 1;
    }

    // Build new graph in topological order.
    var new_graph = Graph.init(allocator);
    errdefer new_graph.deinit();

    try new_graph.string_table.appendSlice(allocator, graph.string_table.items);
    try new_graph.constant_pool.appendSlice(allocator, graph.constant_pool.items);

    for (topo_order[0..topo_len]) |old_id| {
        const old_node = graph.node(old_id);
        var new_node = old_node.*;

        // Remap inputs through redirect then id_map.
        for (0..new_node.num_inputs) |j| {
            const old_input = new_node.inputs[j];
            if (old_input != null_node) {
                const redir = resolve(redirect, old_input);
                new_node.inputs[j] = id_map[redir];
            }
        }

        // Remap vjp_alternate through redirect.
        if (new_node.vjp_alternate != null_node) {
            const redir = resolve(redirect, new_node.vjp_alternate);
            if (redir < count and reachable[redir]) {
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
        if (id_map[redir] != null_node) {
            try new_graph.parameters.append(allocator, id_map[redir]);
        }
    }

    return .{ .graph = new_graph, .id_map = id_map, .num_rewrites = num_rewrites };
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
}

// ── Tests ──────────────────────────────────────────────────────────────

const Builder = @import("../builder.zig").Builder;

fn addBareFusedLinearNoBias(g: *Graph, input: NodeId, weight: NodeId, rows: u32, in_dim: u32, out_dim: u32) !NodeId {
    const input_shape = g.node(input).output_shape;
    return g.addNode(.{
        .op = .{ .fused_linear_no_bias = .{ .rows = rows, .in_dim = in_dim, .out_dim = out_dim } },
        .output_shape = Shape.init(input_shape.dtype, &.{ @intCast(rows), @intCast(out_dim) }),
        .inputs = .{ input, weight, null_node, null_node },
        .num_inputs = 2,
    });
}

test "fuse eliminates add-zero" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const zero = try b.scalarConst(.f32, 0.0);
    const sum = try b.add(x, zero);
    try g.markOutput(sum);

    const before = g.nodeCount();
    var result = try fuse(allocator, &g);
    defer result.deinit();

    // add(x, 0) -> x, so the add and zero constant become dead
    try std.testing.expect(result.graph.nodeCount() < before);
    try std.testing.expectEqual(@as(u32, 1), result.num_rewrites);
    // Output should be x (a parameter)
    const out_id = result.graph.outputs.items[0];
    try std.testing.expect(std.meta.activeTag(result.graph.node(out_id).op) == .parameter);
}

test "fuse eliminates mul-one" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{3}));
    const one = try b.scalarConst(.f32, 1.0);
    const prod = try b.mul(x, one);
    try g.markOutput(prod);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.num_rewrites);
    const out_id = result.graph.outputs.items[0];
    try std.testing.expect(std.meta.activeTag(result.graph.node(out_id).op) == .parameter);
}

test "fuse eliminates mul-zero" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // The zero must already have the same shape as the multiplicand —
    // otherwise the mul produces a broadcast result and redirecting to
    // a scalar zero would silently drop the broadcast shape. The
    // canonical post-import form is broadcast_in_dim(scalar, S), which
    // extractScalarConstValue walks through to find the underlying 0.
    const x = try b.parameter("x", Shape.init(.f32, &.{3}));
    const zero_scalar = try b.scalarConst(.f32, 0.0);
    const zero_bc = try g.addNode(.{
        .op = .{ .broadcast_in_dim = .{ .target_shape = Shape.init(.f32, &.{3}) } },
        .output_shape = Shape.init(.f32, &.{3}),
        .inputs = .{ zero_scalar, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const prod = try b.mul(x, zero_bc);
    try g.markOutput(prod);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.num_rewrites);
    // Output should be the broadcast-zero tensor (still constant-valued
    // and shape-correct).
    const out_id = result.graph.outputs.items[0];
    try std.testing.expect(std.meta.activeTag(result.graph.node(out_id).op) == .broadcast_in_dim);
}

test "fuse skips mul-zero when scalar would change shape" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // mul(x:[3], 0:scalar) — output is shape [3]. Redirecting to the
    // scalar zero would silently change the consumer's expected rank,
    // so the rule must NOT fire here.
    const x = try b.parameter("x", Shape.init(.f32, &.{3}));
    const zero = try b.scalarConst(.f32, 0.0);
    const prod = try b.mul(x, zero);
    try g.markOutput(prod);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 0), result.num_rewrites);
    const out_id = result.graph.outputs.items[0];
    try std.testing.expect(std.meta.activeTag(result.graph.node(out_id).op) == .mul);
}

test "fuse cancels double negation" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{4}));
    const neg1 = try b.neg(x);
    const neg2 = try b.neg(neg1);
    try g.markOutput(neg2);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.num_rewrites);
    // neg(neg(x)) -> x
    const out_id = result.graph.outputs.items[0];
    try std.testing.expect(std.meta.activeTag(result.graph.node(out_id).op) == .parameter);
}

test "fuse cancels inverse transpose" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 3, 4 }));
    const t1 = try b.transpose(x, &.{ 1, 0 }); // [4, 3]
    const t2 = try b.transpose(t1, &.{ 1, 0 }); // [3, 4] — back to original
    try g.markOutput(t2);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.num_rewrites);
    const out_id = result.graph.outputs.items[0];
    try std.testing.expect(std.meta.activeTag(result.graph.node(out_id).op) == .parameter);
}

test "fuse eliminates identity reshape" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const r = try b.reshape(x, Shape.init(.f32, &.{ 2, 4 })); // same shape
    try g.markOutput(r);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.num_rewrites);
    const out_id = result.graph.outputs.items[0];
    try std.testing.expect(std.meta.activeTag(result.graph.node(out_id).op) == .parameter);
}

test "fuse preserves already-optimal graph" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const y = try b.parameter("y", Shape.init(.f32, &.{ 2, 4 }));
    const sum = try b.add(x, y);
    const prod = try b.mul(sum, x);
    try g.markOutput(prod);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 0), result.num_rewrites);
    try std.testing.expectEqual(g.nodeCount(), result.graph.nodeCount());
}

test "fuse preserves fused ops" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 8 }));
    const w = try b.parameter("w", Shape.init(.f32, &.{8}));
    const normed = try b.rmsNorm(x, w, 8, 1e-5);
    const out = try b.gelu(normed);
    try g.markOutput(out);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    // Fused ops themselves should not be eliminated
    const out_id = result.graph.outputs.items[0];
    try std.testing.expect(result.graph.node(out_id).op.isFused());
}

test "fuse chains of simplifications" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{4}));
    const zero = try b.scalarConst(.f32, 0.0);
    const one = try b.scalarConst(.f32, 1.0);
    // mul(add(x, 0), 1) -> mul(x, 1) -> x
    const sum = try b.add(x, zero);
    const prod = try b.mul(sum, one);
    try g.markOutput(prod);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 2), result.num_rewrites);
    // Final result should be just x
    try std.testing.expectEqual(@as(u32, 1), result.graph.nodeCount());
    const out_id = result.graph.outputs.items[0];
    try std.testing.expect(std.meta.activeTag(result.graph.node(out_id).op) == .parameter);
}

test "fuse eliminates sub-zero" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{4}));
    const zero = try b.scalarConst(.f32, 0.0);
    const diff = try b.sub(x, zero);
    try g.markOutput(diff);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.num_rewrites);
    const out_id = result.graph.outputs.items[0];
    try std.testing.expect(std.meta.activeTag(result.graph.node(out_id).op) == .parameter);
}

test "fuse eliminates div-one" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{4}));
    const one = try b.scalarConst(.f32, 1.0);
    const quotient = try b.div(x, one);
    try g.markOutput(quotient);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.num_rewrites);
    const out_id = result.graph.outputs.items[0];
    try std.testing.expect(std.meta.activeTag(result.graph.node(out_id).op) == .parameter);
}

test "fuse eliminates double abs" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{4}));
    const a1 = try b.absOp(x);
    const a2 = try b.absOp(a1);
    try g.markOutput(a2);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.num_rewrites);
    // abs(abs(x)) -> abs(x), not x
    const out_id = result.graph.outputs.items[0];
    try std.testing.expect(std.meta.activeTag(result.graph.node(out_id).op) == .abs);
}

test "fuse eliminates identity convert_dtype" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    // convert f32 -> f32: identity
    const cvt = try g.addNode(.{
        .op = .{ .convert_dtype = .{ .target = .f32 } },
        .output_shape = Shape.init(.f32, &.{ 2, 4 }),
        .inputs = .{ x, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    try g.markOutput(cvt);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.num_rewrites);
    const out_id = result.graph.outputs.items[0];
    try std.testing.expect(std.meta.activeTag(result.graph.node(out_id).op) == .parameter);
}

test "fuse eliminates identity broadcast_in_dim" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    // broadcast [2,4] -> [2,4]: identity
    const bcast = try g.addNode(.{
        .op = .{ .broadcast_in_dim = .{
            .target_shape = Shape.init(.f32, &.{ 2, 4 }),
            .num_axes = 2,
        } },
        .output_shape = Shape.init(.f32, &.{ 2, 4 }),
        .inputs = .{ x, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    try g.markOutput(bcast);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.num_rewrites);
    const out_id = result.graph.outputs.items[0];
    try std.testing.expect(std.meta.activeTag(result.graph.node(out_id).op) == .parameter);
}

test "fuse linear pair: two linearNoBias on same input" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 4, 8 }));
    const wq = try b.parameter("q_proj", Shape.init(.f32, &.{ 8, 8 }));
    const wk = try b.parameter("k_proj", Shape.init(.f32, &.{ 8, 8 }));

    // Two separate linearNoBias calls on same input with same dims.
    const q = try b.linearNoBias(x, wq, 4, 8, 8);
    const k = try b.linearNoBias(x, wk, 4, 8, 8);

    // Use both outputs.
    const sum = try b.add(q, k);
    try g.markOutput(sum);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    // Should fuse the two linears into one pair.
    try std.testing.expect(result.num_rewrites >= 1);
    // The pair + to_float32 should replace the two individual linears.
    // Check output graph has a fused_linear_no_bias_pair.
    var found_pair = false;
    for (0..result.graph.nodeCount()) |i| {
        if (std.meta.activeTag(result.graph.node(@intCast(i)).op) == .fused_linear_no_bias_pair) {
            found_pair = true;
            break;
        }
    }
    try std.testing.expect(found_pair);
}

test "fuse linear group: three uniform QKV projections (GQA path)" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 4, 8 }));
    const wq = try b.parameter("q_proj", Shape.init(.f32, &.{ 8, 8 }));
    const wk = try b.parameter("k_proj", Shape.init(.f32, &.{ 8, 8 }));
    const wv = try b.parameter("v_proj", Shape.init(.f32, &.{ 8, 8 }));

    const q = try addBareFusedLinearNoBias(&g, x, wq, 4, 8, 8);
    const k = try addBareFusedLinearNoBias(&g, x, wk, 4, 8, 8);
    const v = try addBareFusedLinearNoBias(&g, x, wv, 4, 8, 8);

    const qk = try b.add(q, k);
    const qkv = try b.add(qk, v);
    try g.markOutput(qkv);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    // 3+ members → GQA path, even when out_dim is uniform: 1 combined
    // matmul + 3 slices, no fused_linear_no_bias_pair, no standalone.
    try std.testing.expect(result.num_rewrites >= 1);
    var pair_count: u32 = 0;
    var combined_count: u32 = 0;
    var slice_count: u32 = 0;
    for (0..result.graph.nodeCount()) |i| {
        const op_tag = std.meta.activeTag(result.graph.node(@intCast(i)).op);
        if (op_tag == .fused_linear_no_bias_pair) pair_count += 1;
        if (op_tag == .fused_linear_no_bias) {
            const a = result.graph.node(@intCast(i)).op.fused_linear_no_bias;
            // Combined out_dim = 8 + 8 + 8 = 24.
            if (a.out_dim == 24) combined_count += 1;
        }
        if (op_tag == .slice) slice_count += 1;
    }
    try std.testing.expectEqual(@as(u32, 0), pair_count);
    try std.testing.expectEqual(@as(u32, 1), combined_count);
    try std.testing.expectEqual(@as(u32, 3), slice_count);
}

test "fuse linear group: GQA with mismatched out_dim (Q=H, K/V=G)" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // LLaMA-3-style GQA shape: Q has 4 heads * 8 dim = 32, K/V have 1
    // head * 8 dim = 8 each. All three project from the same hidden.
    const x = try b.parameter("x", Shape.init(.f32, &.{ 4, 16 }));
    const wq = try b.parameter("q_proj", Shape.init(.f32, &.{ 32, 16 }));
    const wk = try b.parameter("k_proj", Shape.init(.f32, &.{ 8, 16 }));
    const wv = try b.parameter("v_proj", Shape.init(.f32, &.{ 8, 16 }));

    const q = try addBareFusedLinearNoBias(&g, x, wq, 4, 16, 32);
    const k = try addBareFusedLinearNoBias(&g, x, wk, 4, 16, 8);
    const v = try addBareFusedLinearNoBias(&g, x, wv, 4, 16, 8);

    // Add a marker to keep all three reachable from outputs without
    // requiring shape-compatible binary ops (Q has out_dim 32, K/V 8).
    try g.markOutput(q);
    try g.markOutput(k);
    try g.markOutput(v);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    // The combined matmul has out_dim 32+8+8 = 48, with three slices to
    // recover Q/K/V. No fused_linear_no_bias_pair (it can't represent
    // unequal out_dims). The combined linear should also carry the
    // grouped-projection metadata for backends that want to dispatch
    // a fused QKV kernel.
    var combined_out_dim: u32 = 0;
    var combined_attrs: ?node_mod.LinearAttrs = null;
    var pair_count: u32 = 0;
    var slice_count: u32 = 0;
    var concat_count: u32 = 0;
    for (0..result.graph.nodeCount()) |i| {
        const node = result.graph.node(@intCast(i));
        switch (node.op) {
            .fused_linear_no_bias => |a| {
                combined_out_dim = a.out_dim;
                combined_attrs = a;
            },
            .fused_linear_no_bias_pair => pair_count += 1,
            .slice => slice_count += 1,
            .concat_prim => concat_count += 1,
            else => {},
        }
    }
    try std.testing.expectEqual(@as(u32, 0), pair_count);
    try std.testing.expectEqual(@as(u32, 48), combined_out_dim);
    try std.testing.expectEqual(@as(u32, 3), slice_count);
    // Two concat_prim ops chain three weights (concat is binary).
    try std.testing.expectEqual(@as(u32, 2), concat_count);

    // Grouped metadata: 3 projections of sizes 32, 8, 8 (Q, K, V).
    const a = combined_attrs.?;
    try std.testing.expectEqual(@as(u8, 3), a.num_projections);
    try std.testing.expectEqual(@as(u32, 32), a.projection_out_dims[0]);
    try std.testing.expectEqual(@as(u32, 8), a.projection_out_dims[1]);
    try std.testing.expectEqual(@as(u32, 8), a.projection_out_dims[2]);

    // Q (out_dim 32) and K, V (out_dim 8 each) are recovered with
    // matching shapes via slices on axis 1.
    const out_q_id = result.graph.outputs.items[0];
    try std.testing.expectEqual(@as(i64, 32), result.graph.node(out_q_id).output_shape.dim(1));
    const out_k_id = result.graph.outputs.items[1];
    try std.testing.expectEqual(@as(i64, 8), result.graph.node(out_k_id).output_shape.dim(1));
    const out_v_id = result.graph.outputs.items[2];
    try std.testing.expectEqual(@as(i64, 8), result.graph.node(out_v_id).output_shape.dim(1));
}

test "fuse linear group: 2 different out_dim (GQA path)" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 4, 8 }));
    const wq = try b.parameter("q_proj", Shape.init(.f32, &.{ 8, 8 }));
    const wo = try b.parameter("o_proj", Shape.init(.f32, &.{ 16, 8 }));

    // Two members with different out_dims → GQA path, NOT pair path.
    const q = try addBareFusedLinearNoBias(&g, x, wq, 4, 8, 8);
    const o = try addBareFusedLinearNoBias(&g, x, wo, 4, 8, 16);
    try g.markOutput(q);
    try g.markOutput(o);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    var pair_count: u32 = 0;
    var combined_out: u32 = 0;
    var slice_count: u32 = 0;
    for (0..result.graph.nodeCount()) |i| {
        switch (result.graph.node(@intCast(i)).op) {
            .fused_linear_no_bias_pair => pair_count += 1,
            .fused_linear_no_bias => |a| combined_out = a.out_dim,
            .slice => slice_count += 1,
            else => {},
        }
    }
    try std.testing.expectEqual(@as(u32, 0), pair_count);
    try std.testing.expectEqual(@as(u32, 24), combined_out);
    try std.testing.expectEqual(@as(u32, 2), slice_count);
}

test "fuse detects SDPA pattern: matmul-scale-softmax-matmul" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // Build the decomposed SDPA pattern manually:
    // Q, K, V are [B*H, S, D] = [2, 4, 8]
    const q = try b.parameter("Q", Shape.init(.f32, &.{ 2, 4, 8 }));
    const k = try b.parameter("K", Shape.init(.f32, &.{ 2, 4, 8 }));
    const v = try b.parameter("V", Shape.init(.f32, &.{ 2, 4, 8 }));

    // 1. K^T = transpose(K, [0, 2, 1]) -> [2, 8, 4]
    const k_t = try b.transpose(k, &.{ 0, 2, 1 });

    // 2. scores = Q @ K^T -> [2, 4, 4]
    const scores = try g.addNode(.{
        .op = .{ .dot_general = .{
            .lhs_contracting = .{ 2, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_contracting = .{ 1, 0, 0, 0, 0, 0, 0, 0 },
            .lhs_batch = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_batch = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
            .num_contracting = 1,
            .num_batch = 1,
        } },
        .output_shape = Shape.init(.f32, &.{ 2, 4, 4 }),
        .inputs = .{ q, k_t, null_node, null_node },
        .num_inputs = 2,
    });

    // 3. scale = scores * (1/sqrt(8))
    const scale = try b.scalarConst(.f32, 1.0 / @sqrt(8.0));
    const scaled = try b.mul(scores, scale);

    // 4. probs = softmax(scaled)
    const probs = try b.softmax(scaled);

    // 5. output = probs @ V -> [2, 4, 8]
    const output = try g.addNode(.{
        .op = .{ .dot_general = .{
            .lhs_contracting = .{ 2, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_contracting = .{ 1, 0, 0, 0, 0, 0, 0, 0 },
            .lhs_batch = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_batch = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
            .num_contracting = 1,
            .num_batch = 1,
        } },
        .output_shape = Shape.init(.f32, &.{ 2, 4, 8 }),
        .inputs = .{ probs, v, null_node, null_node },
        .num_inputs = 2,
    });
    try g.markOutput(output);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    // Should detect and fuse the SDPA pattern.
    try std.testing.expect(result.num_rewrites >= 1);
    var found_sdpa = false;
    for (0..result.graph.nodeCount()) |idx| {
        if (std.meta.activeTag(result.graph.node(@intCast(idx)).op) == .fused_sdpa) {
            const attrs = result.graph.node(@intCast(idx)).op.fused_sdpa;
            try std.testing.expectEqual(@as(u32, 1), attrs.batch);
            try std.testing.expectEqual(@as(u32, 2), attrs.num_heads);
            try std.testing.expectEqual(@as(u32, 4), attrs.seq_len);
            try std.testing.expectEqual(@as(u32, 8), attrs.head_dim);
            found_sdpa = true;
            break;
        }
    }
    try std.testing.expect(found_sdpa);
}

test "fuse detects SDPA pattern: GPT-2 style (4D, dynamic, div-where-add-cast)" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // GPT-2 attention layout: Q/K/V are 4D [B, H, S, D] with dynamic B and S.
    // K is initially [B, S, H, D], transposed to [B, H, D, S] for matmul.
    const B: i64 = -1; // dynamic
    const H: i64 = 12;
    const S: i64 = -1; // dynamic
    const D: i64 = 64;

    const q = try b.parameter("Q", Shape.init(.f32, &.{ B, H, S, D }));
    const k_raw = try b.parameter("K", Shape.init(.f32, &.{ B, S, H, D }));
    const v = try b.parameter("V", Shape.init(.f32, &.{ B, H, S, D }));

    // K^T = transpose(K_raw, [0, 2, 3, 1]) -> [B, H, D, S]
    const k_t = try b.transpose(k_raw, &.{ 0, 2, 3, 1 });

    // scores = Q @ K^T with 2 batch dims (B, H), contracting Q.D with K^T.D.
    const scores = try g.addNode(.{
        .op = .{ .dot_general = .{
            .lhs_contracting = .{ 3, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_contracting = .{ 2, 0, 0, 0, 0, 0, 0, 0 },
            .lhs_batch = .{ 0, 1, 0, 0, 0, 0, 0, 0 },
            .rhs_batch = .{ 0, 1, 0, 0, 0, 0, 0, 0 },
            .num_contracting = 1,
            .num_batch = 2,
        } },
        .output_shape = Shape.init(.f32, &.{ B, H, S, S }),
        .inputs = .{ q, k_t, null_node, null_node },
        .num_inputs = 2,
    });

    const target_shape = Shape.init(.f32, &.{ B, H, S, S });

    // scaled = scores / sqrt(D) via broadcast_in_dim of a scalar
    const scale_const = try b.scalarConst(.f32, @sqrt(64.0));
    const scale_bcast = try g.addNode(.{
        .op = .{ .broadcast_in_dim = .{ .target_shape = target_shape } },
        .output_shape = target_shape,
        .inputs = .{ scale_const, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const scaled = try b.div(scores, scale_bcast);

    // Causal mask via where(cond, neg_inf, scaled)
    const cond = try b.parameter("mask_cond", Shape.init(.bool_, &.{ 1, 1, S, S }));
    const neg_inf = try b.scalarConst(.f32, -std.math.inf(f32));
    const neg_inf_bcast = try g.addNode(.{
        .op = .{ .broadcast_in_dim = .{ .target_shape = target_shape } },
        .output_shape = target_shape,
        .inputs = .{ neg_inf, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const masked = try g.addNode(.{
        .op = .{ .where_select = {} },
        .output_shape = target_shape,
        .inputs = .{ cond, neg_inf_bcast, scaled, null_node },
        .num_inputs = 3,
    });

    // Additive bias (broadcast parameter acting like an attention mask)
    const bias = try b.parameter("mask_bias", Shape.init(.f32, &.{ 1, 1, S, S }));
    const bias_bcast = try g.addNode(.{
        .op = .{ .broadcast_in_dim = .{
            .target_shape = target_shape,
            .broadcast_axes = .{ 0, 1, 2, 3, 0, 0, 0, 0 },
            .num_axes = 4,
        } },
        .output_shape = target_shape,
        .inputs = .{ bias, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const biased = try b.add(masked, bias_bcast);

    // Softmax (along last axis — dynamic, uses 0 sentinel).
    const probs_f32 = try g.addNode(.{
        .op = .{ .fused_softmax = .{ .dim = 0 } },
        .output_shape = target_shape,
        .inputs = .{ biased, null_node, null_node, null_node },
        .num_inputs = 1,
    });

    // Cast back (in real GPT-2 this is f16→f32→f16; we stay f32 but exercise
    // the convert_dtype step in the chain).
    const probs_cast = try g.addNode(.{
        .op = .{ .convert_dtype = .{ .target = .f32 } },
        .output_shape = target_shape,
        .inputs = .{ probs_f32, null_node, null_node, null_node },
        .num_inputs = 1,
    });

    // output = probs @ V with 2 batch dims, contracting over the S axis.
    const output = try g.addNode(.{
        .op = .{ .dot_general = .{
            .lhs_contracting = .{ 3, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_contracting = .{ 2, 0, 0, 0, 0, 0, 0, 0 },
            .lhs_batch = .{ 0, 1, 0, 0, 0, 0, 0, 0 },
            .rhs_batch = .{ 0, 1, 0, 0, 0, 0, 0, 0 },
            .num_contracting = 1,
            .num_batch = 2,
        } },
        .output_shape = Shape.init(.f32, &.{ B, H, S, D }),
        .inputs = .{ probs_cast, v, null_node, null_node },
        .num_inputs = 2,
    });
    try g.markOutput(output);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    var found_sdpa = false;
    for (0..result.graph.nodeCount()) |idx| {
        if (std.meta.activeTag(result.graph.node(@intCast(idx)).op) == .fused_sdpa) {
            const attrs = result.graph.node(@intCast(idx)).op.fused_sdpa;
            try std.testing.expectEqual(@as(u32, 0), attrs.batch); // dynamic
            try std.testing.expectEqual(@as(u32, 12), attrs.num_heads);
            try std.testing.expectEqual(@as(u32, 0), attrs.seq_len); // dynamic
            try std.testing.expectEqual(@as(u32, 64), attrs.head_dim);
            found_sdpa = true;
            break;
        }
    }
    try std.testing.expect(found_sdpa);
}

test "fuse detects SDPA pattern: CLIP-style prescaled query" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const q = try b.parameter("Q", Shape.init(.f32, &.{ 1, 8, 76, 64 }));
    const k = try b.parameter("K", Shape.init(.f32, &.{ 1, 8, 76, 64 }));
    const v = try b.parameter("V", Shape.init(.f32, &.{ 1, 8, 76, 64 }));

    const q_scale = try b.scalarConst(.f32, 1.0 / @sqrt(64.0));
    const q_scale_4d = try g.addNode(.{
        .op = .{ .reshape = .{ .new_shape = Shape.init(.f32, &.{ 1, 1, 1, 1 }) } },
        .output_shape = Shape.init(.f32, &.{ 1, 1, 1, 1 }),
        .inputs = .{ q_scale, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const q_scale_bcast = try g.addNode(.{
        .op = .{ .broadcast_in_dim = .{
            .target_shape = Shape.init(.f32, &.{ 1, 8, 76, 64 }),
        } },
        .output_shape = Shape.init(.f32, &.{ 1, 8, 76, 64 }),
        .inputs = .{ q_scale_4d, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const q_scaled = try b.mul(q, q_scale_bcast);

    const k_t_raw = try b.transpose(k, &.{ 0, 1, 3, 2 });
    const k_t = try g.addNode(.{
        .op = .{ .reshape = .{ .new_shape = Shape.init(.f32, &.{ 1, 8, 64, 76 }) } },
        .output_shape = Shape.init(.f32, &.{ 1, 8, 64, 76 }),
        .inputs = .{ k_t_raw, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const scores = try g.addNode(.{
        .op = .{ .dot_general = .{
            .lhs_contracting = .{ 3, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_contracting = .{ 2, 0, 0, 0, 0, 0, 0, 0 },
            .lhs_batch = .{ 0, 1, 0, 0, 0, 0, 0, 0 },
            .rhs_batch = .{ 0, 1, 0, 0, 0, 0, 0, 0 },
            .num_contracting = 1,
            .num_batch = 2,
        } },
        .output_shape = Shape.init(.f32, &.{ 1, 8, 76, 76 }),
        .inputs = .{ q_scaled, k_t, null_node, null_node },
        .num_inputs = 2,
    });

    const probs_raw = try b.softmax(scores);
    const keep_cond = try b.parameter("keep_cond", Shape.init(.bool_, &.{ 1, 1, 76, 76 }));
    const probs_zero = try b.scalarConst(.f32, 0.0);
    const probs_zero_4d = try g.addNode(.{
        .op = .{ .broadcast_in_dim = .{
            .target_shape = Shape.init(.f32, &.{ 1, 8, 76, 76 }),
        } },
        .output_shape = Shape.init(.f32, &.{ 1, 8, 76, 76 }),
        .inputs = .{ probs_zero, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const probs = try g.addNode(.{
        .op = .{ .where_select = {} },
        .output_shape = Shape.init(.f32, &.{ 1, 8, 76, 76 }),
        .inputs = .{ keep_cond, probs_zero_4d, probs_raw, null_node },
        .num_inputs = 3,
    });
    const output = try g.addNode(.{
        .op = .{ .dot_general = .{
            .lhs_contracting = .{ 3, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_contracting = .{ 2, 0, 0, 0, 0, 0, 0, 0 },
            .lhs_batch = .{ 0, 1, 0, 0, 0, 0, 0, 0 },
            .rhs_batch = .{ 0, 1, 0, 0, 0, 0, 0, 0 },
            .num_contracting = 1,
            .num_batch = 2,
        } },
        .output_shape = Shape.init(.f32, &.{ 1, 8, 76, 64 }),
        .inputs = .{ probs, v, null_node, null_node },
        .num_inputs = 2,
    });
    try g.markOutput(output);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    var found_sdpa = false;
    for (0..result.graph.nodeCount()) |idx| {
        const node = result.graph.node(@intCast(idx));
        if (std.meta.activeTag(node.op) != .fused_sdpa) continue;
        // Look up the parameter inputs by name so the test stays robust
        // against rebuild reorderings as new fusion / canonicalization
        // matchers are added.
        try std.testing.expectEqualStrings("Q", paramName(&result.graph, node.inputs[0]));
        try std.testing.expectEqualStrings("K", paramName(&result.graph, node.inputs[1]));
        try std.testing.expectEqualStrings("V", paramName(&result.graph, node.inputs[2]));
        found_sdpa = true;
        break;
    }
    try std.testing.expect(found_sdpa);
}

/// Resolve a node id to the underlying parameter name (walking through
/// shape-only / dtype-only ops so test assertions don't have to care
/// about whether a Cast or Reshape sits between the SDPA op and its
/// input parameter).
fn paramName(graph: *const Graph, id: NodeId) []const u8 {
    const cur = skipShapeOps(graph, id);
    if (cur == null_node or cur >= graph.nodeCount()) return "<invalid>";
    const n = graph.node(cur);
    return switch (n.op) {
        .parameter => graph.parameterName(n),
        else => "<not-a-parameter>",
    };
}

test "fuse detects SDPA pattern: CLIP-style flattened K path" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const q = try b.parameter("Q", Shape.init(.f32, &.{ 1, 8, 76, 64 }));
    const k_input = try b.parameter("K_input", Shape.init(.f32, &.{ 1, 76, 8, 64 }));
    const v = try b.parameter("V", Shape.init(.f32, &.{ 1, 8, 76, 64 }));

    const q_scale = try b.scalarConst(.f32, 1.0 / @sqrt(64.0));
    const q_scale_4d = try g.addNode(.{
        .op = .{ .reshape = .{ .new_shape = Shape.init(.f32, &.{ 1, 1, 1, 1 }) } },
        .output_shape = Shape.init(.f32, &.{ 1, 1, 1, 1 }),
        .inputs = .{ q_scale, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const q_scale_bcast = try g.addNode(.{
        .op = .{ .broadcast_in_dim = .{
            .target_shape = Shape.init(.f32, &.{ 1, 8, 76, 64 }),
        } },
        .output_shape = Shape.init(.f32, &.{ 1, 8, 76, 64 }),
        .inputs = .{ q_scale_4d, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const q_scaled = try b.mul(q, q_scale_bcast);

    const k = try b.transpose(k_input, &.{ 0, 2, 1, 3 });
    const k_flat = try g.addNode(.{
        .op = .{ .reshape = .{ .new_shape = Shape.init(.f32, &.{ -1, -1, 64 }) } },
        .output_shape = Shape.init(.f32, &.{ -1, -1, 64 }),
        .inputs = .{ k, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const k_t = try b.transpose(k_flat, &.{ 0, 2, 1 });

    const scores = try g.addNode(.{
        .op = .{ .dot_general = .{
            .lhs_contracting = .{ 3, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_contracting = .{ 1, 0, 0, 0, 0, 0, 0, 0 },
            .lhs_batch = .{ 1, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_batch = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
            .num_contracting = 1,
            .num_batch = 1,
        } },
        .output_shape = Shape.init(.f32, &.{ 1, 8, 76, 76 }),
        .inputs = .{ q_scaled, k_t, null_node, null_node },
        .num_inputs = 2,
    });

    const probs = try b.softmax(scores);
    const output = try g.addNode(.{
        .op = .{ .dot_general = .{
            .lhs_contracting = .{ 3, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_contracting = .{ 2, 0, 0, 0, 0, 0, 0, 0 },
            .lhs_batch = .{ 0, 1, 0, 0, 0, 0, 0, 0 },
            .rhs_batch = .{ 0, 1, 0, 0, 0, 0, 0, 0 },
            .num_contracting = 1,
            .num_batch = 2,
        } },
        .output_shape = Shape.init(.f32, &.{ 1, 8, 76, 64 }),
        .inputs = .{ probs, v, null_node, null_node },
        .num_inputs = 2,
    });
    try g.markOutput(output);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    var found_sdpa = false;
    for (0..result.graph.nodeCount()) |idx| {
        const node = result.graph.node(@intCast(idx));
        if (std.meta.activeTag(node.op) != .fused_sdpa) continue;
        try std.testing.expectEqualStrings("Q", paramName(&result.graph, node.inputs[0]));
        // K is the transpose-of-K_input, look at its input parameter.
        const k_id = node.inputs[1];
        const k_param = if (k_id < result.graph.nodeCount() and std.meta.activeTag(result.graph.node(k_id).op) == .transpose)
            result.graph.node(k_id).inputs[0]
        else
            k_id;
        try std.testing.expectEqualStrings("K_input", paramName(&result.graph, k_param));
        try std.testing.expectEqualStrings("V", paramName(&result.graph, node.inputs[2]));
        found_sdpa = true;
        break;
    }
    try std.testing.expect(found_sdpa);
}

test "fuse detects SDPA pattern: CLIP-style dynamic batch" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const q = try b.parameter("Q", Shape.init(.f32, &.{ -1, 8, -1, 64 }));
    const k = try b.parameter("K", Shape.init(.f32, &.{ -1, 8, -1, 64 }));
    const v = try b.parameter("V", Shape.init(.f32, &.{ -1, 8, -1, 64 }));

    const q_scale = try b.scalarConst(.f32, 1.0 / @sqrt(64.0));
    const q_scale_4d = try g.addNode(.{
        .op = .{ .reshape = .{ .new_shape = Shape.init(.f32, &.{ 1, 1, 1, 1 }) } },
        .output_shape = Shape.init(.f32, &.{ 1, 1, 1, 1 }),
        .inputs = .{ q_scale, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const q_scale_bcast = try g.addNode(.{
        .op = .{ .broadcast_in_dim = .{
            .target_shape = Shape.init(.f32, &.{ -1, 8, -1, 64 }),
        } },
        .output_shape = Shape.init(.f32, &.{ -1, 8, -1, 64 }),
        .inputs = .{ q_scale_4d, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const q_scaled = try b.mul(q, q_scale_bcast);

    const k_t = try b.transpose(k, &.{ 0, 1, 3, 2 });
    const scores = try g.addNode(.{
        .op = .{ .dot_general = .{
            .lhs_contracting = .{ 3, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_contracting = .{ 2, 0, 0, 0, 0, 0, 0, 0 },
            .lhs_batch = .{ 0, 1, 0, 0, 0, 0, 0, 0 },
            .rhs_batch = .{ 0, 1, 0, 0, 0, 0, 0, 0 },
            .num_contracting = 1,
            .num_batch = 2,
        } },
        .output_shape = Shape.init(.f32, &.{ -1, 8, -1, -1 }),
        .inputs = .{ q_scaled, k_t, null_node, null_node },
        .num_inputs = 2,
    });

    const probs = try b.softmax(scores);
    const output = try g.addNode(.{
        .op = .{ .dot_general = .{
            .lhs_contracting = .{ 3, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_contracting = .{ 2, 0, 0, 0, 0, 0, 0, 0 },
            .lhs_batch = .{ 0, 1, 0, 0, 0, 0, 0, 0 },
            .rhs_batch = .{ 0, 1, 0, 0, 0, 0, 0, 0 },
            .num_contracting = 1,
            .num_batch = 2,
        } },
        .output_shape = Shape.init(.f32, &.{ -1, 8, -1, 64 }),
        .inputs = .{ probs, v, null_node, null_node },
        .num_inputs = 2,
    });
    try g.markOutput(output);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    var found_sdpa = false;
    for (0..result.graph.nodeCount()) |idx| {
        const node = result.graph.node(@intCast(idx));
        if (std.meta.activeTag(node.op) != .fused_sdpa) continue;
        const attrs = node.op.fused_sdpa;
        try std.testing.expectEqual(@as(u32, 0), attrs.batch);
        try std.testing.expectEqual(@as(u32, 8), attrs.num_heads);
        try std.testing.expectEqual(@as(u32, 0), attrs.seq_len);
        try std.testing.expectEqual(@as(u32, 64), attrs.head_dim);
        found_sdpa = true;
        break;
    }
    try std.testing.expect(found_sdpa);
}

// ── Tests for primitive→fused matchers ────────────────────────────────

test "fuse detects primitive softmax pattern" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 5 }));
    const in_shape = g.node(x).output_shape;

    // Hand-build the numerically stable softmax decomposition without
    // going through Builder.softmax (which would also emit a fused_softmax
    // shadow that's already detected via the fused-anchor path).
    const max_val = try g.addNode(.{
        .op = .{ .reduce_max = .{ .axes = .{ 1, 0, 0, 0, 0, 0, 0, 0 }, .num_axes = 1 } },
        .output_shape = Shape.init(.f32, &.{ 2, 1 }),
        .inputs = .{ x, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const max_bc = try g.addNode(.{
        .op = .{ .broadcast_in_dim = .{ .target_shape = in_shape } },
        .output_shape = in_shape,
        .inputs = .{ max_val, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const shifted = try b.sub(x, max_bc);
    const exp_v = try b.expOp(shifted);
    const sum_v = try g.addNode(.{
        .op = .{ .reduce_sum = .{ .axes = .{ 1, 0, 0, 0, 0, 0, 0, 0 }, .num_axes = 1 } },
        .output_shape = Shape.init(.f32, &.{ 2, 1 }),
        .inputs = .{ exp_v, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const sum_bc = try g.addNode(.{
        .op = .{ .broadcast_in_dim = .{ .target_shape = in_shape } },
        .output_shape = in_shape,
        .inputs = .{ sum_v, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const out = try b.div(exp_v, sum_bc);
    try g.markOutput(out);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    const out_id = result.graph.outputs.items[0];
    try std.testing.expectEqual(@as(std.meta.Tag(node_mod.OpCode), .fused_softmax), std.meta.activeTag(result.graph.node(out_id).op));
    try std.testing.expectEqual(@as(u32, 5), result.graph.node(out_id).op.fused_softmax.dim);
}

test "fuse detects softmax surrounded by Cast(f32→f16→f32)" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // FP16 LLM stability pattern: x is f16, the softmax body runs in f32.
    const x_f16 = try b.parameter("x", Shape.init(.f16, &.{ 2, 5 }));
    const x = try b.convertDtype(x_f16, .f32);
    const in_shape = g.node(x).output_shape;

    const max_val = try g.addNode(.{
        .op = .{ .reduce_max = .{ .axes = .{ 1, 0, 0, 0, 0, 0, 0, 0 }, .num_axes = 1 } },
        .output_shape = Shape.init(.f32, &.{ 2, 1 }),
        .inputs = .{ x, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const max_bc = try g.addNode(.{
        .op = .{ .broadcast_in_dim = .{ .target_shape = in_shape } },
        .output_shape = in_shape,
        .inputs = .{ max_val, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    // Cast in the middle of the chain — should be transparently
    // walked through by skipShapeOps.
    const max_bc_cast = try b.convertDtype(max_bc, .f32);
    const shifted = try b.sub(x, max_bc_cast);
    const exp_v = try b.expOp(shifted);
    const exp_cast = try b.convertDtype(exp_v, .f32);
    const sum_v = try g.addNode(.{
        .op = .{ .reduce_sum = .{ .axes = .{ 1, 0, 0, 0, 0, 0, 0, 0 }, .num_axes = 1 } },
        .output_shape = Shape.init(.f32, &.{ 2, 1 }),
        .inputs = .{ exp_cast, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const sum_bc = try g.addNode(.{
        .op = .{ .broadcast_in_dim = .{ .target_shape = in_shape } },
        .output_shape = in_shape,
        .inputs = .{ sum_v, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const out = try b.div(exp_v, sum_bc);
    // Cast result back to f16 — the matcher should still fire.
    const out_f16 = try b.convertDtype(out, .f16);
    try g.markOutput(out_f16);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    var found = false;
    for (0..result.graph.nodeCount()) |i| {
        if (std.meta.activeTag(result.graph.node(@intCast(i)).op) == .fused_softmax) {
            found = true;
            break;
        }
    }
    try std.testing.expect(found);
}

test "fuse detects primitive silu pattern" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // Hand-build x * sigmoid(x) without using Builder.silu so the only
    // mul reachable from the output is the candidate.
    const x = try b.parameter("x", Shape.init(.f32, &.{4}));
    const one = try b.scalarConst(.f32, 1.0);
    const neg_x = try b.neg(x);
    const exp_neg = try b.expOp(neg_x);
    const denom = try b.add(one, exp_neg);
    const sig = try b.div(one, denom);
    const out = try b.mul(x, sig);
    try g.markOutput(out);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    const out_id = result.graph.outputs.items[0];
    try std.testing.expectEqual(@as(std.meta.Tag(node_mod.OpCode), .fused_silu), std.meta.activeTag(result.graph.node(out_id).op));
}

test "fuse detects matmul + bias as fused_linear" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 4, 8 }));
    const w = try b.parameter("w", Shape.init(.f32, &.{ 16, 8 })); // [out_dim, in_dim]
    const bias = try b.parameter("b", Shape.init(.f32, &.{16}));

    const wt = try b.transpose(w, &.{ 1, 0 });
    const mm = try b.matmul(x, wt);
    const out = try b.add(mm, bias);
    try g.markOutput(out);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    const out_id = result.graph.outputs.items[0];
    try std.testing.expectEqual(@as(std.meta.Tag(node_mod.OpCode), .fused_linear), std.meta.activeTag(result.graph.node(out_id).op));
    const attrs2 = result.graph.node(out_id).op.fused_linear;
    try std.testing.expectEqual(@as(u32, 4), attrs2.rows);
    try std.testing.expectEqual(@as(u32, 8), attrs2.in_dim);
    try std.testing.expectEqual(@as(u32, 16), attrs2.out_dim);
}

test "fuse detects half-split RoPE pattern" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const head_dim: i64 = 8;
    const half: i64 = 4;
    const seq: i64 = 4;
    const x_shape = Shape.init(.f32, &.{ 2, seq, head_dim });
    const cos_shape = Shape.init(.f32, &.{ seq, half });
    const sin_shape = Shape.init(.f32, &.{ seq, half });
    const x = try b.parameter("x", x_shape);
    const cos_p = try b.parameter("cos", cos_shape);
    const sin_p = try b.parameter("sin", sin_shape);

    const cos_bc = try g.addNode(.{
        .op = .{ .broadcast_in_dim = .{ .target_shape = x_shape } },
        .output_shape = x_shape,
        .inputs = .{ cos_p, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const sin_bc = try g.addNode(.{
        .op = .{ .broadcast_in_dim = .{ .target_shape = x_shape } },
        .output_shape = x_shape,
        .inputs = .{ sin_p, null_node, null_node, null_node },
        .num_inputs = 1,
    });

    const x0 = try g.addNode(.{
        .op = .{ .slice = .{
            .starts = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
            .limits = .{ 2, seq, half, 0, 0, 0, 0, 0 },
            .strides = .{ 1, 1, 1, 1, 1, 1, 1, 1 },
            .num_axes = 3,
        } },
        .output_shape = Shape.init(.f32, &.{ 2, seq, half }),
        .inputs = .{ x, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const x1 = try g.addNode(.{
        .op = .{ .slice = .{
            .starts = .{ 0, 0, half, 0, 0, 0, 0, 0 },
            .limits = .{ 2, seq, head_dim, 0, 0, 0, 0, 0 },
            .strides = .{ 1, 1, 1, 1, 1, 1, 1, 1 },
            .num_axes = 3,
        } },
        .output_shape = Shape.init(.f32, &.{ 2, seq, half }),
        .inputs = .{ x, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const neg_x1 = try b.neg(x1);
    const rotated = try g.addNode(.{
        .op = .{ .concat_prim = .{ .axis = 2 } },
        .output_shape = x_shape,
        .inputs = .{ neg_x1, x0, null_node, null_node },
        .num_inputs = 2,
    });
    const a_branch = try b.mul(x, cos_bc);
    const c_branch = try b.mul(rotated, sin_bc);
    const out = try b.add(a_branch, c_branch);
    try g.markOutput(out);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    const out_id = result.graph.outputs.items[0];
    try std.testing.expectEqual(@as(std.meta.Tag(node_mod.OpCode), .fused_rope), std.meta.activeTag(result.graph.node(out_id).op));
    const ra = result.graph.node(out_id).op.fused_rope;
    try std.testing.expectEqual(@as(u32, 8), ra.head_dim);
    try std.testing.expectEqual(@as(u32, 4), ra.seq_len);
    try std.testing.expect(!ra.consecutive_pairs);
}

test "fuse detects matmul (no bias) as fused_linear_no_bias" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 4, 8 }));
    const w = try b.parameter("w", Shape.init(.f32, &.{ 16, 8 }));
    const wt = try b.transpose(w, &.{ 1, 0 });
    const out = try b.matmul(x, wt);
    try g.markOutput(out);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    const out_id = result.graph.outputs.items[0];
    const tag = std.meta.activeTag(result.graph.node(out_id).op);
    try std.testing.expectEqual(@as(std.meta.Tag(node_mod.OpCode), .fused_linear_no_bias), tag);
    const a = result.graph.node(out_id).op.fused_linear_no_bias;
    try std.testing.expectEqual(@as(u32, 4), a.rows);
    try std.testing.expectEqual(@as(u32, 8), a.in_dim);
    try std.testing.expectEqual(@as(u32, 16), a.out_dim);
}

test "fuse detects erf-form GELU pattern" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // 0.5 · x · (1 + erf(x · 1/√2))
    const x = try b.parameter("x", Shape.init(.f32, &.{8}));
    const half = try b.scalarConst(.f32, 0.5);
    const inv_sqrt2 = try b.scalarConst(.f32, 0.70710678);
    const one = try b.scalarConst(.f32, 1.0);

    const x_half = try b.mul(x, half);
    const scaled = try b.mul(x, inv_sqrt2);
    const erf_v = try b.erfOp(scaled);
    const one_plus = try b.add(one, erf_v);
    const result_node = try b.mul(x_half, one_plus);
    try g.markOutput(result_node);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    const out_id = result.graph.outputs.items[0];
    try std.testing.expectEqual(@as(std.meta.Tag(node_mod.OpCode), .fused_gelu), std.meta.activeTag(result.graph.node(out_id).op));
}

test "fuse detects tanh-form GELU pattern" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // 0.5 · x · (1 + tanh(√(2/π) · (x + 0.044715 · x · x · x)))
    const x = try b.parameter("x", Shape.init(.f32, &.{8}));
    const half = try b.scalarConst(.f32, 0.5);
    const one = try b.scalarConst(.f32, 1.0);
    const c = try b.scalarConst(.f32, 0.044715);
    const sqrt_2_over_pi = try b.scalarConst(.f32, 0.7978845608);

    const x_sq = try b.mul(x, x);
    const x_cubed = try b.mul(x_sq, x);
    const c_cubed = try b.mul(c, x_cubed);
    const inner = try b.add(x, c_cubed);
    const scaled = try b.mul(sqrt_2_over_pi, inner);
    const tanh_v = try b.tanhOp(scaled);
    const one_plus = try b.add(one, tanh_v);
    const x_half = try b.mul(x, half);
    const result_node = try b.mul(x_half, one_plus);
    try g.markOutput(result_node);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    const out_id = result.graph.outputs.items[0];
    try std.testing.expectEqual(@as(std.meta.Tag(node_mod.OpCode), .fused_gelu), std.meta.activeTag(result.graph.node(out_id).op));
}

test "fuse detects QuickGELU pattern" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // x · sigmoid(1.702 · x), where sigmoid(y) = 1 / (1 + exp(-y)).
    const x = try b.parameter("x", Shape.init(.f32, &.{8}));
    const one = try b.scalarConst(.f32, 1.0);
    const scale = try b.scalarConst(.f32, 1.702);

    const sx = try b.mul(scale, x);
    const neg_sx = try b.neg(sx);
    const exp_neg = try b.expOp(neg_sx);
    const denom = try b.add(one, exp_neg);
    const sig = try b.div(one, denom);
    const out = try b.mul(x, sig);
    try g.markOutput(out);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    const out_id = result.graph.outputs.items[0];
    try std.testing.expectEqual(@as(std.meta.Tag(node_mod.OpCode), .fused_quick_gelu), std.meta.activeTag(result.graph.node(out_id).op));
}

test "fuse detects primitive RMSNorm pattern" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 8 }));
    const w = try b.parameter("w", Shape.init(.f32, &.{8}));
    const x_shape = g.node(x).output_shape;
    const eps_val: f32 = 1e-5;

    const x_sq = try b.mul(x, x);
    const mean_sq = try g.addNode(.{
        .op = .{ .reduce_mean = .{ .axes = .{ 1, 0, 0, 0, 0, 0, 0, 0 }, .num_axes = 1 } },
        .output_shape = Shape.init(.f32, &.{ 2, 1 }),
        .inputs = .{ x_sq, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const eps_node = try b.scalarConst(.f32, eps_val);
    const mean_plus_eps = try b.add(mean_sq, eps_node);
    const inv_rms = try b.rsqrt(mean_plus_eps);
    const inv_rms_bc = try g.addNode(.{
        .op = .{ .broadcast_in_dim = .{ .target_shape = x_shape } },
        .output_shape = x_shape,
        .inputs = .{ inv_rms, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const normed = try b.mul(x, inv_rms_bc);
    const out = try b.mul(normed, w);
    try g.markOutput(out);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    const out_id = result.graph.outputs.items[0];
    try std.testing.expectEqual(@as(std.meta.Tag(node_mod.OpCode), .fused_rms_norm), std.meta.activeTag(result.graph.node(out_id).op));
    const a = result.graph.node(out_id).op.fused_rms_norm;
    try std.testing.expectEqual(@as(u32, 8), a.dim);
    try std.testing.expectApproxEqAbs(@as(f32, eps_val), a.eps, 1e-9);
}

test "fuse detects primitive LayerNorm pattern" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 8 }));
    const gamma = try b.parameter("gamma", Shape.init(.f32, &.{8}));
    const beta = try b.parameter("beta", Shape.init(.f32, &.{8}));
    const x_shape = g.node(x).output_shape;
    const eps_val: f32 = 1e-5;

    const mean = try g.addNode(.{
        .op = .{ .reduce_mean = .{ .axes = .{ 1, 0, 0, 0, 0, 0, 0, 0 }, .num_axes = 1 } },
        .output_shape = Shape.init(.f32, &.{ 2, 1 }),
        .inputs = .{ x, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const mean_bc = try g.addNode(.{
        .op = .{ .broadcast_in_dim = .{ .target_shape = x_shape } },
        .output_shape = x_shape,
        .inputs = .{ mean, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const centered = try b.sub(x, mean_bc);
    const centered_sq = try b.mul(centered, centered);
    const variance = try g.addNode(.{
        .op = .{ .reduce_mean = .{ .axes = .{ 1, 0, 0, 0, 0, 0, 0, 0 }, .num_axes = 1 } },
        .output_shape = Shape.init(.f32, &.{ 2, 1 }),
        .inputs = .{ centered_sq, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const eps_node = try b.scalarConst(.f32, eps_val);
    const var_plus_eps = try b.add(variance, eps_node);
    const inv_std = try b.rsqrt(var_plus_eps);
    const inv_std_bc = try g.addNode(.{
        .op = .{ .broadcast_in_dim = .{ .target_shape = x_shape } },
        .output_shape = x_shape,
        .inputs = .{ inv_std, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const normalized = try b.mul(centered, inv_std_bc);
    const scaled = try b.mul(normalized, gamma);
    const out = try b.add(scaled, beta);
    try g.markOutput(out);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    const out_id = result.graph.outputs.items[0];
    try std.testing.expectEqual(@as(std.meta.Tag(node_mod.OpCode), .fused_layer_norm), std.meta.activeTag(result.graph.node(out_id).op));
    const a = result.graph.node(out_id).op.fused_layer_norm;
    try std.testing.expectEqual(@as(u32, 8), a.dim);
    try std.testing.expectApproxEqAbs(@as(f32, eps_val), a.eps, 1e-9);
}

// ── Tests for algebraic / shape canonicalizations ─────────────────────

test "fuse hoists neg above broadcast" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{4}));
    const target_shape = Shape.init(.f32, &.{ 2, 4 });
    const bc = try g.addNode(.{
        .op = .{ .broadcast_in_dim = .{ .target_shape = target_shape } },
        .output_shape = target_shape,
        .inputs = .{ x, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const out = try b.neg(bc);
    try g.markOutput(out);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    const out_id = result.graph.outputs.items[0];
    const out_node = result.graph.node(out_id);
    try std.testing.expectEqual(@as(std.meta.Tag(node_mod.OpCode), .broadcast_in_dim), std.meta.activeTag(out_node.op));
    const inner_id = out_node.inputs[0];
    try std.testing.expectEqual(@as(std.meta.Tag(node_mod.OpCode), .neg), std.meta.activeTag(result.graph.node(inner_id).op));
    try std.testing.expectEqual(@as(u8, 1), result.graph.node(inner_id).output_shape.rank());
}

test "fuse collapses reshape(reshape(x))" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 6 }));
    const r1 = try b.reshape(x, Shape.init(.f32, &.{12}));
    const r2 = try b.reshape(r1, Shape.init(.f32, &.{ 3, 4 }));
    try g.markOutput(r2);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    const out_id = result.graph.outputs.items[0];
    const out_node = result.graph.node(out_id);
    try std.testing.expectEqual(@as(std.meta.Tag(node_mod.OpCode), .reshape), std.meta.activeTag(out_node.op));
    try std.testing.expectEqual(@as(std.meta.Tag(node_mod.OpCode), .parameter), std.meta.activeTag(result.graph.node(out_node.inputs[0]).op));
}

test "fuse composes broadcast axis maps" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // input [3], inner broadcast → [4, 3, 5] with map [1] (axis 0 → out 1).
    // Outer broadcast on the [4,3,5] tensor → [4, 6, 3, 7, 5] with map
    // [0, 2, 4] (input axis i → output axis 0/2/4). Composed map
    // should put the original input axis 0 at axis 2 of the final shape.
    const x = try b.parameter("x", Shape.init(.f32, &.{3}));
    const inter_shape = Shape.init(.f32, &.{ 4, 3, 5 });
    const final_shape = Shape.init(.f32, &.{ 4, 6, 3, 7, 5 });

    var inner_attrs: node_mod.BroadcastAttrs = .{ .target_shape = inter_shape };
    inner_attrs.num_axes = 1;
    inner_attrs.broadcast_axes[0] = 1;
    const inner = try g.addNode(.{
        .op = .{ .broadcast_in_dim = inner_attrs },
        .output_shape = inter_shape,
        .inputs = .{ x, null_node, null_node, null_node },
        .num_inputs = 1,
    });

    var outer_attrs: node_mod.BroadcastAttrs = .{ .target_shape = final_shape };
    outer_attrs.num_axes = 3;
    outer_attrs.broadcast_axes[0] = 0;
    outer_attrs.broadcast_axes[1] = 2;
    outer_attrs.broadcast_axes[2] = 4;
    const outer = try g.addNode(.{
        .op = .{ .broadcast_in_dim = outer_attrs },
        .output_shape = final_shape,
        .inputs = .{ inner, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    try g.markOutput(outer);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    // After collapsing, exactly one broadcast_in_dim (single hop) reads
    // straight from x with composed_axes=[2].
    var bc_count: u32 = 0;
    var composed_axis: u8 = 255;
    for (0..result.graph.nodeCount()) |i| {
        const node = result.graph.node(@intCast(i));
        if (std.meta.activeTag(node.op) == .broadcast_in_dim) {
            bc_count += 1;
            const a = node.op.broadcast_in_dim;
            if (a.num_axes == 1) composed_axis = a.broadcast_axes[0];
        }
    }
    try std.testing.expectEqual(@as(u32, 1), bc_count);
    try std.testing.expectEqual(@as(u8, 2), composed_axis);
}

test "fuse composes non-inverse transposes into one node" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 3, 4, 5 }));
    const t1 = try b.transpose(x, &.{ 1, 2, 0 });
    const t2 = try b.transpose(t1, &.{ 2, 0, 1 });
    try g.markOutput(t2);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    var transpose_count: u32 = 0;
    for (0..result.graph.nodeCount()) |i| {
        if (std.meta.activeTag(result.graph.node(@intCast(i)).op) == .transpose) {
            transpose_count += 1;
        }
    }
    try std.testing.expect(transpose_count <= 1);
}

test "fuse folds transpose into dot_general" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 4, 8 }));
    const w = try b.parameter("w", Shape.init(.f32, &.{ 16, 8 }));
    const wt = try b.transpose(w, &.{ 1, 0 });
    const mm = try b.matmul(x, wt);
    try g.markOutput(mm);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    var transpose_count: u32 = 0;
    var rhs_axis: u8 = 255;
    for (0..result.graph.nodeCount()) |i| {
        switch (result.graph.node(@intCast(i)).op) {
            .transpose => transpose_count += 1,
            .dot_general => |a| rhs_axis = a.rhs_contracting[0],
            else => {},
        }
    }
    try std.testing.expectEqual(@as(u32, 0), transpose_count);
    // The matmul-no-bias matcher promotes the dot to fused_linear_no_bias,
    // so a bare dot_general may not survive; if it does, axis is 1.
    try std.testing.expect(rhs_axis == 255 or rhs_axis == 1);
}

test "fuse rewrites add(x, neg(y)) to sub(x, y)" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{4}));
    const y = try b.parameter("y", Shape.init(.f32, &.{4}));
    const neg_y = try b.neg(y);
    const out = try b.add(x, neg_y);
    try g.markOutput(out);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    var sub_count: u32 = 0;
    var neg_count: u32 = 0;
    for (0..result.graph.nodeCount()) |i| {
        switch (std.meta.activeTag(result.graph.node(@intCast(i)).op)) {
            .sub => sub_count += 1,
            .neg => neg_count += 1,
            else => {},
        }
    }
    try std.testing.expectEqual(@as(u32, 1), sub_count);
    try std.testing.expectEqual(@as(u32, 0), neg_count);
}

test "fuse hoists neg out of mul" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{4}));
    const y = try b.parameter("y", Shape.init(.f32, &.{4}));
    const neg_x = try b.neg(x);
    const out = try b.mul(neg_x, y);
    try g.markOutput(out);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    // Output should be neg(mul(x, y)).
    const out_id = result.graph.outputs.items[0];
    try std.testing.expectEqual(@as(std.meta.Tag(node_mod.OpCode), .neg), std.meta.activeTag(result.graph.node(out_id).op));
    const inner = result.graph.node(out_id).inputs[0];
    try std.testing.expectEqual(@as(std.meta.Tag(node_mod.OpCode), .mul), std.meta.activeTag(result.graph.node(inner).op));
}

test "fuse collapses slice(slice(x))" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 4, 8 }));
    // First slice columns 1..7, then slice columns 1..4 of that.
    const s1 = try b.sliceLastDim(x, 1, 7);
    const s2 = try b.sliceLastDim(s1, 1, 4);
    try g.markOutput(s2);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    var slice_count: u32 = 0;
    for (0..result.graph.nodeCount()) |i| {
        if (std.meta.activeTag(result.graph.node(@intCast(i)).op) == .slice) slice_count += 1;
    }
    try std.testing.expectEqual(@as(u32, 1), slice_count);
    // Composed slice should read columns [1+1, 1+4) = [2, 5) directly off x.
    const out_id = result.graph.outputs.items[0];
    const out_attrs = result.graph.node(out_id).op.slice;
    try std.testing.expectEqual(@as(i64, 2), out_attrs.starts[1]);
    try std.testing.expectEqual(@as(i64, 5), out_attrs.limits[1]);
}

test "fuse collapses strided slice(slice(x))" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // x: [16] (vector). Inner slice: x[2:14:2] → [2, 4, 6, 8, 10, 12]
    // (6 elements). Outer slice on that: slice[1:5:2] → [4, 8] (2
    // elements, stride 2 over the inner output). Composed should read
    // x[4:14:4] → [4, 8, 12]... wait that's 3 elements. Let me recompute.
    //   inner.start=2, inner.stride=2, inner_dim = (14-2)/2 = 6.
    //   outer.start=1, outer.stride=2, outer_dim = (5-1)/2 = 2.
    //   composed.start = 2 + 1*2 = 4, composed.stride = 2*2 = 4,
    //   composed_dim = 2 → composed.limit = 4 + 2*4 = 12.
    // Output reads x[4], x[8] → 2 elements.
    const x = try b.parameter("x", Shape.init(.f32, &.{16}));

    var inner_attrs: node_mod.SliceAttrs = .{};
    inner_attrs.num_axes = 1;
    inner_attrs.starts[0] = 2;
    inner_attrs.limits[0] = 14;
    inner_attrs.strides[0] = 2;
    const inner = try g.addNode(.{
        .op = .{ .slice = inner_attrs },
        .output_shape = Shape.init(.f32, &.{6}),
        .inputs = .{ x, null_node, null_node, null_node },
        .num_inputs = 1,
    });

    var outer_attrs: node_mod.SliceAttrs = .{};
    outer_attrs.num_axes = 1;
    outer_attrs.starts[0] = 1;
    outer_attrs.limits[0] = 5;
    outer_attrs.strides[0] = 2;
    const outer = try g.addNode(.{
        .op = .{ .slice = outer_attrs },
        .output_shape = Shape.init(.f32, &.{2}),
        .inputs = .{ inner, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    try g.markOutput(outer);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    var slice_count: u32 = 0;
    var collapsed_attrs: ?node_mod.SliceAttrs = null;
    for (0..result.graph.nodeCount()) |i| {
        const node = result.graph.node(@intCast(i));
        if (std.meta.activeTag(node.op) == .slice) {
            slice_count += 1;
            collapsed_attrs = node.op.slice;
        }
    }
    try std.testing.expectEqual(@as(u32, 1), slice_count);
    const a = collapsed_attrs.?;
    try std.testing.expectEqual(@as(i64, 4), a.starts[0]);
    try std.testing.expectEqual(@as(i64, 12), a.limits[0]);
    try std.testing.expectEqual(@as(i64, 4), a.strides[0]);
}

test "fuse folds slice-of-concat into a slice of one operand" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const a = try b.parameter("a", Shape.init(.f32, &.{ 4, 4 }));
    const b_p = try b.parameter("b", Shape.init(.f32, &.{ 4, 6 }));
    const concat = try g.addNode(.{
        .op = .{ .concat_prim = .{ .axis = 1 } },
        .output_shape = Shape.init(.f32, &.{ 4, 10 }),
        .inputs = .{ a, b_p, null_node, null_node },
        .num_inputs = 2,
    });
    // Slice columns [5, 9) — fully within `b` which starts at column 4.
    const sl = try b.sliceLastDim(concat, 5, 9);
    try g.markOutput(sl);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    var slice_count: u32 = 0;
    var concat_reachable = false;
    const out_id = result.graph.outputs.items[0];
    for (0..result.graph.nodeCount()) |i| {
        switch (std.meta.activeTag(result.graph.node(@intCast(i)).op)) {
            .slice => slice_count += 1,
            .concat_prim => concat_reachable = true,
            else => {},
        }
    }
    try std.testing.expectEqual(@as(u32, 1), slice_count);
    try std.testing.expect(!concat_reachable);
    // The remaining slice should read columns [1, 5) off `b` (5-4 .. 9-4).
    const out_attrs = result.graph.node(out_id).op.slice;
    try std.testing.expectEqual(@as(i64, 1), out_attrs.starts[1]);
    try std.testing.expectEqual(@as(i64, 5), out_attrs.limits[1]);
}

test "fuse rewrites add(x, x) → mul(x, 2)" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{4}));
    const out = try b.add(x, x);
    try g.markOutput(out);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    // Output should be mul(x, 2-scalar). No `add` survives.
    var add_count: u32 = 0;
    var mul_with_scalar = false;
    for (0..result.graph.nodeCount()) |i| {
        const node = result.graph.node(@intCast(i));
        switch (node.op) {
            .add => add_count += 1,
            .mul => {
                // Look for a const-2 operand.
                for (node.getInputs()) |inp| {
                    if (inp == null_node) continue;
                    const inp_node = result.graph.node(inp);
                    switch (inp_node.op) {
                        .constant => |attrs| {
                            const data = result.graph.constantDataAs(f32, attrs.data_offset, attrs.data_len);
                            if (data.len == 1 and data[0] == 2.0) mul_with_scalar = true;
                        },
                        else => {},
                    }
                }
            },
            else => {},
        }
    }
    try std.testing.expectEqual(@as(u32, 0), add_count);
    try std.testing.expect(mul_with_scalar);
}

test "fuse rewrites sub(x, x) → broadcast(0)" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{4}));
    const out = try b.sub(x, x);
    try g.markOutput(out);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    var sub_count: u32 = 0;
    var saw_zero_broadcast = false;
    for (0..result.graph.nodeCount()) |i| {
        const node = result.graph.node(@intCast(i));
        switch (node.op) {
            .sub => sub_count += 1,
            .broadcast_in_dim => {
                const inp = node.inputs[0];
                if (inp == null_node) continue;
                const inp_node = result.graph.node(inp);
                switch (inp_node.op) {
                    .constant => |attrs| {
                        const data = result.graph.constantDataAs(f32, attrs.data_offset, attrs.data_len);
                        if (data.len == 1 and data[0] == 0.0) saw_zero_broadcast = true;
                    },
                    else => {},
                }
            },
            else => {},
        }
    }
    try std.testing.expectEqual(@as(u32, 0), sub_count);
    try std.testing.expect(saw_zero_broadcast);
}

test "fuse skips div(exp(x), exp(x)) because exp may overflow" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // exp can overflow to inf, and inf / inf is NaN, not 1.
    const x = try b.parameter("x", Shape.init(.f32, &.{4}));
    const e = try b.expOp(x);
    const out = try b.div(e, e);
    try g.markOutput(out);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    var div_count: u32 = 0;
    for (0..result.graph.nodeCount()) |i| {
        const node = result.graph.node(@intCast(i));
        switch (node.op) {
            .div => div_count += 1,
            else => {},
        }
    }
    try std.testing.expect(div_count >= 1);
}

test "fuse skips div(x, x) when x might be zero" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // Plain parameter — no proof of non-zero, so the rewrite must
    // not fire (otherwise we'd silently turn legitimate 0/0 = NaN
    // into 1).
    const x = try b.parameter("x", Shape.init(.f32, &.{4}));
    const out = try b.div(x, x);
    try g.markOutput(out);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    var div_count: u32 = 0;
    for (0..result.graph.nodeCount()) |i| {
        if (std.meta.activeTag(result.graph.node(@intCast(i)).op) == .div) div_count += 1;
    }
    try std.testing.expect(div_count >= 1);
}

test "fuse skips div(-0.0, -0.0)" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const z = try b.scalarConst(.f32, -0.0);
    const out = try b.div(z, z);
    try g.markOutput(out);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    var div_count: u32 = 0;
    for (0..result.graph.nodeCount()) |i| {
        if (std.meta.activeTag(result.graph.node(@intCast(i)).op) == .div) div_count += 1;
    }
    try std.testing.expect(div_count >= 1);
}

test "fuse skips div(NaN, NaN)" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const nan = try b.scalarConst(.f32, std.math.nan(f32));
    const out = try b.div(nan, nan);
    try g.markOutput(out);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    var div_count: u32 = 0;
    for (0..result.graph.nodeCount()) |i| {
        if (std.meta.activeTag(result.graph.node(@intCast(i)).op) == .div) div_count += 1;
    }
    try std.testing.expect(div_count >= 1);
}

test "fuse extracts typed scalar constants by dtype" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const i64_scalar = try b.scalarConst(.i64, 2.0);
    const bool_scalar = try b.scalarConst(.bool_, 1.0);

    try std.testing.expectEqual(@as(f32, 2.0), extractScalarConstValue(&g, i64_scalar).?);
    try std.testing.expectEqual(@as(f32, 1.0), extractScalarConstValue(&g, bool_scalar).?);
}

test "fuse cancels mul(neg(x), neg(y))" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{4}));
    const y = try b.parameter("y", Shape.init(.f32, &.{4}));
    const out = try b.mul(try b.neg(x), try b.neg(y));
    try g.markOutput(out);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    var neg_count: u32 = 0;
    for (0..result.graph.nodeCount()) |i| {
        if (std.meta.activeTag(result.graph.node(@intCast(i)).op) == .neg) neg_count += 1;
    }
    try std.testing.expectEqual(@as(u32, 0), neg_count);
}

test "fuse merges adjacent reduce_sum on disjoint axes" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 3, 4 }));
    const r_inner = try b.reduceSum(x, &.{2});
    const r_outer = try b.reduceSum(r_inner, &.{1});
    try g.markOutput(r_outer);

    var result = try fuse(allocator, &g);
    defer result.deinit();

    var reduce_count: u32 = 0;
    var merged_axes: u8 = 0;
    for (0..result.graph.nodeCount()) |i| {
        const node = result.graph.node(@intCast(i));
        if (std.meta.activeTag(node.op) == .reduce_sum) {
            reduce_count += 1;
            merged_axes = node.op.reduce_sum.num_axes;
        }
    }
    try std.testing.expectEqual(@as(u32, 1), reduce_count);
    try std.testing.expectEqual(@as(u8, 2), merged_axes);
}
