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

// Reverse-mode automatic differentiation.
//
// Given a computation graph and a scalar loss node, computes gradients
// of the loss with respect to requested parameter nodes by walking the
// graph backward and applying VJP (vector-Jacobian product) rules.
//
// Most fused ops are lowered to primitives first (via lower.zig), so VJPs
// can be defined against a compact primitive surface. Some hot training
// ops stay fused and carry hand-written VJPs when preserving fused semantics
// avoids expensive decomposed backward graphs.
//
// Usage:
//   var result = try gradient(allocator, &graph, loss_id, &.{param_a, param_b});
//   defer result.deinit();
//   // result.param_grads[0] = NodeId of dL/d(param_a)
//   // result.param_grads[1] = NodeId of dL/d(param_b)

const std = @import("std");
const graph_mod = @import("graph.zig");
const node_mod = @import("node.zig");
const shape_mod = @import("shape.zig");
const builder_mod = @import("builder.zig");
const lower_mod = @import("lower.zig");

const Graph = graph_mod.Graph;
const Node = node_mod.Node;
const NodeId = node_mod.NodeId;
const null_node = node_mod.null_node;
const OpCode = node_mod.OpCode;
const Shape = shape_mod.Shape;
const DType = shape_mod.DType;
const Builder = builder_mod.Builder;

pub const GradientError = error{
    /// A primitive op has no VJP rule defined.
    NoVjpRule,
    /// The loss node must produce a scalar.
    LossNotScalar,
};

pub const GradientResult = struct {
    /// The lowered graph with gradient nodes appended.
    /// Owns all memory (caller must deinit).
    graph: Graph,
    /// Gradient NodeIds for each requested parameter (in the lowered graph).
    param_grads: []NodeId,
    /// old_id → new_id mapping from lowering. Caller can use this to
    /// translate other node references.
    id_map: []NodeId,
    /// Boundary between the lowered forward graph and appended VJP nodes.
    /// Enables a caller to identify exactly which forward values a retained
    /// activation tape must bind when executing the backward graph.
    forward_node_count: u32 = 0,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *GradientResult) void {
        self.graph.deinit();
        self.allocator.free(self.param_grads);
        self.allocator.free(self.id_map);
    }
};

/// Compute gradients of a scalar loss with respect to parameter nodes.
///
/// 1. Lowers most fused ops to primitives via vjp_alternate.
/// 2. Walks backward from loss, applying VJP rules to accumulate adjoints.
/// 3. Returns gradient NodeIds for each requested parameter.
///
/// The returned GradientResult contains a modified copy of the graph
/// with gradient computation nodes appended.
pub fn gradient(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    loss: NodeId,
    wrt: []const NodeId,
) !GradientResult {
    // Step 1: Lower fused ops to primitives.
    var lowered = try lower_mod.lower(allocator, graph);
    // We'll take ownership of lowered.graph; free only id_map on error.
    errdefer {
        lowered.graph.deinit();
        allocator.free(lowered.id_map);
    }

    // Translate loss and wrt through the lowering id_map.
    const lowered_loss = lowered.id_map[loss];
    if (lowered_loss == null_node) return error.LossNotScalar;

    const lowered_wrt = try allocator.alloc(NodeId, wrt.len);
    defer allocator.free(lowered_wrt);
    for (wrt, 0..) |w, i| {
        lowered_wrt[i] = lowered.id_map[w];
    }

    // Step 2: Build the backward pass on the lowered graph.
    var b = Builder.init(&lowered.graph);
    const g = &lowered.graph;
    const forward_count = g.nodeCount();

    // Adjoint map: forward node → accumulated gradient NodeId.
    const adjoints = try allocator.alloc(NodeId, forward_count);
    defer allocator.free(adjoints);
    @memset(adjoints, null_node);

    // Seed: dL/d(loss) = 1.0
    const loss_shape = g.node(lowered_loss).output_shape;
    adjoints[lowered_loss] = try b.scalarConst(loss_shape.dtype, 1.0);

    // Step 3: Walk forward nodes in reverse order, propagating adjoints.
    var i: u32 = forward_count;
    while (i > 0) {
        i -= 1;
        if (adjoints[i] == null_node) continue;

        // IMPORTANT: copy node data before VJP computation, because
        // VJP rules add new nodes to the graph which can reallocate
        // the node array and invalidate pointers into it.
        const n_copy = g.node(i).*;
        const adj = adjoints[i];

        try applyVjp(&b, g, &n_copy, i, adj, adjoints, false);
    }

    // Step 4: Collect parameter gradients.
    const param_grads = try allocator.alloc(NodeId, wrt.len);
    for (lowered_wrt, 0..) |lw, idx| {
        param_grads[idx] = adjoints[lw]; // null_node if no gradient flows
    }

    return .{
        .graph = lowered.graph,
        .param_grads = param_grads,
        .id_map = lowered.id_map,
        .forward_node_count = forward_count,
        .allocator = allocator,
    };
}

/// A caller-computed cotangent is a detached runtime parameter. Discrete
/// proposal selection and matching may happen outside this graph; their loss
/// gradients still propagate through the original differentiable outputs.
pub const Seed = struct { output: NodeId, cotangent: NodeId };

pub const SeedOptions = struct {
    /// Inactive task heads may legitimately have no loss in a given batch.
    /// Callers can require coverage when every requested parameter is active.
    require_all_gradients: bool = false,
    max_forward_nodes: usize = 1_000_000,
    max_gradient_nodes: usize = 4_000_000,
};

fn floatingShape(shape: Shape) bool {
    if (shape.rank_ > shape_mod.max_rank) return false;
    const count = shape.numElements() orelse return false;
    if (count <= 0) return false;
    return switch (shape.dtype) {
        .f32, .f16, .bf16, .f64 => true,
        else => false,
    };
}

/// Strict reverse mode with explicit vector seeds. This function does not
/// evaluate a graph, retain activations, create losses or generate randomness.
/// A training executor must bind seeds to the exact forward weights, masks,
/// sample, schema and selected candidate identities that produced those seeds.
///
/// Unlike the compatibility scalar entry point, missing VJPs on a requested
/// gradient path fail explicitly. Frozen paths are pruned, so head-only
/// differentiation never requires encoder gradients. The caller graph and
/// its output list are left unchanged, including on allocation failures.
pub fn gradientWithSeeds(allocator: std.mem.Allocator, graph: *const Graph, seeds: []const Seed, wrt: []const NodeId, options: SeedOptions) !GradientResult {
    const count = graph.nodeCount();
    if (count == 0 or count > options.max_forward_nodes or seeds.len == 0 or seeds.len > 4096 or wrt.len > 65536 or options.max_gradient_nodes == 0)
        return error.InvalidGradientRequest;
    for (graph.nodes.items) |n| {
        if (n.num_inputs > n.inputs.len or n.output_shape.rank_ > shape_mod.max_rank) return error.InvalidGradientGraph;
        for (n.getInputs()) |input| if (input != null_node and input >= count) return error.InvalidGradientGraph;
        if (n.vjp_alternate != null_node and n.vjp_alternate >= count) return error.InvalidGradientGraph;
    }
    for (wrt, 0..) |id, index| {
        if (id >= count or graph.node(id).op != .parameter or !floatingShape(graph.node(id).output_shape)) return error.InvalidGradientParameter;
        for (wrt[0..index]) |previous| if (id == previous) return error.DuplicateGradientParameter;
    }
    var roots = std.ArrayListUnmanaged(NodeId).empty;
    defer roots.deinit(allocator);
    for (seeds) |seed| {
        if (seed.output >= count or seed.cotangent >= count or seed.output == seed.cotangent) return error.InvalidGradientSeed;
        const output = graph.node(seed.output);
        const cotangent = graph.node(seed.cotangent);
        if (cotangent.op != .parameter or !floatingShape(output.output_shape) or !output.output_shape.eq(cotangent.output_shape)) return error.InvalidGradientSeed;
        for (wrt) |id| if (id == seed.cotangent) return error.TrainableGradientSeed;
        try roots.append(allocator, seed.output);
        try roots.append(allocator, seed.cotangent);
    }
    // Only outputs are replaced on this read-only shallow view. Lowering owns
    // the returned graph; it never mutates borrowed nodes or constant pools.
    var view = graph.*;
    view.outputs = roots;
    var lowered = try lower_mod.lower(allocator, &view);
    errdefer lowered.deinit();
    var builder = Builder.init(&lowered.graph);
    const forward_count = lowered.graph.nodeCount();
    const needed = try allocator.alloc(bool, forward_count);
    defer allocator.free(needed);
    @memset(needed, false);
    for (wrt) |id| {
        const mapped = lowered.id_map[id];
        if (mapped != null_node) needed[mapped] = true;
    }
    for (0..forward_count) |index| {
        for (lowered.graph.node(@intCast(index)).getInputs()) |input| {
            if (input != null_node) needed[index] = needed[index] or needed[input];
        }
    }
    const seed_dependent = try allocator.alloc(bool, forward_count);
    defer allocator.free(seed_dependent);
    @memset(seed_dependent, false);
    for (seeds) |seed| {
        const output = lowered.id_map[seed.output];
        const cotangent = lowered.id_map[seed.cotangent];
        if (output == null_node or cotangent == null_node) return error.InvalidGradientSeed;
        seed_dependent[cotangent] = true;
    }
    for (0..forward_count) |i| {
        for (lowered.graph.node(@intCast(i)).getInputs()) |input| {
            if (input != null_node) seed_dependent[i] = seed_dependent[i] or seed_dependent[input];
        }
    }
    for (seeds) |seed| if (seed_dependent[lowered.id_map[seed.output]]) return error.GradientSeedUsedByForward;
    const adjoints = try allocator.alloc(NodeId, forward_count);
    defer allocator.free(adjoints);
    @memset(adjoints, null_node);
    for (seeds) |seed| {
        const output = lowered.id_map[seed.output];
        const cotangent = lowered.id_map[seed.cotangent];
        if (output == null_node or cotangent == null_node) return error.InvalidGradientSeed;
        try accumulate(&builder, adjoints, output, cotangent);
    }
    var index = forward_count;
    while (index != 0) {
        index -= 1;
        if (adjoints[index] == null_node or !needed[index]) continue;
        const node_copy = lowered.graph.node(index).*;
        try applyVjp(&builder, &lowered.graph, &node_copy, index, adjoints[index], adjoints, true);
        if (lowered.graph.nodeCount() - forward_count > options.max_gradient_nodes) return error.GradientGraphLimitExceeded;
    }
    const grads = try allocator.alloc(NodeId, wrt.len);
    errdefer allocator.free(grads);
    for (wrt, grads) |id, *grad| {
        const mapped = lowered.id_map[id];
        grad.* = if (mapped == null_node) null_node else adjoints[mapped];
        if (grad.* == null_node and options.require_all_gradients) return error.DisconnectedGradientParameter;
        if (grad.* != null_node and !lowered.graph.node(grad.*).output_shape.eq(graph.node(id).output_shape)) {
            return error.GradientShapeMismatch;
        }
    }
    return .{ .graph = lowered.graph, .param_grads = grads, .id_map = lowered.id_map, .forward_node_count = forward_count, .allocator = allocator };
}

/// Accumulate an adjoint contribution into the adjoint map.
/// If the target already has an adjoint, sum them.
fn accumulate(b: *Builder, adjoints: []NodeId, target: NodeId, contrib: NodeId) !void {
    if (target == null_node) return;
    if (target >= adjoints.len) return;
    if (adjoints[target] == null_node) {
        adjoints[target] = contrib;
    } else {
        adjoints[target] = try b.add(adjoints[target], contrib);
    }
}

/// Apply the VJP rule for a single node, accumulating gradient
/// contributions into its inputs' adjoint slots.
fn applyVjp(
    b: *Builder,
    g: *const Graph,
    n: *const Node,
    node_id: NodeId,
    adj: NodeId,
    adjoints: []NodeId,
    strict: bool,
) !void {
    const ins = n.getInputs();
    switch (n.op) {
        // ── No gradient ──────────────────────────────────────────────
        .parameter, .constant, .stop_gradient => {},

        // ── Elementwise unary ────────────────────────────────────────

        .neg => {
            // d/dx(-x) = -1 → grad = -adj
            try accumulate(b, adjoints, ins[0], try b.neg(adj));
        },

        .sqrt => {
            // d/dx(sqrt(x)) = 0.5 / sqrt(x) = 0.5 * rsqrt(x)
            const half = try b.scalarConst(n.output_shape.dtype, 0.5);
            const inv = try b.rsqrt(ins[0]);
            // Put tensor-shaped operand first so binaryOp picks the correct shape.
            try accumulate(b, adjoints, ins[0], try b.mul(adj, try b.mul(inv, half)));
        },

        .rsqrt => {
            // d/dx(x^{-1/2}) = -0.5 * x^{-3/2}
            const rsqrt_x = try b.rsqrt(ins[0]);
            const rsqrt_cubed = try b.mul(rsqrt_x, try b.mul(rsqrt_x, rsqrt_x));
            const neg_half = try b.scalarConst(n.output_shape.dtype, -0.5);
            try accumulate(b, adjoints, ins[0], try b.mul(adj, try b.mul(rsqrt_cubed, neg_half)));
        },

        .exp => {
            // d/dx(exp(x)) = exp(x)
            const exp_x = try b.expOp(ins[0]);
            try accumulate(b, adjoints, ins[0], try b.mul(adj, exp_x));
        },

        .log => {
            // d/dx(log(x)) = 1/x → grad = adj / x
            try accumulate(b, adjoints, ins[0], try b.div(adj, ins[0]));
        },

        .sin => {
            // d/dx(sin(x)) = cos(x)
            try accumulate(b, adjoints, ins[0], try b.mul(adj, try b.cosOp(ins[0])));
        },

        .cos => {
            // d/dx(cos(x)) = -sin(x)
            try accumulate(b, adjoints, ins[0], try b.mul(adj, try b.neg(try b.sinOp(ins[0]))));
        },

        .tanh => {
            // d/dx(tanh(x)) = 1 - tanh(x)^2 = -(tanh² - 1)
            const tanh_x = if (strict) node_id else try b.tanhOp(ins[0]);
            const tanh_sq = try b.mul(tanh_x, tanh_x);
            const one = try b.scalarConst(n.output_shape.dtype, 1.0);
            // sub(tanh_sq, one) keeps tensor shape, then negate.
            const grad = try b.neg(try b.sub(tanh_sq, one));
            try accumulate(b, adjoints, ins[0], try b.mul(adj, grad));
        },

        .erf => {
            // d/dx(erf(x)) = (2/sqrt(pi)) * exp(-x^2)
            const two_over_sqrt_pi = try b.scalarConst(n.output_shape.dtype, 1.1283791671); // 2/sqrt(pi)
            const x_sq = try b.mul(ins[0], ins[0]);
            const neg_x_sq = try b.neg(x_sq);
            const exp_val = try b.expOp(neg_x_sq);
            // Put tensor-shaped operand first for correct output shape.
            const grad = try b.mul(exp_val, two_over_sqrt_pi);
            try accumulate(b, adjoints, ins[0], try b.mul(adj, grad));
        },

        .fused_gelu => {
            const grad = try b.graph.addNode(.{
                .op = .{ .fused_gelu_backward = {} },
                .output_shape = n.output_shape,
                .inputs = .{ ins[0], adj, null_node, null_node },
                .num_inputs = 2,
            });
            try accumulate(b, adjoints, ins[0], grad);
        },

        .fused_gelu_exact => {
            const grad = try b.graph.addNode(.{
                .op = .{ .fused_gelu_exact_backward = {} },
                .output_shape = n.output_shape,
                .inputs = .{ ins[0], adj, null_node, null_node },
                .num_inputs = 2,
            });
            try accumulate(b, adjoints, ins[0], grad);
        },

        .fused_softmax => {
            // For y = softmax(x), dL/dx = y * (dL/dy - sum(dL/dy * y)).
            // Keeping this fused avoids routing attention gradients through
            // the reduce_max stabilization subgraph used by the forward
            // decomposition.
            const y = node_id;
            const rank = n.output_shape.rank();
            const last_axis: u8 = @intCast(rank - 1);
            const adj_times_y = try b.mul(adj, y);
            const dot = try b.reduceSum(adj_times_y, &.{last_axis});
            const dot_shape = b.graph.node(dot).output_shape;
            const dot_bc = try broadcastToShape(b, dot, dot_shape, n.output_shape, &.{last_axis});
            const centered = try b.sub(adj, dot_bc);
            try accumulate(b, adjoints, ins[0], try b.mul(y, centered));
        },

        .abs => {
            // d/dx(|x|) = sign(x) = x > 0 ? 1 : -1  (0 at x=0, use subgradient 0)
            const dtype = n.output_shape.dtype;
            const zero = try b.scalarConst(dtype, 0.0);
            const one = try b.scalarConst(dtype, 1.0);
            const neg_one = try b.scalarConst(dtype, -1.0);
            const is_neg = try b.graph.addNode(.{
                .op = .{ .less_than = {} },
                .output_shape = n.output_shape,
                .inputs = .{ ins[0], zero, null_node, null_node },
                .num_inputs = 2,
            });
            const sign = try b.graph.addNode(.{
                .op = .{ .where_select = {} },
                .output_shape = n.output_shape,
                .inputs = .{ is_neg, neg_one, one, null_node },
                .num_inputs = 3,
            });
            try accumulate(b, adjoints, ins[0], try b.mul(adj, sign));
        },

        // ── Elementwise binary ───────────────────────────────────────

        .add => {
            // d/d(a)(a + b) = 1, d/d(b)(a + b) = 1
            // Reduce along broadcast dimensions if inputs differ in shape.
            try accumulate(b, adjoints, ins[0], try reduceToBroadcast(b, g, adj, ins[0], strict));
            try accumulate(b, adjoints, ins[1], try reduceToBroadcast(b, g, adj, ins[1], strict));
        },

        .mul => {
            // d/d(a)(a * b) = b, d/d(b)(a * b) = a
            const grad_a = try b.mul(adj, ins[1]);
            const grad_b = try b.mul(adj, ins[0]);
            try accumulate(b, adjoints, ins[0], try reduceToBroadcast(b, g, grad_a, ins[0], strict));
            try accumulate(b, adjoints, ins[1], try reduceToBroadcast(b, g, grad_b, ins[1], strict));
        },

        .sub => {
            // d/d(a)(a - b) = 1, d/d(b)(a - b) = -1
            try accumulate(b, adjoints, ins[0], try reduceToBroadcast(b, g, adj, ins[0], strict));
            try accumulate(b, adjoints, ins[1], try reduceToBroadcast(b, g, try b.neg(adj), ins[1], strict));
        },

        .div => {
            // d/d(a)(a / b) = 1/b → grad_a = adj / b
            try accumulate(b, adjoints, ins[0], try reduceToBroadcast(b, g, try b.div(adj, ins[1]), ins[0], strict));

            // d/d(b)(a / b) = -a / b^2
            const b_sq = try b.mul(ins[1], ins[1]);
            const neg_a = try b.neg(ins[0]);
            const grad_b = try b.div(neg_a, b_sq);
            try accumulate(b, adjoints, ins[1], try reduceToBroadcast(b, g, try b.mul(adj, grad_b), ins[1], strict));
        },

        // ── Comparison / selection (no gradient through condition) ───

        .less_than => {},

        .where_select => {
            // where(cond, on_true, on_false): gradient flows through selected branch
            // grad_on_true = adj * cond, grad_on_false = adj * !cond
            // Since cond is boolean, we use where_select to route the adjoint.
            const zero = try b.scalarConst(n.output_shape.dtype, 0.0);
            const grad_true = try b.graph.addNode(.{
                .op = .{ .where_select = {} },
                .output_shape = n.output_shape,
                .inputs = .{ ins[0], adj, zero, null_node },
                .num_inputs = 3,
            });
            const grad_false = try b.graph.addNode(.{
                .op = .{ .where_select = {} },
                .output_shape = n.output_shape,
                .inputs = .{ ins[0], zero, adj, null_node },
                .num_inputs = 3,
            });
            try accumulate(b, adjoints, ins[1], if (strict) try reduceToBroadcast(b, g, grad_true, ins[1], true) else grad_true);
            try accumulate(b, adjoints, ins[2], if (strict) try reduceToBroadcast(b, g, grad_false, ins[2], true) else grad_false);
        },

        // ── Reduction ────────────────────────────────────────────────

        .reduce_sum => |attrs| {
            // Gradient of reduce_sum is broadcast of adjoint back to input shape.
            const in_shape = g.node(ins[0]).output_shape;
            const grad = try broadcastToShape(b, adj, n.output_shape, in_shape, attrs.axes[0..attrs.num_axes]);
            try accumulate(b, adjoints, ins[0], grad);
        },

        .reduce_max => |attrs| {
            // Gradient flows only to the position(s) where input equals
            // the reduced max. The previous "subgradient approximation"
            // spread the adjoint to every input position, which makes
            // softmax's `x − broadcast(max(x))` shift contribute spurious
            // gradient and broke any chain that flows through softmax
            // (SDPA, attention, anything reading partial softmax output).
            const in_shape = g.node(ins[0]).output_shape;
            const dtype = in_shape.dtype;

            // Broadcast both the adjoint and the max output back to the
            // input shape so we can mask element-wise.
            const adj_bc = try broadcastToShape(b, adj, n.output_shape, in_shape, attrs.axes[0..attrs.num_axes]);
            const max_self_id = try b.graph.addNode(.{
                .op = n.op,
                .output_shape = n.output_shape,
                .inputs = .{ ins[0], null_node, null_node, null_node },
                .num_inputs = 1,
            });
            const max_bc = try broadcastToShape(b, max_self_id, n.output_shape, in_shape, attrs.axes[0..attrs.num_axes]);

            // mask = (x == max_bc) → keep adj; else zero. Use two
            // less_than compares: where(x < max, 0, adj) gives adj at
            // positions x ≥ max, then where(max < x, 0, that) leaves
            // adj only where neither comparison holds (equality).
            const zero_scalar = try b.scalarConst(dtype, 0.0);
            var bc_zero_attrs: node_mod.BroadcastAttrs = .{ .target_shape = in_shape };
            bc_zero_attrs.num_axes = 0;
            const zero_bc = try b.graph.addNode(.{
                .op = .{ .broadcast_in_dim = bc_zero_attrs },
                .output_shape = in_shape,
                .inputs = .{ zero_scalar, null_node, null_node, null_node },
                .num_inputs = 1,
            });

            const cmp_lt = try b.graph.addNode(.{
                .op = .{ .less_than = {} },
                .output_shape = in_shape,
                .inputs = .{ ins[0], max_bc, null_node, null_node },
                .num_inputs = 2,
            });
            const after_lt = try b.graph.addNode(.{
                .op = .{ .where_select = {} },
                .output_shape = in_shape,
                .inputs = .{ cmp_lt, zero_bc, adj_bc, null_node },
                .num_inputs = 3,
            });
            const cmp_gt = try b.graph.addNode(.{
                .op = .{ .less_than = {} },
                .output_shape = in_shape,
                .inputs = .{ max_bc, ins[0], null_node, null_node },
                .num_inputs = 2,
            });
            var grad = try b.graph.addNode(.{
                .op = .{ .where_select = {} },
                .output_shape = in_shape,
                .inputs = .{ cmp_gt, zero_bc, after_lt, null_node },
                .num_inputs = 3,
            });
            if (strict) {
                // amax distributes an adjoint equally among tied maxima.
                // Counting the equality mask separately also handles a zero
                // upstream adjoint without dividing by that adjoint.
                const one = try b.scalarConst(dtype, 1.0);
                const one_bc = try b.graph.addNode(.{
                    .op = .{ .broadcast_in_dim = bc_zero_attrs },
                    .output_shape = in_shape,
                    .inputs = .{ one, null_node, null_node, null_node },
                    .num_inputs = 1,
                });
                const ge = try b.graph.addNode(.{
                    .op = .{ .where_select = {} },
                    .output_shape = in_shape,
                    .inputs = .{ cmp_lt, zero_bc, one_bc, null_node },
                    .num_inputs = 3,
                });
                const equal = try b.graph.addNode(.{
                    .op = .{ .where_select = {} },
                    .output_shape = in_shape,
                    .inputs = .{ cmp_gt, zero_bc, ge, null_node },
                    .num_inputs = 3,
                });
                const count = try b.reduceSum(equal, attrs.axes[0..attrs.num_axes]);
                const count_bc = try broadcastToShape(b, count, b.graph.node(count).output_shape, in_shape, attrs.axes[0..attrs.num_axes]);
                grad = try b.div(grad, count_bc);
            }
            try accumulate(b, adjoints, ins[0], grad);
        },

        .reduce_mean => |attrs| {
            // d/dx(mean(x, axes)) = 1/count * broadcast(adj)
            const in_shape = g.node(ins[0]).output_shape;
            var count: i64 = 1;
            for (attrs.axes[0..attrs.num_axes]) |ax| {
                const d = in_shape.dim(ax);
                if (d > 0) count *= d;
            }
            const scale = try b.scalarConst(n.output_shape.dtype, 1.0 / @as(f32, @floatFromInt(count)));
            const scaled_adj = try b.mul(adj, scale);
            const grad = try broadcastToShape(b, scaled_adj, n.output_shape, in_shape, attrs.axes[0..attrs.num_axes]);
            try accumulate(b, adjoints, ins[0], grad);
        },

        // ── Shape manipulation ───────────────────────────────────────

        .reshape => {
            // Gradient: reshape adjoint back to input shape.
            const in_shape = g.node(ins[0]).output_shape;
            try accumulate(b, adjoints, ins[0], try b.reshape(adj, in_shape));
        },

        .transpose => |attrs| {
            // Gradient: transpose adjoint with inverse permutation.
            var inv_perm: [shape_mod.max_rank]u8 = undefined;
            for (attrs.perm[0..attrs.num_axes], 0..) |p, i| {
                inv_perm[p] = @intCast(i);
            }
            try accumulate(b, adjoints, ins[0], try b.transpose(adj, inv_perm[0..attrs.num_axes]));
        },

        .broadcast_in_dim => |attrs| {
            const in_shape = g.node(ins[0]).output_shape;
            const out_shape = n.output_shape;
            var reduce_axes: [shape_mod.max_rank]u8 = undefined;
            var reduce_count: usize = 0;

            for (0..out_shape.rank()) |axis_usize| {
                const axis: u8 = @intCast(axis_usize);
                var mapped_input_axis: ?u8 = null;
                for (attrs.broadcast_axes[0..attrs.num_axes], 0..) |mapped_axis, input_axis_usize| {
                    if (mapped_axis == axis) {
                        mapped_input_axis = @intCast(input_axis_usize);
                        break;
                    }
                }

                if (mapped_input_axis) |input_axis| {
                    if (in_shape.dim(input_axis) == 1 and out_shape.dim(axis) != 1) {
                        reduce_axes[reduce_count] = axis;
                        reduce_count += 1;
                    }
                } else {
                    reduce_axes[reduce_count] = axis;
                    reduce_count += 1;
                }
            }

            const reduced = if (reduce_count > 0)
                try b.reduceSum(adj, reduce_axes[0..reduce_count])
            else
                adj;
            try accumulate(b, adjoints, ins[0], try b.reshape(reduced, in_shape));
        },

        .slice => {
            const in_shape = g.node(ins[0]).output_shape;
            try accumulate(b, adjoints, ins[0], try padSliceAdjoint(b, adj, n.op.slice, in_shape));
        },

        .concat_prim => {
            const axis = n.op.concat_prim.axis;
            const lhs_shape = g.node(ins[0]).output_shape;
            const lhs_grad = try sliceConcatAdjoint(b, adj, lhs_shape, axis, 0);
            try accumulate(b, adjoints, ins[0], lhs_grad);
            if (n.num_inputs > 1 and ins[1] != null_node) {
                const rhs_shape = g.node(ins[1]).output_shape;
                const rhs_start = lhs_shape.dim(axis);
                const rhs_grad = try sliceConcatAdjoint(b, adj, rhs_shape, axis, rhs_start);
                try accumulate(b, adjoints, ins[1], rhs_grad);
            }
        },

        // ── Contraction ──────────────────────────────────────────────

        .dot_general => |attrs| {
            // For standard matmul Y = A @ B (contracting A's last axis with B's first):
            // dL/dA = dL/dY @ B^T
            // dL/dB = A^T @ dL/dY
            const a_shape = g.node(ins[0]).output_shape;
            const b_shape = g.node(ins[1]).output_shape;
            const adj_shape = b.graph.node(adj).output_shape;
            var adj_for_dot = adj;
            if (adj_shape.rank() == 0 and n.output_shape.rank() > 0) {
                var all_axes: [shape_mod.max_rank]u8 = undefined;
                for (0..n.output_shape.rank()) |axis_usize| {
                    all_axes[axis_usize] = @intCast(axis_usize);
                }
                adj_for_dot = try broadcastToShape(b, adj, adj_shape, n.output_shape, all_axes[0..n.output_shape.rank()]);
            }

            if (attrs.num_contracting == 1 and attrs.num_batch == 0 and a_shape.rank() == 2 and b_shape.rank() == 2) {
                const lhs_ax = attrs.lhs_contracting[0];
                const rhs_ax = attrs.rhs_contracting[0];
                if (lhs_ax == 1 and rhs_ax == 0) {
                    // Y = A @ B → dA = dY @ B^T, dB = A^T @ dY
                    const bt = try b.transpose(ins[1], &.{ 1, 0 });
                    const grad_a = try b.matmul(adj_for_dot, bt);
                    try accumulate(b, adjoints, ins[0], grad_a);

                    const at = try b.transpose(ins[0], &.{ 1, 0 });
                    const grad_b = try b.matmul(at, adj_for_dot);
                    try accumulate(b, adjoints, ins[1], grad_b);
                } else if (strict) return error.NoVjpRule;
            } else if (attrs.num_contracting == 1 and attrs.num_batch == 1 and a_shape.rank() == 3 and b_shape.rank() == 3) {
                // 3D batched matmul with batch dim 0.
                // Y[b] = A[b] @ B[b] → dA[b] = dY[b] @ B[b]^T, dB[b] = A[b]^T @ dY[b]
                // Implemented via transpose([0,2,1]) + batched dot_general(lc=2, rc=1).
                const lc = attrs.lhs_contracting[0];
                const rc = attrs.rhs_contracting[0];
                const lb = attrs.lhs_batch[0];
                const rb = attrs.rhs_batch[0];
                if (strict and (lb != 0 or rb != 0 or lc != 2 or rc != 1)) return error.NoVjpRule;

                // Find free dims (the one that's not batch and not contracting).
                const a_free: u8 = for ([_]u8{ 0, 1, 2 }) |d| {
                    if (d != lb and d != lc) break d;
                } else 1;
                const b_free: u8 = for ([_]u8{ 0, 1, 2 }) |d| {
                    if (d != rb and d != rc) break d;
                } else 1;
                _ = a_free;
                _ = b_free;

                // dA = adj @ B^T (contract adj's last dim with B's free dim)
                const bt = try b.transpose(ins[1], &.{ 0, 2, 1 });
                const grad_a_raw = try batchedDotGeneral3D(b, adj, bt);
                // If A's layout isn't [batch, free, contract], transpose back.
                const grad_a = if (lc == 1) try b.transpose(grad_a_raw, &.{ 0, 2, 1 }) else grad_a_raw;
                try accumulate(b, adjoints, ins[0], grad_a);

                // dB = A^T @ adj (contract A's free dim with adj's first non-batch dim)
                const at = try b.transpose(ins[0], &.{ 0, 2, 1 });
                const grad_b_raw = try batchedDotGeneral3D(b, at, adj);
                // If B's layout isn't [batch, free, contract], transpose back.
                const grad_b = if (rc == 1) grad_b_raw else try b.transpose(grad_b_raw, &.{ 0, 2, 1 });
                try accumulate(b, adjoints, ins[1], grad_b);
            } else if (strict) return error.NoVjpRule;
        },

        // ── Data movement ────────────────────────────────────────────

        .gather => {
            if (strict and n.op.gather.axis != 0) return error.NoVjpRule;
            // d/d(table)(gather(table, indices)) = scatter_add(adj, indices)
            const table_shape = g.node(ins[0]).output_shape;
            const grad = try b.graph.addNode(.{
                .op = .{ .scatter_add = .{ .axis = 0 } },
                .output_shape = table_shape,
                .inputs = .{ adj, ins[1], null_node, null_node },
                .num_inputs = 2,
            });
            try accumulate(b, adjoints, ins[0], grad);
            // No gradient through indices (integer-valued).
        },

        .scatter_add => {
            if (strict and n.op.scatter_add.axis != 0) return error.NoVjpRule;
            // d/d(values)(scatter_add(values, indices)) = gather(adj, indices)
            const val_shape = g.node(ins[0]).output_shape;
            const grad = try b.gather(adj, ins[1], val_shape);
            try accumulate(b, adjoints, ins[0], grad);
        },

        // ── Convolution ──────────────────────────────────────────────
        .conv_general => {
            if (strict) return error.NoVjpRule;
            // Convolution gradient is complex; skip for MVP.
            // Training with conv layers needs this implemented.
        },

        // ── Type conversion ──────────────────────────────────────────
        .convert_dtype => {
            // Pass gradient through (type conversion is differentiable
            // in the sense that we just convert the adjoint back).
            const in_dtype = g.node(ins[0]).output_shape.dtype;
            const grad = try b.graph.addNode(.{
                .op = .{ .convert_dtype = .{ .target = in_dtype } },
                .output_shape = g.node(ins[0]).output_shape,
                .inputs = .{ adj, null_node, null_node, null_node },
                .num_inputs = 1,
            });
            try accumulate(b, adjoints, ins[0], grad);
        },

        // ── Fused RoPE (hand-written VJP; no vjp_alternate) ──────────
        .fused_rope => |attrs| {
            // Forward, for each pair i in [0, rope_dim/2):
            //   out[..., i]           = x0 * cos[i] - x1 * sin[i]
            //   out[..., i + D/2]     = x0 * sin[i] + x1 * cos[i]
            // Elements past rope_dim pass through unchanged.
            //
            // This is an orthogonal rotation, so the Jacobian transpose
            // (the VJP) is the rotation by the *inverse* angle, i.e. the
            // same operation with sin → −sin. Concretely:
            //   dL/dx0 = dL/dout_i * cos + dL/dout_{i+D/2} * sin
            //   dL/dx1 = −dL/dout_i * sin + dL/dout_{i+D/2} * cos
            // which is exactly `fused_rope(adj, cos, −sin)`.
            //
            // The passthrough region (head_dim > rope_dim) is handled
            // correctly by the backend: fused_rope leaves those lanes
            // untouched in both forward and backward.
            const input_id = ins[0];
            const cos_id = ins[1];
            const sin_id = ins[2];

            const neg_sin = try b.neg(sin_id);
            const input_shape = g.node(input_id).output_shape;
            const grad_input = try b.graph.addNode(.{
                .op = .{ .fused_rope = attrs },
                .output_shape = input_shape,
                .inputs = .{ adj, cos_id, neg_sin, null_node },
                .num_inputs = 3,
                // Give this backward op its own null vjp_alternate so any
                // downstream second-order pass still treats it as a leaf
                // fused op.
                .vjp_alternate = null_node,
            });
            try accumulate(b, adjoints, input_id, grad_input);
            // cos/sin are treated as frozen position embeddings; no grad.
        },

        // ── Fused disentangled attention (hand-written VJP) ──────────
        // Forward inputs: ins[0]=qkv_packed [3*B*S, H], ins[1]=qr_kr_packed
        // [2*num_rel, H], ins[2]=attn_bias. The backward op recomputes the
        // softmax and emits packed grads [dQ;dK;dV;dQ_r;dK_r] =
        // [3*B*S + 2*num_rel, H]; two row-slices feed the packed-input
        // adjoints, and the upstream concat VJPs split those into the real
        // q/k/v/q_r/k_r gradients.
        .fused_disentangled_attention => |attrs| {
            const bs: i64 = @intCast(@as(usize, attrs.batch) * @as(usize, attrs.seq_len));
            const num_rel: i64 = @intCast(2 * @as(usize, attrs.seq_len) - 1);
            const hh: i64 = @intCast(@as(usize, attrs.num_heads) * @as(usize, attrs.head_dim));
            const dtype = g.node(ins[0]).output_shape.dtype;

            const grad_packed = try b.graph.addNode(.{
                .op = .{ .fused_disentangled_attention_backward = attrs },
                .output_shape = Shape.init(dtype, &.{ 3 * bs + 2 * num_rel, hh }),
                .inputs = .{ ins[0], ins[1], ins[2], adj },
                .num_inputs = 4,
                .vjp_alternate = null_node,
            });

            const d_qkv = try sliceRows(b, grad_packed, 0, 3 * bs, hh, dtype);
            const d_qr_kr = try sliceRows(b, grad_packed, 3 * bs, 3 * bs + 2 * num_rel, hh, dtype);
            try accumulate(b, adjoints, ins[0], d_qkv);
            try accumulate(b, adjoints, ins[1], d_qr_kr);
            // attn_bias (ins[2]) is a frozen padding mask — no gradient.
        },

        .fused_deberta_training_attention_v1 => |attrs| {
            const layout = try attrs.layout();
            if (ins.len != 3) return error.InvalidDebertaTrainingAttentionShape;
            for (ins) |id|
                if (id == null_node or id >= g.nodes.items.len) return error.InvalidGraphDependency;
            if (!g.node(ins[0]).output_shape.eq(layout.qkvShape()) or
                !g.node(ins[1]).output_shape.eq(layout.relativeShape()) or
                !g.node(ins[2]).output_shape.eq(layout.controlShape()) or
                !n.output_shape.eq(layout.outputShape())) return error.InvalidDebertaTrainingAttentionShape;
            const grad_packed = try b.graph.addNode(.{
                .op = .{ .fused_deberta_training_attention_backward_v1 = attrs },
                .output_shape = layout.gradientShape(),
                .inputs = .{ ins[0], ins[1], ins[2], adj },
                .num_inputs = 4,
                .vjp_alternate = null_node,
            });
            const d_qkv = try sliceRows(b, grad_packed, 0, layout.qkv_rows, layout.hidden, .f32);
            const d_relative = try sliceRows(b, grad_packed, layout.qkv_rows, layout.gradient_rows, layout.hidden, .f32);
            try accumulate(b, adjoints, ins[0], d_qkv);
            try accumulate(b, adjoints, ins[1], d_relative);
            // Token validity, bucket indices and RNG counters have no VJP.
        },

        .fused_layer_norm => |attrs| {
            // Only reached when the fused op survived lowering (the
            // `fuse_layer_norm_backward` builder flag was on, so it carries no
            // vjp_alternate). Backward yields gradients for input, gamma, beta.
            const in_shape = g.node(ins[0]).output_shape;
            const dtype = in_shape.dtype;
            const dim: i64 = @intCast(attrs.dim);
            const rank = in_shape.rank();
            var rows: i64 = 1;
            for (0..rank - 1) |ax| rows *= in_shape.dim(@intCast(ax));

            const grad_packed = try b.graph.addNode(.{
                .op = .{ .fused_layer_norm_backward = attrs },
                .output_shape = Shape.init(dtype, &.{ rows + 2, dim }),
                .inputs = .{ ins[0], ins[1], ins[2], adj },
                .num_inputs = 4,
                .vjp_alternate = null_node,
            });

            const d_input_2d = try sliceRows(b, grad_packed, 0, rows, dim, dtype);
            const d_input = if (rank == 2) d_input_2d else try b.reshape(d_input_2d, in_shape);
            const d_gamma_row = try sliceRows(b, grad_packed, rows, rows + 1, dim, dtype);
            const d_gamma = try b.reshape(d_gamma_row, g.node(ins[1]).output_shape);
            const d_beta_row = try sliceRows(b, grad_packed, rows + 1, rows + 2, dim, dtype);
            const d_beta = try b.reshape(d_beta_row, g.node(ins[2]).output_shape);
            try accumulate(b, adjoints, ins[0], d_input);
            try accumulate(b, adjoints, ins[1], d_gamma);
            try accumulate(b, adjoints, ins[2], d_beta);
        },

        .fused_masked_bce_with_logits_loss => |attrs| {
            const grad_logits = try b.graph.addNode(.{
                .op = .{ .fused_masked_bce_with_logits_backward = attrs },
                .output_shape = g.node(ins[0]).output_shape,
                .inputs = .{ ins[0], ins[1], ins[2], adj },
                .num_inputs = 4,
                .vjp_alternate = null_node,
            });
            try accumulate(b, adjoints, ins[0], grad_logits);
        },

        // ── Fused ops should not appear after lowering ───────────────
        else => {
            if (strict) return error.NoVjpRule;
            // Fused ops should have been lowered. If we hit one, it had
            // no vjp_alternate — no gradient can flow through it.
        },
    }
}

/// Slice rows [start, end) of a 2-D [N, cols] tensor (axis-0 slice).
fn sliceRows(b: *Builder, input: NodeId, start: i64, end: i64, cols: i64, dtype: shape_mod.DType) !NodeId {
    var attrs = node_mod.SliceAttrs{};
    attrs.num_axes = 2;
    attrs.starts[0] = start;
    attrs.starts[1] = 0;
    attrs.limits[0] = end;
    attrs.limits[1] = cols;
    attrs.strides[0] = 1;
    attrs.strides[1] = 1;
    return b.graph.addNode(.{
        .op = .{ .slice = attrs },
        .output_shape = Shape.init(dtype, &.{ end - start, cols }),
        .inputs = .{ input, null_node, null_node, null_node },
        .num_inputs = 1,
    });
}

/// Emit a 3D batched matmul: C[b] = A[b] @ B[b] with batch dim 0,
/// contracting A's dim 2 with B's dim 1 (standard layout).
fn batchedDotGeneral3D(b: *Builder, lhs: NodeId, rhs: NodeId) !NodeId {
    const a_shape = b.graph.node(lhs).output_shape;
    const b_shape = b.graph.node(rhs).output_shape;
    const batch_dim = a_shape.dim(0);
    const m = a_shape.dim(1);
    const n_dim = b_shape.dim(2);
    const out_shape = Shape.init(a_shape.dtype, &.{ batch_dim, m, n_dim });
    return b.graph.addNode(.{
        .op = .{ .dot_general = .{
            .lhs_contracting = .{ 2, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_contracting = .{ 1, 0, 0, 0, 0, 0, 0, 0 },
            .lhs_batch = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_batch = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
            .num_contracting = 1,
            .num_batch = 1,
        } },
        .output_shape = out_shape,
        .inputs = .{ lhs, rhs, null_node, null_node },
        .num_inputs = 2,
    });
}

/// Reduce a gradient to match a smaller (broadcast source) shape.
/// When z = op(x, y) and y was broadcast from a smaller shape,
/// d_y must sum along the broadcast dimensions.
fn reduceToBroadcast(b: *Builder, g: *const Graph, grad: NodeId, target_id: NodeId, strict: bool) !NodeId {
    const grad_shape = b.graph.node(grad).output_shape;
    const target_shape = g.node(target_id).output_shape;
    if (strict) {
        if (grad_shape.eq(target_shape)) return grad;
        if (grad_shape.rank() < target_shape.rank()) return error.GradientShapeMismatch;
        const leading = grad_shape.rank() - target_shape.rank();
        var axes: [shape_mod.max_rank]u8 = undefined;
        var count: usize = 0;
        for (0..grad_shape.rank()) |i| {
            const target_dim = if (i < leading) 1 else target_shape.dim(@intCast(i - leading));
            if (target_dim != 1 and target_dim != grad_shape.dim(@intCast(i))) return error.GradientShapeMismatch;
            if (i < leading or (target_dim == 1 and grad_shape.dim(@intCast(i)) != 1)) {
                axes[count] = @intCast(i);
                count += 1;
            }
        }
        const reduced = if (count == 0) grad else try b.reduceSum(grad, axes[0..count]);
        return b.reshape(reduced, target_shape);
    }
    const grad_elems = grad_shape.numElements() orelse 1;
    const target_elems = target_shape.numElements() orelse 1;

    if (grad_elems == target_elems) return grad;

    // Scalar target: reduce all axes.
    if (target_elems == 1) {
        var axes: [shape_mod.max_rank]u8 = undefined;
        for (0..grad_shape.rank()) |i| axes[i] = @intCast(i);
        return b.reduceSum(grad, axes[0..grad_shape.rank()]);
    }

    // 2D → 1D: reduce axis 0 (e.g., [M,N] bias gradient → [N]).
    if (grad_shape.rank() == 2 and target_shape.rank() == 1) {
        const reduced = try b.reduceSum(grad, &.{0}); // shape [1, N]
        return b.reshape(reduced, target_shape);
    }

    // Fallback: reshape (correct when total elements match).
    return b.reshape(grad, target_shape);
}

fn padSliceAdjoint(
    b: *Builder,
    adj: NodeId,
    attrs: node_mod.SliceAttrs,
    input_shape: Shape,
) !NodeId {
    var grad = adj;
    var current_shape = b.graph.node(grad).output_shape;

    for (0..attrs.num_axes) |axis_usize| {
        const axis: u8 = @intCast(axis_usize);
        if (attrs.strides[axis] != 1) return error.NoVjpRule;
        const start = attrs.starts[axis];
        const limit = attrs.limits[axis];
        const input_dim = input_shape.dim(axis);
        if (start < 0 or limit > input_dim or start > limit) return error.ShapeMismatch;

        if (start > 0) {
            var prefix_shape = current_shape;
            prefix_shape.dims[axis] = start;
            const prefix = try zeroConstLike(b, prefix_shape);
            grad = try b.concat(prefix, grad, axis);
            current_shape = b.graph.node(grad).output_shape;
        }

        if (limit < input_dim) {
            var suffix_shape = current_shape;
            suffix_shape.dims[axis] = input_dim - limit;
            const suffix = try zeroConstLike(b, suffix_shape);
            grad = try b.concat(grad, suffix, axis);
            current_shape = b.graph.node(grad).output_shape;
        }
    }

    if (!current_shape.eq(input_shape)) {
        grad = try b.reshape(grad, input_shape);
    }
    return grad;
}

fn sliceConcatAdjoint(
    b: *Builder,
    adj: NodeId,
    target_shape: Shape,
    axis: u8,
    start: i64,
) !NodeId {
    const adj_shape = b.graph.node(adj).output_shape;
    if (axis >= adj_shape.rank() or axis >= target_shape.rank()) return error.ShapeMismatch;

    var attrs = node_mod.SliceAttrs{};
    attrs.num_axes = adj_shape.rank();
    for (0..adj_shape.rank()) |dim_idx| {
        const axis_i: u8 = @intCast(dim_idx);
        attrs.starts[dim_idx] = 0;
        attrs.limits[dim_idx] = adj_shape.dim(axis_i);
        attrs.strides[dim_idx] = 1;
    }
    attrs.starts[axis] = start;
    attrs.limits[axis] = start + target_shape.dim(axis);

    return b.graph.addNode(.{
        .op = .{ .slice = attrs },
        .output_shape = target_shape,
        .inputs = .{ adj, null_node, null_node, null_node },
        .num_inputs = 1,
    });
}

fn zeroConstLike(b: *Builder, shape: Shape) !NodeId {
    const zero = try b.scalarConst(shape.dtype, 0.0);
    return b.graph.addNode(.{
        .op = .{ .broadcast_in_dim = .{
            .target_shape = shape,
            .num_axes = 0,
        } },
        .output_shape = shape,
        .inputs = .{ zero, null_node, null_node, null_node },
        .num_inputs = 1,
    });
}

/// Broadcast a reduced adjoint back to the original input shape.
/// This handles the common case where reduce_sum/reduce_mean collapsed
/// one or more axes.
fn broadcastToShape(
    b: *Builder,
    adj: NodeId,
    adj_shape: Shape,
    target_shape: Shape,
    reduced_axes: []const u8,
) !NodeId {
    // If shapes already match, return as-is.
    if (adj_shape.eq(target_shape)) return adj;

    // Use broadcast_in_dim for shape expansion, mapping adjoint axes onto
    // the unreduced target axes.
    var reduced_mask: [shape_mod.max_rank]bool = .{false} ** shape_mod.max_rank;
    for (reduced_axes) |axis| reduced_mask[axis] = true;

    var broadcast_axes: [shape_mod.max_rank]u8 = undefined;
    var num_axes: u8 = 0;
    for (0..target_shape.rank()) |axis_usize| {
        const axis: u8 = @intCast(axis_usize);
        if (!reduced_mask[axis]) {
            broadcast_axes[num_axes] = axis;
            num_axes += 1;
        }
    }
    if (num_axes != adj_shape.rank()) {
        num_axes = 0;
        const adj_rank = adj_shape.rank();
        const start_axis = target_shape.rank() - adj_rank;
        for (0..adj_rank) |i| {
            broadcast_axes[num_axes] = @intCast(start_axis + i);
            num_axes += 1;
        }
    }

    return b.graph.addNode(.{
        .op = .{ .broadcast_in_dim = .{
            .target_shape = target_shape,
            .broadcast_axes = broadcast_axes,
            .num_axes = num_axes,
        } },
        .output_shape = target_shape,
        .inputs = .{ adj, null_node, null_node, null_node },
        .num_inputs = 1,
    });
}

// ── Tests ──────────────────────────────────────────────────────────────

test "gradient of add" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    const a = try bld.parameter("a", Shape.init(.f32, &.{ 2, 3 }));
    const b_param = try bld.parameter("b", Shape.init(.f32, &.{ 2, 3 }));
    const sum = try bld.add(a, b_param);
    // reduce to scalar loss
    const loss = try bld.reduceSum(sum, &.{ 0, 1 });
    try g.markOutput(loss);

    var result = try gradient(allocator, &g, loss, &.{ a, b_param });
    defer result.deinit();

    // Both gradients should exist
    try std.testing.expect(result.param_grads[0] != null_node);
    try std.testing.expect(result.param_grads[1] != null_node);
}

test "gradient of scalar regression linear mse" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    const features = try bld.parameter("features", Shape.init(.f32, &.{ 1, 3 }));
    const targets = try bld.parameter("targets", Shape.init(.f32, &.{ 1, 1 }));
    const weight = try bld.parameter("weight", Shape.init(.f32, &.{ 1, 3 }));
    const bias = try bld.parameter("bias", Shape.init(.f32, &.{1}));
    const logits = try bld.linear(features, weight, bias, 1, 3, 1);
    const loss = try bld.mseLoss(logits, targets);
    try g.markOutput(loss);

    var result = try gradient(allocator, &g, loss, &.{ weight, bias });
    defer result.deinit();

    try std.testing.expect(result.param_grads[0] != null_node);
    try std.testing.expect(result.param_grads[1] != null_node);
    try std.testing.expectEqual(@as(usize, 2), result.param_grads.len);
}

test "gradient of mul" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    const x = try bld.parameter("x", Shape.init(.f32, &.{ 2, 3 }));
    const w = try bld.parameter("w", Shape.init(.f32, &.{ 2, 3 }));
    const prod = try bld.mul(x, w);
    const loss = try bld.reduceSum(prod, &.{ 0, 1 });
    try g.markOutput(loss);

    var result = try gradient(allocator, &g, loss, &.{ x, w });
    defer result.deinit();

    // dL/dx should involve w, dL/dw should involve x
    try std.testing.expect(result.param_grads[0] != null_node);
    try std.testing.expect(result.param_grads[1] != null_node);
}

test "gradient of fused gelu emits fused backward op" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    const x = try bld.parameter("x", Shape.init(.f32, &.{ 2, 3 }));
    const activated = try bld.gelu(x);
    const loss = try bld.reduceSum(activated, &.{ 0, 1 });
    try g.markOutput(loss);

    var result = try gradient(allocator, &g, loss, &.{x});
    defer result.deinit();

    const grad = result.param_grads[0];
    try std.testing.expect(grad != null_node);
    try std.testing.expectEqual(@as(std.meta.Tag(node_mod.OpCode), .fused_gelu_backward), std.meta.activeTag(result.graph.node(grad).op));
}

test "gradient of fused exact gelu emits exact fused backward op" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    const x = try bld.parameter("x", Shape.init(.f32, &.{ 2, 3 }));
    const activated = try bld.geluExact(x);
    const loss = try bld.reduceSum(activated, &.{ 0, 1 });
    try g.markOutput(loss);

    var result = try gradient(allocator, &g, loss, &.{x});
    defer result.deinit();

    var saw_exact_forward = false;
    var saw_exact_backward = false;
    for (0..result.graph.nodeCount()) |node_index| {
        switch (result.graph.node(@intCast(node_index)).op) {
            .fused_gelu_exact => saw_exact_forward = true,
            .fused_gelu_exact_backward => saw_exact_backward = true,
            else => {},
        }
    }
    try std.testing.expect(saw_exact_forward);
    try std.testing.expect(saw_exact_backward);
    try std.testing.expect(result.param_grads[0] != null_node);
}

test "gradient of 2d slice pads adjoint to input shape" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    const x = try bld.parameter("x", Shape.init(.f32, &.{ 3, 4 }));
    var attrs = node_mod.SliceAttrs{};
    attrs.num_axes = 2;
    attrs.starts[0] = 1;
    attrs.starts[1] = 1;
    attrs.limits[0] = 3;
    attrs.limits[1] = 4;
    attrs.strides[0] = 1;
    attrs.strides[1] = 1;
    const sliced = try g.addNode(.{
        .op = .{ .slice = attrs },
        .output_shape = Shape.init(.f32, &.{ 2, 3 }),
        .inputs = .{ x, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const loss = try bld.reduceSum(sliced, &.{ 0, 1 });
    try g.markOutput(loss);

    var result = try gradient(allocator, &g, loss, &.{x});
    defer result.deinit();

    const grad = result.param_grads[0];
    try std.testing.expect(grad != null_node);
    try std.testing.expect(result.graph.node(grad).output_shape.eq(Shape.init(.f32, &.{ 3, 4 })));
}

test "gradient of concat slices adjoint per input" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    const a = try bld.parameter("a", Shape.init(.f32, &.{ 2, 3 }));
    const c = try bld.parameter("c", Shape.init(.f32, &.{ 2, 2 }));
    const joined = try bld.concat(a, c, 1);
    const loss = try bld.reduceSum(joined, &.{ 0, 1 });
    try g.markOutput(loss);

    var result = try gradient(allocator, &g, loss, &.{ a, c });
    defer result.deinit();

    try std.testing.expect(result.param_grads[0] != null_node);
    try std.testing.expect(result.param_grads[1] != null_node);
    try std.testing.expect(result.graph.node(result.param_grads[0]).output_shape.eq(Shape.init(.f32, &.{ 2, 3 })));
    try std.testing.expect(result.graph.node(result.param_grads[1]).output_shape.eq(Shape.init(.f32, &.{ 2, 2 })));
}

test "gradient of matmul (linear)" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    const x = try bld.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const w = try bld.parameter("w", Shape.init(.f32, &.{ 4, 3 }));
    const y = try bld.matmul(x, w);
    const loss = try bld.reduceSum(y, &.{ 0, 1 });
    try g.markOutput(loss);

    var result = try gradient(allocator, &g, loss, &.{ x, w });
    defer result.deinit();

    // Both should have gradients (matmul VJP)
    try std.testing.expect(result.param_grads[0] != null_node);
    try std.testing.expect(result.param_grads[1] != null_node);
}

test "gradient through fused rmsNorm (lowered)" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    const x = try bld.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const w = try bld.parameter("w", Shape.init(.f32, &.{4}));
    const normed = try bld.rmsNorm(x, w, 4, 1e-5);
    const loss = try bld.reduceSum(normed, &.{ 0, 1 });
    try g.markOutput(loss);

    var result = try gradient(allocator, &g, loss, &.{ x, w });
    defer result.deinit();

    // Gradient should flow through the lowered primitive subgraph.
    // rmsNorm decomposes to: mul, reduceMean, add, rsqrt, mul, mul
    // All have VJP rules, so gradients should reach both params.
    try std.testing.expect(result.param_grads[0] != null_node);
    try std.testing.expect(result.param_grads[1] != null_node);
}

test "gradient through fused gelu (lowered)" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    const x = try bld.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const activated = try bld.gelu(x);
    const loss = try bld.reduceSum(activated, &.{ 0, 1 });
    try g.markOutput(loss);

    var result = try gradient(allocator, &g, loss, &.{x});
    defer result.deinit();

    try std.testing.expect(result.param_grads[0] != null_node);
}

test "gradient through fused masked BCE uses custom backward" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    const logits = try bld.parameter("logits", Shape.init(.f32, &.{ 2, 3 }));
    const labels = try bld.parameter("labels", Shape.init(.f32, &.{ 2, 3 }));
    const mask = try bld.parameter("mask", Shape.init(.f32, &.{ 2, 3 }));
    const loss = try bld.maskedBceWithLogitsLoss(logits, labels, mask, .{
        .positive_weight = 2.0,
        .negative_weight = 0.5,
        .reduction = .mean,
    });
    try g.markOutput(loss);

    var result = try gradient(allocator, &g, loss, &.{logits});
    defer result.deinit();

    try std.testing.expect(result.param_grads[0] != null_node);
    try std.testing.expectEqual(.fused_masked_bce_with_logits_backward, std.meta.activeTag(result.graph.node(result.param_grads[0]).op));
}

test "gradient through fused linear (lowered)" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    const x = try bld.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const w = try bld.parameter("w", Shape.init(.f32, &.{ 3, 4 }));
    const bias = try bld.parameter("bias", Shape.init(.f32, &.{3}));
    const y = try bld.linear(x, w, bias, 2, 4, 3);
    const loss = try bld.reduceSum(y, &.{ 0, 1 });
    try g.markOutput(loss);

    var result = try gradient(allocator, &g, loss, &.{ x, w, bias });
    defer result.deinit();

    // All three params should have gradients via lowered path:
    // linear → transpose(w) + matmul + add(bias)
    try std.testing.expect(result.param_grads[0] != null_node); // dL/dx
    try std.testing.expect(result.param_grads[1] != null_node); // dL/dw
    try std.testing.expect(result.param_grads[2] != null_node); // dL/dbias
}

test "gradient chain: linear -> gelu -> loss" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    const x = try bld.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const w = try bld.parameter("w", Shape.init(.f32, &.{ 3, 4 }));
    const bias = try bld.parameter("bias", Shape.init(.f32, &.{3}));

    const y = try bld.linear(x, w, bias, 2, 4, 3);
    const activated = try bld.gelu(y);
    const loss = try bld.reduceSum(activated, &.{ 0, 1 });
    try g.markOutput(loss);

    var result = try gradient(allocator, &g, loss, &.{ w, bias });
    defer result.deinit();

    // Gradients should flow through gelu → linear → params
    try std.testing.expect(result.param_grads[0] != null_node); // dL/dw
    try std.testing.expect(result.param_grads[1] != null_node); // dL/dbias
}
