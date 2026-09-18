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
    /// None of the requested differentiation targets can reach the loss.
    NoGradientPath,
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

    const forward_count = lowered.graph.nodeCount();
    const param_grads = try appendBackwardPass(allocator, &lowered.graph, lowered_loss, lowered_wrt);

    return .{
        .graph = lowered.graph,
        .param_grads = param_grads,
        .id_map = lowered.id_map,
        .forward_node_count = forward_count,
        .allocator = allocator,
    };
}

/// Append the reverse-mode graph for an already-lowered forward graph.
///
/// Keeping this separate from lowering makes the backward traversal directly
/// testable against valid DAGs whose NodeIds are not topologically ordered.
fn appendBackwardPass(
    allocator: std.mem.Allocator,
    graph: *Graph,
    loss: NodeId,
    wrt: []const NodeId,
) ![]NodeId {
    var b = Builder.init(graph);
    const forward_count = graph.nodeCount();

    // Adjoint map: forward node → accumulated gradient NodeId.
    const adjoints = try allocator.alloc(NodeId, forward_count);
    defer allocator.free(adjoints);
    @memset(adjoints, null_node);

    // Backward work outside paths that reach a requested differentiation
    // target cannot contribute to a requested gradient. Pruning it is also
    // what keeps frozen BF16 base weights out of trainable-weight VJPs.
    const depends_on_wrt = try dependencyMaskFromWrt(allocator, graph, forward_count, wrt);
    defer allocator.free(depends_on_wrt);
    if (wrt.len != 0 and !depends_on_wrt[loss]) {
        return error.NoGradientPath;
    }

    // Seed: dL/d(loss) = 1.0
    const loss_shape = graph.node(loss).output_shape;
    adjoints[loss] = try b.scalarConst(loss_shape.dtype, 1.0);

    const topological_order = try forwardTopologicalOrder(allocator, graph, forward_count);
    defer allocator.free(topological_order);

    // Walk the forward DAG in reverse topological order. Rewrites can create
    // later-ID dependencies of earlier-ID consumers, so reverse NodeId order
    // is not a valid reverse-mode traversal.
    var order_index = topological_order.len;
    while (order_index > 0) {
        order_index -= 1;
        const node_id = topological_order[order_index];
        if (adjoints[node_id] == null_node or !depends_on_wrt[node_id]) continue;

        // IMPORTANT: copy node data before VJP computation, because
        // VJP rules add new nodes to the graph which can reallocate
        // the node array and invalidate pointers into it.
        const n_copy = graph.node(node_id).*;
        const adj = adjoints[node_id];

        try applyVjp(&b, graph, &n_copy, node_id, adj, adjoints, depends_on_wrt, false);
    }

    // A structurally connected loss with no symbolic gradient for any
    // requested target means a VJP stopped propagation. Treat that as an
    // autodiff construction failure rather than executing a forward-only
    // "training" graph. The bounded boundary report makes missing VJP input
    // propagation actionable without dumping the full graph.
    var produced_gradient = false;
    for (wrt) |wrt_node| {
        if (adjoints[wrt_node] != null_node) {
            produced_gradient = true;
            break;
        }
    }
    if (wrt.len != 0 and !produced_gradient) {
        var boundaries_reported: usize = 0;
        for (topological_order) |consumer| {
            if (adjoints[consumer] == null_node or !depends_on_wrt[consumer]) continue;
            const consumer_node = graph.node(consumer);
            for (consumer_node.getInputs()) |input| {
                if (!depends_on_wrt[input] or adjoints[input] != null_node) continue;
                if (boundaries_reported < 16) {
                    std.log.err(
                        "autodiff adjoint stopped at consumer={} op={s} input={} input_op={s}",
                        .{
                            consumer,
                            @tagName(std.meta.activeTag(consumer_node.op)),
                            input,
                            @tagName(std.meta.activeTag(graph.node(input).op)),
                        },
                    );
                }
                boundaries_reported += 1;
            }
        }
        std.log.err("autodiff produced no requested gradients; stopped_boundaries={}", .{boundaries_reported});
        return error.NoGradientProduced;
    }

    // Collect parameter gradients.
    const param_grads = try allocator.alloc(NodeId, wrt.len);
    for (wrt, 0..) |wrt_node, idx| {
        param_grads[idx] = adjoints[wrt_node]; // null_node if no gradient flows
    }
    return param_grads;
}

/// Mark every forward node reachable from a requested differentiation target.
///
/// Lowering and graph rewrites are allowed to produce an edge whose input has
/// a greater NodeId than its consumer, so a single increasing-ID scan is not a
/// valid reachability algorithm. Build the input -> consumer adjacency in CSR
/// form and traverse it from all WRT roots instead. This is O(V + E), visits
/// each node once, and does not rely on NodeId order.
fn dependencyMaskFromWrt(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    forward_count: u32,
    wrt: []const NodeId,
) ![]bool {
    const node_count: usize = forward_count;
    const consumer_counts = try allocator.alloc(usize, node_count);
    defer allocator.free(consumer_counts);
    @memset(consumer_counts, 0);

    var edge_count: usize = 0;
    for (0..node_count) |consumer_index| {
        for (graph.node(@intCast(consumer_index)).getInputs()) |input| {
            if (input >= forward_count) continue;
            consumer_counts[input] = std.math.add(usize, consumer_counts[input], 1) catch
                return error.GraphTooLarge;
            edge_count = std.math.add(usize, edge_count, 1) catch return error.GraphTooLarge;
        }
    }

    const offsets = try allocator.alloc(usize, node_count + 1);
    defer allocator.free(offsets);
    offsets[0] = 0;
    for (consumer_counts, 0..) |count, node_index| {
        offsets[node_index + 1] = std.math.add(usize, offsets[node_index], count) catch
            return error.GraphTooLarge;
    }

    const next_slot = try allocator.dupe(usize, offsets[0..node_count]);
    defer allocator.free(next_slot);
    const consumers = try allocator.alloc(NodeId, edge_count);
    defer allocator.free(consumers);
    for (0..node_count) |consumer_index| {
        for (graph.node(@intCast(consumer_index)).getInputs()) |input| {
            if (input >= forward_count) continue;
            consumers[next_slot[input]] = @intCast(consumer_index);
            next_slot[input] += 1;
        }
    }

    const depends_on_wrt = try allocator.alloc(bool, node_count);
    errdefer allocator.free(depends_on_wrt);
    @memset(depends_on_wrt, false);
    var worklist = std.ArrayListUnmanaged(NodeId).empty;
    defer worklist.deinit(allocator);
    for (wrt) |root| {
        if (root >= forward_count or depends_on_wrt[root]) continue;
        depends_on_wrt[root] = true;
        try worklist.append(allocator, root);
    }

    var head: usize = 0;
    while (head < worklist.items.len) : (head += 1) {
        const dependency = worklist.items[head];
        for (consumers[offsets[dependency]..offsets[dependency + 1]]) |consumer| {
            if (depends_on_wrt[consumer]) continue;
            depends_on_wrt[consumer] = true;
            try worklist.append(allocator, consumer);
        }
    }
    return depends_on_wrt;
}

/// Return an input-before-consumer ordering of the immutable forward graph.
/// Kahn's algorithm makes the ordering independent of NodeId assignment and
/// rejects malformed cyclic forward graphs rather than silently dropping a
/// gradient path. Complexity is O(V + E).
fn forwardTopologicalOrder(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    forward_count: u32,
) ![]NodeId {
    const node_count: usize = forward_count;
    const consumer_counts = try allocator.alloc(usize, node_count);
    defer allocator.free(consumer_counts);
    @memset(consumer_counts, 0);

    const in_degree = try allocator.alloc(usize, node_count);
    defer allocator.free(in_degree);
    @memset(in_degree, 0);

    var edge_count: usize = 0;
    for (0..node_count) |consumer_index| {
        for (graph.node(@intCast(consumer_index)).getInputs()) |input| {
            if (input >= forward_count) return error.InvalidAutodiffGraphInput;
            consumer_counts[input] = std.math.add(usize, consumer_counts[input], 1) catch
                return error.GraphTooLarge;
            in_degree[consumer_index] = std.math.add(usize, in_degree[consumer_index], 1) catch
                return error.GraphTooLarge;
            edge_count = std.math.add(usize, edge_count, 1) catch return error.GraphTooLarge;
        }
    }

    const offsets_len = std.math.add(usize, node_count, 1) catch return error.GraphTooLarge;
    const offsets = try allocator.alloc(usize, offsets_len);
    defer allocator.free(offsets);
    offsets[0] = 0;
    for (consumer_counts, 0..) |count, node_index| {
        offsets[node_index + 1] = std.math.add(usize, offsets[node_index], count) catch
            return error.GraphTooLarge;
    }

    const next_slot = try allocator.dupe(usize, offsets[0..node_count]);
    defer allocator.free(next_slot);
    const consumers = try allocator.alloc(NodeId, edge_count);
    defer allocator.free(consumers);
    for (0..node_count) |consumer_index| {
        for (graph.node(@intCast(consumer_index)).getInputs()) |input| {
            consumers[next_slot[input]] = @intCast(consumer_index);
            next_slot[input] += 1;
        }
    }

    var ready = std.ArrayListUnmanaged(NodeId).empty;
    defer ready.deinit(allocator);
    for (in_degree, 0..) |degree, node_index| {
        if (degree == 0) try ready.append(allocator, @intCast(node_index));
    }

    const order = try allocator.alloc(NodeId, node_count);
    errdefer allocator.free(order);
    var emitted: usize = 0;
    var ready_head: usize = 0;
    while (ready_head < ready.items.len) : (ready_head += 1) {
        const dependency = ready.items[ready_head];
        order[emitted] = dependency;
        emitted += 1;

        for (consumers[offsets[dependency]..offsets[dependency + 1]]) |consumer| {
            std.debug.assert(in_degree[consumer] > 0);
            in_degree[consumer] -= 1;
            if (in_degree[consumer] == 0) try ready.append(allocator, consumer);
        }
    }

    if (emitted != node_count) return error.AutodiffGraphCycle;
    return order;
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
    const lowered_wrt = try allocator.alloc(NodeId, wrt.len);
    defer allocator.free(lowered_wrt);
    for (wrt, lowered_wrt) |id, *mapped| mapped.* = lowered.id_map[id];
    const needed = try dependencyMaskFromWrt(allocator, &lowered.graph, forward_count, lowered_wrt);
    defer allocator.free(needed);

    const lowered_cotangents = try allocator.alloc(NodeId, seeds.len);
    defer allocator.free(lowered_cotangents);
    for (seeds, lowered_cotangents) |seed, *mapped_cotangent| {
        const output = lowered.id_map[seed.output];
        const cotangent = lowered.id_map[seed.cotangent];
        if (output == null_node or cotangent == null_node) return error.InvalidGradientSeed;
        mapped_cotangent.* = cotangent;
    }
    const seed_dependent = try dependencyMaskFromWrt(allocator, &lowered.graph, forward_count, lowered_cotangents);
    defer allocator.free(seed_dependent);
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

    const topological_order = try forwardTopologicalOrder(allocator, &lowered.graph, forward_count);
    defer allocator.free(topological_order);
    var order_index = topological_order.len;
    while (order_index != 0) {
        order_index -= 1;
        const node_id = topological_order[order_index];
        if (adjoints[node_id] == null_node or !needed[node_id]) continue;
        const node_copy = lowered.graph.node(node_id).*;
        try applyVjp(&builder, &lowered.graph, &node_copy, node_id, adjoints[node_id], adjoints, needed, true);
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
    depends_on_wrt: []const bool,
    strict: bool,
) !void {
    const ins = n.getInputs();
    switch (n.op) {
        // ── No gradient ──────────────────────────────────────────────
        .parameter, .constant, .stop_gradient => {},
        .frozen_span_features_v1 => |attrs| {
            if (n.num_inputs != 2) return error.InvalidFrozenSpanFeaturesShape;
            try attrs.validate(n.output_shape, b.graph.node(ins[0]).output_shape, b.graph.node(ins[1]).output_shape);
            // Selected integer span geometry and mask counts are detached by
            // contract, even when a caller asks for their gradients explicitly.
        },

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

        .fused_prefix_scan_v1 => |attrs| {
            const shape = try attrs.shape();
            if (n.num_inputs != 1 or !n.output_shape.eq(shape) or
                !b.graph.node(ins[0]).output_shape.eq(shape) or !b.graph.node(adj).output_shape.eq(shape)) return error.InvalidPrefixScanShape;
            var reverse = attrs;
            reverse.reverse = !attrs.reverse;
            const grad = if (attrs.width == 1) adj else try b.prefixScanV1(adj, reverse);
            try accumulate(b, adjoints, ins[0], grad);
        },

        .fused_silu => {
            // Only explicitly retained SiLU reaches this rule. Ordinary
            // builder graphs still lower through their primitive alternate.
            if (n.num_inputs != 1 or n.output_shape.dtype != .f32 or
                !b.graph.node(ins[0]).output_shape.eq(n.output_shape) or
                !b.graph.node(adj).output_shape.eq(n.output_shape)) return error.InvalidSiluShape;
            const grad = try b.graph.addNode(.{
                .op = .{ .fused_silu_backward = {} },
                .output_shape = n.output_shape,
                .inputs = .{ ins[0], adj, null_node, null_node },
                .num_inputs = 2,
            });
            try accumulate(b, adjoints, ins[0], grad);
        },

        .fused_sigmoid => {
            if (n.num_inputs != 1 or n.output_shape.dtype != .f32 or
                !b.graph.node(ins[0]).output_shape.eq(n.output_shape) or
                !b.graph.node(adj).output_shape.eq(n.output_shape)) return error.InvalidSigmoidShape;
            const grad = try b.graph.addNode(.{
                .op = .{ .fused_sigmoid_backward = {} },
                .output_shape = n.output_shape,
                .inputs = .{ node_id, adj, null_node, null_node },
                .num_inputs = 2,
            });
            try accumulate(b, adjoints, ins[0], grad);
        },

        .fused_softmax => |attrs| {
            // For y = softmax(x), dL/dx = y * (dL/dy - sum(dL/dy * y)).
            // Keeping this fused avoids routing attention gradients through
            // the reduce_max stabilization subgraph used by the forward
            // decomposition.
            const y = node_id;
            const rank = n.output_shape.rank();
            if (attrs.fuse_backward and (n.num_inputs != 1 or rank == 0 or attrs.dim == 0 or
                attrs.dim != n.output_shape.dim(rank - 1) or n.output_shape.dtype != .f32 or
                !b.graph.node(ins[0]).output_shape.eq(n.output_shape))) return error.InvalidSoftmaxShape;
            const last_axis: u8 = @intCast(rank - 1);
            const adj_times_y = try b.mul(adj, y);
            if (attrs.fuse_backward) {
                const grad = try b.graph.addNode(.{
                    .op = .{ .fused_softmax_backward = attrs },
                    .output_shape = n.output_shape,
                    .inputs = .{ adj_times_y, y, null_node, null_node },
                    .num_inputs = 2,
                });
                try accumulate(b, adjoints, ins[0], grad);
            } else {
                const dot = try b.reduceSum(adj_times_y, &.{last_axis});
                const dot_shape = b.graph.node(dot).output_shape;
                const dot_bc = try broadcastToShape(b, dot, dot_shape, n.output_shape, &.{last_axis});
                const centered = try b.sub(adj, dot_bc);
                try accumulate(b, adjoints, ins[0], try b.mul(y, centered));
            }
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
            var vjp_geometry_supported = false;

            if (attrs.num_contracting == 1 and attrs.num_batch == 0 and a_shape.rank() == 2 and b_shape.rank() == 2) {
                const lhs_ax = attrs.lhs_contracting[0];
                const rhs_ax = attrs.rhs_contracting[0];
                if (lhs_ax == 1 and rhs_ax == 0 and !attrs.retain_backward_storage) {
                    vjp_geometry_supported = true;
                    // Y = A @ B → dA = dY @ B^T, dB = A^T @ dY
                    const bt = try b.transpose(ins[1], &.{ 1, 0 });
                    const grad_a = try b.matmul(adj_for_dot, bt);
                    try accumulate(b, adjoints, ins[0], grad_a);

                    const at = try b.transpose(ins[0], &.{ 1, 0 });
                    const grad_b = try b.matmul(at, adj_for_dot);
                    try accumulate(b, adjoints, ins[1], grad_b);
                } else if (lhs_ax == 1 and rhs_ax == 1) {
                    vjp_geometry_supported = true;
                    // Y = A @ B^T with B stored physically as [out, in].
                    // Preserve that layout in both VJPs so large frozen
                    // weights never need a materialized transpose.
                    if (depends_on_wrt[ins[0]]) {
                        const grad_a = try dotGeneral2DDirect(b, adj_for_dot, ins[1], 1, 0);
                        try accumulate(b, adjoints, ins[0], grad_a);
                    }
                    if (depends_on_wrt[ins[1]]) {
                        const grad_b = try dotGeneral2DDirect(b, adj_for_dot, ins[0], 0, 0);
                        try accumulate(b, adjoints, ins[1], grad_b);
                    }
                } else if (lhs_ax <= 1 and rhs_ax <= 1) {
                    vjp_geometry_supported = true;
                    // Retained forward storage must stay in its physical
                    // layout. Form each derivative directly in that layout
                    // rather than materializing a transpose.
                    const grad_a = if (lhs_ax == 1)
                        try b.matmul2DLayout(adj_for_dot, ins[1], false, rhs_ax == 0)
                    else
                        try b.matmul2DLayout(ins[1], adj_for_dot, rhs_ax == 1, true);
                    const grad_b = if (rhs_ax == 0)
                        try b.matmul2DLayout(ins[0], adj_for_dot, lhs_ax == 1, false)
                    else
                        try b.matmul2DLayout(adj_for_dot, ins[0], true, lhs_ax == 0);
                    try accumulate(b, adjoints, ins[0], grad_a);
                    try accumulate(b, adjoints, ins[1], grad_b);
                } else if (strict) return error.NoVjpRule;
            } else if (attrs.num_contracting == 1 and attrs.num_batch == 1 and a_shape.rank() == 3 and b_shape.rank() == 3) {
                vjp_geometry_supported = true;
                // 3D batched matmul with batch dim 0.
                // Y[b] = A[b] @ B[b] → dA[b] = dY[b] @ B[b]^T, dB[b] = A[b]^T @ dY[b]
                // Implemented via transpose([0,2,1]) + batched dot_general(lc=2, rc=1).
                const lc = attrs.lhs_contracting[0];
                const rc = attrs.rhs_contracting[0];
                const lb = attrs.lhs_batch[0];
                const rb = attrs.rhs_batch[0];
                if (lb != 0 or rb != 0 or (lc != 1 and lc != 2) or (rc != 1 and rc != 2)) return error.NoVjpRule;

                if (attrs.retain_backward_storage) {
                    // Retain both contraction operands. Match the reference's
                    // dA product before restoring a transposed A layout:
                    // reversing the product to avoid that output copy changes
                    // FP32 reduction order even with the same BLAS library.
                    const grad_a_raw = try b.matmul3DLayout(adj_for_dot, ins[1], false, rc == 1);
                    const grad_a = if (lc == 1) try b.transpose(grad_a_raw, &.{ 0, 2, 1 }) else grad_a_raw;
                    const grad_b = if (rc == 1)
                        try b.matmul3DLayout(ins[0], adj_for_dot, lc == 2, false)
                    else
                        try b.matmul3DLayout(adj_for_dot, ins[0], true, lc == 1);
                    try accumulate(b, adjoints, ins[0], grad_a);
                    try accumulate(b, adjoints, ins[1], grad_b);
                    return;
                }

                // dA = adj @ B^T (contract adj's last dim with B's free dim)
                const bt = if (rc == 1) try b.transpose(ins[1], &.{ 0, 2, 1 }) else ins[1];
                const grad_a_raw = try batchedDotGeneral3D(b, adj_for_dot, bt);
                // If A's layout isn't [batch, free, contract], transpose back.
                const grad_a = if (lc == 1) try b.transpose(grad_a_raw, &.{ 0, 2, 1 }) else grad_a_raw;
                try accumulate(b, adjoints, ins[0], grad_a);

                // dB = A^T @ adj (contract A's free dim with adj's first non-batch dim)
                // For a retained transposed RHS, form dB = adj^T @ A
                // directly in its storage layout.
                const grad_b = if (rc == 1) blk: {
                    const at = if (lc == 2) try b.transpose(ins[0], &.{ 0, 2, 1 }) else ins[0];
                    break :blk try batchedDotGeneral3D(b, at, adj_for_dot);
                } else blk: {
                    const a = if (lc == 2) ins[0] else try b.transpose(ins[0], &.{ 0, 2, 1 });
                    const adj_t = try b.transpose(adj_for_dot, &.{ 0, 2, 1 });
                    break :blk try batchedDotGeneral3D(b, adj_t, a);
                };
                try accumulate(b, adjoints, ins[1], grad_b);
            } else if (attrs.num_contracting == 1 and attrs.num_batch == 2 and a_shape.rank() == 4 and b_shape.rank() == 4 and
                attrs.lhs_batch[0] == 0 and attrs.lhs_batch[1] == 1 and
                attrs.rhs_batch[0] == 0 and attrs.rhs_batch[1] == 1)
            {
                // Gemma attention keeps batch and head as separate leading
                // batch axes. Choose operand order and contracting axes so
                // both VJPs directly match the physical input layouts:
                //
                //   scores: [B,H,Q,D] @ [B,H,K,D] -> [B,H,Q,K]
                //   values: [B,H,Q,K] @ [B,H,K,D] -> [B,H,Q,D]
                const lc = attrs.lhs_contracting[0];
                const rc = attrs.rhs_contracting[0];
                if ((lc == 2 or lc == 3) and (rc == 2 or rc == 3)) {
                    vjp_geometry_supported = true;
                    const a_free: u8 = if (lc == 2) 3 else 2;
                    const b_free: u8 = if (rc == 2) 3 else 2;
                    if (depends_on_wrt[ins[0]]) {
                        const grad_a = if (lc == 3)
                            try batchedDotGeneralDirect(b, adj_for_dot, ins[1], 2, 3, b_free)
                        else
                            try batchedDotGeneralDirect(b, ins[1], adj_for_dot, 2, b_free, 3);
                        try accumulate(b, adjoints, ins[0], grad_a);
                    }
                    if (depends_on_wrt[ins[1]]) {
                        const grad_b = if (rc == 2)
                            try batchedDotGeneralDirect(b, ins[0], adj_for_dot, 2, a_free, 2)
                        else
                            try batchedDotGeneralDirect(b, adj_for_dot, ins[0], 2, 2, a_free);
                        try accumulate(b, adjoints, ins[1], grad_b);
                    }
                }
            }
            if (!vjp_geometry_supported and (strict or depends_on_wrt[ins[0]] or depends_on_wrt[ins[1]])) {
                return error.NoVjpRule;
            }
        },

        // ── Data movement ────────────────────────────────────────────

        .gather => |attrs| {
            if (strict and n.op.gather.axis != 0) return error.NoVjpRule;
            // d/d(table)(gather(table, indices)) = scatter_add(adj, indices)
            const table_shape = g.node(ins[0]).output_shape;
            const grad = try b.graph.addNode(.{
                .op = .{ .scatter_add = .{ .axis = 0, .reduction = attrs.backward_reduction, .padding_index = attrs.backward_padding_index } },
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

        .fused_boundary_training_attention_v1 => |attrs| {
            const layout = try attrs.layout();
            if (ins.len != 2 or !g.node(ins[0]).output_shape.eq(layout.qkvShape()) or
                !g.node(ins[1]).output_shape.eq(attrs.maskShape()) or !n.output_shape.eq(layout.savedShape()) or
                !g.node(adj).output_shape.eq(layout.savedShape())) return error.InvalidBoundaryTrainingAttentionShape;
            const grad_qkv = try b.graph.addNode(.{
                .op = .{ .fused_boundary_training_attention_backward_v1 = attrs },
                .output_shape = layout.qkvShape(),
                .inputs = .{ ins[0], ins[1], node_id, adj },
                .num_inputs = 4,
            });
            try accumulate(b, adjoints, ins[0], grad_qkv);
            // Binary routing mask and saved normalization have no cotangent.
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

        .fused_linear => |attrs| {
            // Preserve fused forward rounding while reusing the ordinary
            // matrix and reduction VJPs. Only explicitly retained FP32
            // linear nodes reach this rule; normal lowering is unchanged.
            if (ins.len != 3 or attrs.rows == 0 or attrs.in_dim == 0 or attrs.out_dim == 0)
                return error.InvalidLinearShape;
            const input_shape = g.node(ins[0]).output_shape;
            const weight_shape = Shape.init(.f32, &.{ attrs.out_dim, attrs.in_dim });
            const bias_shape = Shape.init(.f32, &.{attrs.out_dim});
            const input_elements = std.math.mul(i64, attrs.rows, attrs.in_dim) catch return error.InvalidLinearShape;
            if (input_shape.dtype != .f32 or input_shape.numElements() != input_elements or
                !g.node(ins[1]).output_shape.eq(weight_shape) or !g.node(ins[2]).output_shape.eq(bias_shape) or
                !n.output_shape.eq(Shape.init(.f32, &.{ attrs.rows, attrs.out_dim })))
                return error.InvalidLinearShape;
            const matrix_shape = Shape.init(.f32, &.{ attrs.rows, attrs.in_dim });
            const input_matrix = if (input_shape.eq(matrix_shape)) ins[0] else try b.reshape(ins[0], matrix_shape);
            const dx_matrix = try b.matmul(adj, ins[1]);
            const dx = if (input_shape.eq(matrix_shape)) dx_matrix else try b.reshape(dx_matrix, input_shape);
            const dw = if (attrs.retain_backward_storage)
                try b.matmul2DLayout(adj, input_matrix, true, false)
            else
                try b.matmul(try b.transpose(adj, &.{ 1, 0 }), input_matrix);
            const db = try b.reshape(try b.reduceSum(adj, &.{0}), bias_shape);
            try accumulate(b, adjoints, ins[0], dx);
            try accumulate(b, adjoints, ins[1], dw);
            try accumulate(b, adjoints, ins[2], db);
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

        .fused_selected_tied_head_logits => |attrs| {
            if (!attrs.frozen_weight or depends_on_wrt[ins[1]]) {
                return error.SelectedTiedHeadWeightGradientUnsupported;
            }
            if (depends_on_wrt[ins[0]]) {
                const grad_hidden = try b.graph.addNode(.{
                    .op = .{ .fused_selected_tied_head_backward = attrs },
                    .output_shape = g.node(ins[0]).output_shape,
                    .inputs = .{ ins[1], ins[2], adj, null_node },
                    .num_inputs = 3,
                    .vjp_alternate = null_node,
                });
                try accumulate(b, adjoints, ins[0], grad_hidden);
            }
            // Token ids are discrete and the tied head is frozen.
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

/// Emit a rank-2 contraction without materializing either operand transpose.
/// Dot-general orders the result as [lhs_free, rhs_free], so choosing the
/// contracting axes is sufficient to express transpose-left/right GEMMs.
fn dotGeneral2DDirect(
    b: *Builder,
    lhs: NodeId,
    rhs: NodeId,
    lhs_contracting: u8,
    rhs_contracting: u8,
) !NodeId {
    const lhs_shape = b.graph.node(lhs).output_shape;
    const rhs_shape = b.graph.node(rhs).output_shape;
    if (lhs_shape.rank() != 2 or rhs_shape.rank() != 2) return error.InvalidDotGeneralShape;
    if (lhs_contracting > 1 or rhs_contracting > 1) return error.InvalidDotGeneralShape;
    if (lhs_shape.dim(lhs_contracting) != rhs_shape.dim(rhs_contracting)) return error.InvalidDotGeneralShape;
    const lhs_free: u8 = 1 - lhs_contracting;
    const rhs_free: u8 = 1 - rhs_contracting;
    return b.graph.addNode(.{
        .op = .{ .dot_general = .{
            .lhs_contracting = .{ lhs_contracting, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_contracting = .{ rhs_contracting, 0, 0, 0, 0, 0, 0, 0 },
            .num_contracting = 1,
            .num_batch = 0,
        } },
        .output_shape = Shape.init(lhs_shape.dtype, &.{ lhs_shape.dim(lhs_free), rhs_shape.dim(rhs_free) }),
        .inputs = .{ lhs, rhs, null_node, null_node },
        .num_inputs = 2,
    });
}

/// Emit a rank-3/rank-4 batched contraction whose leading axes are batch
/// dimensions and whose final two axes form the matrices. Dot-general emits
/// [batch..., lhs_free, rhs_free], allowing VJPs to preserve input layouts
/// without materialized transposes.
fn batchedDotGeneralDirect(
    b: *Builder,
    lhs: NodeId,
    rhs: NodeId,
    num_batch: u8,
    lhs_contracting: u8,
    rhs_contracting: u8,
) !NodeId {
    const lhs_shape = b.graph.node(lhs).output_shape;
    const rhs_shape = b.graph.node(rhs).output_shape;
    const rank = lhs_shape.rank();
    if (rank != rhs_shape.rank() or rank != @as(usize, num_batch) + 2) return error.InvalidDotGeneralShape;
    const matrix_axis0: u8 = num_batch;
    const matrix_axis1: u8 = num_batch + 1;
    if ((lhs_contracting != matrix_axis0 and lhs_contracting != matrix_axis1) or
        (rhs_contracting != matrix_axis0 and rhs_contracting != matrix_axis1))
    {
        return error.InvalidDotGeneralShape;
    }
    const lhs_free = if (lhs_contracting == matrix_axis0) matrix_axis1 else matrix_axis0;
    const rhs_free = if (rhs_contracting == matrix_axis0) matrix_axis1 else matrix_axis0;
    if (lhs_shape.dim(lhs_contracting) != rhs_shape.dim(rhs_contracting)) return error.InvalidDotGeneralShape;

    var out_dims: [shape_mod.max_rank]i64 = undefined;
    var lhs_batch = [_]u8{0} ** shape_mod.max_rank;
    var rhs_batch = [_]u8{0} ** shape_mod.max_rank;
    for (0..num_batch) |axis| {
        const batch_axis: u8 = @intCast(axis);
        if (lhs_shape.dim(batch_axis) != rhs_shape.dim(batch_axis)) return error.InvalidDotGeneralShape;
        out_dims[axis] = lhs_shape.dim(batch_axis);
        lhs_batch[axis] = batch_axis;
        rhs_batch[axis] = batch_axis;
    }
    out_dims[num_batch] = lhs_shape.dim(lhs_free);
    out_dims[@as(usize, num_batch) + 1] = rhs_shape.dim(rhs_free);
    return b.graph.addNode(.{
        .op = .{ .dot_general = .{
            .lhs_contracting = .{ lhs_contracting, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_contracting = .{ rhs_contracting, 0, 0, 0, 0, 0, 0, 0 },
            .lhs_batch = lhs_batch,
            .rhs_batch = rhs_batch,
            .num_contracting = 1,
            .num_batch = num_batch,
        } },
        .output_shape = Shape.init(lhs_shape.dtype, out_dims[0..rank]),
        .inputs = .{ lhs, rhs, null_node, null_node },
        .num_inputs = 2,
    });
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
    return b.matmul3D(lhs, rhs);
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

test "WRT dependency propagation handles non-topological lowered node IDs" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    const wrt = try bld.parameter("wrt", Shape.scalar(.f32)); // node 0
    const earlier_consumer = try g.addNode(.{ // node 1 consumes future node 2
        .op = .{ .neg = {} },
        .output_shape = Shape.scalar(.f32),
        .inputs = .{ 2, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const later_bridge = try g.addNode(.{ // node 2 depends on WRT node 0
        .op = .{ .neg = {} },
        .output_shape = Shape.scalar(.f32),
        .inputs = .{ wrt, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const unrelated = try bld.parameter("unrelated", Shape.scalar(.f32));

    const depends = try dependencyMaskFromWrt(allocator, &g, g.nodeCount(), &.{wrt});
    defer allocator.free(depends);

    try std.testing.expect(depends[wrt]);
    try std.testing.expect(depends[later_bridge]);
    try std.testing.expect(depends[earlier_consumer]);
    try std.testing.expect(!depends[unrelated]);
}

test "backward pass propagates through non-topological lowered node IDs" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    const wrt = try bld.parameter("wrt", Shape.scalar(.f32)); // node 0
    const loss = try g.addNode(.{ // node 1 consumes future node 2
        .op = .{ .neg = {} },
        .output_shape = Shape.scalar(.f32),
        .inputs = .{ 2, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    _ = try g.addNode(.{ // node 2 bridges WRT node 0 to loss node 1
        .op = .{ .neg = {} },
        .output_shape = Shape.scalar(.f32),
        .inputs = .{ wrt, null_node, null_node, null_node },
        .num_inputs = 1,
    });

    const param_grads = try appendBackwardPass(allocator, &g, loss, &.{wrt});
    defer allocator.free(param_grads);

    // d(-(-wrt))/d(wrt) exists. A reverse NodeId walk visits bridge node 2
    // before loss node 1 creates its adjoint and incorrectly returns null.
    try std.testing.expectEqual(@as(usize, 1), param_grads.len);
    try std.testing.expect(param_grads[0] != null_node);
}

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

test "rank4 two-batch dot VJP emits Gemma attention gradients" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const q_shape = Shape.init(.f32, &.{ 1, 2, 3, 4 });
    const k_shape = Shape.init(.f32, &.{ 1, 2, 5, 4 });
    const q = try b.parameter("q", q_shape);
    const k = try b.parameter("k", k_shape);
    const scores = try g.addNode(.{
        .op = .{ .dot_general = .{
            .lhs_contracting = .{ 3, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_contracting = .{ 3, 0, 0, 0, 0, 0, 0, 0 },
            .lhs_batch = .{ 0, 1, 0, 0, 0, 0, 0, 0 },
            .rhs_batch = .{ 0, 1, 0, 0, 0, 0, 0, 0 },
            .num_contracting = 1,
            .num_batch = 2,
        } },
        .output_shape = Shape.init(.f32, &.{ 1, 2, 3, 5 }),
        .inputs = .{ q, k, null_node, null_node },
        .num_inputs = 2,
    });
    const loss = try b.reduceSum(scores, &.{ 0, 1, 2, 3 });
    try g.markOutput(loss);

    var result = try gradient(allocator, &g, loss, &.{ q, k });
    defer result.deinit();
    try std.testing.expect(result.param_grads[0] != null_node);
    try std.testing.expect(result.param_grads[1] != null_node);
    try std.testing.expect(result.graph.node(result.param_grads[0]).output_shape.eq(q_shape));
    try std.testing.expect(result.graph.node(result.param_grads[1]).output_shape.eq(k_shape));
    try std.testing.expectEqual(@as(u8, 2), result.graph.node(result.param_grads[0]).op.dot_general.num_batch);
    try std.testing.expectEqual(@as(u8, 2), result.graph.node(result.param_grads[1]).op.dot_general.num_batch);
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

test "gradient through selected tied head emits d_hidden only" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    const hidden = try bld.parameter("hidden", Shape.init(.f32, &.{ 1, 4 }));
    const weight = try bld.parameter("weight", Shape.init(.f32, &.{ 5, 4 }));
    const token_ids = try bld.parameter("token_ids", Shape.init(.f32, &.{2}));
    const logits = try bld.selectedTiedHeadLogits(hidden, weight, token_ids, .{
        .in_dim = 4,
        .vocab_size = 5,
        .frozen_weight = true,
    });
    const chosen = try bld.reshape(try bld.sliceLastDim(logits, 0, 1), Shape.scalar(.f32));
    const rejected = try bld.reshape(try bld.sliceLastDim(logits, 1, 2), Shape.scalar(.f32));
    const loss = try bld.sub(chosen, rejected);
    try g.markOutput(loss);

    var result = try gradient(allocator, &g, loss, &.{hidden});
    defer result.deinit();
    const grad_node = result.graph.node(result.param_grads[0]);
    try std.testing.expectEqual(.fused_selected_tied_head_backward, std.meta.activeTag(grad_node.op));
    try std.testing.expect(grad_node.output_shape.eq(Shape.init(.f32, &.{ 1, 4 })));
    try std.testing.expectEqual(@as(u8, 3), grad_node.num_inputs);

    try std.testing.expectError(
        error.SelectedTiedHeadWeightGradientUnsupported,
        gradient(allocator, &g, loss, &.{weight}),
    );
}

test "retained FP32 fused linear has strict VJPs for matrix and flattened inputs" {
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |retain| {
        for ([_]Shape{ Shape.init(.f32, &.{ 2, 3 }), Shape.init(.f32, &.{6}) }) |input_shape| {
            var graph = Graph.init(a);
            defer graph.deinit();
            var builder = Builder.init(&graph);
            const x = try builder.parameter("x", input_shape);
            const w = try builder.parameter("w", Shape.init(.f32, &.{ 4, 3 }));
            const bias = try builder.parameter("bias", Shape.init(.f32, &.{4}));
            const seed = try builder.parameter("seed", Shape.init(.f32, &.{ 2, 4 }));
            const y = try builder.linear(x, w, bias, 2, 3, 4);
            graph.nodeMut(y).vjp_alternate = null_node;
            graph.nodeMut(y).op.fused_linear.retain_backward_storage = retain;
            var result = try gradientWithSeeds(a, &graph, &.{.{ .output = y, .cotangent = seed }}, &.{ x, w, bias }, .{});
            defer result.deinit();
            try std.testing.expect(result.graph.node(result.id_map[y]).op == .fused_linear);
            for ([_]NodeId{ x, w, bias }, result.param_grads) |id, grad| {
                try std.testing.expect(grad != null_node);
                try std.testing.expect(graph.node(id).output_shape.eq(result.graph.node(grad).output_shape));
            }
            graph.nodeMut(y).op.fused_linear.rows = 3;
            try std.testing.expectError(error.InvalidLinearShape, gradientWithSeeds(a, &graph, &.{.{ .output = y, .cotangent = seed }}, &.{ x, w, bias }, .{}));
        }
    }
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

test "linear input gradient keeps frozen weight in resident layout" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    const x = try bld.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const w = try bld.parameter("w", Shape.init(.f32, &.{ 3, 4 }));
    const y = try bld.linearNoBias(x, w, 2, 4, 3);
    const loss = try bld.reduceSum(y, &.{ 0, 1 });
    try g.markOutput(loss);

    var result = try gradient(allocator, &g, loss, &.{x});
    defer result.deinit();
    const grad_x = result.graph.node(result.param_grads[0]);
    try std.testing.expectEqual(.dot_general, std.meta.activeTag(grad_x.op));
    try std.testing.expectEqual(@as(u8, 1), grad_x.op.dot_general.lhs_contracting[0]);
    try std.testing.expectEqual(@as(u8, 0), grad_x.op.dot_general.rhs_contracting[0]);
    try std.testing.expect(grad_x.output_shape.eq(Shape.init(.f32, &.{ 2, 4 })));
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

test "softmax backward fusion is explicit and preserves seeded gradient shape" {
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |fused| {
        var graph = Graph.init(a);
        defer graph.deinit();
        var builder = Builder.init(&graph);
        const shape = Shape.init(.f32, &.{ 2, 7 });
        const x = try builder.parameter("x", shape);
        const seed = try builder.parameter("seed", shape);
        const y = try builder.softmax(x);
        graph.nodeMut(y).op.fused_softmax.fuse_backward = fused;
        var result = try gradientWithSeeds(a, &graph, &.{.{ .output = y, .cotangent = seed }}, &.{x}, .{});
        defer result.deinit();
        const grad = result.graph.node(result.param_grads[0]);
        try std.testing.expect(grad.output_shape.eq(shape));
        try std.testing.expectEqual(fused, grad.op == .fused_softmax_backward);
        if (fused) {
            try std.testing.expectEqual(@as(u8, 2), grad.num_inputs);
            try std.testing.expect(result.graph.node(grad.inputs[0]).op == .mul);
            try std.testing.expectEqual(result.id_map[y], grad.inputs[1]);
        }
        if (fused) {
            graph.nodeMut(y).op.fused_softmax.dim = 8;
            try std.testing.expectError(error.InvalidSoftmaxShape, gradientWithSeeds(a, &graph, &.{.{ .output = y, .cotangent = seed }}, &.{x}, .{}));
        }
    }
}

test "boundary attention strict VJP retains forward state and freezes mask" {
    const a = std.testing.allocator;
    var graph = Graph.init(a);
    defer graph.deinit();
    var b = Builder.init(&graph);
    const attrs = node_mod.BoundaryTrainingAttentionAttrs{ .batch = 2, .seq_len = 65, .num_heads = 4, .window = 3 };
    const layout = try attrs.layout();
    const qkv = try b.parameter("qkv", layout.qkvShape());
    const mask = try b.parameter("mask", attrs.maskShape());
    const seed = try b.parameter("seed", layout.attendedShape());
    const saved = try b.boundaryTrainingAttentionV1(qkv, mask, attrs);
    const output = try b.reshape(try b.sliceLastDim(saved, 0, layout.output_elements), layout.attendedShape());
    var ad = try gradientWithSeeds(a, &graph, &.{.{ .output = output, .cotangent = seed }}, &.{ qkv, mask }, .{});
    defer ad.deinit();
    try std.testing.expectEqual(null_node, ad.param_grads[1]);
    const grad = ad.graph.node(ad.param_grads[0]);
    try std.testing.expectEqual(.fused_boundary_training_attention_backward_v1, std.meta.activeTag(grad.op));
    try std.testing.expectEqualDeep(attrs, grad.op.fused_boundary_training_attention_backward_v1);
    try std.testing.expectEqual(ad.id_map[saved], grad.inputs[2]);
    try std.testing.expect(grad.output_shape.eq(layout.qkvShape()));
    try std.testing.expect(ad.graph.node(grad.inputs[3]).output_shape.eq(layout.savedShape()));
    var forwards: usize = 0;
    for (ad.graph.nodes.items) |node| if (node.op == .fused_boundary_training_attention_v1) {
        forwards += 1;
    };
    try std.testing.expectEqual(@as(usize, 1), forwards);
    try std.testing.expectError(error.InvalidBoundaryTrainingAttentionShape, b.boundaryTrainingAttentionV1(qkv, seed, attrs));
}

test "retained sigmoid strict VJP uses saved output and seed" {
    const a = std.testing.allocator;
    var g = Graph.init(a);
    defer g.deinit();
    var b = Builder.init(&g);
    const shape = Shape.init(.f32, &.{ 2, 3 });
    const x = try b.parameter("x", shape);
    const seed = try b.parameter("seed", shape);
    const y = try b.sigmoidRetained(x);
    var ad = try gradientWithSeeds(a, &g, &.{.{ .output = y, .cotangent = seed }}, &.{x}, .{});
    defer ad.deinit();
    const grad = ad.graph.node(ad.param_grads[0]);
    try std.testing.expect(grad.op == .fused_sigmoid_backward);
    try std.testing.expect(grad.output_shape.eq(shape));
    try std.testing.expectEqualSlices(NodeId, &.{ ad.id_map[y], ad.id_map[seed] }, grad.getInputs());
    try std.testing.expect(ad.graph.node(grad.inputs[0]).op == .fused_sigmoid);
    try std.testing.expectError(error.InvalidSigmoidShape, b.sigmoidRetained(null_node));
    const integer = try b.parameter("integer", Shape.init(.i32, &.{ 2, 3 }));
    try std.testing.expectError(error.InvalidSigmoidShape, b.sigmoidRetained(integer));
    g.nodeMut(y).num_inputs = 2;
    g.nodeMut(y).inputs[1] = x;
    try std.testing.expectError(error.InvalidSigmoidShape, gradientWithSeeds(a, &g, &.{.{ .output = y, .cotangent = seed }}, &.{x}, .{}));
}

test "retained SiLU strict VJP uses original input and seed while default decomposes" {
    const a = std.testing.allocator;
    var g = Graph.init(a);
    defer g.deinit();
    var b = Builder.init(&g);
    const shape = Shape.init(.f32, &.{ 2, 3 });
    const x = try b.parameter("x", shape);
    const seed = try b.parameter("seed", shape);
    const y = try b.silu(x);
    var materialized = try gradientWithSeeds(a, &g, &.{.{ .output = y, .cotangent = seed }}, &.{x}, .{});
    defer materialized.deinit();
    for (materialized.graph.nodes.items) |n| {
        try std.testing.expect(n.op != .fused_silu and n.op != .fused_silu_backward);
    }
    g.nodeMut(y).vjp_alternate = null_node;
    var retained = try gradientWithSeeds(a, &g, &.{.{ .output = y, .cotangent = seed }}, &.{x}, .{});
    defer retained.deinit();
    const grad = retained.graph.node(retained.param_grads[0]);
    try std.testing.expect(grad.op == .fused_silu_backward);
    try std.testing.expectEqualSlices(NodeId, &.{ retained.id_map[x], retained.id_map[seed] }, grad.getInputs());
    g.nodeMut(y).num_inputs = 2;
    g.nodeMut(y).inputs[1] = x;
    try std.testing.expectError(error.InvalidSiluShape, gradientWithSeeds(a, &g, &.{.{ .output = y, .cotangent = seed }}, &.{x}, .{}));
}

test "prefix scan strict VJP reverses direction without retaining values" {
    const a = std.testing.allocator;
    var g = Graph.init(a);
    defer g.deinit();
    var b = Builder.init(&g);
    const attrs = node_mod.PrefixScanAttrs{ .batch = 3, .width = 65, .channels = 1, .reference = .inner };
    const shape = try attrs.shape();
    const x = try b.parameter("x", shape);
    const seed = try b.parameter("seed", shape);
    const y = try b.prefixScanV1(x, attrs);
    var ad = try gradientWithSeeds(a, &g, &.{.{ .output = y, .cotangent = seed }}, &.{x}, .{});
    defer ad.deinit();
    const grad = ad.graph.node(ad.param_grads[0]);
    var reverse = attrs;
    reverse.reverse = true;
    try std.testing.expectEqualDeep(reverse, grad.op.fused_prefix_scan_v1);
    try std.testing.expectEqualSlices(NodeId, &.{ad.id_map[seed]}, grad.getInputs());
    g.nodeMut(y).op.fused_prefix_scan_v1.channels = 2;
    try std.testing.expectError(error.InvalidPrefixScanShape, gradientWithSeeds(a, &g, &.{.{ .output = y, .cotangent = seed }}, &.{x}, .{}));
}

test "frozen span features validate geometry and detach both metadata inputs" {
    const a = std.testing.allocator;
    var g = Graph.init(a);
    defer g.deinit();
    var b = Builder.init(&g);
    const attrs = node_mod.FrozenSpanFeaturesAttrs{ .batch = 2, .capacity = 3 };
    const lengths = try b.parameter("lengths", Shape.init(.f32, &.{ 6, 1 }));
    const counts = try b.parameter("counts", Shape.init(.f32, &.{ 2, 1 }));
    const seed = try b.parameter("seed", try attrs.shape());
    const y = try b.frozenSpanFeaturesV1(lengths, counts, attrs);
    var ad = try gradientWithSeeds(a, &g, &.{.{ .output = y, .cotangent = seed }}, &.{ lengths, counts }, .{});
    defer ad.deinit();
    try std.testing.expectEqualSlices(NodeId, &.{ null_node, null_node }, ad.param_grads);
    try std.testing.expect(ad.graph.node(ad.id_map[y]).op == .frozen_span_features_v1);
    try std.testing.expectError(error.InvalidFrozenSpanFeaturesShape, b.frozenSpanFeaturesV1(counts, lengths, attrs));
    try std.testing.expectError(error.InvalidFrozenSpanFeaturesShape, b.frozenSpanFeaturesV1(null_node, counts, attrs));
    try std.testing.expectError(error.InvalidFrozenSpanFeaturesShape, b.frozenSpanFeaturesV1(lengths, counts, .{ .batch = 0, .capacity = 3 }));
    try std.testing.expectError(error.InvalidFrozenSpanFeaturesShape, b.frozenSpanFeaturesV1(lengths, counts, .{ .batch = 65536, .capacity = 65536 }));
    g.nodeMut(y).op.frozen_span_features_v1.capacity = 4;
    try std.testing.expectError(error.InvalidFrozenSpanFeaturesShape, gradientWithSeeds(a, &g, &.{.{ .output = y, .cotangent = seed }}, &.{lengths}, .{}));
}

test "gather strict VJP preserves explicit scatter reduction profile" {
    const a = std.testing.allocator;
    var g = Graph.init(a);
    defer g.deinit();
    var b = Builder.init(&g);
    const table = try b.parameter("table", Shape.init(.f32, &.{ 3, 7 }));
    const index = try b.parameter("index", Shape.init(.i32, &.{65}));
    const shape = Shape.init(.f32, &.{ 65, 7 });
    const seed = try b.parameter("seed", shape);
    const output = try b.gather(table, index, shape);
    for ([_]node_mod.ScatterReduction{ .serial_v1, .pytorch_gather_v1, .pytorch_embedding_v1 }) |reference| {
        g.nodeMut(output).op.gather.backward_reduction = reference;
        g.nodeMut(output).op.gather.backward_padding_index = if (reference == .pytorch_embedding_v1) 2 else null;
        var ad = try gradientWithSeeds(a, &g, &.{.{ .output = output, .cotangent = seed }}, &.{table}, .{});
        defer ad.deinit();
        const grad = ad.graph.node(ad.param_grads[0]);
        try std.testing.expect(grad.op == .scatter_add);
        try std.testing.expectEqual(reference, grad.op.scatter_add.reduction);
        try std.testing.expectEqual(g.node(output).op.gather.backward_padding_index, grad.op.scatter_add.padding_index);
        try std.testing.expectEqualSlices(NodeId, &.{ ad.id_map[seed], ad.id_map[index] }, grad.getInputs());
        try std.testing.expect(grad.output_shape.eq(g.node(table).output_shape));
    }
}
