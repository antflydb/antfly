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

// Automatic parallel execution strategy planner.
//
// Given a traced graph and a device mesh, produces a DevicePartitionPlan
// that distributes execution across devices. Supports two strategies:
//
// - **Pipeline**: Splits the graph at attention-layer boundaries and
//   assigns contiguous groups of layers to different devices. Each device
//   processes its pipeline stage sequentially; cross-stage transfers
//   happen automatically via the multi-device executor.
//
// - **Tensor**: Replicates the full graph on all devices. Combined with
//   a ShardingSpec (from sharding.zig), each device executes the same
//   ops on sharded weights. The caller is responsible for inserting
//   all_reduce / all_gather at partition boundaries via collective_ops.
//
// The `single` strategy is a no-op: all nodes on device 0.

const std = @import("std");
const ml = @import("ml");

const Graph = ml.graph.Graph;
const Node = ml.graph.Node;
const NodeId = ml.graph.NodeId;
const null_node = ml.graph.null_node;
const OpCode = ml.graph.OpCode;

const contracts = @import("backend_contracts.zig");
const BackendKind = contracts.BackendKind;

const partition_mod = @import("partition.zig");
const Partition = partition_mod.Partition;
const PartitionPlan = partition_mod.PartitionPlan;
const ExternalInput = partition_mod.ExternalInput;
const operator_plan = @import("operator_plan.zig");

const device_mesh_mod = @import("device_mesh.zig");
const DeviceId = device_mesh_mod.DeviceId;
const DeviceMesh = device_mesh_mod.DeviceMesh;

const multi_executor = @import("multi_executor.zig");
const DevicePartitionPlan = multi_executor.DevicePartitionPlan;

const sharding_mod = @import("sharding.zig");
const ShardingSpec = sharding_mod.ShardingSpec;

pub const Strategy = enum {
    /// All nodes on device 0.
    single,
    /// Split graph at attention-layer boundaries across devices.
    pipeline,
    /// Replicate graph on all devices with sharded weights.
    tensor,
};

pub const ParallelConfig = struct {
    strategy: Strategy,
    /// Number of devices to distribute across.
    num_devices: u16,
    /// For pipeline: number of attention layers per pipeline stage.
    /// If 0, layers are split as evenly as possible.
    layers_per_stage: u16 = 0,
    /// For tensor: sharding spec (may be null for pipeline/single).
    sharding_spec: ?*const ShardingSpec = null,
    /// Backend kind to assign to each partition.
    backend: BackendKind = .native,
};

pub const PlanError = error{
    OutOfMemory,
    EmptyGraph,
    NoDevices,
};

/// Produce a DevicePartitionPlan from a graph and parallel config.
pub fn planParallel(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    config: ParallelConfig,
) PlanError!DevicePartitionPlan {
    if (graph.nodeCount() == 0) return emptyPlan(allocator);
    if (config.num_devices == 0) return error.NoDevices;

    return switch (config.strategy) {
        .single => planSingle(allocator, graph, config),
        .pipeline => planPipeline(allocator, graph, config),
        .tensor => planTensor(allocator, graph, config),
    };
}

// ── Single strategy ──────────────────────────────────────────────────

fn planSingle(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    config: ParallelConfig,
) PlanError!DevicePartitionPlan {
    const count = graph.nodeCount();

    // One partition containing all nodes.
    const node_ids = try allocator.alloc(NodeId, count);
    for (0..count) |i| node_ids[i] = @intCast(i);

    const ext_inputs = try allocator.alloc(ExternalInput, 0);

    const partitions = try allocator.alloc(Partition, 1);
    partitions[0] = .{
        .backend = config.backend,
        .device_id = 0,
        .node_ids = node_ids,
        .external_inputs = ext_inputs,
    };

    const node_assignment = try allocator.alloc(u32, count);
    @memset(node_assignment, 0);
    const node_operator_plans = try allocEmptyOperatorPlans(allocator, count);

    const dev_assign = try allocator.alloc(DeviceId, 1);
    dev_assign[0] = 0;

    return .{
        .base = .{
            .partitions = partitions,
            .node_assignment = node_assignment,
            .node_operator_plans = node_operator_plans,
            .allocator = allocator,
        },
        .device_assignment = dev_assign,
        .allocator = allocator,
    };
}

// ── Pipeline strategy ────────────────────────────────────────────────

/// Returns true if this opcode is an attention operation (layer boundary marker).
fn isAttentionOp(op: OpCode) bool {
    return switch (op) {
        .fused_sdpa,
        .fused_causal_self_attention,
        .fused_cross_attention,
        .fused_gqa_causal_attention,
        => true,
        else => false,
    };
}

/// Find all attention node positions in topological order.
/// Returns indices into the node array (not NodeIds — they're the same
/// since the graph is topologically sorted with sequential IDs).
fn findAttentionNodes(
    allocator: std.mem.Allocator,
    graph: *const Graph,
) ![]u32 {
    var attn_positions = std.ArrayListUnmanaged(u32).empty;
    errdefer attn_positions.deinit(allocator);

    const count = graph.nodeCount();
    for (0..count) |i| {
        const n = graph.node(@intCast(i));
        if (isAttentionOp(n.op)) {
            try attn_positions.append(allocator, @intCast(i));
        }
    }

    return attn_positions.toOwnedSlice(allocator);
}

fn planPipeline(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    config: ParallelConfig,
) PlanError!DevicePartitionPlan {
    const count = graph.nodeCount();
    const num_devices: usize = @intCast(config.num_devices);

    // Find attention layer boundaries.
    const attn_nodes = try findAttentionNodes(allocator, graph);
    defer allocator.free(attn_nodes);

    const num_layers = attn_nodes.len;

    // If no attention layers found or only 1 device, fall back to single.
    if (num_layers == 0 or num_devices <= 1) {
        return planSingle(allocator, graph, config);
    }

    // Compute layers per stage.
    const lps: usize = if (config.layers_per_stage > 0)
        @intCast(config.layers_per_stage)
    else
        // Divide layers as evenly as possible.
        (num_layers + num_devices - 1) / num_devices;

    // Compute split points: after each group of `lps` attention nodes.
    // split_after[i] is the node index after which we start a new stage.
    // We split after the last node that depends on the attention node,
    // but for simplicity we split right after the attention node itself.
    var split_points = std.ArrayListUnmanaged(u32).empty;
    defer split_points.deinit(allocator);

    var layer_idx: usize = 0;
    while (layer_idx < num_layers) : (layer_idx += lps) {
        const end = @min(layer_idx + lps, num_layers);
        if (end < num_layers) {
            // Split after this attention node (next stage starts at attn_node + 1).
            try split_points.append(allocator, attn_nodes[end - 1]);
        }
    }

    // Number of stages = split_points + 1
    const num_stages = split_points.items.len + 1;
    const actual_devices = @min(num_stages, num_devices);

    // Build stage ranges: [start, end) node indices.
    const stage_starts = try allocator.alloc(u32, num_stages);
    defer allocator.free(stage_starts);
    const stage_ends = try allocator.alloc(u32, num_stages);
    defer allocator.free(stage_ends);

    stage_starts[0] = 0;
    for (split_points.items, 0..) |sp, i| {
        stage_ends[i] = sp + 1;
        stage_starts[i + 1] = sp + 1;
    }
    stage_ends[num_stages - 1] = @intCast(count);

    // Build partitions.
    const partitions = try allocator.alloc(Partition, num_stages);
    errdefer {
        for (partitions[0..num_stages]) |p| {
            allocator.free(p.node_ids);
            allocator.free(p.external_inputs);
        }
        allocator.free(partitions);
    }

    const node_assignment = try allocator.alloc(u32, count);
    errdefer allocator.free(node_assignment);
    const node_operator_plans = try allocEmptyOperatorPlans(allocator, count);
    errdefer allocator.free(node_operator_plans);

    for (0..num_stages) |stage| {
        const start: usize = @intCast(stage_starts[stage]);
        const end: usize = @intCast(stage_ends[stage]);
        const stage_size = end - start;

        const node_ids = try allocator.alloc(NodeId, stage_size);
        for (0..stage_size) |j| {
            node_ids[j] = @intCast(start + j);
            node_assignment[start + j] = @intCast(stage);
        }

        // Compute external inputs: nodes in this stage that read from prior stages.
        var ext_inputs = std.ArrayListUnmanaged(ExternalInput).empty;
        errdefer ext_inputs.deinit(allocator);

        for (node_ids) |nid| {
            const n = graph.node(nid);
            for (n.getInputs()) |inp| {
                if (inp == null_node or inp >= count) continue;
                const inp_stage = node_assignment[@intCast(inp)];
                if (inp_stage != @as(u32, @intCast(stage))) {
                    // Deduplicate.
                    var found = false;
                    for (ext_inputs.items) |existing| {
                        if (existing.node_id == inp) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        try ext_inputs.append(allocator, .{
                            .node_id = inp,
                            .source_partition = inp_stage,
                        });
                    }
                }
            }
        }

        const dev_id: DeviceId = @intCast(stage % actual_devices);
        partitions[stage] = .{
            .backend = config.backend,
            .device_id = dev_id,
            .node_ids = node_ids,
            .external_inputs = try ext_inputs.toOwnedSlice(allocator),
        };
    }

    // Device assignment: round-robin across available devices.
    const dev_assign = try allocator.alloc(DeviceId, num_stages);
    for (0..num_stages) |i| {
        dev_assign[i] = @intCast(i % actual_devices);
    }

    return .{
        .base = .{
            .partitions = partitions,
            .node_assignment = node_assignment,
            .node_operator_plans = node_operator_plans,
            .allocator = allocator,
        },
        .device_assignment = dev_assign,
        .allocator = allocator,
    };
}

// ── Tensor strategy ──────────────────────────────────────────────────

fn planTensor(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    config: ParallelConfig,
) PlanError!DevicePartitionPlan {
    // Tensor parallelism replicates the graph on all devices.
    // Each device runs the full graph but with sharded weights.
    // The actual sharding is applied at weight-loading time via
    // ShardedBackend (sharded_backend.zig). Here we just create
    // one partition per device, each containing all nodes.
    //
    // Collective operations (all_reduce after row-sharded matmuls)
    // are handled by the caller at execution time, not in the plan.

    const count = graph.nodeCount();
    const num_devices: usize = @intCast(config.num_devices);

    if (num_devices <= 1) {
        return planSingle(allocator, graph, config);
    }

    // One partition per device, each containing the full graph.
    const partitions = try allocator.alloc(Partition, num_devices);
    errdefer {
        for (partitions[0..num_devices]) |p| {
            allocator.free(p.node_ids);
            allocator.free(p.external_inputs);
        }
        allocator.free(partitions);
    }

    // All nodes assigned to partition 0 (the primary).
    // Replicas share the same node_assignment since they execute
    // the same graph — the differentiation is in the weight sharding.
    const node_assignment = try allocator.alloc(u32, count);
    @memset(node_assignment, 0);
    const node_operator_plans = try allocEmptyOperatorPlans(allocator, count);

    for (0..num_devices) |dev| {
        const node_ids = try allocator.alloc(NodeId, count);
        for (0..count) |i| node_ids[i] = @intCast(i);

        const ext_inputs = try allocator.alloc(ExternalInput, 0);

        partitions[dev] = .{
            .backend = config.backend,
            .device_id = @intCast(dev),
            .node_ids = node_ids,
            .external_inputs = ext_inputs,
        };
    }

    const dev_assign = try allocator.alloc(DeviceId, num_devices);
    for (0..num_devices) |i| {
        dev_assign[i] = @intCast(i);
    }

    return .{
        .base = .{
            .partitions = partitions,
            .node_assignment = node_assignment,
            .node_operator_plans = node_operator_plans,
            .allocator = allocator,
        },
        .device_assignment = dev_assign,
        .allocator = allocator,
    };
}

// ── Helpers ──────────────────────────────────────────────────────────

fn emptyPlan(allocator: std.mem.Allocator) PlanError!DevicePartitionPlan {
    return .{
        .base = .{
            .partitions = try allocator.alloc(Partition, 0),
            .node_assignment = try allocator.alloc(u32, 0),
            .node_operator_plans = try allocator.alloc(?operator_plan.OperatorPlan, 0),
            .allocator = allocator,
        },
        .device_assignment = try allocator.alloc(DeviceId, 0),
        .allocator = allocator,
    };
}

fn allocEmptyOperatorPlans(
    allocator: std.mem.Allocator,
    count: usize,
) ![]?operator_plan.OperatorPlan {
    const plans = try allocator.alloc(?operator_plan.OperatorPlan, count);
    @memset(plans, null);
    return plans;
}

// ── Tests ────────────────────────────────────────────────────────────

const Builder = ml.graph.Builder;
const Shape = ml.graph.Shape;

/// Test helper: add a fused attention node (fused_sdpa) to the graph.
/// Returns the node ID. This bypasses Builder since it has no attention method.
fn addTestAttention(g: *Graph, input: NodeId, shape: Shape) !NodeId {
    return g.addNode(.{
        .op = .{ .fused_sdpa = .{
            .batch = @intCast(shape.dims[0]),
            .seq_len = @intCast(shape.dims[1]),
            .num_heads = 1,
            .head_dim = @intCast(shape.dims[2]),
        } },
        .output_shape = shape,
        .inputs = .{ input, input, input, null_node },
        .num_inputs = 3,
    });
}

test "single strategy puts everything on device 0" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const out = try b.gelu(x);
    try g.markOutput(out);

    var plan = try planParallel(allocator, &g, .{
        .strategy = .single,
        .num_devices = 2,
    });
    defer plan.deinit();

    try std.testing.expectEqual(@as(usize, 1), plan.base.partitions.len);
    try std.testing.expectEqual(@as(DeviceId, 0), plan.device_assignment[0]);
    try std.testing.expectEqual(g.nodeCount(), @as(u32, @intCast(plan.base.partitions[0].node_ids.len)));
}

test "pipeline splits at attention boundaries" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // Build a simplified 2-layer transformer:
    //   param → linear → attention → linear → attention → output
    const shape = Shape.init(.f32, &.{ 1, 4, 8 });
    const w_shape = Shape.init(.f32, &.{ 8, 8 });

    const x = try b.parameter("x", shape);
    const w1 = try b.parameter("w1", w_shape);
    const h1 = try b.linearNoBias(x, w1, 4, 8, 8);
    // Attention layer 1
    const a1 = try addTestAttention(&g, h1, shape);
    const w2 = try b.parameter("w2", w_shape);
    const h2 = try b.linearNoBias(a1, w2, 4, 8, 8);
    // Attention layer 2
    const a2 = try addTestAttention(&g, h2, shape);
    try g.markOutput(a2);

    // Pipeline across 2 devices → should split between the two attention layers.
    var plan = try planParallel(allocator, &g, .{
        .strategy = .pipeline,
        .num_devices = 2,
    });
    defer plan.deinit();

    // Should have 2 stages.
    try std.testing.expectEqual(@as(usize, 2), plan.base.partitions.len);

    // Device 0 gets stage 0, device 1 gets stage 1.
    try std.testing.expectEqual(@as(DeviceId, 0), plan.device_assignment[0]);
    try std.testing.expectEqual(@as(DeviceId, 1), plan.device_assignment[1]);

    // Stage 1 should have external inputs from stage 0.
    try std.testing.expect(plan.base.partitions[1].external_inputs.len > 0);

    // All nodes should be covered.
    var total_nodes: usize = 0;
    for (plan.base.partitions) |p| total_nodes += p.node_ids.len;
    try std.testing.expectEqual(@as(usize, g.nodeCount()), total_nodes);
}

test "pipeline falls back to single with no attention" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const out = try b.gelu(x);
    try g.markOutput(out);

    var plan = try planParallel(allocator, &g, .{
        .strategy = .pipeline,
        .num_devices = 2,
    });
    defer plan.deinit();

    // No attention ops → single partition.
    try std.testing.expectEqual(@as(usize, 1), plan.base.partitions.len);
    try std.testing.expectEqual(@as(DeviceId, 0), plan.device_assignment[0]);
}

test "tensor strategy creates one partition per device" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const out = try b.gelu(x);
    try g.markOutput(out);

    var plan = try planParallel(allocator, &g, .{
        .strategy = .tensor,
        .num_devices = 3,
    });
    defer plan.deinit();

    // One partition per device, each with the full graph.
    try std.testing.expectEqual(@as(usize, 3), plan.base.partitions.len);

    for (0..3) |i| {
        try std.testing.expectEqual(@as(DeviceId, @intCast(i)), plan.device_assignment[i]);
        try std.testing.expectEqual(@as(usize, g.nodeCount()), plan.base.partitions[i].node_ids.len);
    }
}

test "tensor strategy single device falls back to single" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const out = try b.gelu(x);
    try g.markOutput(out);

    var plan = try planParallel(allocator, &g, .{
        .strategy = .tensor,
        .num_devices = 1,
    });
    defer plan.deinit();

    try std.testing.expectEqual(@as(usize, 1), plan.base.partitions.len);
}

test "pipeline with 4 layers on 2 devices splits evenly" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const shape = Shape.init(.f32, &.{ 1, 4, 8 });

    var prev = try b.parameter("x", shape);

    // 4 attention layers.
    for (0..4) |_| {
        prev = try addTestAttention(&g, prev, shape);
    }
    try g.markOutput(prev);

    var plan = try planParallel(allocator, &g, .{
        .strategy = .pipeline,
        .num_devices = 2,
    });
    defer plan.deinit();

    // 4 layers / 2 devices = 2 layers per stage = 2 stages.
    try std.testing.expectEqual(@as(usize, 2), plan.base.partitions.len);

    // All nodes covered.
    var total_nodes: usize = 0;
    for (plan.base.partitions) |p| total_nodes += p.node_ids.len;
    try std.testing.expectEqual(@as(usize, g.nodeCount()), total_nodes);
}

test "isAttentionOp identifies attention opcodes" {
    const sdpa: OpCode = .{ .fused_sdpa = .{ .batch = 1, .seq_len = 4, .num_heads = 1, .head_dim = 8 } };
    try std.testing.expect(isAttentionOp(sdpa));

    const causal: OpCode = .{ .fused_causal_self_attention = .{ .batch = 1, .seq_len = 4, .num_heads = 1, .head_dim = 8 } };
    try std.testing.expect(isAttentionOp(causal));

    const gqa: OpCode = .{ .fused_gqa_causal_attention = .{ .batch = 1, .seq_len = 4, .num_heads = 1, .head_dim = 8 } };
    try std.testing.expect(isAttentionOp(gqa));

    const cross: OpCode = .{ .fused_cross_attention = .{ .batch = 1, .dec_seq = 4, .enc_seq = 4, .num_heads = 1, .head_dim = 8 } };
    try std.testing.expect(isAttentionOp(cross));

    const gelu: OpCode = .{ .fused_gelu = {} };
    try std.testing.expect(!isAttentionOp(gelu));
}
