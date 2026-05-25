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

// Execution provider partitioning.
//
// Partitions a computation graph across multiple backends based on
// capability declarations. Each backend declares which ops it supports;
// the partitioner greedily assigns maximal connected subgraphs to the
// highest-priority backend that supports them.
//
// Inspired by ONNX Runtime's execution provider model:
// - Backends declare capabilities (which ops they handle)
// - Partitioner walks backends in priority order
// - Default backend (native/CPU) is the fallback — guarantees completeness
// - Each partition is a contiguous subgraph executed on one backend
//
// Use case: compiled or device backends handle supported graph regions,
// native handles anything unsupported. Also the foundation for tensor
// parallelism: partitioning across devices uses the same mechanism.

const std = @import("std");
const ml = @import("ml");

const Graph = ml.graph.Graph;
const NodeId = ml.graph.NodeId;
const null_node = ml.graph.null_node;
const OpCode = ml.graph.OpCode;
const ops_mod = @import("../ops/ops.zig");
const contracts = @import("backend_contracts.zig");
const interpreter = @import("interpreter.zig");
const quant_matmul = @import("quant_matmul.zig");
const operator_plan = @import("operator_plan.zig");
const buffer_plan_mod = @import("buffer_plan.zig");
const transpose_utils = @import("transpose_utils.zig");
const runtime_root = @import("../runtime/root.zig");
const kv_pool = @import("../runtime/kv/pool.zig");
const BackendKind = contracts.BackendKind;
const CT = contracts.CT;
const ComputeBackend = ops_mod.ComputeBackend;
const DeviceId = @import("device_mesh.zig").DeviceId;
const DeviceMesh = @import("device_mesh.zig").DeviceMesh;

/// Type-erased partition executor.
///
/// When a partition has a compiled executor, the
/// multi-device executor calls it instead of interpreting nodes one by one.
/// The executor receives external input values and produces output values
/// for any nodes consumed by downstream partitions or marked as graph outputs.
pub const PartitionExecutor = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const ExecutionStats = struct {
        partitions_executed: u64 = 0,
        cross_device_transfers: u64 = 0,
        runtime_input_transfers: u64 = 0,
        device_resident_transfers: u64 = 0,
        backend_command_dispatches: u64 = 0,
        planned_operator_dispatches: u64 = 0,
        metadata_aliases: u64 = 0,
        descriptor_materializations: u64 = 0,
        constant_materializations: u64 = 0,
        graph_regions: u64 = 0,
        graph_region_ops: u64 = 0,
        graph_region_fallbacks: u64 = 0,
        metal_qkv_regions: u64 = 0,
        metal_attention_regions: u64 = 0,
        metal_ffn_regions: u64 = 0,
        metal_ple_regions: u64 = 0,
        metal_tail_regions: u64 = 0,
        fused_graph_pattern_dispatches: u64 = 0,
        fused_graph_nodes_elided: u64 = 0,
        metal_attention_output_residual_fusions: u64 = 0,
        metal_attention_output_residual_partial_fallbacks: u64 = 0,
        metal_gated_ffn_residual_fusions: u64 = 0,
        metal_linear_pair_fusions: u64 = 0,
        interpreter_fallbacks: u64 = 0,
        device_resident_outputs: u64 = 0,
        host_materialized_outputs: u64 = 0,
        boundary_output_materializations: u64 = 0,
        graph_plan_slots_reserved: u64 = 0,
        graph_plan_bytes_reserved: u64 = 0,
        runtime_region_plan_compiles: u64 = 0,
        runtime_region_plan_regions: u64 = 0,
        runtime_region_plan_dispatches: u64 = 0,
        runtime_region_plan_reuses: u64 = 0,
        runtime_prepare_slot_calls: u64 = 0,
        runtime_prepare_slot_cache_hits: u64 = 0,
        runtime_region_fallbacks: u64 = 0,
        runtime_frame_candidates: u64 = 0,
        runtime_frame_eligible: u64 = 0,
        runtime_frame_metadata_ready: u64 = 0,
        runtime_frame_ineligible_no_regions: u64 = 0,
        runtime_frame_ineligible_missing_qkv: u64 = 0,
        runtime_frame_ineligible_missing_attention: u64 = 0,
        runtime_frame_ineligible_missing_ffn: u64 = 0,
        runtime_frame_ineligible_missing_ple: u64 = 0,
        runtime_frame_ineligible_single_row: u64 = 0,
        runtime_frame_ineligible_non_layer_order: u64 = 0,
        runtime_frame_ineligible_shape_mismatch: u64 = 0,
        runtime_frame_ineligible_missing_model_metadata: u64 = 0,
        gemma_qkv_hits: u64 = 0,
        gemma_qkv_fallbacks: u64 = 0,
        gemma_o_proj_hits: u64 = 0,
        gemma_o_proj_fallbacks: u64 = 0,
        gemma_mlp_proj_hits: u64 = 0,
        gemma_mlp_proj_fallbacks: u64 = 0,
        gemma_attention_matmul_hits: u64 = 0,
        gemma_attention_matmul_fallbacks: u64 = 0,
        gemma_rms_norm_hits: u64 = 0,
        gemma_rms_norm_fallbacks: u64 = 0,
        gemma_softmax_hits: u64 = 0,
        gemma_softmax_fallbacks: u64 = 0,
        gemma_residual_add_hits: u64 = 0,
        gemma_residual_add_fallbacks: u64 = 0,
        gemma_elementwise_mul_hits: u64 = 0,
        gemma_elementwise_mul_fallbacks: u64 = 0,
    };

    pub const ExecutionContext = struct {
        allocator: ?std.mem.Allocator = null,
        graph: ?*const Graph = null,
        backend: ?*const ComputeBackend = null,
        mesh: ?*const DeviceMesh = null,
        options: ?interpreter.ExecuteOptions = null,
        reachable: ?[]const bool = null,
        last_use: ?[]const u32 = null,
        partition_plan: ?*const PartitionPlan = null,
        buffer_plan: ?*const buffer_plan_mod.BufferPlan = null,
        owned_runtime_transfers: ?*std.AutoHashMapUnmanaged(NodeId, void) = null,
        materialize_boundary_outputs: bool = true,
        stats: ?*ExecutionStats = null,
        attention: ?*const contracts.AttentionContext = null,
        attention_layer: ?*usize = null,
        pair_second: ?*?CT = null,
        embedding_ids: ?[]const i64 = null,
    };

    pub const VTable = struct {
        /// Execute the partition. `values` is the full per-node value array —
        /// external inputs are already populated. The executor must fill in
        /// values for all nodes it owns.
        execute: *const fn (
            ctx: *anyopaque,
            values: []?CT,
            value_device: []DeviceId,
            node_ids: []const NodeId,
            device_id: DeviceId,
            exec_ctx: ExecutionContext,
        ) anyerror!void,
        /// Release resources held by this executor.
        deinit: *const fn (ctx: *anyopaque) void,
    };

    pub fn execute(
        self: *const PartitionExecutor,
        values: []?CT,
        value_device: []DeviceId,
        node_ids: []const NodeId,
        device_id: DeviceId,
        exec_ctx: ExecutionContext,
    ) !void {
        return self.vtable.execute(self.ptr, values, value_device, node_ids, device_id, exec_ctx);
    }

    pub fn deinitExecutor(self: *const PartitionExecutor) void {
        self.vtable.deinit(self.ptr);
    }
};

/// Why a backend did or did not accept a graph node.
///
/// `supported` means the backend accepted the node for partition assignment.
/// The other tags are intentionally broad diagnostic buckets; later phases can
/// make partition reports count them without baking backend-specific strings
/// into the partitioner.
pub const CapabilityReason = enum {
    supported,
    unsupported_op,
    unprofitable_shape,
    wrong_storage,
    missing_quant_kernel,
    backend_disabled,
};

pub const CapabilityDiagnostics = struct {
    counts: [@typeInfo(CapabilityReason).@"enum".fields.len]usize = .{0} ** @typeInfo(CapabilityReason).@"enum".fields.len,
    operator_stats: operator_plan.Stats = .{},

    pub fn record(self: *CapabilityDiagnostics, decision: CapabilityDecision) void {
        self.counts[@intFromEnum(decision.reason)] += 1;
        if (decision.can_execute and decision.should_execute) {
            if (decision.operator_plan) |plan| self.operator_stats.add(plan.operator());
        }
    }

    pub fn count(self: *const CapabilityDiagnostics, reason: CapabilityReason) usize {
        return self.counts[@intFromEnum(reason)];
    }

    pub fn operatorCount(self: *const CapabilityDiagnostics, operator: operator_plan.Operator) usize {
        return self.operator_stats.count(operator);
    }
};

/// Rich capability query for a graph node.
///
/// Today most callers still provide the simpler `supports(OpCode)` callback.
/// New backends can use this query to inspect shape, attrs, and eventually
/// storage/residency before deciding whether a backend both can and should own
/// the node.
pub const CapabilityQuery = struct {
    graph: *const Graph,
    node_id: NodeId,
    op: OpCode,
    tensor_descs: ?[]const ?contracts.TensorDesc = null,
};

/// Backend capability result.
///
/// `can_execute` is correctness. `should_execute` is profitability after shape,
/// setup, and transfer costs. The partitioner assigns nodes only when both are
/// true; this mirrors ggml-style support checks such as BLAS accepting only
/// sufficiently large contiguous GEMMs.
pub const CapabilityDecision = struct {
    can_execute: bool,
    should_execute: bool,
    reason: CapabilityReason,
    estimated_cost: u64 = 0,
    operator_plan: ?operator_plan.OperatorPlan = null,

    pub fn accept() CapabilityDecision {
        return acceptCost(0);
    }

    pub fn acceptCost(estimated_cost: u64) CapabilityDecision {
        return .{
            .can_execute = true,
            .should_execute = true,
            .reason = .supported,
            .estimated_cost = estimated_cost,
        };
    }

    pub fn acceptCostWithOperator(estimated_cost: u64, plan: operator_plan.OperatorPlan) CapabilityDecision {
        return .{
            .can_execute = true,
            .should_execute = true,
            .reason = .supported,
            .estimated_cost = estimated_cost,
            .operator_plan = plan,
        };
    }

    pub fn reject(reason: CapabilityReason) CapabilityDecision {
        return .{
            .can_execute = false,
            .should_execute = false,
            .reason = reason,
        };
    }

    pub fn unprofitable() CapabilityDecision {
        return .{
            .can_execute = true,
            .should_execute = false,
            .reason = .unprofitable_shape,
        };
    }
};

pub const PartitionOptions = struct {
    tensor_descs: ?[]const ?contracts.TensorDesc = null,
    diagnostics: ?*CapabilityDiagnostics = null,
};

/// A backend's capability declaration: which graph nodes it should execute.
pub const Capability = struct {
    /// Which backend this capability is for.
    backend: BackendKind,
    /// Priority (higher = preferred). When multiple backends can handle
    /// an op, the highest-priority one wins.
    priority: u8,
    /// Legacy op-only support hook. Kept so existing compiled backends and
    /// tests can migrate incrementally.
    supports: ?*const fn (op: OpCode) bool = null,
    /// Rich graph-node decision hook. Prefer this for new capability logic that
    /// depends on shape, storage, or profitability.
    decide: ?*const fn (query: CapabilityQuery) CapabilityDecision = null,

    pub fn decision(self: Capability, query: CapabilityQuery) CapabilityDecision {
        if (self.decide) |decide| return decide(query);
        if (self.supports) |supports| {
            return if (supports(query.op)) CapabilityDecision.accept() else CapabilityDecision.reject(.unsupported_op);
        }
        return CapabilityDecision.reject(.unsupported_op);
    }
};

/// A partition: a set of nodes assigned to one backend.
pub const Partition = struct {
    /// Which backend executes this partition.
    backend: BackendKind,
    /// Logical device ID (default 0 for single-device). Set by
    /// device-aware partitioners; the multi-device executor uses this
    /// to dispatch to the correct ComputeBackend.
    device_id: DeviceId = 0,
    /// Node IDs in topological order within this partition.
    node_ids: []const NodeId,
    /// Inputs to this partition from other partitions (cross-partition edges).
    /// These are (node_id, source_partition_index) pairs that need data transfer.
    external_inputs: []const ExternalInput,
    /// Optional compiled executor. When set, the multi-device executor calls
    /// this instead of interpreting nodes one by one. Owned by whoever sets it
    /// (typically the compiler that created it); freed in PartitionPlan.deinit.
    executor: ?*const PartitionExecutor = null,
};

pub const ExternalInput = struct {
    /// Node ID in the graph that this partition reads from.
    node_id: NodeId,
    /// Index of the partition that produces this node.
    source_partition: u32,
};

pub const PartitionPlan = struct {
    /// Partitions in execution order. Dependencies flow forward:
    /// partition[i] may depend on partition[j] only if j < i.
    partitions: []Partition,
    /// Per-node assignment: node_assignment[node_id] = partition index.
    node_assignment: []const u32,
    /// Optional concrete operator plan selected by the winning backend.
    ///
    /// This is intentionally per-node rather than per-partition: a partition can
    /// contain a mix of generic ops and planned quant/attention commands.
    node_operator_plans: []const ?operator_plan.OperatorPlan,
    allocator: std.mem.Allocator,
    /// When false, deinit skips executor cleanup (executors are borrowed
    /// from an external cache and will be freed by their owner).
    owns_executors: bool = true,

    pub fn deinit(self: *PartitionPlan) void {
        for (self.partitions) |p| {
            if (self.owns_executors) {
                if (p.executor) |exec| exec.deinitExecutor();
            }
            self.allocator.free(p.node_ids);
            self.allocator.free(p.external_inputs);
        }
        self.allocator.free(self.partitions);
        self.allocator.free(self.node_assignment);
        self.allocator.free(self.node_operator_plans);
    }

    pub fn operatorPlanForNode(self: *const PartitionPlan, node_id: NodeId) ?operator_plan.OperatorPlan {
        if (node_id == null_node) return null;
        const index: usize = @intCast(node_id);
        if (index >= self.node_operator_plans.len) return null;
        return self.node_operator_plans[index];
    }
};

/// Partition a graph across backends based on their capabilities.
///
/// `capabilities` should be sorted by priority (highest first). The last
/// entry should be the fallback backend that supports all ops.
pub fn partition(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    capabilities: []const Capability,
) !PartitionPlan {
    return partitionWithOptions(allocator, graph, capabilities, .{});
}

pub fn partitionWithOptions(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    capabilities: []const Capability,
    options: PartitionOptions,
) !PartitionPlan {
    const count = graph.nodeCount();
    if (count == 0 or capabilities.len == 0) {
        const empty_partitions = try allocator.alloc(Partition, 0);
        errdefer allocator.free(empty_partitions);
        const empty_assignment = try allocator.alloc(u32, 0);
        errdefer allocator.free(empty_assignment);
        const empty_operator_plans = try allocator.alloc(?operator_plan.OperatorPlan, 0);
        return .{
            .partitions = empty_partitions,
            .node_assignment = empty_assignment,
            .node_operator_plans = empty_operator_plans,
            .allocator = allocator,
        };
    }

    const tensor_descs = try buildTensorDescriptors(allocator, graph, options.tensor_descs);
    defer allocator.free(tensor_descs);

    // Step 1: Assign each node to the highest-priority backend that
    // supports it.
    const backend_assignment = try allocator.alloc(BackendKind, count);
    defer allocator.free(backend_assignment);
    const node_operator_plans = try allocator.alloc(?operator_plan.OperatorPlan, count);
    errdefer allocator.free(node_operator_plans);
    @memset(node_operator_plans, null);

    for (0..count) |i| {
        const n = graph.node(@intCast(i));
        const assignment = assignNode(graph, @intCast(i), n.op, capabilities, tensor_descs, options.diagnostics);
        backend_assignment[i] = assignment.backend;
        node_operator_plans[i] = assignment.operator_plan;
    }

    // Step 2: Group contiguous runs of the same backend into partitions.
    // Since the graph is in topological order, we can greedily merge
    // adjacent nodes with the same backend assignment.
    var partition_ids = try allocator.alloc(u32, count);
    defer allocator.free(partition_ids);

    var num_partitions: u32 = 0;
    var current_backend = backend_assignment[0];
    partition_ids[0] = 0;
    num_partitions = 1;

    for (1..count) |i| {
        if (backend_assignment[i] != current_backend or
            shouldForcePartitionBoundary(graph, backend_assignment[i], @intCast(i - 1), @intCast(i)))
        {
            // Check if we should merge with this partition or start new.
            // Start a new partition when the backend changes.
            current_backend = backend_assignment[i];
            num_partitions += 1;
        }
        partition_ids[i] = num_partitions - 1;
    }

    // Step 3: Build partition structures.
    // Count nodes per partition.
    const partition_counts = try allocator.alloc(u32, num_partitions);
    defer allocator.free(partition_counts);
    @memset(partition_counts, 0);
    for (partition_ids[0..count]) |pid| partition_counts[@intCast(pid)] += 1;

    // Collect node IDs per partition.
    const partitions = try allocator.alloc(Partition, num_partitions);
    // Zero-initialize so errdefer can safely free all entries.
    for (partitions) |*p| {
        p.* = .{ .backend = undefined, .node_ids = &.{}, .external_inputs = &.{} };
    }
    errdefer {
        for (partitions[0..num_partitions]) |p| {
            allocator.free(p.node_ids);
            allocator.free(p.external_inputs);
        }
        allocator.free(partitions);
    }

    // Allocate node_ids arrays.
    var offsets = try allocator.alloc(u32, num_partitions);
    defer allocator.free(offsets);
    @memset(offsets, 0);

    for (0..num_partitions) |pid| {
        const ids = try allocator.alloc(NodeId, partition_counts[pid]);
        partitions[pid] = .{
            .backend = undefined,
            .node_ids = ids,
            .external_inputs = &.{},
        };
    }

    // Fill node_ids in topological order.
    for (0..count) |i| {
        const pid: usize = @intCast(partition_ids[i]);
        @constCast(partitions[pid].node_ids)[@intCast(offsets[pid])] = @intCast(i);
        offsets[pid] += 1;
    }

    // Set backend for each partition from first node.
    for (0..num_partitions) |pid| {
        partitions[pid].backend = backend_assignment[@as(usize, @intCast(partitions[pid].node_ids[0]))];
    }

    // Step 4: Compute external inputs for each partition.
    for (0..num_partitions) |pid| {
        var ext_inputs = std.ArrayListUnmanaged(ExternalInput).empty;
        errdefer ext_inputs.deinit(allocator);

        for (partitions[pid].node_ids) |nid| {
            const n = graph.node(nid);
            for (n.getInputs()) |inp| {
                if (inp == null_node or inp >= count) continue;
                const inp_partition: usize = @intCast(partition_ids[@intCast(inp)]);
                if (inp_partition != pid) {
                    // Check if we already recorded this input.
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
                            .source_partition = @intCast(inp_partition),
                        });
                    }
                }
            }
        }

        partitions[pid].external_inputs = try ext_inputs.toOwnedSlice(allocator);
    }

    // Build caller's node_assignment array.
    const node_assignment = try allocator.alloc(u32, count);
    @memcpy(node_assignment, partition_ids[0..count]);

    return .{
        .partitions = partitions,
        .node_assignment = node_assignment,
        .node_operator_plans = node_operator_plans,
        .allocator = allocator,
    };
}

const NodeAssignmentDecision = struct {
    backend: BackendKind,
    operator_plan: ?operator_plan.OperatorPlan = null,
};

fn assignNode(
    graph: *const Graph,
    node_id: NodeId,
    op: OpCode,
    capabilities: []const Capability,
    tensor_descs: ?[]const ?contracts.TensorDesc,
    diagnostics: ?*CapabilityDiagnostics,
) NodeAssignmentDecision {
    const query = CapabilityQuery{
        .graph = graph,
        .node_id = node_id,
        .op = op,
        .tensor_descs = tensor_descs,
    };
    var best_backend: ?BackendKind = null;
    var best_priority: u8 = 0;
    var best_cost: u64 = 0;
    var best_operator_plan: ?operator_plan.OperatorPlan = null;

    // Walk capabilities in order (highest priority first).
    for (capabilities) |cap| {
        const decision = cap.decision(query);
        if (diagnostics) |d| d.record(decision);
        if (!decision.can_execute or !decision.should_execute) continue;
        if (best_backend == null or cap.priority > best_priority or
            (cap.priority == best_priority and decision.estimated_cost < best_cost))
        {
            best_backend = cap.backend;
            best_priority = cap.priority;
            best_cost = decision.estimated_cost;
            best_operator_plan = decision.operator_plan;
        }
    }
    if (best_backend) |backend| return .{
        .backend = backend,
        .operator_plan = best_operator_plan,
    };
    // Fallback: last capability should always match.
    // If nothing matched, default to native.
    return .{ .backend = .native };
}

pub fn buildTensorDescriptors(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    seeds: ?[]const ?contracts.TensorDesc,
) ![]?contracts.TensorDesc {
    const count: usize = @intCast(graph.nodeCount());
    if (seeds) |seed_descs| {
        if (seed_descs.len != count) return error.InvalidTensorDescriptorTable;
    }

    const descs = try allocator.alloc(?contracts.TensorDesc, count);
    errdefer allocator.free(descs);
    @memset(descs, null);

    for (0..count) |i| {
        const node_id: NodeId = @intCast(i);
        const n = graph.node(node_id);
        if (seeds) |seed_descs| {
            if (seed_descs[i]) |seed| {
                if (!seed.shape.eq(n.output_shape)) return error.TensorDescriptorShapeMismatch;
                descs[i] = seed;
                continue;
            }
        }
        descs[i] = inferTensorDesc(graph, descs, node_id);
        if (descs[i]) |desc| {
            if (!desc.shape.eq(n.output_shape)) return error.TensorDescriptorShapeMismatch;
        }
    }

    return descs;
}

pub fn allocTensorDescriptorSeeds(
    allocator: std.mem.Allocator,
    graph: *const Graph,
) ![]?contracts.TensorDesc {
    const seeds = try allocator.alloc(?contracts.TensorDesc, @intCast(graph.nodeCount()));
    @memset(seeds, null);
    return seeds;
}

pub fn seedTensorDescriptor(
    seeds: []?contracts.TensorDesc,
    graph: *const Graph,
    node_id: NodeId,
    desc: contracts.TensorDesc,
) !void {
    const index: usize = @intCast(node_id);
    if (index >= seeds.len or index >= graph.nodeCount()) return error.InvalidTensorDescriptorTable;
    if (!desc.shape.eq(graph.node(node_id).output_shape)) return error.TensorDescriptorShapeMismatch;
    seeds[index] = desc;
}

pub fn seedParameterResidency(
    seeds: []?contracts.TensorDesc,
    graph: *const Graph,
    node_id: NodeId,
    backend: BackendKind,
    device_id: DeviceId,
) !void {
    const n = graph.node(node_id);
    switch (n.op) {
        .parameter => {},
        else => return error.ExpectedParameterNode,
    }
    var desc = contracts.TensorDesc.init(n.output_shape, tensorStorageForBackend(backend));
    desc.resident_backend = backend;
    desc.device_id = @intCast(device_id);
    try seedTensorDescriptor(seeds, graph, node_id, desc);
}

pub fn seedConstantResidency(
    seeds: []?contracts.TensorDesc,
    graph: *const Graph,
    node_id: NodeId,
    backend: BackendKind,
    device_id: DeviceId,
) !void {
    const n = graph.node(node_id);
    switch (n.op) {
        .constant => {},
        else => return error.ExpectedConstantNode,
    }
    var desc = contracts.TensorDesc.init(n.output_shape, tensorStorageForBackend(backend));
    desc.resident_backend = backend;
    desc.device_id = @intCast(device_id);
    try seedTensorDescriptor(seeds, graph, node_id, desc);
}

pub fn seedAllParameterResidency(
    seeds: []?contracts.TensorDesc,
    graph: *const Graph,
    backend: BackendKind,
    device_id: DeviceId,
) !void {
    for (graph.parameters.items) |param_id| {
        try seedParameterResidency(seeds, graph, param_id, backend, device_id);
    }
}

pub fn seedAllUploadableResidency(
    seeds: []?contracts.TensorDesc,
    graph: *const Graph,
    backend: BackendKind,
    device_id: DeviceId,
) !void {
    try seedAllParameterResidency(seeds, graph, backend, device_id);
    for (0..graph.nodeCount()) |i| {
        const node_id: NodeId = @intCast(i);
        switch (graph.node(node_id).op) {
            .constant => try seedConstantResidency(seeds, graph, node_id, backend, device_id),
            .fused_from_float32, .fused_zero_tensor => {
                var desc = contracts.TensorDesc.init(graph.node(node_id).output_shape, tensorStorageForBackend(backend));
                desc.resident_backend = backend;
                desc.device_id = @intCast(device_id);
                try seedTensorDescriptor(seeds, graph, node_id, desc);
            },
            else => {},
        }
    }
}

pub const AttentionKvDescriptorSeed = struct {
    kv_format: quant_matmul.AttentionKvFormat,
    attention_storage: quant_matmul.AttentionStorage,
    backend: BackendKind,
    device_id: DeviceId = 0,
    layer_index: ?usize = null,
};

pub fn seedAttentionKvDescriptors(
    seeds: []?contracts.TensorDesc,
    graph: *const Graph,
    seed: AttentionKvDescriptorSeed,
) !usize {
    var seeded: usize = 0;
    for (0..graph.nodeCount()) |i| {
        const node_id: NodeId = @intCast(i);
        const n = graph.node(node_id);
        const attrs = switch (n.op) {
            .fused_sdpa => |attrs| attrs,
            .fused_causal_self_attention => |attrs| attrs,
            .fused_gqa_causal_attention => |attrs| attrs,
            else => continue,
        };
        if (!attentionLayerMatches(attrs.layer_index, seed.layer_index, n.op)) continue;
        const inputs = n.getInputs();
        if (inputs.len < 3) continue;
        try seedAttentionKvInputDescriptor(seeds, graph, inputs[1], seed);
        try seedAttentionKvInputDescriptor(seeds, graph, inputs[2], seed);
        seeded += 2;
    }
    return seeded;
}

pub fn seedAttentionKvDescriptorsFromContext(
    seeds: []?contracts.TensorDesc,
    graph: *const Graph,
    attention: ?contracts.AttentionContext,
    backend: BackendKind,
    device_id: DeviceId,
) !usize {
    const attn = attention orelse return 0;
    const meta = attentionKvDescriptorMeta(attn) orelse return 0;
    return seedAttentionKvDescriptors(seeds, graph, .{
        .kv_format = meta.kv_format,
        .attention_storage = meta.attention_storage,
        .backend = backend,
        .device_id = device_id,
        .layer_index = attn.layer_index,
    });
}

fn attentionLayerMatches(layer_index: u32, requested: ?usize, op: OpCode) bool {
    const dynamic_layer = layer_index == std.math.maxInt(u32);
    if (requested) |target| {
        if (!dynamic_layer) return layer_index == target;
        return switch (op) {
            .fused_causal_self_attention, .fused_gqa_causal_attention => true,
            .fused_sdpa => false,
            else => false,
        };
    }
    return true;
}

fn seedAttentionKvInputDescriptor(
    seeds: []?contracts.TensorDesc,
    graph: *const Graph,
    node_id: NodeId,
    seed: AttentionKvDescriptorSeed,
) !void {
    if (node_id == null_node) return;
    const n = graph.node(node_id);
    var desc = if (inputDesc(seeds, node_id)) |existing|
        existing
    else
        contracts.TensorDesc.init(n.output_shape, tensorStorageForBackend(seed.backend));
    desc.storage = tensorStorageForBackend(seed.backend);
    desc.resident_backend = seed.backend;
    desc.device_id = @intCast(seed.device_id);
    desc.attention_kv_format = seed.kv_format;
    desc.attention_storage = seed.attention_storage;
    try seedTensorDescriptor(seeds, graph, node_id, desc);
}

fn attentionKvDescriptorMeta(attention: contracts.AttentionContext) ?struct {
    kv_format: quant_matmul.AttentionKvFormat,
    attention_storage: quant_matmul.AttentionStorage,
} {
    if (attention.kv_cache) |kv| {
        if (attentionKvDTypeForCache(kv, attention.kv_manager, attention.kv_storage)) |dtype| {
            return .{
                .kv_format = attentionKvFormatFromDType(dtype),
                .attention_storage = .paged,
            };
        }
    }
    if (attention.kv_batch) |batch| {
        for (batch) |item| {
            if (attentionKvDTypeForCache(item.kv_cache, item.kv_manager, item.kv_storage)) |dtype| {
                return .{
                    .kv_format = attentionKvFormatFromDType(dtype),
                    .attention_storage = .paged,
                };
            }
        }
    }
    return null;
}

fn attentionKvDTypeForCache(
    kv: contracts.KvCacheView,
    manager: ?*runtime_root.kv.manager.KvManager,
    storage_runtime: ?*runtime_root.kv.storage_runtime.KvStorageRuntime,
) ?kv_pool.KvDType {
    const storage = storage_runtime orelse kv.kv_storage;
    if (storage) |s| {
        const pool = s.getPool(kv.pool_id) orelse return null;
        return pool.config.dtype;
    }
    if (manager) |m| {
        const pool = m.getPool(kv.pool_id) orelse return null;
        return pool.config.dtype;
    }
    return null;
}

fn attentionKvFormatFromDType(dtype: kv_pool.KvDType) quant_matmul.AttentionKvFormat {
    return switch (dtype) {
        .f32 => .f32,
        .polar4 => .polar4,
        .turbo3 => .turbo3,
        else => .quantized,
    };
}

pub fn seedParameterQuantFormatByName(
    seeds: []?contracts.TensorDesc,
    graph: *const Graph,
    name: []const u8,
    format: quant_matmul.Format,
) !bool {
    const param_id = findParameterByName(graph, name) orelse return false;
    try seedTensorDescriptor(seeds, graph, param_id, contracts.TensorDesc.packedQuant(graph.node(param_id).output_shape, format));
    return true;
}

pub fn seedParameterQuantGgufTypeByName(
    seeds: []?contracts.TensorDesc,
    graph: *const Graph,
    name: []const u8,
    tensor_type: @import("../gguf/tensor_types.zig").TensorType,
) !bool {
    const format = contracts.quantFormatFromGgufTensorType(tensor_type) orelse return false;
    return seedParameterQuantFormatByName(seeds, graph, name, format);
}

pub fn tensorStorageForBackend(backend: BackendKind) contracts.TensorStorageClass {
    return switch (backend) {
        .native, .graph => .host_dense,
        .webgpu => .webgpu_buffer,
        .metal => .metal_buffer,
        .onnx => .onnx_tensor,
        .pjrt => .pjrt_buffer,
        .cuda => .cuda_buffer,
        else => .unknown,
    };
}

fn findParameterByName(graph: *const Graph, name: []const u8) ?NodeId {
    for (graph.parameters.items) |param_id| {
        const n = graph.node(param_id);
        switch (n.op) {
            .parameter => {
                if (std.mem.eql(u8, graph.parameterName(n), name)) return param_id;
            },
            else => {},
        }
    }
    return null;
}

fn inferTensorDesc(graph: *const Graph, descs: []const ?contracts.TensorDesc, node_id: NodeId) contracts.TensorDesc {
    const n = graph.node(node_id);
    return switch (n.op) {
        .parameter => contracts.TensorDesc.init(n.output_shape, .runtime_input),
        .constant => contracts.TensorDesc.init(n.output_shape, .constant),
        .reshape => inferViewDesc(graph, descs, node_id, n.inputs[0], contracts.TensorStrides.dense(n.output_shape)),
        .transpose => |attrs| inferTransposeDesc(graph, descs, node_id, n.inputs[0], attrs),
        .slice => inferViewDesc(graph, descs, node_id, n.inputs[0], inputStrides(descs, n.inputs[0]) orelse contracts.TensorStrides.none()),
        .broadcast_in_dim => inferViewDesc(graph, descs, node_id, n.inputs[0], contracts.TensorStrides.none()),
        .concat_prim => inferConcatDesc(graph, descs, node_id),
        .shape_of, .range => contracts.TensorDesc.init(n.output_shape, .host_dense),
        else => inferComputeDesc(graph, descs, node_id),
    };
}

fn inferConcatDesc(graph: *const Graph, descs: []const ?contracts.TensorDesc, node_id: NodeId) contracts.TensorDesc {
    const n = graph.node(node_id);
    const inputs = n.getInputs();
    var quant_format: ?quant_matmul.Format = null;
    var all_packed_quant = inputs.len > 0;

    for (inputs) |input_id| {
        const desc = inputDesc(descs, input_id) orelse {
            all_packed_quant = false;
            break;
        };
        const format = desc.quant_format orelse {
            all_packed_quant = false;
            break;
        };
        if (!desc.isPackedQuant()) {
            all_packed_quant = false;
            break;
        }
        if (quant_format) |existing| {
            if (existing != format) {
                all_packed_quant = false;
                break;
            }
        } else {
            quant_format = format;
        }
    }

    if (all_packed_quant) {
        return contracts.TensorDesc.packedQuant(n.output_shape, quant_format.?);
    }
    return inferComputeDesc(graph, descs, node_id);
}

fn inferComputeDesc(graph: *const Graph, descs: []const ?contracts.TensorDesc, node_id: NodeId) contracts.TensorDesc {
    const n = graph.node(node_id);
    var result = contracts.TensorDesc.init(n.output_shape, .unknown);
    var resident_backend: ?BackendKind = null;
    var device_id: u32 = 0;

    for (n.getInputs()) |input_id| {
        const desc = inputDesc(descs, input_id) orelse continue;
        const backend = desc.resident_backend orelse switch (desc.storage) {
            .webgpu_buffer => BackendKind.webgpu,
            .metal_buffer => BackendKind.metal,
            .onnx_tensor => BackendKind.onnx,
            .pjrt_buffer => BackendKind.pjrt,
            .cuda_buffer => BackendKind.cuda,
            else => continue,
        };
        if (resident_backend) |existing| {
            if (existing != backend or device_id != desc.device_id) return result;
        } else {
            resident_backend = backend;
            device_id = desc.device_id;
        }
    }

    if (resident_backend) |backend| {
        result.resident_backend = backend;
        result.device_id = device_id;
        result.storage = tensorStorageForBackend(backend);
    }
    return result;
}

fn inferViewDesc(
    graph: *const Graph,
    descs: []const ?contracts.TensorDesc,
    node_id: NodeId,
    input_id: NodeId,
    strides: contracts.TensorStrides,
) contracts.TensorDesc {
    const n = graph.node(node_id);
    const source_desc = inputDesc(descs, input_id);
    var desc = contracts.TensorDesc.view(
        n.output_shape,
        input_id,
        strides,
        if (source_desc) |desc| desc.resident_backend else null,
        if (source_desc) |desc| desc.device_id else 0,
    );
    if (source_desc) |source| {
        desc.quant_format = source.quant_format;
        desc.attention_kv_format = source.attention_kv_format;
        desc.attention_storage = source.attention_storage;
    }
    return desc;
}

fn inferTransposeDesc(
    graph: *const Graph,
    descs: []const ?contracts.TensorDesc,
    node_id: NodeId,
    input_id: NodeId,
    attrs: ml.graph.node.TransposeAttrs,
) contracts.TensorDesc {
    const input_shape = graph.node(input_id).output_shape;
    var perm_buf: [ml.graph.shape.max_rank]u8 = undefined;
    const perm = transpose_utils.effectivePerm(attrs, input_shape.rank(), &perm_buf);
    var strides = contracts.TensorStrides{ .rank = @intCast(perm.len) };
    if (inputStrides(descs, input_id)) |src_strides| {
        for (perm, 0..) |src_axis, axis| {
            strides.values[axis] = if (src_axis < src_strides.rank) src_strides.values[src_axis] else 0;
        }
    }
    return inferViewDesc(graph, descs, node_id, input_id, strides);
}

fn inputDesc(descs: []const ?contracts.TensorDesc, input_id: NodeId) ?contracts.TensorDesc {
    if (input_id == null_node) return null;
    const index: usize = @intCast(input_id);
    if (index >= descs.len) return null;
    return descs[index];
}

fn inputStrides(descs: []const ?contracts.TensorDesc, input_id: NodeId) ?contracts.TensorStrides {
    if (inputDesc(descs, input_id)) |desc| return desc.strides;
    return null;
}

fn shouldForcePartitionBoundary(
    graph: *const Graph,
    backend: BackendKind,
    prev_node_id: NodeId,
    next_node_id: NodeId,
) bool {
    _ = graph;
    _ = backend;
    _ = prev_node_id;
    _ = next_node_id;
    return false;
}

/// Standard capability: supports all fused ops (for native/MLX backends
/// that implement the full VTable).
pub fn supportsAll(_: OpCode) bool {
    return true;
}

pub fn decideNative(query: CapabilityQuery) CapabilityDecision {
    return CapabilityDecision.acceptCost(nodeElementCost(query.graph, query.node_id));
}

pub fn decideBlasAccelerate(query: CapabilityQuery) CapabilityDecision {
    if (!blasCandidate(query)) return CapabilityDecision.reject(.unsupported_op);
    if (nodeHasPackedQuantInput(query)) return CapabilityDecision.reject(.missing_quant_kernel);
    if (!nodeInputsAreDenseF32HostOrUnknown(query)) return CapabilityDecision.reject(.wrong_storage);

    const cost = nodeComputeCost(query.graph, query.node_id);
    if (cost < blasMinComputeCost) return CapabilityDecision.unprofitable();
    return CapabilityDecision.acceptCost(blasEstimatedCost(query));
}

/// Capability filter: supports only fused ops (not primitives).
/// Useful for backends that implement the ComputeBackend VTable but
/// don't handle lowered primitive ops.
pub fn supportsFusedOnly(op: OpCode) bool {
    return op.isFused();
}

/// Capability filter: supports linear + norm + activation ops.
/// Typical for backends that excel at these fused patterns.
pub fn supportsLinearNormActivation(op: OpCode) bool {
    return switch (op) {
        .fused_linear,
        .fused_linear_no_bias,
        .fused_linear_no_bias_pair,
        .fused_rms_norm,
        .fused_layer_norm,
        .fused_gelu,
        .fused_relu,
        .fused_silu,
        .fused_quick_gelu,
        .fused_sigmoid,
        .fused_tanh_act,
        .fused_elem_add,
        .fused_elem_multiply,
        .fused_from_float32,
        .fused_to_float32,
        .fused_embedding_lookup,
        .fused_rope,
        .reshape,
        => true,
        else => false,
    };
}

/// Capability filter: supports ops compilable to HLO for PJRT/TPU.
/// Superset of supportsLinearNormActivation — also includes embedding
/// and concatenation ops that the HLO compiler handles.
pub fn supportsPjrt(op: OpCode) bool {
    return switch (op) {
        .fused_gqa_causal_attention => |attrs| blk: {
            const num_kv_heads = if (attrs.num_kv_heads > 0) attrs.num_kv_heads else attrs.num_heads;
            break :blk attrs.batch == 1 and
                !attrs.skip_kv_write and
                attrs.seq_len > 0 and
                attrs.num_heads > 0 and
                num_kv_heads > 0 and
                attrs.num_heads % num_kv_heads == 0 and
                attrs.head_dim > 0;
        },
        .fused_linear,
        .fused_linear_no_bias,
        .fused_linear_no_bias_pair,
        .fused_rms_norm,
        .fused_layer_norm,
        .fused_gelu,
        .fused_relu,
        .fused_silu,
        .fused_quick_gelu,
        .fused_sigmoid,
        .fused_tanh_act,
        .fused_elem_add,
        .fused_elem_multiply,
        .fused_embedding_lookup,
        .fused_concat,
        .fused_from_float32,
        .fused_to_float32,
        .fused_rope,
        .convert_dtype,
        .slice,
        => true,
        else => false,
    };
}

/// Capability filter: supports attention ops.
/// Typical for MLX which has efficient paged KV attention.
pub fn supportsAttention(op: OpCode) bool {
    return switch (op) {
        .fused_causal_self_attention,
        .fused_gqa_causal_attention,
        .fused_sdpa,
        .fused_cross_attention,
        .fused_rope,
        => true,
        else => false,
    };
}

fn blasCandidate(query: CapabilityQuery) bool {
    return switch (query.op) {
        .dot_general,
        .fused_linear,
        .fused_linear_no_bias,
        .fused_linear_no_bias_pair,
        => query.graph.node(query.node_id).output_shape.dtype == .f32,
        else => false,
    };
}

const blasMinComputeCost: u64 = 32 * 1024;

fn nodeElementCost(graph: *const Graph, node_id: NodeId) u64 {
    const shape = graph.node(node_id).output_shape;
    const elems = shape.maxElements() orelse shape.numElements() orelse 1;
    if (elems <= 0) return 1;
    return @intCast(elems);
}

fn tensorByteSize(shape: ml.graph.Shape) u64 {
    const elems = shape.maxElements() orelse shape.numElements() orelse 1;
    if (elems <= 0) return @intCast(shape.dtype.byteSize());
    return checkedMul(@intCast(elems), @intCast(shape.dtype.byteSize()));
}

fn descByteSize(desc: contracts.TensorDesc) u64 {
    return tensorByteSize(desc.shape);
}

fn nodeComputeCost(graph: *const Graph, node_id: NodeId) u64 {
    const n = graph.node(node_id);
    return switch (n.op) {
        .fused_linear, .fused_linear_no_bias, .fused_linear_no_bias_pair => |attrs| mul3Cost(attrs.rows, attrs.in_dim, attrs.out_dim),
        .dot_general => |attrs| dotGeneralCost(graph, node_id, attrs) orelse nodeElementCost(graph, node_id),
        else => nodeElementCost(graph, node_id),
    };
}

fn dotGeneralCost(graph: *const Graph, node_id: NodeId, attrs: ml.graph.node.DotGeneralAttrs) ?u64 {
    if (attrs.num_contracting != 1) return null;
    const n = graph.node(node_id);
    if (n.num_inputs < 2) return null;
    const lhs = graph.node(n.inputs[0]).output_shape;
    const lhs_axis = attrs.lhs_contracting[0];
    if (lhs_axis >= lhs.rank()) return null;
    const k = lhs.dims[lhs_axis];
    if (k <= 0) return null;
    return checkedMul(nodeElementCost(graph, node_id), @intCast(k));
}

fn mul3Cost(a: u32, b: u32, c: u32) u64 {
    return checkedMul(checkedMul(a, b), c);
}

fn checkedMul(a: u64, b: u64) u64 {
    return std.math.mul(u64, a, b) catch std.math.maxInt(u64);
}

fn checkedAdd(a: u64, b: u64) u64 {
    return std.math.add(u64, a, b) catch std.math.maxInt(u64);
}

fn blasEstimatedCost(query: CapabilityQuery) u64 {
    return checkedAdd(nodeComputeCost(query.graph, query.node_id) / 8, nodeInputBytes(query));
}

fn nodeInputBytes(query: CapabilityQuery) u64 {
    const descs = query.tensor_descs orelse return 0;
    const n = query.graph.node(query.node_id);
    var total: u64 = 0;
    for (n.getInputs()) |input_id| {
        if (inputDesc(descs, input_id)) |desc| total = checkedAdd(total, descByteSize(desc));
    }
    return total;
}

fn nodeHasPackedQuantInput(query: CapabilityQuery) bool {
    const descs = query.tensor_descs orelse return false;
    const n = query.graph.node(query.node_id);
    for (n.getInputs()) |input_id| {
        if (inputDesc(descs, input_id)) |desc| {
            if (desc.isPackedQuant()) return true;
        }
    }
    return false;
}

fn nodeInputsAreHostOrUnknown(query: CapabilityQuery) bool {
    const descs = query.tensor_descs orelse return true;
    const n = query.graph.node(query.node_id);
    for (n.getInputs()) |input_id| {
        if (inputDesc(descs, input_id)) |desc| {
            if (desc.storage == .unknown or desc.storage == .metadata_view) continue;
            if (!desc.isHostResident()) return false;
        }
    }
    return true;
}

fn nodeInputsAreDenseF32HostOrUnknown(query: CapabilityQuery) bool {
    const descs = query.tensor_descs orelse return true;
    const n = query.graph.node(query.node_id);
    for (n.getInputs()) |input_id| {
        if (inputDesc(descs, input_id)) |desc| {
            if (!tensorDescIsDenseF32HostOrUnknown(desc)) return false;
        }
    }
    return true;
}

fn tensorDescIsDenseF32HostOrUnknown(desc: contracts.TensorDesc) bool {
    if (desc.storage == .unknown) return true;
    if (desc.isPackedQuant()) return false;
    if (desc.shape.dtype != .f32) return false;
    switch (desc.storage) {
        .runtime_input, .constant, .host_f32, .host_dense => {},
        else => return false,
    }
    if (desc.strides.rank == 0) return true;
    const dense = contracts.TensorStrides.dense(desc.shape);
    return std.mem.eql(i64, desc.strides.asSlice(), dense.asSlice());
}

fn descIsMetalResident(desc: contracts.TensorDesc) bool {
    return desc.storage == .metal_buffer or desc.resident_backend == .metal;
}

// ── Tests ─────────────────────────────────────────────────────────────

test "supportsAttention references current graph attention tags" {
    const gqa: OpCode = .{ .fused_gqa_causal_attention = .{
        .batch = 1,
        .seq_len = 1,
        .num_heads = 1,
        .num_kv_heads = 1,
        .head_dim = 8,
    } };
    const rope: OpCode = .{ .fused_rope = .{
        .seq_len = 1,
        .head_dim = 8,
        .rope_dim = 8,
        .theta = 10000,
        .freq_scale = 1,
        .position_offset = 0,
        .consecutive_pairs = false,
    } };
    const linear: OpCode = .{ .fused_linear_no_bias = .{ .rows = 1, .in_dim = 8, .out_dim = 8 } };

    try std.testing.expect(supportsAttention(gqa));
    try std.testing.expect(supportsAttention(rope));
    try std.testing.expect(!supportsAttention(linear));
}

const Builder = ml.graph.Builder;
const Shape = ml.graph.Shape;

test "single backend partitions everything together" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const out = try b.gelu(x);
    try g.markOutput(out);

    const caps = [_]Capability{
        .{ .backend = .native, .priority = 0, .supports = &supportsAll },
    };

    var plan = try partition(allocator, &g, &caps);
    defer plan.deinit();

    try std.testing.expectEqual(@as(usize, 1), plan.partitions.len);
    try std.testing.expectEqual(BackendKind.native, plan.partitions[0].backend);
    // gelu decomposes into many primitive nodes + fused node, all on one backend.
    try std.testing.expect(plan.partitions[0].node_ids.len >= 2);
}

test "two backends split on capability" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // param -> rms_norm -> gelu
    // If "accelerator" only supports gelu/relu/silu, rms_norm goes to fallback.
    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const w = try b.parameter("w", Shape.init(.f32, &.{4}));
    const normed = try b.rmsNorm(x, w, 4, 1e-5);
    const out = try b.gelu(normed);
    try g.markOutput(out);

    const onlyActivations = struct {
        fn f(op: OpCode) bool {
            return switch (op) {
                .fused_gelu, .fused_relu, .fused_silu => true,
                else => false,
            };
        }
    }.f;

    const caps = [_]Capability{
        .{ .backend = .metal, .priority = 10, .supports = &onlyActivations },
        .{ .backend = .native, .priority = 0, .supports = &supportsAll },
    };

    var plan = try partition(allocator, &g, &caps);
    defer plan.deinit();

    // Should have at least 2 partitions: native (param+rms_norm) and metal (gelu).
    try std.testing.expect(plan.partitions.len >= 2);

    // The gelu partition should be assigned to metal.
    const node_count = g.nodeCount();
    const gelu_id: NodeId = @intCast(node_count - 1);
    const gelu_partition = plan.node_assignment[gelu_id];
    try std.testing.expectEqual(BackendKind.metal, plan.partitions[gelu_partition].backend);

    // The rms_norm partition should have external inputs from gelu's input.
    // The gelu partition should have external inputs from the norm partition.
    const gelu_p = plan.partitions[gelu_partition];
    try std.testing.expect(gelu_p.external_inputs.len > 0);
}

test "rich capability decision can reject profitable placement" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const out = try b.gelu(x);
    try g.markOutput(out);

    const acceleratorDecision = struct {
        fn f(query: CapabilityQuery) CapabilityDecision {
            _ = query.graph;
            _ = query.node_id;
            return switch (query.op) {
                .fused_gelu => CapabilityDecision.unprofitable(),
                else => CapabilityDecision.reject(.unsupported_op),
            };
        }
    }.f;

    const caps = [_]Capability{
        .{ .backend = .metal, .priority = 10, .decide = &acceleratorDecision },
        .{ .backend = .native, .priority = 0, .supports = &supportsAll },
    };

    var plan = try partition(allocator, &g, &caps);
    defer plan.deinit();

    for (plan.partitions) |part| {
        try std.testing.expectEqual(BackendKind.native, part.backend);
    }
}

test "tensor descriptor inference preserves runtime constants and views" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 3 }));
    const c = try b.tensorConst(&.{ 1, 2, 3, 4, 5, 6 }, Shape.init(.f32, &.{ 2, 3 }));
    const reshaped = try b.reshape(x, Shape.init(.f32, &.{ 3, 2 }));
    const transposed = try b.transpose(c, &.{ 1, 0 });
    try g.markOutput(reshaped);
    try g.markOutput(transposed);

    const descs = try buildTensorDescriptors(allocator, &g, null);
    defer allocator.free(descs);

    try std.testing.expectEqual(contracts.TensorStorageClass.runtime_input, descs[@intCast(x)].?.storage);
    try std.testing.expectEqual(contracts.TensorStorageClass.constant, descs[@intCast(c)].?.storage);
    try std.testing.expect(descs[@intCast(reshaped)].?.isView());
    try std.testing.expectEqual(@as(?NodeId, x), descs[@intCast(reshaped)].?.view_source);
    try std.testing.expectEqualSlices(i64, &.{ 2, 1 }, descs[@intCast(reshaped)].?.strides.asSlice());
    try std.testing.expect(descs[@intCast(transposed)].?.isView());
    try std.testing.expectEqual(@as(?NodeId, c), descs[@intCast(transposed)].?.view_source);
    try std.testing.expectEqualSlices(i64, &.{ 1, 3 }, descs[@intCast(transposed)].?.strides.asSlice());
}

test "default transpose descriptor uses reverse-axis view strides" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 3, 4 }));
    const transposed = try g.addNode(.{
        .op = .{ .transpose = .{} },
        .output_shape = Shape.init(.f32, &.{ 4, 3, 2 }),
        .inputs = .{ x, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    try g.markOutput(transposed);

    const descs = try buildTensorDescriptors(allocator, &g, null);
    defer allocator.free(descs);

    try std.testing.expect(descs[@intCast(transposed)].?.isView());
    try std.testing.expectEqual(@as(?NodeId, x), descs[@intCast(transposed)].?.view_source);
    try std.testing.expectEqualSlices(i64, &.{ 1, 4, 12 }, descs[@intCast(transposed)].?.strides.asSlice());
}

test "uploadable residency seeding marks parameters and constants device resident" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 3 }));
    const c = try b.tensorConst(&.{ 1, 2, 3, 4, 5, 6 }, Shape.init(.f32, &.{ 2, 3 }));
    const out = try b.add(x, c);
    try g.markOutput(out);

    const seeds = try allocTensorDescriptorSeeds(allocator, &g);
    defer allocator.free(seeds);
    try seedAllUploadableResidency(seeds, &g, .metal, 0);

    const descs = try buildTensorDescriptors(allocator, &g, seeds);
    defer allocator.free(descs);

    try std.testing.expect(descIsMetalResident(descs[@intCast(x)].?));
    try std.testing.expect(descIsMetalResident(descs[@intCast(c)].?));
    try std.testing.expect(descIsMetalResident(descs[@intCast(out)].?));
}

test "packed quant descriptors propagate through concat" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const lhs = try b.parameter("lhs", Shape.init(.f32, &.{ 8, 4 }));
    const rhs = try b.parameter("rhs", Shape.init(.f32, &.{ 8, 4 }));
    const out = try b.concat(lhs, rhs, 0);
    try g.markOutput(out);

    const seeds = try allocTensorDescriptorSeeds(allocator, &g);
    defer allocator.free(seeds);
    try std.testing.expect(try seedParameterQuantFormatByName(seeds, &g, "lhs", .q8_0));
    try std.testing.expect(try seedParameterQuantFormatByName(seeds, &g, "rhs", .q8_0));

    const descs = try buildTensorDescriptors(allocator, &g, seeds);
    defer allocator.free(descs);

    const out_desc = descs[@intCast(out)].?;
    try std.testing.expect(out_desc.isPackedQuant());
    try std.testing.expectEqual(quant_matmul.Format.q8_0, out_desc.quant_format.?);
    try std.testing.expectEqual(contracts.TensorStorageClass.host_packed_quant, out_desc.storage);
}

test "attention kv descriptor seeding tags layer scoped k and v inputs" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const q = try b.parameter("q", Shape.init(.f32, &.{ 4, 2, 8 }));
    const k = try b.parameter("k", Shape.init(.f32, &.{ 2, 3, 8 }));
    const v = try b.parameter("v", Shape.init(.f32, &.{ 2, 3, 8 }));
    _ = try g.addNode(.{
        .op = .{ .fused_sdpa = .{
            .batch = 1,
            .seq_len = 2,
            .kv_seq_len = 3,
            .num_heads = 4,
            .num_kv_heads = 2,
            .head_dim = 8,
            .layer_index = 7,
        } },
        .output_shape = Shape.init(.f32, &.{ 4, 2, 8 }),
        .inputs = .{ q, k, v, null_node },
        .num_inputs = 3,
    });

    const seeds = try allocTensorDescriptorSeeds(allocator, &g);
    defer allocator.free(seeds);
    try seedAllUploadableResidency(seeds, &g, .metal, 0);
    const seeded = try seedAttentionKvDescriptors(seeds, &g, .{
        .kv_format = .polar4,
        .attention_storage = .paged,
        .backend = .metal,
        .device_id = 0,
        .layer_index = 7,
    });
    try std.testing.expectEqual(@as(usize, 2), seeded);

    const k_desc = seeds[@intCast(k)].?;
    const v_desc = seeds[@intCast(v)].?;
    try std.testing.expectEqual(quant_matmul.AttentionKvFormat.polar4, k_desc.attention_kv_format.?);
    try std.testing.expectEqual(quant_matmul.AttentionKvFormat.polar4, v_desc.attention_kv_format.?);
    try std.testing.expectEqual(quant_matmul.AttentionStorage.paged, k_desc.attention_storage.?);
    try std.testing.expectEqual(BackendKind.metal, k_desc.resident_backend.?);
}

test "partition passes inferred tensor descriptors to capability decisions" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 8, 1024 }));
    const out = try b.gelu(x);
    try g.markOutput(out);

    const descriptorAwareDecision = struct {
        fn f(query: CapabilityQuery) CapabilityDecision {
            switch (query.op) {
                .fused_gelu => {},
                else => return CapabilityDecision.reject(.unsupported_op),
            }
            const descs = query.tensor_descs orelse return CapabilityDecision.reject(.wrong_storage);
            const n = query.graph.node(query.node_id);
            const input = n.inputs[0];
            const desc = inputDesc(descs, input) orelse return CapabilityDecision.reject(.wrong_storage);
            if (desc.storage != .runtime_input) return CapabilityDecision.reject(.wrong_storage);
            return CapabilityDecision.acceptCost(1);
        }
    }.f;

    var diagnostics = CapabilityDiagnostics{};
    const caps = [_]Capability{
        .{ .backend = .metal, .priority = 10, .decide = &descriptorAwareDecision },
        .{ .backend = .native, .priority = 0, .decide = &decideNative },
    };

    var plan = try partitionWithOptions(allocator, &g, &caps, .{ .diagnostics = &diagnostics });
    defer plan.deinit();

    const gelu_partition = plan.node_assignment[out];
    try std.testing.expectEqual(BackendKind.metal, plan.partitions[gelu_partition].backend);
    try std.testing.expect(diagnostics.count(.wrong_storage) == 0);
}

test "tiny dense host matmul stays native instead of cblas graph" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 1, 32 }));
    const w = try b.parameter("linear.weight", Shape.init(.f32, &.{ 32, 32 }));
    const out = try b.linearNoBias(x, w, 1, 32, 32);
    try g.markOutput(out);

    var diagnostics = CapabilityDiagnostics{};
    const caps = [_]Capability{
        .{ .backend = .graph, .priority = 10, .decide = &decideBlasAccelerate },
        .{ .backend = .native, .priority = 0, .decide = &decideNative },
    };
    var plan = try partitionWithOptions(allocator, &g, &caps, .{ .diagnostics = &diagnostics });
    defer plan.deinit();

    try std.testing.expectEqual(BackendKind.native, plan.partitions[plan.node_assignment[out]].backend);
    try std.testing.expect(diagnostics.count(.unprofitable_shape) >= 1);
}

test "large dense host matmul goes to cblas graph backend" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 128, 128 }));
    const w = try b.parameter("linear.weight", Shape.init(.f32, &.{ 128, 128 }));
    const out = try b.linearNoBias(x, w, 128, 128, 128);
    try g.markOutput(out);

    var diagnostics = CapabilityDiagnostics{};
    const caps = [_]Capability{
        .{ .backend = .graph, .priority = 10, .decide = &decideBlasAccelerate },
        .{ .backend = .native, .priority = 0, .decide = &decideNative },
    };
    var plan = try partitionWithOptions(allocator, &g, &caps, .{ .diagnostics = &diagnostics });
    defer plan.deinit();

    try std.testing.expectEqual(BackendKind.graph, plan.partitions[plan.node_assignment[out]].backend);
    try std.testing.expectEqual(@as(usize, 0), diagnostics.count(.missing_quant_kernel));
}

test "partition empty graph" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();

    const caps = [_]Capability{
        .{ .backend = .native, .priority = 0, .supports = &supportsAll },
    };

    var plan = try partition(allocator, &g, &caps);
    defer plan.deinit();

    try std.testing.expectEqual(@as(usize, 0), plan.partitions.len);
}

test "external inputs track cross-partition edges" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    // Build: param(native) -> gelu(metal) -> neg(native)
    // gelu should have external input from param.
    // neg should have external input from gelu.
    const x = try b.parameter("x", Shape.init(.f32, &.{4}));
    const activated = try b.gelu(x);
    const out = try b.neg(activated);
    try g.markOutput(out);

    const onlyGelu = struct {
        fn f(op: OpCode) bool {
            return switch (op) {
                .fused_gelu => true,
                else => false,
            };
        }
    }.f;

    const caps = [_]Capability{
        .{ .backend = .metal, .priority = 10, .supports = &onlyGelu },
        .{ .backend = .native, .priority = 0, .supports = &supportsAll },
    };

    var plan = try partition(allocator, &g, &caps);
    defer plan.deinit();

    // Should have 3 partitions: native(param), metal(gelu), native(neg)
    try std.testing.expectEqual(@as(usize, 3), plan.partitions.len);

    // Middle partition (metal/gelu) should have 1 external input from param.
    try std.testing.expectEqual(@as(usize, 1), plan.partitions[1].external_inputs.len);

    // Last partition (native/neg) should have 1 external input from gelu.
    try std.testing.expectEqual(@as(usize, 1), plan.partitions[2].external_inputs.len);
    try std.testing.expectEqual(@as(u32, 1), plan.partitions[2].external_inputs[0].source_partition);
}

test "partition executor default is null" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{4}));
    const out = try b.gelu(x);
    try g.markOutput(out);

    const caps = [_]Capability{
        .{ .backend = .native, .priority = 0, .supports = &supportsAll },
    };

    var plan = try partition(allocator, &g, &caps);
    defer plan.deinit();

    // All partitions should have null executor by default.
    for (plan.partitions) |p| {
        try std.testing.expect(p.executor == null);
    }
}

test "mock partition executor dispatches correctly" {
    // Verify the PartitionExecutor vtable works with a mock.
    const Mock = struct {
        called: bool = false,

        fn execute(
            ctx: *anyopaque,
            values: []?CT,
            _: []DeviceId,
            node_ids: []const NodeId,
            _: DeviceId,
            _: PartitionExecutor.ExecutionContext,
        ) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.called = true;
            // A real executor would fill values for its owned nodes.
            // Here just mark that we were called and set a sentinel.
            for (node_ids) |nid| {
                values[@intCast(nid)] = @ptrFromInt(0xDEADBEEF);
            }
        }

        fn deinitFn(ctx: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.called = false; // reset as deinit signal
        }
    };

    var mock = Mock{};
    const vtable = PartitionExecutor.VTable{
        .execute = &Mock.execute,
        .deinit = &Mock.deinitFn,
    };
    const exec = PartitionExecutor{ .ptr = &mock, .vtable = &vtable };

    // Test execute dispatch.
    var values = [_]?CT{ null, null, null };
    var devices = [_]DeviceId{ 0, 0, 0 };
    const node_ids = [_]NodeId{ 1, 2 };
    try exec.execute(&values, &devices, &node_ids, 0, .{});
    try std.testing.expect(mock.called);
    try std.testing.expectEqual(@as(usize, 0xDEADBEEF), @intFromPtr(values[1].?));
    try std.testing.expectEqual(@as(usize, 0xDEADBEEF), @intFromPtr(values[2].?));

    // Test deinit dispatch.
    exec.deinitExecutor();
    try std.testing.expect(!mock.called); // deinit resets the flag
}
