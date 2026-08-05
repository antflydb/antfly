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
const ml = @import("ml");
const build_options = @import("build_options");
const platform = @import("antfly_platform");

const contracts = @import("backend_contracts.zig");
const ops_mod = @import("../ops/ops.zig");
const interpreter = @import("interpreter.zig");
const partition_mod = @import("partition.zig");
const metal_capabilities = @import("metal_capabilities.zig");
const buffer_plan_mod = @import("buffer_plan.zig");
const operator_plan_mod = @import("operator_plan.zig");
const quant_kernel_compiler = @import("quant_kernel_compiler.zig");
const device_mesh_mod = @import("device_mesh.zig");
const gpu_hosted_store_mod = @import("../ops/gpu_hosted_store.zig");
const metal_compute_mod = @import("../ops/metal_compute.zig");
const metal_tensor_mod = if (build_options.enable_metal) @import("../backends/metal_tensor.zig") else struct {
    pub fn setOwnedAllocationContext(_: []const u8) void {}
    pub fn clearOwnedAllocationContext() void {}
};
const weight_source_mod = @import("../models/weight_source.zig");
const quant_codec = @import("../gguf/quant_codec.zig");
const transpose_utils = @import("transpose_utils.zig");
const metal_runtime_mod = if (build_options.enable_metal) @import("../backends/metal_runtime.zig") else struct {
    pub fn metalDeviceAvailable() bool {
        return false;
    }
};

const Graph = ml.graph.Graph;
const NodeId = ml.graph.NodeId;
const null_node = ml.graph.null_node;
const Shape = ml.graph.Shape;

const CT = contracts.CT;
const ComputeBackend = ops_mod.ComputeBackend;
const PartitionExecutor = partition_mod.PartitionExecutor;
const DeviceId = device_mesh_mod.DeviceId;
const GraphPlanSlot = ops_mod.GraphPlanSlot;
const QuantizedStorage = weight_source_mod.QuantizedStorage;
const OperatorPlan = operator_plan_mod.OperatorPlan;

const max_graph_plan_slots = 26;

/// Upper element-count bound for the graph-output "scalar tail" elision
/// protection. Nodes at most this many elements that feed a graph output
/// (transitively, through equally tiny producers) are never left elided by
/// fused patterns / runtime regions: they are re-executed as plain commands
/// so the loss-reduction tail (scalar masked-BCE sums, the final loss add)
/// always materializes. Large tensors terminate the upstream walk, so real
/// fusion targets (hidden states, attention scores, per-element losses)
/// keep their fused execution.
const graph_output_scalar_tail_max_elems: i64 = 64;

const MetalExecutionKind = enum {
    command,
    metadata_alias,
    descriptor_materialization,
    constant_materialization,
};

const OpExecutionCount = struct {
    name: []const u8 = "",
    count: usize = 0,
    total_ns: u64 = 0,
};

const OpExecutionStats = struct {
    command_counts: [96]OpExecutionCount = [_]OpExecutionCount{.{}} ** 96,
    command_used: usize = 0,
    command_class_counts: [160]CommandExecutionSummary = [_]CommandExecutionSummary{.{}} ** 160,
    command_class_used: usize = 0,
    fallback_counts: [96]OpExecutionCount = [_]OpExecutionCount{.{}} ** 96,
    fallback_used: usize = 0,
    host_output_counts: [96]OpExecutionCount = [_]OpExecutionCount{.{}} ** 96,
    host_output_used: usize = 0,
    host_output_reason_counts: [32]OpExecutionCount = [_]OpExecutionCount{.{}} ** 32,
    host_output_reason_used: usize = 0,
    runtime_region_counts: [32]OpExecutionCount = [_]OpExecutionCount{.{}} ** 32,
    runtime_region_used: usize = 0,
    dot_command_shapes: [128]DotShapeExecutionSummary = [_]DotShapeExecutionSummary{.{}} ** 128,
    dot_command_shape_used: usize = 0,

    fn recordCommand(self: *OpExecutionStats, name: []const u8, elapsed_ns: u64) void {
        recordOpCount(&self.command_counts, &self.command_used, name, elapsed_ns);
    }

    fn recordCommandClass(self: *OpExecutionStats, graph: *const Graph, node_id: NodeId, node_pos: usize, elapsed_ns: u64) void {
        recordCommandExecutionSummary(graph, node_id, node_pos, &self.command_class_counts, &self.command_class_used, elapsed_ns);
    }

    fn recordFallback(self: *OpExecutionStats, name: []const u8, elapsed_ns: u64) void {
        recordOpCount(&self.fallback_counts, &self.fallback_used, name, elapsed_ns);
    }

    fn recordHostOutput(self: *OpExecutionStats, name: []const u8, elapsed_ns: u64) void {
        recordOpCount(&self.host_output_counts, &self.host_output_used, name, elapsed_ns);
    }

    fn recordHostOutputReason(self: *OpExecutionStats, name: []const u8, elapsed_ns: u64) void {
        recordOpCount(&self.host_output_reason_counts, &self.host_output_reason_used, name, elapsed_ns);
    }

    fn recordRuntimeRegion(self: *OpExecutionStats, name: []const u8, elapsed_ns: u64) void {
        recordOpCount(&self.runtime_region_counts, &self.runtime_region_used, name, elapsed_ns);
    }

    fn recordDotCommand(self: *OpExecutionStats, graph: *const Graph, node_id: NodeId, node_pos: usize, elapsed_ns: u64) void {
        recordDotShapeExecutionSummary(graph, node_id, node_pos, &self.dot_command_shapes, &self.dot_command_shape_used, elapsed_ns);
    }
};

const ExecutorLoopProfile = struct {
    nodes: usize = 0,
    executed_nodes: usize = 0,
    partition_view_ns: u64 = 0,
    graph_plan_ns: u64 = 0,
    materialize_runtime_inputs_ns: u64 = 0,
    materialize_parameters_ns: u64 = 0,
    materialize_constants_ns: u64 = 0,
    begin_frame_ns: u64 = 0,
    runtime_region_plan_ns: u64 = 0,
    execution_ns: u64 = 0,
    planned_region_ns: u64 = 0,
    planned_region_hits: usize = 0,
    fused_pattern_ns: u64 = 0,
    fused_pattern_hits: usize = 0,
    command_path_ns: u64 = 0,
    command_path_hits: usize = 0,
    interpreter_ns: u64 = 0,
    interpreter_hits: usize = 0,
    stats_ns: u64 = 0,
    alias_clone_ns: u64 = 0,
    free_expired_ns: u64 = 0,
    submit_frame_ns: u64 = 0,
    boundary_outputs_ns: u64 = 0,

    fn print(self: ExecutorLoopProfile, label: []const u8) void {
        std.debug.print(
            "{s}: nodes={d}:executed={d}:partition_view_ms={d:.3}:graph_plan_ms={d:.3}:runtime_inputs_ms={d:.3}:parameters_ms={d:.3}:constants_ms={d:.3}:begin_frame_ms={d:.3}:runtime_plan_ms={d:.3}:execution_ms={d:.3}:planned_region_ms={d:.3}(hits={d}):fused_pattern_ms={d:.3}(hits={d}):command_path_ms={d:.3}(hits={d}):interpreter_ms={d:.3}(hits={d}):stats_ms={d:.3}:alias_clone_ms={d:.3}:free_expired_ms={d:.3}:submit_frame_ms={d:.3}:boundary_outputs_ms={d:.3}:accounted_ms={d:.3}\n",
            .{
                label,
                self.nodes,
                self.executed_nodes,
                nsToMs(self.partition_view_ns),
                nsToMs(self.graph_plan_ns),
                nsToMs(self.materialize_runtime_inputs_ns),
                nsToMs(self.materialize_parameters_ns),
                nsToMs(self.materialize_constants_ns),
                nsToMs(self.begin_frame_ns),
                nsToMs(self.runtime_region_plan_ns),
                nsToMs(self.execution_ns),
                nsToMs(self.planned_region_ns),
                self.planned_region_hits,
                nsToMs(self.fused_pattern_ns),
                self.fused_pattern_hits,
                nsToMs(self.command_path_ns),
                self.command_path_hits,
                nsToMs(self.interpreter_ns),
                self.interpreter_hits,
                nsToMs(self.stats_ns),
                nsToMs(self.alias_clone_ns),
                nsToMs(self.free_expired_ns),
                nsToMs(self.submit_frame_ns),
                nsToMs(self.boundary_outputs_ns),
                nsToMs(self.partition_view_ns +
                    self.graph_plan_ns +
                    self.materialize_runtime_inputs_ns +
                    self.materialize_parameters_ns +
                    self.materialize_constants_ns +
                    self.begin_frame_ns +
                    self.runtime_region_plan_ns +
                    self.execution_ns +
                    self.stats_ns +
                    self.alias_clone_ns +
                    self.free_expired_ns +
                    self.submit_frame_ns +
                    self.boundary_outputs_ns),
            },
        );
    }
};

const RuntimeRegionKind = enum(u8) {
    none = 0,
    raw_linear_dot,
    raw_linear_pair,
    raw_linear_bias,
    raw_linear_bias_pair,
    lora_linear,
    lora_linear_qkv,
    deberta_attention,
    deberta_ffn_forward,
    deberta_encoder_lora_layer,
    lora_backward,
    low_rank_lora_backward,
    rank_adapter_backward,
    ffn_gelu_backward,
    q_linear,
    linear_qkv,
    grouped_linear_qkv_slice,
    packed_linear_qkv_slice,
    rms_norm_grouped_linear_qkv_slice,
    attention_output_residual,
    rms_norm_gated_ffn_residual,
    gated_ffn_residual,
    ple_residual,
};

const RuntimeRegion = union(RuntimeRegionKind) {
    none: void,
    raw_linear_dot: RawLinearDotPattern,
    raw_linear_pair: RawLinearPairPattern,
    raw_linear_bias: RawLinearBiasPattern,
    raw_linear_bias_pair: RawLinearBiasPairPattern,
    lora_linear: LoraLinearPattern,
    lora_linear_qkv: LoraLinearQkvPattern,
    deberta_attention: DebertaAttentionPattern,
    deberta_ffn_forward: DebertaFfnForwardPattern,
    deberta_encoder_lora_layer: DebertaEncoderLoraLayerPattern,
    lora_backward: LoraBackwardPattern,
    low_rank_lora_backward: LowRankLoraBackwardPattern,
    rank_adapter_backward: RankAdapterBackwardPattern,
    ffn_gelu_backward: FfnGeluBackwardPattern,
    q_linear: QLinearPattern,
    linear_qkv: LinearNoBiasQkvPattern,
    grouped_linear_qkv_slice: GroupedLinearQkvSlicePattern,
    packed_linear_qkv_slice: PackedLinearQkvSlicePattern,
    rms_norm_grouped_linear_qkv_slice: RmsNormGroupedLinearQkvSlicePattern,
    attention_output_residual: AttentionOutputResidualPattern,
    rms_norm_gated_ffn_residual: RmsNormGatedFfnResidualPattern,
    gated_ffn_residual: GatedFfnResidualPattern,
    ple_residual: PleResidualPattern,
};

const PreparedQkvRegion = struct {
    q_slot: usize,
    k_slot: usize,
    v_slot: usize,
};

const PreparedLinearRegion = struct {
    linear_slot: usize,
};

const PreparedLinearPairRegion = struct {
    first_slot: usize,
    second_slot: usize,
};

const PreparedRmsNormGroupedQkvRegion = struct {
    norm_slot: usize,
    qkv: PreparedQkvRegion,
};

const PreparedAttentionOutputResidualRegion = struct {
    linear_slot: usize,
    pre_linear_rms_norm_slot: ?usize = null,
    post_linear_rms_norm_slot: ?usize = null,
};

const PreparedRmsNormGatedFfnResidualRegion = struct {
    norm_slot: usize,
    ffn: PreparedGatedFfnResidualRegion,
};

const PreparedGatedFfnResidualRegion = struct {
    gate_slot: usize,
    up_slot: usize,
    down_slot: usize,
    post_down_rms_norm_slot: ?usize = null,
};

const PreparedPleResidualRegion = struct {
    gate_slot: usize,
    projection_slot: usize,
    post_norm_slot: usize,
};

const PreparedDebertaFfnForwardRegion = struct {
    first_slot: usize,
    second_slot: usize,
};

const PreparedRuntimeRegion = union(RuntimeRegionKind) {
    none: void,
    raw_linear_dot: PreparedLinearRegion,
    raw_linear_pair: PreparedLinearPairRegion,
    raw_linear_bias: PreparedLinearRegion,
    raw_linear_bias_pair: PreparedLinearPairRegion,
    lora_linear: void,
    lora_linear_qkv: void,
    deberta_attention: void,
    deberta_ffn_forward: PreparedDebertaFfnForwardRegion,
    deberta_encoder_lora_layer: PreparedDebertaFfnForwardRegion,
    lora_backward: void,
    low_rank_lora_backward: void,
    rank_adapter_backward: void,
    ffn_gelu_backward: void,
    q_linear: PreparedLinearRegion,
    linear_qkv: PreparedQkvRegion,
    grouped_linear_qkv_slice: PreparedQkvRegion,
    packed_linear_qkv_slice: PreparedQkvRegion,
    rms_norm_grouped_linear_qkv_slice: PreparedRmsNormGroupedQkvRegion,
    attention_output_residual: PreparedAttentionOutputResidualRegion,
    rms_norm_gated_ffn_residual: PreparedRmsNormGatedFfnResidualRegion,
    gated_ffn_residual: PreparedGatedFfnResidualRegion,
    ple_residual: PreparedPleResidualRegion,
};

fn hashNodeIds(node_ids: []const NodeId) u64 {
    return std.hash.Wyhash.hash(0, std.mem.sliceAsBytes(node_ids));
}

const RuntimeRegionPlan = struct {
    node_count: usize = 0,
    value_count: usize = 0,
    first_node: NodeId = null_node,
    last_node: NodeId = null_node,
    // Structural fingerprint of node_ids so a cached plan is only reused for the
    // exact same node ordering. Length + endpoints alone can collide across two
    // structurally-different graphs with equal counts and equal first/last ids.
    node_ids_hash: u64 = 0,
    regions_by_pos: []RuntimeRegion = &.{},
    prepared_by_pos: []PreparedRuntimeRegion = &.{},
    attention_input_max_first_node: []NodeId = &.{},
    pre_skipped_by_region: []bool = &.{},
    region_count: usize = 0,
    covered_node_count: usize = 0,
    elided_node_count: usize = 0,
    pre_skipped_node_count: usize = 0,
    pre_skipped_transpose_count: usize = 0,
    pre_skip_declined_external_consumer_count: usize = 0,

    fn deinit(self: *RuntimeRegionPlan, allocator: std.mem.Allocator) void {
        allocator.free(self.regions_by_pos);
        allocator.free(self.prepared_by_pos);
        allocator.free(self.attention_input_max_first_node);
        allocator.free(self.pre_skipped_by_region);
        self.* = .{};
    }

    fn matches(self: RuntimeRegionPlan, node_ids: []const NodeId, value_count: usize) bool {
        if (self.regions_by_pos.len != node_ids.len) return false;
        if (self.prepared_by_pos.len != node_ids.len) return false;
        if (self.attention_input_max_first_node.len != value_count) return false;
        if (self.pre_skipped_by_region.len != value_count) return false;
        if (self.node_count != node_ids.len or self.value_count != value_count) return false;
        if (node_ids.len == 0) return self.first_node == null_node and self.last_node == null_node;
        if (self.first_node != node_ids[0] or self.last_node != node_ids[node_ids.len - 1]) return false;
        return self.node_ids_hash == hashNodeIds(node_ids);
    }

    fn regionAt(self: RuntimeRegionPlan, node_pos: usize, node_id: NodeId, node_ids: []const NodeId) RuntimeRegion {
        if (node_pos >= self.regions_by_pos.len or node_pos >= node_ids.len) return .{ .none = {} };
        if (node_ids[node_pos] != node_id) return .{ .none = {} };
        return self.regions_by_pos[node_pos];
    }

    fn preparedPtrAt(self: RuntimeRegionPlan, node_pos: usize, node_id: NodeId, node_ids: []const NodeId) ?*PreparedRuntimeRegion {
        if (node_pos >= self.prepared_by_pos.len or node_pos >= node_ids.len) return null;
        if (node_ids[node_pos] != node_id) return null;
        return &self.prepared_by_pos[node_pos];
    }

    fn needsAttentionInputAfterNode(self: RuntimeRegionPlan, input_id: NodeId, node_id: NodeId) bool {
        if (input_id == null_node) return false;
        const input_index: usize = @intCast(input_id);
        if (input_index >= self.attention_input_max_first_node.len) return false;
        const max_first_node = self.attention_input_max_first_node[input_index];
        return max_first_node != null_node and max_first_node > node_id;
    }
};

const CachedPartitionBufferView = struct {
    partition_index: u32 = 0,
    partition_count: usize = 0,
    slot_count: usize = 0,
    transfer_count: usize = 0,
    first_slot_node: NodeId = null_node,
    last_slot_node: NodeId = null_node,
    backend: contracts.BackendKind = .native,
    view: buffer_plan_mod.PartitionBufferView,

    fn init(
        buffer_plan: *const buffer_plan_mod.BufferPlan,
        partition_plan: *const partition_mod.PartitionPlan,
        partition_index: u32,
        view: buffer_plan_mod.PartitionBufferView,
    ) CachedPartitionBufferView {
        return .{
            .partition_index = partition_index,
            .partition_count = partition_plan.partitions.len,
            .slot_count = buffer_plan.slots.len,
            .transfer_count = buffer_plan.transfers.len,
            .first_slot_node = if (buffer_plan.slots.len == 0) null_node else buffer_plan.slots[0].node_id,
            .last_slot_node = if (buffer_plan.slots.len == 0) null_node else buffer_plan.slots[buffer_plan.slots.len - 1].node_id,
            .backend = view.backend,
            .view = view,
        };
    }

    fn deinit(self: *CachedPartitionBufferView, allocator: std.mem.Allocator) void {
        self.view.deinit(allocator);
        self.* = undefined;
    }

    fn matches(
        self: CachedPartitionBufferView,
        buffer_plan: *const buffer_plan_mod.BufferPlan,
        partition_plan: *const partition_mod.PartitionPlan,
        partition_index: u32,
    ) bool {
        if (partition_index >= partition_plan.partitions.len) return false;
        return self.partition_index == partition_index and
            self.partition_count == partition_plan.partitions.len and
            self.slot_count == buffer_plan.slots.len and
            self.transfer_count == buffer_plan.transfers.len and
            self.first_slot_node == (if (buffer_plan.slots.len == 0) null_node else buffer_plan.slots[0].node_id) and
            self.last_slot_node == (if (buffer_plan.slots.len == 0) null_node else buffer_plan.slots[buffer_plan.slots.len - 1].node_id) and
            self.backend == partition_plan.partitions[@intCast(partition_index)].backend;
    }

    fn traceMismatch(
        self: CachedPartitionBufferView,
        buffer_plan: *const buffer_plan_mod.BufferPlan,
        partition_plan: *const partition_mod.PartitionPlan,
        partition_index: u32,
    ) void {
        if (!tracePartitionViewCacheEnabled()) return;
        const current_backend = if (partition_index < partition_plan.partitions.len)
            partition_plan.partitions[@intCast(partition_index)].backend
        else
            .native;
        std.debug.print(
            "partition_view_cache_miss: cached_partition={d} current_partition={d} cached_partitions={d} current_partitions={d} cached_slots={d} current_slots={d} cached_transfers={d} current_transfers={d} cached_first={} current_first={} cached_last={} current_last={} cached_backend={s} current_backend={s}\n",
            .{
                self.partition_index,
                partition_index,
                self.partition_count,
                partition_plan.partitions.len,
                self.slot_count,
                buffer_plan.slots.len,
                self.transfer_count,
                buffer_plan.transfers.len,
                self.first_slot_node,
                if (buffer_plan.slots.len == 0) null_node else buffer_plan.slots[0].node_id,
                self.last_slot_node,
                if (buffer_plan.slots.len == 0) null_node else buffer_plan.slots[buffer_plan.slots.len - 1].node_id,
                @tagName(self.backend),
                @tagName(current_backend),
            },
        );
    }
};

const PartitionBufferViewResult = struct {
    view: buffer_plan_mod.PartitionBufferView,
    cache_hit: bool = false,
};

const RuntimeFrameIneligibleReason = enum {
    none,
    no_regions,
    missing_qkv,
    missing_attention,
    missing_ffn,
    missing_ple,
    single_row,
    non_layer_order,
    shape_mismatch,
    missing_model_metadata,
};

const RuntimeFrameEligibility = struct {
    layers: usize = 0,
    reason: RuntimeFrameIneligibleReason = .none,

    fn eligible(self: RuntimeFrameEligibility) bool {
        return self.reason == .none and self.layers > 0;
    }
};

const RuntimeFrameLayerShape = struct {
    rows: usize,
    hidden_size: usize,
    attention_input_size: usize = 0,
};

const RuntimeFrameQkvMetadata = struct {
    layer_index: usize,
    rows: usize,
    hidden_size: usize,
    q_dim: usize,
    kv_dim: usize,
    q_weight_id: NodeId,
    k_weight_id: ?NodeId = null,
    v_weight_id: ?NodeId = null,
};

const RuntimeFrameLayerMetadata = struct {
    layer_index: usize,
    shares_kv: bool,
    kv_layer_index: usize,
    kv_heads: usize,
    head_dim: usize,
    intermediate_size: usize,
    hidden_size: usize,
    attention_input_size: usize,
    ple_hidden_size: usize,
    activation: ops_mod.DecoderRuntimeActivationKind,
};

const RuntimeFrameMetadata = struct {
    rows: usize,
    layer_count: usize,
    hidden_size: usize,
    num_attention_heads: usize,
    global_head_dim: usize,
    ple_hidden_size: usize,
    activation: ops_mod.DecoderRuntimeActivationKind,
};

const runtime_frame_max_layers = 128;

const RuntimeFrameLayerBucket = struct {
    qkv: ?RuntimeFrameQkvMetadata = null,
    qkv_region: RuntimeRegion = .{ .none = {} },
    qkv_input_id: NodeId = null_node,
    attention: ?AttentionOutputResidualPattern = null,
    ffn: RuntimeRegion = .{ .none = {} },
    ple: ?PleResidualPattern = null,
};

const RuntimeFrameLayerCollection = struct {
    buckets: [runtime_frame_max_layers]RuntimeFrameLayerBucket = [_]RuntimeFrameLayerBucket{.{}} ** runtime_frame_max_layers,
    count: usize = 0,
    completed_layers: usize = 0,
    reason: RuntimeFrameIneligibleReason = .none,
};

pub fn isMetalDeviceResident(cb: *const ComputeBackend, tensor: CT) bool {
    if (cb.kind() != .metal) return false;
    if (comptime !build_options.enable_metal) return false;
    return metal_compute_mod.MetalCompute.debugHasDeviceTensor(cb, tensor);
}

fn isMetalResidentOrQuantizedDescriptor(cb: *const ComputeBackend, tensor: CT) bool {
    if (isMetalDeviceResident(cb, tensor)) return true;
    if (cb.kind() != .metal) return false;
    if (comptime !build_options.enable_metal) return false;
    if (metal_compute_mod.MetalCompute.debugHasDeviceLazyMultiply(cb, tensor)) return true;
    return metal_compute_mod.MetalCompute.getQuantizedStorage(cb, tensor) != null;
}

fn isMetalStorageAlias(cb: *const ComputeBackend, lhs: CT, rhs: CT) bool {
    if (cb.kind() != .metal) return false;
    if (comptime !build_options.enable_metal) return false;
    return metal_compute_mod.MetalCompute.debugSharesStorage(cb, lhs, rhs);
}

fn classifyMetalExecutionKind(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    node_id: NodeId,
) MetalExecutionKind {
    const output = valueFor(values, node_id) orelse return .command;
    const node = graph.node(node_id);
    const inputs = node.getInputs();
    switch (node.op) {
        .constant, .fused_zero_tensor => return .constant_materialization,
        .reshape, .slice => {
            if (inputs.len == 0) return .command;
            const input = valueFor(values, inputs[0]) orelse return .command;
            if (isMetalStorageAlias(cb, input, output)) return .metadata_alias;
        },
        .concat_prim => {
            if (comptime build_options.enable_metal) {
                if (cb.kind() == .metal) {
                    if (metal_compute_mod.MetalCompute.getQuantizedStorage(cb, output) != null) {
                        return .descriptor_materialization;
                    }
                }
            }
        },
        else => {},
    }
    return .command;
}

pub fn makeMetalDeviceResident(cb: *const ComputeBackend, tensor: CT) !?CT {
    if (cb.kind() != .metal) return null;
    if (comptime !build_options.enable_metal) return null;
    return metal_compute_mod.MetalCompute.makeDeviceResident(cb, tensor);
}

fn promoteMetalOutputIfNeeded(cb: *const ComputeBackend, output: ?CT) !?CT {
    const ct = output orelse return null;
    if (isMetalDeviceResident(cb, ct)) return ct;
    if (try makeMetalDeviceResident(cb, ct)) |device_ct| {
        if (device_ct != ct) cb.free(ct);
        return device_ct;
    }
    return ct;
}

const TemporaryMetalResidentValue = struct {
    value: CT,
    owned: bool = false,

    fn deinit(self: TemporaryMetalResidentValue, cb: *const ComputeBackend) void {
        if (self.owned) cb.free(self.value);
    }
};

fn temporaryMetalResidentValue(cb: *const ComputeBackend, tensor: CT) !TemporaryMetalResidentValue {
    if (isMetalDeviceResident(cb, tensor)) return .{ .value = tensor };
    if (try makeMetalDeviceResident(cb, tensor)) |device_ct| {
        return .{ .value = device_ct, .owned = device_ct != tensor };
    }
    return .{ .value = tensor };
}

const ResidentInputCacheEntry = struct {
    remaining_consumers: u32 = 0,
    value: ?CT = null,
    owned: bool = false,
    bytes: u64 = 0,
};

const ResidentInputCache = struct {
    entries: std.AutoHashMapUnmanaged(NodeId, ResidentInputCacheEntry) = .empty,
    live_bytes: u64 = 0,
    peak_bytes: u64 = 0,

    fn init(self: *ResidentInputCache, allocator: std.mem.Allocator, graph: *const Graph, node_ids: []const NodeId) !void {
        var counts = std.AutoHashMapUnmanaged(NodeId, u32).empty;
        defer counts.deinit(allocator);

        for (node_ids) |node_id| {
            const source_id = lowSyncResidentInputSource(graph, node_id) orelse continue;
            const entry = try counts.getOrPut(allocator, source_id);
            if (!entry.found_existing) entry.value_ptr.* = 0;
            entry.value_ptr.* += 1;
        }

        var iter = counts.iterator();
        while (iter.next()) |entry| {
            const count = entry.value_ptr.*;
            if (count < 2) continue;
            try self.entries.put(allocator, entry.key_ptr.*, .{
                .remaining_consumers = count,
            });
        }
    }

    fn deinit(self: *ResidentInputCache, allocator: std.mem.Allocator, cb: *const ComputeBackend) void {
        var iter = self.entries.iterator();
        while (iter.next()) |entry| {
            if (entry.value_ptr.owned) {
                if (entry.value_ptr.value) |ct| cb.free(ct);
            }
        }
        self.entries.deinit(allocator);
        self.* = .{};
    }

    fn hasEntries(self: *const ResidentInputCache) bool {
        return self.entries.count() != 0;
    }

    fn residentValueForConsumer(
        self: *ResidentInputCache,
        cb: *const ComputeBackend,
        graph: *const Graph,
        source_id: NodeId,
        source_value: CT,
        stats: ?*PartitionExecutor.ExecutionStats,
    ) !?CT {
        const entry = self.entries.getPtr(source_id) orelse return null;
        if (entry.value) |cached| {
            if (stats) |s| {
                s.metal_resident_input_cache_hits += 1;
                s.metal_resident_input_cache_reused_bytes += entry.bytes;
            }
            return cached;
        }
        if (isMetalDeviceResident(cb, source_value)) return source_value;
        if (stats) |s| s.metal_resident_input_cache_misses += 1;

        const promoted = (try makeMetalDeviceResident(cb, source_value)) orelse return null;
        if (!isMetalDeviceResident(cb, promoted)) return null;

        const bytes = if (tensorElementCount(graph.node(source_id).output_shape)) |elems|
            @as(u64, @intCast(elems * @sizeOf(f32)))
        else
            0;
        entry.value = promoted;
        entry.owned = promoted != source_value;
        entry.bytes = bytes;
        if (entry.owned) {
            self.live_bytes += bytes;
            self.peak_bytes = @max(self.peak_bytes, self.live_bytes);
        }
        if (stats) |s| {
            s.metal_resident_input_cache_unique_promotions += 1;
            s.metal_resident_input_cache_retained_live_bytes = self.live_bytes;
            s.metal_resident_input_cache_retained_peak_bytes = @max(s.metal_resident_input_cache_retained_peak_bytes, self.peak_bytes);
        }
        return promoted;
    }

    fn releaseAfterConsumer(
        self: *ResidentInputCache,
        cb: *const ComputeBackend,
        graph: *const Graph,
        consumer_id: NodeId,
        stats: ?*PartitionExecutor.ExecutionStats,
    ) void {
        const source_id = lowSyncResidentInputSource(graph, consumer_id) orelse return;
        const entry = self.entries.getPtr(source_id) orelse return;
        if (entry.remaining_consumers > 0) entry.remaining_consumers -= 1;
        if (entry.remaining_consumers != 0) return;
        if (entry.value) |ct| {
            if (entry.owned) {
                cb.free(ct);
                if (self.live_bytes >= entry.bytes) self.live_bytes -= entry.bytes else self.live_bytes = 0;
                if (stats) |s| {
                    s.metal_resident_input_cache_released_bytes += entry.bytes;
                    s.metal_resident_input_cache_retained_live_bytes = self.live_bytes;
                    s.metal_resident_input_cache_retained_peak_bytes = @max(s.metal_resident_input_cache_retained_peak_bytes, self.peak_bytes);
                }
            }
            entry.value = null;
            entry.owned = false;
        }
    }
};

pub const MetalGraphPlanAllocation = struct {
    allocation: buffer_plan_mod.AllocationId,
    graph_slot: usize,
    bytes: usize,
};

pub const MetalPartitionGraphPlan = struct {
    slots: []const GraphPlanSlot,
    allocations: []const MetalGraphPlanAllocation,

    pub fn deinit(self: *MetalPartitionGraphPlan, allocator: std.mem.Allocator) void {
        allocator.free(self.slots);
        allocator.free(self.allocations);
    }
};

/// Slot-bound output pool: one persistent device buffer per reusable
/// `buffer_plan` tensor `AllocationId`, reused across non-overlapping nodes and
/// across steps. The pool owns the buffer CTs; output hints are borrowed views
/// over those buffers and are safe to free at node last-use.
const OutputBufferPool = struct {
    buffers: []?CT = &.{}, // indexed by buffer_plan AllocationId
    byte_sizes: []u64 = &.{},
    built: bool = false,
    allocations_created: u64 = 0,
    allocation_reuses: u64 = 0,
    budget_declines: u64 = 0,
    bytes_live: u64 = 0,
    peak_bytes_live: u64 = 0,

    fn deinit(self: *OutputBufferPool, allocator: std.mem.Allocator, cb: *const ComputeBackend) void {
        for (self.buffers) |b| {
            if (b) |ct| cb.free(ct);
        }
        allocator.free(self.buffers);
        allocator.free(self.byte_sizes);
        self.buffers = &.{};
        self.byte_sizes = &.{};
        self.built = false;
        self.allocations_created = 0;
        self.allocation_reuses = 0;
        self.budget_declines = 0;
        self.bytes_live = 0;
        self.peak_bytes_live = 0;
    }

    /// Lazily allocate (or resize) allocation `allocation_id`'s buffer to `byte_len`,
    /// returning the owning pool CT (the pool keeps ownership).
    fn ensureBuffer(self: *OutputBufferPool, cb: *const ComputeBackend, allocation_id: buffer_plan_mod.AllocationId, byte_len: u64) ?CT {
        const idx: usize = @intCast(allocation_id);
        if (idx >= self.buffers.len) return null;
        if (self.buffers[idx]) |existing| {
            if (self.byte_sizes[idx] == byte_len) {
                self.allocation_reuses += 1;
                return existing;
            }
            if (poolWouldExceedBudget(self.bytes_live, self.byte_sizes[idx], byte_len)) {
                self.budget_declines += 1;
                return null;
            }
            self.bytes_live -|= self.byte_sizes[idx];
            cb.free(existing);
            self.buffers[idx] = null;
            self.byte_sizes[idx] = 0;
        } else {
            if (poolWouldExceedBudget(self.bytes_live, 0, byte_len)) {
                self.budget_declines += 1;
                return null;
            }
        }
        const ct = metal_compute_mod.MetalCompute.poolAllocateOutputCt(cb, @intCast(byte_len)) catch return null;
        self.buffers[idx] = ct;
        self.byte_sizes[idx] = byte_len;
        self.allocations_created += 1;
        self.bytes_live += byte_len;
        self.peak_bytes_live = @max(self.peak_bytes_live, self.bytes_live);
        return ct;
    }

    fn allocationCount(self: *const OutputBufferPool) usize {
        var count: usize = 0;
        for (self.buffers) |maybe| {
            if (maybe != null) count += 1;
        }
        return count;
    }
};

fn poolWouldExceedBudget(bytes_live: u64, old_size: u64, new_size: u64) bool {
    const max_bytes = slotBoundOutputPoolMaxBytes();
    if (max_bytes == 0) return false;
    if (new_size > max_bytes) return true;
    const next_live = (bytes_live -| old_size) +| new_size;
    return next_live > max_bytes;
}

/// Per-execution Metal eager arena. Unlike the persistent slot-bound pool, this
/// arena is torn down at partition end and is intended to approximate PyTorch's
/// step-local working set: local reusable outputs borrow storage from physical
/// buffer-plan allocation ids, while escaping values keep ordinary ownership.
const MetalEagerArena = struct {
    buffers: []?CT = &.{}, // indexed by buffer_plan AllocationId
    byte_sizes: []u64 = &.{},
    built: bool = false,
    allocations: u64 = 0,
    reuse_hits: u64 = 0,
    spill_bytes: u64 = 0,
    hazard_declines: u64 = 0,
    alias_conflicts: u64 = 0,
    alias_reclaims: u64 = 0,
    alias_reclaim_bytes: u64 = 0,
    live_bytes: u64 = 0,
    peak_bytes: u64 = 0,

    fn init(self: *MetalEagerArena, allocator: std.mem.Allocator, allocation_count: usize) !void {
        if (self.built and self.buffers.len == allocation_count) return;
        const buffers = try allocator.alloc(?CT, allocation_count);
        errdefer allocator.free(buffers);
        @memset(buffers, null);
        const byte_sizes = try allocator.alloc(u64, allocation_count);
        errdefer allocator.free(byte_sizes);
        @memset(byte_sizes, 0);
        self.* = .{ .buffers = buffers, .byte_sizes = byte_sizes, .built = true };
    }

    fn deinit(self: *MetalEagerArena, allocator: std.mem.Allocator, cb: *const ComputeBackend) void {
        for (self.buffers) |maybe| {
            if (maybe) |ct| cb.free(ct);
        }
        allocator.free(self.buffers);
        allocator.free(self.byte_sizes);
        self.* = .{};
    }

    fn ensureBuffer(self: *MetalEagerArena, cb: *const ComputeBackend, allocation_id: buffer_plan_mod.AllocationId, byte_len: u64) ?CT {
        const idx: usize = @intCast(allocation_id);
        if (idx >= self.buffers.len) return null;
        if (self.buffers[idx]) |existing| {
            if (self.byte_sizes[idx] == byte_len) {
                self.reuse_hits += 1;
                return existing;
            }
            if (arenaWouldExceedBudget(self.live_bytes, self.byte_sizes[idx], byte_len)) {
                self.spill_bytes += byte_len;
                return null;
            }
            self.live_bytes -|= self.byte_sizes[idx];
            cb.free(existing);
            self.buffers[idx] = null;
            self.byte_sizes[idx] = 0;
        } else if (arenaWouldExceedBudget(self.live_bytes, 0, byte_len)) {
            self.spill_bytes += byte_len;
            return null;
        }

        const ct = metal_compute_mod.MetalCompute.poolAllocateOutputCt(cb, @intCast(byte_len)) catch {
            self.spill_bytes += byte_len;
            return null;
        };
        self.buffers[idx] = ct;
        self.byte_sizes[idx] = byte_len;
        self.allocations += 1;
        self.live_bytes += byte_len;
        self.peak_bytes = @max(self.peak_bytes, self.live_bytes);
        return ct;
    }

    fn outputHintForNode(
        self: *MetalEagerArena,
        cb: *const ComputeBackend,
        graph: *const Graph,
        buffer_plan: *const buffer_plan_mod.BufferPlan,
        values: []const ?CT,
        node_id: NodeId,
    ) ?CT {
        if (!self.built) return null;
        const node = graph.node(node_id);
        if (!opSupportsOutputHint(node.op)) return null;
        const slot = buffer_plan.slotForNode(node_id) orelse return null;
        if (!slot.reusable or slot.roles.graph_output or slot.roles.partition_output or slot.roles.transfer_source) return null;
        if (slot.kind != .allocation) return null;
        if (slot.backend != .metal or slot.storage != .metal_buffer) return null;
        if (slot.allocation == buffer_plan_mod.invalid_allocation) return null;
        const allocation = buffer_plan.allocationForSlot(slot.id) orelse return null;
        if (!allocation.reusable or allocation.kind != .tensor) return null;
        if (allocation.backend != .metal or allocation.storage != .metal_buffer) return null;
        const byte_len = outputByteLen(node.output_shape) orelse return null;
        if (byte_len < metalEagerArenaMinBytes()) return null;
        if (allocation.byte_size < byte_len) {
            self.hazard_declines += 1;
            self.spill_bytes += byte_len;
            return null;
        }
        if (allocationHasMaterializedValue(buffer_plan, values, slot.allocation, node_id)) {
            self.alias_conflicts += 1;
            self.spill_bytes += byte_len;
            return null;
        }
        const arena_ct = self.ensureBuffer(cb, slot.allocation, byte_len) orelse return null;
        return viewCtForShape(cb, arena_ct, node.output_shape) catch {
            self.spill_bytes += byte_len;
            return null;
        };
    }

    fn reclaimDeadAliasesForNode(
        self: *MetalEagerArena,
        allocator: std.mem.Allocator,
        graph: *const Graph,
        cb: *const ComputeBackend,
        buffer_plan: *const buffer_plan_mod.BufferPlan,
        values: []?CT,
        value_device: []DeviceId,
        node_ids: []const NodeId,
        node_pos: usize,
        node_id: NodeId,
        device_id: DeviceId,
        last_use: []const u32,
        runtime_region_plan: ?RuntimeRegionPlan,
        rt_map: std.AutoHashMapUnmanaged(NodeId, CT),
        donated: std.AutoHashMapUnmanaged(NodeId, void),
        exec_ctx: PartitionExecutor.ExecutionContext,
    ) !usize {
        if (!self.built) return 0;
        const slot = buffer_plan.slotForNode(node_id) orelse return 0;
        if (slot.kind != .allocation or slot.allocation == buffer_plan_mod.invalid_allocation) return 0;

        var released = std.AutoHashMapUnmanaged(usize, void).empty;
        defer released.deinit(allocator);

        var reclaimed: usize = 0;
        const current_index: usize = @intCast(node_id);
        for (values, 0..) |maybe_ct, raw_index| {
            const ct = maybe_ct orelse continue;
            if (raw_index == current_index) continue;
            const other_node: NodeId = @intCast(raw_index);
            const other_slot = buffer_plan.slotForNode(other_node) orelse continue;
            if (other_slot.allocation != slot.allocation) continue;
            if (arenaAliasStillLive(
                graph,
                buffer_plan,
                node_ids,
                node_pos,
                other_node,
                node_id,
                last_use,
                runtime_region_plan,
                rt_map,
                donated,
                exec_ctx,
            )) continue;

            values[raw_index] = null;
            self.alias_reclaims += 1;
            if (outputByteLen(graph.node(other_node).output_shape)) |bytes| {
                self.alias_reclaim_bytes += bytes;
            }
            traceMetalValueLifetime("eager_arena_alias_reclaim_clear", node_id, other_node);

            const ct_key = @intFromPtr(ct);
            if (released.contains(ct_key)) continue;
            if (ctStillReferenced(values, ct)) continue;
            try released.put(allocator, ct_key, {});
            if (exec_ctx.mesh) |mesh| {
                const value_dev = if (raw_index < value_device.len) value_device[raw_index] else device_id;
                if (mesh.device(value_dev)) |entry| {
                    entry.backend.free(ct);
                } else {
                    cb.free(ct);
                }
            } else {
                cb.free(ct);
            }
            traceMetalValueLifetime("eager_arena_alias_reclaim_release", node_id, other_node);
            reclaimed += 1;
        }
        return reclaimed;
    }

    fn recordStats(self: *const MetalEagerArena, stats: ?*PartitionExecutor.ExecutionStats) void {
        if (stats) |s| {
            s.metal_eager_arena_peak_bytes = @max(s.metal_eager_arena_peak_bytes, self.peak_bytes);
            s.metal_eager_arena_live_bytes = @max(s.metal_eager_arena_live_bytes, self.live_bytes);
            s.metal_eager_arena_reuse_hits += self.reuse_hits;
            s.metal_eager_arena_allocations += self.allocations;
            s.metal_eager_arena_spill_bytes += self.spill_bytes;
            s.metal_eager_arena_hazard_declines += self.hazard_declines;
            s.metal_eager_arena_alias_conflicts += self.alias_conflicts;
            s.metal_eager_arena_alias_reclaims += self.alias_reclaims;
            s.metal_eager_arena_alias_reclaim_bytes += self.alias_reclaim_bytes;
        }
    }
};

fn arenaWouldExceedBudget(bytes_live: u64, old_size: u64, new_size: u64) bool {
    const max_bytes = metalEagerArenaMaxBytes();
    if (max_bytes == 0) return false;
    if (new_size > max_bytes) return true;
    const next_live = (bytes_live -| old_size) +| new_size;
    return next_live > max_bytes;
}

fn outputByteLen(shape: Shape) ?u64 {
    const rank = shape.rank();
    if (rank == 0 or rank > ml.graph.shape.max_rank) return null;
    const numel = shape.numElements() orelse return null;
    if (numel == 0) return null;
    return @as(u64, @intCast(numel)) * @sizeOf(f32);
}

fn viewCtForShape(cb: *const ComputeBackend, storage_ct: CT, shape: Shape) !CT {
    const rank = shape.rank();
    if (rank == 0 or rank > ml.graph.shape.max_rank) return error.UnsupportedShape;
    var dims_buf: [ml.graph.shape.max_rank]i32 = undefined;
    for (0..rank) |ax| {
        const d = shape.dim(@intCast(ax));
        if (d <= 0) return error.UnsupportedShape;
        dims_buf[ax] = @intCast(d);
    }
    return metal_compute_mod.MetalCompute.ctFromPoolCtView(cb, storage_ct, dims_buf[0..rank]);
}

const OutputHintSource = enum {
    chunk_local,
    eager_arena,
    slot_bound_pool,
};

/// Chunk-local output pool: command-output backing storage that is eligible to
/// die at the next frame chunk boundary. This is intentionally shorter-lived
/// than MetalEagerArena: borrowed views may be consumed by normal command ops,
/// but backing buffers are released after submit/wait once no live value still
/// references the corresponding buffer-plan allocation.
const ChunkLocalOutputPool = struct {
    buffers: []?CT = &.{}, // indexed by buffer_plan AllocationId
    byte_sizes: []u64 = &.{},
    borrowed_nodes: []bool = &.{},
    built: bool = false,
    allocations: u64 = 0,
    reuse_hits: u64 = 0,
    consumed_hints: u64 = 0,
    unconsumed_hints: u64 = 0,
    spill_bytes: u64 = 0,
    alias_conflicts: u64 = 0,
    reset_count: u64 = 0,
    reset_freed_bytes: u64 = 0,
    discard_freed_bytes: u64 = 0,
    reset_live_carry_values: u64 = 0,
    live_bytes: u64 = 0,
    peak_bytes: u64 = 0,

    fn init(self: *ChunkLocalOutputPool, allocator: std.mem.Allocator, allocation_count: usize, value_count: usize) !void {
        if (self.built and self.buffers.len == allocation_count and self.borrowed_nodes.len == value_count) return;
        const buffers = try allocator.alloc(?CT, allocation_count);
        errdefer allocator.free(buffers);
        @memset(buffers, null);
        const byte_sizes = try allocator.alloc(u64, allocation_count);
        errdefer allocator.free(byte_sizes);
        @memset(byte_sizes, 0);
        const borrowed_nodes = try allocator.alloc(bool, value_count);
        errdefer allocator.free(borrowed_nodes);
        @memset(borrowed_nodes, false);
        self.* = .{
            .buffers = buffers,
            .byte_sizes = byte_sizes,
            .borrowed_nodes = borrowed_nodes,
            .built = true,
        };
    }

    fn deinit(self: *ChunkLocalOutputPool, allocator: std.mem.Allocator, cb: *const ComputeBackend) void {
        for (self.buffers) |maybe| {
            if (maybe) |ct| cb.free(ct);
        }
        allocator.free(self.buffers);
        allocator.free(self.byte_sizes);
        allocator.free(self.borrowed_nodes);
        self.* = .{};
    }

    fn ensureBuffer(self: *ChunkLocalOutputPool, cb: *const ComputeBackend, allocation_id: buffer_plan_mod.AllocationId, byte_len: u64) ?CT {
        const idx: usize = @intCast(allocation_id);
        if (idx >= self.buffers.len) return null;
        if (self.buffers[idx]) |existing| {
            if (self.byte_sizes[idx] == byte_len) {
                self.reuse_hits += 1;
                return existing;
            }
            if (chunkLocalOutputsWouldExceedBudget(self.live_bytes, self.byte_sizes[idx], byte_len)) {
                self.spill_bytes += byte_len;
                return null;
            }
            self.live_bytes -|= self.byte_sizes[idx];
            cb.free(existing);
            self.buffers[idx] = null;
            self.byte_sizes[idx] = 0;
        } else if (chunkLocalOutputsWouldExceedBudget(self.live_bytes, 0, byte_len)) {
            self.spill_bytes += byte_len;
            return null;
        }

        const ct = metal_compute_mod.MetalCompute.poolAllocateOutputCt(cb, @intCast(byte_len)) catch {
            self.spill_bytes += byte_len;
            return null;
        };
        self.buffers[idx] = ct;
        self.byte_sizes[idx] = byte_len;
        self.allocations += 1;
        self.live_bytes += byte_len;
        self.peak_bytes = @max(self.peak_bytes, self.live_bytes);
        return ct;
    }

    fn outputHintForNode(
        self: *ChunkLocalOutputPool,
        cb: *const ComputeBackend,
        graph: *const Graph,
        buffer_plan: *const buffer_plan_mod.BufferPlan,
        values: []const ?CT,
        node_ids: []const NodeId,
        node_pos: usize,
        chunk_boundary_pos: usize,
        node_id: NodeId,
    ) ?CT {
        if (!self.built) return null;
        if (node_pos > chunk_boundary_pos or chunk_boundary_pos >= node_ids.len) return null;
        const node = graph.node(node_id);
        if (!opSupportsOutputHint(node.op)) return null;
        if (!nodeCanConsumeOutputHint(graph, cb, values, node_id)) return null;
        const slot = buffer_plan.slotForNode(node_id) orelse return null;
        if (!slot.reusable or slot.roles.graph_output or slot.roles.partition_output or slot.roles.transfer_source) return null;
        if (slot.kind != .allocation) return null;
        if (slot.backend != .metal or slot.storage != .metal_buffer) return null;
        if (slot.allocation == buffer_plan_mod.invalid_allocation) return null;
        const boundary_node_id = node_ids[chunk_boundary_pos];
        if (slot.last_use > @as(u32, @intCast(boundary_node_id))) return null;
        if (valueReferencedAfterBoundary(graph, node_ids, chunk_boundary_pos, node_id)) return null;
        const allocation = buffer_plan.allocationForSlot(slot.id) orelse return null;
        if (!allocation.reusable or allocation.kind != .tensor) return null;
        if (allocation.backend != .metal or allocation.storage != .metal_buffer) return null;
        const byte_len = outputByteLen(node.output_shape) orelse return null;
        if (byte_len < metalChunkLocalOutputsMinBytes()) return null;
        if (allocation.byte_size < byte_len) {
            self.spill_bytes += byte_len;
            return null;
        }
        if (allocationHasMaterializedValue(buffer_plan, values, slot.allocation, node_id)) {
            self.alias_conflicts += 1;
            self.spill_bytes += byte_len;
            return null;
        }
        const pool_ct = self.ensureBuffer(cb, slot.allocation, byte_len) orelse return null;
        const view = viewCtForShape(cb, pool_ct, node.output_shape) catch {
            self.spill_bytes += byte_len;
            return null;
        };
        const node_index: usize = @intCast(node_id);
        if (node_index < self.borrowed_nodes.len) self.borrowed_nodes[node_index] = true;
        return view;
    }

    fn recordConsumedHint(self: *ChunkLocalOutputPool) void {
        self.consumed_hints += 1;
    }

    fn discardUnconsumedHintForNode(
        self: *ChunkLocalOutputPool,
        cb: *const ComputeBackend,
        buffer_plan: *const buffer_plan_mod.BufferPlan,
        node_id: NodeId,
    ) void {
        self.unconsumed_hints += 1;
        const node_index: usize = @intCast(node_id);
        if (node_index < self.borrowed_nodes.len) self.borrowed_nodes[node_index] = false;
        const slot = buffer_plan.slotForNode(node_id) orelse return;
        if (slot.allocation == buffer_plan_mod.invalid_allocation) return;
        const idx: usize = @intCast(slot.allocation);
        if (idx >= self.buffers.len) return;
        const ct = self.buffers[idx] orelse return;
        const bytes = self.byte_sizes[idx];
        cb.free(ct);
        self.buffers[idx] = null;
        self.byte_sizes[idx] = 0;
        self.live_bytes -|= bytes;
        self.discard_freed_bytes += bytes;
    }

    fn resetAfterChunk(
        self: *ChunkLocalOutputPool,
        allocator: std.mem.Allocator,
        cb: *const ComputeBackend,
        buffer_plan: *const buffer_plan_mod.BufferPlan,
        values: []const ?CT,
    ) !void {
        if (!self.built) return;
        self.reset_count += 1;
        const live_allocations = try allocator.alloc(bool, self.buffers.len);
        defer allocator.free(live_allocations);
        @memset(live_allocations, false);

        var live_carry: u64 = 0;
        for (self.borrowed_nodes, 0..) |*borrowed, raw_id| {
            if (!borrowed.*) continue;
            if (raw_id >= values.len or values[raw_id] == null) {
                borrowed.* = false;
                continue;
            }
            const slot = buffer_plan.slotForNode(@intCast(raw_id)) orelse {
                borrowed.* = false;
                continue;
            };
            if (slot.allocation == buffer_plan_mod.invalid_allocation) {
                borrowed.* = false;
                continue;
            }
            const alloc_index: usize = @intCast(slot.allocation);
            if (alloc_index < live_allocations.len) live_allocations[alloc_index] = true;
            live_carry += 1;
        }
        self.reset_live_carry_values += live_carry;

        for (self.buffers, 0..) |maybe, idx| {
            const ct = maybe orelse continue;
            if (idx < live_allocations.len and live_allocations[idx]) continue;
            const bytes = self.byte_sizes[idx];
            cb.free(ct);
            self.buffers[idx] = null;
            self.byte_sizes[idx] = 0;
            self.live_bytes -|= bytes;
            self.reset_freed_bytes += bytes;
        }
    }

    fn recordStats(self: *const ChunkLocalOutputPool, stats: ?*PartitionExecutor.ExecutionStats) void {
        if (stats) |s| {
            s.metal_chunk_local_output_peak_bytes = @max(s.metal_chunk_local_output_peak_bytes, self.peak_bytes);
            s.metal_chunk_local_output_live_bytes = @max(s.metal_chunk_local_output_live_bytes, self.live_bytes);
            s.metal_chunk_local_output_allocations += self.allocations;
            s.metal_chunk_local_output_reuse_hits += self.reuse_hits;
            s.metal_chunk_local_output_consumed_hints += self.consumed_hints;
            s.metal_chunk_local_output_unconsumed_hints += self.unconsumed_hints;
            s.metal_chunk_local_output_spill_bytes += self.spill_bytes;
            s.metal_chunk_local_output_alias_conflicts += self.alias_conflicts;
            s.metal_chunk_local_output_resets += self.reset_count;
            s.metal_chunk_local_output_reset_freed_bytes += self.reset_freed_bytes;
            s.metal_chunk_local_output_discard_freed_bytes += self.discard_freed_bytes;
            s.metal_chunk_local_output_reset_live_carry_values += self.reset_live_carry_values;
        }
    }
};

pub const MetalPartitionExecutor = struct {
    allocator: std.mem.Allocator,
    graph: *const Graph,
    backend: *const ComputeBackend,
    pe: PartitionExecutor = undefined,
    owned: bool = false,
    partition_view: ?CachedPartitionBufferView = null,
    runtime_region_plan: ?RuntimeRegionPlan = null,
    output_pool: OutputBufferPool = .{},

    const vtable = PartitionExecutor.VTable{
        .execute = &executeFn,
        .deinit = &deinitFn,
    };

    pub fn initBorrowed(
        allocator: std.mem.Allocator,
        graph: *const Graph,
        backend: *const ComputeBackend,
    ) MetalPartitionExecutor {
        return .{
            .allocator = allocator,
            .graph = graph,
            .backend = backend,
        };
    }

    pub fn create(
        allocator: std.mem.Allocator,
        graph: *const Graph,
        backend: *const ComputeBackend,
    ) !*MetalPartitionExecutor {
        const exec = try allocator.create(MetalPartitionExecutor);
        exec.* = .{
            .allocator = allocator,
            .graph = graph,
            .backend = backend,
            .owned = true,
        };
        exec.pe = .{ .ptr = exec, .vtable = &vtable };
        return exec;
    }

    pub fn partitionExecutor(self: *MetalPartitionExecutor) *const PartitionExecutor {
        self.pe = .{ .ptr = self, .vtable = &vtable };
        return &self.pe;
    }

    fn executeFn(
        ctx: *anyopaque,
        values: []?CT,
        value_device: []DeviceId,
        node_ids: []const NodeId,
        device_id: DeviceId,
        exec_ctx: PartitionExecutor.ExecutionContext,
    ) anyerror!void {
        const self: *MetalPartitionExecutor = @ptrCast(@alignCast(ctx));
        return self.execute(values, value_device, node_ids, device_id, exec_ctx);
    }

    fn deinitFn(ctx: *anyopaque) void {
        const self: *MetalPartitionExecutor = @ptrCast(@alignCast(ctx));
        if (self.runtime_region_plan) |*plan| {
            plan.deinit(self.allocator);
            self.runtime_region_plan = null;
        }
        if (self.partition_view) |*view| {
            view.deinit(self.allocator);
            self.partition_view = null;
        }
        self.output_pool.deinit(self.allocator, self.backend);
        if (self.owned) self.allocator.destroy(self);
    }

    fn ensureOutputPool(
        self: *MetalPartitionExecutor,
        cb: *const ComputeBackend,
        buffer_plan: *const buffer_plan_mod.BufferPlan,
    ) !void {
        const allocation_count = buffer_plan.allocations.len;
        if (self.output_pool.built and self.output_pool.buffers.len == allocation_count) return;
        self.output_pool.deinit(self.allocator, cb);
        const buffers = try self.allocator.alloc(?CT, allocation_count);
        errdefer self.allocator.free(buffers);
        @memset(buffers, null);
        const byte_sizes = try self.allocator.alloc(u64, allocation_count);
        errdefer self.allocator.free(byte_sizes);
        @memset(byte_sizes, 0);
        self.output_pool = .{ .buffers = buffers, .byte_sizes = byte_sizes, .built = true };
    }

    /// Borrowed pool view for `node_id`'s planned allocation, or null if not
    /// eligible. Reuse is allowed only when no currently-materialized graph
    /// value still uses the same allocation id.
    /// Caller owns the returned CT and frees it if the op doesn't consume it.
    fn outputHintForNode(
        self: *MetalPartitionExecutor,
        cb: *const ComputeBackend,
        graph: *const Graph,
        buffer_plan: *const buffer_plan_mod.BufferPlan,
        values: []const ?CT,
        node_id: NodeId,
    ) ?CT {
        if (!self.output_pool.built) return null;
        const node = graph.node(node_id);
        if (!opSupportsOutputHint(node.op)) return null;
        const slot = buffer_plan.slotForNode(node_id) orelse return null;
        if (!slot.reusable or slot.roles.graph_output) return null;
        if (slot.kind != .allocation) return null;
        if (slot.backend != .metal or slot.storage != .metal_buffer) return null;
        if (slot.allocation == buffer_plan_mod.invalid_allocation) return null;
        const allocation = buffer_plan.allocationForSlot(slot.id) orelse return null;
        if (!allocation.reusable or allocation.kind != .tensor) return null;
        if (allocation.backend != .metal or allocation.storage != .metal_buffer) return null;
        const out_shape = node.output_shape;
        const byte_len = outputByteLen(out_shape) orelse return null;
        if (byte_len < slotBoundOutputMinBytes()) return null;
        if (allocation.byte_size < byte_len) return null;
        if (allocationHasMaterializedValue(buffer_plan, values, slot.allocation, node_id)) return null;
        const pool_ct = self.output_pool.ensureBuffer(cb, slot.allocation, byte_len) orelse return null;
        return viewCtForShape(cb, pool_ct, out_shape) catch null;
    }

    fn partitionBufferView(
        self: *MetalPartitionExecutor,
        allocator: std.mem.Allocator,
        buffer_plan: *const buffer_plan_mod.BufferPlan,
        partition_plan: *const partition_mod.PartitionPlan,
        partition_index: u32,
        transient: *?buffer_plan_mod.PartitionBufferView,
    ) !PartitionBufferViewResult {
        if (self.owned and partitionViewCacheEnabled()) {
            if (self.partition_view) |view| {
                if (view.matches(buffer_plan, partition_plan, partition_index)) {
                    if (tracePartitionViewCacheEnabled()) std.debug.print("partition_view_cache_hit: partition={d} slots={d} transfers={d}\n", .{ partition_index, buffer_plan.slots.len, buffer_plan.transfers.len });
                    return .{ .view = view.view, .cache_hit = true };
                }
                view.traceMismatch(buffer_plan, partition_plan, partition_index);
                var old = self.partition_view.?;
                old.deinit(self.allocator);
                self.partition_view = null;
            }
            const view = try buffer_plan.partitionView(self.allocator, partition_plan, partition_index);
            self.partition_view = CachedPartitionBufferView.init(buffer_plan, partition_plan, partition_index, view);
            if (tracePartitionViewCacheEnabled()) std.debug.print("partition_view_cache_store: partition={d} slots={d} transfers={d}\n", .{ partition_index, buffer_plan.slots.len, buffer_plan.transfers.len });
            return .{ .view = self.partition_view.?.view };
        }

        if (tracePartitionViewCacheEnabled()) std.debug.print("partition_view_cache_bypass: owned={} enabled={} partition={d} slots={d} transfers={d}\n", .{ self.owned, partitionViewCacheEnabled(), partition_index, buffer_plan.slots.len, buffer_plan.transfers.len });
        transient.* = try buffer_plan.partitionView(allocator, partition_plan, partition_index);
        return .{ .view = transient.*.? };
    }

    fn runtimeRegionPlan(
        self: *MetalPartitionExecutor,
        allocator: std.mem.Allocator,
        graph: *const Graph,
        node_ids: []const NodeId,
        value_count: usize,
        reachable: []const bool,
        last_use: []const u32,
        stats: ?*PartitionExecutor.ExecutionStats,
        transient: *?RuntimeRegionPlan,
    ) !RuntimeRegionPlan {
        if (runtimeRegionPlanDisabled()) return .{};

        if (self.owned) {
            if (self.runtime_region_plan) |plan| {
                if (plan.matches(node_ids, value_count)) {
                    if (stats) |s| s.runtime_region_plan_reuses += 1;
                    return plan;
                }
                var old = self.runtime_region_plan.?;
                old.deinit(self.allocator);
                self.runtime_region_plan = null;
            }
            self.runtime_region_plan = try buildRuntimeRegionPlan(self.allocator, graph, node_ids, value_count, reachable, last_use);
            if (stats) |s| {
                s.runtime_region_plan_compiles += 1;
                s.runtime_region_plan_regions += self.runtime_region_plan.?.region_count;
            }
            return self.runtime_region_plan.?;
        }

        transient.* = try buildRuntimeRegionPlan(allocator, graph, node_ids, value_count, reachable, last_use);
        if (stats) |s| {
            s.runtime_region_plan_compiles += 1;
            s.runtime_region_plan_regions += transient.*.?.region_count;
        }
        return transient.*.?;
    }

    fn execute(
        self: *MetalPartitionExecutor,
        values: []?CT,
        value_device: []DeviceId,
        node_ids: []const NodeId,
        device_id: DeviceId,
        exec_ctx: PartitionExecutor.ExecutionContext,
    ) !void {
        const allocator = exec_ctx.allocator orelse self.allocator;
        const graph = exec_ctx.graph orelse self.graph;
        const cb = exec_ctx.backend orelse self.backend;
        const reachable = exec_ctx.reachable orelse return error.MissingPartitionExecutionContext;
        const last_use = exec_ctx.last_use orelse return error.MissingPartitionExecutionContext;
        const buffer_plan = exec_ctx.buffer_plan orelse return error.MissingPartitionExecutionContext;
        const partition_plan = exec_ctx.partition_plan orelse return error.MissingPartitionExecutionContext;
        const trace_nodes = traceMetalGraphNodesEnabled();
        const progress_interval = metalGraphProgressInterval();
        const progress_start = metalGraphProgressStart();
        const progress_end = metalGraphProgressEnd();
        const trace_progress = progress_interval != 0 or progress_start != std.math.maxInt(usize) or progress_end != std.math.maxInt(usize);
        const collect_op_stats = metalPartitionOpStatsEnabled();
        const collect_loop_profile = metalPartitionLoopProfileEnabled();
        const collect_residency_stats = metalPartitionResidencyStatsEnabled() or collect_op_stats or traceMetalHostOutputsEnabled();
        var op_execution_stats = OpExecutionStats{};
        var loop_profile = ExecutorLoopProfile{};
        const partition_index = try partitionIndexForNodes(buffer_plan, node_ids);
        if (trace_nodes) std.debug.print("graph_executor_node_trace: executor_begin partition={d} nodes={d}\n", .{ partition_index, node_ids.len });
        if (trace_progress) std.debug.print("metal_partition_progress: phase=executor_begin partition={d} nodes={d}\n", .{ partition_index, node_ids.len });

        var transient_partition_view: ?buffer_plan_mod.PartitionBufferView = null;
        defer if (transient_partition_view) |*view| view.deinit(allocator);
        const partition_view_start_ns = if (collect_loop_profile) metalPartitionNowNs() else 0;
        const partition_view_result = try self.partitionBufferView(
            allocator,
            buffer_plan,
            partition_plan,
            partition_index,
            &transient_partition_view,
        );
        const partition_view = partition_view_result.view;
        if (!partition_view_result.cache_hit) try validatePartitionView(partition_view, node_ids);
        if (collect_loop_profile) loop_profile.partition_view_ns += metalPartitionElapsedNs(partition_view_start_ns, metalPartitionNowNs());
        if (trace_progress) std.debug.print("metal_partition_progress: phase=partition_view_ready partition={d} slots={d} transfers_in={d} transfers_out={d}\n", .{ partition_index, partition_view.slots.len, partition_view.transfers_in.len, partition_view.transfers_out.len });
        if (trace_nodes) {
            std.debug.print(
                "graph_executor_node_trace: partition_view partition={d} slots={d} transfers_in={d} transfers_out={d}\n",
                .{ partition_index, partition_view.slots.len, partition_view.transfers_in.len, partition_view.transfers_out.len },
            );
        }

        const chunk_ops = frameChunkOps();
        const graph_plan_start_ns = if (collect_loop_profile) metalPartitionNowNs() else 0;
        if (reservePartitionGraphPlanForExecution(chunk_ops)) {
            var metal_graph_plan = try buildMetalGraphPlan(allocator, buffer_plan, partition_view);
            defer metal_graph_plan.deinit(allocator);
            if (trace_nodes) printMetalGraphPlanTrace(partition_index, metal_graph_plan);
            if (trace_progress) std.debug.print("metal_partition_progress: phase=reserve_graph_slots_begin partition={d} slots={d}\n", .{ partition_index, metal_graph_plan.slots.len });
            _ = try cb.reserveGraphPlanSlots(metal_graph_plan.slots);
            if (trace_progress) std.debug.print("metal_partition_progress: phase=reserve_graph_slots_end partition={d}\n", .{partition_index});
            if (trace_nodes) std.debug.print("graph_executor_node_trace: graph_plan_reserved partition={d}\n", .{partition_index});
            if (exec_ctx.stats) |stats| {
                stats.graph_plan_slots_reserved += metal_graph_plan.slots.len;
                for (metal_graph_plan.slots) |slot| stats.graph_plan_bytes_reserved += slot.bytes;
            }
        } else if (trace_nodes) {
            std.debug.print("graph_executor_node_trace: graph_plan_reserved partition={d} skipped=frame_chunking\n", .{partition_index});
        }
        if (collect_loop_profile) loop_profile.graph_plan_ns += metalPartitionElapsedNs(graph_plan_start_ns, metalPartitionNowNs());

        // Phase-0 slot-bound output pool (persistent executor only). Binds
        // node outputs to fixed device buffers reused across steps.
        const slot_bound_outputs = self.owned and cb.kind() == .metal and slotBoundOutputsEnabled();
        if (slot_bound_outputs) self.ensureOutputPool(cb, buffer_plan) catch {};
        const eager_arena_outputs = self.owned and cb.kind() == .metal and metalEagerArenaEnabled();
        var eager_arena = MetalEagerArena{};
        defer eager_arena.deinit(allocator, cb);
        if (eager_arena_outputs) {
            eager_arena.init(allocator, buffer_plan.allocations.len) catch {};
        }
        const chunk_local_outputs = self.owned and cb.kind() == .metal and chunk_ops > 0 and metalChunkLocalOutputsEnabled();
        var chunk_local_pool = ChunkLocalOutputPool{};
        defer chunk_local_pool.deinit(allocator, cb);
        if (chunk_local_outputs) {
            chunk_local_pool.init(allocator, buffer_plan.allocations.len, values.len) catch {};
        }
        var resident_input_cache = ResidentInputCache{};
        defer resident_input_cache.deinit(allocator, cb);
        const resident_input_cache_enabled = self.owned and cb.kind() == .metal and (gatherPromoteInputEnabled() or reducePromoteInputEnabled());
        if (resident_input_cache_enabled) {
            try resident_input_cache.init(allocator, graph, node_ids);
        }
        const resident_input_cache_ptr: ?*ResidentInputCache = if (resident_input_cache_enabled and resident_input_cache.hasEntries())
            &resident_input_cache
        else
            null;

        const options = exec_ctx.options orelse interpreter.ExecuteOptions{
            .attention = if (exec_ctx.attention) |attention| attention.* else null,
            .embedding_ids = exec_ctx.embedding_ids,
        };

        var local_owned_runtime_transfers = std.AutoHashMapUnmanaged(NodeId, void).empty;
        defer local_owned_runtime_transfers.deinit(allocator);
        var effective_exec_ctx = exec_ctx;
        if (effective_exec_ctx.owned_runtime_transfers == null) {
            effective_exec_ctx.owned_runtime_transfers = &local_owned_runtime_transfers;
        }

        var rt_map = std.AutoHashMapUnmanaged(NodeId, CT).empty;
        defer rt_map.deinit(allocator);
        var donated = std.AutoHashMapUnmanaged(NodeId, void).empty;
        defer donated.deinit(allocator);
        if (options.runtime_inputs) |inputs| {
            for (inputs, 0..) |ri, idx| {
                try rt_map.put(allocator, ri.node_id, ri.value);
                if (options.donate) |donate| {
                    if (idx < donate.len and donate[idx]) try donated.put(allocator, ri.node_id, {});
                }
            }
        }

        if (trace_nodes) std.debug.print("graph_executor_node_trace: materialize_runtime_inputs_begin partition={d}\n", .{partition_index});
        if (trace_progress) std.debug.print("metal_partition_progress: phase=materialize_runtime_inputs_begin partition={d}\n", .{partition_index});
        const runtime_inputs_start_ns = if (collect_loop_profile) metalPartitionNowNs() else 0;
        try materializePartitionRuntimeInputs(
            allocator,
            graph,
            values,
            value_device,
            node_ids,
            device_id,
            effective_exec_ctx,
            cb,
            rt_map,
        );
        if (collect_loop_profile) loop_profile.materialize_runtime_inputs_ns += metalPartitionElapsedNs(runtime_inputs_start_ns, metalPartitionNowNs());
        if (trace_progress) std.debug.print("metal_partition_progress: phase=materialize_runtime_inputs_end partition={d}\n", .{partition_index});
        if (trace_nodes) std.debug.print("graph_executor_node_trace: materialize_runtime_inputs_end partition={d}\n", .{partition_index});

        if (trace_nodes) std.debug.print("graph_executor_node_trace: materialize_parameters_begin partition={d}\n", .{partition_index});
        if (trace_progress) std.debug.print("metal_partition_progress: phase=materialize_parameters_begin partition={d}\n", .{partition_index});
        const parameters_start_ns = if (collect_loop_profile) metalPartitionNowNs() else 0;
        try materializePartitionParameters(
            graph,
            cb,
            values,
            value_device,
            node_ids,
            reachable,
            device_id,
            rt_map,
            exec_ctx.stats,
        );
        if (collect_loop_profile) loop_profile.materialize_parameters_ns += metalPartitionElapsedNs(parameters_start_ns, metalPartitionNowNs());
        if (trace_progress) std.debug.print("metal_partition_progress: phase=materialize_parameters_end partition={d}\n", .{partition_index});
        if (trace_nodes) std.debug.print("graph_executor_node_trace: materialize_parameters_end partition={d}\n", .{partition_index});

        if (trace_nodes) std.debug.print("graph_executor_node_trace: materialize_constants_begin partition={d}\n", .{partition_index});
        if (trace_progress) std.debug.print("metal_partition_progress: phase=materialize_constants_begin partition={d}\n", .{partition_index});
        const constants_start_ns = if (collect_loop_profile) metalPartitionNowNs() else 0;
        try materializePartitionConstants(
            graph,
            cb,
            values,
            value_device,
            node_ids,
            reachable,
            device_id,
        );
        if (collect_loop_profile) loop_profile.materialize_constants_ns += metalPartitionElapsedNs(constants_start_ns, metalPartitionNowNs());
        if (trace_progress) std.debug.print("metal_partition_progress: phase=materialize_constants_end partition={d}\n", .{partition_index});
        if (trace_nodes) std.debug.print("graph_executor_node_trace: materialize_constants_end partition={d}\n", .{partition_index});

        var transient_runtime_region_plan: ?RuntimeRegionPlan = null;
        defer if (transient_runtime_region_plan) |*plan| plan.deinit(allocator);
        const runtime_region_plan_start_ns = if (collect_loop_profile) metalPartitionNowNs() else 0;
        const runtime_region_plan = try self.runtimeRegionPlan(
            allocator,
            graph,
            node_ids,
            values.len,
            reachable,
            last_use,
            exec_ctx.stats,
            &transient_runtime_region_plan,
        );
        if (collect_loop_profile) loop_profile.runtime_region_plan_ns += metalPartitionElapsedNs(runtime_region_plan_start_ns, metalPartitionNowNs());
        if (trace_progress) std.debug.print("metal_partition_progress: phase=runtime_region_plan_ready partition={d} regions={d}\n", .{ partition_index, runtime_region_plan.region_count });
        if (traceRuntimeRegionsEnabled()) printRuntimeRegionPlanSummary(graph, runtime_region_plan, partition_index);
        if (metalPartitionOpRunsEnabled()) printMetalPartitionOpRuns(graph, node_ids, reachable, last_use, partition_index);
        if (exec_ctx.stats) |stats| {
            stats.runtime_region_plan_active_regions += runtime_region_plan.region_count;
            stats.runtime_region_plan_covered_nodes += runtime_region_plan.covered_node_count;
            stats.runtime_region_plan_elided_nodes += runtime_region_plan.elided_node_count;
            stats.runtime_region_pre_skip_declined_external_consumers += runtime_region_plan.pre_skip_declined_external_consumer_count;
            const frame_eligibility = analyzeRuntimeFrameEligibility(graph, runtime_region_plan);
            recordRuntimeFrameEligibilityStats(stats, frame_eligibility);
            if (frame_eligibility.eligible()) {
                stats.runtime_frame_metadata_ready += 1;
            }
        }

        if (trace_nodes) std.debug.print("graph_executor_node_trace: begin_frame_begin partition={d}\n", .{partition_index});
        if (trace_progress) std.debug.print("metal_partition_progress: phase=begin_frame_begin partition={d}\n", .{partition_index});
        const begin_frame_start_ns = if (collect_loop_profile) metalPartitionNowNs() else 0;
        var frame_active = if (metalPartitionFrameDisabled() or runtime_region_plan.region_count == 0) false else try cb.decoderRuntimeBeginFrame();
        errdefer if (frame_active) cb.decoderRuntimeCancelFrame() catch {};
        if (collect_loop_profile) loop_profile.begin_frame_ns += metalPartitionElapsedNs(begin_frame_start_ns, metalPartitionNowNs());
        if (trace_progress) std.debug.print("metal_partition_progress: phase=begin_frame_end partition={d} active={}\n", .{ partition_index, frame_active });
        if (trace_nodes) std.debug.print("graph_executor_node_trace: begin_frame_end partition={d} active={}\n", .{ partition_index, frame_active });
        var planned_scope = if (frame_active and !metalPartitionPlannedScopeDisabled())
            try metal_compute_mod.MetalCompute.beginPlannedGraphScope(cb, .ffn)
        else
            metal_compute_mod.MetalCompute.PlannedGraphScope{};
        errdefer metal_compute_mod.MetalCompute.endPlannedGraphScope(cb, planned_scope) catch {};

        // Coarse frame chunking: split the single frame every `chunk_ops`
        // executed nodes so dead/pooled device buffers release mid-step.
        var chunk_executed: usize = 0;

        var exec_state = interpreter.ExecState{
            .attention_layer = if (exec_ctx.attention_layer) |layer| layer.* else 0,
            .options = options,
            .last_use = last_use,
            .pair_second = if (exec_ctx.pair_second) |pair| pair.* else null,
        };

        // Per-step disentangled-attention padding-mask cache (computed once,
        // reused across all attention calls this step — see AttnMaskCache).
        var attn_mask_cache: AttnMaskCache = .{};
        defer if (attn_mask_cache.mask) |m| allocator.free(m);
        defer exec_state.freeMoeState();

        const skipped_nodes = try allocator.alloc(bool, values.len);
        defer allocator.free(skipped_nodes);
        @memset(skipped_nodes, false);

        // Graph outputs must always be written to their value slots: they
        // escape the executor entirely (training extraction reads the loss
        // and LoRA gradients after partition teardown), so a fused pattern
        // or runtime region may never elide them as fused-interior nodes —
        // even when they have no consumers in the partition (the final
        // loss has none; many gradient outputs are leaves). The fusion
        // matchers and markXxxSkipped sets only consider consumer edges,
        // not `graph.outputs` membership, so the skip override below is
        // the single chokepoint that restores materialization.
        //
        // Protection extends past the outputs themselves to the tiny
        // "scalar tail" feeding them: re-executing an elided OUTPUT node is
        // useless when its INPUTS were also fused-pattern / runtime-region
        // interiors whose slots were never written (the training loss is a
        // scalar `add` of scalar loss-component reductions — exactly such a
        // tail). Walking upstream from every graph output through producers
        // whose total element count is tiny marks the whole tail
        // override-protected, so each tail node executes as a plain command
        // at its own (topologically ordered) loop position and the chain
        // materializes bottom-up. The element-count bound keeps the walk
        // from protecting real fusion targets: large interiors (hidden
        // states, attention scores, per-element losses) terminate it.
        const elision_protected_nodes = try allocator.alloc(bool, values.len);
        defer allocator.free(elision_protected_nodes);
        @memset(elision_protected_nodes, false);
        // TERMITE_DISABLE_GRAPH_OUTPUT_ELISION_OVERRIDE=1 leaves the
        // protection set empty, disabling every downstream use of the
        // mechanism (skip override, defer-heuristic exemption) at its
        // single source.
        if (!graphOutputElisionOverrideDisabled()) {
            var protect_worklist = std.ArrayListUnmanaged(NodeId).empty;
            defer protect_worklist.deinit(allocator);
            for (graph.outputs.items) |output_id| {
                const output_index: usize = @intCast(output_id);
                if (output_index >= elision_protected_nodes.len) continue;
                if (elision_protected_nodes[output_index]) continue;
                elision_protected_nodes[output_index] = true;
                try protect_worklist.append(allocator, output_id);
            }
            while (protect_worklist.pop()) |protected_id| {
                for (graph.node(protected_id).getInputs()) |input_id| {
                    if (input_id == null_node or input_id >= graph.nodeCount()) continue;
                    const input_index: usize = @intCast(input_id);
                    if (input_index >= elision_protected_nodes.len) continue;
                    if (elision_protected_nodes[input_index]) continue;
                    const input_node = graph.node(input_id);
                    // Parameters / pre-materialized constants always have
                    // values; protecting them would only widen the walk.
                    if (input_node.op == .parameter) continue;
                    if (isPreMaterializedConstantOp(input_node.op)) continue;
                    const elems = input_node.output_shape.numElements() orelse continue;
                    if (elems > graph_output_scalar_tail_max_elems) continue;
                    elision_protected_nodes[input_index] = true;
                    try protect_worklist.append(allocator, input_id);
                }
            }
        }

        try prepareRuntimeRegionPlan(
            graph,
            cb,
            values,
            value_device,
            device_id,
            runtime_region_plan,
            exec_ctx.stats,
        );

        applyRuntimeRegionPreSkippedNodes(
            graph,
            runtime_region_plan,
            skipped_nodes,
            elision_protected_nodes,
            rt_map,
            exec_ctx.stats,
        );

        // Precompute per-node consumer counts (capped at 2) once so the mul-for-add
        // deferral checks in the loop below are O(1) instead of an O(nodes) scan
        // each — avoids an O(nodes^2) term on the per-step hot path.
        const reachable_use_counts = try computeReachableUseCountsCapped(allocator, graph, reachable);
        defer allocator.free(reachable_use_counts);

        // Precompute, per node, the last position in node_ids where it is consumed
        // as an input (stored as pos+1; 0 = never) plus a graph-output flag, so the
        // "referenced after this position" query in the skip path below is O(1)
        // instead of an O(remaining-nodes) scan per skipped node.
        const partition_last_ref_plus1 = try allocator.alloc(usize, graph.nodeCount());
        defer allocator.free(partition_last_ref_plus1);
        @memset(partition_last_ref_plus1, 0);
        for (node_ids, 0..) |ref_node_id, ref_pos| {
            for (graph.node(ref_node_id).getInputs()) |input_id| {
                if (input_id == null_node or input_id >= graph.nodeCount()) continue;
                partition_last_ref_plus1[@intCast(input_id)] = ref_pos + 1;
            }
        }
        const partition_is_output = try allocator.alloc(bool, graph.nodeCount());
        defer allocator.free(partition_is_output);
        @memset(partition_is_output, false);
        for (graph.outputs.items) |output_id| {
            if (output_id != null_node and output_id < graph.nodeCount()) partition_is_output[@intCast(output_id)] = true;
        }

        var node_pos: usize = 0;
        while (node_pos < node_ids.len) : (node_pos += 1) {
            const node_id = node_ids[node_pos];
            if (collect_loop_profile) loop_profile.nodes += 1;
            const i: usize = @intCast(node_id);
            if (i >= reachable.len or !reachable[i]) continue;
            if (i < skipped_nodes.len and skipped_nodes[i]) {
                traceMetalValueLifetime("top_skip_continue", node_id, node_id);
                if (traceRuntimeRegionsEnabled()) {
                    const skipped_region = runtime_region_plan.regionAt(node_pos, node_id, node_ids);
                    if (std.meta.activeTag(skipped_region) != .none) {
                        std.debug.print(
                            "runtime_region_plan_skip: partition={d} pos={d} node={} kind={s}\n",
                            .{ partition_index, node_pos, node_id, @tagName(skipped_region) },
                        );
                    }
                }
                const protected_output = i < elision_protected_nodes.len and elision_protected_nodes[i];
                const region_pre_skipped = i < runtime_region_plan.pre_skipped_by_region.len and runtime_region_plan.pre_skipped_by_region[i];
                // Equivalent to valueReferencedAfterBoundary(graph, node_ids, node_pos, node_id)
                // but O(1) via the precomputed last-reference-position + output flags.
                const future_consumer = partition_is_output[i] or partition_last_ref_plus1[i] > node_pos + 1;
                if (!protected_output and (region_pre_skipped or !future_consumer)) continue;
                if (future_consumer and values[i] != null) continue;
                // A fused pattern / runtime region elided this node as
                // fused-interior state, but its slot must escape the elided
                // region (graph output/scalar tail, or a later unskipped
                // consumer). Clear the skip and execute the node normally
                // below; producers the fusion also elided are materialized
                // on demand below and by the interpreter-fallback safety
                // net. Any value a fused executor left behind cannot be
                // trusted (it may be a plan-slot view no kernel writes), so
                // drop it first — freeing only when this CT handle is not
                // caller-owned and not aliased by another node's slot.
                skipped_nodes[i] = false;
                if (protected_output) {
                    if (exec_ctx.stats) |stats| stats.graph_output_elision_overrides += 1;
                }
                const override_op = graph.node(node_id).op;
                const caller_or_weight_owned = rt_map.contains(node_id) or
                    override_op == .parameter or
                    isPreMaterializedConstantOp(override_op);
                if (!caller_or_weight_owned) {
                    if (values[i]) |stale| {
                        var stale_aliased = false;
                        for (values, 0..) |maybe, other_index| {
                            if (other_index != i and maybe == stale) {
                                stale_aliased = true;
                                break;
                            }
                        }
                        if (!stale_aliased) cb.free(stale);
                        values[i] = null;
                    }
                }
                // Materialize still-elided (null + skipped) inputs BEFORE
                // any execution attempt: the metal-command path consumes
                // input slots directly, so an elided-interior producer that
                // was never written must be computed first. The fallback
                // safety net only runs when the command path already
                // missed — too late for a command that silently read a
                // null/absent input.
                try materializeDeferredSkippedInputs(allocator, graph, cb, values, value_device, device_id, node_id, skipped_nodes, &exec_state, 0);
            }

            if (rt_map.contains(node_id)) {
                value_device[i] = device_id;
                continue;
            }

            if (graph.node(node_id).op == .parameter and values[i] != null) {
                value_device[i] = device_id;
                continue;
            }

            if (graph.node(node_id).op == .fused_from_float32) continue;

            // Defer heuristics may not skip override-protected nodes (graph
            // outputs / scalar-tail producers): the loop never revisits a
            // node's position, so a protected node deferred here would rely
            // on a consumer fusing it — which writes the consumer's slot,
            // not this one.
            const node_elision_protected = i < elision_protected_nodes.len and elision_protected_nodes[i];
            const defer_allowed = deferredProducerConsumerStaysInFrameChunk(
                node_ids,
                node_pos,
                node_id,
                chunk_ops,
                chunk_executed,
                last_use,
            );
            if (defer_allowed and !node_elision_protected and shouldDeferScaleMulForAdd(graph, node_id, reachable, last_use, reachable_use_counts)) {
                traceMetalValueLifetime("defer_scale_mul_for_add", node_id, @intCast(last_use[i]));
                skipped_nodes[i] = true;
                continue;
            }
            if (defer_allowed and !node_elision_protected and shouldDeferElementwiseMulForAdd(graph, node_id, reachable, last_use, reachable_use_counts)) {
                traceMetalValueLifetime("defer_elementwise_mul_for_add", node_id, @intCast(last_use[i]));
                skipped_nodes[i] = true;
                continue;
            }
            if (isPreMaterializedConstantOp(graph.node(node_id).op)) {
                if (values[i] != null) {
                    if (exec_ctx.stats) |stats| {
                        stats.constant_materializations += 1;
                        if (!collect_residency_stats) {
                            stats.device_resident_outputs += 1;
                        } else if (isMetalResidentOrQuantizedDescriptor(cb, values[i].?)) {
                            stats.device_resident_outputs += 1;
                        } else {
                            stats.host_materialized_outputs += 1;
                            stats.host_materialized_pre_materialized_constant_outputs += 1;
                            if (traceMetalHostOutputsEnabled()) traceMetalHostOutput(graph, node_id, "pre_materialized_constant");
                        }
                    }
                    value_device[i] = device_id;
                    continue;
                }
            }

            // Activation transposes are only deferred in unchunked frames: a
            // later chunk boundary may otherwise need to carry their output.
            // Parameter transposes are different. Their source parameter is
            // already stable for the full step, and the linear-dot command can
            // consume it directly, so keep deferring them under chunked
            // batch-32 training to avoid materializing hundreds of A/B/head
            // weight transposes before their fused regions run.
            if (!node_elision_protected and shouldDeferTransposeForLinearDot(graph, node_id, reachable, last_use) and
                (defer_allowed or transposeSourceIsWeightParameter(graph, node_id)))
            {
                skipped_nodes[i] = true;
                continue;
            }

            const trace_node_progress = traceMetalGraphProgressNode(node_pos, progress_interval, progress_start, progress_end);
            if (trace_node_progress) {
                printMetalProgressNode("node_begin", graph, partition_index, node_pos, node_ids.len, node_id);
            }
            if (trace_nodes) printMetalNodeTraceBegin(graph, node_id);
            if (trace_nodes) printMetalNodeTraceInputs(graph, cb, values, node_id);
            const op_start_ns = if (collect_op_stats) metalPartitionNowNs() else 0;
            const execution_start_ns = if (collect_loop_profile) metalPartitionNowNs() else 0;

            const op_plan = partition_plan.operatorPlanForNode(node_id);
            var execution_kind: ?MetalExecutionKind = null;
            if (trace_node_progress) std.debug.print("metal_partition_progress: phase=planned_region_begin partition={d} pos={d} node={}\n", .{ partition_index, node_pos, node_id });
            const runtime_region = runtime_region_plan.regionAt(node_pos, node_id, node_ids);
            const region_start_ns = if (collect_op_stats) metalPartitionNowNs() else 0;
            const planned_region_start_ns = if (collect_loop_profile) metalPartitionNowNs() else 0;
            if (try tryExecutePlannedRuntimeRegion(
                runtime_region,
                runtime_region_plan.preparedPtrAt(node_pos, node_id, node_ids),
                allocator,
                graph,
                cb,
                values,
                value_device,
                node_ids,
                node_pos,
                reachable,
                device_id,
                effective_exec_ctx,
                &exec_state,
                skipped_nodes,
                last_use,
                rt_map,
                donated,
            )) {
                if (trace_node_progress) std.debug.print("metal_partition_progress: phase=planned_region_hit partition={d} pos={d} node={}\n", .{ partition_index, node_pos, node_id });
                if (collect_loop_profile) {
                    loop_profile.planned_region_ns += metalPartitionElapsedNs(planned_region_start_ns, metalPartitionNowNs());
                    loop_profile.planned_region_hits += 1;
                }
                execution_kind = .command;
                if (exec_ctx.stats) |stats| stats.runtime_region_plan_dispatches += 1;
                if (collect_op_stats) {
                    op_execution_stats.recordRuntimeRegion(@tagName(runtime_region), metalPartitionElapsedNs(region_start_ns, metalPartitionNowNs()));
                }
            } else {
                if (collect_loop_profile) loop_profile.planned_region_ns += metalPartitionElapsedNs(planned_region_start_ns, metalPartitionNowNs());
                if (trace_node_progress) std.debug.print("metal_partition_progress: phase=planned_region_miss partition={d} pos={d} node={}\n", .{ partition_index, node_pos, node_id });
                if (trace_node_progress) std.debug.print("metal_partition_progress: phase=fused_pattern_begin partition={d} pos={d} node={}\n", .{ partition_index, node_pos, node_id });
                const fused_pattern_start_ns = if (collect_loop_profile) metalPartitionNowNs() else 0;
                if (try tryExecuteFusedMetalGraphPattern(
                    allocator,
                    graph,
                    cb,
                    values,
                    value_device,
                    node_ids,
                    node_pos,
                    reachable,
                    device_id,
                    effective_exec_ctx,
                    &exec_state,
                    skipped_nodes,
                    elision_protected_nodes,
                    runtime_region_plan,
                    last_use,
                    rt_map,
                    donated,
                )) {
                    if (trace_node_progress) std.debug.print("metal_partition_progress: phase=fused_pattern_hit partition={d} pos={d} node={}\n", .{ partition_index, node_pos, node_id });
                    if (collect_loop_profile) {
                        loop_profile.fused_pattern_ns += metalPartitionElapsedNs(fused_pattern_start_ns, metalPartitionNowNs());
                        loop_profile.fused_pattern_hits += 1;
                    }
                    execution_kind = .command;
                } else {
                    if (collect_loop_profile) loop_profile.fused_pattern_ns += metalPartitionElapsedNs(fused_pattern_start_ns, metalPartitionNowNs());
                    if (trace_node_progress) std.debug.print("metal_partition_progress: phase=fused_pattern_miss partition={d} pos={d} node={}\n", .{ partition_index, node_pos, node_id });
                    if (trace_node_progress) std.debug.print("metal_partition_progress: phase=metal_command_begin partition={d} pos={d} node={}\n", .{ partition_index, node_pos, node_id });
                    const command_path_start_ns = if (collect_loop_profile) metalPartitionNowNs() else 0;
                    var output_hint_source: ?OutputHintSource = null;
                    const output_hint: ?CT = blk: {
                        if (chunk_local_outputs) {
                            if (currentFrameChunkBoundaryPos(node_ids.len, node_pos, chunk_ops, chunk_executed)) |chunk_boundary_pos| {
                                if (chunk_local_pool.outputHintForNode(cb, graph, buffer_plan, values, node_ids, node_pos, chunk_boundary_pos, node_id)) |hint| {
                                    output_hint_source = .chunk_local;
                                    break :blk hint;
                                }
                            }
                        }
                        if (eager_arena_outputs) {
                            if (metalEagerArenaReclaimAliasesEnabled()) {
                                _ = try eager_arena.reclaimDeadAliasesForNode(
                                    allocator,
                                    graph,
                                    cb,
                                    buffer_plan,
                                    values,
                                    value_device,
                                    node_ids,
                                    node_pos,
                                    node_id,
                                    device_id,
                                    last_use,
                                    runtime_region_plan,
                                    rt_map,
                                    donated,
                                    effective_exec_ctx,
                                );
                            }
                            if (eager_arena.outputHintForNode(cb, graph, buffer_plan, values, node_id)) |hint| {
                                output_hint_source = .eager_arena;
                                break :blk hint;
                            }
                        }
                        if (slot_bound_outputs) {
                            if (self.outputHintForNode(cb, graph, buffer_plan, values, node_id)) |hint| {
                                output_hint_source = .slot_bound_pool;
                                break :blk hint;
                            }
                        }
                        break :blk null;
                    };
                    const command_output_opt = if (!metalPartitionRuntimeCommandsDisabled())
                        try tryExecuteMetalCommand(allocator, graph, cb, values, node_id, op_plan, &exec_state, output_hint, &attn_mask_cache, exec_ctx.stats, resident_input_cache_ptr)
                    else
                        null;
                    // Free an unconsumed pooled-output view (op didn't take the hint).
                    if (output_hint) |hint| {
                        const consumed = if (command_output_opt) |co| co == hint else false;
                        switch (output_hint_source orelse .slot_bound_pool) {
                            .chunk_local => {
                                if (consumed) {
                                    chunk_local_pool.recordConsumedHint();
                                } else {
                                    if (traceChunkLocalOutputHintsEnabled()) {
                                        const hinted_node = graph.node(node_id);
                                        std.debug.print(
                                            "metal_chunk_local_output_hint: unconsumed node={d} op={s} source=chunk_local\n",
                                            .{ node_id, @tagName(hinted_node.op) },
                                        );
                                    }
                                    cb.free(hint);
                                    chunk_local_pool.discardUnconsumedHintForNode(cb, buffer_plan, node_id);
                                }
                            },
                            .slot_bound_pool => {
                                if (consumed) {
                                    slot_bound_consumed_total += 1;
                                } else {
                                    slot_bound_fallback_total += 1;
                                    cb.free(hint);
                                }
                            },
                            .eager_arena => {
                                if (!consumed) cb.free(hint);
                            },
                        }
                    }
                    if (command_output_opt) |command_output| {
                        if (trace_node_progress) std.debug.print("metal_partition_progress: phase=metal_command_hit partition={d} pos={d} node={}\n", .{ partition_index, node_pos, node_id });
                        values[i] = command_output;
                        execution_kind = classifyMetalExecutionKind(graph, cb, values, node_id);
                        if (collect_loop_profile) {
                            loop_profile.command_path_ns += metalPartitionElapsedNs(command_path_start_ns, metalPartitionNowNs());
                            loop_profile.command_path_hits += 1;
                        }
                    } else {
                        if (trace_node_progress) std.debug.print("metal_partition_progress: phase=metal_command_miss partition={d} pos={d} node={}\n", .{ partition_index, node_pos, node_id });
                        if (trace_node_progress) std.debug.print("metal_partition_progress: phase=interpreter_begin partition={d} pos={d} node={}\n", .{ partition_index, node_pos, node_id });
                        if (interpreterFallbackHasMissingInput(graph, values, node_id)) {
                            // Safety net: a producer skipped by the defer
                            // heuristics was not fused by this consumer.
                            // Materialize it on demand instead of aborting the
                            // whole partition with MissingRuntimeInput.
                            try materializeDeferredSkippedInputs(allocator, graph, cb, values, value_device, device_id, node_id, skipped_nodes, &exec_state, 0);
                        }
                        if (interpreterFallbackHasMissingInput(graph, values, node_id)) {
                            printInterpreterFallbackNullInputs(graph, values, node_id, partition_index, node_pos);
                            return error.MissingRuntimeInput;
                        }
                        values[i] = try interpreter.executeNode(graph, cb, values, node_id, &exec_state);
                        if (collect_loop_profile) {
                            loop_profile.interpreter_ns += metalPartitionElapsedNs(command_path_start_ns, metalPartitionNowNs());
                            loop_profile.interpreter_hits += 1;
                        }
                        if (trace_node_progress) std.debug.print("metal_partition_progress: phase=interpreter_end partition={d} pos={d} node={}\n", .{ partition_index, node_pos, node_id });
                    }
                }
            }
            if (collect_loop_profile) {
                loop_profile.executed_nodes += 1;
                loop_profile.execution_ns += metalPartitionElapsedNs(execution_start_ns, metalPartitionNowNs());
            }
            const stats_start_ns = if (collect_loop_profile) metalPartitionNowNs() else 0;
            if (graph.node(node_id).op == .constant) {
                if (values[i]) |current| {
                    if (!isMetalDeviceResident(cb, current)) {
                        if (try makeMetalDeviceResident(cb, current)) |device_value| {
                            if (device_value != current) {
                                cb.free(current);
                                values[i] = device_value;
                            }
                        }
                    }
                }
            }
            if (exec_ctx.stats) |stats| {
                const op_name = @tagName(graph.node(node_id).op);
                const elapsed_ns = if (collect_op_stats) metalPartitionElapsedNs(op_start_ns, metalPartitionNowNs()) else 0;
                if (execution_kind) |kind| {
                    switch (kind) {
                        .command => {
                            stats.backend_command_dispatches += 1;
                            recordMetalCommandDispatchFamily(stats, graph, node_id);
                            if (op_plan != null) {
                                stats.planned_operator_dispatches += 1;
                                recordQuantKernelCompilerPlan(stats, graph.node(node_id).op, op_plan.?);
                            }
                            if (collect_op_stats) {
                                op_execution_stats.recordCommand(op_name, elapsed_ns);
                                op_execution_stats.recordCommandClass(graph, node_id, node_pos, elapsed_ns);
                                op_execution_stats.recordDotCommand(graph, node_id, node_pos, elapsed_ns);
                            }
                        },
                        .metadata_alias => stats.metadata_aliases += 1,
                        .descriptor_materialization => stats.descriptor_materializations += 1,
                        .constant_materialization => stats.constant_materializations += 1,
                    }
                } else {
                    stats.interpreter_fallbacks += 1;
                    if (collect_op_stats) op_execution_stats.recordFallback(op_name, elapsed_ns);
                }
                if (collect_residency_stats) {
                    if (values[i]) |node_value| {
                        const output_resident = isMetalResidentOrQuantizedDescriptor(cb, node_value);
                        if (output_resident) {
                            stats.device_resident_outputs += 1;
                        } else {
                            stats.host_materialized_outputs += 1;
                            if (execution_kind != null) {
                                stats.host_materialized_command_outputs += 1;
                            } else {
                                stats.host_materialized_interpreter_outputs += 1;
                            }
                            if (collect_op_stats) {
                                op_execution_stats.recordHostOutput(op_name, elapsed_ns);
                                op_execution_stats.recordHostOutputReason(hostOutputReasonName(execution_kind), elapsed_ns);
                            }
                            if (traceMetalHostOutputsEnabled()) traceMetalHostOutput(graph, node_id, hostOutputReasonName(execution_kind));
                        }
                        recordGemmaRuntimeResidency(stats, graph, node_id, output_resident);
                    } else {
                        stats.device_resident_outputs += 1;
                    }
                } else if (execution_kind != null) {
                    stats.device_resident_outputs += 1;
                } else {
                    // Residency inspection is optional on the hot path, but a
                    // null execution kind is not unattributed: it is exactly
                    // the interpreter fallback counted above. Keep the
                    // conservative host classification while preserving its
                    // known provenance for strict accelerator diagnostics.
                    stats.host_materialized_outputs += 1;
                    stats.host_materialized_interpreter_outputs += 1;
                }
            }
            if (collect_loop_profile) loop_profile.stats_ns += metalPartitionElapsedNs(stats_start_ns, metalPartitionNowNs());
            value_device[i] = device_id;
            if (values[i] != null) traceMetalValueLifetime("value_set", node_id, node_id);
            const traced_command = if (execution_kind) |kind| kind == .command else false;
            if (trace_nodes) {
                if (values[i]) |node_value| printMetalNodeTraceEnd(graph, cb, node_id, node_value, traced_command);
            }
            if (resident_input_cache_ptr) |cache| {
                cache.releaseAfterConsumer(cb, graph, node_id, exec_ctx.stats);
            }

            const alias_clone_start_ns = if (collect_loop_profile) metalPartitionNowNs() else 0;
            try interpreter.cloneOutputIfAliasedInputWouldBeFreed(
                allocator,
                graph,
                cb,
                values,
                node_id,
                last_use,
                rt_map,
                donated,
            );
            if (collect_loop_profile) loop_profile.alias_clone_ns += metalPartitionElapsedNs(alias_clone_start_ns, metalPartitionNowNs());

            const free_expired_start_ns = if (collect_loop_profile) metalPartitionNowNs() else 0;
            const expired_free_stats = try freeExpiredInputs(
                allocator,
                graph,
                cb,
                values,
                value_device,
                node_id,
                device_id,
                last_use,
                runtime_region_plan,
                rt_map,
                donated,
                effective_exec_ctx,
            );
            if (collect_loop_profile) loop_profile.free_expired_ns += metalPartitionElapsedNs(free_expired_start_ns, metalPartitionNowNs());

            if (i < skipped_nodes.len and skipped_nodes[i]) {
                traceMetalValueLifetime("skip_node_clear", node_id, node_id);
                values[i] = null;
            }
            if (trace_node_progress) {
                printMetalProgressNode("node_end", graph, partition_index, node_pos, node_ids.len, node_id);
            }

            var forced_liveness_boundary = false;
            const expired_boundary_threshold = metalFrameChunkExpiredBytesThreshold();
            if (expired_boundary_threshold > 0 and frame_active and node_pos + 1 < node_ids.len and expired_free_stats.bytes >= expired_boundary_threshold) {
                forced_liveness_boundary = true;
                chunk_executed = 0;
                const swept = try sweepExpiredValuesThroughNode(
                    allocator,
                    graph,
                    cb,
                    values,
                    value_device,
                    node_ids,
                    node_pos,
                    node_id,
                    device_id,
                    last_use,
                    runtime_region_plan,
                    rt_map,
                    donated,
                    effective_exec_ctx,
                );
                metal_compute_mod.MetalCompute.endPlannedGraphScope(cb, planned_scope) catch {};
                planned_scope = metal_compute_mod.MetalCompute.PlannedGraphScope{};
                try cb.decoderRuntimeSubmitAndWaitFrame();
                frame_active = false;
                const promoted = try promoteLiveValuesAcrossFrameBoundary(
                    allocator,
                    graph,
                    cb,
                    values,
                    value_device,
                    node_ids,
                    node_pos,
                    device_id,
                    rt_map,
                    donated,
                    effective_exec_ctx,
                );
                if (exec_ctx.stats) |stats| {
                    stats.metal_frame_chunk_boundaries += 1;
                    stats.metal_frame_chunk_promoted_values += promoted;
                    stats.metal_frame_chunk_swept_values += swept + expired_free_stats.count;
                }
                if (chunk_local_outputs) {
                    try chunk_local_pool.resetAfterChunk(allocator, cb, buffer_plan, values);
                }
                frame_active = try cb.decoderRuntimeBeginFrame();
                planned_scope = if (frame_active and !metalPartitionPlannedScopeDisabled())
                    try metal_compute_mod.MetalCompute.beginPlannedGraphScope(cb, .ffn)
                else
                    metal_compute_mod.MetalCompute.PlannedGraphScope{};
            }

            // Coarse frame chunking boundary: after every `chunk_ops` executed
            // nodes, sweep values whose last use is behind the boundary,
            // submit+wait the current frame, then copy still-live values into
            // owned storage before reopening a fresh frame + scope. Promoting
            // after submit avoids duplicating the cross-boundary live set
            // inside the same command-buffer window.
            if (!forced_liveness_boundary and chunk_ops > 0 and frame_active and node_pos + 1 < node_ids.len) {
                chunk_executed += 1;
                if (chunk_executed >= chunk_ops) {
                    chunk_executed = 0;
                    const swept = try sweepExpiredValuesThroughNode(
                        allocator,
                        graph,
                        cb,
                        values,
                        value_device,
                        node_ids,
                        node_pos,
                        node_id,
                        device_id,
                        last_use,
                        runtime_region_plan,
                        rt_map,
                        donated,
                        effective_exec_ctx,
                    );
                    metal_compute_mod.MetalCompute.endPlannedGraphScope(cb, planned_scope) catch {};
                    planned_scope = metal_compute_mod.MetalCompute.PlannedGraphScope{};
                    try cb.decoderRuntimeSubmitAndWaitFrame();
                    frame_active = false;
                    const promoted = try promoteLiveValuesAcrossFrameBoundary(
                        allocator,
                        graph,
                        cb,
                        values,
                        value_device,
                        node_ids,
                        node_pos,
                        device_id,
                        rt_map,
                        donated,
                        effective_exec_ctx,
                    );
                    if (exec_ctx.stats) |stats| {
                        stats.metal_frame_chunk_boundaries += 1;
                        stats.metal_frame_chunk_promoted_values += promoted;
                        stats.metal_frame_chunk_swept_values += swept;
                    }
                    if (chunk_local_outputs) {
                        try chunk_local_pool.resetAfterChunk(allocator, cb, buffer_plan, values);
                    }
                    frame_active = try cb.decoderRuntimeBeginFrame();
                    planned_scope = if (frame_active and !metalPartitionPlannedScopeDisabled())
                        try metal_compute_mod.MetalCompute.beginPlannedGraphScope(cb, .ffn)
                    else
                        metal_compute_mod.MetalCompute.PlannedGraphScope{};
                }
            }
        }

        if (frame_active) {
            try metal_compute_mod.MetalCompute.endPlannedGraphScope(cb, planned_scope);
            planned_scope = metal_compute_mod.MetalCompute.PlannedGraphScope{};
            // Drain BEFORE copying graph outputs to owned storage. The
            // previous order encoded the owned-copy blits into the
            // partition's frame ahead of the submit, but runtime region
            // plans commit work through their own command buffers and
            // recycle runtime-owned storage (graph-plan slots, frame
            // scratch) on plan/frame boundaries, so a blit scheduled
            // inside the frame could observe pre-completion (zeroed)
            // bytes: the trainer read loss=0 and all-zero gradients even
            // though per-node parity probes saw exact values. Submitting
            // and waiting the frame first guarantees every device write
            // for this partition has completed; the copies then run as
            // synchronous one-shot blits (bounded: once per step over
            // graph outputs only).
            if (trace_progress) std.debug.print("metal_partition_progress: phase=submit_frame_begin partition={d}\n", .{partition_index});
            const submit_frame_start_ns = if (collect_loop_profile) metalPartitionNowNs() else 0;
            try cb.decoderRuntimeSubmitAndWaitFrame();
            if (collect_loop_profile) loop_profile.submit_frame_ns += metalPartitionElapsedNs(submit_frame_start_ns, metalPartitionNowNs());
            frame_active = false;
            if (trace_progress) std.debug.print("metal_partition_progress: phase=submit_frame_end partition={d}\n", .{partition_index});
        } else {
            try metal_compute_mod.MetalCompute.endPlannedGraphScope(cb, planned_scope);
            planned_scope = metal_compute_mod.MetalCompute.PlannedGraphScope{};
        }

        if (exec_ctx.materialize_boundary_outputs) {
            if (trace_progress) std.debug.print("metal_partition_progress: phase=materialize_boundary_outputs_begin partition={d}\n", .{partition_index});
            if (exec_ctx.stats) |stats| {
                stats.boundary_output_materializations += countPartitionBoundaryOutputs(partition_view);
            }
            const boundary_outputs_start_ns = if (collect_loop_profile) metalPartitionNowNs() else 0;
            try evalPartitionBoundaryOutputs(cb, values, partition_view);
            if (collect_loop_profile) loop_profile.boundary_outputs_ns += metalPartitionElapsedNs(boundary_outputs_start_ns, metalPartitionNowNs());
            if (trace_progress) std.debug.print("metal_partition_progress: phase=materialize_boundary_outputs_end partition={d}\n", .{partition_index});
        } else {
            // Metal-resident partitions skip boundary materialization for
            // cross-partition outputs (downstream consumers read device
            // storage directly), but GRAPH outputs escape the executor
            // entirely: callers (e.g. training extraction) read them on the
            // host after this partition's frame has been submitted and
            // drained — and possibly after further device work (optimizer
            // steps, the next training step) has recycled the runtime-owned
            // plan-slot storage backing them. Deep-copy each graph output
            // into device memory its CT owns (the frame was already
            // submitted and waited above, so each copy is a synchronous
            // one-shot blit ordered after every device write), then
            // synchronize so a host mirror cached before the final device
            // writes or an unmaterialized lazy product cannot serve
            // stale/empty data.
            if (trace_progress) std.debug.print("metal_partition_progress: phase=sync_graph_outputs_begin partition={d}\n", .{partition_index});
            const graph_outputs_start_ns = if (collect_loop_profile) metalPartitionNowNs() else 0;
            if (!graphOutputOwnedCopyDisabled()) {
                try copyPartitionGraphOutputsToOwnedStorage(cb, values, partition_view, rt_map, exec_ctx.stats);
            }
            // syncPartitionGraphOutputs also materializes pending lazy
            // products; the TERMITE_DISABLE_OUTPUT_HOST_MIRROR_RESYNC
            // kill-switch is honored inside syncOutputTensor so only the
            // mirror re-download is skipped.
            try syncPartitionGraphOutputs(cb, values, partition_view);
            if (collect_loop_profile) loop_profile.boundary_outputs_ns += metalPartitionElapsedNs(graph_outputs_start_ns, metalPartitionNowNs());
            if (trace_progress) std.debug.print("metal_partition_progress: phase=sync_graph_outputs_end partition={d}\n", .{partition_index});
        }

        if (exec_ctx.attention_layer) |layer| layer.* = exec_state.attention_layer;
        if (exec_ctx.pair_second) |pair| pair.* = exec_state.pair_second;
        if (collect_op_stats) {
            printOpExecutionStats("metal_partition_command_ops", &op_execution_stats.command_counts, op_execution_stats.command_used);
            printCommandExecutionStats("metal_partition_command_classes", &op_execution_stats.command_class_counts, op_execution_stats.command_class_used);
            printOpExecutionStats("metal_partition_runtime_regions", &op_execution_stats.runtime_region_counts, op_execution_stats.runtime_region_used);
            printDotShapeExecutionStats("metal_partition_command_dot_shapes", &op_execution_stats.dot_command_shapes, op_execution_stats.dot_command_shape_used);
            printOpExecutionStats("metal_partition_fallback_ops", &op_execution_stats.fallback_counts, op_execution_stats.fallback_used);
            printOpExecutionStats("metal_partition_host_output_ops", &op_execution_stats.host_output_counts, op_execution_stats.host_output_used);
            printOpExecutionStats("metal_partition_host_output_reasons", &op_execution_stats.host_output_reason_counts, op_execution_stats.host_output_reason_used);
        }
        if (collect_loop_profile) loop_profile.print("metal_partition_loop_profile");
        if (slot_bound_outputs) {
            std.debug.print(
                "metal_slot_bound_outputs: consumed={d} fallback={d} pool_allocs={d} pool_reuses={d} pool_budget_declines={d} pool_live_bytes={d} pool_peak_bytes={d}\n",
                .{
                    slot_bound_consumed_total,
                    slot_bound_fallback_total,
                    self.output_pool.allocationCount(),
                    self.output_pool.allocation_reuses,
                    self.output_pool.budget_declines,
                    self.output_pool.bytes_live,
                    self.output_pool.peak_bytes_live,
                },
            );
        }
        if (eager_arena_outputs) {
            eager_arena.recordStats(exec_ctx.stats);
            std.debug.print(
                "metal_eager_arena: peak_bytes={d} live_bytes={d} allocations={d} reuse_hits={d} spill_bytes={d} hazard_declines={d} alias_conflicts={d} alias_reclaims={d} alias_reclaim_bytes={d}\n",
                .{
                    eager_arena.peak_bytes,
                    eager_arena.live_bytes,
                    eager_arena.allocations,
                    eager_arena.reuse_hits,
                    eager_arena.spill_bytes,
                    eager_arena.hazard_declines,
                    eager_arena.alias_conflicts,
                    eager_arena.alias_reclaims,
                    eager_arena.alias_reclaim_bytes,
                },
            );
        }
        if (chunk_local_outputs) {
            chunk_local_pool.recordStats(exec_ctx.stats);
            std.debug.print(
                "metal_chunk_local_outputs: peak_bytes={d} live_bytes={d} allocations={d} reuse_hits={d} consumed_hints={d} unconsumed_hints={d} spill_bytes={d} alias_conflicts={d} resets={d} reset_freed_bytes={d} discard_freed_bytes={d} reset_live_carry_values={d}\n",
                .{
                    chunk_local_pool.peak_bytes,
                    chunk_local_pool.live_bytes,
                    chunk_local_pool.allocations,
                    chunk_local_pool.reuse_hits,
                    chunk_local_pool.consumed_hints,
                    chunk_local_pool.unconsumed_hints,
                    chunk_local_pool.spill_bytes,
                    chunk_local_pool.alias_conflicts,
                    chunk_local_pool.reset_count,
                    chunk_local_pool.reset_freed_bytes,
                    chunk_local_pool.discard_freed_bytes,
                    chunk_local_pool.reset_live_carry_values,
                },
            );
        }
        if (trace_progress) std.debug.print("metal_partition_progress: phase=executor_end partition={d}\n", .{partition_index});
    }
};

fn recordOpCount(counts: []OpExecutionCount, used: *usize, name: []const u8, elapsed_ns: u64) void {
    for (counts[0..used.*]) |*entry| {
        if (std.mem.eql(u8, entry.name, name)) {
            entry.count += 1;
            entry.total_ns += elapsed_ns;
            return;
        }
    }
    if (used.* >= counts.len) return;
    counts[used.*] = .{ .name = name, .count = 1, .total_ns = elapsed_ns };
    used.* += 1;
}

fn sortOpExecutionCounts(counts: []OpExecutionCount) void {
    std.mem.sort(OpExecutionCount, counts, {}, struct {
        fn lessThan(_: void, a: OpExecutionCount, b: OpExecutionCount) bool {
            if (a.total_ns == b.total_ns) {
                if (a.count == b.count) return std.mem.lessThan(u8, a.name, b.name);
                return a.count > b.count;
            }
            return a.total_ns > b.total_ns;
        }
    }.lessThan);
}

fn printOpExecutionStats(label: []const u8, counts: []OpExecutionCount, used: usize) void {
    var sorted_buf = [_]OpExecutionCount{.{}} ** 96;
    const n = @min(used, sorted_buf.len);
    @memcpy(sorted_buf[0..n], counts[0..n]);
    sortOpExecutionCounts(sorted_buf[0..n]);
    std.debug.print("{s}: ", .{label});
    if (n == 0) {
        std.debug.print("none\n", .{});
        return;
    }
    const limit = @min(n, 16);
    for (sorted_buf[0..limit], 0..) |entry, idx| {
        if (idx > 0) std.debug.print(",", .{});
        const avg_ms = if (entry.count == 0) 0.0 else nsToMs(entry.total_ns) / @as(f64, @floatFromInt(entry.count));
        std.debug.print("{s}:count={d}:total_ms={d:.3}:avg_ms={d:.3}", .{ entry.name, entry.count, nsToMs(entry.total_ns), avg_ms });
    }
    if (n > limit) std.debug.print(",...", .{});
    std.debug.print("\n", .{});
}

const CommandExecutionSummary = struct {
    op_name: []const u8 = "",
    phase: []const u8 = "",
    family: []const u8 = "",
    source_op: []const u8 = "",
    first_node: NodeId = null_node,
    first_pos: usize = 0,
    last_node: NodeId = null_node,
    last_pos: usize = 0,
    count: usize = 0,
    total_ns: u64 = 0,
};

fn sameCommandExecutionSummary(a: CommandExecutionSummary, b: CommandExecutionSummary) bool {
    return std.mem.eql(u8, a.op_name, b.op_name) and
        std.mem.eql(u8, a.phase, b.phase) and
        std.mem.eql(u8, a.family, b.family) and
        std.mem.eql(u8, a.source_op, b.source_op);
}

fn recordCommandExecutionSummary(
    graph: *const Graph,
    node_id: NodeId,
    node_pos: usize,
    summaries: *[160]CommandExecutionSummary,
    used: *usize,
    elapsed_ns: u64,
) void {
    if (node_id == null_node or node_id >= graph.nodeCount()) return;
    const node = graph.node(node_id);
    const classification = commandSourceClassification(graph, node_id, 4);
    const summary = CommandExecutionSummary{
        .op_name = @tagName(std.meta.activeTag(node.op)),
        .phase = classification.phase,
        .family = classification.family,
        .source_op = classification.source_op,
        .first_node = node_id,
        .first_pos = node_pos,
        .last_node = node_id,
        .last_pos = node_pos,
    };
    for (summaries[0..used.*]) |*entry| {
        if (!sameCommandExecutionSummary(entry.*, summary)) continue;
        entry.count += 1;
        entry.total_ns += elapsed_ns;
        entry.last_node = node_id;
        entry.last_pos = node_pos;
        return;
    }
    if (used.* >= summaries.len) return;
    summaries[used.*] = summary;
    summaries[used.*].count = 1;
    summaries[used.*].total_ns = elapsed_ns;
    used.* += 1;
}

fn sortCommandExecutionSummaries(summaries: []CommandExecutionSummary) void {
    std.mem.sort(CommandExecutionSummary, summaries, {}, struct {
        fn lessThan(_: void, a: CommandExecutionSummary, b: CommandExecutionSummary) bool {
            if (a.total_ns == b.total_ns) {
                if (a.count == b.count) return std.mem.lessThan(u8, a.op_name, b.op_name);
                return a.count > b.count;
            }
            return a.total_ns > b.total_ns;
        }
    }.lessThan);
}

fn printCommandExecutionStats(label: []const u8, summaries: []const CommandExecutionSummary, used: usize) void {
    var sorted_buf = [_]CommandExecutionSummary{.{}} ** 160;
    const n = @min(used, sorted_buf.len);
    @memcpy(sorted_buf[0..n], summaries[0..n]);
    sortCommandExecutionSummaries(sorted_buf[0..n]);
    std.debug.print("{s}: ", .{label});
    if (n == 0) {
        std.debug.print("none\n", .{});
        return;
    }
    const limit = @min(n, 24);
    for (sorted_buf[0..limit], 0..) |entry, idx| {
        if (idx > 0) std.debug.print(",", .{});
        const avg_ms = if (entry.count == 0) 0.0 else nsToMs(entry.total_ns) / @as(f64, @floatFromInt(entry.count));
        std.debug.print(
            "{s}:phase={s}:family={s}:source={s}:count={d}:total_ms={d:.3}:avg_ms={d:.3}:pos={d}-{d}:node={}-{}",
            .{
                entry.op_name,
                entry.phase,
                entry.family,
                entry.source_op,
                entry.count,
                nsToMs(entry.total_ns),
                avg_ms,
                entry.first_pos,
                entry.last_pos,
                entry.first_node,
                entry.last_node,
            },
        );
    }
    if (n > limit) std.debug.print(",...", .{});
    std.debug.print("\n", .{});
}

fn sameDotShapeExecutionSummary(a: DotShapeExecutionSummary, b: DotShapeExecutionSummary) bool {
    return a.lhs0 == b.lhs0 and
        a.lhs1 == b.lhs1 and
        a.rhs0 == b.rhs0 and
        a.rhs1 == b.rhs1 and
        a.out0 == b.out0 and
        a.out1 == b.out1 and
        a.rhs_transpose == b.rhs_transpose and
        a.rhs_parameter == b.rhs_parameter and
        a.rhs_lora == b.rhs_lora and
        std.mem.eql(u8, a.phase, b.phase) and
        std.mem.eql(u8, a.family, b.family) and
        std.mem.eql(u8, a.lhs_source_op, b.lhs_source_op) and
        std.mem.eql(u8, a.rhs_source_op, b.rhs_source_op);
}

fn recordDotShapeExecutionSummary(
    graph: *const Graph,
    node_id: NodeId,
    node_pos: usize,
    summaries: *[128]DotShapeExecutionSummary,
    used: *usize,
    elapsed_ns: u64,
) void {
    if (node_id == null_node or node_id >= graph.nodeCount()) return;
    const node = graph.node(node_id);
    switch (node.op) {
        .dot_general => {},
        else => return,
    }
    if (node.num_inputs < 2) return;
    const lhs_id = node.inputs[0];
    const rhs_id = node.inputs[1];
    if (lhs_id == null_node or rhs_id == null_node or lhs_id >= graph.nodeCount() or rhs_id >= graph.nodeCount()) return;
    const lhs_shape = graph.node(lhs_id).output_shape;
    const rhs_shape = graph.node(rhs_id).output_shape;
    const out_shape = node.output_shape;
    if (lhs_shape.rank() != 2 or rhs_shape.rank() != 2 or out_shape.rank() != 2) return;

    const lhs_source = dotSourceInfo(graph, lhs_id);
    const rhs_source = dotSourceInfo(graph, rhs_id);

    const summary = DotShapeExecutionSummary{
        .first_lhs_id = lhs_id,
        .first_rhs_id = rhs_id,
        .first_rhs_source_id = rhs_source.source_id,
        .lhs0 = lhs_shape.dims[0],
        .lhs1 = lhs_shape.dims[1],
        .rhs0 = rhs_shape.dims[0],
        .rhs1 = rhs_shape.dims[1],
        .out0 = out_shape.dims[0],
        .out1 = out_shape.dims[1],
        .rhs_transpose = rhs_source.is_transpose,
        .rhs_parameter = rhs_source.is_parameter,
        .rhs_lora = rhs_source.is_lora,
        .phase = classifyDotPhase(lhs_source, rhs_source),
        .family = classifyDotParameterFamily(rhs_source.parameter_name orelse lhs_source.parameter_name),
        .lhs_source_op = lhs_source.op_name,
        .rhs_source_op = rhs_source.op_name,
        .first_node = node_id,
        .first_pos = node_pos,
        .last_node = node_id,
        .last_pos = node_pos,
    };
    for (summaries[0..used.*]) |*entry| {
        if (!sameDotShapeExecutionSummary(entry.*, summary)) continue;
        entry.count += 1;
        entry.total_ns += elapsed_ns;
        entry.last_node = node_id;
        entry.last_pos = node_pos;
        return;
    }
    if (used.* >= summaries.len) return;
    summaries[used.*] = summary;
    summaries[used.*].count = 1;
    summaries[used.*].total_ns = elapsed_ns;
    used.* += 1;
}

fn sortDotShapeExecutionSummaries(summaries: []DotShapeExecutionSummary) void {
    std.mem.sort(DotShapeExecutionSummary, summaries, {}, struct {
        fn lessThan(_: void, a: DotShapeExecutionSummary, b: DotShapeExecutionSummary) bool {
            if (a.total_ns == b.total_ns) {
                if (a.count == b.count) {
                    if (a.lhs0 != b.lhs0) return a.lhs0 < b.lhs0;
                    if (a.lhs1 != b.lhs1) return a.lhs1 < b.lhs1;
                    if (a.rhs0 != b.rhs0) return a.rhs0 < b.rhs0;
                    return a.rhs1 < b.rhs1;
                }
                return a.count > b.count;
            }
            return a.total_ns > b.total_ns;
        }
    }.lessThan);
}

fn printDotShapeExecutionStats(label: []const u8, summaries: []const DotShapeExecutionSummary, used: usize) void {
    var sorted_buf = [_]DotShapeExecutionSummary{.{}} ** 128;
    const n = @min(used, sorted_buf.len);
    @memcpy(sorted_buf[0..n], summaries[0..n]);
    sortDotShapeExecutionSummaries(sorted_buf[0..n]);
    std.debug.print("{s}: ", .{label});
    if (n == 0) {
        std.debug.print("none\n", .{});
        return;
    }
    const limit = @min(n, 16);
    for (sorted_buf[0..limit], 0..) |entry, idx| {
        if (idx > 0) std.debug.print(",", .{});
        const avg_ms = if (entry.count == 0) 0.0 else nsToMs(entry.total_ns) / @as(f64, @floatFromInt(entry.count));
        std.debug.print(
            "{d}x{d}*{d}x{d}->{d}x{d}:count={d}:total_ms={d:.3}:avg_ms={d:.3}:pos={d}-{d}:node={}-{}:lhs_id={}:rhs_id={}:rhs_source={}:phase={s}:family={s}:lhs={s}:rhs={s}:rhs_transpose={}:rhs_parameter={}:rhs_lora={}",
            .{
                entry.lhs0,
                entry.lhs1,
                entry.rhs0,
                entry.rhs1,
                entry.out0,
                entry.out1,
                entry.count,
                nsToMs(entry.total_ns),
                avg_ms,
                entry.first_pos,
                entry.last_pos,
                entry.first_node,
                entry.last_node,
                entry.first_lhs_id,
                entry.first_rhs_id,
                entry.first_rhs_source_id,
                entry.phase,
                entry.family,
                entry.lhs_source_op,
                entry.rhs_source_op,
                entry.rhs_transpose,
                entry.rhs_parameter,
                entry.rhs_lora,
            },
        );
    }
    if (n > limit) std.debug.print(",...", .{});
    std.debug.print("\n", .{});
}

fn printInterpreterFallbackNullInputs(
    graph: *const Graph,
    values: []?CT,
    node_id: NodeId,
    partition_index: usize,
    node_pos: usize,
) void {
    const node = graph.node(node_id);
    var missing = false;
    for (node.getInputs()) |input_id| {
        const input_index: usize = @intCast(input_id);
        if (input_index >= values.len or values[input_index] == null) {
            missing = true;
            break;
        }
    }
    if (!missing) return;
    std.debug.print("metal_partition_interpreter_null_inputs: partition={d} pos={d} node={} op={s} inputs=", .{
        partition_index,
        node_pos,
        node_id,
        @tagName(node.op),
    });
    for (node.getInputs(), 0..) |input_id, idx| {
        if (idx > 0) std.debug.print(",", .{});
        const input_index: usize = @intCast(input_id);
        const input_op = if (input_id < graph.nodeCount()) @tagName(graph.node(input_id).op) else "invalid";
        const state = if (input_index < values.len and values[input_index] != null) "set" else "null";
        std.debug.print("{}:{s}:{s}", .{ input_id, input_op, state });
    }
    std.debug.print("\n", .{});
}

fn interpreterFallbackHasMissingInput(
    graph: *const Graph,
    values: []?CT,
    node_id: NodeId,
) bool {
    if (node_id == null_node or node_id >= graph.nodeCount()) return true;
    const node = graph.node(node_id);
    for (node.getInputs()) |input_id| {
        if (input_id == null_node) continue;
        const input_index: usize = @intCast(input_id);
        if (input_index >= values.len or values[input_index] == null) return true;
    }
    return false;
}

const max_deferred_input_materialization_depth: usize = 4;

/// On-demand materialization safety net for the defer heuristics
/// (shouldDeferScaleMulForAdd / shouldDeferElementwiseMulForAdd /
/// shouldDeferTransposeForLinearDot). Those heuristics skip a producer node
/// expecting its single consumer to fuse it; when the consumer ends up on an
/// execution path that does not fuse (planned runtime region, fused graph
/// pattern, or interpreter fallback), the deferred output is still null when
/// the consumer executes. Execute such skipped producers here so the consumer
/// can proceed, instead of aborting the whole partition (which previously
/// forced an expensive full-step retry on the regular compiled path).
///
/// Inputs of a skipped node are never freed by last-use bookkeeping (the
/// skipped node never "executes"), so its operands are still available.
fn materializeDeferredSkippedInputs(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    value_device: []DeviceId,
    device_id: DeviceId,
    node_id: NodeId,
    skipped_nodes: []bool,
    exec_state: *interpreter.ExecState,
    depth: usize,
) !void {
    if (depth >= max_deferred_input_materialization_depth) return;
    if (node_id == null_node or node_id >= graph.nodeCount()) return;
    const node = graph.node(node_id);
    for (node.getInputs()) |input_id| {
        if (input_id == null_node) continue;
        const input_index: usize = @intCast(input_id);
        if (input_index >= values.len) continue;
        if (values[input_index] != null) continue;
        if (input_index >= skipped_nodes.len or !skipped_nodes[input_index]) continue;
        // Deferred producers can chain (e.g. a deferred transpose feeding a
        // deferred mul); resolve the producer's own skipped inputs first.
        try materializeDeferredSkippedInputs(allocator, graph, cb, values, value_device, device_id, input_id, skipped_nodes, exec_state, depth + 1);
        if (interpreterFallbackHasMissingInput(graph, values, input_id)) {
            traceFirstMissingInput(graph, values, input_id, "materialize_deferred_missing_input");
            continue;
        }
        const materialized = (try tryExecuteMetalCommand(allocator, graph, cb, values, input_id, null, exec_state, null, null, null, null)) orelse
            try interpreter.executeNode(graph, cb, values, input_id, exec_state);
        values[input_index] = materialized;
        if (input_index < value_device.len) value_device[input_index] = device_id;
        skipped_nodes[input_index] = false;
        traceMetalValueLifetime("materialize_deferred_input", input_id, input_id);
    }
}

fn traceFirstMissingInput(
    graph: *const Graph,
    values: []?CT,
    node_id: NodeId,
    event: []const u8,
) void {
    if (node_id == null_node or node_id >= graph.nodeCount()) return;
    const node = graph.node(node_id);
    for (node.getInputs()) |input_id| {
        if (input_id == null_node) continue;
        const input_index: usize = @intCast(input_id);
        if (input_index < values.len and values[input_index] != null) continue;
        traceMetalValueLifetime(event, node_id, input_id);
        return;
    }
}

fn metalPartitionOpStatsEnabled() bool {
    return platform.env.getenvBoolDefault("TERMITE_METAL_PARTITION_OP_STATS", false);
}

fn metalPartitionLoopProfileEnabled() bool {
    return platform.env.getenvBoolDefault("TERMITE_METAL_PARTITION_LOOP_PROFILE", false);
}

fn metalPartitionResidencyStatsEnabled() bool {
    return platform.env.getenvBoolDefault("TERMITE_METAL_PARTITION_RESIDENCY_STATS", false);
}

fn traceMetalHostOutputsEnabled() bool {
    return platform.env.getenvBoolDefault("TERMITE_METAL_TRACE_HOST_OUTPUTS", false);
}

fn traceRuntimeCommandFallbacksEnabled() bool {
    return platform.env.getenvBoolDefault("TERMITE_METAL_TRACE_RUNTIME_COMMAND_FALLBACKS", false);
}

fn metalPartitionOpRunsEnabled() bool {
    return platform.env.getenvBoolDefault("TERMITE_METAL_PARTITION_OP_RUNS", false);
}

fn hostOutputReasonName(execution_kind: ?MetalExecutionKind) []const u8 {
    return if (execution_kind) |kind| switch (kind) {
        .command => "command_host_output",
        .metadata_alias => "metadata_alias_host_output",
        .descriptor_materialization => "descriptor_materialization_host_output",
        .constant_materialization => "constant_materialization_host_output",
    } else "interpreter_fallback_host_output";
}

fn traceMetalHostOutput(graph: *const Graph, node_id: NodeId, reason: []const u8) void {
    const node = graph.node(node_id);
    std.debug.print(
        "metal_partition_host_output_trace: reason={s} node={d} op={s} shape={any}\n",
        .{ reason, node_id, @tagName(node.op), node.output_shape },
    );
}

fn traceRuntimeBinaryFallback(
    graph: *const Graph,
    node_id: NodeId,
    op: RuntimeBinaryOp,
    reason: []const u8,
    lhs: ?RuntimeBinaryOperand,
    rhs: ?RuntimeBinaryOperand,
    output_shape: ml.graph.Shape,
) void {
    if (node_id == null_node or node_id >= graph.nodeCount()) return;
    const node = graph.node(node_id);
    const inputs = node.getInputs();
    const lhs_id = if (inputs.len > 0) inputs[0] else null_node;
    const rhs_id = if (inputs.len > 1) inputs[1] else null_node;
    std.debug.print(
        "metal_runtime_command_fallback_trace: node={d} op={s} reason={s} out_shape={any} lhs_id={d} rhs_id={d}",
        .{ node_id, @tagName(op), reason, output_shape, lhs_id, rhs_id },
    );
    if (lhs) |operand| {
        std.debug.print(" lhs_shape={any}", .{operand.shape});
    } else {
        std.debug.print(" lhs_shape=null", .{});
    }
    if (rhs) |operand| {
        std.debug.print(" rhs_shape={any}", .{operand.shape});
    } else {
        std.debug.print(" rhs_shape=null", .{});
    }
    std.debug.print("\n", .{});
}

const OpRunSummary = struct {
    name: []const u8 = "",
    count: usize = 0,
};

const LongOpRun = struct {
    name: []const u8 = "",
    start_pos: usize = 0,
    end_pos: usize = 0,
    count: usize = 0,
};

const DotShapeRunSummary = struct {
    first_node: NodeId = null_node,
    first_lhs_id: NodeId = null_node,
    first_rhs_id: NodeId = null_node,
    first_rhs_source_id: NodeId = null_node,
    lhs0: i64 = 0,
    lhs1: i64 = 0,
    rhs0: i64 = 0,
    rhs1: i64 = 0,
    out0: i64 = 0,
    out1: i64 = 0,
    rhs_transpose: bool = false,
    rhs_parameter: bool = false,
    rhs_lora: bool = false,
    raw_linear_match: bool = false,
    phase: []const u8 = "",
    family: []const u8 = "",
    lhs_source_op: []const u8 = "",
    rhs_source_op: []const u8 = "",
    count: usize = 0,
};

const DotShapeExecutionSummary = struct {
    first_lhs_id: NodeId = null_node,
    first_rhs_id: NodeId = null_node,
    first_rhs_source_id: NodeId = null_node,
    lhs0: i64 = 0,
    lhs1: i64 = 0,
    rhs0: i64 = 0,
    rhs1: i64 = 0,
    out0: i64 = 0,
    out1: i64 = 0,
    rhs_transpose: bool = false,
    rhs_parameter: bool = false,
    rhs_lora: bool = false,
    phase: []const u8 = "",
    family: []const u8 = "",
    lhs_source_op: []const u8 = "",
    rhs_source_op: []const u8 = "",
    first_node: NodeId = null_node,
    first_pos: usize = 0,
    last_node: NodeId = null_node,
    last_pos: usize = 0,
    count: usize = 0,
    total_ns: u64 = 0,
};

const DotSourceInfo = struct {
    op_name: []const u8 = "",
    source_id: NodeId = null_node,
    is_transpose: bool = false,
    is_parameter: bool = false,
    is_lora: bool = false,
    parameter_name: ?[]const u8 = null,
};

const CommandSourceClassification = struct {
    phase: []const u8 = "activation",
    family: []const u8 = "activation",
    source_op: []const u8 = "none",
};

fn commandSourceClassification(graph: *const Graph, node_id: NodeId, depth: usize) CommandSourceClassification {
    if (node_id == null_node or node_id >= graph.nodeCount()) return .{};
    const node = graph.node(node_id);
    if (std.meta.activeTag(node.op) == .dot_general and node.num_inputs >= 2) {
        const lhs_source = dotSourceInfo(graph, node.inputs[0]);
        const rhs_source = dotSourceInfo(graph, node.inputs[1]);
        return .{
            .phase = classifyDotPhase(lhs_source, rhs_source),
            .family = classifyDotParameterFamily(rhs_source.parameter_name orelse lhs_source.parameter_name),
            .source_op = rhs_source.op_name,
        };
    }
    if (commandParameterName(graph, node_id, depth)) |name| {
        return .{
            .phase = "parameter_ancestry",
            .family = classifyDotParameterFamily(name),
            .source_op = "parameter",
        };
    }
    if (hasTransposedActivationAncestor(graph, node_id, depth)) {
        return .{
            .phase = "activation_transpose",
            .family = "activation",
            .source_op = "transpose(other)",
        };
    }
    return .{
        .phase = "activation",
        .family = "activation",
        .source_op = @tagName(std.meta.activeTag(node.op)),
    };
}

fn recordMetalCommandDispatchFamily(stats: *PartitionExecutor.ExecutionStats, graph: *const Graph, node_id: NodeId) void {
    if (node_id == null_node or node_id >= graph.nodeCount()) {
        stats.metal_command_other_dispatches += 1;
        return;
    }
    const node = graph.node(node_id);
    switch (node.op) {
        .dot_general => {
            stats.metal_command_dot_general_dispatches += 1;
            const classification = commandSourceClassification(graph, node_id, 4);
            if (std.mem.eql(u8, classification.family, "head")) {
                stats.metal_command_head_dot_dispatches += 1;
            }
        },
        .transpose => stats.metal_command_transpose_dispatches += 1,
        .gather => stats.metal_command_gather_dispatches += 1,
        .reduce_sum, .reduce_max, .reduce_mean => stats.metal_command_reduce_dispatches += 1,
        .add, .mul, .sub, .div, .neg, .sqrt, .rsqrt, .exp, .log, .sin, .cos, .tanh, .erf, .abs, .less_than, .where_select, .broadcast_in_dim, .convert_dtype => {
            stats.metal_command_elementwise_dispatches += 1;
        },
        .fused_gelu, .fused_gelu_exact, .fused_relu, .fused_silu, .fused_quick_gelu, .fused_sigmoid, .fused_tanh_act => {
            stats.metal_command_activation_dispatches += 1;
        },
        .fused_gelu_backward, .fused_gelu_exact_backward, .fused_layer_norm_backward, .fused_disentangled_attention_backward, .fused_masked_bce_with_logits_backward => {
            stats.metal_command_activation_backward_dispatches += 1;
        },
        else => stats.metal_command_other_dispatches += 1,
    }
}

fn commandParameterName(graph: *const Graph, node_id: NodeId, depth: usize) ?[]const u8 {
    if (depth == 0 or node_id == null_node or node_id >= graph.nodeCount()) return null;
    const node = graph.node(node_id);
    if (std.meta.activeTag(node.op) == .parameter) return graph.parameterName(node);
    for (node.getInputs()) |input_id| {
        if (commandParameterName(graph, input_id, depth - 1)) |name| return name;
    }
    return null;
}

fn hasTransposedActivationAncestor(graph: *const Graph, node_id: NodeId, depth: usize) bool {
    if (depth == 0 or node_id == null_node or node_id >= graph.nodeCount()) return false;
    const node = graph.node(node_id);
    if (std.meta.activeTag(node.op) == .transpose) {
        if (node.num_inputs == 0 or node.inputs[0] == null_node or node.inputs[0] >= graph.nodeCount()) return true;
        return std.meta.activeTag(graph.node(node.inputs[0]).op) != .parameter;
    }
    for (node.getInputs()) |input_id| {
        if (hasTransposedActivationAncestor(graph, input_id, depth - 1)) return true;
    }
    return false;
}

fn dotSourceInfo(graph: *const Graph, node_id: NodeId) DotSourceInfo {
    if (node_id == null_node or node_id >= graph.nodeCount()) return .{ .op_name = "invalid" };
    const node = graph.node(node_id);
    switch (node.op) {
        .transpose => {
            if (node.num_inputs == 0 or node.inputs[0] == null_node or node.inputs[0] >= graph.nodeCount()) {
                return .{ .op_name = "transpose", .source_id = node_id, .is_transpose = true };
            }
            const source = graph.node(node.inputs[0]);
            if (std.meta.activeTag(source.op) == .parameter) {
                const name = graph.parameterName(source);
                return .{
                    .op_name = "transpose(parameter)",
                    .source_id = node.inputs[0],
                    .is_transpose = true,
                    .is_parameter = true,
                    .is_lora = isLoRAAdapterParameterName(name),
                    .parameter_name = name,
                };
            }
            return .{
                .op_name = if (std.meta.activeTag(source.op) == .dot_general) "transpose(dot_general)" else "transpose(other)",
                .source_id = node.inputs[0],
                .is_transpose = true,
            };
        },
        .parameter => {
            const name = graph.parameterName(node);
            return .{
                .op_name = "parameter",
                .source_id = node_id,
                .is_parameter = true,
                .is_lora = isLoRAAdapterParameterName(name),
                .parameter_name = name,
            };
        },
        else => return .{ .op_name = @tagName(std.meta.activeTag(node.op)), .source_id = node_id },
    }
}

fn classifyDotParameterFamily(name_opt: ?[]const u8) []const u8 {
    const name = name_opt orelse return "activation";
    if (std.mem.indexOf(u8, name, "lora_A") != null) return "lora_A";
    if (std.mem.indexOf(u8, name, "lora_B") != null) return "lora_B";
    if (std.mem.indexOf(u8, name, "query") != null or std.mem.indexOf(u8, name, "q_proj") != null) return "attention_q";
    if (std.mem.indexOf(u8, name, "key") != null or std.mem.indexOf(u8, name, "k_proj") != null) return "attention_k";
    if (std.mem.indexOf(u8, name, "value") != null or std.mem.indexOf(u8, name, "v_proj") != null) return "attention_v";
    if (std.mem.indexOf(u8, name, "attention.output.dense") != null or std.mem.indexOf(u8, name, "out_proj") != null) return "attention_out";
    if (std.mem.indexOf(u8, name, "intermediate.dense") != null or std.mem.indexOf(u8, name, "linear1") != null) return "ffn_up";
    if (std.mem.indexOf(u8, name, "output.dense") != null or std.mem.indexOf(u8, name, "linear2") != null) return "ffn_down";
    if (std.mem.indexOf(u8, name, "LayerNorm") != null or std.mem.indexOf(u8, name, "layer_norm") != null or std.mem.indexOf(u8, name, "norm") != null) return "norm";
    if (std.mem.indexOf(u8, name, "embeddings") != null or std.mem.indexOf(u8, name, "embedding") != null) return "embedding";
    if (std.mem.indexOf(u8, name, "classifier") != null or std.mem.indexOf(u8, name, "span_rep") != null or std.mem.indexOf(u8, name, "count_") != null) return "head";
    return "parameter_other";
}

fn classifyDotPhase(lhs: DotSourceInfo, rhs: DotSourceInfo) []const u8 {
    if (rhs.is_parameter) return "forward_parameter";
    if (lhs.is_parameter) return "backward_input";
    if (lhs.is_transpose and !rhs.is_parameter) return "backward_weight";
    if (rhs.is_transpose and !rhs.is_parameter) return "backward_activation";
    return "activation";
}

fn recordOpRunCount(counts: *[96]OpRunSummary, used: *usize, name: []const u8) void {
    for (counts[0..used.*]) |*entry| {
        if (std.mem.eql(u8, entry.name, name)) {
            entry.count += 1;
            return;
        }
    }
    if (used.* >= counts.len) return;
    counts[used.*] = .{ .name = name, .count = 1 };
    used.* += 1;
}

fn insertLongOpRun(runs: *[12]LongOpRun, run: LongOpRun) void {
    if (run.count == 0) return;
    var insert_at: ?usize = null;
    for (runs, 0..) |entry, idx| {
        if (run.count > entry.count) {
            insert_at = idx;
            break;
        }
    }
    const idx = insert_at orelse return;
    var move_idx = runs.len - 1;
    while (move_idx > idx) : (move_idx -= 1) {
        runs[move_idx] = runs[move_idx - 1];
    }
    runs[idx] = run;
}

fn lessOpRunCount(_: void, a: OpRunSummary, b: OpRunSummary) bool {
    if (a.count == b.count) return std.mem.lessThan(u8, a.name, b.name);
    return a.count > b.count;
}

fn sameDotShapeRunSummary(a: DotShapeRunSummary, b: DotShapeRunSummary) bool {
    return a.lhs0 == b.lhs0 and
        a.lhs1 == b.lhs1 and
        a.rhs0 == b.rhs0 and
        a.rhs1 == b.rhs1 and
        a.out0 == b.out0 and
        a.out1 == b.out1 and
        a.rhs_transpose == b.rhs_transpose and
        a.rhs_parameter == b.rhs_parameter and
        a.rhs_lora == b.rhs_lora and
        a.raw_linear_match == b.raw_linear_match and
        std.mem.eql(u8, a.phase, b.phase) and
        std.mem.eql(u8, a.family, b.family) and
        std.mem.eql(u8, a.lhs_source_op, b.lhs_source_op) and
        std.mem.eql(u8, a.rhs_source_op, b.rhs_source_op);
}

fn recordDotShapeRunSummary(
    graph: *const Graph,
    node_ids: []const NodeId,
    node_pos: usize,
    reachable: []const bool,
    last_use: []const u32,
    summaries: *[96]DotShapeRunSummary,
    used: *usize,
) void {
    const node_id = node_ids[node_pos];
    const node = graph.node(node_id);
    switch (node.op) {
        .dot_general => {},
        else => return,
    }
    if (node.num_inputs < 2) return;
    const lhs_id = node.inputs[0];
    const rhs_id = node.inputs[1];
    if (lhs_id == null_node or rhs_id == null_node) return;
    const lhs_shape = graph.node(lhs_id).output_shape;
    const rhs_shape = graph.node(rhs_id).output_shape;
    const out_shape = node.output_shape;
    if (lhs_shape.rank() != 2 or rhs_shape.rank() != 2 or out_shape.rank() != 2) return;

    const lhs_source = dotSourceInfo(graph, lhs_id);
    const rhs_source = dotSourceInfo(graph, rhs_id);

    var summary = DotShapeRunSummary{
        .first_node = node_id,
        .first_lhs_id = lhs_id,
        .first_rhs_id = rhs_id,
        .first_rhs_source_id = rhs_source.source_id,
        .lhs0 = lhs_shape.dims[0],
        .lhs1 = lhs_shape.dims[1],
        .rhs0 = rhs_shape.dims[0],
        .rhs1 = rhs_shape.dims[1],
        .out0 = out_shape.dims[0],
        .out1 = out_shape.dims[1],
        .rhs_transpose = rhs_source.is_transpose,
        .rhs_parameter = rhs_source.is_parameter,
        .rhs_lora = rhs_source.is_lora,
        .raw_linear_match = matchRawLinearDotPattern(graph, node_ids, node_pos, reachable, last_use) != null,
        .phase = classifyDotPhase(lhs_source, rhs_source),
        .family = classifyDotParameterFamily(rhs_source.parameter_name orelse lhs_source.parameter_name),
        .lhs_source_op = lhs_source.op_name,
        .rhs_source_op = rhs_source.op_name,
    };

    for (summaries[0..used.*]) |*entry| {
        if (sameDotShapeRunSummary(entry.*, summary)) {
            entry.count += 1;
            return;
        }
    }
    if (used.* >= summaries.len) return;
    summary.count = 1;
    summaries[used.*] = summary;
    used.* += 1;
}

fn lessDotShapeRunSummary(_: void, a: DotShapeRunSummary, b: DotShapeRunSummary) bool {
    if (a.count == b.count) {
        if (a.lhs0 != b.lhs0) return a.lhs0 < b.lhs0;
        if (a.lhs1 != b.lhs1) return a.lhs1 < b.lhs1;
        if (a.rhs0 != b.rhs0) return a.rhs0 < b.rhs0;
        return a.rhs1 < b.rhs1;
    }
    return a.count > b.count;
}

fn printMetalPartitionOpRuns(
    graph: *const Graph,
    node_ids: []const NodeId,
    reachable: []const bool,
    last_use: []const u32,
    partition_index: usize,
) void {
    var counts = [_]OpRunSummary{.{}} ** 96;
    var counts_used: usize = 0;
    var longest = [_]LongOpRun{.{}} ** 12;
    var dot_shapes = [_]DotShapeRunSummary{.{}} ** 96;
    var dot_shapes_used: usize = 0;

    var reachable_nodes: usize = 0;
    var current_name: []const u8 = "";
    var current_start: usize = 0;
    var current_count: usize = 0;

    for (node_ids, 0..) |node_id, node_pos| {
        const i: usize = @intCast(node_id);
        if (i >= reachable.len or !reachable[i]) continue;
        const name = @tagName(graph.node(node_id).op);
        reachable_nodes += 1;
        recordOpRunCount(&counts, &counts_used, name);
        recordDotShapeRunSummary(graph, node_ids, node_pos, reachable, last_use, &dot_shapes, &dot_shapes_used);

        if (current_count == 0) {
            current_name = name;
            current_start = node_pos;
            current_count = 1;
            continue;
        }
        if (std.mem.eql(u8, current_name, name)) {
            current_count += 1;
            continue;
        }
        insertLongOpRun(&longest, .{
            .name = current_name,
            .start_pos = current_start,
            .end_pos = node_pos - 1,
            .count = current_count,
        });
        current_name = name;
        current_start = node_pos;
        current_count = 1;
    }
    if (current_count != 0) {
        insertLongOpRun(&longest, .{
            .name = current_name,
            .start_pos = current_start,
            .end_pos = if (node_ids.len == 0) 0 else node_ids.len - 1,
            .count = current_count,
        });
    }

    std.sort.pdq(OpRunSummary, counts[0..counts_used], {}, lessOpRunCount);
    std.sort.pdq(DotShapeRunSummary, dot_shapes[0..dot_shapes_used], {}, lessDotShapeRunSummary);

    std.debug.print("metal_partition_op_runs: partition={d} reachable_nodes={d} distinct_ops={d} top=", .{ partition_index, reachable_nodes, counts_used });
    const top_limit = @min(counts_used, 16);
    if (top_limit == 0) {
        std.debug.print("none", .{});
    } else {
        for (counts[0..top_limit], 0..) |entry, idx| {
            if (idx > 0) std.debug.print(",", .{});
            std.debug.print("{s}:{d}", .{ entry.name, entry.count });
        }
    }
    std.debug.print(" long_runs=", .{});
    var printed_runs: usize = 0;
    for (longest) |run| {
        if (run.count == 0) continue;
        if (printed_runs > 0) std.debug.print(",", .{});
        std.debug.print("{s}:{d}@{d}-{d}", .{ run.name, run.count, run.start_pos, run.end_pos });
        printed_runs += 1;
    }
    if (printed_runs == 0) std.debug.print("none", .{});
    std.debug.print("\n", .{});

    std.debug.print("metal_partition_dot_shapes: partition={d} distinct_shapes={d} top=", .{ partition_index, dot_shapes_used });
    const dot_limit = @min(dot_shapes_used, 16);
    if (dot_limit == 0) {
        std.debug.print("none", .{});
    } else {
        for (dot_shapes[0..dot_limit], 0..) |entry, idx| {
            if (idx > 0) std.debug.print(",", .{});
            std.debug.print(
                "{d}x{d}*{d}x{d}->{d}x{d}:count={d}:node={}:lhs_id={}:rhs_id={}:rhs_source={}:phase={s}:family={s}:lhs={s}:rhs={s}:rhs_transpose={}:rhs_parameter={}:rhs_lora={}:raw_linear={}",
                .{
                    entry.lhs0,
                    entry.lhs1,
                    entry.rhs0,
                    entry.rhs1,
                    entry.out0,
                    entry.out1,
                    entry.count,
                    entry.first_node,
                    entry.first_lhs_id,
                    entry.first_rhs_id,
                    entry.first_rhs_source_id,
                    entry.phase,
                    entry.family,
                    entry.lhs_source_op,
                    entry.rhs_source_op,
                    entry.rhs_transpose,
                    entry.rhs_parameter,
                    entry.rhs_lora,
                    entry.raw_linear_match,
                },
            );
        }
    }
    std.debug.print("\n", .{});
}

fn metalPartitionNowNs() u64 {
    return platform.time.monotonicNs();
}

fn metalPartitionElapsedNs(start_ns: u64, end_ns: u64) u64 {
    if (end_ns <= start_ns) return 0;
    return end_ns - start_ns;
}

fn nsToMs(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / 1_000_000.0;
}

fn traceMetalGraphNodesEnabled() bool {
    return platform.env.getenvBoolDefault("TERMITE_GRAPH_EXECUTOR_TRACE_NODES", false);
}

fn slotBoundOutputsEnabled() bool {
    return platform.env.getenvBoolDefault("TERMITE_METAL_SLOT_BOUND_OUTPUTS", false);
}

fn slotBoundOutputMinBytes() u64 {
    return @intCast(platform.env.getenvUsize("TERMITE_METAL_SLOT_BOUND_MIN_BYTES") orelse (4 * 1024 * 1024));
}

fn slotBoundOutputPoolMaxBytes() u64 {
    return @intCast(platform.env.getenvUsize("TERMITE_METAL_SLOT_BOUND_POOL_MAX_BYTES") orelse (1024 * 1024 * 1024));
}

fn metalEagerArenaEnabled() bool {
    return platform.env.getenvBoolDefault("TERMITE_METAL_EAGER_ARENA", false);
}

fn metalEagerArenaMinBytes() u64 {
    return @intCast(platform.env.getenvUsize("TERMITE_METAL_EAGER_ARENA_MIN_BYTES") orelse (1024 * 1024));
}

fn metalEagerArenaMaxBytes() u64 {
    return @intCast(platform.env.getenvUsize("TERMITE_METAL_EAGER_ARENA_MAX_BYTES") orelse (4 * 1024 * 1024 * 1024));
}

fn metalEagerArenaReclaimAliasesEnabled() bool {
    return platform.env.getenvBoolDefault("TERMITE_METAL_EAGER_ARENA_RECLAIM_ALIASES", false);
}

fn metalChunkLocalOutputsEnabled() bool {
    return platform.env.getenvBoolDefault("TERMITE_METAL_CHUNK_LOCAL_OUTPUTS", false);
}

fn metalChunkLocalOutputsMinBytes() u64 {
    return @intCast(platform.env.getenvUsize("TERMITE_METAL_CHUNK_LOCAL_OUTPUT_MIN_BYTES") orelse (1024 * 1024));
}

fn metalChunkLocalOutputsMaxBytes() u64 {
    return @intCast(platform.env.getenvUsize("TERMITE_METAL_CHUNK_LOCAL_OUTPUT_MAX_BYTES") orelse (4 * 1024 * 1024 * 1024));
}

fn metalFrameChunkExpiredBytesThreshold() u64 {
    if (!platform.env.getenvBoolDefault("TERMITE_METAL_ENABLE_EXPERIMENTAL_LIVENESS_BOUNDARIES", false)) return 0;
    return @intCast(platform.env.getenvUsize("TERMITE_METAL_FRAME_CHUNK_ON_EXPIRED_BYTES") orelse 0);
}

fn chunkLocalOutputsWouldExceedBudget(bytes_live: u64, old_size: u64, new_size: u64) bool {
    const max_bytes = metalChunkLocalOutputsMaxBytes();
    if (max_bytes == 0) return false;
    if (new_size > max_bytes) return true;
    const next_live = (bytes_live -| old_size) +| new_size;
    return next_live > max_bytes;
}

fn currentFrameChunkBoundaryPos(node_count: usize, node_pos: usize, chunk_ops: usize, chunk_executed: usize) ?usize {
    if (chunk_ops == 0 or node_pos >= node_count) return null;
    const remaining_in_chunk = chunk_ops - @min(chunk_ops, chunk_executed);
    if (remaining_in_chunk == 0) return node_pos;
    const boundary = node_pos + remaining_in_chunk - 1;
    return @min(boundary, node_count - 1);
}

fn allocationHasMaterializedValue(
    buffer_plan: *const buffer_plan_mod.BufferPlan,
    values: []const ?CT,
    allocation_id: buffer_plan_mod.AllocationId,
    node_id: NodeId,
) bool {
    const node_index: usize = @intCast(node_id);
    for (values, 0..) |maybe_value, raw_index| {
        if (raw_index == node_index or maybe_value == null) continue;
        const other_node: NodeId = @intCast(raw_index);
        const other_slot = buffer_plan.slotForNode(other_node) orelse continue;
        if (other_slot.allocation == allocation_id) return true;
    }
    return false;
}

fn arenaAliasStillLive(
    graph: *const Graph,
    buffer_plan: *const buffer_plan_mod.BufferPlan,
    node_ids: []const NodeId,
    node_pos: usize,
    alias_node: NodeId,
    current_node: NodeId,
    last_use: []const u32,
    runtime_region_plan: ?RuntimeRegionPlan,
    rt_map: std.AutoHashMapUnmanaged(NodeId, CT),
    donated: std.AutoHashMapUnmanaged(NodeId, void),
    exec_ctx: PartitionExecutor.ExecutionContext,
) bool {
    const alias_index: usize = @intCast(alias_node);
    if (alias_index >= last_use.len) return true;
    if (last_use[alias_index] == std.math.maxInt(u32)) return true;
    const slot = buffer_plan.slotForNode(alias_node) orelse return true;
    if (slot.last_use >= @as(u32, @intCast(node_pos))) return true;
    if (last_use[alias_index] >= @as(u32, @intCast(current_node))) return true;
    if (valueReferencedAtOrAfterPosition(graph, node_ids, node_pos, alias_node)) return true;
    if (slot.roles.graph_output or slot.roles.partition_output or slot.roles.transfer_source) return true;
    if (runtime_region_plan) |plan| {
        if (plan.needsAttentionInputAfterNode(alias_node, current_node)) return true;
    }
    if (rt_map.contains(alias_node) and
        !donated.contains(alias_node) and
        !ownedRuntimeTransferContains(exec_ctx, alias_node))
    {
        return true;
    }
    return false;
}

fn ctStillReferenced(values: []const ?CT, ct: CT) bool {
    for (values) |maybe_ct| {
        if (maybe_ct == ct) return true;
    }
    return false;
}

// Phase-0 coverage counters (process-wide; executor runs single-threaded).
var slot_bound_consumed_total: usize = 0;
var slot_bound_fallback_total: usize = 0;

/// Ops whose device path can write into a caller-provided output buffer
/// through a `*Into` runtime command. The allocation-backed pool only enables
/// these when the buffer plan proves the output allocation is reusable and no
/// current value still materializes that allocation.
fn opSupportsOutputHint(op: anytype) bool {
    return switch (op) {
        .mul, .fused_elem_multiply, .add, .fused_elem_add, .transpose, .dot_general, .reduce_sum, .reduce_max, .reduce_mean, .broadcast_in_dim, .neg, .sqrt, .rsqrt, .exp, .log, .sin, .cos, .tanh, .erf, .abs, .sub, .div, .fused_gelu, .fused_gelu_exact, .fused_relu, .fused_silu, .fused_quick_gelu => true,
        else => false,
    };
}

fn nodeCanConsumeOutputHint(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []const ?CT,
    node_id: NodeId,
) bool {
    const node = graph.node(node_id);
    const inputs = node.getInputs();
    return switch (node.op) {
        .neg,
        .sqrt,
        .rsqrt,
        .exp,
        .log,
        .sin,
        .cos,
        .tanh,
        .erf,
        .abs,
        .fused_gelu,
        .fused_gelu_exact,
        .fused_relu,
        .fused_silu,
        .fused_quick_gelu,
        => inputs.len >= 1 and inputIsMetalResident(values, cb, inputs[0]),
        .transpose => blk: {
            if (inputs.len < 1 or !inputIsMetalResident(values, cb, inputs[0])) break :blk false;
            const attrs = switch (node.op) {
                .transpose => |attrs| attrs,
                else => unreachable,
            };
            break :blk transposeIsSimpleEnoughForHint(attrs, graph.node(inputs[0]).output_shape);
        },
        .mul, .fused_elem_multiply, .sub, .div => blk: {
            if (inputs.len < 2) break :blk false;
            if (!inputIsMetalResident(values, cb, inputs[0]) or !inputIsMetalResident(values, cb, inputs[1])) break :blk false;
            break :blk sameElementCount(graph.node(inputs[0]).output_shape, graph.node(inputs[1]).output_shape);
        },
        .add, .fused_elem_add => blk: {
            if (inputs.len < 2) break :blk false;
            if (!inputIsMetalResident(values, cb, inputs[0]) or !inputIsMetalResident(values, cb, inputs[1])) break :blk false;
            if (!sameElementCount(graph.node(inputs[0]).output_shape, graph.node(inputs[1]).output_shape)) break :blk false;
            // Deferred mul+add fusions return before the addInto path and do not
            // consume a supplied hint, so skip likely fused add nodes here.
            if (isMulLikeOp(graph.node(inputs[0]).op) or isMulLikeOp(graph.node(inputs[1]).op)) break :blk false;
            break :blk true;
        },
        .broadcast_in_dim => |attrs| blk: {
            if (inputs.len < 1 or !inputIsMetalResident(values, cb, inputs[0])) break :blk false;
            const in_shape = graph.node(inputs[0]).output_shape;
            const in_rank = in_shape.rank();
            const target_rank = attrs.target_shape.rank();
            if (in_rank != target_rank or attrs.num_axes != in_rank) break :blk false;
            for (attrs.broadcast_axes[0..attrs.num_axes], 0..) |axis, i| {
                if (axis != i) break :blk false;
            }
            break :blk true;
        },
        .reduce_sum, .reduce_max, .reduce_mean => |attrs| blk: {
            if (inputs.len < 1 or !inputIsMetalResident(values, cb, inputs[0])) break :blk false;
            const rank = graph.node(inputs[0]).output_shape.rank();
            if (rank == 0 or attrs.num_axes == 0) break :blk false;
            const last_axis: u8 = @intCast(rank - 1);
            break :blk attrs.num_axes == 1 and attrs.axes[0] == last_axis;
        },
        // dotGeneral2DInto has backend-only declines for quantized/lazy/view
        // buffers. Until the graph executor can query those cheaply, avoid
        // allocating chunk-local hints that may be ignored.
        .dot_general => false,
        else => false,
    };
}

fn inputIsMetalResident(values: []const ?CT, cb: *const ComputeBackend, node_id: NodeId) bool {
    const input = valueForConst(values, node_id) orelse return false;
    return isMetalDeviceResident(cb, input);
}

fn valueForConst(values: []const ?CT, node_id: NodeId) ?CT {
    if (node_id == null_node) return null;
    const index: usize = @intCast(node_id);
    if (index >= values.len) return null;
    return values[index];
}

fn sameElementCount(a: Shape, b: Shape) bool {
    const a_count = tensorElementCount(a) orelse return false;
    const b_count = tensorElementCount(b) orelse return false;
    return a_count == b_count;
}

fn isMulLikeOp(op: anytype) bool {
    return switch (op) {
        .mul, .fused_elem_multiply => true,
        else => false,
    };
}

fn transposeIsSimpleEnoughForHint(attrs: anytype, input_shape: Shape) bool {
    var perm_buf: [ml.graph.shape.max_rank]u8 = undefined;
    const rank = input_shape.rank();
    if (rank == 0 or rank > ml.graph.shape.max_rank) return false;
    const perm = transpose_utils.effectivePerm(attrs, rank, &perm_buf);
    if (perm.len != rank) return false;
    var seen: [ml.graph.shape.max_rank]bool = [_]bool{false} ** ml.graph.shape.max_rank;
    for (perm) |axis| {
        if (axis >= rank or seen[axis]) return false;
        seen[axis] = true;
    }
    return true;
}

fn traceChunkLocalOutputHintsEnabled() bool {
    return platform.env.getenvBoolDefault("TERMITE_METAL_TRACE_CHUNK_LOCAL_OUTPUT_HINTS", false);
}

fn metalGraphProgressInterval() usize {
    return platform.env.getenvUsize("TERMITE_METAL_PARTITION_PROGRESS_INTERVAL") orelse 0;
}

fn metalGraphProgressStart() usize {
    return platform.env.getenvUsize("TERMITE_METAL_PARTITION_PROGRESS_START") orelse std.math.maxInt(usize);
}

fn metalGraphProgressEnd() usize {
    return platform.env.getenvUsize("TERMITE_METAL_PARTITION_PROGRESS_END") orelse std.math.maxInt(usize);
}

fn metalPartitionFrameDisabled() bool {
    return platform.env.getenvBoolDefault("TERMITE_METAL_PARTITION_DISABLE_FRAME", false);
}

/// Coarse frame chunking: when `TERMITE_METAL_FRAME_CHUNK_OPS=N` (N>0), the
/// partition's single command-buffer frame is split — after every N executed
/// nodes the executor closes the planned scope, submits+waits the frame (which
/// releases that chunk's dead/pooled device buffers), then reopens a new frame
/// and scope. Cross-chunk live values survive via their owned device CTs; only
/// the dead working set frees, bounding peak device memory to the live set
/// instead of the sum of every intermediate. Deferred producer fusion is
/// allowed only when the consumer is provably inside the current chunk, so no
/// deferred/lazy value straddles a submit boundary (regions already execute
/// atomically within one iteration, so a node-boundary == a region boundary).
/// 0 = disabled (single-frame, current behavior).
fn frameChunkOps() usize {
    return platform.env.getenvUsize("TERMITE_METAL_FRAME_CHUNK_OPS") orelse 0;
}

fn deferredProducerConsumerStaysInFrameChunk(
    node_ids: []const NodeId,
    node_pos: usize,
    node_id: NodeId,
    chunk_ops: usize,
    chunk_executed: usize,
    last_use: []const u32,
) bool {
    if (chunk_ops == 0) return true;
    // Experimental byte-triggered boundaries can split the frame before the
    // structural consumer even when it lies before the coarse op boundary.
    if (metalFrameChunkExpiredBytesThreshold() > 0) return false;
    const node_index: usize = @intCast(node_id);
    if (node_index >= last_use.len) return false;
    const consumer_id: NodeId = @intCast(last_use[node_index]);
    if (consumer_id == null_node) return false;
    const boundary_pos = currentFrameChunkBoundaryPos(node_ids.len, node_pos, chunk_ops, chunk_executed) orelse return false;
    if (boundary_pos <= node_pos) return false;
    var consumer_pos = node_pos + 1;
    while (consumer_pos <= boundary_pos and consumer_pos < node_ids.len) : (consumer_pos += 1) {
        if (node_ids[consumer_pos] == consumer_id) return true;
    }
    return false;
}

fn reservePartitionGraphPlanForExecution(chunk_ops: usize) bool {
    if (platform.env.getenvBoolDefault("TERMITE_METAL_DISABLE_PARTITION_GRAPH_PLAN", false)) return false;
    if (chunk_ops == 0) return true;
    return platform.env.getenvBoolDefault("TERMITE_METAL_RESERVE_PARTITION_GRAPH_PLAN_WITH_CHUNKS", false);
}

/// TERMITE_DISABLE_GRAPH_OUTPUT_OWNED_COPY=1 skips the partition-end
/// deep copy of graph outputs into exclusively owned device buffers.
/// Diagnostic kill-switch for bisecting graph-output zero reads.
fn graphOutputOwnedCopyDisabled() bool {
    return platform.env.getenvBoolDefault("TERMITE_DISABLE_GRAPH_OUTPUT_OWNED_COPY", false);
}

/// TERMITE_DISABLE_GRAPH_OUTPUT_ELISION_OVERRIDE=1 disables the
/// graph-output/scalar-tail elision-override protection (no node is
/// forced back into execution after a fused pattern or runtime region
/// elided it). Diagnostic kill-switch for bisecting graph-output zero
/// reads.
fn graphOutputElisionOverrideDisabled() bool {
    return platform.env.getenvBoolDefault("TERMITE_DISABLE_GRAPH_OUTPUT_ELISION_OVERRIDE", false);
}

fn metalPartitionPlannedScopeDisabled() bool {
    return platform.env.getenvBoolDefault("TERMITE_METAL_PARTITION_DISABLE_PLANNED_SCOPE", false);
}

fn partitionViewCacheEnabled() bool {
    if (platform.env.getenvBoolDefault("TERMITE_METAL_DISABLE_PARTITION_VIEW_CACHE", false)) return false;
    if (platform.env.getenvBoolDefault("TERMITE_METAL_ENABLE_PARTITION_VIEW_CACHE", false)) return true;
    return platform.env.getenvBoolDefault("TERMITE_ENABLE_TRAINING_GRAPH_EXECUTOR", false) and
        !platform.env.getenvBoolDefault("TERMITE_DISABLE_TRAINING_GRAPH_EXECUTOR", false);
}

fn tracePartitionViewCacheEnabled() bool {
    return platform.env.getenvBoolDefault("TERMITE_METAL_TRACE_PARTITION_VIEW_CACHE", false);
}

fn metalPartitionFusedPatternsDisabled() bool {
    return platform.env.getenvBoolDefault("TERMITE_METAL_PARTITION_DISABLE_FUSED_PATTERNS", false);
}

fn metalPartitionRuntimeCommandsDisabled() bool {
    return platform.env.getenvBoolDefault("TERMITE_METAL_PARTITION_DISABLE_RUNTIME_COMMANDS", false);
}

fn fusedPatternProbingDisabled(exec_ctx: PartitionExecutor.ExecutionContext) bool {
    if (metalPartitionFusedPatternsDisabled()) return true;
    if (exec_ctx.options) |options| return options.skip_metal_fused_patterns;
    return false;
}

fn traceMetalGraphProgressNode(node_pos: usize, interval: usize, start: usize, end: usize) bool {
    if (start != std.math.maxInt(usize) and node_pos >= start and node_pos <= end) return true;
    return interval != 0 and (node_pos == 0 or node_pos % interval == 0);
}

fn traceMetalValueLifetimeNode() ?NodeId {
    const raw = platform.env.getenvUsize("TERMITE_METAL_TRACE_VALUE_LIFETIME_NODE") orelse return null;
    return @intCast(raw);
}

fn traceMetalValueLifetime(event: []const u8, node_id: NodeId, detail_id: NodeId) void {
    if (traceMetalValueLifetimeNode()) |target| {
        if (node_id == target or detail_id == target) {
            std.debug.print("metal_value_lifetime: event={s} node={d} detail={d}\n", .{ event, node_id, detail_id });
        }
    }
}

fn printMetalProgressNode(
    phase: []const u8,
    graph: *const Graph,
    partition_index: usize,
    node_pos: usize,
    node_count: usize,
    node_id: NodeId,
) void {
    const n = graph.node(node_id);
    const inputs = n.getInputs();
    const in0 = if (inputs.len > 0) inputs[0] else null_node;
    const in1 = if (inputs.len > 1) inputs[1] else null_node;
    const in2 = if (inputs.len > 2) inputs[2] else null_node;
    std.debug.print(
        "metal_partition_progress: phase={s} partition={d} pos={d}/{d} node={} op={s} out_shape={any} in0={} in0_op={s} in0_shape={any} in1={} in1_op={s} in1_shape={any} in2={} in2_op={s} in2_shape={any}\n",
        .{
            phase,
            partition_index,
            node_pos,
            node_count,
            node_id,
            @tagName(n.op),
            n.output_shape,
            in0,
            if (in0 != null_node) @tagName(graph.node(in0).op) else "none",
            if (in0 != null_node) graph.node(in0).output_shape else Shape.scalar(.f32),
            in1,
            if (in1 != null_node) @tagName(graph.node(in1).op) else "none",
            if (in1 != null_node) graph.node(in1).output_shape else Shape.scalar(.f32),
            in2,
            if (in2 != null_node) @tagName(graph.node(in2).op) else "none",
            if (in2 != null_node) graph.node(in2).output_shape else Shape.scalar(.f32),
        },
    );
}

fn traceMetalGraphFusionsEnabled() bool {
    return platform.env.getenvBoolDefault("TERMITE_METAL_TRACE_GRAPH_FUSIONS", false);
}

fn traceRuntimeRegionsEnabled() bool {
    return platform.env.getenvBoolDefault("TERMITE_METAL_TRACE_RUNTIME_REGIONS", false);
}

fn traceLoraQkvMatchingEnabled() bool {
    return platform.env.getenvBoolDefault("TERMITE_METAL_TRACE_LORA_QKV_MATCH", false);
}

fn traceDebertaAttentionMatchingEnabled() bool {
    return platform.env.getenvBoolDefault("TERMITE_METAL_TRACE_DEBERTA_ATTENTION_MATCH", false);
}

fn traceDebertaFfnForwardMatchingEnabled() bool {
    return platform.env.getenvBoolDefault("TERMITE_METAL_TRACE_DEBERTA_FFN_FORWARD_MATCH", false);
}

fn traceFfnGeluBackwardMatchingEnabled() bool {
    return platform.env.getenvBoolDefault("TERMITE_METAL_TRACE_FFN_GELU_BACKWARD_MATCH", false) or traceMetalGraphFusionsEnabled();
}

fn traceRankAdapterBackwardMatchingEnabled() bool {
    return platform.env.getenvBoolDefault("TERMITE_METAL_TRACE_RANK_ADAPTER_BACKWARD_MATCH", false) or traceMetalGraphFusionsEnabled();
}

fn traceDebertaEncoderLoraLayerMatchingEnabled() bool {
    return platform.env.getenvBoolDefault("TERMITE_METAL_TRACE_DEBERTA_ENCODER_LORA_LAYER_MATCH", false) or
        traceDebertaFfnForwardMatchingEnabled();
}

fn debertaAttentionRuntimeRegionEnabled() bool {
    return platform.env.getenvBoolDefault("TERMITE_ENABLE_DEBERTA_ATTENTION_RUNTIME_REGION", false);
}

fn debertaFfnForwardRuntimeRegionEnabled() bool {
    if (platform.env.getenvBoolDefault("TERMITE_METAL_DISABLE_DEBERTA_FFN_FORWARD_RUNTIME_REGION", false)) return false;
    return platform.env.getenvBoolDefault("TERMITE_METAL_ENABLE_DEBERTA_FFN_FORWARD_RUNTIME_REGION", false);
}

fn headMlpForwardRuntimeRegionEnabled() bool {
    if (platform.env.getenvBoolDefault("TERMITE_METAL_DISABLE_HEAD_MLP_FORWARD_RUNTIME_REGION", false)) return false;
    // ponytail: schema parity caught this fusion; keep it opt-in until the head-MLP contract is fixed.
    return platform.env.getenvBoolDefault("TERMITE_METAL_ENABLE_HEAD_MLP_FORWARD_RUNTIME_REGION", false);
}

fn groupedHeadDotRuntimeCommandEnabled() bool {
    if (platform.env.getenvBoolDefault("TERMITE_METAL_DISABLE_GROUPED_HEAD_DOT", false)) return false;
    return platform.env.getenvBoolDefault("TERMITE_ENABLE_TRAINING_GRAPH_EXECUTOR", false) and
        !platform.env.getenvBoolDefault("TERMITE_DISABLE_TRAINING_GRAPH_EXECUTOR", false);
}

/// When set, promote a gather's input operand to device residency before the
/// gather command. Frozen embedding-table gathers otherwise run the host
/// fallback (no_input_metal) and force a host-output drain. Default OFF; this
/// uploads the (possibly large) input per step, so it is an experiment knob
/// pending a persistent frozen-parameter device cache.
fn gatherPromoteInputEnabled() bool {
    return platform.env.getenvBoolDefault("TERMITE_METAL_GATHER_PROMOTE_INPUT", false);
}

fn reducePromoteInputEnabled() bool {
    return platform.env.getenvBoolDefault("TERMITE_METAL_REDUCE_PROMOTE_INPUT", false);
}

fn lowSyncResidentInputSource(graph: *const Graph, node_id: NodeId) ?NodeId {
    if (node_id == null_node or node_id >= graph.nodeCount()) return null;
    const node = graph.node(node_id);
    const inputs = node.getInputs();
    if (inputs.len == 0 or inputs[0] == null_node) return null;
    return switch (node.op) {
        .gather => if (gatherPromoteInputEnabled()) inputs[0] else null,
        .reduce_sum, .reduce_max, .reduce_mean => if (reducePromoteInputEnabled()) inputs[0] else null,
        else => null,
    };
}

fn runtimeRegionPlanDisabled() bool {
    return platform.env.getenvBoolDefault("TERMITE_METAL_DISABLE_RUNTIME_REGION_PLAN", false);
}

fn loraBackwardRuntimeRegionEnabled() bool {
    if (platform.env.getenvBoolDefault("TERMITE_METAL_DISABLE_LORA_BACKWARD_RUNTIME_REGION", false)) return false;
    return platform.env.getenvBoolDefault("TERMITE_METAL_ENABLE_LORA_BACKWARD_RUNTIME_REGION", true);
}

fn lowRankLoraBackwardRuntimeRegionEnabled() bool {
    if (platform.env.getenvBoolDefault("TERMITE_METAL_DISABLE_LOW_RANK_LORA_BACKWARD_RUNTIME_REGION", false)) return false;
    return platform.env.getenvBoolDefault("TERMITE_METAL_ENABLE_LOW_RANK_LORA_BACKWARD_RUNTIME_REGION", false);
}

fn rankAdapterBackwardRuntimeRegionEnabled() bool {
    if (platform.env.getenvBoolDefault("TERMITE_METAL_DISABLE_RANK_ADAPTER_BACKWARD_RUNTIME_REGION", false)) return false;
    if (platform.env.getenvBoolDefault("TERMITE_METAL_ENABLE_RANK_ADAPTER_BACKWARD_RUNTIME_REGION", false)) return true;
    return platform.env.getenvBoolDefault("TERMITE_ENABLE_TRAINING_GRAPH_EXECUTOR", false) and
        !platform.env.getenvBoolDefault("TERMITE_DISABLE_TRAINING_GRAPH_EXECUTOR", false);
}

fn ffnGeluBackwardRuntimeRegionEnabled() bool {
    if (platform.env.getenvBoolDefault("TERMITE_METAL_DISABLE_FFN_GELU_BACKWARD_RUNTIME_REGION", false)) return false;
    if (platform.env.getenvBoolDefault("TERMITE_METAL_ENABLE_FFN_GELU_BACKWARD_RUNTIME_REGION", false)) return true;
    return platform.env.getenvBoolDefault("TERMITE_ENABLE_TRAINING_GRAPH_EXECUTOR", false) and
        !platform.env.getenvBoolDefault("TERMITE_DISABLE_TRAINING_GRAPH_EXECUTOR", false);
}

fn rank1DotSpecializationEnabled() bool {
    if (platform.env.getenvBoolDefault("TERMITE_METAL_DISABLE_RANK1_DOT_SPECIALIZATION", false)) return false;
    if (platform.env.getenvBoolDefault("TERMITE_METAL_ENABLE_RANK1_DOT_SPECIALIZATION", false)) return true;
    return platform.env.getenvBoolDefault("TERMITE_ENABLE_TRAINING_GRAPH_EXECUTOR", false) and
        !platform.env.getenvBoolDefault("TERMITE_DISABLE_TRAINING_GRAPH_EXECUTOR", false);
}

fn rawLinearBiasPairRuntimeRegionEnabled() bool {
    if (platform.env.getenvBoolDefault("TERMITE_METAL_DISABLE_RAW_LINEAR_BIAS_PAIR_RUNTIME_REGION", false)) return false;
    return platform.env.getenvBoolDefault("TERMITE_METAL_ENABLE_RAW_LINEAR_BIAS_PAIR_RUNTIME_REGION", true);
}

/// Training-graph-executor runs require bit-parity with the direct
/// interpreter. The raw_linear_* runtime regions reroute raw
/// `dot_general(x, transpose(W))` nodes through prepared dynamic linear
/// slots (`decoderRuntimeApplyLinear`), a code path the interpreter never
/// takes for these nodes and which diverged numerically on the GLiNER2 LoRA
/// training graph (first at the [127,768] x [768,768]ᵀ relative-position
/// projection). When the training graph executor is active, decline those
/// region matches so the dots execute through the interpreter-equivalent
/// dot_general command instead. `TERMITE_METAL_ENABLE_RAW_LINEAR_RUNTIME_REGIONS_IN_TRAINING=1`
/// restores the previous behavior.
fn rawLinearRuntimeRegionsSuppressedForTraining() bool {
    if (platform.env.getenvBoolDefault("TERMITE_METAL_ENABLE_RAW_LINEAR_RUNTIME_REGIONS_IN_TRAINING", false)) return false;
    return platform.env.getenvBoolDefault("TERMITE_ENABLE_TRAINING_GRAPH_EXECUTOR", false) and
        !platform.env.getenvBoolDefault("TERMITE_DISABLE_TRAINING_GRAPH_EXECUTOR", false);
}

fn gatedFfnGraphFusionDisabled() bool {
    return platform.env.getenvBoolDefault("TERMITE_METAL_DISABLE_GATED_FFN_GRAPH_FUSION", false);
}

fn gatedFfnGraphFusionEnabled() bool {
    if (gatedFfnGraphFusionDisabled()) return false;
    return platform.env.getenvBoolDefault("TERMITE_METAL_ENABLE_GATED_FFN_GRAPH_FUSION", true);
}

fn attentionOutputResidualGraphFusionDisabled() bool {
    return platform.env.getenvBoolDefault("TERMITE_METAL_DISABLE_ATTENTION_OUTPUT_RESIDUAL_GRAPH_FUSION", false);
}

fn attentionOutputResidualGraphFusionEnabled() bool {
    if (attentionOutputResidualGraphFusionDisabled()) return false;
    return platform.env.getenvBoolDefault("TERMITE_METAL_ENABLE_ATTENTION_OUTPUT_RESIDUAL_GRAPH_FUSION", true);
}

fn debertaEncoderLoraLayerRuntimeRegionEnabled() bool {
    return platform.env.getenvBoolDefault("TERMITE_METAL_ENABLE_DEBERTA_ENCODER_LAYER_LORA_REGION", false);
}

fn buildRuntimeRegionPlan(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    node_ids: []const NodeId,
    value_count: usize,
    reachable: []const bool,
    last_use: []const u32,
) !RuntimeRegionPlan {
    const regions = try allocator.alloc(RuntimeRegion, node_ids.len);
    errdefer allocator.free(regions);
    @memset(regions, .{ .none = {} });
    const prepared = try allocator.alloc(PreparedRuntimeRegion, node_ids.len);
    errdefer allocator.free(prepared);
    @memset(prepared, .{ .none = {} });
    const attention_input_max_first_node = try allocator.alloc(NodeId, value_count);
    errdefer allocator.free(attention_input_max_first_node);
    @memset(attention_input_max_first_node, null_node);
    const pre_skipped_by_region = try allocator.alloc(bool, value_count);
    errdefer allocator.free(pre_skipped_by_region);
    @memset(pre_skipped_by_region, false);
    const null_values = try allocator.alloc(?CT, value_count);
    defer allocator.free(null_values);
    @memset(null_values, null);

    const skipped = try allocator.alloc(bool, value_count);
    defer allocator.free(skipped);
    @memset(skipped, false);

    var region_count: usize = 0;
    var pre_skip_stats = RuntimeRegionPreSkipStats{};
    const raw_linear_regions_suppressed = rawLinearRuntimeRegionsSuppressedForTraining();
    // Hoist the env-flag region gates out of the per-node loop below — each is an
    // uncached getenv (an environ scan, and several read 2-4 vars), so reading them
    // once per plan build instead of once per node avoids O(nodes * gates) scans.
    const deberta_encoder_lora_enabled = debertaEncoderLoraLayerRuntimeRegionEnabled();
    const deberta_ffn_forward_enabled = debertaFfnForwardRuntimeRegionEnabled();
    const head_mlp_forward_enabled = headMlpForwardRuntimeRegionEnabled();
    const raw_linear_bias_pair_enabled = rawLinearBiasPairRuntimeRegionEnabled();
    const deberta_attention_enabled = debertaAttentionRuntimeRegionEnabled();
    const rank_adapter_backward_enabled = rankAdapterBackwardRuntimeRegionEnabled();
    const low_rank_lora_backward_enabled = lowRankLoraBackwardRuntimeRegionEnabled();
    const lora_backward_enabled = loraBackwardRuntimeRegionEnabled();
    const ffn_gelu_backward_enabled = ffnGeluBackwardRuntimeRegionEnabled();
    for (node_ids, 0..) |node_id, node_pos| {
        const i: usize = @intCast(node_id);
        if (i >= reachable.len or !reachable[i]) continue;
        if (i < skipped.len and skipped[i]) continue;

        if (deberta_encoder_lora_enabled) {
            if (matchDebertaEncoderLoraLayerPattern(graph, node_ids, node_pos, reachable, skipped, regions)) |pattern| {
                regions[node_pos] = .{ .deberta_encoder_lora_layer = pattern };
                markDebertaEncoderLoraLayerSkipped(skipped, pattern);
                region_count += 1;
                continue;
            }
        }
        if (deberta_ffn_forward_enabled) {
            if (matchDebertaFfnForwardPattern(graph, node_ids, node_pos, reachable, skipped)) |pattern| {
                regions[node_pos] = .{ .deberta_ffn_forward = pattern };
                markDebertaFfnForwardSkipped(skipped, pattern);
                region_count += 1;
                continue;
            }
        }
        if (head_mlp_forward_enabled) {
            if (matchHeadMlpForwardPattern(graph, node_ids, node_pos, reachable, skipped)) |pattern| {
                regions[node_pos] = .{ .deberta_ffn_forward = pattern };
                markDebertaFfnForwardSkipped(skipped, pattern);
                region_count += 1;
                continue;
            }
        }
        if (!raw_linear_regions_suppressed and raw_linear_bias_pair_enabled) {
            if (matchRawLinearBiasPairPattern(graph, node_ids, node_pos, reachable, last_use, skipped)) |pattern| {
                regions[node_pos] = .{ .raw_linear_bias_pair = pattern };
                markRawLinearBiasPairSkipped(skipped, pattern);
                region_count += 1;
                continue;
            }
        }
        if (!raw_linear_regions_suppressed) {
            if (matchRawLinearBiasPattern(graph, node_ids, node_pos, reachable, last_use, skipped)) |pattern| {
                regions[node_pos] = .{ .raw_linear_bias = pattern };
                markRawLinearBiasSkipped(skipped, pattern);
                region_count += 1;
                continue;
            }
            if (matchRawLinearPairPattern(graph, node_ids, node_pos, reachable, last_use, skipped)) |pattern| {
                regions[node_pos] = .{ .raw_linear_pair = pattern };
                markRawLinearPairSkipped(skipped, pattern);
                region_count += 1;
                continue;
            }
            if (matchRawLinearDotPattern(graph, node_ids, node_pos, reachable, last_use)) |pattern| {
                regions[node_pos] = .{ .raw_linear_dot = pattern };
                region_count += 1;
                continue;
            }
        }
        if (matchLoraLinearQkvPattern(graph, node_ids, node_pos, reachable, skipped)) |pattern| {
            regions[node_pos] = .{ .lora_linear_qkv = pattern };
            markLoraLinearQkvSkipped(skipped, pattern);
            region_count += 1;
            continue;
        }
        if (matchLoraLinearPattern(graph, node_ids, node_pos, reachable, skipped)) |pattern| {
            regions[node_pos] = .{ .lora_linear = pattern };
            markLoraLinearSkipped(skipped, pattern);
            region_count += 1;
            continue;
        }
        if (deberta_attention_enabled) {
            if (matchDebertaAttentionPattern(graph, node_ids, node_pos, reachable, skipped, regions)) |pattern| {
                regions[node_pos] = .{ .deberta_attention = pattern };
                markDebertaAttentionSkipped(graph, skipped, pattern);
                recordAttentionInputMaxFirstNode(attention_input_max_first_node, pattern);
                region_count += 1;
                continue;
            }
        }
        if (rank_adapter_backward_enabled) {
            if (matchRankAdapterBackwardPattern(graph, node_ids, node_pos, reachable, skipped)) |pattern| {
                regions[node_pos] = .{ .rank_adapter_backward = pattern };
                pre_skip_stats.add(markRankAdapterBackwardPreSkipped(graph, reachable, skipped, pre_skipped_by_region, pattern));
                markRankAdapterBackwardSkipped(skipped, pattern);
                region_count += 1;
                continue;
            }
        }
        if (low_rank_lora_backward_enabled) {
            if (matchLowRankLoraBackwardPattern(graph, node_ids, node_pos, reachable, skipped)) |pattern| {
                regions[node_pos] = .{ .low_rank_lora_backward = pattern };
                pre_skip_stats.add(markLowRankLoraBackwardPreSkipped(graph, reachable, skipped, pre_skipped_by_region, pattern));
                markLowRankLoraBackwardSkipped(skipped, pattern);
                region_count += 1;
                continue;
            }
        }
        if (lora_backward_enabled) {
            if (matchLoraBackwardPattern(graph, node_ids, node_pos, reachable, skipped)) |pattern| {
                regions[node_pos] = .{ .lora_backward = pattern };
                pre_skip_stats.add(markLoraBackwardPreSkipped(graph, reachable, skipped, pre_skipped_by_region, pattern));
                markLoraBackwardSkipped(skipped, pattern);
                region_count += 1;
                continue;
            }
        }
        if (ffn_gelu_backward_enabled) {
            if (matchFfnGeluBackwardPattern(graph, node_ids, node_pos, reachable, skipped)) |pattern| {
                regions[node_pos] = .{ .ffn_gelu_backward = pattern };
                markFfnGeluBackwardSkipped(skipped, pattern);
                region_count += 1;
                continue;
            }
        }

        if (matchRmsNormGroupedLinearQkvSlicePattern(graph, null_values, node_ids, node_pos, reachable, skipped)) |pattern| {
            regions[node_pos] = .{ .rms_norm_grouped_linear_qkv_slice = pattern };
            markRmsNormGroupedLinearQkvSkipped(skipped, pattern);
            region_count += 1;
            continue;
        }
        if (matchGroupedLinearQkvSlicePattern(graph, null_values, node_ids, node_pos, reachable, skipped)) |pattern| {
            regions[node_pos] = .{ .grouped_linear_qkv_slice = pattern };
            markGroupedLinearQkvSkipped(skipped, pattern);
            region_count += 1;
            continue;
        }
        if (matchPackedLinearQkvSlicePattern(graph, null_values, node_ids, node_pos, reachable, skipped)) |pattern| {
            regions[node_pos] = .{ .packed_linear_qkv_slice = pattern };
            markPackedLinearQkvSkipped(skipped, pattern);
            region_count += 1;
            continue;
        }
        if (matchLinearNoBiasQkvPattern(graph, null_values, node_ids, node_pos, reachable, skipped)) |pattern| {
            regions[node_pos] = .{ .linear_qkv = pattern };
            markLinearNoBiasQkvSkipped(skipped, pattern);
            region_count += 1;
            continue;
        }
        if (matchQLinearPattern(graph, null_values, node_ids, node_pos, reachable, skipped)) |pattern| {
            regions[node_pos] = .{ .q_linear = pattern };
            region_count += 1;
            continue;
        }

        if (attentionOutputResidualGraphFusionEnabled()) {
            if (matchAttentionOutputResidualPattern(graph, node_ids, node_pos, reachable, skipped, last_use)) |pattern| {
                regions[node_pos] = .{ .attention_output_residual = pattern };
                markAttentionOutputResidualSkipped(skipped, pattern);
                region_count += 1;
                continue;
            }
        }

        if (gatedFfnGraphFusionEnabled()) {
            if (matchRmsNormGatedFfnResidualPattern(graph, node_ids, node_pos, reachable, skipped, last_use)) |pattern| {
                regions[node_pos] = .{ .rms_norm_gated_ffn_residual = pattern };
                markRmsNormGatedFfnResidualSkipped(skipped, pattern);
                region_count += 1;
                continue;
            }
            if (matchGatedFfnResidualPattern(graph, node_ids, node_pos, reachable, skipped, last_use)) |pattern| {
                regions[node_pos] = .{ .gated_ffn_residual = pattern };
                markGatedFfnResidualSkipped(skipped, pattern);
                region_count += 1;
                continue;
            }
        }

        if (matchPleResidualPattern(graph, node_ids, node_pos, reachable, skipped)) |pattern| {
            regions[node_pos] = .{ .ple_residual = pattern };
            markPleResidualSkipped(skipped, pattern);
            region_count += 1;
            continue;
        }
    }

    return .{
        .node_count = node_ids.len,
        .value_count = value_count,
        .first_node = if (node_ids.len == 0) null_node else node_ids[0],
        .last_node = if (node_ids.len == 0) null_node else node_ids[node_ids.len - 1],
        .node_ids_hash = hashNodeIds(node_ids),
        .regions_by_pos = regions,
        .prepared_by_pos = prepared,
        .attention_input_max_first_node = attention_input_max_first_node,
        .pre_skipped_by_region = pre_skipped_by_region,
        .region_count = region_count,
        .covered_node_count = runtimeRegionCoveredNodeCount(node_ids, regions, skipped),
        .elided_node_count = runtimeRegionElidedNodeCount(node_ids, regions, skipped),
        .pre_skipped_node_count = pre_skip_stats.nodes,
        .pre_skipped_transpose_count = pre_skip_stats.transposes,
        .pre_skip_declined_external_consumer_count = pre_skip_stats.declined_external_consumer,
    };
}

fn recordAttentionInputMaxFirstNode(max_first_by_input: []NodeId, pattern: DebertaAttentionPattern) void {
    recordAttentionInputFirstNode(max_first_by_input, pattern.q_id, pattern.first_node_id);
    recordAttentionInputFirstNode(max_first_by_input, pattern.k_id, pattern.first_node_id);
    recordAttentionInputFirstNode(max_first_by_input, pattern.v_id, pattern.first_node_id);
    recordAttentionInputFirstNode(max_first_by_input, pattern.q_r_id, pattern.first_node_id);
    recordAttentionInputFirstNode(max_first_by_input, pattern.k_r_id, pattern.first_node_id);
    recordAttentionInputFirstNode(max_first_by_input, pattern.attn_bias_id, pattern.first_node_id);
}

fn recordAttentionInputFirstNode(max_first_by_input: []NodeId, input_id: NodeId, first_node_id: NodeId) void {
    if (input_id == null_node) return;
    const input_index: usize = @intCast(input_id);
    if (input_index >= max_first_by_input.len) return;
    if (max_first_by_input[input_index] == null_node or first_node_id > max_first_by_input[input_index]) {
        max_first_by_input[input_index] = first_node_id;
    }
}

fn runtimeRegionCoveredNodeCount(node_ids: []const NodeId, regions: []const RuntimeRegion, skipped: []const bool) usize {
    var count: usize = 0;
    for (node_ids, 0..) |node_id, pos| {
        if (pos < regions.len and std.meta.activeTag(regions[pos]) != .none) {
            count += 1;
            continue;
        }
        const i: usize = @intCast(node_id);
        if (i < skipped.len and skipped[i]) count += 1;
    }
    return count;
}

fn runtimeRegionElidedNodeCount(node_ids: []const NodeId, regions: []const RuntimeRegion, skipped: []const bool) usize {
    var count: usize = 0;
    for (node_ids, 0..) |node_id, pos| {
        if (pos < regions.len and std.meta.activeTag(regions[pos]) != .none) continue;
        const i: usize = @intCast(node_id);
        if (i < skipped.len and skipped[i]) count += 1;
    }
    return count;
}

fn applyRuntimeRegionPreSkippedNodes(
    graph: *const Graph,
    plan: RuntimeRegionPlan,
    skipped_nodes: []bool,
    elision_protected_nodes: []const bool,
    rt_map: std.AutoHashMapUnmanaged(NodeId, CT),
    stats: ?*PartitionExecutor.ExecutionStats,
) void {
    var applied_nodes: u64 = 0;
    var applied_transposes: u64 = 0;
    for (plan.pre_skipped_by_region, 0..) |owned, raw_id| {
        if (!owned) continue;
        if (raw_id >= skipped_nodes.len or raw_id >= graph.nodeCount()) continue;
        if (raw_id < elision_protected_nodes.len and elision_protected_nodes[raw_id]) continue;
        const node_id: NodeId = @intCast(raw_id);
        if (rt_map.contains(node_id)) continue;
        const node = graph.node(node_id);
        if (node.op == .parameter or isPreMaterializedConstantOp(node.op)) continue;
        skipped_nodes[raw_id] = true;
        applied_nodes += 1;
        if (node.op == .transpose) applied_transposes += 1;
        if (traceRuntimeRegionsEnabled()) {
            std.debug.print(
                "runtime_region_pre_skip: node={} op={s} reason=region_owned\n",
                .{ node_id, @tagName(node.op) },
            );
        }
    }
    if (stats) |s| {
        s.runtime_region_pre_skipped_nodes += applied_nodes;
        s.runtime_region_pre_skipped_transposes += applied_transposes;
    }
}

const RuntimeRegionSummary = struct {
    kind: RuntimeRegionKind = .none,
    count: usize = 0,
    first_pos: usize = 0,
    last_pos: usize = 0,
};

fn recordRuntimeRegionSummary(summaries: *[32]RuntimeRegionSummary, used: *usize, kind: RuntimeRegionKind, pos: usize) void {
    if (kind == .none) return;
    for (summaries[0..used.*]) |*summary| {
        if (summary.kind != kind) continue;
        summary.count += 1;
        summary.last_pos = pos;
        return;
    }
    if (used.* >= summaries.len) return;
    summaries[used.*] = .{
        .kind = kind,
        .count = 1,
        .first_pos = pos,
        .last_pos = pos,
    };
    used.* += 1;
}

fn printRuntimeRegionPlanSummary(graph: *const Graph, plan: RuntimeRegionPlan, partition_index: usize) void {
    var summaries = [_]RuntimeRegionSummary{.{}} ** 32;
    var used: usize = 0;
    for (plan.regions_by_pos, 0..) |region, pos| {
        recordRuntimeRegionSummary(&summaries, &used, std.meta.activeTag(region), pos);
    }
    const eligibility = analyzeRuntimeFrameEligibility(graph, plan);
    const metadata_ready = runtimeFrameMetadataFromPlan(graph, plan) != null;
    std.debug.print(
        "runtime_region_plan_summary: partition={d} regions={d} frame_reason={s} frame_layers={d} metadata_ready={} kinds=",
        .{ partition_index, plan.region_count, @tagName(eligibility.reason), eligibility.layers, metadata_ready },
    );
    if (used == 0) {
        std.debug.print("none", .{});
    } else {
        for (summaries[0..used], 0..) |summary, idx| {
            if (idx > 0) std.debug.print(",", .{});
            std.debug.print("{s}:{d}@{d}-{d}", .{
                @tagName(summary.kind),
                summary.count,
                summary.first_pos,
                summary.last_pos,
            });
        }
    }
    std.debug.print("\n", .{});
}

fn runtimeFrameLayerShapeFromQkv(region: RuntimeRegion) ?RuntimeFrameLayerShape {
    return switch (region) {
        .q_linear => |pattern| .{ .rows = pattern.rows, .hidden_size = pattern.in_dim, .attention_input_size = pattern.out_dim },
        .linear_qkv => |pattern| .{ .rows = pattern.rows, .hidden_size = pattern.in_dim, .attention_input_size = pattern.q_out_dim },
        .grouped_linear_qkv_slice => |pattern| .{ .rows = pattern.rows, .hidden_size = pattern.in_dim, .attention_input_size = pattern.q_out_dim },
        .rms_norm_grouped_linear_qkv_slice => |pattern| .{ .rows = pattern.qkv.rows, .hidden_size = pattern.qkv.in_dim, .attention_input_size = pattern.qkv.q_out_dim },
        .lora_linear_qkv => |pattern| .{ .rows = pattern.rows, .hidden_size = pattern.in_dim, .attention_input_size = pattern.q_out_dim },
        else => null,
    };
}

fn runtimeFrameLayerShapeFromAttention(pattern: AttentionOutputResidualPattern) RuntimeFrameLayerShape {
    return .{ .rows = pattern.rows, .hidden_size = pattern.hidden_size, .attention_input_size = pattern.attention_input_size };
}

fn runtimeFrameLayerShapeFromFfn(region: RuntimeRegion) ?RuntimeFrameLayerShape {
    return switch (region) {
        .rms_norm_gated_ffn_residual => |pattern| .{ .rows = pattern.ffn.rows, .hidden_size = pattern.ffn.hidden_size },
        .gated_ffn_residual => |pattern| .{ .rows = pattern.rows, .hidden_size = pattern.hidden_size },
        else => null,
    };
}

fn runtimeFrameLayerShapeFromPle(pattern: PleResidualPattern) RuntimeFrameLayerShape {
    return .{ .rows = pattern.rows, .hidden_size = pattern.hidden_size };
}

fn runtimeFrameShapesMatch(lhs: RuntimeFrameLayerShape, rhs: RuntimeFrameLayerShape) bool {
    if (lhs.rows != rhs.rows or lhs.hidden_size != rhs.hidden_size) return false;
    if (lhs.attention_input_size != 0 and rhs.attention_input_size != 0 and lhs.attention_input_size != rhs.attention_input_size) return false;
    return true;
}

fn runtimeFrameQkvMetadataFromRegion(graph: *const Graph, region: RuntimeRegion) ?RuntimeFrameQkvMetadata {
    return switch (region) {
        .q_linear => |pattern| .{
            .layer_index = layerIndexForWeight(graph, pattern.weight_id) orelse return null,
            .rows = pattern.rows,
            .hidden_size = pattern.in_dim,
            .q_dim = pattern.out_dim,
            .kv_dim = 0,
            .q_weight_id = pattern.weight_id,
        },
        .linear_qkv => |pattern| .{
            .layer_index = layerIndexForWeight(graph, pattern.q_weight_id) orelse return null,
            .rows = pattern.rows,
            .hidden_size = pattern.in_dim,
            .q_dim = pattern.q_out_dim,
            .kv_dim = pattern.kv_out_dim,
            .q_weight_id = pattern.q_weight_id,
            .k_weight_id = pattern.k_weight_id,
            .v_weight_id = pattern.v_weight_id,
        },
        .grouped_linear_qkv_slice => |pattern| .{
            .layer_index = layerIndexForWeight(graph, pattern.q_weight_id) orelse return null,
            .rows = pattern.rows,
            .hidden_size = pattern.in_dim,
            .q_dim = pattern.q_out_dim,
            .kv_dim = pattern.kv_out_dim,
            .q_weight_id = pattern.q_weight_id,
            .k_weight_id = pattern.k_weight_id,
            .v_weight_id = pattern.v_weight_id,
        },
        .rms_norm_grouped_linear_qkv_slice => |pattern| .{
            .layer_index = layerIndexForWeight(graph, pattern.qkv.q_weight_id) orelse return null,
            .rows = pattern.qkv.rows,
            .hidden_size = pattern.qkv.in_dim,
            .q_dim = pattern.qkv.q_out_dim,
            .kv_dim = pattern.qkv.kv_out_dim,
            .q_weight_id = pattern.qkv.q_weight_id,
            .k_weight_id = pattern.qkv.k_weight_id,
            .v_weight_id = pattern.qkv.v_weight_id,
        },
        .lora_linear_qkv => |pattern| .{
            .layer_index = layerIndexForWeight(graph, pattern.q_base_weight_id) orelse return null,
            .rows = pattern.rows,
            .hidden_size = pattern.in_dim,
            .q_dim = pattern.q_out_dim,
            .kv_dim = pattern.kv_out_dim,
            .q_weight_id = pattern.q_base_weight_id,
            .k_weight_id = pattern.k_base_weight_id,
            .v_weight_id = pattern.v_base_weight_id,
        },
        else => null,
    };
}

fn runtimeFrameLayerMetadata(
    graph: *const Graph,
    qkv: RuntimeFrameQkvMetadata,
    attention: AttentionOutputResidualPattern,
    ffn_region: RuntimeRegion,
    ple: PleResidualPattern,
) ?RuntimeFrameLayerMetadata {
    const attention_node = graph.node(attention.attention_id);
    const attention_attrs = switch (attention_node.op) {
        .fused_gqa_causal_attention => |attrs| attrs,
        else => return null,
    };
    const num_heads: usize = attention_attrs.num_heads;
    const num_kv_heads: usize = if (attention_attrs.num_kv_heads == 0) attention_attrs.num_heads else attention_attrs.num_kv_heads;
    const head_dim: usize = attention_attrs.head_dim;
    if (num_heads == 0 or num_kv_heads == 0 or head_dim == 0) return null;
    const attention_input_size = num_heads * head_dim;
    if (attention_input_size != qkv.q_dim or attention_input_size != attention.attention_input_size) return null;
    if (attention.hidden_size != qkv.hidden_size or ple.hidden_size != qkv.hidden_size) return null;
    if (attention.rows != qkv.rows or ple.rows != qkv.rows) return null;

    const ffn_shape = runtimeFrameLayerShapeFromFfn(ffn_region) orelse return null;
    if (ffn_shape.rows != qkv.rows or ffn_shape.hidden_size != qkv.hidden_size) return null;
    const ffn = switch (ffn_region) {
        .rms_norm_gated_ffn_residual => |pattern| pattern.ffn,
        .gated_ffn_residual => |pattern| pattern,
        else => return null,
    };
    if (ffn.activation != ple.activation) return null;

    const shares_kv = attention_attrs.skip_kv_write;
    const kv_dim = num_kv_heads * head_dim;
    if (!shares_kv and qkv.kv_dim != kv_dim) return null;
    if (shares_kv and qkv.kv_dim != 0) return null;
    const kv_layer_index: usize = if (attention_attrs.layer_index == std.math.maxInt(u32))
        qkv.layer_index
    else
        attention_attrs.layer_index;

    return .{
        .layer_index = qkv.layer_index,
        .shares_kv = shares_kv,
        .kv_layer_index = kv_layer_index,
        .kv_heads = num_kv_heads,
        .head_dim = head_dim,
        .intermediate_size = ffn.intermediate_size,
        .hidden_size = qkv.hidden_size,
        .attention_input_size = attention_input_size,
        .ple_hidden_size = ple.ple_hidden_size,
        .activation = ffn.activation,
    };
}

fn runtimeFrameQkvInputId(region: RuntimeRegion) NodeId {
    return switch (region) {
        .q_linear => |pattern| pattern.input_id,
        .linear_qkv => |pattern| pattern.input_id,
        .grouped_linear_qkv_slice => |pattern| pattern.input_id,
        .rms_norm_grouped_linear_qkv_slice => |pattern| pattern.qkv.input_id,
        .lora_linear_qkv => |pattern| pattern.q.input_id,
        else => null_node,
    };
}

fn runtimeFrameFfnResidualId(region: RuntimeRegion) NodeId {
    return switch (region) {
        .rms_norm_gated_ffn_residual => |pattern| pattern.ffn.residual_id,
        .gated_ffn_residual => |pattern| pattern.residual_id,
        else => null_node,
    };
}

fn runtimeFrameFfnOutputId(region: RuntimeRegion) NodeId {
    return switch (region) {
        .rms_norm_gated_ffn_residual => |pattern| pattern.ffn.add_id,
        .gated_ffn_residual => |pattern| pattern.add_id,
        else => null_node,
    };
}

fn runtimeFrameBucketHasFfn(bucket: RuntimeFrameLayerBucket) bool {
    return runtimeFrameFfnOutputId(bucket.ffn) != null_node;
}

fn runtimeFrameCollectQkv(
    graph: ?*const Graph,
    collection: *RuntimeFrameLayerCollection,
    region: RuntimeRegion,
) bool {
    const input_id = runtimeFrameQkvInputId(region);
    if (input_id == null_node) {
        collection.reason = .non_layer_order;
        return false;
    }
    for (collection.buckets[0..collection.count]) |bucket| {
        if (bucket.qkv_input_id == input_id) {
            collection.reason = .non_layer_order;
            return false;
        }
    }
    if (collection.count >= runtime_frame_max_layers) {
        collection.reason = .non_layer_order;
        return false;
    }
    const qkv = if (graph) |g| runtimeFrameQkvMetadataFromRegion(g, region) else null;
    collection.buckets[collection.count] = .{
        .qkv = qkv,
        .qkv_region = region,
        .qkv_input_id = input_id,
    };
    collection.count += 1;
    return true;
}

fn runtimeFrameAttachAttention(collection: *RuntimeFrameLayerCollection, pattern: AttentionOutputResidualPattern) bool {
    for (collection.buckets[0..collection.count]) |*bucket| {
        if (bucket.qkv_input_id == pattern.residual_id) {
            if (bucket.attention != null) {
                collection.reason = .non_layer_order;
                return false;
            }
            bucket.attention = pattern;
            return true;
        }
    }
    for (collection.buckets[0..collection.count]) |*bucket| {
        if (bucket.attention == null) {
            bucket.attention = pattern;
            return true;
        }
    }
    collection.reason = .non_layer_order;
    return false;
}

fn runtimeFrameAttachFfn(collection: *RuntimeFrameLayerCollection, region: RuntimeRegion) bool {
    const residual_id = runtimeFrameFfnResidualId(region);
    if (residual_id == null_node) {
        collection.reason = .non_layer_order;
        return false;
    }
    var saw_attention = false;
    for (collection.buckets[0..collection.count]) |*bucket| {
        const attention = bucket.attention orelse continue;
        saw_attention = true;
        if (attention.add_id == residual_id) {
            if (runtimeFrameBucketHasFfn(bucket.*)) {
                collection.reason = .non_layer_order;
                return false;
            }
            bucket.ffn = region;
            return true;
        }
    }
    collection.reason = if (saw_attention) .non_layer_order else .missing_attention;
    return false;
}

fn runtimeFrameAttachPle(collection: *RuntimeFrameLayerCollection, pattern: PleResidualPattern) bool {
    var saw_ffn = false;
    for (collection.buckets[0..collection.count]) |*bucket| {
        if (!runtimeFrameBucketHasFfn(bucket.*)) continue;
        saw_ffn = true;
        if (runtimeFrameFfnOutputId(bucket.ffn) == pattern.hidden_id) {
            if (bucket.ple != null) {
                collection.reason = .non_layer_order;
                return false;
            }
            bucket.ple = pattern;
            return true;
        }
    }
    collection.reason = if (saw_ffn) .non_layer_order else .missing_ffn;
    return false;
}

fn runtimeFrameSortLayerBuckets(collection: *RuntimeFrameLayerCollection) void {
    for (collection.buckets[0..collection.count]) |bucket| {
        if (bucket.qkv == null) return;
    }
    var i: usize = 1;
    while (i < collection.count) : (i += 1) {
        const key = collection.buckets[i];
        var j = i;
        while (j > 0 and collection.buckets[j - 1].qkv.?.layer_index > key.qkv.?.layer_index) : (j -= 1) {
            collection.buckets[j] = collection.buckets[j - 1];
        }
        collection.buckets[j] = key;
    }
}

fn collectRuntimeFrameLayers(graph: ?*const Graph, plan: RuntimeRegionPlan) RuntimeFrameLayerCollection {
    var collection = RuntimeFrameLayerCollection{};
    if (plan.region_count == 0) {
        collection.reason = .no_regions;
        return collection;
    }

    var saw_frame_region = false;
    var saw_attention_output_residual = false;
    var saw_deberta_encoder_layer = false;
    for (plan.regions_by_pos) |region| {
        switch (region) {
            .q_linear, .linear_qkv, .grouped_linear_qkv_slice, .rms_norm_grouped_linear_qkv_slice, .lora_linear_qkv => {
                saw_frame_region = true;
                if (!runtimeFrameCollectQkv(graph, &collection, region)) return collection;
            },
            .attention_output_residual => {
                saw_frame_region = true;
                saw_attention_output_residual = true;
            },
            .deberta_encoder_lora_layer => {
                saw_frame_region = true;
                saw_deberta_encoder_layer = true;
            },
            .deberta_attention, .rms_norm_gated_ffn_residual, .gated_ffn_residual, .ple_residual => {
                saw_frame_region = true;
            },
            else => {},
        }
    }
    if (!saw_frame_region) {
        collection.reason = .no_regions;
        return collection;
    }
    if (collection.count == 0) {
        collection.reason = .missing_qkv;
        return collection;
    }
    if (!saw_attention_output_residual and saw_deberta_encoder_layer) {
        collection.reason = .missing_model_metadata;
        return collection;
    }

    for (plan.regions_by_pos) |region| {
        switch (region) {
            .attention_output_residual => |pattern| {
                if (!runtimeFrameAttachAttention(&collection, pattern)) return collection;
            },
            else => {},
        }
    }
    for (plan.regions_by_pos) |region| {
        switch (region) {
            .rms_norm_gated_ffn_residual, .gated_ffn_residual => {
                if (!runtimeFrameAttachFfn(&collection, region)) return collection;
            },
            else => {},
        }
    }
    for (plan.regions_by_pos) |region| {
        switch (region) {
            .ple_residual => |pattern| {
                if (!runtimeFrameAttachPle(&collection, pattern)) return collection;
            },
            else => {},
        }
    }

    runtimeFrameSortLayerBuckets(&collection);

    var frame_rows: usize = 0;
    for (collection.buckets[0..collection.count]) |bucket| {
        const qkv_shape = runtimeFrameLayerShapeFromQkv(bucket.qkv_region) orelse {
            collection.reason = .non_layer_order;
            return collection;
        };
        const attention = bucket.attention orelse {
            collection.reason = .missing_attention;
            return collection;
        };
        const ffn_shape = runtimeFrameLayerShapeFromFfn(bucket.ffn) orelse {
            collection.reason = .missing_ffn;
            return collection;
        };
        const ple = bucket.ple orelse {
            collection.reason = .missing_ple;
            return collection;
        };
        const attention_shape = runtimeFrameLayerShapeFromAttention(attention);
        const ple_shape = runtimeFrameLayerShapeFromPle(ple);
        if (!runtimeFrameShapesMatch(qkv_shape, attention_shape) or
            !runtimeFrameShapesMatch(qkv_shape, ffn_shape) or
            !runtimeFrameShapesMatch(qkv_shape, ple_shape))
        {
            collection.reason = .shape_mismatch;
            return collection;
        }
        if (frame_rows == 0) frame_rows = ple_shape.rows;
        collection.completed_layers += 1;
    }

    if (collection.completed_layers == 0) {
        collection.reason = .no_regions;
        return collection;
    }
    if (frame_rows <= 1) {
        collection.reason = .single_row;
        return collection;
    }
    collection.reason = .none;
    return collection;
}

fn traceRuntimeFrameOrder(graph: ?*const Graph, plan: RuntimeRegionPlan, collection: RuntimeFrameLayerCollection) void {
    if (!platform.env.getenvBoolDefault("TERMITE_METAL_TRACE_RUNTIME_FRAME_ORDER", false)) return;
    std.debug.print(
        "runtime_frame_order: regions={d} layers={d} completed={d} reason={s}\n",
        .{ plan.region_count, collection.count, collection.completed_layers, @tagName(collection.reason) },
    );
    for (plan.regions_by_pos, 0..) |region, pos| {
        switch (region) {
            .q_linear, .linear_qkv, .grouped_linear_qkv_slice, .rms_norm_grouped_linear_qkv_slice, .lora_linear_qkv => {
                const qkv = if (graph) |g| runtimeFrameQkvMetadataFromRegion(g, region) else null;
                std.debug.print(
                    "runtime_frame_order_region: pos={d} kind={s} q_layer={d} q_input={d}\n",
                    .{ pos, @tagName(region), if (qkv) |q| q.layer_index else std.math.maxInt(usize), runtimeFrameQkvInputId(region) },
                );
            },
            .attention_output_residual => |pattern| std.debug.print(
                "runtime_frame_order_region: pos={d} kind={s} residual={d} add={d} attention={d}\n",
                .{ pos, @tagName(region), pattern.residual_id, pattern.add_id, pattern.attention_id },
            ),
            .rms_norm_gated_ffn_residual, .gated_ffn_residual => std.debug.print(
                "runtime_frame_order_region: pos={d} kind={s} residual={d} output={d}\n",
                .{ pos, @tagName(region), runtimeFrameFfnResidualId(region), runtimeFrameFfnOutputId(region) },
            ),
            .ple_residual => |pattern| std.debug.print(
                "runtime_frame_order_region: pos={d} kind={s} hidden={d} add={d}\n",
                .{ pos, @tagName(region), pattern.hidden_id, pattern.add_id },
            ),
            else => {},
        }
    }
    for (collection.buckets[0..collection.count], 0..) |bucket, idx| {
        std.debug.print(
            "runtime_frame_order_bucket: idx={d} q_layer={d} q_input={d} attention_add={d} ffn_output={d} ple_add={d}\n",
            .{
                idx,
                if (bucket.qkv) |q| q.layer_index else std.math.maxInt(usize),
                bucket.qkv_input_id,
                if (bucket.attention) |a| a.add_id else null_node,
                runtimeFrameFfnOutputId(bucket.ffn),
                if (bucket.ple) |p| p.add_id else null_node,
            },
        );
    }
}

fn traceRuntimeFrameMetadataDeclined(
    reason: []const u8,
    layer_index: usize,
    qkv: ?RuntimeFrameQkvMetadata,
    attention: ?AttentionOutputResidualPattern,
    ffn: RuntimeRegion,
    ple: ?PleResidualPattern,
) void {
    if (!platform.env.getenvBoolDefault("TERMITE_METAL_TRACE_FRAME_METADATA", false)) return;
    const q_layer = if (qkv) |q| q.layer_index else std.math.maxInt(usize);
    const q_rows = if (qkv) |q| q.rows else 0;
    const q_hidden = if (qkv) |q| q.hidden_size else 0;
    const q_dim = if (qkv) |q| q.q_dim else 0;
    const attn_id = if (attention) |a| a.attention_id else null_node;
    const attn_rows = if (attention) |a| a.rows else 0;
    const attn_hidden = if (attention) |a| a.hidden_size else 0;
    const attn_dim = if (attention) |a| a.attention_input_size else 0;
    const ffn_shape = runtimeFrameLayerShapeFromFfn(ffn);
    const ffn_rows = if (ffn_shape) |shape| shape.rows else 0;
    const ffn_hidden = if (ffn_shape) |shape| shape.hidden_size else 0;
    const ple_rows = if (ple) |p| p.rows else 0;
    const ple_hidden = if (ple) |p| p.hidden_size else 0;
    const ple_dim = if (ple) |p| p.ple_hidden_size else 0;
    std.debug.print(
        "runtime_frame_metadata_declined reason={s} layer_count={d} q_layer={d} q_rows={d} q_hidden={d} q_dim={d} attn={d} attn_rows={d} attn_hidden={d} attn_dim={d} ffn_rows={d} ffn_hidden={d} ple_rows={d} ple_hidden={d} ple_dim={d}\n",
        .{ reason, layer_index, q_layer, q_rows, q_hidden, q_dim, attn_id, attn_rows, attn_hidden, attn_dim, ffn_rows, ffn_hidden, ple_rows, ple_hidden, ple_dim },
    );
}

fn runtimeFrameMetadataFromPlan(graph: *const Graph, plan: RuntimeRegionPlan) ?RuntimeFrameMetadata {
    const collection = collectRuntimeFrameLayers(graph, plan);
    traceRuntimeFrameOrder(graph, plan, collection);
    if (collection.reason != .none) {
        const bucket = if (collection.count > collection.completed_layers)
            collection.buckets[collection.completed_layers]
        else
            RuntimeFrameLayerBucket{};
        traceRuntimeFrameMetadataDeclined(
            @tagName(collection.reason),
            collection.completed_layers,
            bucket.qkv,
            bucket.attention,
            bucket.ffn,
            bucket.ple,
        );
        return null;
    }

    var rows: usize = 0;
    var hidden_size: usize = 0;
    var num_attention_heads: usize = 0;
    var global_head_dim: usize = 0;
    var ple_hidden_size: usize = 0;
    var activation: ?ops_mod.DecoderRuntimeActivationKind = null;
    var layer_count: usize = 0;

    for (collection.buckets[0..collection.count]) |bucket| {
        const qkv = bucket.qkv orelse {
            traceRuntimeFrameMetadataDeclined("qkv_metadata", layer_count, null, bucket.attention, bucket.ffn, bucket.ple);
            return null;
        };
        const attention = bucket.attention orelse {
            traceRuntimeFrameMetadataDeclined("attention_phase", layer_count, bucket.qkv, null, bucket.ffn, bucket.ple);
            return null;
        };
        const ple = bucket.ple orelse {
            traceRuntimeFrameMetadataDeclined("ple_phase", layer_count, bucket.qkv, bucket.attention, bucket.ffn, null);
            return null;
        };
        const layer = runtimeFrameLayerMetadata(
            graph,
            qkv,
            attention,
            bucket.ffn,
            ple,
        ) orelse {
            traceRuntimeFrameMetadataDeclined("layer_metadata", layer_count, bucket.qkv, bucket.attention, bucket.ffn, bucket.ple);
            return null;
        };
        if (layer.layer_index != layer_count) {
            traceRuntimeFrameMetadataDeclined("layer_index_order", layer_count, bucket.qkv, bucket.attention, bucket.ffn, bucket.ple);
            return null;
        }
        if (rows == 0) {
            rows = ple.rows;
            hidden_size = layer.hidden_size;
            ple_hidden_size = layer.ple_hidden_size;
            activation = layer.activation;
            num_attention_heads = if (layer.head_dim == 0) return null else layer.attention_input_size / layer.head_dim;
            global_head_dim = if (layer.shares_kv) layer.head_dim else 0;
        } else {
            if (rows != ple.rows or hidden_size != layer.hidden_size) {
                traceRuntimeFrameMetadataDeclined("frame_shape_mismatch", layer_count, bucket.qkv, bucket.attention, bucket.ffn, bucket.ple);
                return null;
            }
            if (ple_hidden_size != layer.ple_hidden_size) {
                traceRuntimeFrameMetadataDeclined("ple_size_mismatch", layer_count, bucket.qkv, bucket.attention, bucket.ffn, bucket.ple);
                return null;
            }
            if (activation.? != layer.activation) {
                traceRuntimeFrameMetadataDeclined("activation_mismatch", layer_count, bucket.qkv, bucket.attention, bucket.ffn, bucket.ple);
                return null;
            }
            const heads = if (layer.head_dim == 0) {
                traceRuntimeFrameMetadataDeclined("zero_head_dim", layer_count, bucket.qkv, bucket.attention, bucket.ffn, bucket.ple);
                return null;
            } else layer.attention_input_size / layer.head_dim;
            if (num_attention_heads != heads) {
                traceRuntimeFrameMetadataDeclined("num_heads_mismatch", layer_count, bucket.qkv, bucket.attention, bucket.ffn, bucket.ple);
                return null;
            }
            if (layer.shares_kv) {
                if (global_head_dim < layer.head_dim) {
                    global_head_dim = layer.head_dim;
                }
            }
        }
        layer_count += 1;
    }

    if (layer_count == 0) {
        traceRuntimeFrameMetadataDeclined("final_phase", layer_count, null, null, .{ .none = {} }, null);
        return null;
    }
    return .{
        .rows = rows,
        .layer_count = layer_count,
        .hidden_size = hidden_size,
        .num_attention_heads = num_attention_heads,
        .global_head_dim = global_head_dim,
        .ple_hidden_size = ple_hidden_size,
        .activation = activation orelse return null,
    };
}

fn analyzeRuntimeFrameEligibility(graph: ?*const Graph, plan: RuntimeRegionPlan) RuntimeFrameEligibility {
    const collection = collectRuntimeFrameLayers(graph, plan);
    traceRuntimeFrameOrder(graph, plan, collection);
    if (collection.reason != .none) return .{ .layers = collection.completed_layers, .reason = collection.reason };
    const g = graph orelse return .{ .layers = collection.completed_layers, .reason = .missing_model_metadata };
    if (runtimeFrameMetadataFromPlan(g, plan) == null) {
        return .{ .layers = collection.completed_layers, .reason = .missing_model_metadata };
    }
    return .{ .layers = collection.completed_layers, .reason = .none };
}

fn recordRuntimeFrameEligibilityStats(stats: *PartitionExecutor.ExecutionStats, eligibility: RuntimeFrameEligibility) void {
    if (eligibility.reason != .no_regions) stats.runtime_frame_candidates += 1;
    if (eligibility.eligible()) {
        stats.runtime_frame_eligible += 1;
        return;
    }
    switch (eligibility.reason) {
        .none => {},
        .no_regions => stats.runtime_frame_ineligible_no_regions += 1,
        .missing_qkv => stats.runtime_frame_ineligible_missing_qkv += 1,
        .missing_attention => stats.runtime_frame_ineligible_missing_attention += 1,
        .missing_ffn => stats.runtime_frame_ineligible_missing_ffn += 1,
        .missing_ple => stats.runtime_frame_ineligible_missing_ple += 1,
        .single_row => stats.runtime_frame_ineligible_single_row += 1,
        .non_layer_order => stats.runtime_frame_ineligible_non_layer_order += 1,
        .shape_mismatch => stats.runtime_frame_ineligible_shape_mismatch += 1,
        .missing_model_metadata => stats.runtime_frame_ineligible_missing_model_metadata += 1,
    }
}

fn markSkipped(skipped: []bool, node_id: NodeId) void {
    const i: usize = @intCast(node_id);
    if (i < skipped.len) skipped[i] = true;
}

fn markLinearNoBiasQkvSkipped(skipped: []bool, pattern: LinearNoBiasQkvPattern) void {
    markSkipped(skipped, pattern.k_id);
    markSkipped(skipped, pattern.v_id);
}

fn markLoraLinearSkipped(skipped: []bool, pattern: LoraLinearPattern) void {
    if (pattern.dropout_mul_id) |dropout_mul_id| markSkipped(skipped, dropout_mul_id);
    markSkipped(skipped, pattern.after_a_id);
    markSkipped(skipped, pattern.after_b_id);
    markSkipped(skipped, pattern.scaled_id);
}

fn markLoraLinearQkvSkipped(skipped: []bool, pattern: LoraLinearQkvPattern) void {
    markLoraLinearSkipped(skipped, pattern.q);
    markLoraLinearSkipped(skipped, pattern.k);
    markLoraLinearSkipped(skipped, pattern.v);
    markSkipped(skipped, pattern.k.add_id);
    markSkipped(skipped, pattern.v.add_id);
}

fn markDebertaAttentionSkipped(graph: *const Graph, skipped: []bool, pattern: DebertaAttentionPattern) void {
    var node_id = pattern.first_node_id;
    while (node_id <= pattern.output_id) : (node_id += 1) {
        if (debertaAttentionNodeFeedsOutsideConsumer(
            graph,
            node_id,
            pattern.first_node_id,
            pattern.output_id,
            pattern.output_id,
            @intCast(pattern.output_id - pattern.first_node_id + 1),
        )) continue;
        markSkipped(skipped, node_id);
        if (node_id == std.math.maxInt(NodeId)) break;
    }
}

fn debertaAttentionNodeFeedsOutsideConsumer(
    graph: *const Graph,
    node_id: NodeId,
    first_id: NodeId,
    last_id: NodeId,
    output_id: NodeId,
    depth_remaining: usize,
) bool {
    if (node_id == output_id) return false;
    if (depth_remaining == 0) return false;

    var candidate_id: NodeId = 0;
    while (candidate_id < graph.nodeCount()) : (candidate_id += 1) {
        const candidate = graph.node(candidate_id);
        for (candidate.getInputs()) |input_id| {
            if (input_id != node_id) continue;
            if (candidate_id < first_id or candidate_id > last_id) return true;
            if (candidate_id == output_id) continue;
            if (debertaAttentionNodeFeedsOutsideConsumer(
                graph,
                candidate_id,
                first_id,
                last_id,
                output_id,
                depth_remaining - 1,
            )) return true;
        }
    }
    return false;
}

fn markGroupedLinearQkvSkipped(skipped: []bool, pattern: GroupedLinearQkvSlicePattern) void {
    markSkipped(skipped, pattern.linear_id);
    markSkipped(skipped, pattern.q_slice_id);
    markSkipped(skipped, pattern.k_slice_id);
    markSkipped(skipped, pattern.v_slice_id);
}

fn markPackedLinearQkvSkipped(skipped: []bool, pattern: PackedLinearQkvSlicePattern) void {
    markSkipped(skipped, pattern.linear_id);
    markSkipped(skipped, pattern.q_slice_id);
    markSkipped(skipped, pattern.k_slice_id);
    markSkipped(skipped, pattern.v_slice_id);
}

fn markRmsNormGroupedLinearQkvSkipped(skipped: []bool, pattern: RmsNormGroupedLinearQkvSlicePattern) void {
    markSkipped(skipped, pattern.norm_id);
    markGroupedLinearQkvSkipped(skipped, pattern.qkv);
}

fn markAttentionOutputResidualSkipped(skipped: []bool, pattern: AttentionOutputResidualPattern) void {
    if (pattern.pre_linear_norm_id) |norm_id| markSkipped(skipped, norm_id);
    markSkipped(skipped, pattern.linear_id);
    if (pattern.post_linear_norm_id) |norm_id| markSkipped(skipped, norm_id);
    markSkipped(skipped, pattern.add_id);
}

fn markRmsNormGatedFfnResidualSkipped(skipped: []bool, pattern: RmsNormGatedFfnResidualPattern) void {
    markGatedFfnResidualSkipped(skipped, pattern.ffn);
}

fn markGatedFfnResidualSkipped(skipped: []bool, pattern: GatedFfnResidualPattern) void {
    markSkipped(skipped, pattern.pair_second_id);
    markSkipped(skipped, pattern.activation_id);
    markSkipped(skipped, pattern.multiply_id);
    markSkipped(skipped, pattern.down_id);
    if (pattern.post_down_norm_id) |norm_id| markSkipped(skipped, norm_id);
    markSkipped(skipped, pattern.add_id);
}

fn markPleResidualSkipped(skipped: []bool, pattern: PleResidualPattern) void {
    markSkipped(skipped, pattern.activation_id);
    markSkipped(skipped, pattern.multiply_id);
    markSkipped(skipped, pattern.projection_id);
    markSkipped(skipped, pattern.post_norm_id);
    markSkipped(skipped, pattern.add_id);
}

fn printMetalGraphPlanTrace(partition_index: u32, plan: MetalPartitionGraphPlan) void {
    var total_bytes: usize = 0;
    for (plan.slots) |slot| total_bytes += slot.bytes;
    std.debug.print(
        "graph_executor_node_trace: graph_plan partition={d} slots={d} bytes={d}",
        .{ partition_index, plan.slots.len, total_bytes },
    );
    for (plan.slots) |slot| {
        std.debug.print(" slot{d}={d}", .{ slot.slot, slot.bytes });
    }
    std.debug.print("\n", .{});
}

fn printMetalNodeTraceBegin(graph: *const Graph, node_id: NodeId) void {
    const n = graph.node(node_id);
    switch (n.op) {
        .parameter => {
            std.debug.print(
                "graph_executor_node_trace: begin node={d} op=parameter name={s} shape={any}\n",
                .{ node_id, graph.parameterName(n), n.output_shape },
            );
        },
        else => {
            std.debug.print(
                "graph_executor_node_trace: begin node={d} op={s} shape={any}\n",
                .{ node_id, @tagName(n.op), n.output_shape },
            );
        },
    }
}

fn printMetalNodeTraceInputs(graph: *const Graph, cb: *const ComputeBackend, values: []?CT, node_id: NodeId) void {
    const n = graph.node(node_id);
    for (n.getInputs(), 0..) |input_id, input_index| {
        const input_node = graph.node(input_id);
        const value = valueFor(values, input_id);
        const device = if (value) |ct| isMetalDeviceResident(cb, ct) else false;
        const quant = if (comptime build_options.enable_metal)
            if (value) |ct| metal_compute_mod.MetalCompute.getQuantizedStorage(cb, ct) != null else false
        else
            false;
        const runtime_quant = if (comptime build_options.enable_metal)
            if (value) |ct| metal_compute_mod.MetalCompute.debugHasRuntimeQuantizedStorage(cb, ct) else false
        else
            false;
        if (value) |ct| {
            const tensor_shape = cb.tensorShape(ct, std.heap.page_allocator) catch null;
            defer if (tensor_shape) |shape| std.heap.page_allocator.free(shape);
            if (tensor_shape) |shape| {
                std.debug.print(
                    "graph_executor_node_trace: input node={d} input_index={d} input_node={d} op={s} graph_shape={any} tensor_shape={any} device={} quant={} runtime_quant={}\n",
                    .{ node_id, input_index, input_id, @tagName(input_node.op), input_node.output_shape, shape, device, quant, runtime_quant },
                );
            } else {
                std.debug.print(
                    "graph_executor_node_trace: input node={d} input_index={d} input_node={d} op={s} graph_shape={any} tensor_shape=<unavailable> device={} quant={} runtime_quant={}\n",
                    .{ node_id, input_index, input_id, @tagName(input_node.op), input_node.output_shape, device, quant, runtime_quant },
                );
            }
        } else {
            std.debug.print(
                "graph_executor_node_trace: input node={d} input_index={d} input_node={d} op={s} graph_shape={any} tensor_shape=<null> device={} quant={} runtime_quant={}\n",
                .{ node_id, input_index, input_id, @tagName(input_node.op), input_node.output_shape, device, quant, runtime_quant },
            );
        }
    }
}

fn printMetalNodeTraceEnd(graph: *const Graph, cb: *const ComputeBackend, node_id: NodeId, output: CT, used_command: bool) void {
    const n = graph.node(node_id);
    const quant = if (comptime build_options.enable_metal)
        metal_compute_mod.MetalCompute.getQuantizedStorage(cb, output) != null
    else
        false;
    const runtime_quant = if (comptime build_options.enable_metal)
        metal_compute_mod.MetalCompute.debugHasRuntimeQuantizedStorage(cb, output)
    else
        false;
    std.debug.print(
        "graph_executor_node_trace: end node={d} op={s} command={} device={} quant={} runtime_quant={}\n",
        .{
            node_id,
            @tagName(n.op),
            used_command,
            isMetalDeviceResident(cb, output),
            quant,
            runtime_quant,
        },
    );
}

const GemmaRuntimeResidencyCategory = enum {
    qkv,
    o_proj,
    mlp_proj,
    attention_matmul,
    rms_norm,
    softmax,
    residual_add,
    elementwise_mul,
};

fn recordGemmaRuntimeResidency(
    stats: *PartitionExecutor.ExecutionStats,
    graph: *const Graph,
    node_id: NodeId,
    hit: bool,
) void {
    const category = classifyGemmaRuntimeResidencyNode(graph, node_id) orelse return;
    switch (category) {
        .qkv => if (hit) {
            stats.gemma_qkv_hits += 1;
        } else {
            stats.gemma_qkv_fallbacks += 1;
        },
        .o_proj => if (hit) {
            stats.gemma_o_proj_hits += 1;
        } else {
            stats.gemma_o_proj_fallbacks += 1;
        },
        .mlp_proj => if (hit) {
            stats.gemma_mlp_proj_hits += 1;
        } else {
            stats.gemma_mlp_proj_fallbacks += 1;
        },
        .attention_matmul => if (hit) {
            stats.gemma_attention_matmul_hits += 1;
        } else {
            stats.gemma_attention_matmul_fallbacks += 1;
        },
        .rms_norm => if (hit) {
            stats.gemma_rms_norm_hits += 1;
        } else {
            stats.gemma_rms_norm_fallbacks += 1;
        },
        .softmax => if (hit) {
            stats.gemma_softmax_hits += 1;
        } else {
            stats.gemma_softmax_fallbacks += 1;
        },
        .residual_add => if (hit) {
            stats.gemma_residual_add_hits += 1;
        } else {
            stats.gemma_residual_add_fallbacks += 1;
        },
        .elementwise_mul => if (hit) {
            stats.gemma_elementwise_mul_hits += 1;
        } else {
            stats.gemma_elementwise_mul_fallbacks += 1;
        },
    }
}

fn tryExecuteFusedMetalGraphPattern(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    value_device: []DeviceId,
    node_ids: []const NodeId,
    node_pos: usize,
    reachable: []const bool,
    device_id: DeviceId,
    exec_ctx: PartitionExecutor.ExecutionContext,
    exec_state: *interpreter.ExecState,
    skipped_nodes: []bool,
    elision_protected_nodes: []const bool,
    runtime_region_plan: RuntimeRegionPlan,
    last_use: []const u32,
    rt_map: std.AutoHashMapUnmanaged(NodeId, CT),
    donated: std.AutoHashMapUnmanaged(NodeId, void),
) !bool {
    if (fusedPatternProbingDisabled(exec_ctx)) return false;
    if (try tryExecuteGroupedHeadDotPattern(
        allocator,
        graph,
        cb,
        values,
        value_device,
        node_ids,
        node_pos,
        reachable,
        device_id,
        skipped_nodes,
        elision_protected_nodes,
        runtime_region_plan,
        exec_ctx.stats,
    )) return true;
    if (try tryExecuteRmsNormGroupedLinearQkvSlicePattern(
        allocator,
        graph,
        cb,
        values,
        value_device,
        node_ids,
        node_pos,
        reachable,
        device_id,
        exec_ctx,
        skipped_nodes,
        last_use,
        rt_map,
        donated,
    )) return true;
    if (try tryExecuteGroupedLinearQkvSlicePattern(
        allocator,
        graph,
        cb,
        values,
        value_device,
        node_ids,
        node_pos,
        reachable,
        device_id,
        exec_ctx,
        skipped_nodes,
        last_use,
        rt_map,
        donated,
    )) return true;
    if (try tryExecuteLinearNoBiasQkvPattern(
        allocator,
        graph,
        cb,
        values,
        value_device,
        node_ids,
        node_pos,
        reachable,
        device_id,
        exec_ctx,
        skipped_nodes,
        last_use,
        rt_map,
        donated,
    )) return true;
    if (try tryExecuteAttentionOutputResidualPattern(
        graph,
        cb,
        values,
        value_device,
        node_ids,
        node_pos,
        reachable,
        device_id,
        exec_ctx,
        exec_state,
        skipped_nodes,
        last_use,
    )) return true;
    if (try tryExecuteRmsNormGatedFfnResidualPattern(
        graph,
        cb,
        values,
        value_device,
        node_ids,
        node_pos,
        reachable,
        device_id,
        skipped_nodes,
        last_use,
        exec_ctx.stats,
    )) return true;
    if (try tryExecuteGatedFfnResidualPattern(
        graph,
        cb,
        values,
        value_device,
        node_ids,
        node_pos,
        reachable,
        device_id,
        skipped_nodes,
        last_use,
        exec_ctx.stats,
    )) return true;
    if (try tryExecutePleResidualPattern(
        graph,
        cb,
        values,
        value_device,
        node_ids,
        node_pos,
        reachable,
        device_id,
        skipped_nodes,
        exec_ctx.stats,
    )) return true;
    return tryExecuteLinearNoBiasPairPattern(
        allocator,
        graph,
        cb,
        values,
        value_device,
        node_ids,
        node_pos,
        reachable,
        device_id,
        exec_ctx,
        skipped_nodes,
        last_use,
        rt_map,
        donated,
    );
}

fn preparedRuntimeRegionMatches(region: RuntimeRegion, prepared: PreparedRuntimeRegion) bool {
    return std.meta.activeTag(region) == std.meta.activeTag(prepared);
}

fn preparedRuntimeRegionSlotCount(prepared: PreparedRuntimeRegion) u64 {
    return switch (prepared) {
        .none => 0,
        .raw_linear_dot => 1,
        .raw_linear_pair => 2,
        .raw_linear_bias => 1,
        .raw_linear_bias_pair => 2,
        .lora_linear => 0,
        .lora_linear_qkv => 0,
        .deberta_attention => 0,
        .deberta_ffn_forward => 2,
        .deberta_encoder_lora_layer => 2,
        .lora_backward => 0,
        .low_rank_lora_backward => 0,
        .rank_adapter_backward => 0,
        .ffn_gelu_backward => 0,
        .q_linear => 1,
        .linear_qkv, .grouped_linear_qkv_slice, .packed_linear_qkv_slice => 3,
        .rms_norm_grouped_linear_qkv_slice => 4,
        .attention_output_residual => |slots| 1 +
            @as(u64, if (slots.pre_linear_rms_norm_slot != null) 1 else 0) +
            @as(u64, if (slots.post_linear_rms_norm_slot != null) 1 else 0),
        .rms_norm_gated_ffn_residual => |slots| 1 + preparedRuntimeRegionSlotCount(.{ .gated_ffn_residual = slots.ffn }),
        .gated_ffn_residual => |slots| 3 + @as(u64, if (slots.post_down_rms_norm_slot != null) 1 else 0),
        .ple_residual => 3,
    };
}

fn prepareRuntimeRegionPlan(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    value_device: []DeviceId,
    device_id: DeviceId,
    plan: RuntimeRegionPlan,
    stats: ?*PartitionExecutor.ExecutionStats,
) !void {
    for (plan.regions_by_pos, 0..) |region, node_pos| {
        if (std.meta.activeTag(region) == .none) continue;
        if (node_pos >= plan.prepared_by_pos.len) continue;
        _ = try prepareRuntimeRegion(
            graph,
            cb,
            values,
            value_device,
            device_id,
            region,
            &plan.prepared_by_pos[node_pos],
            stats,
        );
    }
}

fn ensurePreparedLinearSlot(
    cb: *const ComputeBackend,
    values: []?CT,
    weight_id: NodeId,
    in_dim: usize,
    out_dim: usize,
    stats: ?*PartitionExecutor.ExecutionStats,
) !?usize {
    const weight = valueFor(values, weight_id) orelse return null;
    if (stats) |s| s.runtime_prepare_slot_calls += 1;
    return try cb.decoderRuntimeEnsureLinearSlot(&.{
        .weight = weight,
        .bias = null,
        .in_dim = in_dim,
        .out_dim = out_dim,
    });
}

fn ensurePreparedLinearSlotWithOptionalBias(
    cb: *const ComputeBackend,
    values: []?CT,
    weight_id: NodeId,
    bias_id: ?NodeId,
    in_dim: usize,
    out_dim: usize,
    stats: ?*PartitionExecutor.ExecutionStats,
) !?usize {
    const weight = valueFor(values, weight_id) orelse return null;
    const bias = if (bias_id) |id| valueFor(values, id) orelse return null else null;
    if (stats) |s| s.runtime_prepare_slot_calls += 1;
    return try cb.decoderRuntimeEnsureLinearSlot(&.{
        .weight = weight,
        .bias = bias,
        .in_dim = in_dim,
        .out_dim = out_dim,
    });
}

fn ensurePreparedRmsNormSlot(
    cb: *const ComputeBackend,
    values: []?CT,
    weight_id: NodeId,
    hidden_size: usize,
    stats: ?*PartitionExecutor.ExecutionStats,
) !?usize {
    const weight = valueFor(values, weight_id) orelse return null;
    if (stats) |s| s.runtime_prepare_slot_calls += 1;
    return try cb.decoderRuntimeEnsureRmsNormSlot(&.{
        .weight = weight,
        .hidden_size = hidden_size,
    });
}

fn prepareQLinearRegion(
    cb: *const ComputeBackend,
    values: []?CT,
    pattern: QLinearPattern,
    stats: ?*PartitionExecutor.ExecutionStats,
) !?PreparedLinearRegion {
    const linear_slot = (try ensurePreparedLinearSlot(
        cb,
        values,
        pattern.weight_id,
        pattern.in_dim,
        pattern.out_dim,
        stats,
    )) orelse return null;
    return .{ .linear_slot = linear_slot };
}

fn prepareQkvRegion(
    cb: *const ComputeBackend,
    values: []?CT,
    q_weight_id: NodeId,
    k_weight_id: NodeId,
    v_weight_id: NodeId,
    in_dim: usize,
    q_out_dim: usize,
    kv_out_dim: usize,
    stats: ?*PartitionExecutor.ExecutionStats,
) !?PreparedQkvRegion {
    const q_slot = (try ensurePreparedLinearSlot(cb, values, q_weight_id, in_dim, q_out_dim, stats)) orelse return null;
    const k_slot = (try ensurePreparedLinearSlot(cb, values, k_weight_id, in_dim, kv_out_dim, stats)) orelse return null;
    const v_slot = (try ensurePreparedLinearSlot(cb, values, v_weight_id, in_dim, kv_out_dim, stats)) orelse return null;
    return .{
        .q_slot = q_slot,
        .k_slot = k_slot,
        .v_slot = v_slot,
    };
}

fn prepareLinearNoBiasQkvRegion(
    cb: *const ComputeBackend,
    values: []?CT,
    pattern: LinearNoBiasQkvPattern,
    stats: ?*PartitionExecutor.ExecutionStats,
) !?PreparedQkvRegion {
    return prepareQkvRegion(
        cb,
        values,
        pattern.q_weight_id,
        pattern.k_weight_id,
        pattern.v_weight_id,
        pattern.in_dim,
        pattern.q_out_dim,
        pattern.kv_out_dim,
        stats,
    );
}

fn prepareGroupedLinearQkvSliceRegion(
    cb: *const ComputeBackend,
    values: []?CT,
    pattern: GroupedLinearQkvSlicePattern,
    stats: ?*PartitionExecutor.ExecutionStats,
) !?PreparedQkvRegion {
    return prepareQkvRegion(
        cb,
        values,
        pattern.q_weight_id,
        pattern.k_weight_id,
        pattern.v_weight_id,
        pattern.in_dim,
        pattern.q_out_dim,
        pattern.kv_out_dim,
        stats,
    );
}

fn ensurePreparedLinearSlotFromTensor(
    cb: *const ComputeBackend,
    weight: CT,
    bias: CT,
    in_dim: usize,
    out_dim: usize,
    stats: ?*PartitionExecutor.ExecutionStats,
) !?usize {
    if (stats) |s| s.runtime_prepare_slot_calls += 1;
    return try cb.decoderRuntimeEnsureLinearSlot(&.{
        .weight = weight,
        .bias = bias,
        .in_dim = in_dim,
        .out_dim = out_dim,
    });
}

fn preparePackedLinearQkvSliceRegion(
    allocator: std.mem.Allocator,
    cb: *const ComputeBackend,
    values: []?CT,
    pattern: PackedLinearQkvSlicePattern,
    stats: ?*PartitionExecutor.ExecutionStats,
) !?PreparedQkvRegion {
    const weight = valueFor(values, pattern.weight_id) orelse return null;
    const bias = valueFor(values, pattern.bias_id) orelse return null;

    const q_weight = try cb.sliceRows2D(allocator, weight, 0, pattern.q_out_dim, pattern.in_dim);
    defer cb.free(q_weight);
    const k_weight = try cb.sliceRows2D(allocator, weight, pattern.q_out_dim, pattern.kv_out_dim, pattern.in_dim);
    defer cb.free(k_weight);
    const v_weight = try cb.sliceRows2D(allocator, weight, pattern.q_out_dim + pattern.kv_out_dim, pattern.kv_out_dim, pattern.in_dim);
    defer cb.free(v_weight);

    const q_bias = try slicePackedBias(cb, bias, 0, pattern.q_out_dim, pattern.q_out_dim + pattern.kv_out_dim * 2);
    defer cb.free(q_bias);
    const k_bias = try slicePackedBias(cb, bias, pattern.q_out_dim, pattern.q_out_dim + pattern.kv_out_dim, pattern.q_out_dim + pattern.kv_out_dim * 2);
    defer cb.free(k_bias);
    const v_bias = try slicePackedBias(cb, bias, pattern.q_out_dim + pattern.kv_out_dim, pattern.q_out_dim + pattern.kv_out_dim * 2, pattern.q_out_dim + pattern.kv_out_dim * 2);
    defer cb.free(v_bias);

    const q_slot = (try ensurePreparedLinearSlotFromTensor(cb, q_weight, q_bias, pattern.in_dim, pattern.q_out_dim, stats)) orelse return null;
    const k_slot = (try ensurePreparedLinearSlotFromTensor(cb, k_weight, k_bias, pattern.in_dim, pattern.kv_out_dim, stats)) orelse return null;
    const v_slot = (try ensurePreparedLinearSlotFromTensor(cb, v_weight, v_bias, pattern.in_dim, pattern.kv_out_dim, stats)) orelse return null;
    return .{
        .q_slot = q_slot,
        .k_slot = k_slot,
        .v_slot = v_slot,
    };
}

fn slicePackedBias(cb: *const ComputeBackend, bias: CT, start: usize, limit: usize, total: usize) !CT {
    const starts = [_]i64{@intCast(start)};
    const limits = [_]i64{@intCast(limit)};
    const strides = [_]i64{1};
    const shape = [_]i64{@intCast(total)};
    return cb.primSlice(bias, &starts, &limits, &strides, &shape);
}

fn prepareRmsNormGroupedLinearQkvSliceRegion(
    cb: *const ComputeBackend,
    values: []?CT,
    pattern: RmsNormGroupedLinearQkvSlicePattern,
    stats: ?*PartitionExecutor.ExecutionStats,
) !?PreparedRmsNormGroupedQkvRegion {
    const norm_slot = (try ensurePreparedRmsNormSlot(cb, values, pattern.norm_weight_id, pattern.norm_dim, stats)) orelse return null;
    const qkv = (try prepareGroupedLinearQkvSliceRegion(cb, values, pattern.qkv, stats)) orelse return null;
    return .{
        .norm_slot = norm_slot,
        .qkv = qkv,
    };
}

fn prepareAttentionOutputResidualRegion(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    pattern: AttentionOutputResidualPattern,
    stats: ?*PartitionExecutor.ExecutionStats,
) !?PreparedAttentionOutputResidualRegion {
    const linear_inputs = graph.node(pattern.linear_id).getInputs();
    if (linear_inputs.len < 2) return null;
    const linear_slot = (try ensurePreparedLinearSlot(
        cb,
        values,
        linear_inputs[1],
        pattern.attention_input_size,
        pattern.hidden_size,
        stats,
    )) orelse return null;

    var pre_linear_slot: ?usize = null;
    if (pattern.pre_linear_norm_id) |norm_id| {
        const norm_inputs = graph.node(norm_id).getInputs();
        if (norm_inputs.len < 2) return null;
        pre_linear_slot = (try ensurePreparedRmsNormSlot(
            cb,
            values,
            norm_inputs[1],
            pattern.attention_input_size,
            stats,
        )) orelse return null;
    }

    var post_linear_slot: ?usize = null;
    if (pattern.post_linear_norm_id) |norm_id| {
        const norm_inputs = graph.node(norm_id).getInputs();
        if (norm_inputs.len < 2) return null;
        post_linear_slot = (try ensurePreparedRmsNormSlot(
            cb,
            values,
            norm_inputs[1],
            pattern.hidden_size,
            stats,
        )) orelse return null;
    }

    return .{
        .linear_slot = linear_slot,
        .pre_linear_rms_norm_slot = pre_linear_slot,
        .post_linear_rms_norm_slot = post_linear_slot,
    };
}

fn prepareGatedFfnResidualRegion(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    pattern: GatedFfnResidualPattern,
    stats: ?*PartitionExecutor.ExecutionStats,
) !?PreparedGatedFfnResidualRegion {
    const pair_inputs = graph.node(pattern.pair_id).getInputs();
    if (pair_inputs.len < 3) return null;
    const gate_slot = (try ensurePreparedLinearSlot(
        cb,
        values,
        pair_inputs[1],
        pattern.hidden_size,
        pattern.intermediate_size,
        stats,
    )) orelse return null;
    const up_slot = (try ensurePreparedLinearSlot(
        cb,
        values,
        pair_inputs[2],
        pattern.hidden_size,
        pattern.intermediate_size,
        stats,
    )) orelse return null;

    const down_inputs = graph.node(pattern.down_id).getInputs();
    if (down_inputs.len < 2) return null;
    const down_slot = (try ensurePreparedLinearSlot(
        cb,
        values,
        down_inputs[1],
        pattern.intermediate_size,
        pattern.hidden_size,
        stats,
    )) orelse return null;

    var post_down_slot: ?usize = null;
    if (pattern.post_down_norm_id) |norm_id| {
        const norm_inputs = graph.node(norm_id).getInputs();
        if (norm_inputs.len < 2) return null;
        post_down_slot = (try ensurePreparedRmsNormSlot(
            cb,
            values,
            norm_inputs[1],
            pattern.hidden_size,
            stats,
        )) orelse return null;
    }

    return .{
        .gate_slot = gate_slot,
        .up_slot = up_slot,
        .down_slot = down_slot,
        .post_down_rms_norm_slot = post_down_slot,
    };
}

fn prepareRmsNormGatedFfnResidualRegion(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    pattern: RmsNormGatedFfnResidualPattern,
    stats: ?*PartitionExecutor.ExecutionStats,
) !?PreparedRmsNormGatedFfnResidualRegion {
    const norm_slot = (try ensurePreparedRmsNormSlot(
        cb,
        values,
        pattern.norm_weight_id,
        pattern.norm_dim,
        stats,
    )) orelse return null;
    const ffn = (try prepareGatedFfnResidualRegion(graph, cb, values, pattern.ffn, stats)) orelse return null;
    return .{ .norm_slot = norm_slot, .ffn = ffn };
}

fn preparePleResidualRegion(
    cb: *const ComputeBackend,
    values: []?CT,
    pattern: PleResidualPattern,
    stats: ?*PartitionExecutor.ExecutionStats,
) !?PreparedPleResidualRegion {
    const gate_slot = (try ensurePreparedLinearSlot(
        cb,
        values,
        pattern.gate_weight_id,
        pattern.hidden_size,
        pattern.ple_hidden_size,
        stats,
    )) orelse return null;
    const projection_slot = (try ensurePreparedLinearSlot(
        cb,
        values,
        pattern.projection_weight_id,
        pattern.ple_hidden_size,
        pattern.hidden_size,
        stats,
    )) orelse return null;
    const post_norm_slot = (try ensurePreparedRmsNormSlot(
        cb,
        values,
        pattern.post_norm_weight_id,
        pattern.hidden_size,
        stats,
    )) orelse return null;
    return .{
        .gate_slot = gate_slot,
        .projection_slot = projection_slot,
        .post_norm_slot = post_norm_slot,
    };
}

fn prepareDebertaFfnForwardRegion(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    value_device: []DeviceId,
    device_id: DeviceId,
    pattern: DebertaFfnForwardPattern,
    stats: ?*PartitionExecutor.ExecutionStats,
) !?PreparedDebertaFfnForwardRegion {
    _ = (try valueForOrMaterializeParameter(graph, cb, values, value_device, device_id, pattern.first_weight_id)) orelse return null;
    _ = (try valueForOrMaterializeParameter(graph, cb, values, value_device, device_id, pattern.first_bias_id)) orelse return null;
    _ = (try valueForOrMaterializeParameter(graph, cb, values, value_device, device_id, pattern.second_weight_id)) orelse return null;
    _ = (try valueForOrMaterializeParameter(graph, cb, values, value_device, device_id, pattern.second_bias_id)) orelse return null;
    const first_slot = (try ensurePreparedLinearSlotWithOptionalBias(
        cb,
        values,
        pattern.first_weight_id,
        pattern.first_bias_id,
        pattern.hidden_size,
        pattern.intermediate_size,
        stats,
    )) orelse return null;
    const second_slot = (try ensurePreparedLinearSlotWithOptionalBias(
        cb,
        values,
        pattern.second_weight_id,
        pattern.second_bias_id,
        pattern.intermediate_size,
        pattern.output_size,
        stats,
    )) orelse return null;
    return .{ .first_slot = first_slot, .second_slot = second_slot };
}

fn prepareDebertaEncoderLoraLayerRegion(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    value_device: []DeviceId,
    device_id: DeviceId,
    pattern: DebertaEncoderLoraLayerPattern,
    stats: ?*PartitionExecutor.ExecutionStats,
) !?PreparedDebertaFfnForwardRegion {
    const ffn = (try prepareDebertaFfnForwardRegion(graph, cb, values, value_device, device_id, pattern.ffn, stats)) orelse return null;
    if (pattern.layer_norm_id != null_node) {
        _ = (try valueForOrMaterializeParameter(graph, cb, values, value_device, device_id, pattern.layer_norm_weight_id)) orelse return null;
        _ = (try valueForOrMaterializeParameter(graph, cb, values, value_device, device_id, pattern.layer_norm_bias_id)) orelse return null;
    }
    return ffn;
}

fn prepareRuntimeRegion(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    value_device: []DeviceId,
    device_id: DeviceId,
    region: RuntimeRegion,
    prepared_region: ?*PreparedRuntimeRegion,
    stats: ?*PartitionExecutor.ExecutionStats,
) !?PreparedRuntimeRegion {
    if (prepared_region) |prepared_ptr| {
        if (preparedRuntimeRegionMatches(region, prepared_ptr.*)) {
            if (stats) |s| s.runtime_prepare_slot_cache_hits += preparedRuntimeRegionSlotCount(prepared_ptr.*);
            return prepared_ptr.*;
        }
    }

    const prepared: PreparedRuntimeRegion = switch (region) {
        .none => return null,
        .raw_linear_dot => |pattern| .{
            .raw_linear_dot = (try prepareRawLinearDotRegion(cb, values, pattern, stats)) orelse return null,
        },
        .raw_linear_pair => |pattern| .{
            .raw_linear_pair = (try prepareRawLinearPairRegion(cb, values, pattern, stats)) orelse return null,
        },
        .raw_linear_bias => |pattern| .{
            .raw_linear_bias = (try prepareRawLinearBiasRegion(cb, values, pattern, stats)) orelse return null,
        },
        .raw_linear_bias_pair => |pattern| .{
            .raw_linear_bias_pair = (try prepareRawLinearBiasPairRegion(cb, values, pattern, stats)) orelse return null,
        },
        .lora_linear => .{ .lora_linear = {} },
        .lora_linear_qkv => .{ .lora_linear_qkv = {} },
        .deberta_attention => .{ .deberta_attention = {} },
        .deberta_ffn_forward => |pattern| .{
            .deberta_ffn_forward = (try prepareDebertaFfnForwardRegion(graph, cb, values, value_device, device_id, pattern, stats)) orelse return null,
        },
        .deberta_encoder_lora_layer => |pattern| .{
            .deberta_encoder_lora_layer = (try prepareDebertaEncoderLoraLayerRegion(graph, cb, values, value_device, device_id, pattern, stats)) orelse return null,
        },
        .lora_backward => .{ .lora_backward = {} },
        .low_rank_lora_backward => .{ .low_rank_lora_backward = {} },
        .rank_adapter_backward => .{ .rank_adapter_backward = {} },
        .ffn_gelu_backward => .{ .ffn_gelu_backward = {} },
        .q_linear => |pattern| .{
            .q_linear = (try prepareQLinearRegion(cb, values, pattern, stats)) orelse return null,
        },
        .linear_qkv => |pattern| .{
            .linear_qkv = (try prepareLinearNoBiasQkvRegion(cb, values, pattern, stats)) orelse return null,
        },
        .grouped_linear_qkv_slice => |pattern| .{
            .grouped_linear_qkv_slice = (try prepareGroupedLinearQkvSliceRegion(cb, values, pattern, stats)) orelse return null,
        },
        .packed_linear_qkv_slice => |pattern| .{
            .packed_linear_qkv_slice = (try preparePackedLinearQkvSliceRegion(graph.allocator, cb, values, pattern, stats)) orelse return null,
        },
        .rms_norm_grouped_linear_qkv_slice => |pattern| .{
            .rms_norm_grouped_linear_qkv_slice = (try prepareRmsNormGroupedLinearQkvSliceRegion(cb, values, pattern, stats)) orelse return null,
        },
        .attention_output_residual => |pattern| .{
            .attention_output_residual = (try prepareAttentionOutputResidualRegion(graph, cb, values, pattern, stats)) orelse return null,
        },
        .rms_norm_gated_ffn_residual => |pattern| .{
            .rms_norm_gated_ffn_residual = (try prepareRmsNormGatedFfnResidualRegion(graph, cb, values, pattern, stats)) orelse return null,
        },
        .gated_ffn_residual => |pattern| .{
            .gated_ffn_residual = (try prepareGatedFfnResidualRegion(graph, cb, values, pattern, stats)) orelse return null,
        },
        .ple_residual => |pattern| .{
            .ple_residual = (try preparePleResidualRegion(cb, values, pattern, stats)) orelse return null,
        },
    };
    if (prepared_region) |prepared_ptr| prepared_ptr.* = prepared;
    return prepared;
}

fn tryExecutePlannedRuntimeRegion(
    region: RuntimeRegion,
    prepared_region: ?*PreparedRuntimeRegion,
    allocator: std.mem.Allocator,
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    value_device: []DeviceId,
    node_ids: []const NodeId,
    node_pos: usize,
    reachable: []const bool,
    device_id: DeviceId,
    exec_ctx: PartitionExecutor.ExecutionContext,
    exec_state: *interpreter.ExecState,
    skipped_nodes: []bool,
    last_use: []const u32,
    rt_map: std.AutoHashMapUnmanaged(NodeId, CT),
    donated: std.AutoHashMapUnmanaged(NodeId, void),
) !bool {
    _ = node_ids;
    _ = node_pos;
    _ = reachable;
    const prepared = (try prepareRuntimeRegion(graph, cb, values, value_device, device_id, region, prepared_region, exec_ctx.stats)) orelse {
        if (std.meta.activeTag(region) != .none) {
            if (exec_ctx.stats) |stats| stats.runtime_region_fallbacks += 1;
        }
        return false;
    };
    return switch (region) {
        .none => false,
        .raw_linear_dot => |pattern| executeRawLinearDotPattern(
            graph,
            cb,
            values,
            value_device,
            device_id,
            exec_ctx.stats,
            pattern,
            switch (prepared) {
                .raw_linear_dot => |slots| slots,
                else => return false,
            },
        ),
        .raw_linear_pair => |pattern| executeRawLinearPairPattern(
            graph,
            cb,
            values,
            value_device,
            device_id,
            exec_ctx.stats,
            pattern,
            switch (prepared) {
                .raw_linear_pair => |slots| slots,
                else => return false,
            },
            skipped_nodes,
        ),
        .raw_linear_bias => |pattern| executeRawLinearBiasPattern(
            graph,
            cb,
            values,
            value_device,
            device_id,
            exec_ctx.stats,
            pattern,
            switch (prepared) {
                .raw_linear_bias => |slots| slots,
                else => return false,
            },
            skipped_nodes,
        ),
        .raw_linear_bias_pair => |pattern| executeRawLinearBiasPairPattern(
            graph,
            cb,
            values,
            value_device,
            device_id,
            exec_ctx.stats,
            pattern,
            switch (prepared) {
                .raw_linear_bias_pair => |slots| slots,
                else => return false,
            },
            skipped_nodes,
        ),
        .lora_linear => |pattern| executeLoraLinearPattern(
            graph,
            cb,
            values,
            value_device,
            device_id,
            exec_ctx.stats,
            pattern,
        ),
        .lora_linear_qkv => |pattern| executeLoraLinearQkvPattern(
            graph,
            cb,
            values,
            value_device,
            device_id,
            exec_ctx.stats,
            pattern,
            skipped_nodes,
        ),
        .deberta_attention => |pattern| executeDebertaAttentionPattern(
            graph,
            cb,
            allocator,
            values,
            value_device,
            device_id,
            exec_ctx.stats,
            pattern,
            skipped_nodes,
        ),
        .deberta_ffn_forward => |pattern| executeDebertaFfnForwardPattern(
            graph,
            cb,
            values,
            value_device,
            device_id,
            exec_ctx.stats,
            pattern,
            switch (prepared) {
                .deberta_ffn_forward => |slots| slots,
                else => return false,
            },
            skipped_nodes,
        ),
        .deberta_encoder_lora_layer => |pattern| executeDebertaEncoderLoraLayerPattern(
            graph,
            cb,
            values,
            value_device,
            device_id,
            exec_ctx.stats,
            pattern,
            switch (prepared) {
                .deberta_encoder_lora_layer => |slots| slots,
                else => return false,
            },
            skipped_nodes,
        ),
        .lora_backward => |pattern| executeLoraBackwardPattern(
            graph,
            cb,
            values,
            value_device,
            device_id,
            exec_ctx.stats,
            pattern,
            skipped_nodes,
        ),
        .low_rank_lora_backward => |pattern| executeLowRankLoraBackwardPattern(
            graph,
            cb,
            values,
            value_device,
            device_id,
            exec_ctx.stats,
            pattern,
            skipped_nodes,
        ),
        .rank_adapter_backward => |pattern| executeRankAdapterBackwardPattern(
            graph,
            cb,
            values,
            value_device,
            device_id,
            exec_ctx.stats,
            pattern,
            skipped_nodes,
        ),
        .ffn_gelu_backward => |pattern| executeFfnGeluBackwardPattern(
            graph,
            cb,
            values,
            value_device,
            device_id,
            exec_ctx.stats,
            exec_ctx.partition_plan,
            pattern,
            skipped_nodes,
        ),
        .q_linear => |pattern| executeQLinearPattern(
            graph,
            cb,
            values,
            value_device,
            device_id,
            exec_ctx.stats,
            pattern,
            switch (prepared) {
                .q_linear => |slots| slots,
                else => return false,
            },
        ),
        .linear_qkv => |pattern| executeLinearNoBiasQkvPattern(
            allocator,
            graph,
            cb,
            values,
            value_device,
            device_id,
            exec_ctx,
            skipped_nodes,
            last_use,
            rt_map,
            donated,
            pattern,
            switch (prepared) {
                .linear_qkv => |slots| slots,
                else => return false,
            },
        ),
        .grouped_linear_qkv_slice => |pattern| executeGroupedLinearQkvSlicePattern(
            allocator,
            graph,
            cb,
            values,
            value_device,
            device_id,
            exec_ctx,
            skipped_nodes,
            last_use,
            rt_map,
            donated,
            pattern,
            switch (prepared) {
                .grouped_linear_qkv_slice => |slots| slots,
                else => return false,
            },
        ),
        .packed_linear_qkv_slice => |pattern| executePackedLinearQkvSlicePattern(
            allocator,
            graph,
            cb,
            values,
            value_device,
            device_id,
            exec_ctx,
            skipped_nodes,
            last_use,
            rt_map,
            donated,
            pattern,
            switch (prepared) {
                .packed_linear_qkv_slice => |slots| slots,
                else => return false,
            },
        ),
        .rms_norm_grouped_linear_qkv_slice => |pattern| executeRmsNormGroupedLinearQkvSlicePattern(
            allocator,
            graph,
            cb,
            values,
            value_device,
            device_id,
            exec_ctx,
            skipped_nodes,
            last_use,
            rt_map,
            donated,
            pattern,
            switch (prepared) {
                .rms_norm_grouped_linear_qkv_slice => |slots| slots,
                else => return false,
            },
        ),
        .attention_output_residual => |pattern| executeAttentionOutputResidualPattern(
            graph,
            cb,
            values,
            value_device,
            device_id,
            exec_ctx,
            exec_state,
            skipped_nodes,
            pattern,
            switch (prepared) {
                .attention_output_residual => |slots| slots,
                else => return false,
            },
        ),
        .rms_norm_gated_ffn_residual => |pattern| executeRmsNormGatedFfnResidualPattern(
            graph,
            cb,
            values,
            value_device,
            device_id,
            skipped_nodes,
            exec_ctx.stats,
            pattern,
            switch (prepared) {
                .rms_norm_gated_ffn_residual => |slots| slots,
                else => return false,
            },
        ),
        .gated_ffn_residual => |pattern| executeMatchedGatedFfnResidualPattern(
            graph,
            cb,
            values,
            value_device,
            device_id,
            skipped_nodes,
            exec_ctx.stats,
            pattern,
            switch (prepared) {
                .gated_ffn_residual => |slots| slots,
                else => return false,
            },
        ),
        .ple_residual => |pattern| executePleResidualPattern(
            graph,
            cb,
            values,
            value_device,
            device_id,
            skipped_nodes,
            exec_ctx.stats,
            pattern,
            switch (prepared) {
                .ple_residual => |slots| slots,
                else => return false,
            },
        ),
    } catch |err| switch (err) {
        error.UnsupportedOperation,
        error.UnsupportedPrimitiveOp,
        error.UnsupportedShape,
        error.ShapeMismatch,
        error.UnsupportedTensorType,
        => false,
        else => return err,
    };
}

const AttentionOutputResidualPattern = struct {
    attention_id: NodeId,
    pre_linear_norm_id: ?NodeId,
    linear_id: NodeId,
    post_linear_norm_id: ?NodeId,
    add_id: NodeId,
    residual_id: NodeId,
    rows: usize,
    attention_input_size: usize,
    hidden_size: usize,
    eps: f32,

    fn elidedNodeCount(self: AttentionOutputResidualPattern) u64 {
        return 2 + @as(u64, if (self.pre_linear_norm_id != null) 1 else 0) +
            @as(u64, if (self.post_linear_norm_id != null) 1 else 0);
    }
};

fn tryExecuteAttentionOutputResidualPattern(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    value_device: []DeviceId,
    node_ids: []const NodeId,
    node_pos: usize,
    reachable: []const bool,
    device_id: DeviceId,
    exec_ctx: PartitionExecutor.ExecutionContext,
    exec_state: *interpreter.ExecState,
    skipped_nodes: []bool,
    last_use: []const u32,
) !bool {
    if (!attentionOutputResidualGraphFusionEnabled()) return false;
    const pattern = matchAttentionOutputResidualPattern(graph, node_ids, node_pos, reachable, skipped_nodes, last_use) orelse return false;
    const prepared = (try prepareAttentionOutputResidualRegion(graph, cb, values, pattern, exec_ctx.stats)) orelse return false;
    return executeAttentionOutputResidualPattern(
        graph,
        cb,
        values,
        value_device,
        device_id,
        exec_ctx,
        exec_state,
        skipped_nodes,
        pattern,
        prepared,
    );
}

fn executeAttentionOutputResidualPattern(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    value_device: []DeviceId,
    device_id: DeviceId,
    exec_ctx: PartitionExecutor.ExecutionContext,
    exec_state: *interpreter.ExecState,
    skipped_nodes: []bool,
    pattern: AttentionOutputResidualPattern,
    prepared: PreparedAttentionOutputResidualRegion,
) !bool {
    const attention_node = graph.node(pattern.attention_id);
    const attention_inputs = attention_node.getInputs();
    const attention_attrs = switch (attention_node.op) {
        .fused_gqa_causal_attention => |attrs| attrs,
        else => return false,
    };

    const residual = valueFor(values, pattern.residual_id) orelse return false;
    const attention_output = (try executeRuntimeGqaCausalAttention(
        cb,
        values,
        attention_inputs,
        attention_attrs,
        attention_node.num_inputs,
        exec_state,
    )) orelse return false;
    errdefer cb.free(attention_output);

    const planned_scope = try metal_compute_mod.MetalCompute.beginPlannedGraphScope(cb, .attention_project);
    defer metal_compute_mod.MetalCompute.endPlannedGraphScope(cb, planned_scope) catch {};
    const fused = cb.runAttentionOutputResidual(&.{
        .attention_output = attention_output,
        .residual = residual,
        .rows = pattern.rows,
        .attention_input_size = pattern.attention_input_size,
        .hidden_size = pattern.hidden_size,
        .linear_slot = prepared.linear_slot,
        .pre_linear_rms_norm_slot = prepared.pre_linear_rms_norm_slot,
        .post_linear_rms_norm_slot = prepared.post_linear_rms_norm_slot,
        .eps = pattern.eps,
    }) catch |err| switch (err) {
        error.UnsupportedOperation, error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch, error.UnsupportedTensorType => null,
        else => return err,
    };

    if (fused) |output| {
        if (output != attention_output) cb.free(attention_output);
        values[@intCast(pattern.attention_id)] = output;
        values[@intCast(pattern.add_id)] = output;
        value_device[@intCast(pattern.attention_id)] = device_id;
        value_device[@intCast(pattern.add_id)] = device_id;
        skipped_nodes[@intCast(pattern.attention_id)] = true;
        if (pattern.pre_linear_norm_id) |norm_id| skipped_nodes[@intCast(norm_id)] = true;
        skipped_nodes[@intCast(pattern.linear_id)] = true;
        if (pattern.post_linear_norm_id) |norm_id| skipped_nodes[@intCast(norm_id)] = true;
        skipped_nodes[@intCast(pattern.add_id)] = true;
        if (exec_ctx.stats) |stats| {
            recordMetalGraphRegion(stats, .attention, pattern.elidedNodeCount());
            stats.fused_graph_pattern_dispatches += 1;
            stats.fused_graph_nodes_elided += pattern.elidedNodeCount();
            stats.metal_attention_output_residual_fusions += 1;
        }
        if (traceMetalGraphFusionsEnabled()) {
            std.debug.print(
                "metal_graph_fusion_trace: attention_output_residual executed attention={d} linear={d} post_norm={?d} add={d} rows={d} attention_dim={d} hidden={d}\n",
                .{ pattern.attention_id, pattern.linear_id, pattern.post_linear_norm_id, pattern.add_id, pattern.rows, pattern.attention_input_size, pattern.hidden_size },
            );
        }
        return true;
    }

    values[@intCast(pattern.attention_id)] = attention_output;
    value_device[@intCast(pattern.attention_id)] = device_id;
    if (exec_ctx.stats) |stats| {
        stats.graph_region_fallbacks += 1;
        stats.metal_attention_output_residual_partial_fallbacks += 1;
    }
    return true;
}

fn matchAttentionOutputResidualPattern(
    graph: *const Graph,
    node_ids: []const NodeId,
    node_pos: usize,
    reachable: []const bool,
    skipped_nodes: []const bool,
    last_use: []const u32,
) ?AttentionOutputResidualPattern {
    const attention_id = node_ids[node_pos];
    const attention = graph.node(attention_id);
    const attention_attrs = switch (attention.op) {
        .fused_gqa_causal_attention => |attrs| attrs,
        else => return null,
    };
    if (attention_attrs.num_heads == 0 or attention_attrs.head_dim == 0) return null;
    const attention_input_size = @as(usize, attention_attrs.num_heads) * @as(usize, attention_attrs.head_dim);

    var linear_input_id = attention_id;
    var pre_linear_norm_id: ?NodeId = null;
    var eps: f32 = 0.0;
    if (findSingleInputNode(graph, node_ids, node_pos + 1, reachable, skipped_nodes, attention_id, &isMatchingPostDownRmsNorm)) |norm_id| {
        const norm = graph.node(norm_id);
        const norm_attrs = switch (norm.op) {
            .fused_rms_norm => |attrs| attrs,
            else => return null,
        };
        if (norm_attrs.dim == attention_input_size) {
            pre_linear_norm_id = norm_id;
            linear_input_id = norm_id;
            eps = norm_attrs.eps;
        }
    }

    const linear_id = findSingleInputNode(graph, node_ids, node_pos + 1, reachable, skipped_nodes, linear_input_id, &isPlainLinearNoBiasNode) orelse return null;
    const linear = graph.node(linear_id);
    const linear_attrs = switch (linear.op) {
        .fused_linear_no_bias => |attrs| attrs,
        else => return null,
    };
    if (linear_attrs.in_dim != attention_input_size or linear_attrs.out_dim == 0 or linear_attrs.rows == 0) return null;

    var add_lhs_id = linear_id;
    var post_linear_norm_id: ?NodeId = null;
    if (findSingleInputNode(graph, node_ids, node_pos + 1, reachable, skipped_nodes, linear_id, &isMatchingPostDownRmsNorm)) |norm_id| {
        const norm = graph.node(norm_id);
        const norm_attrs = switch (norm.op) {
            .fused_rms_norm => |attrs| attrs,
            else => return null,
        };
        if (norm_attrs.dim == linear_attrs.out_dim) {
            if (pre_linear_norm_id != null and norm_attrs.eps != eps) return null;
            post_linear_norm_id = norm_id;
            add_lhs_id = norm_id;
            eps = norm_attrs.eps;
        }
    }

    const add_id = findSingleInputNodeAsBinaryLhs(graph, node_ids, node_pos + 1, reachable, skipped_nodes, add_lhs_id, &isAddNode) orelse return null;
    const add_inputs = graph.node(add_id).getInputs();
    if (add_inputs.len < 2) return null;
    const residual_id = if (add_inputs[0] == add_lhs_id) add_inputs[1] else add_inputs[0];
    if (residual_id == null_node) return null;

    if (pre_linear_norm_id) |norm_id| {
        if (!hasOnlyExpectedUses(graph, reachable, skipped_nodes, attention_id, &.{norm_id})) return null;
        if (!hasOnlyExpectedUses(graph, reachable, skipped_nodes, norm_id, &.{linear_id})) return null;
        if (!nodeLastUseIs(last_use, attention_id, norm_id)) return null;
        if (!nodeLastUseIs(last_use, norm_id, linear_id)) return null;
    } else {
        if (!hasOnlyExpectedUses(graph, reachable, skipped_nodes, attention_id, &.{linear_id})) return null;
        if (!nodeLastUseIs(last_use, attention_id, linear_id)) return null;
    }
    if (post_linear_norm_id) |norm_id| {
        if (!hasOnlyExpectedUses(graph, reachable, skipped_nodes, linear_id, &.{norm_id})) return null;
        if (!hasOnlyExpectedUses(graph, reachable, skipped_nodes, norm_id, &.{add_id})) return null;
        if (!nodeLastUseIs(last_use, linear_id, norm_id)) return null;
        if (!nodeLastUseIs(last_use, norm_id, add_id)) return null;
    } else {
        if (!hasOnlyExpectedUses(graph, reachable, skipped_nodes, linear_id, &.{add_id})) return null;
        if (!nodeLastUseIs(last_use, linear_id, add_id)) return null;
    }

    return .{
        .attention_id = attention_id,
        .pre_linear_norm_id = pre_linear_norm_id,
        .linear_id = linear_id,
        .post_linear_norm_id = post_linear_norm_id,
        .add_id = add_id,
        .residual_id = residual_id,
        .rows = linear_attrs.rows,
        .attention_input_size = attention_input_size,
        .hidden_size = linear_attrs.out_dim,
        .eps = eps,
    };
}

const GatedFfnResidualPattern = struct {
    pair_id: NodeId,
    pair_second_id: NodeId,
    activation_id: NodeId,
    multiply_id: NodeId,
    down_id: NodeId,
    post_down_norm_id: ?NodeId,
    add_id: NodeId,
    residual_id: NodeId,
    activation: ops_mod.DecoderRuntimeActivationKind,
    hidden_size: usize,
    intermediate_size: usize,
    rows: usize,
    eps: f32,

    fn elidedNodeCount(self: GatedFfnResidualPattern) u64 {
        return 6 + @as(u64, if (self.post_down_norm_id != null) 1 else 0);
    }
};

const RmsNormGatedFfnResidualPattern = struct {
    norm_id: NodeId,
    norm_input_id: NodeId,
    norm_weight_id: NodeId,
    norm_dim: usize,
    norm_eps: f32,
    ffn: GatedFfnResidualPattern,

    fn elidedNodeCount(self: RmsNormGatedFfnResidualPattern) u64 {
        return 1 + self.ffn.elidedNodeCount();
    }
};

fn tryExecuteRmsNormGatedFfnResidualPattern(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    value_device: []DeviceId,
    node_ids: []const NodeId,
    node_pos: usize,
    reachable: []const bool,
    device_id: DeviceId,
    skipped_nodes: []bool,
    last_use: []const u32,
    stats: ?*PartitionExecutor.ExecutionStats,
) !bool {
    if (!gatedFfnGraphFusionEnabled()) return false;
    const pattern = matchRmsNormGatedFfnResidualPattern(graph, node_ids, node_pos, reachable, skipped_nodes, last_use) orelse return false;
    const prepared = (try prepareRmsNormGatedFfnResidualRegion(graph, cb, values, pattern, stats)) orelse return false;
    return executeRmsNormGatedFfnResidualPattern(
        graph,
        cb,
        values,
        value_device,
        device_id,
        skipped_nodes,
        stats,
        pattern,
        prepared,
    );
}

fn executeRmsNormGatedFfnResidualPattern(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    value_device: []DeviceId,
    device_id: DeviceId,
    skipped_nodes: []bool,
    stats: ?*PartitionExecutor.ExecutionStats,
    pattern: RmsNormGatedFfnResidualPattern,
    prepared: PreparedRmsNormGatedFfnResidualRegion,
) !bool {
    const input = valueFor(values, pattern.norm_input_id) orelse return traceGatedFfnDeclined("missing_rms_input", pattern.norm_input_id);
    const normed = cb.decoderRuntimeApplyRmsNorm(&.{
        .slot = prepared.norm_slot,
        .input = input,
        .hidden_size = pattern.norm_dim,
        .eps = pattern.norm_eps,
    }) catch |err| switch (err) {
        error.UnsupportedOperation, error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => return traceGatedFfnDeclined("rms_norm_unavailable", pattern.norm_id),
        else => return err,
    } orelse return traceGatedFfnDeclined("rms_norm_unavailable", pattern.norm_id);
    errdefer cb.free(normed);

    const output = try executeGatedFfnResidualPattern(
        graph,
        cb,
        values,
        value_device,
        device_id,
        skipped_nodes,
        stats,
        pattern.ffn,
        normed,
        pattern.norm_id,
        false,
        1,
        prepared.ffn,
    );
    // `normed` is a pure temporary never published into `values`; the errdefer above
    // only fires on error unwind, so free it explicitly on the non-error decline path
    // (executeGatedFfnResidualPattern returns null) as well as on success.
    if (output == null) {
        cb.free(normed);
        return false;
    }
    cb.free(normed);
    return true;
}

fn matchRmsNormGatedFfnResidualPattern(
    graph: *const Graph,
    node_ids: []const NodeId,
    node_pos: usize,
    reachable: []const bool,
    skipped_nodes: []const bool,
    last_use: []const u32,
) ?RmsNormGatedFfnResidualPattern {
    const norm_id = node_ids[node_pos];
    const norm = graph.node(norm_id);
    const norm_attrs = switch (norm.op) {
        .fused_rms_norm => |attrs| attrs,
        else => return null,
    };
    if (norm_attrs.dim == 0) return null;
    const norm_inputs = norm.getInputs();
    if (norm_inputs.len < 2) return null;
    const pair_id = findSingleInputNode(graph, node_ids, node_pos + 1, reachable, skipped_nodes, norm_id, &isLinearNoBiasPairNode) orelse return null;
    if (!hasOnlyExpectedUses(graph, reachable, skipped_nodes, norm_id, &.{pair_id})) return null;
    const pair_pos = findNodePos(node_ids, pair_id) orelse return null;
    const ffn = matchGatedFfnResidualPattern(graph, node_ids, pair_pos, reachable, skipped_nodes, last_use) orelse return null;

    return .{
        .norm_id = norm_id,
        .norm_input_id = norm_inputs[0],
        .norm_weight_id = norm_inputs[1],
        .norm_dim = norm_attrs.dim,
        .norm_eps = norm_attrs.eps,
        .ffn = ffn,
    };
}

fn tryExecuteGatedFfnResidualPattern(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    value_device: []DeviceId,
    node_ids: []const NodeId,
    node_pos: usize,
    reachable: []const bool,
    device_id: DeviceId,
    skipped_nodes: []bool,
    last_use: []const u32,
    stats: ?*PartitionExecutor.ExecutionStats,
) !bool {
    if (!gatedFfnGraphFusionEnabled()) return false;
    const pattern = matchGatedFfnResidualPattern(graph, node_ids, node_pos, reachable, skipped_nodes, last_use) orelse {
        if (traceMetalGraphFusionsEnabled()) traceGatedFfnResidualCandidate(graph, node_ids, node_pos, reachable, skipped_nodes, last_use);
        return false;
    };
    const prepared = (try prepareGatedFfnResidualRegion(graph, cb, values, pattern, stats)) orelse return false;
    return executeMatchedGatedFfnResidualPattern(
        graph,
        cb,
        values,
        value_device,
        device_id,
        skipped_nodes,
        stats,
        pattern,
        prepared,
    );
}

fn executeMatchedGatedFfnResidualPattern(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    value_device: []DeviceId,
    device_id: DeviceId,
    skipped_nodes: []bool,
    stats: ?*PartitionExecutor.ExecutionStats,
    pattern: GatedFfnResidualPattern,
    prepared: PreparedGatedFfnResidualRegion,
) !bool {
    if (traceMetalGraphFusionsEnabled()) {
        std.debug.print(
            "metal_graph_fusion_trace: gated_ffn match pair={d} pair_second={d} activation={d} multiply={d} down={d} post_norm={?d} add={d} residual={d} rows={d} hidden={d} intermediate={d} activation_kind={s}\n",
            .{
                pattern.pair_id,
                pattern.pair_second_id,
                pattern.activation_id,
                pattern.multiply_id,
                pattern.down_id,
                pattern.post_down_norm_id,
                pattern.add_id,
                pattern.residual_id,
                pattern.rows,
                pattern.hidden_size,
                pattern.intermediate_size,
                @tagName(pattern.activation),
            },
        );
    }
    const pair = graph.node(pattern.pair_id);
    const pair_inputs = pair.getInputs();
    if (pair_inputs.len < 3) return traceGatedFfnDeclined("short_pair_inputs", pattern.pair_id);
    const input = valueFor(values, pair_inputs[0]) orelse return traceGatedFfnDeclined("missing_input", pair_inputs[0]);
    return (try executeGatedFfnResidualPattern(
        graph,
        cb,
        values,
        value_device,
        device_id,
        skipped_nodes,
        stats,
        pattern,
        input,
        pattern.pair_id,
        true,
        0,
        prepared,
    )) != null;
}

fn executeGatedFfnResidualPattern(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    value_device: []DeviceId,
    device_id: DeviceId,
    skipped_nodes: []bool,
    stats: ?*PartitionExecutor.ExecutionStats,
    pattern: GatedFfnResidualPattern,
    input: CT,
    current_output_id: NodeId,
    publish_pair_output: bool,
    extra_elided_nodes: u64,
    prepared: PreparedGatedFfnResidualRegion,
) !?CT {
    const pair = graph.node(pattern.pair_id);
    const pair_inputs = pair.getInputs();
    if (pair_inputs.len < 3) return traceGatedFfnDeclinedNull("short_pair_inputs", pattern.pair_id);
    const residual = valueFor(values, pattern.residual_id) orelse return traceGatedFfnDeclinedNull("missing_residual", pattern.residual_id);

    var post_down_weight: ?CT = null;
    const post_down_slot = if (pattern.post_down_norm_id) |norm_id| blk: {
        const norm = graph.node(norm_id);
        const norm_inputs = norm.getInputs();
        if (norm_inputs.len < 2) return traceGatedFfnDeclinedNull("short_norm_inputs", norm_id);
        const norm_weight = valueFor(values, norm_inputs[1]) orelse return traceGatedFfnDeclinedNull("missing_norm_weight", norm_inputs[1]);
        post_down_weight = norm_weight;
        break :blk prepared.post_down_rms_norm_slot orelse return traceGatedFfnDeclinedNull("norm_slot_unavailable", norm_id);
    } else null;

    const planned_scope = try metal_compute_mod.MetalCompute.beginPlannedGraphScope(cb, .ffn);
    defer metal_compute_mod.MetalCompute.endPlannedGraphScope(cb, planned_scope) catch {};
    const output = (try cb.runGatedFfnResidual(&.{
        .gate_linear_slot = prepared.gate_slot,
        .up_linear_slot = prepared.up_slot,
        .down_linear_slot = prepared.down_slot,
        .input = input,
        .residual = residual,
        .post_down_rms_norm_slot = post_down_slot,
        .post_down_rms_norm_weight = post_down_weight,
        .hidden_size = pattern.hidden_size,
        .intermediate_size = pattern.intermediate_size,
        .eps = pattern.eps,
        .activation = pattern.activation,
    })) orelse return traceGatedFfnDeclinedNull("backend_returned_null", pattern.pair_id);

    values[@intCast(current_output_id)] = output;
    if (publish_pair_output) values[@intCast(pattern.pair_id)] = output;
    values[@intCast(pattern.add_id)] = output;
    value_device[@intCast(current_output_id)] = device_id;
    if (publish_pair_output) value_device[@intCast(pattern.pair_id)] = device_id;
    value_device[@intCast(pattern.add_id)] = device_id;

    skipped_nodes[@intCast(current_output_id)] = true;
    skipped_nodes[@intCast(pattern.pair_id)] = true;
    skipped_nodes[@intCast(pattern.pair_second_id)] = true;
    skipped_nodes[@intCast(pattern.activation_id)] = true;
    skipped_nodes[@intCast(pattern.multiply_id)] = true;
    skipped_nodes[@intCast(pattern.down_id)] = true;
    if (pattern.post_down_norm_id) |norm_id| skipped_nodes[@intCast(norm_id)] = true;
    skipped_nodes[@intCast(pattern.add_id)] = true;
    if (stats) |s| {
        recordMetalGraphRegion(s, .ffn, pattern.elidedNodeCount() + extra_elided_nodes);
        s.fused_graph_pattern_dispatches += 1;
        s.fused_graph_nodes_elided += pattern.elidedNodeCount() + extra_elided_nodes;
        s.metal_gated_ffn_residual_fusions += 1;
        if (extra_elided_nodes != 0) s.gemma_rms_norm_hits += 1;
    }
    if (traceMetalGraphFusionsEnabled()) {
        std.debug.print(
            "metal_graph_fusion_trace: gated_ffn executed pair={d} add={d}\n",
            .{ pattern.pair_id, pattern.add_id },
        );
    }
    return output;
}

const PleResidualPattern = struct {
    gate_id: NodeId,
    activation_id: NodeId,
    multiply_id: NodeId,
    projection_id: NodeId,
    post_norm_id: NodeId,
    add_id: NodeId,
    hidden_id: NodeId,
    ple_id: NodeId,
    gate_weight_id: NodeId,
    projection_weight_id: NodeId,
    post_norm_weight_id: NodeId,
    rows: usize,
    hidden_size: usize,
    ple_hidden_size: usize,
    eps: f32,
    activation: ops_mod.DecoderRuntimeActivationKind,

    fn elidedNodeCount(_: PleResidualPattern) u64 {
        return 5;
    }
};

fn tryExecutePleResidualPattern(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    value_device: []DeviceId,
    node_ids: []const NodeId,
    node_pos: usize,
    reachable: []const bool,
    device_id: DeviceId,
    skipped_nodes: []bool,
    stats: ?*PartitionExecutor.ExecutionStats,
) !bool {
    const pattern = matchPleResidualPattern(graph, node_ids, node_pos, reachable, skipped_nodes) orelse return false;
    const prepared = (try preparePleResidualRegion(cb, values, pattern, stats)) orelse return false;
    return executePleResidualPattern(
        graph,
        cb,
        values,
        value_device,
        device_id,
        skipped_nodes,
        stats,
        pattern,
        prepared,
    );
}

fn executePleResidualPattern(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    value_device: []DeviceId,
    device_id: DeviceId,
    skipped_nodes: []bool,
    stats: ?*PartitionExecutor.ExecutionStats,
    pattern: PleResidualPattern,
    prepared: PreparedPleResidualRegion,
) !bool {
    _ = graph;
    const hidden = valueFor(values, pattern.hidden_id) orelse return false;
    const ple = valueFor(values, pattern.ple_id) orelse return false;

    const planned_scope = try metal_compute_mod.MetalCompute.beginPlannedGraphScope(cb, .ple);
    defer metal_compute_mod.MetalCompute.endPlannedGraphScope(cb, planned_scope) catch {};
    const output = (try runMetalPleResidual(
        cb,
        hidden,
        ple,
        prepared.gate_slot,
        prepared.projection_slot,
        prepared.post_norm_slot,
        pattern.hidden_size,
        pattern.ple_hidden_size,
        pattern.eps,
        pattern.activation,
    )) orelse return tracePleDeclined("backend_unavailable", pattern.gate_id);

    values[@intCast(pattern.gate_id)] = output;
    values[@intCast(pattern.add_id)] = output;
    value_device[@intCast(pattern.gate_id)] = device_id;
    value_device[@intCast(pattern.add_id)] = device_id;
    skipped_nodes[@intCast(pattern.gate_id)] = true;
    skipped_nodes[@intCast(pattern.activation_id)] = true;
    skipped_nodes[@intCast(pattern.multiply_id)] = true;
    skipped_nodes[@intCast(pattern.projection_id)] = true;
    skipped_nodes[@intCast(pattern.post_norm_id)] = true;
    skipped_nodes[@intCast(pattern.add_id)] = true;
    if (stats) |s| {
        recordMetalGraphRegion(s, .ple, pattern.elidedNodeCount());
        s.fused_graph_pattern_dispatches += 1;
        s.fused_graph_nodes_elided += pattern.elidedNodeCount();
    }
    if (traceMetalGraphFusionsEnabled()) {
        std.debug.print(
            "metal_graph_fusion_trace: ple_residual executed gate={d} activation={d} multiply={d} projection={d} post_norm={d} add={d} rows={d} hidden={d} ple_hidden={d}\n",
            .{ pattern.gate_id, pattern.activation_id, pattern.multiply_id, pattern.projection_id, pattern.post_norm_id, pattern.add_id, pattern.rows, pattern.hidden_size, pattern.ple_hidden_size },
        );
    }
    return true;
}

fn runMetalPleResidual(
    cb: *const ComputeBackend,
    hidden: CT,
    ple: CT,
    gate_linear_slot: usize,
    projection_linear_slot: usize,
    post_norm_slot: usize,
    hidden_size: usize,
    ple_hidden_size: usize,
    eps: f32,
    activation: ops_mod.DecoderRuntimeActivationKind,
) !?CT {
    if (cb.kind() != .metal) return null;
    if (comptime !build_options.enable_metal) return null;
    return metal_compute_mod.MetalCompute.applyPleResidual(
        cb,
        hidden,
        ple,
        gate_linear_slot,
        projection_linear_slot,
        post_norm_slot,
        hidden_size,
        ple_hidden_size,
        eps,
        activation,
    );
}

fn matchPleResidualPattern(
    graph: *const Graph,
    node_ids: []const NodeId,
    node_pos: usize,
    reachable: []const bool,
    skipped_nodes: []const bool,
) ?PleResidualPattern {
    const gate_id = node_ids[node_pos];
    const gate = graph.node(gate_id);
    const gate_attrs = switch (gate.op) {
        .fused_linear_no_bias => |attrs| attrs,
        else => return null,
    };
    if (gate_attrs.num_projections != 0 or gate_attrs.rows == 0 or gate_attrs.in_dim == 0 or gate_attrs.out_dim == 0) return tracePleDeclinedNull("unsupported_gate_attrs", gate_id);
    const gate_inputs = gate.getInputs();
    if (gate_inputs.len < 2) return tracePleDeclinedNull("short_gate_inputs", gate_id);
    const hidden_id = gate_inputs[0];
    const gate_weight_id = gate_inputs[1];

    const activation_id = findSingleInputNode(graph, node_ids, node_pos + 1, reachable, skipped_nodes, gate_id, &isSupportedGatedFfnActivation) orelse return tracePleDeclinedNull("missing_activation", gate_id);
    if (!hasOnlyExpectedUses(graph, reachable, skipped_nodes, gate_id, &.{activation_id})) return tracePleDeclinedNull("escaped_gate", gate_id);
    const activation_pos = findNodePos(node_ids, activation_id) orelse return tracePleDeclinedNull("activation_not_in_partition", activation_id);
    const activation = activationKindForGraphNode(graph.node(activation_id)) orelse return tracePleDeclinedNull("unsupported_activation", activation_id);

    const multiply_id = findSingleInputNodeAsBinaryLhs(graph, node_ids, activation_pos + 1, reachable, skipped_nodes, activation_id, &isMultiplyNode) orelse return tracePleDeclinedNull("missing_multiply", activation_id);
    if (!hasOnlyExpectedUses(graph, reachable, skipped_nodes, activation_id, &.{multiply_id})) return tracePleDeclinedNull("escaped_activation", activation_id);
    const multiply = graph.node(multiply_id);
    const multiply_inputs = multiply.getInputs();
    if (multiply_inputs.len < 2) return tracePleDeclinedNull("short_multiply_inputs", multiply_id);
    const ple_id = if (multiply_inputs[0] == activation_id) multiply_inputs[1] else multiply_inputs[0];
    if (ple_id == null_node or ple_id == hidden_id) return tracePleDeclinedNull("invalid_ple_input", multiply_id);

    const multiply_pos = findNodePos(node_ids, multiply_id) orelse return tracePleDeclinedNull("multiply_not_in_partition", multiply_id);
    const projection_id = findSingleInputNode(graph, node_ids, multiply_pos + 1, reachable, skipped_nodes, multiply_id, &isPlainLinearNoBiasNode) orelse return tracePleDeclinedNull("missing_projection", multiply_id);
    if (!hasOnlyExpectedUses(graph, reachable, skipped_nodes, multiply_id, &.{projection_id})) return tracePleDeclinedNull("escaped_multiply", multiply_id);
    const projection = graph.node(projection_id);
    const projection_attrs = switch (projection.op) {
        .fused_linear_no_bias => |attrs| attrs,
        else => return tracePleDeclinedNull("projection_not_linear", projection_id),
    };
    if (projection_attrs.rows != gate_attrs.rows or
        projection_attrs.in_dim != gate_attrs.out_dim or
        projection_attrs.out_dim != gate_attrs.in_dim)
    {
        return tracePleDeclinedNull("projection_shape_mismatch", projection_id);
    }
    const projection_inputs = projection.getInputs();
    if (projection_inputs.len < 2) return tracePleDeclinedNull("short_projection_inputs", projection_id);
    const projection_weight_id = projection_inputs[1];

    const projection_pos = findNodePos(node_ids, projection_id) orelse return tracePleDeclinedNull("projection_not_in_partition", projection_id);
    const post_norm_id = findSingleInputNode(graph, node_ids, projection_pos + 1, reachable, skipped_nodes, projection_id, &isMatchingPostDownRmsNorm) orelse return tracePleDeclinedNull("missing_post_norm", projection_id);
    if (!hasOnlyExpectedUses(graph, reachable, skipped_nodes, projection_id, &.{post_norm_id})) return tracePleDeclinedNull("escaped_projection", projection_id);
    const post_norm = graph.node(post_norm_id);
    const post_norm_attrs = switch (post_norm.op) {
        .fused_rms_norm => |attrs| attrs,
        else => return tracePleDeclinedNull("post_norm_not_rms", post_norm_id),
    };
    if (post_norm_attrs.dim != gate_attrs.in_dim) return tracePleDeclinedNull("post_norm_dim_mismatch", post_norm_id);
    const post_norm_inputs = post_norm.getInputs();
    if (post_norm_inputs.len < 2) return tracePleDeclinedNull("short_post_norm_inputs", post_norm_id);
    const post_norm_weight_id = post_norm_inputs[1];

    const post_norm_pos = findNodePos(node_ids, post_norm_id) orelse return tracePleDeclinedNull("post_norm_not_in_partition", post_norm_id);
    const add_id = findBinaryInputNode(graph, node_ids, post_norm_pos + 1, reachable, skipped_nodes, hidden_id, post_norm_id, &isAddNode) orelse return tracePleDeclinedNull("missing_add", post_norm_id);
    if (!hasOnlyExpectedUses(graph, reachable, skipped_nodes, post_norm_id, &.{add_id})) return tracePleDeclinedNull("escaped_post_norm", post_norm_id);

    return .{
        .gate_id = gate_id,
        .activation_id = activation_id,
        .multiply_id = multiply_id,
        .projection_id = projection_id,
        .post_norm_id = post_norm_id,
        .add_id = add_id,
        .hidden_id = hidden_id,
        .ple_id = ple_id,
        .gate_weight_id = gate_weight_id,
        .projection_weight_id = projection_weight_id,
        .post_norm_weight_id = post_norm_weight_id,
        .rows = gate_attrs.rows,
        .hidden_size = gate_attrs.in_dim,
        .ple_hidden_size = gate_attrs.out_dim,
        .eps = post_norm_attrs.eps,
        .activation = activation,
    };
}

fn tracePleDeclinedNull(reason: []const u8, node_id: NodeId) ?PleResidualPattern {
    _ = tracePleDeclined(reason, node_id);
    return null;
}

fn tracePleDeclined(reason: []const u8, node_id: NodeId) bool {
    if (traceMetalGraphFusionsEnabled()) {
        std.debug.print(
            "metal_graph_fusion_trace: ple_residual declined reason={s} node={d}\n",
            .{ reason, node_id },
        );
    }
    return false;
}

fn traceGatedFfnDeclined(reason: []const u8, node_id: NodeId) bool {
    if (traceMetalGraphFusionsEnabled()) {
        std.debug.print(
            "metal_graph_fusion_trace: gated_ffn declined reason={s} node={d}\n",
            .{ reason, node_id },
        );
    }
    return false;
}

fn traceGatedFfnDeclinedNull(reason: []const u8, node_id: NodeId) ?CT {
    _ = traceGatedFfnDeclined(reason, node_id);
    return null;
}

fn matchGatedFfnResidualPattern(
    graph: *const Graph,
    node_ids: []const NodeId,
    node_pos: usize,
    reachable: []const bool,
    skipped_nodes: []const bool,
    last_use: []const u32,
) ?GatedFfnResidualPattern {
    const pair_id = node_ids[node_pos];
    const pair = graph.node(pair_id);
    const pair_attrs = switch (pair.op) {
        .fused_linear_no_bias_pair => |attrs| attrs,
        else => return null,
    };
    const pair_inputs = pair.getInputs();
    if (pair_inputs.len < 3) return null;
    if (pair_attrs.rows == 0 or pair_attrs.in_dim == 0 or pair_attrs.out_dim == 0) return null;

    const norm = graph.node(pair_inputs[0]);
    const residual_id = switch (norm.op) {
        .fused_rms_norm => blk: {
            const norm_inputs = norm.getInputs();
            if (norm_inputs.len < 1) return null;
            break :blk norm_inputs[0];
        },
        else => return null,
    };

    const activation_id = findSingleInputNode(graph, node_ids, node_pos + 1, reachable, skipped_nodes, pair_id, &isSupportedGatedFfnActivation) orelse return null;
    const activation = activationKindForGraphNode(graph.node(activation_id)) orelse return null;
    const pair_second_id = findSingleInputNode(graph, node_ids, node_pos + 1, reachable, skipped_nodes, pair_id, &isPairSecondMarker) orelse return null;
    const multiply_id = findBinaryInputNode(graph, node_ids, node_pos + 1, reachable, skipped_nodes, activation_id, pair_second_id, &isMultiplyNode) orelse return null;
    const down_id = findSingleInputNode(graph, node_ids, node_pos + 1, reachable, skipped_nodes, multiply_id, &isPlainLinearNoBiasNode) orelse return null;
    const down = graph.node(down_id);
    const down_attrs = switch (down.op) {
        .fused_linear_no_bias => |attrs| attrs,
        else => return null,
    };
    if (down_attrs.num_projections != 0 or
        down_attrs.rows != pair_attrs.rows or
        down_attrs.in_dim != pair_attrs.out_dim or
        down_attrs.out_dim != pair_attrs.in_dim)
    {
        return null;
    }

    var post_down_norm_id: ?NodeId = null;
    var add_lhs_id = down_id;
    var eps: f32 = 0.0;
    if (findSingleInputNode(graph, node_ids, node_pos + 1, reachable, skipped_nodes, down_id, &isMatchingPostDownRmsNorm)) |norm_id| {
        const post_norm = graph.node(norm_id);
        const norm_attrs = switch (post_norm.op) {
            .fused_rms_norm => |attrs| attrs,
            else => return null,
        };
        if (norm_attrs.dim == down_attrs.out_dim) {
            post_down_norm_id = norm_id;
            add_lhs_id = norm_id;
            eps = norm_attrs.eps;
        }
    }

    const add_id = findBinaryInputNode(graph, node_ids, node_pos + 1, reachable, skipped_nodes, add_lhs_id, residual_id, &isAddNode) orelse return null;
    if (!hasOnlyExpectedUses(graph, reachable, skipped_nodes, pair_id, &.{ activation_id, pair_second_id })) return null;
    if (!hasOnlyExpectedUses(graph, reachable, skipped_nodes, activation_id, &.{multiply_id})) return null;
    if (!hasOnlyExpectedUses(graph, reachable, skipped_nodes, pair_second_id, &.{multiply_id})) return null;
    if (!hasOnlyExpectedUses(graph, reachable, skipped_nodes, multiply_id, &.{down_id})) return null;
    if (post_down_norm_id) |norm_id| {
        if (!hasOnlyExpectedUses(graph, reachable, skipped_nodes, down_id, &.{norm_id})) return null;
        if (!hasOnlyExpectedUses(graph, reachable, skipped_nodes, norm_id, &.{add_id})) return null;
    } else if (!hasOnlyExpectedUses(graph, reachable, skipped_nodes, down_id, &.{add_id})) return null;

    if (!nodeLastUseIs(last_use, pair_id, pair_second_id)) return null;
    if (!nodeLastUseIs(last_use, activation_id, multiply_id)) return null;
    if (!nodeLastUseIs(last_use, pair_second_id, multiply_id)) return null;
    if (!nodeLastUseIs(last_use, multiply_id, down_id)) return null;
    if (post_down_norm_id) |norm_id| {
        if (!nodeLastUseIs(last_use, down_id, norm_id)) return null;
        if (!nodeLastUseIs(last_use, norm_id, add_id)) return null;
    } else if (!nodeLastUseIs(last_use, down_id, add_id)) return null;

    return .{
        .pair_id = pair_id,
        .pair_second_id = pair_second_id,
        .activation_id = activation_id,
        .multiply_id = multiply_id,
        .down_id = down_id,
        .post_down_norm_id = post_down_norm_id,
        .add_id = add_id,
        .residual_id = residual_id,
        .activation = activation,
        .hidden_size = pair_attrs.in_dim,
        .intermediate_size = pair_attrs.out_dim,
        .rows = pair_attrs.rows,
        .eps = eps,
    };
}

fn traceGatedFfnResidualCandidate(
    graph: *const Graph,
    node_ids: []const NodeId,
    node_pos: usize,
    reachable: []const bool,
    skipped_nodes: []const bool,
    last_use: []const u32,
) void {
    const pair_id = node_ids[node_pos];
    const pair = graph.node(pair_id);
    const pair_attrs = switch (pair.op) {
        .fused_linear_no_bias_pair => |attrs| attrs,
        else => return,
    };
    const pair_inputs = pair.getInputs();
    if (pair_inputs.len < 3) {
        std.debug.print("metal_graph_fusion_trace: gated_ffn miss pair={d} reason=short_pair_inputs\n", .{pair_id});
        return;
    }
    if (pair_attrs.rows == 0 or pair_attrs.in_dim == 0 or pair_attrs.out_dim == 0) {
        std.debug.print("metal_graph_fusion_trace: gated_ffn miss pair={d} reason=zero_dims rows={d} hidden={d} intermediate={d}\n", .{
            pair_id,
            pair_attrs.rows,
            pair_attrs.in_dim,
            pair_attrs.out_dim,
        });
        return;
    }

    const norm = graph.node(pair_inputs[0]);
    const residual_id = switch (norm.op) {
        .fused_rms_norm => blk: {
            const norm_inputs = norm.getInputs();
            if (norm_inputs.len < 1) {
                std.debug.print("metal_graph_fusion_trace: gated_ffn miss pair={d} reason=short_prenorm_inputs norm={d}\n", .{ pair_id, pair_inputs[0] });
                return;
            }
            break :blk norm_inputs[0];
        },
        else => {
            std.debug.print("metal_graph_fusion_trace: gated_ffn miss pair={d} reason=input_not_prenorm input={d} input_op={s}\n", .{ pair_id, pair_inputs[0], @tagName(norm.op) });
            return;
        },
    };

    const activation_id = findSingleInputNode(graph, node_ids, node_pos + 1, reachable, skipped_nodes, pair_id, &isSupportedGatedFfnActivation) orelse {
        std.debug.print("metal_graph_fusion_trace: gated_ffn miss pair={d} reason=missing_activation residual={d}\n", .{ pair_id, residual_id });
        return;
    };
    const pair_second_id = findSingleInputNode(graph, node_ids, node_pos + 1, reachable, skipped_nodes, pair_id, &isPairSecondMarker) orelse {
        std.debug.print("metal_graph_fusion_trace: gated_ffn miss pair={d} activation={d} reason=missing_pair_second\n", .{ pair_id, activation_id });
        return;
    };
    const multiply_id = findBinaryInputNode(graph, node_ids, node_pos + 1, reachable, skipped_nodes, activation_id, pair_second_id, &isMultiplyNode) orelse {
        std.debug.print("metal_graph_fusion_trace: gated_ffn miss pair={d} activation={d} pair_second={d} reason=missing_multiply\n", .{ pair_id, activation_id, pair_second_id });
        return;
    };
    const down_id = findSingleInputNode(graph, node_ids, node_pos + 1, reachable, skipped_nodes, multiply_id, &isPlainLinearNoBiasNode) orelse {
        std.debug.print("metal_graph_fusion_trace: gated_ffn miss pair={d} multiply={d} reason=missing_down_linear\n", .{ pair_id, multiply_id });
        return;
    };
    const down = graph.node(down_id);
    const down_attrs = switch (down.op) {
        .fused_linear_no_bias => |attrs| attrs,
        else => unreachable,
    };
    if (down_attrs.num_projections != 0 or
        down_attrs.rows != pair_attrs.rows or
        down_attrs.in_dim != pair_attrs.out_dim or
        down_attrs.out_dim != pair_attrs.in_dim)
    {
        std.debug.print(
            "metal_graph_fusion_trace: gated_ffn miss pair={d} down={d} reason=down_shape_mismatch pair_rows={d} pair_in={d} pair_out={d} down_rows={d} down_in={d} down_out={d} down_proj={d}\n",
            .{
                pair_id,
                down_id,
                pair_attrs.rows,
                pair_attrs.in_dim,
                pair_attrs.out_dim,
                down_attrs.rows,
                down_attrs.in_dim,
                down_attrs.out_dim,
                down_attrs.num_projections,
            },
        );
        return;
    }

    var post_down_norm_id: ?NodeId = null;
    var add_lhs_id = down_id;
    if (findSingleInputNode(graph, node_ids, node_pos + 1, reachable, skipped_nodes, down_id, &isMatchingPostDownRmsNorm)) |norm_id| {
        const post_norm = graph.node(norm_id);
        const norm_attrs = switch (post_norm.op) {
            .fused_rms_norm => |attrs| attrs,
            else => unreachable,
        };
        if (norm_attrs.dim == down_attrs.out_dim) {
            post_down_norm_id = norm_id;
            add_lhs_id = norm_id;
        }
    }

    const add_id = findBinaryInputNode(graph, node_ids, node_pos + 1, reachable, skipped_nodes, add_lhs_id, residual_id, &isAddNode) orelse {
        std.debug.print("metal_graph_fusion_trace: gated_ffn miss pair={d} down={d} post_norm={?d} residual={d} reason=missing_residual_add\n", .{ pair_id, down_id, post_down_norm_id, residual_id });
        return;
    };

    if (!hasOnlyExpectedUses(graph, reachable, skipped_nodes, pair_id, &.{ activation_id, pair_second_id })) {
        traceUnexpectedUses(graph, reachable, skipped_nodes, pair_id, &.{ activation_id, pair_second_id }, "pair_extra_use");
        return;
    }
    if (!hasOnlyExpectedUses(graph, reachable, skipped_nodes, activation_id, &.{multiply_id})) {
        traceUnexpectedUses(graph, reachable, skipped_nodes, activation_id, &.{multiply_id}, "activation_extra_use");
        return;
    }
    if (!hasOnlyExpectedUses(graph, reachable, skipped_nodes, pair_second_id, &.{multiply_id})) {
        traceUnexpectedUses(graph, reachable, skipped_nodes, pair_second_id, &.{multiply_id}, "pair_second_extra_use");
        return;
    }
    if (!hasOnlyExpectedUses(graph, reachable, skipped_nodes, multiply_id, &.{down_id})) {
        traceUnexpectedUses(graph, reachable, skipped_nodes, multiply_id, &.{down_id}, "multiply_extra_use");
        return;
    }
    if (post_down_norm_id) |norm_id| {
        if (!hasOnlyExpectedUses(graph, reachable, skipped_nodes, down_id, &.{norm_id})) {
            traceUnexpectedUses(graph, reachable, skipped_nodes, down_id, &.{norm_id}, "down_extra_use");
            return;
        }
        if (!hasOnlyExpectedUses(graph, reachable, skipped_nodes, norm_id, &.{add_id})) {
            traceUnexpectedUses(graph, reachable, skipped_nodes, norm_id, &.{add_id}, "norm_extra_use");
            return;
        }
    } else if (!hasOnlyExpectedUses(graph, reachable, skipped_nodes, down_id, &.{add_id})) {
        traceUnexpectedUses(graph, reachable, skipped_nodes, down_id, &.{add_id}, "down_extra_use");
        return;
    }

    if (!nodeLastUseIs(last_use, pair_id, pair_second_id)) return traceLastUseMismatch(last_use, pair_id, pair_second_id, "pair_last_use");
    if (!nodeLastUseIs(last_use, activation_id, multiply_id)) return traceLastUseMismatch(last_use, activation_id, multiply_id, "activation_last_use");
    if (!nodeLastUseIs(last_use, pair_second_id, multiply_id)) return traceLastUseMismatch(last_use, pair_second_id, multiply_id, "pair_second_last_use");
    if (!nodeLastUseIs(last_use, multiply_id, down_id)) return traceLastUseMismatch(last_use, multiply_id, down_id, "multiply_last_use");
    if (post_down_norm_id) |norm_id| {
        if (!nodeLastUseIs(last_use, down_id, norm_id)) return traceLastUseMismatch(last_use, down_id, norm_id, "down_last_use");
        if (!nodeLastUseIs(last_use, norm_id, add_id)) return traceLastUseMismatch(last_use, norm_id, add_id, "norm_last_use");
    } else if (!nodeLastUseIs(last_use, down_id, add_id)) return traceLastUseMismatch(last_use, down_id, add_id, "down_last_use");

    std.debug.print("metal_graph_fusion_trace: gated_ffn miss pair={d} reason=unknown_after_trace add={d}\n", .{ pair_id, add_id });
}

fn traceUnexpectedUses(
    graph: *const Graph,
    reachable: []const bool,
    skipped_nodes: []const bool,
    node_id: NodeId,
    expected: []const NodeId,
    reason: []const u8,
) void {
    std.debug.print("metal_graph_fusion_trace: gated_ffn miss node={d} reason={s} expected=", .{ node_id, reason });
    for (expected, 0..) |expected_id, i| {
        if (i != 0) std.debug.print(",", .{});
        std.debug.print("{d}", .{expected_id});
    }
    std.debug.print(" actual=", .{});
    var first = true;
    for (0..graph.nodeCount()) |raw_candidate| {
        const candidate_id: NodeId = @intCast(raw_candidate);
        if (raw_candidate >= reachable.len or !reachable[raw_candidate]) continue;
        if (raw_candidate < skipped_nodes.len and skipped_nodes[raw_candidate]) continue;
        const candidate = graph.node(candidate_id);
        for (candidate.getInputs()) |input_id| {
            if (input_id != node_id) continue;
            if (!first) std.debug.print(",", .{});
            first = false;
            std.debug.print("{d}:{s}", .{ candidate_id, @tagName(candidate.op) });
            break;
        }
    }
    std.debug.print("\n", .{});
}

fn traceLastUseMismatch(last_use: []const u32, node_id: NodeId, expected: NodeId, reason: []const u8) void {
    const idx: usize = @intCast(node_id);
    const actual: u32 = if (idx < last_use.len) last_use[idx] else std.math.maxInt(u32);
    std.debug.print(
        "metal_graph_fusion_trace: gated_ffn miss node={d} reason={s} expected_last_use={d} actual_last_use={d}\n",
        .{ node_id, reason, expected, actual },
    );
}

fn findSingleInputNode(
    graph: *const Graph,
    node_ids: []const NodeId,
    start_pos: usize,
    reachable: []const bool,
    skipped_nodes: []const bool,
    input_id: NodeId,
    predicate: *const fn (*const ml.graph.Node) bool,
) ?NodeId {
    for (node_ids[start_pos..]) |candidate_id| {
        const candidate_index: usize = @intCast(candidate_id);
        if (candidate_index >= reachable.len or !reachable[candidate_index]) continue;
        if (candidate_index < skipped_nodes.len and skipped_nodes[candidate_index]) continue;
        const candidate = graph.node(candidate_id);
        if (!predicate(candidate)) continue;
        const inputs = candidate.getInputs();
        if (inputs.len >= 1 and inputs[0] == input_id) return candidate_id;
    }
    return null;
}

fn findBinaryInputNode(
    graph: *const Graph,
    node_ids: []const NodeId,
    start_pos: usize,
    reachable: []const bool,
    skipped_nodes: []const bool,
    lhs_id: NodeId,
    rhs_id: NodeId,
    predicate: *const fn (*const ml.graph.Node) bool,
) ?NodeId {
    for (node_ids[start_pos..]) |candidate_id| {
        const candidate_index: usize = @intCast(candidate_id);
        if (candidate_index >= reachable.len or !reachable[candidate_index]) continue;
        if (candidate_index < skipped_nodes.len and skipped_nodes[candidate_index]) continue;
        const candidate = graph.node(candidate_id);
        if (!predicate(candidate)) continue;
        const inputs = candidate.getInputs();
        if (inputs.len < 2) continue;
        if ((inputs[0] == lhs_id and inputs[1] == rhs_id) or
            (inputs[0] == rhs_id and inputs[1] == lhs_id))
        {
            return candidate_id;
        }
    }
    return null;
}

fn findSingleInputNodeAsBinaryLhs(
    graph: *const Graph,
    node_ids: []const NodeId,
    start_pos: usize,
    reachable: []const bool,
    skipped_nodes: []const bool,
    lhs_id: NodeId,
    predicate: *const fn (*const ml.graph.Node) bool,
) ?NodeId {
    for (node_ids[start_pos..]) |candidate_id| {
        const candidate_index: usize = @intCast(candidate_id);
        if (candidate_index >= reachable.len or !reachable[candidate_index]) continue;
        if (candidate_index < skipped_nodes.len and skipped_nodes[candidate_index]) continue;
        const candidate = graph.node(candidate_id);
        if (!predicate(candidate)) continue;
        const inputs = candidate.getInputs();
        if (inputs.len >= 2 and (inputs[0] == lhs_id or inputs[1] == lhs_id)) return candidate_id;
    }
    return null;
}

fn hasOnlyExpectedUses(
    graph: *const Graph,
    reachable: []const bool,
    skipped_nodes: []const bool,
    node_id: NodeId,
    expected: []const NodeId,
) bool {
    var seen: usize = 0;
    for (0..graph.nodeCount()) |raw_candidate| {
        const candidate_id: NodeId = @intCast(raw_candidate);
        if (raw_candidate >= reachable.len or !reachable[raw_candidate]) continue;
        if (raw_candidate < skipped_nodes.len and skipped_nodes[raw_candidate]) continue;
        const candidate = graph.node(candidate_id);
        var uses_candidate = false;
        for (candidate.getInputs()) |input_id| {
            if (input_id == node_id) {
                uses_candidate = true;
                break;
            }
        }
        if (!uses_candidate) continue;
        for (expected) |expected_id| {
            if (candidate_id == expected_id) {
                seen += 1;
                break;
            }
        } else return false;
    }
    return seen == expected.len;
}

fn hasExpectedReachableUseThrough(
    graph: *const Graph,
    reachable: []const bool,
    skipped_nodes: []const bool,
    node_id: NodeId,
    predicate: *const fn (*const ml.graph.Node) bool,
    passthrough: *const fn (*const ml.graph.Node) bool,
    depth: usize,
) bool {
    if (depth == 0) return false;
    for (0..graph.nodeCount()) |raw_candidate| {
        const candidate_id: NodeId = @intCast(raw_candidate);
        if (raw_candidate >= reachable.len or !reachable[raw_candidate]) continue;
        if (raw_candidate < skipped_nodes.len and skipped_nodes[raw_candidate]) continue;
        const candidate = graph.node(candidate_id);
        var uses_node = false;
        for (candidate.getInputs()) |input_id| {
            if (input_id == node_id) {
                uses_node = true;
                break;
            }
        }
        if (!uses_node) continue;
        if (predicate(candidate)) return true;
        if (passthrough(candidate) and hasExpectedReachableUseThrough(
            graph,
            reachable,
            skipped_nodes,
            candidate_id,
            predicate,
            passthrough,
            depth - 1,
        )) return true;
    }
    return false;
}

fn nodeLastUseIs(last_use: []const u32, node_id: NodeId, expected: NodeId) bool {
    const idx: usize = @intCast(node_id);
    return idx < last_use.len and last_use[idx] == @as(u32, @intCast(expected));
}

fn isSupportedGatedFfnActivation(node: *const ml.graph.Node) bool {
    return activationKindForGraphNode(node) != null;
}

fn activationKindForGraphNode(node: *const ml.graph.Node) ?ops_mod.DecoderRuntimeActivationKind {
    return switch (node.op) {
        .fused_gelu => .gelu,
        .fused_gelu_exact => .gelu_exact,
        .fused_silu => .silu,
        .fused_relu => .relu,
        .fused_quick_gelu => .quick_gelu,
        else => null,
    };
}

fn isPairSecondMarker(node: *const ml.graph.Node) bool {
    return node.op == .fused_to_float32;
}

fn isMultiplyNode(node: *const ml.graph.Node) bool {
    return node.op == .mul or node.op == .fused_elem_multiply;
}

fn isAddNode(node: *const ml.graph.Node) bool {
    return node.op == .add or node.op == .fused_elem_add;
}

fn isMatchingPostDownRmsNorm(node: *const ml.graph.Node) bool {
    return node.op == .fused_rms_norm;
}

fn isPlainLinearNoBiasNode(node: *const ml.graph.Node) bool {
    return switch (node.op) {
        .fused_linear_no_bias => |attrs| attrs.num_projections == 0,
        else => false,
    };
}

fn isLinearNoBiasPairNode(node: *const ml.graph.Node) bool {
    return node.op == .fused_linear_no_bias_pair;
}

const MetalGraphRegionKind = enum {
    qkv,
    attention,
    ffn,
    ple,
    tail,
};

fn recordMetalGraphRegion(
    stats: *PartitionExecutor.ExecutionStats,
    kind: MetalGraphRegionKind,
    op_count: u64,
) void {
    stats.graph_regions += 1;
    stats.graph_region_ops += op_count;
    switch (kind) {
        .qkv => stats.metal_qkv_regions += 1,
        .attention => stats.metal_attention_regions += 1,
        .ffn => stats.metal_ffn_regions += 1,
        .ple => stats.metal_ple_regions += 1,
        .tail => stats.metal_tail_regions += 1,
    }
}

const RawLinearDotPattern = struct {
    id: NodeId,
    input_id: NodeId,
    transpose_id: NodeId,
    weight_id: NodeId,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
};

const RawLinearBiasPattern = struct {
    dot: RawLinearDotPattern,
    add_id: NodeId,
    bias_id: NodeId,
};

const RawLinearPairPattern = struct {
    first: RawLinearDotPattern,
    second: RawLinearDotPattern,
};

const RawLinearBiasPairPattern = struct {
    first: RawLinearBiasPattern,
    second: RawLinearBiasPattern,
};

const LoraLinearPattern = struct {
    add_id: NodeId,
    base_linear_id: NodeId,
    input_id: NodeId,
    dropout_mul_id: ?NodeId = null,
    dropout_mask_id: ?NodeId = null,
    lora_input_id: NodeId,
    lora_a_id: NodeId,
    lora_b_id: NodeId,
    after_a_id: NodeId,
    after_b_id: NodeId,
    scaled_id: NodeId,
    scale_id: NodeId,
    populate_scaled: bool = false,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    rank: usize,
};

const LoraLinearQkvPattern = struct {
    q: LoraLinearPattern,
    k: LoraLinearPattern,
    v: LoraLinearPattern,
    q_base_weight_id: NodeId,
    k_base_weight_id: NodeId,
    v_base_weight_id: NodeId,
    rows: usize,
    in_dim: usize,
    q_out_dim: usize,
    kv_out_dim: usize,
};

const DebertaAttentionPattern = struct {
    first_node_id: NodeId,
    output_id: NodeId,
    q_id: NodeId,
    k_id: NodeId,
    v_id: NodeId,
    q_r_id: NodeId,
    k_r_id: NodeId,
    attn_bias_id: NodeId,
    layer_index: usize,
    batch: usize,
    seq_len: usize,
    num_heads: usize,
    head_dim: usize,
    hidden_size: usize,
};

const DebertaAttentionShape = struct {
    batch: usize,
    seq_len: usize,
    num_heads: usize,
    head_dim: usize,
};

const DebertaFfnForwardPattern = struct {
    first_dot_id: NodeId,
    first_base_add_id: NodeId,
    first_add_id: NodeId,
    gelu_id: NodeId,
    output_dot_id: NodeId,
    output_base_add_id: NodeId,
    output_add_id: NodeId,
    input_id: NodeId,
    first_weight_id: NodeId,
    first_bias_id: NodeId,
    second_weight_id: NodeId,
    second_bias_id: NodeId,
    activation_aux_id: NodeId = null_node,
    first_lora: ?LoraLinearPattern = null,
    second_lora: ?LoraLinearPattern = null,
    rows: usize,
    hidden_size: usize,
    intermediate_size: usize,
    output_size: usize,
    activation: ops_mod.DecoderRuntimeActivationKind = .gelu_exact,
    is_head_mlp: bool = false,
};

const DebertaEncoderLoraLayerPattern = struct {
    ffn: DebertaFfnForwardPattern,
    residual_add_id: NodeId,
    layer_norm_id: NodeId,
    layer_norm_weight_id: NodeId,
    layer_norm_bias_id: NodeId,
    layer_index: usize,
    norm_eps: f32,
    residual_layer_norm_internal_only: bool = false,
};

const LayerNormForwardPattern = struct {
    output_id: NodeId,
    weight_id: NodeId,
    bias_id: NodeId,
    dim: usize,
    eps: f32,
    internal_node_ids: [16]NodeId = [_]NodeId{null_node} ** 16,
    internal_node_count: usize = 0,
};

const LoraBackwardPattern = struct {
    d_after_a_id: NodeId,
    grad_a_dot_id: NodeId,
    grad_a_id: NodeId,
    grad_b_dot_id: NodeId,
    grad_b_id: NodeId,
    input_transpose_id: NodeId,
    after_a_transpose_id: NodeId,
    b_transpose_id: NodeId,
    input_id: NodeId,
    after_a_id: NodeId,
    lora_b_id: NodeId,
    output_grad_id: NodeId,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    rank: usize,
};

const LowRankLoraBackwardPattern = struct {
    d_after_a_id: NodeId,
    grad_b_dot_id: NodeId,
    after_a_transpose_id: NodeId,
    b_transpose_id: NodeId,
    after_a_id: NodeId,
    lora_b_id: NodeId,
    output_grad_id: NodeId,
    rows: usize,
    out_dim: usize,
    rank: usize,
};

const RankAdapterBackwardPattern = struct {
    d_after_a_id: NodeId,
    grad_a_dot_id: NodeId,
    grad_a_id: NodeId,
    grad_b_dot_id: NodeId,
    grad_b_id: NodeId,
    input_transpose_id: NodeId,
    after_a_transpose_id: NodeId,
    rhs_transpose_id: NodeId,
    input_id: NodeId,
    after_a_id: NodeId,
    rhs_source_id: NodeId,
    rhs_uses_transpose_value: bool,
    output_grad_id: NodeId,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    rank: usize,
};

const FfnGeluBackwardPattern = struct {
    first_dot_id: NodeId,
    second_branch_dot_id: NodeId,
    upstream_add_id: NodeId,
    gelu_backward_id: NodeId,
    output_dot_id: NodeId,
    rows: usize,
    hidden_size: usize,
    intermediate_size: usize,
    exact: bool = false,
};

const RuntimeDot2DInputs = struct {
    lhs: CT,
    rhs: CT,
    rhs_contract_axis: u32,
    k: usize,
};

const RuntimeDot2DRhs = struct {
    rhs: CT,
    rhs_contract_axis: u32,
    k: usize,
};

const RuntimeDot2DResolvedRhs = struct {
    rhs: CT,
    rhs_contract_axis: u32,
};

const GroupedHeadDotCandidate = struct {
    node_id: NodeId,
    lhs: CT,
    rhs: CT,
    m: usize,
    n: usize,
    k: usize,
    rhs_contract_axis: u32,
};

fn executeLoraLinearPattern(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    value_device: []DeviceId,
    device_id: DeviceId,
    stats: ?*PartitionExecutor.ExecutionStats,
    pattern: LoraLinearPattern,
) !bool {
    const base = valueFor(values, pattern.base_linear_id) orelse return false;
    const lora_input = if (pattern.dropout_mul_id) |dropout_mul_id| blk: {
        const input = valueFor(values, pattern.input_id) orelse return false;
        const mask = valueFor(values, pattern.dropout_mask_id orelse return false) orelse return false;
        const masked = cb.multiply(input, mask) catch |err| switch (err) {
            error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => return false,
            else => return err,
        };
        values[@intCast(dropout_mul_id)] = masked;
        value_device[@intCast(dropout_mul_id)] = device_id;
        break :blk masked;
    } else valueFor(values, pattern.lora_input_id) orelse return false;
    const lora_a = valueFor(values, pattern.lora_a_id) orelse return false;
    const lora_b = valueFor(values, pattern.lora_b_id) orelse return false;
    const scale = valueFor(values, pattern.scale_id) orelse return false;
    const scale_value = scalarConstantF32(graph, pattern.scale_id);

    if (scale_value) |scale_f32| {
        if (try cb.loraLinearBranch(&.{
            .input = lora_input,
            .base = base,
            .lora_a = lora_a,
            .lora_b = lora_b,
            .rows = pattern.rows,
            .in_dim = pattern.in_dim,
            .rank = pattern.rank,
            .out_dim = pattern.out_dim,
            .scale = scale_f32,
        })) |fused| {
            values[@intCast(pattern.after_a_id)] = fused.after_a;
            value_device[@intCast(pattern.after_a_id)] = device_id;
            values[@intCast(pattern.after_b_id)] = fused.after_b;
            value_device[@intCast(pattern.after_b_id)] = device_id;
            if (pattern.populate_scaled) {
                const scaled = cb.multiply(fused.after_b, scale) catch |err| switch (err) {
                    error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => null,
                    else => return err,
                };
                if (scaled) |scaled_tensor| {
                    values[@intCast(pattern.scaled_id)] = scaled_tensor;
                    value_device[@intCast(pattern.scaled_id)] = device_id;
                }
            }
            values[@intCast(pattern.add_id)] = fused.output;
            value_device[@intCast(pattern.add_id)] = device_id;

            if (stats) |s| {
                recordMetalGraphRegion(s, .ffn, 4);
                s.metal_lora_linear_regions += 1;
                s.fused_graph_pattern_dispatches += 1;
                s.fused_graph_nodes_elided += 3 + @as(u64, if (pattern.dropout_mul_id != null) 1 else 0);
                recordGemmaRuntimeResidency(s, graph, pattern.add_id, isMetalResidentOrQuantizedDescriptor(cb, fused.output));
            }
            if (traceMetalGraphFusionsEnabled()) {
                std.debug.print(
                    "metal_graph_fusion_trace: lora_linear_region fused add={d} base={d} after_a={d} after_b={d} rows={d} in={d} rank={d} out={d}\n",
                    .{ pattern.add_id, pattern.base_linear_id, pattern.after_a_id, pattern.after_b_id, pattern.rows, pattern.in_dim, pattern.rank, pattern.out_dim },
                );
            }
            return true;
        }
    }

    const after_a = cb.linearNoBias(lora_input, lora_a, pattern.rows, pattern.in_dim, pattern.rank) catch |err| switch (err) {
        error.UnsupportedOperation, error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => return false,
        else => return err,
    };
    values[@intCast(pattern.after_a_id)] = after_a;
    value_device[@intCast(pattern.after_a_id)] = device_id;

    const after_b = cb.linearNoBias(after_a, lora_b, pattern.rows, pattern.rank, pattern.out_dim) catch |err| switch (err) {
        error.UnsupportedOperation, error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => return false,
        else => return err,
    };
    values[@intCast(pattern.after_b_id)] = after_b;
    value_device[@intCast(pattern.after_b_id)] = device_id;

    const output = scaled_add: {
        if (try cb.decoderRuntimeApplyScaledAddScale(&.{
            .lhs = after_b,
            .rhs = base,
            .dim = pattern.rows * pattern.out_dim,
            .lhs_scale = scale_value orelse break :scaled_add null,
            .output_scale = 1.0,
        })) |out| {
            if (pattern.populate_scaled) {
                const scaled = cb.multiply(after_b, scale) catch |err| switch (err) {
                    error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => break :scaled_add out,
                    else => return err,
                };
                values[@intCast(pattern.scaled_id)] = scaled;
                value_device[@intCast(pattern.scaled_id)] = device_id;
            }
            break :scaled_add out;
        }
        break :scaled_add null;
    } orelse fallback: {
        const scaled = cb.multiply(after_b, scale) catch |err| switch (err) {
            error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => return false,
            else => return err,
        };
        values[@intCast(pattern.scaled_id)] = scaled;
        value_device[@intCast(pattern.scaled_id)] = device_id;

        const out = cb.add(base, scaled) catch |err| switch (err) {
            error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => return false,
            else => return err,
        };
        break :fallback out;
    };
    values[@intCast(pattern.add_id)] = output;
    value_device[@intCast(pattern.add_id)] = device_id;

    if (stats) |s| {
        recordMetalGraphRegion(s, .ffn, 4);
        s.metal_lora_linear_regions += 1;
        s.fused_graph_pattern_dispatches += 1;
        s.fused_graph_nodes_elided += 3 + @as(u64, if (pattern.dropout_mul_id != null) 1 else 0);
        recordGemmaRuntimeResidency(s, graph, pattern.add_id, isMetalResidentOrQuantizedDescriptor(cb, output));
    }
    if (traceMetalGraphFusionsEnabled()) {
        std.debug.print(
            "metal_graph_fusion_trace: lora_linear_region executed add={d} base={d} after_a={d} after_b={d} rows={d} in={d} rank={d} out={d}\n",
            .{ pattern.add_id, pattern.base_linear_id, pattern.after_a_id, pattern.after_b_id, pattern.rows, pattern.in_dim, pattern.rank, pattern.out_dim },
        );
    }
    return true;
}

fn executeLoraLinearQkvPattern(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    value_device: []DeviceId,
    device_id: DeviceId,
    stats: ?*PartitionExecutor.ExecutionStats,
    pattern: LoraLinearQkvPattern,
    skipped_nodes: []bool,
) !bool {
    if (!try executeLoraLinearPattern(graph, cb, values, value_device, device_id, stats, pattern.q)) return false;
    if (!try executeLoraLinearPattern(graph, cb, values, value_device, device_id, stats, pattern.k)) return false;
    if (!try executeLoraLinearPattern(graph, cb, values, value_device, device_id, stats, pattern.v)) return false;
    markLoraLinearQkvSkipped(skipped_nodes, pattern);
    if (stats) |s| {
        s.metal_lora_qkv_regions += 1;
        s.gemma_qkv_hits += 3;
    }
    if (traceMetalGraphFusionsEnabled()) {
        std.debug.print(
            "metal_graph_fusion_trace: lora_qkv_region executed q={d} k={d} v={d} rows={d} in={d} q_out={d} kv_out={d}\n",
            .{ pattern.q.add_id, pattern.k.add_id, pattern.v.add_id, pattern.rows, pattern.in_dim, pattern.q_out_dim, pattern.kv_out_dim },
        );
    }
    return true;
}

fn executeDebertaAttentionPattern(
    graph: *const Graph,
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    values: []?CT,
    value_device: []DeviceId,
    device_id: DeviceId,
    stats: ?*PartitionExecutor.ExecutionStats,
    pattern: DebertaAttentionPattern,
    skipped_nodes: []bool,
) !bool {
    const q = valueFor(values, pattern.q_id) orelse {
        traceDebertaAttentionExecutionDecline("missing_q", pattern);
        return false;
    };
    const k = valueFor(values, pattern.k_id) orelse {
        traceDebertaAttentionExecutionDecline("missing_k", pattern);
        return false;
    };
    const v = valueFor(values, pattern.v_id) orelse {
        traceDebertaAttentionExecutionDecline("missing_v", pattern);
        return false;
    };
    const q_r = valueFor(values, pattern.q_r_id) orelse {
        traceDebertaAttentionExecutionDecline("missing_q_r", pattern);
        return false;
    };
    const k_r = valueFor(values, pattern.k_r_id) orelse {
        traceDebertaAttentionExecutionDecline("missing_k_r", pattern);
        return false;
    };
    const attn_bias = valueFor(values, pattern.attn_bias_id) orelse {
        traceDebertaAttentionExecutionDecline("missing_bias", pattern);
        return false;
    };

    const mask = try attentionMaskFromBias(allocator, cb, attn_bias, pattern.batch, pattern.seq_len, pattern.num_heads);
    defer allocator.free(mask);

    const output = cb.disentangledRelativeAttention(
        q,
        k,
        v,
        q_r,
        k_r,
        mask,
        pattern.batch,
        pattern.seq_len,
        pattern.num_heads,
        pattern.head_dim,
    ) catch |err| switch (err) {
        error.UnsupportedOperation, error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch, error.UnsupportedTensorType => {
            traceDebertaAttentionExecutionDecline(@errorName(err), pattern);
            return false;
        },
        else => return err,
    };
    errdefer cb.free(output);

    const output_shape = graph.node(pattern.output_id).output_shape;
    var output_shape_buf: [8]i32 = undefined;
    const output_rank = output_shape.rank();
    if (output_rank > output_shape_buf.len) return error.UnsupportedShape;
    for (0..output_rank) |dim_index| {
        output_shape_buf[dim_index] = @intCast(output_shape.dims[dim_index]);
    }
    const output_alias = (try cb.cloneTensorShape(output, output_shape_buf[0..output_rank])) orelse {
        traceDebertaAttentionExecutionDecline("output_alias", pattern);
        return false;
    };
    errdefer cb.free(output_alias);

    values[@intCast(pattern.first_node_id)] = output;
    value_device[@intCast(pattern.first_node_id)] = device_id;
    values[@intCast(pattern.output_id)] = output_alias;
    value_device[@intCast(pattern.output_id)] = device_id;
    markDebertaAttentionSkipped(graph, skipped_nodes, pattern);

    if (stats) |s| {
        recordMetalGraphRegion(s, .attention, 1);
        s.fused_graph_pattern_dispatches += 1;
        s.fused_graph_nodes_elided += @as(u64, pattern.output_id - pattern.first_node_id + 1);
        s.gemma_attention_matmul_hits += 1;
        recordGemmaRuntimeResidency(s, graph, pattern.output_id, isMetalResidentOrQuantizedDescriptor(cb, output));
    }
    if (traceMetalGraphFusionsEnabled()) {
        std.debug.print(
            "metal_graph_fusion_trace: deberta_attention_region executed first={d} output={d} q={d} k={d} v={d} qr={d} kr={d} bias={d} layer={d} batch={d} seq={d} heads={d} head_dim={d}\n",
            .{ pattern.first_node_id, pattern.output_id, pattern.q_id, pattern.k_id, pattern.v_id, pattern.q_r_id, pattern.k_r_id, pattern.attn_bias_id, pattern.layer_index, pattern.batch, pattern.seq_len, pattern.num_heads, pattern.head_dim },
        );
    }
    return true;
}

fn traceDebertaAttentionExecutionDecline(reason: []const u8, pattern: DebertaAttentionPattern) void {
    if (!traceMetalGraphFusionsEnabled()) return;
    std.debug.print(
        "metal_graph_fusion_trace: deberta_attention_region declined reason={s} first={d} output={d} q={d} k={d} v={d} qr={d} kr={d} bias={d} layer={d}\n",
        .{ reason, pattern.first_node_id, pattern.output_id, pattern.q_id, pattern.k_id, pattern.v_id, pattern.q_r_id, pattern.k_r_id, pattern.attn_bias_id, pattern.layer_index },
    );
}

fn executeDebertaFfnForwardPattern(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    value_device: []DeviceId,
    device_id: DeviceId,
    stats: ?*PartitionExecutor.ExecutionStats,
    pattern: DebertaFfnForwardPattern,
    prepared: PreparedDebertaFfnForwardRegion,
    skipped_nodes: []bool,
) !bool {
    const first = (try executeDebertaFfnForwardLinear(
        graph,
        cb,
        values,
        value_device,
        device_id,
        pattern.input_id,
        pattern.first_weight_id,
        pattern.first_bias_id,
        pattern.first_base_add_id,
        pattern.first_add_id,
        pattern.first_lora,
        prepared.first_slot,
        pattern.rows,
        pattern.hidden_size,
        pattern.intermediate_size,
    )) orelse {
        traceDebertaFfnForwardExecutionDecline("first_linear", pattern);
        return false;
    };

    const gelu = (try cb.decoderRuntimeApplyActivation(&.{
        .input = first,
        .kind = pattern.activation,
        .dim = pattern.intermediate_size,
    })) orelse {
        traceDebertaFfnForwardExecutionDecline("activation", pattern);
        return false;
    };
    errdefer cb.free(gelu);

    values[@intCast(pattern.gelu_id)] = gelu;
    value_device[@intCast(pattern.gelu_id)] = device_id;

    const output = (try executeDebertaFfnForwardLinear(
        graph,
        cb,
        values,
        value_device,
        device_id,
        pattern.gelu_id,
        pattern.second_weight_id,
        pattern.second_bias_id,
        pattern.output_base_add_id,
        pattern.output_add_id,
        pattern.second_lora,
        prepared.second_slot,
        pattern.rows,
        pattern.intermediate_size,
        pattern.output_size,
    )) orelse {
        traceDebertaFfnForwardExecutionDecline("second_linear", pattern);
        return false;
    };
    markDebertaFfnForwardSkipped(skipped_nodes, pattern);

    if (stats) |s| {
        recordMetalGraphRegion(s, .ffn, 3);
        if (pattern.is_head_mlp) {
            s.metal_head_mlp_forward_regions += 1;
        } else {
            s.metal_deberta_ffn_forward_regions += 1;
        }
        s.fused_graph_pattern_dispatches += 1;
        s.fused_graph_nodes_elided += debertaFfnForwardElidedNodeCount(pattern);
        recordGemmaRuntimeResidency(s, graph, pattern.output_add_id, isMetalResidentOrQuantizedDescriptor(cb, output));
    }
    if (traceMetalGraphFusionsEnabled()) {
        std.debug.print(
            "metal_graph_fusion_trace: deberta_ffn_forward_region executed first_dot={d} first_add={d} activation={d} output_dot={d} output_add={d} rows={d} hidden={d} intermediate={d} output={d} activation_kind={s} head={}\n",
            .{ pattern.first_dot_id, pattern.first_add_id, pattern.gelu_id, pattern.output_dot_id, pattern.output_add_id, pattern.rows, pattern.hidden_size, pattern.intermediate_size, pattern.output_size, @tagName(pattern.activation), pattern.is_head_mlp },
        );
    }
    return true;
}

fn executeDebertaEncoderLoraLayerPattern(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    value_device: []DeviceId,
    device_id: DeviceId,
    stats: ?*PartitionExecutor.ExecutionStats,
    pattern: DebertaEncoderLoraLayerPattern,
    prepared: PreparedDebertaFfnForwardRegion,
    skipped_nodes: []bool,
) !bool {
    if (!try executeDebertaFfnForwardPattern(
        graph,
        cb,
        values,
        value_device,
        device_id,
        null,
        pattern.ffn,
        prepared,
        skipped_nodes,
    )) {
        if (stats) |s| s.metal_deberta_encoder_lora_layer_fallbacks += 1;
        return false;
    }

    if (pattern.layer_norm_id == null_node) {
        markDebertaEncoderLoraLayerSkipped(skipped_nodes, pattern);
        if (stats) |s| {
            recordMetalGraphRegion(s, .ffn, 1);
            s.metal_deberta_encoder_lora_layer_regions += 1;
            s.metal_deberta_encoder_lora_layer_scaffold_regions += 1;
            s.fused_graph_pattern_dispatches += 1;
        }
        return true;
    }

    const ffn_output = valueFor(values, pattern.ffn.output_add_id) orelse {
        if (stats) |s| s.metal_deberta_encoder_lora_layer_fallbacks += 1;
        return false;
    };
    const residual_input = valueFor(values, pattern.ffn.input_id) orelse {
        if (stats) |s| s.metal_deberta_encoder_lora_layer_fallbacks += 1;
        return false;
    };
    const ln_weight = valueFor(values, pattern.layer_norm_weight_id) orelse {
        if (stats) |s| s.metal_deberta_encoder_lora_layer_fallbacks += 1;
        return false;
    };
    const ln_bias = valueFor(values, pattern.layer_norm_bias_id) orelse {
        if (stats) |s| s.metal_deberta_encoder_lora_layer_fallbacks += 1;
        return false;
    };

    const normed = fused: {
        if (pattern.residual_layer_norm_internal_only) {
            if (try cb.addLayerNorm(ffn_output, residual_input, ln_weight, ln_bias, pattern.ffn.hidden_size, pattern.norm_eps)) |fused_normed| {
                break :fused fused_normed;
            }
        }

        const residual = cb.add(ffn_output, residual_input) catch |err| switch (err) {
            error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => {
                if (stats) |s| s.metal_deberta_encoder_lora_layer_fallbacks += 1;
                return false;
            },
            else => return err,
        };
        values[@intCast(pattern.residual_add_id)] = residual;
        value_device[@intCast(pattern.residual_add_id)] = device_id;

        break :fused cb.layerNorm(
            residual,
            ln_weight,
            ln_bias,
            pattern.ffn.hidden_size,
            pattern.norm_eps,
        ) catch |err| switch (err) {
            error.UnsupportedOperation, error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => {
                if (stats) |s| s.metal_deberta_encoder_lora_layer_fallbacks += 1;
                return false;
            },
            else => return err,
        };
    };

    values[@intCast(pattern.layer_norm_id)] = normed;
    value_device[@intCast(pattern.layer_norm_id)] = device_id;
    markDebertaEncoderLoraLayerSkipped(skipped_nodes, pattern);

    if (stats) |s| {
        recordMetalGraphRegion(s, .ffn, 2);
        s.metal_deberta_encoder_lora_layer_regions += 1;
        s.metal_deberta_encoder_lora_residual_layernorm_regions += 1;
        s.fused_graph_pattern_dispatches += 1;
        s.fused_graph_nodes_elided += 2;
        recordGemmaRuntimeResidency(s, graph, pattern.layer_norm_id, isMetalResidentOrQuantizedDescriptor(cb, normed));
    }
    if (traceMetalGraphFusionsEnabled()) {
        std.debug.print(
            "metal_graph_fusion_trace: deberta_encoder_lora_layer_region executed layer={d} ffn_output={d} residual={d} layer_norm={d} hidden={d}\n",
            .{ pattern.layer_index, pattern.ffn.output_add_id, pattern.residual_add_id, pattern.layer_norm_id, pattern.ffn.hidden_size },
        );
    }
    return true;
}

fn executeDebertaFfnForwardLinear(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    value_device: []DeviceId,
    device_id: DeviceId,
    input_id: NodeId,
    weight_id: NodeId,
    bias_id: NodeId,
    base_add_id: NodeId,
    output_id: NodeId,
    lora: ?LoraLinearPattern,
    prepared_slot: usize,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !?CT {
    const input_raw = valueFor(values, input_id) orelse {
        traceDebertaFfnForwardLinearDecline("missing_input", input_id, weight_id, bias_id, base_add_id, rows, in_dim, out_dim, null);
        return null;
    };
    // This region only needs a resident view for the dispatch. Replacing the
    // graph slot would either drop the old slot ownership or require plumbing
    // runtime-transfer ownership through this helper. Keep the promotion
    // local and release only the temporary handle we own.
    const resident_input = try temporaryMetalResidentValue(cb, input_raw);
    defer resident_input.deinit(cb);
    const input = resident_input.value;
    const base = (try cb.decoderRuntimeApplyLinear(&.{
        .slot = prepared_slot,
        .input = input,
        .in_dim = in_dim,
        .out_dim = out_dim,
    })) orelse {
        traceDebertaFfnForwardLinearDecline("linear_runtime_miss", input_id, weight_id, bias_id, base_add_id, rows, in_dim, out_dim, null);
        return null;
    };
    errdefer cb.free(base);
    values[@intCast(base_add_id)] = base;
    value_device[@intCast(base_add_id)] = device_id;

    if (lora) |lora_pattern| {
        if (!try executeLoraLinearPattern(graph, cb, values, value_device, device_id, null, lora_pattern)) return null;
        return valueFor(values, output_id);
    }

    return base;
}

fn valueForOrMaterializeParameter(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    value_device: []DeviceId,
    device_id: DeviceId,
    node_id: NodeId,
) !?CT {
    if (valueFor(values, node_id)) |value| return value;
    if (node_id == null_node or node_id >= graph.nodeCount()) return null;
    const index: usize = @intCast(node_id);
    if (index >= values.len or index >= value_device.len) return null;
    const node = graph.node(node_id);
    if (node.op != .parameter) return null;
    const materialized = try cb.getWeight(graph.parameterName(node));
    values[index] = materialized;
    value_device[index] = device_id;
    return materialized;
}

fn traceDebertaFfnForwardLinearDecline(
    reason: []const u8,
    input_id: NodeId,
    weight_id: NodeId,
    bias_id: NodeId,
    base_add_id: NodeId,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    err_name: ?[]const u8,
) void {
    if (!traceMetalGraphFusionsEnabled()) return;
    std.debug.print(
        "metal_graph_fusion_trace: deberta_ffn_forward_linear declined reason={s} err={?s} input={d} weight={d} bias={d} base_add={d} rows={d} in={d} out={d}\n",
        .{ reason, err_name, input_id, weight_id, bias_id, base_add_id, rows, in_dim, out_dim },
    );
}

fn debertaFfnForwardElidedNodeCount(pattern: DebertaFfnForwardPattern) u64 {
    var count: u64 = 5;
    if (pattern.activation_aux_id != null_node) count += 1;
    if (pattern.first_lora != null) count += 4;
    if (pattern.second_lora != null) count += 4;
    return count;
}

fn traceDebertaFfnForwardExecutionDecline(reason: []const u8, pattern: DebertaFfnForwardPattern) void {
    if (!traceMetalGraphFusionsEnabled()) return;
    std.debug.print(
        "metal_graph_fusion_trace: deberta_ffn_forward_region declined reason={s} first_dot={d} first_add={d} gelu={d} output_dot={d} output_add={d}\n",
        .{ reason, pattern.first_dot_id, pattern.first_add_id, pattern.gelu_id, pattern.output_dot_id, pattern.output_add_id },
    );
}

// Per-step cache for the disentangled-attention padding mask. The mask is
// derived from `attn_bias < -1e8` (padding only) and is IDENTICAL across every
// attention layer and the fwd/bwd of a step — a standard transformer property.
// Without this, each of the ~24 attention calls/step does a full device->host
// readback of the [B,H,S,S] bias (toFloat32) just to recover B*S padding bits,
// forcing a frame sync each time. Cached, the readback happens once per step.
const AttnMaskCache = struct {
    mask: ?[]i64 = null,
    batch: usize = 0,
    seq_len: usize = 0,
    num_heads: usize = 0,
};

fn cachedAttentionMaskFromBias(
    cache: ?*AttnMaskCache,
    allocator: std.mem.Allocator,
    cb: *const ComputeBackend,
    attn_bias: CT,
    batch: usize,
    seq_len: usize,
    num_heads: usize,
) ![]i64 {
    if (cache) |c| {
        if (c.mask) |m| {
            if (c.batch == batch and c.seq_len == seq_len and c.num_heads == num_heads) return m;
            allocator.free(m);
            c.mask = null;
        }
        const m = try attentionMaskFromBias(allocator, cb, attn_bias, batch, seq_len, num_heads);
        c.* = .{ .mask = m, .batch = batch, .seq_len = seq_len, .num_heads = num_heads };
        return m;
    }
    return attentionMaskFromBias(allocator, cb, attn_bias, batch, seq_len, num_heads);
}

fn attentionMaskFromBias(
    allocator: std.mem.Allocator,
    cb: *const ComputeBackend,
    attn_bias: CT,
    batch: usize,
    seq_len: usize,
    num_heads: usize,
) ![]i64 {
    const bias = try cb.toFloat32(attn_bias, allocator);
    defer allocator.free(bias);
    if (bias.len < batch * num_heads * seq_len * seq_len) return error.InvalidAttentionShape;

    const mask = try allocator.alloc(i64, batch * seq_len);
    errdefer allocator.free(mask);
    for (0..batch) |b| {
        for (0..seq_len) |k| {
            const idx = ((b * num_heads) * seq_len + 0) * seq_len + k;
            mask[b * seq_len + k] = if (bias[idx] < -1.0e8) 0 else 1;
        }
    }
    return mask;
}

fn executeLoraBackwardPattern(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    value_device: []DeviceId,
    device_id: DeviceId,
    stats: ?*PartitionExecutor.ExecutionStats,
    pattern: LoraBackwardPattern,
    skipped_nodes: []bool,
) !bool {
    const input = valueFor(values, pattern.input_id) orelse return false;
    const after_a = valueFor(values, pattern.after_a_id) orelse return false;
    const lora_b = valueFor(values, pattern.lora_b_id) orelse return false;
    const output_grad = valueFor(values, pattern.output_grad_id) orelse return false;
    const fused = (try cb.loraLinearBackward(&.{
        .input = input,
        .after_a = after_a,
        .lora_b = lora_b,
        .output_grad = output_grad,
        .rows = pattern.rows,
        .in_dim = pattern.in_dim,
        .rank = pattern.rank,
        .out_dim = pattern.out_dim,
        .scale = 1.0,
    })) orelse return false;

    values[@intCast(pattern.d_after_a_id)] = fused.grad_after_a;
    value_device[@intCast(pattern.d_after_a_id)] = device_id;
    values[@intCast(pattern.grad_a_id)] = fused.grad_a;
    value_device[@intCast(pattern.grad_a_id)] = device_id;
    values[@intCast(pattern.grad_b_id)] = fused.grad_b;
    value_device[@intCast(pattern.grad_b_id)] = device_id;
    markLoraBackwardSkipped(skipped_nodes, pattern);

    if (stats) |s| {
        recordMetalGraphRegion(s, .ffn, 5);
        s.metal_lora_backward_regions += 1;
        s.fused_graph_pattern_dispatches += 1;
        s.fused_graph_nodes_elided += 4;
        recordGemmaRuntimeResidency(s, graph, pattern.grad_a_id, isMetalResidentOrQuantizedDescriptor(cb, fused.grad_a));
        recordGemmaRuntimeResidency(s, graph, pattern.grad_b_id, isMetalResidentOrQuantizedDescriptor(cb, fused.grad_b));
    }
    if (traceMetalGraphFusionsEnabled()) {
        std.debug.print(
            "metal_graph_fusion_trace: lora_backward_region executed d_after_a={d} grad_a={d} grad_b={d} input={d} after_a={d} lora_b={d} rows={d} in={d} rank={d} out={d}\n",
            .{ pattern.d_after_a_id, pattern.grad_a_id, pattern.grad_b_id, pattern.input_id, pattern.after_a_id, pattern.lora_b_id, pattern.rows, pattern.in_dim, pattern.rank, pattern.out_dim },
        );
    }
    return true;
}

fn executeLowRankLoraBackwardPattern(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    value_device: []DeviceId,
    device_id: DeviceId,
    stats: ?*PartitionExecutor.ExecutionStats,
    pattern: LowRankLoraBackwardPattern,
    skipped_nodes: []bool,
) !bool {
    const after_a = valueFor(values, pattern.after_a_id) orelse return false;
    const lora_b = valueFor(values, pattern.lora_b_id) orelse return false;
    const output_grad = valueFor(values, pattern.output_grad_id) orelse return false;
    const fused = (try cb.loraLinearBackwardB(&.{
        .after_a = after_a,
        .lora_b = lora_b,
        .output_grad = output_grad,
        .rows = pattern.rows,
        .rank = pattern.rank,
        .out_dim = pattern.out_dim,
        .scale = 1.0,
    })) orelse return false;

    values[@intCast(pattern.d_after_a_id)] = fused.grad_after_a;
    value_device[@intCast(pattern.d_after_a_id)] = device_id;
    values[@intCast(pattern.grad_b_dot_id)] = fused.grad_b_transposed;
    value_device[@intCast(pattern.grad_b_dot_id)] = device_id;
    markLowRankLoraBackwardSkipped(skipped_nodes, pattern);

    if (stats) |s| {
        recordMetalGraphRegion(s, .ffn, 2);
        s.metal_low_rank_lora_backward_regions += 1;
        s.fused_graph_pattern_dispatches += 1;
        s.fused_graph_nodes_elided += 2;
        recordGemmaRuntimeResidency(s, graph, pattern.d_after_a_id, isMetalResidentOrQuantizedDescriptor(cb, fused.grad_after_a));
        recordGemmaRuntimeResidency(s, graph, pattern.grad_b_dot_id, isMetalResidentOrQuantizedDescriptor(cb, fused.grad_b_transposed));
    }
    if (traceMetalGraphFusionsEnabled()) {
        std.debug.print(
            "metal_graph_fusion_trace: low_rank_lora_backward_region executed d_after_a={d} grad_b_t={d} after_a={d} lora_b={d} rows={d} rank={d} out={d}\n",
            .{ pattern.d_after_a_id, pattern.grad_b_dot_id, pattern.after_a_id, pattern.lora_b_id, pattern.rows, pattern.rank, pattern.out_dim },
        );
    }
    return true;
}

fn executeRankAdapterBackwardPattern(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    value_device: []DeviceId,
    device_id: DeviceId,
    stats: ?*PartitionExecutor.ExecutionStats,
    pattern: RankAdapterBackwardPattern,
    skipped_nodes: []bool,
) !bool {
    const input = valueFor(values, pattern.input_id) orelse {
        traceRankAdapterBackwardExecutionDecline("missing_input", pattern);
        return false;
    };
    const after_a = valueFor(values, pattern.after_a_id) orelse {
        traceRankAdapterBackwardExecutionDecline("missing_after_a", pattern);
        return false;
    };
    const rhs_source = if (pattern.rhs_uses_transpose_value)
        (try rankAdapterBackwardTransposeValue(graph, cb, values, value_device, device_id, pattern)) orelse {
            traceRankAdapterBackwardExecutionDecline("missing_rhs_transpose", pattern);
            return false;
        }
    else
        valueFor(values, pattern.rhs_source_id) orelse {
            traceRankAdapterBackwardExecutionDecline("missing_rhs_source", pattern);
            return false;
        };
    const output_grad = valueFor(values, pattern.output_grad_id) orelse {
        traceRankAdapterBackwardExecutionDecline("missing_output_grad", pattern);
        return false;
    };
    const fused = (try cb.loraLinearBackward(&.{
        .input = input,
        .after_a = after_a,
        .lora_b = rhs_source,
        .output_grad = output_grad,
        .rows = pattern.rows,
        .in_dim = pattern.in_dim,
        .rank = pattern.rank,
        .out_dim = pattern.out_dim,
        .scale = 1.0,
    })) orelse {
        traceRankAdapterBackwardExecutionDecline("backend_miss", pattern);
        return false;
    };

    values[@intCast(pattern.d_after_a_id)] = fused.grad_after_a;
    value_device[@intCast(pattern.d_after_a_id)] = device_id;
    values[@intCast(pattern.grad_a_id)] = fused.grad_a;
    value_device[@intCast(pattern.grad_a_id)] = device_id;
    values[@intCast(pattern.grad_b_id)] = fused.grad_b;
    value_device[@intCast(pattern.grad_b_id)] = device_id;
    markRankAdapterBackwardSkipped(skipped_nodes, pattern);

    if (stats) |s| {
        recordMetalGraphRegion(s, .ffn, 5);
        s.metal_rank_adapter_backward_regions += 1;
        s.fused_graph_pattern_dispatches += 1;
        s.fused_graph_nodes_elided += 4;
        recordGemmaRuntimeResidency(s, graph, pattern.grad_a_id, isMetalResidentOrQuantizedDescriptor(cb, fused.grad_a));
        recordGemmaRuntimeResidency(s, graph, pattern.grad_b_id, isMetalResidentOrQuantizedDescriptor(cb, fused.grad_b));
    }
    if (traceMetalGraphFusionsEnabled() or traceRankAdapterBackwardMatchingEnabled()) {
        std.debug.print(
            "metal_graph_fusion_trace: rank_adapter_backward_region executed d_after_a={d} grad_a={d} grad_b={d} input={d} after_a={d} rhs_source={d} rhs_uses_transpose={} rows={d} in={d} rank={d} out={d}\n",
            .{ pattern.d_after_a_id, pattern.grad_a_id, pattern.grad_b_id, pattern.input_id, pattern.after_a_id, pattern.rhs_source_id, pattern.rhs_uses_transpose_value, pattern.rows, pattern.in_dim, pattern.rank, pattern.out_dim },
        );
    }
    return true;
}

fn rankAdapterBackwardTransposeValue(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    value_device: []DeviceId,
    device_id: DeviceId,
    pattern: RankAdapterBackwardPattern,
) !?CT {
    if (valueFor(values, pattern.rhs_transpose_id)) |value| return value;
    const transpose_node = graph.node(pattern.rhs_transpose_id);
    const attrs = switch (transpose_node.op) {
        .transpose => |transpose_attrs| transpose_attrs,
        else => return null,
    };
    if (transpose_node.num_inputs < 1 or transpose_node.inputs[0] != pattern.rhs_source_id) return null;
    const input = valueFor(values, pattern.rhs_source_id) orelse return null;
    var in_shape_buf: [ml.graph.shape.max_rank]i64 = undefined;
    const in_shape = try fillShapeDims(graph.node(pattern.rhs_source_id).output_shape, &in_shape_buf);
    var perm_buf: [ml.graph.shape.max_rank]u8 = undefined;
    const perm = transpose_utils.effectivePerm(attrs, graph.node(pattern.rhs_source_id).output_shape.rank(), &perm_buf);
    const value = cb.primTranspose(input, perm, in_shape) catch |err| switch (err) {
        error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => return null,
        else => return err,
    };
    values[@intCast(pattern.rhs_transpose_id)] = value;
    value_device[@intCast(pattern.rhs_transpose_id)] = device_id;
    return value;
}

fn traceRankAdapterBackwardExecutionDecline(reason: []const u8, pattern: RankAdapterBackwardPattern) void {
    if (!traceRankAdapterBackwardMatchingEnabled()) return;
    std.debug.print(
        "rank_adapter_backward_match: execution_declined reason={s} d_after_a={d} grad_a={d} grad_b={d} input={d} after_a={d} rhs_source={d} rhs_uses_transpose={} rows={d} in={d} rank={d} out={d}\n",
        .{ reason, pattern.d_after_a_id, pattern.grad_a_id, pattern.grad_b_id, pattern.input_id, pattern.after_a_id, pattern.rhs_source_id, pattern.rhs_uses_transpose_value, pattern.rows, pattern.in_dim, pattern.rank, pattern.out_dim },
    );
}

fn executeFfnGeluBackwardPattern(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    value_device: []DeviceId,
    device_id: DeviceId,
    stats: ?*PartitionExecutor.ExecutionStats,
    partition_plan: ?*const partition_mod.PartitionPlan,
    pattern: FfnGeluBackwardPattern,
    skipped_nodes: []bool,
) !bool {
    if (try executeFfnGeluBackwardOutputPattern(graph, cb, values, value_device, device_id, stats, pattern, skipped_nodes)) {
        return true;
    }
    if (try executeFfnGeluBackwardChainPattern(graph, cb, values, value_device, device_id, stats, pattern, skipped_nodes)) {
        return true;
    }

    const first_dot_node = graph.node(pattern.first_dot_id);
    const first_attrs = switch (first_dot_node.op) {
        .dot_general => |attrs| attrs,
        else => return false,
    };
    const first = (try executeRuntimeDotGeneral(graph, cb, values, first_dot_node.getInputs(), first_attrs, operatorPlanForRegionNode(partition_plan, pattern.first_dot_id))) orelse return false;

    values[@intCast(pattern.first_dot_id)] = first;
    value_device[@intCast(pattern.first_dot_id)] = device_id;

    const second_branch_node = graph.node(pattern.second_branch_dot_id);
    const second_branch_attrs = switch (second_branch_node.op) {
        .dot_general => |attrs| attrs,
        else => return false,
    };
    const second_branch = (try executeRuntimeDotGeneral(graph, cb, values, second_branch_node.getInputs(), second_branch_attrs, operatorPlanForRegionNode(partition_plan, pattern.second_branch_dot_id))) orelse {
        values[@intCast(pattern.first_dot_id)] = null;
        cb.free(first);
        return false;
    };
    values[@intCast(pattern.second_branch_dot_id)] = second_branch;
    value_device[@intCast(pattern.second_branch_dot_id)] = device_id;

    const add_node = graph.node(pattern.upstream_add_id);
    const upstream = (try executeRuntimeAdd(graph, cb, values, add_node.getInputs(), add_node.output_shape, null)) orelse {
        values[@intCast(pattern.second_branch_dot_id)] = null;
        values[@intCast(pattern.first_dot_id)] = null;
        cb.free(second_branch);
        cb.free(first);
        return false;
    };
    values[@intCast(pattern.upstream_add_id)] = upstream;
    value_device[@intCast(pattern.upstream_add_id)] = device_id;

    const gelu_node = graph.node(pattern.gelu_backward_id);
    const gelu = (try executeRuntimeGeluBackward(cb, values, gelu_node.getInputs(), gelu_node.output_shape, pattern.exact)) orelse {
        values[@intCast(pattern.upstream_add_id)] = null;
        values[@intCast(pattern.second_branch_dot_id)] = null;
        values[@intCast(pattern.first_dot_id)] = null;
        cb.free(upstream);
        cb.free(second_branch);
        cb.free(first);
        return false;
    };
    values[@intCast(pattern.gelu_backward_id)] = gelu;
    value_device[@intCast(pattern.gelu_backward_id)] = device_id;

    const output_dot_node = graph.node(pattern.output_dot_id);
    const output_attrs = switch (output_dot_node.op) {
        .dot_general => |attrs| attrs,
        else => return false,
    };
    const output = (try executeRuntimeDotGeneral(graph, cb, values, output_dot_node.getInputs(), output_attrs, operatorPlanForRegionNode(partition_plan, pattern.output_dot_id))) orelse {
        values[@intCast(pattern.gelu_backward_id)] = null;
        values[@intCast(pattern.upstream_add_id)] = null;
        values[@intCast(pattern.second_branch_dot_id)] = null;
        values[@intCast(pattern.first_dot_id)] = null;
        cb.free(gelu);
        cb.free(upstream);
        cb.free(second_branch);
        cb.free(first);
        return false;
    };

    values[@intCast(pattern.output_dot_id)] = output;
    value_device[@intCast(pattern.output_dot_id)] = device_id;
    markFfnGeluBackwardSkipped(skipped_nodes, pattern);

    if (stats) |s| {
        recordMetalGraphRegion(s, .ffn, 5);
        s.metal_ffn_gelu_backward_regions += 1;
        s.fused_graph_pattern_dispatches += 1;
        s.fused_graph_nodes_elided += 4;
        recordGemmaRuntimeResidency(s, graph, pattern.output_dot_id, isMetalResidentOrQuantizedDescriptor(cb, output));
    }
    if (traceMetalGraphFusionsEnabled()) {
        std.debug.print(
            "metal_graph_fusion_trace: ffn_gelu_backward_region executed first={d} second_branch={d} add={d} gelu={d} output={d} rows={d} hidden={d} intermediate={d}\n",
            .{ pattern.first_dot_id, pattern.second_branch_dot_id, pattern.upstream_add_id, pattern.gelu_backward_id, pattern.output_dot_id, pattern.rows, pattern.hidden_size, pattern.intermediate_size },
        );
    }
    return true;
}

fn executeFfnGeluBackwardOutputPattern(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    value_device: []DeviceId,
    device_id: DeviceId,
    stats: ?*PartitionExecutor.ExecutionStats,
    pattern: FfnGeluBackwardPattern,
    skipped_nodes: []bool,
) !bool {
    if (platform.env.getenvBoolDefault("TERMITE_METAL_DISABLE_FFN_GELU_BACKWARD_OUTPUT_CHAIN", false)) {
        traceFfnGeluBackwardOutputDecline("disabled", pattern, 0, 0);
        return false;
    }

    const first_dot_node = graph.node(pattern.first_dot_id);
    const first_attrs = switch (first_dot_node.op) {
        .dot_general => |attrs| attrs,
        else => {
            traceFfnGeluBackwardOutputDecline("first_not_dot", pattern, 0, 0);
            return false;
        },
    };
    const first = (try runtimeDot2DInputs(graph, values, first_dot_node.getInputs(), first_attrs)) orelse {
        traceFfnGeluBackwardDot2DDecline(graph, "first_inputs", pattern.first_dot_id, first_dot_node.getInputs(), first_attrs);
        traceFfnGeluBackwardOutputDecline("first_inputs", pattern, 0, 0);
        return false;
    };
    if (first.k != 1) {
        traceFfnGeluBackwardOutputDecline("first_k", pattern, first.k, 0);
        return false;
    }

    const second_branch_node = graph.node(pattern.second_branch_dot_id);
    const second_branch_attrs = switch (second_branch_node.op) {
        .dot_general => |attrs| attrs,
        else => {
            traceFfnGeluBackwardOutputDecline("second_not_dot", pattern, first.k, 0);
            return false;
        },
    };
    const second_branch = (try runtimeDot2DInputs(graph, values, second_branch_node.getInputs(), second_branch_attrs)) orelse {
        traceFfnGeluBackwardDot2DDecline(graph, "second_inputs", pattern.second_branch_dot_id, second_branch_node.getInputs(), second_branch_attrs);
        traceFfnGeluBackwardOutputDecline("second_inputs", pattern, first.k, 0);
        return false;
    };

    const gelu_node = graph.node(pattern.gelu_backward_id);
    if (gelu_node.num_inputs < 2) {
        traceFfnGeluBackwardOutputDecline("gelu_arity", pattern, first.k, second_branch.k);
        return false;
    }
    const gelu_input = valueFor(values, gelu_node.inputs[0]) orelse {
        traceFfnGeluBackwardOutputDecline("gelu_input", pattern, first.k, second_branch.k);
        return false;
    };

    const output_dot_node = graph.node(pattern.output_dot_id);
    const output_attrs = switch (output_dot_node.op) {
        .dot_general => |attrs| attrs,
        else => {
            traceFfnGeluBackwardOutputDecline("output_not_dot", pattern, first.k, second_branch.k);
            return false;
        },
    };
    const output_dot = (try runtimeDot2DRhs(graph, values, output_dot_node.getInputs(), output_attrs)) orelse {
        traceFfnGeluBackwardDot2DDecline(graph, "output_rhs", pattern.output_dot_id, output_dot_node.getInputs(), output_attrs);
        traceFfnGeluBackwardOutputDecline("output_rhs", pattern, first.k, second_branch.k);
        return false;
    };

    const result = cb.decoderRuntimeFfnGeluBackwardOutput(&.{
        .first_lhs = first.lhs,
        .first_rhs = first.rhs,
        .first_rhs_contract_axis = first.rhs_contract_axis,
        .second_lhs = second_branch.lhs,
        .second_rhs = second_branch.rhs,
        .second_rhs_contract_axis = second_branch.rhs_contract_axis,
        .gelu_input = gelu_input,
        .output_rhs = output_dot.rhs,
        .output_rhs_contract_axis = output_dot.rhs_contract_axis,
        .rows = pattern.rows,
        .hidden_size = pattern.hidden_size,
        .intermediate_size = pattern.intermediate_size,
        .first_k = first.k,
        .second_k = second_branch.k,
        .exact = pattern.exact,
    }) catch |err| switch (err) {
        error.UnsupportedOperation, error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => null,
        else => return err,
    } orelse {
        traceFfnGeluBackwardOutputDecline("runtime_null", pattern, first.k, second_branch.k);
        return false;
    };

    values[@intCast(pattern.first_dot_id)] = result.first;
    values[@intCast(pattern.gelu_backward_id)] = result.gelu;
    values[@intCast(pattern.output_dot_id)] = result.output;
    value_device[@intCast(pattern.first_dot_id)] = device_id;
    value_device[@intCast(pattern.gelu_backward_id)] = device_id;
    value_device[@intCast(pattern.output_dot_id)] = device_id;
    markFfnGeluBackwardSkipped(skipped_nodes, pattern);

    if (stats) |s| {
        recordMetalGraphRegion(s, .ffn, 5);
        s.metal_ffn_gelu_backward_regions += 1;
        s.fused_graph_pattern_dispatches += 1;
        s.fused_graph_nodes_elided += 4;
        recordGemmaRuntimeResidency(s, graph, pattern.output_dot_id, isMetalResidentOrQuantizedDescriptor(cb, result.output));
    }
    if (traceMetalGraphFusionsEnabled()) {
        std.debug.print(
            "metal_graph_fusion_trace: ffn_gelu_backward_output_chain executed first={d} second_branch={d} add={d} gelu={d} output={d} rows={d} hidden={d} intermediate={d}\n",
            .{ pattern.first_dot_id, pattern.second_branch_dot_id, pattern.upstream_add_id, pattern.gelu_backward_id, pattern.output_dot_id, pattern.rows, pattern.hidden_size, pattern.intermediate_size },
        );
    }
    return true;
}

fn traceFfnGeluBackwardOutputDecline(reason: []const u8, pattern: FfnGeluBackwardPattern, first_k: usize, second_k: usize) void {
    if (!traceMetalGraphFusionsEnabled()) return;
    std.debug.print(
        "metal_graph_fusion_trace: ffn_gelu_backward_output_chain declined reason={s} first={d} second_branch={d} add={d} gelu={d} output={d} rows={d} hidden={d} intermediate={d} first_k={d} second_k={d}\n",
        .{ reason, pattern.first_dot_id, pattern.second_branch_dot_id, pattern.upstream_add_id, pattern.gelu_backward_id, pattern.output_dot_id, pattern.rows, pattern.hidden_size, pattern.intermediate_size, first_k, second_k },
    );
}

fn traceFfnGeluBackwardDot2DDecline(
    graph: *const Graph,
    reason: []const u8,
    node_id: NodeId,
    inputs: []const NodeId,
    attrs: anytype,
) void {
    if (!traceMetalGraphFusionsEnabled()) return;
    const lhs_id = if (inputs.len > 0) inputs[0] else null_node;
    const rhs_id = if (inputs.len > 1) inputs[1] else null_node;
    std.debug.print(
        "metal_graph_fusion_trace: ffn_gelu_backward_dot2d declined reason={s} node={d} inputs={d} lhs={d} rhs={d} num_contracting={d} num_batch={d} lhs_contract0={d} rhs_contract0={d} lhs_rank={d} lhs0={d} lhs1={d} rhs_rank={d} rhs0={d} rhs1={d}\n",
        .{
            reason,
            node_id,
            inputs.len,
            lhs_id,
            rhs_id,
            attrs.num_contracting,
            attrs.num_batch,
            if (attrs.num_contracting > 0) attrs.lhs_contracting[0] else 255,
            if (attrs.num_contracting > 0) attrs.rhs_contracting[0] else 255,
            shapeRankForNodeOr(graph, lhs_id, 0),
            shapeDimForNodeOr(graph, lhs_id, 0, -1),
            shapeDimForNodeOr(graph, lhs_id, 1, -1),
            shapeRankForNodeOr(graph, rhs_id, 0),
            shapeDimForNodeOr(graph, rhs_id, 0, -1),
            shapeDimForNodeOr(graph, rhs_id, 1, -1),
        },
    );
}

fn operatorPlanForRegionNode(partition_plan: ?*const partition_mod.PartitionPlan, node_id: NodeId) ?OperatorPlan {
    const plan = partition_plan orelse return null;
    return plan.operatorPlanForNode(node_id);
}

fn executeFfnGeluBackwardChainPattern(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    value_device: []DeviceId,
    device_id: DeviceId,
    stats: ?*PartitionExecutor.ExecutionStats,
    pattern: FfnGeluBackwardPattern,
    skipped_nodes: []bool,
) !bool {
    if (platform.env.getenvBoolDefault("TERMITE_METAL_DISABLE_FFN_GELU_BACKWARD_CHAIN", false)) return false;
    if (!platform.env.getenvBoolDefault("TERMITE_METAL_ENABLE_FFN_GELU_BACKWARD_CHAIN", false) and
        !(platform.env.getenvBoolDefault("TERMITE_ENABLE_TRAINING_GRAPH_EXECUTOR", false) and
            !platform.env.getenvBoolDefault("TERMITE_DISABLE_TRAINING_GRAPH_EXECUTOR", false)))
    {
        return false;
    }

    const first_dot_node = graph.node(pattern.first_dot_id);
    const first_attrs = switch (first_dot_node.op) {
        .dot_general => |attrs| attrs,
        else => return false,
    };
    const first = (try runtimeDot2DInputs(graph, values, first_dot_node.getInputs(), first_attrs)) orelse return false;

    const second_branch_node = graph.node(pattern.second_branch_dot_id);
    const second_branch_attrs = switch (second_branch_node.op) {
        .dot_general => |attrs| attrs,
        else => return false,
    };
    const second_branch = (try runtimeDot2DInputs(graph, values, second_branch_node.getInputs(), second_branch_attrs)) orelse return false;

    const gelu_node = graph.node(pattern.gelu_backward_id);
    if (gelu_node.num_inputs < 2) return false;
    const gelu_input = valueFor(values, gelu_node.inputs[0]) orelse return false;

    const output_dot_node = graph.node(pattern.output_dot_id);
    const output_attrs = switch (output_dot_node.op) {
        .dot_general => |attrs| attrs,
        else => return false,
    };
    const output_dot = (try runtimeDot2DRhs(graph, values, output_dot_node.getInputs(), output_attrs)) orelse return false;

    const chain = cb.decoderRuntimeFfnGeluBackwardChain(&.{
        .first_lhs = first.lhs,
        .first_rhs = first.rhs,
        .first_rhs_contract_axis = first.rhs_contract_axis,
        .second_lhs = second_branch.lhs,
        .second_rhs = second_branch.rhs,
        .second_rhs_contract_axis = second_branch.rhs_contract_axis,
        .gelu_input = gelu_input,
        .output_rhs = output_dot.rhs,
        .output_rhs_contract_axis = output_dot.rhs_contract_axis,
        .rows = pattern.rows,
        .hidden_size = pattern.hidden_size,
        .intermediate_size = pattern.intermediate_size,
        .first_k = first.k,
        .second_k = second_branch.k,
        .exact = pattern.exact,
    }) catch |err| switch (err) {
        error.UnsupportedOperation, error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => null,
        else => return err,
    } orelse return false;

    values[@intCast(pattern.first_dot_id)] = chain.first;
    values[@intCast(pattern.second_branch_dot_id)] = chain.second_branch;
    values[@intCast(pattern.upstream_add_id)] = chain.upstream;
    values[@intCast(pattern.gelu_backward_id)] = chain.gelu;
    values[@intCast(pattern.output_dot_id)] = chain.output;
    value_device[@intCast(pattern.first_dot_id)] = device_id;
    value_device[@intCast(pattern.second_branch_dot_id)] = device_id;
    value_device[@intCast(pattern.upstream_add_id)] = device_id;
    value_device[@intCast(pattern.gelu_backward_id)] = device_id;
    value_device[@intCast(pattern.output_dot_id)] = device_id;
    markFfnGeluBackwardSkipped(skipped_nodes, pattern);

    if (stats) |s| {
        recordMetalGraphRegion(s, .ffn, 5);
        s.metal_ffn_gelu_backward_regions += 1;
        s.fused_graph_pattern_dispatches += 1;
        s.fused_graph_nodes_elided += 4;
        recordGemmaRuntimeResidency(s, graph, pattern.output_dot_id, isMetalResidentOrQuantizedDescriptor(cb, chain.output));
    }
    if (traceMetalGraphFusionsEnabled()) {
        std.debug.print(
            "metal_graph_fusion_trace: ffn_gelu_backward_chain executed first={d} second_branch={d} add={d} gelu={d} output={d} rows={d} hidden={d} intermediate={d}\n",
            .{ pattern.first_dot_id, pattern.second_branch_dot_id, pattern.upstream_add_id, pattern.gelu_backward_id, pattern.output_dot_id, pattern.rows, pattern.hidden_size, pattern.intermediate_size },
        );
    }
    return true;
}

fn runtimeDot2DInputs(
    graph: *const Graph,
    values: []?CT,
    inputs: []const NodeId,
    attrs: anytype,
) !?RuntimeDot2DInputs {
    if (inputs.len < 2) return null;
    if (attrs.num_contracting != 1 or attrs.num_batch != 0) return null;
    const lhs_contracting = attrs.lhs_contracting[0];
    const rhs_contracting = attrs.rhs_contracting[0];
    if (lhs_contracting != 1 or (rhs_contracting != 0 and rhs_contracting != 1)) return null;

    var lhs_shape_buf: [ml.graph.shape.max_rank]i64 = undefined;
    var rhs_shape_buf: [ml.graph.shape.max_rank]i64 = undefined;
    const lhs_shape = try fillShapeDims(graph.node(inputs[0]).output_shape, &lhs_shape_buf);
    const rhs_shape = try fillShapeDims(graph.node(inputs[1]).output_shape, &rhs_shape_buf);
    if (lhs_shape.len != 2 or rhs_shape.len != 2) return null;
    if (lhs_shape[0] <= 0 or lhs_shape[1] <= 0) return null;
    const rhs_axis: usize = @intCast(rhs_contracting);
    const rhs_k = rhs_shape[rhs_axis];
    if (rhs_k <= 0 or rhs_k != lhs_shape[1]) return null;
    const lhs = valueFor(values, inputs[0]) orelse return null;
    const rhs = resolvedRuntimeDot2DRhs(graph, values, inputs[1], rhs_contracting) orelse return null;
    return .{
        .lhs = lhs,
        .rhs = rhs.rhs,
        .rhs_contract_axis = rhs.rhs_contract_axis,
        .k = @intCast(lhs_shape[1]),
    };
}

fn runtimeDot2DRhs(
    graph: *const Graph,
    values: []?CT,
    inputs: []const NodeId,
    attrs: anytype,
) !?RuntimeDot2DRhs {
    if (inputs.len < 2) return null;
    if (attrs.num_contracting != 1 or attrs.num_batch != 0) return null;
    const lhs_contracting = attrs.lhs_contracting[0];
    const rhs_contracting = attrs.rhs_contracting[0];
    if (lhs_contracting != 1 or (rhs_contracting != 0 and rhs_contracting != 1)) return null;

    var lhs_shape_buf: [ml.graph.shape.max_rank]i64 = undefined;
    var rhs_shape_buf: [ml.graph.shape.max_rank]i64 = undefined;
    const lhs_shape = try fillShapeDims(graph.node(inputs[0]).output_shape, &lhs_shape_buf);
    const rhs_shape = try fillShapeDims(graph.node(inputs[1]).output_shape, &rhs_shape_buf);
    if (lhs_shape.len != 2 or rhs_shape.len != 2) return null;
    if (lhs_shape[0] <= 0 or lhs_shape[1] <= 0) return null;
    const rhs_axis: usize = @intCast(rhs_contracting);
    const rhs_k = rhs_shape[rhs_axis];
    if (rhs_k <= 0 or rhs_k != lhs_shape[1]) return null;
    const rhs = resolvedRuntimeDot2DRhs(graph, values, inputs[1], rhs_contracting) orelse return null;
    return .{
        .rhs = rhs.rhs,
        .rhs_contract_axis = rhs.rhs_contract_axis,
        .k = @intCast(lhs_shape[1]),
    };
}

fn groupedHeadDotCandidate(
    graph: *const Graph,
    values: []?CT,
    node_id: NodeId,
    reachable: []const bool,
    skipped_nodes: []const bool,
) !?GroupedHeadDotCandidate {
    if (node_id == null_node or node_id >= graph.nodeCount()) return null;
    const raw_id: usize = @intCast(node_id);
    if (raw_id >= values.len) return null;
    if (raw_id >= reachable.len or !reachable[raw_id]) return null;
    if (raw_id < skipped_nodes.len and skipped_nodes[raw_id]) return null;
    if (values[raw_id] != null) return null;
    const node = graph.node(node_id);
    const attrs = switch (node.op) {
        .dot_general => |a| a,
        else => return null,
    };
    if (!std.mem.eql(u8, commandSourceClassification(graph, node_id, 4).family, "head")) return null;
    if (node.output_shape.rank() != 2) return null;
    const m = shapeDimUsize(node.output_shape, 0) orelse return null;
    const n = shapeDimUsize(node.output_shape, 1) orelse return null;
    const dot = (try runtimeDot2DInputs(graph, values, node.getInputs(), attrs)) orelse return null;
    // ponytail: exact production head-dot shape only; widen after the gate proves it helps.
    if (m != 96 or n != 2304 or dot.k != 768) return null;
    return .{
        .node_id = node_id,
        .lhs = dot.lhs,
        .rhs = dot.rhs,
        .m = m,
        .n = n,
        .k = dot.k,
        .rhs_contract_axis = dot.rhs_contract_axis,
    };
}

fn tryExecuteGroupedHeadDotPattern(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    value_device: []DeviceId,
    node_ids: []const NodeId,
    node_pos: usize,
    reachable: []const bool,
    device_id: DeviceId,
    skipped_nodes: []bool,
    elision_protected_nodes: []const bool,
    runtime_region_plan: RuntimeRegionPlan,
    stats: ?*PartitionExecutor.ExecutionStats,
) !bool {
    if (!groupedHeadDotRuntimeCommandEnabled() or metalPartitionRuntimeCommandsDisabled()) return false;
    if (node_pos >= node_ids.len) return false;

    var candidates: [8]GroupedHeadDotCandidate = undefined;
    const first = (try groupedHeadDotCandidate(graph, values, node_ids[node_pos], reachable, skipped_nodes)) orelse return false;
    if (std.meta.activeTag(runtime_region_plan.regionAt(node_pos, first.node_id, node_ids)) != .none) return false;
    candidates[0] = first;
    var count: usize = 1;

    const scan_end = @min(node_ids.len, node_pos + 64);
    var scan_pos = node_pos + 1;
    while (scan_pos < scan_end and count < candidates.len) : (scan_pos += 1) {
        const candidate = (try groupedHeadDotCandidate(graph, values, node_ids[scan_pos], reachable, skipped_nodes)) orelse continue;
        if (candidate.m != first.m or candidate.n != first.n or candidate.k != first.k or candidate.rhs_contract_axis != first.rhs_contract_axis) continue;
        if (std.meta.activeTag(runtime_region_plan.regionAt(scan_pos, candidate.node_id, node_ids)) != .none) continue;
        const raw_id: usize = @intCast(candidate.node_id);
        if (raw_id < elision_protected_nodes.len and elision_protected_nodes[raw_id]) continue;
        candidates[count] = candidate;
        count += 1;
    }
    if (count < 2) return false;

    var lhs: [8]CT = undefined;
    var rhs: [8]CT = undefined;
    for (candidates[0..count], 0..) |candidate, idx| {
        lhs[idx] = candidate.lhs;
        rhs[idx] = candidate.rhs;
    }
    const result = (try cb.dotGeneral2DMany(&.{
        .allocator = allocator,
        .lhs = lhs[0..count],
        .rhs = rhs[0..count],
        .m = first.m,
        .n = first.n,
        .k = first.k,
        .rhs_contract_axis = first.rhs_contract_axis,
    })) orelse return false;
    defer allocator.free(result.outputs);
    if (result.outputs.len != count) {
        for (result.outputs) |ct| cb.free(ct);
        return false;
    }

    for (candidates[0..count], 0..) |candidate, idx| {
        const raw_id: usize = @intCast(candidate.node_id);
        values[raw_id] = result.outputs[idx];
        value_device[raw_id] = device_id;
        if (idx > 0 and raw_id < skipped_nodes.len) skipped_nodes[raw_id] = true;
    }
    if (stats) |s| {
        recordMetalGraphRegion(s, .tail, @intCast(count));
        s.fused_graph_pattern_dispatches += 1;
        s.fused_graph_nodes_elided += @intCast(count - 1);
        recordGemmaRuntimeResidency(s, graph, first.node_id, isMetalResidentOrQuantizedDescriptor(cb, result.outputs[0]));
    }
    if (traceMetalGraphFusionsEnabled()) {
        std.debug.print(
            "metal_graph_fusion_trace: grouped_head_dot executed first={d} count={d} m={d} n={d} k={d} rhs_axis={d}\n",
            .{ first.node_id, count, first.m, first.n, first.k, first.rhs_contract_axis },
        );
    }
    return true;
}

fn resolvedRuntimeDot2DRhs(
    graph: *const Graph,
    values: []?CT,
    rhs_id: NodeId,
    rhs_contracting: u32,
) ?RuntimeDot2DResolvedRhs {
    if (valueFor(values, rhs_id)) |rhs| {
        return .{ .rhs = rhs, .rhs_contract_axis = rhs_contracting };
    }
    if (rhs_id == null_node or rhs_id >= graph.nodeCount()) return null;
    const rhs_node = graph.node(rhs_id);
    const transpose_attrs = switch (rhs_node.op) {
        .transpose => |attrs| attrs,
        else => return null,
    };
    if (rhs_node.num_inputs == 0 or rhs_node.inputs[0] == null_node) return null;
    const source_id = rhs_node.inputs[0];
    if (source_id >= graph.nodeCount()) return null;
    if (!transposeIsSimple2D(transpose_attrs, graph.node(source_id).output_shape)) return null;
    const source = valueFor(values, source_id) orelse return null;
    const source_contract_axis: u32 = switch (rhs_contracting) {
        0 => 1,
        1 => 0,
        else => return null,
    };
    return .{ .rhs = source, .rhs_contract_axis = source_contract_axis };
}

fn matchDebertaEncoderLoraLayerPattern(
    graph: *const Graph,
    node_ids: []const NodeId,
    node_pos: usize,
    reachable: []const bool,
    skipped_nodes: []const bool,
    regions: []const RuntimeRegion,
) ?DebertaEncoderLoraLayerPattern {
    _ = regions;
    const ffn = matchDebertaFfnForwardPattern(graph, node_ids, node_pos, reachable, skipped_nodes) orelse return null;
    if (ffn.first_lora == null or ffn.second_lora == null) return traceDebertaEncoderLoraLayerMatchDecline("missing_ffn_lora", ffn.first_dot_id, null);
    const layer_index = layerIndexForWeight(graph, ffn.first_weight_id) orelse return traceDebertaEncoderLoraLayerMatchDecline("ffn_layer", ffn.first_dot_id, null);

    const residual_add_id = singleAddConsumerForInput(graph, ffn.output_add_id, reachable, skipped_nodes) orelse return traceDebertaEncoderLoraLayerMatchDecline("missing_residual", ffn.output_add_id, null);
    const residual_node = graph.node(residual_add_id);
    if (residual_node.num_inputs < 2) return traceDebertaEncoderLoraLayerMatchDecline("residual_inputs", residual_add_id, null);
    const residual_other = if (residual_node.inputs[0] == ffn.output_add_id)
        residual_node.inputs[1]
    else if (residual_node.inputs[1] == ffn.output_add_id)
        residual_node.inputs[0]
    else
        return traceDebertaEncoderLoraLayerMatchDecline("residual_output_input", residual_add_id, null);
    if (residual_other != ffn.input_id) return traceDebertaEncoderLoraLayerMatchDecline("residual_skip_input", residual_add_id, residual_other);

    const layer_norm = matchLayerNormFromInput(graph, residual_add_id, reachable, skipped_nodes) orelse {
        traceLayerNormConsumerChain(graph, residual_add_id, reachable, skipped_nodes);
        return .{
            .ffn = ffn,
            .residual_add_id = null_node,
            .layer_norm_id = null_node,
            .layer_norm_weight_id = null_node,
            .layer_norm_bias_id = null_node,
            .layer_index = layer_index,
            .norm_eps = 0,
            .residual_layer_norm_internal_only = false,
        };
    };
    if (layer_norm.dim != ffn.hidden_size) return traceDebertaEncoderLoraLayerMatchDecline("layer_norm_dim", layer_norm.output_id, null);
    if (layerIndexForWeight(graph, layer_norm.weight_id) != layer_index) return traceDebertaEncoderLoraLayerMatchDecline("layer_norm_layer", layer_norm.output_id, layer_norm.weight_id);
    if (!isDebertaOutputLayerNormName(graph.parameterName(graph.node(layer_norm.weight_id)))) return traceDebertaEncoderLoraLayerMatchDecline("layer_norm_weight_name", layer_norm.output_id, layer_norm.weight_id);
    if (!isDebertaOutputLayerNormName(graph.parameterName(graph.node(layer_norm.bias_id)))) return traceDebertaEncoderLoraLayerMatchDecline("layer_norm_bias_name", layer_norm.output_id, layer_norm.bias_id);
    const residual_layer_norm_internal_only = layer_norm.internal_node_count == 0 and
        !graphOutputContains(graph, residual_add_id) and
        hasOnlyExpectedUses(graph, reachable, skipped_nodes, residual_add_id, &.{layer_norm.output_id});

    return .{
        .ffn = ffn,
        .residual_add_id = residual_add_id,
        .layer_norm_id = layer_norm.output_id,
        .layer_norm_weight_id = layer_norm.weight_id,
        .layer_norm_bias_id = layer_norm.bias_id,
        .layer_index = layer_index,
        .norm_eps = layer_norm.eps,
        .residual_layer_norm_internal_only = residual_layer_norm_internal_only,
    };
}

fn matchLayerNormFromInput(
    graph: *const Graph,
    producer_id: NodeId,
    reachable: []const bool,
    skipped_nodes: []const bool,
) ?LayerNormForwardPattern {
    var found: ?LayerNormForwardPattern = null;
    var node_id: NodeId = 0;
    while (node_id < graph.nodeCount()) : (node_id += 1) {
        if (!isReachableUnskippedNode(reachable, skipped_nodes, node_id)) continue;
        const node = graph.node(node_id);
        const attrs = switch (node.op) {
            .fused_layer_norm => |a| a,
            else => continue,
        };
        if (node.num_inputs < 3 or node.inputs[0] != producer_id) continue;
        const weight_id = node.inputs[1];
        const bias_id = node.inputs[2];
        if (weight_id == null_node or bias_id == null_node) continue;
        if (std.meta.activeTag(graph.node(weight_id).op) != .parameter or
            std.meta.activeTag(graph.node(bias_id).op) != .parameter) continue;
        const weight_shape = graph.node(weight_id).output_shape;
        const bias_shape = graph.node(bias_id).output_shape;
        if (weight_shape.rank() != 1 or bias_shape.rank() != 1) continue;
        if (shapeDimUsize(weight_shape, 0) != attrs.dim or shapeDimUsize(bias_shape, 0) != attrs.dim) continue;
        if (found != null) return null;
        found = .{
            .output_id = node_id,
            .weight_id = weight_id,
            .bias_id = bias_id,
            .dim = attrs.dim,
            .eps = attrs.eps,
        };
    }
    return found orelse matchDecomposedLayerNormFromInput(graph, producer_id, reachable, skipped_nodes);
}

fn matchDecomposedLayerNormFromInput(
    graph: *const Graph,
    producer_id: NodeId,
    reachable: []const bool,
    skipped_nodes: []const bool,
) ?LayerNormForwardPattern {
    const input_shape = graph.node(producer_id).output_shape;
    if (input_shape.rank() != 2) return traceDecomposedLayerNormDecline("input_rank", producer_id, null);
    const hidden_size = shapeDimUsize(input_shape, 1) orelse return traceDecomposedLayerNormDecline("hidden_dim", producer_id, null);

    var saw_mean = false;
    var saw_mean_broadcast = false;
    var saw_centered = false;
    var saw_square = false;
    var saw_variance = false;
    var saw_variance_eps = false;
    var saw_norm_scale = false;
    var saw_normalized = false;
    var saw_param_shape_mismatch = false;

    var mean_id: NodeId = 0;
    while (mean_id < graph.nodeCount()) : (mean_id += 1) {
        if (!isUnaryConsumerByTag(graph, mean_id, producer_id, reachable, skipped_nodes, .reduce_mean)) continue;
        saw_mean = true;

        var mean_broadcast_id: NodeId = 0;
        while (mean_broadcast_id < graph.nodeCount()) : (mean_broadcast_id += 1) {
            if (!isUnaryConsumerByTag(graph, mean_broadcast_id, mean_id, reachable, skipped_nodes, .broadcast_in_dim)) continue;
            if (!shapesEqual(graph.node(mean_broadcast_id).output_shape, input_shape)) continue;
            saw_mean_broadcast = true;

            var centered_id: NodeId = 0;
            while (centered_id < graph.nodeCount()) : (centered_id += 1) {
                if (!isBinaryConsumerForPair(graph, centered_id, producer_id, mean_broadcast_id, reachable, skipped_nodes, .sub)) continue;
                saw_centered = true;

                var square_id: NodeId = 0;
                while (square_id < graph.nodeCount()) : (square_id += 1) {
                    if (!isBinaryConsumerForPair(graph, square_id, centered_id, centered_id, reachable, skipped_nodes, .mul)) continue;
                    saw_square = true;

                    var variance_id: NodeId = 0;
                    while (variance_id < graph.nodeCount()) : (variance_id += 1) {
                        if (!isUnaryConsumerByTag(graph, variance_id, square_id, reachable, skipped_nodes, .reduce_mean)) continue;
                        saw_variance = true;

                        var variance_eps_id: NodeId = 0;
                        while (variance_eps_id < graph.nodeCount()) : (variance_eps_id += 1) {
                            if (!isAddConsumerWithScalar(graph, variance_eps_id, variance_id, reachable, skipped_nodes)) continue;
                            saw_variance_eps = true;
                            const eps = scalarOtherInputF32(graph, variance_eps_id, variance_id) orelse continue;
                            const norm_scale = matchLayerNormScale(graph, variance_eps_id, input_shape, reachable, skipped_nodes) orelse continue;
                            saw_norm_scale = true;
                            const normalized = normalizedLayerNormOutputCandidate(graph, centered_id, norm_scale.broadcast_id, norm_scale.normalize_op, reachable, skipped_nodes) orelse continue;
                            saw_normalized = true;

                            const weight_shape = graph.node(normalized.scaled.param_id).output_shape;
                            const bias_shape = graph.node(normalized.output.param_id).output_shape;
                            if (weight_shape.rank() != 1 or bias_shape.rank() != 1) {
                                saw_param_shape_mismatch = true;
                                continue;
                            }
                            if (shapeDimUsize(weight_shape, 0) != hidden_size or shapeDimUsize(bias_shape, 0) != hidden_size) {
                                saw_param_shape_mismatch = true;
                                continue;
                            }

                            var internal_nodes = [_]NodeId{null_node} ** 16;
                            const internal_count: usize = 9;
                            internal_nodes[0] = mean_id;
                            internal_nodes[1] = mean_broadcast_id;
                            internal_nodes[2] = centered_id;
                            internal_nodes[3] = square_id;
                            internal_nodes[4] = variance_id;
                            internal_nodes[5] = variance_eps_id;
                            internal_nodes[6] = norm_scale.scale_id;
                            internal_nodes[7] = norm_scale.broadcast_id;
                            internal_nodes[8] = normalized.normalized_id;

                            return .{
                                .output_id = normalized.output.consumer_id,
                                .weight_id = normalized.scaled.param_id,
                                .bias_id = normalized.output.param_id,
                                .dim = hidden_size,
                                .eps = eps,
                                .internal_node_ids = internal_nodes,
                                .internal_node_count = internal_count,
                            };
                        }
                    }
                }
            }
        }
    }

    if (!saw_mean) return traceDecomposedLayerNormDecline("mean", producer_id, null);
    if (!saw_mean_broadcast) return traceDecomposedLayerNormDecline("mean_broadcast", producer_id, null);
    if (!saw_centered) return traceDecomposedLayerNormDecline("centered", producer_id, null);
    if (!saw_square) return traceDecomposedLayerNormDecline("square", producer_id, null);
    if (!saw_variance) return traceDecomposedLayerNormDecline("variance", producer_id, null);
    if (!saw_variance_eps) return traceDecomposedLayerNormDecline("variance_eps", producer_id, null);
    if (!saw_norm_scale) return traceDecomposedLayerNormDecline("norm_scale", producer_id, null);
    if (!saw_normalized) return traceDecomposedLayerNormDecline("normalized_output", producer_id, null);
    if (saw_param_shape_mismatch) return traceDecomposedLayerNormDecline("param_dim", producer_id, null);
    return traceDecomposedLayerNormDecline("unknown", producer_id, null);
}

fn traceDecomposedLayerNormDecline(reason: []const u8, producer_id: NodeId, detail_id: ?NodeId) ?LayerNormForwardPattern {
    if (!traceDebertaEncoderLoraLayerMatchingEnabled()) return null;
    std.debug.print(
        "deberta_encoder_lora_layer_match: decomposed_layer_norm declined reason={s} producer={d} detail={d}\n",
        .{ reason, producer_id, detail_id orelse null_node },
    );
    return null;
}

const OpTag = std.meta.Tag(@TypeOf(@as(*const Graph, undefined).node(0).op));

const BinaryParamConsumer = struct {
    consumer_id: NodeId,
    param_id: NodeId,
};

const NormalizedLayerNormCandidate = struct {
    normalized_id: NodeId,
    scaled: BinaryParamConsumer,
    output: BinaryParamConsumer,
};

const LayerNormScalePattern = struct {
    scale_id: NodeId,
    broadcast_id: NodeId,
    normalize_op: OpTag,
};

fn matchLayerNormScale(
    graph: *const Graph,
    variance_eps_id: NodeId,
    input_shape: ml.graph.Shape,
    reachable: []const bool,
    skipped_nodes: []const bool,
) ?LayerNormScalePattern {
    var rsqrt_id: NodeId = 0;
    while (rsqrt_id < graph.nodeCount()) : (rsqrt_id += 1) {
        if (!isUnaryConsumerByTag(graph, rsqrt_id, variance_eps_id, reachable, skipped_nodes, .rsqrt)) continue;
        var broadcast_id: NodeId = 0;
        while (broadcast_id < graph.nodeCount()) : (broadcast_id += 1) {
            if (!isUnaryConsumerByTag(graph, broadcast_id, rsqrt_id, reachable, skipped_nodes, .broadcast_in_dim)) continue;
            if (!shapesEqual(graph.node(broadcast_id).output_shape, input_shape)) continue;
            return .{
                .scale_id = rsqrt_id,
                .broadcast_id = broadcast_id,
                .normalize_op = .mul,
            };
        }
    }
    var sqrt_id: NodeId = 0;
    while (sqrt_id < graph.nodeCount()) : (sqrt_id += 1) {
        if (!isUnaryConsumerByTag(graph, sqrt_id, variance_eps_id, reachable, skipped_nodes, .sqrt)) continue;
        var broadcast_id: NodeId = 0;
        while (broadcast_id < graph.nodeCount()) : (broadcast_id += 1) {
            if (!isUnaryConsumerByTag(graph, broadcast_id, sqrt_id, reachable, skipped_nodes, .broadcast_in_dim)) continue;
            if (!shapesEqual(graph.node(broadcast_id).output_shape, input_shape)) continue;
            return .{
                .scale_id = sqrt_id,
                .broadcast_id = broadcast_id,
                .normalize_op = .div,
            };
        }
    }
    return null;
}

fn isUnaryConsumerByTag(
    graph: *const Graph,
    node_id: NodeId,
    producer_id: NodeId,
    reachable: []const bool,
    skipped_nodes: []const bool,
    tag: OpTag,
) bool {
    if (!isReachableUnskippedNode(reachable, skipped_nodes, node_id)) return false;
    const node = graph.node(node_id);
    return std.meta.activeTag(node.op) == tag and node.num_inputs >= 1 and node.inputs[0] == producer_id;
}

fn normalizedLayerNormOutputCandidate(
    graph: *const Graph,
    centered_id: NodeId,
    scale_broadcast_id: NodeId,
    normalize_op: OpTag,
    reachable: []const bool,
    skipped_nodes: []const bool,
) ?NormalizedLayerNormCandidate {
    var found: ?NormalizedLayerNormCandidate = null;
    var node_id: NodeId = 0;
    while (node_id < graph.nodeCount()) : (node_id += 1) {
        if (!isReachableUnskippedNode(reachable, skipped_nodes, node_id)) continue;
        const node = graph.node(node_id);
        if (std.meta.activeTag(node.op) != normalize_op or node.num_inputs < 2) continue;
        const exact = node.inputs[0] == centered_id and node.inputs[1] == scale_broadcast_id;
        const commuted = normalize_op != .div and node.inputs[0] == scale_broadcast_id and node.inputs[1] == centered_id;
        if (!exact and !commuted) continue;
        const scaled = binaryConsumerWithDebertaLayerNormParam(graph, node_id, reachable, skipped_nodes, .mul, "output.LayerNorm", ".weight") orelse continue;
        const output = binaryConsumerWithDebertaLayerNormParam(graph, scaled.consumer_id, reachable, skipped_nodes, .add, "output.LayerNorm", ".bias") orelse continue;
        found = .{
            .normalized_id = node_id,
            .scaled = scaled,
            .output = output,
        };
        break;
    }
    return found;
}

fn isBinaryConsumerForPair(
    graph: *const Graph,
    node_id: NodeId,
    lhs_id: NodeId,
    rhs_id: NodeId,
    reachable: []const bool,
    skipped_nodes: []const bool,
    tag: OpTag,
) bool {
    if (!isReachableUnskippedNode(reachable, skipped_nodes, node_id)) return false;
    const node = graph.node(node_id);
    if (std.meta.activeTag(node.op) != tag or node.num_inputs < 2) return false;
    const exact = node.inputs[0] == lhs_id and node.inputs[1] == rhs_id;
    const commuted = tag != .sub and node.inputs[0] == rhs_id and node.inputs[1] == lhs_id;
    return exact or commuted;
}

fn isAddConsumerWithScalar(
    graph: *const Graph,
    node_id: NodeId,
    producer_id: NodeId,
    reachable: []const bool,
    skipped_nodes: []const bool,
) bool {
    if (!isReachableUnskippedNode(reachable, skipped_nodes, node_id)) return false;
    const node = graph.node(node_id);
    if (std.meta.activeTag(node.op) != .add or node.num_inputs < 2) return false;
    if (node.inputs[0] == producer_id) return scalarConstantF32(graph, node.inputs[1]) != null;
    if (node.inputs[1] == producer_id) return scalarConstantF32(graph, node.inputs[0]) != null;
    return false;
}

fn scalarOtherInputF32(graph: *const Graph, binary_id: NodeId, producer_id: NodeId) ?f32 {
    if (binary_id == null_node or binary_id >= graph.nodeCount()) return null;
    const node = graph.node(binary_id);
    if (node.num_inputs < 2) return null;
    if (node.inputs[0] == producer_id) return scalarConstantF32(graph, node.inputs[1]);
    if (node.inputs[1] == producer_id) return scalarConstantF32(graph, node.inputs[0]);
    return null;
}

fn binaryConsumerWithDebertaLayerNormParam(
    graph: *const Graph,
    producer_id: NodeId,
    reachable: []const bool,
    skipped_nodes: []const bool,
    tag: OpTag,
    name_substring: []const u8,
    suffix: []const u8,
) ?BinaryParamConsumer {
    var found: ?BinaryParamConsumer = null;
    var node_id: NodeId = 0;
    while (node_id < graph.nodeCount()) : (node_id += 1) {
        if (!isReachableUnskippedNode(reachable, skipped_nodes, node_id)) continue;
        const node = graph.node(node_id);
        if (std.meta.activeTag(node.op) != tag or node.num_inputs < 2) continue;
        const param_id = if (node.inputs[0] == producer_id)
            node.inputs[1]
        else if (node.inputs[1] == producer_id)
            node.inputs[0]
        else
            continue;
        const source_param_id = debertaLayerNormParamSource(graph, param_id, name_substring, suffix) orelse continue;
        found = .{ .consumer_id = node_id, .param_id = source_param_id };
        break;
    }
    return found;
}

fn debertaLayerNormParamSource(graph: *const Graph, node_id: NodeId, substring: []const u8, suffix: []const u8) ?NodeId {
    if (node_id == null_node or node_id >= graph.nodeCount()) return null;
    const node = graph.node(node_id);
    if (parameterNameContainsSuffix(graph, node_id, substring, suffix)) return node_id;
    switch (node.op) {
        .broadcast_in_dim, .reshape => {
            if (node.num_inputs < 1) return null;
            const source_id = node.inputs[0];
            if (parameterNameContainsSuffix(graph, source_id, substring, suffix)) return source_id;
        },
        else => {},
    }
    return null;
}

fn parameterNameContainsSuffix(graph: *const Graph, node_id: NodeId, substring: []const u8, suffix: []const u8) bool {
    if (node_id == null_node or node_id >= graph.nodeCount()) return false;
    const node = graph.node(node_id);
    if (std.meta.activeTag(node.op) != .parameter) return false;
    const name = graph.parameterName(node);
    return std.mem.indexOf(u8, name, substring) != null and std.mem.endsWith(u8, name, suffix);
}

fn traceLayerNormConsumerChain(
    graph: *const Graph,
    producer_id: NodeId,
    reachable: []const bool,
    skipped_nodes: []const bool,
) void {
    if (!traceDebertaEncoderLoraLayerMatchingEnabled()) return;
    std.debug.print(
        "deberta_encoder_lora_layer_match: layer_norm_consumer_scan producer={d} producer_op={s} producer_rank={d}\n",
        .{ producer_id, @tagName(std.meta.activeTag(graph.node(producer_id).op)), graph.node(producer_id).output_shape.rank() },
    );

    var direct_count: usize = 0;
    var node_id: NodeId = 0;
    while (node_id < graph.nodeCount()) : (node_id += 1) {
        const node = graph.node(node_id);
        var consumes = false;
        for (node.getInputs()) |input_id| {
            if (input_id == producer_id) {
                consumes = true;
                break;
            }
        }
        if (!consumes) continue;
        direct_count += 1;
        std.debug.print(
            "deberta_encoder_lora_layer_match: layer_norm_consumer depth=1 node={d} op={s} reachable={} skipped={} inputs={d} rank={d} dim0={d} dim1={d}\n",
            .{
                node_id,
                @tagName(std.meta.activeTag(node.op)),
                isReachableUnskippedNode(reachable, skipped_nodes, node_id),
                node_id < skipped_nodes.len and skipped_nodes[@intCast(node_id)],
                node.num_inputs,
                node.output_shape.rank(),
                shapeDimForNodeOr(graph, node_id, 0, -1),
                shapeDimForNodeOr(graph, node_id, 1, -1),
            },
        );
        traceLayerNormConsumerSecondHop(graph, node_id, reachable, skipped_nodes);
    }
    std.debug.print(
        "deberta_encoder_lora_layer_match: layer_norm_consumer_scan producer={d} direct_consumers={d}\n",
        .{ producer_id, direct_count },
    );
}

fn traceLayerNormConsumerSecondHop(
    graph: *const Graph,
    producer_id: NodeId,
    reachable: []const bool,
    skipped_nodes: []const bool,
) void {
    var emitted: usize = 0;
    var node_id: NodeId = 0;
    while (node_id < graph.nodeCount()) : (node_id += 1) {
        if (emitted >= 8) return;
        const node = graph.node(node_id);
        var consumes = false;
        for (node.getInputs()) |input_id| {
            if (input_id == producer_id) {
                consumes = true;
                break;
            }
        }
        if (!consumes) continue;
        emitted += 1;
        std.debug.print(
            "deberta_encoder_lora_layer_match: layer_norm_consumer depth=2 parent={d} node={d} op={s} reachable={} skipped={} inputs={d} rank={d} dim0={d} dim1={d}\n",
            .{
                producer_id,
                node_id,
                @tagName(std.meta.activeTag(node.op)),
                isReachableUnskippedNode(reachable, skipped_nodes, node_id),
                node_id < skipped_nodes.len and skipped_nodes[@intCast(node_id)],
                node.num_inputs,
                node.output_shape.rank(),
                shapeDimForNodeOr(graph, node_id, 0, -1),
                shapeDimForNodeOr(graph, node_id, 1, -1),
            },
        );
    }
}

fn isDebertaOutputLayerNormName(name: []const u8) bool {
    return std.mem.indexOf(u8, name, "output.LayerNorm") != null;
}

fn traceDebertaEncoderLoraLayerMatchDecline(reason: []const u8, node_id: NodeId, detail_id: ?NodeId) ?DebertaEncoderLoraLayerPattern {
    if (!traceDebertaEncoderLoraLayerMatchingEnabled()) return null;
    std.debug.print(
        "deberta_encoder_lora_layer_match: declined reason={s} node={d} detail={d}\n",
        .{ reason, node_id, detail_id orelse null_node },
    );
    return null;
}

fn matchDebertaFfnForwardPattern(
    graph: *const Graph,
    node_ids: []const NodeId,
    node_pos: usize,
    reachable: []const bool,
    skipped_nodes: []const bool,
) ?DebertaFfnForwardPattern {
    const first = matchDebertaFfnLinearFromBaseDot(graph, node_ids, node_pos, reachable, skipped_nodes) orelse return null;
    if (first.rows == 0 or first.in_dim == 0 or first.out_dim == 0) return traceDebertaFfnForwardMatchDecline("zero_shape", node_ids[node_pos], null, null, null);
    if (first.in_dim >= first.out_dim) return null;
    traceDebertaFfnForwardMatchCandidate("first_linear", first);

    const gelu_id = singleExactGeluConsumerForInput(graph, first.output_id, reachable, skipped_nodes) orelse {
        traceDebertaFfnForwardConsumers(graph, reachable, skipped_nodes, first.output_id, "missing_gelu");
        return traceDebertaFfnForwardMatchDecline("missing_gelu", first.dot_id, first.output_id, null, null);
    };
    const gelu = graph.node(gelu_id);
    if (!gelu.output_shape.eq(graph.node(first.output_id).output_shape)) return traceDebertaFfnForwardMatchDecline("gelu_shape", first.dot_id, first.output_id, gelu_id, null);

    const output_dot_id = findDebertaFfnBaseDotConsumer(graph, gelu_id, first.rows, first.in_dim, reachable, skipped_nodes) orelse return traceDebertaFfnForwardMatchDecline("missing_output_dot", first.dot_id, first.output_id, gelu_id, null);
    const output_pos = findNodePos(node_ids, output_dot_id) orelse return traceDebertaFfnForwardMatchDecline("missing_output_pos", first.dot_id, first.output_id, gelu_id, output_dot_id);
    const second = matchDebertaFfnLinearFromBaseDot(graph, node_ids, output_pos, reachable, skipped_nodes) orelse return traceDebertaFfnForwardMatchDecline("bad_second_linear", first.dot_id, first.output_id, gelu_id, output_dot_id);
    if (second.input_id != gelu_id) return traceDebertaFfnForwardMatchDecline("second_input", first.dot_id, first.output_id, gelu_id, second.input_id);
    if (second.rows != first.rows or second.in_dim != first.out_dim or second.out_dim != first.in_dim) return traceDebertaFfnForwardMatchDecline("second_shape", first.dot_id, first.output_id, gelu_id, second.dot_id);
    traceDebertaFfnForwardMatchCandidate("matched", second);

    return .{
        .first_dot_id = first.dot_id,
        .first_base_add_id = first.base_add_id,
        .first_add_id = first.output_id,
        .gelu_id = gelu_id,
        .output_dot_id = second.dot_id,
        .output_base_add_id = second.base_add_id,
        .output_add_id = second.output_id,
        .input_id = first.input_id,
        .first_weight_id = first.weight_id,
        .first_bias_id = first.bias_id,
        .second_weight_id = second.weight_id,
        .second_bias_id = second.bias_id,
        .first_lora = first.lora,
        .second_lora = second.lora,
        .rows = first.rows,
        .hidden_size = first.in_dim,
        .intermediate_size = first.out_dim,
        .output_size = second.out_dim,
    };
}

fn matchHeadMlpForwardPattern(
    graph: *const Graph,
    node_ids: []const NodeId,
    node_pos: usize,
    reachable: []const bool,
    skipped_nodes: []const bool,
) ?DebertaFfnForwardPattern {
    const first = matchDebertaFfnLinearFromBaseDot(graph, node_ids, node_pos, reachable, skipped_nodes) orelse return null;
    if (first.rows == 0 or first.in_dim == 0 or first.out_dim == 0) return traceDebertaFfnForwardMatchDecline("head_zero_shape", node_ids[node_pos], null, null, null);
    const first_name = graph.parameterName(graph.node(first.weight_id));
    if (!isGlinerHeadMlpFirstWeightName(first_name)) return null;
    traceDebertaFfnForwardMatchCandidate("head_first_linear", first);

    var relu_candidate_count: usize = 0;
    var matched: ?DebertaFfnForwardPattern = null;
    var node_id: NodeId = 0;
    while (node_id < graph.nodeCount()) : (node_id += 1) {
        if (!isReachableUnskippedNode(reachable, skipped_nodes, node_id)) continue;
        const relu = matchReluActivationConsumerNode(graph, reachable, skipped_nodes, node_id, first.output_id) orelse continue;
        relu_candidate_count += 1;
        const relu_node = graph.node(relu.output_id);
        if (!relu_node.output_shape.eq(graph.node(first.output_id).output_shape)) {
            _ = traceDebertaFfnForwardMatchDecline("head_relu_shape", first.dot_id, first.output_id, relu.output_id, null);
            continue;
        }

        const output_dot_id = findHeadMlpBaseDotConsumer(graph, relu.output_id, first.rows, reachable, skipped_nodes) orelse {
            _ = traceDebertaFfnForwardMatchDecline("head_missing_output_dot", first.dot_id, first.output_id, relu.output_id, null);
            continue;
        };
        const output_pos = findNodePos(node_ids, output_dot_id) orelse {
            _ = traceDebertaFfnForwardMatchDecline("head_missing_output_pos", first.dot_id, first.output_id, relu.output_id, output_dot_id);
            continue;
        };
        const second = matchDebertaFfnLinearFromBaseDot(graph, node_ids, output_pos, reachable, skipped_nodes) orelse {
            _ = traceDebertaFfnForwardMatchDecline("head_bad_second_linear", first.dot_id, first.output_id, relu.output_id, output_dot_id);
            continue;
        };
        const second_name = graph.parameterName(graph.node(second.weight_id));
        if (!isGlinerHeadMlpWeightPair(first_name, second_name)) {
            _ = traceDebertaFfnForwardMatchDecline("head_weight_pair", first.dot_id, first.output_id, relu.output_id, second.dot_id);
            continue;
        }
        if (second.input_id != relu.output_id) {
            _ = traceDebertaFfnForwardMatchDecline("head_second_input", first.dot_id, first.output_id, relu.output_id, second.input_id);
            continue;
        }
        if (second.rows != first.rows or second.in_dim != first.out_dim) {
            _ = traceDebertaFfnForwardMatchDecline("head_second_shape", first.dot_id, first.output_id, relu.output_id, second.dot_id);
            continue;
        }
        if (matched != null) return traceDebertaFfnForwardMatchDecline("head_ambiguous_match", first.dot_id, first.output_id, relu.output_id, second.dot_id);
        traceDebertaFfnForwardMatchCandidate("head_matched", second);
        matched = .{
            .first_dot_id = first.dot_id,
            .first_base_add_id = first.base_add_id,
            .first_add_id = first.output_id,
            .gelu_id = relu.output_id,
            .output_dot_id = second.dot_id,
            .output_base_add_id = second.base_add_id,
            .output_add_id = second.output_id,
            .input_id = first.input_id,
            .first_weight_id = first.weight_id,
            .first_bias_id = first.bias_id,
            .second_weight_id = second.weight_id,
            .second_bias_id = second.bias_id,
            .activation_aux_id = relu.aux_id,
            .first_lora = first.lora,
            .second_lora = second.lora,
            .rows = first.rows,
            .hidden_size = first.in_dim,
            .intermediate_size = first.out_dim,
            .output_size = second.out_dim,
            .activation = .relu,
            .is_head_mlp = true,
        };
    }
    if (matched) |pattern| return pattern;
    if (relu_candidate_count == 0) {
        traceDebertaFfnForwardConsumers(graph, reachable, skipped_nodes, first.output_id, "head_missing_relu");
        return traceDebertaFfnForwardMatchDecline("head_missing_relu", first.dot_id, first.output_id, null, null);
    }
    return traceDebertaFfnForwardMatchDecline("head_no_matching_output", first.dot_id, first.output_id, null, null);
}

fn isGlinerHeadMlpFirstWeightName(name: []const u8) bool {
    return std.mem.endsWith(u8, name, "classifier.0.weight") or
        std.mem.endsWith(u8, name, "count_pred.0.weight") or
        std.mem.endsWith(u8, name, "span_rep.span_rep_layer.project_start.0.weight") or
        std.mem.endsWith(u8, name, "span_rep.span_rep_layer.project_end.0.weight") or
        std.mem.endsWith(u8, name, "span_rep.span_rep_layer.out_project.0.weight") or
        std.mem.endsWith(u8, name, "count_embed.transformer.out_projector.0.weight") or
        std.mem.endsWith(u8, name, "count_embed.transformer.out_projector.2.weight");
}

fn isGlinerHeadMlpWeightPair(first_name: []const u8, second_name: []const u8) bool {
    const pairs = [_]struct { first: []const u8, second: []const u8 }{
        .{ .first = "classifier.0.weight", .second = "classifier.2.weight" },
        .{ .first = "count_pred.0.weight", .second = "count_pred.2.weight" },
        .{ .first = "span_rep.span_rep_layer.project_start.0.weight", .second = "span_rep.span_rep_layer.project_start.3.weight" },
        .{ .first = "span_rep.span_rep_layer.project_end.0.weight", .second = "span_rep.span_rep_layer.project_end.3.weight" },
        .{ .first = "span_rep.span_rep_layer.out_project.0.weight", .second = "span_rep.span_rep_layer.out_project.3.weight" },
        .{ .first = "count_embed.transformer.out_projector.0.weight", .second = "count_embed.transformer.out_projector.2.weight" },
        .{ .first = "count_embed.transformer.out_projector.2.weight", .second = "count_embed.transformer.out_projector.4.weight" },
    };
    for (pairs) |pair| {
        if (std.mem.endsWith(u8, first_name, pair.first) and std.mem.endsWith(u8, second_name, pair.second)) return true;
    }
    return false;
}

fn traceDebertaFfnForwardMatchDecline(reason: []const u8, dot_id: NodeId, output_id: ?NodeId, gelu_id: ?NodeId, detail_id: ?NodeId) ?DebertaFfnForwardPattern {
    if (traceDebertaFfnForwardMatchingEnabled()) {
        std.debug.print(
            "deberta_ffn_forward_match: declined reason={s} dot={d} output={?d} gelu={?d} detail={?d}\n",
            .{ reason, dot_id, output_id, gelu_id, detail_id },
        );
    }
    return null;
}

fn traceDebertaFfnForwardMatchCandidate(label: []const u8, pattern: DebertaFfnLinearMatch) void {
    if (!traceDebertaFfnForwardMatchingEnabled()) return;
    std.debug.print(
        "deberta_ffn_forward_match: {s} dot={d} base_add={d} output={d} input={d} weight={d} rows={d} in={d} out={d} lora={}\n",
        .{ label, pattern.dot_id, pattern.base_add_id, pattern.output_id, pattern.input_id, pattern.weight_id, pattern.rows, pattern.in_dim, pattern.out_dim, pattern.lora != null },
    );
}

fn traceDebertaFfnForwardConsumers(
    graph: *const Graph,
    reachable: []const bool,
    skipped_nodes: []const bool,
    producer_id: NodeId,
    reason: []const u8,
) void {
    if (!traceDebertaFfnForwardMatchingEnabled()) return;
    var printed: usize = 0;
    var node_id: NodeId = 0;
    while (node_id < graph.nodeCount()) : (node_id += 1) {
        if (!isReachableUnskippedNode(reachable, skipped_nodes, node_id)) continue;
        const node = graph.node(node_id);
        var consumes = false;
        for (node.getInputs()) |input_id| {
            if (input_id == producer_id) {
                consumes = true;
                break;
            }
        }
        if (!consumes) continue;
        if (printed == 0) {
            std.debug.print("deberta_ffn_forward_match: consumers reason={s} producer={d}", .{ reason, producer_id });
        }
        std.debug.print(" {d}:{s}", .{ node_id, @tagName(node.op) });
        printed += 1;
        if (printed >= 8) break;
    }
    if (printed == 0) {
        std.debug.print("deberta_ffn_forward_match: consumers reason={s} producer={d} none", .{ reason, producer_id });
    }
    std.debug.print("\n", .{});
}

const DebertaFfnLinearMatch = struct {
    dot_id: NodeId,
    base_add_id: NodeId,
    output_id: NodeId,
    input_id: NodeId,
    weight_id: NodeId,
    bias_id: NodeId,
    lora: ?LoraLinearPattern = null,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
};

const ReluActivationConsumerMatch = struct {
    output_id: NodeId,
    aux_id: NodeId = null_node,
};

fn matchDebertaFfnLinearFromBaseDot(
    graph: *const Graph,
    node_ids: []const NodeId,
    node_pos: usize,
    reachable: []const bool,
    skipped_nodes: []const bool,
) ?DebertaFfnLinearMatch {
    const dot_id = node_ids[node_pos];
    if (!isReachableUnskippedNode(reachable, skipped_nodes, dot_id)) return null;
    const dot = graph.node(dot_id);
    const attrs = switch (dot.op) {
        .dot_general => |dot_attrs| dot_attrs,
        else => return null,
    };
    if (!isLinearDotAttrs(attrs) or dot.num_inputs < 2) return null;
    const output_shape = dot.output_shape;
    if (output_shape.rank() != 2) return null;
    const rows = shapeDimUsize(output_shape, 0) orelse return null;
    const out_dim = shapeDimUsize(output_shape, 1) orelse return null;
    const input_id = dot.inputs[0];
    const transpose_id = dot.inputs[1];
    if (input_id == null_node or transpose_id == null_node) return null;
    const input_shape = graph.node(input_id).output_shape;
    if (input_shape.rank() != 2) return null;
    if ((shapeDimUsize(input_shape, 0) orelse return null) != rows) return null;
    const in_dim = shapeDimUsize(input_shape, 1) orelse return null;
    if (rows == 0 or in_dim == 0 or out_dim == 0) return null;
    if (!linearDotConsumesTranspose(graph, dot_id, transpose_id)) return null;
    const weight_id = sourceFromSimpleTranspose(graph, transpose_id) orelse return null;
    const weight_shape = graph.node(weight_id).output_shape;
    if ((shapeDimUsize(weight_shape, 0) orelse return null) != out_dim) return null;
    if ((shapeDimUsize(weight_shape, 1) orelse return null) != in_dim) return null;

    const trace_ffn_like = out_dim > in_dim and !isLoRAAdapterParameterName(graph.parameterName(graph.node(weight_id)));
    const base_add_id = singleAddConsumerForInput(graph, dot_id, reachable, skipped_nodes) orelse {
        if (trace_ffn_like and traceDebertaFfnForwardMatchingEnabled()) {
            std.debug.print("deberta_ffn_forward_match: linear_declined reason=missing_base_add dot={d} input={d} weight={d} rows={d} in={d} out={d}\n", .{ dot_id, input_id, weight_id, rows, in_dim, out_dim });
        }
        return null;
    };
    if (!hasOnlyExpectedUses(graph, reachable, skipped_nodes, dot_id, &.{base_add_id})) {
        if (trace_ffn_like and traceDebertaFfnForwardMatchingEnabled()) {
            std.debug.print("deberta_ffn_forward_match: linear_declined reason=escaped_base_dot dot={d} base_add={d} input={d} weight={d} rows={d} in={d} out={d}\n", .{ dot_id, base_add_id, input_id, weight_id, rows, in_dim, out_dim });
        }
        return null;
    }
    const bias_id = linearBiasForAdd(graph, base_add_id, dot_id, out_dim) orelse {
        if (trace_ffn_like and traceDebertaFfnForwardMatchingEnabled()) {
            std.debug.print("deberta_ffn_forward_match: linear_declined reason=missing_bias dot={d} base_add={d} input={d} weight={d} rows={d} in={d} out={d}\n", .{ dot_id, base_add_id, input_id, weight_id, rows, in_dim, out_dim });
        }
        return null;
    };
    if (!graph.node(base_add_id).output_shape.eq(output_shape)) {
        if (trace_ffn_like and traceDebertaFfnForwardMatchingEnabled()) {
            std.debug.print("deberta_ffn_forward_match: linear_declined reason=base_add_shape dot={d} base_add={d} input={d} weight={d} rows={d} in={d} out={d}\n", .{ dot_id, base_add_id, input_id, weight_id, rows, in_dim, out_dim });
        }
        return null;
    }

    const lora = findCompatibleLoraLinearConsumer(
        graph,
        node_ids,
        reachable,
        skipped_nodes,
        base_add_id,
        input_id,
        rows,
        in_dim,
        out_dim,
    );
    const final_output_id = if (lora) |lora_pattern| lora_pattern.add_id else base_add_id;

    if (lora == null and trace_ffn_like and traceDebertaFfnForwardMatchingEnabled()) {
        traceDebertaFfnForwardConsumers(graph, reachable, skipped_nodes, base_add_id, "no_lora_consumer");
    }

    return .{
        .dot_id = dot_id,
        .base_add_id = base_add_id,
        .output_id = final_output_id,
        .input_id = input_id,
        .weight_id = weight_id,
        .bias_id = bias_id,
        .lora = lora,
        .rows = rows,
        .in_dim = in_dim,
        .out_dim = out_dim,
    };
}

fn findCompatibleLoraLinearConsumer(
    graph: *const Graph,
    node_ids: []const NodeId,
    reachable: []const bool,
    skipped_nodes: []const bool,
    base_add_id: NodeId,
    input_id: NodeId,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) ?LoraLinearPattern {
    var found: ?LoraLinearPattern = null;
    var found_pos: usize = std.math.maxInt(usize);
    var node_id: NodeId = 0;
    while (node_id < graph.nodeCount()) : (node_id += 1) {
        if (!isReachableUnskippedNode(reachable, skipped_nodes, node_id)) continue;
        const node = graph.node(node_id);
        switch (node.op) {
            .add, .fused_elem_add => {},
            else => continue,
        }
        const inputs = node.getInputs();
        if (inputs.len < 2) continue;
        const other_input_id: NodeId = if (inputs[0] == base_add_id)
            inputs[1]
        else if (inputs[1] == base_add_id)
            inputs[0]
        else
            continue;
        const candidate_pos = findNodePos(node_ids, node_id) orelse {
            if (traceDebertaFfnForwardMatchingEnabled()) {
                std.debug.print("deberta_ffn_forward_match: lora_candidate_declined reason=missing_pos add={d} base_add={d}\n", .{ node_id, base_add_id });
            }
            continue;
        };
        const candidate_lora = matchLoraLinearPattern(graph, node_ids, candidate_pos, reachable, skipped_nodes) orelse {
            if (traceDebertaFfnForwardMatchingEnabled()) {
                std.debug.print("deberta_ffn_forward_match: lora_candidate_declined reason=pattern add={d} base_add={d} other={d}\n", .{ node_id, base_add_id, other_input_id });
            }
            continue;
        };
        if (candidate_lora.base_linear_id != base_add_id or
            candidate_lora.input_id != input_id or
            candidate_lora.rows != rows or
            candidate_lora.in_dim != in_dim or
            candidate_lora.out_dim != out_dim)
        {
            if (traceDebertaFfnForwardMatchingEnabled()) {
                std.debug.print(
                    "deberta_ffn_forward_match: lora_candidate_declined reason=shape_or_input add={d} base_add={d} got_base={d} got_input={d} want_input={d} got_rows={d} got_in={d} got_out={d} want_rows={d} want_in={d} want_out={d}\n",
                    .{
                        node_id,
                        base_add_id,
                        candidate_lora.base_linear_id,
                        candidate_lora.input_id,
                        input_id,
                        candidate_lora.rows,
                        candidate_lora.in_dim,
                        candidate_lora.out_dim,
                        rows,
                        in_dim,
                        out_dim,
                    },
                );
            }
            continue;
        }
        if (found != null and traceDebertaFfnForwardMatchingEnabled()) {
            std.debug.print(
                "deberta_ffn_forward_match: lora_candidate_ambiguous base_add={d} keep_add={d} keep_pos={d} candidate_add={d} candidate_pos={d}\n",
                .{ base_add_id, found.?.add_id, found_pos, candidate_lora.add_id, candidate_pos },
            );
        }
        if (candidate_pos < found_pos) {
            found_pos = candidate_pos;
            found = candidate_lora;
        }
    }
    return found;
}

fn findDebertaFfnBaseDotConsumer(
    graph: *const Graph,
    producer_id: NodeId,
    rows: usize,
    out_dim: usize,
    reachable: []const bool,
    skipped_nodes: []const bool,
) ?NodeId {
    var found: ?NodeId = null;
    var node_id: NodeId = 0;
    while (node_id < graph.nodeCount()) : (node_id += 1) {
        if (!isReachableUnskippedNode(reachable, skipped_nodes, node_id)) continue;
        const node = graph.node(node_id);
        const attrs = switch (node.op) {
            .dot_general => |dot_attrs| dot_attrs,
            else => continue,
        };
        if (!isLinearDotAttrs(attrs) or node.num_inputs < 2 or node.inputs[0] != producer_id) continue;
        const shape = node.output_shape;
        if (shape.rank() != 2) continue;
        if (shapeDimUsize(shape, 0) != rows or shapeDimUsize(shape, 1) != out_dim) continue;
        const weight_id = sourceParameterFromSimpleTranspose(graph, node.inputs[1]) orelse continue;
        if (isLoRAAdapterParameterName(graph.parameterName(graph.node(weight_id)))) continue;
        if (found != null) return null;
        found = node_id;
    }
    return found;
}

fn findHeadMlpBaseDotConsumer(
    graph: *const Graph,
    producer_id: NodeId,
    rows: usize,
    reachable: []const bool,
    skipped_nodes: []const bool,
) ?NodeId {
    var found: ?NodeId = null;
    var node_id: NodeId = 0;
    while (node_id < graph.nodeCount()) : (node_id += 1) {
        if (!isReachableUnskippedNode(reachable, skipped_nodes, node_id)) continue;
        const node = graph.node(node_id);
        const attrs = switch (node.op) {
            .dot_general => |dot_attrs| dot_attrs,
            else => continue,
        };
        if (!isLinearDotAttrs(attrs) or node.num_inputs < 2 or node.inputs[0] != producer_id) continue;
        const shape = node.output_shape;
        if (shape.rank() != 2) continue;
        if (shapeDimUsize(shape, 0) != rows) continue;
        const weight_id = sourceParameterFromSimpleTranspose(graph, node.inputs[1]) orelse continue;
        if (isLoRAAdapterParameterName(graph.parameterName(graph.node(weight_id)))) continue;
        if (found != null) return null;
        found = node_id;
    }
    return found;
}

fn singleExactGeluConsumerForInput(
    graph: *const Graph,
    producer_id: NodeId,
    reachable: []const bool,
    skipped_nodes: []const bool,
) ?NodeId {
    var found: ?NodeId = null;
    var node_id: NodeId = 0;
    while (node_id < graph.nodeCount()) : (node_id += 1) {
        if (!isReachableUnskippedNode(reachable, skipped_nodes, node_id)) continue;
        const node = graph.node(node_id);
        switch (node.op) {
            .fused_gelu_exact => {},
            else => continue,
        }
        if (node.num_inputs < 1 or node.inputs[0] != producer_id) continue;
        if (found != null) return null;
        found = node_id;
    }
    return found;
}

fn matchReluActivationConsumerNode(
    graph: *const Graph,
    reachable: []const bool,
    skipped_nodes: []const bool,
    node_id: NodeId,
    producer_id: NodeId,
) ?ReluActivationConsumerMatch {
    if (node_id == null_node or node_id >= graph.nodeCount()) return null;
    const node = graph.node(node_id);
    switch (node.op) {
        .fused_relu => {
            if (node.num_inputs < 1 or node.inputs[0] != producer_id) return null;
            return .{ .output_id = node_id };
        },
        .where_select => {
            if (node.num_inputs < 3) return null;
            if (node.inputs[2] != producer_id) return null;
            const zero = scalarConstantF32(graph, node.inputs[1]) orelse return null;
            if (zero != 0.0) return null;

            const cmp_id = node.inputs[0];
            if (cmp_id == null_node or !isReachableUnskippedNode(reachable, skipped_nodes, cmp_id)) return null;
            const cmp = graph.node(cmp_id);
            switch (cmp.op) {
                .less_than => {},
                else => return null,
            }
            if (cmp.num_inputs < 2 or cmp.inputs[0] != producer_id) return null;
            const threshold = scalarConstantF32(graph, cmp.inputs[1]) orelse return null;
            if (threshold != 0.0) return null;

            const aux_id = if (hasOnlyExpectedUses(graph, reachable, skipped_nodes, cmp_id, &.{node_id})) cmp_id else null_node;
            return .{ .output_id = node_id, .aux_id = aux_id };
        },
        else => return null,
    }
}

fn linearBiasForAdd(graph: *const Graph, add_id: NodeId, producer_id: NodeId, expected_dim: usize) ?NodeId {
    if (add_id == null_node or add_id >= graph.nodeCount()) return null;
    const add_node = graph.node(add_id);
    switch (add_node.op) {
        .add, .fused_elem_add => {},
        else => return null,
    }
    if (add_node.num_inputs < 2) return null;
    const bias_id = if (add_node.inputs[0] == producer_id)
        add_node.inputs[1]
    else if (add_node.inputs[1] == producer_id)
        add_node.inputs[0]
    else
        return null;
    if (bias_id == null_node or bias_id >= graph.nodeCount()) return null;
    const bias = graph.node(bias_id);
    if (std.meta.activeTag(bias.op) != .parameter) return null;
    if (bias.output_shape.rank() != 1) return null;
    if ((shapeDimUsize(bias.output_shape, 0) orelse return null) != expected_dim) return null;
    return bias_id;
}

fn markDebertaFfnForwardSkipped(skipped: []bool, pattern: DebertaFfnForwardPattern) void {
    markSkipped(skipped, pattern.first_dot_id);
    markSkipped(skipped, pattern.first_base_add_id);
    markSkipped(skipped, pattern.first_add_id);
    if (pattern.activation_aux_id != null_node) markSkipped(skipped, pattern.activation_aux_id);
    markSkipped(skipped, pattern.gelu_id);
    markSkipped(skipped, pattern.output_dot_id);
    markSkipped(skipped, pattern.output_base_add_id);
    markSkipped(skipped, pattern.output_add_id);
    if (pattern.first_lora) |lora| markLoraLinearSkipped(skipped, lora);
    if (pattern.second_lora) |lora| markLoraLinearSkipped(skipped, lora);
}

fn markDebertaEncoderLoraLayerSkipped(skipped: []bool, pattern: DebertaEncoderLoraLayerPattern) void {
    markDebertaFfnForwardSkipped(skipped, pattern.ffn);
    if (pattern.residual_add_id != null_node) markSkipped(skipped, pattern.residual_add_id);
    if (pattern.layer_norm_id != null_node) markSkipped(skipped, pattern.layer_norm_id);
}

fn matchFfnGeluBackwardPattern(
    graph: *const Graph,
    node_ids: []const NodeId,
    node_pos: usize,
    reachable: []const bool,
    skipped_nodes: []const bool,
) ?FfnGeluBackwardPattern {
    const first_dot_id = node_ids[node_pos];
    if (!isReachableUnskippedNode(reachable, skipped_nodes, first_dot_id)) return null;
    const first_dot = graph.node(first_dot_id);
    const first_attrs = switch (first_dot.op) {
        .dot_general => |attrs| attrs,
        else => return null,
    };
    if (!isLinearDotAttrs(first_attrs) or first_dot.num_inputs < 2) return null;

    const first_shape = first_dot.output_shape;
    if (first_shape.rank() != 2) return null;
    const rows = shapeDimUsize(first_shape, 0) orelse return null;
    const intermediate_size = shapeDimUsize(first_shape, 1) orelse return null;
    if (rows <= 1 or intermediate_size < 512) return null;

    const upstream_add_id = singleAddConsumerForInput(graph, first_dot_id, reachable, skipped_nodes) orelse
        return declineFfnGeluBackwardMatch(graph, "missing_upstream_add", first_dot_id, null_node);
    const upstream_add = graph.node(upstream_add_id);
    if (!upstream_add.output_shape.eq(first_shape) or upstream_add.num_inputs < 2)
        return declineFfnGeluBackwardMatch(graph, "bad_upstream_add", first_dot_id, upstream_add_id);
    const second_branch_dot_id = if (upstream_add.inputs[0] == first_dot_id)
        upstream_add.inputs[1]
    else if (upstream_add.inputs[1] == first_dot_id)
        upstream_add.inputs[0]
    else
        return declineFfnGeluBackwardMatch(graph, "upstream_add_not_consumer", first_dot_id, upstream_add_id);
    if (second_branch_dot_id == null_node or !isReachableUnskippedNode(reachable, skipped_nodes, second_branch_dot_id))
        return declineFfnGeluBackwardMatch(graph, "second_branch_unreachable", first_dot_id, second_branch_dot_id);
    const second_branch_dot = graph.node(second_branch_dot_id);
    const second_branch_attrs = switch (second_branch_dot.op) {
        .dot_general => |attrs| attrs,
        else => return declineFfnGeluBackwardMatch(graph, "second_branch_not_dot", first_dot_id, second_branch_dot_id),
    };
    if (!isLinearDotAttrs(second_branch_attrs) or !second_branch_dot.output_shape.eq(first_shape))
        return declineFfnGeluBackwardMatch(graph, "bad_second_branch", first_dot_id, second_branch_dot_id);

    const gelu_backward_id = singleFfnGeluBackwardConsumer(graph, upstream_add_id, reachable, skipped_nodes) orelse {
        traceFfnGeluBackwardConsumers(graph, upstream_add_id, reachable, skipped_nodes, "missing_gelu_backward");
        return declineFfnGeluBackwardMatch(graph, "missing_gelu_backward", first_dot_id, upstream_add_id);
    };
    const gelu_backward = graph.node(gelu_backward_id);
    const exact_gelu_backward = switch (gelu_backward.op) {
        .fused_gelu_exact_backward => true,
        else => false,
    };
    if (!gelu_backward.output_shape.eq(first_shape))
        return declineFfnGeluBackwardMatch(graph, "bad_gelu_backward_shape", first_dot_id, gelu_backward_id);

    const output_dot_id = singleLinearDotConsumerForInput(graph, gelu_backward_id, reachable, skipped_nodes) orelse
        return declineFfnGeluBackwardMatch(graph, "missing_output_dot", first_dot_id, gelu_backward_id);
    const output_dot = graph.node(output_dot_id);
    const output_attrs = switch (output_dot.op) {
        .dot_general => |attrs| attrs,
        else => return declineFfnGeluBackwardMatch(graph, "output_not_dot", first_dot_id, output_dot_id),
    };
    if (!isLinearDotAttrs(output_attrs) or output_dot.num_inputs < 2)
        return declineFfnGeluBackwardMatch(graph, "bad_output_dot", first_dot_id, output_dot_id);
    if (output_dot.inputs[0] != gelu_backward_id)
        return declineFfnGeluBackwardMatch(graph, "output_dot_wrong_lhs", first_dot_id, output_dot_id);
    const output_shape = output_dot.output_shape;
    if (output_shape.rank() != 2) return declineFfnGeluBackwardMatch(graph, "bad_output_rank", first_dot_id, output_dot_id);
    if ((shapeDimUsize(output_shape, 0) orelse return null) != rows)
        return declineFfnGeluBackwardMatch(graph, "bad_output_rows", first_dot_id, output_dot_id);
    const hidden_size = shapeDimUsize(output_shape, 1) orelse return declineFfnGeluBackwardMatch(graph, "missing_output_hidden", first_dot_id, output_dot_id);
    if (hidden_size == 0 or hidden_size >= intermediate_size)
        return declineFfnGeluBackwardMatch(graph, "bad_output_hidden", first_dot_id, output_dot_id);

    const rhs_id = output_dot.inputs[1];
    if (rhs_id == null_node) return declineFfnGeluBackwardMatch(graph, "missing_output_rhs", first_dot_id, output_dot_id);
    if (!linearDotConsumesTranspose(graph, output_dot_id, rhs_id))
        return declineFfnGeluBackwardMatch(graph, "output_rhs_not_transposed_weight", first_dot_id, rhs_id);
    const rhs_source_id = graph.node(rhs_id).inputs[0];
    if (rhs_source_id == null_node) return declineFfnGeluBackwardMatch(graph, "missing_output_rhs_source", first_dot_id, rhs_id);
    const rhs_source_shape = graph.node(rhs_source_id).output_shape;
    if ((shapeDimUsize(rhs_source_shape, 0) orelse return null) != hidden_size)
        return declineFfnGeluBackwardMatch(graph, "output_rhs_hidden_mismatch", first_dot_id, rhs_source_id);
    if ((shapeDimUsize(rhs_source_shape, 1) orelse return null) != intermediate_size)
        return declineFfnGeluBackwardMatch(graph, "output_rhs_intermediate_mismatch", first_dot_id, rhs_source_id);

    return .{
        .first_dot_id = first_dot_id,
        .second_branch_dot_id = second_branch_dot_id,
        .upstream_add_id = upstream_add_id,
        .gelu_backward_id = gelu_backward_id,
        .output_dot_id = output_dot_id,
        .rows = rows,
        .hidden_size = hidden_size,
        .intermediate_size = intermediate_size,
        .exact = exact_gelu_backward,
    };
}

fn declineFfnGeluBackwardMatch(
    graph: *const Graph,
    reason: []const u8,
    node_id: NodeId,
    aux_id: NodeId,
) ?FfnGeluBackwardPattern {
    if (!traceFfnGeluBackwardMatchingEnabled()) return null;
    const node = graph.node(node_id);
    const aux_op = if (aux_id != null_node and aux_id < graph.nodeCount()) @tagName(graph.node(aux_id).op) else "none";
    const aux_shape = if (aux_id != null_node and aux_id < graph.nodeCount()) graph.node(aux_id).output_shape else Shape.scalar(.f32);
    std.debug.print(
        "metal_graph_fusion_trace: ffn_gelu_backward_match declined reason={s} node={d} op={s} shape={} aux={d} aux_op={s} aux_shape={}\n",
        .{ reason, node_id, @tagName(node.op), node.output_shape, aux_id, aux_op, aux_shape },
    );
    return null;
}

fn traceFfnGeluBackwardConsumers(
    graph: *const Graph,
    producer_id: NodeId,
    reachable: []const bool,
    skipped_nodes: []const bool,
    reason: []const u8,
) void {
    if (!traceFfnGeluBackwardMatchingEnabled()) return;
    var printed: usize = 0;
    var node_id: NodeId = 0;
    while (node_id < graph.nodeCount()) : (node_id += 1) {
        const node = graph.node(node_id);
        var consumes = false;
        for (node.getInputs()) |input_id| {
            if (input_id == producer_id) {
                consumes = true;
                break;
            }
        }
        if (!consumes) continue;
        if (printed == 0) {
            std.debug.print("metal_graph_fusion_trace: ffn_gelu_backward_consumers reason={s} producer={d}", .{ reason, producer_id });
        }
        std.debug.print(
            " {d}:{s}:reachable={}:skipped={}:rank={d}:dim0={d}:dim1={d}",
            .{
                node_id,
                @tagName(node.op),
                isReachableUnskippedNode(reachable, skipped_nodes, node_id),
                node_id < skipped_nodes.len and skipped_nodes[@intCast(node_id)],
                node.output_shape.rank(),
                shapeDimForNodeOr(graph, node_id, 0, -1),
                shapeDimForNodeOr(graph, node_id, 1, -1),
            },
        );
        printed += 1;
        if (printed >= 8) break;
    }
    if (printed == 0) {
        std.debug.print("metal_graph_fusion_trace: ffn_gelu_backward_consumers reason={s} producer={d} none", .{ reason, producer_id });
    }
    std.debug.print("\n", .{});
}

fn markFfnGeluBackwardSkipped(skipped: []bool, pattern: FfnGeluBackwardPattern) void {
    markSkipped(skipped, pattern.second_branch_dot_id);
    markSkipped(skipped, pattern.upstream_add_id);
    markSkipped(skipped, pattern.gelu_backward_id);
    markSkipped(skipped, pattern.output_dot_id);
}

fn isReachableUnskippedNode(reachable: []const bool, skipped_nodes: []const bool, node_id: NodeId) bool {
    const index: usize = @intCast(node_id);
    if (index >= reachable.len or !reachable[index]) return false;
    if (index < skipped_nodes.len and skipped_nodes[index]) return false;
    return true;
}

fn singleFfnGeluBackwardConsumer(
    graph: *const Graph,
    producer_id: NodeId,
    reachable: []const bool,
    skipped_nodes: []const bool,
) ?NodeId {
    var found: ?NodeId = null;
    var node_id: NodeId = 0;
    while (node_id < graph.nodeCount()) : (node_id += 1) {
        if (!isReachableUnskippedNode(reachable, skipped_nodes, node_id)) continue;
        const node = graph.node(node_id);
        switch (node.op) {
            .fused_gelu_backward, .fused_gelu_exact_backward => {},
            else => continue,
        }
        if (node.num_inputs < 2 or node.inputs[1] != producer_id) continue;
        if (found != null) return null;
        found = node_id;
    }
    return found;
}

fn singleAddConsumerForInput(
    graph: *const Graph,
    producer_id: NodeId,
    reachable: []const bool,
    skipped_nodes: []const bool,
) ?NodeId {
    var found: ?NodeId = null;
    var node_id: NodeId = 0;
    while (node_id < graph.nodeCount()) : (node_id += 1) {
        if (!isReachableUnskippedNode(reachable, skipped_nodes, node_id)) continue;
        const node = graph.node(node_id);
        switch (node.op) {
            .add => {},
            else => continue,
        }
        if (node.num_inputs < 2 or (node.inputs[0] != producer_id and node.inputs[1] != producer_id)) continue;
        if (found != null) return null;
        found = node_id;
    }
    return found;
}

fn singleLinearDotConsumerForInput(
    graph: *const Graph,
    producer_id: NodeId,
    reachable: []const bool,
    skipped_nodes: []const bool,
) ?NodeId {
    var found: ?NodeId = null;
    var node_id: NodeId = 0;
    while (node_id < graph.nodeCount()) : (node_id += 1) {
        if (!isReachableUnskippedNode(reachable, skipped_nodes, node_id)) continue;
        const node = graph.node(node_id);
        const attrs = switch (node.op) {
            .dot_general => |dot_attrs| dot_attrs,
            else => continue,
        };
        if (!isLinearDotAttrs(attrs) or node.num_inputs < 2 or node.inputs[0] != producer_id) continue;
        if (found != null) return null;
        found = node_id;
    }
    return found;
}

fn matchLoraBackwardPattern(
    graph: *const Graph,
    node_ids: []const NodeId,
    node_pos: usize,
    reachable: []const bool,
    skipped_nodes: []const bool,
) ?LoraBackwardPattern {
    const d_after_a_id = node_ids[node_pos];
    const d_after_a = graph.node(d_after_a_id);
    const d_after_attrs = switch (d_after_a.op) {
        .dot_general => |attrs| attrs,
        else => return null,
    };
    if (!isLinearDotAttrs(d_after_attrs) or d_after_a.num_inputs < 2) return null;
    const output_grad_id = d_after_a.inputs[0];
    const b_transpose_id = d_after_a.inputs[1];
    if (output_grad_id == null_node or b_transpose_id == null_node) return null;
    const lora_b_id = loraBParameterFromBackwardTranspose(graph, b_transpose_id) orelse return null;

    const output_grad_shape = graph.node(output_grad_id).output_shape;
    const d_after_shape = d_after_a.output_shape;
    const lora_b_shape = graph.node(lora_b_id).output_shape;
    if (output_grad_shape.rank() != 2 or d_after_shape.rank() != 2 or lora_b_shape.rank() != 2) return null;
    const rows = shapeDimUsize(output_grad_shape, 0) orelse return null;
    const out_dim = shapeDimUsize(output_grad_shape, 1) orelse return null;
    const rank = shapeDimUsize(d_after_shape, 1) orelse return null;
    if (shapeDimUsize(d_after_shape, 0) != rows) return null;
    if (shapeDimUsize(lora_b_shape, 0) != out_dim or shapeDimUsize(lora_b_shape, 1) != rank) return null;

    const grad_b_match = findLoraBackwardGradB(graph, reachable, skipped_nodes, d_after_a_id, output_grad_id, rows, out_dim, rank) orelse return null;
    const grad_a_match = findLoraBackwardGradA(graph, reachable, skipped_nodes, d_after_a_id, rows, rank) orelse return null;
    const input_shape = graph.node(grad_a_match.input_id).output_shape;
    const after_a_shape = graph.node(grad_b_match.after_a_id).output_shape;
    if (input_shape.rank() != 2 or after_a_shape.rank() != 2) return null;
    const in_dim = shapeDimUsize(input_shape, 1) orelse return null;
    if (shapeDimUsize(input_shape, 0) != rows) return null;
    if (shapeDimUsize(after_a_shape, 0) != rows or shapeDimUsize(after_a_shape, 1) != rank) return null;
    if (shapeDimUsize(graph.node(grad_a_match.grad_a_id).output_shape, 0) != rank) return null;
    if (shapeDimUsize(graph.node(grad_a_match.grad_a_id).output_shape, 1) != in_dim) return null;

    return .{
        .d_after_a_id = d_after_a_id,
        .grad_a_dot_id = grad_a_match.dot_id,
        .grad_a_id = grad_a_match.grad_a_id,
        .grad_b_dot_id = grad_b_match.dot_id,
        .grad_b_id = grad_b_match.grad_b_id,
        .input_transpose_id = grad_a_match.input_transpose_id,
        .after_a_transpose_id = grad_b_match.after_a_transpose_id,
        .b_transpose_id = b_transpose_id,
        .input_id = grad_a_match.input_id,
        .after_a_id = grad_b_match.after_a_id,
        .lora_b_id = lora_b_id,
        .output_grad_id = output_grad_id,
        .rows = rows,
        .in_dim = in_dim,
        .out_dim = out_dim,
        .rank = rank,
    };
}

fn matchLowRankLoraBackwardPattern(
    graph: *const Graph,
    node_ids: []const NodeId,
    node_pos: usize,
    reachable: []const bool,
    skipped_nodes: []const bool,
) ?LowRankLoraBackwardPattern {
    const d_after_a_id = node_ids[node_pos];
    const d_after_a = graph.node(d_after_a_id);
    const d_after_attrs = switch (d_after_a.op) {
        .dot_general => |attrs| attrs,
        else => return null,
    };
    if (!isLinearDotAttrs(d_after_attrs) or d_after_a.num_inputs < 2) return null;
    const output_grad_id = d_after_a.inputs[0];
    const b_transpose_id = d_after_a.inputs[1];
    if (output_grad_id == null_node or b_transpose_id == null_node) return null;
    const lora_b_id = loraBParameterFromBackwardTranspose(graph, b_transpose_id) orelse return null;

    const output_grad_shape = graph.node(output_grad_id).output_shape;
    const d_after_shape = d_after_a.output_shape;
    const lora_b_shape = graph.node(lora_b_id).output_shape;
    if (output_grad_shape.rank() != 2 or d_after_shape.rank() != 2 or lora_b_shape.rank() != 2) return null;
    const rows = shapeDimUsize(output_grad_shape, 0) orelse return null;
    const out_dim = shapeDimUsize(output_grad_shape, 1) orelse return null;
    const rank = shapeDimUsize(d_after_shape, 1) orelse return null;
    if (rows == 0 or out_dim == 0 or rank == 0 or rank > 64) return null;
    if (shapeDimUsize(d_after_shape, 0) != rows) return null;
    if (shapeDimUsize(lora_b_shape, 0) != out_dim or shapeDimUsize(lora_b_shape, 1) != rank) return null;

    const grad_b_match = findLowRankLoraBackwardGradBTransposed(graph, reachable, skipped_nodes, output_grad_id, rows, out_dim, rank) orelse return null;
    if (!onlyReachableConsumerIs(graph, grad_b_match.after_a_transpose_id, grad_b_match.dot_id, reachable, skipped_nodes)) return null;
    const after_a = graph.node(grad_b_match.after_a_id);
    switch (after_a.op) {
        .dot_general => {},
        else => return null,
    }

    return .{
        .d_after_a_id = d_after_a_id,
        .grad_b_dot_id = grad_b_match.dot_id,
        .after_a_transpose_id = grad_b_match.after_a_transpose_id,
        .b_transpose_id = b_transpose_id,
        .after_a_id = grad_b_match.after_a_id,
        .lora_b_id = lora_b_id,
        .output_grad_id = output_grad_id,
        .rows = rows,
        .out_dim = out_dim,
        .rank = rank,
    };
}

fn matchRankAdapterBackwardPattern(
    graph: *const Graph,
    node_ids: []const NodeId,
    node_pos: usize,
    reachable: []const bool,
    skipped_nodes: []const bool,
) ?RankAdapterBackwardPattern {
    const d_after_a_id = node_ids[node_pos];
    const d_after_a = graph.node(d_after_a_id);
    const d_after_attrs = switch (d_after_a.op) {
        .dot_general => |attrs| attrs,
        else => return null,
    };
    if (!isLinearDotAttrs(d_after_attrs) or d_after_a.num_inputs < 2) return null;
    const output_grad_id = d_after_a.inputs[0];
    const rhs_transpose_id = d_after_a.inputs[1];
    if (output_grad_id == null_node or rhs_transpose_id == null_node) return null;
    const rhs_source_id = sourceFromSimpleTranspose(graph, rhs_transpose_id) orelse {
        traceRankAdapterBackwardMatchDecline("rhs_not_simple_transpose", d_after_a_id, null_node, 0, 0, 0, 0);
        return null;
    };
    if (isLoraParameter(graph, rhs_source_id, ".lora_B")) return null;

    const output_grad_shape = graph.node(output_grad_id).output_shape;
    const d_after_shape = d_after_a.output_shape;
    const rhs_shape = graph.node(rhs_source_id).output_shape;
    const rhs_transpose_shape = graph.node(rhs_transpose_id).output_shape;
    if (output_grad_shape.rank() != 2 or d_after_shape.rank() != 2 or rhs_shape.rank() != 2 or rhs_transpose_shape.rank() != 2) {
        traceRankAdapterBackwardMatchDecline("rank", d_after_a_id, rhs_source_id, 0, 0, 0, 0);
        return null;
    }
    const rows = shapeDimUsize(output_grad_shape, 0) orelse return null;
    const out_dim = shapeDimUsize(output_grad_shape, 1) orelse return null;
    const rank = shapeDimUsize(d_after_shape, 1) orelse return null;
    if (rows == 0 or out_dim == 0 or rank == 0 or rank > 64) {
        traceRankAdapterBackwardMatchDecline("dims", d_after_a_id, rhs_source_id, rows, 0, rank, out_dim);
        return null;
    }
    if (shapeDimUsize(d_after_shape, 0) != rows) return null;
    if (shapeDimUsize(rhs_transpose_shape, 0) != out_dim or shapeDimUsize(rhs_transpose_shape, 1) != rank) {
        traceRankAdapterBackwardMatchDecline("rhs_transpose_shape", d_after_a_id, rhs_source_id, rows, 0, rank, out_dim);
        return null;
    }

    const rhs_rows = shapeDimUsize(rhs_shape, 0) orelse return null;
    const rhs_cols = shapeDimUsize(rhs_shape, 1) orelse return null;
    const rhs_uses_transpose_value = rhs_rows == rank and rhs_cols == out_dim;
    if (!((rhs_rows == out_dim and rhs_cols == rank) or rhs_uses_transpose_value)) {
        traceRankAdapterBackwardMatchDecline("rhs_source_shape", d_after_a_id, rhs_source_id, rows, 0, rank, out_dim);
        return null;
    }

    const grad_b_match = findLoraBackwardGradB(graph, reachable, skipped_nodes, d_after_a_id, output_grad_id, rows, out_dim, rank) orelse {
        traceRankAdapterBackwardMatchDecline("missing_grad_b", d_after_a_id, rhs_source_id, rows, 0, rank, out_dim);
        return null;
    };
    const grad_a_match = findLoraBackwardGradA(graph, reachable, skipped_nodes, d_after_a_id, rows, rank) orelse {
        traceRankAdapterBackwardMatchDecline("missing_grad_a", d_after_a_id, rhs_source_id, rows, 0, rank, out_dim);
        return null;
    };
    const input_shape = graph.node(grad_a_match.input_id).output_shape;
    const after_a_shape = graph.node(grad_b_match.after_a_id).output_shape;
    if (input_shape.rank() != 2 or after_a_shape.rank() != 2) return null;
    const in_dim = shapeDimUsize(input_shape, 1) orelse return null;
    if (in_dim == 0) return null;
    if (shapeDimUsize(input_shape, 0) != rows) return null;
    if (shapeDimUsize(after_a_shape, 0) != rows or shapeDimUsize(after_a_shape, 1) != rank) return null;
    if (shapeDimUsize(graph.node(grad_a_match.grad_a_id).output_shape, 0) != rank) return null;
    if (shapeDimUsize(graph.node(grad_a_match.grad_a_id).output_shape, 1) != in_dim) return null;

    const pattern: RankAdapterBackwardPattern = .{
        .d_after_a_id = d_after_a_id,
        .grad_a_dot_id = grad_a_match.dot_id,
        .grad_a_id = grad_a_match.grad_a_id,
        .grad_b_dot_id = grad_b_match.dot_id,
        .grad_b_id = grad_b_match.grad_b_id,
        .input_transpose_id = grad_a_match.input_transpose_id,
        .after_a_transpose_id = grad_b_match.after_a_transpose_id,
        .rhs_transpose_id = rhs_transpose_id,
        .input_id = grad_a_match.input_id,
        .after_a_id = grad_b_match.after_a_id,
        .rhs_source_id = rhs_source_id,
        .rhs_uses_transpose_value = rhs_uses_transpose_value,
        .output_grad_id = output_grad_id,
        .rows = rows,
        .in_dim = in_dim,
        .out_dim = out_dim,
        .rank = rank,
    };
    if (traceRankAdapterBackwardMatchingEnabled()) {
        std.debug.print(
            "rank_adapter_backward_match: matched d_after_a={d} grad_a={d} grad_b={d} input={d} after_a={d} rhs_source={d} rhs_uses_transpose={} rows={d} in={d} rank={d} out={d}\n",
            .{ pattern.d_after_a_id, pattern.grad_a_id, pattern.grad_b_id, pattern.input_id, pattern.after_a_id, pattern.rhs_source_id, pattern.rhs_uses_transpose_value, pattern.rows, pattern.in_dim, pattern.rank, pattern.out_dim },
        );
    }
    return pattern;
}

fn traceRankAdapterBackwardMatchDecline(
    reason: []const u8,
    d_after_a_id: NodeId,
    rhs_source_id: NodeId,
    rows: usize,
    in_dim: usize,
    rank: usize,
    out_dim: usize,
) void {
    if (!traceRankAdapterBackwardMatchingEnabled()) return;
    std.debug.print(
        "rank_adapter_backward_match: declined reason={s} d_after_a={d} rhs_source={d} rows={d} in={d} rank={d} out={d}\n",
        .{ reason, d_after_a_id, rhs_source_id, rows, in_dim, rank, out_dim },
    );
}

const LoraBackwardGradBMatch = struct {
    dot_id: NodeId,
    grad_b_id: NodeId,
    after_a_transpose_id: NodeId,
    after_a_id: NodeId,
};

const LowRankLoraBackwardGradBMatch = struct {
    dot_id: NodeId,
    after_a_transpose_id: NodeId,
    after_a_id: NodeId,
};

const LoraBackwardGradAMatch = struct {
    dot_id: NodeId,
    grad_a_id: NodeId,
    input_transpose_id: NodeId,
    input_id: NodeId,
};

fn findLowRankLoraBackwardGradBTransposed(
    graph: *const Graph,
    reachable: []const bool,
    skipped_nodes: []const bool,
    output_grad_id: NodeId,
    rows: usize,
    out_dim: usize,
    rank: usize,
) ?LowRankLoraBackwardGradBMatch {
    var dot_id: NodeId = 0;
    while (dot_id < graph.nodeCount()) : (dot_id += 1) {
        if (!isReachableUnskippedNode(reachable, skipped_nodes, dot_id)) continue;
        const dot = graph.node(dot_id);
        const attrs = switch (dot.op) {
            .dot_general => |a| a,
            else => continue,
        };
        if (!isLinearDotAttrs(attrs) or dot.num_inputs < 2 or dot.inputs[1] != output_grad_id) continue;
        if (shapeDimUsize(dot.output_shape, 0) != rank or shapeDimUsize(dot.output_shape, 1) != out_dim) continue;
        const after_a_transpose_id = dot.inputs[0];
        const after_a_id = sourceFromSimpleTranspose(graph, after_a_transpose_id) orelse continue;
        const after_a_shape = graph.node(after_a_id).output_shape;
        if (shapeDimUsize(after_a_shape, 0) != rows or shapeDimUsize(after_a_shape, 1) != rank) continue;
        return .{
            .dot_id = dot_id,
            .after_a_transpose_id = after_a_transpose_id,
            .after_a_id = after_a_id,
        };
    }
    return null;
}

fn findLoraBackwardGradB(
    graph: *const Graph,
    reachable: []const bool,
    skipped_nodes: []const bool,
    d_after_a_id: NodeId,
    output_grad_id: NodeId,
    rows: usize,
    out_dim: usize,
    rank: usize,
) ?LoraBackwardGradBMatch {
    _ = d_after_a_id;
    var dot_id: NodeId = 0;
    while (dot_id < graph.nodeCount()) : (dot_id += 1) {
        const dot_index: usize = @intCast(dot_id);
        if (dot_index >= reachable.len or !reachable[dot_index]) continue;
        if (dot_index < skipped_nodes.len and skipped_nodes[dot_index]) continue;
        const dot = graph.node(dot_id);
        const attrs = switch (dot.op) {
            .dot_general => |a| a,
            else => continue,
        };
        if (!isLinearDotAttrs(attrs) or dot.num_inputs < 2 or dot.inputs[1] != output_grad_id) continue;
        if (shapeDimUsize(dot.output_shape, 0) != rank or shapeDimUsize(dot.output_shape, 1) != out_dim) continue;
        const after_a_id = sourceFromSimpleTranspose(graph, dot.inputs[0]) orelse continue;
        const after_a_shape = graph.node(after_a_id).output_shape;
        if (shapeDimUsize(after_a_shape, 0) != rows or shapeDimUsize(after_a_shape, 1) != rank) continue;
        const grad_b_id = findSimpleTransposeConsumer(graph, reachable, skipped_nodes, dot_id, out_dim, rank) orelse continue;
        return .{
            .dot_id = dot_id,
            .grad_b_id = grad_b_id,
            .after_a_transpose_id = dot.inputs[0],
            .after_a_id = after_a_id,
        };
    }
    return null;
}

fn findLoraBackwardGradA(
    graph: *const Graph,
    reachable: []const bool,
    skipped_nodes: []const bool,
    d_after_a_id: NodeId,
    rows: usize,
    rank: usize,
) ?LoraBackwardGradAMatch {
    var dot_id: NodeId = 0;
    while (dot_id < graph.nodeCount()) : (dot_id += 1) {
        const dot_index: usize = @intCast(dot_id);
        if (dot_index >= reachable.len or !reachable[dot_index]) continue;
        if (dot_index < skipped_nodes.len and skipped_nodes[dot_index]) continue;
        const dot = graph.node(dot_id);
        const attrs = switch (dot.op) {
            .dot_general => |a| a,
            else => continue,
        };
        if (!isLinearDotAttrs(attrs) or dot.num_inputs < 2 or dot.inputs[1] != d_after_a_id) continue;
        if (shapeDimUsize(dot.output_shape, 1) != rank) continue;
        const input_id = sourceFromSimpleTranspose(graph, dot.inputs[0]) orelse continue;
        const input_shape = graph.node(input_id).output_shape;
        const in_dim = shapeDimUsize(input_shape, 1) orelse continue;
        if (shapeDimUsize(input_shape, 0) != rows) continue;
        if (shapeDimUsize(dot.output_shape, 0) != in_dim) continue;
        const grad_a_id = findSimpleTransposeConsumer(graph, reachable, skipped_nodes, dot_id, rank, in_dim) orelse continue;
        return .{
            .dot_id = dot_id,
            .grad_a_id = grad_a_id,
            .input_transpose_id = dot.inputs[0],
            .input_id = input_id,
        };
    }
    return null;
}

fn onlyReachableConsumerIs(
    graph: *const Graph,
    producer_id: NodeId,
    expected_consumer_id: NodeId,
    reachable: []const bool,
    skipped_nodes: []const bool,
) bool {
    var consumer_id: NodeId = 0;
    while (consumer_id < graph.nodeCount()) : (consumer_id += 1) {
        if (!isReachableUnskippedNode(reachable, skipped_nodes, consumer_id)) continue;
        const consumer = graph.node(consumer_id);
        for (consumer.getInputs()) |input_id| {
            if (input_id == producer_id and consumer_id != expected_consumer_id) return false;
        }
    }
    return true;
}

fn findSimpleTransposeConsumer(
    graph: *const Graph,
    reachable: []const bool,
    skipped_nodes: []const bool,
    input_id: NodeId,
    rows: usize,
    cols: usize,
) ?NodeId {
    var consumer_id: NodeId = 0;
    while (consumer_id < graph.nodeCount()) : (consumer_id += 1) {
        const consumer_index: usize = @intCast(consumer_id);
        if (consumer_index >= reachable.len or !reachable[consumer_index]) continue;
        if (consumer_index < skipped_nodes.len and skipped_nodes[consumer_index]) continue;
        const consumer = graph.node(consumer_id);
        const attrs = switch (consumer.op) {
            .transpose => |a| a,
            else => continue,
        };
        if (consumer.num_inputs == 0 or consumer.inputs[0] != input_id) continue;
        if (!transposeIsSimple2D(attrs, graph.node(input_id).output_shape)) continue;
        if (shapeDimUsize(consumer.output_shape, 0) != rows or shapeDimUsize(consumer.output_shape, 1) != cols) continue;
        return consumer_id;
    }
    return null;
}

fn sourceFromSimpleTranspose(graph: *const Graph, node_id: NodeId) ?NodeId {
    if (node_id == null_node or node_id >= graph.nodeCount()) return null;
    const node = graph.node(node_id);
    const attrs = switch (node.op) {
        .transpose => |a| a,
        else => return null,
    };
    if (node.num_inputs == 0 or node.inputs[0] == null_node) return null;
    if (!transposeIsSimple2D(attrs, graph.node(node.inputs[0]).output_shape)) return null;
    return node.inputs[0];
}

const RuntimeRegionPreSkipStats = struct {
    nodes: usize = 0,
    transposes: usize = 0,
    declined_external_consumer: usize = 0,

    fn add(self: *RuntimeRegionPreSkipStats, other: RuntimeRegionPreSkipStats) void {
        self.nodes += other.nodes;
        self.transposes += other.transposes;
        self.declined_external_consumer += other.declined_external_consumer;
    }
};

fn markLoraBackwardPreSkipped(
    graph: *const Graph,
    reachable: []const bool,
    skipped_nodes: []const bool,
    pre_skipped: []bool,
    pattern: LoraBackwardPattern,
) RuntimeRegionPreSkipStats {
    var stats = RuntimeRegionPreSkipStats{};
    const input_consumers = [_]NodeId{pattern.grad_a_dot_id};
    stats.add(markRegionOwnedTransposePreSkipped(graph, reachable, skipped_nodes, pre_skipped, pattern.input_transpose_id, input_consumers[0..]));
    const after_a_consumers = [_]NodeId{pattern.grad_b_dot_id};
    stats.add(markRegionOwnedTransposePreSkipped(graph, reachable, skipped_nodes, pre_skipped, pattern.after_a_transpose_id, after_a_consumers[0..]));
    const b_consumers = [_]NodeId{pattern.d_after_a_id};
    stats.add(markRegionOwnedTransposePreSkipped(graph, reachable, skipped_nodes, pre_skipped, pattern.b_transpose_id, b_consumers[0..]));
    return stats;
}

fn markLowRankLoraBackwardPreSkipped(
    graph: *const Graph,
    reachable: []const bool,
    skipped_nodes: []const bool,
    pre_skipped: []bool,
    pattern: LowRankLoraBackwardPattern,
) RuntimeRegionPreSkipStats {
    var stats = RuntimeRegionPreSkipStats{};
    const after_a_consumers = [_]NodeId{pattern.grad_b_dot_id};
    stats.add(markRegionOwnedTransposePreSkipped(graph, reachable, skipped_nodes, pre_skipped, pattern.after_a_transpose_id, after_a_consumers[0..]));
    const b_consumers = [_]NodeId{pattern.d_after_a_id};
    stats.add(markRegionOwnedTransposePreSkipped(graph, reachable, skipped_nodes, pre_skipped, pattern.b_transpose_id, b_consumers[0..]));
    return stats;
}

fn markRankAdapterBackwardPreSkipped(
    graph: *const Graph,
    reachable: []const bool,
    skipped_nodes: []const bool,
    pre_skipped: []bool,
    pattern: RankAdapterBackwardPattern,
) RuntimeRegionPreSkipStats {
    var stats = RuntimeRegionPreSkipStats{};
    const input_consumers = [_]NodeId{pattern.grad_a_dot_id};
    stats.add(markRegionOwnedTransposePreSkipped(graph, reachable, skipped_nodes, pre_skipped, pattern.input_transpose_id, input_consumers[0..]));
    const after_a_consumers = [_]NodeId{pattern.grad_b_dot_id};
    stats.add(markRegionOwnedTransposePreSkipped(graph, reachable, skipped_nodes, pre_skipped, pattern.after_a_transpose_id, after_a_consumers[0..]));
    if (!pattern.rhs_uses_transpose_value) {
        const rhs_consumers = [_]NodeId{pattern.d_after_a_id};
        stats.add(markRegionOwnedTransposePreSkipped(graph, reachable, skipped_nodes, pre_skipped, pattern.rhs_transpose_id, rhs_consumers[0..]));
    }
    return stats;
}

fn markRegionOwnedTransposePreSkipped(
    graph: *const Graph,
    reachable: []const bool,
    skipped_nodes: []const bool,
    pre_skipped: []bool,
    node_id: NodeId,
    allowed_consumers: []const NodeId,
) RuntimeRegionPreSkipStats {
    var stats = RuntimeRegionPreSkipStats{};
    if (node_id == null_node or node_id >= graph.nodeCount()) return stats;
    const index: usize = @intCast(node_id);
    if (index >= reachable.len or !reachable[index]) return stats;
    if (index >= pre_skipped.len) return stats;
    if (graphOutputContains(graph, node_id)) return stats;
    const node = graph.node(node_id);
    switch (node.op) {
        .transpose => {},
        else => return stats,
    }
    if (sourceFromSimpleTranspose(graph, node_id) == null) return stats;
    if (!allReachableConsumersAllowed(graph, reachable, skipped_nodes, node_id, allowed_consumers)) {
        stats.declined_external_consumer += 1;
        if (traceRuntimeRegionsEnabled()) {
            std.debug.print(
                "runtime_region_pre_skip_declined: node={} op={s} reason=external_consumer\n",
                .{ node_id, @tagName(node.op) },
            );
        }
        return stats;
    }
    if (!pre_skipped[index]) {
        pre_skipped[index] = true;
        stats.nodes += 1;
        stats.transposes += 1;
    }
    return stats;
}

fn allReachableConsumersAllowed(
    graph: *const Graph,
    reachable: []const bool,
    skipped_nodes: []const bool,
    producer_id: NodeId,
    allowed_consumers: []const NodeId,
) bool {
    var consumer_id: NodeId = 0;
    while (consumer_id < graph.nodeCount()) : (consumer_id += 1) {
        const consumer_index: usize = @intCast(consumer_id);
        if (consumer_index >= reachable.len or !reachable[consumer_index]) continue;
        if (consumer_index < skipped_nodes.len and skipped_nodes[consumer_index]) continue;
        const consumer = graph.node(consumer_id);
        for (consumer.getInputs()) |input_id| {
            if (input_id != producer_id) continue;
            if (!nodeIdListContains(allowed_consumers, consumer_id)) return false;
        }
    }
    return true;
}

fn nodeIdListContains(ids: []const NodeId, needle: NodeId) bool {
    for (ids) |id| {
        if (id == needle) return true;
    }
    return false;
}

fn graphOutputContains(graph: *const Graph, node_id: NodeId) bool {
    for (graph.outputs.items) |output_id| {
        if (output_id == node_id) return true;
    }
    return false;
}

fn loraBParameterFromBackwardTranspose(graph: *const Graph, node_id: NodeId) ?NodeId {
    const source_id = sourceFromSimpleTranspose(graph, node_id) orelse return null;
    if (isLoraParameter(graph, source_id, ".lora_B")) return source_id;
    const param_id = sourceFromSimpleTranspose(graph, source_id) orelse return null;
    if (isLoraParameter(graph, param_id, ".lora_B")) return param_id;
    return null;
}

fn markLoraBackwardSkipped(skipped: []bool, pattern: LoraBackwardPattern) void {
    const ids = [_]NodeId{
        pattern.grad_a_dot_id,
        pattern.grad_a_id,
        pattern.grad_b_dot_id,
        pattern.grad_b_id,
        pattern.input_transpose_id,
        pattern.after_a_transpose_id,
        pattern.b_transpose_id,
    };
    for (ids) |node_id| {
        const index: usize = @intCast(node_id);
        if (index < skipped.len) skipped[index] = true;
    }
}

fn markLowRankLoraBackwardSkipped(skipped: []bool, pattern: LowRankLoraBackwardPattern) void {
    const ids = [_]NodeId{
        pattern.grad_b_dot_id,
        pattern.after_a_transpose_id,
        pattern.b_transpose_id,
    };
    for (ids) |node_id| {
        const index: usize = @intCast(node_id);
        if (index < skipped.len) skipped[index] = true;
    }
}

fn markRankAdapterBackwardSkipped(skipped: []bool, pattern: RankAdapterBackwardPattern) void {
    const ids = [_]NodeId{
        pattern.grad_a_dot_id,
        pattern.grad_a_id,
        pattern.grad_b_dot_id,
        pattern.grad_b_id,
        pattern.input_transpose_id,
        pattern.after_a_transpose_id,
        pattern.rhs_transpose_id,
    };
    for (ids) |node_id| {
        const index: usize = @intCast(node_id);
        if (index < skipped.len) skipped[index] = true;
    }
}

fn matchLoraLinearQkvPattern(
    graph: *const Graph,
    node_ids: []const NodeId,
    node_pos: usize,
    reachable: []const bool,
    skipped_nodes: []const bool,
) ?LoraLinearQkvPattern {
    const current = matchLoraLinearPattern(graph, node_ids, node_pos, reachable, skipped_nodes) orelse return null;
    const current_weight_name = loraBaseWeightParameterName(graph, current) orelse {
        traceLoraQkvMatchDecline("no_base_weight", current, null);
        return null;
    };
    traceLoraQkvMatchCandidate("candidate", current, current_weight_name);
    const q = if (isQueryProjectionWeightName(current_weight_name))
        current
    else
        findLoraQkvSibling(graph, node_ids, 0, reachable, skipped_nodes, current, &isQueryProjectionWeightName) orelse {
            traceLoraQkvMatchDecline("missing_q", current, current_weight_name);
            return null;
        };
    const k = if (isKeyProjectionWeightName(current_weight_name))
        current
    else
        findLoraQkvSibling(graph, node_ids, 0, reachable, skipped_nodes, current, &isKeyProjectionWeightName) orelse {
            traceLoraQkvMatchDecline("missing_k", current, current_weight_name);
            return null;
        };
    const v = if (isValueProjectionWeightName(current_weight_name))
        current
    else
        findLoraQkvSibling(graph, node_ids, 0, reachable, skipped_nodes, current, &isValueProjectionWeightName) orelse {
            traceLoraQkvMatchDecline("missing_v", current, current_weight_name);
            return null;
        };
    const q_pos = findNodePos(node_ids, q.add_id) orelse return null;
    const k_pos = findNodePos(node_ids, k.add_id) orelse return null;
    const v_pos = findNodePos(node_ids, v.add_id) orelse return null;
    if (node_pos != @min(q_pos, @min(k_pos, v_pos))) {
        traceLoraQkvMatchDecline("not_earliest", current, current_weight_name);
        return null;
    }
    const q_base_weight_id = loraBaseWeightId(graph, q) orelse return null;
    const k_base_weight_id = loraBaseWeightId(graph, k) orelse return null;
    const v_base_weight_id = loraBaseWeightId(graph, v) orelse return null;
    if (k.out_dim != v.out_dim) {
        traceLoraQkvMatchDecline("kv_dim_mismatch", current, current_weight_name);
        return null;
    }
    if (traceLoraQkvMatchingEnabled()) {
        std.debug.print(
            "lora_qkv_match: matched q={d} k={d} v={d} q_pos={d} k_pos={d} v_pos={d} rows={d} in={d} q_out={d} kv_out={d}\n",
            .{ q.add_id, k.add_id, v.add_id, q_pos, k_pos, v_pos, q.rows, q.in_dim, q.out_dim, k.out_dim },
        );
    }
    return .{
        .q = q,
        .k = k,
        .v = v,
        .q_base_weight_id = q_base_weight_id,
        .k_base_weight_id = k_base_weight_id,
        .v_base_weight_id = v_base_weight_id,
        .rows = q.rows,
        .in_dim = q.in_dim,
        .q_out_dim = q.out_dim,
        .kv_out_dim = k.out_dim,
    };
}

fn matchDebertaAttentionPattern(
    graph: *const Graph,
    node_ids: []const NodeId,
    node_pos: usize,
    reachable: []const bool,
    skipped_nodes: []const bool,
    regions: []const RuntimeRegion,
) ?DebertaAttentionPattern {
    const first_node_id = node_ids[node_pos];
    const first_node = graph.node(first_node_id);
    const first_shape = first_node.output_shape;
    if (std.meta.activeTag(first_node.op) != .reshape) return null;
    if (previousDebertaAttentionRegionCovers(regions, node_pos, first_node_id)) {
        traceDebertaAttentionMatchDecline("overlapping_region", first_node_id, first_shape, null, null);
        return null;
    }
    if (first_shape.rank() != 4) {
        traceDebertaAttentionMatchDecline("rank", first_node_id, first_shape, null, null);
        return null;
    }
    const qkv = previousLoraQkvRegion(regions, node_pos) orelse {
        traceDebertaAttentionMatchDecline("missing_qkv", first_node_id, first_shape, null, null);
        return null;
    };
    const shape = parseDebertaAttentionStartShape(first_shape, qkv.rows, qkv.q_out_dim) orelse {
        traceDebertaAttentionMatchDecline("shape", first_node_id, first_shape, qkv.q.add_id, null);
        return null;
    };
    traceDebertaAttentionMatchCandidate(first_node_id, first_shape, shape.num_heads, shape.seq_len, shape.seq_len, shape.head_dim);
    const hidden_size = shape.num_heads * shape.head_dim;
    const rel_len = shape.seq_len * 2 - 1;

    if (qkv.rows != shape.seq_len or qkv.q_out_dim != hidden_size or qkv.kv_out_dim != hidden_size) {
        traceDebertaAttentionMatchDecline("qkv_shape", first_node_id, first_shape, qkv.q.add_id, null);
        return null;
    }
    const layer_index = layerIndexForWeight(graph, qkv.q_base_weight_id) orelse {
        traceDebertaAttentionMatchDecline("qkv_layer", first_node_id, first_shape, qkv.q.add_id, null);
        return null;
    };

    const q_r_id = findCompactDebertaRelativeProjection(graph, node_ids, node_pos, reachable, layer_index, rel_len, hidden_size, &isQueryProjectionWeightName) orelse {
        traceDebertaAttentionMatchDecline("missing_q_r", first_node_id, first_shape, qkv.q.add_id, null);
        return null;
    };
    const k_r_id = findCompactDebertaRelativeProjection(graph, node_ids, node_pos, reachable, layer_index, rel_len, hidden_size, &isKeyProjectionWeightName) orelse {
        traceDebertaAttentionMatchDecline("missing_k_r", first_node_id, first_shape, qkv.q.add_id, q_r_id);
        return null;
    };
    if (q_r_id == k_r_id) {
        traceDebertaAttentionMatchDecline("same_relative_projection", first_node_id, first_shape, q_r_id, k_r_id);
        return null;
    }

    const attn_bias_id = findDebertaAttentionBiasInput(graph, node_ids, node_pos, reachable, skipped_nodes, shape.num_heads, shape.seq_len) orelse {
        traceDebertaAttentionMatchDecline("missing_bias", first_node_id, first_shape, q_r_id, k_r_id);
        return null;
    };
    const output_id = findDebertaAttentionMergedOutput(graph, node_ids, node_pos, reachable, skipped_nodes, shape.seq_len, hidden_size) orelse {
        traceDebertaAttentionMatchDecline("missing_output", first_node_id, first_shape, attn_bias_id, null);
        return null;
    };
    if (output_id <= first_node_id) {
        traceDebertaAttentionMatchDecline("output_before_start", first_node_id, first_shape, output_id, null);
        return null;
    }

    return .{
        .first_node_id = first_node_id,
        .output_id = output_id,
        .q_id = qkv.q.add_id,
        .k_id = qkv.k.add_id,
        .v_id = qkv.v.add_id,
        .q_r_id = q_r_id,
        .k_r_id = k_r_id,
        .attn_bias_id = attn_bias_id,
        .layer_index = layer_index,
        .batch = shape.batch,
        .seq_len = shape.seq_len,
        .num_heads = shape.num_heads,
        .head_dim = shape.head_dim,
        .hidden_size = hidden_size,
    };
}

fn parseDebertaAttentionStartShape(first_shape: Shape, qkv_rows: usize, hidden_size: usize) ?DebertaAttentionShape {
    if (first_shape.rank() != 4 or qkv_rows == 0 or hidden_size == 0) return null;
    const d0 = shapeDimUsize(first_shape, 0) orelse return null;
    const d1 = shapeDimUsize(first_shape, 1) orelse return null;
    const d2 = shapeDimUsize(first_shape, 2) orelse return null;
    const d3 = shapeDimUsize(first_shape, 3) orelse return null;
    if (d3 == 0 or hidden_size % d3 != 0) return null;
    const heads = hidden_size / d3;
    if (heads == 0) return null;

    if (d0 == heads and d1 == qkv_rows and d2 == qkv_rows) {
        return .{ .batch = 1, .seq_len = qkv_rows, .num_heads = heads, .head_dim = d3 };
    }
    if (d0 == 1 and d1 == qkv_rows and d2 == heads) {
        return .{ .batch = 1, .seq_len = qkv_rows, .num_heads = heads, .head_dim = d3 };
    }
    if (d0 == heads and d1 == 1 and d2 == qkv_rows) {
        return .{ .batch = 1, .seq_len = qkv_rows, .num_heads = heads, .head_dim = d3 };
    }
    if (d0 == 1 and d1 == heads and d2 == qkv_rows) {
        return .{ .batch = 1, .seq_len = qkv_rows, .num_heads = heads, .head_dim = d3 };
    }
    return null;
}

fn previousDebertaAttentionRegionCovers(regions: []const RuntimeRegion, node_pos: usize, node_id: NodeId) bool {
    var pos = node_pos;
    while (pos > 0) {
        pos -= 1;
        switch (regions[pos]) {
            .deberta_attention => |pattern| {
                if (node_id >= pattern.first_node_id and node_id <= pattern.output_id) return true;
            },
            else => {},
        }
    }
    return false;
}

fn previousLoraQkvRegion(regions: []const RuntimeRegion, node_pos: usize) ?LoraLinearQkvPattern {
    var pos = node_pos;
    while (pos > 0) {
        pos -= 1;
        switch (regions[pos]) {
            .lora_linear_qkv => |pattern| return pattern,
            else => {},
        }
    }
    return null;
}

fn findCompactDebertaRelativeProjection(
    graph: *const Graph,
    node_ids: []const NodeId,
    end_pos: usize,
    reachable: []const bool,
    layer_index: usize,
    rel_len: usize,
    hidden_size: usize,
    weight_name_predicate: *const fn ([]const u8) bool,
) ?NodeId {
    var pos = end_pos;
    while (pos > 0) {
        pos -= 1;
        const node_id = node_ids[pos];
        const node_index: usize = @intCast(node_id);
        if (node_index >= reachable.len or !reachable[node_index]) continue;
        const node = graph.node(node_id);
        const shape = node.output_shape;
        if (shape.rank() != 2) continue;
        if (shapeDimUsize(shape, 0) != rel_len or shapeDimUsize(shape, 1) != hidden_size) continue;
        const weight_id = projectionWeightParameterFromNode(graph, node_id, 8) orelse continue;
        const weight = graph.node(weight_id);
        if (std.meta.activeTag(weight.op) != .parameter) continue;
        if (layerIndexForWeight(graph, weight_id) != layer_index) continue;
        const weight_name = graph.parameterName(weight);
        if (!weight_name_predicate(weight_name)) continue;
        return node_id;
    }
    return null;
}

fn findDebertaAttentionBiasInput(
    graph: *const Graph,
    node_ids: []const NodeId,
    start_pos: usize,
    reachable: []const bool,
    skipped_nodes: []const bool,
    num_heads: usize,
    seq_len: usize,
) ?NodeId {
    for (node_ids[start_pos..], start_pos..) |node_id, pos| {
        if (pos > start_pos + 160) break;
        const node_index: usize = @intCast(node_id);
        if (node_index >= reachable.len or !reachable[node_index]) continue;
        if (node_index < skipped_nodes.len and skipped_nodes[node_index]) continue;
        const node = graph.node(node_id);
        switch (node.op) {
            .add, .fused_elem_add => {},
            else => continue,
        }
        const shape = node.output_shape;
        if (shape.rank() != 3) continue;
        if (shapeDimUsize(shape, 0) != num_heads or shapeDimUsize(shape, 1) != seq_len or shapeDimUsize(shape, 2) != seq_len) continue;
        for (node.getInputs()) |input_id| {
            if (input_id == null_node or input_id >= graph.nodeCount()) continue;
            const input = graph.node(input_id);
            if (std.meta.activeTag(input.op) != .parameter) continue;
            const input_shape = input.output_shape;
            if (input_shape.rank() != 3) continue;
            if (shapeDimUsize(input_shape, 0) != num_heads or shapeDimUsize(input_shape, 1) != seq_len or shapeDimUsize(input_shape, 2) != seq_len) continue;
            if (std.mem.indexOf(u8, graph.parameterName(input), "attn_bias") == null) continue;
            return input_id;
        }
    }
    return findDebertaAttentionBiasParameter(graph, reachable, num_heads, seq_len);
}

fn findDebertaAttentionBiasParameter(
    graph: *const Graph,
    reachable: []const bool,
    num_heads: usize,
    seq_len: usize,
) ?NodeId {
    var node_id: NodeId = 0;
    while (node_id < graph.nodeCount()) : (node_id += 1) {
        const node_index: usize = @intCast(node_id);
        if (node_index >= reachable.len or !reachable[node_index]) continue;
        const node = graph.node(node_id);
        if (std.meta.activeTag(node.op) != .parameter) continue;
        const shape = node.output_shape;
        if (shape.rank() != 3) continue;
        if (shapeDimUsize(shape, 0) != num_heads or shapeDimUsize(shape, 1) != seq_len or shapeDimUsize(shape, 2) != seq_len) continue;
        if (std.mem.indexOf(u8, graph.parameterName(node), "attn_bias") == null) continue;
        return node_id;
    }
    return null;
}

fn findDebertaAttentionMergedOutput(
    graph: *const Graph,
    node_ids: []const NodeId,
    start_pos: usize,
    reachable: []const bool,
    skipped_nodes: []const bool,
    seq_len: usize,
    hidden_size: usize,
) ?NodeId {
    for (node_ids[start_pos..], start_pos..) |node_id, pos| {
        if (pos > start_pos + 180) break;
        const node_index: usize = @intCast(node_id);
        if (node_index >= reachable.len or !reachable[node_index]) continue;
        if (node_index < skipped_nodes.len and skipped_nodes[node_index]) continue;
        const pattern = matchLoraLinearPattern(graph, node_ids, pos, reachable, skipped_nodes) orelse continue;
        const weight_name = loraBaseWeightParameterName(graph, pattern) orelse {
            traceDebertaAttentionOutputCandidate("no_weight", node_id, pattern, null);
            continue;
        };
        traceDebertaAttentionOutputCandidate("candidate", node_id, pattern, weight_name);
        if (!isDebertaAttentionOutputDenseName(weight_name)) continue;
        if (pattern.rows != seq_len or pattern.in_dim != hidden_size or pattern.out_dim != hidden_size) {
            traceDebertaAttentionOutputCandidate("shape_mismatch", node_id, pattern, weight_name);
            return null;
        }
        return pattern.input_id;
    }
    return null;
}

fn traceLoraQkvMatchCandidate(label: []const u8, pattern: LoraLinearPattern, weight_name: []const u8) void {
    if (!traceLoraQkvMatchingEnabled()) return;
    std.debug.print(
        "lora_qkv_match: {s} add={d} base={d} input={d} weight={s} rows={d} in={d} out={d} rank={d}\n",
        .{ label, pattern.add_id, pattern.base_linear_id, pattern.input_id, weight_name, pattern.rows, pattern.in_dim, pattern.out_dim, pattern.rank },
    );
}

fn traceLoraQkvMatchDecline(reason: []const u8, pattern: LoraLinearPattern, weight_name: ?[]const u8) void {
    if (!traceLoraQkvMatchingEnabled()) return;
    std.debug.print(
        "lora_qkv_match: declined reason={s} add={d} base={d} input={d} weight={s} rows={d} in={d} out={d} rank={d}\n",
        .{ reason, pattern.add_id, pattern.base_linear_id, pattern.input_id, weight_name orelse "none", pattern.rows, pattern.in_dim, pattern.out_dim, pattern.rank },
    );
}

fn traceDebertaAttentionMatchCandidate(node_id: NodeId, shape: ml.graph.Shape, dim0: usize, dim1: usize, dim2: usize, dim3: usize) void {
    if (!traceDebertaAttentionMatchingEnabled()) return;
    std.debug.print(
        "deberta_attention_match: candidate node={d} rank={d} dims={d},{d},{d},{d}\n",
        .{ node_id, shape.rank(), dim0, dim1, dim2, dim3 },
    );
}

fn traceDebertaAttentionMatchDecline(reason: []const u8, node_id: NodeId, shape: ml.graph.Shape, id0: ?NodeId, id1: ?NodeId) void {
    if (!traceDebertaAttentionMatchingEnabled()) return;
    std.debug.print(
        "deberta_attention_match: declined reason={s} node={d} rank={d} id0={d} id1={d}\n",
        .{ reason, node_id, shape.rank(), id0 orelse null_node, id1 orelse null_node },
    );
}

fn traceDebertaAttentionOutputCandidate(label: []const u8, node_id: NodeId, pattern: LoraLinearPattern, weight_name: ?[]const u8) void {
    if (!traceDebertaAttentionMatchingEnabled()) return;
    std.debug.print(
        "deberta_attention_output_match: {s} node={d} add={d} input={d} base={d} weight={s} rows={d} in={d} out={d}\n",
        .{ label, node_id, pattern.add_id, pattern.input_id, pattern.base_linear_id, weight_name orelse "none", pattern.rows, pattern.in_dim, pattern.out_dim },
    );
}

fn findLoraQkvSibling(
    graph: *const Graph,
    node_ids: []const NodeId,
    start_pos: usize,
    reachable: []const bool,
    skipped_nodes: []const bool,
    q: LoraLinearPattern,
    weight_name_predicate: *const fn ([]const u8) bool,
) ?LoraLinearPattern {
    for (node_ids[start_pos..], start_pos..) |candidate_id, candidate_pos| {
        const candidate_index: usize = @intCast(candidate_id);
        if (candidate_index >= reachable.len or !reachable[candidate_index]) continue;
        if (candidate_index < skipped_nodes.len and skipped_nodes[candidate_index]) continue;
        const candidate = matchLoraLinearPattern(graph, node_ids, candidate_pos, reachable, skipped_nodes) orelse continue;
        if (candidate.input_id != q.input_id or candidate.rows != q.rows or candidate.in_dim != q.in_dim) continue;
        const weight_name = loraBaseWeightParameterName(graph, candidate) orelse continue;
        if (!weight_name_predicate(weight_name)) continue;
        return candidate;
    }
    return null;
}

fn loraBaseWeightId(graph: *const Graph, pattern: LoraLinearPattern) ?NodeId {
    if (projectionWeightParameterFromNode(graph, pattern.base_linear_id, 6)) |weight_id| return weight_id;
    const base = graph.node(pattern.base_linear_id);
    if (base.num_inputs < 2) return null;
    const weight_id = base.inputs[1];
    if (weight_id == null_node) return null;
    if (graph.node(weight_id).op == .parameter) return weight_id;
    return sourceParameterFromSimpleTranspose(graph, weight_id);
}

fn projectionWeightParameterFromNode(graph: *const Graph, node_id: NodeId, depth: usize) ?NodeId {
    if (node_id == null_node or node_id >= graph.nodeCount() or depth == 0) return null;
    const node = graph.node(node_id);
    switch (node.op) {
        .parameter => {
            const name = graph.parameterName(node);
            if (isQueryProjectionWeightName(name) or isKeyProjectionWeightName(name) or isValueProjectionWeightName(name)) return node_id;
            return null;
        },
        .transpose => {
            if (node.num_inputs < 1) return null;
            return projectionWeightParameterFromNode(graph, node.inputs[0], depth - 1);
        },
        .dot_general, .fused_linear, .fused_linear_no_bias => {
            if (node.num_inputs >= 2) {
                if (projectionWeightParameterFromNode(graph, node.inputs[1], depth - 1)) |weight_id| return weight_id;
            }
            if (node.num_inputs >= 1) return projectionWeightParameterFromNode(graph, node.inputs[0], depth - 1);
            return null;
        },
        .add, .fused_elem_add => {
            for (node.getInputs()) |input_id| {
                if (projectionWeightParameterFromNode(graph, input_id, depth - 1)) |weight_id| return weight_id;
            }
            return null;
        },
        else => return null,
    }
}

fn loraBaseWeightParameterName(graph: *const Graph, pattern: LoraLinearPattern) ?[]const u8 {
    const weight_id = loraBaseWeightId(graph, pattern) orelse return null;
    const weight = graph.node(weight_id);
    if (weight.op != .parameter) return null;
    return graph.parameterName(weight);
}

fn matchLoraLinearPattern(
    graph: *const Graph,
    node_ids: []const NodeId,
    node_pos: usize,
    reachable: []const bool,
    skipped_nodes: []const bool,
) ?LoraLinearPattern {
    const add_id = node_ids[node_pos];
    const add_index: usize = @intCast(add_id);
    if (add_index < skipped_nodes.len and skipped_nodes[add_index]) return null;
    const add = graph.node(add_id);
    switch (add.op) {
        .add, .fused_elem_add => {},
        else => return null,
    }
    if (add.num_inputs < 2) return null;
    return matchLoraLinearPatternWithOrder(graph, add_id, add.inputs[0], add.inputs[1], reachable, skipped_nodes) orelse
        matchLoraLinearPatternWithOrder(graph, add_id, add.inputs[1], add.inputs[0], reachable, skipped_nodes);
}

fn matchLoraLinearPatternWithOrder(
    graph: *const Graph,
    add_id: NodeId,
    base_linear_id: NodeId,
    scaled_id: NodeId,
    reachable: []const bool,
    skipped_nodes: []const bool,
) ?LoraLinearPattern {
    if (matchLoweredLoraLinearPatternWithOrder(graph, add_id, base_linear_id, scaled_id, reachable, skipped_nodes)) |pattern| {
        return pattern;
    }
    if (base_linear_id == null_node or scaled_id == null_node) return null;
    const scaled_index: usize = @intCast(scaled_id);
    if (scaled_index >= reachable.len or !reachable[scaled_index]) return null;
    if (scaled_index < skipped_nodes.len and skipped_nodes[scaled_index]) return null;
    const base = graph.node(base_linear_id);
    const base_attrs = switch (base.op) {
        .fused_linear => |attrs| attrs,
        .fused_linear_no_bias => |attrs| attrs,
        else => return null,
    };
    if (base.num_inputs < 2 or base.inputs[0] == null_node) return null;

    const scaled = graph.node(scaled_id);
    switch (scaled.op) {
        .mul, .fused_elem_multiply => {},
        else => return null,
    }
    if (scaled.num_inputs < 2) return null;

    const after_b_id, const scale_id = blk: {
        if (isScalarF32Node(graph, scaled.inputs[0])) break :blk .{ scaled.inputs[1], scaled.inputs[0] };
        if (isScalarF32Node(graph, scaled.inputs[1])) break :blk .{ scaled.inputs[0], scaled.inputs[1] };
        return null;
    };
    if (after_b_id == null_node or scale_id == null_node) return null;

    const after_b = graph.node(after_b_id);
    const after_b_attrs = switch (after_b.op) {
        .fused_linear_no_bias => |attrs| attrs,
        else => return null,
    };
    if (after_b.num_inputs < 2) return null;
    const after_a_id = after_b.inputs[0];
    const lora_b_id = after_b.inputs[1];
    if (after_a_id == null_node or lora_b_id == null_node) return null;
    if (!isLoraParameter(graph, lora_b_id, ".lora_B")) return null;

    const after_a = graph.node(after_a_id);
    const after_a_attrs = switch (after_a.op) {
        .fused_linear_no_bias => |attrs| attrs,
        else => return null,
    };
    if (after_a.num_inputs < 2) return null;
    const lora_input_id = after_a.inputs[0];
    const lora_a_id = after_a.inputs[1];
    if (lora_input_id == null_node or lora_a_id == null_node) return null;
    if (!isLoraParameter(graph, lora_a_id, ".lora_A")) return null;

    var dropout_mask_id: ?NodeId = null;
    const dropout_mul_id: ?NodeId = if (lora_input_id != base.inputs[0]) blk: {
        const maybe_mul = graph.node(lora_input_id);
        switch (maybe_mul.op) {
            .mul, .fused_elem_multiply => {},
            else => return null,
        }
        if (maybe_mul.num_inputs < 2) return null;
        const matches_base_input =
            maybe_mul.inputs[0] == base.inputs[0] or maybe_mul.inputs[1] == base.inputs[0];
        if (!matches_base_input) return null;
        const other = if (maybe_mul.inputs[0] == base.inputs[0]) maybe_mul.inputs[1] else maybe_mul.inputs[0];
        if (!isLoraParameter(graph, other, ".lora_dropout_mask")) return null;
        dropout_mask_id = other;
        break :blk lora_input_id;
    } else null;

    const rows: usize = @intCast(base_attrs.rows);
    const in_dim: usize = @intCast(base_attrs.in_dim);
    const out_dim: usize = @intCast(base_attrs.out_dim);
    const rank: usize = @intCast(after_a_attrs.out_dim);
    if (rows == 0 or in_dim == 0 or out_dim == 0 or rank == 0) return null;
    if (after_a_attrs.rows != base_attrs.rows or after_a_attrs.in_dim != base_attrs.in_dim) return null;
    if (after_b_attrs.rows != base_attrs.rows or after_b_attrs.in_dim != after_a_attrs.out_dim or after_b_attrs.out_dim != base_attrs.out_dim) return null;
    const a_shape = graph.node(lora_a_id).output_shape;
    const b_shape = graph.node(lora_b_id).output_shape;
    if (shapeDimUsize(a_shape, 0) != rank or shapeDimUsize(a_shape, 1) != in_dim) return null;
    if (shapeDimUsize(b_shape, 0) != out_dim or shapeDimUsize(b_shape, 1) != rank) return null;

    return .{
        .add_id = add_id,
        .base_linear_id = base_linear_id,
        .input_id = base.inputs[0],
        .dropout_mul_id = dropout_mul_id,
        .dropout_mask_id = dropout_mask_id,
        .lora_input_id = lora_input_id,
        .lora_a_id = lora_a_id,
        .lora_b_id = lora_b_id,
        .after_a_id = after_a_id,
        .after_b_id = after_b_id,
        .scaled_id = scaled_id,
        .scale_id = scale_id,
        .populate_scaled = reachableUseCount(graph, scaled_id, reachable, 2) > 1,
        .rows = rows,
        .in_dim = in_dim,
        .out_dim = out_dim,
        .rank = rank,
    };
}

fn matchLoweredLoraLinearPatternWithOrder(
    graph: *const Graph,
    add_id: NodeId,
    base_id: NodeId,
    scaled_id: NodeId,
    reachable: []const bool,
    skipped_nodes: []const bool,
) ?LoraLinearPattern {
    if (base_id == null_node or scaled_id == null_node) return null;
    const scaled_index: usize = @intCast(scaled_id);
    if (scaled_index >= reachable.len or !reachable[scaled_index]) return null;
    if (scaled_index < skipped_nodes.len and skipped_nodes[scaled_index]) return null;
    const scaled = graph.node(scaled_id);
    switch (scaled.op) {
        .mul, .fused_elem_multiply => {},
        else => return null,
    }
    if (scaled.num_inputs < 2) return null;

    const after_b_id, const scale_id = blk: {
        if (isScalarF32Node(graph, scaled.inputs[0])) break :blk .{ scaled.inputs[1], scaled.inputs[0] };
        if (isScalarF32Node(graph, scaled.inputs[1])) break :blk .{ scaled.inputs[0], scaled.inputs[1] };
        return null;
    };
    if (after_b_id == null_node or scale_id == null_node) return null;
    const after_b = graph.node(after_b_id);
    const after_b_attrs = switch (after_b.op) {
        .dot_general => |attrs| attrs,
        else => return null,
    };
    if (!isLinearDotAttrs(after_b_attrs)) return null;
    if (after_b.num_inputs < 2) return null;
    const after_a_id = after_b.inputs[0];
    const lora_b_id = sourceParameterFromSimpleTranspose(graph, after_b.inputs[1]) orelse return null;
    if (!isLoraParameter(graph, lora_b_id, ".lora_B")) return null;

    const after_a = graph.node(after_a_id);
    const after_a_attrs = switch (after_a.op) {
        .dot_general => |attrs| attrs,
        else => return null,
    };
    if (!isLinearDotAttrs(after_a_attrs)) return null;
    if (after_a.num_inputs < 2) return null;
    const lora_input_id = after_a.inputs[0];
    const lora_a_id = sourceParameterFromSimpleTranspose(graph, after_a.inputs[1]) orelse return null;
    if (!isLoraParameter(graph, lora_a_id, ".lora_A")) return null;

    const lora_input_shape = graph.node(lora_input_id).output_shape;
    const after_a_shape = after_a.output_shape;
    const after_b_shape = after_b.output_shape;
    const base_shape = graph.node(base_id).output_shape;
    if (lora_input_shape.rank() != 2 or after_a_shape.rank() != 2 or after_b_shape.rank() != 2 or base_shape.rank() != 2) return null;
    if (!shapesEqual(after_b_shape, base_shape) or !shapesEqual(after_b_shape, graph.node(add_id).output_shape)) return null;
    const rows = shapeDimUsize(lora_input_shape, 0) orelse return null;
    const in_dim = shapeDimUsize(lora_input_shape, 1) orelse return null;
    const rank = shapeDimUsize(after_a_shape, 1) orelse return null;
    const out_dim = shapeDimUsize(after_b_shape, 1) orelse return null;
    if (shapeDimUsize(after_a_shape, 0) != rows or shapeDimUsize(after_b_shape, 0) != rows) return null;
    const a_shape = graph.node(lora_a_id).output_shape;
    const b_shape = graph.node(lora_b_id).output_shape;
    if (shapeDimUsize(a_shape, 0) != rank or shapeDimUsize(a_shape, 1) != in_dim) return null;
    if (shapeDimUsize(b_shape, 0) != out_dim or shapeDimUsize(b_shape, 1) != rank) return null;

    return .{
        .add_id = add_id,
        .base_linear_id = base_id,
        .input_id = lora_input_id,
        .lora_input_id = lora_input_id,
        .lora_a_id = lora_a_id,
        .lora_b_id = lora_b_id,
        .after_a_id = after_a_id,
        .after_b_id = after_b_id,
        .scaled_id = scaled_id,
        .scale_id = scale_id,
        .populate_scaled = reachableUseCount(graph, scaled_id, reachable, 2) > 1,
        .rows = rows,
        .in_dim = in_dim,
        .out_dim = out_dim,
        .rank = rank,
    };
}

fn isLinearDotAttrs(attrs: anytype) bool {
    return attrs.num_contracting == 1 and
        attrs.num_batch == 0 and
        attrs.lhs_contracting[0] == 1 and
        attrs.rhs_contracting[0] == 0;
}

fn sourceParameterFromSimpleTranspose(graph: *const Graph, node_id: NodeId) ?NodeId {
    if (node_id == null_node or node_id >= graph.nodeCount()) return null;
    const node = graph.node(node_id);
    const attrs = switch (node.op) {
        .transpose => |transpose_attrs| transpose_attrs,
        else => return null,
    };
    if (node.num_inputs == 0 or node.inputs[0] == null_node) return null;
    if (!transposeIsSimple2D(attrs, graph.node(node.inputs[0]).output_shape)) return null;
    const source_id = node.inputs[0];
    if (std.meta.activeTag(graph.node(source_id).op) != .parameter) return null;
    return source_id;
}

fn isScalarF32Node(graph: *const Graph, node_id: NodeId) bool {
    if (node_id == null_node or node_id >= graph.nodeCount()) return false;
    return graph.node(node_id).output_shape.dtype == .f32 and graph.node(node_id).output_shape.rank() == 0;
}

fn isLoraParameter(graph: *const Graph, node_id: NodeId, needle: []const u8) bool {
    if (node_id == null_node or node_id >= graph.nodeCount()) return false;
    const node = graph.node(node_id);
    if (std.meta.activeTag(node.op) != .parameter) return false;
    return std.mem.indexOf(u8, graph.parameterName(node), needle) != null;
}

fn executeRawLinearDotPattern(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    value_device: []DeviceId,
    device_id: DeviceId,
    stats: ?*PartitionExecutor.ExecutionStats,
    pattern: RawLinearDotPattern,
    prepared: PreparedLinearRegion,
) !bool {
    const input = valueFor(values, pattern.input_id) orelse return false;
    const output = (try cb.decoderRuntimeApplyLinear(&.{
        .slot = prepared.linear_slot,
        .input = input,
        .in_dim = pattern.in_dim,
        .out_dim = pattern.out_dim,
    })) orelse return false;

    values[@intCast(pattern.id)] = output;
    value_device[@intCast(pattern.id)] = device_id;
    if (stats) |s| {
        recordMetalGraphRegion(s, .qkv, 1);
        s.fused_graph_pattern_dispatches += 1;
        s.fused_graph_nodes_elided += 1;
        recordGemmaRuntimeResidency(s, graph, pattern.id, isMetalResidentOrQuantizedDescriptor(cb, output));
    }
    if (traceMetalGraphFusionsEnabled()) {
        const weight_name = graph.parameterName(graph.node(pattern.weight_id));
        std.debug.print(
            "metal_graph_fusion_trace: raw_linear_dot executed dot={d} transpose={d} weight={s} rows={d} in={d} out={d}\n",
            .{ pattern.id, pattern.transpose_id, weight_name, pattern.rows, pattern.in_dim, pattern.out_dim },
        );
    }
    return true;
}

fn executeRawLinearPairPattern(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    value_device: []DeviceId,
    device_id: DeviceId,
    stats: ?*PartitionExecutor.ExecutionStats,
    pattern: RawLinearPairPattern,
    prepared: PreparedLinearPairRegion,
    skipped_nodes: []bool,
) !bool {
    const input = valueFor(values, pattern.first.input_id) orelse return false;
    const pair = (try cb.decoderRuntimeApplyLinearPair(&.{
        .slot_a = prepared.first_slot,
        .slot_b = prepared.second_slot,
        .input = input,
        .in_dim = pattern.first.in_dim,
        .out_dim = pattern.first.out_dim,
    })) orelse return false;

    values[@intCast(pattern.first.id)] = pair.first;
    value_device[@intCast(pattern.first.id)] = device_id;
    values[@intCast(pattern.second.id)] = pair.second;
    value_device[@intCast(pattern.second.id)] = device_id;
    const second_index: usize = @intCast(pattern.second.id);
    if (second_index < skipped_nodes.len) skipped_nodes[second_index] = true;

    if (stats) |s| {
        recordMetalGraphRegion(s, .qkv, 2);
        s.fused_graph_pattern_dispatches += 1;
        s.fused_graph_nodes_elided += 1;
        recordGemmaRuntimeResidency(s, graph, pattern.first.id, isMetalResidentOrQuantizedDescriptor(cb, pair.first));
        recordGemmaRuntimeResidency(s, graph, pattern.second.id, isMetalResidentOrQuantizedDescriptor(cb, pair.second));
    }
    if (traceMetalGraphFusionsEnabled()) {
        const first_weight_name = graph.parameterName(graph.node(pattern.first.weight_id));
        const second_weight_name = graph.parameterName(graph.node(pattern.second.weight_id));
        std.debug.print(
            "metal_graph_fusion_trace: raw_linear_pair executed first={d} second={d} first_weight={s} second_weight={s} rows={d} in={d} out={d}\n",
            .{ pattern.first.id, pattern.second.id, first_weight_name, second_weight_name, pattern.first.rows, pattern.first.in_dim, pattern.first.out_dim },
        );
    }
    return true;
}

fn executeRawLinearBiasPattern(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    value_device: []DeviceId,
    device_id: DeviceId,
    stats: ?*PartitionExecutor.ExecutionStats,
    pattern: RawLinearBiasPattern,
    prepared: PreparedLinearRegion,
    skipped_nodes: []bool,
) !bool {
    const input = valueFor(values, pattern.dot.input_id) orelse return false;
    const output = (try cb.decoderRuntimeApplyLinear(&.{
        .slot = prepared.linear_slot,
        .input = input,
        .in_dim = pattern.dot.in_dim,
        .out_dim = pattern.dot.out_dim,
    })) orelse return false;

    var dot_shape = [_]i32{
        @intCast(pattern.dot.rows),
        @intCast(pattern.dot.out_dim),
    };
    // `output` is owned but not yet published into values[]; free it if the clone
    // declines/errors so it is not orphaned (the frame sweep only frees values[]).
    const dot_output = cb.cloneTensorShape(output, &dot_shape) catch |err| {
        cb.free(output);
        return err;
    } orelse {
        cb.free(output);
        return false;
    };
    values[@intCast(pattern.dot.id)] = dot_output;
    value_device[@intCast(pattern.dot.id)] = device_id;
    values[@intCast(pattern.add_id)] = output;
    value_device[@intCast(pattern.add_id)] = device_id;
    const add_index: usize = @intCast(pattern.add_id);
    if (add_index < skipped_nodes.len) skipped_nodes[add_index] = true;
    if (stats) |s| {
        recordMetalGraphRegion(s, .qkv, 1);
        s.fused_graph_pattern_dispatches += 1;
        s.fused_graph_nodes_elided += 2;
        recordGemmaRuntimeResidency(s, graph, pattern.add_id, isMetalResidentOrQuantizedDescriptor(cb, output));
    }
    if (traceMetalGraphFusionsEnabled()) {
        const weight_name = graph.parameterName(graph.node(pattern.dot.weight_id));
        const bias_name = graph.parameterName(graph.node(pattern.bias_id));
        std.debug.print(
            "metal_graph_fusion_trace: raw_linear_bias executed dot={d} add={d} transpose={d} weight={s} bias={s} rows={d} in={d} out={d}\n",
            .{ pattern.dot.id, pattern.add_id, pattern.dot.transpose_id, weight_name, bias_name, pattern.dot.rows, pattern.dot.in_dim, pattern.dot.out_dim },
        );
    }
    return true;
}

fn executeRawLinearBiasPairPattern(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    value_device: []DeviceId,
    device_id: DeviceId,
    stats: ?*PartitionExecutor.ExecutionStats,
    pattern: RawLinearBiasPairPattern,
    prepared: PreparedLinearPairRegion,
    skipped_nodes: []bool,
) !bool {
    const input = valueFor(values, pattern.first.dot.input_id) orelse return false;
    const pair = (try cb.decoderRuntimeApplyLinearPair(&.{
        .slot_a = prepared.first_slot,
        .slot_b = prepared.second_slot,
        .input = input,
        .in_dim = pattern.first.dot.in_dim,
        .out_dim = pattern.first.dot.out_dim,
    })) orelse return false;

    var first_dot_shape = [_]i32{
        @intCast(pattern.first.dot.rows),
        @intCast(pattern.first.dot.out_dim),
    };
    // pair.first/pair.second are owned but not yet published into values[]; free both
    // if the clone declines/errors so neither is orphaned.
    const first_dot_output = cb.cloneTensorShape(pair.first, &first_dot_shape) catch |err| {
        cb.free(pair.first);
        cb.free(pair.second);
        return err;
    } orelse {
        cb.free(pair.first);
        cb.free(pair.second);
        return false;
    };
    values[@intCast(pattern.first.dot.id)] = first_dot_output;
    value_device[@intCast(pattern.first.dot.id)] = device_id;
    values[@intCast(pattern.first.add_id)] = pair.first;
    value_device[@intCast(pattern.first.add_id)] = device_id;
    values[@intCast(pattern.second.add_id)] = pair.second;
    value_device[@intCast(pattern.second.add_id)] = device_id;
    markRawLinearBiasPairSkipped(skipped_nodes, pattern);

    if (stats) |s| {
        recordMetalGraphRegion(s, .qkv, 4);
        s.fused_graph_pattern_dispatches += 1;
        s.fused_graph_nodes_elided += 3;
        recordGemmaRuntimeResidency(s, graph, pattern.first.add_id, isMetalResidentOrQuantizedDescriptor(cb, pair.first));
        recordGemmaRuntimeResidency(s, graph, pattern.second.add_id, isMetalResidentOrQuantizedDescriptor(cb, pair.second));
    }
    if (traceMetalGraphFusionsEnabled()) {
        const first_weight_name = graph.parameterName(graph.node(pattern.first.dot.weight_id));
        const second_weight_name = graph.parameterName(graph.node(pattern.second.dot.weight_id));
        const first_bias_name = graph.parameterName(graph.node(pattern.first.bias_id));
        const second_bias_name = graph.parameterName(graph.node(pattern.second.bias_id));
        std.debug.print(
            "metal_graph_fusion_trace: raw_linear_bias_pair executed first_dot={d} first_add={d} second_dot={d} second_add={d} first_weight={s} second_weight={s} first_bias={s} second_bias={s} rows={d} in={d} out={d}\n",
            .{
                pattern.first.dot.id,
                pattern.first.add_id,
                pattern.second.dot.id,
                pattern.second.add_id,
                first_weight_name,
                second_weight_name,
                first_bias_name,
                second_bias_name,
                pattern.first.dot.rows,
                pattern.first.dot.in_dim,
                pattern.first.dot.out_dim,
            },
        );
    }
    return true;
}

fn matchRawLinearDotPattern(
    graph: *const Graph,
    node_ids: []const NodeId,
    node_pos: usize,
    reachable: []const bool,
    last_use: []const u32,
) ?RawLinearDotPattern {
    const dot_id = node_ids[node_pos];
    const dot = graph.node(dot_id);
    const attrs = switch (dot.op) {
        .dot_general => |dot_attrs| dot_attrs,
        else => return null,
    };
    if (dot.num_inputs < 2) return null;
    if (attrs.num_contracting != 1 or attrs.num_batch != 0) return null;
    if (attrs.lhs_contracting[0] != 1 or attrs.rhs_contracting[0] != 0) return null;
    const lhs_id = dot.inputs[0];
    const transpose_id = dot.inputs[1];
    if (lhs_id == null_node or transpose_id == null_node) return null;
    if (!shouldDeferTransposeForLinearDot(graph, transpose_id, reachable, last_use)) return null;
    const transpose = graph.node(transpose_id);
    if (transpose.num_inputs == 0 or transpose.inputs[0] == null_node) return null;
    const weight_id = transpose.inputs[0];
    const weight_node = graph.node(weight_id);
    if (std.meta.activeTag(weight_node.op) != .parameter) return null;
    const weight_name = graph.parameterName(weight_node);
    if (isLoRAAdapterParameterName(weight_name)) return null;
    const lhs_shape = graph.node(lhs_id).output_shape;
    const rhs_shape = graph.node(transpose_id).output_shape;
    if (lhs_shape.rank() != 2 or rhs_shape.rank() != 2) return null;
    const rows = shapeDimUsize(lhs_shape, 0) orelse return null;
    const in_dim = shapeDimUsize(lhs_shape, 1) orelse return null;
    const out_dim = shapeDimUsize(rhs_shape, 1) orelse return null;
    const weight_shape = graph.node(weight_id).output_shape;
    const weight_out_dim = shapeDimUsize(weight_shape, 0) orelse return null;
    const weight_in_dim = shapeDimUsize(weight_shape, 1) orelse return null;
    if (weight_out_dim != out_dim or weight_in_dim != in_dim) return null;
    return .{
        .id = dot_id,
        .input_id = lhs_id,
        .transpose_id = transpose_id,
        .weight_id = weight_id,
        .rows = rows,
        .in_dim = in_dim,
        .out_dim = out_dim,
    };
}

fn matchRawLinearBiasPattern(
    graph: *const Graph,
    node_ids: []const NodeId,
    node_pos: usize,
    reachable: []const bool,
    last_use: []const u32,
    skipped_nodes: []const bool,
) ?RawLinearBiasPattern {
    const dot = matchRawLinearDotPattern(graph, node_ids, node_pos, reachable, last_use) orelse return null;
    if (reachableUseCount(graph, dot.id, reachable, 2) != 1) return null;

    var consumer_id: NodeId = 0;
    while (consumer_id < graph.nodeCount()) : (consumer_id += 1) {
        const consumer_index: usize = @intCast(consumer_id);
        if (consumer_index >= reachable.len or !reachable[consumer_index]) continue;
        if (consumer_index < skipped_nodes.len and skipped_nodes[consumer_index]) continue;
        const consumer = graph.node(consumer_id);
        switch (consumer.op) {
            .add, .fused_elem_add => {},
            else => continue,
        }
        if (consumer.num_inputs < 2) continue;
        if (consumer.inputs[0] != dot.id and consumer.inputs[1] != dot.id) continue;
        const bias_id = if (consumer.inputs[0] == dot.id) consumer.inputs[1] else consumer.inputs[0];
        if (bias_id == null_node) return null;
        const bias_node = graph.node(bias_id);
        if (std.meta.activeTag(bias_node.op) != .parameter) return null;
        const bias_name = graph.parameterName(bias_node);
        if (isLoRAAdapterParameterName(bias_name)) return null;
        const bias_shape = bias_node.output_shape;
        if (bias_shape.rank() != 1) return null;
        if (shapeDimUsize(bias_shape, 0) != dot.out_dim) return null;
        if (consumer.output_shape.rank() != 2) return null;
        if (shapeDimUsize(consumer.output_shape, 0) != dot.rows) return null;
        if (shapeDimUsize(consumer.output_shape, 1) != dot.out_dim) return null;
        return .{
            .dot = dot,
            .add_id = consumer_id,
            .bias_id = bias_id,
        };
    }
    return null;
}

fn matchRawLinearPairPattern(
    graph: *const Graph,
    node_ids: []const NodeId,
    node_pos: usize,
    reachable: []const bool,
    last_use: []const u32,
    skipped_nodes: []const bool,
) ?RawLinearPairPattern {
    const first = matchRawLinearDotPattern(graph, node_ids, node_pos, reachable, last_use) orelse return null;
    var candidate_pos = node_pos + 1;
    while (candidate_pos < node_ids.len) : (candidate_pos += 1) {
        const candidate_id = node_ids[candidate_pos];
        const candidate_index: usize = @intCast(candidate_id);
        if (candidate_index >= reachable.len or !reachable[candidate_index]) continue;
        if (candidate_index < skipped_nodes.len and skipped_nodes[candidate_index]) continue;
        const second = matchRawLinearDotPattern(graph, node_ids, candidate_pos, reachable, last_use) orelse return null;
        if (second.input_id != first.input_id) return null;
        if (second.rows != first.rows or second.in_dim != first.in_dim or second.out_dim != first.out_dim) return null;
        if (second.weight_id == first.weight_id) return null;
        return .{
            .first = first,
            .second = second,
        };
    }
    return null;
}

fn matchRawLinearBiasPairPattern(
    graph: *const Graph,
    node_ids: []const NodeId,
    node_pos: usize,
    reachable: []const bool,
    last_use: []const u32,
    skipped_nodes: []const bool,
) ?RawLinearBiasPairPattern {
    const first = matchRawLinearBiasPattern(graph, node_ids, node_pos, reachable, last_use, skipped_nodes) orelse return null;
    var candidate_pos = node_pos + 1;
    while (candidate_pos < node_ids.len) : (candidate_pos += 1) {
        const candidate_id = node_ids[candidate_pos];
        const candidate_index: usize = @intCast(candidate_id);
        if (candidate_index >= reachable.len or !reachable[candidate_index]) continue;
        if (candidate_index < skipped_nodes.len and skipped_nodes[candidate_index]) continue;
        const second = matchRawLinearBiasPattern(graph, node_ids, candidate_pos, reachable, last_use, skipped_nodes) orelse return null;
        if (second.dot.input_id != first.dot.input_id) return null;
        if (second.dot.rows != first.dot.rows or second.dot.in_dim != first.dot.in_dim or second.dot.out_dim != first.dot.out_dim) return null;
        if (second.dot.weight_id == first.dot.weight_id) return null;
        if (second.bias_id == first.bias_id) return null;
        return .{
            .first = first,
            .second = second,
        };
    }
    return null;
}

fn markRawLinearBiasSkipped(skipped_nodes: []bool, pattern: RawLinearBiasPattern) void {
    const add_index: usize = @intCast(pattern.add_id);
    if (add_index < skipped_nodes.len) skipped_nodes[add_index] = true;
}

fn markRawLinearBiasPairSkipped(skipped_nodes: []bool, pattern: RawLinearBiasPairPattern) void {
    markRawLinearBiasSkipped(skipped_nodes, pattern.first);
    markRawLinearBiasSkipped(skipped_nodes, pattern.second);
    const second_dot_index: usize = @intCast(pattern.second.dot.id);
    if (second_dot_index < skipped_nodes.len) skipped_nodes[second_dot_index] = true;
}

fn markRawLinearPairSkipped(skipped_nodes: []bool, pattern: RawLinearPairPattern) void {
    const second_index: usize = @intCast(pattern.second.id);
    if (second_index < skipped_nodes.len) skipped_nodes[second_index] = true;
}

fn isLoRAAdapterParameterName(name: []const u8) bool {
    return std.mem.indexOf(u8, name, ".lora_A") != null or
        std.mem.indexOf(u8, name, ".lora_B") != null or
        std.mem.indexOf(u8, name, ".lora_dropout_mask") != null;
}

fn prepareRawLinearDotRegion(
    cb: *const ComputeBackend,
    values: []?CT,
    pattern: RawLinearDotPattern,
    stats: ?*PartitionExecutor.ExecutionStats,
) !?PreparedLinearRegion {
    const linear_slot = (try ensurePreparedLinearSlot(
        cb,
        values,
        pattern.weight_id,
        pattern.in_dim,
        pattern.out_dim,
        stats,
    )) orelse return null;
    return .{ .linear_slot = linear_slot };
}

fn prepareRawLinearPairRegion(
    cb: *const ComputeBackend,
    values: []?CT,
    pattern: RawLinearPairPattern,
    stats: ?*PartitionExecutor.ExecutionStats,
) !?PreparedLinearPairRegion {
    const first_slot = (try ensurePreparedLinearSlot(
        cb,
        values,
        pattern.first.weight_id,
        pattern.first.in_dim,
        pattern.first.out_dim,
        stats,
    )) orelse return null;
    const second_slot = (try ensurePreparedLinearSlot(
        cb,
        values,
        pattern.second.weight_id,
        pattern.second.in_dim,
        pattern.second.out_dim,
        stats,
    )) orelse return null;
    return .{
        .first_slot = first_slot,
        .second_slot = second_slot,
    };
}

fn prepareRawLinearBiasRegion(
    cb: *const ComputeBackend,
    values: []?CT,
    pattern: RawLinearBiasPattern,
    stats: ?*PartitionExecutor.ExecutionStats,
) !?PreparedLinearRegion {
    const linear_slot = (try ensurePreparedLinearSlotWithOptionalBias(
        cb,
        values,
        pattern.dot.weight_id,
        pattern.bias_id,
        pattern.dot.in_dim,
        pattern.dot.out_dim,
        stats,
    )) orelse return null;
    return .{ .linear_slot = linear_slot };
}

fn prepareRawLinearBiasPairRegion(
    cb: *const ComputeBackend,
    values: []?CT,
    pattern: RawLinearBiasPairPattern,
    stats: ?*PartitionExecutor.ExecutionStats,
) !?PreparedLinearPairRegion {
    const first_slot = (try ensurePreparedLinearSlotWithOptionalBias(
        cb,
        values,
        pattern.first.dot.weight_id,
        pattern.first.bias_id,
        pattern.first.dot.in_dim,
        pattern.first.dot.out_dim,
        stats,
    )) orelse return null;
    const second_slot = (try ensurePreparedLinearSlotWithOptionalBias(
        cb,
        values,
        pattern.second.dot.weight_id,
        pattern.second.bias_id,
        pattern.second.dot.in_dim,
        pattern.second.dot.out_dim,
        stats,
    )) orelse return null;
    return .{
        .first_slot = first_slot,
        .second_slot = second_slot,
    };
}

const QLinearPattern = struct {
    id: NodeId,
    input_id: NodeId,
    weight_id: NodeId,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
};

const LinearNoBiasQkvPattern = struct {
    q_id: NodeId,
    k_id: NodeId,
    v_id: NodeId,
    input_id: NodeId,
    q_weight_id: NodeId,
    k_weight_id: NodeId,
    v_weight_id: NodeId,
    rows: usize,
    in_dim: usize,
    q_out_dim: usize,
    kv_out_dim: usize,
};

fn executeQLinearPattern(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    value_device: []DeviceId,
    device_id: DeviceId,
    stats: ?*PartitionExecutor.ExecutionStats,
    pattern: QLinearPattern,
    prepared: PreparedLinearRegion,
) !bool {
    const input = valueFor(values, pattern.input_id) orelse return false;
    const output = (try cb.decoderRuntimeApplyLinear(&.{
        .slot = prepared.linear_slot,
        .input = input,
        .in_dim = pattern.in_dim,
        .out_dim = pattern.out_dim,
    })) orelse return traceQkvRegionDeclined("q_linear_backend_returned_null", pattern.id);

    values[@intCast(pattern.id)] = output;
    value_device[@intCast(pattern.id)] = device_id;
    if (stats) |s| {
        recordMetalGraphRegion(s, .qkv, 1);
        s.fused_graph_pattern_dispatches += 1;
        s.gemma_qkv_hits += 1;
        recordGemmaRuntimeResidency(s, graph, pattern.id, isMetalResidentOrQuantizedDescriptor(cb, output));
    }
    if (traceMetalGraphFusionsEnabled()) {
        std.debug.print(
            "metal_graph_fusion_trace: q_linear_region executed q={d} rows={d} in={d} out={d}\n",
            .{ pattern.id, pattern.rows, pattern.in_dim, pattern.out_dim },
        );
    }
    return true;
}

fn matchQLinearPattern(
    graph: *const Graph,
    values: []?CT,
    node_ids: []const NodeId,
    node_pos: usize,
    reachable: []const bool,
    skipped_nodes: []const bool,
) ?QLinearPattern {
    const q_id = node_ids[node_pos];
    const q_index: usize = @intCast(q_id);
    if (q_index < values.len and values[q_index] != null) return null;
    const q = graph.node(q_id);
    const q_attrs = switch (q.op) {
        .fused_linear_no_bias => |attrs| attrs,
        else => return null,
    };
    if (q_attrs.num_projections != 0 or q_attrs.rows == 0 or q_attrs.in_dim == 0 or q_attrs.out_dim == 0) return null;
    const q_inputs = q.getInputs();
    if (q_inputs.len < 2) return null;
    const q_weight_name = linearWeightParameterName(graph, q) orelse return null;
    if (!isGemmaQWeightName(q_weight_name)) return null;
    if (!hasExpectedReachableUseThrough(
        graph,
        reachable,
        skipped_nodes,
        q_id,
        &isAttentionNode,
        &isQLinearAttentionPathNode,
        8,
    )) return null;
    return .{
        .id = q_id,
        .input_id = q_inputs[0],
        .weight_id = q_inputs[1],
        .rows = q_attrs.rows,
        .in_dim = q_attrs.in_dim,
        .out_dim = q_attrs.out_dim,
    };
}

fn isQLinearAttentionPathNode(node: *const ml.graph.Node) bool {
    return switch (node.op) {
        .reshape,
        .transpose,
        .slice,
        .convert_dtype,
        .mul,
        .fused_rms_norm,
        .fused_elem_multiply,
        .fused_add_mul_scalar,
        .fused_rope,
        .fused_to_float32,
        .fused_from_float32,
        => true,
        else => false,
    };
}

fn isAttentionNode(node: *const ml.graph.Node) bool {
    return switch (node.op) {
        .fused_causal_self_attention, .fused_gqa_causal_attention, .fused_sdpa => true,
        else => false,
    };
}

const GroupedLinearQkvSlicePattern = struct {
    linear_id: NodeId,
    q_slice_id: NodeId,
    k_slice_id: NodeId,
    v_slice_id: NodeId,
    input_id: NodeId,
    q_weight_id: NodeId,
    k_weight_id: NodeId,
    v_weight_id: NodeId,
    rows: usize,
    in_dim: usize,
    q_out_dim: usize,
    kv_out_dim: usize,

    fn elidedNodeCount(_: GroupedLinearQkvSlicePattern) u64 {
        return 3;
    }
};

const PackedLinearQkvSlicePattern = struct {
    linear_id: NodeId,
    q_slice_id: NodeId,
    k_slice_id: NodeId,
    v_slice_id: NodeId,
    input_id: NodeId,
    weight_id: NodeId,
    bias_id: NodeId,
    rows: usize,
    in_dim: usize,
    q_out_dim: usize,
    kv_out_dim: usize,

    fn elidedNodeCount(_: PackedLinearQkvSlicePattern) u64 {
        return 3;
    }
};

const RmsNormGroupedLinearQkvSlicePattern = struct {
    norm_id: NodeId,
    norm_input_id: NodeId,
    norm_weight_id: NodeId,
    norm_dim: usize,
    norm_eps: f32,
    qkv: GroupedLinearQkvSlicePattern,

    fn elidedNodeCount(self: RmsNormGroupedLinearQkvSlicePattern) u64 {
        return 1 + self.qkv.elidedNodeCount();
    }
};

fn tryExecuteRmsNormGroupedLinearQkvSlicePattern(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    value_device: []DeviceId,
    node_ids: []const NodeId,
    node_pos: usize,
    reachable: []const bool,
    device_id: DeviceId,
    exec_ctx: PartitionExecutor.ExecutionContext,
    skipped_nodes: []bool,
    last_use: []const u32,
    rt_map: std.AutoHashMapUnmanaged(NodeId, CT),
    donated: std.AutoHashMapUnmanaged(NodeId, void),
) !bool {
    const pattern = matchRmsNormGroupedLinearQkvSlicePattern(graph, values, node_ids, node_pos, reachable, skipped_nodes) orelse return false;
    const prepared = (try prepareRmsNormGroupedLinearQkvSliceRegion(cb, values, pattern, exec_ctx.stats)) orelse return false;
    return executeRmsNormGroupedLinearQkvSlicePattern(
        allocator,
        graph,
        cb,
        values,
        value_device,
        device_id,
        exec_ctx,
        skipped_nodes,
        last_use,
        rt_map,
        donated,
        pattern,
        prepared,
    );
}

fn executeRmsNormGroupedLinearQkvSlicePattern(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    value_device: []DeviceId,
    device_id: DeviceId,
    exec_ctx: PartitionExecutor.ExecutionContext,
    skipped_nodes: []bool,
    last_use: []const u32,
    rt_map: std.AutoHashMapUnmanaged(NodeId, CT),
    donated: std.AutoHashMapUnmanaged(NodeId, void),
    pattern: RmsNormGroupedLinearQkvSlicePattern,
    prepared: PreparedRmsNormGroupedQkvRegion,
) !bool {
    const input = valueFor(values, pattern.norm_input_id) orelse return false;

    const normed = cb.decoderRuntimeApplyRmsNorm(&.{
        .slot = prepared.norm_slot,
        .input = input,
        .hidden_size = pattern.norm_dim,
        .eps = pattern.norm_eps,
    }) catch |err| switch (err) {
        error.UnsupportedOperation, error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => return traceQkvRegionDeclined("rms_grouped_norm_unavailable", pattern.norm_id),
        else => return err,
    } orelse return traceQkvRegionDeclined("rms_grouped_norm_unavailable", pattern.norm_id);
    const normed_ct = normed;
    var normed_owned = true;
    errdefer if (normed_owned) cb.free(normed_ct);
    values[@intCast(pattern.norm_id)] = normed_ct;
    value_device[@intCast(pattern.norm_id)] = device_id;

    const qkv = (try cb.decoderRuntimeApplyLinearQkv(&.{
        .q_slot = prepared.qkv.q_slot,
        .k_slot = prepared.qkv.k_slot,
        .v_slot = prepared.qkv.v_slot,
        .input = normed_ct,
        .in_dim = pattern.qkv.in_dim,
        .q_out_dim = pattern.qkv.q_out_dim,
        .kv_out_dim = pattern.qkv.kv_out_dim,
    })) orelse return traceQkvRegionDeclined("rms_grouped_backend_returned_null", pattern.qkv.linear_id);

    _ = try freeExpiredInputs(
        allocator,
        graph,
        cb,
        values,
        value_device,
        pattern.norm_id,
        device_id,
        last_use,
        null,
        rt_map,
        donated,
        exec_ctx,
    );
    _ = try freeExpiredInputs(
        allocator,
        graph,
        cb,
        values,
        value_device,
        pattern.qkv.linear_id,
        device_id,
        last_use,
        null,
        rt_map,
        donated,
        exec_ctx,
    );

    if (values[@intCast(pattern.norm_id)]) |maybe_normed| {
        if (maybe_normed == normed_ct) {
            cb.free(normed_ct);
            values[@intCast(pattern.norm_id)] = null;
        }
    }
    normed_owned = false;

    values[@intCast(pattern.norm_id)] = qkv.first;
    values[@intCast(pattern.qkv.linear_id)] = null;
    values[@intCast(pattern.qkv.q_slice_id)] = qkv.first;
    values[@intCast(pattern.qkv.k_slice_id)] = qkv.second;
    values[@intCast(pattern.qkv.v_slice_id)] = qkv.third;
    value_device[@intCast(pattern.norm_id)] = device_id;
    value_device[@intCast(pattern.qkv.linear_id)] = device_id;
    value_device[@intCast(pattern.qkv.q_slice_id)] = device_id;
    value_device[@intCast(pattern.qkv.k_slice_id)] = device_id;
    value_device[@intCast(pattern.qkv.v_slice_id)] = device_id;
    skipped_nodes[@intCast(pattern.norm_id)] = true;
    skipped_nodes[@intCast(pattern.qkv.linear_id)] = true;
    skipped_nodes[@intCast(pattern.qkv.q_slice_id)] = true;
    skipped_nodes[@intCast(pattern.qkv.k_slice_id)] = true;
    skipped_nodes[@intCast(pattern.qkv.v_slice_id)] = true;

    if (exec_ctx.stats) |stats| {
        recordMetalGraphRegion(stats, .qkv, 5);
        stats.fused_graph_pattern_dispatches += 1;
        stats.fused_graph_nodes_elided += pattern.elidedNodeCount();
        stats.gemma_rms_norm_hits += 1;
        stats.gemma_qkv_hits += 3;
        const k_resident = isMetalResidentOrQuantizedDescriptor(cb, qkv.second);
        const v_resident = isMetalResidentOrQuantizedDescriptor(cb, qkv.third);
        if (k_resident) {
            stats.device_resident_outputs += 1;
        } else {
            stats.host_materialized_outputs += 1;
            stats.host_materialized_runtime_region_outputs += 1;
            if (traceMetalHostOutputsEnabled()) traceMetalHostOutput(graph, pattern.qkv.k_slice_id, "qkv_region_k_host_output");
        }
        if (v_resident) {
            stats.device_resident_outputs += 1;
        } else {
            stats.host_materialized_outputs += 1;
            stats.host_materialized_runtime_region_outputs += 1;
            if (traceMetalHostOutputsEnabled()) traceMetalHostOutput(graph, pattern.qkv.v_slice_id, "qkv_region_v_host_output");
        }
    }

    if (traceMetalGraphFusionsEnabled()) {
        std.debug.print(
            "metal_graph_fusion_trace: rms_grouped_qkv_region executed norm={d} linear={d} q={d} k={d} v={d} rows={d} in={d} q_out={d} kv_out={d}\n",
            .{ pattern.norm_id, pattern.qkv.linear_id, pattern.qkv.q_slice_id, pattern.qkv.k_slice_id, pattern.qkv.v_slice_id, pattern.qkv.rows, pattern.qkv.in_dim, pattern.qkv.q_out_dim, pattern.qkv.kv_out_dim },
        );
    }
    return true;
}

fn matchRmsNormGroupedLinearQkvSlicePattern(
    graph: *const Graph,
    values: []?CT,
    node_ids: []const NodeId,
    node_pos: usize,
    reachable: []const bool,
    skipped_nodes: []const bool,
) ?RmsNormGroupedLinearQkvSlicePattern {
    const norm_id = node_ids[node_pos];
    const norm = graph.node(norm_id);
    const norm_attrs = switch (norm.op) {
        .fused_rms_norm => |attrs| attrs,
        else => return null,
    };
    if (norm_attrs.dim == 0) return null;
    const norm_inputs = norm.getInputs();
    if (norm_inputs.len < 2) return null;

    const linear_id = findSingleInputNode(graph, node_ids, node_pos + 1, reachable, skipped_nodes, norm_id, &isGroupedLinearQkvCandidate) orelse return null;
    if (!hasOnlyExpectedUses(graph, reachable, skipped_nodes, norm_id, &.{linear_id})) return null;
    const linear_pos = findNodePos(node_ids, linear_id) orelse return null;
    const qkv = matchGroupedLinearQkvSlicePatternAt(graph, values, node_ids, linear_pos, reachable, skipped_nodes) orelse return null;

    return .{
        .norm_id = norm_id,
        .norm_input_id = norm_inputs[0],
        .norm_weight_id = norm_inputs[1],
        .norm_dim = norm_attrs.dim,
        .norm_eps = norm_attrs.eps,
        .qkv = qkv,
    };
}

fn tryExecuteGroupedLinearQkvSlicePattern(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    value_device: []DeviceId,
    node_ids: []const NodeId,
    node_pos: usize,
    reachable: []const bool,
    device_id: DeviceId,
    exec_ctx: PartitionExecutor.ExecutionContext,
    skipped_nodes: []bool,
    last_use: []const u32,
    rt_map: std.AutoHashMapUnmanaged(NodeId, CT),
    donated: std.AutoHashMapUnmanaged(NodeId, void),
) !bool {
    const pattern = matchGroupedLinearQkvSlicePattern(graph, values, node_ids, node_pos, reachable, skipped_nodes) orelse return false;
    const prepared = (try prepareGroupedLinearQkvSliceRegion(cb, values, pattern, exec_ctx.stats)) orelse return false;
    return executeGroupedLinearQkvSlicePattern(
        allocator,
        graph,
        cb,
        values,
        value_device,
        device_id,
        exec_ctx,
        skipped_nodes,
        last_use,
        rt_map,
        donated,
        pattern,
        prepared,
    );
}

fn executeGroupedLinearQkvSlicePattern(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    value_device: []DeviceId,
    device_id: DeviceId,
    exec_ctx: PartitionExecutor.ExecutionContext,
    skipped_nodes: []bool,
    last_use: []const u32,
    rt_map: std.AutoHashMapUnmanaged(NodeId, CT),
    donated: std.AutoHashMapUnmanaged(NodeId, void),
    pattern: GroupedLinearQkvSlicePattern,
    prepared: PreparedQkvRegion,
) !bool {
    const input = valueFor(values, pattern.input_id) orelse return false;

    const qkv = (try cb.decoderRuntimeApplyLinearQkv(&.{
        .q_slot = prepared.q_slot,
        .k_slot = prepared.k_slot,
        .v_slot = prepared.v_slot,
        .input = input,
        .in_dim = pattern.in_dim,
        .q_out_dim = pattern.q_out_dim,
        .kv_out_dim = pattern.kv_out_dim,
    })) orelse return traceQkvRegionDeclined("grouped_backend_returned_null", pattern.linear_id);

    values[@intCast(pattern.linear_id)] = qkv.first;
    values[@intCast(pattern.q_slice_id)] = qkv.first;
    values[@intCast(pattern.k_slice_id)] = qkv.second;
    values[@intCast(pattern.v_slice_id)] = qkv.third;
    value_device[@intCast(pattern.linear_id)] = device_id;
    value_device[@intCast(pattern.q_slice_id)] = device_id;
    value_device[@intCast(pattern.k_slice_id)] = device_id;
    value_device[@intCast(pattern.v_slice_id)] = device_id;
    skipped_nodes[@intCast(pattern.linear_id)] = true;
    skipped_nodes[@intCast(pattern.q_slice_id)] = true;
    skipped_nodes[@intCast(pattern.k_slice_id)] = true;
    skipped_nodes[@intCast(pattern.v_slice_id)] = true;

    if (exec_ctx.stats) |stats| {
        recordMetalGraphRegion(stats, .qkv, 4);
        stats.fused_graph_pattern_dispatches += 1;
        stats.fused_graph_nodes_elided += pattern.elidedNodeCount();
        stats.gemma_qkv_hits += 3;
        const k_resident = isMetalResidentOrQuantizedDescriptor(cb, qkv.second);
        const v_resident = isMetalResidentOrQuantizedDescriptor(cb, qkv.third);
        if (k_resident) {
            stats.device_resident_outputs += 1;
        } else {
            stats.host_materialized_outputs += 1;
            stats.host_materialized_runtime_region_outputs += 1;
            if (traceMetalHostOutputsEnabled()) traceMetalHostOutput(graph, pattern.k_slice_id, "qkv_region_k_host_output");
        }
        if (v_resident) {
            stats.device_resident_outputs += 1;
        } else {
            stats.host_materialized_outputs += 1;
            stats.host_materialized_runtime_region_outputs += 1;
            if (traceMetalHostOutputsEnabled()) traceMetalHostOutput(graph, pattern.v_slice_id, "qkv_region_v_host_output");
        }
    }

    _ = try freeExpiredInputs(
        allocator,
        graph,
        cb,
        values,
        value_device,
        pattern.linear_id,
        device_id,
        last_use,
        null,
        rt_map,
        donated,
        exec_ctx,
    );

    if (traceMetalGraphFusionsEnabled()) {
        std.debug.print(
            "metal_graph_fusion_trace: grouped_qkv_region executed linear={d} q={d} k={d} v={d} rows={d} in={d} q_out={d} kv_out={d}\n",
            .{ pattern.linear_id, pattern.q_slice_id, pattern.k_slice_id, pattern.v_slice_id, pattern.rows, pattern.in_dim, pattern.q_out_dim, pattern.kv_out_dim },
        );
    }
    return true;
}

fn executePackedLinearQkvSlicePattern(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    value_device: []DeviceId,
    device_id: DeviceId,
    exec_ctx: PartitionExecutor.ExecutionContext,
    skipped_nodes: []bool,
    last_use: []const u32,
    rt_map: std.AutoHashMapUnmanaged(NodeId, CT),
    donated: std.AutoHashMapUnmanaged(NodeId, void),
    pattern: PackedLinearQkvSlicePattern,
    prepared: PreparedQkvRegion,
) !bool {
    const input = valueFor(values, pattern.input_id) orelse return false;

    const qkv = (try cb.decoderRuntimeApplyLinearQkv(&.{
        .q_slot = prepared.q_slot,
        .k_slot = prepared.k_slot,
        .v_slot = prepared.v_slot,
        .input = input,
        .in_dim = pattern.in_dim,
        .q_out_dim = pattern.q_out_dim,
        .kv_out_dim = pattern.kv_out_dim,
    })) orelse return traceQkvRegionDeclined("packed_backend_returned_null", pattern.linear_id);

    values[@intCast(pattern.linear_id)] = qkv.first;
    values[@intCast(pattern.q_slice_id)] = qkv.first;
    values[@intCast(pattern.k_slice_id)] = qkv.second;
    values[@intCast(pattern.v_slice_id)] = qkv.third;
    value_device[@intCast(pattern.linear_id)] = device_id;
    value_device[@intCast(pattern.q_slice_id)] = device_id;
    value_device[@intCast(pattern.k_slice_id)] = device_id;
    value_device[@intCast(pattern.v_slice_id)] = device_id;
    skipped_nodes[@intCast(pattern.linear_id)] = true;
    skipped_nodes[@intCast(pattern.q_slice_id)] = true;
    skipped_nodes[@intCast(pattern.k_slice_id)] = true;
    skipped_nodes[@intCast(pattern.v_slice_id)] = true;

    if (exec_ctx.stats) |stats| {
        recordMetalGraphRegion(stats, .qkv, 4);
        stats.fused_graph_pattern_dispatches += 1;
        stats.fused_graph_nodes_elided += pattern.elidedNodeCount();
        stats.gemma_qkv_hits += 3;
        const k_resident = isMetalResidentOrQuantizedDescriptor(cb, qkv.second);
        const v_resident = isMetalResidentOrQuantizedDescriptor(cb, qkv.third);
        if (k_resident) {
            stats.device_resident_outputs += 1;
        } else {
            stats.host_materialized_outputs += 1;
            stats.host_materialized_runtime_region_outputs += 1;
            if (traceMetalHostOutputsEnabled()) traceMetalHostOutput(graph, pattern.k_slice_id, "packed_qkv_region_k_host_output");
        }
        if (v_resident) {
            stats.device_resident_outputs += 1;
        } else {
            stats.host_materialized_outputs += 1;
            stats.host_materialized_runtime_region_outputs += 1;
            if (traceMetalHostOutputsEnabled()) traceMetalHostOutput(graph, pattern.v_slice_id, "packed_qkv_region_v_host_output");
        }
    }

    _ = try freeExpiredInputs(
        allocator,
        graph,
        cb,
        values,
        value_device,
        pattern.linear_id,
        device_id,
        last_use,
        null,
        rt_map,
        donated,
        exec_ctx,
    );

    if (traceMetalGraphFusionsEnabled()) {
        std.debug.print(
            "metal_graph_fusion_trace: packed_qkv_region executed linear={d} q={d} k={d} v={d} rows={d} in={d} q_out={d} kv_out={d}\n",
            .{ pattern.linear_id, pattern.q_slice_id, pattern.k_slice_id, pattern.v_slice_id, pattern.rows, pattern.in_dim, pattern.q_out_dim, pattern.kv_out_dim },
        );
    }
    return true;
}

fn matchGroupedLinearQkvSlicePattern(
    graph: *const Graph,
    values: []?CT,
    node_ids: []const NodeId,
    node_pos: usize,
    reachable: []const bool,
    skipped_nodes: []const bool,
) ?GroupedLinearQkvSlicePattern {
    return matchGroupedLinearQkvSlicePatternAt(graph, values, node_ids, node_pos, reachable, skipped_nodes);
}

fn matchGroupedLinearQkvSlicePatternAt(
    graph: *const Graph,
    values: []?CT,
    node_ids: []const NodeId,
    node_pos: usize,
    reachable: []const bool,
    skipped_nodes: []const bool,
) ?GroupedLinearQkvSlicePattern {
    const linear_id = node_ids[node_pos];
    const linear = graph.node(linear_id);
    const attrs = switch (linear.op) {
        .fused_linear_no_bias => |linear_attrs| linear_attrs,
        else => return null,
    };
    const rows = shapeDimUsize(linear.output_shape, 0) orelse return null;
    const total_out_dim = shapeDimUsize(linear.output_shape, 1) orelse return null;
    if (rows == 0 or attrs.in_dim == 0 or total_out_dim == 0) return null;
    const inputs = linear.getInputs();
    if (inputs.len < 2) return null;

    var leaves: [3]NodeId = undefined;
    if (!collectThreeRowConcatLeaves(graph, inputs[1], &leaves)) return null;
    const q_rows = shapeDimUsize(graph.node(leaves[0]).output_shape, 0) orelse return null;
    const k_rows = shapeDimUsize(graph.node(leaves[1]).output_shape, 0) orelse return null;
    const v_rows = shapeDimUsize(graph.node(leaves[2]).output_shape, 0) orelse return null;
    const q_cols = shapeDimUsize(graph.node(leaves[0]).output_shape, 1) orelse return null;
    const k_cols = shapeDimUsize(graph.node(leaves[1]).output_shape, 1) orelse return null;
    const v_cols = shapeDimUsize(graph.node(leaves[2]).output_shape, 1) orelse return null;
    if (q_cols != attrs.in_dim or k_cols != attrs.in_dim or v_cols != attrs.in_dim) return null;
    if (k_rows != v_rows or total_out_dim != q_rows + k_rows + v_rows) return null;

    const q_slice = findLinearSliceCandidate(graph, values, node_ids, node_pos + 1, reachable, skipped_nodes, linear_id, 0, q_rows) orelse return null;
    const k_slice = findLinearSliceCandidate(graph, values, node_ids, node_pos + 1, reachable, skipped_nodes, linear_id, q_rows, q_rows + k_rows) orelse return null;
    const v_slice = findLinearSliceCandidate(graph, values, node_ids, node_pos + 1, reachable, skipped_nodes, linear_id, q_rows + k_rows, q_rows + k_rows + v_rows) orelse return null;
    if (!hasOnlyExpectedUses(graph, reachable, skipped_nodes, linear_id, &.{ q_slice, k_slice, v_slice })) return null;

    return .{
        .linear_id = linear_id,
        .q_slice_id = q_slice,
        .k_slice_id = k_slice,
        .v_slice_id = v_slice,
        .input_id = inputs[0],
        .q_weight_id = leaves[0],
        .k_weight_id = leaves[1],
        .v_weight_id = leaves[2],
        .rows = rows,
        .in_dim = attrs.in_dim,
        .q_out_dim = q_rows,
        .kv_out_dim = k_rows,
    };
}

fn isGroupedLinearQkvCandidate(node: *const ml.graph.Node) bool {
    return switch (node.op) {
        .fused_linear_no_bias => true,
        else => false,
    };
}

fn matchPackedLinearQkvSlicePattern(
    graph: *const Graph,
    values: []?CT,
    node_ids: []const NodeId,
    node_pos: usize,
    reachable: []const bool,
    skipped_nodes: []const bool,
) ?PackedLinearQkvSlicePattern {
    const linear_id = node_ids[node_pos];
    const linear_index: usize = @intCast(linear_id);
    if (linear_index < values.len and values[linear_index] != null) return null;
    const linear = graph.node(linear_id);
    const attrs = switch (linear.op) {
        .fused_linear => |linear_attrs| linear_attrs,
        else => return null,
    };
    if (attrs.rows == 0 or attrs.in_dim == 0 or attrs.out_dim == 0) return null;
    const inputs = linear.getInputs();
    if (inputs.len < 3) return null;
    const weight_name = linearWeightParameterName(graph, linear) orelse return null;
    if (!isGlinerPackedQkvWeightName(weight_name)) return null;
    if (attrs.out_dim % 3 != 0) return null;
    const projection_dim = attrs.out_dim / 3;
    if (projection_dim == 0) return null;

    const rows = shapeDimUsize(linear.output_shape, 0) orelse return null;
    const total_out_dim = shapeDimUsize(linear.output_shape, 1) orelse return null;
    if (rows != attrs.rows or total_out_dim != attrs.out_dim) return null;
    const weight_shape = graph.node(inputs[1]).output_shape;
    if (weight_shape.rank() != 2 or weight_shape.dim(0) != attrs.out_dim or weight_shape.dim(1) != attrs.in_dim) return null;
    const bias_shape = graph.node(inputs[2]).output_shape;
    if (bias_shape.rank() != 1 or bias_shape.dim(0) != attrs.out_dim) return null;

    const q_slice = findLinearSliceCandidate(graph, values, node_ids, node_pos + 1, reachable, skipped_nodes, linear_id, 0, projection_dim) orelse return null;
    const k_slice = findLinearSliceCandidate(graph, values, node_ids, node_pos + 1, reachable, skipped_nodes, linear_id, projection_dim, projection_dim * 2) orelse return null;
    const v_slice = findLinearSliceCandidate(graph, values, node_ids, node_pos + 1, reachable, skipped_nodes, linear_id, projection_dim * 2, projection_dim * 3) orelse return null;
    if (!hasOnlyExpectedUses(graph, reachable, skipped_nodes, linear_id, &.{ q_slice, k_slice, v_slice })) return null;

    return .{
        .linear_id = linear_id,
        .q_slice_id = q_slice,
        .k_slice_id = k_slice,
        .v_slice_id = v_slice,
        .input_id = inputs[0],
        .weight_id = inputs[1],
        .bias_id = inputs[2],
        .rows = attrs.rows,
        .in_dim = attrs.in_dim,
        .q_out_dim = projection_dim,
        .kv_out_dim = projection_dim,
    };
}

fn isGlinerPackedQkvWeightName(name: []const u8) bool {
    return (std.mem.indexOf(u8, name, "count_embed.transformer.transformer.layers.") != null and
        std.mem.endsWith(u8, name, ".self_attn.in_proj_weight")) or
        std.mem.endsWith(u8, name, "count_embed.gru.weight_ih_l0") or
        std.mem.endsWith(u8, name, "count_embed.gru.weight_hh_l0");
}

fn findNodePos(node_ids: []const NodeId, needle: NodeId) ?usize {
    for (node_ids, 0..) |node_id, pos| {
        if (node_id == needle) return pos;
    }
    return null;
}

fn collectThreeRowConcatLeaves(graph: *const Graph, root_id: NodeId, out: *[3]NodeId) bool {
    var count: usize = 0;
    collectRowConcatLeaves(graph, root_id, out, &count) catch return false;
    return count == 3;
}

fn collectRowConcatLeaves(graph: *const Graph, node_id: NodeId, out: *[3]NodeId, count: *usize) !void {
    const node = graph.node(node_id);
    switch (node.op) {
        .concat_prim => |attrs| {
            if (attrs.axis != 0) return error.UnsupportedShape;
            const inputs = node.getInputs();
            if (inputs.len < 2) return error.UnsupportedShape;
            try collectRowConcatLeaves(graph, inputs[0], out, count);
            try collectRowConcatLeaves(graph, inputs[1], out, count);
        },
        .parameter => {
            if (count.* >= out.len) return error.UnsupportedShape;
            out[count.*] = node_id;
            count.* += 1;
        },
        else => return error.UnsupportedShape,
    }
}

fn findLinearSliceCandidate(
    graph: *const Graph,
    values: []?CT,
    node_ids: []const NodeId,
    start_pos: usize,
    reachable: []const bool,
    skipped_nodes: []const bool,
    linear_id: NodeId,
    start: usize,
    limit: usize,
) ?NodeId {
    for (node_ids[start_pos..]) |candidate_id| {
        const candidate_index: usize = @intCast(candidate_id);
        if (candidate_index >= reachable.len or !reachable[candidate_index]) continue;
        if (candidate_index < skipped_nodes.len and skipped_nodes[candidate_index]) continue;
        if (values[candidate_index] != null) continue;
        const candidate = graph.node(candidate_id);
        const attrs = switch (candidate.op) {
            .slice => |slice_attrs| slice_attrs,
            else => continue,
        };
        const inputs = candidate.getInputs();
        if (inputs.len < 1 or inputs[0] != linear_id) continue;
        if (attrs.num_axes != 2 or attrs.starts[0] != 0 or attrs.strides[0] != 1 or attrs.strides[1] != 1) continue;
        if (std.math.cast(usize, attrs.starts[1]) orelse continue != start) continue;
        if (std.math.cast(usize, attrs.limits[1]) orelse continue != limit) continue;
        return candidate_id;
    }
    return null;
}

fn tryExecuteLinearNoBiasQkvPattern(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    value_device: []DeviceId,
    node_ids: []const NodeId,
    node_pos: usize,
    reachable: []const bool,
    device_id: DeviceId,
    exec_ctx: PartitionExecutor.ExecutionContext,
    skipped_nodes: []bool,
    last_use: []const u32,
    rt_map: std.AutoHashMapUnmanaged(NodeId, CT),
    donated: std.AutoHashMapUnmanaged(NodeId, void),
) !bool {
    const pattern = matchLinearNoBiasQkvPattern(graph, values, node_ids, node_pos, reachable, skipped_nodes) orelse return false;
    const prepared = (try prepareLinearNoBiasQkvRegion(cb, values, pattern, exec_ctx.stats)) orelse return false;
    return executeLinearNoBiasQkvPattern(
        allocator,
        graph,
        cb,
        values,
        value_device,
        device_id,
        exec_ctx,
        skipped_nodes,
        last_use,
        rt_map,
        donated,
        pattern,
        prepared,
    );
}

fn executeLinearNoBiasQkvPattern(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    value_device: []DeviceId,
    device_id: DeviceId,
    exec_ctx: PartitionExecutor.ExecutionContext,
    skipped_nodes: []bool,
    last_use: []const u32,
    rt_map: std.AutoHashMapUnmanaged(NodeId, CT),
    donated: std.AutoHashMapUnmanaged(NodeId, void),
    pattern: LinearNoBiasQkvPattern,
    prepared: PreparedQkvRegion,
) !bool {
    const input = valueFor(values, pattern.input_id) orelse return false;

    const qkv = (try cb.decoderRuntimeApplyLinearQkv(&.{
        .q_slot = prepared.q_slot,
        .k_slot = prepared.k_slot,
        .v_slot = prepared.v_slot,
        .input = input,
        .in_dim = pattern.in_dim,
        .q_out_dim = pattern.q_out_dim,
        .kv_out_dim = pattern.kv_out_dim,
    })) orelse return traceQkvRegionDeclined("backend_returned_null", pattern.q_id);

    values[@intCast(pattern.q_id)] = qkv.first;
    values[@intCast(pattern.k_id)] = qkv.second;
    values[@intCast(pattern.v_id)] = qkv.third;
    value_device[@intCast(pattern.q_id)] = device_id;
    value_device[@intCast(pattern.k_id)] = device_id;
    value_device[@intCast(pattern.v_id)] = device_id;
    skipped_nodes[@intCast(pattern.k_id)] = true;
    skipped_nodes[@intCast(pattern.v_id)] = true;

    if (exec_ctx.stats) |stats| {
        recordMetalGraphRegion(stats, .qkv, 3);
        stats.fused_graph_pattern_dispatches += 1;
        stats.fused_graph_nodes_elided += 2;
        const q_resident = isMetalResidentOrQuantizedDescriptor(cb, qkv.first);
        const k_resident = isMetalResidentOrQuantizedDescriptor(cb, qkv.second);
        const v_resident = isMetalResidentOrQuantizedDescriptor(cb, qkv.third);
        if (q_resident) {
            stats.device_resident_outputs += 1;
        } else {
            stats.host_materialized_outputs += 1;
            stats.host_materialized_runtime_region_outputs += 1;
            if (traceMetalHostOutputsEnabled()) traceMetalHostOutput(graph, pattern.q_id, "qkv_region_q_host_output");
        }
        if (k_resident) {
            stats.device_resident_outputs += 1;
        } else {
            stats.host_materialized_outputs += 1;
            stats.host_materialized_runtime_region_outputs += 1;
            if (traceMetalHostOutputsEnabled()) traceMetalHostOutput(graph, pattern.k_id, "qkv_region_k_host_output");
        }
        if (v_resident) {
            stats.device_resident_outputs += 1;
        } else {
            stats.host_materialized_outputs += 1;
            stats.host_materialized_runtime_region_outputs += 1;
            if (traceMetalHostOutputsEnabled()) traceMetalHostOutput(graph, pattern.v_id, "qkv_region_v_host_output");
        }
        recordGemmaRuntimeResidency(stats, graph, pattern.q_id, q_resident);
        recordGemmaRuntimeResidency(stats, graph, pattern.k_id, k_resident);
        recordGemmaRuntimeResidency(stats, graph, pattern.v_id, v_resident);
    }

    try interpreter.cloneOutputIfAliasedInputWouldBeFreed(
        allocator,
        graph,
        cb,
        values,
        pattern.k_id,
        last_use,
        rt_map,
        donated,
    );
    try interpreter.cloneOutputIfAliasedInputWouldBeFreed(
        allocator,
        graph,
        cb,
        values,
        pattern.v_id,
        last_use,
        rt_map,
        donated,
    );
    _ = try freeExpiredInputs(
        allocator,
        graph,
        cb,
        values,
        value_device,
        pattern.k_id,
        device_id,
        last_use,
        null,
        rt_map,
        donated,
        exec_ctx,
    );
    _ = try freeExpiredInputs(
        allocator,
        graph,
        cb,
        values,
        value_device,
        pattern.v_id,
        device_id,
        last_use,
        null,
        rt_map,
        donated,
        exec_ctx,
    );

    if (traceMetalGraphFusionsEnabled()) {
        std.debug.print(
            "metal_graph_fusion_trace: qkv_region executed q={d} k={d} v={d} rows={d} in={d} q_out={d} kv_out={d}\n",
            .{ pattern.q_id, pattern.k_id, pattern.v_id, pattern.rows, pattern.in_dim, pattern.q_out_dim, pattern.kv_out_dim },
        );
    }
    return true;
}

fn traceQkvRegionDeclined(reason: []const u8, node_id: NodeId) bool {
    if (traceMetalGraphFusionsEnabled()) {
        std.debug.print(
            "metal_graph_fusion_trace: qkv_region declined reason={s} node={d}\n",
            .{ reason, node_id },
        );
    }
    return false;
}

fn matchLinearNoBiasQkvPattern(
    graph: *const Graph,
    values: []?CT,
    node_ids: []const NodeId,
    node_pos: usize,
    reachable: []const bool,
    skipped_nodes: []const bool,
) ?LinearNoBiasQkvPattern {
    const q_id = node_ids[node_pos];
    const q = graph.node(q_id);
    const q_attrs = switch (q.op) {
        .fused_linear_no_bias => |attrs| attrs,
        else => return null,
    };
    if (q_attrs.num_projections != 0 or q_attrs.rows == 0 or q_attrs.in_dim == 0 or q_attrs.out_dim == 0) return null;
    const q_inputs = q.getInputs();
    if (q_inputs.len < 2) return null;
    const q_weight_name = linearWeightParameterName(graph, q) orelse return null;
    if (!isGemmaQWeightName(q_weight_name)) return null;

    const k_id = findQkvSiblingLinear(graph, values, node_ids, node_pos + 1, reachable, skipped_nodes, q_inputs[0], q_attrs, &isGemmaKWeightName) orelse return null;
    const v_id = findQkvSiblingLinear(graph, values, node_ids, node_pos + 1, reachable, skipped_nodes, q_inputs[0], q_attrs, &isGemmaVWeightName) orelse return null;
    const k = graph.node(k_id);
    const v = graph.node(v_id);
    const k_attrs = switch (k.op) {
        .fused_linear_no_bias => |attrs| attrs,
        else => return null,
    };
    const v_attrs = switch (v.op) {
        .fused_linear_no_bias => |attrs| attrs,
        else => return null,
    };
    if (k_attrs.out_dim != v_attrs.out_dim) return null;
    const k_inputs = k.getInputs();
    const v_inputs = v.getInputs();
    if (k_inputs.len < 2 or v_inputs.len < 2) return null;

    return .{
        .q_id = q_id,
        .k_id = k_id,
        .v_id = v_id,
        .input_id = q_inputs[0],
        .q_weight_id = q_inputs[1],
        .k_weight_id = k_inputs[1],
        .v_weight_id = v_inputs[1],
        .rows = q_attrs.rows,
        .in_dim = q_attrs.in_dim,
        .q_out_dim = q_attrs.out_dim,
        .kv_out_dim = k_attrs.out_dim,
    };
}

fn findQkvSiblingLinear(
    graph: *const Graph,
    values: []?CT,
    node_ids: []const NodeId,
    start_pos: usize,
    reachable: []const bool,
    skipped_nodes: []const bool,
    input_id: NodeId,
    q_attrs: anytype,
    weight_name_predicate: *const fn ([]const u8) bool,
) ?NodeId {
    for (node_ids[start_pos..]) |candidate_id| {
        const candidate_index: usize = @intCast(candidate_id);
        if (candidate_index >= reachable.len or !reachable[candidate_index]) continue;
        if (candidate_index < skipped_nodes.len and skipped_nodes[candidate_index]) continue;
        if (values[candidate_index] != null) continue;

        const candidate = graph.node(candidate_id);
        const candidate_attrs = switch (candidate.op) {
            .fused_linear_no_bias => |attrs| attrs,
            else => continue,
        };
        if (candidate_attrs.num_projections != 0) continue;
        if (candidate_attrs.rows != q_attrs.rows or candidate_attrs.in_dim != q_attrs.in_dim) continue;
        const candidate_inputs = candidate.getInputs();
        if (candidate_inputs.len < 2 or candidate_inputs[0] != input_id) continue;
        const weight_name = linearWeightParameterName(graph, candidate) orelse continue;
        if (!weight_name_predicate(weight_name)) continue;
        return candidate_id;
    }
    return null;
}

fn isGemmaQWeightName(name: []const u8) bool {
    return std.mem.indexOf(u8, name, ".self_attn.q_proj.weight") != null;
}

fn isGemmaKWeightName(name: []const u8) bool {
    return std.mem.indexOf(u8, name, ".self_attn.k_proj.weight") != null;
}

fn isGemmaVWeightName(name: []const u8) bool {
    return std.mem.indexOf(u8, name, ".self_attn.v_proj.weight") != null;
}

fn isQueryProjectionWeightName(name: []const u8) bool {
    return isGemmaQWeightName(name) or std.mem.indexOf(u8, name, ".attention.self.query_proj.weight") != null;
}

fn isKeyProjectionWeightName(name: []const u8) bool {
    return isGemmaKWeightName(name) or std.mem.indexOf(u8, name, ".attention.self.key_proj.weight") != null;
}

fn isValueProjectionWeightName(name: []const u8) bool {
    return isGemmaVWeightName(name) or std.mem.indexOf(u8, name, ".attention.self.value_proj.weight") != null;
}

fn isDebertaAttentionOutputDenseName(name: []const u8) bool {
    return std.mem.indexOf(u8, name, ".attention.output.dense.") != null;
}

fn tryExecuteLinearNoBiasPairPattern(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    value_device: []DeviceId,
    node_ids: []const NodeId,
    node_pos: usize,
    reachable: []const bool,
    device_id: DeviceId,
    exec_ctx: PartitionExecutor.ExecutionContext,
    skipped_nodes: []bool,
    last_use: []const u32,
    rt_map: std.AutoHashMapUnmanaged(NodeId, CT),
    donated: std.AutoHashMapUnmanaged(NodeId, void),
) !bool {
    const first_id = node_ids[node_pos];
    const first = graph.node(first_id);
    const first_attrs = switch (first.op) {
        .fused_linear_no_bias => |attrs| attrs,
        else => return false,
    };
    if (first_attrs.num_projections != 0) return false;
    const first_inputs = first.getInputs();
    if (first_inputs.len < 2) return false;
    const input_id = first_inputs[0];
    const weight_a_id = first_inputs[1];
    const input = valueFor(values, input_id) orelse return false;
    const weight_a = valueFor(values, weight_a_id) orelse return false;

    const second_id = findLinearNoBiasPairCandidate(
        graph,
        values,
        node_ids,
        node_pos + 1,
        reachable,
        skipped_nodes,
        first_inputs,
        first_attrs,
    ) orelse return false;
    const second = graph.node(second_id);
    const second_inputs = second.getInputs();
    if (second_inputs.len < 2) return false;
    const weight_b = valueFor(values, second_inputs[1]) orelse return false;

    const pair = cb.linearNoBiasPair(
        input,
        weight_a,
        weight_b,
        first_attrs.rows,
        first_attrs.in_dim,
        first_attrs.out_dim,
    ) catch |err| switch (err) {
        error.UnsupportedOperation, error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => return false,
        else => return err,
    };

    values[@intCast(first_id)] = pair.first;
    values[@intCast(second_id)] = pair.second;
    value_device[@intCast(first_id)] = device_id;
    value_device[@intCast(second_id)] = device_id;
    skipped_nodes[@intCast(second_id)] = true;

    if (exec_ctx.stats) |stats| {
        stats.fused_graph_pattern_dispatches += 1;
        stats.fused_graph_nodes_elided += 1;
        stats.metal_linear_pair_fusions += 1;
        const second_resident = isMetalResidentOrQuantizedDescriptor(cb, pair.second);
        if (second_resident) {
            stats.device_resident_outputs += 1;
        } else {
            stats.host_materialized_outputs += 1;
            stats.host_materialized_runtime_region_outputs += 1;
            if (traceMetalHostOutputsEnabled()) traceMetalHostOutput(graph, second_id, "linear_pair_second_host_output");
        }
        recordGemmaRuntimeResidency(stats, graph, second_id, second_resident);
    }

    try interpreter.cloneOutputIfAliasedInputWouldBeFreed(
        allocator,
        graph,
        cb,
        values,
        second_id,
        last_use,
        rt_map,
        donated,
    );
    _ = try freeExpiredInputs(
        allocator,
        graph,
        cb,
        values,
        value_device,
        second_id,
        device_id,
        last_use,
        null,
        rt_map,
        donated,
        exec_ctx,
    );
    return true;
}

fn findLinearNoBiasPairCandidate(
    graph: *const Graph,
    values: []?CT,
    node_ids: []const NodeId,
    start_pos: usize,
    reachable: []const bool,
    skipped_nodes: []const bool,
    first_inputs: []const NodeId,
    first_attrs: anytype,
) ?NodeId {
    if (first_inputs.len < 2) return null;
    for (node_ids[start_pos..]) |candidate_id| {
        const candidate_index: usize = @intCast(candidate_id);
        if (candidate_index >= reachable.len or !reachable[candidate_index]) continue;
        if (candidate_index < skipped_nodes.len and skipped_nodes[candidate_index]) continue;
        if (values[candidate_index] != null) continue;

        const candidate = graph.node(candidate_id);
        const candidate_attrs = switch (candidate.op) {
            .fused_linear_no_bias => |attrs| attrs,
            else => continue,
        };
        if (candidate_attrs.num_projections != 0) continue;
        if (candidate_attrs.rows != first_attrs.rows or
            candidate_attrs.in_dim != first_attrs.in_dim or
            candidate_attrs.out_dim != first_attrs.out_dim)
        {
            continue;
        }

        const candidate_inputs = candidate.getInputs();
        if (candidate_inputs.len < 2) continue;
        if (candidate_inputs[0] != first_inputs[0]) continue;
        if (candidate_inputs[1] == first_inputs[1]) continue;
        return candidate_id;
    }
    return null;
}

fn classifyGemmaRuntimeResidencyNode(graph: *const Graph, node_id: NodeId) ?GemmaRuntimeResidencyCategory {
    const node = graph.node(node_id);
    switch (node.op) {
        .fused_linear, .fused_linear_no_bias => {
            const weight_name = linearWeightParameterName(graph, node) orelse return null;
            if (!isGemmaWeightName(weight_name)) return null;
            if (std.mem.indexOf(u8, weight_name, ".self_attn.q_proj.weight") != null or
                std.mem.indexOf(u8, weight_name, ".self_attn.k_proj.weight") != null or
                std.mem.indexOf(u8, weight_name, ".self_attn.v_proj.weight") != null)
            {
                return .qkv;
            }
            if (std.mem.indexOf(u8, weight_name, ".self_attn.o_proj.weight") != null) return .o_proj;
            if (std.mem.indexOf(u8, weight_name, ".mlp.gate_proj.weight") != null or
                std.mem.indexOf(u8, weight_name, ".mlp.up_proj.weight") != null or
                std.mem.indexOf(u8, weight_name, ".mlp.down_proj.weight") != null)
            {
                return .mlp_proj;
            }
            return null;
        },
        .dot_general, .fused_gqa_causal_attention => return if (nodeDependsOnGemmaParameter(graph, node_id, 64)) .attention_matmul else null,
        .fused_rms_norm => return if (nodeDependsOnGemmaParameter(graph, node_id, 8)) .rms_norm else null,
        .fused_softmax => return if (nodeDependsOnGemmaParameter(graph, node_id, 64)) .softmax else null,
        .add, .fused_elem_add => return if (nodeDependsOnGemmaParameter(graph, node_id, 64)) .residual_add else null,
        .mul, .fused_elem_multiply => return if (nodeDependsOnGemmaParameter(graph, node_id, 64)) .elementwise_mul else null,
        .fused_add_mul_scalar => return if (nodeDependsOnGemmaParameter(graph, node_id, 64)) .elementwise_mul else null,
        else => return null,
    }
}

fn linearWeightParameterName(graph: *const Graph, node: *const ml.graph.Node) ?[]const u8 {
    const inputs = node.getInputs();
    if (inputs.len < 2 or inputs[1] == null_node) return null;
    const weight = graph.node(inputs[1]);
    if (std.meta.activeTag(weight.op) != .parameter) return null;
    return graph.parameterName(weight);
}

fn layerIndexForWeight(graph: *const Graph, weight_id: NodeId) ?usize {
    return layerIndexForWeightDepth(graph, weight_id, 8);
}

fn layerIndexForWeightDepth(graph: *const Graph, weight_id: NodeId, depth: usize) ?usize {
    if (weight_id == null_node) return null;
    const weight = graph.node(weight_id);
    if (std.meta.activeTag(weight.op) == .parameter) {
        const name = graph.parameterName(weight);
        if (parseGemmaLayerIndex(name)) |layer_index| return layer_index;
        return parseDebertaLayerIndex(name);
    }
    if (depth == 0) return null;
    for (weight.getInputs()) |input_id| {
        if (layerIndexForWeightDepth(graph, input_id, depth - 1)) |layer_index| return layer_index;
    }
    return null;
}

fn parseGemmaLayerIndex(name: []const u8) ?usize {
    const prefix = "model.layers.";
    if (!std.mem.startsWith(u8, name, prefix)) return null;
    const rest = name[prefix.len..];
    var end: usize = 0;
    while (end < rest.len and std.ascii.isDigit(rest[end])) : (end += 1) {}
    if (end == 0) return null;
    return std.fmt.parseUnsigned(usize, rest[0..end], 10) catch null;
}

fn parseDebertaLayerIndex(name: []const u8) ?usize {
    const prefix = "encoder.layer.";
    if (!std.mem.startsWith(u8, name, prefix)) return null;
    const rest = name[prefix.len..];
    var end: usize = 0;
    while (end < rest.len and std.ascii.isDigit(rest[end])) : (end += 1) {}
    if (end == 0) return null;
    return std.fmt.parseUnsigned(usize, rest[0..end], 10) catch null;
}

fn isGemmaWeightName(name: []const u8) bool {
    return std.mem.startsWith(u8, name, "model.layers.") or
        std.mem.startsWith(u8, name, "model.norm.") or
        std.mem.startsWith(u8, name, "model.embed_tokens.");
}

const gemma_dependency_visit_limit = 8192;

fn nodeVisited(visited: []const NodeId, node_id: NodeId) bool {
    for (visited) |seen| {
        if (seen == node_id) return true;
    }
    return false;
}

fn nodeDependsOnGemmaParameter(graph: *const Graph, node_id: NodeId, max_depth: usize) bool {
    if (node_id == null_node) return false;
    const StackItem = struct {
        id: NodeId,
        depth: usize,
    };
    var stack: [gemma_dependency_visit_limit]StackItem = undefined;
    var stack_len: usize = 1;
    stack[0] = .{ .id = node_id, .depth = max_depth };
    var visited: [gemma_dependency_visit_limit]NodeId = undefined;
    var visited_len: usize = 0;

    while (stack_len != 0) {
        stack_len -= 1;
        const item = stack[stack_len];
        if (item.id == null_node) continue;
        if (nodeVisited(visited[0..visited_len], item.id)) continue;
        if (visited_len == visited.len) return false;
        visited[visited_len] = item.id;
        visited_len += 1;

        const node = graph.node(item.id);
        if (std.meta.activeTag(node.op) == .parameter) {
            if (isGemmaWeightName(graph.parameterName(node))) return true;
            continue;
        }
        if (item.depth == 0) continue;
        for (node.getInputs()) |input_id| {
            if (input_id == null_node) continue;
            if (stack_len == stack.len) return false;
            stack[stack_len] = .{ .id = input_id, .depth = item.depth - 1 };
            stack_len += 1;
        }
    }
    return false;
}

test "gemma runtime residency stats classify gemma graph nodes only" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const rows: usize = 1;
    const dim: usize = 4;
    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ rows, dim }));
    const rms_weight = try b.parameter("model.layers.0.input_layernorm.weight", ml.graph.Shape.init(.f32, &.{dim}));
    const q_weight = try b.parameter("model.layers.0.self_attn.q_proj.weight", ml.graph.Shape.init(.f32, &.{ dim, dim }));
    const k_weight = try b.parameter("model.layers.0.self_attn.k_proj.weight", ml.graph.Shape.init(.f32, &.{ dim, dim }));
    const v_weight = try b.parameter("model.layers.0.self_attn.v_proj.weight", ml.graph.Shape.init(.f32, &.{ dim, dim }));
    const o_weight = try b.parameter("model.layers.0.self_attn.o_proj.weight", ml.graph.Shape.init(.f32, &.{ dim, dim }));
    const gate_weight = try b.parameter("model.layers.0.mlp.gate_proj.weight", ml.graph.Shape.init(.f32, &.{ dim, dim }));
    const up_weight = try b.parameter("model.layers.0.mlp.up_proj.weight", ml.graph.Shape.init(.f32, &.{ dim, dim }));
    const down_weight = try b.parameter("model.layers.0.mlp.down_proj.weight", ml.graph.Shape.init(.f32, &.{ dim, dim }));
    const non_gemma_weight = try b.parameter("clip.text_projection.weight", ml.graph.Shape.init(.f32, &.{ dim, dim }));

    const normed = try b.rmsNorm(x, rms_weight, @intCast(dim), 1e-5);
    const q = try b.linearNoBias(normed, q_weight, @intCast(rows), @intCast(dim), @intCast(dim));
    const k = try b.linearNoBias(normed, k_weight, @intCast(rows), @intCast(dim), @intCast(dim));
    const v = try b.linearNoBias(normed, v_weight, @intCast(rows), @intCast(dim), @intCast(dim));
    const q_4d = try b.reshape(q, ml.graph.Shape.init(.f32, &.{ 1, 1, rows, dim }));
    const k_4d = try b.reshape(k, ml.graph.Shape.init(.f32, &.{ 1, 1, rows, dim }));
    const v_4d = try b.reshape(v, ml.graph.Shape.init(.f32, &.{ 1, 1, rows, dim }));
    const scores = try g.addNode(.{
        .op = .{ .dot_general = .{
            .lhs_contracting = .{ 3, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_contracting = .{ 3, 0, 0, 0, 0, 0, 0, 0 },
            .lhs_batch = .{ 0, 1, 0, 0, 0, 0, 0, 0 },
            .rhs_batch = .{ 0, 1, 0, 0, 0, 0, 0, 0 },
            .num_contracting = 1,
            .num_batch = 2,
        } },
        .output_shape = ml.graph.Shape.init(.f32, &.{ 1, 1, rows, rows }),
        .inputs = .{ q_4d, k_4d, null_node, null_node },
        .num_inputs = 2,
    });
    const scale = try b.scalarConst(.f32, 0.5);
    const scaled_scores = try b.mul(scores, scale);
    const mask = try b.scalarConst(.f32, 0.0);
    const masked_scores = try b.add(scaled_scores, mask);
    const probs = try b.softmax(masked_scores);
    const attn = try g.addNode(.{
        .op = .{ .dot_general = .{
            .lhs_contracting = .{ 3, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_contracting = .{ 2, 0, 0, 0, 0, 0, 0, 0 },
            .lhs_batch = .{ 0, 1, 0, 0, 0, 0, 0, 0 },
            .rhs_batch = .{ 0, 1, 0, 0, 0, 0, 0, 0 },
            .num_contracting = 1,
            .num_batch = 2,
        } },
        .output_shape = ml.graph.Shape.init(.f32, &.{ 1, 1, rows, dim }),
        .inputs = .{ probs, v_4d, null_node, null_node },
        .num_inputs = 2,
    });
    const attn_flat = try b.reshape(attn, ml.graph.Shape.init(.f32, &.{ rows, dim }));
    const o = try b.linearNoBias(attn_flat, o_weight, @intCast(rows), @intCast(dim), @intCast(dim));
    const resid = try b.add(x, o);
    const gate = try b.linearNoBias(resid, gate_weight, @intCast(rows), @intCast(dim), @intCast(dim));
    const up = try b.linearNoBias(resid, up_weight, @intCast(rows), @intCast(dim), @intCast(dim));
    const act = try b.gelu(gate);
    const gated = try b.mul(act, up);
    const down = try b.linearNoBias(gated, down_weight, @intCast(rows), @intCast(dim), @intCast(dim));
    const non_gemma_linear = try b.linearNoBias(x, non_gemma_weight, @intCast(rows), @intCast(dim), @intCast(dim));
    const non_gemma_add = try b.add(x, non_gemma_linear);

    var shared_tail = down;
    for (0..32) |_| {
        const lhs = try b.add(shared_tail, resid);
        const rhs = try b.mul(lhs, scale);
        shared_tail = try b.add(lhs, rhs);
    }
    try std.testing.expect(nodeDependsOnGemmaParameter(&g, shared_tail, 64));

    var stats: PartitionExecutor.ExecutionStats = .{};
    for (&[_]NodeId{ q, k, v, o, gate, up, down, scores, scaled_scores, masked_scores, probs, attn, normed, resid, gated }) |node_id| {
        recordGemmaRuntimeResidency(&stats, &g, node_id, true);
    }
    recordGemmaRuntimeResidency(&stats, &g, non_gemma_linear, true);
    recordGemmaRuntimeResidency(&stats, &g, non_gemma_add, true);

    try std.testing.expectEqual(@as(u64, 3), stats.gemma_qkv_hits);
    try std.testing.expectEqual(@as(u64, 1), stats.gemma_o_proj_hits);
    try std.testing.expectEqual(@as(u64, 3), stats.gemma_mlp_proj_hits);
    try std.testing.expectEqual(@as(u64, 2), stats.gemma_attention_matmul_hits);
    try std.testing.expectEqual(@as(u64, 1), stats.gemma_rms_norm_hits);
    try std.testing.expectEqual(@as(u64, 1), stats.gemma_softmax_hits);
    try std.testing.expectEqual(@as(u64, 2), stats.gemma_residual_add_hits);
    try std.testing.expectEqual(@as(u64, 2), stats.gemma_elementwise_mul_hits);
    try std.testing.expectEqual(@as(u64, 0), stats.gemma_qkv_fallbacks);
    try std.testing.expectEqual(@as(u64, 0), stats.gemma_o_proj_fallbacks);
    try std.testing.expectEqual(@as(u64, 0), stats.gemma_mlp_proj_fallbacks);
    try std.testing.expectEqual(@as(u64, 0), stats.gemma_attention_matmul_fallbacks);
    try std.testing.expectEqual(@as(u64, 0), stats.gemma_rms_norm_fallbacks);
    try std.testing.expectEqual(@as(u64, 0), stats.gemma_softmax_fallbacks);
    try std.testing.expectEqual(@as(u64, 0), stats.gemma_residual_add_fallbacks);
    try std.testing.expectEqual(@as(u64, 0), stats.gemma_elementwise_mul_fallbacks);
}

fn partitionIndexForNodes(
    buffer_plan: *const buffer_plan_mod.BufferPlan,
    node_ids: []const NodeId,
) !u32 {
    if (node_ids.len == 0) return error.InvalidPartitionPlan;
    const first = buffer_plan.slotForNode(node_ids[0]) orelse return error.InvalidBufferPlan;
    const partition_index = first.partition_index;
    for (node_ids) |node_id| {
        const slot = buffer_plan.slotForNode(node_id) orelse return error.InvalidBufferPlan;
        if (slot.partition_index != partition_index) return error.InvalidPartitionPlan;
    }
    return partition_index;
}

fn validatePartitionView(
    view: buffer_plan_mod.PartitionBufferView,
    node_ids: []const NodeId,
) !void {
    if (view.backend != .metal) return error.InvalidPartitionPlan;
    for (node_ids) |node_id| {
        var found = false;
        for (view.slots) |slot_view| {
            if (slot_view.slot.node_id == node_id and slot_view.roles.local) {
                found = true;
                break;
            }
        }
        if (!found) return error.InvalidBufferPlan;
    }
}

const RuntimeUnaryOp = enum {
    negate,
    sqrt,
    rsqrt,
    exp,
    log,
    sin,
    cos,
    tanh,
    erf,
    abs,
};

fn runtimeUnaryActivationKind(op: RuntimeUnaryOp) u32 {
    return switch (op) {
        .negate => 6,
        .sqrt => 7,
        .rsqrt => 8,
        .exp => 9,
        .log => 10,
        .sin => 11,
        .cos => 12,
        .tanh => 13,
        .erf => 14,
        .abs => 15,
    };
}

fn executeRuntimeUnary(
    cb: *const ComputeBackend,
    values: []?CT,
    inputs: []const NodeId,
    op: RuntimeUnaryOp,
    output_hint: ?CT,
) !?CT {
    const input = valueFor(values, inputs[0]) orelse return null;
    // Slot-bound output: write the unary result directly into the pooled buffer.
    if (output_hint) |hint| {
        if (isMetalDeviceResident(cb, input)) {
            if (try metal_compute_mod.MetalCompute.unaryInto(cb, input, runtimeUnaryActivationKind(op), hint)) |out| return out;
        }
    }
    return switch (op) {
        .negate => cb.primNegate(input),
        .sqrt => cb.primSqrt(input),
        .rsqrt => cb.primRsqrt(input),
        .exp => cb.primExp(input),
        .log => cb.primLog(input),
        .sin => cb.primSin(input),
        .cos => cb.primCos(input),
        .tanh => cb.primTanh(input),
        .erf => cb.primErf(input),
        .abs => cb.primAbs(input),
    } catch |err| switch (err) {
        error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => null,
        else => return err,
    };
}

fn tryExecuteMetalCommand(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    node_id: NodeId,
    op_plan: ?OperatorPlan,
    exec_state: *interpreter.ExecState,
    output_hint: ?CT,
    attn_mask_cache: ?*AttnMaskCache,
    stats: ?*PartitionExecutor.ExecutionStats,
    resident_input_cache: ?*ResidentInputCache,
) !?CT {
    const n = graph.node(node_id);
    const inputs = n.getInputs();
    if (comptime build_options.enable_metal) {
        metal_tensor_mod.setOwnedAllocationContext(@tagName(n.op));
        defer metal_tensor_mod.clearOwnedAllocationContext();
    }
    return switch (n.op) {
        .constant => |attrs| try executeRuntimeConstant(graph, cb, n.output_shape, attrs),
        // Fused disentangled attention forward/backward: run in-frame (instead of
        // the interpreter fallback's separate command buffer) by slicing the
        // packed device inputs and dispatching the fused kernels directly.
        .fused_disentangled_attention => |attrs| blk: {
            const bs: i64 = @intCast(attrs.batch * attrs.seq_len);
            const hh: i64 = @intCast(attrs.num_heads * attrs.head_dim);
            const num_rel: i64 = @intCast(2 * attrs.seq_len - 1);
            const qkv = valueFor(values, inputs[0]) orelse break :blk null;
            const qr_kr = valueFor(values, inputs[1]) orelse break :blk null;
            const bias = valueFor(values, inputs[2]) orelse break :blk null;
            const qkv_shape = [_]i64{ 3 * bs, hh };
            const qr_shape = [_]i64{ 2 * num_rel, hh };
            const q = cb.primSlice(qkv, &.{ 0, 0 }, &.{ bs, hh }, &.{ 1, 1 }, &qkv_shape) catch break :blk null;
            defer cb.free(q);
            const k = cb.primSlice(qkv, &.{ bs, 0 }, &.{ 2 * bs, hh }, &.{ 1, 1 }, &qkv_shape) catch break :blk null;
            defer cb.free(k);
            const v = cb.primSlice(qkv, &.{ 2 * bs, 0 }, &.{ 3 * bs, hh }, &.{ 1, 1 }, &qkv_shape) catch break :blk null;
            defer cb.free(v);
            const q_r = cb.primSlice(qr_kr, &.{ 0, 0 }, &.{ num_rel, hh }, &.{ 1, 1 }, &qr_shape) catch break :blk null;
            defer cb.free(q_r);
            const k_r = cb.primSlice(qr_kr, &.{ num_rel, 0 }, &.{ 2 * num_rel, hh }, &.{ 1, 1 }, &qr_shape) catch break :blk null;
            defer cb.free(k_r);
            const mask = cachedAttentionMaskFromBias(attn_mask_cache, allocator, cb, bias, attrs.batch, attrs.seq_len, attrs.num_heads) catch break :blk null;
            defer if (attn_mask_cache == null) allocator.free(mask);
            break :blk cb.disentangledRelativeAttention(q, k, v, q_r, k_r, mask, attrs.batch, attrs.seq_len, attrs.num_heads, attrs.head_dim) catch |err| switch (err) {
                error.UnsupportedOperation, error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch, error.UnsupportedTensorType => null,
                else => return err,
            };
        },
        .fused_disentangled_attention_backward => |attrs| blk: {
            const bs: i64 = @intCast(attrs.batch * attrs.seq_len);
            const hh: i64 = @intCast(attrs.num_heads * attrs.head_dim);
            const num_rel: i64 = @intCast(2 * attrs.seq_len - 1);
            const qkv = valueFor(values, inputs[0]) orelse break :blk null;
            const qr_kr = valueFor(values, inputs[1]) orelse break :blk null;
            const bias = valueFor(values, inputs[2]) orelse break :blk null;
            const d_out = valueFor(values, inputs[3]) orelse break :blk null;
            const qkv_shape = [_]i64{ 3 * bs, hh };
            const qr_shape = [_]i64{ 2 * num_rel, hh };
            const q = cb.primSlice(qkv, &.{ 0, 0 }, &.{ bs, hh }, &.{ 1, 1 }, &qkv_shape) catch break :blk null;
            defer cb.free(q);
            const k = cb.primSlice(qkv, &.{ bs, 0 }, &.{ 2 * bs, hh }, &.{ 1, 1 }, &qkv_shape) catch break :blk null;
            defer cb.free(k);
            const v = cb.primSlice(qkv, &.{ 2 * bs, 0 }, &.{ 3 * bs, hh }, &.{ 1, 1 }, &qkv_shape) catch break :blk null;
            defer cb.free(v);
            const q_r = cb.primSlice(qr_kr, &.{ 0, 0 }, &.{ num_rel, hh }, &.{ 1, 1 }, &qr_shape) catch break :blk null;
            defer cb.free(q_r);
            const k_r = cb.primSlice(qr_kr, &.{ num_rel, 0 }, &.{ 2 * num_rel, hh }, &.{ 1, 1 }, &qr_shape) catch break :blk null;
            defer cb.free(k_r);
            const mask = cachedAttentionMaskFromBias(attn_mask_cache, allocator, cb, bias, attrs.batch, attrs.seq_len, attrs.num_heads) catch break :blk null;
            defer if (attn_mask_cache == null) allocator.free(mask);
            break :blk cb.disentangledRelativeAttentionBackward(q, k, v, q_r, k_r, mask, d_out, attrs.batch, attrs.seq_len, attrs.num_heads, attrs.head_dim) catch |err| switch (err) {
                error.UnsupportedOperation, error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch, error.UnsupportedTensorType => null,
                else => return err,
            };
        },
        .reshape => |attrs| blk: {
            const input = valueFor(values, inputs[0]) orelse break :blk null;
            var dims_buf: [ml.graph.shape.max_rank]i64 = undefined;
            const dims = try fillShapeDims(attrs.new_shape, &dims_buf);
            break :blk cb.primReshape(input, dims) catch |err| switch (err) {
                error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => null,
                else => return err,
            };
        },
        .transpose => |attrs| blk: {
            var host_input_to_free: ?CT = null;
            defer if (host_input_to_free) |ct| cb.free(ct);
            const input_source = valueFor(values, inputs[0]) orelse break :blk null;
            const input_source_resident = isMetalDeviceResident(cb, input_source);
            const preserve_host_view = !input_source_resident;
            const input = if (preserve_host_view and input_source_resident) host_input: {
                var host_shape_buf: [ml.graph.shape.max_rank]i32 = undefined;
                const source_shape = graph.node(inputs[0]).output_shape;
                if (source_shape.rank() > host_shape_buf.len) break :host_input input_source;
                for (0..source_shape.rank()) |axis| {
                    const dim = source_shape.dim(@intCast(axis));
                    if (dim <= 0) break :host_input input_source;
                    host_shape_buf[axis] = @intCast(dim);
                }
                const host_data = cb.toFloat32(input_source, allocator) catch break :host_input input_source;
                defer allocator.free(host_data);
                const host_ct = cb.fromFloat32Shape(host_data, host_shape_buf[0..source_shape.rank()]) catch break :host_input input_source;
                host_input_to_free = host_ct;
                break :host_input host_ct;
            } else input_source;
            var in_shape_buf: [ml.graph.shape.max_rank]i64 = undefined;
            const in_shape = try fillShapeDims(graph.node(inputs[0]).output_shape, &in_shape_buf);
            var perm_buf: [ml.graph.shape.max_rank]u8 = undefined;
            const perm = transpose_utils.effectivePerm(attrs, graph.node(inputs[0]).output_shape.rank(), &perm_buf);
            // Slot-bound output: transpose directly into the per-node pool buffer.
            if (output_hint) |hint| {
                if (input_source_resident) {
                    if (try metal_compute_mod.MetalCompute.transposeInto(cb, input, perm, in_shape, hint)) |out| break :blk out;
                }
            }
            const transposed = cb.primTranspose(input, perm, in_shape) catch |err| switch (err) {
                error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => null,
                else => return err,
            };
            if (transposed) |ct| {
                if (!preserve_host_view and !isMetalDeviceResident(cb, ct)) {
                    if (try makeMetalDeviceResident(cb, ct)) |device_ct| {
                        cb.free(ct);
                        break :blk device_ct;
                    }
                }
            }
            break :blk transposed;
        },
        .broadcast_in_dim => |attrs| try executeRuntimeBroadcast(cb, values, inputs, graph.node(inputs[0]).output_shape, attrs, output_hint),
        .neg => try executeRuntimeUnary(cb, values, inputs, .negate, output_hint),
        .sqrt => try executeRuntimeUnary(cb, values, inputs, .sqrt, output_hint),
        .rsqrt => try executeRuntimeUnary(cb, values, inputs, .rsqrt, output_hint),
        .exp => try executeRuntimeUnary(cb, values, inputs, .exp, output_hint),
        .log => try executeRuntimeUnary(cb, values, inputs, .log, output_hint),
        .sin => try executeRuntimeUnary(cb, values, inputs, .sin, output_hint),
        .cos => try executeRuntimeUnary(cb, values, inputs, .cos, output_hint),
        .tanh => try executeRuntimeUnary(cb, values, inputs, .tanh, output_hint),
        .erf => try executeRuntimeUnary(cb, values, inputs, .erf, output_hint),
        .abs => try executeRuntimeUnary(cb, values, inputs, .abs, output_hint),
        .slice => |attrs| try executeRuntimeSlice(graph, cb, values, inputs, attrs),
        .concat_prim => |attrs| try executeRuntimeConcatPrim(graph, cb, values, inputs, attrs),
        .gather => |attrs| try executeRuntimeGather(graph, cb, values, node_id, inputs, attrs, stats, resident_input_cache),
        .scatter_add => |attrs| try executeRuntimeScatterAdd(graph, cb, values, node_id, inputs, attrs),
        .convert_dtype => |attrs| try executeRuntimeConvertDType(graph, cb, values, inputs, attrs),
        .fused_gelu => try executeRuntimeActivation(cb, values, inputs, .gelu, n.output_shape, output_hint),
        .fused_gelu_exact => try executeRuntimeActivation(cb, values, inputs, .gelu_exact, n.output_shape, output_hint),
        .fused_gelu_backward => try executeRuntimeGeluBackward(cb, values, inputs, n.output_shape, false),
        .fused_gelu_exact_backward => try executeRuntimeGeluBackward(cb, values, inputs, n.output_shape, true),
        .fused_relu => try executeRuntimeActivation(cb, values, inputs, .relu, n.output_shape, output_hint),
        .fused_silu => try executeRuntimeActivation(cb, values, inputs, .silu, n.output_shape, output_hint),
        .fused_quick_gelu => try executeRuntimeActivation(cb, values, inputs, .quick_gelu, n.output_shape, output_hint),
        .fused_sigmoid => try executeRuntimeFusedUnary(cb, values, inputs, .sigmoid),
        .fused_tanh_act => try executeRuntimeFusedUnary(cb, values, inputs, .tanh_act),
        .fused_elem_add, .add => try executeRuntimeAdd(graph, cb, values, inputs, n.output_shape, output_hint),
        .fused_elem_multiply, .mul => try executeRuntimeBinary(graph, cb, values, node_id, inputs, n.output_shape, .multiply, output_hint),
        .fused_add_mul_scalar => try executeRuntimeAddMulScalar(cb, values, inputs),
        .sub => try executeRuntimeBinary(graph, cb, values, node_id, inputs, n.output_shape, .subtract, output_hint),
        .div => try executeRuntimeBinary(graph, cb, values, node_id, inputs, n.output_shape, .divide, output_hint),
        .less_than => try executeRuntimeBinary(graph, cb, values, node_id, inputs, n.output_shape, .less_than, null),
        .where_select => try executeRuntimeWhereSelect(cb, values, inputs),
        .reduce_sum => |attrs| try executeRuntimeReduce(graph, cb, values, inputs, attrs, .sum, output_hint, stats, resident_input_cache),
        .reduce_max => |attrs| try executeRuntimeReduce(graph, cb, values, inputs, attrs, .max, output_hint, stats, resident_input_cache),
        .reduce_mean => |attrs| try executeRuntimeReduce(graph, cb, values, inputs, attrs, .mean, output_hint, stats, resident_input_cache),
        .fused_softmax => |attrs| try executeRuntimeSoftmax(cb, values, inputs, attrs.dim),
        .fused_log_softmax => |attrs| try executeRuntimeLogSoftmax(cb, values, inputs, attrs.dim),
        .fused_masked_bce_with_logits_loss => |attrs| blk: {
            const logits = valueFor(values, inputs[0]) orelse break :blk null;
            const labels = valueFor(values, inputs[1]) orelse break :blk null;
            const mask = valueFor(values, inputs[2]) orelse break :blk null;
            var out_shape_buf: [ml.graph.shape.max_rank]i64 = undefined;
            const out_shape = try fillShapeDims(n.output_shape, &out_shape_buf);
            break :blk cb.maskedBceWithLogitsLoss(&.{
                .logits = logits,
                .labels = labels,
                .mask = mask,
                .positive_weight = attrs.positive_weight,
                .negative_weight = attrs.negative_weight,
                .eps = attrs.eps,
                .mean_reduction = attrs.reduction == .mean,
                .output_shape = out_shape,
            }) catch |err| switch (err) {
                error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch, error.UnsupportedTensorType => null,
                else => return err,
            };
        },
        .fused_masked_bce_with_logits_backward => |attrs| blk: {
            const logits = valueFor(values, inputs[0]) orelse break :blk null;
            const labels = valueFor(values, inputs[1]) orelse break :blk null;
            const mask = valueFor(values, inputs[2]) orelse break :blk null;
            const upstream = valueFor(values, inputs[3]) orelse break :blk null;
            var logits_shape_buf: [ml.graph.shape.max_rank]i64 = undefined;
            const logits_shape = try fillShapeDims(graph.node(inputs[0]).output_shape, &logits_shape_buf);
            break :blk cb.maskedBceWithLogitsBackward(&.{
                .logits = logits,
                .labels = labels,
                .mask = mask,
                .upstream = upstream,
                .positive_weight = attrs.positive_weight,
                .negative_weight = attrs.negative_weight,
                .eps = attrs.eps,
                .mean_reduction = attrs.reduction == .mean,
                .logits_shape = logits_shape,
            }) catch |err| switch (err) {
                error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch, error.UnsupportedTensorType => null,
                else => return err,
            };
        },
        .fused_sdpa => |attrs| try executeRuntimeSdpa(cb, values, inputs, attrs, op_plan, exec_state),
        .fused_gqa_causal_attention => |attrs| try executeRuntimeGqaCausalAttention(cb, values, inputs, attrs, n.num_inputs, exec_state),
        .dot_general => |attrs| try executeRuntimeDotGeneralHinted(graph, cb, values, inputs, attrs, op_plan, output_hint),
        .conv_general => |attrs| try executeRuntimeConvGeneral(graph, cb, values, inputs, attrs),
        .fused_conv1d => |attrs| try executeRuntimeConv1d(graph, cb, values, inputs, attrs),
        .fused_conv2d => |attrs| try executeRuntimeConv2d(graph, cb, values, inputs, attrs),
        .fused_linear => |attrs| try executeRuntimeLinear(cb, values, inputs, attrs.rows, attrs.in_dim, attrs.out_dim, true, op_plan),
        .fused_linear_no_bias => |attrs| blk: {
            if (attrs.num_projections != 0) {
                break :blk try executeRuntimeLinearNoBiasGrouped(cb, values, inputs, attrs);
            }
            break :blk try executeRuntimeLinear(cb, values, inputs, attrs.rows, attrs.in_dim, attrs.out_dim, false, op_plan);
        },
        .fused_linear_no_bias_pair => |attrs| try executeRuntimeLinearNoBiasPair(cb, values, inputs, attrs, exec_state),
        .fused_to_float32 => blk: {
            if (exec_state.pair_second) |second| {
                exec_state.pair_second = null;
                break :blk second;
            }
            break :blk valueFor(values, inputs[0]);
        },
        .fused_embedding_lookup => |attrs| try executeRuntimeEmbeddingLookup(graph, cb, values, inputs, attrs, exec_state),
        .fused_take_rows => |attrs| try executeRuntimeTakeRows(cb, values, inputs, attrs.rows, attrs.dim, op_plan, exec_state),
        .fused_zero_tensor => |attrs| try executeRuntimeZeroTensor(cb, attrs.rows, attrs.out_dim),
        .fused_rope => |attrs| try executeRuntimeRope(cb, values, inputs, attrs, exec_state),
        .fused_layer_norm => |attrs| try executeRuntimeLayerNorm(cb, values, inputs, attrs.dim, attrs.eps, n.output_shape),
        .fused_layer_norm_backward => |attrs| blk: {
            const input = valueFor(values, inputs[0]) orelse break :blk null;
            const gamma = valueFor(values, inputs[1]) orelse break :blk null;
            const beta = valueFor(values, inputs[2]) orelse break :blk null;
            const dy = valueFor(values, inputs[3]) orelse break :blk null;
            break :blk cb.layerNormBackward(input, gamma, beta, dy, attrs.dim, attrs.eps) catch |err| switch (err) {
                error.UnsupportedOperation, error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch, error.UnsupportedTensorType => null,
                else => return err,
            };
        },
        .fused_rms_norm => |attrs| try executeRuntimeRmsNorm(cb, values, inputs, attrs.dim, attrs.eps, n.output_shape),
        else => null,
    };
}

fn executeRuntimeZeroTensor(
    cb: *const ComputeBackend,
    rows: usize,
    out_dim: usize,
) !?CT {
    const ct = (try cb.zeroTensor(rows, out_dim)) orelse return null;
    errdefer cb.free(ct);
    if (isMetalDeviceResident(cb, ct)) return ct;
    if (try makeMetalDeviceResident(cb, ct)) |device_ct| {
        if (device_ct != ct) cb.free(ct);
        return device_ct;
    }
    return ct;
}

fn executeRuntimeConstant(
    graph: *const Graph,
    cb: *const ComputeBackend,
    output_shape: ml.graph.Shape,
    attrs: anytype,
) !?CT {
    const constant = try graph.constantDataAsF32(
        graph.allocator,
        output_shape.dtype,
        attrs.data_offset,
        attrs.data_len,
    );
    defer constant.deinit(graph.allocator);

    var shape_buf: [ml.graph.shape.max_rank]i32 = undefined;
    const rank = output_shape.rank();
    const ct = if (rank > 1) blk: {
        for (0..rank) |axis| shape_buf[axis] = @intCast(output_shape.dim(@intCast(axis)));
        break :blk try cb.fromFloat32Shape(constant.data, shape_buf[0..rank]);
    } else try cb.fromFloat32(constant.data);
    errdefer cb.free(ct);

    if (isMetalDeviceResident(cb, ct)) return ct;
    if (try makeMetalDeviceResident(cb, ct)) |device_ct| {
        if (device_ct != ct) cb.free(ct);
        return device_ct;
    }
    return ct;
}

fn executeRuntimeGqaCausalAttention(
    cb: *const ComputeBackend,
    values: []?CT,
    inputs: []const NodeId,
    attrs: anytype,
    num_inputs: u8,
    exec_state: *interpreter.ExecState,
) !?CT {
    const q = valueFor(values, inputs[0]) orelse return null;
    const k = valueFor(values, inputs[1]) orelse return null;
    const v = valueFor(values, inputs[2]) orelse return null;
    const bias = valueFor(values, if (num_inputs > 3) inputs[3] else null_node);
    const kv_heads = if (attrs.num_kv_heads != 0) attrs.num_kv_heads else attrs.num_heads;

    if (exec_state.options.attention) |base_attn| {
        var attn = base_attn;
        attn.layer_index = if (attrs.layer_index == std.math.maxInt(u32))
            exec_state.attention_layer
        else
            attrs.layer_index;
        attn.skip_kv_write = attrs.skip_kv_write;
        const out = cb.gqaPagedAttention(
            q,
            k,
            v,
            bias,
            attn,
            attrs.batch,
            attrs.num_heads,
            kv_heads,
            attrs.head_dim,
        ) catch |err| switch (err) {
            error.UnsupportedOperation, error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch, error.UnsupportedTensorType, error.InvalidAttentionShape => null,
            else => return err,
        };
        if (out != null) exec_state.attention_layer += 1;
        return out;
    }

    return cb.gqaCausalAttention(
        q,
        k,
        v,
        bias,
        attrs.batch,
        attrs.seq_len,
        attrs.num_heads,
        kv_heads,
        attrs.head_dim,
    ) catch |err| switch (err) {
        error.UnsupportedOperation, error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch, error.UnsupportedTensorType, error.InvalidAttentionShape => null,
        else => return err,
    };
}

fn executeRuntimeSdpa(
    cb: *const ComputeBackend,
    values: []?CT,
    inputs: []const NodeId,
    attrs: anytype,
    op_plan: ?OperatorPlan,
    exec_state: *interpreter.ExecState,
) !?CT {
    const q = valueFor(values, inputs[0]) orelse return null;
    const k = valueFor(values, inputs[1]) orelse return null;
    const v = valueFor(values, inputs[2]) orelse return null;
    const kv_len = if (attrs.kv_seq_len != 0) attrs.kv_seq_len else attrs.seq_len;
    const attention_plan = try validatePlannedAttentionOp(attrs.seq_len, kv_len, attrs.head_dim, op_plan);
    const bias = valueFor(values, if (inputs.len > 3) inputs[3] else null_node);
    const kv_heads = if (attrs.num_kv_heads != 0) attrs.num_kv_heads else attrs.num_heads;

    if (attention_plan.operator == .attention_paged or attention_plan.operator == .attention_quantized_kv) {
        const attention = exec_state.options.attention orelse return null;
        return cb.gqaPagedAttention(
            q,
            k,
            v,
            bias,
            attention,
            attrs.batch,
            attrs.num_heads,
            kv_heads,
            attrs.head_dim,
        ) catch |err| switch (err) {
            error.UnsupportedOperation, error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch, error.UnsupportedTensorType, error.InvalidAttentionShape => null,
            else => return err,
        };
    }

    var synthesized_mask: ?[]i64 = null;
    defer if (synthesized_mask) |buf| std.heap.page_allocator.free(buf);
    const mask = blk: {
        if (exec_state.options.sdpa_mask) |runtime_mask| break :blk runtime_mask;
        if (attrs.batch == 0 or attrs.seq_len == 0) return error.MissingRuntimeInput;
        const full_mask = try std.heap.page_allocator.alloc(i64, @as(usize, attrs.batch) * @as(usize, attrs.seq_len));
        @memset(full_mask, 1);
        synthesized_mask = full_mask;
        break :blk full_mask;
    };

    return cb.scaledDotProductAttention(
        q,
        k,
        v,
        mask,
        bias,
        attrs.batch,
        attrs.seq_len,
        attrs.num_heads,
        attrs.head_dim,
    ) catch |err| switch (err) {
        error.UnsupportedOperation, error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => null,
        else => return err,
    };
}

const RuntimeFusedUnaryOp = enum {
    sigmoid,
    tanh_act,
};

fn executeRuntimeFusedUnary(
    cb: *const ComputeBackend,
    values: []?CT,
    inputs: []const NodeId,
    comptime op: RuntimeFusedUnaryOp,
) !?CT {
    const input = valueFor(values, inputs[0]) orelse return null;
    return switch (op) {
        .sigmoid => cb.sigmoid(input),
        .tanh_act => cb.tanh_act(input),
    } catch |err| switch (err) {
        error.UnsupportedOperation, error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => null,
        else => return err,
    };
}

fn executeRuntimeConcatPrim(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    inputs: []const NodeId,
    attrs: anytype,
) !?CT {
    const lhs = valueFor(values, inputs[0]) orelse return null;
    const rhs = valueFor(values, inputs[1]) orelse return null;
    var lhs_shape_buf: [ml.graph.shape.max_rank]i64 = undefined;
    var rhs_shape_buf: [ml.graph.shape.max_rank]i64 = undefined;
    const lhs_shape = try fillShapeDims(graph.node(inputs[0]).output_shape, &lhs_shape_buf);
    const rhs_shape = try fillShapeDims(graph.node(inputs[1]).output_shape, &rhs_shape_buf);
    return cb.primConcatPrim(lhs, rhs, attrs.axis, lhs_shape, rhs_shape) catch |err| switch (err) {
        error.UnsupportedOperation, error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => null,
        else => return err,
    };
}

fn executeRuntimeScatterAdd(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    node_id: NodeId,
    inputs: []const NodeId,
    attrs: anytype,
) !?CT {
    if (attrs.axis != 0) return null;
    if (inputs.len == 2) {
        const update_values = valueFor(values, inputs[0]) orelse return null;
        const indices = valueFor(values, inputs[1]) orelse return null;

        var values_shape_buf: [ml.graph.shape.max_rank]i64 = undefined;
        var out_shape_buf: [ml.graph.shape.max_rank]i64 = undefined;
        const values_shape = try fillShapeDims(graph.node(inputs[0]).output_shape, &values_shape_buf);
        const out_shape = try fillShapeDims(graph.node(node_id).output_shape, &out_shape_buf);

        return cb.primScatterAdd(update_values, indices, values_shape, out_shape, attrs.axis) catch |err| switch (err) {
            error.UnsupportedOperation,
            error.UnsupportedPrimitiveOp,
            error.UnsupportedTensorType,
            error.UnsupportedShape,
            error.ShapeMismatch,
            => null,
            else => return err,
        };
    }
    if (inputs.len != 3) return null;
    const dest = valueFor(values, inputs[0]) orelse return null;
    const update_values = valueFor(values, inputs[1]) orelse return null;
    const indices = valueFor(values, inputs[2]) orelse return null;

    var dest_shape_buf: [ml.graph.shape.max_rank]i64 = undefined;
    var values_shape_buf: [ml.graph.shape.max_rank]i64 = undefined;
    var indices_shape_buf: [ml.graph.shape.max_rank]i64 = undefined;
    const dest_shape = try fillShapeDims(graph.node(inputs[0]).output_shape, &dest_shape_buf);
    const values_shape = try fillShapeDims(graph.node(inputs[1]).output_shape, &values_shape_buf);
    const indices_shape = try fillShapeDims(graph.node(inputs[2]).output_shape, &indices_shape_buf);

    if (dest_shape.len != 2 or values_shape.len != 2 or indices_shape.len != 1) return null;
    if (dest_shape[0] <= 0 or dest_shape[1] <= 0 or values_shape[0] < 0 or values_shape[1] != dest_shape[1]) return null;

    const out_rows: usize = @intCast(dest_shape[0]);
    const value_rows: usize = @intCast(values_shape[0]);
    const dim: usize = @intCast(dest_shape[1]);

    const allocator = std.heap.page_allocator;
    const dest_data = try cb.toFloat32(dest, allocator);
    defer allocator.free(dest_data);
    const values_data = try cb.toFloat32(update_values, allocator);
    defer allocator.free(values_data);
    const index_data = try cb.toFloat32(indices, allocator);
    defer allocator.free(index_data);

    if (dest_data.len != out_rows * dim or values_data.len != value_rows * dim or index_data.len < value_rows) return error.ShapeMismatch;

    const output = try allocator.dupe(f32, dest_data);
    defer allocator.free(output);
    for (0..value_rows) |row_idx| {
        const out_row_f = @round(index_data[row_idx]);
        if (out_row_f < 0) return error.IndexOutOfBounds;
        const out_row: usize = @intFromFloat(out_row_f);
        if (out_row >= out_rows) return error.IndexOutOfBounds;
        const src = values_data[row_idx * dim ..][0..dim];
        const dst = output[out_row * dim ..][0..dim];
        for (src, dst) |v, *d| d.* += v;
    }

    const out_shape = [_]i32{ @intCast(out_rows), @intCast(dim) };
    const host_result = try cb.fromFloat32Shape(output, &out_shape);
    errdefer cb.free(host_result);
    if (isMetalDeviceResident(cb, host_result)) return host_result;
    if (try makeMetalDeviceResident(cb, host_result)) |device_result| {
        if (device_result != host_result) cb.free(host_result);
        return device_result;
    }
    return host_result;
}

fn executeRuntimeConvertDType(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    inputs: []const NodeId,
    attrs: anytype,
) !?CT {
    if (inputs.len != 1) return null;
    const input = valueFor(values, inputs[0]) orelse return null;
    if (graph.node(inputs[0]).output_shape.dtype == attrs.target) return input;
    return cb.tryConvertDType(input, attrs.target) catch |err| switch (err) {
        error.UnsupportedOperation,
        error.UnsupportedPrimitiveOp,
        error.UnsupportedTensorType,
        error.UnsupportedShape,
        error.ShapeMismatch,
        => null,
        else => return err,
    };
}

fn executeRuntimeEmbeddingLookup(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    inputs: []const NodeId,
    attrs: anytype,
    exec_state: *interpreter.ExecState,
) !?CT {
    const weight = valueFor(values, inputs[0]) orelse return null;
    var owned_ids: ?[]i64 = null;
    defer if (owned_ids) |buf| std.heap.page_allocator.free(buf);
    const ids = blk: {
        if (graph.node(inputs[1]).op == .fused_from_float32) {
            break :blk exec_state.options.embedding_ids orelse return error.MissingRuntimeInput;
        }
        const ids_ct = valueFor(values, inputs[1]) orelse return null;
        const raw = try cb.toFloat32(ids_ct, std.heap.page_allocator);
        defer std.heap.page_allocator.free(raw);
        const converted = try std.heap.page_allocator.alloc(i64, raw.len);
        for (converted, raw) |*dst, value| dst.* = @intFromFloat(@round(value));
        owned_ids = converted;
        break :blk converted;
    };
    return cb.embeddingLookup(weight, ids, attrs.total, attrs.dim) catch |err| switch (err) {
        error.UnsupportedOperation, error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => null,
        else => return err,
    };
}

fn executeRuntimeBroadcast(
    cb: *const ComputeBackend,
    values: []?CT,
    inputs: []const NodeId,
    input_shape: ml.graph.Shape,
    attrs: anytype,
    output_hint: ?CT,
) !?CT {
    const input = valueFor(values, inputs[0]) orelse return null;
    var in_shape_buf: [ml.graph.shape.max_rank]i64 = undefined;
    const in_rank = input_shape.rank();
    if (in_rank > in_shape_buf.len) return error.UnsupportedShape;
    for (0..in_rank) |axis| in_shape_buf[axis] = input_shape.dim(@intCast(axis));

    var target_shape_buf: [ml.graph.shape.max_rank]i64 = undefined;
    const target_rank = attrs.target_shape.rank();
    if (target_rank > target_shape_buf.len) return error.UnsupportedShape;
    for (0..target_rank) |axis| target_shape_buf[axis] = attrs.target_shape.dim(@intCast(axis));

    // Slot-bound output: identity-axis last-dim broadcast directly into the pooled buffer.
    if (output_hint) |hint| {
        if (isMetalDeviceResident(cb, input) and in_rank == target_rank) {
            var identity = true;
            for (attrs.broadcast_axes[0..attrs.num_axes], 0..) |axis, i| {
                if (axis != i) identity = false;
            }
            if (identity and attrs.num_axes == in_rank) {
                if (try metal_compute_mod.MetalCompute.broadcastLastDimInto(cb, input, in_shape_buf[0..in_rank], target_shape_buf[0..target_rank], hint)) |out| return out;
            }
        }
    }

    return cb.primBroadcastInDim(
        input,
        target_shape_buf[0..target_rank],
        attrs.broadcast_axes[0..attrs.num_axes],
        in_shape_buf[0..in_rank],
    ) catch |err| switch (err) {
        error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => null,
        else => return err,
    };
}

const RuntimeReduceOp = enum {
    sum,
    max,
    mean,
};

fn executeRuntimeReduce(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    inputs: []const NodeId,
    attrs: anytype,
    comptime op: RuntimeReduceOp,
    output_hint: ?CT,
    stats: ?*PartitionExecutor.ExecutionStats,
    resident_input_cache: ?*ResidentInputCache,
) !?CT {
    const input = valueFor(values, inputs[0]) orelse return null;
    var in_shape_buf: [ml.graph.shape.max_rank]i64 = undefined;
    const in_shape = try fillShapeDims(graph.node(inputs[0]).output_shape, &in_shape_buf);
    const axes = attrs.axes[0..attrs.num_axes];
    var cached_input: ?CT = null;
    if (resident_input_cache) |cache| {
        cached_input = try cache.residentValueForConsumer(cb, graph, inputs[0], input, stats);
    }
    const promote_input = cached_input == null and cb.kind() == .metal and reducePromoteInputEnabled() and !isMetalDeviceResident(cb, input);
    const promotion_start_ns = if (promote_input) metalPartitionNowNs() else 0;
    const input_resident = if (cached_input) |cached|
        TemporaryMetalResidentValue{ .value = cached }
    else if (promote_input)
        try temporaryMetalResidentValue(cb, input)
    else
        TemporaryMetalResidentValue{ .value = input };
    defer input_resident.deinit(cb);
    if (promote_input and isMetalDeviceResident(cb, input_resident.value)) {
        if (stats) |s| {
            s.metal_reduce_input_promotions += 1;
            s.metal_reduce_input_promotion_ns += metalPartitionNowNs() - promotion_start_ns;
            if (tensorElementCount(graph.node(inputs[0]).output_shape)) |elems| {
                s.metal_reduce_input_promotion_bytes += @intCast(elems * @sizeOf(f32));
            }
        }
    }
    // Slot-bound output: last-axis reduce directly into the pooled buffer.
    if (output_hint) |hint| {
        if (isMetalDeviceResident(cb, input_resident.value)) {
            const kind_id: u32 = switch (op) {
                .sum => 0,
                .max => 1,
                .mean => 2,
            };
            if (try metal_compute_mod.MetalCompute.reduceLastDimInto(cb, input_resident.value, axes, in_shape, kind_id, hint)) |out| return out;
        }
    }
    return switch (op) {
        .sum => cb.primReduceSum(input_resident.value, axes, in_shape),
        .max => cb.primReduceMax(input_resident.value, axes, in_shape),
        .mean => cb.primReduceMean(input_resident.value, axes, in_shape),
    } catch |err| switch (err) {
        error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => null,
        else => return err,
    };
}

const RuntimeBinaryOp = enum {
    multiply,
    subtract,
    divide,
    less_than,
};

fn executeRuntimeBinary(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    node_id: NodeId,
    inputs: []const NodeId,
    output_shape: ml.graph.Shape,
    op: RuntimeBinaryOp,
    output_hint: ?CT,
) !?CT {
    const lhs_operand = runtimeBinaryOperand(graph, values, inputs[0], output_shape) orelse {
        if (traceRuntimeCommandFallbacksEnabled()) traceRuntimeBinaryFallback(graph, node_id, op, "missing_lhs", null, null, output_shape);
        return null;
    };
    const rhs_operand = runtimeBinaryOperand(graph, values, inputs[1], output_shape) orelse {
        if (traceRuntimeCommandFallbacksEnabled()) traceRuntimeBinaryFallback(graph, node_id, op, "missing_rhs", lhs_operand, null, output_shape);
        return null;
    };
    if (!binaryBroadcastResultMatches(lhs_operand.shape, rhs_operand.shape, output_shape)) {
        if (traceRuntimeCommandFallbacksEnabled()) traceRuntimeBinaryFallback(graph, node_id, op, "broadcast_mismatch", lhs_operand, rhs_operand, output_shape);
        return null;
    }
    var lhs = lhs_operand.value;
    var rhs = rhs_operand.value;
    var owned_lhs: ?CT = null;
    defer if (owned_lhs) |ct| cb.free(ct);
    var owned_rhs: ?CT = null;
    defer if (owned_rhs) |ct| cb.free(ct);
    if (cb.kind() == .metal) {
        if (!isMetalDeviceResident(cb, lhs)) {
            if (try makeMetalDeviceResident(cb, lhs)) |device_lhs| {
                if (device_lhs != lhs) owned_lhs = device_lhs;
                lhs = device_lhs;
            }
        }
        if (!isMetalDeviceResident(cb, rhs)) {
            if (try makeMetalDeviceResident(cb, rhs)) |device_rhs| {
                if (device_rhs != rhs) owned_rhs = device_rhs;
                rhs = device_rhs;
            }
        }
        // Slot-bound output: write a*b directly into the pooled buffer.
        // force_barrier guards reused pooled buffers against non-declaring
        // consumer reads (conservative: barrier on every pooled write).
        if (output_hint) |hint| {
            if (sameElementCount(lhs_operand.shape, rhs_operand.shape)) {
                switch (op) {
                    .multiply => if (try metal_compute_mod.MetalCompute.multiplyInto(cb, lhs, rhs, hint, false)) |out| return out,
                    .subtract => if (try metal_compute_mod.MetalCompute.subtractInto(cb, lhs, rhs, hint)) |out| return out,
                    .divide => if (try metal_compute_mod.MetalCompute.divideInto(cb, lhs, rhs, hint)) |out| return out,
                    .less_than => {},
                }
            }
        }
    }
    return switch (op) {
        .multiply => cb.multiply(lhs, rhs),
        .subtract => cb.primSubtract(lhs, rhs),
        .divide => cb.primDivide(lhs, rhs),
        .less_than => cb.primLessThan(lhs, rhs),
    } catch |err| switch (err) {
        error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => {
            if (traceRuntimeCommandFallbacksEnabled()) traceRuntimeBinaryFallback(graph, node_id, op, @errorName(err), lhs_operand, rhs_operand, output_shape);
            return null;
        },
        else => return err,
    };
}

fn executeRuntimeAddMulScalar(
    cb: *const ComputeBackend,
    values: []?CT,
    inputs: []const NodeId,
) !?CT {
    const lhs = valueFor(values, inputs[0]) orelse return null;
    const rhs = valueFor(values, inputs[1]) orelse return null;
    const scalar = valueFor(values, inputs[2]) orelse return null;
    if (try cb.addMultiplyScalarTensor(lhs, rhs, scalar)) |fused| return fused;
    const sum = try cb.add(lhs, rhs);
    errdefer cb.free(sum);
    const scaled = try cb.multiply(sum, scalar);
    cb.free(sum);
    return scaled;
}

fn executeRuntimeWhereSelect(
    cb: *const ComputeBackend,
    values: []?CT,
    inputs: []const NodeId,
) !?CT {
    const cond = valueFor(values, inputs[0]) orelse return null;
    const on_true = valueFor(values, inputs[1]) orelse return null;
    const on_false = valueFor(values, inputs[2]) orelse return null;
    return cb.primWhereSelect(cond, on_true, on_false) catch |err| switch (err) {
        error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => null,
        else => return err,
    };
}

fn executeRuntimeSlice(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    inputs: []const NodeId,
    attrs: anytype,
) !?CT {
    const input = valueFor(values, inputs[0]) orelse return null;
    var in_shape_buf: [ml.graph.shape.max_rank]i64 = undefined;
    const in_shape = try fillShapeDims(graph.node(inputs[0]).output_shape, &in_shape_buf);
    const rank = @as(usize, attrs.num_axes);
    if (rank > ml.graph.shape.max_rank) return error.UnsupportedShape;
    var starts: [ml.graph.shape.max_rank]i64 = undefined;
    var limits: [ml.graph.shape.max_rank]i64 = undefined;
    var strides: [ml.graph.shape.max_rank]i64 = undefined;
    for (0..rank) |axis| {
        starts[axis] = attrs.starts[axis];
        limits[axis] = attrs.limits[axis];
        strides[axis] = attrs.strides[axis];
    }
    if (in_shape.len == 2 and rank == 2 and
        starts[0] == 0 and limits[0] == in_shape[0] and
        strides[0] == 1 and strides[1] == 1)
    {
        return cb.sliceLastDim(input, @intCast(starts[1]), @intCast(limits[1])) catch |err| switch (err) {
            error.UnsupportedOperation, error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => null,
            else => return err,
        };
    }
    return cb.primSlice(input, starts[0..rank], limits[0..rank], strides[0..rank], in_shape) catch |err| switch (err) {
        error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => null,
        else => return err,
    };
}

fn executeRuntimeLinear(
    cb: *const ComputeBackend,
    values: []?CT,
    inputs: []const NodeId,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    comptime has_bias: bool,
    op_plan: ?OperatorPlan,
) !?CT {
    const input = valueFor(values, inputs[0]) orelse return null;
    const weight = valueFor(values, inputs[1]) orelse return null;
    try validatePlannedLinearOp(cb, weight, rows, in_dim, out_dim, op_plan);
    if (has_bias) {
        const bias = valueFor(values, inputs[2]) orelse return null;
        const output = cb.linearWithPlan(input, weight, bias, rows, in_dim, out_dim, op_plan) catch |err| switch (err) {
            error.UnsupportedOperation, error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => null,
            else => return err,
        };
        return try promoteMetalOutputIfNeeded(cb, output);
    } else {
        const output = cb.linearNoBiasWithPlan(input, weight, rows, in_dim, out_dim, op_plan) catch |err| switch (err) {
            error.UnsupportedOperation, error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => null,
            else => return err,
        };
        return try promoteMetalOutputIfNeeded(cb, output);
    }
}

fn executeRuntimeLinearNoBiasGrouped(
    cb: *const ComputeBackend,
    values: []?CT,
    inputs: []const NodeId,
    attrs: anytype,
) !?CT {
    const input = valueFor(values, inputs[0]) orelse return null;
    const weight = valueFor(values, inputs[1]) orelse return null;
    return cb.linearNoBiasGrouped(
        input,
        weight,
        attrs.rows,
        attrs.in_dim,
        attrs.out_dim,
        attrs.projection_out_dims[0..attrs.num_projections],
        attrs.num_projections,
    ) catch |err| switch (err) {
        error.UnsupportedOperation, error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => null,
        else => return err,
    };
}

/// Command-path wrapper: try the slot-bound 2D matmul-into for the simple
/// (single-contracting, no-batch, 2D, device-resident) case before falling
/// back to the general dot_general path. Region executors call
/// `executeRuntimeDotGeneral` directly (no hint).
fn executeRuntimeDotGeneralHinted(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    inputs: []const NodeId,
    attrs: anytype,
    op_plan: ?OperatorPlan,
    output_hint: ?CT,
) !?CT {
    if (output_hint) |hint| try_hint: {
        if (attrs.num_contracting != 1 or attrs.num_batch != 0) break :try_hint;
        const lhs = valueFor(values, inputs[0]) orelse break :try_hint;
        const rhs = valueFor(values, inputs[1]) orelse break :try_hint;
        if (!isMetalDeviceResident(cb, lhs) or !isMetalDeviceResident(cb, rhs)) break :try_hint;
        const lhs_shape = graph.node(inputs[0]).output_shape;
        const rhs_shape = graph.node(inputs[1]).output_shape;
        if (lhs_shape.rank() != 2 or rhs_shape.rank() != 2) break :try_hint;
        const lc = attrs.lhs_contracting[0];
        const rc = attrs.rhs_contracting[0];
        if (lc != 1 or rc > 1) break :try_hint;
        const m = positiveI64ToUsize(lhs_shape.dim(0)) orelse break :try_hint;
        const k = positiveI64ToUsize(lhs_shape.dim(1)) orelse break :try_hint;
        const rhs_k = positiveI64ToUsize(rhs_shape.dim(@intCast(rc))) orelse break :try_hint;
        const n = positiveI64ToUsize(rhs_shape.dim(@intCast(1 - @as(usize, rc)))) orelse break :try_hint;
        if (k != rhs_k) break :try_hint;
        if (try metal_compute_mod.MetalCompute.dotGeneral2DInto(cb, lhs, rhs, m, n, k, rc, hint)) |out| return out;
    }
    return executeRuntimeDotGeneral(graph, cb, values, inputs, attrs, op_plan);
}

fn executeRuntimeDotGeneral(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    inputs: []const NodeId,
    attrs: anytype,
    op_plan: ?OperatorPlan,
) !?CT {
    const lhs = valueFor(values, inputs[0]) orelse return null;
    var lhs_shape_buf: [ml.graph.shape.max_rank]i64 = undefined;
    var rhs_shape_buf: [ml.graph.shape.max_rank]i64 = undefined;
    const lhs_shape = try fillShapeDims(graph.node(inputs[0]).output_shape, &lhs_shape_buf);
    const rhs_shape = try fillShapeDims(graph.node(inputs[1]).output_shape, &rhs_shape_buf);
    const lhs_contracting = attrs.lhs_contracting[0..attrs.num_contracting];
    const rhs_contracting = attrs.rhs_contracting[0..attrs.num_contracting];
    const lhs_batch = attrs.lhs_batch[0..attrs.num_batch];
    const rhs_batch = attrs.rhs_batch[0..attrs.num_batch];
    if (try executeRuntimeLinearDotFromDeferredTranspose(graph, cb, values, inputs, lhs_shape, rhs_shape, lhs_contracting, rhs_contracting, lhs_batch, rhs_batch, op_plan)) |linear_output| {
        return linear_output;
    }
    const rhs = valueFor(values, inputs[1]) orelse return null;
    if (op_plan != null and attrs.num_contracting == 1 and attrs.num_batch == 0 and lhs_shape.len == 2 and rhs_shape.len == 2 and lhs_contracting[0] == 1 and rhs_contracting[0] == 1) {
        const rows = positiveI64ToUsize(lhs_shape[0]) orelse return null;
        const in_dim = positiveI64ToUsize(lhs_shape[1]) orelse return null;
        const out_dim = positiveI64ToUsize(rhs_shape[0]) orelse return null;
        if (rhs_shape[1] == lhs_shape[1]) {
            return executeRuntimeLinear(cb, values, inputs, rows, in_dim, out_dim, false, op_plan);
        }
    }
    const output = cb.primDotGeneral(lhs, rhs, lhs_shape, rhs_shape, lhs_contracting, rhs_contracting, lhs_batch, rhs_batch) catch |err| switch (err) {
        error.UnsupportedOperation, error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => null,
        else => return err,
    };
    return try promoteMetalOutputIfNeeded(cb, output);
}

fn executeRuntimeLinearDotFromDeferredTranspose(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    inputs: []const NodeId,
    lhs_shape: []const i64,
    rhs_shape: []const i64,
    lhs_contracting: []const u8,
    rhs_contracting: []const u8,
    lhs_batch: []const u8,
    rhs_batch: []const u8,
    op_plan: ?OperatorPlan,
) !?CT {
    if (inputs.len < 2) return null;
    if (lhs_batch.len != 0 or rhs_batch.len != 0) return null;
    if (lhs_contracting.len != 1 or rhs_contracting.len != 1) return null;
    if (lhs_shape.len != 2 or rhs_shape.len != 2) return null;
    if (lhs_contracting[0] != 1 or rhs_contracting[0] != 0) return null;

    const rhs_id = inputs[1];
    if (rhs_id == null_node) return null;
    const rhs_node = graph.node(rhs_id);
    const transpose_attrs = switch (rhs_node.op) {
        .transpose => |attrs| attrs,
        else => return null,
    };
    if (rhs_node.num_inputs == 0 or rhs_node.inputs[0] == null_node) return null;
    if (!transposeIsSimple2D(transpose_attrs, graph.node(rhs_node.inputs[0]).output_shape)) return null;

    const source_weight_id = rhs_node.inputs[0];
    const input = valueFor(values, inputs[0]) orelse return null;
    const weight = valueFor(values, source_weight_id) orelse return null;
    const rows = positiveI64ToUsize(lhs_shape[0]) orelse return null;
    const in_dim = positiveI64ToUsize(lhs_shape[1]) orelse return null;
    const out_dim = positiveI64ToUsize(rhs_shape[1]) orelse return null;
    const weight_shape = graph.node(source_weight_id).output_shape;
    const weight_out_dim = shapeDimUsize(weight_shape, 0) orelse return null;
    const weight_in_dim = shapeDimUsize(weight_shape, 1) orelse return null;
    if (weight_out_dim != out_dim or weight_in_dim != in_dim) return null;
    if (in_dim == 1 and rank1DotSpecializationEnabled()) {
        var weight_shape_buf: [ml.graph.shape.max_rank]i64 = undefined;
        const source_weight_shape = try fillShapeDims(weight_shape, &weight_shape_buf);
        const output = cb.primDotGeneral(input, weight, lhs_shape, source_weight_shape, lhs_contracting, &.{1}, lhs_batch, rhs_batch) catch |err| switch (err) {
            error.UnsupportedOperation, error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => null,
            else => return err,
        };
        return try promoteMetalOutputIfNeeded(cb, output);
    }
    // Unplanned dense weights: execute as `dot_general(input, weight)` with
    // the weight's second axis contracted (X @ Wᵀ) instead of routing through
    // `linearNoBias`. This is numerically identical to the interpreter's
    // `dot_general(input, transpose(W))` device command (same reduce kernel,
    // same accumulation order, device byte offsets forwarded for sub-buffer
    // view operands) whereas the linear route goes through dynamic linear
    // slot caches / MPS / host-mirror sub-paths that the regular interpreter
    // never exercises for raw dot_general nodes. The GLiNER2 training graph
    // executor diverged on exactly this configuration (rel-position
    // projection, gather output [127,768] x transposed square [768,768]
    // weight) while the interpreter was bit-exact; keeping the shortcut on
    // the interpreter-equivalent command preserves parity by construction.
    // Planned (quantized) dispatches keep the linear route below (their
    // packed weight descriptors are only consumable by the planned kernels),
    // as do host-resident inputs (the host sgemm linear route is exact and
    // avoids demoting the whole product to the naive host dot fallback).
    if (op_plan == null and isMetalDeviceResident(cb, input) and !weightRequiresPlannedLinear(cb, weight)) {
        var weight_shape_buf: [ml.graph.shape.max_rank]i64 = undefined;
        const source_weight_shape = try fillShapeDims(weight_shape, &weight_shape_buf);
        const dot_output = cb.primDotGeneral(input, weight, lhs_shape, source_weight_shape, lhs_contracting, &.{1}, lhs_batch, rhs_batch) catch |err| switch (err) {
            error.UnsupportedOperation, error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch, error.UnsupportedTensorType => null,
            else => return err,
        };
        if (dot_output != null) return try promoteMetalOutputIfNeeded(cb, dot_output);
    }
    const output = cb.linearNoBiasWithPlan(input, weight, rows, in_dim, out_dim, op_plan) catch |err| switch (err) {
        error.UnsupportedOperation, error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => null,
        else => return err,
    };
    return try promoteMetalOutputIfNeeded(cb, output);
}

/// Quantized weight descriptors must stay on the planned `linearNoBias`
/// route — `primDotGeneral` either rejects them or falls back to a slow host
/// dequantization. Dense (f32 host or device-resident) weights are safe on
/// the dot_general command path.
fn weightRequiresPlannedLinear(cb: *const ComputeBackend, weight: CT) bool {
    if (cb.kind() != .metal) return false;
    if (comptime !build_options.enable_metal) return false;
    return metal_compute_mod.MetalCompute.getQuantizedStorage(cb, weight) != null;
}

fn shouldDeferTransposeForLinearDot(
    graph: *const Graph,
    node_id: NodeId,
    reachable: []const bool,
    last_use: []const u32,
) bool {
    _ = last_use;
    const node_index: usize = @intCast(node_id);
    if (node_index >= reachable.len or !reachable[node_index]) return false;
    const node = graph.node(node_id);
    const attrs = switch (node.op) {
        .transpose => |transpose_attrs| transpose_attrs,
        else => return false,
    };
    if (node.num_inputs == 0 or node.inputs[0] == null_node) return false;
    if (!transposeIsSimple2D(attrs, graph.node(node.inputs[0]).output_shape)) return false;
    var compatible_consumers: usize = 0;
    var consumer_id: NodeId = 0;
    while (consumer_id < graph.nodeCount()) : (consumer_id += 1) {
        const consumer_index: usize = @intCast(consumer_id);
        if (consumer_index >= reachable.len or !reachable[consumer_index]) continue;
        const consumer = graph.node(consumer_id);
        var consumes_transpose = false;
        for (consumer.getInputs()) |input_id| {
            if (input_id == node_id) {
                consumes_transpose = true;
                break;
            }
        }
        if (!consumes_transpose) continue;
        if (!linearDotConsumesTranspose(graph, consumer_id, node_id)) return false;
        compatible_consumers += 1;
    }
    return compatible_consumers > 0;
}

fn transposeSourceIsWeightParameter(graph: *const Graph, node_id: NodeId) bool {
    const source_id = sourceFromSimpleTranspose(graph, node_id) orelse return false;
    const source = graph.node(source_id);
    if (std.meta.activeTag(source.op) != .parameter) return false;
    const name = graph.parameterName(source);
    return std.mem.indexOf(u8, name, "weight") != null;
}

fn linearDotConsumesTranspose(graph: *const Graph, consumer_id: NodeId, transpose_id: NodeId) bool {
    if (consumer_id >= graph.nodeCount()) return false;
    const consumer = graph.node(consumer_id);
    const dot_attrs = switch (consumer.op) {
        .dot_general => |dot_general_attrs| dot_general_attrs,
        else => return false,
    };
    if (consumer.num_inputs < 2 or consumer.inputs[1] != transpose_id) return false;
    if (dot_attrs.num_contracting != 1 or dot_attrs.num_batch != 0) return false;
    if (dot_attrs.lhs_contracting[0] != 1 or dot_attrs.rhs_contracting[0] != 0) return false;
    const transpose = graph.node(transpose_id);
    if (transpose.num_inputs == 0 or transpose.inputs[0] == null_node) return false;
    const lhs_shape = graph.node(consumer.inputs[0]).output_shape;
    if (lhs_shape.rank() != 2 or consumer.output_shape.rank() != 2) return false;
    const weight_shape = graph.node(transpose.inputs[0]).output_shape;
    const in_dim = shapeDimUsize(lhs_shape, 1) orelse return false;
    const out_dim = shapeDimUsize(weight_shape, 0) orelse return false;
    const weight_in_dim = shapeDimUsize(weight_shape, 1) orelse return false;
    const output_out_dim = shapeDimUsize(consumer.output_shape, 1) orelse return false;
    if (weight_in_dim != in_dim or output_out_dim != out_dim) return false;
    return true;
}

fn transposeIsSimple2D(attrs: ml.graph.node.TransposeAttrs, input_shape: Shape) bool {
    if (input_shape.rank() != 2) return false;
    var perm_buf: [ml.graph.shape.max_rank]u8 = undefined;
    const perm = transpose_utils.effectivePerm(attrs, input_shape.rank(), &perm_buf);
    return perm.len == 2 and perm[0] == 1 and perm[1] == 0;
}

fn recordQuantKernelCompilerPlan(
    stats: *PartitionExecutor.ExecutionStats,
    op: ml.graph.OpCode,
    op_plan: OperatorPlan,
) void {
    const plan = switch (op_plan) {
        .quant_matmul => |quant| quant,
        else => return,
    };
    const epilogue = quantKernelEpilogueForMetalOp(op);
    const counters = quant_kernel_compiler.plannedCountersFor(.metal, plan.format, plan.row_bucket, epilogue, plan.dispatch);
    quant_kernel_compiler.addCountersToStats(stats, counters);
}

fn quantKernelEpilogueForMetalOp(op: ml.graph.OpCode) quant_kernel_compiler.Epilogue {
    return switch (op) {
        // Metal runs quant matmul first and applies bias as a separate op.
        .fused_linear => .none,
        .fused_linear_no_bias_pair => .pair,
        else => .none,
    };
}

test "metal partition executor records pair epilogue in quant kernel compiler stats" {
    const quant_matmul = @import("quant_matmul.zig");
    const plan = quant_matmul.plan(.{
        .rows = 4,
        .in_dim = 256,
        .out_dim = 8,
        .format = .q4_k,
    });
    const op_plan: OperatorPlan = .{ .quant_matmul = plan };

    var pair_stats: PartitionExecutor.ExecutionStats = .{};
    recordQuantKernelCompilerPlan(&pair_stats, .{ .fused_linear_no_bias_pair = .{ .rows = 4, .in_dim = 256, .out_dim = 8 } }, op_plan);
    try std.testing.expectEqual(@as(u64, 1), pair_stats.quant_kernel_planned_ops);
    try std.testing.expectEqual(@as(u64, 1), pair_stats.quant_kernel_handwritten_production);
    try std.testing.expectEqual(@as(u64, 0), pair_stats.quant_kernel_generated_candidates);
    try std.testing.expectEqual(@as(u64, 0), pair_stats.quant_kernel_fallback_generated_artifact_missing);
    try std.testing.expectEqual(@as(u64, 0), pair_stats.quant_kernel_fallback_unsupported_epilogue);

    var no_bias_stats: PartitionExecutor.ExecutionStats = .{};
    recordQuantKernelCompilerPlan(&no_bias_stats, .{ .fused_linear_no_bias = .{ .rows = 4, .in_dim = 256, .out_dim = 8 } }, op_plan);
    try std.testing.expectEqual(@as(u64, 1), no_bias_stats.quant_kernel_planned_ops);
    try std.testing.expectEqual(@as(u64, 0), no_bias_stats.quant_kernel_handwritten_production);
    try std.testing.expectEqual(@as(u64, 1), no_bias_stats.quant_kernel_generated_production);
    try std.testing.expectEqual(@as(u64, 0), no_bias_stats.quant_kernel_generated_candidates);
    try std.testing.expectEqual(@as(u64, 0), no_bias_stats.quant_kernel_fallback_generated_artifact_missing);

    var bias_stats: PartitionExecutor.ExecutionStats = .{};
    recordQuantKernelCompilerPlan(&bias_stats, .{ .fused_linear = .{ .rows = 4, .in_dim = 256, .out_dim = 8 } }, op_plan);
    try std.testing.expectEqual(@as(u64, 1), bias_stats.quant_kernel_planned_ops);
    try std.testing.expectEqual(@as(u64, 0), bias_stats.quant_kernel_handwritten_production);
    try std.testing.expectEqual(@as(u64, 1), bias_stats.quant_kernel_generated_production);
    try std.testing.expectEqual(@as(u64, 0), bias_stats.quant_kernel_generated_candidates);
}

fn executeRuntimeConv1d(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    inputs: []const NodeId,
    attrs: anytype,
) !?CT {
    const input = valueFor(values, inputs[0]) orelse return null;
    const weight = valueFor(values, inputs[1]) orelse return null;
    const bias = valueFor(values, inputs[2]) orelse return null;
    const input_shape = graph.node(inputs[0]).output_shape;
    return cb.conv1d(
        input,
        weight,
        bias,
        shapeDimUsize(input_shape, 0) orelse return null,
        shapeDimUsize(input_shape, 1) orelse return null,
        attrs.out_channels,
        shapeDimUsize(input_shape, 2) orelse return null,
        attrs.kernel_size,
        attrs.stride,
        attrs.padding,
    ) catch |err| switch (err) {
        error.UnsupportedOperation, error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => null,
        else => return err,
    };
}

fn executeRuntimeConv2d(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    inputs: []const NodeId,
    attrs: anytype,
) !?CT {
    const input = valueFor(values, inputs[0]) orelse return null;
    const weight = valueFor(values, inputs[1]) orelse return null;
    const bias = valueFor(values, inputs[2]) orelse return null;
    const input_shape = graph.node(inputs[0]).output_shape;
    return cb.conv2d(
        input,
        weight,
        bias,
        shapeDimUsize(input_shape, 0) orelse return null,
        shapeDimUsize(input_shape, 1) orelse return null,
        attrs.out_channels,
        shapeDimUsize(input_shape, 2) orelse return null,
        shapeDimUsize(input_shape, 3) orelse return null,
        attrs.kernel_h,
        attrs.kernel_w,
        attrs.stride_h,
        attrs.stride_w,
        attrs.padding_h,
        attrs.padding_w,
        attrs.groups,
    ) catch |err| switch (err) {
        error.UnsupportedOperation, error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => null,
        else => return err,
    };
}

fn executeRuntimeConvGeneral(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    inputs: []const NodeId,
    attrs: anytype,
) !?CT {
    const input = valueFor(values, inputs[0]) orelse return null;
    const weight = valueFor(values, inputs[1]) orelse return null;
    const input_shape = graph.node(inputs[0]).output_shape;
    const weight_shape = graph.node(inputs[1]).output_shape;
    if (attrs.num_spatial == 1 and attrs.groups == 1 and input_shape.rank() == 3 and weight_shape.rank() == 3 and attrs.padding[0][0] == attrs.padding[0][1]) {
        const out_channels = shapeDimUsize(weight_shape, 0) orelse return null;
        const bias_data = try std.heap.page_allocator.alloc(f32, out_channels);
        defer std.heap.page_allocator.free(bias_data);
        @memset(bias_data, 0.0);
        const bias = try cb.fromFloat32(bias_data);
        defer cb.free(bias);
        return cb.conv1d(
            input,
            weight,
            bias,
            shapeDimUsize(input_shape, 0) orelse return null,
            shapeDimUsize(input_shape, 1) orelse return null,
            out_channels,
            shapeDimUsize(input_shape, 2) orelse return null,
            shapeDimUsize(weight_shape, 2) orelse return null,
            std.math.cast(usize, attrs.strides[0]) orelse return null,
            std.math.cast(usize, attrs.padding[0][0]) orelse return null,
        ) catch |err| switch (err) {
            error.UnsupportedOperation, error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => null,
            else => return err,
        };
    }
    if (attrs.num_spatial == 2 and input_shape.rank() == 4 and weight_shape.rank() == 4 and attrs.padding[0][0] == attrs.padding[0][1] and attrs.padding[1][0] == attrs.padding[1][1]) {
        const out_channels = shapeDimUsize(weight_shape, 0) orelse return null;
        const bias_data = try std.heap.page_allocator.alloc(f32, out_channels);
        defer std.heap.page_allocator.free(bias_data);
        @memset(bias_data, 0.0);
        const bias = try cb.fromFloat32(bias_data);
        defer cb.free(bias);
        return cb.conv2d(
            input,
            weight,
            bias,
            shapeDimUsize(input_shape, 0) orelse return null,
            shapeDimUsize(input_shape, 1) orelse return null,
            out_channels,
            shapeDimUsize(input_shape, 2) orelse return null,
            shapeDimUsize(input_shape, 3) orelse return null,
            shapeDimUsize(weight_shape, 2) orelse return null,
            shapeDimUsize(weight_shape, 3) orelse return null,
            std.math.cast(usize, attrs.strides[0]) orelse return null,
            std.math.cast(usize, attrs.strides[1]) orelse return null,
            std.math.cast(usize, attrs.padding[0][0]) orelse return null,
            std.math.cast(usize, attrs.padding[1][0]) orelse return null,
            std.math.cast(usize, attrs.groups) orelse return null,
        ) catch |err| switch (err) {
            error.UnsupportedOperation, error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => null,
            else => return err,
        };
    }
    return null;
}

fn executeRuntimeLinearNoBiasPair(
    cb: *const ComputeBackend,
    values: []?CT,
    inputs: []const NodeId,
    attrs: anytype,
    exec_state: *interpreter.ExecState,
) !?CT {
    const input = valueFor(values, inputs[0]) orelse return null;
    const weight_a = valueFor(values, inputs[1]) orelse return null;
    const weight_b = valueFor(values, inputs[2]) orelse return null;
    const result = cb.linearNoBiasPair(
        input,
        weight_a,
        weight_b,
        attrs.rows,
        attrs.in_dim,
        attrs.out_dim,
    ) catch |err| switch (err) {
        error.UnsupportedOperation, error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => return null,
        else => return err,
    };
    exec_state.pair_second = result.second;
    return result.first;
}

fn validatePlannedLinearOp(
    cb: *const ComputeBackend,
    weight: CT,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    op_plan: ?OperatorPlan,
) !void {
    const plan = op_plan orelse return;
    switch (plan) {
        .quant_matmul => |quant| {
            if (quant.operator == .fallback or
                quant.rows != rows or
                quant.in_dim != in_dim or
                quant.out_dim != out_dim)
            {
                return error.InvalidPartitionPlan;
            }
            if (comptime build_options.enable_metal) {
                const storage = metal_compute_mod.MetalCompute.getQuantizedStorage(cb, weight) orelse
                    return error.InvalidPartitionPlan;
                const format = contracts.quantFormatFromGgufTensorType(storage.tensor_type) orelse
                    return error.InvalidPartitionPlan;
                if (format != quant.format) return error.InvalidPartitionPlan;
            }
        },
        else => return error.InvalidPartitionPlan,
    }
}

fn validatePlannedAttentionOp(
    q_len: usize,
    kv_len: usize,
    head_dim: usize,
    op_plan: ?OperatorPlan,
) !operator_plan_mod.AttentionOpPlan {
    const plan = op_plan orelse return error.InvalidPartitionPlan;
    switch (plan) {
        .attention => |attention| {
            if (attention.operator == .fallback or
                attention.q_len != q_len or
                attention.kv_len != kv_len or
                attention.head_dim != head_dim)
            {
                return error.InvalidPartitionPlan;
            }
            switch (attention.operator) {
                .attention_flash => {
                    if (attention.storage != .dense or attention.kv_format != .f32) return error.InvalidPartitionPlan;
                },
                .attention_paged => {
                    if (attention.storage != .paged) return error.InvalidPartitionPlan;
                },
                .attention_quantized_kv => {
                    if (attention.kv_format != .polar4 and
                        attention.kv_format != .turbo3 and
                        attention.kv_format != .quantized)
                    {
                        return error.InvalidPartitionPlan;
                    }
                },
                else => return error.InvalidPartitionPlan,
            }
            return attention;
        },
        else => return error.InvalidPartitionPlan,
    }
}

fn executeRuntimeTakeRows(
    cb: *const ComputeBackend,
    values: []?CT,
    inputs: []const NodeId,
    rows: usize,
    dim: usize,
    op_plan: ?OperatorPlan,
    exec_state: *interpreter.ExecState,
) !?CT {
    const input = valueFor(values, inputs[0]) orelse return null;
    const grouped = exec_state.moe_grouped orelse return null;
    if (grouped.rows.len != rows) return error.InvalidPartitionPlan;
    try validatePlannedQuantRowOp(cb, input, rows, dim, op_plan);
    return cb.takeRows(input, grouped.rows, rows, dim) catch |err| switch (err) {
        error.UnsupportedOperation, error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => null,
        else => return err,
    };
}

fn executeRuntimeRope(
    cb: *const ComputeBackend,
    values: []?CT,
    inputs: []const NodeId,
    attrs: anytype,
    exec_state: *interpreter.ExecState,
) !?CT {
    const input = valueFor(values, inputs[0]) orelse return null;
    const rope_dim: usize = if (attrs.rope_dim > 0) attrs.rope_dim else attrs.head_dim;
    const position_offset = if (exec_state.options.attention) |attn|
        attn.total_sequence_len - attn.query_sequence_len
    else
        attrs.position_offset;
    return cb.rope(
        input,
        attrs.seq_len,
        attrs.head_dim,
        rope_dim,
        attrs.theta,
        attrs.freq_scale,
        position_offset,
        attrs.consecutive_pairs,
    ) catch |err| switch (err) {
        error.UnsupportedOperation, error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => null,
        else => return err,
    };
}

fn validatePlannedQuantRowOp(
    cb: *const ComputeBackend,
    input: CT,
    rows: usize,
    dim: usize,
    op_plan: ?OperatorPlan,
) !void {
    const plan = op_plan orelse return error.InvalidPartitionPlan;
    switch (plan) {
        .quant_row => |row| {
            if (row.operator == .fallback or
                row.kind != .get_rows or
                row.rows != rows or
                row.dim != dim)
            {
                return error.InvalidPartitionPlan;
            }
            if (comptime build_options.enable_metal) {
                const storage = metal_compute_mod.MetalCompute.getQuantizedStorage(cb, input) orelse
                    return error.InvalidPartitionPlan;
                const format = contracts.quantFormatFromGgufTensorType(storage.tensor_type) orelse
                    return error.InvalidPartitionPlan;
                if (format != row.format) return error.InvalidPartitionPlan;
            }
        },
        else => return error.InvalidPartitionPlan,
    }
}

fn executeRuntimeLayerNorm(
    cb: *const ComputeBackend,
    values: []?CT,
    inputs: []const NodeId,
    dim: usize,
    eps: f32,
    output_shape: ml.graph.Shape,
) !?CT {
    const output_elems = tensorElementCount(output_shape) orelse return null;
    if (dim == 0 or output_elems % dim != 0) return null;
    const input = valueFor(values, inputs[0]) orelse return null;
    const weight = valueFor(values, inputs[1]) orelse return null;
    const bias = valueFor(values, inputs[2]) orelse return null;
    return cb.layerNorm(input, weight, bias, dim, eps) catch |err| switch (err) {
        error.UnsupportedOperation, error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => null,
        else => return err,
    };
}

fn executeRuntimeRmsNorm(
    cb: *const ComputeBackend,
    values: []?CT,
    inputs: []const NodeId,
    dim: usize,
    eps: f32,
    output_shape: ml.graph.Shape,
) !?CT {
    const output_elems = tensorElementCount(output_shape) orelse return null;
    if (dim == 0 or output_elems % dim != 0) return null;
    const input = valueFor(values, inputs[0]) orelse return null;
    const weight = valueFor(values, inputs[1]) orelse return null;
    return cb.rmsNorm(input, weight, dim, eps) catch |err| switch (err) {
        error.UnsupportedOperation, error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => null,
        else => return err,
    };
}

fn executeRuntimeActivation(
    cb: *const ComputeBackend,
    values: []?CT,
    inputs: []const NodeId,
    kind: ops_mod.DecoderRuntimeActivationKind,
    output_shape: ml.graph.Shape,
    output_hint: ?CT,
) !?CT {
    const input = valueFor(values, inputs[0]) orelse return null;
    const dim = activationLastDim(output_shape) orelse return null;
    // Slot-bound output: write the activation directly into the pooled buffer.
    // gelu/relu/silu/quick_gelu are elementwise, so the rows/dim split is
    // immaterial — pass the full element count (rows=1) like the eager path.
    if (output_hint) |hint| {
        if (isMetalDeviceResident(cb, input)) {
            if (try metal_compute_mod.MetalCompute.activationInto(cb, input, @intFromEnum(kind), dim, hint)) |out| return out;
        }
    }
    if (try cb.decoderRuntimeApplyActivation(&.{
        .input = input,
        .kind = kind,
        .dim = dim,
    })) |result| return result;
    return switch (kind) {
        .gelu => cb.gelu(input),
        .gelu_exact => cb.decoderRuntimeApplyActivation(&.{
            .input = input,
            .kind = .gelu_exact,
            .dim = dim,
        }),
        .relu => cb.relu(input),
        .silu => cb.silu(input),
        .quick_gelu => cb.quickGelu(input),
        else => return null,
    } catch |err| switch (err) {
        error.UnsupportedOperation, error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => null,
        else => return err,
    };
}

fn activationLastDim(shape: ml.graph.Shape) ?usize {
    const rank = shape.rank();
    if (rank == 0) return tensorElementCount(shape);
    const dim = shape.dim(@intCast(rank - 1));
    if (dim <= 0) return null;
    return @intCast(dim);
}

fn executeRuntimeGeluBackward(
    cb: *const ComputeBackend,
    values: []?CT,
    inputs: []const NodeId,
    output_shape: ml.graph.Shape,
    exact: bool,
) !?CT {
    const input = valueFor(values, inputs[0]) orelse return null;
    const upstream_grad = valueFor(values, inputs[1]) orelse return null;
    const dim = tensorElementCount(output_shape) orelse return null;
    return cb.decoderRuntimeApplyGeluBackward(&.{
        .input = input,
        .upstream_grad = upstream_grad,
        .dim = dim,
        .exact = exact,
    }) catch |err| switch (err) {
        error.UnsupportedOperation, error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => null,
        else => return err,
    };
}

fn executeRuntimeGather(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    node_id: NodeId,
    inputs: []const NodeId,
    attrs: anytype,
    stats: ?*PartitionExecutor.ExecutionStats,
    resident_input_cache: ?*ResidentInputCache,
) !?CT {
    if (inputs.len < 2) return null;
    var input_shape_buf: [ml.graph.shape.max_rank]i64 = undefined;
    const input_shape = try fillShapeDims(graph.node(inputs[0]).output_shape, &input_shape_buf);
    const input_value = valueFor(values, inputs[0]) orelse return null;
    const indices_value = valueFor(values, inputs[1]) orelse return null;
    const indices = try temporaryMetalResidentValue(cb, indices_value);
    defer indices.deinit(cb);
    var cached_input: ?CT = null;
    if (resident_input_cache) |cache| {
        cached_input = try cache.residentValueForConsumer(cb, graph, inputs[0], input_value, stats);
    }
    const input_bytes = if (tensorElementCount(graph.node(inputs[0]).output_shape)) |elems| elems * @sizeOf(f32) else 0;
    const output_bytes = if (tensorElementCount(graph.node(node_id).output_shape)) |elems| elems * @sizeOf(f32) else 0;
    const promote_output = cached_input == null and cb.kind() == .metal and gatherPromoteInputEnabled() and !isMetalDeviceResident(cb, input_value) and output_bytes > 0 and input_bytes > output_bytes;
    const promote_input = cached_input == null and !promote_output and cb.kind() == .metal and gatherPromoteInputEnabled() and !isMetalDeviceResident(cb, input_value);
    const promotion_start_ns = if (promote_input) metalPartitionNowNs() else 0;
    const input_resident = if (cached_input) |cached|
        TemporaryMetalResidentValue{ .value = cached }
    else if (promote_input)
        try temporaryMetalResidentValue(cb, input_value)
    else
        TemporaryMetalResidentValue{ .value = input_value };
    defer input_resident.deinit(cb);
    if (promote_input and isMetalDeviceResident(cb, input_resident.value)) {
        if (stats) |s| {
            s.metal_gather_input_promotions += 1;
            s.metal_gather_input_promotion_ns += metalPartitionNowNs() - promotion_start_ns;
            if (tensorElementCount(graph.node(inputs[0]).output_shape)) |elems| {
                s.metal_gather_input_promotion_bytes += @intCast(elems * @sizeOf(f32));
            }
        }
    }

    if (!promote_output and attrs.axis == 0) gather_add_bias: {
        const input_node = graph.node(inputs[0]);
        if (input_node.op != .add) break :gather_add_bias;
        const add_inputs = input_node.getInputs();
        if (add_inputs.len != 2) break :gather_add_bias;

        const lhs = add_inputs[0];
        const rhs = add_inputs[1];
        if (lhs == null_node or rhs == null_node) break :gather_add_bias;

        var lhs_shape_buf: [ml.graph.shape.max_rank]i64 = undefined;
        var rhs_shape_buf: [ml.graph.shape.max_rank]i64 = undefined;
        const lhs_shape = try fillShapeDims(graph.node(lhs).output_shape, &lhs_shape_buf);
        const rhs_shape = try fillShapeDims(graph.node(rhs).output_shape, &rhs_shape_buf);

        var matrix_id: NodeId = null_node;
        var bias_id: NodeId = null_node;
        var matrix_shape_buf: [ml.graph.shape.max_rank]i64 = undefined;
        var matrix_shape: []const i64 = &.{};
        if (lhs_shape.len == 2 and rhs_shape.len == 1 and lhs_shape[1] == rhs_shape[0]) {
            matrix_id = lhs;
            bias_id = rhs;
            @memcpy(matrix_shape_buf[0..lhs_shape.len], lhs_shape);
            matrix_shape = matrix_shape_buf[0..lhs_shape.len];
        } else if (rhs_shape.len == 2 and lhs_shape.len == 1 and rhs_shape[1] == lhs_shape[0]) {
            matrix_id = rhs;
            bias_id = lhs;
            @memcpy(matrix_shape_buf[0..rhs_shape.len], rhs_shape);
            matrix_shape = matrix_shape_buf[0..rhs_shape.len];
        } else {
            break :gather_add_bias;
        }

        const matrix = valueFor(values, matrix_id) orelse break :gather_add_bias;
        const bias = valueFor(values, bias_id) orelse break :gather_add_bias;
        const fused = cb.primGatherAddBiasAxis0(matrix, bias, indices.value, matrix_shape) catch |err| switch (err) {
            error.UnsupportedOperation, error.UnsupportedPrimitiveOp, error.UnsupportedTensorType, error.UnsupportedShape, error.ShapeMismatch, error.InvalidTensorShape => null,
            else => return err,
        };
        if (fused) |output| return output;
    }

    const gathered = cb.primGather(input_resident.value, indices.value, attrs.axis, input_shape) catch |err| switch (err) {
        error.UnsupportedOperation, error.UnsupportedPrimitiveOp, error.UnsupportedTensorType, error.UnsupportedShape, error.ShapeMismatch, error.InvalidTensorShape => null,
        else => return err,
    };
    if (promote_output) {
        if (gathered) |host_result| {
            if (!isMetalDeviceResident(cb, host_result)) {
                const output_promotion_start_ns = metalPartitionNowNs();
                if (try makeMetalDeviceResident(cb, host_result)) |device_result| {
                    if (stats) |s| {
                        s.metal_gather_output_promotions += 1;
                        s.metal_gather_output_promotion_ns += metalPartitionNowNs() - output_promotion_start_ns;
                        s.metal_gather_output_promotion_bytes += @intCast(output_bytes);
                    }
                    if (device_result != host_result) cb.free(host_result);
                    return device_result;
                }
            }
        }
    }
    return gathered;
}

fn executeRuntimeAdd(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    inputs: []const NodeId,
    output_shape: ml.graph.Shape,
    output_hint: ?CT,
) !?CT {
    const dim = tensorElementCount(output_shape) orelse return null;
    if (try executeRuntimeScaledAddFromDeferredMul(graph, cb, values, inputs, output_shape, dim)) |result| return result;
    if (try executeRuntimeMultiplyAddFromDeferredMul(graph, cb, values, inputs, output_shape, dim)) |result| return result;
    const lhs_operand = runtimeBinaryOperand(graph, values, inputs[0], output_shape) orelse return null;
    const rhs_operand = runtimeBinaryOperand(graph, values, inputs[1], output_shape) orelse return null;
    if (!binaryBroadcastResultMatches(lhs_operand.shape, rhs_operand.shape, output_shape)) return null;
    const lhs = lhs_operand.value;
    const rhs = rhs_operand.value;
    // Slot-bound output: same-shape elementwise add into the pooled buffer
    // (both operands already device-resident; addInto self-checks equal sizes).
    if (output_hint) |hint| {
        if (isMetalDeviceResident(cb, lhs) and isMetalDeviceResident(cb, rhs) and sameElementCount(lhs_operand.shape, rhs_operand.shape)) {
            if (try metal_compute_mod.MetalCompute.addInto(cb, lhs, rhs, hint, false)) |out| return out;
        }
    }
    if (isMetalDeviceResident(cb, lhs) or isMetalDeviceResident(cb, rhs)) {
        return cb.add(lhs, rhs) catch |err| switch (err) {
            error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => null,
            else => return err,
        };
    }
    if (try cb.decoderRuntimeApplyAdd(&.{ .lhs = lhs, .rhs = rhs, .dim = dim })) |result| return result;
    return cb.add(lhs, rhs) catch |err| switch (err) {
        error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => null,
        else => return err,
    };
}

const DeferredScaleMul = struct {
    source_id: NodeId,
    scalar_id: NodeId,
    scale: f32,
};

const DeferredElementwiseMul = struct {
    lhs_id: NodeId,
    rhs_id: NodeId,
};

const RuntimeBinaryOperand = struct {
    value: CT,
    shape: ml.graph.Shape,
    node_id: NodeId,
};

fn runtimeBinaryOperand(
    graph: *const Graph,
    values: []?CT,
    node_id: NodeId,
    output_shape: ml.graph.Shape,
) ?RuntimeBinaryOperand {
    _ = output_shape;
    if (node_id == null_node or node_id >= graph.nodeCount()) return null;
    if (valueFor(values, node_id)) |value| {
        return .{ .value = value, .shape = graph.node(node_id).output_shape, .node_id = node_id };
    }
    return null;
}

fn executeRuntimeScaledAddFromDeferredMul(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    inputs: []const NodeId,
    output_shape: ml.graph.Shape,
    dim: usize,
) !?CT {
    if (inputs.len < 2) return null;
    if (pendingDeferredScaleMul(graph, values, inputs[0], output_shape)) |lhs_scaled| {
        if (pendingDeferredScaleMul(graph, values, inputs[1], output_shape)) |rhs_scaled| {
            const lhs = (try executeDeferredScaleMulValue(cb, values, lhs_scaled)) orelse return null;
            defer cb.free(lhs);
            const rhs = (try executeDeferredScaleMulValue(cb, values, rhs_scaled)) orelse return null;
            defer cb.free(rhs);
            const output = cb.add(lhs, rhs) catch |err| switch (err) {
                error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => null,
                else => return err,
            };
            return try promoteMetalOutputIfNeeded(cb, output);
        }
    }
    if (pendingDeferredScaleMul(graph, values, inputs[0], output_shape)) |scaled| {
        const scaled_lhs = valueFor(values, scaled.source_id) orelse return null;
        const residual = valueFor(values, inputs[1]) orelse return null;
        const output = cb.decoderRuntimeApplyScaledAddScale(&.{
            .lhs = scaled_lhs,
            .rhs = residual,
            .dim = dim,
            .lhs_scale = scaled.scale,
            .output_scale = 1.0,
        }) catch |err| switch (err) {
            error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => null,
            else => return err,
        };
        if (try promoteMetalOutputIfNeeded(cb, output)) |result| return result;
        return try executeDeferredScaleAddFallback(cb, values, scaled, residual);
    }
    if (pendingDeferredScaleMul(graph, values, inputs[1], output_shape)) |scaled| {
        const scaled_lhs = valueFor(values, scaled.source_id) orelse return null;
        const residual = valueFor(values, inputs[0]) orelse return null;
        const output = cb.decoderRuntimeApplyScaledAddScale(&.{
            .lhs = scaled_lhs,
            .rhs = residual,
            .dim = dim,
            .lhs_scale = scaled.scale,
            .output_scale = 1.0,
        }) catch |err| switch (err) {
            error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => null,
            else => return err,
        };
        if (try promoteMetalOutputIfNeeded(cb, output)) |result| return result;
        return try executeDeferredScaleAddFallback(cb, values, scaled, residual);
    }
    return null;
}

fn executeRuntimeMultiplyAddFromDeferredMul(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    inputs: []const NodeId,
    output_shape: ml.graph.Shape,
    dim: usize,
) !?CT {
    if (inputs.len < 2) return null;
    if (pendingDeferredElementwiseMul(graph, values, inputs[0], output_shape)) |mul| {
        if (pendingDeferredElementwiseMul(graph, values, inputs[1], output_shape)) |rhs_mul| {
            return try executeDeferredElementwiseMultiplyAdd2(cb, values, mul, rhs_mul, dim);
        }
        const addend = valueFor(values, inputs[1]) orelse return null;
        return try executeDeferredElementwiseMultiplyAdd(cb, values, mul, addend, dim);
    }
    if (pendingDeferredElementwiseMul(graph, values, inputs[1], output_shape)) |mul| {
        const addend = valueFor(values, inputs[0]) orelse return null;
        return try executeDeferredElementwiseMultiplyAdd(cb, values, mul, addend, dim);
    }
    return null;
}

fn executeDeferredElementwiseMultiplyAdd2(
    cb: *const ComputeBackend,
    values: []?CT,
    lhs_mul: DeferredElementwiseMul,
    rhs_mul: DeferredElementwiseMul,
    dim: usize,
) !?CT {
    var lhs0 = valueFor(values, lhs_mul.lhs_id) orelse return null;
    var rhs0 = valueFor(values, lhs_mul.rhs_id) orelse return null;
    var lhs1 = valueFor(values, rhs_mul.lhs_id) orelse return null;
    var rhs1 = valueFor(values, rhs_mul.rhs_id) orelse return null;
    var owned_lhs0: ?CT = null;
    defer if (owned_lhs0) |ct| cb.free(ct);
    var owned_rhs0: ?CT = null;
    defer if (owned_rhs0) |ct| cb.free(ct);
    var owned_lhs1: ?CT = null;
    defer if (owned_lhs1) |ct| cb.free(ct);
    var owned_rhs1: ?CT = null;
    defer if (owned_rhs1) |ct| cb.free(ct);
    if (cb.kind() == .metal) {
        if (!isMetalDeviceResident(cb, lhs0)) {
            if (try makeMetalDeviceResident(cb, lhs0)) |device_lhs0| {
                if (device_lhs0 != lhs0) owned_lhs0 = device_lhs0;
                lhs0 = device_lhs0;
            }
        }
        if (!isMetalDeviceResident(cb, rhs0)) {
            if (try makeMetalDeviceResident(cb, rhs0)) |device_rhs0| {
                if (device_rhs0 != rhs0) owned_rhs0 = device_rhs0;
                rhs0 = device_rhs0;
            }
        }
        if (!isMetalDeviceResident(cb, lhs1)) {
            if (try makeMetalDeviceResident(cb, lhs1)) |device_lhs1| {
                if (device_lhs1 != lhs1) owned_lhs1 = device_lhs1;
                lhs1 = device_lhs1;
            }
        }
        if (!isMetalDeviceResident(cb, rhs1)) {
            if (try makeMetalDeviceResident(cb, rhs1)) |device_rhs1| {
                if (device_rhs1 != rhs1) owned_rhs1 = device_rhs1;
                rhs1 = device_rhs1;
            }
        }
    }
    const output = cb.decoderRuntimeApplyMultiplyAdd2(&.{
        .lhs0 = lhs0,
        .rhs0 = rhs0,
        .lhs1 = lhs1,
        .rhs1 = rhs1,
        .dim = dim,
    }) catch |err| switch (err) {
        error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => null,
        else => return err,
    };
    if (try promoteMetalOutputIfNeeded(cb, output)) |result| return result;

    const lhs_product = cb.multiply(lhs0, rhs0) catch |err| switch (err) {
        error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => return null,
        else => return err,
    };
    defer cb.free(lhs_product);
    const rhs_product = cb.multiply(lhs1, rhs1) catch |err| switch (err) {
        error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => return null,
        else => return err,
    };
    defer cb.free(rhs_product);
    const fallback = cb.add(lhs_product, rhs_product) catch |err| switch (err) {
        error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => return null,
        else => return err,
    };
    return try promoteMetalOutputIfNeeded(cb, fallback);
}

fn executeDeferredElementwiseMultiplyAdd(
    cb: *const ComputeBackend,
    values: []?CT,
    mul: DeferredElementwiseMul,
    addend: CT,
    dim: usize,
) !?CT {
    var lhs = valueFor(values, mul.lhs_id) orelse return null;
    var rhs = valueFor(values, mul.rhs_id) orelse return null;
    var residual = addend;
    var owned_lhs: ?CT = null;
    defer if (owned_lhs) |ct| cb.free(ct);
    var owned_rhs: ?CT = null;
    defer if (owned_rhs) |ct| cb.free(ct);
    var owned_residual: ?CT = null;
    defer if (owned_residual) |ct| cb.free(ct);
    if (cb.kind() == .metal) {
        if (!isMetalDeviceResident(cb, lhs)) {
            if (try makeMetalDeviceResident(cb, lhs)) |device_lhs| {
                if (device_lhs != lhs) owned_lhs = device_lhs;
                lhs = device_lhs;
            }
        }
        if (!isMetalDeviceResident(cb, rhs)) {
            if (try makeMetalDeviceResident(cb, rhs)) |device_rhs| {
                if (device_rhs != rhs) owned_rhs = device_rhs;
                rhs = device_rhs;
            }
        }
        if (!isMetalDeviceResident(cb, residual)) {
            if (try makeMetalDeviceResident(cb, residual)) |device_residual| {
                if (device_residual != residual) owned_residual = device_residual;
                residual = device_residual;
            }
        }
    }
    const output = cb.decoderRuntimeApplyMultiplyAdd(&.{
        .lhs = lhs,
        .rhs = rhs,
        .addend = residual,
        .dim = dim,
    }) catch |err| switch (err) {
        error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => null,
        else => return err,
    };
    if (try promoteMetalOutputIfNeeded(cb, output)) |result| return result;

    const multiplied = cb.multiply(lhs, rhs) catch |err| switch (err) {
        error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => return null,
        else => return err,
    };
    defer cb.free(multiplied);
    const fallback = cb.add(multiplied, residual) catch |err| switch (err) {
        error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => return null,
        else => return err,
    };
    return try promoteMetalOutputIfNeeded(cb, fallback);
}

fn executeDeferredScaleMulValue(
    cb: *const ComputeBackend,
    values: []?CT,
    scaled: DeferredScaleMul,
) !?CT {
    var lhs = valueFor(values, scaled.source_id) orelse return null;
    var scalar = valueFor(values, scaled.scalar_id) orelse return null;
    var owned_lhs: ?CT = null;
    defer if (owned_lhs) |ct| cb.free(ct);
    var owned_scalar: ?CT = null;
    defer if (owned_scalar) |ct| cb.free(ct);
    if (cb.kind() == .metal) {
        if (!isMetalDeviceResident(cb, lhs)) {
            if (try makeMetalDeviceResident(cb, lhs)) |device_lhs| {
                if (device_lhs != lhs) owned_lhs = device_lhs;
                lhs = device_lhs;
            }
        }
        if (!isMetalDeviceResident(cb, scalar)) {
            if (try makeMetalDeviceResident(cb, scalar)) |device_scalar| {
                if (device_scalar != scalar) owned_scalar = device_scalar;
                scalar = device_scalar;
            }
        }
    }
    return cb.multiply(lhs, scalar) catch |err| switch (err) {
        error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => null,
        else => return err,
    };
}

fn executeDeferredScaleAddFallback(
    cb: *const ComputeBackend,
    values: []?CT,
    scaled: DeferredScaleMul,
    residual: CT,
) !?CT {
    var rhs = residual;
    var owned_rhs: ?CT = null;
    defer if (owned_rhs) |ct| cb.free(ct);
    if (cb.kind() == .metal) {
        if (!isMetalDeviceResident(cb, rhs)) {
            if (try makeMetalDeviceResident(cb, rhs)) |device_rhs| {
                if (device_rhs != rhs) owned_rhs = device_rhs;
                rhs = device_rhs;
            }
        }
    }
    const multiplied_ct = (try executeDeferredScaleMulValue(cb, values, scaled)) orelse return null;
    defer cb.free(multiplied_ct);
    const output = cb.add(multiplied_ct, rhs) catch |err| switch (err) {
        error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => null,
        else => return err,
    };
    return try promoteMetalOutputIfNeeded(cb, output);
}

/// A mul input may only be consumed as a "deferred" (fused) producer when its
/// value has NOT been materialized. The defer heuristics
/// (shouldDeferScaleMulForAdd / shouldDeferElementwiseMulForAdd) additionally
/// require single-use + last-use-at-consumer before skipping a node, so a mul
/// that merely matches the structural pattern can still have been executed
/// normally (e.g. multi-use gradient terms in backward graphs). Recomputing
/// such a materialized mul from its sources is unsound: those sources may
/// already have been freed by last-use bookkeeping, which previously made the
/// whole fused-add path bail and leave the genuinely-deferred sibling input
/// unmaterialized (error.MissingRuntimeInput, see gliner2 LoRA backward
/// add(mul, mul) at node 4996).
fn pendingDeferredScaleMul(graph: *const Graph, values: []?CT, node_id: NodeId, output_shape: ml.graph.Shape) ?DeferredScaleMul {
    if (valueFor(values, node_id) != null) return null;
    return deferredScaleMul(graph, node_id, output_shape);
}

fn pendingDeferredElementwiseMul(graph: *const Graph, values: []?CT, node_id: NodeId, output_shape: ml.graph.Shape) ?DeferredElementwiseMul {
    if (valueFor(values, node_id) != null) return null;
    return deferredElementwiseMul(graph, node_id, output_shape);
}

fn deferredScaleMul(graph: *const Graph, node_id: NodeId, output_shape: ml.graph.Shape) ?DeferredScaleMul {
    if (node_id == null_node or node_id >= graph.nodeCount()) return null;
    const node = graph.node(node_id);
    switch (node.op) {
        .mul, .fused_elem_multiply => {},
        else => return null,
    }
    if (!shapesEqual(node.output_shape, output_shape)) return null;
    if (node.num_inputs < 2) return null;
    if (scalarConstantF32(graph, node.inputs[0])) |scale| {
        return .{ .source_id = node.inputs[1], .scalar_id = node.inputs[0], .scale = scale };
    }
    if (scalarConstantF32(graph, node.inputs[1])) |scale| {
        return .{ .source_id = node.inputs[0], .scalar_id = node.inputs[1], .scale = scale };
    }
    return null;
}

fn deferredElementwiseMul(graph: *const Graph, node_id: NodeId, output_shape: ml.graph.Shape) ?DeferredElementwiseMul {
    if (node_id == null_node or node_id >= graph.nodeCount()) return null;
    const node = graph.node(node_id);
    switch (node.op) {
        .mul, .fused_elem_multiply => {},
        else => return null,
    }
    if (!shapesEqual(node.output_shape, output_shape)) return null;
    if (node.output_shape.dtype != .f32 or node.num_inputs < 2) return null;
    if (deferredScaleMul(graph, node_id, output_shape) != null) return null;
    const lhs_id = node.inputs[0];
    const rhs_id = node.inputs[1];
    if (lhs_id == null_node or rhs_id == null_node or lhs_id >= graph.nodeCount() or rhs_id >= graph.nodeCount()) return null;
    if (!shapesEqual(graph.node(lhs_id).output_shape, output_shape)) return null;
    if (!shapesEqual(graph.node(rhs_id).output_shape, output_shape)) return null;
    if (isSameShapeElementwiseMul(graph, lhs_id, output_shape)) return null;
    if (isSameShapeElementwiseMul(graph, rhs_id, output_shape)) return null;
    return .{ .lhs_id = lhs_id, .rhs_id = rhs_id };
}

fn isSameShapeElementwiseMul(graph: *const Graph, node_id: NodeId, output_shape: ml.graph.Shape) bool {
    if (node_id == null_node or node_id >= graph.nodeCount()) return false;
    const node = graph.node(node_id);
    switch (node.op) {
        .mul, .fused_elem_multiply => {},
        else => return false,
    }
    return shapesEqual(node.output_shape, output_shape);
}

fn binaryBroadcastResultMatches(
    lhs_shape: ml.graph.Shape,
    rhs_shape: ml.graph.Shape,
    output_shape: ml.graph.Shape,
) bool {
    if (lhs_shape.dtype != output_shape.dtype or rhs_shape.dtype != output_shape.dtype) return false;
    const lhs_rank = lhs_shape.rank();
    const rhs_rank = rhs_shape.rank();
    const output_rank = output_shape.rank();
    if (output_rank == 0) {
        const lhs_count = tensorElementCount(lhs_shape) orelse return false;
        const rhs_count = tensorElementCount(rhs_shape) orelse return false;
        return lhs_count == 1 and rhs_count == 1;
    }
    if (lhs_rank > output_rank or rhs_rank > output_rank) return false;
    var axis_from_end: usize = 0;
    while (axis_from_end < output_rank) : (axis_from_end += 1) {
        const output_axis = output_rank - 1 - axis_from_end;
        const lhs_dim = if (axis_from_end < lhs_rank)
            lhs_shape.dim(@intCast(lhs_rank - 1 - axis_from_end))
        else
            1;
        const rhs_dim = if (axis_from_end < rhs_rank)
            rhs_shape.dim(@intCast(rhs_rank - 1 - axis_from_end))
        else
            1;
        const output_dim = output_shape.dim(@intCast(output_axis));
        if (lhs_dim <= 0 or rhs_dim <= 0 or output_dim <= 0) return false;
        const result_dim = if (lhs_dim == rhs_dim) lhs_dim else if (lhs_dim == 1) rhs_dim else if (rhs_dim == 1) lhs_dim else return false;
        if (result_dim != output_dim) return false;
    }
    return true;
}

fn shouldDeferScaleMulForAdd(
    graph: *const Graph,
    node_id: NodeId,
    reachable: []const bool,
    last_use: []const u32,
    use_counts: ?[]const u8,
) bool {
    const node_index: usize = @intCast(node_id);
    if (node_index >= reachable.len or !reachable[node_index]) return false;
    if (node_index >= last_use.len) return false;
    const consumer_id: NodeId = @intCast(last_use[node_index]);
    if (consumer_id == null_node or consumer_id >= graph.nodeCount()) return false;
    const consumer_index: usize = @intCast(consumer_id);
    if (consumer_index >= reachable.len or !reachable[consumer_index]) return false;
    const consumer = graph.node(consumer_id);
    switch (consumer.op) {
        .add, .fused_elem_add => {},
        else => return false,
    }
    if (consumer.num_inputs < 2 or (consumer.inputs[0] != node_id and consumer.inputs[1] != node_id)) return false;
    if (!hasSingleReachableConsumer(graph, node_id, reachable, use_counts)) return false;
    if (deferredScaleMul(graph, node_id, consumer.output_shape) == null) return false;
    return deferredProducerInputsLiveUntilConsumer(graph, node_id, consumer_id, last_use);
}

fn shouldDeferElementwiseMulForAdd(
    graph: *const Graph,
    node_id: NodeId,
    reachable: []const bool,
    last_use: []const u32,
    use_counts: ?[]const u8,
) bool {
    const node_index: usize = @intCast(node_id);
    if (node_index >= reachable.len or !reachable[node_index]) return false;
    if (node_index >= last_use.len) return false;
    const consumer_id: NodeId = @intCast(last_use[node_index]);
    if (consumer_id == null_node or consumer_id >= graph.nodeCount()) return false;
    const consumer_index: usize = @intCast(consumer_id);
    if (consumer_index >= reachable.len or !reachable[consumer_index]) return false;
    const consumer = graph.node(consumer_id);
    switch (consumer.op) {
        .add, .fused_elem_add => {},
        else => return false,
    }
    if (consumer.num_inputs < 2 or (consumer.inputs[0] != node_id and consumer.inputs[1] != node_id)) return false;
    if (!hasSingleReachableConsumer(graph, node_id, reachable, use_counts)) return false;
    if (deferredElementwiseMul(graph, node_id, consumer.output_shape) == null) return false;
    if (!deferredProducerInputsLiveUntilConsumer(graph, node_id, consumer_id, last_use)) return false;
    return true;
}

fn deferredProducerInputsLiveUntilConsumer(
    graph: *const Graph,
    producer_id: NodeId,
    consumer_id: NodeId,
    last_use: []const u32,
) bool {
    if (producer_id == null_node or producer_id >= graph.nodeCount()) return false;
    const producer_index: usize = @intCast(producer_id);
    const consumer_index: usize = @intCast(consumer_id);
    const producer = graph.node(producer_id);
    for (producer.getInputs()) |input_id| {
        if (input_id == null_node or input_id >= graph.nodeCount()) return false;
        const input_index: usize = @intCast(input_id);
        if (input_index >= last_use.len) return false;
        // Pre-materialized constants (scalar/tensor consts, zero tensors) are
        // re-materialized every execution and never recycled by buffer reuse, so
        // their last-use position does not constrain deferral — a deferred mul can
        // safely read them at the consumer even if a sibling consumed them earlier.
        // Exempting them recovers fusion for the common `(x*s) + (y*s)` pattern
        // where a shared scalar's last use is the sibling multiply.
        if (isPreMaterializedConstantOp(graph.node(input_id).op)) continue;
        const input_last_use = last_use[input_index];
        if (input_last_use == std.math.maxInt(u32)) continue;
        const input_last_use_index: usize = @intCast(input_last_use);
        if (input_last_use_index == producer_index) continue;
        if (input_last_use_index >= consumer_index) continue;
        return false;
    }
    return true;
}

fn reachableUseCount(graph: *const Graph, node_id: NodeId, reachable: []const bool, stop_after: usize) usize {
    var count: usize = 0;
    var candidate: NodeId = 0;
    while (candidate < graph.nodeCount()) : (candidate += 1) {
        const candidate_index: usize = @intCast(candidate);
        if (candidate_index >= reachable.len or !reachable[candidate_index]) continue;
        const node = graph.node(candidate);
        for (node.getInputs()) |input_id| {
            if (input_id != node_id) continue;
            count += 1;
            if (count >= stop_after) return count;
        }
    }
    return count;
}

/// Per-node count of reachable consumers, capped at 2 (callers only need to tell
/// "exactly one consumer" from "more than one"). Built once in O(edges) so the
/// per-node execution loop can answer the deferral use-count query in O(1)
/// instead of an O(nodes) `reachableUseCount` scan per candidate — the fix for
/// the O(nodes^2) term on the hot path. Caller owns the returned slice.
fn computeReachableUseCountsCapped(allocator: std.mem.Allocator, graph: *const Graph, reachable: []const bool) ![]u8 {
    const counts = try allocator.alloc(u8, graph.nodeCount());
    @memset(counts, 0);
    var candidate: NodeId = 0;
    while (candidate < graph.nodeCount()) : (candidate += 1) {
        const candidate_index: usize = @intCast(candidate);
        if (candidate_index >= reachable.len or !reachable[candidate_index]) continue;
        for (graph.node(candidate).getInputs()) |input_id| {
            if (input_id == null_node or input_id >= graph.nodeCount()) continue;
            const input_index: usize = @intCast(input_id);
            if (counts[input_index] < 2) counts[input_index] += 1;
        }
    }
    return counts;
}

/// True when `node_id` has exactly one reachable consumer. Uses the precomputed
/// capped use-count array when supplied (O(1)); otherwise falls back to the
/// O(nodes) scan (fine for the small graphs in unit tests).
fn hasSingleReachableConsumer(graph: *const Graph, node_id: NodeId, reachable: []const bool, use_counts: ?[]const u8) bool {
    if (use_counts) |counts| {
        const idx: usize = @intCast(node_id);
        if (idx < counts.len) return counts[idx] == 1;
    }
    return reachableUseCount(graph, node_id, reachable, 2) == 1;
}

fn scalarConstantF32(graph: *const Graph, node_id: NodeId) ?f32 {
    if (node_id == null_node or node_id >= graph.nodeCount()) return null;
    const node = graph.node(node_id);
    const attrs = switch (node.op) {
        .constant => |constant_attrs| constant_attrs,
        else => return null,
    };
    if (node.output_shape.dtype != .f32 or node.output_shape.rank() != 0 or attrs.data_len != 1) return null;
    const values = graph.constantDataAs(f32, attrs.data_offset, attrs.data_len);
    if (values.len != 1) return null;
    return values[0];
}

fn shapesEqual(lhs: ml.graph.Shape, rhs: ml.graph.Shape) bool {
    if (lhs.dtype != rhs.dtype or lhs.rank() != rhs.rank()) return false;
    for (0..lhs.rank()) |idx| {
        if (lhs.dims[idx] != rhs.dims[idx]) return false;
    }
    return true;
}

fn executeRuntimeSoftmax(
    cb: *const ComputeBackend,
    values: []?CT,
    inputs: []const NodeId,
    dim: u32,
) !?CT {
    const input = valueFor(values, inputs[0]) orelse return null;
    return cb.primSoftmax(input, dim) catch |err| switch (err) {
        error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => null,
        else => return err,
    };
}

fn executeRuntimeLogSoftmax(
    cb: *const ComputeBackend,
    values: []?CT,
    inputs: []const NodeId,
    dim: u32,
) !?CT {
    const input = valueFor(values, inputs[0]) orelse return null;
    return cb.primLogSoftmax(input, dim) catch |err| switch (err) {
        error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => null,
        else => return err,
    };
}

fn valueFor(values: []?CT, node_id: NodeId) ?CT {
    if (node_id == null_node) return null;
    const index: usize = @intCast(node_id);
    if (index >= values.len) return null;
    return values[index];
}

fn fillShapeDims(shape: ml.graph.Shape, buf: *[ml.graph.shape.max_rank]i64) ![]const i64 {
    const rank = shape.rank();
    if (rank > buf.len) return error.UnsupportedShape;
    for (0..rank) |axis| buf[axis] = shape.dim(@intCast(axis));
    return buf[0..rank];
}

fn tensorElementCount(shape: ml.graph.Shape) ?usize {
    const elems = shape.maxElements() orelse shape.numElements() orelse return null;
    if (elems <= 0) return null;
    return @intCast(elems);
}

fn positiveI64ToUsize(dim: i64) ?usize {
    if (dim <= 0) return null;
    return std.math.cast(usize, dim);
}

fn shapeDimUsize(shape: ml.graph.Shape, axis: usize) ?usize {
    if (axis >= shape.rank()) return null;
    return positiveI64ToUsize(shape.dim(@intCast(axis)));
}

fn shapeRankForNodeOr(graph: *const Graph, node_id: NodeId, fallback: usize) usize {
    if (node_id == null_node or node_id >= graph.nodeCount()) return fallback;
    return graph.node(node_id).output_shape.rank();
}

fn shapeDimForNodeOr(graph: *const Graph, node_id: NodeId, axis: usize, fallback: i64) i64 {
    if (node_id == null_node or node_id >= graph.nodeCount()) return fallback;
    const shape = graph.node(node_id).output_shape;
    if (axis >= shape.rank()) return fallback;
    return shape.dim(@intCast(axis));
}

fn buildMetalGraphPlan(
    allocator: std.mem.Allocator,
    buffer_plan: *const buffer_plan_mod.BufferPlan,
    view: buffer_plan_mod.PartitionBufferView,
) !MetalPartitionGraphPlan {
    var mappings = std.ArrayListUnmanaged(MetalGraphPlanAllocation).empty;
    errdefer mappings.deinit(allocator);

    for (view.slots) |slot_view| {
        if (!slot_view.roles.local) continue;
        const allocation_id = slot_view.slot.allocation;
        if (allocation_id == buffer_plan_mod.invalid_allocation) continue;
        const allocation = buffer_plan.allocations[@intCast(allocation_id)];
        if (allocation.kind != .tensor) continue;
        try addGraphPlanAllocation(allocator, &mappings, allocation_id, allocation.byte_size);
    }

    const slots = try allocator.alloc(GraphPlanSlot, mappings.items.len);
    errdefer allocator.free(slots);
    for (mappings.items, slots) |mapping, *slot| {
        slot.* = .{ .slot = mapping.graph_slot, .bytes = mapping.bytes };
    }

    return .{
        .slots = slots,
        .allocations = try mappings.toOwnedSlice(allocator),
    };
}

fn addGraphPlanAllocation(
    allocator: std.mem.Allocator,
    mappings: *std.ArrayListUnmanaged(MetalGraphPlanAllocation),
    allocation_id: buffer_plan_mod.AllocationId,
    bytes_u64: u64,
) !void {
    const bytes: usize = std.math.cast(usize, bytes_u64) orelse return error.OutOfMemory;
    if (bytes == 0) return;
    for (mappings.items) |*mapping| {
        if (mapping.allocation != allocation_id) continue;
        mapping.bytes = @max(mapping.bytes, bytes);
        return;
    }
    if (mappings.items.len >= max_graph_plan_slots) {
        var smallest_idx: usize = 0;
        for (mappings.items[1..], 1..) |mapping, idx| {
            if (mapping.bytes < mappings.items[smallest_idx].bytes) smallest_idx = idx;
        }
        if (bytes <= mappings.items[smallest_idx].bytes) return;
        mappings.items[smallest_idx] = .{
            .allocation = allocation_id,
            .graph_slot = smallest_idx,
            .bytes = bytes,
        };
        return;
    }
    const graph_slot = mappings.items.len;
    try mappings.append(allocator, .{
        .allocation = allocation_id,
        .graph_slot = graph_slot,
        .bytes = bytes,
    });
}

fn materializePartitionRuntimeInputs(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    values: []?CT,
    value_device: []DeviceId,
    node_ids: []const NodeId,
    device_id: DeviceId,
    exec_ctx: PartitionExecutor.ExecutionContext,
    cb: *const ComputeBackend,
    rt_map: std.AutoHashMapUnmanaged(NodeId, CT),
) !void {
    const trace_nodes = traceMetalGraphNodesEnabled();
    for (node_ids) |node_id| {
        const rt_val = rt_map.get(node_id) orelse continue;
        const i: usize = @intCast(node_id);
        const current_dev = value_device[i];
        if (trace_nodes) std.debug.print(
            "graph_executor_node_trace: materialize_runtime_input node={d} current_device={d} target_device={d} resident={}\n",
            .{ node_id, current_dev, device_id, isMetalDeviceResident(cb, rt_val) },
        );
        const stay_host_backed = runtimeInputShouldStayHostBackedForDynamicMetalSlot(graph, node_id);
        if (current_dev != device_id and stay_host_backed) {
            const mesh = exec_ctx.mesh orelse return error.DeviceNotFound;
            const src_entry = mesh.device(current_dev) orelse return error.DeviceNotFound;
            if (trace_nodes) std.debug.print(
                "graph_executor_node_trace: materialize_runtime_input_transfer_host node={d} from_backend={s} to_backend={s}\n",
                .{ node_id, @tagName(src_entry.backend.kind()), @tagName(cb.kind()) },
            );
            const transferred = try transferTensorHostBacked(allocator, rt_val, src_entry.backend, cb);
            values[i] = transferred;
            if (exec_ctx.owned_runtime_transfers) |owned| try owned.put(allocator, node_id, {});
            if (exec_ctx.stats) |stats| stats.runtime_input_transfers += 1;
        } else if (current_dev != device_id) {
            const mesh = exec_ctx.mesh orelse return error.DeviceNotFound;
            const src_entry = mesh.device(current_dev) orelse return error.DeviceNotFound;
            if (trace_nodes) std.debug.print(
                "graph_executor_node_trace: materialize_runtime_input_transfer node={d} from_backend={s} to_backend={s}\n",
                .{ node_id, @tagName(src_entry.backend.kind()), @tagName(cb.kind()) },
            );
            const transferred = try transferTensor(allocator, rt_val, src_entry.backend, cb);
            values[i] = transferred;
            if (exec_ctx.owned_runtime_transfers) |owned| try owned.put(allocator, node_id, {});
            if (exec_ctx.stats) |stats| {
                stats.runtime_input_transfers += 1;
                if (isMetalDeviceResident(cb, transferred)) stats.device_resident_transfers += 1;
            }
        } else {
            values[i] = rt_val;
        }
        const preserve_runtime_input_residency = if (exec_ctx.options) |options| options.preserve_runtime_input_residency else false;
        if (!preserve_runtime_input_residency and values[i] != null) {
            const current = values[i].?;
            if (!stay_host_backed and !isMetalDeviceResident(cb, current)) {
                if (trace_nodes) std.debug.print(
                    "graph_executor_node_trace: materialize_runtime_input_make_resident node={d}\n",
                    .{node_id},
                );
                if (try makeMetalDeviceResident(cb, current)) |device_value| {
                    if (device_value != current) {
                        values[i] = device_value;
                        if (exec_ctx.owned_runtime_transfers) |owned| try owned.put(allocator, node_id, {});
                    }
                    if (exec_ctx.stats) |stats| stats.device_resident_transfers += 1;
                }
                if (trace_nodes) std.debug.print(
                    "graph_executor_node_trace: materialize_runtime_input_make_resident_done node={d} resident={}\n",
                    .{ node_id, if (values[i]) |updated| isMetalDeviceResident(cb, updated) else false },
                );
            }
        }
        value_device[i] = device_id;
    }
}

fn runtimeInputShouldStayHostBackedForDynamicMetalSlot(graph: *const Graph, node_id: NodeId) bool {
    const node = graph.node(node_id);
    if (std.meta.activeTag(node.op) != .parameter and std.meta.activeTag(node.op) != .constant) return false;
    for (graph.nodes.items) |consumer| {
        const inputs = consumer.getInputs();
        for (inputs, 0..) |input_id, input_index| {
            if (input_id != node_id) continue;
            switch (consumer.op) {
                .fused_linear => {
                    if (input_index == 1 or input_index == 2) return true;
                },
                .fused_linear_no_bias => {
                    if (input_index == 1) return true;
                },
                .fused_linear_no_bias_pair => {
                    if (input_index == 1 or input_index == 2) return true;
                },
                else => {},
            }
        }
    }
    return false;
}

fn isPreMaterializedConstantOp(op: ml.graph.OpCode) bool {
    return switch (op) {
        .constant, .fused_zero_tensor => true,
        else => false,
    };
}

fn materializePartitionParameters(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    value_device: []DeviceId,
    node_ids: []const NodeId,
    reachable: []const bool,
    device_id: DeviceId,
    rt_map: std.AutoHashMapUnmanaged(NodeId, CT),
    stats: ?*PartitionExecutor.ExecutionStats,
) !void {
    for (node_ids) |node_id| {
        const i: usize = @intCast(node_id);
        if (i >= reachable.len or !reachable[i]) continue;
        if (values[i] != null) continue;
        if (rt_map.contains(node_id)) continue;
        const node = graph.node(node_id);
        if (node.op != .parameter) continue;
        const materialized = try cb.getWeight(graph.parameterName(node));
        values[i] = materialized;
        value_device[i] = device_id;
        if (stats) |s| {
            s.descriptor_materializations += 1;
            if (isMetalResidentOrQuantizedDescriptor(cb, materialized)) {
                s.device_resident_outputs += 1;
                s.device_resident_parameter_outputs += 1;
            } else {
                s.host_materialized_outputs += 1;
                s.host_materialized_parameter_outputs += 1;
                if (traceMetalHostOutputsEnabled()) traceMetalHostOutput(graph, node_id, "parameter_materialization_host_output");
            }
        }
    }
}

fn materializePartitionConstants(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    value_device: []DeviceId,
    node_ids: []const NodeId,
    reachable: []const bool,
    device_id: DeviceId,
) !void {
    for (node_ids) |node_id| {
        const i: usize = @intCast(node_id);
        if (i >= reachable.len or !reachable[i]) continue;
        if (values[i] != null) continue;
        const node = graph.node(node_id);
        const materialized = switch (node.op) {
            .constant => |attrs| try executeRuntimeConstant(graph, cb, node.output_shape, attrs),
            .fused_zero_tensor => |attrs| try executeRuntimeZeroTensor(cb, attrs.rows, attrs.out_dim),
            else => null,
        } orelse continue;
        values[i] = materialized;
        value_device[i] = device_id;
    }
}

const ExpiredFreeStats = struct {
    count: usize = 0,
    bytes: u64 = 0,
};

fn freeExpiredInputs(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    value_device: []DeviceId,
    node_id: NodeId,
    device_id: DeviceId,
    last_use: []const u32,
    runtime_region_plan: ?RuntimeRegionPlan,
    rt_map: std.AutoHashMapUnmanaged(NodeId, CT),
    donated: std.AutoHashMapUnmanaged(NodeId, void),
    exec_ctx: PartitionExecutor.ExecutionContext,
) !ExpiredFreeStats {
    const n = graph.node(node_id);
    const node_index: usize = @intCast(node_id);
    var released = std.AutoHashMapUnmanaged(usize, void).empty;
    defer released.deinit(allocator);
    var stats = ExpiredFreeStats{};
    for (n.getInputs()) |input_id| {
        if (input_id == null_node or input_id >= values.len) continue;
        const input_index: usize = @intCast(input_id);
        if (last_use[input_index] != node_index) continue;
        if (runtime_region_plan) |plan| {
            if (plan.needsAttentionInputAfterNode(input_id, node_id)) continue;
        }
        if (rt_map.contains(input_id) and
            !donated.contains(input_id) and
            !ownedRuntimeTransferContains(exec_ctx, input_id)) continue;
        const ct = values[input_index] orelse continue;
        const input_bytes = outputByteLen(graph.node(input_id).output_shape) orelse 0;
        if (values[node_index]) |out_ct| {
            if (ct == out_ct and interpreter.canKeepAliasedOutput(n.op)) {
                values[input_index] = null;
                continue;
            }
        }
        const ct_key = @intFromPtr(ct);
        if (released.contains(ct_key)) {
            traceMetalValueLifetime("free_expired_alias_clear", node_id, input_id);
            values[input_index] = null;
            continue;
        }
        try released.put(allocator, ct_key, {});
        if (exec_ctx.mesh) |mesh| {
            const inp_dev = value_device[input_index];
            if (mesh.device(inp_dev)) |entry| {
                entry.backend.free(ct);
            } else {
                cb.free(ct);
            }
        } else if (value_device[input_index] == device_id) {
            cb.free(ct);
        } else {
            cb.free(ct);
        }
        traceMetalValueLifetime("free_expired_release", node_id, input_id);
        values[input_index] = null;
        stats.count += 1;
        stats.bytes += input_bytes;
    }
    return stats;
}

fn sweepExpiredValuesThroughNode(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    value_device: []DeviceId,
    node_ids: []const NodeId,
    boundary_pos: usize,
    boundary_node_id: NodeId,
    device_id: DeviceId,
    last_use: []const u32,
    runtime_region_plan: ?RuntimeRegionPlan,
    rt_map: std.AutoHashMapUnmanaged(NodeId, CT),
    donated: std.AutoHashMapUnmanaged(NodeId, void),
    exec_ctx: PartitionExecutor.ExecutionContext,
) !usize {
    var released = std.AutoHashMapUnmanaged(usize, void).empty;
    defer released.deinit(allocator);

    var freed_count: usize = 0;
    for (values, 0..) |maybe_ct, raw_id| {
        const ct = maybe_ct orelse continue;
        if (raw_id >= last_use.len) continue;
        const node_id: NodeId = @intCast(raw_id);
        const use_id = last_use[raw_id];
        if (use_id == std.math.maxInt(u32)) continue;
        if (valueReferencedAfterBoundary(graph, node_ids, boundary_pos, node_id)) continue;
        if (runtime_region_plan) |plan| {
            if (plan.needsAttentionInputAfterNode(node_id, boundary_node_id)) continue;
        }
        if (rt_map.contains(node_id) and
            !donated.contains(node_id) and
            !ownedRuntimeTransferContains(exec_ctx, node_id)) continue;

        var has_live_alias = false;
        for (values, 0..) |other_maybe, other_raw_id| {
            if (other_raw_id == raw_id) continue;
            const other_ct = other_maybe orelse continue;
            if (other_ct != ct) continue;
            if (other_raw_id >= last_use.len) {
                has_live_alias = true;
                break;
            }
            const other_id: NodeId = @intCast(other_raw_id);
            const other_use = last_use[other_raw_id];
            if (other_use == std.math.maxInt(u32) or valueReferencedAfterBoundary(graph, node_ids, boundary_pos, other_id)) {
                has_live_alias = true;
                break;
            }
            if (rt_map.contains(other_id) and
                !donated.contains(other_id) and
                !ownedRuntimeTransferContains(exec_ctx, other_id))
            {
                has_live_alias = true;
                break;
            }
        }

        values[raw_id] = null;
        traceMetalValueLifetime("sweep_clear", boundary_node_id, node_id);
        if (has_live_alias) continue;
        const ct_key = @intFromPtr(ct);
        if (released.contains(ct_key)) continue;
        try released.put(allocator, ct_key, {});
        if (exec_ctx.mesh) |mesh| {
            const value_dev = if (raw_id < value_device.len) value_device[raw_id] else device_id;
            if (mesh.device(value_dev)) |entry| {
                entry.backend.free(ct);
            } else {
                cb.free(ct);
            }
        } else {
            cb.free(ct);
        }
        traceMetalValueLifetime("sweep_release", boundary_node_id, node_id);
        freed_count += 1;
    }

    return freed_count;
}

fn promoteLiveValuesAcrossFrameBoundary(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    value_device: []DeviceId,
    node_ids: []const NodeId,
    boundary_pos: usize,
    device_id: DeviceId,
    rt_map: std.AutoHashMapUnmanaged(NodeId, CT),
    donated: std.AutoHashMapUnmanaged(NodeId, void),
    exec_ctx: PartitionExecutor.ExecutionContext,
) !usize {
    var old_values = std.ArrayListUnmanaged(CT).empty;
    defer old_values.deinit(allocator);

    var promoted_count: usize = 0;
    for (values, 0..) |maybe_ct, raw_id| {
        const ct = maybe_ct orelse continue;
        const node_id: NodeId = @intCast(raw_id);
        if (!valueReferencedAfterBoundary(graph, node_ids, boundary_pos, node_id)) continue;
        if (rt_map.contains(node_id) and
            !donated.contains(node_id) and
            !ownedRuntimeTransferContains(exec_ctx, node_id)) continue;

        const owned = (try metal_compute_mod.MetalCompute.cloneOutputTensorOwned(cb, ct)) orelse continue;
        errdefer cb.free(owned);
        try old_values.append(allocator, ct);
        values[raw_id] = owned;
        if (raw_id < value_device.len) value_device[raw_id] = device_id;
        traceMetalValueLifetime("promote_live", @intCast(node_ids[boundary_pos]), node_id);
        promoted_count += 1;
    }

    var released = std.AutoHashMapUnmanaged(usize, void).empty;
    defer released.deinit(allocator);
    for (old_values.items) |old_ct| {
        const key = @intFromPtr(old_ct);
        if (released.contains(key)) continue;
        var still_referenced = false;
        for (values) |maybe_ct| {
            if (maybe_ct == old_ct) {
                still_referenced = true;
                break;
            }
        }
        if (still_referenced) continue;
        try released.put(allocator, key, {});
        cb.free(old_ct);
        traceMetalValueLifetime("promote_old_release", @intCast(node_ids[boundary_pos]), null_node);
    }

    return promoted_count;
}

fn valueReferencedAfterBoundary(
    graph: *const Graph,
    node_ids: []const NodeId,
    boundary_pos: usize,
    value_id: NodeId,
) bool {
    for (graph.outputs.items) |output_id| {
        if (output_id == value_id) return true;
    }
    if (boundary_pos + 1 >= node_ids.len) return false;
    for (node_ids[boundary_pos + 1 ..]) |future_id| {
        const future = graph.node(future_id);
        for (future.getInputs()) |input_id| {
            if (input_id == value_id) return true;
        }
    }
    return false;
}

fn valueReferencedAtOrAfterPosition(
    graph: *const Graph,
    node_ids: []const NodeId,
    start_pos: usize,
    value_id: NodeId,
) bool {
    for (graph.outputs.items) |output_id| {
        if (output_id == value_id) return true;
    }
    if (start_pos >= node_ids.len) return false;
    for (node_ids[start_pos..]) |future_id| {
        const future = graph.node(future_id);
        for (future.getInputs()) |input_id| {
            if (input_id == value_id) return true;
        }
    }
    return false;
}

fn evalPartitionBoundaryOutputs(
    cb: *const ComputeBackend,
    values: []?CT,
    view: buffer_plan_mod.PartitionBufferView,
) !void {
    for (view.slots) |slot_view| {
        if (!slot_view.roles.output and !slot_view.roles.graph_output) continue;
        const index: usize = @intCast(slot_view.slot.node_id);
        if (index >= values.len) return error.InvalidBufferPlan;
        if (values[index]) |ct| try cb.evalTensor(ct);
    }
}

/// TERMITE_METAL_OUTPUT_LIFETIME_DEBUG=1 keeps the ORIGINAL (runtime-
/// recycled) source value of each copied graph output alive in this
/// registry so training extraction can compare it against the owned copy
/// after a full device drain. Decisive diagnostic for copy-captured-zeros:
/// source nonzero + copy zero => command-buffer ordering; both zero =>
/// the slot was never written (or written elsewhere). Debug-only — the
/// sources are leaked into this map until the caller clears it.
var metal_output_lifetime_debug_sources = std.AutoHashMapUnmanaged(NodeId, CT).empty;

pub fn metalOutputLifetimeDebugEnabled() bool {
    return platform.env.getenvBoolDefault("TERMITE_METAL_OUTPUT_LIFETIME_DEBUG", false);
}

/// Original (pre-copy) source value recorded for a graph-output node, if
/// the lifetime debug env is set and the copier would otherwise have
/// freed it. The CT stays owned by the registry.
pub fn metalOutputLifetimeDebugSource(node_id: NodeId) ?CT {
    return metal_output_lifetime_debug_sources.get(node_id);
}

/// Free all retained debug sources. Call after the extraction-time
/// comparison so a long run does not accumulate device buffers.
pub fn metalOutputLifetimeDebugClear(cb: *const ComputeBackend) void {
    var it = metal_output_lifetime_debug_sources.valueIterator();
    while (it.next()) |ct| cb.free(ct.*);
    metal_output_lifetime_debug_sources.clearRetainingCapacity();
}

/// Deep-copy graph-output values out of runtime-recycled device storage.
///
/// Planned Metal commands return views of runtime-owned buffers
/// (graph-plan slots reserved via `reserveGraphPlanSlots`, projection and
/// frame scratch, hidden-state pairs). The runtime reuses those buffers
/// on the next plan commit / frame begin — long before the caller reads
/// the graph outputs (training extraction runs after partition teardown
/// and, for gradients, around a device-resident optimizer step), so the
/// aliased values would read back stale or zeroed data. Copy each
/// graph-output value into a private device buffer its CT exclusively
/// owns so it survives slot recycling. Bounded work: only graph outputs
/// (e.g. a loss scalar plus per-parameter LoRA gradients) are copied,
/// on-device with no host roundtrip. Callers must have drained the
/// partition's frame first (submit + wait) so each copy — a synchronous
/// one-shot blit — is ordered after every device write to its source.
fn copyPartitionGraphOutputsToOwnedStorage(
    cb: *const ComputeBackend,
    values: []?CT,
    view: buffer_plan_mod.PartitionBufferView,
    rt_map: std.AutoHashMapUnmanaged(NodeId, CT),
    stats: ?*PartitionExecutor.ExecutionStats,
) !void {
    if (comptime !build_options.enable_metal) return;
    if (cb.kind() != .metal) return;
    for (view.slots) |slot_view| {
        if (!slot_view.roles.graph_output) continue;
        const index: usize = @intCast(slot_view.slot.node_id);
        if (index >= values.len) return error.InvalidBufferPlan;
        const ct = values[index] orelse continue;
        const owned_ct = (try metal_compute_mod.MetalCompute.cloneOutputTensorOwned(cb, ct)) orelse continue;
        values[index] = owned_ct;
        if (stats) |s| s.graph_output_owned_copies += 1;
        // Free the recyclable original unless the caller provided it
        // (runtime input) or another node's value slot still aliases the
        // same CT handle (metadata aliases share CTs across nodes).
        if (rt_map.contains(slot_view.slot.node_id)) continue;
        var aliased = false;
        for (values) |maybe| {
            if (maybe == ct) {
                aliased = true;
                break;
            }
        }
        if (!aliased) {
            if (metalOutputLifetimeDebugEnabled()) {
                // Keep the original alive so extraction can compare it
                // against the owned copy after a full device drain.
                if (metal_output_lifetime_debug_sources.getOrPut(std.heap.c_allocator, slot_view.slot.node_id)) |entry| {
                    if (entry.found_existing) cb.free(entry.value_ptr.*);
                    entry.value_ptr.* = ct;
                    continue;
                } else |_| {}
            }
            cb.free(ct);
        }
    }
}

/// Synchronize graph-output values produced by a Metal partition so host
/// extraction (which runs after the partition's frame completed) reads the
/// final device contents: materializes pending lazy products and refreshes
/// cached host mirrors. Cross-partition (`roles.output`) values stay
/// device-resident — downstream Metal consumers read device storage.
fn syncPartitionGraphOutputs(
    cb: *const ComputeBackend,
    values: []?CT,
    view: buffer_plan_mod.PartitionBufferView,
) !void {
    if (comptime !build_options.enable_metal) return;
    if (cb.kind() != .metal) return;
    for (view.slots) |slot_view| {
        if (!slot_view.roles.graph_output) continue;
        const index: usize = @intCast(slot_view.slot.node_id);
        if (index >= values.len) return error.InvalidBufferPlan;
        const ct = values[index] orelse continue;
        try metal_compute_mod.MetalCompute.syncOutputTensor(cb, ct);
    }
}

fn countPartitionBoundaryOutputs(view: buffer_plan_mod.PartitionBufferView) u64 {
    var count: u64 = 0;
    for (view.slots) |slot_view| {
        if (slot_view.roles.output or slot_view.roles.graph_output) count += 1;
    }
    return count;
}

fn ownedRuntimeTransferContains(
    exec_ctx: PartitionExecutor.ExecutionContext,
    node_id: NodeId,
) bool {
    const owned = exec_ctx.owned_runtime_transfers orelse return false;
    return owned.contains(node_id);
}

fn transferTensor(
    allocator: std.mem.Allocator,
    value: CT,
    from: *const ComputeBackend,
    to: *const ComputeBackend,
) !CT {
    const shape_i64 = try from.tensorShape(value, allocator);
    defer allocator.free(shape_i64);
    const shape_i32 = try tensorShapeI32(allocator, shape_i64);
    defer allocator.free(shape_i32);
    const f32_data = try from.toFloat32(value, allocator);
    defer allocator.free(f32_data);
    const transferred = try to.fromFloat32Shape(f32_data, shape_i32);
    errdefer to.free(transferred);
    if (try makeMetalDeviceResident(to, transferred)) |device_transferred| {
        to.free(transferred);
        return device_transferred;
    }
    return transferred;
}

fn transferTensorHostBacked(
    allocator: std.mem.Allocator,
    value: CT,
    from: *const ComputeBackend,
    to: *const ComputeBackend,
) !CT {
    const shape_i64 = try from.tensorShape(value, allocator);
    defer allocator.free(shape_i64);
    const shape_i32 = try tensorShapeI32(allocator, shape_i64);
    defer allocator.free(shape_i32);
    const f32_data = try from.toFloat32(value, allocator);
    defer allocator.free(f32_data);
    return to.fromFloat32Shape(f32_data, shape_i32);
}

fn tensorShapeI32(allocator: std.mem.Allocator, shape: []const i64) ![]i32 {
    const out = try allocator.alloc(i32, shape.len);
    errdefer allocator.free(out);
    for (shape, 0..) |dim, i| {
        out[i] = std.math.cast(i32, dim) orelse return error.UnsupportedShape;
    }
    return out;
}

const native_compute = @import("../ops/native_compute.zig");

fn deinitEmptyNativeWeightStore(weight_store: *native_compute.WeightStore, allocator: std.mem.Allocator) void {
    native_compute.deinitPrefetchQueue(weight_store);
    weight_store.resident_weights.deinit(allocator);
    weight_store.lazy_weights.deinit(allocator);
}

fn initEmptyMetalWeightStore(allocator: std.mem.Allocator) gpu_hosted_store_mod.WeightStore {
    return .{
        .allocator = allocator,
        .prefix = "",
        .lazy_weights = .empty,
    };
}

fn deinitEmptyMetalWeightStore(weight_store: *gpu_hosted_store_mod.WeightStore, allocator: std.mem.Allocator) void {
    metal_compute_mod.deinitPrefetchQueue(weight_store);
    weight_store.lazy_weights.deinit(allocator);
}

test "metal partition executor computes frame chunk boundary positions" {
    try std.testing.expectEqual(@as(?usize, 3), currentFrameChunkBoundaryPos(10, 0, 4, 0));
    try std.testing.expectEqual(@as(?usize, 3), currentFrameChunkBoundaryPos(10, 2, 4, 2));
    try std.testing.expectEqual(@as(?usize, 7), currentFrameChunkBoundaryPos(10, 4, 4, 0));
    try std.testing.expectEqual(@as(?usize, 9), currentFrameChunkBoundaryPos(10, 8, 4, 0));
    try std.testing.expectEqual(@as(?usize, null), currentFrameChunkBoundaryPos(10, 0, 0, 0));
}

test "metal partition executor consumes buffer plan and evaluates partition" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{4}));
    const out = try b.gelu(x);
    try g.markOutput(out);

    const caps = [_]partition_mod.Capability{
        .{ .backend = .metal, .priority = 10, .supports = &partition_mod.supportsAll },
    };
    var partition_plan = try partition_mod.partition(allocator, &g, &caps);
    defer partition_plan.deinit();
    var buffer_plan = try buffer_plan_mod.build(allocator, &g, &partition_plan, .{});
    defer buffer_plan.deinit();
    var partition_view = try buffer_plan.partitionView(allocator, &partition_plan, 0);
    defer partition_view.deinit(allocator);
    var graph_plan = try buildMetalGraphPlan(allocator, &buffer_plan, partition_view);
    defer graph_plan.deinit(allocator);
    try std.testing.expect(graph_plan.slots.len > 0);
    try std.testing.expect(graph_plan.slots.len <= max_graph_plan_slots);
    for (graph_plan.slots) |slot| {
        try std.testing.expect(slot.slot < max_graph_plan_slots);
        try std.testing.expect(slot.bytes >= 16);
    }

    var weight_store = native_compute.WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    defer deinitEmptyNativeWeightStore(&weight_store, allocator);
    var compute = native_compute.NativeCompute.init(allocator, &weight_store, null);
    var cb = compute.computeBackend();

    const count: usize = @intCast(g.nodeCount());
    const values = try allocator.alloc(?CT, count);
    defer allocator.free(values);
    @memset(values, null);
    const value_device = try allocator.alloc(DeviceId, count);
    defer allocator.free(value_device);
    @memset(value_device, 0);

    const input_data = [_]f32{ -1.0, 0.0, 1.0, 2.0 };
    const input_ct = try cb.fromFloat32Shape(&input_data, &.{4});
    defer cb.free(input_ct);
    values[@intCast(x)] = input_ct;

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);

    const partition_index = partition_plan.node_assignment[out];
    var exec = MetalPartitionExecutor.initBorrowed(allocator, &g, &cb);
    cb.resetDebugTimingStats();
    try exec.partitionExecutor().execute(values, value_device, partition_plan.partitions[partition_index].node_ids, 0, .{
        .allocator = allocator,
        .graph = &g,
        .backend = &cb,
        .options = .{
            .runtime_inputs = &.{.{ .node_id = x, .value = input_ct }},
        },
        .reachable = reachable,
        .last_use = last_use,
        .partition_plan = &partition_plan,
        .buffer_plan = &buffer_plan,
    });

    const raw = try cb.toFloat32(values[@intCast(out)].?, allocator);
    defer allocator.free(raw);
    defer cb.free(values[@intCast(out)].?);
    try std.testing.expect(raw[0] < 0.0);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), raw[1], 1e-6);
    try std.testing.expect(raw[2] > 0.8 and raw[2] < 0.9);
    try std.testing.expect(raw[3] > 1.9 and raw[3] < 2.0);
}

test "metal partition executor command path handles add softmax and reshape" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ 1, 4 }));
    const y = try b.parameter("y", ml.graph.Shape.init(.f32, &.{ 1, 4 }));
    const sum = try b.add(x, y);
    const probs = try b.softmax(sum);
    const out = try b.reshape(probs, ml.graph.Shape.init(.f32, &.{4}));
    try g.markOutput(out);

    const caps = [_]partition_mod.Capability{
        .{ .backend = .metal, .priority = 10, .supports = &partition_mod.supportsAll },
    };
    var partition_plan = try partition_mod.partition(allocator, &g, &caps);
    defer partition_plan.deinit();
    var buffer_plan = try buffer_plan_mod.build(allocator, &g, &partition_plan, .{});
    defer buffer_plan.deinit();

    var weight_store = native_compute.WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    defer deinitEmptyNativeWeightStore(&weight_store, allocator);
    var compute = native_compute.NativeCompute.init(allocator, &weight_store, null);
    var cb = compute.computeBackend();

    const count: usize = @intCast(g.nodeCount());
    const values = try allocator.alloc(?CT, count);
    defer allocator.free(values);
    @memset(values, null);
    const value_device = try allocator.alloc(DeviceId, count);
    defer allocator.free(value_device);
    @memset(value_device, 0);

    const x_ct = try cb.fromFloat32Shape(&.{ 1.0, 2.0, 3.0, 4.0 }, &.{ 1, 4 });
    defer cb.free(x_ct);
    const y_ct = try cb.fromFloat32Shape(&.{ 0.5, 0.5, 0.5, 0.5 }, &.{ 1, 4 });
    defer cb.free(y_ct);
    values[@intCast(x)] = x_ct;
    values[@intCast(y)] = y_ct;

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);

    const partition_index = partition_plan.node_assignment[out];
    var exec = MetalPartitionExecutor.initBorrowed(allocator, &g, &cb);
    try exec.partitionExecutor().execute(values, value_device, partition_plan.partitions[partition_index].node_ids, 0, .{
        .allocator = allocator,
        .graph = &g,
        .backend = &cb,
        .options = .{
            .runtime_inputs = &.{
                .{ .node_id = x, .value = x_ct },
                .{ .node_id = y, .value = y_ct },
            },
        },
        .reachable = reachable,
        .last_use = last_use,
        .partition_plan = &partition_plan,
        .buffer_plan = &buffer_plan,
    });

    const raw = try cb.toFloat32(values[@intCast(out)].?, allocator);
    defer allocator.free(raw);
    defer cb.free(values[@intCast(out)].?);
    try std.testing.expectEqual(@as(usize, 4), raw.len);
    var total: f32 = 0;
    for (raw) |v| total += v;
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), total, 1e-5);
    try std.testing.expect(raw[3] > raw[2] and raw[2] > raw[1] and raw[1] > raw[0]);
}

test "metal partition executor command path handles linear and norms" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);
    const hidden: usize = 16;

    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ 1, hidden }));
    const w = try b.parameter("w", ml.graph.Shape.init(.f32, &.{ hidden, hidden }));
    const bias = try b.parameter("bias", ml.graph.Shape.init(.f32, &.{hidden}));
    const gamma = try b.parameter("gamma", ml.graph.Shape.init(.f32, &.{hidden}));
    const beta = try b.parameter("beta", ml.graph.Shape.init(.f32, &.{hidden}));
    const rms_weight = try b.parameter("rms_weight", ml.graph.Shape.init(.f32, &.{hidden}));
    const lin = try b.linear(x, w, bias, 1, hidden, hidden);
    const ln = try b.layerNorm(lin, gamma, beta, hidden, 1e-5);
    const out = try b.rmsNorm(ln, rms_weight, hidden, 1e-5);
    try g.markOutput(out);

    const caps = [_]partition_mod.Capability{
        .{ .backend = .metal, .priority = 10, .supports = &partition_mod.supportsAll },
    };
    var partition_plan = try partition_mod.partition(allocator, &g, &caps);
    defer partition_plan.deinit();
    var buffer_plan = try buffer_plan_mod.build(allocator, &g, &partition_plan, .{});
    defer buffer_plan.deinit();

    var weight_store = native_compute.WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    defer deinitEmptyNativeWeightStore(&weight_store, allocator);
    var compute = native_compute.NativeCompute.init(allocator, &weight_store, null);
    var cb = compute.computeBackend();

    const count: usize = @intCast(g.nodeCount());
    const values = try allocator.alloc(?CT, count);
    defer allocator.free(values);
    @memset(values, null);
    const value_device = try allocator.alloc(DeviceId, count);
    defer allocator.free(value_device);
    @memset(value_device, 0);

    var x_data: [hidden]f32 = undefined;
    var w_data: [hidden * hidden]f32 = .{0} ** (hidden * hidden);
    var bias_data: [hidden]f32 = .{0} ** hidden;
    var gamma_data: [hidden]f32 = .{1} ** hidden;
    var beta_data: [hidden]f32 = .{0} ** hidden;
    var rms_weight_data: [hidden]f32 = .{1} ** hidden;
    for (&x_data, 0..) |*value, i| value.* = @floatFromInt(i + 1);
    for (0..hidden) |i| w_data[i * hidden + i] = 1.0;

    const x_ct = try cb.fromFloat32Shape(&x_data, &.{ 1, hidden });
    defer cb.free(x_ct);
    const w_ct = try cb.fromFloat32Shape(&w_data, &.{ hidden, hidden });
    defer cb.free(w_ct);
    const bias_ct = try cb.fromFloat32Shape(&bias_data, &.{hidden});
    defer cb.free(bias_ct);
    const gamma_ct = try cb.fromFloat32Shape(&gamma_data, &.{hidden});
    defer cb.free(gamma_ct);
    const beta_ct = try cb.fromFloat32Shape(&beta_data, &.{hidden});
    defer cb.free(beta_ct);
    const rms_weight_ct = try cb.fromFloat32Shape(&rms_weight_data, &.{hidden});
    defer cb.free(rms_weight_ct);
    values[@intCast(x)] = x_ct;
    values[@intCast(w)] = w_ct;
    values[@intCast(bias)] = bias_ct;
    values[@intCast(gamma)] = gamma_ct;
    values[@intCast(beta)] = beta_ct;
    values[@intCast(rms_weight)] = rms_weight_ct;

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);

    const partition_index = partition_plan.node_assignment[out];
    var exec = MetalPartitionExecutor.initBorrowed(allocator, &g, &cb);
    try exec.partitionExecutor().execute(values, value_device, partition_plan.partitions[partition_index].node_ids, 0, .{
        .allocator = allocator,
        .graph = &g,
        .backend = &cb,
        .options = .{
            .runtime_inputs = &.{
                .{ .node_id = x, .value = x_ct },
                .{ .node_id = w, .value = w_ct },
                .{ .node_id = bias, .value = bias_ct },
                .{ .node_id = gamma, .value = gamma_ct },
                .{ .node_id = beta, .value = beta_ct },
                .{ .node_id = rms_weight, .value = rms_weight_ct },
            },
        },
        .reachable = reachable,
        .last_use = last_use,
        .partition_plan = &partition_plan,
        .buffer_plan = &buffer_plan,
    });

    const raw = try cb.toFloat32(values[@intCast(out)].?, allocator);
    defer allocator.free(raw);
    defer cb.free(values[@intCast(out)].?);
    try std.testing.expectEqual(hidden, raw.len);
    try std.testing.expect(raw[0] < -1.5);
    try std.testing.expect(raw[hidden - 1] > 1.5);
    try std.testing.expect(raw[hidden - 1] > raw[0]);
}

test "metal partition executor command path runs linear and norms on metal backend" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime_mod.metalDeviceAvailable()) return error.SkipZigTest;

    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);
    const hidden: usize = 16;

    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ 1, hidden }));
    const w = try b.parameter("w", ml.graph.Shape.init(.f32, &.{ hidden, hidden }));
    const bias = try b.parameter("bias", ml.graph.Shape.init(.f32, &.{hidden}));
    const gamma = try b.parameter("gamma", ml.graph.Shape.init(.f32, &.{hidden}));
    const beta = try b.parameter("beta", ml.graph.Shape.init(.f32, &.{hidden}));
    const rms_weight = try b.parameter("rms_weight", ml.graph.Shape.init(.f32, &.{hidden}));
    const lin = try b.linear(x, w, bias, 1, hidden, hidden);
    const ln = try b.layerNorm(lin, gamma, beta, hidden, 1e-5);
    const out = try b.rmsNorm(ln, rms_weight, hidden, 1e-5);
    try g.markOutput(out);

    const caps = [_]partition_mod.Capability{
        .{ .backend = .metal, .priority = 10, .supports = &partition_mod.supportsAll },
    };
    var partition_plan = try partition_mod.partition(allocator, &g, &caps);
    defer partition_plan.deinit();
    var buffer_plan = try buffer_plan_mod.build(allocator, &g, &partition_plan, .{});
    defer buffer_plan.deinit();

    var weight_store = initEmptyMetalWeightStore(allocator);
    defer deinitEmptyMetalWeightStore(&weight_store, allocator);
    var metal_compute = try metal_compute_mod.MetalCompute.init(allocator, &weight_store, null);
    defer metal_compute.deinit();
    var cb = metal_compute.computeBackend();
    if (!cb.decoderRuntimeReady()) return error.SkipZigTest;

    const count: usize = @intCast(g.nodeCount());
    const values = try allocator.alloc(?CT, count);
    defer allocator.free(values);
    @memset(values, null);
    const value_device = try allocator.alloc(DeviceId, count);
    defer allocator.free(value_device);
    @memset(value_device, 0);

    var x_data: [hidden]f32 = undefined;
    var w_data: [hidden * hidden]f32 = .{0} ** (hidden * hidden);
    var bias_data: [hidden]f32 = .{0} ** hidden;
    var gamma_data: [hidden]f32 = .{1} ** hidden;
    var beta_data: [hidden]f32 = .{0} ** hidden;
    var rms_weight_data: [hidden]f32 = .{1} ** hidden;
    for (&x_data, 0..) |*value, i| value.* = @floatFromInt(i + 1);
    for (0..hidden) |i| w_data[i * hidden + i] = 1.0;

    const x_ct = try cb.fromFloat32Shape(&x_data, &.{ 1, hidden });
    defer cb.free(x_ct);
    const w_ct = try cb.fromFloat32Shape(&w_data, &.{ hidden, hidden });
    defer cb.free(w_ct);
    const bias_ct = try cb.fromFloat32Shape(&bias_data, &.{hidden});
    defer cb.free(bias_ct);
    const gamma_ct = try cb.fromFloat32Shape(&gamma_data, &.{hidden});
    defer cb.free(gamma_ct);
    const beta_ct = try cb.fromFloat32Shape(&beta_data, &.{hidden});
    defer cb.free(beta_ct);
    const rms_weight_ct = try cb.fromFloat32Shape(&rms_weight_data, &.{hidden});
    defer cb.free(rms_weight_ct);
    values[@intCast(x)] = x_ct;
    values[@intCast(w)] = w_ct;
    values[@intCast(bias)] = bias_ct;
    values[@intCast(gamma)] = gamma_ct;
    values[@intCast(beta)] = beta_ct;
    values[@intCast(rms_weight)] = rms_weight_ct;

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);

    var owned_runtime_transfers = std.AutoHashMapUnmanaged(NodeId, void).empty;
    defer owned_runtime_transfers.deinit(allocator);
    var exec = MetalPartitionExecutor.initBorrowed(allocator, &g, &cb);
    try exec.partitionExecutor().execute(values, value_device, partition_plan.partitions[0].node_ids, 0, .{
        .allocator = allocator,
        .graph = &g,
        .backend = &cb,
        .options = .{
            .runtime_inputs = &.{
                .{ .node_id = x, .value = x_ct },
                .{ .node_id = w, .value = w_ct },
                .{ .node_id = bias, .value = bias_ct },
                .{ .node_id = gamma, .value = gamma_ct },
                .{ .node_id = beta, .value = beta_ct },
                .{ .node_id = rms_weight, .value = rms_weight_ct },
            },
        },
        .reachable = reachable,
        .last_use = last_use,
        .partition_plan = &partition_plan,
        .buffer_plan = &buffer_plan,
        .owned_runtime_transfers = &owned_runtime_transfers,
    });

    const out_index: usize = @intCast(out);
    defer if (values[out_index]) |ct| cb.free(ct);
    const raw = try cb.toFloat32(values[out_index].?, allocator);
    defer allocator.free(raw);
    try std.testing.expectEqual(hidden, raw.len);
    try std.testing.expect(raw[0] < -1.5);
    try std.testing.expect(raw[raw.len - 1] > 1.5);
    try std.testing.expect(raw[raw.len - 1] > raw[0]);
}

test "metal partition executor eager multi op chain matches host" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime_mod.metalDeviceAvailable()) return error.SkipZigTest;

    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);
    const dim: usize = 8;

    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ 1, dim }));
    const w = try b.parameter("w", ml.graph.Shape.init(.f32, &.{ dim, dim }));
    const bias = try b.parameter("bias", ml.graph.Shape.init(.f32, &.{dim}));
    const lin = try b.linear(x, w, bias, 1, dim, dim);
    const act = try b.silu(lin);
    const sum = try b.add(lin, act);
    const out = try b.softmax(sum);
    try g.markOutput(out);

    const caps = [_]partition_mod.Capability{
        .{ .backend = .metal, .priority = 10, .supports = &partition_mod.supportsAll },
    };
    var partition_plan = try partition_mod.partition(allocator, &g, &caps);
    defer partition_plan.deinit();
    var buffer_plan = try buffer_plan_mod.build(allocator, &g, &partition_plan, .{});
    defer buffer_plan.deinit();

    var weight_store = initEmptyMetalWeightStore(allocator);
    defer deinitEmptyMetalWeightStore(&weight_store, allocator);
    var metal_compute = try metal_compute_mod.MetalCompute.init(allocator, &weight_store, null);
    defer metal_compute.deinit();
    var cb = metal_compute.computeBackend();
    if (!cb.decoderRuntimeReady()) return error.SkipZigTest;

    var native_weight_store = native_compute.WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    defer deinitEmptyNativeWeightStore(&native_weight_store, allocator);
    var native_compute_impl = native_compute.NativeCompute.init(allocator, &native_weight_store, null);
    var native_cb = native_compute_impl.computeBackend();
    var mesh = try device_mesh_mod.DeviceMesh.init(allocator, &.{
        .{ .id = 0, .backend = &native_cb, .kind = .native },
        .{ .id = 1, .backend = &cb, .kind = .metal },
    });
    defer mesh.deinit();

    const count: usize = @intCast(g.nodeCount());
    const values = try allocator.alloc(?CT, count);
    defer allocator.free(values);
    @memset(values, null);
    const value_device = try allocator.alloc(DeviceId, count);
    defer allocator.free(value_device);
    @memset(value_device, 0);

    const x_data = [_]f32{ -4.0, -2.0, -1.0, -0.25, 0.25, 1.0, 2.0, 4.0 };
    var w_data: [dim * dim]f32 = .{0} ** (dim * dim);
    const bias_data = [_]f32{ 0.5, -0.25, 0.125, -0.5, 0.25, 0.75, -0.125, 0.0 };
    for (0..dim) |i| w_data[i * dim + i] = 1.0;

    const x_ct = try native_cb.fromFloat32Shape(&x_data, &.{ 1, dim });
    defer native_cb.free(x_ct);
    const w_ct = try native_cb.fromFloat32Shape(&w_data, &.{ dim, dim });
    defer native_cb.free(w_ct);
    const bias_ct = try native_cb.fromFloat32Shape(&bias_data, &.{dim});
    defer native_cb.free(bias_ct);
    values[@intCast(x)] = x_ct;
    values[@intCast(w)] = w_ct;
    values[@intCast(bias)] = bias_ct;

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);

    const partition_index = partition_plan.node_assignment[out];
    var owned_runtime_transfers = std.AutoHashMapUnmanaged(NodeId, void).empty;
    defer owned_runtime_transfers.deinit(allocator);
    var exec_stats: PartitionExecutor.ExecutionStats = .{};
    var exec = MetalPartitionExecutor.initBorrowed(allocator, &g, &cb);
    try exec.partitionExecutor().execute(values, value_device, partition_plan.partitions[partition_index].node_ids, 1, .{
        .allocator = allocator,
        .graph = &g,
        .backend = &cb,
        .mesh = &mesh,
        .options = .{
            .runtime_inputs = &.{
                .{ .node_id = x, .value = x_ct },
                .{ .node_id = w, .value = w_ct },
                .{ .node_id = bias, .value = bias_ct },
            },
        },
        .reachable = reachable,
        .last_use = last_use,
        .partition_plan = &partition_plan,
        .buffer_plan = &buffer_plan,
        .owned_runtime_transfers = &owned_runtime_transfers,
        .materialize_boundary_outputs = false,
        .stats = &exec_stats,
    });

    const out_index: usize = @intCast(out);
    defer if (values[out_index]) |ct| cb.free(ct);
    const stats = cb.debugTimingSnapshot().provider;
    try std.testing.expectEqual(@as(u64, 0), stats.decoder_runtime_frame_begins);
    try std.testing.expectEqual(@as(u64, 0), stats.decoder_runtime_frame_submits);
    try std.testing.expect(!cb.decoderRuntimeHasActiveFrame());
    try std.testing.expect(exec_stats.runtime_input_transfers >= 3);
    try std.testing.expect(exec_stats.device_resident_transfers >= 1);
    try std.testing.expect(exec_stats.backend_command_dispatches >= 4);
    try std.testing.expectEqual(@as(u64, 0), exec_stats.interpreter_fallbacks);
    try std.testing.expectEqual(@as(u64, 0), exec_stats.boundary_output_materializations);

    var logits: [dim]f32 = undefined;
    for (&logits, 0..) |*value, i| {
        const lin_value = x_data[i] + bias_data[i];
        const silu = lin_value / (1.0 + @exp(-lin_value));
        value.* = lin_value + silu;
    }
    var max_logit = logits[0];
    for (logits[1..]) |value| max_logit = @max(max_logit, value);
    var denom: f32 = 0.0;
    for (logits) |value| denom += @exp(value - max_logit);
    var expected: [dim]f32 = undefined;
    for (&expected, logits) |*value, logit| value.* = @exp(logit - max_logit) / denom;

    const raw = try cb.toFloat32(values[out_index].?, allocator);
    defer allocator.free(raw);
    try std.testing.expectEqual(dim, raw.len);
    for (expected, raw) |exp, actual| {
        try expectApproxEqAbsOrRel(exp, actual, 1e-2, 0.0);
    }
}

test "metal partition executor fuses sibling no-bias linears into one pair command" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ 1, 3 }));
    const w_a = try b.parameter("w_a", ml.graph.Shape.init(.f32, &.{ 2, 3 }));
    const w_b = try b.parameter("w_b", ml.graph.Shape.init(.f32, &.{ 2, 3 }));
    const a = try b.linearNoBias(x, w_a, 1, 3, 2);
    const c = try b.linearNoBias(x, w_b, 1, 3, 2);
    const out = try b.add(a, c);
    try g.markOutput(out);

    const caps = [_]partition_mod.Capability{
        .{ .backend = .metal, .priority = 10, .supports = &partition_mod.supportsAll },
    };
    var partition_plan = try partition_mod.partition(allocator, &g, &caps);
    defer partition_plan.deinit();
    var buffer_plan = try buffer_plan_mod.build(allocator, &g, &partition_plan, .{});
    defer buffer_plan.deinit();

    var weight_store = native_compute.WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    defer deinitEmptyNativeWeightStore(&weight_store, allocator);
    var compute = native_compute.NativeCompute.init(allocator, &weight_store, null);
    var cb = compute.computeBackend();

    const count: usize = @intCast(g.nodeCount());
    const values = try allocator.alloc(?CT, count);
    defer allocator.free(values);
    @memset(values, null);
    const value_device = try allocator.alloc(DeviceId, count);
    defer allocator.free(value_device);
    @memset(value_device, 0);

    const x_data = [_]f32{ 1.0, 2.0, 3.0 };
    const w_a_data = [_]f32{
        1.0, 0.0, 0.0,
        0.0, 1.0, 0.0,
    };
    const w_b_data = [_]f32{
        0.0, 0.0, 1.0,
        1.0, 1.0, 1.0,
    };
    const x_ct = try cb.fromFloat32Shape(&x_data, &.{ 1, 3 });
    defer cb.free(x_ct);
    const w_a_ct = try cb.fromFloat32Shape(&w_a_data, &.{ 2, 3 });
    defer cb.free(w_a_ct);
    const w_b_ct = try cb.fromFloat32Shape(&w_b_data, &.{ 2, 3 });
    defer cb.free(w_b_ct);
    values[@intCast(x)] = x_ct;
    values[@intCast(w_a)] = w_a_ct;
    values[@intCast(w_b)] = w_b_ct;

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);

    const partition_index = partition_plan.node_assignment[out];
    var exec_stats: PartitionExecutor.ExecutionStats = .{};
    var exec = MetalPartitionExecutor.initBorrowed(allocator, &g, &cb);
    try exec.partitionExecutor().execute(values, value_device, partition_plan.partitions[partition_index].node_ids, 0, .{
        .allocator = allocator,
        .graph = &g,
        .backend = &cb,
        .options = .{
            .runtime_inputs = &.{
                .{ .node_id = x, .value = x_ct },
                .{ .node_id = w_a, .value = w_a_ct },
                .{ .node_id = w_b, .value = w_b_ct },
            },
        },
        .reachable = reachable,
        .last_use = last_use,
        .partition_plan = &partition_plan,
        .buffer_plan = &buffer_plan,
        .stats = &exec_stats,
    });

    const out_ct = values[@intCast(out)].?;
    defer cb.free(out_ct);
    const raw = try cb.toFloat32(out_ct, allocator);
    defer allocator.free(raw);
    try std.testing.expectEqualSlices(f32, &[_]f32{ 4.0, 8.0 }, raw);
    try std.testing.expectEqual(@as(u64, 2), exec_stats.backend_command_dispatches);
    try std.testing.expectEqual(@as(u64, 0), exec_stats.interpreter_fallbacks);
}

test "metal partition executor recognizes pre-norm gated ffn residual graph pattern" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const rows: usize = 2;
    const hidden: usize = 4;
    const intermediate: usize = 6;
    const residual = try b.parameter("residual", ml.graph.Shape.init(.f32, &.{ rows, hidden }));
    const norm_w = try b.parameter("norm_w", ml.graph.Shape.init(.f32, &.{hidden}));
    const gate_w = try b.parameter("gate_w", ml.graph.Shape.init(.f32, &.{ intermediate, hidden }));
    const up_w = try b.parameter("up_w", ml.graph.Shape.init(.f32, &.{ intermediate, hidden }));
    const down_w = try b.parameter("down_w", ml.graph.Shape.init(.f32, &.{ hidden, intermediate }));
    const post_w = try b.parameter("post_w", ml.graph.Shape.init(.f32, &.{hidden}));
    const normed = try b.rmsNorm(residual, norm_w, @intCast(hidden), 1e-5);
    const pair = try g.addNode(.{
        .op = .{ .fused_linear_no_bias_pair = .{
            .rows = @intCast(rows),
            .in_dim = @intCast(hidden),
            .out_dim = @intCast(intermediate),
        } },
        .output_shape = ml.graph.Shape.init(.f32, &.{ rows, intermediate }),
        .inputs = .{ normed, gate_w, up_w, null_node },
        .num_inputs = 3,
    });
    const activated = try b.gelu(pair);
    const pair_second = try g.addNode(.{
        .op = .{ .fused_to_float32 = {} },
        .output_shape = ml.graph.Shape.init(.f32, &.{ rows, intermediate }),
        .inputs = .{ pair, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const gated = try b.elemMultiply(activated, pair_second);
    const down = try b.linearNoBias(gated, down_w, @intCast(rows), @intCast(intermediate), @intCast(hidden));
    const post = try b.rmsNorm(down, post_w, @intCast(hidden), 1e-6);
    const out = try b.add(post, residual);
    try g.markOutput(out);

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);
    const node_ids = try allocator.alloc(NodeId, @intCast(g.nodeCount()));
    defer allocator.free(node_ids);
    for (node_ids, 0..) |*node_id, idx| node_id.* = @intCast(idx);
    const skipped = try allocator.alloc(bool, node_ids.len);
    defer allocator.free(skipped);
    @memset(skipped, false);

    const pattern = matchGatedFfnResidualPattern(&g, node_ids, @intCast(pair), reachable, skipped, last_use) orelse return error.ExpectedGatedFfnPattern;
    try std.testing.expectEqual(pair, pattern.pair_id);
    try std.testing.expectEqual(pair_second, pattern.pair_second_id);
    try std.testing.expectEqual(activated, pattern.activation_id);
    try std.testing.expectEqual(gated, pattern.multiply_id);
    try std.testing.expectEqual(down, pattern.down_id);
    try std.testing.expectEqual(post, pattern.post_down_norm_id.?);
    try std.testing.expectEqual(out, pattern.add_id);
    try std.testing.expectEqual(residual, pattern.residual_id);
    try std.testing.expectEqual(@as(usize, hidden), pattern.hidden_size);
    try std.testing.expectEqual(@as(usize, intermediate), pattern.intermediate_size);
    try std.testing.expectEqual(ops_mod.DecoderRuntimeActivationKind.gelu, pattern.activation);
}

test "metal partition executor recognizes attention output residual graph pattern" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const rows: usize = 3;
    const hidden: usize = 8;
    const heads: usize = 2;
    const kv_heads: usize = 1;
    const head_dim: usize = 4;
    const attn_dim: usize = heads * head_dim;
    const q = try b.parameter("q", ml.graph.Shape.init(.f32, &.{ rows, attn_dim }));
    const k = try b.parameter("k", ml.graph.Shape.init(.f32, &.{ rows, kv_heads * head_dim }));
    const v = try b.parameter("v", ml.graph.Shape.init(.f32, &.{ rows, kv_heads * head_dim }));
    const residual = try b.parameter("residual", ml.graph.Shape.init(.f32, &.{ rows, hidden }));
    const pre_w = try b.parameter("pre_w", ml.graph.Shape.init(.f32, &.{attn_dim}));
    const out_w = try b.parameter("out_w", ml.graph.Shape.init(.f32, &.{ hidden, attn_dim }));
    const post_w = try b.parameter("post_w", ml.graph.Shape.init(.f32, &.{hidden}));
    const attention = try g.addNode(.{
        .op = .{ .fused_gqa_causal_attention = .{
            .batch = 1,
            .seq_len = @intCast(rows),
            .num_heads = @intCast(heads),
            .num_kv_heads = @intCast(kv_heads),
            .head_dim = @intCast(head_dim),
        } },
        .output_shape = ml.graph.Shape.init(.f32, &.{ rows, attn_dim }),
        .inputs = .{ q, k, v, null_node },
        .num_inputs = 3,
    });
    const pre = try b.rmsNorm(attention, pre_w, @intCast(attn_dim), 1e-5);
    const projected = try b.linearNoBias(pre, out_w, @intCast(rows), @intCast(attn_dim), @intCast(hidden));
    const post = try b.rmsNorm(projected, post_w, @intCast(hidden), 1e-5);
    const out = try b.add(post, residual);
    try g.markOutput(out);

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);
    const node_ids = try allocator.alloc(NodeId, @intCast(g.nodeCount()));
    defer allocator.free(node_ids);
    for (node_ids, 0..) |*node_id, idx| node_id.* = @intCast(idx);
    const skipped = try allocator.alloc(bool, node_ids.len);
    defer allocator.free(skipped);
    @memset(skipped, false);

    const pattern = matchAttentionOutputResidualPattern(&g, node_ids, @intCast(attention), reachable, skipped, last_use) orelse return error.ExpectedAttentionOutputResidualPattern;
    try std.testing.expectEqual(attention, pattern.attention_id);
    try std.testing.expectEqual(pre, pattern.pre_linear_norm_id.?);
    try std.testing.expectEqual(projected, pattern.linear_id);
    try std.testing.expectEqual(post, pattern.post_linear_norm_id.?);
    try std.testing.expectEqual(out, pattern.add_id);
    try std.testing.expectEqual(residual, pattern.residual_id);
    try std.testing.expectEqual(@as(usize, rows), pattern.rows);
    try std.testing.expectEqual(@as(usize, attn_dim), pattern.attention_input_size);
    try std.testing.expectEqual(@as(usize, hidden), pattern.hidden_size);

    var plan = try buildRuntimeRegionPlan(allocator, &g, node_ids, @intCast(g.nodeCount()), reachable, last_use);
    defer plan.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), plan.region_count);
    switch (plan.regionAt(@intCast(attention), attention, node_ids)) {
        .attention_output_residual => |planned| {
            try std.testing.expectEqual(attention, planned.attention_id);
            try std.testing.expectEqual(projected, planned.linear_id);
            try std.testing.expectEqual(out, planned.add_id);
        },
        else => return error.ExpectedPlannedAttentionOutputResidualRegion,
    }
}

test "metal partition executor recognizes deberta ffn forward graph region with escaped activations" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const rows: usize = 2;
    const hidden: usize = 4;
    const intermediate: usize = 8;
    const x = try b.parameter("hidden", ml.graph.Shape.init(.f32, &.{ rows, hidden }));
    const w1 = try b.parameter("encoder.layer.0.intermediate.dense.weight", ml.graph.Shape.init(.f32, &.{ intermediate, hidden }));
    const b1 = try b.parameter("encoder.layer.0.intermediate.dense.bias", ml.graph.Shape.init(.f32, &.{intermediate}));
    const w2 = try b.parameter("encoder.layer.0.output.dense.weight", ml.graph.Shape.init(.f32, &.{ hidden, intermediate }));
    const b2 = try b.parameter("encoder.layer.0.output.dense.bias", ml.graph.Shape.init(.f32, &.{hidden}));

    const w1_t = try b.transpose(w1, &.{ 1, 0 });
    const first_dot = try b.matmul(x, w1_t);
    const ffn_inter = try b.add(first_dot, b1);
    const ffn_gelu = try b.geluExact(ffn_inter);
    const w2_t = try b.transpose(w2, &.{ 1, 0 });
    const output_dot = try b.matmul(ffn_gelu, w2_t);
    const out = try b.add(output_dot, b2);
    const escaped_inter = try b.add(ffn_inter, ffn_inter);
    const escaped_gelu = try b.add(ffn_gelu, ffn_gelu);
    try g.markOutput(out);
    try g.markOutput(escaped_inter);
    try g.markOutput(escaped_gelu);

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);
    const node_ids = try allocator.alloc(NodeId, @intCast(g.nodeCount()));
    defer allocator.free(node_ids);
    for (node_ids, 0..) |*node_id, idx| node_id.* = @intCast(idx);
    const skipped = try allocator.alloc(bool, node_ids.len);
    defer allocator.free(skipped);
    @memset(skipped, false);

    const pattern = matchDebertaFfnForwardPattern(&g, node_ids, @intCast(first_dot), reachable, skipped) orelse return error.ExpectedDebertaFfnForwardPattern;
    try std.testing.expectEqual(first_dot, pattern.first_dot_id);
    try std.testing.expectEqual(ffn_inter, pattern.first_add_id);
    try std.testing.expectEqual(ffn_gelu, pattern.gelu_id);
    try std.testing.expectEqual(output_dot, pattern.output_dot_id);
    try std.testing.expectEqual(out, pattern.output_add_id);
    try std.testing.expectEqual(w1, pattern.first_weight_id);
    try std.testing.expectEqual(b1, pattern.first_bias_id);
    try std.testing.expectEqual(w2, pattern.second_weight_id);
    try std.testing.expectEqual(b2, pattern.second_bias_id);
    try std.testing.expectEqual(@as(usize, rows), pattern.rows);
    try std.testing.expectEqual(@as(usize, hidden), pattern.hidden_size);
    try std.testing.expectEqual(@as(usize, intermediate), pattern.intermediate_size);
    try std.testing.expectEqual(@as(usize, hidden), pattern.output_size);
    try std.testing.expectEqual(ops_mod.DecoderRuntimeActivationKind.gelu_exact, pattern.activation);
    try std.testing.expect(!pattern.is_head_mlp);
}

test "metal partition executor recognizes lora-wrapped deberta ffn forward graph region" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const rows: usize = 2;
    const hidden: usize = 4;
    const intermediate: usize = 8;
    const rank: usize = 2;
    const x = try b.parameter("hidden", ml.graph.Shape.init(.f32, &.{ rows, hidden }));
    const w1 = try b.parameter("encoder.layer.0.intermediate.dense.weight", ml.graph.Shape.init(.f32, &.{ intermediate, hidden }));
    const b1 = try b.parameter("encoder.layer.0.intermediate.dense.bias", ml.graph.Shape.init(.f32, &.{intermediate}));
    const w1_lora_a = try b.parameter("encoder.layer.0.intermediate.dense.lora_A.weight", ml.graph.Shape.init(.f32, &.{ rank, hidden }));
    const w1_lora_b = try b.parameter("encoder.layer.0.intermediate.dense.lora_B.weight", ml.graph.Shape.init(.f32, &.{ intermediate, rank }));
    const w2 = try b.parameter("encoder.layer.0.output.dense.weight", ml.graph.Shape.init(.f32, &.{ hidden, intermediate }));
    const b2 = try b.parameter("encoder.layer.0.output.dense.bias", ml.graph.Shape.init(.f32, &.{hidden}));
    const w2_lora_a = try b.parameter("encoder.layer.0.output.dense.lora_A.weight", ml.graph.Shape.init(.f32, &.{ rank, intermediate }));
    const w2_lora_b = try b.parameter("encoder.layer.0.output.dense.lora_B.weight", ml.graph.Shape.init(.f32, &.{ hidden, rank }));
    const scale = try b.scalarConst(.f32, 2.0);

    const w1_t = try b.transpose(w1, &.{ 1, 0 });
    const first_dot = try b.matmul(x, w1_t);
    const first_base = try b.add(first_dot, b1);
    const w1_lora_a_t = try b.transpose(w1_lora_a, &.{ 1, 0 });
    const first_after_a = try b.matmul(x, w1_lora_a_t);
    const w1_lora_b_t = try b.transpose(w1_lora_b, &.{ 1, 0 });
    const first_after_b = try b.matmul(first_after_a, w1_lora_b_t);
    const first_scaled = try b.mul(first_after_b, scale);
    const first_add = try b.add(first_base, first_scaled);
    const duplicate_first_add = try b.add(first_base, first_scaled);
    const ffn_gelu = try b.geluExact(first_add);

    const w2_t = try b.transpose(w2, &.{ 1, 0 });
    const output_dot = try b.matmul(ffn_gelu, w2_t);
    const output_base = try b.add(output_dot, b2);
    const w2_lora_a_t = try b.transpose(w2_lora_a, &.{ 1, 0 });
    const second_after_a = try b.matmul(ffn_gelu, w2_lora_a_t);
    const w2_lora_b_t = try b.transpose(w2_lora_b, &.{ 1, 0 });
    const second_after_b = try b.matmul(second_after_a, w2_lora_b_t);
    const second_scaled = try b.mul(second_after_b, scale);
    const out = try b.add(output_base, second_scaled);
    try g.markOutput(out);
    try g.markOutput(duplicate_first_add);

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const node_ids = try allocator.alloc(NodeId, @intCast(g.nodeCount()));
    defer allocator.free(node_ids);
    for (node_ids, 0..) |*node_id, idx| node_id.* = @intCast(idx);
    const skipped = try allocator.alloc(bool, node_ids.len);
    defer allocator.free(skipped);
    @memset(skipped, false);

    const pattern = matchDebertaFfnForwardPattern(&g, node_ids, @intCast(first_dot), reachable, skipped) orelse return error.ExpectedDebertaFfnForwardPattern;
    try std.testing.expectEqual(first_dot, pattern.first_dot_id);
    try std.testing.expectEqual(first_base, pattern.first_base_add_id);
    try std.testing.expectEqual(first_add, pattern.first_add_id);
    try std.testing.expectEqual(ffn_gelu, pattern.gelu_id);
    try std.testing.expectEqual(output_dot, pattern.output_dot_id);
    try std.testing.expectEqual(output_base, pattern.output_base_add_id);
    try std.testing.expectEqual(out, pattern.output_add_id);
    try std.testing.expect(pattern.first_lora != null);
    try std.testing.expect(pattern.second_lora != null);
    try std.testing.expectEqual(@as(usize, hidden), pattern.output_size);
    try std.testing.expectEqual(ops_mod.DecoderRuntimeActivationKind.gelu_exact, pattern.activation);
    try std.testing.expect(!pattern.is_head_mlp);
}

test "metal partition executor recognizes gliner head relu mlp forward graph region" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const rows: usize = 3;
    const hidden: usize = 4;
    const intermediate: usize = 8;
    const output_dim: usize = 1;
    const x = try b.parameter("schema_hidden", ml.graph.Shape.init(.f32, &.{ rows, hidden }));
    const w1 = try b.parameter("classifier.0.weight", ml.graph.Shape.init(.f32, &.{ intermediate, hidden }));
    const b1 = try b.parameter("classifier.0.bias", ml.graph.Shape.init(.f32, &.{intermediate}));
    const w2 = try b.parameter("classifier.2.weight", ml.graph.Shape.init(.f32, &.{ output_dim, intermediate }));
    const b2 = try b.parameter("classifier.2.bias", ml.graph.Shape.init(.f32, &.{output_dim}));

    const w1_t = try b.transpose(w1, &.{ 1, 0 });
    const first_dot = try b.matmul(x, w1_t);
    const first_add = try b.add(first_dot, b1);
    const relu = try b.relu(first_add);
    const w2_t = try b.transpose(w2, &.{ 1, 0 });
    const output_dot = try b.matmul(relu, w2_t);
    const out = try b.add(output_dot, b2);
    try g.markOutput(out);

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const node_ids = try allocator.alloc(NodeId, @intCast(g.nodeCount()));
    defer allocator.free(node_ids);
    for (node_ids, 0..) |*node_id, idx| node_id.* = @intCast(idx);
    const skipped = try allocator.alloc(bool, node_ids.len);
    defer allocator.free(skipped);
    @memset(skipped, false);

    const pattern = matchHeadMlpForwardPattern(&g, node_ids, @intCast(first_dot), reachable, skipped) orelse return error.ExpectedHeadMlpForwardPattern;
    try std.testing.expectEqual(first_dot, pattern.first_dot_id);
    try std.testing.expectEqual(first_add, pattern.first_add_id);
    try std.testing.expectEqual(relu, pattern.gelu_id);
    try std.testing.expectEqual(output_dot, pattern.output_dot_id);
    try std.testing.expectEqual(out, pattern.output_add_id);
    try std.testing.expectEqual(w1, pattern.first_weight_id);
    try std.testing.expectEqual(b1, pattern.first_bias_id);
    try std.testing.expectEqual(w2, pattern.second_weight_id);
    try std.testing.expectEqual(b2, pattern.second_bias_id);
    try std.testing.expectEqual(@as(usize, rows), pattern.rows);
    try std.testing.expectEqual(@as(usize, hidden), pattern.hidden_size);
    try std.testing.expectEqual(@as(usize, intermediate), pattern.intermediate_size);
    try std.testing.expectEqual(@as(usize, output_dim), pattern.output_size);
    try std.testing.expectEqual(ops_mod.DecoderRuntimeActivationKind.relu, pattern.activation);
    try std.testing.expect(pattern.is_head_mlp);
}

test "metal partition executor recognizes gliner head decomposed relu mlp forward graph region" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const rows: usize = 3;
    const hidden: usize = 4;
    const intermediate: usize = 8;
    const output_dim: usize = 1;
    const x = try b.parameter("schema_hidden", ml.graph.Shape.init(.f32, &.{ rows, hidden }));
    const w1 = try b.parameter("classifier.0.weight", ml.graph.Shape.init(.f32, &.{ intermediate, hidden }));
    const b1 = try b.parameter("classifier.0.bias", ml.graph.Shape.init(.f32, &.{intermediate}));
    const w2 = try b.parameter("classifier.2.weight", ml.graph.Shape.init(.f32, &.{ output_dim, intermediate }));
    const b2 = try b.parameter("classifier.2.bias", ml.graph.Shape.init(.f32, &.{output_dim}));

    const w1_t = try b.transpose(w1, &.{ 1, 0 });
    const first_dot = try b.matmul(x, w1_t);
    const first_add = try b.add(first_dot, b1);
    const zero = try b.scalarConst(.f32, 0.0);
    const cmp = try g.addNode(.{
        .op = .{ .less_than = {} },
        .output_shape = g.node(first_add).output_shape,
        .inputs = .{ first_add, zero, null_node, null_node },
        .num_inputs = 2,
    });
    const relu = try g.addNode(.{
        .op = .{ .where_select = {} },
        .output_shape = g.node(first_add).output_shape,
        .inputs = .{ cmp, zero, first_add, null_node },
        .num_inputs = 3,
    });
    const w2_t = try b.transpose(w2, &.{ 1, 0 });
    const output_dot = try b.matmul(relu, w2_t);
    const out = try b.add(output_dot, b2);
    const cmp_probe = try b.add(cmp, zero);
    try g.markOutput(out);
    try g.markOutput(cmp_probe);

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const node_ids = try allocator.alloc(NodeId, @intCast(g.nodeCount()));
    defer allocator.free(node_ids);
    for (node_ids, 0..) |*node_id, idx| node_id.* = @intCast(idx);
    const skipped = try allocator.alloc(bool, node_ids.len);
    defer allocator.free(skipped);
    @memset(skipped, false);

    const pattern = matchHeadMlpForwardPattern(&g, node_ids, @intCast(first_dot), reachable, skipped) orelse return error.ExpectedHeadMlpForwardPattern;
    try std.testing.expectEqual(first_dot, pattern.first_dot_id);
    try std.testing.expectEqual(first_add, pattern.first_add_id);
    try std.testing.expectEqual(relu, pattern.gelu_id);
    try std.testing.expectEqual(null_node, pattern.activation_aux_id);
    try std.testing.expectEqual(output_dot, pattern.output_dot_id);
    try std.testing.expectEqual(out, pattern.output_add_id);
    try std.testing.expectEqual(w1, pattern.first_weight_id);
    try std.testing.expectEqual(w2, pattern.second_weight_id);
    try std.testing.expectEqual(@as(usize, rows), pattern.rows);
    try std.testing.expectEqual(@as(usize, hidden), pattern.hidden_size);
    try std.testing.expectEqual(@as(usize, intermediate), pattern.intermediate_size);
    try std.testing.expectEqual(@as(usize, output_dim), pattern.output_size);
    try std.testing.expectEqual(ops_mod.DecoderRuntimeActivationKind.relu, pattern.activation);
    try std.testing.expect(pattern.is_head_mlp);
}

test "metal partition executor selects gliner head relu mlp among multiple activation consumers" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const rows: usize = 3;
    const hidden: usize = 4;
    const intermediate: usize = 8;
    const output_dim: usize = 1;
    const x = try b.parameter("schema_hidden", ml.graph.Shape.init(.f32, &.{ rows, hidden }));
    const w1 = try b.parameter("classifier.0.weight", ml.graph.Shape.init(.f32, &.{ intermediate, hidden }));
    const b1 = try b.parameter("classifier.0.bias", ml.graph.Shape.init(.f32, &.{intermediate}));
    const probe_w2 = try b.parameter("classifier.probe.weight", ml.graph.Shape.init(.f32, &.{ output_dim, intermediate }));
    const probe_b2 = try b.parameter("classifier.probe.bias", ml.graph.Shape.init(.f32, &.{output_dim}));
    const w2 = try b.parameter("classifier.2.weight", ml.graph.Shape.init(.f32, &.{ output_dim, intermediate }));
    const b2 = try b.parameter("classifier.2.bias", ml.graph.Shape.init(.f32, &.{output_dim}));

    const w1_t = try b.transpose(w1, &.{ 1, 0 });
    const first_dot = try b.matmul(x, w1_t);
    const first_add = try b.add(first_dot, b1);
    const zero = try b.scalarConst(.f32, 0.0);
    const probe_cmp = try g.addNode(.{
        .op = .{ .less_than = {} },
        .output_shape = g.node(first_add).output_shape,
        .inputs = .{ first_add, zero, null_node, null_node },
        .num_inputs = 2,
    });
    const probe_relu = try g.addNode(.{
        .op = .{ .where_select = {} },
        .output_shape = g.node(first_add).output_shape,
        .inputs = .{ probe_cmp, zero, first_add, null_node },
        .num_inputs = 3,
    });
    const probe_w2_t = try b.transpose(probe_w2, &.{ 1, 0 });
    const probe_dot = try b.matmul(probe_relu, probe_w2_t);
    const probe_out = try b.add(probe_dot, probe_b2);
    const relu = try b.relu(first_add);
    const w2_t = try b.transpose(w2, &.{ 1, 0 });
    const output_dot = try b.matmul(relu, w2_t);
    const out = try b.add(output_dot, b2);
    try g.markOutput(probe_out);
    try g.markOutput(out);

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const node_ids = try allocator.alloc(NodeId, @intCast(g.nodeCount()));
    defer allocator.free(node_ids);
    for (node_ids, 0..) |*node_id, idx| node_id.* = @intCast(idx);
    const skipped = try allocator.alloc(bool, node_ids.len);
    defer allocator.free(skipped);
    @memset(skipped, false);

    const pattern = matchHeadMlpForwardPattern(&g, node_ids, @intCast(first_dot), reachable, skipped) orelse return error.ExpectedHeadMlpForwardPattern;
    try std.testing.expectEqual(first_dot, pattern.first_dot_id);
    try std.testing.expectEqual(first_add, pattern.first_add_id);
    try std.testing.expectEqual(relu, pattern.gelu_id);
    try std.testing.expectEqual(output_dot, pattern.output_dot_id);
    try std.testing.expectEqual(out, pattern.output_add_id);
    try std.testing.expectEqual(w1, pattern.first_weight_id);
    try std.testing.expectEqual(w2, pattern.second_weight_id);
    try std.testing.expectEqual(@as(usize, rows), pattern.rows);
    try std.testing.expectEqual(@as(usize, hidden), pattern.hidden_size);
    try std.testing.expectEqual(@as(usize, intermediate), pattern.intermediate_size);
    try std.testing.expectEqual(@as(usize, output_dim), pattern.output_size);
    try std.testing.expectEqual(ops_mod.DecoderRuntimeActivationKind.relu, pattern.activation);
    try std.testing.expect(pattern.is_head_mlp);
}

test "metal partition executor recognizes gemma qkv sibling linear graph region" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const rows: usize = 2;
    const hidden: usize = 8;
    const q_dim: usize = 16;
    const kv_dim: usize = 4;
    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ rows, hidden }));
    const q_w = try b.parameter("model.layers.0.self_attn.q_proj.weight", ml.graph.Shape.init(.f32, &.{ q_dim, hidden }));
    const k_w = try b.parameter("model.layers.0.self_attn.k_proj.weight", ml.graph.Shape.init(.f32, &.{ kv_dim, hidden }));
    const v_w = try b.parameter("model.layers.0.self_attn.v_proj.weight", ml.graph.Shape.init(.f32, &.{ kv_dim, hidden }));
    const q = try b.linearNoBias(x, q_w, @intCast(rows), @intCast(hidden), @intCast(q_dim));
    const k = try b.linearNoBias(x, k_w, @intCast(rows), @intCast(hidden), @intCast(kv_dim));
    const v = try b.linearNoBias(x, v_w, @intCast(rows), @intCast(hidden), @intCast(kv_dim));
    try g.markOutput(q);
    try g.markOutput(k);
    try g.markOutput(v);

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);
    const node_ids = try allocator.alloc(NodeId, @intCast(g.nodeCount()));
    defer allocator.free(node_ids);
    for (node_ids, 0..) |*node_id, idx| node_id.* = @intCast(idx);
    const skipped = try allocator.alloc(bool, node_ids.len);
    defer allocator.free(skipped);
    @memset(skipped, false);
    const values = try allocator.alloc(?CT, node_ids.len);
    defer allocator.free(values);
    @memset(values, null);

    const pattern = matchLinearNoBiasQkvPattern(&g, values, node_ids, @intCast(q), reachable, skipped) orelse return error.ExpectedQkvRegion;
    try std.testing.expectEqual(q, pattern.q_id);
    try std.testing.expectEqual(k, pattern.k_id);
    try std.testing.expectEqual(v, pattern.v_id);
    try std.testing.expectEqual(x, pattern.input_id);
    try std.testing.expectEqual(q_w, pattern.q_weight_id);
    try std.testing.expectEqual(k_w, pattern.k_weight_id);
    try std.testing.expectEqual(v_w, pattern.v_weight_id);
    try std.testing.expectEqual(@as(usize, rows), pattern.rows);
    try std.testing.expectEqual(@as(usize, hidden), pattern.in_dim);
    try std.testing.expectEqual(@as(usize, q_dim), pattern.q_out_dim);
    try std.testing.expectEqual(@as(usize, kv_dim), pattern.kv_out_dim);

    var plan = try buildRuntimeRegionPlan(allocator, &g, node_ids, @intCast(g.nodeCount()), reachable, last_use);
    defer plan.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), plan.region_count);
    switch (plan.regionAt(@intCast(q), q, node_ids)) {
        .linear_qkv => |planned| {
            try std.testing.expectEqual(q, planned.q_id);
            try std.testing.expectEqual(k, planned.k_id);
            try std.testing.expectEqual(v, planned.v_id);
        },
        else => return error.ExpectedPlannedQkvRegion,
    }
}

test "metal partition executor recognizes gemma q-only linear through attention layout path" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const rows: usize = 1;
    const hidden: usize = 1536;
    const heads: usize = 8;
    const kv_heads: usize = 1;
    const head_dim: usize = 256;
    const q_dim: usize = heads * head_dim;
    const kv_dim: usize = kv_heads * head_dim;
    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ rows, hidden }));
    const q_w = try b.parameter("model.layers.1.self_attn.q_proj.weight", ml.graph.Shape.init(.f32, &.{ q_dim, hidden }));
    const k = try b.parameter("k", ml.graph.Shape.init(.f32, &.{ rows, kv_dim }));
    const v = try b.parameter("v", ml.graph.Shape.init(.f32, &.{ rows, kv_dim }));
    const q_norm_w = try b.parameter("model.layers.1.self_attn.q_norm.weight", ml.graph.Shape.init(.f32, &.{head_dim}));
    const q_scale = try b.parameter("model.layers.1.self_attn.q_scale", ml.graph.Shape.init(.f32, &.{ rows, q_dim }));
    const cos = try b.tensorConst(&[_]f32{1.0}, ml.graph.Shape.init(.f32, &.{1}));
    const sin = try b.tensorConst(&[_]f32{0.0}, ml.graph.Shape.init(.f32, &.{1}));

    const q = try b.linearNoBias(x, q_w, @intCast(rows), @intCast(hidden), @intCast(q_dim));
    const q_heads = try b.reshape(q, ml.graph.Shape.init(.f32, &.{ @intCast(heads), @intCast(head_dim) }));
    const q_norm = try b.rmsNorm(q_heads, q_norm_w, @intCast(head_dim), 1e-5);
    const q_flat = try b.reshape(q_norm, ml.graph.Shape.init(.f32, &.{ rows, q_dim }));
    const q_scaled = try b.mul(q_flat, q_scale);
    const q_rope = try b.rope(q_scaled, cos, sin, @intCast(rows), @intCast(head_dim), @intCast(head_dim), 10000.0);
    const attention = try g.addNode(.{
        .op = .{ .fused_gqa_causal_attention = .{
            .batch = 1,
            .seq_len = @intCast(rows),
            .num_heads = @intCast(heads),
            .num_kv_heads = @intCast(kv_heads),
            .head_dim = @intCast(head_dim),
            .skip_kv_write = true,
        } },
        .output_shape = ml.graph.Shape.init(.f32, &.{ rows, q_dim }),
        .inputs = .{ q_rope, k, v, null_node },
        .num_inputs = 3,
    });
    try g.markOutput(attention);

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);
    const node_ids = try allocator.alloc(NodeId, @intCast(g.nodeCount()));
    defer allocator.free(node_ids);
    for (node_ids, 0..) |*node_id, idx| node_id.* = @intCast(idx);
    const skipped = try allocator.alloc(bool, node_ids.len);
    defer allocator.free(skipped);
    @memset(skipped, false);
    const values = try allocator.alloc(?CT, node_ids.len);
    defer allocator.free(values);
    @memset(values, null);

    const pattern = matchQLinearPattern(&g, values, node_ids, @intCast(q), reachable, skipped) orelse return error.ExpectedQLinearRegion;
    try std.testing.expectEqual(q, pattern.id);
    try std.testing.expectEqual(x, pattern.input_id);
    try std.testing.expectEqual(q_w, pattern.weight_id);
    try std.testing.expectEqual(@as(usize, rows), pattern.rows);
    try std.testing.expectEqual(@as(usize, hidden), pattern.in_dim);
    try std.testing.expectEqual(@as(usize, q_dim), pattern.out_dim);

    var plan = try buildRuntimeRegionPlan(allocator, &g, node_ids, @intCast(g.nodeCount()), reachable, last_use);
    defer plan.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), plan.region_count);
    switch (plan.regionAt(@intCast(q), q, node_ids)) {
        .q_linear => |planned| try std.testing.expectEqual(q, planned.id),
        else => return error.ExpectedPlannedQLinearRegion,
    }
}

test "metal partition executor recognizes raw transposed linear dot graph region" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const rows: usize = 2;
    const in_dim: usize = 3;
    const out_dim: usize = 4;
    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ rows, in_dim }));
    const weight = try b.parameter("encoder.layer.0.attention.self.query_proj.weight", ml.graph.Shape.init(.f32, &.{ out_dim, in_dim }));
    const weight_t = try b.transpose(weight, &.{ 1, 0 });
    const dot = try g.addNode(.{
        .op = .{ .dot_general = .{
            .lhs_contracting = .{ 1, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_contracting = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
            .lhs_batch = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_batch = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
            .num_contracting = 1,
            .num_batch = 0,
        } },
        .output_shape = ml.graph.Shape.init(.f32, &.{ rows, out_dim }),
        .inputs = .{ x, weight_t, null_node, null_node },
        .num_inputs = 2,
    });
    try g.markOutput(dot);

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);
    const node_ids = try allocator.alloc(NodeId, @intCast(g.nodeCount()));
    defer allocator.free(node_ids);
    for (node_ids, 0..) |*node_id, idx| node_id.* = @intCast(idx);

    var plan = try buildRuntimeRegionPlan(allocator, &g, node_ids, @intCast(g.nodeCount()), reachable, last_use);
    defer plan.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), plan.region_count);
    switch (plan.regionAt(@intCast(dot), dot, node_ids)) {
        .raw_linear_dot => |pattern| {
            try std.testing.expectEqual(dot, pattern.id);
            try std.testing.expectEqual(x, pattern.input_id);
            try std.testing.expectEqual(weight_t, pattern.transpose_id);
            try std.testing.expectEqual(weight, pattern.weight_id);
            try std.testing.expectEqual(@as(usize, rows), pattern.rows);
            try std.testing.expectEqual(@as(usize, in_dim), pattern.in_dim);
            try std.testing.expectEqual(@as(usize, out_dim), pattern.out_dim);
        },
        else => return error.ExpectedRawLinearDotRegion,
    }
}

test "metal partition executor recognizes raw transposed linear dot plus bias graph region" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const rows: usize = 2;
    const in_dim: usize = 3;
    const out_dim: usize = 4;
    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ rows, in_dim }));
    const weight = try b.parameter("encoder.layer.0.attention.self.query_proj.weight", ml.graph.Shape.init(.f32, &.{ out_dim, in_dim }));
    const bias = try b.parameter("encoder.layer.0.attention.self.query_proj.bias", ml.graph.Shape.init(.f32, &.{out_dim}));
    const weight_t = try b.transpose(weight, &.{ 1, 0 });
    const dot = try g.addNode(.{
        .op = .{ .dot_general = .{
            .lhs_contracting = .{ 1, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_contracting = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
            .lhs_batch = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_batch = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
            .num_contracting = 1,
            .num_batch = 0,
        } },
        .output_shape = ml.graph.Shape.init(.f32, &.{ rows, out_dim }),
        .inputs = .{ x, weight_t, null_node, null_node },
        .num_inputs = 2,
    });
    const biased = try b.add(dot, bias);
    try g.markOutput(biased);

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);
    const node_ids = try allocator.alloc(NodeId, @intCast(g.nodeCount()));
    defer allocator.free(node_ids);
    for (node_ids, 0..) |*node_id, idx| node_id.* = @intCast(idx);

    var plan = try buildRuntimeRegionPlan(allocator, &g, node_ids, @intCast(g.nodeCount()), reachable, last_use);
    defer plan.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), plan.region_count);
    switch (plan.regionAt(@intCast(dot), dot, node_ids)) {
        .raw_linear_bias => |pattern| {
            try std.testing.expectEqual(dot, pattern.dot.id);
            try std.testing.expectEqual(biased, pattern.add_id);
            try std.testing.expectEqual(bias, pattern.bias_id);
            try std.testing.expectEqual(x, pattern.dot.input_id);
            try std.testing.expectEqual(weight_t, pattern.dot.transpose_id);
            try std.testing.expectEqual(weight, pattern.dot.weight_id);
            try std.testing.expectEqual(@as(usize, rows), pattern.dot.rows);
            try std.testing.expectEqual(@as(usize, in_dim), pattern.dot.in_dim);
            try std.testing.expectEqual(@as(usize, out_dim), pattern.dot.out_dim);
        },
        else => return error.ExpectedRawLinearBiasRegion,
    }
    try std.testing.expectEqual(RuntimeRegion{ .none = {} }, plan.regionAt(@intCast(biased), biased, node_ids));
}

test "metal partition executor transposed linear dot with multi-use transpose matches host reference" {
    // Regression test for the GLiNER2 training-graph-executor divergence at
    // the relative-position projection: dot_general(x[127,K], transpose(W))
    // where the transpose is ALSO consumed outside the dot (the autodiff
    // backward graph transposes it again), so it is materialized rather than
    // deferred and the raw_linear_dot runtime region declines. The dot must
    // then execute through the interpreter-equivalent dot_general command —
    // the dynamic-linear-slot shortcut diverged here. Uses 127 rows and a
    // square, non-symmetric weight: an orientation (X@W vs X@Wᵀ) or
    // row-stride bug is invisible to shape checks in this configuration.
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime_mod.metalDeviceAvailable()) return error.SkipZigTest;

    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);
    const rows: usize = 127;
    const dim: usize = 192; // >=128 so the reduce-kernel dispatch path is used

    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ rows, dim }));
    const w = try b.parameter("encoder.layer.0.attention.self.query_proj.weight", ml.graph.Shape.init(.f32, &.{ dim, dim }));
    // Device-resident producer for the dot's lhs (mirrors the gather output).
    const pre = try b.add(x, x);
    const wt = try b.transpose(w, &.{ 1, 0 });
    const dot = try b.matmul(pre, wt);
    // Second consumer keeps the transpose from being deferred/region-fused,
    // mirroring the training graph where backward nodes also consume it.
    const wt_escape = try b.add(wt, wt);
    try g.markOutput(dot);
    try g.markOutput(wt_escape);

    const caps = [_]partition_mod.Capability{
        .{ .backend = .metal, .priority = 10, .supports = &partition_mod.supportsAll },
    };
    var partition_plan = try partition_mod.partition(allocator, &g, &caps);
    defer partition_plan.deinit();
    var buffer_plan = try buffer_plan_mod.build(allocator, &g, &partition_plan, .{});
    defer buffer_plan.deinit();

    var weight_store = initEmptyMetalWeightStore(allocator);
    defer deinitEmptyMetalWeightStore(&weight_store, allocator);
    var metal_compute = try metal_compute_mod.MetalCompute.init(allocator, &weight_store, null);
    defer metal_compute.deinit();
    var cb = metal_compute.computeBackend();
    if (!cb.decoderRuntimeReady()) return error.SkipZigTest;

    const count: usize = @intCast(g.nodeCount());
    const values = try allocator.alloc(?CT, count);
    defer allocator.free(values);
    @memset(values, null);
    const value_device = try allocator.alloc(DeviceId, count);
    defer allocator.free(value_device);
    @memset(value_device, 0);

    const x_data = try allocator.alloc(f32, rows * dim);
    defer allocator.free(x_data);
    const w_data = try allocator.alloc(f32, dim * dim);
    defer allocator.free(w_data);
    var lcg: u32 = 0x12345678;
    for (x_data) |*value| {
        lcg = lcg *% 1664525 +% 1013904223;
        value.* = @as(f32, @floatFromInt((lcg >> 9) % 1024)) / 1024.0 - 0.5;
    }
    for (w_data) |*value| {
        lcg = lcg *% 1664525 +% 1013904223;
        value.* = @as(f32, @floatFromInt((lcg >> 9) % 1024)) / 1024.0 - 0.5;
    }

    const x_ct = try cb.fromFloat32Shape(x_data, &.{ @intCast(rows), @intCast(dim) });
    defer cb.free(x_ct);
    const w_ct = try cb.fromFloat32Shape(w_data, &.{ @intCast(dim), @intCast(dim) });
    defer cb.free(w_ct);
    values[@intCast(x)] = x_ct;
    values[@intCast(w)] = w_ct;

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);

    var owned_runtime_transfers = std.AutoHashMapUnmanaged(NodeId, void).empty;
    defer owned_runtime_transfers.deinit(allocator);
    var exec = MetalPartitionExecutor.initBorrowed(allocator, &g, &cb);
    const partition_index = partition_plan.node_assignment[dot];
    try exec.partitionExecutor().execute(values, value_device, partition_plan.partitions[partition_index].node_ids, 0, .{
        .allocator = allocator,
        .graph = &g,
        .backend = &cb,
        .options = .{
            .runtime_inputs = &.{
                .{ .node_id = x, .value = x_ct },
                .{ .node_id = w, .value = w_ct },
            },
        },
        .reachable = reachable,
        .last_use = last_use,
        .partition_plan = &partition_plan,
        .buffer_plan = &buffer_plan,
        .owned_runtime_transfers = &owned_runtime_transfers,
    });

    const dot_index: usize = @intCast(dot);
    const escape_index: usize = @intCast(wt_escape);
    defer if (values[dot_index]) |ct| cb.free(ct);
    defer if (values[escape_index]) |ct| cb.free(ct);
    const raw = try cb.toFloat32(values[dot_index].?, allocator);
    defer allocator.free(raw);
    try std.testing.expectEqual(rows * dim, raw.len);

    // Host reference: dot(pre, Wᵀ) = (2x) @ Wᵀ.
    var max_abs_diff: f32 = 0;
    for (0..rows) |r| {
        for (0..dim) |c| {
            var expected: f64 = 0;
            for (0..dim) |k| {
                expected += @as(f64, 2.0 * x_data[r * dim + k]) * @as(f64, w_data[c * dim + k]);
            }
            const got = raw[r * dim + c];
            const diff = @abs(got - @as(f32, @floatCast(expected)));
            if (diff > max_abs_diff) max_abs_diff = diff;
            const tolerance = 1e-3 + 1e-3 * @abs(@as(f32, @floatCast(expected)));
            if (diff > tolerance) {
                std.debug.print(
                    "transposed_linear_dot_mismatch: row={d} col={d} got={d:.6} expected={d:.6}\n",
                    .{ r, c, got, expected },
                );
                return error.TransposedLinearDotMismatch;
            }
        }
    }
}

test "metal partition executor defers scalar scale mul only for single add consumer" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const shape = ml.graph.Shape.init(.f32, &.{ 2, 3 });
    const x = try b.parameter("x", shape);
    const y = try b.parameter("y", shape);
    const z = try b.parameter("z", shape);
    const scale = try b.scalarConst(.f32, 0.5);
    const scaled = try b.mul(x, scale);
    const first = try b.add(scaled, y);
    const second = try b.add(scaled, z);
    try g.markOutput(first);
    try g.markOutput(second);

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);

    try std.testing.expectEqual(@as(usize, 2), reachableUseCount(&g, scaled, reachable, 2));
    try std.testing.expect(!shouldDeferScaleMulForAdd(&g, scaled, reachable, last_use, null));
}

test "metal partition executor defers two independent scalar scale mul add inputs" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const shape = ml.graph.Shape.init(.f32, &.{ 2, 3 });
    const x = try b.parameter("x", shape);
    const y = try b.parameter("y", shape);
    const scale = try b.scalarConst(.f32, 0.5);
    const lhs_scaled = try b.mul(x, scale);
    const rhs_scaled = try b.mul(y, scale);
    const out = try b.add(lhs_scaled, rhs_scaled);
    try g.markOutput(out);

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);

    try std.testing.expectEqual(@as(usize, 1), reachableUseCount(&g, lhs_scaled, reachable, 2));
    try std.testing.expectEqual(@as(usize, 1), reachableUseCount(&g, rhs_scaled, reachable, 2));
    try std.testing.expect(shouldDeferScaleMulForAdd(&g, lhs_scaled, reachable, last_use, null));
    try std.testing.expect(shouldDeferScaleMulForAdd(&g, rhs_scaled, reachable, last_use, null));
    try std.testing.expect(deferredScaleMul(&g, lhs_scaled, g.node(out).output_shape) != null);
    try std.testing.expect(deferredScaleMul(&g, rhs_scaled, g.node(out).output_shape) != null);
}

test "metal partition executor defers same shape multiply for multiply-add" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const shape = ml.graph.Shape.init(.f32, &.{ 2, 3 });
    const x = try b.parameter("x", shape);
    const y = try b.parameter("y", shape);
    const z = try b.parameter("z", shape);
    const multiplied = try b.mul(x, y);
    const out = try b.add(multiplied, z);
    try g.markOutput(out);

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);

    try std.testing.expectEqual(@as(usize, 1), reachableUseCount(&g, multiplied, reachable, 2));
    try std.testing.expect(deferredElementwiseMul(&g, multiplied, g.node(out).output_shape) != null);
    try std.testing.expect(shouldDeferElementwiseMulForAdd(&g, multiplied, reachable, last_use, null));
}

test "metal partition executor allows deferred multiply-add inside current frame chunk" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const shape = ml.graph.Shape.init(.f32, &.{ 2, 3 });
    const x = try b.parameter("x", shape);
    const y = try b.parameter("y", shape);
    const z = try b.parameter("z", shape);
    const multiplied = try b.mul(x, y);
    const out = try b.add(multiplied, z);
    try g.markOutput(out);

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);

    const node_ids = [_]NodeId{ x, y, z, multiplied, out };
    try std.testing.expect(deferredProducerConsumerStaysInFrameChunk(&node_ids, 3, multiplied, 4, 0, last_use));
    try std.testing.expect(shouldDeferElementwiseMulForAdd(&g, multiplied, reachable, last_use, null));
}

test "metal partition executor does not defer multiply-add when producer input dies before add" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const shape = ml.graph.Shape.init(.f32, &.{ 2, 3 });
    const x = try b.parameter("x", shape);
    const y = try b.parameter("y", shape);
    const z = try b.parameter("z", shape);
    const residual = try b.parameter("residual", shape);
    const multiplied = try b.mul(x, y);
    const intervening_consumer = try b.add(x, z);
    const out = try b.add(multiplied, residual);
    try g.markOutput(intervening_consumer);
    try g.markOutput(out);

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);

    try std.testing.expectEqual(@as(u32, @intCast(intervening_consumer)), last_use[@intCast(x)]);
    try std.testing.expect(!deferredProducerInputsLiveUntilConsumer(&g, multiplied, out, last_use));
    try std.testing.expect(!shouldDeferElementwiseMulForAdd(&g, multiplied, reachable, last_use, null));
}

test "metal partition executor defers multiply-add when producer inputs live through add" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const shape = ml.graph.Shape.init(.f32, &.{ 2, 3 });
    const x = try b.parameter("x", shape);
    const y = try b.parameter("y", shape);
    const z = try b.parameter("z", shape);
    const residual = try b.parameter("residual", shape);
    const multiplied = try b.mul(x, y);
    const out = try b.add(multiplied, residual);
    const later_consumer = try b.add(x, z);
    try g.markOutput(out);
    try g.markOutput(later_consumer);

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);

    try std.testing.expectEqual(@as(u32, @intCast(later_consumer)), last_use[@intCast(x)]);
    try std.testing.expect(deferredProducerInputsLiveUntilConsumer(&g, multiplied, out, last_use));
    try std.testing.expect(shouldDeferElementwiseMulForAdd(&g, multiplied, reachable, last_use, null));
}

test "metal partition executor declines deferred multiply-add across frame chunk boundary" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const shape = ml.graph.Shape.init(.f32, &.{ 2, 3 });
    const x = try b.parameter("x", shape);
    const y = try b.parameter("y", shape);
    const z = try b.parameter("z", shape);
    const multiplied = try b.mul(x, y);
    const out = try b.add(multiplied, z);
    try g.markOutput(out);

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);

    const node_ids = [_]NodeId{ x, y, z, multiplied, out };
    try std.testing.expect(!deferredProducerConsumerStaysInFrameChunk(&node_ids, 3, multiplied, 1, 0, last_use));
    try std.testing.expect(!deferredProducerConsumerStaysInFrameChunk(&node_ids, 3, multiplied, 4, 3, last_use));
    try std.testing.expect(shouldDeferElementwiseMulForAdd(&g, multiplied, reachable, last_use, null));
}

test "metal partition executor accepts singleton operands for scalar binary result" {
    const scalar = Shape.scalar(.f32);
    const singleton_vector = Shape.init(.f32, &.{1});
    const singleton_matrix = Shape.init(.f32, &.{ 1, 1 });
    const non_singleton_vector = Shape.init(.f32, &.{2});

    try std.testing.expect(binaryBroadcastResultMatches(scalar, singleton_matrix, scalar));
    try std.testing.expect(binaryBroadcastResultMatches(singleton_vector, singleton_matrix, scalar));
    try std.testing.expect(!binaryBroadcastResultMatches(scalar, non_singleton_vector, scalar));
}

test "metal partition executor recognizes production grouped head dot candidate" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ 96, 768 }));
    const w = try b.parameter("classifier.weight", ml.graph.Shape.init(.f32, &.{ 2304, 768 }));
    const w_t = try b.transpose(w, &.{ 1, 0 });
    const dot = try b.matmul(x, w_t);
    try g.markOutput(dot);

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const skipped = try allocator.alloc(bool, @intCast(g.nodeCount()));
    defer allocator.free(skipped);
    @memset(skipped, false);
    const values = try allocator.alloc(?CT, @intCast(g.nodeCount()));
    defer allocator.free(values);
    @memset(values, null);
    var x_sentinel: u8 = 0;
    var w_sentinel: u8 = 0;
    values[@intCast(x)] = @as(CT, @ptrCast(&x_sentinel));
    values[@intCast(w)] = @as(CT, @ptrCast(&w_sentinel));

    const candidate = (try groupedHeadDotCandidate(&g, values, dot, reachable, skipped)) orelse return error.ExpectedGroupedHeadDotCandidate;
    try std.testing.expectEqual(dot, candidate.node_id);
    try std.testing.expectEqual(@as(usize, 96), candidate.m);
    try std.testing.expectEqual(@as(usize, 2304), candidate.n);
    try std.testing.expectEqual(@as(usize, 768), candidate.k);
    try std.testing.expectEqual(@as(u32, 1), candidate.rhs_contract_axis);
    try std.testing.expect(candidate.lhs == values[@intCast(x)].?);
    try std.testing.expect(candidate.rhs == values[@intCast(w)].?);
}

test "metal partition executor declines grouped head dot candidate for non-head parameter" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ 96, 768 }));
    const w = try b.parameter("encoder.weight", ml.graph.Shape.init(.f32, &.{ 2304, 768 }));
    const w_t = try b.transpose(w, &.{ 1, 0 });
    const dot = try b.matmul(x, w_t);
    try g.markOutput(dot);

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const skipped = try allocator.alloc(bool, @intCast(g.nodeCount()));
    defer allocator.free(skipped);
    @memset(skipped, false);
    const values = try allocator.alloc(?CT, @intCast(g.nodeCount()));
    defer allocator.free(values);
    @memset(values, null);
    var x_sentinel: u8 = 0;
    var w_sentinel: u8 = 0;
    values[@intCast(x)] = @as(CT, @ptrCast(&x_sentinel));
    values[@intCast(w)] = @as(CT, @ptrCast(&w_sentinel));

    try std.testing.expect((try groupedHeadDotCandidate(&g, values, dot, reachable, skipped)) == null);
}

test "metal partition executor defers both same shape multiply add inputs" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const shape = ml.graph.Shape.init(.f32, &.{ 2, 3 });
    const x = try b.parameter("x", shape);
    const y = try b.parameter("y", shape);
    const z = try b.parameter("z", shape);
    const w = try b.parameter("w", shape);
    const lhs = try b.mul(x, y);
    const rhs = try b.mul(z, w);
    const out = try b.add(lhs, rhs);
    try g.markOutput(out);

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);

    try std.testing.expectEqual(@as(usize, 1), reachableUseCount(&g, lhs, reachable, 2));
    try std.testing.expectEqual(@as(usize, 1), reachableUseCount(&g, rhs, reachable, 2));
    try std.testing.expect(deferredElementwiseMul(&g, lhs, g.node(out).output_shape) != null);
    try std.testing.expect(deferredElementwiseMul(&g, rhs, g.node(out).output_shape) != null);
    try std.testing.expect(shouldDeferElementwiseMulForAdd(&g, lhs, reachable, last_use, null));
    try std.testing.expect(shouldDeferElementwiseMulForAdd(&g, rhs, reachable, last_use, null));
}

test "metal partition executor only treats unmaterialized mul inputs as deferred" {
    // Regression test for the gliner2 LoRA backward add(mul, mul) failure:
    // one mul input of the add is materialized (multi-use, so the defer
    // heuristic never skipped it) while the other was skipped expecting the
    // add to fuse it. The consume side must treat the materialized mul as a
    // plain operand (its sources may already be freed) and only recompute the
    // genuinely deferred (null-valued) mul.
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const shape = ml.graph.Shape.init(.f32, &.{ 2, 3 });
    const x = try b.parameter("x", shape);
    const y = try b.parameter("y", shape);
    const z = try b.parameter("z", shape);
    const scale = try b.scalarConst(.f32, 0.5);
    const materialized_mul = try b.mul(x, scale);
    const deferred_mul = try b.mul(y, scale);
    const out = try b.add(materialized_mul, deferred_mul);
    // Second consumer keeps materialized_mul from being deferred.
    const escaped = try b.add(materialized_mul, z);
    try g.markOutput(out);
    try g.markOutput(escaped);

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);

    // Only the single-use mul is deferred by the heuristic.
    try std.testing.expect(!shouldDeferScaleMulForAdd(&g, materialized_mul, reachable, last_use, null));
    try std.testing.expect(shouldDeferScaleMulForAdd(&g, deferred_mul, reachable, last_use, null));

    // Both muls match the structural pattern, which is what previously made
    // the consume side treat the materialized one as deferred too.
    const out_shape = g.node(out).output_shape;
    try std.testing.expect(deferredScaleMul(&g, materialized_mul, out_shape) != null);
    try std.testing.expect(deferredScaleMul(&g, deferred_mul, out_shape) != null);

    const values = try allocator.alloc(?CT, @intCast(g.nodeCount()));
    defer allocator.free(values);
    @memset(values, null);
    var sentinel: u8 = 0;
    values[@intCast(materialized_mul)] = @as(CT, @ptrCast(&sentinel));

    // The materialized mul must be consumed as a plain operand...
    try std.testing.expect(pendingDeferredScaleMul(&g, values, materialized_mul, out_shape) == null);
    // ...while the skipped (null-valued) mul is still eligible for fusion.
    try std.testing.expect(pendingDeferredScaleMul(&g, values, deferred_mul, out_shape) != null);
    try std.testing.expect(pendingDeferredElementwiseMul(&g, values, materialized_mul, out_shape) == null);
}

test "metal partition executor does not defer nested same shape multiply add input" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const shape = ml.graph.Shape.init(.f32, &.{ 2, 3 });
    const x = try b.parameter("x", shape);
    const y = try b.parameter("y", shape);
    const z = try b.parameter("z", shape);
    const residual = try b.parameter("residual", shape);
    const inner = try b.mul(x, y);
    const outer = try b.mul(inner, z);
    const out = try b.add(outer, residual);
    try g.markOutput(out);

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);

    try std.testing.expect(deferredElementwiseMul(&g, inner, g.node(out).output_shape) != null);
    try std.testing.expect(deferredElementwiseMul(&g, outer, g.node(out).output_shape) == null);
    try std.testing.expect(!shouldDeferElementwiseMulForAdd(&g, outer, reachable, last_use, null));
}

test "metal partition executor does not defer transpose when it escapes non-linear-dot consumers" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const rows: usize = 2;
    const in_dim: usize = 3;
    const out_dim: usize = 4;
    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ rows, in_dim }));
    const weight = try b.parameter("encoder.layer.0.attention.self.query_proj.weight", ml.graph.Shape.init(.f32, &.{ out_dim, in_dim }));
    const residual = try b.parameter("residual", ml.graph.Shape.init(.f32, &.{ in_dim, out_dim }));
    const weight_t = try b.transpose(weight, &.{ 1, 0 });
    const dot = try g.addNode(.{
        .op = .{ .dot_general = .{
            .lhs_contracting = .{ 1, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_contracting = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
            .lhs_batch = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_batch = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
            .num_contracting = 1,
            .num_batch = 0,
        } },
        .output_shape = ml.graph.Shape.init(.f32, &.{ rows, out_dim }),
        .inputs = .{ x, weight_t, null_node, null_node },
        .num_inputs = 2,
    });
    const escaped = try b.add(weight_t, residual);
    try g.markOutput(dot);
    try g.markOutput(escaped);

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);

    try std.testing.expectEqual(@as(usize, 2), reachableUseCount(&g, weight_t, reachable, 2));
    try std.testing.expect(!shouldDeferTransposeForLinearDot(&g, weight_t, reachable, last_use));
}

test "metal partition executor defers transpose shared by compatible linear dots" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const rows: usize = 2;
    const in_dim: usize = 3;
    const out_dim: usize = 4;
    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ rows, in_dim }));
    const y = try b.parameter("y", ml.graph.Shape.init(.f32, &.{ rows, in_dim }));
    const weight = try b.parameter("encoder.layer.0.attention.self.query_proj.weight", ml.graph.Shape.init(.f32, &.{ out_dim, in_dim }));
    const weight_t = try b.transpose(weight, &.{ 1, 0 });
    const first = try g.addNode(.{
        .op = .{ .dot_general = .{
            .lhs_contracting = .{ 1, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_contracting = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
            .lhs_batch = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_batch = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
            .num_contracting = 1,
            .num_batch = 0,
        } },
        .output_shape = ml.graph.Shape.init(.f32, &.{ rows, out_dim }),
        .inputs = .{ x, weight_t, null_node, null_node },
        .num_inputs = 2,
    });
    const second = try g.addNode(.{
        .op = .{ .dot_general = .{
            .lhs_contracting = .{ 1, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_contracting = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
            .lhs_batch = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_batch = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
            .num_contracting = 1,
            .num_batch = 0,
        } },
        .output_shape = ml.graph.Shape.init(.f32, &.{ rows, out_dim }),
        .inputs = .{ y, weight_t, null_node, null_node },
        .num_inputs = 2,
    });
    try g.markOutput(first);
    try g.markOutput(second);

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);

    try std.testing.expectEqual(@as(usize, 2), reachableUseCount(&g, weight_t, reachable, 2));
    try std.testing.expect(shouldDeferTransposeForLinearDot(&g, weight_t, reachable, last_use));
}

test "metal partition executor chunk-safe transpose defer is limited to weight parameters" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const rows: usize = 2;
    const in_dim: usize = 3;
    const out_dim: usize = 4;
    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ rows, in_dim }));
    const y = try b.parameter("y", ml.graph.Shape.init(.f32, &.{ rows, in_dim }));
    const weight = try b.parameter("encoder.layer.0.output.dense.weight", ml.graph.Shape.init(.f32, &.{ out_dim, in_dim }));
    const input_like = try b.parameter("runtime_input", ml.graph.Shape.init(.f32, &.{ out_dim, in_dim }));
    const weight_t = try b.transpose(weight, &.{ 1, 0 });
    const input_like_t = try b.transpose(input_like, &.{ 1, 0 });
    const first = try g.addNode(.{
        .op = .{ .dot_general = .{
            .lhs_contracting = .{ 1, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_contracting = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
            .lhs_batch = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_batch = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
            .num_contracting = 1,
            .num_batch = 0,
        } },
        .output_shape = ml.graph.Shape.init(.f32, &.{ rows, out_dim }),
        .inputs = .{ x, weight_t, null_node, null_node },
        .num_inputs = 2,
    });
    const second = try g.addNode(.{
        .op = .{ .dot_general = .{
            .lhs_contracting = .{ 1, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_contracting = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
            .lhs_batch = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
            .rhs_batch = .{ 0, 0, 0, 0, 0, 0, 0, 0 },
            .num_contracting = 1,
            .num_batch = 0,
        } },
        .output_shape = ml.graph.Shape.init(.f32, &.{ rows, out_dim }),
        .inputs = .{ y, input_like_t, null_node, null_node },
        .num_inputs = 2,
    });
    try g.markOutput(first);
    try g.markOutput(second);

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);

    try std.testing.expect(shouldDeferTransposeForLinearDot(&g, weight_t, reachable, last_use));
    try std.testing.expect(transposeSourceIsWeightParameter(&g, weight_t));
    try std.testing.expect(shouldDeferTransposeForLinearDot(&g, input_like_t, reachable, last_use));
    try std.testing.expect(!transposeSourceIsWeightParameter(&g, input_like_t));
}

test "metal partition executor rank adapter backward pre-skips safe internal transposes" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const rows: usize = 3;
    const in_dim: usize = 4;
    const rank: usize = 2;
    const out_dim: usize = 5;
    const input = try b.parameter("input", ml.graph.Shape.init(.f32, &.{ rows, in_dim }));
    const after_a = try b.parameter("after_a", ml.graph.Shape.init(.f32, &.{ rows, rank }));
    const output_grad = try b.parameter("output_grad", ml.graph.Shape.init(.f32, &.{ rows, out_dim }));
    const rhs = try b.parameter("adapter_b", ml.graph.Shape.init(.f32, &.{ rank, out_dim }));
    const rhs_t = try b.transpose(rhs, &.{ 1, 0 });
    const d_after_a = try b.matmul(output_grad, rhs_t);
    const input_t = try b.transpose(input, &.{ 1, 0 });
    const grad_a_dot = try b.matmul(input_t, d_after_a);
    const grad_a = try b.transpose(grad_a_dot, &.{ 1, 0 });
    const after_a_t = try b.transpose(after_a, &.{ 1, 0 });
    const grad_b_dot = try b.matmul(after_a_t, output_grad);
    const grad_b = try b.transpose(grad_b_dot, &.{ 1, 0 });
    try g.markOutput(d_after_a);
    try g.markOutput(grad_a);
    try g.markOutput(grad_b);

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const skipped = try allocator.alloc(bool, @intCast(g.nodeCount()));
    defer allocator.free(skipped);
    @memset(skipped, false);
    const pre_skipped = try allocator.alloc(bool, @intCast(g.nodeCount()));
    defer allocator.free(pre_skipped);
    @memset(pre_skipped, false);

    const pattern = RankAdapterBackwardPattern{
        .d_after_a_id = d_after_a,
        .grad_a_dot_id = grad_a_dot,
        .grad_a_id = grad_a,
        .grad_b_dot_id = grad_b_dot,
        .grad_b_id = grad_b,
        .input_transpose_id = input_t,
        .after_a_transpose_id = after_a_t,
        .rhs_transpose_id = rhs_t,
        .input_id = input,
        .after_a_id = after_a,
        .rhs_source_id = rhs,
        .rhs_uses_transpose_value = false,
        .output_grad_id = output_grad,
        .rows = rows,
        .in_dim = in_dim,
        .out_dim = out_dim,
        .rank = rank,
    };

    const stats = markRankAdapterBackwardPreSkipped(&g, reachable, skipped, pre_skipped, pattern);
    try std.testing.expectEqual(@as(usize, 3), stats.nodes);
    try std.testing.expectEqual(@as(usize, 3), stats.transposes);
    try std.testing.expectEqual(@as(usize, 0), stats.declined_external_consumer);
    try std.testing.expect(pre_skipped[@intCast(input_t)]);
    try std.testing.expect(pre_skipped[@intCast(after_a_t)]);
    try std.testing.expect(pre_skipped[@intCast(rhs_t)]);
}

test "metal partition executor rank adapter backward pre-skip declines external transpose consumer" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const rows: usize = 3;
    const in_dim: usize = 4;
    const rank: usize = 2;
    const out_dim: usize = 5;
    const input = try b.parameter("input", ml.graph.Shape.init(.f32, &.{ rows, in_dim }));
    const after_a = try b.parameter("after_a", ml.graph.Shape.init(.f32, &.{ rows, rank }));
    const output_grad = try b.parameter("output_grad", ml.graph.Shape.init(.f32, &.{ rows, out_dim }));
    const rhs = try b.parameter("adapter_b", ml.graph.Shape.init(.f32, &.{ rank, out_dim }));
    const rhs_t = try b.transpose(rhs, &.{ 1, 0 });
    const rhs_probe = try b.parameter("rhs_probe", ml.graph.Shape.init(.f32, &.{ out_dim, rank }));
    const escaped_rhs = try b.add(rhs_t, rhs_probe);
    const d_after_a = try b.matmul(output_grad, rhs_t);
    const input_t = try b.transpose(input, &.{ 1, 0 });
    const grad_a_dot = try b.matmul(input_t, d_after_a);
    const grad_a = try b.transpose(grad_a_dot, &.{ 1, 0 });
    const after_a_t = try b.transpose(after_a, &.{ 1, 0 });
    const grad_b_dot = try b.matmul(after_a_t, output_grad);
    const grad_b = try b.transpose(grad_b_dot, &.{ 1, 0 });
    try g.markOutput(d_after_a);
    try g.markOutput(grad_a);
    try g.markOutput(grad_b);
    try g.markOutput(escaped_rhs);

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const skipped = try allocator.alloc(bool, @intCast(g.nodeCount()));
    defer allocator.free(skipped);
    @memset(skipped, false);
    const pre_skipped = try allocator.alloc(bool, @intCast(g.nodeCount()));
    defer allocator.free(pre_skipped);
    @memset(pre_skipped, false);

    const pattern = RankAdapterBackwardPattern{
        .d_after_a_id = d_after_a,
        .grad_a_dot_id = grad_a_dot,
        .grad_a_id = grad_a,
        .grad_b_dot_id = grad_b_dot,
        .grad_b_id = grad_b,
        .input_transpose_id = input_t,
        .after_a_transpose_id = after_a_t,
        .rhs_transpose_id = rhs_t,
        .input_id = input,
        .after_a_id = after_a,
        .rhs_source_id = rhs,
        .rhs_uses_transpose_value = false,
        .output_grad_id = output_grad,
        .rows = rows,
        .in_dim = in_dim,
        .out_dim = out_dim,
        .rank = rank,
    };

    const stats = markRankAdapterBackwardPreSkipped(&g, reachable, skipped, pre_skipped, pattern);
    try std.testing.expectEqual(@as(usize, 2), stats.nodes);
    try std.testing.expectEqual(@as(usize, 2), stats.transposes);
    try std.testing.expectEqual(@as(usize, 1), stats.declined_external_consumer);
    try std.testing.expect(pre_skipped[@intCast(input_t)]);
    try std.testing.expect(pre_skipped[@intCast(after_a_t)]);
    try std.testing.expect(!pre_skipped[@intCast(rhs_t)]);
}

test "metal partition executor rank adapter backward keeps rhs transpose when region needs transpose value" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const rows: usize = 3;
    const in_dim: usize = 4;
    const rank: usize = 2;
    const out_dim: usize = 5;
    const input = try b.parameter("input", ml.graph.Shape.init(.f32, &.{ rows, in_dim }));
    const after_a = try b.parameter("after_a", ml.graph.Shape.init(.f32, &.{ rows, rank }));
    const output_grad = try b.parameter("output_grad", ml.graph.Shape.init(.f32, &.{ rows, out_dim }));
    const rhs = try b.parameter("adapter_b", ml.graph.Shape.init(.f32, &.{ rank, out_dim }));
    const rhs_t = try b.transpose(rhs, &.{ 1, 0 });
    const d_after_a = try b.matmul(output_grad, rhs_t);
    const input_t = try b.transpose(input, &.{ 1, 0 });
    const grad_a_dot = try b.matmul(input_t, d_after_a);
    const grad_a = try b.transpose(grad_a_dot, &.{ 1, 0 });
    const after_a_t = try b.transpose(after_a, &.{ 1, 0 });
    const grad_b_dot = try b.matmul(after_a_t, output_grad);
    const grad_b = try b.transpose(grad_b_dot, &.{ 1, 0 });
    try g.markOutput(d_after_a);
    try g.markOutput(grad_a);
    try g.markOutput(grad_b);

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const skipped = try allocator.alloc(bool, @intCast(g.nodeCount()));
    defer allocator.free(skipped);
    @memset(skipped, false);
    const pre_skipped = try allocator.alloc(bool, @intCast(g.nodeCount()));
    defer allocator.free(pre_skipped);
    @memset(pre_skipped, false);

    const pattern = RankAdapterBackwardPattern{
        .d_after_a_id = d_after_a,
        .grad_a_dot_id = grad_a_dot,
        .grad_a_id = grad_a,
        .grad_b_dot_id = grad_b_dot,
        .grad_b_id = grad_b,
        .input_transpose_id = input_t,
        .after_a_transpose_id = after_a_t,
        .rhs_transpose_id = rhs_t,
        .input_id = input,
        .after_a_id = after_a,
        .rhs_source_id = rhs,
        .rhs_uses_transpose_value = true,
        .output_grad_id = output_grad,
        .rows = rows,
        .in_dim = in_dim,
        .out_dim = out_dim,
        .rank = rank,
    };

    const stats = markRankAdapterBackwardPreSkipped(&g, reachable, skipped, pre_skipped, pattern);
    try std.testing.expectEqual(@as(usize, 2), stats.nodes);
    try std.testing.expectEqual(@as(usize, 2), stats.transposes);
    try std.testing.expectEqual(@as(usize, 0), stats.declined_external_consumer);
    try std.testing.expect(pre_skipped[@intCast(input_t)]);
    try std.testing.expect(pre_skipped[@intCast(after_a_t)]);
    try std.testing.expect(!pre_skipped[@intCast(rhs_t)]);
}

test "metal partition executor applies runtime region pre-skip mask before node execution" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ 2, 3 }));
    const x_t = try b.transpose(x, &.{ 1, 0 });
    const probe = try b.parameter("probe", ml.graph.Shape.init(.f32, &.{ 3, 2 }));
    const out = try b.add(x_t, probe);
    try g.markOutput(out);

    const pre_skipped = try allocator.alloc(bool, @intCast(g.nodeCount()));
    defer allocator.free(pre_skipped);
    @memset(pre_skipped, false);
    pre_skipped[@intCast(x_t)] = true;
    const skipped = try allocator.alloc(bool, @intCast(g.nodeCount()));
    defer allocator.free(skipped);
    @memset(skipped, false);
    const protected = try allocator.alloc(bool, @intCast(g.nodeCount()));
    defer allocator.free(protected);
    @memset(protected, false);
    var rt_map = std.AutoHashMapUnmanaged(NodeId, CT).empty;
    defer rt_map.deinit(allocator);
    var stats = PartitionExecutor.ExecutionStats{};

    const plan = RuntimeRegionPlan{ .pre_skipped_by_region = pre_skipped };
    applyRuntimeRegionPreSkippedNodes(&g, plan, skipped, protected, rt_map, &stats);

    try std.testing.expect(skipped[@intCast(x_t)]);
    try std.testing.expectEqual(@as(u64, 1), stats.runtime_region_pre_skipped_nodes);
    try std.testing.expectEqual(@as(u64, 1), stats.runtime_region_pre_skipped_transposes);
}

test "metal partition executor recognizes grouped qkv linear slice graph region" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const rows: usize = 2;
    const hidden: usize = 8;
    const q_dim: usize = 16;
    const kv_dim: usize = 4;
    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ rows, hidden }));
    const q_w = try b.parameter("model.layers.0.self_attn.q_proj.weight", ml.graph.Shape.init(.f32, &.{ q_dim, hidden }));
    const k_w = try b.parameter("model.layers.0.self_attn.k_proj.weight", ml.graph.Shape.init(.f32, &.{ kv_dim, hidden }));
    const v_w = try b.parameter("model.layers.0.self_attn.v_proj.weight", ml.graph.Shape.init(.f32, &.{ kv_dim, hidden }));
    const qk_w = try b.concat(q_w, k_w, 0);
    const qkv_w = try b.concat(qk_w, v_w, 0);
    const qkv = try b.linearNoBias(x, qkv_w, @intCast(rows), @intCast(hidden), @intCast(q_dim + kv_dim * 2));
    const q = try b.sliceLastDim(qkv, 0, @intCast(q_dim));
    const k = try b.sliceLastDim(qkv, @intCast(q_dim), @intCast(q_dim + kv_dim));
    const v = try b.sliceLastDim(qkv, @intCast(q_dim + kv_dim), @intCast(q_dim + kv_dim * 2));
    const kv = try b.add(k, v);
    const out = try b.add(q, kv);
    try g.markOutput(out);

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);
    const node_ids = try allocator.alloc(NodeId, @intCast(g.nodeCount()));
    defer allocator.free(node_ids);
    for (node_ids, 0..) |*node_id, idx| node_id.* = @intCast(idx);
    const skipped = try allocator.alloc(bool, node_ids.len);
    defer allocator.free(skipped);
    @memset(skipped, false);
    const values = try allocator.alloc(?CT, node_ids.len);
    defer allocator.free(values);
    @memset(values, null);

    const pattern = matchGroupedLinearQkvSlicePattern(&g, values, node_ids, @intCast(qkv), reachable, skipped) orelse return error.ExpectedGroupedQkvRegion;
    try std.testing.expectEqual(qkv, pattern.linear_id);
    try std.testing.expectEqual(q, pattern.q_slice_id);
    try std.testing.expectEqual(k, pattern.k_slice_id);
    try std.testing.expectEqual(v, pattern.v_slice_id);
    try std.testing.expectEqual(x, pattern.input_id);
    try std.testing.expectEqual(q_w, pattern.q_weight_id);
    try std.testing.expectEqual(k_w, pattern.k_weight_id);
    try std.testing.expectEqual(v_w, pattern.v_weight_id);
    try std.testing.expectEqual(@as(usize, q_dim), pattern.q_out_dim);
    try std.testing.expectEqual(@as(usize, kv_dim), pattern.kv_out_dim);

    var plan = try buildRuntimeRegionPlan(allocator, &g, node_ids, @intCast(g.nodeCount()), reachable, last_use);
    defer plan.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), plan.region_count);
    switch (plan.regionAt(@intCast(qkv), qkv, node_ids)) {
        .grouped_linear_qkv_slice => |planned| {
            try std.testing.expectEqual(qkv, planned.linear_id);
            try std.testing.expectEqual(q, planned.q_slice_id);
            try std.testing.expectEqual(v, planned.v_slice_id);
        },
        else => return error.ExpectedPlannedGroupedQkvRegion,
    }
}

test "metal partition executor recognizes gliner packed biased qkv slice graph region" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const rows: usize = 3;
    const hidden: usize = 8;
    const dim: usize = 8;
    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ rows, hidden }));
    const w = try b.parameter("count_embed.transformer.transformer.layers.0.self_attn.in_proj_weight", ml.graph.Shape.init(.f32, &.{ dim * 3, hidden }));
    const bias = try b.parameter("count_embed.transformer.transformer.layers.0.self_attn.in_proj_bias", ml.graph.Shape.init(.f32, &.{dim * 3}));
    const qkv = try b.linear(x, w, bias, @intCast(rows), @intCast(hidden), @intCast(dim * 3));
    const q = try b.sliceLastDim(qkv, 0, @intCast(dim));
    const k = try b.sliceLastDim(qkv, @intCast(dim), @intCast(dim * 2));
    const v = try b.sliceLastDim(qkv, @intCast(dim * 2), @intCast(dim * 3));
    const kv = try b.add(k, v);
    const out = try b.add(q, kv);
    try g.markOutput(out);

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);
    const node_ids = try allocator.alloc(NodeId, @intCast(g.nodeCount()));
    defer allocator.free(node_ids);
    for (node_ids, 0..) |*node_id, idx| node_id.* = @intCast(idx);
    const skipped = try allocator.alloc(bool, node_ids.len);
    defer allocator.free(skipped);
    @memset(skipped, false);
    const values = try allocator.alloc(?CT, node_ids.len);
    defer allocator.free(values);
    @memset(values, null);

    const pattern = matchPackedLinearQkvSlicePattern(&g, values, node_ids, @intCast(qkv), reachable, skipped) orelse return error.ExpectedPackedQkvRegion;
    try std.testing.expectEqual(qkv, pattern.linear_id);
    try std.testing.expectEqual(q, pattern.q_slice_id);
    try std.testing.expectEqual(k, pattern.k_slice_id);
    try std.testing.expectEqual(v, pattern.v_slice_id);
    try std.testing.expectEqual(x, pattern.input_id);
    try std.testing.expectEqual(w, pattern.weight_id);
    try std.testing.expectEqual(bias, pattern.bias_id);
    try std.testing.expectEqual(@as(usize, rows), pattern.rows);
    try std.testing.expectEqual(@as(usize, hidden), pattern.in_dim);
    try std.testing.expectEqual(@as(usize, dim), pattern.q_out_dim);
    try std.testing.expectEqual(@as(usize, dim), pattern.kv_out_dim);

    var plan = try buildRuntimeRegionPlan(allocator, &g, node_ids, @intCast(g.nodeCount()), reachable, last_use);
    defer plan.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), plan.region_count);
    switch (plan.regionAt(@intCast(qkv), qkv, node_ids)) {
        .packed_linear_qkv_slice => |planned| {
            try std.testing.expectEqual(qkv, planned.linear_id);
            try std.testing.expectEqual(q, planned.q_slice_id);
            try std.testing.expectEqual(v, planned.v_slice_id);
        },
        else => return error.ExpectedPlannedPackedQkvRegion,
    }
}

test "metal partition executor ignores non gliner packed biased qkv slice graph region" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const rows: usize = 3;
    const hidden: usize = 8;
    const dim: usize = 8;
    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ rows, hidden }));
    const w = try b.parameter("unrelated.in_proj_weight", ml.graph.Shape.init(.f32, &.{ dim * 3, hidden }));
    const bias = try b.parameter("unrelated.in_proj_bias", ml.graph.Shape.init(.f32, &.{dim * 3}));
    const qkv = try b.linear(x, w, bias, @intCast(rows), @intCast(hidden), @intCast(dim * 3));
    const q = try b.sliceLastDim(qkv, 0, @intCast(dim));
    const k = try b.sliceLastDim(qkv, @intCast(dim), @intCast(dim * 2));
    const v = try b.sliceLastDim(qkv, @intCast(dim * 2), @intCast(dim * 3));
    const kv = try b.add(k, v);
    const out = try b.add(q, kv);
    const escaped_qkv = try b.add(qkv, qkv);
    try g.markOutput(out);
    try g.markOutput(escaped_qkv);

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const node_ids = try allocator.alloc(NodeId, @intCast(g.nodeCount()));
    defer allocator.free(node_ids);
    for (node_ids, 0..) |*node_id, idx| node_id.* = @intCast(idx);
    const skipped = try allocator.alloc(bool, node_ids.len);
    defer allocator.free(skipped);
    @memset(skipped, false);
    const values = try allocator.alloc(?CT, node_ids.len);
    defer allocator.free(values);
    @memset(values, null);

    try std.testing.expect(matchPackedLinearQkvSlicePattern(&g, values, node_ids, @intCast(qkv), reachable, skipped) == null);
}

test "metal partition executor recognizes rms norm grouped qkv graph region" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const rows: usize = 2;
    const hidden: usize = 8;
    const q_dim: usize = 16;
    const kv_dim: usize = 4;
    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ rows, hidden }));
    const norm_w = try b.parameter("model.layers.0.input_layernorm.weight", ml.graph.Shape.init(.f32, &.{hidden}));
    const q_w = try b.parameter("model.layers.0.self_attn.q_proj.weight", ml.graph.Shape.init(.f32, &.{ q_dim, hidden }));
    const k_w = try b.parameter("model.layers.0.self_attn.k_proj.weight", ml.graph.Shape.init(.f32, &.{ kv_dim, hidden }));
    const v_w = try b.parameter("model.layers.0.self_attn.v_proj.weight", ml.graph.Shape.init(.f32, &.{ kv_dim, hidden }));
    const normed = try b.rmsNorm(x, norm_w, @intCast(hidden), 1e-5);
    const qk_w = try b.concat(q_w, k_w, 0);
    const qkv_w = try b.concat(qk_w, v_w, 0);
    const qkv = try b.linearNoBias(normed, qkv_w, @intCast(rows), @intCast(hidden), @intCast(q_dim + kv_dim * 2));
    const q = try b.sliceLastDim(qkv, 0, @intCast(q_dim));
    const k = try b.sliceLastDim(qkv, @intCast(q_dim), @intCast(q_dim + kv_dim));
    const v = try b.sliceLastDim(qkv, @intCast(q_dim + kv_dim), @intCast(q_dim + kv_dim * 2));
    const kv = try b.add(k, v);
    const out = try b.add(q, kv);
    try g.markOutput(out);

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);
    const node_ids = try allocator.alloc(NodeId, @intCast(g.nodeCount()));
    defer allocator.free(node_ids);
    for (node_ids, 0..) |*node_id, idx| node_id.* = @intCast(idx);
    const skipped = try allocator.alloc(bool, node_ids.len);
    defer allocator.free(skipped);
    @memset(skipped, false);
    const values = try allocator.alloc(?CT, node_ids.len);
    defer allocator.free(values);
    @memset(values, null);

    const pattern = matchRmsNormGroupedLinearQkvSlicePattern(&g, values, node_ids, @intCast(normed), reachable, skipped) orelse return error.ExpectedRmsGroupedQkvRegion;
    try std.testing.expectEqual(normed, pattern.norm_id);
    try std.testing.expectEqual(x, pattern.norm_input_id);
    try std.testing.expectEqual(norm_w, pattern.norm_weight_id);
    try std.testing.expectEqual(@as(usize, hidden), pattern.norm_dim);
    try std.testing.expectEqual(qkv, pattern.qkv.linear_id);
    try std.testing.expectEqual(q, pattern.qkv.q_slice_id);
    try std.testing.expectEqual(k, pattern.qkv.k_slice_id);
    try std.testing.expectEqual(v, pattern.qkv.v_slice_id);
    try std.testing.expectEqual(q_w, pattern.qkv.q_weight_id);
    try std.testing.expectEqual(k_w, pattern.qkv.k_weight_id);
    try std.testing.expectEqual(v_w, pattern.qkv.v_weight_id);

    var plan = try buildRuntimeRegionPlan(allocator, &g, node_ids, @intCast(g.nodeCount()), reachable, last_use);
    defer plan.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), plan.region_count);
    switch (plan.regionAt(@intCast(normed), normed, node_ids)) {
        .rms_norm_grouped_linear_qkv_slice => |planned| {
            try std.testing.expectEqual(normed, planned.norm_id);
            try std.testing.expectEqual(qkv, planned.qkv.linear_id);
            try std.testing.expectEqual(k, planned.qkv.k_slice_id);
        },
        else => return error.ExpectedPlannedRmsGroupedQkvRegion,
    }
}

test "metal partition executor recognizes rms norm gated ffn graph region" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const rows: usize = 2;
    const hidden: usize = 8;
    const intermediate: usize = 16;
    const residual = try b.parameter("residual", ml.graph.Shape.init(.f32, &.{ rows, hidden }));
    const norm_w = try b.parameter("model.layers.0.post_attention_layernorm.weight", ml.graph.Shape.init(.f32, &.{hidden}));
    const gate_w = try b.parameter("model.layers.0.mlp.gate_proj.weight", ml.graph.Shape.init(.f32, &.{ intermediate, hidden }));
    const up_w = try b.parameter("model.layers.0.mlp.up_proj.weight", ml.graph.Shape.init(.f32, &.{ intermediate, hidden }));
    const down_w = try b.parameter("model.layers.0.mlp.down_proj.weight", ml.graph.Shape.init(.f32, &.{ hidden, intermediate }));
    const normed = try b.rmsNorm(residual, norm_w, @intCast(hidden), 1e-5);
    const pair = try g.addNode(.{
        .op = .{ .fused_linear_no_bias_pair = .{
            .rows = @intCast(rows),
            .in_dim = @intCast(hidden),
            .out_dim = @intCast(intermediate),
        } },
        .output_shape = ml.graph.Shape.init(.f32, &.{ rows, intermediate }),
        .inputs = .{ normed, gate_w, up_w, null_node },
        .num_inputs = 3,
    });
    const activated = try b.silu(pair);
    const pair_second = try g.addNode(.{
        .op = .{ .fused_to_float32 = {} },
        .output_shape = ml.graph.Shape.init(.f32, &.{ rows, intermediate }),
        .inputs = .{ pair, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const gated = try b.elemMultiply(activated, pair_second);
    const down = try b.linearNoBias(gated, down_w, @intCast(rows), @intCast(intermediate), @intCast(hidden));
    const out = try b.add(down, residual);
    try g.markOutput(out);

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);
    const node_ids = try allocator.alloc(NodeId, @intCast(g.nodeCount()));
    defer allocator.free(node_ids);
    for (node_ids, 0..) |*node_id, idx| node_id.* = @intCast(idx);
    const skipped = try allocator.alloc(bool, node_ids.len);
    defer allocator.free(skipped);
    @memset(skipped, false);

    const pattern = matchRmsNormGatedFfnResidualPattern(&g, node_ids, @intCast(normed), reachable, skipped, last_use) orelse return error.ExpectedRmsGatedFfnRegion;
    try std.testing.expectEqual(normed, pattern.norm_id);
    try std.testing.expectEqual(residual, pattern.norm_input_id);
    try std.testing.expectEqual(norm_w, pattern.norm_weight_id);
    try std.testing.expectEqual(pair, pattern.ffn.pair_id);
    try std.testing.expectEqual(pair_second, pattern.ffn.pair_second_id);
    try std.testing.expectEqual(activated, pattern.ffn.activation_id);
    try std.testing.expectEqual(gated, pattern.ffn.multiply_id);
    try std.testing.expectEqual(down, pattern.ffn.down_id);
    try std.testing.expectEqual(out, pattern.ffn.add_id);
    try std.testing.expectEqual(residual, pattern.ffn.residual_id);
    try std.testing.expectEqual(ops_mod.DecoderRuntimeActivationKind.silu, pattern.ffn.activation);

    var plan = try buildRuntimeRegionPlan(allocator, &g, node_ids, @intCast(g.nodeCount()), reachable, last_use);
    defer plan.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), plan.region_count);
    switch (plan.regionAt(@intCast(normed), normed, node_ids)) {
        .rms_norm_gated_ffn_residual => |planned| {
            try std.testing.expectEqual(normed, planned.norm_id);
            try std.testing.expectEqual(pair, planned.ffn.pair_id);
            try std.testing.expectEqual(out, planned.ffn.add_id);
        },
        else => return error.ExpectedPlannedRmsNormGatedFfnRegion,
    }
}

test "metal partition executor recognizes ple residual graph region" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const rows: usize = 2;
    const hidden: usize = 8;
    const ple_hidden: usize = 4;
    const hidden_in = try b.parameter("hidden", ml.graph.Shape.init(.f32, &.{ rows, hidden }));
    const ple = try b.parameter("ple", ml.graph.Shape.init(.f32, &.{ rows, ple_hidden }));
    const gate_w = try b.parameter("model.layers.0.ple.gate_proj.weight", ml.graph.Shape.init(.f32, &.{ ple_hidden, hidden }));
    const proj_w = try b.parameter("model.layers.0.ple.down_proj.weight", ml.graph.Shape.init(.f32, &.{ hidden, ple_hidden }));
    const norm_w = try b.parameter("model.layers.0.ple.post_norm.weight", ml.graph.Shape.init(.f32, &.{hidden}));
    const gate = try b.linearNoBias(hidden_in, gate_w, @intCast(rows), @intCast(hidden), @intCast(ple_hidden));
    const activated = try b.gelu(gate);
    const modulated = try b.elemMultiply(activated, ple);
    const projected = try b.linearNoBias(modulated, proj_w, @intCast(rows), @intCast(ple_hidden), @intCast(hidden));
    const post_norm = try b.rmsNorm(projected, norm_w, @intCast(hidden), 1e-5);
    const out = try b.add(hidden_in, post_norm);
    try g.markOutput(out);

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const node_ids = try allocator.alloc(NodeId, @intCast(g.nodeCount()));
    defer allocator.free(node_ids);
    for (node_ids, 0..) |*node_id, idx| node_id.* = @intCast(idx);
    const skipped = try allocator.alloc(bool, node_ids.len);
    defer allocator.free(skipped);
    @memset(skipped, false);

    const pattern = matchPleResidualPattern(&g, node_ids, @intCast(gate), reachable, skipped) orelse return error.ExpectedPleResidualRegion;
    try std.testing.expectEqual(gate, pattern.gate_id);
    try std.testing.expectEqual(activated, pattern.activation_id);
    try std.testing.expectEqual(modulated, pattern.multiply_id);
    try std.testing.expectEqual(projected, pattern.projection_id);
    try std.testing.expectEqual(post_norm, pattern.post_norm_id);
    try std.testing.expectEqual(out, pattern.add_id);
    try std.testing.expectEqual(hidden_in, pattern.hidden_id);
    try std.testing.expectEqual(ple, pattern.ple_id);
    try std.testing.expectEqual(gate_w, pattern.gate_weight_id);
    try std.testing.expectEqual(proj_w, pattern.projection_weight_id);
    try std.testing.expectEqual(norm_w, pattern.post_norm_weight_id);
    try std.testing.expectEqual(@as(usize, rows), pattern.rows);
    try std.testing.expectEqual(@as(usize, hidden), pattern.hidden_size);
    try std.testing.expectEqual(@as(usize, ple_hidden), pattern.ple_hidden_size);
    try std.testing.expectEqual(ops_mod.DecoderRuntimeActivationKind.gelu, pattern.activation);

    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);
    var plan = try buildRuntimeRegionPlan(allocator, &g, node_ids, @intCast(g.nodeCount()), reachable, last_use);
    defer plan.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 1), plan.region_count);
    switch (plan.regionAt(@intCast(gate), gate, node_ids)) {
        .ple_residual => |planned| {
            try std.testing.expectEqual(gate, planned.gate_id);
            try std.testing.expectEqual(projected, planned.projection_id);
            try std.testing.expectEqual(out, planned.add_id);
        },
        else => return error.ExpectedPlannedPleResidualRegion,
    }
}

test "metal partition executor prepared runtime regions count cached slots" {
    const attention_prepared: PreparedRuntimeRegion = .{
        .attention_output_residual = .{
            .linear_slot = 10,
            .pre_linear_rms_norm_slot = 11,
            .post_linear_rms_norm_slot = 12,
        },
    };
    const ffn_prepared: PreparedRuntimeRegion = .{
        .gated_ffn_residual = .{
            .gate_slot = 20,
            .up_slot = 21,
            .down_slot = 22,
            .post_down_rms_norm_slot = null,
        },
    };
    const rms_ffn_prepared: PreparedRuntimeRegion = .{
        .rms_norm_gated_ffn_residual = .{
            .norm_slot = 30,
            .ffn = .{
                .gate_slot = 31,
                .up_slot = 32,
                .down_slot = 33,
                .post_down_rms_norm_slot = 34,
            },
        },
    };
    const ple_prepared: PreparedRuntimeRegion = .{
        .ple_residual = .{
            .gate_slot = 40,
            .projection_slot = 41,
            .post_norm_slot = 42,
        },
    };

    try std.testing.expectEqual(@as(u64, 3), preparedRuntimeRegionSlotCount(attention_prepared));
    try std.testing.expectEqual(@as(u64, 3), preparedRuntimeRegionSlotCount(ffn_prepared));
    try std.testing.expectEqual(@as(u64, 5), preparedRuntimeRegionSlotCount(rms_ffn_prepared));
    try std.testing.expectEqual(@as(u64, 3), preparedRuntimeRegionSlotCount(ple_prepared));
    try std.testing.expect(preparedRuntimeRegionMatches(.{
        .attention_output_residual = .{
            .attention_id = 1,
            .pre_linear_norm_id = null,
            .linear_id = 2,
            .post_linear_norm_id = null,
            .add_id = 3,
            .residual_id = 4,
            .rows = 1,
            .attention_input_size = 8,
            .hidden_size = 8,
            .eps = 1e-5,
        },
    }, attention_prepared));
}

test "metal partition executor owned runtime region plan reuses cached plan" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const input = try b.parameter("input", ml.graph.Shape.init(.f32, &.{ 1, 4 }));
    try g.markOutput(input);

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);
    const node_ids = try allocator.alloc(NodeId, @intCast(g.nodeCount()));
    defer allocator.free(node_ids);
    for (node_ids, 0..) |*node_id, idx| node_id.* = @intCast(idx);

    var weight_store = native_compute.WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    defer deinitEmptyNativeWeightStore(&weight_store, allocator);
    var compute = native_compute.NativeCompute.init(allocator, &weight_store, null);
    var cb = compute.computeBackend();
    var exec = MetalPartitionExecutor.initBorrowed(allocator, &g, &cb);
    exec.owned = true;
    defer if (exec.runtime_region_plan) |*plan| plan.deinit(allocator);

    var stats: PartitionExecutor.ExecutionStats = .{};
    var transient: ?RuntimeRegionPlan = null;
    _ = try exec.runtimeRegionPlan(allocator, &g, node_ids, @intCast(g.nodeCount()), reachable, last_use, &stats, &transient);
    try std.testing.expectEqual(@as(u64, 1), stats.runtime_region_plan_compiles);
    try std.testing.expectEqual(@as(u64, 0), stats.runtime_region_plan_reuses);

    _ = try exec.runtimeRegionPlan(allocator, &g, node_ids, @intCast(g.nodeCount()), reachable, last_use, &stats, &transient);
    try std.testing.expectEqual(@as(u64, 1), stats.runtime_region_plan_compiles);
    try std.testing.expectEqual(@as(u64, 1), stats.runtime_region_plan_reuses);
}

test "metal partition executor runtime frame eligibility recognizes layer triples" {
    var regions = [_]RuntimeRegion{
        .{ .linear_qkv = .{
            .q_id = 1,
            .k_id = 2,
            .v_id = 3,
            .input_id = 0,
            .q_weight_id = 4,
            .k_weight_id = 5,
            .v_weight_id = 6,
            .rows = 2,
            .in_dim = 8,
            .q_out_dim = 16,
            .kv_out_dim = 4,
        } },
        .{ .attention_output_residual = .{
            .attention_id = 10,
            .pre_linear_norm_id = null,
            .linear_id = 11,
            .post_linear_norm_id = null,
            .add_id = 12,
            .residual_id = 1,
            .rows = 2,
            .attention_input_size = 16,
            .hidden_size = 8,
            .eps = 1e-5,
        } },
        .{ .rms_norm_gated_ffn_residual = .{
            .norm_id = 20,
            .norm_input_id = 12,
            .norm_weight_id = 21,
            .norm_dim = 8,
            .norm_eps = 1e-5,
            .ffn = .{
                .pair_id = 22,
                .pair_second_id = 23,
                .activation_id = 24,
                .multiply_id = 25,
                .down_id = 26,
                .post_down_norm_id = null,
                .add_id = 27,
                .residual_id = 12,
                .activation = .silu,
                .hidden_size = 8,
                .intermediate_size = 32,
                .rows = 2,
                .eps = 1e-5,
            },
        } },
        .{ .ple_residual = .{
            .gate_id = 30,
            .activation_id = 31,
            .multiply_id = 32,
            .projection_id = 33,
            .post_norm_id = 34,
            .add_id = 35,
            .hidden_id = 27,
            .ple_id = 2,
            .gate_weight_id = 36,
            .projection_weight_id = 37,
            .post_norm_weight_id = 38,
            .rows = 2,
            .hidden_size = 8,
            .ple_hidden_size = 4,
            .eps = 1e-5,
            .activation = .gelu,
        } },
    };
    const plan = RuntimeRegionPlan{
        .regions_by_pos = regions[0..],
        .region_count = regions.len,
    };

    const eligibility = analyzeRuntimeFrameEligibility(null, plan);
    try std.testing.expectEqual(@as(usize, 1), eligibility.layers);
    try std.testing.expectEqual(RuntimeFrameIneligibleReason.missing_model_metadata, eligibility.reason);

    var stats: PartitionExecutor.ExecutionStats = .{};
    recordRuntimeFrameEligibilityStats(&stats, eligibility);
    try std.testing.expectEqual(@as(u64, 1), stats.runtime_frame_candidates);
    try std.testing.expectEqual(@as(u64, 0), stats.runtime_frame_eligible);
    try std.testing.expectEqual(@as(u64, 1), stats.runtime_frame_ineligible_missing_model_metadata);

    for (&regions) |*region| switch (region.*) {
        .linear_qkv => |*pattern| pattern.rows = 1,
        .attention_output_residual => |*pattern| pattern.rows = 1,
        .rms_norm_gated_ffn_residual => |*pattern| pattern.ffn.rows = 1,
        .gated_ffn_residual => |*pattern| pattern.rows = 1,
        .ple_residual => |*pattern| pattern.rows = 1,
        else => {},
    };
    const single_row_eligibility = analyzeRuntimeFrameEligibility(null, plan);
    try std.testing.expectEqual(@as(usize, 1), single_row_eligibility.layers);
    try std.testing.expectEqual(RuntimeFrameIneligibleReason.single_row, single_row_eligibility.reason);
}

test "metal partition executor runtime frame eligibility rejects incomplete layer triples" {
    var regions = [_]RuntimeRegion{
        .{ .linear_qkv = .{
            .q_id = 1,
            .k_id = 2,
            .v_id = 3,
            .input_id = 0,
            .q_weight_id = 4,
            .k_weight_id = 5,
            .v_weight_id = 6,
            .rows = 2,
            .in_dim = 8,
            .q_out_dim = 16,
            .kv_out_dim = 4,
        } },
        .{ .attention_output_residual = .{
            .attention_id = 10,
            .pre_linear_norm_id = null,
            .linear_id = 11,
            .post_linear_norm_id = null,
            .add_id = 12,
            .residual_id = 1,
            .rows = 2,
            .attention_input_size = 16,
            .hidden_size = 8,
            .eps = 1e-5,
        } },
        .{ .gated_ffn_residual = .{
            .pair_id = 22,
            .pair_second_id = 23,
            .activation_id = 24,
            .multiply_id = 25,
            .down_id = 26,
            .post_down_norm_id = null,
            .add_id = 27,
            .residual_id = 12,
            .activation = .silu,
            .hidden_size = 8,
            .intermediate_size = 32,
            .rows = 2,
            .eps = 1e-5,
        } },
    };
    const plan = RuntimeRegionPlan{
        .regions_by_pos = regions[0..],
        .region_count = regions.len,
    };

    const eligibility = analyzeRuntimeFrameEligibility(null, plan);
    try std.testing.expectEqual(@as(usize, 0), eligibility.layers);
    try std.testing.expectEqual(RuntimeFrameIneligibleReason.missing_ple, eligibility.reason);
}

test "metal partition executor runtime frame eligibility treats deberta encoder regions as non generic-frame metadata" {
    var regions = [_]RuntimeRegion{
        .{ .linear_qkv = .{
            .q_id = 1,
            .k_id = 2,
            .v_id = 3,
            .input_id = 0,
            .q_weight_id = 4,
            .k_weight_id = 5,
            .v_weight_id = 6,
            .rows = 2,
            .in_dim = 8,
            .q_out_dim = 16,
            .kv_out_dim = 4,
        } },
        .{ .deberta_encoder_lora_layer = .{
            .ffn = .{
                .first_dot_id = 20,
                .first_base_add_id = 21,
                .first_add_id = 22,
                .gelu_id = 23,
                .output_dot_id = 24,
                .output_base_add_id = 25,
                .output_add_id = 26,
                .input_id = 12,
                .first_weight_id = 30,
                .first_bias_id = 31,
                .second_weight_id = 32,
                .second_bias_id = 33,
                .rows = 2,
                .hidden_size = 8,
                .intermediate_size = 32,
                .output_size = 8,
            },
            .residual_add_id = 27,
            .layer_norm_id = 28,
            .layer_norm_weight_id = 34,
            .layer_norm_bias_id = 35,
            .layer_index = 0,
            .norm_eps = 1e-7,
        } },
    };
    const plan = RuntimeRegionPlan{
        .regions_by_pos = regions[0..],
        .region_count = regions.len,
    };

    const eligibility = analyzeRuntimeFrameEligibility(null, plan);
    try std.testing.expectEqual(@as(usize, 0), eligibility.layers);
    try std.testing.expectEqual(RuntimeFrameIneligibleReason.missing_model_metadata, eligibility.reason);
}

test "metal partition executor derives runtime frame metadata with variable shared head dims" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const x0 = try b.parameter("x0", ml.graph.Shape.init(.f32, &.{ 2, 8 }));
    const q0_w = try b.parameter("model.layers.0.self_attn.q_proj.weight", ml.graph.Shape.init(.f32, &.{ 16, 8 }));
    const k0_w = try b.parameter("model.layers.0.self_attn.k_proj.weight", ml.graph.Shape.init(.f32, &.{ 4, 8 }));
    const v0_w = try b.parameter("model.layers.0.self_attn.v_proj.weight", ml.graph.Shape.init(.f32, &.{ 4, 8 }));
    const q1_w = try b.parameter("model.layers.1.self_attn.q_proj.weight", ml.graph.Shape.init(.f32, &.{ 32, 8 }));
    const attn0 = try g.addNode(.{
        .op = .{ .fused_gqa_causal_attention = .{
            .batch = 1,
            .seq_len = 2,
            .num_heads = 4,
            .num_kv_heads = 1,
            .head_dim = 4,
            .layer_index = 0,
        } },
        .output_shape = ml.graph.Shape.init(.f32, &.{ 2, 16 }),
        .inputs = .{ 100, 101, 102, null_node },
        .num_inputs = 3,
    });
    const attn1 = try g.addNode(.{
        .op = .{ .fused_gqa_causal_attention = .{
            .batch = 1,
            .seq_len = 2,
            .num_heads = 4,
            .num_kv_heads = 1,
            .head_dim = 8,
            .layer_index = 0,
            .skip_kv_write = true,
        } },
        .output_shape = ml.graph.Shape.init(.f32, &.{ 2, 32 }),
        .inputs = .{ 103, 101, 102, null_node },
        .num_inputs = 3,
    });
    _ = x0;

    var regions = [_]RuntimeRegion{
        .{ .linear_qkv = .{
            .q_id = 10,
            .k_id = 11,
            .v_id = 12,
            .input_id = 0,
            .q_weight_id = q0_w,
            .k_weight_id = k0_w,
            .v_weight_id = v0_w,
            .rows = 2,
            .in_dim = 8,
            .q_out_dim = 16,
            .kv_out_dim = 4,
        } },
        .{ .attention_output_residual = .{
            .attention_id = attn0,
            .pre_linear_norm_id = null,
            .linear_id = 13,
            .post_linear_norm_id = null,
            .add_id = 14,
            .residual_id = 0,
            .rows = 2,
            .attention_input_size = 16,
            .hidden_size = 8,
            .eps = 1e-5,
        } },
        .{ .gated_ffn_residual = .{
            .pair_id = 15,
            .pair_second_id = 16,
            .activation_id = 17,
            .multiply_id = 18,
            .down_id = 19,
            .post_down_norm_id = null,
            .add_id = 20,
            .residual_id = 14,
            .activation = .gelu,
            .hidden_size = 8,
            .intermediate_size = 32,
            .rows = 2,
            .eps = 1e-5,
        } },
        .{ .ple_residual = .{
            .gate_id = 21,
            .activation_id = 22,
            .multiply_id = 23,
            .projection_id = 24,
            .post_norm_id = 25,
            .add_id = 26,
            .hidden_id = 20,
            .ple_id = 2,
            .gate_weight_id = 27,
            .projection_weight_id = 28,
            .post_norm_weight_id = 29,
            .rows = 2,
            .hidden_size = 8,
            .ple_hidden_size = 4,
            .eps = 1e-5,
            .activation = .gelu,
        } },
        .{ .q_linear = .{
            .id = 30,
            .input_id = 26,
            .weight_id = q1_w,
            .rows = 2,
            .in_dim = 8,
            .out_dim = 32,
        } },
        .{ .attention_output_residual = .{
            .attention_id = attn1,
            .pre_linear_norm_id = null,
            .linear_id = 31,
            .post_linear_norm_id = null,
            .add_id = 32,
            .residual_id = 26,
            .rows = 2,
            .attention_input_size = 32,
            .hidden_size = 8,
            .eps = 1e-5,
        } },
        .{ .gated_ffn_residual = .{
            .pair_id = 33,
            .pair_second_id = 34,
            .activation_id = 35,
            .multiply_id = 36,
            .down_id = 37,
            .post_down_norm_id = null,
            .add_id = 38,
            .residual_id = 32,
            .activation = .gelu,
            .hidden_size = 8,
            .intermediate_size = 32,
            .rows = 2,
            .eps = 1e-5,
        } },
        .{ .ple_residual = .{
            .gate_id = 39,
            .activation_id = 40,
            .multiply_id = 41,
            .projection_id = 42,
            .post_norm_id = 43,
            .add_id = 44,
            .hidden_id = 38,
            .ple_id = 3,
            .gate_weight_id = 45,
            .projection_weight_id = 46,
            .post_norm_weight_id = 47,
            .rows = 2,
            .hidden_size = 8,
            .ple_hidden_size = 4,
            .eps = 1e-5,
            .activation = .gelu,
        } },
    };
    const plan = RuntimeRegionPlan{
        .regions_by_pos = regions[0..],
        .region_count = regions.len,
    };

    const metadata = runtimeFrameMetadataFromPlan(&g, plan) orelse return error.ExpectedRuntimeFrameMetadata;
    try std.testing.expectEqual(@as(usize, 2), metadata.layer_count);
    try std.testing.expectEqual(@as(usize, 2), metadata.rows);
    try std.testing.expectEqual(@as(usize, 8), metadata.hidden_size);
    try std.testing.expectEqual(@as(usize, 4), metadata.num_attention_heads);
    try std.testing.expectEqual(@as(usize, 8), metadata.global_head_dim);
    try std.testing.expectEqual(@as(usize, 4), metadata.ple_hidden_size);
    try std.testing.expectEqual(ops_mod.DecoderRuntimeActivationKind.gelu, metadata.activation);

    var reordered_regions = [_]RuntimeRegion{
        regions[0],
        regions[4],
        regions[1],
        regions[2],
        regions[3],
        regions[5],
        regions[6],
        regions[7],
    };
    const reordered_plan = RuntimeRegionPlan{
        .regions_by_pos = reordered_regions[0..],
        .region_count = reordered_regions.len,
    };
    const reordered_eligibility = analyzeRuntimeFrameEligibility(&g, reordered_plan);
    try std.testing.expectEqual(@as(usize, 2), reordered_eligibility.layers);
    try std.testing.expectEqual(RuntimeFrameIneligibleReason.none, reordered_eligibility.reason);
    const reordered_metadata = runtimeFrameMetadataFromPlan(&g, reordered_plan) orelse return error.ExpectedRuntimeFrameMetadata;
    try std.testing.expectEqual(@as(usize, 2), reordered_metadata.layer_count);
    try std.testing.expectEqual(@as(usize, 8), reordered_metadata.global_head_dim);
}

test "metal partition executor rejects gated ffn pattern with escaped intermediate" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const rows: usize = 1;
    const hidden: usize = 3;
    const intermediate: usize = 5;
    const residual = try b.parameter("residual", ml.graph.Shape.init(.f32, &.{ rows, hidden }));
    const norm_w = try b.parameter("norm_w", ml.graph.Shape.init(.f32, &.{hidden}));
    const gate_w = try b.parameter("gate_w", ml.graph.Shape.init(.f32, &.{ intermediate, hidden }));
    const up_w = try b.parameter("up_w", ml.graph.Shape.init(.f32, &.{ intermediate, hidden }));
    const down_w = try b.parameter("down_w", ml.graph.Shape.init(.f32, &.{ hidden, intermediate }));
    const normed = try b.rmsNorm(residual, norm_w, @intCast(hidden), 1e-5);
    const pair = try g.addNode(.{
        .op = .{ .fused_linear_no_bias_pair = .{
            .rows = @intCast(rows),
            .in_dim = @intCast(hidden),
            .out_dim = @intCast(intermediate),
        } },
        .output_shape = ml.graph.Shape.init(.f32, &.{ rows, intermediate }),
        .inputs = .{ normed, gate_w, up_w, null_node },
        .num_inputs = 3,
    });
    const activated = try b.silu(pair);
    const pair_second = try g.addNode(.{
        .op = .{ .fused_to_float32 = {} },
        .output_shape = ml.graph.Shape.init(.f32, &.{ rows, intermediate }),
        .inputs = .{ pair, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const escaped = try b.add(pair, pair_second);
    const gated = try b.elemMultiply(activated, pair_second);
    const down = try b.linearNoBias(gated, down_w, @intCast(rows), @intCast(intermediate), @intCast(hidden));
    const out = try b.add(down, residual);
    try g.markOutput(out);
    try g.markOutput(escaped);

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);
    const node_ids = try allocator.alloc(NodeId, @intCast(g.nodeCount()));
    defer allocator.free(node_ids);
    for (node_ids, 0..) |*node_id, idx| node_id.* = @intCast(idx);
    const skipped = try allocator.alloc(bool, node_ids.len);
    defer allocator.free(skipped);
    @memset(skipped, false);

    try std.testing.expect(matchGatedFfnResidualPattern(&g, node_ids, @intCast(pair), reachable, skipped, last_use) == null);
}

test "metal partition executor runtime add keeps resident input device backed" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime_mod.metalDeviceAvailable()) return error.SkipZigTest;

    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const dim: usize = 4;
    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ 1, dim }));
    const y = try b.parameter("y", ml.graph.Shape.init(.f32, &.{ 1, dim }));
    const sum = try b.add(x, y);

    var weight_store = initEmptyMetalWeightStore(allocator);
    defer deinitEmptyMetalWeightStore(&weight_store, allocator);
    var metal_compute = try metal_compute_mod.MetalCompute.init(allocator, &weight_store, null);
    defer metal_compute.deinit();
    var cb = metal_compute.computeBackend();
    if (!cb.decoderRuntimeReady()) return error.SkipZigTest;

    const values = try allocator.alloc(?CT, @intCast(g.nodeCount()));
    defer allocator.free(values);
    @memset(values, null);

    const x_host = try cb.fromFloat32Shape(&.{ 1.0, 2.0, 3.0, 4.0 }, &.{ 1, dim });
    defer cb.free(x_host);
    const x_device = (try makeMetalDeviceResident(&cb, x_host)) orelse return error.SkipZigTest;
    defer cb.free(x_device);
    const y_host = try cb.fromFloat32Shape(&.{ 10.0, 20.0, 30.0, 40.0 }, &.{ 1, dim });
    defer cb.free(y_host);
    values[@intCast(x)] = x_device;
    values[@intCast(y)] = y_host;

    var exec_state = interpreter.ExecState{
        .attention_layer = 0,
        .options = .{},
        .last_use = &.{},
    };
    const out = (try tryExecuteMetalCommand(allocator, &g, &cb, values, sum, null, &exec_state, null, null, null, null)) orelse return error.UnsupportedPrimitiveOp;
    defer cb.free(out);
    try std.testing.expect(isMetalDeviceResident(&cb, out));

    const raw = try cb.toFloat32(out, allocator);
    defer allocator.free(raw);
    try std.testing.expectEqualSlices(f32, &.{ 11.0, 22.0, 33.0, 44.0 }, raw);
}

test "metal partition executor runtime rms norm supports row-wise resident shapes" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime_mod.metalDeviceAvailable()) return error.SkipZigTest;

    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const rows: usize = 2;
    const dim: usize = 4;
    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ rows, dim }));
    const weight = try b.parameter("weight", ml.graph.Shape.init(.f32, &.{dim}));
    const normed = try b.rmsNorm(x, weight, dim, 0.0);

    var weight_store = initEmptyMetalWeightStore(allocator);
    defer deinitEmptyMetalWeightStore(&weight_store, allocator);
    var metal_compute = try metal_compute_mod.MetalCompute.init(allocator, &weight_store, null);
    defer metal_compute.deinit();
    var cb = metal_compute.computeBackend();
    if (!cb.decoderRuntimeReady()) return error.SkipZigTest;

    const values = try allocator.alloc(?CT, @intCast(g.nodeCount()));
    defer allocator.free(values);
    @memset(values, null);

    const x_data = [_]f32{ 1.0, 2.0, 3.0, 4.0, 2.0, 4.0, 6.0, 8.0 };
    const x_host = try cb.fromFloat32Shape(&x_data, &.{ rows, dim });
    defer cb.free(x_host);
    const x_device = (try makeMetalDeviceResident(&cb, x_host)) orelse return error.SkipZigTest;
    defer cb.free(x_device);
    const weight_host = try cb.fromFloat32Shape(&.{ 1.0, 1.0, 1.0, 1.0 }, &.{dim});
    defer cb.free(weight_host);
    values[@intCast(x)] = x_device;
    values[@intCast(weight)] = weight_host;

    var exec_state = interpreter.ExecState{
        .attention_layer = 0,
        .options = .{},
        .last_use = &.{},
    };
    const out = (try tryExecuteMetalCommand(allocator, &g, &cb, values, normed, null, &exec_state, null, null, null, null)) orelse return error.UnsupportedPrimitiveOp;
    defer cb.free(out);
    try std.testing.expect(isMetalDeviceResident(&cb, out));

    const raw = try cb.toFloat32(out, allocator);
    defer allocator.free(raw);
    const denom0: f32 = @sqrt((1.0 + 4.0 + 9.0 + 16.0) / 4.0);
    const denom1: f32 = @sqrt((4.0 + 16.0 + 36.0 + 64.0) / 4.0);
    const expected = [_]f32{
        1.0 / denom0, 2.0 / denom0, 3.0 / denom0, 4.0 / denom0,
        2.0 / denom1, 4.0 / denom1, 6.0 / denom1, 8.0 / denom1,
    };
    for (expected, raw) |exp, actual| {
        try std.testing.expectApproxEqAbs(exp, actual, 1e-5);
    }
}

test "metal partition executor resident primitive chain stays device backed" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime_mod.metalDeviceAvailable()) return error.SkipZigTest;

    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const dim: usize = 4;
    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ 2, dim }));
    const e = try b.expOp(x);
    const l = try b.logOp(e);
    const product = try b.mul(e, l);
    const divided = try b.div(product, l);
    const diff = try b.sub(divided, l);
    const t = try b.tanhOp(diff);
    const a = try b.absOp(t);
    const out = try b.sliceLastDim(a, 1, 3);
    try g.markOutput(out);

    const caps = [_]partition_mod.Capability{
        .{ .backend = .metal, .priority = 10, .supports = &partition_mod.supportsAll },
    };
    var partition_plan = try partition_mod.partition(allocator, &g, &caps);
    defer partition_plan.deinit();
    var buffer_plan = try buffer_plan_mod.build(allocator, &g, &partition_plan, .{});
    defer buffer_plan.deinit();

    var weight_store = initEmptyMetalWeightStore(allocator);
    defer deinitEmptyMetalWeightStore(&weight_store, allocator);
    var metal_compute = try metal_compute_mod.MetalCompute.init(allocator, &weight_store, null);
    defer metal_compute.deinit();
    var cb = metal_compute.computeBackend();
    if (!cb.decoderRuntimeReady()) return error.SkipZigTest;

    var native_weight_store = native_compute.WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    defer deinitEmptyNativeWeightStore(&native_weight_store, allocator);
    var native_compute_impl = native_compute.NativeCompute.init(allocator, &native_weight_store, null);
    var native_cb = native_compute_impl.computeBackend();
    var mesh = try device_mesh_mod.DeviceMesh.init(allocator, &.{
        .{ .id = 0, .backend = &native_cb, .kind = .native },
        .{ .id = 1, .backend = &cb, .kind = .metal },
    });
    defer mesh.deinit();

    const count: usize = @intCast(g.nodeCount());
    const values = try allocator.alloc(?CT, count);
    defer allocator.free(values);
    @memset(values, null);
    const value_device = try allocator.alloc(DeviceId, count);
    defer allocator.free(value_device);
    @memset(value_device, 0);

    const x_data = [_]f32{ 0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 1.75, 2.0 };
    const x_ct = try native_cb.fromFloat32Shape(&x_data, &.{ 2, dim });
    defer native_cb.free(x_ct);
    values[@intCast(x)] = x_ct;

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);

    const partition_index = partition_plan.node_assignment[out];
    var owned_runtime_transfers = std.AutoHashMapUnmanaged(NodeId, void).empty;
    defer owned_runtime_transfers.deinit(allocator);
    var exec_stats: PartitionExecutor.ExecutionStats = .{};
    var exec = MetalPartitionExecutor.initBorrowed(allocator, &g, &cb);
    try exec.partitionExecutor().execute(values, value_device, partition_plan.partitions[partition_index].node_ids, 1, .{
        .allocator = allocator,
        .graph = &g,
        .backend = &cb,
        .mesh = &mesh,
        .options = .{
            .runtime_inputs = &.{.{ .node_id = x, .value = x_ct }},
        },
        .reachable = reachable,
        .last_use = last_use,
        .partition_plan = &partition_plan,
        .buffer_plan = &buffer_plan,
        .owned_runtime_transfers = &owned_runtime_transfers,
        .materialize_boundary_outputs = false,
        .stats = &exec_stats,
    });

    const out_index: usize = @intCast(out);
    defer if (values[out_index]) |ct| cb.free(ct);
    try std.testing.expect(isMetalDeviceResident(&cb, values[out_index].?));
    try std.testing.expect(exec_stats.backend_command_dispatches >= 7);
    try std.testing.expectEqual(@as(u64, 0), exec_stats.interpreter_fallbacks);
    try std.testing.expectEqual(@as(u64, 0), exec_stats.host_materialized_outputs);

    const raw = try cb.toFloat32(values[out_index].?, allocator);
    defer allocator.free(raw);
    try std.testing.expectEqual(@as(usize, 4), raw.len);
    for (0..2) |row| {
        for (0..2) |col| {
            const original = x_data[row * dim + col + 1];
            const expected = @abs(std.math.tanh(@exp(original) - original));
            try std.testing.expectApproxEqAbs(expected, raw[row * 2 + col], 1e-5);
        }
    }
}

test "metal partition executor resident concat prim stays device backed" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime_mod.metalDeviceAvailable()) return error.SkipZigTest;

    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const lhs = try b.parameter("lhs", ml.graph.Shape.init(.f32, &.{ 2, 2 }));
    const rhs = try b.parameter("rhs", ml.graph.Shape.init(.f32, &.{ 2, 3 }));
    const out = try b.concat(lhs, rhs, 1);
    try g.markOutput(out);

    const caps = [_]partition_mod.Capability{
        .{ .backend = .metal, .priority = 10, .supports = &metal_capabilities.supportsMetalEagerGraph },
    };
    var partition_plan = try partition_mod.partition(allocator, &g, &caps);
    defer partition_plan.deinit();
    var buffer_plan = try buffer_plan_mod.build(allocator, &g, &partition_plan, .{});
    defer buffer_plan.deinit();

    var weight_store = initEmptyMetalWeightStore(allocator);
    defer deinitEmptyMetalWeightStore(&weight_store, allocator);
    var metal_compute = try metal_compute_mod.MetalCompute.init(allocator, &weight_store, null);
    defer metal_compute.deinit();
    var cb = metal_compute.computeBackend();
    if (!cb.decoderRuntimeReady()) return error.SkipZigTest;

    var native_weight_store = native_compute.WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    defer deinitEmptyNativeWeightStore(&native_weight_store, allocator);
    var native_compute_impl = native_compute.NativeCompute.init(allocator, &native_weight_store, null);
    var native_cb = native_compute_impl.computeBackend();
    var mesh = try device_mesh_mod.DeviceMesh.init(allocator, &.{
        .{ .id = 0, .backend = &native_cb, .kind = .native },
        .{ .id = 1, .backend = &cb, .kind = .metal },
    });
    defer mesh.deinit();

    const count: usize = @intCast(g.nodeCount());
    const values = try allocator.alloc(?CT, count);
    defer allocator.free(values);
    @memset(values, null);
    const value_device = try allocator.alloc(DeviceId, count);
    defer allocator.free(value_device);
    @memset(value_device, 0);

    const lhs_data = [_]f32{ 1, 2, 3, 4 };
    const rhs_data = [_]f32{ 10, 11, 12, 13, 14, 15 };
    const lhs_ct = try native_cb.fromFloat32Shape(&lhs_data, &.{ 2, 2 });
    defer native_cb.free(lhs_ct);
    const rhs_ct = try native_cb.fromFloat32Shape(&rhs_data, &.{ 2, 3 });
    defer native_cb.free(rhs_ct);
    values[@intCast(lhs)] = lhs_ct;
    values[@intCast(rhs)] = rhs_ct;

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);

    var owned_runtime_transfers = std.AutoHashMapUnmanaged(NodeId, void).empty;
    defer owned_runtime_transfers.deinit(allocator);
    var exec_stats: PartitionExecutor.ExecutionStats = .{};
    const partition_index = partition_plan.node_assignment[out];
    var exec = MetalPartitionExecutor.initBorrowed(allocator, &g, &cb);
    try exec.partitionExecutor().execute(values, value_device, partition_plan.partitions[partition_index].node_ids, 1, .{
        .allocator = allocator,
        .graph = &g,
        .backend = &cb,
        .mesh = &mesh,
        .options = .{
            .runtime_inputs = &.{
                .{ .node_id = lhs, .value = lhs_ct },
                .{ .node_id = rhs, .value = rhs_ct },
            },
        },
        .reachable = reachable,
        .last_use = last_use,
        .partition_plan = &partition_plan,
        .buffer_plan = &buffer_plan,
        .owned_runtime_transfers = &owned_runtime_transfers,
        .materialize_boundary_outputs = false,
        .stats = &exec_stats,
    });

    const out_index: usize = @intCast(out);
    defer if (values[out_index]) |ct| cb.free(ct);
    try std.testing.expect(isMetalDeviceResident(&cb, values[out_index].?));
    try std.testing.expectEqual(@as(u64, 1), exec_stats.backend_command_dispatches);
    try std.testing.expectEqual(@as(u64, 0), exec_stats.interpreter_fallbacks);
    try std.testing.expectEqual(@as(u64, 0), exec_stats.host_materialized_outputs);

    const raw = try cb.toFloat32(values[out_index].?, allocator);
    defer allocator.free(raw);
    try std.testing.expectEqualSlices(f32, &.{ 1, 2, 10, 11, 12, 3, 4, 13, 14, 15 }, raw);
}

test "metal partition executor planned sdpa stays device backed without interpreter fallback" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime_mod.metalDeviceAvailable()) return error.SkipZigTest;

    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const q = try b.parameter("q", ml.graph.Shape.init(.f32, &.{ 1, 2, 2 }));
    const k = try b.parameter("k", ml.graph.Shape.init(.f32, &.{ 1, 2, 2 }));
    const v = try b.parameter("v", ml.graph.Shape.init(.f32, &.{ 1, 2, 2 }));
    const out = try b.sdpa(q, k, v, 1, 2, 1, 2);
    try g.markOutput(out);

    const seeds = try partition_mod.allocTensorDescriptorSeeds(allocator, &g);
    defer allocator.free(seeds);
    try partition_mod.seedAllParameterResidency(seeds, &g, .metal, 0);

    const caps = [_]partition_mod.Capability{
        .{ .backend = .metal, .priority = 20, .decide = &metal_capabilities.decideMetalEagerGraph },
        .{ .backend = .native, .priority = 0, .decide = &partition_mod.decideNative },
    };
    var partition_plan = try partition_mod.partitionWithOptions(allocator, &g, &caps, .{
        .tensor_descs = seeds,
    });
    defer partition_plan.deinit();
    try std.testing.expectEqual(operator_plan_mod.Operator.attention_flash, partition_plan.operatorPlanForNode(out).?.operator());

    var buffer_plan = try buffer_plan_mod.build(allocator, &g, &partition_plan, .{});
    defer buffer_plan.deinit();

    var weight_store = initEmptyMetalWeightStore(allocator);
    defer deinitEmptyMetalWeightStore(&weight_store, allocator);
    var metal_compute = try metal_compute_mod.MetalCompute.init(allocator, &weight_store, null);
    defer metal_compute.deinit();
    var cb = metal_compute.computeBackend();
    if (!cb.decoderRuntimeReady()) return error.SkipZigTest;

    const count: usize = @intCast(g.nodeCount());
    const values = try allocator.alloc(?CT, count);
    defer allocator.free(values);
    @memset(values, null);
    const value_device = try allocator.alloc(DeviceId, count);
    defer allocator.free(value_device);
    @memset(value_device, 0);

    const q_data = [_]f32{ 1.0, 0.0, 0.0, 1.0 };
    const k_data = [_]f32{ 1.0, 0.0, 0.0, 1.0 };
    const v_data = [_]f32{ 1.0, 2.0, 3.0, 4.0 };
    const q_ct = try cb.fromFloat32Shape(&q_data, &.{ 1, 2, 2 });
    defer cb.free(q_ct);
    const k_ct = try cb.fromFloat32Shape(&k_data, &.{ 1, 2, 2 });
    defer cb.free(k_ct);
    const v_ct = try cb.fromFloat32Shape(&v_data, &.{ 1, 2, 2 });
    defer cb.free(v_ct);
    const q_dev = (try makeMetalDeviceResident(&cb, q_ct)) orelse return error.SkipZigTest;
    defer cb.free(q_dev);
    const k_dev = (try makeMetalDeviceResident(&cb, k_ct)) orelse return error.SkipZigTest;
    defer cb.free(k_dev);
    const v_dev = (try makeMetalDeviceResident(&cb, v_ct)) orelse return error.SkipZigTest;
    defer cb.free(v_dev);
    values[@intCast(q)] = q_dev;
    values[@intCast(k)] = k_dev;
    values[@intCast(v)] = v_dev;

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);

    var exec_stats: PartitionExecutor.ExecutionStats = .{};
    const partition_index = partition_plan.node_assignment[out];
    var exec = MetalPartitionExecutor.initBorrowed(allocator, &g, &cb);
    try exec.partitionExecutor().execute(values, value_device, partition_plan.partitions[partition_index].node_ids, 0, .{
        .allocator = allocator,
        .graph = &g,
        .backend = &cb,
        .options = .{
            .runtime_inputs = &.{
                .{ .node_id = q, .value = q_dev },
                .{ .node_id = k, .value = k_dev },
                .{ .node_id = v, .value = v_dev },
            },
        },
        .reachable = reachable,
        .last_use = last_use,
        .partition_plan = &partition_plan,
        .buffer_plan = &buffer_plan,
        .materialize_boundary_outputs = false,
        .stats = &exec_stats,
    });

    const out_index: usize = @intCast(out);
    defer if (values[out_index]) |ct| cb.free(ct);
    try std.testing.expect(isMetalDeviceResident(&cb, values[out_index].?));
    try std.testing.expectEqual(@as(u64, 1), exec_stats.backend_command_dispatches);
    try std.testing.expectEqual(@as(u64, 1), exec_stats.planned_operator_dispatches);
    try std.testing.expectEqual(@as(u64, 0), exec_stats.interpreter_fallbacks);
    try std.testing.expectEqual(@as(u64, 0), exec_stats.host_materialized_outputs);

    const raw = try cb.toFloat32(values[out_index].?, allocator);
    defer allocator.free(raw);
    try std.testing.expectEqual(@as(usize, 4), raw.len);
    const inv_sqrt_2: f32 = 1.0 / @sqrt(@as(f32, 2.0));
    const p_diag = @exp(inv_sqrt_2) / (@exp(inv_sqrt_2) + @exp(@as(f32, 0.0)));
    const p_off = 1.0 - p_diag;
    const expected = [_]f32{
        p_diag * 1.0 + p_off * 3.0,
        p_diag * 2.0 + p_off * 4.0,
        p_off * 1.0 + p_diag * 3.0,
        p_off * 2.0 + p_diag * 4.0,
    };
    for (expected, raw) |exp, actual| {
        try std.testing.expectApproxEqAbs(exp, actual, 1e-4);
    }
}

test "metal partition executor planned sdpa bias and mask layouts stay device backed" {
    try runPlannedSdpaBiasMaskCase(.shared_heads);
    try runPlannedSdpaBiasMaskCase(.batched_heads);
    try runPlannedSdpaBiasMaskCase(.broadcast_head);
}

const TestSdpaBiasMode = enum {
    shared_heads,
    batched_heads,
    broadcast_head,
};

fn runPlannedSdpaBiasMaskCase(mode: TestSdpaBiasMode) !void {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime_mod.metalDeviceAvailable()) return error.SkipZigTest;

    const allocator = std.testing.allocator;
    const batch: usize = 2;
    const num_heads: usize = 3;
    const seq_len: usize = 2;
    const head_dim: usize = 2;
    const total = batch * num_heads * seq_len * head_dim;

    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const q = try b.parameter("q", ml.graph.Shape.init(.f32, &.{ @as(i64, @intCast(batch * num_heads)), seq_len, head_dim }));
    const k = try b.parameter("k", ml.graph.Shape.init(.f32, &.{ @as(i64, @intCast(batch * num_heads)), seq_len, head_dim }));
    const v = try b.parameter("v", ml.graph.Shape.init(.f32, &.{ @as(i64, @intCast(batch * num_heads)), seq_len, head_dim }));
    const bias_shape = sdpaBiasShape(mode, batch, num_heads, seq_len);
    const bias = try b.parameter("bias", bias_shape);
    const out = try g.addNode(.{
        .op = .{ .fused_sdpa = .{
            .batch = @intCast(batch),
            .seq_len = @intCast(seq_len),
            .num_heads = @intCast(num_heads),
            .head_dim = @intCast(head_dim),
        } },
        .output_shape = ml.graph.Shape.init(.f32, &.{ @as(i64, @intCast(batch * num_heads)), seq_len, head_dim }),
        .inputs = .{ q, k, v, bias },
        .num_inputs = 4,
    });
    try g.markOutput(out);

    const seeds = try partition_mod.allocTensorDescriptorSeeds(allocator, &g);
    defer allocator.free(seeds);
    try partition_mod.seedAllParameterResidency(seeds, &g, .metal, 0);

    const caps = [_]partition_mod.Capability{
        .{ .backend = .metal, .priority = 20, .decide = &metal_capabilities.decideMetalEagerGraph },
        .{ .backend = .native, .priority = 0, .decide = &partition_mod.decideNative },
    };
    var partition_plan = try partition_mod.partitionWithOptions(allocator, &g, &caps, .{
        .tensor_descs = seeds,
    });
    defer partition_plan.deinit();
    try std.testing.expectEqual(operator_plan_mod.Operator.attention_flash, partition_plan.operatorPlanForNode(out).?.operator());

    var buffer_plan = try buffer_plan_mod.build(allocator, &g, &partition_plan, .{});
    defer buffer_plan.deinit();

    var weight_store = initEmptyMetalWeightStore(allocator);
    defer deinitEmptyMetalWeightStore(&weight_store, allocator);
    var metal_compute = try metal_compute_mod.MetalCompute.init(allocator, &weight_store, null);
    defer metal_compute.deinit();
    var cb = metal_compute.computeBackend();
    if (!cb.decoderRuntimeReady()) return error.SkipZigTest;

    const count: usize = @intCast(g.nodeCount());
    const values = try allocator.alloc(?CT, count);
    defer allocator.free(values);
    @memset(values, null);
    const value_device = try allocator.alloc(DeviceId, count);
    defer allocator.free(value_device);
    @memset(value_device, 0);

    var q_data: [total]f32 = undefined;
    var k_data: [total]f32 = undefined;
    var v_data: [total]f32 = undefined;
    @memset(q_data[0..], 0.0);
    @memset(k_data[0..], 0.0);
    for (&v_data, 0..) |*value, idx| value.* = @as(f32, @floatFromInt(idx + 1)) * 0.25;

    const bias_len: usize = @intCast(bias_shape.numElements().?);
    const bias_data = try allocator.alloc(f32, bias_len);
    defer allocator.free(bias_data);
    fillSdpaBiasData(mode, bias_data, batch, num_heads, seq_len);

    const qkv_shape = [_]i32{ @intCast(batch * num_heads), @intCast(seq_len), @intCast(head_dim) };
    var bias_shape_i32: [ml.graph.shape.max_rank]i32 = undefined;
    for (0..bias_shape.rank()) |axis| bias_shape_i32[axis] = @intCast(bias_shape.dim(@intCast(axis)));

    const q_ct = try cb.fromFloat32Shape(&q_data, &qkv_shape);
    defer cb.free(q_ct);
    const k_ct = try cb.fromFloat32Shape(&k_data, &qkv_shape);
    defer cb.free(k_ct);
    const v_ct = try cb.fromFloat32Shape(&v_data, &qkv_shape);
    defer cb.free(v_ct);
    const bias_ct = try cb.fromFloat32Shape(bias_data, bias_shape_i32[0..bias_shape.rank()]);
    defer cb.free(bias_ct);

    const q_dev = (try makeMetalDeviceResident(&cb, q_ct)) orelse return error.SkipZigTest;
    defer cb.free(q_dev);
    const k_dev = (try makeMetalDeviceResident(&cb, k_ct)) orelse return error.SkipZigTest;
    defer cb.free(k_dev);
    const v_dev = (try makeMetalDeviceResident(&cb, v_ct)) orelse return error.SkipZigTest;
    defer cb.free(v_dev);
    const bias_dev = (try makeMetalDeviceResident(&cb, bias_ct)) orelse return error.SkipZigTest;
    defer cb.free(bias_dev);
    values[@intCast(q)] = q_dev;
    values[@intCast(k)] = k_dev;
    values[@intCast(v)] = v_dev;
    values[@intCast(bias)] = bias_dev;

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);

    const mask = [_]i64{ 1, 0, 1, 1 };
    var exec_stats: PartitionExecutor.ExecutionStats = .{};
    const partition_index = partition_plan.node_assignment[out];
    var exec = MetalPartitionExecutor.initBorrowed(allocator, &g, &cb);
    try exec.partitionExecutor().execute(values, value_device, partition_plan.partitions[partition_index].node_ids, 0, .{
        .allocator = allocator,
        .graph = &g,
        .backend = &cb,
        .options = .{
            .runtime_inputs = &.{
                .{ .node_id = q, .value = q_dev },
                .{ .node_id = k, .value = k_dev },
                .{ .node_id = v, .value = v_dev },
                .{ .node_id = bias, .value = bias_dev },
            },
            .sdpa_mask = &mask,
        },
        .reachable = reachable,
        .last_use = last_use,
        .partition_plan = &partition_plan,
        .buffer_plan = &buffer_plan,
        .materialize_boundary_outputs = false,
        .stats = &exec_stats,
    });

    const out_index: usize = @intCast(out);
    defer if (values[out_index]) |ct| cb.free(ct);
    try std.testing.expect(isMetalDeviceResident(&cb, values[out_index].?));
    try std.testing.expectEqual(@as(u64, 1), exec_stats.backend_command_dispatches);
    try std.testing.expectEqual(@as(u64, 1), exec_stats.planned_operator_dispatches);
    try std.testing.expectEqual(@as(u64, 0), exec_stats.interpreter_fallbacks);
    try std.testing.expectEqual(@as(u64, 0), exec_stats.host_materialized_outputs);

    const raw = try cb.toFloat32(values[out_index].?, allocator);
    defer allocator.free(raw);
    try std.testing.expectEqual(@as(usize, total), raw.len);

    var expected: [total]f32 = undefined;
    computeExpectedSdpaBiasMask(mode, &expected, &v_data, &mask, batch, num_heads, seq_len, head_dim);
    for (expected, raw) |exp, actual| {
        try std.testing.expectApproxEqAbs(exp, actual, 1e-4);
    }
}

fn sdpaBiasShape(mode: TestSdpaBiasMode, batch: usize, num_heads: usize, seq_len: usize) ml.graph.Shape {
    return switch (mode) {
        .shared_heads => ml.graph.Shape.init(.f32, &.{ @as(i64, @intCast(num_heads)), @as(i64, @intCast(seq_len)), @as(i64, @intCast(seq_len)) }),
        .batched_heads => ml.graph.Shape.init(.f32, &.{ @as(i64, @intCast(batch)), @as(i64, @intCast(num_heads)), @as(i64, @intCast(seq_len)), @as(i64, @intCast(seq_len)) }),
        .broadcast_head => ml.graph.Shape.init(.f32, &.{ @as(i64, @intCast(batch)), 1, @as(i64, @intCast(seq_len)), @as(i64, @intCast(seq_len)) }),
    };
}

fn fillSdpaBiasData(mode: TestSdpaBiasMode, bias: []f32, batch: usize, num_heads: usize, seq_len: usize) void {
    for (0..batch) |b| {
        for (0..num_heads) |h| {
            for (0..seq_len) |qi| {
                for (0..seq_len) |ki| {
                    const value = sdpaBiasValue(mode, b, h, qi, ki);
                    switch (mode) {
                        .shared_heads => bias[(h * seq_len + qi) * seq_len + ki] = value,
                        .batched_heads => bias[((b * num_heads + h) * seq_len + qi) * seq_len + ki] = value,
                        .broadcast_head => bias[(b * seq_len + qi) * seq_len + ki] = value,
                    }
                }
            }
        }
    }
}

fn sdpaBiasValue(mode: TestSdpaBiasMode, batch: usize, head: usize, query: usize, key: usize) f32 {
    const b: f32 = @floatFromInt(batch);
    const h: f32 = @floatFromInt(head);
    const q: f32 = @floatFromInt(query);
    const k: f32 = @floatFromInt(key);
    return switch (mode) {
        .shared_heads => 0.10 * h + 0.20 * q - 0.15 * k,
        .batched_heads => 0.30 * b + 0.10 * h + 0.20 * q - 0.15 * k,
        .broadcast_head => 0.35 * b + 0.20 * q - 0.15 * k,
    };
}

fn computeExpectedSdpaBiasMask(
    mode: TestSdpaBiasMode,
    expected: []f32,
    values: []const f32,
    mask: []const i64,
    batch: usize,
    num_heads: usize,
    seq_len: usize,
    head_dim: usize,
) void {
    for (0..batch) |b| {
        for (0..num_heads) |h| {
            const bh = b * num_heads + h;
            for (0..seq_len) |qi| {
                var best = -std.math.inf(f32);
                for (0..seq_len) |ki| {
                    if (mask[b * seq_len + ki] == 0) continue;
                    best = @max(best, sdpaBiasValue(mode, b, h, qi, ki));
                }
                var sum: f32 = 0.0;
                var weights: [2]f32 = .{ 0.0, 0.0 };
                for (0..seq_len) |ki| {
                    if (mask[b * seq_len + ki] == 0) continue;
                    const weight = @exp(sdpaBiasValue(mode, b, h, qi, ki) - best);
                    weights[ki] = weight;
                    sum += weight;
                }
                for (0..head_dim) |d| {
                    var accum: f32 = 0.0;
                    for (0..seq_len) |ki| {
                        if (weights[ki] == 0.0) continue;
                        accum += weights[ki] * values[(bh * seq_len + ki) * head_dim + d];
                    }
                    expected[(bh * seq_len + qi) * head_dim + d] = accum / sum;
                }
            }
        }
    }
}

test "metal partition executor resident last-dim reductions stay device backed" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime_mod.metalDeviceAvailable()) return error.SkipZigTest;

    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const rows: usize = 2;
    const dim: usize = 4;
    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ rows, dim }));
    const sum = try b.reduceSum(x, &.{1});
    const max = try b.reduceMax(x, &.{1});
    const mean = try b.reduceMean(x, &.{1});
    try g.markOutput(sum);
    try g.markOutput(max);
    try g.markOutput(mean);

    const caps = [_]partition_mod.Capability{
        .{ .backend = .metal, .priority = 10, .supports = &partition_mod.supportsAll },
    };
    var partition_plan = try partition_mod.partition(allocator, &g, &caps);
    defer partition_plan.deinit();
    var buffer_plan = try buffer_plan_mod.build(allocator, &g, &partition_plan, .{});
    defer buffer_plan.deinit();

    var weight_store = initEmptyMetalWeightStore(allocator);
    defer deinitEmptyMetalWeightStore(&weight_store, allocator);
    var metal_compute = try metal_compute_mod.MetalCompute.init(allocator, &weight_store, null);
    defer metal_compute.deinit();
    var cb = metal_compute.computeBackend();
    if (!cb.decoderRuntimeReady()) return error.SkipZigTest;

    var native_weight_store = native_compute.WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    defer deinitEmptyNativeWeightStore(&native_weight_store, allocator);
    var native_compute_impl = native_compute.NativeCompute.init(allocator, &native_weight_store, null);
    var native_cb = native_compute_impl.computeBackend();
    var mesh = try device_mesh_mod.DeviceMesh.init(allocator, &.{
        .{ .id = 0, .backend = &native_cb, .kind = .native },
        .{ .id = 1, .backend = &cb, .kind = .metal },
    });
    defer mesh.deinit();

    const count: usize = @intCast(g.nodeCount());
    const values = try allocator.alloc(?CT, count);
    defer allocator.free(values);
    @memset(values, null);
    const value_device = try allocator.alloc(DeviceId, count);
    defer allocator.free(value_device);
    @memset(value_device, 0);

    const x_data = [_]f32{ -1.0, 2.0, 4.0, -3.0, 0.5, 1.5, -2.5, 3.5 };
    const x_ct = try native_cb.fromFloat32Shape(&x_data, &.{ rows, dim });
    defer native_cb.free(x_ct);
    values[@intCast(x)] = x_ct;

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);

    const partition_index = partition_plan.node_assignment[sum];
    var owned_runtime_transfers = std.AutoHashMapUnmanaged(NodeId, void).empty;
    defer owned_runtime_transfers.deinit(allocator);
    var exec_stats: PartitionExecutor.ExecutionStats = .{};
    var exec = MetalPartitionExecutor.initBorrowed(allocator, &g, &cb);
    try exec.partitionExecutor().execute(values, value_device, partition_plan.partitions[partition_index].node_ids, 1, .{
        .allocator = allocator,
        .graph = &g,
        .backend = &cb,
        .mesh = &mesh,
        .options = .{
            .runtime_inputs = &.{.{ .node_id = x, .value = x_ct }},
        },
        .reachable = reachable,
        .last_use = last_use,
        .partition_plan = &partition_plan,
        .buffer_plan = &buffer_plan,
        .owned_runtime_transfers = &owned_runtime_transfers,
        .materialize_boundary_outputs = false,
        .stats = &exec_stats,
    });

    const output_nodes = [_]NodeId{ sum, max, mean };
    defer for (output_nodes) |node_id| {
        const idx: usize = @intCast(node_id);
        if (values[idx]) |ct| cb.free(ct);
    };
    try std.testing.expect(exec_stats.backend_command_dispatches >= 3);
    try std.testing.expectEqual(@as(u64, 0), exec_stats.interpreter_fallbacks);
    for (output_nodes) |node_id| {
        try std.testing.expect(isMetalDeviceResident(&cb, values[@intCast(node_id)].?));
    }

    const expected_sum = [_]f32{ 2.0, 3.0 };
    const expected_max = [_]f32{ 4.0, 3.5 };
    const expected_mean = [_]f32{ 0.5, 0.75 };
    const expected = [_][]const f32{ &expected_sum, &expected_max, &expected_mean };
    for (output_nodes, expected) |node_id, expected_values| {
        const raw = try cb.toFloat32(values[@intCast(node_id)].?, allocator);
        defer allocator.free(raw);
        try std.testing.expectEqual(@as(usize, rows), raw.len);
        for (expected_values, raw) |exp, actual| {
            try std.testing.expectApproxEqAbs(exp, actual, 1e-5);
        }
    }
}

fn addBroadcastReducedForTest(g: *Graph, input: NodeId, target_shape: ml.graph.Shape) !NodeId {
    const reduced_shape = g.node(input).output_shape;
    if (reduced_shape.numElements() == target_shape.numElements()) return input;

    var attrs = ml.graph.node.BroadcastAttrs{ .target_shape = target_shape };
    const rank = reduced_shape.rank();
    for (0..rank) |axis| attrs.broadcast_axes[axis] = @intCast(axis);
    attrs.num_axes = @intCast(rank);
    return g.addNode(.{
        .op = .{ .broadcast_in_dim = attrs },
        .output_shape = target_shape,
        .inputs = .{ input, null_node, null_node, null_node },
        .num_inputs = 1,
    });
}

test "metal partition executor decomposed softmax stays device resident" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime_mod.metalDeviceAvailable()) return error.SkipZigTest;

    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const rows: usize = 2;
    const dim: usize = 4;
    const x_shape = ml.graph.Shape.init(.f32, &.{ rows, dim });
    const x = try b.parameter("x", x_shape);
    const max = try b.reduceMax(x, &.{1});
    const max_bc = try addBroadcastReducedForTest(&g, max, x_shape);
    const shifted = try b.sub(x, max_bc);
    const exp_shifted = try b.expOp(shifted);
    const denom = try b.reduceSum(exp_shifted, &.{1});
    const denom_bc = try addBroadcastReducedForTest(&g, denom, x_shape);
    const out = try b.div(exp_shifted, denom_bc);
    try g.markOutput(out);

    const caps = [_]partition_mod.Capability{
        .{ .backend = .metal, .priority = 10, .supports = &partition_mod.supportsAll },
    };
    var partition_plan = try partition_mod.partition(allocator, &g, &caps);
    defer partition_plan.deinit();
    var buffer_plan = try buffer_plan_mod.build(allocator, &g, &partition_plan, .{});
    defer buffer_plan.deinit();

    var weight_store = initEmptyMetalWeightStore(allocator);
    defer deinitEmptyMetalWeightStore(&weight_store, allocator);
    var metal_compute = try metal_compute_mod.MetalCompute.init(allocator, &weight_store, null);
    defer metal_compute.deinit();
    var cb = metal_compute.computeBackend();
    if (!cb.decoderRuntimeReady()) return error.SkipZigTest;

    const count: usize = @intCast(g.nodeCount());
    const values = try allocator.alloc(?CT, count);
    defer allocator.free(values);
    @memset(values, null);
    const value_device = try allocator.alloc(DeviceId, count);
    defer allocator.free(value_device);
    @memset(value_device, 0);

    const x_data = [_]f32{ -1.0, 2.0, 4.0, -3.0, 0.5, 1.5, -2.5, 3.5 };
    const x_ct = try cb.fromFloat32Shape(&x_data, &.{ rows, dim });
    defer cb.free(x_ct);
    const x_dev = (try makeMetalDeviceResident(&cb, x_ct)) orelse return error.SkipZigTest;
    defer cb.free(x_dev);
    values[@intCast(x)] = x_dev;

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);

    const partition_index = partition_plan.node_assignment[out];
    var exec_stats: PartitionExecutor.ExecutionStats = .{};
    var exec = MetalPartitionExecutor.initBorrowed(allocator, &g, &cb);
    try exec.partitionExecutor().execute(values, value_device, partition_plan.partitions[partition_index].node_ids, 0, .{
        .allocator = allocator,
        .graph = &g,
        .backend = &cb,
        .options = .{
            .runtime_inputs = &.{.{ .node_id = x, .value = values[@intCast(x)].? }},
        },
        .reachable = reachable,
        .last_use = last_use,
        .partition_plan = &partition_plan,
        .buffer_plan = &buffer_plan,
        .materialize_boundary_outputs = false,
        .stats = &exec_stats,
    });

    const out_index: usize = @intCast(out);
    defer if (values[out_index]) |ct| cb.free(ct);
    try std.testing.expect(isMetalDeviceResident(&cb, values[out_index].?));
    try std.testing.expect(exec_stats.backend_command_dispatches >= 7);
    try std.testing.expectEqual(@as(u64, 0), exec_stats.interpreter_fallbacks);
    try std.testing.expectEqual(@as(u64, 0), exec_stats.host_materialized_outputs);

    const raw = try cb.toFloat32(values[out_index].?, allocator);
    defer allocator.free(raw);
    try std.testing.expectEqual(rows * dim, raw.len);
    for (0..rows) |row| {
        const base = row * dim;
        var row_max = x_data[base];
        for (x_data[base + 1 .. base + dim]) |value| row_max = @max(row_max, value);
        var denom_host: f32 = 0.0;
        for (x_data[base .. base + dim]) |value| denom_host += @exp(value - row_max);
        for (0..dim) |col| {
            const expected = @exp(x_data[base + col] - row_max) / denom_host;
            try std.testing.expectApproxEqAbs(expected, raw[base + col], 1e-5);
        }
    }
}

test "metal partition executor resident where select chain stays device backed" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime_mod.metalDeviceAvailable()) return error.SkipZigTest;

    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const dim: usize = 6;
    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{dim}));
    const threshold = try b.scalarConst(.f32, 0.0);
    const neg_one = try b.scalarConst(.f32, -1.0);
    const pos_one = try b.scalarConst(.f32, 1.0);
    const cond = try g.addNode(.{
        .op = .{ .less_than = {} },
        .output_shape = g.node(x).output_shape,
        .inputs = .{ x, threshold, null_node, null_node },
        .num_inputs = 2,
    });
    const out = try g.addNode(.{
        .op = .{ .where_select = {} },
        .output_shape = g.node(x).output_shape,
        .inputs = .{ cond, neg_one, pos_one, null_node },
        .num_inputs = 3,
    });
    try g.markOutput(out);

    const caps = [_]partition_mod.Capability{
        .{ .backend = .metal, .priority = 10, .supports = &partition_mod.supportsAll },
    };
    var partition_plan = try partition_mod.partition(allocator, &g, &caps);
    defer partition_plan.deinit();
    var buffer_plan = try buffer_plan_mod.build(allocator, &g, &partition_plan, .{});
    defer buffer_plan.deinit();

    var weight_store = initEmptyMetalWeightStore(allocator);
    defer deinitEmptyMetalWeightStore(&weight_store, allocator);
    var metal_compute = try metal_compute_mod.MetalCompute.init(allocator, &weight_store, null);
    defer metal_compute.deinit();
    var cb = metal_compute.computeBackend();
    if (!cb.decoderRuntimeReady()) return error.SkipZigTest;

    const count: usize = @intCast(g.nodeCount());
    const values = try allocator.alloc(?CT, count);
    defer allocator.free(values);
    @memset(values, null);
    const value_device = try allocator.alloc(DeviceId, count);
    defer allocator.free(value_device);
    @memset(value_device, 0);

    const x_data = [_]f32{ -3.0, -0.25, 0.0, 0.5, 2.0, -1.0 };
    const x_ct = try cb.fromFloat32Shape(&x_data, &.{dim});
    defer cb.free(x_ct);
    const threshold_ct = try cb.fromFloat32Shape(&.{0.0}, &.{1});
    defer cb.free(threshold_ct);
    const neg_one_ct = try cb.fromFloat32Shape(&.{-1.0}, &.{1});
    defer cb.free(neg_one_ct);
    const pos_one_ct = try cb.fromFloat32Shape(&.{1.0}, &.{1});
    defer cb.free(pos_one_ct);
    const x_dev = (try makeMetalDeviceResident(&cb, x_ct)) orelse return error.SkipZigTest;
    defer cb.free(x_dev);
    const threshold_dev = (try makeMetalDeviceResident(&cb, threshold_ct)) orelse return error.SkipZigTest;
    defer cb.free(threshold_dev);
    const neg_one_dev = (try makeMetalDeviceResident(&cb, neg_one_ct)) orelse return error.SkipZigTest;
    defer cb.free(neg_one_dev);
    const pos_one_dev = (try makeMetalDeviceResident(&cb, pos_one_ct)) orelse return error.SkipZigTest;
    defer cb.free(pos_one_dev);
    values[@intCast(x)] = x_dev;
    values[@intCast(threshold)] = threshold_dev;
    values[@intCast(neg_one)] = neg_one_dev;
    values[@intCast(pos_one)] = pos_one_dev;

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);

    var exec_stats: PartitionExecutor.ExecutionStats = .{};
    const partition_index = partition_plan.node_assignment[out];
    var exec = MetalPartitionExecutor.initBorrowed(allocator, &g, &cb);
    try exec.partitionExecutor().execute(values, value_device, partition_plan.partitions[partition_index].node_ids, 0, .{
        .allocator = allocator,
        .graph = &g,
        .backend = &cb,
        .options = .{
            .runtime_inputs = &.{
                .{ .node_id = x, .value = values[@intCast(x)].? },
                .{ .node_id = threshold, .value = values[@intCast(threshold)].? },
                .{ .node_id = neg_one, .value = values[@intCast(neg_one)].? },
                .{ .node_id = pos_one, .value = values[@intCast(pos_one)].? },
            },
        },
        .reachable = reachable,
        .last_use = last_use,
        .partition_plan = &partition_plan,
        .buffer_plan = &buffer_plan,
        .materialize_boundary_outputs = false,
        .stats = &exec_stats,
    });

    const out_index: usize = @intCast(out);
    defer if (values[out_index]) |ct| cb.free(ct);
    try std.testing.expect(isMetalDeviceResident(&cb, values[out_index].?));
    try std.testing.expect(exec_stats.backend_command_dispatches >= 2);
    try std.testing.expectEqual(@as(u64, 0), exec_stats.interpreter_fallbacks);
    try std.testing.expectEqual(@as(u64, 0), exec_stats.host_materialized_outputs);

    const raw = try cb.toFloat32(values[out_index].?, allocator);
    defer allocator.free(raw);
    try std.testing.expectEqualSlices(f32, &[_]f32{ -1.0, -1.0, 1.0, 1.0, 1.0, -1.0 }, raw);
}

test "metal partition executor resident pair and fused unary commands stay device backed" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime_mod.metalDeviceAvailable()) return error.SkipZigTest;

    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const dim: usize = 4;
    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ 1, dim }));
    const w_a = try b.parameter("w_a", ml.graph.Shape.init(.f32, &.{ dim, dim }));
    const w_b = try b.parameter("w_b", ml.graph.Shape.init(.f32, &.{ dim, dim }));
    const pair = try g.addNode(.{
        .op = .{ .fused_linear_no_bias_pair = .{ .rows = 1, .in_dim = dim, .out_dim = dim } },
        .output_shape = ml.graph.Shape.init(.f32, &.{ 1, dim }),
        .inputs = .{ x, w_a, w_b, null_node },
        .num_inputs = 3,
    });
    const pair_second = try g.addNode(.{
        .op = .{ .fused_to_float32 = {} },
        .output_shape = ml.graph.Shape.init(.f32, &.{ 1, dim }),
        .inputs = .{ pair, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const sigmoid = try g.addNode(.{
        .op = .{ .fused_sigmoid = {} },
        .output_shape = ml.graph.Shape.init(.f32, &.{ 1, dim }),
        .inputs = .{ x, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const tanh_act = try g.addNode(.{
        .op = .{ .fused_tanh_act = {} },
        .output_shape = ml.graph.Shape.init(.f32, &.{ 1, dim }),
        .inputs = .{ x, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    try g.markOutput(pair);
    try g.markOutput(pair_second);
    try g.markOutput(sigmoid);
    try g.markOutput(tanh_act);

    const caps = [_]partition_mod.Capability{
        .{ .backend = .metal, .priority = 10, .supports = &partition_mod.supportsAll },
    };
    var partition_plan = try partition_mod.partition(allocator, &g, &caps);
    defer partition_plan.deinit();
    var buffer_plan = try buffer_plan_mod.build(allocator, &g, &partition_plan, .{});
    defer buffer_plan.deinit();

    var weight_store = initEmptyMetalWeightStore(allocator);
    defer deinitEmptyMetalWeightStore(&weight_store, allocator);
    var metal_compute = try metal_compute_mod.MetalCompute.init(allocator, &weight_store, null);
    defer metal_compute.deinit();
    var cb = metal_compute.computeBackend();
    if (!cb.decoderRuntimeReady()) return error.SkipZigTest;

    const count: usize = @intCast(g.nodeCount());
    const values = try allocator.alloc(?CT, count);
    defer allocator.free(values);
    @memset(values, null);
    const value_device = try allocator.alloc(DeviceId, count);
    defer allocator.free(value_device);
    @memset(value_device, 0);

    const x_data = [_]f32{ -2.0, -0.5, 0.5, 2.0 };
    const w_a_data = [_]f32{
        1.0, 0.0, 0.0, 0.0,
        0.0, 1.0, 0.0, 0.0,
        0.0, 0.0, 1.0, 0.0,
        0.0, 0.0, 0.0, 1.0,
    };
    const w_b_data = [_]f32{
        2.0, 0.0, 0.0, 0.0,
        0.0, 2.0, 0.0, 0.0,
        0.0, 0.0, 2.0, 0.0,
        0.0, 0.0, 0.0, 2.0,
    };
    const x_ct = try cb.fromFloat32Shape(&x_data, &.{ 1, dim });
    defer cb.free(x_ct);
    const w_a_ct = try cb.fromFloat32Shape(&w_a_data, &.{ dim, dim });
    defer cb.free(w_a_ct);
    const w_b_ct = try cb.fromFloat32Shape(&w_b_data, &.{ dim, dim });
    defer cb.free(w_b_ct);
    const x_dev = (try makeMetalDeviceResident(&cb, x_ct)) orelse return error.SkipZigTest;
    defer if (x_dev != x_ct) cb.free(x_dev);
    const w_a_dev = (try makeMetalDeviceResident(&cb, w_a_ct)) orelse return error.SkipZigTest;
    defer if (w_a_dev != w_a_ct) cb.free(w_a_dev);
    const w_b_dev = (try makeMetalDeviceResident(&cb, w_b_ct)) orelse return error.SkipZigTest;
    defer if (w_b_dev != w_b_ct) cb.free(w_b_dev);
    values[@intCast(x)] = x_dev;
    values[@intCast(w_a)] = w_a_dev;
    values[@intCast(w_b)] = w_b_dev;

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);

    var exec_stats: PartitionExecutor.ExecutionStats = .{};
    const partition_index = partition_plan.node_assignment[pair];
    var exec = MetalPartitionExecutor.initBorrowed(allocator, &g, &cb);
    try exec.partitionExecutor().execute(values, value_device, partition_plan.partitions[partition_index].node_ids, 0, .{
        .allocator = allocator,
        .graph = &g,
        .backend = &cb,
        .options = .{
            .runtime_inputs = &.{
                .{ .node_id = x, .value = x_dev },
                .{ .node_id = w_a, .value = w_a_dev },
                .{ .node_id = w_b, .value = w_b_dev },
            },
        },
        .reachable = reachable,
        .last_use = last_use,
        .partition_plan = &partition_plan,
        .buffer_plan = &buffer_plan,
        .materialize_boundary_outputs = false,
        .stats = &exec_stats,
    });

    const output_nodes = [_]NodeId{ pair, pair_second, sigmoid, tanh_act };
    defer for (output_nodes) |node_id| {
        const idx: usize = @intCast(node_id);
        if (values[idx]) |ct| cb.free(ct);
    };
    try std.testing.expect(exec_stats.backend_command_dispatches >= output_nodes.len);
    try std.testing.expectEqual(@as(u64, 0), exec_stats.interpreter_fallbacks);
    try std.testing.expectEqual(@as(u64, 0), exec_stats.host_materialized_outputs);
    for (output_nodes) |node_id| {
        try std.testing.expect(isMetalDeviceResident(&cb, values[@intCast(node_id)].?));
    }

    const first_raw = try cb.toFloat32(values[@intCast(pair)].?, allocator);
    defer allocator.free(first_raw);
    const second_raw = try cb.toFloat32(values[@intCast(pair_second)].?, allocator);
    defer allocator.free(second_raw);
    try std.testing.expectApproxEqAbs(@as(f32, -2.0), first_raw[0], 1e-5);
    try std.testing.expectApproxEqAbs(@as(f32, 2.0), first_raw[3], 1e-5);
    try std.testing.expectApproxEqAbs(@as(f32, -4.0), second_raw[0], 1e-5);
    try std.testing.expectApproxEqAbs(@as(f32, 4.0), second_raw[3], 1e-5);
}

test "metal partition executor resident masked softmax projection chain stays device backed" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime_mod.metalDeviceAvailable()) return error.SkipZigTest;

    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const rows: usize = 2;
    const dim: usize = 4;
    const out_dim: usize = 3;
    const scores = try b.parameter("scores", ml.graph.Shape.init(.f32, &.{ rows, dim }));
    const projection = try b.parameter("projection", ml.graph.Shape.init(.f32, &.{ out_dim, dim }));
    const threshold = try b.scalarConst(.f32, 0.0);
    const masked_value = try b.scalarConst(.f32, -1.0e9);
    const keep_value = try b.scalarConst(.f32, 0.0);
    const cond = try g.addNode(.{
        .op = .{ .less_than = {} },
        .output_shape = g.node(scores).output_shape,
        .inputs = .{ scores, threshold, null_node, null_node },
        .num_inputs = 2,
    });
    const bias = try g.addNode(.{
        .op = .{ .where_select = {} },
        .output_shape = g.node(scores).output_shape,
        .inputs = .{ cond, masked_value, keep_value, null_node },
        .num_inputs = 3,
    });
    const masked = try b.add(scores, bias);
    const probs = try b.softmax(masked);
    const out = try b.linearNoBias(probs, projection, @intCast(rows), @intCast(dim), @intCast(out_dim));
    try g.markOutput(out);

    const caps = [_]partition_mod.Capability{
        .{ .backend = .metal, .priority = 10, .supports = &partition_mod.supportsAll },
    };
    var partition_plan = try partition_mod.partition(allocator, &g, &caps);
    defer partition_plan.deinit();
    var buffer_plan = try buffer_plan_mod.build(allocator, &g, &partition_plan, .{});
    defer buffer_plan.deinit();

    var weight_store = initEmptyMetalWeightStore(allocator);
    defer deinitEmptyMetalWeightStore(&weight_store, allocator);
    var metal_compute = try metal_compute_mod.MetalCompute.init(allocator, &weight_store, null);
    defer metal_compute.deinit();
    var cb = metal_compute.computeBackend();
    if (!cb.decoderRuntimeReady()) return error.SkipZigTest;

    const count: usize = @intCast(g.nodeCount());
    const values = try allocator.alloc(?CT, count);
    defer allocator.free(values);
    @memset(values, null);
    const value_device = try allocator.alloc(DeviceId, count);
    defer allocator.free(value_device);
    @memset(value_device, 0);

    const score_data = [_]f32{
        -2.0, 0.5,  1.5,  -0.25,
        2.0,  -1.0, 0.25, 1.0,
    };
    const projection_data = [_]f32{
        1.0,   0.0,  -0.5, 0.25,
        -0.25, 0.75, 0.5,  -1.0,
        0.5,   -0.5, 0.25, 1.0,
    };
    const score_ct = try cb.fromFloat32Shape(&score_data, &.{ rows, dim });
    defer cb.free(score_ct);
    const projection_ct = try cb.fromFloat32Shape(&projection_data, &.{ out_dim, dim });
    defer cb.free(projection_ct);
    const threshold_ct = try cb.fromFloat32Shape(&.{0.0}, &.{1});
    defer cb.free(threshold_ct);
    const masked_value_ct = try cb.fromFloat32Shape(&.{-1.0e9}, &.{1});
    defer cb.free(masked_value_ct);
    const keep_value_ct = try cb.fromFloat32Shape(&.{0.0}, &.{1});
    defer cb.free(keep_value_ct);

    const score_dev = (try makeMetalDeviceResident(&cb, score_ct)) orelse return error.SkipZigTest;
    defer cb.free(score_dev);
    const projection_dev = (try makeMetalDeviceResident(&cb, projection_ct)) orelse return error.SkipZigTest;
    defer cb.free(projection_dev);
    const threshold_dev = (try makeMetalDeviceResident(&cb, threshold_ct)) orelse return error.SkipZigTest;
    defer cb.free(threshold_dev);
    const masked_value_dev = (try makeMetalDeviceResident(&cb, masked_value_ct)) orelse return error.SkipZigTest;
    defer cb.free(masked_value_dev);
    const keep_value_dev = (try makeMetalDeviceResident(&cb, keep_value_ct)) orelse return error.SkipZigTest;
    defer cb.free(keep_value_dev);
    values[@intCast(scores)] = score_dev;
    values[@intCast(projection)] = projection_dev;
    values[@intCast(threshold)] = threshold_dev;
    values[@intCast(masked_value)] = masked_value_dev;
    values[@intCast(keep_value)] = keep_value_dev;

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);

    var exec_stats: PartitionExecutor.ExecutionStats = .{};
    const partition_index = partition_plan.node_assignment[out];
    var exec = MetalPartitionExecutor.initBorrowed(allocator, &g, &cb);
    try exec.partitionExecutor().execute(values, value_device, partition_plan.partitions[partition_index].node_ids, 0, .{
        .allocator = allocator,
        .graph = &g,
        .backend = &cb,
        .options = .{
            .runtime_inputs = &.{
                .{ .node_id = scores, .value = values[@intCast(scores)].? },
                .{ .node_id = projection, .value = values[@intCast(projection)].? },
                .{ .node_id = threshold, .value = values[@intCast(threshold)].? },
                .{ .node_id = masked_value, .value = values[@intCast(masked_value)].? },
                .{ .node_id = keep_value, .value = values[@intCast(keep_value)].? },
            },
        },
        .reachable = reachable,
        .last_use = last_use,
        .partition_plan = &partition_plan,
        .buffer_plan = &buffer_plan,
        .materialize_boundary_outputs = false,
        .stats = &exec_stats,
    });

    const out_index: usize = @intCast(out);
    defer if (values[out_index]) |ct| cb.free(ct);
    try std.testing.expect(isMetalDeviceResident(&cb, values[out_index].?));
    try std.testing.expect(exec_stats.backend_command_dispatches >= 5);
    try std.testing.expectEqual(@as(u64, 0), exec_stats.interpreter_fallbacks);
    try std.testing.expectEqual(@as(u64, 0), exec_stats.host_materialized_outputs);
    try std.testing.expect(exec_stats.graph_plan_slots_reserved > 0);
    try std.testing.expect(exec_stats.graph_plan_bytes_reserved >= rows * dim * @sizeOf(f32));

    const raw = try cb.toFloat32(values[out_index].?, allocator);
    defer allocator.free(raw);
    try std.testing.expectEqual(rows * out_dim, raw.len);
    var expected: [rows * out_dim]f32 = undefined;
    for (0..rows) |row| {
        const base = row * dim;
        var masked_scores: [dim]f32 = undefined;
        var row_max: f32 = -std.math.inf(f32);
        for (0..dim) |col| {
            const value = if (score_data[base + col] < 0.0) -1.0e9 else score_data[base + col];
            masked_scores[col] = value;
            row_max = @max(row_max, value);
        }
        var denom: f32 = 0.0;
        var probs_host: [dim]f32 = undefined;
        for (0..dim) |col| {
            probs_host[col] = @exp(masked_scores[col] - row_max);
            denom += probs_host[col];
        }
        for (0..dim) |col| probs_host[col] /= denom;
        for (0..out_dim) |out_col| {
            var acc: f32 = 0.0;
            for (0..dim) |col| acc += probs_host[col] * projection_data[out_col * dim + col];
            expected[row * out_dim + out_col] = acc;
        }
    }
    for (expected, raw) |exp, actual| {
        try std.testing.expectApproxEqAbs(exp, actual, 1e-5);
    }
}

test "metal partition executor resident rope stays device backed" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime_mod.metalDeviceAvailable()) return error.SkipZigTest;

    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ 1, 4 }));
    const rope = try g.addNode(.{
        .op = .{ .fused_rope = .{
            .seq_len = 1,
            .head_dim = 4,
            .rope_dim = 4,
            .theta = 10000.0,
            .freq_scale = 1.0,
            .position_offset = 0,
            .consecutive_pairs = false,
        } },
        .output_shape = g.node(x).output_shape,
        .inputs = .{ x, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    try g.markOutput(rope);

    const caps = [_]partition_mod.Capability{
        .{ .backend = .metal, .priority = 10, .supports = &metal_capabilities.supportsMetalEagerGraph },
    };
    var partition_plan = try partition_mod.partition(allocator, &g, &caps);
    defer partition_plan.deinit();
    var buffer_plan = try buffer_plan_mod.build(allocator, &g, &partition_plan, .{});
    defer buffer_plan.deinit();

    var weight_store = initEmptyMetalWeightStore(allocator);
    defer deinitEmptyMetalWeightStore(&weight_store, allocator);
    var metal_compute = try metal_compute_mod.MetalCompute.init(allocator, &weight_store, null);
    defer metal_compute.deinit();
    var cb = metal_compute.computeBackend();
    if (!cb.decoderRuntimeReady()) return error.SkipZigTest;

    const count: usize = @intCast(g.nodeCount());
    const values = try allocator.alloc(?CT, count);
    defer allocator.free(values);
    @memset(values, null);
    const value_device = try allocator.alloc(DeviceId, count);
    defer allocator.free(value_device);
    @memset(value_device, 0);

    const input_ct = try cb.fromFloat32Shape(&.{ 1.0, 2.0, 3.0, 4.0 }, &.{ 1, 4 });
    defer cb.free(input_ct);
    const input_dev = (try makeMetalDeviceResident(&cb, input_ct)) orelse return error.SkipZigTest;
    defer cb.free(input_dev);
    values[@intCast(x)] = input_dev;

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);

    var exec_stats: PartitionExecutor.ExecutionStats = .{};
    const partition_index = partition_plan.node_assignment[rope];
    var exec = MetalPartitionExecutor.initBorrowed(allocator, &g, &cb);
    try exec.partitionExecutor().execute(values, value_device, partition_plan.partitions[partition_index].node_ids, 0, .{
        .allocator = allocator,
        .graph = &g,
        .backend = &cb,
        .options = .{
            .runtime_inputs = &.{
                .{ .node_id = x, .value = values[@intCast(x)].? },
            },
        },
        .reachable = reachable,
        .last_use = last_use,
        .partition_plan = &partition_plan,
        .buffer_plan = &buffer_plan,
        .materialize_boundary_outputs = false,
        .stats = &exec_stats,
    });

    const out_index: usize = @intCast(rope);
    defer if (values[out_index]) |ct| cb.free(ct);
    try std.testing.expect(isMetalDeviceResident(&cb, values[out_index].?));
    try std.testing.expect(exec_stats.backend_command_dispatches >= 1);
    try std.testing.expectEqual(@as(u64, 0), exec_stats.interpreter_fallbacks);

    const raw = try cb.toFloat32(values[out_index].?, allocator);
    defer allocator.free(raw);
    try std.testing.expectEqualSlices(f32, &.{ 1.0, 2.0, 3.0, 4.0 }, raw);
}

test "metal partition executor resident zero tensor materializes without fallback" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime_mod.metalDeviceAvailable()) return error.SkipZigTest;

    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();

    const zero = try g.addNode(.{
        .op = .{ .fused_zero_tensor = .{ .rows = 1, .in_dim = 0, .out_dim = 4 } },
        .output_shape = ml.graph.Shape.init(.f32, &.{ 1, 4 }),
        .inputs = .{ null_node, null_node, null_node, null_node },
        .num_inputs = 0,
    });
    try g.markOutput(zero);

    const seeds = try partition_mod.allocTensorDescriptorSeeds(allocator, &g);
    defer allocator.free(seeds);
    try partition_mod.seedAllUploadableResidency(seeds, &g, .metal, 0);
    const caps = [_]partition_mod.Capability{
        .{ .backend = .metal, .priority = 10, .decide = &metal_capabilities.decideMetalEagerGraph },
        .{ .backend = .native, .priority = 0, .decide = &partition_mod.decideNative },
    };
    var partition_plan = try partition_mod.partitionWithOptions(allocator, &g, &caps, .{ .tensor_descs = seeds });
    defer partition_plan.deinit();
    try std.testing.expectEqual(contracts.BackendKind.metal, partition_plan.partitions[partition_plan.node_assignment[zero]].backend);
    var buffer_plan = try buffer_plan_mod.build(allocator, &g, &partition_plan, .{});
    defer buffer_plan.deinit();

    var weight_store = initEmptyMetalWeightStore(allocator);
    defer deinitEmptyMetalWeightStore(&weight_store, allocator);
    var metal_compute = try metal_compute_mod.MetalCompute.init(allocator, &weight_store, null);
    defer metal_compute.deinit();
    var cb = metal_compute.computeBackend();
    if (!cb.decoderRuntimeReady()) return error.SkipZigTest;

    const values = try allocator.alloc(?CT, @intCast(g.nodeCount()));
    defer allocator.free(values);
    @memset(values, null);
    const value_device = try allocator.alloc(DeviceId, @intCast(g.nodeCount()));
    defer allocator.free(value_device);
    @memset(value_device, 0);

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);

    var exec_stats: PartitionExecutor.ExecutionStats = .{};
    const partition_index = partition_plan.node_assignment[zero];
    var exec = MetalPartitionExecutor.initBorrowed(allocator, &g, &cb);
    try exec.partitionExecutor().execute(values, value_device, partition_plan.partitions[partition_index].node_ids, 0, .{
        .allocator = allocator,
        .graph = &g,
        .backend = &cb,
        .reachable = reachable,
        .last_use = last_use,
        .partition_plan = &partition_plan,
        .buffer_plan = &buffer_plan,
        .materialize_boundary_outputs = false,
        .stats = &exec_stats,
    });

    defer if (values[@intCast(zero)]) |ct| cb.free(ct);
    try std.testing.expect(isMetalDeviceResident(&cb, values[@intCast(zero)].?));
    try std.testing.expectEqual(@as(u64, 0), exec_stats.backend_command_dispatches);
    try std.testing.expectEqual(@as(u64, 1), exec_stats.constant_materializations);
    try std.testing.expectEqual(@as(u64, 0), exec_stats.interpreter_fallbacks);
    const raw = try cb.toFloat32(values[@intCast(zero)].?, allocator);
    defer allocator.free(raw);
    try std.testing.expectEqualSlices(f32, &.{ 0.0, 0.0, 0.0, 0.0 }, raw);
}

test "metal partition executor resident gqa attention uses command path" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime_mod.metalDeviceAvailable()) return error.SkipZigTest;

    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const batch: usize = 1;
    const seq_len: usize = 1;
    const num_heads: usize = 1;
    const head_dim: usize = 4;
    const dim: usize = num_heads * head_dim;
    const q = try b.parameter("q", ml.graph.Shape.init(.f32, &.{ seq_len, dim }));
    const k = try b.parameter("k", ml.graph.Shape.init(.f32, &.{ seq_len, dim }));
    const v = try b.parameter("v", ml.graph.Shape.init(.f32, &.{ seq_len, dim }));
    const attn = try g.addNode(.{
        .op = .{ .fused_gqa_causal_attention = .{
            .batch = @intCast(batch),
            .seq_len = @intCast(seq_len),
            .kv_seq_len = @intCast(seq_len),
            .num_heads = @intCast(num_heads),
            .num_kv_heads = @intCast(num_heads),
            .head_dim = @intCast(head_dim),
        } },
        .output_shape = ml.graph.Shape.init(.f32, &.{ seq_len, dim }),
        .inputs = .{ q, k, v, null_node },
        .num_inputs = 3,
    });
    try g.markOutput(attn);

    const caps = [_]partition_mod.Capability{
        .{ .backend = .metal, .priority = 10, .supports = &metal_capabilities.supportsMetalEagerGraph },
    };
    var partition_plan = try partition_mod.partition(allocator, &g, &caps);
    defer partition_plan.deinit();
    var buffer_plan = try buffer_plan_mod.build(allocator, &g, &partition_plan, .{});
    defer buffer_plan.deinit();

    var weight_store = initEmptyMetalWeightStore(allocator);
    defer deinitEmptyMetalWeightStore(&weight_store, allocator);
    var metal_compute = try metal_compute_mod.MetalCompute.init(allocator, &weight_store, null);
    defer metal_compute.deinit();
    var cb = metal_compute.computeBackend();
    if (!cb.decoderRuntimeReady()) return error.SkipZigTest;

    const count: usize = @intCast(g.nodeCount());
    const values = try allocator.alloc(?CT, count);
    defer allocator.free(values);
    @memset(values, null);
    const value_device = try allocator.alloc(DeviceId, count);
    defer allocator.free(value_device);
    @memset(value_device, 0);

    const q_ct = try cb.fromFloat32Shape(&.{ 1.0, 0.0, 0.0, 0.0 }, &.{ seq_len, dim });
    defer cb.free(q_ct);
    const k_ct = try cb.fromFloat32Shape(&.{ 1.0, 0.0, 0.0, 0.0 }, &.{ seq_len, dim });
    defer cb.free(k_ct);
    const v_ct = try cb.fromFloat32Shape(&.{ 5.0, 6.0, 7.0, 8.0 }, &.{ seq_len, dim });
    defer cb.free(v_ct);
    const q_dev = (try makeMetalDeviceResident(&cb, q_ct)) orelse return error.SkipZigTest;
    defer cb.free(q_dev);
    const k_dev = (try makeMetalDeviceResident(&cb, k_ct)) orelse return error.SkipZigTest;
    defer cb.free(k_dev);
    const v_dev = (try makeMetalDeviceResident(&cb, v_ct)) orelse return error.SkipZigTest;
    defer cb.free(v_dev);
    values[@intCast(q)] = q_dev;
    values[@intCast(k)] = k_dev;
    values[@intCast(v)] = v_dev;

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);

    var exec_stats: PartitionExecutor.ExecutionStats = .{};
    const partition_index = partition_plan.node_assignment[attn];
    var exec = MetalPartitionExecutor.initBorrowed(allocator, &g, &cb);
    try exec.partitionExecutor().execute(values, value_device, partition_plan.partitions[partition_index].node_ids, 0, .{
        .allocator = allocator,
        .graph = &g,
        .backend = &cb,
        .options = .{
            .runtime_inputs = &.{
                .{ .node_id = q, .value = values[@intCast(q)].? },
                .{ .node_id = k, .value = values[@intCast(k)].? },
                .{ .node_id = v, .value = values[@intCast(v)].? },
            },
        },
        .reachable = reachable,
        .last_use = last_use,
        .partition_plan = &partition_plan,
        .buffer_plan = &buffer_plan,
        .materialize_boundary_outputs = false,
        .stats = &exec_stats,
    });

    const out_index: usize = @intCast(attn);
    defer if (values[out_index]) |ct| cb.free(ct);
    try std.testing.expect(isMetalDeviceResident(&cb, values[out_index].?));
    try std.testing.expect(exec_stats.backend_command_dispatches >= 1);
    try std.testing.expectEqual(@as(u64, 0), exec_stats.interpreter_fallbacks);

    const raw = try cb.toFloat32(values[out_index].?, allocator);
    defer allocator.free(raw);
    try std.testing.expectEqualSlices(f32, &.{ 5.0, 6.0, 7.0, 8.0 }, raw);
}

fn putTestQuantizedWeight(
    allocator: std.mem.Allocator,
    weight_store: *gpu_hosted_store_mod.WeightStore,
    name: []const u8,
    raw: []const u8,
    shape: []const i64,
    format: @import("quant_matmul.zig").Format,
) !void {
    try weight_store.lazy_weights.put(allocator, name, .{
        .tensor_ref = undefined,
        .quantized_storage = QuantizedStorage{
            .tensor_type = try quantFormatTensorType(format),
            .raw_bytes = raw,
            .shape = shape,
            .raw_owned = false,
            .allocator = allocator,
        },
    });
}

fn quantFormatTensorType(format: @import("quant_matmul.zig").Format) !@import("../gguf/tensor_types.zig").TensorType {
    return switch (format) {
        .q1_0 => .{ .known = .Q1_0 },
        .q2_k => .{ .known = .Q2_K },
        .q3_k => .{ .known = .Q3_K },
        .q4_0 => .{ .known = .Q4_0 },
        .q4_1 => .{ .known = .Q4_1 },
        .q5_0 => .{ .known = .Q5_0 },
        .q5_1 => .{ .known = .Q5_1 },
        .q5_k => .{ .known = .Q5_K },
        .q8_0 => .{ .known = .Q8_0 },
        .q8_1 => .{ .known = .Q8_1 },
        .q8_k => .{ .known = .Q8_K },
        else => error.UnsupportedTensorType,
    };
}

fn quantizedBlockSize(format: @import("quant_matmul.zig").Format) !struct { values: usize, bytes: usize } {
    return switch (format) {
        .q1_0 => .{ .values = 128, .bytes = 18 },
        .q2_k => .{ .values = 256, .bytes = 84 },
        .q3_k => .{ .values = 256, .bytes = 110 },
        .q4_0 => .{ .values = 32, .bytes = 18 },
        .q4_1 => .{ .values = 32, .bytes = 20 },
        .q5_0 => .{ .values = 32, .bytes = 22 },
        .q5_1 => .{ .values = 32, .bytes = 24 },
        .q5_k => .{ .values = 256, .bytes = 176 },
        .q8_0 => .{ .values = 32, .bytes = 34 },
        .q8_1 => .{ .values = 32, .bytes = 36 },
        .q8_k => .{ .values = 256, .bytes = 292 },
        else => error.UnsupportedTensorType,
    };
}

fn quantizeLinearRowsForTest(
    allocator: std.mem.Allocator,
    format: @import("quant_matmul.zig").Format,
    dense: []const f32,
    out_dim: usize,
    in_dim: usize,
) ![]u8 {
    const layout = try quantizedBlockSize(format);
    if (in_dim % layout.values != 0 or dense.len != out_dim * in_dim) return error.UnsupportedShape;
    const blocks = in_dim / layout.values;
    const raw = try allocator.alloc(u8, out_dim * blocks * layout.bytes);
    errdefer allocator.free(raw);
    for (0..out_dim) |out_col| {
        for (0..blocks) |block| {
            const src = dense[out_col * in_dim + block * layout.values ..][0..layout.values];
            const dst = raw[(out_col * blocks + block) * layout.bytes ..][0..layout.bytes];
            switch (format) {
                .q1_0 => quant_codec.quantizeQ1_0Block(src, dst),
                .q2_k => quant_codec.quantizeQ2_KBlock(src, dst),
                .q3_k => quant_codec.quantizeQ3_KBlock(src, dst),
                .q4_0 => quant_codec.quantizeQ4_0Block(src, dst),
                .q4_1 => quant_codec.quantizeQ4_1Block(src, dst),
                .q5_0 => quant_codec.quantizeQ5_0Block(src, dst),
                .q5_1 => quant_codec.quantizeQ5_1Block(src, dst),
                .q5_k => quant_codec.quantizeQ5_KBlock(src, dst),
                .q8_0 => quant_codec.quantizeQ8_0Block(src, dst),
                .q8_1 => quant_codec.quantizeQ8_1Block(src, dst),
                .q8_k => quant_codec.quantizeQ8_KBlock(src, dst),
                else => return error.UnsupportedTensorType,
            }
        }
    }
    return raw;
}

test "metal partition executor resident qkv rope softmax projection chain stays device backed" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime_mod.metalDeviceAvailable()) return error.SkipZigTest;

    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const rows: usize = 1;
    const dim: usize = 4;
    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ rows, dim }));
    const q_weight_name = "model.layers.0.self_attn.q_proj.weight";
    const k_weight_name = "model.layers.0.self_attn.k_proj.weight";
    const v_weight_name = "model.layers.0.self_attn.v_proj.weight";
    const out_weight_name = "model.layers.0.self_attn.o_proj.weight";
    const q_weight = try b.parameter(q_weight_name, ml.graph.Shape.init(.f32, &.{ dim, dim }));
    const k_weight = try b.parameter(k_weight_name, ml.graph.Shape.init(.f32, &.{ dim, dim }));
    const v_weight = try b.parameter(v_weight_name, ml.graph.Shape.init(.f32, &.{ dim, dim }));
    const out_weight = try b.parameter(out_weight_name, ml.graph.Shape.init(.f32, &.{ dim, dim }));

    const q = try b.linearNoBias(x, q_weight, @intCast(rows), @intCast(dim), @intCast(dim));
    const k = try b.linearNoBias(x, k_weight, @intCast(rows), @intCast(dim), @intCast(dim));
    const v = try b.linearNoBias(x, v_weight, @intCast(rows), @intCast(dim), @intCast(dim));
    const q_3d = try b.reshape(q, ml.graph.Shape.init(.f32, &.{ 1, rows, dim }));
    const q_t = try b.transpose(q_3d, &.{ 0, 2, 1 });
    const q_tt = try b.transpose(q_t, &.{ 0, 2, 1 });
    const q_rope = try g.addNode(.{
        .op = .{ .fused_rope = .{
            .seq_len = rows,
            .head_dim = dim,
            .rope_dim = dim,
            .theta = 10000.0,
            .freq_scale = 1.0,
            .position_offset = 0,
            .consecutive_pairs = false,
        } },
        .output_shape = g.node(q_tt).output_shape,
        .inputs = .{ q_tt, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const q_back = try b.reshape(q_rope, ml.graph.Shape.init(.f32, &.{ rows, dim }));
    const logits = try b.add(q_back, k);
    const probs = try b.softmax(logits);
    const mixed = try b.add(probs, v);
    const out = try b.linearNoBias(mixed, out_weight, @intCast(rows), @intCast(dim), @intCast(dim));
    try g.markOutput(out);

    const caps = [_]partition_mod.Capability{
        .{ .backend = .metal, .priority = 10, .supports = &metal_capabilities.supportsMetalEagerGraph },
    };
    var partition_plan = try partition_mod.partition(allocator, &g, &caps);
    defer partition_plan.deinit();
    var buffer_plan = try buffer_plan_mod.build(allocator, &g, &partition_plan, .{});
    defer buffer_plan.deinit();

    var weight_store = initEmptyMetalWeightStore(allocator);
    defer deinitEmptyMetalWeightStore(&weight_store, allocator);
    var metal_compute = try metal_compute_mod.MetalCompute.init(allocator, &weight_store, null);
    defer metal_compute.deinit();
    var cb = metal_compute.computeBackend();
    if (!cb.decoderRuntimeReady()) return error.SkipZigTest;

    const count: usize = @intCast(g.nodeCount());
    const values = try allocator.alloc(?CT, count);
    defer allocator.free(values);
    @memset(values, null);
    const value_device = try allocator.alloc(DeviceId, count);
    defer allocator.free(value_device);
    @memset(value_device, 0);

    const input_data = [_]f32{
        0.1, 0.2, 0.3, 0.4,
    };
    const identity = [_]f32{
        1.0, 0.0, 0.0, 0.0,
        0.0, 1.0, 0.0, 0.0,
        0.0, 0.0, 1.0, 0.0,
        0.0, 0.0, 0.0, 1.0,
    };

    const input_ct = try cb.fromFloat32Shape(&input_data, &.{ rows, dim });
    defer cb.free(input_ct);
    const q_weight_ct = try cb.fromFloat32Shape(&identity, &.{ dim, dim });
    defer cb.free(q_weight_ct);
    const k_weight_ct = try cb.fromFloat32Shape(&identity, &.{ dim, dim });
    defer cb.free(k_weight_ct);
    const v_weight_ct = try cb.fromFloat32Shape(&identity, &.{ dim, dim });
    defer cb.free(v_weight_ct);
    const out_weight_ct = try cb.fromFloat32Shape(&identity, &.{ dim, dim });
    defer cb.free(out_weight_ct);

    const input_dev = (try makeMetalDeviceResident(&cb, input_ct)) orelse return error.SkipZigTest;
    defer cb.free(input_dev);
    const q_weight_dev = (try makeMetalDeviceResident(&cb, q_weight_ct)) orelse return error.SkipZigTest;
    defer cb.free(q_weight_dev);
    const k_weight_dev = (try makeMetalDeviceResident(&cb, k_weight_ct)) orelse return error.SkipZigTest;
    defer cb.free(k_weight_dev);
    const v_weight_dev = (try makeMetalDeviceResident(&cb, v_weight_ct)) orelse return error.SkipZigTest;
    defer cb.free(v_weight_dev);
    const out_weight_dev = (try makeMetalDeviceResident(&cb, out_weight_ct)) orelse return error.SkipZigTest;
    defer cb.free(out_weight_dev);

    values[@intCast(x)] = input_dev;
    values[@intCast(q_weight)] = q_weight_dev;
    values[@intCast(k_weight)] = k_weight_dev;
    values[@intCast(v_weight)] = v_weight_dev;
    values[@intCast(out_weight)] = out_weight_dev;

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);

    var exec_stats: PartitionExecutor.ExecutionStats = .{};
    var exec = MetalPartitionExecutor.initBorrowed(allocator, &g, &cb);
    const executed_partitions = try allocator.alloc(bool, partition_plan.partitions.len);
    defer allocator.free(executed_partitions);
    @memset(executed_partitions, false);
    for (0..@intCast(g.nodeCount())) |node_index| {
        const node_id: NodeId = @intCast(node_index);
        const partition_index = partition_plan.node_assignment[node_id];
        if (partition_index >= partition_plan.partitions.len or executed_partitions[partition_index]) continue;
        executed_partitions[partition_index] = true;
        const part = partition_plan.partitions[partition_index];
        var has_reachable_compute_node = false;
        for (part.node_ids) |part_node_id| {
            const part_index: usize = @intCast(part_node_id);
            if (part_index >= reachable.len or !reachable[part_index]) continue;
            switch (g.node(part_node_id).op) {
                .parameter, .constant => {},
                else => has_reachable_compute_node = true,
            }
        }
        if (!has_reachable_compute_node) continue;
        try std.testing.expectEqual(contracts.BackendKind.metal, part.backend);
        try exec.partitionExecutor().execute(values, value_device, part.node_ids, 0, .{
            .allocator = allocator,
            .graph = &g,
            .backend = &cb,
            .options = .{
                .runtime_inputs = &.{
                    .{ .node_id = x, .value = values[@intCast(x)].? },
                    .{ .node_id = q_weight, .value = values[@intCast(q_weight)].? },
                    .{ .node_id = k_weight, .value = values[@intCast(k_weight)].? },
                    .{ .node_id = v_weight, .value = values[@intCast(v_weight)].? },
                    .{ .node_id = out_weight, .value = values[@intCast(out_weight)].? },
                },
            },
            .reachable = reachable,
            .last_use = last_use,
            .partition_plan = &partition_plan,
            .buffer_plan = &buffer_plan,
            .materialize_boundary_outputs = false,
            .stats = &exec_stats,
        });
    }

    const out_index: usize = @intCast(out);
    defer if (values[out_index]) |ct| cb.free(ct);
    try std.testing.expect(isMetalDeviceResident(&cb, values[out_index].?));
    try std.testing.expect(exec_stats.backend_command_dispatches >= 1);
    try std.testing.expectEqual(@as(u64, 0), exec_stats.interpreter_fallbacks);
    try std.testing.expectEqual(@as(u64, 0), exec_stats.host_materialized_outputs);
    try std.testing.expectEqual(@as(u64, 3), exec_stats.gemma_qkv_hits);
    // Only the fused qkv region is attributed to the per-category gemma hit
    // counters; the standalone planned ops (o_proj, softmax, residual_add) execute
    // as planned commands (counted in planned_operator_dispatches /
    // backend_command_dispatches) and are not per-category attributed. Their device
    // residency is guaranteed by isMetalDeviceResident + host_materialized_outputs
    // == 0 above, and the gemma_*_fallbacks == 0 checks below confirm none of them
    // fell back to a host materialization.
    try std.testing.expectEqual(@as(u64, 0), exec_stats.gemma_qkv_fallbacks);
    try std.testing.expectEqual(@as(u64, 0), exec_stats.gemma_o_proj_fallbacks);
    try std.testing.expectEqual(@as(u64, 0), exec_stats.gemma_softmax_fallbacks);
    try std.testing.expectEqual(@as(u64, 0), exec_stats.gemma_residual_add_fallbacks);

    const raw = try cb.toFloat32(values[out_index].?, allocator);
    defer allocator.free(raw);
    var expected: [rows * dim]f32 = undefined;
    for (0..rows) |row| {
        const base = row * dim;
        var row_max: f32 = -std.math.inf(f32);
        var logits_host: [dim]f32 = undefined;
        for (0..dim) |col| {
            logits_host[col] = input_data[base + col] * 2.0;
            row_max = @max(row_max, logits_host[col]);
        }
        var denom: f32 = 0.0;
        var probs_host: [dim]f32 = undefined;
        for (0..dim) |col| {
            probs_host[col] = @exp(logits_host[col] - row_max);
            denom += probs_host[col];
        }
        for (0..dim) |col| {
            probs_host[col] /= denom;
            expected[base + col] = probs_host[col] + input_data[base + col];
        }
    }
    for (expected, raw) |exp, actual| {
        try std.testing.expectApproxEqAbs(exp, actual, 1e-5);
    }
}

test "metal partition executor quantized qkv projection chain keeps activation transpose resident" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime_mod.metalDeviceAvailable()) return error.SkipZigTest;

    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const rows: usize = 1;
    const dim: usize = 32;
    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ rows, dim }));
    const q_weight_name = "model.layers.0.self_attn.q_proj.weight";
    const k_weight_name = "model.layers.0.self_attn.k_proj.weight";
    const v_weight_name = "model.layers.0.self_attn.v_proj.weight";
    const out_weight_name = "model.layers.0.self_attn.o_proj.weight";
    const q_weight = try b.parameter(q_weight_name, ml.graph.Shape.init(.f32, &.{ dim, dim }));
    const k_weight = try b.parameter(k_weight_name, ml.graph.Shape.init(.f32, &.{ dim, dim }));
    const v_weight = try b.parameter(v_weight_name, ml.graph.Shape.init(.f32, &.{ dim, dim }));
    const out_weight = try b.parameter(out_weight_name, ml.graph.Shape.init(.f32, &.{ dim, dim }));

    const q = try b.linearNoBias(x, q_weight, @intCast(rows), @intCast(dim), @intCast(dim));
    const k = try b.linearNoBias(x, k_weight, @intCast(rows), @intCast(dim), @intCast(dim));
    const v = try b.linearNoBias(x, v_weight, @intCast(rows), @intCast(dim), @intCast(dim));
    const q_3d = try b.reshape(q, ml.graph.Shape.init(.f32, &.{ 1, rows, dim }));
    const q_t = try b.transpose(q_3d, &.{ 0, 2, 1 });
    const q_tt = try b.transpose(q_t, &.{ 0, 2, 1 });
    const q_rope = try g.addNode(.{
        .op = .{ .fused_rope = .{
            .seq_len = rows,
            .head_dim = dim,
            .rope_dim = dim,
            .theta = 10000.0,
            .freq_scale = 1.0,
            .position_offset = 0,
            .consecutive_pairs = false,
        } },
        .output_shape = g.node(q_tt).output_shape,
        .inputs = .{ q_tt, null_node, null_node, null_node },
        .num_inputs = 1,
    });
    const q_back = try b.reshape(q_rope, ml.graph.Shape.init(.f32, &.{ rows, dim }));
    const logits = try b.add(q_back, k);
    const probs = try b.softmax(logits);
    const mixed = try b.add(probs, v);
    const out = try b.linearNoBias(mixed, out_weight, @intCast(rows), @intCast(dim), @intCast(dim));
    try g.markOutput(out);

    const seeds = try partition_mod.allocTensorDescriptorSeeds(allocator, &g);
    defer allocator.free(seeds);
    try partition_mod.seedParameterResidency(seeds, &g, x, .metal, 0);
    try std.testing.expect(try partition_mod.seedParameterQuantFormatByName(seeds, &g, q_weight_name, .q8_0));
    try std.testing.expect(try partition_mod.seedParameterQuantFormatByName(seeds, &g, k_weight_name, .q8_0));
    try std.testing.expect(try partition_mod.seedParameterQuantFormatByName(seeds, &g, v_weight_name, .q8_0));
    try std.testing.expect(try partition_mod.seedParameterQuantFormatByName(seeds, &g, out_weight_name, .q8_0));

    const caps = [_]partition_mod.Capability{
        .{ .backend = .metal, .priority = 20, .decide = &metal_capabilities.decideMetalEagerGraph },
        .{ .backend = .native, .priority = 0, .decide = &partition_mod.decideNative },
    };
    var partition_plan = try partition_mod.partitionWithOptions(allocator, &g, &caps, .{
        .tensor_descs = seeds,
    });
    defer partition_plan.deinit();
    try std.testing.expectEqual(operator_plan_mod.Operator.mul_mv, partition_plan.operatorPlanForNode(q).?.operator());
    try std.testing.expectEqual(operator_plan_mod.Operator.mul_mv, partition_plan.operatorPlanForNode(k).?.operator());
    try std.testing.expectEqual(operator_plan_mod.Operator.mul_mv, partition_plan.operatorPlanForNode(v).?.operator());
    try std.testing.expectEqual(operator_plan_mod.Operator.mul_mv, partition_plan.operatorPlanForNode(out).?.operator());

    var buffer_plan = try buffer_plan_mod.build(allocator, &g, &partition_plan, .{});
    defer buffer_plan.deinit();

    const identity = try allocator.alloc(f32, dim * dim);
    defer allocator.free(identity);
    @memset(identity, 0.0);
    for (0..dim) |i| identity[i * dim + i] = 1.0;

    const q_raw = try quantizeLinearRowsForTest(allocator, .q8_0, identity, dim, dim);
    defer allocator.free(q_raw);
    const k_raw = try quantizeLinearRowsForTest(allocator, .q8_0, identity, dim, dim);
    defer allocator.free(k_raw);
    const v_raw = try quantizeLinearRowsForTest(allocator, .q8_0, identity, dim, dim);
    defer allocator.free(v_raw);
    const out_raw = try quantizeLinearRowsForTest(allocator, .q8_0, identity, dim, dim);
    defer allocator.free(out_raw);
    const weight_shape = [_]i64{ @intCast(dim), @intCast(dim) };

    var weight_store = initEmptyMetalWeightStore(allocator);
    defer deinitEmptyMetalWeightStore(&weight_store, allocator);
    try putTestQuantizedWeight(allocator, &weight_store, q_weight_name, q_raw, &weight_shape, .q8_0);
    try putTestQuantizedWeight(allocator, &weight_store, k_weight_name, k_raw, &weight_shape, .q8_0);
    try putTestQuantizedWeight(allocator, &weight_store, v_weight_name, v_raw, &weight_shape, .q8_0);
    try putTestQuantizedWeight(allocator, &weight_store, out_weight_name, out_raw, &weight_shape, .q8_0);
    metal_compute_mod.initPrefetchQueue(&weight_store, allocator);

    var metal_compute = try metal_compute_mod.MetalCompute.init(allocator, &weight_store, null);
    defer metal_compute.deinit();
    var cb = metal_compute.computeBackend();
    if (!cb.decoderRuntimeReady()) return error.SkipZigTest;

    const count: usize = @intCast(g.nodeCount());
    const values = try allocator.alloc(?CT, count);
    defer allocator.free(values);
    @memset(values, null);
    const value_device = try allocator.alloc(DeviceId, count);
    defer allocator.free(value_device);
    @memset(value_device, 0);

    var input_data: [rows * dim]f32 = undefined;
    for (&input_data, 0..) |*value, idx| value.* = @as(f32, @floatFromInt(idx + 1)) / 64.0;
    const input_ct = try cb.fromFloat32Shape(&input_data, &.{ rows, dim });
    defer cb.free(input_ct);
    const input_dev = (try makeMetalDeviceResident(&cb, input_ct)) orelse return error.SkipZigTest;
    defer cb.free(input_dev);
    const q_weight_ct = try cb.getWeight(q_weight_name);
    defer cb.free(q_weight_ct);
    const k_weight_ct = try cb.getWeight(k_weight_name);
    defer cb.free(k_weight_ct);
    const v_weight_ct = try cb.getWeight(v_weight_name);
    defer cb.free(v_weight_ct);
    const out_weight_ct = try cb.getWeight(out_weight_name);
    defer cb.free(out_weight_ct);

    values[@intCast(x)] = input_dev;
    values[@intCast(q_weight)] = q_weight_ct;
    values[@intCast(k_weight)] = k_weight_ct;
    values[@intCast(v_weight)] = v_weight_ct;
    values[@intCast(out_weight)] = out_weight_ct;

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);

    var exec_stats: PartitionExecutor.ExecutionStats = .{};
    var exec = MetalPartitionExecutor.initBorrowed(allocator, &g, &cb);
    const executed_partitions = try allocator.alloc(bool, partition_plan.partitions.len);
    defer allocator.free(executed_partitions);
    @memset(executed_partitions, false);
    for (0..@intCast(g.nodeCount())) |node_index| {
        const node_id: NodeId = @intCast(node_index);
        const partition_index = partition_plan.node_assignment[node_id];
        if (partition_index >= partition_plan.partitions.len or executed_partitions[partition_index]) continue;
        executed_partitions[partition_index] = true;
        const part = partition_plan.partitions[partition_index];
        var has_reachable_compute_node = false;
        for (part.node_ids) |part_node_id| {
            const part_index: usize = @intCast(part_node_id);
            if (part_index >= reachable.len or !reachable[part_index]) continue;
            switch (g.node(part_node_id).op) {
                .parameter, .constant => {},
                else => has_reachable_compute_node = true,
            }
        }
        if (!has_reachable_compute_node) continue;
        try std.testing.expectEqual(contracts.BackendKind.metal, part.backend);
        try exec.partitionExecutor().execute(values, value_device, part.node_ids, 0, .{
            .allocator = allocator,
            .graph = &g,
            .backend = &cb,
            .options = .{
                .runtime_inputs = &.{
                    .{ .node_id = x, .value = values[@intCast(x)].? },
                    .{ .node_id = q_weight, .value = values[@intCast(q_weight)].? },
                    .{ .node_id = k_weight, .value = values[@intCast(k_weight)].? },
                    .{ .node_id = v_weight, .value = values[@intCast(v_weight)].? },
                    .{ .node_id = out_weight, .value = values[@intCast(out_weight)].? },
                },
            },
            .reachable = reachable,
            .last_use = last_use,
            .partition_plan = &partition_plan,
            .buffer_plan = &buffer_plan,
            .materialize_boundary_outputs = false,
            .stats = &exec_stats,
        });
    }

    const out_index: usize = @intCast(out);
    defer if (values[out_index]) |ct| cb.free(ct);
    try std.testing.expect(isMetalDeviceResident(&cb, values[out_index].?));
    try std.testing.expect(exec_stats.planned_operator_dispatches >= 2);
    try std.testing.expectEqual(@as(u64, 1), exec_stats.metal_qkv_regions);
    try std.testing.expectEqual(@as(u64, 3), exec_stats.graph_region_ops);
    try std.testing.expectEqual(@as(u64, 0), exec_stats.interpreter_fallbacks);
    try std.testing.expectEqual(@as(u64, 0), exec_stats.host_materialized_outputs);
    try std.testing.expectEqual(@as(u64, 3), exec_stats.gemma_qkv_hits);
    // Only the fused qkv region is attributed to the per-category gemma hit
    // counters; the standalone planned ops (o_proj, softmax, residual_add) execute
    // as planned commands (counted in planned_operator_dispatches /
    // backend_command_dispatches) and are not per-category attributed. Their device
    // residency is guaranteed by isMetalDeviceResident + host_materialized_outputs
    // == 0 above, and the gemma_*_fallbacks == 0 checks below confirm none of them
    // fell back to a host materialization.
    try std.testing.expectEqual(@as(u64, 0), exec_stats.gemma_qkv_fallbacks);
    try std.testing.expectEqual(@as(u64, 0), exec_stats.gemma_o_proj_fallbacks);
    try std.testing.expectEqual(@as(u64, 0), exec_stats.gemma_softmax_fallbacks);
    try std.testing.expectEqual(@as(u64, 0), exec_stats.gemma_residual_add_fallbacks);

    const raw = try cb.toFloat32(values[out_index].?, allocator);
    defer allocator.free(raw);
    try std.testing.expectEqual(rows * dim, raw.len);
    for (raw) |value| try std.testing.expect(std.math.isFinite(value));
}
test "metal partition executor command path runs q8 quantized linear" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime_mod.metalDeviceAvailable()) return error.SkipZigTest;

    try expectPlannedQ8LinearOnMetal(9, 32, 2, .mul_mm, .handwritten_production);
}

test "metal partition executor planned q8 linear uses tiled mm shape" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime_mod.metalDeviceAvailable()) return error.SkipZigTest;

    try expectPlannedQ8LinearOnMetal(9, 64, 64, .mul_mm, .handwritten_production);
}

test "metal partition executor planned q8 linear covers mv and small batch buckets" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime_mod.metalDeviceAvailable()) return error.SkipZigTest;

    try expectPlannedQ8LinearOnMetal(1, 32, 8, .mul_mv, .handwritten_production);
    try expectPlannedQ8LinearOnMetal(4, 32, 8, .mul_mv_ext, .generated_production);
}

test "metal partition executor planned quant linears stay packed on metal" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime_mod.metalDeviceAvailable()) return error.SkipZigTest;

    try expectPlannedQuantLinearOnMetal(.q1_0, 1, 128, 8, .mul_mv, .handwritten_production, 2.0, 0.30);
    try expectPlannedQuantLinearOnMetal(.q2_k, 9, 256, 8, .mul_mm, .handwritten_production, 2.0, 0.30);
    try expectPlannedQuantLinearOnMetal(.q3_k, 4, 256, 8, .mul_mv_ext, .generated_production, 1.5, 0.20);
    try expectPlannedQuantLinearOnMetal(.q4_0, 4, 32, 8, .mul_mv_ext, .handwritten_with_wired_candidate, 1.5, 0.15);
    try expectPlannedQuantLinearOnMetal(.q4_1, 9, 32, 8, .mul_mm, .handwritten_production, 1.0, 0.15);
    try expectPlannedQuantLinearOnMetal(.q5_0, 4, 32, 8, .mul_mv_ext, .handwritten_with_wired_candidate, 1.0, 0.15);
    try expectPlannedQuantLinearOnMetal(.q5_1, 9, 32, 8, .mul_mm, .handwritten_production, 1.5, 0.15);
    try expectPlannedQuantLinearOnMetal(.q5_k, 9, 256, 8, .mul_mm, .handwritten_production, 0.6, 0.18);
    try expectPlannedQuantLinearOnMetal(.q8_1, 4, 32, 8, .mul_mv_ext, .handwritten_with_wired_candidate, 0.15, 0.05);
    try expectPlannedQuantLinearOnMetal(.q8_k, 9, 256, 8, .mul_mm, .handwritten_production, 0.15, 0.05);
}

fn expectPlannedQ8LinearOnMetal(rows: usize, in_dim: usize, out_dim: usize, expected_operator: operator_plan_mod.Operator, expected_route: ExpectedQuantRoute) !void {
    try expectPlannedQuantLinearOnMetal(.q8_0, rows, in_dim, out_dim, expected_operator, expected_route, 1e-3, 1e-4);
}

// handwritten_with_wired_candidate: production dispatch is handwritten but a
// generated dev candidate is wired for the route, which the plan counters
// classify as a generated_artifact_missing fast-path miss.
const ExpectedQuantRoute = enum { handwritten_production, handwritten_with_wired_candidate, generated_production };

fn expectPlannedQuantLinearOnMetal(
    format: @import("quant_matmul.zig").Format,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    expected_operator: operator_plan_mod.Operator,
    expected_route: ExpectedQuantRoute,
    tolerance: f32,
    rel_tolerance: f32,
) !void {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = ml.graph.Builder.init(&g);

    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ @as(i64, @intCast(rows)), @as(i64, @intCast(in_dim)) }));
    const w = try b.parameter("w", ml.graph.Shape.init(.f32, &.{ @as(i64, @intCast(out_dim)), @as(i64, @intCast(in_dim)) }));
    const out = try b.linearNoBias(x, w, @intCast(rows), @intCast(in_dim), @intCast(out_dim));
    try g.markOutput(out);

    const seeds = try partition_mod.allocTensorDescriptorSeeds(allocator, &g);
    defer allocator.free(seeds);
    try partition_mod.seedAllParameterResidency(seeds, &g, .metal, 0);
    try std.testing.expect(try partition_mod.seedParameterQuantFormatByName(seeds, &g, "w", format));

    const caps = [_]partition_mod.Capability{
        .{ .backend = .metal, .priority = 20, .decide = &metal_capabilities.decideMetalEagerGraph },
        .{ .backend = .native, .priority = 0, .decide = &partition_mod.decideNative },
    };
    var partition_plan = try partition_mod.partitionWithOptions(allocator, &g, &caps, .{
        .tensor_descs = seeds,
    });
    defer partition_plan.deinit();
    const selected_plan = partition_plan.operatorPlanForNode(out) orelse return error.InvalidPartitionPlan;
    try std.testing.expectEqual(expected_operator, selected_plan.operator());
    var buffer_plan = try buffer_plan_mod.build(allocator, &g, &partition_plan, .{});
    defer buffer_plan.deinit();

    const weight_dense = try allocator.alloc(f32, out_dim * in_dim);
    defer allocator.free(weight_dense);
    for (0..out_dim) |out_col| {
        const scale: f32 = @floatFromInt((out_col % 5) + 1);
        for (0..in_dim) |col| {
            const signed = @as(i32, @intCast((out_col * 17 + col * 11) % 23)) - 11;
            weight_dense[out_col * in_dim + col] = scale * @as(f32, @floatFromInt(signed)) / 7.0;
        }
    }
    const weight_raw = try quantizeLinearRowsForTest(allocator, format, weight_dense, out_dim, in_dim);
    defer allocator.free(weight_raw);
    const weight_shape = [_]i64{ @intCast(out_dim), @intCast(in_dim) };

    var weight_store = initEmptyMetalWeightStore(allocator);
    defer deinitEmptyMetalWeightStore(&weight_store, allocator);
    try putTestQuantizedWeight(allocator, &weight_store, "w", weight_raw, &weight_shape, format);
    metal_compute_mod.initPrefetchQueue(&weight_store, allocator);
    var metal_compute = try metal_compute_mod.MetalCompute.init(allocator, &weight_store, null);
    defer metal_compute.deinit();
    var cb = metal_compute.computeBackend();
    if (!cb.decoderRuntimeReady()) return error.SkipZigTest;

    const count: usize = @intCast(g.nodeCount());
    const values = try allocator.alloc(?CT, count);
    defer allocator.free(values);
    @memset(values, null);
    const value_device = try allocator.alloc(DeviceId, count);
    defer allocator.free(value_device);
    @memset(value_device, 0);

    const x_data = try allocator.alloc(f32, rows * in_dim);
    defer allocator.free(x_data);
    for (0..rows) |row| {
        for (0..in_dim) |col| {
            x_data[row * in_dim + col] = @as(f32, @floatFromInt(row + 1)) * @as(f32, @floatFromInt((col % 7) + 1));
        }
    }

    const x_ct = try cb.fromFloat32Shape(x_data, &.{ @as(i32, @intCast(rows)), @as(i32, @intCast(in_dim)) });
    defer cb.free(x_ct);
    const w_ct = try cb.getWeight("w");
    defer cb.free(w_ct);
    values[@intCast(x)] = x_ct;
    values[@intCast(w)] = w_ct;

    const reachable = try interpreter.computeReachable(allocator, &g);
    defer allocator.free(reachable);
    const last_use = try interpreter.computeLastUse(allocator, &g, reachable);
    defer allocator.free(last_use);

    var planned_exec_stats: PartitionExecutor.ExecutionStats = .{};
    const partition_index = partition_plan.node_assignment[out];
    var exec = MetalPartitionExecutor.initBorrowed(allocator, &g, &cb);
    try exec.partitionExecutor().execute(values, value_device, partition_plan.partitions[partition_index].node_ids, 0, .{
        .allocator = allocator,
        .graph = &g,
        .backend = &cb,
        .options = .{
            .runtime_inputs = &.{
                .{ .node_id = x, .value = x_ct },
                .{ .node_id = w, .value = w_ct },
            },
        },
        .reachable = reachable,
        .last_use = last_use,
        .partition_plan = &partition_plan,
        .buffer_plan = &buffer_plan,
        .stats = &planned_exec_stats,
    });
    try std.testing.expectEqual(@as(u64, 1), planned_exec_stats.planned_operator_dispatches);
    try std.testing.expectEqual(@as(u64, 1), planned_exec_stats.quant_kernel_planned_ops);
    // Promoted generated routes (see quant_kernel_compiler promotion policy)
    // classify as generated_production; everything else stays handwritten.
    switch (expected_route) {
        .handwritten_production, .handwritten_with_wired_candidate => {
            try std.testing.expectEqual(@as(u64, 1), planned_exec_stats.quant_kernel_handwritten_production);
            try std.testing.expectEqual(@as(u64, 0), planned_exec_stats.quant_kernel_generated_production);
        },
        .generated_production => {
            try std.testing.expectEqual(@as(u64, 0), planned_exec_stats.quant_kernel_handwritten_production);
            try std.testing.expectEqual(@as(u64, 1), planned_exec_stats.quant_kernel_generated_production);
        },
    }
    switch (format) {
        .q1_0, .q2_k, .q3_k, .q4_0, .q4_1, .q5_0, .q5_1, .q4_k, .q5_k, .q6_k, .q8_0, .q8_1, .q8_k => {
            const expected_candidate_miss: u64 = if (expected_route == .handwritten_with_wired_candidate) 1 else 0;
            try std.testing.expectEqual(expected_candidate_miss, planned_exec_stats.quant_kernel_fallback_generated_artifact_missing);
            try std.testing.expectEqual(@as(u64, 0), planned_exec_stats.quant_kernel_fallback_unsupported_format);
            try std.testing.expectEqual(@as(u64, 0), planned_exec_stats.quant_kernel_fallback_unsupported);
        },
        else => {
            try std.testing.expectEqual(@as(u64, 0), planned_exec_stats.quant_kernel_fallback_generated_artifact_missing);
            try std.testing.expectEqual(@as(u64, 1), planned_exec_stats.quant_kernel_fallback_unsupported_format);
            try std.testing.expectEqual(@as(u64, 1), planned_exec_stats.quant_kernel_fallback_unsupported);
        },
    }

    const out_index: usize = @intCast(out);
    defer if (values[out_index]) |ct| cb.free(ct);
    const raw = try cb.toFloat32(values[out_index].?, allocator);
    defer allocator.free(raw);
    try std.testing.expectEqual(rows * out_dim, raw.len);
    const dequantized_weight = try allocator.alloc(f32, out_dim * in_dim);
    defer allocator.free(dequantized_weight);
    try quant_codec.dequantizeToFloat32(try quantFormatTensorType(format), weight_raw, dequantized_weight);
    for (0..rows) |row| {
        for (0..out_dim) |out_col| {
            var expected: f32 = 0;
            for (0..in_dim) |col| {
                expected += x_data[row * in_dim + col] * dequantized_weight[out_col * in_dim + col];
            }
            try expectApproxEqAbsOrRel(expected, raw[row * out_dim + out_col], tolerance, rel_tolerance);
        }
    }
}

fn expectApproxEqAbsOrRel(expected: f32, actual: f32, abs_tolerance: f32, rel_tolerance: f32) !void {
    const diff = @abs(expected - actual);
    const rel_base = @max(@abs(expected), @as(f32, 1.0));
    if (diff <= abs_tolerance or diff <= rel_tolerance * rel_base) return;
    std.debug.print("actual {}, not within absolute tolerance {} or relative tolerance {} of expected {}\n", .{
        actual,
        abs_tolerance,
        rel_tolerance,
        expected,
    });
    return error.TestExpectedApproxEq;
}

test "metal partition executor owned lifecycle deinitializes cleanly" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();

    var weight_store = native_compute.WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    defer deinitEmptyNativeWeightStore(&weight_store, allocator);
    var compute = native_compute.NativeCompute.init(allocator, &weight_store, null);
    var cb = compute.computeBackend();

    const exec = try MetalPartitionExecutor.create(allocator, &g, &cb);
    const pe = exec.partitionExecutor();
    pe.deinitExecutor();
}
