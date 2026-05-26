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
const device_mesh_mod = @import("device_mesh.zig");
const gpu_hosted_store_mod = @import("../ops/gpu_hosted_store.zig");
const metal_compute_mod = @import("../ops/metal_compute.zig");
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

const CT = contracts.CT;
const ComputeBackend = ops_mod.ComputeBackend;
const PartitionExecutor = partition_mod.PartitionExecutor;
const DeviceId = device_mesh_mod.DeviceId;
const GraphPlanSlot = ops_mod.GraphPlanSlot;
const QuantizedStorage = weight_source_mod.QuantizedStorage;
const OperatorPlan = operator_plan_mod.OperatorPlan;

const max_graph_plan_slots = 26;

const MetalExecutionKind = enum {
    command,
    metadata_alias,
    descriptor_materialization,
    constant_materialization,
};

const RuntimeRegionKind = enum(u8) {
    none = 0,
    q_linear,
    linear_qkv,
    grouped_linear_qkv_slice,
    rms_norm_grouped_linear_qkv_slice,
    attention_output_residual,
    rms_norm_gated_ffn_residual,
    gated_ffn_residual,
    ple_residual,
};

const RuntimeRegion = union(RuntimeRegionKind) {
    none: void,
    q_linear: QLinearPattern,
    linear_qkv: LinearNoBiasQkvPattern,
    grouped_linear_qkv_slice: GroupedLinearQkvSlicePattern,
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

const PreparedRuntimeRegion = union(RuntimeRegionKind) {
    none: void,
    q_linear: PreparedLinearRegion,
    linear_qkv: PreparedQkvRegion,
    grouped_linear_qkv_slice: PreparedQkvRegion,
    rms_norm_grouped_linear_qkv_slice: PreparedRmsNormGroupedQkvRegion,
    attention_output_residual: PreparedAttentionOutputResidualRegion,
    rms_norm_gated_ffn_residual: PreparedRmsNormGatedFfnResidualRegion,
    gated_ffn_residual: PreparedGatedFfnResidualRegion,
    ple_residual: PreparedPleResidualRegion,
};

const RuntimeRegionPlan = struct {
    node_count: usize = 0,
    value_count: usize = 0,
    first_node: NodeId = null_node,
    last_node: NodeId = null_node,
    regions_by_pos: []RuntimeRegion = &.{},
    prepared_by_pos: []PreparedRuntimeRegion = &.{},
    region_count: usize = 0,

    fn deinit(self: *RuntimeRegionPlan, allocator: std.mem.Allocator) void {
        allocator.free(self.regions_by_pos);
        allocator.free(self.prepared_by_pos);
        self.* = .{};
    }

    fn matches(self: RuntimeRegionPlan, node_ids: []const NodeId, value_count: usize) bool {
        if (self.regions_by_pos.len != node_ids.len) return false;
        if (self.prepared_by_pos.len != node_ids.len) return false;
        if (self.node_count != node_ids.len or self.value_count != value_count) return false;
        if (node_ids.len == 0) return self.first_node == null_node and self.last_node == null_node;
        return self.first_node == node_ids[0] and self.last_node == node_ids[node_ids.len - 1];
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

pub fn isMetalDeviceResident(cb: *const ComputeBackend, tensor: CT) bool {
    if (cb.kind() != .metal) return false;
    if (comptime !build_options.enable_metal) return false;
    return metal_compute_mod.MetalCompute.debugHasDeviceTensor(cb, tensor);
}

fn isMetalResidentOrQuantizedDescriptor(cb: *const ComputeBackend, tensor: CT) bool {
    if (isMetalDeviceResident(cb, tensor)) return true;
    if (cb.kind() != .metal) return false;
    if (comptime !build_options.enable_metal) return false;
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

pub const MetalPartitionExecutor = struct {
    allocator: std.mem.Allocator,
    graph: *const Graph,
    backend: *const ComputeBackend,
    pe: PartitionExecutor = undefined,
    owned: bool = false,
    runtime_region_plan: ?RuntimeRegionPlan = null,

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
        if (self.owned) self.allocator.destroy(self);
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
        const partition_index = try partitionIndexForNodes(buffer_plan, node_ids);
        if (trace_nodes) std.debug.print("graph_executor_node_trace: executor_begin partition={d} nodes={d}\n", .{ partition_index, node_ids.len });

        var partition_view = try buffer_plan.partitionView(allocator, partition_plan, partition_index);
        defer partition_view.deinit(allocator);
        try validatePartitionView(partition_view, node_ids);
        if (trace_nodes) {
            std.debug.print(
                "graph_executor_node_trace: partition_view partition={d} slots={d} transfers_in={d} transfers_out={d}\n",
                .{ partition_index, partition_view.slots.len, partition_view.transfers_in.len, partition_view.transfers_out.len },
            );
        }

        var metal_graph_plan = try buildMetalGraphPlan(allocator, buffer_plan, partition_view);
        defer metal_graph_plan.deinit(allocator);
        if (trace_nodes) printMetalGraphPlanTrace(partition_index, metal_graph_plan);
        _ = try cb.reserveGraphPlanSlots(metal_graph_plan.slots);
        if (trace_nodes) std.debug.print("graph_executor_node_trace: graph_plan_reserved partition={d}\n", .{partition_index});
        if (exec_ctx.stats) |stats| {
            stats.graph_plan_slots_reserved += metal_graph_plan.slots.len;
            for (metal_graph_plan.slots) |slot| stats.graph_plan_bytes_reserved += slot.bytes;
        }

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
        try materializePartitionRuntimeInputs(
            allocator,
            values,
            value_device,
            node_ids,
            device_id,
            effective_exec_ctx,
            cb,
            rt_map,
        );
        if (trace_nodes) std.debug.print("graph_executor_node_trace: materialize_runtime_inputs_end partition={d}\n", .{partition_index});

        if (trace_nodes) std.debug.print("graph_executor_node_trace: materialize_constants_begin partition={d}\n", .{partition_index});
        try materializePartitionConstants(
            graph,
            cb,
            values,
            value_device,
            node_ids,
            reachable,
            device_id,
        );
        if (trace_nodes) std.debug.print("graph_executor_node_trace: materialize_constants_end partition={d}\n", .{partition_index});

        if (trace_nodes) std.debug.print("graph_executor_node_trace: begin_frame_begin partition={d}\n", .{partition_index});
        var frame_active = try cb.decoderRuntimeBeginFrame();
        errdefer if (frame_active) cb.decoderRuntimeCancelFrame() catch {};
        if (trace_nodes) std.debug.print("graph_executor_node_trace: begin_frame_end partition={d} active={}\n", .{ partition_index, frame_active });

        var exec_state = interpreter.ExecState{
            .attention_layer = if (exec_ctx.attention_layer) |layer| layer.* else 0,
            .options = options,
            .last_use = last_use,
            .pair_second = if (exec_ctx.pair_second) |pair| pair.* else null,
        };
        defer exec_state.freeMoeState();

        const skipped_nodes = try allocator.alloc(bool, values.len);
        defer allocator.free(skipped_nodes);
        @memset(skipped_nodes, false);

        var transient_runtime_region_plan: ?RuntimeRegionPlan = null;
        defer if (transient_runtime_region_plan) |*plan| plan.deinit(allocator);
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
        if (exec_ctx.stats) |stats| {
            recordRuntimeFrameEligibilityStats(stats, analyzeRuntimeFrameEligibility(runtime_region_plan));
            if (runtimeFrameMetadataFromPlan(graph, runtime_region_plan) != null) {
                stats.runtime_frame_metadata_ready += 1;
            }
        }

        var node_pos: usize = 0;
        while (node_pos < node_ids.len) : (node_pos += 1) {
            const node_id = node_ids[node_pos];
            const i: usize = @intCast(node_id);
            if (i >= reachable.len or !reachable[i]) continue;
            if (i < skipped_nodes.len and skipped_nodes[i]) continue;

            if (rt_map.contains(node_id)) {
                value_device[i] = device_id;
                continue;
            }

            if (graph.node(node_id).op == .fused_from_float32) continue;

            if (isPreMaterializedConstantOp(graph.node(node_id).op)) {
                if (values[i] != null) {
                    if (exec_ctx.stats) |stats| {
                        stats.constant_materializations += 1;
                        if (isMetalResidentOrQuantizedDescriptor(cb, values[i].?)) {
                            stats.device_resident_outputs += 1;
                        } else {
                            stats.host_materialized_outputs += 1;
                        }
                    }
                    value_device[i] = device_id;
                    continue;
                }
            }

            if (trace_nodes) printMetalNodeTraceBegin(graph, node_id);
            if (trace_nodes) printMetalNodeTraceInputs(graph, cb, values, node_id);

            const op_plan = partition_plan.operatorPlanForNode(node_id);
            var execution_kind: ?MetalExecutionKind = null;
            if (try tryExecutePlannedRuntimeRegion(
                runtime_region_plan.regionAt(node_pos, node_id, node_ids),
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
                execution_kind = .command;
                if (exec_ctx.stats) |stats| stats.runtime_region_plan_dispatches += 1;
            } else if (try tryExecuteFusedMetalGraphPattern(
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
                execution_kind = .command;
            } else if (try tryExecuteMetalCommand(graph, cb, values, node_id, op_plan, &exec_state)) |command_output| {
                values[i] = command_output;
                execution_kind = classifyMetalExecutionKind(graph, cb, values, node_id);
            } else {
                values[i] = try interpreter.executeNode(graph, cb, values, node_id, &exec_state);
            }
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
                if (execution_kind) |kind| {
                    switch (kind) {
                        .command => {
                            stats.backend_command_dispatches += 1;
                            if (op_plan != null) stats.planned_operator_dispatches += 1;
                        },
                        .metadata_alias => stats.metadata_aliases += 1,
                        .descriptor_materialization => stats.descriptor_materializations += 1,
                        .constant_materialization => stats.constant_materializations += 1,
                    }
                } else {
                    stats.interpreter_fallbacks += 1;
                }
                const output_resident = isMetalResidentOrQuantizedDescriptor(cb, values[i].?);
                if (output_resident) {
                    stats.device_resident_outputs += 1;
                } else {
                    stats.host_materialized_outputs += 1;
                }
                recordGemmaRuntimeResidency(stats, graph, node_id, output_resident);
            }
            value_device[i] = device_id;
            const traced_command = if (execution_kind) |kind| kind == .command else false;
            if (trace_nodes) printMetalNodeTraceEnd(graph, cb, node_id, values[i].?, traced_command);

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

            try freeExpiredInputs(
                allocator,
                graph,
                cb,
                values,
                value_device,
                node_id,
                device_id,
                last_use,
                rt_map,
                donated,
                effective_exec_ctx,
            );

            if (i < skipped_nodes.len and skipped_nodes[i]) {
                values[i] = null;
            }
        }

        if (frame_active) {
            try cb.decoderRuntimeSubmitAndWaitFrame();
            frame_active = false;
        }

        if (exec_ctx.materialize_boundary_outputs) {
            if (exec_ctx.stats) |stats| {
                stats.boundary_output_materializations += countPartitionBoundaryOutputs(partition_view);
            }
            try evalPartitionBoundaryOutputs(cb, values, partition_view);
        }

        if (exec_ctx.attention_layer) |layer| layer.* = exec_state.attention_layer;
        if (exec_ctx.pair_second) |pair| pair.* = exec_state.pair_second;
    }
};

fn traceMetalGraphNodesEnabled() bool {
    return platform.env.getenvBoolDefault("TERMITE_GRAPH_EXECUTOR_TRACE_NODES", false);
}

fn traceMetalGraphFusionsEnabled() bool {
    return platform.env.getenvBoolDefault("TERMITE_METAL_TRACE_GRAPH_FUSIONS", false);
}

fn runtimeRegionPlanDisabled() bool {
    return platform.env.getenvBoolDefault("TERMITE_METAL_DISABLE_RUNTIME_REGION_PLAN", false);
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
    const null_values = try allocator.alloc(?CT, value_count);
    defer allocator.free(null_values);
    @memset(null_values, null);

    const skipped = try allocator.alloc(bool, value_count);
    defer allocator.free(skipped);
    @memset(skipped, false);

    var region_count: usize = 0;
    for (node_ids, 0..) |node_id, node_pos| {
        const i: usize = @intCast(node_id);
        if (i >= reachable.len or !reachable[i]) continue;
        if (i < skipped.len and skipped[i]) continue;

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
        .regions_by_pos = regions,
        .prepared_by_pos = prepared,
        .region_count = region_count,
    };
}

fn runtimeFrameLayerShapeFromQkv(region: RuntimeRegion) ?RuntimeFrameLayerShape {
    return switch (region) {
        .q_linear => |pattern| .{ .rows = pattern.rows, .hidden_size = pattern.in_dim, .attention_input_size = pattern.out_dim },
        .linear_qkv => |pattern| .{ .rows = pattern.rows, .hidden_size = pattern.in_dim, .attention_input_size = pattern.q_out_dim },
        .grouped_linear_qkv_slice => |pattern| .{ .rows = pattern.rows, .hidden_size = pattern.in_dim, .attention_input_size = pattern.q_out_dim },
        .rms_norm_grouped_linear_qkv_slice => |pattern| .{ .rows = pattern.qkv.rows, .hidden_size = pattern.qkv.in_dim, .attention_input_size = pattern.qkv.q_out_dim },
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
    if (plan.region_count == 0) return null;

    var phase: enum { qkv, attention, ffn, ple } = .qkv;
    var pending_qkv: ?RuntimeFrameQkvMetadata = null;
    var pending_attention: ?AttentionOutputResidualPattern = null;
    var pending_ffn: RuntimeRegion = .{ .none = {} };
    var rows: usize = 0;
    var hidden_size: usize = 0;
    var num_attention_heads: usize = 0;
    var global_head_dim: usize = 0;
    var ple_hidden_size: usize = 0;
    var activation: ?ops_mod.DecoderRuntimeActivationKind = null;
    var layer_count: usize = 0;

    for (plan.regions_by_pos) |region| {
        switch (region) {
            .none => continue,
            .q_linear, .linear_qkv, .grouped_linear_qkv_slice, .rms_norm_grouped_linear_qkv_slice => {
                if (phase != .qkv) {
                    traceRuntimeFrameMetadataDeclined("qkv_phase", layer_count, pending_qkv, pending_attention, pending_ffn, null);
                    return null;
                }
                pending_qkv = runtimeFrameQkvMetadataFromRegion(graph, region) orelse {
                    traceRuntimeFrameMetadataDeclined("qkv_metadata", layer_count, null, pending_attention, pending_ffn, null);
                    return null;
                };
                phase = .attention;
            },
            .attention_output_residual => |pattern| {
                if (phase != .attention or pending_qkv == null) {
                    traceRuntimeFrameMetadataDeclined("attention_phase", layer_count, pending_qkv, pending_attention, pending_ffn, null);
                    return null;
                }
                pending_attention = pattern;
                phase = .ffn;
            },
            .rms_norm_gated_ffn_residual, .gated_ffn_residual => {
                if (phase != .ffn or pending_attention == null) {
                    traceRuntimeFrameMetadataDeclined("ffn_phase", layer_count, pending_qkv, pending_attention, pending_ffn, null);
                    return null;
                }
                pending_ffn = region;
                phase = .ple;
            },
            .ple_residual => |pattern| {
                if (phase != .ple) {
                    traceRuntimeFrameMetadataDeclined("ple_phase", layer_count, pending_qkv, pending_attention, pending_ffn, pattern);
                    return null;
                }
                const layer = runtimeFrameLayerMetadata(
                    graph,
                    pending_qkv orelse return null,
                    pending_attention orelse return null,
                    pending_ffn,
                    pattern,
                ) orelse {
                    traceRuntimeFrameMetadataDeclined("layer_metadata", layer_count, pending_qkv, pending_attention, pending_ffn, pattern);
                    return null;
                };
                if (layer.layer_index != layer_count) {
                    traceRuntimeFrameMetadataDeclined("layer_index_order", layer_count, pending_qkv, pending_attention, pending_ffn, pattern);
                    return null;
                }
                if (rows == 0) {
                    rows = pattern.rows;
                    hidden_size = layer.hidden_size;
                    ple_hidden_size = layer.ple_hidden_size;
                    activation = layer.activation;
                    num_attention_heads = if (layer.head_dim == 0) return null else layer.attention_input_size / layer.head_dim;
                    global_head_dim = if (layer.shares_kv) layer.head_dim else 0;
                } else {
                    if (rows != pattern.rows or hidden_size != layer.hidden_size) {
                        traceRuntimeFrameMetadataDeclined("frame_shape_mismatch", layer_count, pending_qkv, pending_attention, pending_ffn, pattern);
                        return null;
                    }
                    if (ple_hidden_size != layer.ple_hidden_size) {
                        traceRuntimeFrameMetadataDeclined("ple_size_mismatch", layer_count, pending_qkv, pending_attention, pending_ffn, pattern);
                        return null;
                    }
                    if (activation.? != layer.activation) {
                        traceRuntimeFrameMetadataDeclined("activation_mismatch", layer_count, pending_qkv, pending_attention, pending_ffn, pattern);
                        return null;
                    }
                    const heads = if (layer.head_dim == 0) {
                        traceRuntimeFrameMetadataDeclined("zero_head_dim", layer_count, pending_qkv, pending_attention, pending_ffn, pattern);
                        return null;
                    } else layer.attention_input_size / layer.head_dim;
                    if (num_attention_heads != heads) {
                        traceRuntimeFrameMetadataDeclined("num_heads_mismatch", layer_count, pending_qkv, pending_attention, pending_ffn, pattern);
                        return null;
                    }
                    if (layer.shares_kv) {
                        if (global_head_dim < layer.head_dim) {
                            global_head_dim = layer.head_dim;
                        }
                    }
                }
                layer_count += 1;
                pending_qkv = null;
                pending_attention = null;
                pending_ffn = .{ .none = {} };
                phase = .qkv;
            },
        }
    }

    if (phase != .qkv or layer_count == 0) {
        traceRuntimeFrameMetadataDeclined("final_phase", layer_count, pending_qkv, pending_attention, pending_ffn, null);
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

fn analyzeRuntimeFrameEligibility(plan: RuntimeRegionPlan) RuntimeFrameEligibility {
    if (plan.region_count == 0) return .{ .reason = .no_regions };

    var phase: enum { qkv, attention, ffn, ple } = .qkv;
    var layer_shape: ?RuntimeFrameLayerShape = null;
    var frame_rows: usize = 0;
    var layers: usize = 0;

    for (plan.regions_by_pos) |region| {
        switch (region) {
            .none => continue,
            .q_linear, .linear_qkv, .grouped_linear_qkv_slice, .rms_norm_grouped_linear_qkv_slice => {
                if (phase != .qkv) return .{ .layers = layers, .reason = .non_layer_order };
                layer_shape = runtimeFrameLayerShapeFromQkv(region) orelse return .{ .layers = layers, .reason = .non_layer_order };
                phase = .attention;
            },
            .attention_output_residual => |pattern| {
                if (phase == .qkv) return .{ .layers = layers, .reason = .missing_qkv };
                if (phase != .attention) return .{ .layers = layers, .reason = .non_layer_order };
                const attention_shape = runtimeFrameLayerShapeFromAttention(pattern);
                if (layer_shape) |shape| {
                    if (!runtimeFrameShapesMatch(shape, attention_shape)) return .{ .layers = layers, .reason = .shape_mismatch };
                } else {
                    return .{ .layers = layers, .reason = .missing_qkv };
                }
                layer_shape = attention_shape;
                phase = .ffn;
            },
            .rms_norm_gated_ffn_residual, .gated_ffn_residual => {
                if (phase == .qkv) return .{ .layers = layers, .reason = .missing_qkv };
                if (phase == .attention) return .{ .layers = layers, .reason = .missing_attention };
                if (phase == .ple) return .{ .layers = layers, .reason = .non_layer_order };
                const ffn_shape = runtimeFrameLayerShapeFromFfn(region) orelse return .{ .layers = layers, .reason = .non_layer_order };
                if (layer_shape) |shape| {
                    if (!runtimeFrameShapesMatch(shape, ffn_shape)) return .{ .layers = layers, .reason = .shape_mismatch };
                } else {
                    return .{ .layers = layers, .reason = .missing_attention };
                }
                phase = .ple;
            },
            .ple_residual => |pattern| {
                if (phase == .qkv) return .{ .layers = layers, .reason = .missing_qkv };
                if (phase == .attention) return .{ .layers = layers, .reason = .missing_attention };
                if (phase == .ffn) return .{ .layers = layers, .reason = .missing_ffn };
                const ple_shape = runtimeFrameLayerShapeFromPle(pattern);
                if (layer_shape) |shape| {
                    if (!runtimeFrameShapesMatch(shape, ple_shape)) return .{ .layers = layers, .reason = .shape_mismatch };
                } else {
                    return .{ .layers = layers, .reason = .missing_attention };
                }
                if (frame_rows == 0) frame_rows = ple_shape.rows;
                layers += 1;
                layer_shape = null;
                phase = .qkv;
            },
        }
    }

    if (layers == 0 and phase == .qkv) return .{ .reason = .no_regions };
    if (phase == .attention) return .{ .layers = layers, .reason = .missing_attention };
    if (phase == .ffn) return .{ .layers = layers, .reason = .missing_ffn };
    if (phase == .ple) return .{ .layers = layers, .reason = .missing_ple };
    if (frame_rows <= 1) return .{ .layers = layers, .reason = .single_row };

    // The current graph plan proves layer structure, but not yet the model
    // metadata needed by the whole-frame decoder runtime: head layout, RoPE/KV
    // policy, PLE vectors, and stable per-layer slot numbering.
    return .{ .layers = layers, .reason = .missing_model_metadata };
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

fn markGroupedLinearQkvSkipped(skipped: []bool, pattern: GroupedLinearQkvSlicePattern) void {
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
    last_use: []const u32,
    rt_map: std.AutoHashMapUnmanaged(NodeId, CT),
    donated: std.AutoHashMapUnmanaged(NodeId, void),
) !bool {
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
        .q_linear => 1,
        .linear_qkv, .grouped_linear_qkv_slice => 3,
        .rms_norm_grouped_linear_qkv_slice => 4,
        .attention_output_residual => |slots| 1 +
            @as(u64, if (slots.pre_linear_rms_norm_slot != null) 1 else 0) +
            @as(u64, if (slots.post_linear_rms_norm_slot != null) 1 else 0),
        .rms_norm_gated_ffn_residual => |slots| 1 + preparedRuntimeRegionSlotCount(.{ .gated_ffn_residual = slots.ffn }),
        .gated_ffn_residual => |slots| 3 + @as(u64, if (slots.post_down_rms_norm_slot != null) 1 else 0),
        .ple_residual => 3,
    };
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

fn prepareRuntimeRegion(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
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
        .q_linear => |pattern| .{
            .q_linear = (try prepareQLinearRegion(cb, values, pattern, stats)) orelse return null,
        },
        .linear_qkv => |pattern| .{
            .linear_qkv = (try prepareLinearNoBiasQkvRegion(cb, values, pattern, stats)) orelse return null,
        },
        .grouped_linear_qkv_slice => |pattern| .{
            .grouped_linear_qkv_slice = (try prepareGroupedLinearQkvSliceRegion(cb, values, pattern, stats)) orelse return null,
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
    const prepared = (try prepareRuntimeRegion(graph, cb, values, region, prepared_region, exec_ctx.stats)) orelse {
        if (std.meta.activeTag(region) != .none) {
            if (exec_ctx.stats) |stats| stats.runtime_region_fallbacks += 1;
        }
        return false;
    };
    return switch (region) {
        .none => false,
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
    if (output == null) return false;
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

    try freeExpiredInputs(
        allocator,
        graph,
        cb,
        values,
        value_device,
        pattern.norm_id,
        device_id,
        last_use,
        rt_map,
        donated,
        exec_ctx,
    );
    try freeExpiredInputs(
        allocator,
        graph,
        cb,
        values,
        value_device,
        pattern.qkv.linear_id,
        device_id,
        last_use,
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
        }
        if (v_resident) {
            stats.device_resident_outputs += 1;
        } else {
            stats.host_materialized_outputs += 1;
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
        }
        if (v_resident) {
            stats.device_resident_outputs += 1;
        } else {
            stats.host_materialized_outputs += 1;
        }
    }

    try freeExpiredInputs(
        allocator,
        graph,
        cb,
        values,
        value_device,
        pattern.linear_id,
        device_id,
        last_use,
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
        const k_resident = isMetalResidentOrQuantizedDescriptor(cb, qkv.second);
        const v_resident = isMetalResidentOrQuantizedDescriptor(cb, qkv.third);
        if (k_resident) {
            stats.device_resident_outputs += 1;
        } else {
            stats.host_materialized_outputs += 1;
        }
        if (v_resident) {
            stats.device_resident_outputs += 1;
        } else {
            stats.host_materialized_outputs += 1;
        }
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
    try freeExpiredInputs(
        allocator,
        graph,
        cb,
        values,
        value_device,
        pattern.k_id,
        device_id,
        last_use,
        rt_map,
        donated,
        exec_ctx,
    );
    try freeExpiredInputs(
        allocator,
        graph,
        cb,
        values,
        value_device,
        pattern.v_id,
        device_id,
        last_use,
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
    try freeExpiredInputs(
        allocator,
        graph,
        cb,
        values,
        value_device,
        second_id,
        device_id,
        last_use,
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
        return parseGemmaLayerIndex(graph.parameterName(weight));
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

fn executeRuntimeUnary(
    cb: *const ComputeBackend,
    values: []?CT,
    inputs: []const NodeId,
    op: RuntimeUnaryOp,
) !?CT {
    const input = valueFor(values, inputs[0]) orelse return null;
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
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    node_id: NodeId,
    op_plan: ?OperatorPlan,
    exec_state: *interpreter.ExecState,
) !?CT {
    const n = graph.node(node_id);
    const inputs = n.getInputs();
    return switch (n.op) {
        .constant => |attrs| try executeRuntimeConstant(graph, cb, n.output_shape, attrs),
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
            const input = valueFor(values, inputs[0]) orelse break :blk null;
            var in_shape_buf: [ml.graph.shape.max_rank]i64 = undefined;
            const in_shape = try fillShapeDims(graph.node(inputs[0]).output_shape, &in_shape_buf);
            var perm_buf: [ml.graph.shape.max_rank]u8 = undefined;
            const perm = transpose_utils.effectivePerm(attrs, graph.node(inputs[0]).output_shape.rank(), &perm_buf);
            const transposed = cb.primTranspose(input, perm, in_shape) catch |err| switch (err) {
                error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => null,
                else => return err,
            };
            if (transposed) |ct| {
                if (!isMetalDeviceResident(cb, ct)) {
                    if (try makeMetalDeviceResident(cb, ct)) |device_ct| {
                        cb.free(ct);
                        break :blk device_ct;
                    }
                }
            }
            break :blk transposed;
        },
        .broadcast_in_dim => |attrs| try executeRuntimeBroadcast(cb, values, inputs, graph.node(inputs[0]).output_shape, attrs),
        .neg => try executeRuntimeUnary(cb, values, inputs, .negate),
        .sqrt => try executeRuntimeUnary(cb, values, inputs, .sqrt),
        .rsqrt => try executeRuntimeUnary(cb, values, inputs, .rsqrt),
        .exp => try executeRuntimeUnary(cb, values, inputs, .exp),
        .log => try executeRuntimeUnary(cb, values, inputs, .log),
        .sin => try executeRuntimeUnary(cb, values, inputs, .sin),
        .cos => try executeRuntimeUnary(cb, values, inputs, .cos),
        .tanh => try executeRuntimeUnary(cb, values, inputs, .tanh),
        .erf => try executeRuntimeUnary(cb, values, inputs, .erf),
        .abs => try executeRuntimeUnary(cb, values, inputs, .abs),
        .slice => |attrs| try executeRuntimeSlice(graph, cb, values, inputs, attrs),
        .concat_prim => |attrs| try executeRuntimeConcatPrim(graph, cb, values, inputs, attrs),
        .fused_gelu => try executeRuntimeActivation(cb, values, inputs, .gelu, n.output_shape),
        .fused_relu => try executeRuntimeActivation(cb, values, inputs, .relu, n.output_shape),
        .fused_silu => try executeRuntimeActivation(cb, values, inputs, .silu, n.output_shape),
        .fused_quick_gelu => try executeRuntimeActivation(cb, values, inputs, .quick_gelu, n.output_shape),
        .fused_sigmoid => try executeRuntimeFusedUnary(cb, values, inputs, .sigmoid),
        .fused_tanh_act => try executeRuntimeFusedUnary(cb, values, inputs, .tanh_act),
        .fused_elem_add, .add => try executeRuntimeAdd(cb, values, inputs, n.output_shape),
        .fused_elem_multiply, .mul => try executeRuntimeBinary(cb, values, inputs, .multiply),
        .sub => try executeRuntimeBinary(cb, values, inputs, .subtract),
        .div => try executeRuntimeBinary(cb, values, inputs, .divide),
        .less_than => try executeRuntimeBinary(cb, values, inputs, .less_than),
        .where_select => try executeRuntimeWhereSelect(cb, values, inputs),
        .reduce_sum => |attrs| try executeRuntimeReduce(graph, cb, values, inputs, attrs, .sum),
        .reduce_max => |attrs| try executeRuntimeReduce(graph, cb, values, inputs, attrs, .max),
        .reduce_mean => |attrs| try executeRuntimeReduce(graph, cb, values, inputs, attrs, .mean),
        .fused_softmax => |attrs| try executeRuntimeSoftmax(cb, values, inputs, attrs.dim),
        .fused_log_softmax => |attrs| try executeRuntimeLogSoftmax(cb, values, inputs, attrs.dim),
        .fused_sdpa => |attrs| try executeRuntimeSdpa(cb, values, inputs, attrs, op_plan, exec_state),
        .fused_gqa_causal_attention => |attrs| try executeRuntimeGqaCausalAttention(cb, values, inputs, attrs, n.num_inputs, exec_state),
        .dot_general => |attrs| try executeRuntimeDotGeneral(graph, cb, values, inputs, attrs, op_plan),
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
) !?CT {
    const input = valueFor(values, inputs[0]) orelse return null;
    var in_shape_buf: [ml.graph.shape.max_rank]i64 = undefined;
    const in_shape = try fillShapeDims(graph.node(inputs[0]).output_shape, &in_shape_buf);
    const axes = attrs.axes[0..attrs.num_axes];
    return switch (op) {
        .sum => cb.primReduceSum(input, axes, in_shape),
        .max => cb.primReduceMax(input, axes, in_shape),
        .mean => cb.primReduceMean(input, axes, in_shape),
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
    cb: *const ComputeBackend,
    values: []?CT,
    inputs: []const NodeId,
    op: RuntimeBinaryOp,
) !?CT {
    const lhs = valueFor(values, inputs[0]) orelse return null;
    const rhs = valueFor(values, inputs[1]) orelse return null;
    return switch (op) {
        .multiply => cb.multiply(lhs, rhs),
        .subtract => cb.primSubtract(lhs, rhs),
        .divide => cb.primDivide(lhs, rhs),
        .less_than => cb.primLessThan(lhs, rhs),
    } catch |err| switch (err) {
        error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => null,
        else => return err,
    };
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
        return cb.linearWithPlan(input, weight, bias, rows, in_dim, out_dim, op_plan) catch |err| switch (err) {
            error.UnsupportedOperation, error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => null,
            else => return err,
        };
    } else {
        return cb.linearNoBiasWithPlan(input, weight, rows, in_dim, out_dim, op_plan) catch |err| switch (err) {
            error.UnsupportedOperation, error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => null,
            else => return err,
        };
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

fn executeRuntimeDotGeneral(
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    inputs: []const NodeId,
    attrs: anytype,
    op_plan: ?OperatorPlan,
) !?CT {
    const lhs = valueFor(values, inputs[0]) orelse return null;
    const rhs = valueFor(values, inputs[1]) orelse return null;
    var lhs_shape_buf: [ml.graph.shape.max_rank]i64 = undefined;
    var rhs_shape_buf: [ml.graph.shape.max_rank]i64 = undefined;
    const lhs_shape = try fillShapeDims(graph.node(inputs[0]).output_shape, &lhs_shape_buf);
    const rhs_shape = try fillShapeDims(graph.node(inputs[1]).output_shape, &rhs_shape_buf);
    const lhs_contracting = attrs.lhs_contracting[0..attrs.num_contracting];
    const rhs_contracting = attrs.rhs_contracting[0..attrs.num_contracting];
    const lhs_batch = attrs.lhs_batch[0..attrs.num_batch];
    const rhs_batch = attrs.rhs_batch[0..attrs.num_batch];
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
    if (output) |ct| {
        if (!isMetalDeviceResident(cb, ct)) {
            if (try makeMetalDeviceResident(cb, ct)) |device_ct| {
                cb.free(ct);
                return device_ct;
            }
        }
    }
    return output;
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
) !?CT {
    const input = valueFor(values, inputs[0]) orelse return null;
    const dim = tensorElementCount(output_shape) orelse return null;
    if (try cb.decoderRuntimeApplyActivation(&.{
        .input = input,
        .kind = kind,
        .dim = dim,
    })) |result| return result;
    return switch (kind) {
        .gelu => cb.gelu(input),
        .relu => cb.relu(input),
        .silu => cb.silu(input),
        .quick_gelu => cb.quickGelu(input),
        else => return null,
    } catch |err| switch (err) {
        error.UnsupportedOperation, error.UnsupportedPrimitiveOp, error.UnsupportedShape, error.ShapeMismatch => null,
        else => return err,
    };
}

fn executeRuntimeAdd(
    cb: *const ComputeBackend,
    values: []?CT,
    inputs: []const NodeId,
    output_shape: ml.graph.Shape,
) !?CT {
    const lhs = valueFor(values, inputs[0]) orelse return null;
    const rhs = valueFor(values, inputs[1]) orelse return null;
    const dim = tensorElementCount(output_shape) orelse return null;
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
        if (current_dev != device_id) {
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
        if (values[i]) |current| {
            if (!isMetalDeviceResident(cb, current)) {
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

fn isPreMaterializedConstantOp(op: ml.graph.OpCode) bool {
    return switch (op) {
        .constant, .fused_zero_tensor => true,
        else => false,
    };
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

fn freeExpiredInputs(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    cb: *const ComputeBackend,
    values: []?CT,
    value_device: []DeviceId,
    node_id: NodeId,
    device_id: DeviceId,
    last_use: []const u32,
    rt_map: std.AutoHashMapUnmanaged(NodeId, CT),
    donated: std.AutoHashMapUnmanaged(NodeId, void),
    exec_ctx: PartitionExecutor.ExecutionContext,
) !void {
    const n = graph.node(node_id);
    const node_index: usize = @intCast(node_id);
    var released = std.AutoHashMapUnmanaged(usize, void).empty;
    defer released.deinit(allocator);
    for (n.getInputs()) |input_id| {
        if (input_id == null_node or input_id >= values.len) continue;
        const input_index: usize = @intCast(input_id);
        if (last_use[input_index] != node_index) continue;
        if (rt_map.contains(input_id) and
            !donated.contains(input_id) and
            !ownedRuntimeTransferContains(exec_ctx, input_id)) continue;
        const ct = values[input_index] orelse continue;
        if (values[node_index]) |out_ct| {
            if (ct == out_ct and interpreter.canKeepAliasedOutput(n.op)) {
                values[input_index] = null;
                continue;
            }
        }
        const ct_key = @intFromPtr(ct);
        if (released.contains(ct_key)) {
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
        values[input_index] = null;
    }
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

test "metal partition executor resident multi op chain matches host" {
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
    try std.testing.expectEqual(@as(u64, 1), stats.decoder_runtime_frame_begins);
    try std.testing.expectEqual(@as(u64, 1), stats.decoder_runtime_frame_submits);
    try std.testing.expect(exec_stats.runtime_input_transfers >= 3);
    try std.testing.expect(exec_stats.device_resident_transfers >= 3);
    try std.testing.expect(exec_stats.backend_command_dispatches >= 4);
    try std.testing.expectEqual(@as(u64, 0), exec_stats.interpreter_fallbacks);
    try std.testing.expectEqual(@as(u64, 0), exec_stats.host_materialized_outputs);
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

    try std.testing.expect(metal_compute_mod.MetalCompute.debugHasDeviceTensor(&cb, values[out_index].?));

    const raw = try cb.toFloat32(values[out_index].?, allocator);
    defer allocator.free(raw);
    try std.testing.expectEqual(dim, raw.len);
    for (expected, raw) |exp, actual| {
        try std.testing.expectApproxEqAbs(exp, actual, 1e-5);
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

    const eligibility = analyzeRuntimeFrameEligibility(plan);
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
    const single_row_eligibility = analyzeRuntimeFrameEligibility(plan);
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

    const eligibility = analyzeRuntimeFrameEligibility(plan);
    try std.testing.expectEqual(@as(usize, 0), eligibility.layers);
    try std.testing.expectEqual(RuntimeFrameIneligibleReason.missing_ple, eligibility.reason);
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
    const out = (try tryExecuteMetalCommand(&g, &cb, values, sum, null, &exec_state)) orelse return error.UnsupportedPrimitiveOp;
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
    const out = (try tryExecuteMetalCommand(&g, &cb, values, normed, null, &exec_state)) orelse return error.UnsupportedPrimitiveOp;
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
        .q4_0 => .{ .known = .Q4_0 },
        .q4_1 => .{ .known = .Q4_1 },
        .q5_k => .{ .known = .Q5_K },
        .q8_0 => .{ .known = .Q8_0 },
        else => error.UnsupportedTensorType,
    };
}

fn quantizedBlockSize(format: @import("quant_matmul.zig").Format) !struct { values: usize, bytes: usize } {
    return switch (format) {
        .q4_0 => .{ .values = 32, .bytes = 18 },
        .q4_1 => .{ .values = 32, .bytes = 20 },
        .q5_k => .{ .values = 256, .bytes = 176 },
        .q8_0 => .{ .values = 32, .bytes = 34 },
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
                .q4_0 => quant_codec.quantizeQ4_0Block(src, dst),
                .q4_1 => quant_codec.quantizeQ4_1Block(src, dst),
                .q5_k => quant_codec.quantizeQ5_KBlock(src, dst),
                .q8_0 => quant_codec.quantizeQ8_0Block(src, dst),
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
    try std.testing.expectEqual(@as(u64, 1), exec_stats.gemma_o_proj_hits);
    try std.testing.expectEqual(@as(u64, 1), exec_stats.gemma_softmax_hits);
    try std.testing.expectEqual(@as(u64, 2), exec_stats.gemma_residual_add_hits);
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
    try std.testing.expectEqual(@as(u64, 1), exec_stats.gemma_o_proj_hits);
    try std.testing.expectEqual(@as(u64, 1), exec_stats.gemma_softmax_hits);
    try std.testing.expectEqual(@as(u64, 2), exec_stats.gemma_residual_add_hits);
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

    try expectPlannedQ8LinearOnMetal(9, 32, 2, .mul_mm);
}

test "metal partition executor planned q8 linear uses tiled mm shape" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime_mod.metalDeviceAvailable()) return error.SkipZigTest;

    try expectPlannedQ8LinearOnMetal(9, 64, 64, .mul_mm);
}

test "metal partition executor planned q8 linear covers mv and small batch buckets" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime_mod.metalDeviceAvailable()) return error.SkipZigTest;

    try expectPlannedQ8LinearOnMetal(1, 32, 8, .mul_mv);
    try expectPlannedQ8LinearOnMetal(4, 32, 8, .mul_mv_ext);
}

test "metal partition executor planned q4 and q5k linear stay packed on metal" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime_mod.metalDeviceAvailable()) return error.SkipZigTest;

    try expectPlannedQuantLinearOnMetal(.q4_0, 4, 32, 8, .mul_mv_ext, 0.35);
    try expectPlannedQuantLinearOnMetal(.q4_1, 9, 32, 8, .mul_mm, 0.35);
    try expectPlannedQuantLinearOnMetal(.q5_k, 9, 256, 8, .mul_mm, 0.18);
}

fn expectPlannedQ8LinearOnMetal(rows: usize, in_dim: usize, out_dim: usize, expected_operator: operator_plan_mod.Operator) !void {
    try expectPlannedQuantLinearOnMetal(.q8_0, rows, in_dim, out_dim, expected_operator, 1e-3);
}

fn expectPlannedQuantLinearOnMetal(
    format: @import("quant_matmul.zig").Format,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    expected_operator: operator_plan_mod.Operator,
    tolerance: f32,
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
            try std.testing.expectApproxEqAbs(expected, raw[row * out_dim + out_col], tolerance);
        }
    }
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
