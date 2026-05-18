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
const build_options = @import("build_options");
const ml = @import("ml");

const contracts = @import("backend_contracts.zig");
const partition_mod = @import("partition.zig");

const Graph = ml.graph.Graph;
const BackendKind = contracts.BackendKind;
pub const ExecutionStats = partition_mod.PartitionExecutor.ExecutionStats;

const AtomicU64 = std.atomic.Value(u64);

var aggregate_partitions_executed: AtomicU64 = .init(0);
var aggregate_cross_device_transfers: AtomicU64 = .init(0);
var aggregate_runtime_input_transfers: AtomicU64 = .init(0);
var aggregate_device_resident_transfers: AtomicU64 = .init(0);
var aggregate_backend_command_dispatches: AtomicU64 = .init(0);
var aggregate_planned_operator_dispatches: AtomicU64 = .init(0);
var aggregate_interpreter_fallbacks: AtomicU64 = .init(0);
var aggregate_device_resident_outputs: AtomicU64 = .init(0);
var aggregate_host_materialized_outputs: AtomicU64 = .init(0);
var aggregate_boundary_output_materializations: AtomicU64 = .init(0);
var aggregate_graph_plan_slots_reserved: AtomicU64 = .init(0);
var aggregate_graph_plan_bytes_reserved: AtomicU64 = .init(0);

pub fn record(stats: ExecutionStats) void {
    _ = aggregate_partitions_executed.fetchAdd(stats.partitions_executed, .monotonic);
    _ = aggregate_cross_device_transfers.fetchAdd(stats.cross_device_transfers, .monotonic);
    _ = aggregate_runtime_input_transfers.fetchAdd(stats.runtime_input_transfers, .monotonic);
    _ = aggregate_device_resident_transfers.fetchAdd(stats.device_resident_transfers, .monotonic);
    _ = aggregate_backend_command_dispatches.fetchAdd(stats.backend_command_dispatches, .monotonic);
    _ = aggregate_planned_operator_dispatches.fetchAdd(stats.planned_operator_dispatches, .monotonic);
    _ = aggregate_interpreter_fallbacks.fetchAdd(stats.interpreter_fallbacks, .monotonic);
    _ = aggregate_device_resident_outputs.fetchAdd(stats.device_resident_outputs, .monotonic);
    _ = aggregate_host_materialized_outputs.fetchAdd(stats.host_materialized_outputs, .monotonic);
    _ = aggregate_boundary_output_materializations.fetchAdd(stats.boundary_output_materializations, .monotonic);
    _ = aggregate_graph_plan_slots_reserved.fetchAdd(stats.graph_plan_slots_reserved, .monotonic);
    _ = aggregate_graph_plan_bytes_reserved.fetchAdd(stats.graph_plan_bytes_reserved, .monotonic);
}

pub fn snapshot() ExecutionStats {
    return .{
        .partitions_executed = aggregate_partitions_executed.load(.monotonic),
        .cross_device_transfers = aggregate_cross_device_transfers.load(.monotonic),
        .runtime_input_transfers = aggregate_runtime_input_transfers.load(.monotonic),
        .device_resident_transfers = aggregate_device_resident_transfers.load(.monotonic),
        .backend_command_dispatches = aggregate_backend_command_dispatches.load(.monotonic),
        .planned_operator_dispatches = aggregate_planned_operator_dispatches.load(.monotonic),
        .interpreter_fallbacks = aggregate_interpreter_fallbacks.load(.monotonic),
        .device_resident_outputs = aggregate_device_resident_outputs.load(.monotonic),
        .host_materialized_outputs = aggregate_host_materialized_outputs.load(.monotonic),
        .boundary_output_materializations = aggregate_boundary_output_materializations.load(.monotonic),
        .graph_plan_slots_reserved = aggregate_graph_plan_slots_reserved.load(.monotonic),
        .graph_plan_bytes_reserved = aggregate_graph_plan_bytes_reserved.load(.monotonic),
    };
}

pub fn reset() void {
    aggregate_partitions_executed.store(0, .monotonic);
    aggregate_cross_device_transfers.store(0, .monotonic);
    aggregate_runtime_input_transfers.store(0, .monotonic);
    aggregate_device_resident_transfers.store(0, .monotonic);
    aggregate_backend_command_dispatches.store(0, .monotonic);
    aggregate_planned_operator_dispatches.store(0, .monotonic);
    aggregate_interpreter_fallbacks.store(0, .monotonic);
    aggregate_device_resident_outputs.store(0, .monotonic);
    aggregate_host_materialized_outputs.store(0, .monotonic);
    aggregate_boundary_output_materializations.store(0, .monotonic);
    aggregate_graph_plan_slots_reserved.store(0, .monotonic);
    aggregate_graph_plan_bytes_reserved.store(0, .monotonic);
}

pub fn enabled() bool {
    if (!build_options.link_libc) return false;
    const value = std.c.getenv("TERMITE_GRAPH_EXECUTOR_STATS") orelse return false;
    const slice = std.mem.span(value);
    return slice.len > 0 and !std.mem.eql(u8, slice, "0");
}

pub fn print(stats: ExecutionStats) void {
    std.debug.print(
        "graph_executor_stats: partitions={d} transfers={d} runtime_input_transfers={d} device_resident_transfers={d} commands={d} planned_commands={d} metadata_aliases={d} descriptor_materializations={d} constant_materializations={d} graph_regions={d} graph_region_ops={d} graph_region_fallbacks={d} fused_patterns={d} fused_nodes_elided={d} interpreter_fallbacks={d} device_outputs={d} host_outputs={d} boundary_materializations={d} graph_plan_slots={d} graph_plan_bytes={d} runtime_plan_compiles={d} runtime_plan_regions={d} runtime_plan_dispatches={d} runtime_plan_reuses={d} runtime_prepare_slot_calls={d} runtime_prepare_slot_cache_hits={d} runtime_region_fallbacks={d}",
        .{
            stats.partitions_executed,
            stats.cross_device_transfers,
            stats.runtime_input_transfers,
            stats.device_resident_transfers,
            stats.backend_command_dispatches,
            stats.planned_operator_dispatches,
            stats.metadata_aliases,
            stats.descriptor_materializations,
            stats.constant_materializations,
            stats.graph_regions,
            stats.graph_region_ops,
            stats.graph_region_fallbacks,
            stats.fused_graph_pattern_dispatches,
            stats.fused_graph_nodes_elided,
            stats.interpreter_fallbacks,
            stats.device_resident_outputs,
            stats.host_materialized_outputs,
            stats.boundary_output_materializations,
            stats.graph_plan_slots_reserved,
            stats.graph_plan_bytes_reserved,
            stats.runtime_region_plan_compiles,
            stats.runtime_region_plan_regions,
            stats.runtime_region_plan_dispatches,
            stats.runtime_region_plan_reuses,
            stats.runtime_prepare_slot_calls,
            stats.runtime_prepare_slot_cache_hits,
            stats.runtime_region_fallbacks,
        },
    );
    std.debug.print(
        " runtime_frame_candidates={d} runtime_frame_eligible={d} runtime_frame_metadata_ready={d} runtime_frame_ineligible_no_regions={d} runtime_frame_ineligible_missing_qkv={d} runtime_frame_ineligible_missing_attention={d} runtime_frame_ineligible_missing_ffn={d} runtime_frame_ineligible_missing_ple={d} runtime_frame_ineligible_single_row={d} runtime_frame_ineligible_non_layer_order={d} runtime_frame_ineligible_shape_mismatch={d} runtime_frame_ineligible_missing_model_metadata={d}\n",
        .{
            stats.runtime_frame_candidates,
            stats.runtime_frame_eligible,
            stats.runtime_frame_metadata_ready,
            stats.runtime_frame_ineligible_no_regions,
            stats.runtime_frame_ineligible_missing_qkv,
            stats.runtime_frame_ineligible_missing_attention,
            stats.runtime_frame_ineligible_missing_ffn,
            stats.runtime_frame_ineligible_missing_ple,
            stats.runtime_frame_ineligible_single_row,
            stats.runtime_frame_ineligible_non_layer_order,
            stats.runtime_frame_ineligible_shape_mismatch,
            stats.runtime_frame_ineligible_missing_model_metadata,
        },
    );
    if (hasMetalFusionStats(stats)) {
        std.debug.print(
            "metal_graph_fusions: qkv_regions={d} attention_regions={d} ffn_regions={d} ple_regions={d} tail_regions={d} attention_output_residual={d} attention_output_residual_partial={d} gated_ffn_residual={d} linear_pair={d}\n",
            .{
                stats.metal_qkv_regions,
                stats.metal_attention_regions,
                stats.metal_ffn_regions,
                stats.metal_ple_regions,
                stats.metal_tail_regions,
                stats.metal_attention_output_residual_fusions,
                stats.metal_attention_output_residual_partial_fallbacks,
                stats.metal_gated_ffn_residual_fusions,
                stats.metal_linear_pair_fusions,
            },
        );
    }
    if (hasGemmaRuntimeResidencyStats(stats)) {
        std.debug.print(
            "gemma_runtime_residency: qkv_hits={d} qkv_fallbacks={d} o_proj_hits={d} o_proj_fallbacks={d} mlp_proj_hits={d} mlp_proj_fallbacks={d} attention_matmul_hits={d} attention_matmul_fallbacks={d} rms_norm_hits={d} rms_norm_fallbacks={d} softmax_hits={d} softmax_fallbacks={d} residual_add_hits={d} residual_add_fallbacks={d} elementwise_mul_hits={d} elementwise_mul_fallbacks={d}\n",
            .{
                stats.gemma_qkv_hits,
                stats.gemma_qkv_fallbacks,
                stats.gemma_o_proj_hits,
                stats.gemma_o_proj_fallbacks,
                stats.gemma_mlp_proj_hits,
                stats.gemma_mlp_proj_fallbacks,
                stats.gemma_attention_matmul_hits,
                stats.gemma_attention_matmul_fallbacks,
                stats.gemma_rms_norm_hits,
                stats.gemma_rms_norm_fallbacks,
                stats.gemma_softmax_hits,
                stats.gemma_softmax_fallbacks,
                stats.gemma_residual_add_hits,
                stats.gemma_residual_add_fallbacks,
                stats.gemma_elementwise_mul_hits,
                stats.gemma_elementwise_mul_fallbacks,
            },
        );
    }
}

fn hasMetalFusionStats(stats: ExecutionStats) bool {
    return stats.graph_regions != 0 or
        stats.graph_region_fallbacks != 0 or
        stats.metal_qkv_regions != 0 or
        stats.metal_attention_regions != 0 or
        stats.metal_ffn_regions != 0 or
        stats.metal_ple_regions != 0 or
        stats.metal_tail_regions != 0 or
        stats.metal_attention_output_residual_fusions != 0 or
        stats.metal_attention_output_residual_partial_fallbacks != 0 or
        stats.metal_gated_ffn_residual_fusions != 0 or
        stats.metal_linear_pair_fusions != 0;
}

fn hasGemmaRuntimeResidencyStats(stats: ExecutionStats) bool {
    return stats.gemma_qkv_hits != 0 or
        stats.gemma_qkv_fallbacks != 0 or
        stats.gemma_o_proj_hits != 0 or
        stats.gemma_o_proj_fallbacks != 0 or
        stats.gemma_mlp_proj_hits != 0 or
        stats.gemma_mlp_proj_fallbacks != 0 or
        stats.gemma_attention_matmul_hits != 0 or
        stats.gemma_attention_matmul_fallbacks != 0 or
        stats.gemma_rms_norm_hits != 0 or
        stats.gemma_rms_norm_fallbacks != 0 or
        stats.gemma_softmax_hits != 0 or
        stats.gemma_softmax_fallbacks != 0 or
        stats.gemma_residual_add_hits != 0 or
        stats.gemma_residual_add_fallbacks != 0 or
        stats.gemma_elementwise_mul_hits != 0 or
        stats.gemma_elementwise_mul_fallbacks != 0;
}

pub fn printBypass(path: []const u8, reason: []const u8) void {
    if (!enabled()) return;
    std.debug.print(
        "graph_executor_stats: bypass=1 path={s} reason={s}\n",
        .{ path, reason },
    );
}

pub fn printPartitionFallbackOps(
    graph: *const Graph,
    plan: *const partition_mod.PartitionPlan,
    target_backend: BackendKind,
) void {
    if (!enabled()) return;

    var counts = [_]OpCount{.{}} ** 32;
    var used: usize = 0;
    var fallback_nodes: usize = 0;
    var target_nodes: usize = 0;
    var target_partitions: usize = 0;
    var fallback_partitions: usize = 0;

    for (plan.partitions) |part| {
        if (part.backend == target_backend) {
            target_nodes += part.node_ids.len;
            target_partitions += 1;
            continue;
        }
        fallback_nodes += part.node_ids.len;
        fallback_partitions += 1;
        for (part.node_ids) |node_id| {
            addOpCount(&counts, &used, @tagName(graph.node(node_id).op));
        }
    }

    sortOpCounts(counts[0..used]);
    std.debug.print(
        "graph_executor_fallback_ops: target={s} target_partitions={d} fallback_partitions={d} target_nodes={d} fallback_nodes={d} ops=",
        .{ @tagName(target_backend), target_partitions, fallback_partitions, target_nodes, fallback_nodes },
    );
    if (used == 0) {
        std.debug.print("none\n", .{});
        return;
    }
    const limit = @min(used, 12);
    for (counts[0..limit], 0..) |entry, idx| {
        if (idx > 0) std.debug.print(",", .{});
        std.debug.print("{s}:{d}", .{ entry.name, entry.count });
    }
    if (used > limit) std.debug.print(",...", .{});
    std.debug.print("\n", .{});
}

const OpCount = struct {
    name: []const u8 = "",
    count: usize = 0,
};

fn addOpCount(counts: []OpCount, used: *usize, name: []const u8) void {
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

fn sortOpCounts(counts: []OpCount) void {
    var i: usize = 1;
    while (i < counts.len) : (i += 1) {
        const item = counts[i];
        var j = i;
        while (j > 0 and counts[j - 1].count < item.count) : (j -= 1) {
            counts[j] = counts[j - 1];
        }
        counts[j] = item;
    }
}
