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
const platform = @import("antfly_platform");

const contracts = @import("backend_contracts.zig");
const partition_mod = @import("partition.zig");

const Graph = ml.graph.Graph;
const BackendKind = contracts.BackendKind;
pub const ExecutionStats = partition_mod.PartitionExecutor.ExecutionStats;

const AtomicU64 = platform.atomic.Value(u64);

var aggregate_partitions_executed: AtomicU64 = .init(0);
var aggregate_cross_device_transfers: AtomicU64 = .init(0);
var aggregate_runtime_input_transfers: AtomicU64 = .init(0);
var aggregate_device_resident_transfers: AtomicU64 = .init(0);
var aggregate_backend_command_dispatches: AtomicU64 = .init(0);
var aggregate_planned_operator_dispatches: AtomicU64 = .init(0);
var aggregate_quant_kernel_planned_ops: AtomicU64 = .init(0);
var aggregate_quant_kernel_handwritten_production: AtomicU64 = .init(0);
var aggregate_quant_kernel_generated_production: AtomicU64 = .init(0);
var aggregate_quant_kernel_unsupported_routes: AtomicU64 = .init(0);
var aggregate_quant_kernel_generated_candidates: AtomicU64 = .init(0);
var aggregate_quant_kernel_fallback_generated_artifact_missing: AtomicU64 = .init(0);
var aggregate_quant_kernel_fallback_generated_runtime_not_wired: AtomicU64 = .init(0);
var aggregate_quant_kernel_fallback_unsupported_format: AtomicU64 = .init(0);
var aggregate_quant_kernel_fallback_unsupported_shape: AtomicU64 = .init(0);
var aggregate_quant_kernel_fallback_unsupported_epilogue: AtomicU64 = .init(0);
var aggregate_quant_kernel_fallback_unsupported_backend: AtomicU64 = .init(0);
var aggregate_quant_kernel_fallback_tensor_core_repack_required: AtomicU64 = .init(0);
var aggregate_quant_kernel_fallback_unsupported: AtomicU64 = .init(0);
var aggregate_interpreter_fallbacks: AtomicU64 = .init(0);
var aggregate_device_resident_outputs: AtomicU64 = .init(0);
var aggregate_host_materialized_outputs: AtomicU64 = .init(0);
var aggregate_host_materialized_command_outputs: AtomicU64 = .init(0);
var aggregate_host_materialized_interpreter_outputs: AtomicU64 = .init(0);
var aggregate_host_materialized_pre_materialized_constant_outputs: AtomicU64 = .init(0);
var aggregate_host_materialized_runtime_region_outputs: AtomicU64 = .init(0);
var aggregate_host_materialized_parameter_outputs: AtomicU64 = .init(0);
var aggregate_host_materialized_unattributed_outputs: AtomicU64 = .init(0);
var aggregate_device_resident_parameter_outputs: AtomicU64 = .init(0);
var aggregate_boundary_output_materializations: AtomicU64 = .init(0);
var aggregate_graph_output_owned_copies: AtomicU64 = .init(0);
var aggregate_metal_frame_chunk_boundaries: AtomicU64 = .init(0);
var aggregate_metal_frame_chunk_promoted_values: AtomicU64 = .init(0);
var aggregate_metal_frame_chunk_swept_values: AtomicU64 = .init(0);
var aggregate_graph_plan_slots_reserved: AtomicU64 = .init(0);
var aggregate_graph_plan_bytes_reserved: AtomicU64 = .init(0);

pub fn record(stats: ExecutionStats) void {
    _ = aggregate_partitions_executed.fetchAdd(stats.partitions_executed, .monotonic);
    _ = aggregate_cross_device_transfers.fetchAdd(stats.cross_device_transfers, .monotonic);
    _ = aggregate_runtime_input_transfers.fetchAdd(stats.runtime_input_transfers, .monotonic);
    _ = aggregate_device_resident_transfers.fetchAdd(stats.device_resident_transfers, .monotonic);
    _ = aggregate_backend_command_dispatches.fetchAdd(stats.backend_command_dispatches, .monotonic);
    _ = aggregate_planned_operator_dispatches.fetchAdd(stats.planned_operator_dispatches, .monotonic);
    _ = aggregate_quant_kernel_planned_ops.fetchAdd(stats.quant_kernel_planned_ops, .monotonic);
    _ = aggregate_quant_kernel_handwritten_production.fetchAdd(stats.quant_kernel_handwritten_production, .monotonic);
    _ = aggregate_quant_kernel_generated_production.fetchAdd(stats.quant_kernel_generated_production, .monotonic);
    _ = aggregate_quant_kernel_unsupported_routes.fetchAdd(stats.quant_kernel_unsupported_routes, .monotonic);
    _ = aggregate_quant_kernel_generated_candidates.fetchAdd(stats.quant_kernel_generated_candidates, .monotonic);
    _ = aggregate_quant_kernel_fallback_generated_artifact_missing.fetchAdd(stats.quant_kernel_fallback_generated_artifact_missing, .monotonic);
    _ = aggregate_quant_kernel_fallback_generated_runtime_not_wired.fetchAdd(stats.quant_kernel_fallback_generated_runtime_not_wired, .monotonic);
    _ = aggregate_quant_kernel_fallback_unsupported_format.fetchAdd(stats.quant_kernel_fallback_unsupported_format, .monotonic);
    _ = aggregate_quant_kernel_fallback_unsupported_shape.fetchAdd(stats.quant_kernel_fallback_unsupported_shape, .monotonic);
    _ = aggregate_quant_kernel_fallback_unsupported_epilogue.fetchAdd(stats.quant_kernel_fallback_unsupported_epilogue, .monotonic);
    _ = aggregate_quant_kernel_fallback_unsupported_backend.fetchAdd(stats.quant_kernel_fallback_unsupported_backend, .monotonic);
    _ = aggregate_quant_kernel_fallback_tensor_core_repack_required.fetchAdd(stats.quant_kernel_fallback_tensor_core_repack_required, .monotonic);
    _ = aggregate_quant_kernel_fallback_unsupported.fetchAdd(stats.quant_kernel_fallback_unsupported, .monotonic);
    _ = aggregate_interpreter_fallbacks.fetchAdd(stats.interpreter_fallbacks, .monotonic);
    _ = aggregate_device_resident_outputs.fetchAdd(stats.device_resident_outputs, .monotonic);
    _ = aggregate_host_materialized_outputs.fetchAdd(stats.host_materialized_outputs, .monotonic);
    _ = aggregate_host_materialized_command_outputs.fetchAdd(stats.host_materialized_command_outputs, .monotonic);
    _ = aggregate_host_materialized_interpreter_outputs.fetchAdd(stats.host_materialized_interpreter_outputs, .monotonic);
    _ = aggregate_host_materialized_pre_materialized_constant_outputs.fetchAdd(stats.host_materialized_pre_materialized_constant_outputs, .monotonic);
    _ = aggregate_host_materialized_runtime_region_outputs.fetchAdd(stats.host_materialized_runtime_region_outputs, .monotonic);
    _ = aggregate_host_materialized_parameter_outputs.fetchAdd(stats.host_materialized_parameter_outputs, .monotonic);
    _ = aggregate_host_materialized_unattributed_outputs.fetchAdd(stats.host_materialized_unattributed_outputs, .monotonic);
    _ = aggregate_device_resident_parameter_outputs.fetchAdd(stats.device_resident_parameter_outputs, .monotonic);
    _ = aggregate_boundary_output_materializations.fetchAdd(stats.boundary_output_materializations, .monotonic);
    _ = aggregate_graph_output_owned_copies.fetchAdd(stats.graph_output_owned_copies, .monotonic);
    _ = aggregate_metal_frame_chunk_boundaries.fetchAdd(stats.metal_frame_chunk_boundaries, .monotonic);
    _ = aggregate_metal_frame_chunk_promoted_values.fetchAdd(stats.metal_frame_chunk_promoted_values, .monotonic);
    _ = aggregate_metal_frame_chunk_swept_values.fetchAdd(stats.metal_frame_chunk_swept_values, .monotonic);
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
        .quant_kernel_planned_ops = aggregate_quant_kernel_planned_ops.load(.monotonic),
        .quant_kernel_handwritten_production = aggregate_quant_kernel_handwritten_production.load(.monotonic),
        .quant_kernel_generated_production = aggregate_quant_kernel_generated_production.load(.monotonic),
        .quant_kernel_unsupported_routes = aggregate_quant_kernel_unsupported_routes.load(.monotonic),
        .quant_kernel_generated_candidates = aggregate_quant_kernel_generated_candidates.load(.monotonic),
        .quant_kernel_fallback_generated_artifact_missing = aggregate_quant_kernel_fallback_generated_artifact_missing.load(.monotonic),
        .quant_kernel_fallback_generated_runtime_not_wired = aggregate_quant_kernel_fallback_generated_runtime_not_wired.load(.monotonic),
        .quant_kernel_fallback_unsupported_format = aggregate_quant_kernel_fallback_unsupported_format.load(.monotonic),
        .quant_kernel_fallback_unsupported_shape = aggregate_quant_kernel_fallback_unsupported_shape.load(.monotonic),
        .quant_kernel_fallback_unsupported_epilogue = aggregate_quant_kernel_fallback_unsupported_epilogue.load(.monotonic),
        .quant_kernel_fallback_unsupported_backend = aggregate_quant_kernel_fallback_unsupported_backend.load(.monotonic),
        .quant_kernel_fallback_tensor_core_repack_required = aggregate_quant_kernel_fallback_tensor_core_repack_required.load(.monotonic),
        .quant_kernel_fallback_unsupported = aggregate_quant_kernel_fallback_unsupported.load(.monotonic),
        .interpreter_fallbacks = aggregate_interpreter_fallbacks.load(.monotonic),
        .device_resident_outputs = aggregate_device_resident_outputs.load(.monotonic),
        .host_materialized_outputs = aggregate_host_materialized_outputs.load(.monotonic),
        .host_materialized_command_outputs = aggregate_host_materialized_command_outputs.load(.monotonic),
        .host_materialized_interpreter_outputs = aggregate_host_materialized_interpreter_outputs.load(.monotonic),
        .host_materialized_pre_materialized_constant_outputs = aggregate_host_materialized_pre_materialized_constant_outputs.load(.monotonic),
        .host_materialized_runtime_region_outputs = aggregate_host_materialized_runtime_region_outputs.load(.monotonic),
        .host_materialized_parameter_outputs = aggregate_host_materialized_parameter_outputs.load(.monotonic),
        .host_materialized_unattributed_outputs = aggregate_host_materialized_unattributed_outputs.load(.monotonic),
        .device_resident_parameter_outputs = aggregate_device_resident_parameter_outputs.load(.monotonic),
        .boundary_output_materializations = aggregate_boundary_output_materializations.load(.monotonic),
        .graph_output_owned_copies = aggregate_graph_output_owned_copies.load(.monotonic),
        .metal_frame_chunk_boundaries = aggregate_metal_frame_chunk_boundaries.load(.monotonic),
        .metal_frame_chunk_promoted_values = aggregate_metal_frame_chunk_promoted_values.load(.monotonic),
        .metal_frame_chunk_swept_values = aggregate_metal_frame_chunk_swept_values.load(.monotonic),
        .graph_plan_slots_reserved = aggregate_graph_plan_slots_reserved.load(.monotonic),
        .graph_plan_bytes_reserved = aggregate_graph_plan_bytes_reserved.load(.monotonic),
    };
}

pub fn delta(after: ExecutionStats, before: ExecutionStats) ExecutionStats {
    var result = after;
    const info = @typeInfo(ExecutionStats).@"struct";
    inline for (info.field_names, info.field_types) |field_name, Field| {
        switch (@typeInfo(Field)) {
            .int => @field(result, field_name) = @field(after, field_name) -| @field(before, field_name),
            else => {},
        }
    }
    return result;
}

pub fn reset() void {
    aggregate_partitions_executed.store(0, .monotonic);
    aggregate_cross_device_transfers.store(0, .monotonic);
    aggregate_runtime_input_transfers.store(0, .monotonic);
    aggregate_device_resident_transfers.store(0, .monotonic);
    aggregate_backend_command_dispatches.store(0, .monotonic);
    aggregate_planned_operator_dispatches.store(0, .monotonic);
    aggregate_quant_kernel_planned_ops.store(0, .monotonic);
    aggregate_quant_kernel_handwritten_production.store(0, .monotonic);
    aggregate_quant_kernel_generated_production.store(0, .monotonic);
    aggregate_quant_kernel_unsupported_routes.store(0, .monotonic);
    aggregate_quant_kernel_generated_candidates.store(0, .monotonic);
    aggregate_quant_kernel_fallback_generated_artifact_missing.store(0, .monotonic);
    aggregate_quant_kernel_fallback_generated_runtime_not_wired.store(0, .monotonic);
    aggregate_quant_kernel_fallback_unsupported_format.store(0, .monotonic);
    aggregate_quant_kernel_fallback_unsupported_shape.store(0, .monotonic);
    aggregate_quant_kernel_fallback_unsupported_epilogue.store(0, .monotonic);
    aggregate_quant_kernel_fallback_unsupported_backend.store(0, .monotonic);
    aggregate_quant_kernel_fallback_tensor_core_repack_required.store(0, .monotonic);
    aggregate_quant_kernel_fallback_unsupported.store(0, .monotonic);
    aggregate_interpreter_fallbacks.store(0, .monotonic);
    aggregate_device_resident_outputs.store(0, .monotonic);
    aggregate_host_materialized_outputs.store(0, .monotonic);
    aggregate_host_materialized_command_outputs.store(0, .monotonic);
    aggregate_host_materialized_interpreter_outputs.store(0, .monotonic);
    aggregate_host_materialized_pre_materialized_constant_outputs.store(0, .monotonic);
    aggregate_host_materialized_runtime_region_outputs.store(0, .monotonic);
    aggregate_host_materialized_parameter_outputs.store(0, .monotonic);
    aggregate_host_materialized_unattributed_outputs.store(0, .monotonic);
    aggregate_device_resident_parameter_outputs.store(0, .monotonic);
    aggregate_boundary_output_materializations.store(0, .monotonic);
    aggregate_graph_output_owned_copies.store(0, .monotonic);
    aggregate_metal_frame_chunk_boundaries.store(0, .monotonic);
    aggregate_metal_frame_chunk_promoted_values.store(0, .monotonic);
    aggregate_metal_frame_chunk_swept_values.store(0, .monotonic);
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
        "graph_executor_stats: partitions={d} transfers={d} runtime_input_transfers={d} device_resident_transfers={d} commands={d} planned_commands={d} metadata_aliases={d} descriptor_materializations={d} constant_materializations={d} graph_regions={d} graph_region_ops={d} graph_region_fallbacks={d} fused_patterns={d} fused_nodes_elided={d} graph_output_elision_overrides={d} interpreter_fallbacks={d} device_outputs={d} host_outputs={d} boundary_materializations={d} graph_output_owned_copies={d}",
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
            stats.graph_output_elision_overrides,
            stats.interpreter_fallbacks,
            stats.device_resident_outputs,
            stats.host_materialized_outputs,
            stats.boundary_output_materializations,
            stats.graph_output_owned_copies,
        },
    );
    std.debug.print(
        " host_output_command={d} host_output_interpreter={d} host_output_pre_materialized_constant={d} host_output_runtime_region={d} host_output_parameter={d} host_output_unattributed={d} device_output_parameter={d}",
        .{
            stats.host_materialized_command_outputs,
            stats.host_materialized_interpreter_outputs,
            stats.host_materialized_pre_materialized_constant_outputs,
            stats.host_materialized_runtime_region_outputs,
            stats.host_materialized_parameter_outputs,
            stats.host_materialized_unattributed_outputs,
            stats.device_resident_parameter_outputs,
        },
    );
    std.debug.print(
        " metal_frame_chunk_boundaries={d} metal_frame_chunk_promoted_values={d} metal_frame_chunk_swept_values={d} metal_chunk_local_output_peak_bytes={d} metal_chunk_local_output_live_bytes={d} metal_chunk_local_output_allocations={d} metal_chunk_local_output_reuse_hits={d} metal_chunk_local_output_consumed_hints={d} metal_chunk_local_output_unconsumed_hints={d} metal_chunk_local_output_spill_bytes={d} metal_chunk_local_output_alias_conflicts={d} metal_chunk_local_output_resets={d} metal_chunk_local_output_reset_freed_bytes={d} metal_chunk_local_output_discard_freed_bytes={d} metal_chunk_local_output_reset_live_carry_values={d}",
        .{
            stats.metal_frame_chunk_boundaries,
            stats.metal_frame_chunk_promoted_values,
            stats.metal_frame_chunk_swept_values,
            stats.metal_chunk_local_output_peak_bytes,
            stats.metal_chunk_local_output_live_bytes,
            stats.metal_chunk_local_output_allocations,
            stats.metal_chunk_local_output_reuse_hits,
            stats.metal_chunk_local_output_consumed_hints,
            stats.metal_chunk_local_output_unconsumed_hints,
            stats.metal_chunk_local_output_spill_bytes,
            stats.metal_chunk_local_output_alias_conflicts,
            stats.metal_chunk_local_output_resets,
            stats.metal_chunk_local_output_reset_freed_bytes,
            stats.metal_chunk_local_output_discard_freed_bytes,
            stats.metal_chunk_local_output_reset_live_carry_values,
        },
    );
    std.debug.print(
        " metal_eager_arena_peak_bytes={d} metal_eager_arena_live_bytes={d} metal_eager_arena_reuse_hits={d} metal_eager_arena_allocations={d} metal_eager_arena_spill_bytes={d} metal_eager_arena_hazard_declines={d} metal_eager_arena_alias_conflicts={d} metal_eager_arena_alias_reclaims={d} metal_eager_arena_alias_reclaim_bytes={d} graph_plan_slots={d} graph_plan_bytes={d} runtime_plan_compiles={d} runtime_plan_regions={d} runtime_plan_dispatches={d} runtime_plan_reuses={d} runtime_region_pre_skipped_nodes={d} runtime_region_pre_skipped_transposes={d} runtime_region_pre_skip_declined_external_consumers={d} runtime_prepare_slot_calls={d} runtime_prepare_slot_cache_hits={d} runtime_region_fallbacks={d}",
        .{
            stats.metal_eager_arena_peak_bytes,
            stats.metal_eager_arena_live_bytes,
            stats.metal_eager_arena_reuse_hits,
            stats.metal_eager_arena_allocations,
            stats.metal_eager_arena_spill_bytes,
            stats.metal_eager_arena_hazard_declines,
            stats.metal_eager_arena_alias_conflicts,
            stats.metal_eager_arena_alias_reclaims,
            stats.metal_eager_arena_alias_reclaim_bytes,
            stats.graph_plan_slots_reserved,
            stats.graph_plan_bytes_reserved,
            stats.runtime_region_plan_compiles,
            stats.runtime_region_plan_regions,
            stats.runtime_region_plan_dispatches,
            stats.runtime_region_plan_reuses,
            stats.runtime_region_pre_skipped_nodes,
            stats.runtime_region_pre_skipped_transposes,
            stats.runtime_region_pre_skip_declined_external_consumers,
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
    if (hasQuantKernelPlanStats(stats)) {
        const top_fallback = quantKernelTopFallbackReason(stats);
        const fast_path_misses = quantKernelFastPathMisses(stats);
        std.debug.print(
            "quant_kernel_plan: planned={d} handwritten_production={d} generated_production={d} unsupported_routes={d} fast_path_misses={d} generated_candidates={d} generated_artifact_missing={d} generated_runtime_not_wired={d} unsupported={d} unsupported_format={d} unsupported_shape={d} unsupported_epilogue={d} unsupported_backend={d} tensor_core_repack_required={d} top_fallback_reason={s} top_fallback_count={d}\n",
            .{
                stats.quant_kernel_planned_ops,
                stats.quant_kernel_handwritten_production,
                stats.quant_kernel_generated_production,
                stats.quant_kernel_unsupported_routes,
                fast_path_misses,
                stats.quant_kernel_generated_candidates,
                stats.quant_kernel_fallback_generated_artifact_missing,
                stats.quant_kernel_fallback_generated_runtime_not_wired,
                stats.quant_kernel_fallback_unsupported,
                stats.quant_kernel_fallback_unsupported_format,
                stats.quant_kernel_fallback_unsupported_shape,
                stats.quant_kernel_fallback_unsupported_epilogue,
                stats.quant_kernel_fallback_unsupported_backend,
                stats.quant_kernel_fallback_tensor_core_repack_required,
                top_fallback.name,
                top_fallback.count,
            },
        );
    }
    if (hasMetalFusionStats(stats)) {
        std.debug.print(
            "metal_graph_fusions: qkv_regions={d} attention_regions={d} ffn_regions={d} deberta_ffn_forward_regions={d} head_mlp_forward_regions={d} head_mlp_backward_regions={d} lora_linear_regions={d} lora_qkv_regions={d} lora_backward_regions={d} low_rank_lora_backward_regions={d} rank_adapter_backward_regions={d} ffn_gelu_backward_regions={d} ple_regions={d} tail_regions={d} attention_output_residual={d} attention_output_residual_partial={d} gated_ffn_residual={d} linear_pair={d}\n",
            .{
                stats.metal_qkv_regions,
                stats.metal_attention_regions,
                stats.metal_ffn_regions,
                stats.metal_deberta_ffn_forward_regions,
                stats.metal_head_mlp_forward_regions,
                stats.metal_head_mlp_backward_regions,
                stats.metal_lora_linear_regions,
                stats.metal_lora_qkv_regions,
                stats.metal_lora_backward_regions,
                stats.metal_low_rank_lora_backward_regions,
                stats.metal_rank_adapter_backward_regions,
                stats.metal_ffn_gelu_backward_regions,
                stats.metal_ple_regions,
                stats.metal_tail_regions,
                stats.metal_attention_output_residual_fusions,
                stats.metal_attention_output_residual_partial_fallbacks,
                stats.metal_gated_ffn_residual_fusions,
                stats.metal_linear_pair_fusions,
            },
        );
    }
    if (hasMetalCommandFamilyStats(stats)) {
        std.debug.print(
            "metal_command_dispatch_families: dot_general={d} head_dot={d} transpose={d} gather={d} reduce={d} elementwise={d} activation={d} activation_backward={d} other={d}\n",
            .{
                stats.metal_command_dot_general_dispatches,
                stats.metal_command_head_dot_dispatches,
                stats.metal_command_transpose_dispatches,
                stats.metal_command_gather_dispatches,
                stats.metal_command_reduce_dispatches,
                stats.metal_command_elementwise_dispatches,
                stats.metal_command_activation_dispatches,
                stats.metal_command_activation_backward_dispatches,
                stats.metal_command_other_dispatches,
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

fn hasQuantKernelPlanStats(stats: ExecutionStats) bool {
    return stats.quant_kernel_planned_ops != 0 or
        stats.quant_kernel_handwritten_production != 0 or
        stats.quant_kernel_generated_production != 0 or
        stats.quant_kernel_unsupported_routes != 0 or
        stats.quant_kernel_generated_candidates != 0 or
        stats.quant_kernel_fallback_generated_artifact_missing != 0 or
        stats.quant_kernel_fallback_generated_runtime_not_wired != 0 or
        stats.quant_kernel_fallback_unsupported_format != 0 or
        stats.quant_kernel_fallback_unsupported_shape != 0 or
        stats.quant_kernel_fallback_unsupported_epilogue != 0 or
        stats.quant_kernel_fallback_unsupported_backend != 0 or
        stats.quant_kernel_fallback_tensor_core_repack_required != 0 or
        stats.quant_kernel_fallback_unsupported != 0;
}

pub const FallbackReasonCount = struct {
    name: []const u8,
    count: u64,
};

pub fn quantKernelFastPathMisses(stats: ExecutionStats) u64 {
    return stats.quant_kernel_fallback_generated_artifact_missing +|
        stats.quant_kernel_fallback_generated_runtime_not_wired +|
        stats.quant_kernel_fallback_unsupported_format +|
        stats.quant_kernel_fallback_unsupported_shape +|
        stats.quant_kernel_fallback_unsupported_epilogue +|
        stats.quant_kernel_fallback_unsupported_backend +|
        stats.quant_kernel_fallback_tensor_core_repack_required;
}

pub fn quantKernelTopFallbackReason(stats: ExecutionStats) FallbackReasonCount {
    const reasons = [_]FallbackReasonCount{
        .{ .name = "generated_artifact_missing", .count = stats.quant_kernel_fallback_generated_artifact_missing },
        .{ .name = "generated_runtime_not_wired", .count = stats.quant_kernel_fallback_generated_runtime_not_wired },
        .{ .name = "unsupported_format", .count = stats.quant_kernel_fallback_unsupported_format },
        .{ .name = "unsupported_shape", .count = stats.quant_kernel_fallback_unsupported_shape },
        .{ .name = "unsupported_epilogue", .count = stats.quant_kernel_fallback_unsupported_epilogue },
        .{ .name = "unsupported_backend", .count = stats.quant_kernel_fallback_unsupported_backend },
        .{ .name = "tensor_core_repack_required", .count = stats.quant_kernel_fallback_tensor_core_repack_required },
    };
    var best = FallbackReasonCount{ .name = "none", .count = 0 };
    for (reasons) |reason| {
        if (reason.count > best.count) best = reason;
    }
    return best;
}

fn hasMetalFusionStats(stats: ExecutionStats) bool {
    return stats.graph_regions != 0 or
        stats.graph_region_fallbacks != 0 or
        stats.metal_qkv_regions != 0 or
        stats.metal_attention_regions != 0 or
        stats.metal_ffn_regions != 0 or
        stats.metal_deberta_ffn_forward_regions != 0 or
        stats.metal_head_mlp_forward_regions != 0 or
        stats.metal_head_mlp_backward_regions != 0 or
        stats.metal_lora_linear_regions != 0 or
        stats.metal_lora_qkv_regions != 0 or
        stats.metal_lora_backward_regions != 0 or
        stats.metal_low_rank_lora_backward_regions != 0 or
        stats.metal_rank_adapter_backward_regions != 0 or
        stats.metal_ffn_gelu_backward_regions != 0 or
        stats.metal_ple_regions != 0 or
        stats.metal_tail_regions != 0 or
        stats.metal_attention_output_residual_fusions != 0 or
        stats.metal_attention_output_residual_partial_fallbacks != 0 or
        stats.metal_gated_ffn_residual_fusions != 0 or
        stats.metal_linear_pair_fusions != 0;
}

fn hasMetalCommandFamilyStats(stats: ExecutionStats) bool {
    return stats.metal_command_dot_general_dispatches != 0 or
        stats.metal_command_head_dot_dispatches != 0 or
        stats.metal_command_transpose_dispatches != 0 or
        stats.metal_command_gather_dispatches != 0 or
        stats.metal_command_reduce_dispatches != 0 or
        stats.metal_command_elementwise_dispatches != 0 or
        stats.metal_command_activation_dispatches != 0 or
        stats.metal_command_activation_backward_dispatches != 0 or
        stats.metal_command_other_dispatches != 0;
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

    var counts = @as([32]OpCount, @splat(.{}));
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

test "executor stats aggregate quant kernel plan counters" {
    reset();
    defer reset();

    record(.{
        .quant_kernel_planned_ops = 2,
        .quant_kernel_handwritten_production = 2,
        .quant_kernel_generated_production = 7,
        .quant_kernel_unsupported_routes = 8,
        .quant_kernel_generated_candidates = 1,
        .quant_kernel_fallback_generated_artifact_missing = 1,
        .quant_kernel_fallback_generated_runtime_not_wired = 6,
        .quant_kernel_fallback_unsupported_format = 1,
        .quant_kernel_fallback_unsupported_shape = 2,
        .quant_kernel_fallback_unsupported_epilogue = 3,
        .quant_kernel_fallback_unsupported_backend = 4,
        .quant_kernel_fallback_tensor_core_repack_required = 5,
        .quant_kernel_fallback_unsupported = 1,
    });

    const stats = snapshot();
    try std.testing.expectEqual(@as(u64, 2), stats.quant_kernel_planned_ops);
    try std.testing.expectEqual(@as(u64, 2), stats.quant_kernel_handwritten_production);
    try std.testing.expectEqual(@as(u64, 7), stats.quant_kernel_generated_production);
    try std.testing.expectEqual(@as(u64, 8), stats.quant_kernel_unsupported_routes);
    try std.testing.expectEqual(@as(u64, 22), quantKernelFastPathMisses(stats));
    try std.testing.expectEqual(@as(u64, 1), stats.quant_kernel_generated_candidates);
    try std.testing.expectEqual(@as(u64, 1), stats.quant_kernel_fallback_generated_artifact_missing);
    try std.testing.expectEqual(@as(u64, 6), stats.quant_kernel_fallback_generated_runtime_not_wired);
    try std.testing.expectEqual(@as(u64, 1), stats.quant_kernel_fallback_unsupported_format);
    try std.testing.expectEqual(@as(u64, 2), stats.quant_kernel_fallback_unsupported_shape);
    try std.testing.expectEqual(@as(u64, 3), stats.quant_kernel_fallback_unsupported_epilogue);
    try std.testing.expectEqual(@as(u64, 4), stats.quant_kernel_fallback_unsupported_backend);
    try std.testing.expectEqual(@as(u64, 5), stats.quant_kernel_fallback_tensor_core_repack_required);
    try std.testing.expectEqual(@as(u64, 1), stats.quant_kernel_fallback_unsupported);
}

test "executor stats delta reports per-run quant kernel counters" {
    const before = ExecutionStats{
        .partitions_executed = 9,
        .quant_kernel_planned_ops = 10,
        .quant_kernel_generated_production = 5,
        .quant_kernel_fallback_unsupported_shape = 3,
    };
    const after = ExecutionStats{
        .partitions_executed = 11,
        .quant_kernel_planned_ops = 17,
        .quant_kernel_generated_production = 8,
        .quant_kernel_fallback_unsupported_shape = 2,
        .quant_kernel_fallback_unsupported_epilogue = 4,
    };

    const run = delta(after, before);
    try std.testing.expectEqual(@as(u64, 2), run.partitions_executed);
    try std.testing.expectEqual(@as(u64, 7), run.quant_kernel_planned_ops);
    try std.testing.expectEqual(@as(u64, 3), run.quant_kernel_generated_production);
    try std.testing.expectEqual(@as(u64, 0), run.quant_kernel_fallback_unsupported_shape);
    try std.testing.expectEqual(@as(u64, 4), run.quant_kernel_fallback_unsupported_epilogue);
}

test "executor stats wires every quant kernel plan counter" {
    reset();
    defer reset();

    try std.testing.expect(!hasQuantKernelPlanStats(.{}));

    const info = @typeInfo(ExecutionStats).@"struct";
    inline for (info.field_names, info.field_types) |field_name, Field| {
        if (comptime std.mem.startsWith(u8, field_name, "quant_kernel_")) {
            var stats = ExecutionStats{};
            @field(stats, field_name) = 1;

            try std.testing.expect(hasQuantKernelPlanStats(stats));

            record(stats);
            const aggregated = snapshot();
            try std.testing.expectEqual(@as(Field, 1), @field(aggregated, field_name));

            reset();
            try std.testing.expectEqual(@as(Field, 0), @field(snapshot(), field_name));
        }
    }
}

test "executor stats reports top quant kernel fallback reason" {
    try std.testing.expectEqual(@as(u64, 0), quantKernelFastPathMisses(.{}));
    try std.testing.expectEqualStrings("none", quantKernelTopFallbackReason(.{}).name);
    try std.testing.expectEqual(@as(u64, 0), quantKernelTopFallbackReason(.{}).count);

    const stats = ExecutionStats{
        .quant_kernel_fallback_generated_artifact_missing = 2,
        .quant_kernel_fallback_generated_runtime_not_wired = 5,
        .quant_kernel_fallback_unsupported_format = 3,
        .quant_kernel_fallback_unsupported_shape = 1,
        .quant_kernel_fallback_unsupported_epilogue = 7,
        .quant_kernel_fallback_unsupported_backend = 4,
        .quant_kernel_fallback_tensor_core_repack_required = 6,
        .quant_kernel_fallback_unsupported = 8,
    };
    const top = quantKernelTopFallbackReason(stats);
    try std.testing.expectEqualStrings("unsupported_epilogue", top.name);
    try std.testing.expectEqual(@as(u64, 7), top.count);
    try std.testing.expectEqual(@as(u64, 28), quantKernelFastPathMisses(stats));

    try std.testing.expectEqual(std.math.maxInt(u64), quantKernelFastPathMisses(.{
        .quant_kernel_fallback_generated_artifact_missing = std.math.maxInt(u64),
        .quant_kernel_fallback_generated_runtime_not_wired = 1,
    }));
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
