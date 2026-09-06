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

// Training step orchestration: forward → loss → gradient → execute → extract.
//
// Given a computation graph with a scalar loss output, computes the loss
// value and parameter gradients in a single call by:
//   1. Running autodiff to produce a gradient graph
//   2. Marking gradient nodes as additional outputs
//   3. Executing the combined graph through the interpreter
//   4. Extracting loss and gradient tensors as f32 slices
//
// Usage:
//   var result = try trainStep(allocator, &graph, loss_id, &cb, runtime_inputs, .{});
//   defer result.deinit();
//   // result.loss = scalar loss value
//   // result.gradients.get("weight") = []f32 gradient data

const std = @import("std");
const builtin = @import("builtin");
const ml = @import("ml");
const platform = @import("antfly_platform");
const Graph = ml.graph.Graph;
const NodeId = ml.graph.NodeId;
const null_node = ml.graph.null_node;
const Builder = ml.graph.Builder;
const Shape = ml.graph.Shape;
const autodiff = ml.graph.autodiff;
const checkpoint = ml.graph.checkpoint;
const ops_mod = @import("../ops/ops.zig");
const metal_compute_mod = @import("../ops/metal_compute.zig");
const native_compute = @import("../ops/native_compute.zig");
const contracts = @import("backend_contracts.zig");
const CT = contracts.CT;
const ComputeBackend = ops_mod.ComputeBackend;
const interpreter = @import("interpreter.zig");
const RuntimeInput = interpreter.RuntimeInput;
const partition_mod = @import("partition.zig");
const buffer_plan_mod = @import("buffer_plan.zig");
const metal_capabilities = @import("metal_capabilities.zig");
const metal_partition_executor = @import("metal_partition_executor.zig");
const device_mesh = @import("device_mesh.zig");
const multi_executor = @import("multi_executor.zig");
const io_compat = @import("../io/compat.zig");

pub const TrainStepResult = struct {
    loss: f32,
    gradients: std.StringHashMapUnmanaged([]f32), // param_name -> gradient f32 data
    device_gradients: std.StringHashMapUnmanaged(CT) = .{},
    profile: TrainStepProfile = .{},
    checkpoint_summary: ?CheckpointSummary = null,
    allocator: std.mem.Allocator,
    compute_backend: ?*const ComputeBackend = null,

    pub fn deinit(self: *TrainStepResult) void {
        var it = self.gradients.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }
        self.gradients.deinit(self.allocator);
        if (self.compute_backend) |cb| {
            var device_it = self.device_gradients.iterator();
            while (device_it.next()) |entry| {
                self.allocator.free(entry.key_ptr.*);
                cb.free(entry.value_ptr.*);
            }
        }
        self.device_gradients.deinit(self.allocator);
    }
};

pub const TrainStepProfile = struct {
    autodiff_ns: u64 = 0,
    checkpoint_ns: u64 = 0,
    execute_ns: u64 = 0,
    extract_ns: u64 = 0,
    total_ns: u64 = 0,
    peak_resident_bytes: usize = 0,
    /// Non-null when the Metal training graph executor was requested but the
    /// step fell back to the regular compiled path. Always a static string.
    graph_executor_fallback_reason: ?[]const u8 = null,
    graph_executor_partitions: u64 = 0,
    graph_executor_native_partitions: u64 = 0,
    graph_executor_unsupported_ops: u64 = 0,
    graph_executor_command_dispatches: u64 = 0,
    graph_executor_planned_dispatches: u64 = 0,
    graph_executor_interpreter_fallbacks: u64 = 0,
    graph_executor_host_outputs: u64 = 0,
    graph_executor_true_host_outputs: u64 = 0,
    graph_executor_host_output_command: u64 = 0,
    graph_executor_host_output_interpreter: u64 = 0,
    graph_executor_host_output_pre_materialized_constant: u64 = 0,
    graph_executor_host_output_runtime_region: u64 = 0,
    graph_executor_host_output_parameter: u64 = 0,
    graph_executor_host_output_unattributed: u64 = 0,
    graph_executor_device_outputs: u64 = 0,
    graph_executor_device_output_parameter: u64 = 0,
    graph_executor_metal_frame_chunk_boundaries: u64 = 0,
    graph_executor_metal_frame_chunk_promoted_values: u64 = 0,
    graph_executor_metal_frame_chunk_swept_values: u64 = 0,
    graph_executor_metal_chunk_local_output_peak_bytes: u64 = 0,
    graph_executor_metal_chunk_local_output_live_bytes: u64 = 0,
    graph_executor_metal_chunk_local_output_allocations: u64 = 0,
    graph_executor_metal_chunk_local_output_reuse_hits: u64 = 0,
    graph_executor_metal_chunk_local_output_consumed_hints: u64 = 0,
    graph_executor_metal_chunk_local_output_unconsumed_hints: u64 = 0,
    graph_executor_metal_chunk_local_output_spill_bytes: u64 = 0,
    graph_executor_metal_chunk_local_output_alias_conflicts: u64 = 0,
    graph_executor_metal_chunk_local_output_resets: u64 = 0,
    graph_executor_metal_chunk_local_output_reset_freed_bytes: u64 = 0,
    graph_executor_metal_chunk_local_output_discard_freed_bytes: u64 = 0,
    graph_executor_metal_chunk_local_output_reset_live_carry_values: u64 = 0,
    graph_executor_metal_gather_input_promotions: u64 = 0,
    graph_executor_metal_gather_input_promotion_bytes: u64 = 0,
    graph_executor_metal_gather_input_promotion_ns: u64 = 0,
    graph_executor_metal_gather_output_promotions: u64 = 0,
    graph_executor_metal_gather_output_promotion_bytes: u64 = 0,
    graph_executor_metal_gather_output_promotion_ns: u64 = 0,
    graph_executor_metal_reduce_input_promotions: u64 = 0,
    graph_executor_metal_reduce_input_promotion_bytes: u64 = 0,
    graph_executor_metal_reduce_input_promotion_ns: u64 = 0,
    graph_executor_metal_resident_input_cache_hits: u64 = 0,
    graph_executor_metal_resident_input_cache_misses: u64 = 0,
    graph_executor_metal_resident_input_cache_unique_promotions: u64 = 0,
    graph_executor_metal_resident_input_cache_retained_live_bytes: u64 = 0,
    graph_executor_metal_resident_input_cache_retained_peak_bytes: u64 = 0,
    graph_executor_metal_resident_input_cache_reused_bytes: u64 = 0,
    graph_executor_metal_resident_input_cache_released_bytes: u64 = 0,
    graph_executor_metal_eager_arena_peak_bytes: u64 = 0,
    graph_executor_metal_eager_arena_live_bytes: u64 = 0,
    graph_executor_metal_eager_arena_reuse_hits: u64 = 0,
    graph_executor_metal_eager_arena_allocations: u64 = 0,
    graph_executor_metal_eager_arena_spill_bytes: u64 = 0,
    graph_executor_metal_eager_arena_hazard_declines: u64 = 0,
    graph_executor_metal_eager_arena_alias_conflicts: u64 = 0,
    graph_executor_metal_eager_arena_alias_reclaims: u64 = 0,
    graph_executor_metal_eager_arena_alias_reclaim_bytes: u64 = 0,
    graph_executor_regions: u64 = 0,
    graph_executor_region_ops: u64 = 0,
    graph_executor_runtime_region_dispatches: u64 = 0,
    graph_executor_runtime_region_fallbacks: u64 = 0,
    graph_executor_runtime_region_active_regions: u64 = 0,
    graph_executor_runtime_region_covered_nodes: u64 = 0,
    graph_executor_runtime_region_elided_nodes: u64 = 0,
    graph_executor_runtime_region_plan_compiles: u64 = 0,
    graph_executor_runtime_region_plan_reuses: u64 = 0,
    graph_executor_runtime_region_pre_skipped_nodes: u64 = 0,
    graph_executor_runtime_region_pre_skipped_transposes: u64 = 0,
    graph_executor_runtime_region_pre_skip_declined_external_consumers: u64 = 0,
    metal_deberta_ffn_forward_regions: u64 = 0,
    metal_deberta_encoder_lora_layer_regions: u64 = 0,
    metal_deberta_encoder_lora_residual_layernorm_regions: u64 = 0,
    metal_deberta_encoder_lora_layer_scaffold_regions: u64 = 0,
    metal_deberta_encoder_lora_layer_fallbacks: u64 = 0,
    metal_lora_backward_regions: u64 = 0,
    metal_low_rank_lora_backward_regions: u64 = 0,
    metal_rank_adapter_backward_regions: u64 = 0,
    metal_ffn_gelu_backward_regions: u64 = 0,
    metal_head_mlp_forward_regions: u64 = 0,
    metal_head_mlp_backward_regions: u64 = 0,
    metal_command_dot_general_dispatches: u64 = 0,
    metal_command_head_dot_dispatches: u64 = 0,
    metal_command_transpose_dispatches: u64 = 0,
    metal_command_gather_dispatches: u64 = 0,
    metal_command_reduce_dispatches: u64 = 0,
    metal_command_elementwise_dispatches: u64 = 0,
    metal_command_activation_dispatches: u64 = 0,
    metal_command_activation_backward_dispatches: u64 = 0,
    metal_command_other_dispatches: u64 = 0,
    graph_executor_runtime_frame_candidates: u64 = 0,
    graph_executor_runtime_frame_eligible: u64 = 0,
    graph_executor_runtime_frame_metadata_ready: u64 = 0,
    graph_executor_runtime_frame_ineligible_no_regions: u64 = 0,
    graph_executor_runtime_frame_ineligible_missing_qkv: u64 = 0,
    graph_executor_runtime_frame_ineligible_missing_attention: u64 = 0,
    graph_executor_runtime_frame_ineligible_missing_ffn: u64 = 0,
    graph_executor_runtime_frame_ineligible_missing_ple: u64 = 0,
    graph_executor_runtime_frame_ineligible_single_row: u64 = 0,
    graph_executor_runtime_frame_ineligible_non_layer_order: u64 = 0,
    graph_executor_runtime_frame_ineligible_shape_mismatch: u64 = 0,
    graph_executor_runtime_frame_ineligible_missing_model_metadata: u64 = 0,
    graph_executor_plan_build_ns: u64 = 0,
    graph_executor_buffer_plan_build_ns: u64 = 0,
    graph_executor_plan_cache_hits: u64 = 0,
    graph_executor_plan_cache_misses: u64 = 0,
    metal_frame_wait_ns: u64 = 0,
    metal_frame_gpu_ns: u64 = 0,
    metal_tensor_device_owned_live_bytes: u64 = 0,
    metal_tensor_device_owned_peak_live_bytes: u64 = 0,
    metal_tensor_device_owned_peak_lt_256kb_bytes: u64 = 0,
    metal_tensor_device_owned_peak_256kb_1mb_bytes: u64 = 0,
    metal_tensor_device_owned_peak_1mb_4mb_bytes: u64 = 0,
    metal_tensor_device_owned_peak_4mb_16mb_bytes: u64 = 0,
    metal_tensor_device_owned_peak_ge_16mb_bytes: u64 = 0,
    metal_runtime_total_bytes: u64 = 0,
    metal_runtime_scratch_bytes: u64 = 0,
    metal_runtime_scratch_pool_bytes: u64 = 0,
    metal_runtime_graph_plan_bytes: u64 = 0,
    metal_runtime_dense_linear_bytes: u64 = 0,
    metal_runtime_attention_span_bytes: u64 = 0,
    metal_runtime_hidden_state_bytes: u64 = 0,
    metal_runtime_frame_retained_bytes: u64 = 0,
    metal_runtime_reuse_pool_bytes: u64 = 0,
    metal_runtime_reuse_pool_slots: u64 = 0,
    metal_runtime_reuse_pool_peak_slots: u64 = 0,
    metal_runtime_reuse_alloc_delta: u64 = 0,
    metal_runtime_reuse_hit_delta: u64 = 0,
    metal_last_frame_compute_encoders: u64 = 0,
    metal_last_frame_blit_encoders: u64 = 0,
    metal_last_frame_planned_scopes: u64 = 0,
    metal_last_frame_planned_barriers: u64 = 0,
    metal_last_frame_planned_command_ops: u64 = 0,
    metal_deberta_encoder_plan_attempts: u64 = 0,
    metal_deberta_encoder_plan_successes: u64 = 0,
    metal_deberta_encoder_plan_reuses: u64 = 0,
    metal_deberta_encoder_plan_failures: u64 = 0,
    metal_deberta_encoder_layer_attempts: u64 = 0,
    metal_deberta_encoder_layer_successes: u64 = 0,
    metal_deberta_encoder_layer_fallbacks: u64 = 0,
    metal_deberta_relative_qk_pair_calls: u64 = 0,
    metal_deberta_relative_qk_pair_fallbacks: u64 = 0,
    metal_deberta_ffn_fused_calls: u64 = 0,
    metal_deberta_ffn_fused_mps_matmuls: u64 = 0,
    metal_deberta_ffn_fused_fallbacks: u64 = 0,
    metal_deberta_attention_flash_calls: u64 = 0,
    metal_deberta_attention_gemm_calls: u64 = 0,
    metal_deberta_attention_gemm_fallbacks: u64 = 0,
    metal_deberta_attention_legacy_calls: u64 = 0,
    cuda_device_allocations: u64 = 0,
    cuda_device_frees: u64 = 0,
    cuda_h2d_bytes: u64 = 0,
    cuda_d2h_bytes: u64 = 0,
    cuda_largest_d2h_transfer_bytes: u64 = 0,
    cuda_to_float32_calls: u64 = 0,
    cuda_download_alloc_calls: u64 = 0,
    cuda_stream_synchronizations: u64 = 0,
    cuda_upload_synchronizations: u64 = 0,
    cuda_temp_cache_hits: u64 = 0,
    cuda_temp_cache_misses: u64 = 0,
    cuda_kernel_launches: u64 = 0,
    cuda_packed_attention_forward_calls: u64 = 0,
    cuda_packed_attention_backward_calls: u64 = 0,
    cuda_exact_gelu_forward_calls: u64 = 0,
    cuda_exact_gelu_backward_calls: u64 = 0,
    cuda_training_input_uploads: u64 = 0,
    cuda_training_input_upload_bytes: u64 = 0,
};

pub fn recordTrainingRuntimeProfile(
    profile: anytype,
    before: ops_mod.TrainingRuntimeStats,
    after: ops_mod.TrainingRuntimeStats,
) void {
    profile.cuda_device_allocations = after.device_allocations -| before.device_allocations;
    profile.cuda_device_frees = after.device_frees -| before.device_frees;
    profile.cuda_h2d_bytes = after.h2d_bytes -| before.h2d_bytes;
    profile.cuda_d2h_bytes = after.d2h_bytes -| before.d2h_bytes;
    profile.cuda_to_float32_calls = after.to_float32_calls -| before.to_float32_calls;
    profile.cuda_download_alloc_calls = after.download_alloc_calls -| before.download_alloc_calls;
    const download_events = after.download_events -| before.download_events;
    const download_event_bytes = after.download_event_bytes -| before.download_event_bytes;
    // The backend's largest-transfer counter is lifetime-scoped, so simply
    // clamping it to this step's total conflates two independent scalar
    // readbacks (loss and gradient norm) into one 8-byte transfer. The strict
    // training path performs at most two scalar downloads per step. When the
    // backend's two-event ring accounts for every byte, it gives the exact
    // per-step maximum. Retain the conservative lifetime fallback for
    // non-standard or repeated download paths.
    const exact_download_max = if (download_events == 2)
        @max(after.download_previous_bytes, after.download_last_bytes)
    else if (download_events == 1)
        after.download_last_bytes
    else
        0;
    const exact_download_accounting =
        download_events <= 2 and
        download_event_bytes == profile.cuda_d2h_bytes;
    profile.cuda_largest_d2h_transfer_bytes = if (exact_download_accounting)
        exact_download_max
    else
        @min(after.largest_d2h_transfer_bytes, profile.cuda_d2h_bytes);
    profile.cuda_stream_synchronizations = after.stream_synchronizations -| before.stream_synchronizations;
    profile.cuda_upload_synchronizations = after.upload_synchronizations -| before.upload_synchronizations;
    profile.cuda_temp_cache_hits = after.temp_cache_hits -| before.temp_cache_hits;
    profile.cuda_temp_cache_misses = after.temp_cache_misses -| before.temp_cache_misses;
    profile.cuda_kernel_launches = after.kernel_launches -| before.kernel_launches;
    profile.cuda_packed_attention_forward_calls = after.packed_attention_forward_calls -| before.packed_attention_forward_calls;
    profile.cuda_packed_attention_backward_calls = after.packed_attention_backward_calls -| before.packed_attention_backward_calls;
    profile.cuda_exact_gelu_forward_calls = after.exact_gelu_forward_calls -| before.exact_gelu_forward_calls;
    profile.cuda_exact_gelu_backward_calls = after.exact_gelu_backward_calls -| before.exact_gelu_backward_calls;
    profile.cuda_training_input_uploads = after.training_input_uploads -| before.training_input_uploads;
    profile.cuda_training_input_upload_bytes = after.training_input_upload_bytes -| before.training_input_upload_bytes;
}

test "runtime profile keeps independent scalar downloads separate" {
    const before = ops_mod.TrainingRuntimeStats{
        .d2h_bytes = 1024,
        .largest_d2h_transfer_bytes = 1024,
        .download_events = 10,
        .download_event_bytes = 40,
        .download_last_bytes = 4,
        .download_previous_bytes = 4,
        .to_float32_calls = 3,
        .download_alloc_calls = 7,
    };
    const after = ops_mod.TrainingRuntimeStats{
        .d2h_bytes = 1032,
        .largest_d2h_transfer_bytes = 1024,
        .download_events = 12,
        .download_event_bytes = 48,
        .download_last_bytes = 4,
        .download_previous_bytes = 4,
        .to_float32_calls = 4,
        .download_alloc_calls = 8,
    };
    var profile = TrainStepProfile{};
    recordTrainingRuntimeProfile(&profile, before, after);
    try std.testing.expectEqual(@as(u64, 8), profile.cuda_d2h_bytes);
    try std.testing.expectEqual(@as(u64, 4), profile.cuda_largest_d2h_transfer_bytes);
}

test "runtime profile conservatively bounds unaccounted downloads" {
    const before = ops_mod.TrainingRuntimeStats{};
    const after = ops_mod.TrainingRuntimeStats{
        .d2h_bytes = 24,
        .largest_d2h_transfer_bytes = 64,
    };
    var profile = TrainStepProfile{};
    recordTrainingRuntimeProfile(&profile, before, after);
    try std.testing.expectEqual(@as(u64, 24), profile.cuda_largest_d2h_transfer_bytes);
}

fn selectCudaTrainingGraphExecutor(strict: bool, enabled: bool, disabled: bool) !bool {
    if (disabled) {
        if (strict) return error.StrictCudaGraphExecutorDisabled;
        return false;
    }
    return strict or enabled;
}

pub const CheckpointSummary = struct {
    strategy: checkpoint.CheckpointStrategy,
    layer_interval: u32,
    total_forward_activations: u32,
    checkpointed_activations: u32,
    recomputable_activations: u32,
    savings_ratio: f32,
};

pub const TrainStepOptions = struct {
    /// Which parameters to compute gradients for. If null, all parameters.
    trainable_params: ?[]const []const u8 = null,
    checkpoint_config: ?checkpoint.CheckpointConfig = null,
    emit_checkpoint_analysis: bool = false,
};

fn beginOwnedExecutionFrame(cb: *const ComputeBackend) !bool {
    if (cb.decoderRuntimeHasActiveFrame()) return false;
    return try cb.decoderRuntimeBeginFrame();
}

fn deltaU64(before: u64, after: u64) u64 {
    return if (after >= before) after - before else 0;
}

fn deltaU128ToU64(before: u128, after: u128) u64 {
    const delta = if (after >= before) after - before else 0;
    return if (delta > std.math.maxInt(u64)) std.math.maxInt(u64) else @intCast(delta);
}

fn recordMetalDebugProfile(
    profile: *TrainStepProfile,
    before: ops_mod.NativeQuantTimingStats,
    after: ops_mod.NativeQuantTimingStats,
) void {
    profile.metal_frame_wait_ns = deltaU128ToU64(before.decoder_runtime_frame_wait_nanos, after.decoder_runtime_frame_wait_nanos);
    profile.metal_frame_gpu_ns = deltaU128ToU64(before.decoder_runtime_frame_gpu_nanos, after.decoder_runtime_frame_gpu_nanos);
    profile.metal_tensor_device_owned_live_bytes = after.metal_tensor_device_owned_live_bytes;
    profile.metal_tensor_device_owned_peak_live_bytes = after.metal_tensor_device_owned_peak_live_bytes;
    profile.metal_tensor_device_owned_peak_lt_256kb_bytes = after.metal_tensor_device_owned_peak_lt_256kb_bytes;
    profile.metal_tensor_device_owned_peak_256kb_1mb_bytes = after.metal_tensor_device_owned_peak_256kb_1mb_bytes;
    profile.metal_tensor_device_owned_peak_1mb_4mb_bytes = after.metal_tensor_device_owned_peak_1mb_4mb_bytes;
    profile.metal_tensor_device_owned_peak_4mb_16mb_bytes = after.metal_tensor_device_owned_peak_4mb_16mb_bytes;
    profile.metal_tensor_device_owned_peak_ge_16mb_bytes = after.metal_tensor_device_owned_peak_ge_16mb_bytes;
    profile.metal_runtime_total_bytes = after.metal_runtime_total_bytes;
    profile.metal_runtime_scratch_bytes = after.metal_runtime_scratch_bytes;
    profile.metal_runtime_scratch_pool_bytes = after.metal_runtime_scratch_pool_bytes;
    profile.metal_runtime_graph_plan_bytes = after.metal_runtime_graph_plan_bytes;
    profile.metal_runtime_dense_linear_bytes = after.metal_runtime_dense_linear_bytes;
    profile.metal_runtime_attention_span_bytes = after.metal_runtime_attention_span_bytes;
    profile.metal_runtime_hidden_state_bytes = after.metal_runtime_hidden_state_bytes;
    profile.metal_runtime_frame_retained_bytes = after.metal_runtime_frame_retained_bytes;
    profile.metal_runtime_reuse_pool_bytes = after.metal_runtime_reuse_pool_bytes;
    profile.metal_runtime_reuse_pool_slots = after.metal_runtime_reuse_pool_slots;
    profile.metal_runtime_reuse_pool_peak_slots = after.metal_runtime_reuse_pool_peak_slots;
    profile.metal_runtime_reuse_alloc_delta = deltaU64(before.metal_runtime_reuse_alloc_count, after.metal_runtime_reuse_alloc_count);
    profile.metal_runtime_reuse_hit_delta = deltaU64(before.metal_runtime_reuse_hit_count, after.metal_runtime_reuse_hit_count);
    profile.metal_last_frame_compute_encoders = after.metal_runtime_last_frame_compute_encoder_count;
    profile.metal_last_frame_blit_encoders = after.metal_runtime_last_frame_blit_encoder_count;
    profile.metal_last_frame_planned_scopes = after.metal_runtime_last_frame_planned_compute_scope_count;
    profile.metal_last_frame_planned_barriers = after.metal_runtime_last_frame_planned_barrier_count;
    profile.metal_last_frame_planned_command_ops = after.metal_runtime_last_frame_planned_command_op_count;
    profile.metal_deberta_encoder_plan_attempts = deltaU64(before.metal_runtime_deberta_encoder_frame_plan_attempts, after.metal_runtime_deberta_encoder_frame_plan_attempts);
    profile.metal_deberta_encoder_plan_successes = deltaU64(before.metal_runtime_deberta_encoder_frame_plan_successes, after.metal_runtime_deberta_encoder_frame_plan_successes);
    profile.metal_deberta_encoder_plan_reuses = deltaU64(before.metal_runtime_deberta_encoder_frame_plan_reuses, after.metal_runtime_deberta_encoder_frame_plan_reuses);
    profile.metal_deberta_encoder_plan_failures = deltaU64(before.metal_runtime_deberta_encoder_frame_plan_failures, after.metal_runtime_deberta_encoder_frame_plan_failures);
    profile.metal_deberta_encoder_layer_attempts = deltaU64(before.metal_runtime_deberta_encoder_layer_attempts, after.metal_runtime_deberta_encoder_layer_attempts);
    profile.metal_deberta_encoder_layer_successes = deltaU64(before.metal_runtime_deberta_encoder_layer_successes, after.metal_runtime_deberta_encoder_layer_successes);
    profile.metal_deberta_encoder_layer_fallbacks = deltaU64(before.metal_runtime_deberta_encoder_layer_fallbacks, after.metal_runtime_deberta_encoder_layer_fallbacks);
    profile.metal_deberta_relative_qk_pair_calls = deltaU64(before.metal_runtime_deberta_relative_qk_pair_calls, after.metal_runtime_deberta_relative_qk_pair_calls);
    profile.metal_deberta_relative_qk_pair_fallbacks = deltaU64(before.metal_runtime_deberta_relative_qk_pair_fallbacks, after.metal_runtime_deberta_relative_qk_pair_fallbacks);
    profile.metal_deberta_ffn_fused_calls = deltaU64(before.metal_runtime_deberta_ffn_fused_calls, after.metal_runtime_deberta_ffn_fused_calls);
    profile.metal_deberta_ffn_fused_mps_matmuls = deltaU64(before.metal_runtime_deberta_ffn_fused_mps_matmuls, after.metal_runtime_deberta_ffn_fused_mps_matmuls);
    profile.metal_deberta_ffn_fused_fallbacks = deltaU64(before.metal_runtime_deberta_ffn_fused_fallbacks, after.metal_runtime_deberta_ffn_fused_fallbacks);
    profile.metal_deberta_attention_flash_calls = deltaU64(before.metal_runtime_deberta_attention_flash_calls, after.metal_runtime_deberta_attention_flash_calls);
    profile.metal_deberta_attention_gemm_calls = deltaU64(before.metal_runtime_deberta_attention_gemm_calls, after.metal_runtime_deberta_attention_gemm_calls);
    profile.metal_deberta_attention_gemm_fallbacks = deltaU64(before.metal_runtime_deberta_attention_gemm_fallbacks, after.metal_runtime_deberta_attention_gemm_fallbacks);
    profile.metal_deberta_attention_legacy_calls = deltaU64(before.metal_runtime_deberta_attention_legacy_calls, after.metal_runtime_deberta_attention_legacy_calls);
}

fn cancelOwnedExecutionFrame(cb: *const ComputeBackend, active: *bool) void {
    if (!active.*) return;
    cb.decoderRuntimeCancelFrame() catch {};
    active.* = false;
}

fn submitOwnedExecutionFrame(cb: *const ComputeBackend, active: *bool) !void {
    if (!active.*) return;
    try cb.decoderRuntimeSubmitAndWaitFrame();
    active.* = false;
}

fn beginTrainingPlannedScope(cb: *const ComputeBackend) !metal_compute_mod.MetalCompute.PlannedGraphScope {
    return metal_compute_mod.MetalCompute.beginPlannedGraphScope(cb, .ffn);
}

fn endTrainingPlannedScope(cb: *const ComputeBackend, scope: *metal_compute_mod.MetalCompute.PlannedGraphScope) !void {
    if (!scope.active and !scope.owns_frame) return;
    try metal_compute_mod.MetalCompute.endPlannedGraphScope(cb, scope.*);
    scope.* = .{};
}

pub const CompiledTrainSession = struct {
    allocator: std.mem.Allocator,
    graph: Graph,
    id_map: []NodeId,
    wrt_names: [][]const u8,
    param_grads: []NodeId,
    analysis: interpreter.CachedAnalysis,
    loss_output_index: usize = 0,
    build_profile: TrainStepProfile = .{},
    cached_metal_graph_executor_plan: ?CachedMetalGraphExecutorPlan = null,
    cached_cuda_graph_executor_plan: ?CachedCudaGraphExecutorPlan = null,
    cached_cuda_constants: std.AutoHashMapUnmanaged(NodeId, CT) = .empty,
    cached_cuda_constants_backend: ?*const ComputeBackend = null,

    pub fn init(
        allocator: std.mem.Allocator,
        graph: *const Graph,
        loss_node: NodeId,
        options: TrainStepOptions,
    ) !CompiledTrainSession {
        var profile: TrainStepProfile = .{};
        profile.peak_resident_bytes = currentResidentBytes();
        compiledDiag(
            "init begin nodes={} params={} outputs={} requested_wrt={} rss={}",
            .{
                graph.nodeCount(),
                graph.parameters.items.len,
                graph.outputs.items.len,
                if (options.trainable_params) |params| params.len else @as(usize, 0),
                profile.peak_resident_bytes,
            },
        );

        const wrt = try resolveWrtParams(allocator, graph, options.trainable_params);
        defer allocator.free(wrt);
        compiledDiag("resolved wrt={} rss={}", .{ wrt.len, currentResidentBytes() });

        const autodiff_start = nowNs();
        compiledDiag("autodiff begin nodes={} loss_node={}", .{ graph.nodeCount(), loss_node });
        var grad_result = try autodiff.gradient(allocator, graph, loss_node, wrt);
        profile.autodiff_ns = elapsedNs(autodiff_start);
        profile.peak_resident_bytes = @max(profile.peak_resident_bytes, currentResidentBytes());
        errdefer grad_result.deinit();
        compiledDiag(
            "autodiff done grad_nodes={} grad_params={} autodiff_ms={d:.3} rss={}",
            .{
                grad_result.graph.nodeCount(),
                grad_result.param_grads.len,
                nsToMs(profile.autodiff_ns),
                profile.peak_resident_bytes,
            },
        );

        if (options.checkpoint_config) |cfg| {
            const checkpoint_start = nowNs();
            compiledDiag("checkpoint begin strategy={s} rss={}", .{ @tagName(cfg.strategy), currentResidentBytes() });
            try checkpoint.applyCheckpointing(allocator, &grad_result, cfg);
            profile.checkpoint_ns = elapsedNs(checkpoint_start);
            profile.peak_resident_bytes = @max(profile.peak_resident_bytes, currentResidentBytes());
            compiledDiag("checkpoint done checkpoint_ms={d:.3} rss={}", .{ nsToMs(profile.checkpoint_ns), profile.peak_resident_bytes });
        }

        const lowered_loss = grad_result.id_map[loss_node];
        compiledDiag("mark outputs begin lowered_loss={} existing_outputs={} rss={}", .{ lowered_loss, grad_result.graph.outputs.items.len, currentResidentBytes() });
        grad_result.graph.outputs.clearRetainingCapacity();
        try grad_result.graph.markOutput(lowered_loss);

        for (grad_result.param_grads) |grad_id| {
            if (grad_id != null_node) try grad_result.graph.markOutput(grad_id);
        }
        compiledDiag("mark outputs done outputs={} rss={}", .{ grad_result.graph.outputs.items.len, currentResidentBytes() });

        const id_map = try allocator.dupe(NodeId, grad_result.id_map);
        errdefer allocator.free(id_map);
        const param_grads = try allocator.dupe(NodeId, grad_result.param_grads);
        errdefer allocator.free(param_grads);

        const wrt_names = try allocator.alloc([]const u8, wrt.len);
        errdefer allocator.free(wrt_names);
        for (wrt, 0..) |param_id, i| {
            const param_node = graph.node(param_id);
            wrt_names[i] = try allocator.dupe(u8, graph.parameterName(param_node));
        }
        errdefer {
            for (wrt_names) |name| allocator.free(name);
        }

        const compiled_graph = grad_result.graph;
        grad_result.graph = Graph.init(allocator);
        grad_result.deinit();
        const analysis = try interpreter.CachedAnalysis.compute(allocator, &compiled_graph);
        errdefer {
            var mutable_analysis = analysis;
            mutable_analysis.deinit(allocator);
        }
        profile.total_ns = profile.autodiff_ns + profile.checkpoint_ns;
        compiledDiag(
            "init done compiled_nodes={} outputs={} total_ms={d:.3} peak_rss={}",
            .{
                compiled_graph.nodeCount(),
                compiled_graph.outputs.items.len,
                nsToMs(profile.total_ns),
                profile.peak_resident_bytes,
            },
        );

        return .{
            .allocator = allocator,
            .graph = compiled_graph,
            .id_map = id_map,
            .wrt_names = wrt_names,
            .param_grads = param_grads,
            .analysis = analysis,
            .build_profile = profile,
        };
    }

    pub fn deinit(self: *CompiledTrainSession) void {
        if (self.cached_metal_graph_executor_plan) |*cached| cached.deinit();
        if (self.cached_cuda_graph_executor_plan) |*cached| cached.deinit();
        if (self.cached_cuda_constants_backend) |backend| {
            var constant_it = self.cached_cuda_constants.iterator();
            while (constant_it.next()) |entry| backend.free(entry.value_ptr.*);
        }
        self.cached_cuda_constants.deinit(self.allocator);
        self.analysis.deinit(self.allocator);
        self.graph.deinit();
        self.allocator.free(self.id_map);
        for (self.wrt_names) |name| self.allocator.free(name);
        self.allocator.free(self.wrt_names);
        self.allocator.free(self.param_grads);
        self.* = undefined;
    }

    const CachedMetalGraphExecutorPlan = struct {
        base_plan: partition_mod.PartitionPlan,
        buffer_plan: buffer_plan_mod.BufferPlan,
        assignments: []device_mesh.DeviceId,
        partitioned: bool,
        unsupported_ops: usize = 0,

        fn deinit(self: *CachedMetalGraphExecutorPlan) void {
            self.buffer_plan.deinit();
            self.base_plan.deinit();
            self.base_plan.allocator.free(self.assignments);
            self.* = undefined;
        }
    };

    const CachedCudaGraphExecutorPlan = struct {
        base_plan: partition_mod.PartitionPlan,
        buffer_plan: buffer_plan_mod.BufferPlan,
        assignments: []device_mesh.DeviceId,

        fn deinit(self: *CachedCudaGraphExecutorPlan) void {
            self.buffer_plan.deinit();
            self.base_plan.deinit();
            self.base_plan.allocator.free(self.assignments);
            self.* = undefined;
        }
    };

    pub fn execute(
        self: *CompiledTrainSession,
        cb: *const ComputeBackend,
        runtime_inputs: ?std.AutoHashMapUnmanaged(NodeId, CT),
    ) !TrainStepResult {
        return self.executeInternal(cb, runtime_inputs, false, false);
    }

    /// Materialize backend-owned immutable graph state before the measured
    /// execution interval. Strict CUDA uses this boundary to distinguish the
    /// one-time constant upload from hidden hot-step staging.
    pub fn prepareBackend(self: *CompiledTrainSession, cb: *const ComputeBackend) !void {
        if (cb.kind() == .cuda) try self.ensureCachedCudaConstants(cb);
    }

    pub fn backendPrepared(self: *const CompiledTrainSession, cb: *const ComputeBackend) bool {
        return cb.kind() != .cuda or self.cached_cuda_constants_backend == cb;
    }

    pub fn executeDeviceGradients(
        self: *CompiledTrainSession,
        cb: *const ComputeBackend,
        runtime_inputs: ?std.AutoHashMapUnmanaged(NodeId, CT),
    ) !TrainStepResult {
        return self.executeInternal(cb, runtime_inputs, true, false);
    }

    /// Execute a strict CUDA training step without requiring the opt-in graph
    /// executor environment variable. The explicit disable switch remains a
    /// hard operational kill switch, and this entry point never falls through
    /// to the direct interpreter path.
    pub fn executeDeviceGradientsStrictCuda(
        self: *CompiledTrainSession,
        cb: *const ComputeBackend,
        runtime_inputs: ?std.AutoHashMapUnmanaged(NodeId, CT),
    ) !TrainStepResult {
        if (cb.kind() != .cuda) return error.StrictCudaBackendRequired;
        if (platform.env.getenvBoolDefault("TERMITE_DISABLE_TRAINING_GRAPH_EXECUTOR", false)) {
            return error.StrictCudaGraphExecutorDisabled;
        }
        return self.executeInternal(cb, runtime_inputs, true, true);
    }

    fn executeInternal(
        self: *CompiledTrainSession,
        cb: *const ComputeBackend,
        runtime_inputs: ?std.AutoHashMapUnmanaged(NodeId, CT),
        retain_device_gradients: bool,
        strict_cuda_execution: bool,
    ) !TrainStepResult {
        const total_start = nowNs();
        var profile: TrainStepProfile = .{};
        profile.peak_resident_bytes = currentResidentBytes();

        var rt_list = std.ArrayListUnmanaged(RuntimeInput).empty;
        defer rt_list.deinit(self.allocator);
        if (runtime_inputs) |rt| {
            var it = rt.iterator();
            while (it.next()) |entry| {
                const old_id = entry.key_ptr.*;
                if (old_id >= self.id_map.len) continue;
                const new_id = self.id_map[old_id];
                if (new_id == null_node) continue;
                try rt_list.append(self.allocator, .{ .node_id = new_id, .value = entry.value_ptr.* });
            }
        }

        if (cb.kind() == .cuda) {
            try self.ensureCachedCudaConstants(cb);
            var constants = self.cached_cuda_constants.iterator();
            while (constants.next()) |entry| {
                try rt_list.append(self.allocator, .{ .node_id = entry.key_ptr.*, .value = entry.value_ptr.* });
            }
        }

        const rt_slice: ?[]const RuntimeInput = if (rt_list.items.len > 0) rt_list.items else null;

        const execute_start = nowNs();
        compiledDiag(
            "execute begin nodes={} outputs={} runtime_inputs={} retain_device_gradients={} rss={}",
            .{
                self.graph.nodeCount(),
                self.graph.outputs.items.len,
                rt_list.items.len,
                retain_device_gradients,
                currentResidentBytes(),
            },
        );
        maybeDumpGraphNodes(&self.graph);

        if (trainingGraphExecutorParityNodeIds()) |node_ids_raw| {
            if (!try self.runSelectedNodeParityDiagnostic(cb, rt_slice, node_ids_raw)) {
                return error.TrainingGraphExecutorParityMismatch;
            }
        }

        if (trainingGraphExecutorParityCheckEnabled() and cb.kind() == .metal and platform.env.getenvBoolDefault("TERMITE_ENABLE_TRAINING_GRAPH_EXECUTOR", false) and !platform.env.getenvBoolDefault("TERMITE_DISABLE_TRAINING_GRAPH_EXECUTOR", false)) {
            var direct_result = try self.executeWithDirectInterpreter(cb, rt_slice, retain_device_gradients, nowNs(), .{});
            errdefer direct_result.deinit();
            const graph_result_opt = try self.executeWithSingleMetalGraphExecutor(cb, rt_slice, retain_device_gradients, total_start, execute_start, &profile);
            var graph_result = graph_result_opt orelse return error.TrainingGraphExecutorUnavailable;
            errdefer graph_result.deinit();
            const report = try self.compareTrainStepResults(cb, &direct_result, &graph_result);
            printTrainingGraphExecutorParityReport(report);
            if (!report.passed) return error.TrainingGraphExecutorParityMismatch;
            direct_result.deinit();
            return graph_result;
        }

        if (try self.executeWithSingleMetalGraphExecutor(cb, rt_slice, retain_device_gradients, total_start, execute_start, &profile)) |result| {
            return result;
        }

        if (try self.executeWithSingleCudaGraphExecutor(cb, rt_slice, retain_device_gradients, strict_cuda_execution, total_start, execute_start, &profile)) |result| {
            return result;
        }
        if (strict_cuda_execution) return error.StrictCudaGraphExecutorUnavailable;

        // Keep the partial profile (plan cache counters, fallback reason) so
        // step rows still report why the graph executor was not used.
        return try self.executeWithDirectInterpreter(cb, rt_slice, retain_device_gradients, total_start, profile);
    }

    fn runSelectedNodeParityDiagnostic(
        self: *CompiledTrainSession,
        cb: *const ComputeBackend,
        runtime_inputs: ?[]const RuntimeInput,
        node_ids_raw: []const u8,
    ) !bool {
        if (cb.kind() != .metal) return error.TrainingGraphExecutorParityDiagnosticRequiresMetal;
        if (!platform.env.getenvBoolDefault("TERMITE_ENABLE_TRAINING_GRAPH_EXECUTOR", false)) return error.TrainingGraphExecutorUnavailable;

        const selected = try parseParityNodeIds(self.allocator, node_ids_raw, self.graph.nodeCount());
        defer self.allocator.free(selected);
        if (selected.len == 0) return error.TrainingGraphExecutorParityNoNodes;

        // Default mode replaces the graph outputs with the selected nodes,
        // which restricts reachability (the backward subgraph is not
        // executed when only forward nodes are selected). KEEP_OUTPUTS mode
        // appends the selected nodes to the existing outputs instead so the
        // parity run executes with the exact reachability/liveness of a real
        // training step — required to reproduce bugs that only manifest with
        // the full loss+gradient output set.
        const keep_outputs = platform.env.getenvBoolDefault("TERMITE_TRAINING_GRAPH_EXECUTOR_PARITY_KEEP_OUTPUTS", false);
        const saved_outputs = try self.allocator.dupe(NodeId, self.graph.outputs.items);
        defer self.allocator.free(saved_outputs);
        if (!keep_outputs) self.graph.outputs.clearRetainingCapacity();
        defer {
            self.graph.outputs.clearRetainingCapacity();
            self.graph.outputs.appendSlice(self.allocator, saved_outputs) catch {};
        }
        // Map each selected node to its output index (reusing an existing
        // output entry when present so we never create duplicate outputs).
        const selected_output_indices = try self.allocator.alloc(usize, selected.len);
        defer self.allocator.free(selected_output_indices);
        for (selected, selected_output_indices) |node_id, *output_index| {
            var existing: ?usize = null;
            for (self.graph.outputs.items, 0..) |out_id, oi| {
                if (out_id == node_id) {
                    existing = oi;
                    break;
                }
            }
            if (existing) |oi| {
                output_index.* = oi;
            } else {
                output_index.* = self.graph.outputs.items.len;
                try self.graph.markOutput(node_id);
            }
        }

        var owned_execution_frame = try beginOwnedExecutionFrame(cb);
        errdefer cancelOwnedExecutionFrame(cb, &owned_execution_frame);
        var planned_scope = try beginTrainingPlannedScope(cb);
        errdefer endTrainingPlannedScope(cb, &planned_scope) catch {};
        var direct_result = try interpreter.execute(
            self.allocator,
            &self.graph,
            cb,
            .{ .runtime_inputs = runtime_inputs },
        );
        // Arm cleanup before the fallible end-scope/submit calls below.
        defer direct_result.deinit(cb);
        try endTrainingPlannedScope(cb, &planned_scope);
        try submitOwnedExecutionFrame(cb, &owned_execution_frame);

        var diagnostics = partition_mod.CapabilityDiagnostics{};
        var base_plan = if (platform.env.getenvBoolDefault("TERMITE_TRAINING_GRAPH_EXECUTOR_PARTITIONED", false))
            try self.buildPartitionedMetalGraphExecutorPlan(&diagnostics)
        else
            try self.buildSingleMetalGraphExecutorPlan();
        defer base_plan.deinit();

        const assignments = try self.allocator.alloc(device_mesh.DeviceId, base_plan.partitions.len);
        defer self.allocator.free(assignments);
        for (base_plan.partitions, 0..) |part, idx| {
            assignments[idx] = if (part.backend == .native) 1 else 0;
        }
        var dpp = multi_executor.DevicePartitionPlan{
            .base = base_plan,
            .device_assignment = assignments,
            .allocator = self.allocator,
        };

        var fallback_native = try FallbackNativeBackend.init(self.allocator);
        defer fallback_native.deinit(self.allocator);
        var devices_buf = [_]device_mesh.DeviceEntry{
            .{ .id = 0, .backend = cb, .kind = .metal },
            .{ .id = 1, .backend = &fallback_native.backend, .kind = .native },
        };
        var mesh = try device_mesh.DeviceMesh.init(self.allocator, devices_buf[0..]);
        defer mesh.deinit();

        var graph_result = try multi_executor.executeMultiDevice(
            self.allocator,
            &self.graph,
            &dpp,
            &mesh,
            .{
                .runtime_inputs = runtime_inputs,
                .skip_metal_fused_patterns = trainingGraphExecutorSkipMetalFusedPatterns(),
                .collect_partition_stats = false,
                .preserve_runtime_input_residency = true,
            },
        );
        defer graph_result.deinit(&mesh);

        var passed = true;
        for (selected, selected_output_indices) |node_id, output_idx| {
            const direct_data = try cb.toFloat32(direct_result.outputs[output_idx], self.allocator);
            defer self.allocator.free(direct_data);
            const graph_device = mesh.device(graph_result.output_devices[output_idx]) orelse return error.DeviceNotFound;
            const graph_data = try graph_device.backend.toFloat32(graph_result.outputs[output_idx], self.allocator);
            defer self.allocator.free(graph_data);
            const diff = compareFloatSlices(direct_data, graph_data);
            if (diff.len_mismatch or (diff.max_abs > trainingGraphExecutorParityGradAbsTolerance() and diff.max_rel > trainingGraphExecutorParityGradRelTolerance())) {
                passed = false;
            }
            const node_passed = !diff.len_mismatch and (diff.max_abs <= trainingGraphExecutorParityGradAbsTolerance() or diff.max_rel <= trainingGraphExecutorParityGradRelTolerance());
            std.debug.print(
                "training_graph_executor_node_parity: node={} op={s} passed={} direct_len={} graph_len={} max_abs_diff={d:.9} max_rel_diff={d:.9} first_diff_index={}\n",
                .{
                    node_id,
                    @tagName(std.meta.activeTag(self.graph.node(node_id).op)),
                    node_passed,
                    direct_data.len,
                    graph_data.len,
                    diff.max_abs,
                    diff.max_rel,
                    diff.first_diff_index,
                },
            );
            if (platform.env.getenvBoolDefault("TERMITE_TRAINING_GRAPH_EXECUTOR_PARITY_PRINT_VALUES", false) and graph_data.len > 0 and direct_data.len > 0) {
                const start = if (diff.first_diff_index != std.math.maxInt(usize)) diff.first_diff_index else 0;
                const limit = @min(@min(direct_data.len, graph_data.len), start + 8);
                var sample_idx = start;
                while (sample_idx < limit) : (sample_idx += 1) {
                    std.debug.print(
                        "training_graph_executor_node_parity_value: node={} index={} direct={d:.9} graph={d:.9} abs_diff={d:.9}\n",
                        .{
                            node_id,
                            sample_idx,
                            direct_data[sample_idx],
                            graph_data[sample_idx],
                            @abs(direct_data[sample_idx] - graph_data[sample_idx]),
                        },
                    );
                }
            }
            if (platform.env.getenv("TERMITE_TRAINING_GRAPH_EXECUTOR_PARITY_SAMPLE_INDICES")) |sample_raw| {
                var parts = std.mem.splitScalar(u8, sample_raw, ',');
                while (parts.next()) |part_raw| {
                    const part = std.mem.trim(u8, part_raw, " \t\r\n");
                    if (part.len == 0) continue;
                    const sample_idx = std.fmt.parseInt(usize, part, 10) catch continue;
                    if (sample_idx >= direct_data.len or sample_idx >= graph_data.len) continue;
                    std.debug.print(
                        "training_graph_executor_node_parity_sample: node={} index={} direct={d:.9} graph={d:.9} abs_diff={d:.9}\n",
                        .{
                            node_id,
                            sample_idx,
                            direct_data[sample_idx],
                            graph_data[sample_idx],
                            @abs(direct_data[sample_idx] - graph_data[sample_idx]),
                        },
                    );
                }
            }
        }
        std.debug.print("training_graph_executor_node_parity_summary: passed={} nodes={}\n", .{ passed, selected.len });
        return passed;
    }

    fn executeWithDirectInterpreter(
        self: *CompiledTrainSession,
        cb: *const ComputeBackend,
        runtime_inputs: ?[]const RuntimeInput,
        retain_device_gradients: bool,
        total_start: u64,
        initial_profile: TrainStepProfile,
    ) !TrainStepResult {
        var profile = initial_profile;
        const training_runtime_before = cb.trainingRuntimeStats();
        profile.peak_resident_bytes = @max(profile.peak_resident_bytes, currentResidentBytes());
        const execute_start = nowNs();

        var owned_execution_frame = try beginOwnedExecutionFrame(cb);
        errdefer cancelOwnedExecutionFrame(cb, &owned_execution_frame);
        var planned_scope = try beginTrainingPlannedScope(cb);
        errdefer endTrainingPlannedScope(cb, &planned_scope) catch {};
        var exec_result = try interpreter.execute(
            self.allocator,
            &self.graph,
            cb,
            .{ .runtime_inputs = runtime_inputs, .cached_analysis = self.analysis },
        );
        // Arm cleanup before the fallible end-scope/submit calls below, otherwise an
        // error from either leaks exec_result's output CTs / device buffers.
        defer exec_result.deinit(cb);
        try endTrainingPlannedScope(cb, &planned_scope);
        try submitOwnedExecutionFrame(cb, &owned_execution_frame);
        profile.execute_ns = elapsedNs(execute_start);
        profile.peak_resident_bytes = @max(profile.peak_resident_bytes, currentResidentBytes());
        compiledDiag("execute done execute_ms={d:.3} rss={}", .{ nsToMs(profile.execute_ns), profile.peak_resident_bytes });

        const extract_start = nowNs();
        compiledDiag("extract begin outputs={} retain_device_gradients={} rss={}", .{ exec_result.outputs.len, retain_device_gradients, currentResidentBytes() });
        maybeDumpGraphOutputValues(
            &self.graph,
            self.allocator,
            self.loss_output_index,
            cb,
            null,
            null,
            exec_result.outputs,
            "interpreter",
        );
        const loss_data = try cb.toFloat32(exec_result.outputs[self.loss_output_index], self.allocator);
        defer self.allocator.free(loss_data);
        if (loss_data.len == 0) {
            const loss_output_node = self.graph.outputs.items[self.loss_output_index];
            std.debug.print(
                "[compiled-train] ERROR: empty loss output node={} op={s} shape={any}\n",
                .{ loss_output_node, @tagName(std.meta.activeTag(self.graph.node(loss_output_node).op)), self.graph.node(loss_output_node).output_shape },
            );
            return error.EmptyLossOutput;
        }
        const loss_value: f32 = loss_data[0];

        var gradients = std.StringHashMapUnmanaged([]f32){};
        var device_gradients = std.StringHashMapUnmanaged(CT){};
        errdefer {
            var it = gradients.iterator();
            while (it.next()) |entry| {
                self.allocator.free(entry.key_ptr.*);
                self.allocator.free(entry.value_ptr.*);
            }
            gradients.deinit(self.allocator);
            var device_it = device_gradients.iterator();
            while (device_it.next()) |entry| {
                self.allocator.free(entry.key_ptr.*);
                cb.free(entry.value_ptr.*);
            }
            device_gradients.deinit(self.allocator);
        }

        var grad_output_idx: usize = self.loss_output_index + 1;
        for (self.wrt_names, 0..) |name, i| {
            if (self.param_grads[i] == null_node) continue;
            const grad_ct = exec_result.outputs[grad_output_idx];
            const grad_output_node = self.graph.outputs.items[grad_output_idx];
            grad_output_idx += 1;
            const owned_name = try self.allocator.dupe(u8, name);
            errdefer self.allocator.free(owned_name);
            if (retain_device_gradients) {
                const retained = try cloneTensorForOutputShape(self.allocator, &self.graph, cb, grad_ct, grad_output_node);
                errdefer cb.free(retained);
                try device_gradients.put(self.allocator, owned_name, retained);
            } else {
                const grad_data = try cb.toFloat32(grad_ct, self.allocator);
                try gradients.put(self.allocator, owned_name, grad_data);
            }
        }

        profile.extract_ns = elapsedNs(extract_start);
        recordTrainingRuntimeProfile(&profile, training_runtime_before, cb.trainingRuntimeStats());
        profile.total_ns = elapsedNs(total_start);
        profile.peak_resident_bytes = @max(profile.peak_resident_bytes, currentResidentBytes());
        compiledDiag(
            "extract done gradients={} device_gradients={} extract_ms={d:.3} total_ms={d:.3} rss={}",
            .{
                gradients.count(),
                device_gradients.count(),
                nsToMs(profile.extract_ns),
                nsToMs(profile.total_ns),
                profile.peak_resident_bytes,
            },
        );

        return .{
            .loss = loss_value,
            .gradients = gradients,
            .device_gradients = device_gradients,
            .profile = profile,
            .allocator = self.allocator,
            .compute_backend = if (retain_device_gradients) cb else null,
        };
    }

    fn executeWithSingleMetalGraphExecutor(
        self: *CompiledTrainSession,
        cb: *const ComputeBackend,
        runtime_inputs: ?[]const RuntimeInput,
        retain_device_gradients: bool,
        total_start: u64,
        execute_start: u64,
        profile: *TrainStepProfile,
    ) !?TrainStepResult {
        if (cb.kind() != .metal) return null;
        if (!platform.env.getenvBoolDefault("TERMITE_ENABLE_TRAINING_GRAPH_EXECUTOR", false)) return null;
        if (platform.env.getenvBoolDefault("TERMITE_DISABLE_TRAINING_GRAPH_EXECUTOR", false)) return null;

        const cached = try self.cachedMetalGraphExecutorPlan(cb, profile);
        if (retain_device_gradients and !self.planKeepsGradientOutputsOnMetal(&cached.base_plan)) {
            profile.graph_executor_fallback_reason = "gradient_output_on_native_partition";
            std.debug.print(
                "[compiled-train] WARNING: graph executor fallback reason=gradient_output_on_native_partition unsupported_ops={}; retrying regular compiled Metal path\n",
                .{cached.unsupported_ops},
            );
            return null;
        }

        var dpp = multi_executor.DevicePartitionPlan{
            .base = cached.base_plan,
            .device_assignment = cached.assignments,
            .allocator = self.allocator,
        };

        var fallback_native = try FallbackNativeBackend.init(self.allocator);
        defer fallback_native.deinit(self.allocator);

        var devices_buf = [_]device_mesh.DeviceEntry{
            .{ .id = 0, .backend = cb, .kind = .metal },
            .{ .id = 1, .backend = &fallback_native.backend, .kind = .native },
        };
        var mesh = try device_mesh.DeviceMesh.init(self.allocator, devices_buf[0..]);
        defer mesh.deinit();

        const metal_debug_before = cb.debugTimingSnapshot().provider;
        var multi_result = multi_executor.executeMultiDevice(
            self.allocator,
            &self.graph,
            &dpp,
            &mesh,
            .{
                .runtime_inputs = runtime_inputs,
                .cached_analysis = self.analysis,
                .cached_buffer_plan = &cached.buffer_plan,
                .skip_metal_fused_patterns = trainingGraphExecutorSkipMetalFusedPatterns(),
                .collect_partition_stats = true,
                .preserve_runtime_input_residency = true,
            },
        ) catch |err| switch (err) {
            error.MissingRuntimeInput => {
                profile.graph_executor_fallback_reason = "missing_partition_input";
                std.debug.print(
                    "[compiled-train] WARNING: graph executor fallback reason=missing_partition_input; retrying regular compiled Metal path\n",
                    .{},
                );
                return null;
            },
            else => return err,
        };
        defer multi_result.deinit(&mesh);

        profile.execute_ns = elapsedNs(execute_start);
        recordMetalDebugProfile(profile, metal_debug_before, cb.debugTimingSnapshot().provider);
        profile.peak_resident_bytes = @max(profile.peak_resident_bytes, currentResidentBytes());
        profile.graph_executor_partitions = multi_result.stats.partitions_executed;
        profile.graph_executor_command_dispatches = multi_result.stats.backend_command_dispatches;
        profile.graph_executor_planned_dispatches = multi_result.stats.planned_operator_dispatches;
        profile.graph_executor_interpreter_fallbacks = multi_result.stats.interpreter_fallbacks;
        profile.graph_executor_host_outputs = multi_result.stats.host_materialized_outputs;
        profile.graph_executor_true_host_outputs =
            multi_result.stats.host_materialized_command_outputs +
            multi_result.stats.host_materialized_interpreter_outputs +
            multi_result.stats.host_materialized_pre_materialized_constant_outputs +
            multi_result.stats.host_materialized_runtime_region_outputs +
            multi_result.stats.host_materialized_unattributed_outputs;
        profile.graph_executor_host_output_command = multi_result.stats.host_materialized_command_outputs;
        profile.graph_executor_host_output_interpreter = multi_result.stats.host_materialized_interpreter_outputs;
        profile.graph_executor_host_output_pre_materialized_constant = multi_result.stats.host_materialized_pre_materialized_constant_outputs;
        profile.graph_executor_host_output_runtime_region = multi_result.stats.host_materialized_runtime_region_outputs;
        profile.graph_executor_host_output_parameter = multi_result.stats.host_materialized_parameter_outputs;
        profile.graph_executor_host_output_unattributed = multi_result.stats.host_materialized_unattributed_outputs;
        profile.graph_executor_device_outputs = multi_result.stats.device_resident_outputs;
        profile.graph_executor_device_output_parameter = multi_result.stats.device_resident_parameter_outputs;
        profile.graph_executor_metal_frame_chunk_boundaries = multi_result.stats.metal_frame_chunk_boundaries;
        profile.graph_executor_metal_frame_chunk_promoted_values = multi_result.stats.metal_frame_chunk_promoted_values;
        profile.graph_executor_metal_frame_chunk_swept_values = multi_result.stats.metal_frame_chunk_swept_values;
        profile.graph_executor_metal_chunk_local_output_peak_bytes = multi_result.stats.metal_chunk_local_output_peak_bytes;
        profile.graph_executor_metal_chunk_local_output_live_bytes = multi_result.stats.metal_chunk_local_output_live_bytes;
        profile.graph_executor_metal_chunk_local_output_allocations = multi_result.stats.metal_chunk_local_output_allocations;
        profile.graph_executor_metal_chunk_local_output_reuse_hits = multi_result.stats.metal_chunk_local_output_reuse_hits;
        profile.graph_executor_metal_chunk_local_output_consumed_hints = multi_result.stats.metal_chunk_local_output_consumed_hints;
        profile.graph_executor_metal_chunk_local_output_unconsumed_hints = multi_result.stats.metal_chunk_local_output_unconsumed_hints;
        profile.graph_executor_metal_chunk_local_output_spill_bytes = multi_result.stats.metal_chunk_local_output_spill_bytes;
        profile.graph_executor_metal_chunk_local_output_alias_conflicts = multi_result.stats.metal_chunk_local_output_alias_conflicts;
        profile.graph_executor_metal_chunk_local_output_resets = multi_result.stats.metal_chunk_local_output_resets;
        profile.graph_executor_metal_chunk_local_output_reset_freed_bytes = multi_result.stats.metal_chunk_local_output_reset_freed_bytes;
        profile.graph_executor_metal_chunk_local_output_discard_freed_bytes = multi_result.stats.metal_chunk_local_output_discard_freed_bytes;
        profile.graph_executor_metal_chunk_local_output_reset_live_carry_values = multi_result.stats.metal_chunk_local_output_reset_live_carry_values;
        profile.graph_executor_metal_gather_input_promotions = multi_result.stats.metal_gather_input_promotions;
        profile.graph_executor_metal_gather_input_promotion_bytes = multi_result.stats.metal_gather_input_promotion_bytes;
        profile.graph_executor_metal_gather_input_promotion_ns = multi_result.stats.metal_gather_input_promotion_ns;
        profile.graph_executor_metal_gather_output_promotions = multi_result.stats.metal_gather_output_promotions;
        profile.graph_executor_metal_gather_output_promotion_bytes = multi_result.stats.metal_gather_output_promotion_bytes;
        profile.graph_executor_metal_gather_output_promotion_ns = multi_result.stats.metal_gather_output_promotion_ns;
        profile.graph_executor_metal_reduce_input_promotions = multi_result.stats.metal_reduce_input_promotions;
        profile.graph_executor_metal_reduce_input_promotion_bytes = multi_result.stats.metal_reduce_input_promotion_bytes;
        profile.graph_executor_metal_reduce_input_promotion_ns = multi_result.stats.metal_reduce_input_promotion_ns;
        profile.graph_executor_metal_resident_input_cache_hits = multi_result.stats.metal_resident_input_cache_hits;
        profile.graph_executor_metal_resident_input_cache_misses = multi_result.stats.metal_resident_input_cache_misses;
        profile.graph_executor_metal_resident_input_cache_unique_promotions = multi_result.stats.metal_resident_input_cache_unique_promotions;
        profile.graph_executor_metal_resident_input_cache_retained_live_bytes = multi_result.stats.metal_resident_input_cache_retained_live_bytes;
        profile.graph_executor_metal_resident_input_cache_retained_peak_bytes = multi_result.stats.metal_resident_input_cache_retained_peak_bytes;
        profile.graph_executor_metal_resident_input_cache_reused_bytes = multi_result.stats.metal_resident_input_cache_reused_bytes;
        profile.graph_executor_metal_resident_input_cache_released_bytes = multi_result.stats.metal_resident_input_cache_released_bytes;
        profile.graph_executor_metal_eager_arena_peak_bytes = multi_result.stats.metal_eager_arena_peak_bytes;
        profile.graph_executor_metal_eager_arena_live_bytes = multi_result.stats.metal_eager_arena_live_bytes;
        profile.graph_executor_metal_eager_arena_reuse_hits = multi_result.stats.metal_eager_arena_reuse_hits;
        profile.graph_executor_metal_eager_arena_allocations = multi_result.stats.metal_eager_arena_allocations;
        profile.graph_executor_metal_eager_arena_spill_bytes = multi_result.stats.metal_eager_arena_spill_bytes;
        profile.graph_executor_metal_eager_arena_hazard_declines = multi_result.stats.metal_eager_arena_hazard_declines;
        profile.graph_executor_metal_eager_arena_alias_conflicts = multi_result.stats.metal_eager_arena_alias_conflicts;
        profile.graph_executor_metal_eager_arena_alias_reclaims = multi_result.stats.metal_eager_arena_alias_reclaims;
        profile.graph_executor_metal_eager_arena_alias_reclaim_bytes = multi_result.stats.metal_eager_arena_alias_reclaim_bytes;
        profile.graph_executor_regions = multi_result.stats.graph_regions;
        profile.graph_executor_region_ops = multi_result.stats.graph_region_ops;
        profile.graph_executor_runtime_region_dispatches = multi_result.stats.runtime_region_plan_dispatches;
        profile.graph_executor_runtime_region_fallbacks = multi_result.stats.runtime_region_fallbacks;
        profile.graph_executor_runtime_region_active_regions = multi_result.stats.runtime_region_plan_active_regions;
        profile.graph_executor_runtime_region_covered_nodes = multi_result.stats.runtime_region_plan_covered_nodes;
        profile.graph_executor_runtime_region_elided_nodes = multi_result.stats.runtime_region_plan_elided_nodes;
        profile.graph_executor_runtime_region_plan_compiles = multi_result.stats.runtime_region_plan_compiles;
        profile.graph_executor_runtime_region_plan_reuses = multi_result.stats.runtime_region_plan_reuses;
        profile.graph_executor_runtime_region_pre_skipped_nodes = multi_result.stats.runtime_region_pre_skipped_nodes;
        profile.graph_executor_runtime_region_pre_skipped_transposes = multi_result.stats.runtime_region_pre_skipped_transposes;
        profile.graph_executor_runtime_region_pre_skip_declined_external_consumers = multi_result.stats.runtime_region_pre_skip_declined_external_consumers;
        profile.metal_deberta_ffn_forward_regions = multi_result.stats.metal_deberta_ffn_forward_regions;
        profile.metal_deberta_encoder_lora_layer_regions = multi_result.stats.metal_deberta_encoder_lora_layer_regions;
        profile.metal_deberta_encoder_lora_residual_layernorm_regions = multi_result.stats.metal_deberta_encoder_lora_residual_layernorm_regions;
        profile.metal_deberta_encoder_lora_layer_scaffold_regions = multi_result.stats.metal_deberta_encoder_lora_layer_scaffold_regions;
        profile.metal_deberta_encoder_lora_layer_fallbacks = multi_result.stats.metal_deberta_encoder_lora_layer_fallbacks;
        profile.metal_lora_backward_regions = multi_result.stats.metal_lora_backward_regions;
        profile.metal_low_rank_lora_backward_regions = multi_result.stats.metal_low_rank_lora_backward_regions;
        profile.metal_rank_adapter_backward_regions = multi_result.stats.metal_rank_adapter_backward_regions;
        profile.metal_ffn_gelu_backward_regions = multi_result.stats.metal_ffn_gelu_backward_regions;
        profile.metal_head_mlp_forward_regions = multi_result.stats.metal_head_mlp_forward_regions;
        profile.metal_head_mlp_backward_regions = multi_result.stats.metal_head_mlp_backward_regions;
        profile.metal_command_dot_general_dispatches = multi_result.stats.metal_command_dot_general_dispatches;
        profile.metal_command_head_dot_dispatches = multi_result.stats.metal_command_head_dot_dispatches;
        profile.metal_command_transpose_dispatches = multi_result.stats.metal_command_transpose_dispatches;
        profile.metal_command_gather_dispatches = multi_result.stats.metal_command_gather_dispatches;
        profile.metal_command_reduce_dispatches = multi_result.stats.metal_command_reduce_dispatches;
        profile.metal_command_elementwise_dispatches = multi_result.stats.metal_command_elementwise_dispatches;
        profile.metal_command_activation_dispatches = multi_result.stats.metal_command_activation_dispatches;
        profile.metal_command_activation_backward_dispatches = multi_result.stats.metal_command_activation_backward_dispatches;
        profile.metal_command_other_dispatches = multi_result.stats.metal_command_other_dispatches;
        profile.graph_executor_runtime_frame_candidates = multi_result.stats.runtime_frame_candidates;
        profile.graph_executor_runtime_frame_eligible = multi_result.stats.runtime_frame_eligible;
        profile.graph_executor_runtime_frame_metadata_ready = multi_result.stats.runtime_frame_metadata_ready;
        profile.graph_executor_runtime_frame_ineligible_no_regions = multi_result.stats.runtime_frame_ineligible_no_regions;
        profile.graph_executor_runtime_frame_ineligible_missing_qkv = multi_result.stats.runtime_frame_ineligible_missing_qkv;
        profile.graph_executor_runtime_frame_ineligible_missing_attention = multi_result.stats.runtime_frame_ineligible_missing_attention;
        profile.graph_executor_runtime_frame_ineligible_missing_ffn = multi_result.stats.runtime_frame_ineligible_missing_ffn;
        profile.graph_executor_runtime_frame_ineligible_missing_ple = multi_result.stats.runtime_frame_ineligible_missing_ple;
        profile.graph_executor_runtime_frame_ineligible_single_row = multi_result.stats.runtime_frame_ineligible_single_row;
        profile.graph_executor_runtime_frame_ineligible_non_layer_order = multi_result.stats.runtime_frame_ineligible_non_layer_order;
        profile.graph_executor_runtime_frame_ineligible_shape_mismatch = multi_result.stats.runtime_frame_ineligible_shape_mismatch;
        profile.graph_executor_runtime_frame_ineligible_missing_model_metadata = multi_result.stats.runtime_frame_ineligible_missing_model_metadata;
        compiledDiag(
            "graph executor execute done partitions={} commands={} planned={} fallbacks={} plan_cache_hits={} plan_cache_misses={} plan_build_ms={d:.3} execute_ms={d:.3} rss={}",
            .{
                dpp.base.partitions.len,
                profile.graph_executor_command_dispatches,
                profile.graph_executor_planned_dispatches,
                profile.graph_executor_interpreter_fallbacks,
                profile.graph_executor_plan_cache_hits,
                profile.graph_executor_plan_cache_misses,
                nsToMs(profile.graph_executor_plan_build_ns),
                nsToMs(profile.execute_ns),
                profile.peak_resident_bytes,
            },
        );

        return try self.extractTrainStepResultFromGraphOutputs(
            cb,
            &mesh,
            &multi_result,
            retain_device_gradients,
            total_start,
            profile,
        );
    }

    fn executeWithSingleCudaGraphExecutor(
        self: *CompiledTrainSession,
        cb: *const ComputeBackend,
        runtime_inputs: ?[]const RuntimeInput,
        retain_device_gradients: bool,
        strict_cuda_execution: bool,
        total_start: u64,
        execute_start: u64,
        profile: *TrainStepProfile,
    ) !?TrainStepResult {
        if (cb.kind() != .cuda) return null;
        const graph_executor_selected = try selectCudaTrainingGraphExecutor(
            strict_cuda_execution,
            platform.env.getenvBoolDefault("TERMITE_ENABLE_TRAINING_GRAPH_EXECUTOR", false),
            platform.env.getenvBoolDefault("TERMITE_DISABLE_TRAINING_GRAPH_EXECUTOR", false),
        );
        if (!graph_executor_selected) return null;

        const runtime_before = cb.trainingRuntimeStats();
        const cached = try self.cachedCudaGraphExecutorPlan(profile);
        var dpp = multi_executor.DevicePartitionPlan{
            .base = cached.base_plan,
            .device_assignment = cached.assignments,
            .allocator = self.allocator,
        };
        var devices_buf = [_]device_mesh.DeviceEntry{
            .{ .id = 0, .backend = cb, .kind = .cuda },
        };
        var mesh = try device_mesh.DeviceMesh.init(self.allocator, devices_buf[0..]);
        defer mesh.deinit();

        var multi_result = try multi_executor.executeMultiDevice(
            self.allocator,
            &self.graph,
            &dpp,
            &mesh,
            .{
                .runtime_inputs = runtime_inputs,
                .cached_analysis = self.analysis,
                .cached_buffer_plan = &cached.buffer_plan,
                .collect_partition_stats = true,
                .preserve_runtime_input_residency = true,
            },
        );
        defer multi_result.deinit(&mesh);

        profile.execute_ns = elapsedNs(execute_start);
        profile.peak_resident_bytes = @max(profile.peak_resident_bytes, currentResidentBytes());
        profile.graph_executor_partitions = multi_result.stats.partitions_executed;
        profile.graph_executor_command_dispatches = multi_result.stats.backend_command_dispatches;
        profile.graph_executor_planned_dispatches = multi_result.stats.planned_operator_dispatches;
        profile.graph_executor_interpreter_fallbacks = multi_result.stats.interpreter_fallbacks;
        profile.graph_executor_device_outputs = @intCast(multi_result.outputs.len);
        profile.graph_executor_device_output_parameter = @intCast(multi_result.outputs.len -| 1);

        var result = try self.extractTrainStepResultFromGraphOutputs(
            cb,
            &mesh,
            &multi_result,
            retain_device_gradients,
            total_start,
            profile,
        );
        recordTrainingRuntimeProfile(&result.profile, runtime_before, cb.trainingRuntimeStats());
        return result;
    }

    fn cachedMetalGraphExecutorPlan(
        self: *CompiledTrainSession,
        cb: *const ComputeBackend,
        profile: *TrainStepProfile,
    ) !*const CachedMetalGraphExecutorPlan {
        const partitioned = platform.env.getenvBoolDefault("TERMITE_TRAINING_GRAPH_EXECUTOR_PARTITIONED", false);
        if (self.cached_metal_graph_executor_plan) |*cached| {
            if (cached.partitioned == partitioned) {
                profile.graph_executor_plan_cache_hits += 1;
                return cached;
            }
            cached.deinit();
            self.cached_metal_graph_executor_plan = null;
        }

        profile.graph_executor_plan_cache_misses += 1;
        const plan_start = nowNs();
        var diagnostics = partition_mod.CapabilityDiagnostics{};
        var base_plan = if (partitioned)
            try self.buildPartitionedMetalGraphExecutorPlan(&diagnostics)
        else
            try self.buildSingleMetalGraphExecutorPlan();
        errdefer base_plan.deinit();
        try self.attachCachedMetalExecutors(cb, &base_plan);

        const buffer_plan_start = nowNs();
        var graph_buffer_plan = try buffer_plan_mod.build(self.allocator, &self.graph, &base_plan, .{});
        errdefer graph_buffer_plan.deinit();
        try graph_buffer_plan.validate(&self.graph, &base_plan);
        profile.graph_executor_buffer_plan_build_ns += elapsedNs(buffer_plan_start);

        const assignments = try self.allocator.alloc(device_mesh.DeviceId, base_plan.partitions.len);
        errdefer self.allocator.free(assignments);
        for (base_plan.partitions, 0..) |part, idx| {
            assignments[idx] = if (part.backend == .native) 1 else 0;
        }

        self.cached_metal_graph_executor_plan = .{
            .base_plan = base_plan,
            .buffer_plan = graph_buffer_plan,
            .assignments = assignments,
            .partitioned = partitioned,
            .unsupported_ops = diagnostics.count(.unsupported_op),
        };
        profile.graph_executor_plan_build_ns += elapsedNs(plan_start);
        return &self.cached_metal_graph_executor_plan.?;
    }

    fn cachedCudaGraphExecutorPlan(
        self: *CompiledTrainSession,
        profile: *TrainStepProfile,
    ) !*const CachedCudaGraphExecutorPlan {
        if (self.cached_cuda_graph_executor_plan) |*cached| {
            profile.graph_executor_plan_cache_hits += 1;
            return cached;
        }

        profile.graph_executor_plan_cache_misses += 1;
        const plan_start = nowNs();
        var base_plan = try self.buildSingleCudaGraphExecutorPlan();
        errdefer base_plan.deinit();
        const buffer_plan_start = nowNs();
        var graph_buffer_plan = try buffer_plan_mod.build(self.allocator, &self.graph, &base_plan, .{});
        errdefer graph_buffer_plan.deinit();
        try graph_buffer_plan.validate(&self.graph, &base_plan);
        profile.graph_executor_buffer_plan_build_ns += elapsedNs(buffer_plan_start);

        const assignments = try self.allocator.alloc(device_mesh.DeviceId, base_plan.partitions.len);
        errdefer self.allocator.free(assignments);
        @memset(assignments, 0);
        self.cached_cuda_graph_executor_plan = .{
            .base_plan = base_plan,
            .buffer_plan = graph_buffer_plan,
            .assignments = assignments,
        };
        profile.graph_executor_plan_build_ns += elapsedNs(plan_start);
        return &self.cached_cuda_graph_executor_plan.?;
    }

    fn ensureCachedCudaConstants(
        self: *CompiledTrainSession,
        cb: *const ComputeBackend,
    ) !void {
        if (self.cached_cuda_constants_backend) |backend| {
            if (backend != cb) return error.CompiledSessionBackendChanged;
            return;
        }

        errdefer {
            var it = self.cached_cuda_constants.iterator();
            while (it.next()) |entry| cb.free(entry.value_ptr.*);
            self.cached_cuda_constants.clearRetainingCapacity();
        }
        const count: usize = @intCast(self.graph.nodeCount());
        for (0..count) |index| {
            const node_id: NodeId = @intCast(index);
            const node = self.graph.node(node_id);
            switch (node.op) {
                .constant => |attrs| {
                    const constant = try self.graph.constantDataAsF32(
                        self.allocator,
                        node.output_shape.dtype,
                        attrs.data_offset,
                        attrs.data_len,
                    );
                    defer constant.deinit(self.allocator);
                    var shape_buf: [ml.graph.shape.max_rank]i32 = undefined;
                    const rank = node.output_shape.rank();
                    for (0..rank) |axis| shape_buf[axis] = @intCast(node.output_shape.dim(@intCast(axis)));
                    const tensor = if (rank > 1)
                        try cb.fromFloat32Shape(constant.data, shape_buf[0..rank])
                    else
                        try cb.fromFloat32(constant.data);
                    errdefer cb.free(tensor);
                    try self.cached_cuda_constants.put(self.allocator, node_id, tensor);
                },
                else => {},
            }
        }
        self.cached_cuda_constants_backend = cb;
    }

    fn attachCachedMetalExecutors(
        self: *CompiledTrainSession,
        cb: *const ComputeBackend,
        plan: *partition_mod.PartitionPlan,
    ) !void {
        for (plan.partitions) |*part| {
            if (part.backend != .metal or part.executor != null) continue;
            const exec = try metal_partition_executor.MetalPartitionExecutor.create(self.allocator, &self.graph, cb);
            part.executor = exec.partitionExecutor();
        }
    }

    fn buildSingleMetalGraphExecutorPlan(self: *CompiledTrainSession) !partition_mod.PartitionPlan {
        const count: usize = @intCast(self.graph.nodeCount());
        const partitions = try self.allocator.alloc(partition_mod.Partition, 1);
        errdefer self.allocator.free(partitions);
        const node_ids = try self.allocator.alloc(NodeId, count);
        errdefer self.allocator.free(node_ids);
        const node_assignment = try self.allocator.alloc(u32, count);
        errdefer self.allocator.free(node_assignment);
        const node_operator_plans = try self.allocator.alloc(?@import("operator_plan.zig").OperatorPlan, count);
        errdefer self.allocator.free(node_operator_plans);
        const external_inputs = try self.allocator.alloc(partition_mod.ExternalInput, 0);
        errdefer self.allocator.free(external_inputs);

        for (0..count) |i| {
            node_ids[i] = @intCast(i);
            node_assignment[i] = 0;
            node_operator_plans[i] = null;
        }

        partitions[0] = .{
            .backend = .metal,
            .device_id = 0,
            .node_ids = node_ids,
            .external_inputs = external_inputs,
        };

        return .{
            .partitions = partitions,
            .node_assignment = node_assignment,
            .node_operator_plans = node_operator_plans,
            .allocator = self.allocator,
        };
    }

    fn buildSingleCudaGraphExecutorPlan(self: *CompiledTrainSession) !partition_mod.PartitionPlan {
        const count: usize = @intCast(self.graph.nodeCount());
        const partitions = try self.allocator.alloc(partition_mod.Partition, 1);
        errdefer self.allocator.free(partitions);
        const node_ids = try self.allocator.alloc(NodeId, count);
        errdefer self.allocator.free(node_ids);
        const node_assignment = try self.allocator.alloc(u32, count);
        errdefer self.allocator.free(node_assignment);
        const node_operator_plans = try self.allocator.alloc(?@import("operator_plan.zig").OperatorPlan, count);
        errdefer self.allocator.free(node_operator_plans);
        const external_inputs = try self.allocator.alloc(partition_mod.ExternalInput, 0);
        errdefer self.allocator.free(external_inputs);

        for (0..count) |i| {
            node_ids[i] = @intCast(i);
            node_assignment[i] = 0;
            node_operator_plans[i] = null;
        }
        partitions[0] = .{
            .backend = .cuda,
            .device_id = 0,
            .node_ids = node_ids,
            .external_inputs = external_inputs,
        };
        return .{
            .partitions = partitions,
            .node_assignment = node_assignment,
            .node_operator_plans = node_operator_plans,
            .allocator = self.allocator,
        };
    }

    fn buildPartitionedMetalGraphExecutorPlan(
        self: *CompiledTrainSession,
        diagnostics: *partition_mod.CapabilityDiagnostics,
    ) !partition_mod.PartitionPlan {
        const descriptor_seeds = try partition_mod.allocTensorDescriptorSeeds(self.allocator, &self.graph);
        defer self.allocator.free(descriptor_seeds);
        try partition_mod.seedAllUploadableResidency(descriptor_seeds, &self.graph, .metal, 0);

        const capabilities = [_]partition_mod.Capability{
            .{
                .backend = .metal,
                .priority = 10,
                .supports = &metal_capabilities.supportsMetalEagerGraph,
                .decide = &metal_capabilities.decideMetalEagerGraph,
            },
            .{
                .backend = .native,
                .priority = 0,
                .supports = &partition_mod.supportsAll,
                .decide = &partition_mod.decideNative,
            },
        };

        return partition_mod.partitionWithOptions(self.allocator, &self.graph, &capabilities, .{
            .tensor_descs = descriptor_seeds,
            .diagnostics = diagnostics,
        });
    }

    fn planKeepsGradientOutputsOnMetal(
        self: *const CompiledTrainSession,
        plan: *const partition_mod.PartitionPlan,
    ) bool {
        var grad_output_idx: usize = self.loss_output_index + 1;
        for (self.param_grads) |grad_id| {
            if (grad_id == null_node) continue;
            const output_node = self.graph.outputs.items[grad_output_idx];
            grad_output_idx += 1;
            const part_idx = plan.node_assignment[@intCast(output_node)];
            if (plan.partitions[@intCast(part_idx)].backend != .metal) return false;
        }
        return true;
    }

    fn extractTrainStepResultFromGraphOutputs(
        self: *CompiledTrainSession,
        cb: *const ComputeBackend,
        mesh: *const device_mesh.DeviceMesh,
        multi_result: *const multi_executor.MultiExecutionResult,
        retain_device_gradients: bool,
        total_start: u64,
        profile: *TrainStepProfile,
    ) !TrainStepResult {
        const extract_start = nowNs();
        const outputs = multi_result.outputs;
        compiledDiag("extract begin outputs={} retain_device_gradients={} rss={}", .{ outputs.len, retain_device_gradients, currentResidentBytes() });
        if (metal_partition_executor.metalOutputLifetimeDebugEnabled()) {
            try self.debugCompareGraphOutputLifetimes(cb, mesh, multi_result);
        }
        maybeDumpGraphOutputValues(
            &self.graph,
            self.allocator,
            self.loss_output_index,
            cb,
            mesh,
            multi_result.output_devices,
            outputs,
            "executor",
        );
        const loss_device = mesh.device(multi_result.output_devices[self.loss_output_index]) orelse return error.DeviceNotFound;
        const loss_data = try loss_device.backend.toFloat32(outputs[self.loss_output_index], self.allocator);
        defer self.allocator.free(loss_data);
        const loss_output_node = self.graph.outputs.items[self.loss_output_index];
        if (loss_data.len == 0) {
            std.debug.print(
                "[compiled-train] ERROR: graph executor produced empty loss output node={} op={s} shape={any} device={}\n",
                .{
                    loss_output_node,
                    @tagName(std.meta.activeTag(self.graph.node(loss_output_node).op)),
                    self.graph.node(loss_output_node).output_shape,
                    multi_result.output_devices[self.loss_output_index],
                },
            );
            return error.EmptyLossOutput;
        }
        compiledDiag(
            "graph executor loss output node={} op={s} shape={any} len={} value={d:.6} device={} resident={}",
            .{
                loss_output_node,
                @tagName(std.meta.activeTag(self.graph.node(loss_output_node).op)),
                self.graph.node(loss_output_node).output_shape,
                loss_data.len,
                loss_data[0],
                multi_result.output_devices[self.loss_output_index],
                metal_partition_executor.isMetalDeviceResident(loss_device.backend, outputs[self.loss_output_index]),
            },
        );
        const loss_value: f32 = loss_data[0];

        var gradients = std.StringHashMapUnmanaged([]f32){};
        var device_gradients = std.StringHashMapUnmanaged(CT){};
        errdefer {
            var it = gradients.iterator();
            while (it.next()) |entry| {
                self.allocator.free(entry.key_ptr.*);
                self.allocator.free(entry.value_ptr.*);
            }
            gradients.deinit(self.allocator);
            var device_it = device_gradients.iterator();
            while (device_it.next()) |entry| {
                self.allocator.free(entry.key_ptr.*);
                cb.free(entry.value_ptr.*);
            }
            device_gradients.deinit(self.allocator);
        }

        var grad_output_idx: usize = self.loss_output_index + 1;
        var grad_diag_emitted = false;
        for (self.wrt_names, 0..) |name, i| {
            if (self.param_grads[i] == null_node) continue;
            const grad_ct = outputs[grad_output_idx];
            const grad_output_node = self.graph.outputs.items[grad_output_idx];
            const grad_device = mesh.device(multi_result.output_devices[grad_output_idx]) orelse return error.DeviceNotFound;
            if (!grad_diag_emitted and platform.env.getenvBoolDefault("TERMITE_COMPILED_TRAIN_TRACE", false)) {
                grad_diag_emitted = true;
                const grad_data_diag = try grad_device.backend.toFloat32(grad_ct, self.allocator);
                defer self.allocator.free(grad_data_diag);
                var abs_sum: f64 = 0;
                for (grad_data_diag) |v| abs_sum += @abs(v);
                compiledDiag(
                    "graph executor first grad output idx={} node={} op={s} shape={any} len={} abs_sum={d:.6} name={s}",
                    .{
                        grad_output_idx,
                        grad_output_node,
                        @tagName(std.meta.activeTag(self.graph.node(grad_output_node).op)),
                        self.graph.node(grad_output_node).output_shape,
                        grad_data_diag.len,
                        abs_sum,
                        name,
                    },
                );
            }
            grad_output_idx += 1;
            const owned_name = try self.allocator.dupe(u8, name);
            errdefer self.allocator.free(owned_name);
            if (retain_device_gradients) {
                if (grad_device.backend != cb) return error.UnexpectedGradientDevice;
                const retained = try cloneTensorForOutputShape(self.allocator, &self.graph, cb, grad_ct, grad_output_node);
                errdefer cb.free(retained);
                try device_gradients.put(self.allocator, owned_name, retained);
            } else {
                const grad_data = try grad_device.backend.toFloat32(grad_ct, self.allocator);
                try gradients.put(self.allocator, owned_name, grad_data);
            }
        }

        profile.extract_ns = elapsedNs(extract_start);
        profile.total_ns = elapsedNs(total_start);
        profile.peak_resident_bytes = @max(profile.peak_resident_bytes, currentResidentBytes());
        compiledDiag(
            "extract done gradients={} device_gradients={} extract_ms={d:.3} total_ms={d:.3} rss={}",
            .{
                gradients.count(),
                device_gradients.count(),
                nsToMs(profile.extract_ns),
                nsToMs(profile.total_ns),
                profile.peak_resident_bytes,
            },
        );

        return .{
            .loss = loss_value,
            .gradients = gradients,
            .device_gradients = device_gradients,
            .profile = profile.*,
            .allocator = self.allocator,
            .compute_backend = if (retain_device_gradients) cb else null,
        };
    }

    /// TERMITE_METAL_OUTPUT_LIFETIME_DEBUG=1: after a full device drain,
    /// read BOTH the owned graph-output copy the trainer extracts from and
    /// the ORIGINAL source buffer it was copied out of (retained by the
    /// partition executor's debug registry), printing the first values of
    /// each. Interpretation: source nonzero + copy zero => the owned-copy
    /// blit raced device work (command-buffer ordering); both zero after
    /// the drain => the source slot was never written (wrong slot / value
    /// kept internal to a fused region).
    fn debugCompareGraphOutputLifetimes(
        self: *CompiledTrainSession,
        cb: *const ComputeBackend,
        mesh: *const device_mesh.DeviceMesh,
        multi_result: *const multi_executor.MultiExecutionResult,
    ) !void {
        // Full drain: waits any submitted Metal frame and flushes pending
        // active-frame work. Runtime one-shot command buffers commit and
        // wait inline, so after this no device work is outstanding.
        cb.decoderRuntimeFlushActiveFrame() catch |err| {
            std.debug.print("[output-lifetime] drain failed: {s}\n", .{@errorName(err)});
        };
        var copy_zero: usize = 0;
        var source_zero: usize = 0;
        var source_missing: usize = 0;
        var zeroness_mismatch: usize = 0;
        // Sample of zero / nonzero outputs (node id + op) so a run can
        // correlate never-written outputs with the executor's fused-pattern
        // / runtime-region elision sets (elided interiors are never written
        // to their slots; region boundaries are).
        const OutputSample = struct { idx: usize, node: NodeId };
        var zero_samples: [10]OutputSample = undefined;
        var zero_sample_count: usize = 0;
        var nonzero_samples: [5]OutputSample = undefined;
        var nonzero_sample_count: usize = 0;
        for (self.graph.outputs.items, 0..) |output_node, idx| {
            if (idx >= multi_result.outputs.len) break;
            const device = mesh.device(multi_result.output_devices[idx]) orelse continue;
            const copy_data = device.backend.toFloat32(multi_result.outputs[idx], self.allocator) catch |err| {
                std.debug.print(
                    "[output-lifetime] idx={} node={} copy read failed: {s}\n",
                    .{ idx, output_node, @errorName(err) },
                );
                continue;
            };
            defer self.allocator.free(copy_data);
            const copy_is_zero = allValuesZeroF32(copy_data);
            if (copy_is_zero) {
                copy_zero += 1;
                if (zero_sample_count < zero_samples.len) {
                    zero_samples[zero_sample_count] = .{ .idx = idx, .node = output_node };
                    zero_sample_count += 1;
                }
            } else if (nonzero_sample_count < nonzero_samples.len) {
                nonzero_samples[nonzero_sample_count] = .{ .idx = idx, .node = output_node };
                nonzero_sample_count += 1;
            }
            var source_data: ?[]f32 = null;
            defer if (source_data) |data| self.allocator.free(data);
            var source_is_zero: ?bool = null;
            if (metal_partition_executor.metalOutputLifetimeDebugSource(output_node)) |source_ct| {
                source_data = device.backend.toFloat32(source_ct, self.allocator) catch null;
                if (source_data) |data| {
                    const is_zero = allValuesZeroF32(data);
                    source_is_zero = is_zero;
                    if (is_zero) source_zero += 1;
                    if (is_zero != copy_is_zero) zeroness_mismatch += 1;
                } else {
                    source_missing += 1;
                }
            } else {
                source_missing += 1;
            }
            const is_loss = idx == self.loss_output_index;
            const mismatch = if (source_is_zero) |src_zero| src_zero != copy_is_zero else false;
            if (is_loss or mismatch) {
                std.debug.print(
                    "[output-lifetime] idx={} node={} loss={} copy_first4={any} source_first4={any}\n",
                    .{
                        idx,
                        output_node,
                        is_loss,
                        copy_data[0..@min(copy_data.len, 4)],
                        if (source_data) |data| data[0..@min(data.len, 4)] else null,
                    },
                );
            }
        }
        std.debug.print(
            "[output-lifetime] summary outputs={} copy_zero={} source_zero={} source_missing={} zeroness_mismatch={} (mismatch>0 => ordering; copy and source both zero => never written)\n",
            .{ self.graph.outputs.items.len, copy_zero, source_zero, source_missing, zeroness_mismatch },
        );
        for (zero_samples[0..zero_sample_count]) |sample| {
            const node = self.graph.node(sample.node);
            std.debug.print(
                "[output-lifetime] zero-sample idx={} node={} op={s} shape={any}\n",
                .{ sample.idx, sample.node, @tagName(std.meta.activeTag(node.op)), node.output_shape },
            );
        }
        for (nonzero_samples[0..nonzero_sample_count]) |sample| {
            const node = self.graph.node(sample.node);
            std.debug.print(
                "[output-lifetime] nonzero-sample idx={} node={} op={s} shape={any}\n",
                .{ sample.idx, sample.node, @tagName(std.meta.activeTag(node.op)), node.output_shape },
            );
        }
        metal_partition_executor.metalOutputLifetimeDebugClear(cb);
    }

    const ParityReport = struct {
        passed: bool = true,
        loss_abs_diff: f32 = 0,
        loss_rel_diff: f32 = 0,
        max_grad_abs_diff: f32 = 0,
        max_grad_rel_diff: f32 = 0,
        compared_gradients: usize = 0,
        compared_gradient_values: usize = 0,
        missing_gradients: usize = 0,
        direct_loss: f32 = 0,
        graph_loss: f32 = 0,
    };

    fn compareTrainStepResults(
        self: *CompiledTrainSession,
        cb: *const ComputeBackend,
        direct: *const TrainStepResult,
        graph_exec: *const TrainStepResult,
    ) !ParityReport {
        var report = ParityReport{
            .direct_loss = direct.loss,
            .graph_loss = graph_exec.loss,
            .loss_abs_diff = absF32(direct.loss - graph_exec.loss),
            .loss_rel_diff = relativeDiffF32(direct.loss, graph_exec.loss),
        };

        const loss_abs_tol = trainingGraphExecutorParityLossAbsTolerance();
        const loss_rel_tol = trainingGraphExecutorParityLossRelTolerance();
        const grad_abs_tol = trainingGraphExecutorParityGradAbsTolerance();
        const grad_rel_tol = trainingGraphExecutorParityGradRelTolerance();
        var remaining_values = trainingGraphExecutorParityMaxGradientValues();

        if (report.loss_abs_diff > loss_abs_tol and report.loss_rel_diff > loss_rel_tol) {
            report.passed = false;
        }

        if (direct.device_gradients.count() > 0 or graph_exec.device_gradients.count() > 0) {
            var it = direct.device_gradients.iterator();
            while (it.next()) |entry| {
                if (remaining_values == 0) break;
                const name = entry.key_ptr.*;
                const graph_ct = graph_exec.device_gradients.get(name) orelse {
                    report.missing_gradients += 1;
                    report.passed = false;
                    continue;
                };
                const direct_data = try cb.toFloat32(entry.value_ptr.*, self.allocator);
                defer self.allocator.free(direct_data);
                const graph_data = try cb.toFloat32(graph_ct, self.allocator);
                defer self.allocator.free(graph_data);
                self.compareGradientData(
                    direct_data,
                    graph_data,
                    grad_abs_tol,
                    grad_rel_tol,
                    &remaining_values,
                    &report,
                );
            }
            if (graph_exec.device_gradients.count() != direct.device_gradients.count()) {
                report.missing_gradients += graph_exec.device_gradients.count() -| direct.device_gradients.count();
                report.passed = false;
            }
            return report;
        }

        var it = direct.gradients.iterator();
        while (it.next()) |entry| {
            if (remaining_values == 0) break;
            const name = entry.key_ptr.*;
            const graph_data = graph_exec.gradients.get(name) orelse {
                report.missing_gradients += 1;
                report.passed = false;
                continue;
            };
            self.compareGradientData(
                entry.value_ptr.*,
                graph_data,
                grad_abs_tol,
                grad_rel_tol,
                &remaining_values,
                &report,
            );
        }
        if (graph_exec.gradients.count() != direct.gradients.count()) {
            report.missing_gradients += graph_exec.gradients.count() -| direct.gradients.count();
            report.passed = false;
        }
        return report;
    }

    fn compareGradientData(
        self: *CompiledTrainSession,
        direct: []const f32,
        graph_exec: []const f32,
        grad_abs_tol: f32,
        grad_rel_tol: f32,
        remaining_values: *usize,
        report: *ParityReport,
    ) void {
        _ = self;
        report.compared_gradients += 1;
        if (direct.len != graph_exec.len) {
            report.missing_gradients += 1;
            report.passed = false;
        }
        const n = @min(@min(direct.len, graph_exec.len), remaining_values.*);
        for (0..n) |i| {
            const abs_diff = absF32(direct[i] - graph_exec[i]);
            const rel_diff = relativeDiffF32(direct[i], graph_exec[i]);
            report.max_grad_abs_diff = @max(report.max_grad_abs_diff, abs_diff);
            report.max_grad_rel_diff = @max(report.max_grad_rel_diff, rel_diff);
            if (abs_diff > grad_abs_tol and rel_diff > grad_rel_tol) {
                report.passed = false;
            }
        }
        report.compared_gradient_values += n;
        remaining_values.* -= n;
    }
};

fn trainingGraphExecutorParityCheckEnabled() bool {
    return platform.env.getenvBoolDefault("TERMITE_TRAINING_GRAPH_EXECUTOR_PARITY_CHECK", false);
}

fn trainingGraphExecutorSkipMetalFusedPatterns() bool {
    return platform.env.getenvBoolDefault("TERMITE_TRAINING_GRAPH_EXECUTOR_SKIP_METAL_FUSED_PATTERNS", false);
}

fn trainingGraphExecutorParityNodeIds() ?[]const u8 {
    return platform.env.getenv("TERMITE_TRAINING_GRAPH_EXECUTOR_PARITY_NODE_IDS");
}

var graph_node_dump_done: bool = false;

/// Env-gated graph node dump for parity debugging. Set
/// `TERMITE_DUMP_GRAPH_NODES="1030-1040"` (or a comma list like
/// `"1031,1033,1035"`) to print, once per process, the definition of the
/// selected nodes in the compiled training graph: op, output shape, inputs
/// (id/op/shape, parameter names) and the full dot_general / transpose
/// attribute configuration.
fn maybeDumpGraphNodes(graph: *const Graph) void {
    if (graph_node_dump_done) return;
    const raw = platform.env.getenv("TERMITE_DUMP_GRAPH_NODES") orelse return;
    graph_node_dump_done = true;
    var parts = std.mem.splitScalar(u8, raw, ',');
    while (parts.next()) |part_raw| {
        const part = std.mem.trim(u8, part_raw, " \t\r\n");
        if (part.len == 0) continue;
        if (std.mem.indexOfScalar(u8, part, '-')) |dash| {
            const lo = std.fmt.parseUnsigned(NodeId, part[0..dash], 10) catch continue;
            const hi = std.fmt.parseUnsigned(NodeId, part[dash + 1 ..], 10) catch continue;
            var id = lo;
            while (id <= hi) : (id += 1) dumpGraphNode(graph, id);
        } else {
            const id = std.fmt.parseUnsigned(NodeId, part, 10) catch continue;
            dumpGraphNode(graph, id);
        }
    }
}

fn dumpGraphNode(graph: *const Graph, node_id: NodeId) void {
    if (node_id >= graph.nodeCount()) return;
    const node = graph.node(node_id);
    std.debug.print(
        "graph_node_dump: node={d} op={s} shape={any}",
        .{ node_id, @tagName(std.meta.activeTag(node.op)), node.output_shape },
    );
    switch (node.op) {
        .dot_general => |attrs| std.debug.print(
            " lhs_contracting={any} rhs_contracting={any} lhs_batch={any} rhs_batch={any}",
            .{
                attrs.lhs_contracting[0..attrs.num_contracting],
                attrs.rhs_contracting[0..attrs.num_contracting],
                attrs.lhs_batch[0..attrs.num_batch],
                attrs.rhs_batch[0..attrs.num_batch],
            },
        ),
        .transpose => |attrs| std.debug.print(" perm={any}", .{attrs.perm[0..attrs.num_axes]}),
        .parameter => std.debug.print(" name={s}", .{graph.parameterName(node)}),
        .gather => |attrs| std.debug.print(" axis={d}", .{attrs.axis}),
        else => {},
    }
    std.debug.print("\n", .{});
    for (node.getInputs(), 0..) |input_id, idx| {
        if (input_id == null_node) continue;
        if (input_id >= graph.nodeCount()) continue;
        const input = graph.node(input_id);
        std.debug.print(
            "graph_node_dump:   input[{d}]: node={d} op={s} shape={any}",
            .{ idx, input_id, @tagName(std.meta.activeTag(input.op)), input.output_shape },
        );
        if (std.meta.activeTag(input.op) == .parameter) {
            std.debug.print(" name={s}", .{graph.parameterName(input)});
        }
        std.debug.print("\n", .{});
    }
}

// ── Graph-output truth-diff dump ───────────────────────────────────────
//
// TERMITE_DUMP_GRAPH_OUTPUT_VALUES=/path.jsonl writes, after every executed
// training step, one JSON line per graph output:
//   {"step":N,"mode":"executor|interpreter","idx":I,"node":ID,"op":"add",
//    "shape":[..],"len":L,"first4":[..],"abs_sum":S}
// Works on BOTH the Metal graph-executor path and the direct-interpreter
// path, so an orchestrator can run each mode once (toggling
// TERMITE_ENABLE_TRAINING_GRAPH_EXECUTOR) into two files and diff them:
// outputs nonzero under the interpreter but zero (or differing beyond fp
// tolerance) under the executor form the genuinely-broken set — outputs
// zero in BOTH modes (e.g. step-1 LoRA grad_A with B=0 init) are
// mathematically legitimate and need no fix.
//
// Lines accumulate in a process-global buffer and the whole file is
// rewritten after each step (avoids append-mode IO; dump size is tiny).

var graph_output_dump_buffer = std.ArrayListUnmanaged(u8).empty;
var graph_output_dump_steps_executor: u64 = 0;
var graph_output_dump_steps_interpreter: u64 = 0;

fn appendGraphOutputDump(comptime fmt: []const u8, args: anytype) !void {
    const gpa = std.heap.c_allocator;
    const text = try std.fmt.allocPrint(gpa, fmt, args);
    defer gpa.free(text);
    try graph_output_dump_buffer.appendSlice(gpa, text);
}

fn dumpGraphOutputValuesJsonl(
    graph: *const Graph,
    allocator: std.mem.Allocator,
    loss_output_index: usize,
    default_backend: *const ComputeBackend,
    mesh: ?*const device_mesh.DeviceMesh,
    output_devices: ?[]const device_mesh.DeviceId,
    outputs: []const CT,
    mode: []const u8,
    path: []const u8,
    step: u64,
) !void {
    for (graph.outputs.items, 0..) |output_node, idx| {
        if (idx >= outputs.len) break;
        var backend = default_backend;
        if (mesh) |m| {
            if (output_devices) |devices| {
                if (idx < devices.len) {
                    if (m.device(devices[idx])) |entry| backend = entry.backend;
                }
            }
        }
        const node = graph.node(output_node);
        const op_name = @tagName(std.meta.activeTag(node.op));
        const data = backend.toFloat32(outputs[idx], allocator) catch |err| {
            try appendGraphOutputDump(
                "{{\"step\":{d},\"mode\":\"{s}\",\"idx\":{d},\"node\":{d},\"op\":\"{s}\",\"error\":\"{s}\"}}\n",
                .{ step, mode, idx, output_node, op_name, @errorName(err) },
            );
            continue;
        };
        defer allocator.free(data);
        var abs_sum: f64 = 0;
        for (data) |value| abs_sum += @abs(value);
        try appendGraphOutputDump(
            "{{\"step\":{d},\"mode\":\"{s}\",\"idx\":{d},\"node\":{d},\"op\":\"{s}\",\"loss\":{},\"shape\":[",
            .{ step, mode, idx, output_node, op_name, idx == loss_output_index },
        );
        const shape = node.output_shape;
        var axis: u8 = 0;
        while (axis < shape.rank()) : (axis += 1) {
            if (axis > 0) try appendGraphOutputDump(",", .{});
            try appendGraphOutputDump("{d}", .{shape.dim(axis)});
        }
        try appendGraphOutputDump("],\"len\":{d},\"first4\":[", .{data.len});
        for (data[0..@min(data.len, 4)], 0..) |value, value_idx| {
            if (value_idx > 0) try appendGraphOutputDump(",", .{});
            try appendGraphOutputDump("{d}", .{value});
        }
        try appendGraphOutputDump("],\"abs_sum\":{d}}}\n", .{abs_sum});
    }
    try io_compat.cwd().writeFile(io_compat.io(), .{
        .sub_path = path,
        .data = graph_output_dump_buffer.items,
    });
}

fn maybeDumpGraphOutputValues(
    graph: *const Graph,
    allocator: std.mem.Allocator,
    loss_output_index: usize,
    default_backend: *const ComputeBackend,
    mesh: ?*const device_mesh.DeviceMesh,
    output_devices: ?[]const device_mesh.DeviceId,
    outputs: []const CT,
    mode: []const u8,
) void {
    const path = platform.env.getenv("TERMITE_DUMP_GRAPH_OUTPUT_VALUES") orelse return;
    const step_counter = if (std.mem.eql(u8, mode, "executor"))
        &graph_output_dump_steps_executor
    else
        &graph_output_dump_steps_interpreter;
    const step = step_counter.*;
    step_counter.* += 1;
    dumpGraphOutputValuesJsonl(
        graph,
        allocator,
        loss_output_index,
        default_backend,
        mesh,
        output_devices,
        outputs,
        mode,
        path,
        step,
    ) catch |err| {
        std.debug.print(
            "[graph-output-dump] mode={s} step={} write to {s} failed: {s}\n",
            .{ mode, step, path, @errorName(err) },
        );
    };
}

fn trainingGraphExecutorParityMaxGradientValues() usize {
    return platform.env.getenvUsize("TERMITE_TRAINING_GRAPH_EXECUTOR_PARITY_MAX_GRAD_VALUES") orelse 4096;
}

fn trainingGraphExecutorParityLossAbsTolerance() f32 {
    return getenvF32("TERMITE_TRAINING_GRAPH_EXECUTOR_PARITY_LOSS_ATOL", 1e-5);
}

fn trainingGraphExecutorParityLossRelTolerance() f32 {
    return getenvF32("TERMITE_TRAINING_GRAPH_EXECUTOR_PARITY_LOSS_RTOL", 1e-4);
}

fn trainingGraphExecutorParityGradAbsTolerance() f32 {
    return getenvF32("TERMITE_TRAINING_GRAPH_EXECUTOR_PARITY_GRAD_ATOL", 1e-4);
}

fn trainingGraphExecutorParityGradRelTolerance() f32 {
    return getenvF32("TERMITE_TRAINING_GRAPH_EXECUTOR_PARITY_GRAD_RTOL", 1e-3);
}

fn getenvF32(comptime name: [*:0]const u8, default: f32) f32 {
    const value = platform.env.getenv(name) orelse return default;
    return std.fmt.parseFloat(f32, value) catch default;
}

fn absF32(value: f32) f32 {
    return if (value < 0) -value else value;
}

fn allValuesZeroF32(data: []const f32) bool {
    for (data) |value| {
        if (value != 0) return false;
    }
    return true;
}

fn relativeDiffF32(lhs: f32, rhs: f32) f32 {
    const denom = @max(@max(absF32(lhs), absF32(rhs)), 1e-12);
    return absF32(lhs - rhs) / denom;
}

const FloatSliceDiff = struct {
    max_abs: f32 = 0,
    max_rel: f32 = 0,
    first_diff_index: usize = std.math.maxInt(usize),
    len_mismatch: bool = false,
};

fn compareFloatSlices(lhs: []const f32, rhs: []const f32) FloatSliceDiff {
    var diff = FloatSliceDiff{ .len_mismatch = lhs.len != rhs.len };
    const n = @min(lhs.len, rhs.len);
    for (0..n) |i| {
        const abs_diff = absF32(lhs[i] - rhs[i]);
        const rel_diff = relativeDiffF32(lhs[i], rhs[i]);
        if (abs_diff > diff.max_abs) diff.max_abs = abs_diff;
        if (rel_diff > diff.max_rel) diff.max_rel = rel_diff;
        if (diff.first_diff_index == std.math.maxInt(usize) and abs_diff != 0) {
            diff.first_diff_index = i;
        }
    }
    return diff;
}

fn parseParityNodeIds(allocator: std.mem.Allocator, raw: []const u8, node_count: u32) ![]NodeId {
    var ids = std.ArrayListUnmanaged(NodeId).empty;
    errdefer ids.deinit(allocator);
    var split = std.mem.splitScalar(u8, raw, ',');
    while (split.next()) |part_raw| {
        const part = std.mem.trim(u8, part_raw, " \t\r\n");
        if (part.len == 0) continue;
        const node_id = try std.fmt.parseUnsigned(NodeId, part, 10);
        if (node_id >= node_count) return error.TrainingGraphExecutorParityInvalidNode;
        try ids.append(allocator, node_id);
    }
    return ids.toOwnedSlice(allocator);
}

fn printTrainingGraphExecutorParityReport(report: CompiledTrainSession.ParityReport) void {
    std.debug.print(
        "training_graph_executor_parity: passed={} direct_loss={d:.9} graph_loss={d:.9} loss_abs_diff={d:.9} loss_rel_diff={d:.9} max_grad_abs_diff={d:.9} max_grad_rel_diff={d:.9} compared_gradients={} compared_gradient_values={} missing_gradients={}\n",
        .{
            report.passed,
            report.direct_loss,
            report.graph_loss,
            report.loss_abs_diff,
            report.loss_rel_diff,
            report.max_grad_abs_diff,
            report.max_grad_rel_diff,
            report.compared_gradients,
            report.compared_gradient_values,
            report.missing_gradients,
        },
    );
}

const FallbackNativeBackend = struct {
    weight_store: *native_compute.WeightStore,
    compute: *native_compute.NativeCompute,
    backend: ComputeBackend,

    fn init(allocator: std.mem.Allocator) !FallbackNativeBackend {
        const weight_store = try allocator.create(native_compute.WeightStore);
        errdefer allocator.destroy(weight_store);
        weight_store.* = .{
            .allocator = allocator,
            .resident_weights = .{},
            .lazy_weights = .empty,
        };

        const compute = try allocator.create(native_compute.NativeCompute);
        errdefer allocator.destroy(compute);
        compute.* = native_compute.NativeCompute.init(allocator, weight_store, null);

        return .{
            .weight_store = weight_store,
            .compute = compute,
            .backend = compute.computeBackend(),
        };
    }

    fn deinit(self: *FallbackNativeBackend, allocator: std.mem.Allocator) void {
        self.backend.deinit();
        native_compute.deinitPrefetchQueue(self.weight_store);
        self.weight_store.resident_weights.deinit(allocator);
        self.weight_store.lazy_weights.deinit(allocator);
        allocator.destroy(self.weight_store);
    }
};

pub fn trainStep(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    loss_node: NodeId,
    cb: *const ComputeBackend,
    runtime_inputs: ?std.AutoHashMapUnmanaged(NodeId, CT),
    options: TrainStepOptions,
) !TrainStepResult {
    const total_start = nowNs();
    var profile: TrainStepProfile = .{};
    profile.peak_resident_bytes = currentResidentBytes();
    var checkpoint_summary: ?CheckpointSummary = null;

    // Determine which parameters to differentiate with respect to.
    const wrt = try resolveWrtParams(allocator, graph, options.trainable_params);
    defer allocator.free(wrt);

    // Run autodiff: lower fused ops and compute gradient graph.
    const autodiff_start = nowNs();
    var grad_result = try autodiff.gradient(allocator, graph, loss_node, wrt);
    profile.autodiff_ns = elapsedNs(autodiff_start);
    profile.peak_resident_bytes = @max(profile.peak_resident_bytes, currentResidentBytes());
    defer grad_result.deinit();

    if (options.checkpoint_config) |cfg| {
        const checkpoint_start = nowNs();
        if (options.emit_checkpoint_analysis) {
            var forward_count: u32 = 0;
            for (grad_result.id_map) |mapped| {
                if (mapped != null_node and mapped >= forward_count) {
                    forward_count = mapped + 1;
                }
            }
            if (forward_count > 0 and forward_count < grad_result.graph.nodeCount()) {
                const is_checkpoint = try checkpoint.identifyCheckpoints(allocator, &grad_result.graph, forward_count, cfg);
                defer allocator.free(is_checkpoint);
                const analysis = checkpoint.analyzeCheckpointSavings(&grad_result.graph, forward_count, is_checkpoint);
                checkpoint_summary = .{
                    .strategy = cfg.strategy,
                    .layer_interval = cfg.layer_interval,
                    .total_forward_activations = analysis.total_forward_activations,
                    .checkpointed_activations = analysis.checkpointed_activations,
                    .recomputable_activations = analysis.recomputable_activations,
                    .savings_ratio = analysis.savingsRatio(),
                };
            }
        }
        try checkpoint.applyCheckpointing(allocator, &grad_result, cfg);
        profile.checkpoint_ns = elapsedNs(checkpoint_start);
        profile.peak_resident_bytes = @max(profile.peak_resident_bytes, currentResidentBytes());
    }

    // Mark the loss (mapped through id_map) as an output if not already.
    // The lowered graph preserves the original outputs; we need the loss
    // output to extract its value, plus all gradient nodes.
    const lowered_loss = grad_result.id_map[loss_node];

    // Mark the lowered loss as the first output (clear existing outputs
    // and rebuild so loss is at index 0).
    grad_result.graph.outputs.clearRetainingCapacity();
    try grad_result.graph.markOutput(lowered_loss);

    // Mark each parameter gradient as an output. Skip null_node entries
    // (parameters unreachable from the loss get no gradient).
    for (grad_result.param_grads) |grad_id| {
        if (grad_id != null_node) {
            try grad_result.graph.markOutput(grad_id);
        }
    }

    // Build RuntimeInput slice, mapping original graph NodeIds through id_map.
    var rt_list = std.ArrayListUnmanaged(RuntimeInput).empty;
    defer rt_list.deinit(allocator);

    if (runtime_inputs) |rt| {
        var it = rt.iterator();
        while (it.next()) |entry| {
            const old_id = entry.key_ptr.*;
            const ct = entry.value_ptr.*;
            if (old_id < grad_result.id_map.len) {
                const new_id = grad_result.id_map[old_id];
                if (new_id != null_node) {
                    try rt_list.append(allocator, .{ .node_id = new_id, .value = ct });
                }
            }
        }
    }

    const rt_slice: ?[]const RuntimeInput = if (rt_list.items.len > 0)
        rt_list.items
    else
        null;

    // Execute the gradient graph.
    const execute_start = nowNs();
    const metal_debug_before = cb.debugTimingSnapshot().provider;
    var owned_execution_frame = try beginOwnedExecutionFrame(cb);
    errdefer cancelOwnedExecutionFrame(cb, &owned_execution_frame);
    var planned_scope = try beginTrainingPlannedScope(cb);
    errdefer endTrainingPlannedScope(cb, &planned_scope) catch {};
    var exec_result = try interpreter.execute(
        allocator,
        &grad_result.graph,
        cb,
        .{ .runtime_inputs = rt_slice },
    );
    // Arm cleanup before the fallible end-scope/submit calls below.
    defer exec_result.deinit(cb);
    try endTrainingPlannedScope(cb, &planned_scope);
    try submitOwnedExecutionFrame(cb, &owned_execution_frame);
    profile.execute_ns = elapsedNs(execute_start);
    recordMetalDebugProfile(&profile, metal_debug_before, cb.debugTimingSnapshot().provider);
    profile.peak_resident_bytes = @max(profile.peak_resident_bytes, currentResidentBytes());

    // Extract loss value from first output.
    const extract_start = nowNs();
    const loss_data = try cb.toFloat32(exec_result.outputs[0], allocator);
    defer allocator.free(loss_data);
    const loss_value: f32 = if (loss_data.len > 0) loss_data[0] else 0.0;

    // Extract gradient tensors keyed by parameter name.
    var gradients = std.StringHashMapUnmanaged([]f32){};
    errdefer {
        var it = gradients.iterator();
        while (it.next()) |entry| {
            allocator.free(entry.key_ptr.*);
            allocator.free(entry.value_ptr.*);
        }
        gradients.deinit(allocator);
    }

    var grad_output_idx: usize = 1;
    for (wrt, 0..) |param_id, i| {
        const grad_id = grad_result.param_grads[i];
        if (grad_id == null_node) continue;

        // Output index: 0 = loss, followed by only the non-null gradients.
        const grad_data = try cb.toFloat32(exec_result.outputs[grad_output_idx], allocator);
        grad_output_idx += 1;

        const param_node = graph.node(param_id);
        const name = try allocator.dupe(u8, graph.parameterName(param_node));
        errdefer allocator.free(name);
        try gradients.put(allocator, name, grad_data);
    }

    profile.extract_ns = elapsedNs(extract_start);
    profile.total_ns = elapsedNs(total_start);
    profile.peak_resident_bytes = @max(profile.peak_resident_bytes, currentResidentBytes());

    return .{
        .loss = loss_value,
        .gradients = gradients,
        .profile = profile,
        .checkpoint_summary = checkpoint_summary,
        .allocator = allocator,
    };
}

fn elapsedNs(start_ns: u64) u64 {
    const end_ns = nowNs();
    return end_ns - start_ns;
}

fn nsToMs(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / 1_000_000.0;
}

fn compiledDiag(comptime fmt: []const u8, args: anytype) void {
    if (!platform.env.getenvBoolDefault("TERMITE_COMPILED_TRAIN_TRACE", false)) return;
    std.debug.print("[compiled-train] " ++ fmt ++ "\n", args);
}

fn nowNs() u64 {
    var timespec: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(std.posix.CLOCK.MONOTONIC, &timespec))) {
        .SUCCESS => return @intCast(@as(i128, timespec.sec) * std.time.ns_per_s + timespec.nsec),
        else => return 0,
    }
}

fn currentResidentBytes() usize {
    const usage = std.posix.getrusage(std.posix.rusage.SELF);
    if (usage.maxrss <= 0) return 0;

    const maxrss: usize = @intCast(usage.maxrss);
    return switch (builtin.os.tag) {
        // Darwin reports ru_maxrss in bytes; Linux reports KiB.
        .driverkit, .ios, .maccatalyst, .macos, .tvos, .visionos, .watchos => maxrss,
        .linux => std.math.mul(usize, maxrss, 1024) catch std.math.maxInt(usize),
        else => maxrss,
    };
}

fn cloneTensorForOutputShape(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    cb: *const ComputeBackend,
    tensor: CT,
    output_node: NodeId,
) !CT {
    const shape = graph.node(output_node).output_shape;
    var dims: [8]i32 = undefined;
    const rank = shape.rank();
    if (rank > dims.len) return error.UnsupportedShape;
    for (0..rank) |axis| {
        dims[axis] = std.math.cast(i32, shape.dim(@intCast(axis))) orelse return error.UnsupportedShape;
    }
    if (try cb.cloneTensorShape(tensor, dims[0..rank])) |cloned| return cloned;
    const data = try cb.toFloat32(tensor, allocator);
    defer allocator.free(data);
    return cb.fromFloat32Shape(data, dims[0..rank]);
}

/// Resolve which parameter NodeIds to differentiate. If trainable_params
/// is null, return all graph parameters. Otherwise filter by name.
fn resolveWrtParams(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    trainable_params: ?[]const []const u8,
) ![]NodeId {
    if (trainable_params) |names| {
        var result = std.ArrayListUnmanaged(NodeId).empty;
        errdefer result.deinit(allocator);

        for (graph.parameters.items) |param_id| {
            const param_node = graph.node(param_id);
            const param_name = graph.parameterName(param_node);
            for (names) |name| {
                if (std.mem.eql(u8, param_name, name)) {
                    try result.append(allocator, param_id);
                    break;
                }
            }
        }
        return try result.toOwnedSlice(allocator);
    } else {
        return allocator.dupe(NodeId, graph.parameters.items);
    }
}

// ── PJRT compile-once training session ─────────────────────────────

const pjrt_compiler_mod = @import("pjrt_compiler.zig");
const build_options = @import("build_options");
const pjrt_pkg = if (build_options.enable_pjrt) @import("pjrt") else struct {
    pub const pjrt = struct {
        pub const Client = void;
        pub const LoadedExecutable = void;
        pub const Buffer = void;
    };
};

/// Input for a PJRT training step (f32 data + shape).
pub const PjrtRuntimeInput = struct {
    data: []const f32,
    shape: []const i32,
};

/// A compiled training session: run autodiff once, then execute many times.
/// Inputs and outputs are identified by their original graph NodeIds.
pub const PjrtTrainSession = struct {
    executable: if (build_options.enable_pjrt) pjrt_pkg.pjrt.LoadedExecutable else void,
    client: if (build_options.enable_pjrt) *pjrt_pkg.pjrt.Client else void,
    /// Ordered list of original (pre-lowering) NodeIds that are runtime inputs.
    /// Caller must provide them in this exact order in execute().
    input_node_ids: []NodeId,
    /// Ordered list of output names: output_names[0] = "loss", output_names[1..] = param names.
    output_names: [][]u8,
    num_outputs: usize,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *PjrtTrainSession) void {
        if (comptime build_options.enable_pjrt) {
            self.executable.deinit();
        }
        self.allocator.free(self.input_node_ids);
        for (self.output_names) |name| self.allocator.free(name);
        self.allocator.free(self.output_names);
    }

    /// Execute the compiled gradient program.
    /// `runtime_inputs`: map from original NodeId → f32 data + shape.
    /// Returns TrainStepResult with loss and gradients.
    pub fn execute(
        self: *const PjrtTrainSession,
        allocator: std.mem.Allocator,
        runtime_inputs: std.AutoHashMapUnmanaged(NodeId, PjrtRuntimeInput),
    ) !TrainStepResult {
        if (comptime !build_options.enable_pjrt) return error.PjrtNotCompiled;

        // Upload inputs to device in order.
        var device_inputs = try allocator.alloc(pjrt_pkg.pjrt.Buffer, self.input_node_ids.len);
        var device_inputs_init: usize = 0;
        defer {
            for (device_inputs[0..device_inputs_init]) |*buf| buf.deinit();
            allocator.free(device_inputs);
        }
        for (self.input_node_ids, 0..) |node_id, ii| {
            const rt = runtime_inputs.get(node_id) orelse return error.MissingRuntimeInput;
            var dims = try allocator.alloc(i64, rt.shape.len);
            defer allocator.free(dims);
            for (rt.shape, 0..) |d, j| dims[j] = @intCast(d);
            device_inputs[ii] = try self.client.bufferFromHostFloat32(rt.data, dims);
            device_inputs_init += 1;
        }

        // Execute.
        const output_buffers = try self.executable.execute(device_inputs, allocator);
        defer {
            for (output_buffers) |*buf| buf.deinit();
            allocator.free(output_buffers);
        }

        // Download outputs. Output 0 = loss, outputs 1..N = gradients.
        const loss_data = try output_buffers[0].toFloat32(allocator);
        defer allocator.free(loss_data);
        const loss_value: f32 = if (loss_data.len > 0) loss_data[0] else 0.0;

        var gradients = std.StringHashMapUnmanaged([]f32){};
        errdefer {
            var it = gradients.iterator();
            while (it.next()) |e| {
                allocator.free(e.key_ptr.*);
                allocator.free(e.value_ptr.*);
            }
            gradients.deinit(allocator);
        }
        for (1..self.num_outputs) |oi| {
            const grad_data = try output_buffers[oi].toFloat32(allocator);
            errdefer allocator.free(grad_data);
            const name = try allocator.dupe(u8, self.output_names[oi]); // output_names[0]="loss", [1..]=param names
            errdefer allocator.free(name);
            try gradients.put(allocator, name, grad_data);
        }

        return .{
            .loss = loss_value,
            .gradients = gradients,
            .allocator = allocator,
        };
    }
};

/// Compile a gradient training program for repeated PJRT execution.
///
/// Runs autodiff on `graph` to produce a lowered gradient graph,
/// compiles it to HLO via compileGradientGraph, then compiles to a
/// PJRT LoadedExecutable. The returned PjrtTrainSession can be executed
/// many times with different runtime inputs.
///
/// `runtime_input_node_ids`: ordered slice of NodeIds (in the ORIGINAL graph)
/// that will be provided as runtime inputs. These must cover all parameter nodes.
pub fn compilePjrtTrainSession(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    loss_node: NodeId,
    pjrt_client: anytype,
    runtime_input_node_ids: []const NodeId,
    options: TrainStepOptions,
) !PjrtTrainSession {
    if (comptime !build_options.enable_pjrt) return error.PjrtNotCompiled;

    // Resolve trainable params.
    const wrt = try resolveWrtParams(allocator, graph, options.trainable_params);
    defer allocator.free(wrt);

    // Run autodiff → lowered graph + gradient node IDs.
    var grad_result = try autodiff.gradient(allocator, graph, loss_node, wrt);
    defer grad_result.deinit();

    // Mark loss + gradients as outputs in the lowered graph.
    grad_result.graph.outputs.clearRetainingCapacity();
    const lowered_loss = grad_result.id_map[loss_node];
    try grad_result.graph.markOutput(lowered_loss);
    for (grad_result.param_grads) |grad_id| {
        try grad_result.graph.markOutput(grad_id);
    }

    // Build input_ids: map original runtime input NodeIds through id_map.
    const input_ids = try allocator.alloc(NodeId, runtime_input_node_ids.len);
    defer allocator.free(input_ids);
    for (runtime_input_node_ids, 0..) |orig_id, ii| {
        input_ids[ii] = grad_result.id_map[orig_id];
    }

    // Build output_ids: [loss, grad_0, grad_1, ...]
    const num_outputs = 1 + grad_result.param_grads.len;
    const output_ids = try allocator.alloc(NodeId, num_outputs);
    defer allocator.free(output_ids);
    output_ids[0] = lowered_loss;
    for (grad_result.param_grads, 0..) |gid, gi| output_ids[gi + 1] = gid;

    // Compile the lowered graph to HLO.
    var compile_result = try pjrt_compiler_mod.compileGradientGraph(
        allocator,
        &grad_result.graph,
        input_ids,
        output_ids,
    );
    defer compile_result.deinit();

    // Compile to PJRT executable.
    var executable = try pjrt_client.compile(compile_result.hlo_bytes, num_outputs);
    errdefer executable.deinit();

    // Build output_names: ["loss", param_name_0, param_name_1, ...]
    const output_names = try allocator.alloc([]u8, num_outputs);
    var output_names_init: usize = 0;
    errdefer {
        for (output_names[0..output_names_init]) |n| allocator.free(n);
        allocator.free(output_names);
    }
    output_names[0] = try allocator.dupe(u8, "loss");
    output_names_init = 1;
    for (wrt, 0..) |param_id, pi| {
        const param_node = graph.node(param_id);
        const name = graph.parameterName(param_node);
        output_names[pi + 1] = try allocator.dupe(u8, name);
        output_names_init += 1;
    }

    // Copy runtime_input_node_ids for storage in the session.
    const stored_input_ids = try allocator.dupe(NodeId, runtime_input_node_ids);
    errdefer allocator.free(stored_input_ids);

    return .{
        .executable = executable,
        .client = pjrt_client,
        .input_node_ids = stored_input_ids,
        .output_names = output_names,
        .num_outputs = num_outputs,
        .allocator = allocator,
    };
}

// ── Tests ──────────────────────────────────────────────────────────────

const native_mod = @import("../ops/native_compute.zig");
const NativeCompute = native_mod.NativeCompute;
const WeightStore = native_mod.WeightStore;

test "trainStep computes loss and gradients for linear model" {
    // Build: y = Wx + b, loss = MSE(y, target) via reduceSum((y - target)^2) / n
    // Simplified: loss = reduceSum(linear(x, w, b))  (scalar loss)
    const allocator = std.testing.allocator;

    var g = Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    // x:[2,4], w:[3,4], bias:[3] -> y:[2,3] -> loss = sum(y)
    const x = try bld.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const w = try bld.parameter("w", Shape.init(.f32, &.{ 3, 4 }));
    const bias = try bld.parameter("bias", Shape.init(.f32, &.{3}));
    const y = try bld.linear(x, w, bias, 2, 4, 3);
    const loss = try bld.reduceSum(y, &.{ 0, 1 });
    try g.markOutput(loss);

    // Set up native backend with empty WeightStore.
    var ws = WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = NativeCompute.init(allocator, &ws, null);
    var cb_val = compute.computeBackend();

    // Create parameter CTs.
    const x_ct = try cb_val.fromFloat32(&.{ 1, 2, 3, 4, 5, 6, 7, 8 });
    defer cb_val.free(x_ct);
    const w_ct = try cb_val.fromFloat32(&.{ 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2 });
    defer cb_val.free(w_ct);
    const bias_ct = try cb_val.fromFloat32(&.{ 0.01, 0.02, 0.03 });
    defer cb_val.free(bias_ct);

    var rt = std.AutoHashMapUnmanaged(NodeId, CT){};
    defer rt.deinit(allocator);
    try rt.put(allocator, x, x_ct);
    try rt.put(allocator, w, w_ct);
    try rt.put(allocator, bias, bias_ct);

    var result = try trainStep(allocator, &g, loss, &cb_val, rt, .{});
    defer result.deinit();

    // Loss should be positive (sum of y values).
    try std.testing.expect(result.loss > 0.0);

    // Should have gradients for w and bias (and x, though x is also a "parameter" here).
    try std.testing.expect(result.gradients.get("w") != null);
    try std.testing.expect(result.gradients.get("bias") != null);
    try std.testing.expect(result.gradients.get("x") != null);

    // w gradient should have 12 elements (3x4)
    try std.testing.expectEqual(@as(usize, 12), result.gradients.get("w").?.len);
    // bias gradient should have 3 elements
    try std.testing.expectEqual(@as(usize, 3), result.gradients.get("bias").?.len);
}

test "strict CUDA execution forces graph executor while preserving ordinary opt-in policy" {
    try std.testing.expect(!try selectCudaTrainingGraphExecutor(false, false, false));
    try std.testing.expect(try selectCudaTrainingGraphExecutor(false, true, false));
    try std.testing.expect(try selectCudaTrainingGraphExecutor(true, false, false));
    try std.testing.expectError(
        error.StrictCudaGraphExecutorDisabled,
        selectCudaTrainingGraphExecutor(true, true, true),
    );
    try std.testing.expect(!try selectCudaTrainingGraphExecutor(false, true, true));
}

test "CompiledTrainSession can retain gradient tensors without host extraction" {
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

    var session = try CompiledTrainSession.init(allocator, &g, loss, .{ .trainable_params = &.{ "w", "bias" } });
    defer session.deinit();

    var ws = WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = NativeCompute.init(allocator, &ws, null);
    var cb_val = compute.computeBackend();

    const x_ct = try cb_val.fromFloat32Shape(&.{ 1, 2, 3, 4, 5, 6, 7, 8 }, &.{ 2, 4 });
    defer cb_val.free(x_ct);
    const w_ct = try cb_val.fromFloat32Shape(&.{ 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2 }, &.{ 3, 4 });
    defer cb_val.free(w_ct);
    const bias_ct = try cb_val.fromFloat32Shape(&.{ 0.01, 0.02, 0.03 }, &.{3});
    defer cb_val.free(bias_ct);

    var rt = std.AutoHashMapUnmanaged(NodeId, CT){};
    defer rt.deinit(allocator);
    try rt.put(allocator, x, x_ct);
    try rt.put(allocator, w, w_ct);
    try rt.put(allocator, bias, bias_ct);

    var result = try session.executeDeviceGradients(&cb_val, rt);
    defer result.deinit();

    try std.testing.expect(result.loss > 0.0);
    try std.testing.expect(result.gradients.count() == 0);
    const w_grad = result.device_gradients.get("w") orelse return error.MissingGradient;
    const w_data = try cb_val.toFloat32(w_grad, allocator);
    defer allocator.free(w_data);
    try std.testing.expectEqual(@as(usize, 12), w_data.len);
    try std.testing.expect(result.device_gradients.get("bias") != null);
}

test "trainStep on linear-gelu chain" {
    // Build: y = gelu(linear(x, w, b)), loss = reduceSum(y)
    const allocator = std.testing.allocator;

    var g = Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    // x:[2,4], w:[3,4], bias:[3] -> linear:[2,3] -> gelu -> loss = sum
    const x = try bld.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const w = try bld.parameter("w", Shape.init(.f32, &.{ 3, 4 }));
    const bias = try bld.parameter("bias", Shape.init(.f32, &.{3}));
    const y = try bld.linear(x, w, bias, 2, 4, 3);
    const activated = try bld.gelu(y);
    const loss = try bld.reduceSum(activated, &.{ 0, 1 });
    try g.markOutput(loss);

    // Set up native backend.
    var ws = WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = NativeCompute.init(allocator, &ws, null);
    var cb_val = compute.computeBackend();

    // Create parameter CTs with small positive values.
    const x_ct = try cb_val.fromFloat32(&.{ 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5 });
    defer cb_val.free(x_ct);
    const w_ct = try cb_val.fromFloat32(&.{ 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2 });
    defer cb_val.free(w_ct);
    const bias_ct = try cb_val.fromFloat32(&.{ 0.01, 0.02, 0.03 });
    defer cb_val.free(bias_ct);

    var rt = std.AutoHashMapUnmanaged(NodeId, CT){};
    defer rt.deinit(allocator);
    try rt.put(allocator, x, x_ct);
    try rt.put(allocator, w, w_ct);
    try rt.put(allocator, bias, bias_ct);

    var result = try trainStep(allocator, &g, loss, &cb_val, rt, .{});
    defer result.deinit();

    // Loss should be positive (gelu of positive values -> positive).
    try std.testing.expect(result.loss > 0.0);

    // All three params should have gradients.
    try std.testing.expect(result.gradients.get("x") != null);
    try std.testing.expect(result.gradients.get("w") != null);
    try std.testing.expect(result.gradients.get("bias") != null);

    // Gradient shapes should match parameter shapes.
    try std.testing.expectEqual(@as(usize, 8), result.gradients.get("x").?.len); // 2x4
    try std.testing.expectEqual(@as(usize, 12), result.gradients.get("w").?.len); // 3x4
    try std.testing.expectEqual(@as(usize, 3), result.gradients.get("bias").?.len); // 3
}

test "trainStep emits checkpoint summary when enabled" {
    const allocator = std.testing.allocator;

    var g = Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    const x = try bld.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const w1 = try bld.parameter("w1", Shape.init(.f32, &.{ 4, 4 }));
    const w2 = try bld.parameter("w2", Shape.init(.f32, &.{ 4, 4 }));
    const y1 = try bld.linearNoBias(x, w1, 2, 4, 4);
    const y2 = try bld.linearNoBias(y1, w2, 2, 4, 4);
    const loss = try bld.reduceSum(y2, &.{ 0, 1 });
    try g.markOutput(loss);

    var ws = WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = NativeCompute.init(allocator, &ws, null);
    var cb_val = compute.computeBackend();

    const x_ct = try cb_val.fromFloat32(&.{ 1, 1, 1, 1, 1, 1, 1, 1 });
    defer cb_val.free(x_ct);
    const w1_ct = try cb_val.fromFloat32(&.{ 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6 });
    defer cb_val.free(w1_ct);
    const w2_ct = try cb_val.fromFloat32(&.{ 0.2, 0.1, 0.4, 0.3, 0.6, 0.5, 0.8, 0.7, 1.0, 0.9, 1.2, 1.1, 1.4, 1.3, 1.6, 1.5 });
    defer cb_val.free(w2_ct);

    var rt = std.AutoHashMapUnmanaged(NodeId, CT){};
    defer rt.deinit(allocator);
    try rt.put(allocator, x, x_ct);
    try rt.put(allocator, w1, w1_ct);
    try rt.put(allocator, w2, w2_ct);

    var result = try trainStep(allocator, &g, loss, &cb_val, rt, .{
        .checkpoint_config = .{ .strategy = .every_n_layers, .layer_interval = 1 },
        .emit_checkpoint_analysis = true,
    });
    defer result.deinit();

    try std.testing.expect(result.checkpoint_summary != null);
    try std.testing.expect(result.checkpoint_summary.?.recomputable_activations > 0);
}
