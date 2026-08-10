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
const compat = @import("../io/compat.zig");
const gliner2 = @import("gliner2.zig");
const safetensors = @import("../models/safetensors.zig");

pub const manifest_file_name = "training_manifest.json";
pub const metrics_file_name = "training_metrics.jsonl";
pub const adapter_file_suffix = ".bin";
pub const expected_manifest_schema_version = "gliner2_autodiff_training/v3";
pub const expected_artifact_family_version = "gliner2_autodiff_adapter/v2";
const loss_trend_window = 20;

pub const ValidationOptions = struct {
    require_loss_decrease: bool = false,
    min_supervised_tokens_per_second: ?f64 = null,
    max_avg_step_wall_ms: ?f64 = null,
    max_total_execute_ms: ?f64 = null,
    max_peak_resident_bytes: ?usize = null,
    min_examples: ?usize = null,
    min_steps: ?usize = null,
    min_entity_labels: ?usize = null,
    min_supervised_tokens: ?usize = null,
    min_entity_tokens: ?usize = null,
    require_backend: ?[]const u8 = null,
    require_objective: ?[]const u8 = null,
    require_optimizer_backend: ?[]const u8 = null,
    max_device_trainable_transfer_count: ?u64 = null,
    max_device_resident_transfer_count: ?u64 = null,
    min_device_trainable_bytes: ?usize = null,
    max_metal_tensor_device_owned_peak_live_bytes: ?u64 = null,
    max_metal_runtime_total_bytes: ?u64 = null,
    max_metal_eager_arena_peak_bytes: ?u64 = null,
    max_metal_eager_arena_spill_bytes: ?u64 = null,
    max_metal_chunk_local_output_peak_bytes: ?u64 = null,
    max_metal_chunk_local_output_spill_bytes: ?u64 = null,
    max_metal_chunk_local_output_unconsumed_hints: ?u64 = null,
    min_metal_chunk_local_output_consumed_hints: ?u64 = null,
    min_metal_runtime_reuse_hit_count: ?u64 = null,
    max_graph_command_dispatch_count: ?u64 = null,
    max_graph_host_output_count: ?u64 = null,
    max_metal_frame_gpu_ms: ?f64 = null,
    max_metal_last_frame_compute_encoder_count: ?u64 = null,
    min_metal_frame_chunk_boundary_count: ?u64 = null,
    min_metal_frame_chunk_promoted_value_count: ?u64 = null,
    min_metal_frame_chunk_swept_value_count: ?u64 = null,
    min_graph_runtime_region_dispatch_count: ?u64 = null,
    max_graph_runtime_region_fallback_count: ?u64 = null,
    min_graph_runtime_region_elided_node_count: ?u64 = null,
    min_metal_deberta_ffn_forward_region_count: ?u64 = null,
    min_metal_deberta_encoder_lora_layer_region_count: ?u64 = null,
    min_metal_deberta_encoder_lora_residual_layernorm_region_count: ?u64 = null,
    max_metal_deberta_encoder_lora_layer_scaffold_count: ?u64 = null,
    max_metal_deberta_encoder_lora_layer_fallback_count: ?u64 = null,
    min_metal_deberta_attention_flash_call_count: ?u64 = null,
    max_metal_deberta_attention_gemm_fallback_count: ?u64 = null,
    min_metal_deberta_encoder_layer_success_count: ?u64 = null,
    min_metal_deberta_ffn_fused_call_count: ?u64 = null,
    max_metal_deberta_ffn_fused_fallback_count: ?u64 = null,
    max_runtime_frame_ineligible_missing_model_metadata: ?u64 = null,
};

pub const RunValidationSummary = struct {
    output_dir: []const u8,
    manifest_path: []const u8,
    metrics_path: []const u8,
    peft_adapter_checkpoint_path: []const u8,
    peft_adapter_config_path: []const u8,
    task_head_checkpoint_path: []const u8,
    adapter_file_count: usize,
    peft_adapter_tensor_count: usize,
    task_head_tensor_count: usize,
    task_head_num_classes: usize,
    task_head_hidden_size: usize,
    manifest_adapter_file_count: usize,
    manifest_peft_adapter_tensor_count: usize,
    manifest_task_head_tensor_count: usize,
    manifest_epochs: usize,
    manifest_example_count: usize,
    manifest_total_steps: usize,
    manifest_optimizer_steps: usize,
    manifest_batch_size: usize,
    manifest_seq_len: usize,
    manifest_entity_label_count: usize,
    manifest_backend: []const u8,
    manifest_objective: []const u8,
    metric_record_count: usize,
    step_record_count: usize,
    epoch_record_count: usize,
    max_optimizer_step: u64,
    supervised_token_count: usize,
    entity_token_count: usize,
    ignored_token_count: usize,
    total_step_wall_ms: f64,
    avg_step_wall_ms: f64,
    supervised_tokens_per_second: f64,
    total_graph_build_ms: f64,
    total_runtime_input_ms: f64,
    total_autodiff_ms: f64,
    total_execute_ms: f64,
    total_extract_ms: f64,
    total_optimizer_update_ms: f64,
    total_device_optimizer_ms: f64,
    optimizer_backend: ?[]const u8,
    optimizer_backend_mismatch: bool,
    max_device_trainable_transfer_count: u64,
    max_device_resident_transfer_count: u64,
    max_device_trainable_bytes: usize,
    max_peak_resident_bytes: usize,
    max_metal_tensor_device_owned_live_bytes: u64,
    max_metal_tensor_device_owned_peak_live_bytes: u64,
    max_metal_runtime_total_bytes: u64,
    max_metal_runtime_frame_retained_bytes: u64,
    max_metal_runtime_reuse_pool_bytes: u64,
    max_metal_runtime_reuse_pool_slots: u64,
    max_metal_runtime_reuse_pool_peak_slots: u64,
    total_metal_runtime_reuse_alloc_delta: u64,
    total_metal_runtime_reuse_hit_delta: u64,
    metal_runtime_reuse_hit_rate: f64,
    max_metal_eager_arena_peak_bytes: u64,
    max_metal_eager_arena_live_bytes: u64,
    total_metal_eager_arena_reuse_hits: u64,
    total_metal_eager_arena_allocations: u64,
    total_metal_eager_arena_spill_bytes: u64,
    total_metal_eager_arena_hazard_declines: u64,
    total_metal_eager_arena_alias_conflicts: u64,
    total_metal_eager_arena_alias_reclaims: u64,
    total_metal_eager_arena_alias_reclaim_bytes: u64,
    max_metal_chunk_local_output_peak_bytes: u64,
    max_metal_chunk_local_output_live_bytes: u64,
    total_metal_chunk_local_output_allocations: u64,
    total_metal_chunk_local_output_reuse_hits: u64,
    total_metal_chunk_local_output_consumed_hints: u64,
    total_metal_chunk_local_output_unconsumed_hints: u64,
    total_metal_chunk_local_output_spill_bytes: u64,
    total_metal_chunk_local_output_alias_conflicts: u64,
    total_metal_chunk_local_output_resets: u64,
    total_metal_chunk_local_output_reset_freed_bytes: u64,
    total_metal_chunk_local_output_discard_freed_bytes: u64,
    total_metal_chunk_local_output_reset_live_carry_values: u64,
    total_graph_command_dispatches: u64,
    total_graph_planned_dispatches: u64,
    total_graph_host_outputs: u64,
    total_graph_true_host_outputs: u64,
    total_graph_parameter_materializations: u64,
    total_graph_device_parameter_outputs: u64,
    max_metal_frame_gpu_ms: f64,
    max_metal_last_frame_compute_encoders: u64,
    total_metal_frame_chunk_boundaries: u64,
    total_metal_frame_chunk_promoted_values: u64,
    total_metal_frame_chunk_swept_values: u64,
    total_graph_runtime_region_dispatches: u64,
    total_graph_runtime_region_fallbacks: u64,
    total_graph_runtime_region_active_regions: u64,
    total_graph_runtime_region_covered_nodes: u64,
    total_graph_runtime_region_elided_nodes: u64,
    total_graph_runtime_region_plan_compiles: u64,
    total_graph_runtime_region_plan_reuses: u64,
    total_graph_runtime_frame_metadata_ready: u64,
    total_graph_runtime_frame_ineligible_no_regions: u64,
    total_graph_runtime_frame_ineligible_missing_qkv: u64,
    total_graph_runtime_frame_ineligible_missing_attention: u64,
    total_graph_runtime_frame_ineligible_missing_ffn: u64,
    total_graph_runtime_frame_ineligible_missing_ple: u64,
    total_graph_runtime_frame_ineligible_single_row: u64,
    total_graph_runtime_frame_ineligible_non_layer_order: u64,
    total_graph_runtime_frame_ineligible_shape_mismatch: u64,
    total_graph_runtime_frame_ineligible_missing_model_metadata: u64,
    total_metal_deberta_ffn_forward_regions: u64,
    total_metal_deberta_encoder_lora_layer_regions: u64,
    total_metal_deberta_encoder_lora_residual_layernorm_regions: u64,
    total_metal_deberta_encoder_lora_layer_scaffold_regions: u64,
    total_metal_deberta_encoder_lora_layer_fallbacks: u64,
    total_metal_deberta_encoder_layer_successes: u64,
    total_metal_deberta_attention_flash_calls: u64,
    total_metal_deberta_attention_gemm_calls: u64,
    total_metal_deberta_attention_gemm_fallbacks: u64,
    total_metal_deberta_attention_legacy_calls: u64,
    total_metal_deberta_ffn_fused_calls: u64,
    total_metal_deberta_ffn_fused_mps_matmuls: u64,
    total_metal_deberta_ffn_fused_fallbacks: u64,
    first_step_loss: ?f64 = null,
    final_step_loss: ?f64 = null,
    all_step_losses_finite: bool,
    loss_decreased: bool,
};

pub fn validateRun(
    allocator: std.mem.Allocator,
    out_dir: []const u8,
    options: ValidationOptions,
) !RunValidationSummary {
    const manifest_path = try std.fs.path.join(allocator, &.{ out_dir, manifest_file_name });
    errdefer allocator.free(manifest_path);
    const metrics_path = try std.fs.path.join(allocator, &.{ out_dir, metrics_file_name });
    errdefer allocator.free(metrics_path);
    const peft_adapter_checkpoint_path = try std.fs.path.join(allocator, &.{ out_dir, gliner2.adapter_checkpoint_file_name });
    errdefer allocator.free(peft_adapter_checkpoint_path);
    const peft_adapter_config_path = try std.fs.path.join(allocator, &.{ out_dir, gliner2.adapter_config_file_name });
    errdefer allocator.free(peft_adapter_config_path);
    const task_head_checkpoint_path = try std.fs.path.join(allocator, &.{ out_dir, gliner2.task_head_checkpoint_file_name });
    errdefer allocator.free(task_head_checkpoint_path);

    const manifest_bytes = try compat.cwd().readFileAlloc(compat.io(), manifest_path, allocator, .limited(4 * 1024 * 1024));
    defer allocator.free(manifest_bytes);
    var manifest_parsed = try std.json.parseFromSlice(std.json.Value, allocator, manifest_bytes, .{ .ignore_unknown_fields = true });
    defer manifest_parsed.deinit();
    if (manifest_parsed.value != .object) return error.InvalidTrainingManifest;
    const manifest = inspectManifest(manifest_parsed.value.object) catch return error.InvalidTrainingManifest;

    const io = compat.io();
    var metrics_file = try compat.cwd().openFile(io, metrics_path, .{});
    defer metrics_file.close(io);
    const metrics_reader_buffer = try allocator.alloc(u8, 4 * 1024 * 1024);
    defer allocator.free(metrics_reader_buffer);
    var metrics_file_reader = metrics_file.readerStreaming(io, metrics_reader_buffer);
    const metrics = try inspectMetricsJsonl(allocator, &metrics_file_reader.interface);
    defer if (metrics.optimizer_backend) |backend| allocator.free(backend);
    if (metrics.step_record_count == 0) return error.NoStepMetrics;
    if (metrics.step_record_count != manifest.total_steps) return error.TrainingManifestMetricsMismatch;
    if (metrics.max_optimizer_step != manifest.optimizer_steps) return error.TrainingManifestMetricsMismatch;
    if (metrics.epoch_record_count != manifest.epochs) return error.TrainingManifestMetricsMismatch;
    const metrics_avg_loss = metrics.step_loss_sum / @as(f64, @floatFromInt(metrics.step_record_count));
    // Manifests produced by this trainer are exact after f32 round-trip. Keep
    // compatibility with older/manually generated v2 fixtures at f32 scale.
    const avg_loss_tolerance = 1e-6 * @max(@as(f64, 1.0), @abs(metrics_avg_loss));
    if (@abs(metrics_avg_loss - manifest.final_avg_loss) > avg_loss_tolerance) return error.TrainingManifestMetricsMismatch;
    if (!metrics.all_step_losses_finite) return error.NonFiniteLoss;
    if (metrics.total_step_wall_ms <= 0) return error.InvalidPerformanceMetrics;
    if (metrics.total_autodiff_ms <= 0 or metrics.total_execute_ms <= 0 or metrics.max_peak_resident_bytes == 0) return error.InvalidPerformanceMetrics;
    // Fail fast on a token-objective run that supervised nothing (e.g. a masking or
    // tokenization regression that IGNORE-marked every label, so no gradient signal
    // reached the adapter). span-start / gliner2-total-loss legitimately report zero
    // per-token counts, so this hard gate is scoped to the token objective; the
    // optional min_supervised_tokens / min_entity_tokens floors below still apply to
    // every objective when the caller sets them.
    if (std.mem.eql(u8, manifest.objective, "token")) {
        if (metrics.supervised_token_count == 0) return error.NoSupervisedTokens;
        if (metrics.entity_token_count == 0) return error.NoEntityPositiveTokens;
    }
    if (options.min_supervised_tokens_per_second) |min_tps| {
        if (!std.math.isFinite(min_tps) or min_tps < 0) return error.InvalidPerformanceThreshold;
        if (metrics.supervised_tokens_per_second < min_tps) return error.ThroughputBelowThreshold;
    }
    if (options.max_avg_step_wall_ms) |max_ms| {
        if (!std.math.isFinite(max_ms) or max_ms <= 0) return error.InvalidPerformanceThreshold;
        if (metrics.avg_step_wall_ms > max_ms) return error.AvgStepWallAboveThreshold;
    }
    if (options.max_total_execute_ms) |max_ms| {
        if (!std.math.isFinite(max_ms) or max_ms <= 0) return error.InvalidPerformanceThreshold;
        if (metrics.total_execute_ms > max_ms) return error.TotalExecuteMsAboveThreshold;
    }
    if (options.max_peak_resident_bytes) |max_bytes| {
        if (max_bytes == 0) return error.InvalidPerformanceThreshold;
        if (metrics.max_peak_resident_bytes > max_bytes) return error.PeakResidentBytesAboveThreshold;
    }
    if (options.min_examples) |min_examples| {
        if (manifest.example_count < min_examples) return error.ExampleCountBelowThreshold;
    }
    if (options.min_steps) |min_steps| {
        if (manifest.optimizer_steps < min_steps) return error.StepCountBelowThreshold;
    }
    if (options.min_entity_labels) |min_entity_labels| {
        if (manifest.entity_label_count < min_entity_labels) return error.EntityLabelCountBelowThreshold;
    }
    if (options.min_supervised_tokens) |min_supervised_tokens| {
        if (metrics.supervised_token_count < min_supervised_tokens) return error.SupervisedTokenCountBelowThreshold;
    }
    if (options.min_entity_tokens) |min_entity_tokens| {
        if (metrics.entity_token_count < min_entity_tokens) return error.EntityTokenCountBelowThreshold;
    }
    if (options.require_backend) |backend| {
        if (!std.mem.eql(u8, manifest.backend, backend)) return error.TrainingBackendMismatch;
    }
    if (options.require_objective) |objective| {
        if (!std.mem.eql(u8, manifest.objective, objective)) return error.TrainingObjectiveMismatch;
    }
    if (options.require_optimizer_backend) |backend| {
        if (metrics.optimizer_backend_mismatch or !std.mem.eql(u8, metrics.optimizer_backend orelse "", backend)) return error.OptimizerBackendMismatch;
    }
    const max_device_trainable_transfer_count = options.max_device_trainable_transfer_count orelse options.max_device_resident_transfer_count;
    if (max_device_trainable_transfer_count) |max_transfers| {
        if (metrics.max_device_trainable_transfer_count > max_transfers) {
            if (options.max_device_trainable_transfer_count != null) return error.DeviceTrainableTransferCountAboveThreshold;
            return error.DeviceResidentTransferCountAboveThreshold;
        }
    }
    if (options.min_device_trainable_bytes) |min_bytes| {
        if (metrics.max_device_trainable_bytes < min_bytes) return error.DeviceTrainableBytesBelowThreshold;
    }
    if (options.max_metal_runtime_total_bytes) |max_bytes| {
        if (max_bytes == 0) return error.InvalidPerformanceThreshold;
        if (metrics.max_metal_runtime_total_bytes > max_bytes) return error.MetalRuntimeTotalBytesAboveThreshold;
    }
    if (options.max_metal_tensor_device_owned_peak_live_bytes) |max_bytes| {
        if (max_bytes == 0) return error.InvalidPerformanceThreshold;
        if (metrics.max_metal_tensor_device_owned_peak_live_bytes > max_bytes) return error.MetalTensorDeviceOwnedPeakLiveBytesAboveThreshold;
    }
    if (options.max_metal_eager_arena_peak_bytes) |max_bytes| {
        if (max_bytes == 0) return error.InvalidPerformanceThreshold;
        if (metrics.max_metal_eager_arena_peak_bytes > max_bytes) return error.MetalEagerArenaPeakBytesAboveThreshold;
    }
    if (options.max_metal_eager_arena_spill_bytes) |max_bytes| {
        if (metrics.total_metal_eager_arena_spill_bytes > max_bytes) return error.MetalEagerArenaSpillBytesAboveThreshold;
    }
    if (options.max_metal_chunk_local_output_peak_bytes) |max_bytes| {
        if (max_bytes == 0) return error.InvalidPerformanceThreshold;
        if (metrics.max_metal_chunk_local_output_peak_bytes > max_bytes) return error.MetalChunkLocalOutputPeakBytesAboveThreshold;
    }
    if (options.max_metal_chunk_local_output_spill_bytes) |max_bytes| {
        if (metrics.total_metal_chunk_local_output_spill_bytes > max_bytes) return error.MetalChunkLocalOutputSpillBytesAboveThreshold;
    }
    if (options.max_metal_chunk_local_output_unconsumed_hints) |max_hints| {
        if (metrics.total_metal_chunk_local_output_unconsumed_hints > max_hints) return error.MetalChunkLocalOutputUnconsumedHintsAboveThreshold;
    }
    if (options.min_metal_chunk_local_output_consumed_hints) |min_hints| {
        if (metrics.total_metal_chunk_local_output_consumed_hints < min_hints) return error.MetalChunkLocalOutputConsumedHintsBelowThreshold;
    }
    if (options.min_metal_runtime_reuse_hit_count) |min_hits| {
        if (metrics.total_metal_runtime_reuse_hit_delta < min_hits) return error.MetalRuntimeReuseHitCountBelowThreshold;
    }
    if (options.max_graph_command_dispatch_count) |max_dispatches| {
        if (max_dispatches == 0) return error.InvalidPerformanceThreshold;
        if (metrics.total_graph_command_dispatches > max_dispatches) return error.GraphCommandDispatchCountAboveThreshold;
    }
    if (options.max_graph_host_output_count) |max_outputs| {
        if (metrics.total_graph_host_outputs > max_outputs) return error.GraphHostOutputCountAboveThreshold;
    }
    if (options.max_metal_frame_gpu_ms) |max_ms| {
        if (!std.math.isFinite(max_ms) or max_ms <= 0) return error.InvalidPerformanceThreshold;
        if (metrics.max_metal_frame_gpu_ms > max_ms) return error.MetalFrameGpuMsAboveThreshold;
    }
    if (options.max_metal_last_frame_compute_encoder_count) |max_encoders| {
        if (max_encoders == 0) return error.InvalidPerformanceThreshold;
        if (metrics.max_metal_last_frame_compute_encoders > max_encoders) return error.MetalLastFrameComputeEncoderCountAboveThreshold;
    }
    if (options.min_metal_frame_chunk_boundary_count) |min_boundaries| {
        if (metrics.total_metal_frame_chunk_boundaries < min_boundaries) return error.MetalFrameChunkBoundaryCountBelowThreshold;
    }
    if (options.min_metal_frame_chunk_promoted_value_count) |min_promoted| {
        if (metrics.total_metal_frame_chunk_promoted_values < min_promoted) return error.MetalFrameChunkPromotedValueCountBelowThreshold;
    }
    if (options.min_metal_frame_chunk_swept_value_count) |min_swept| {
        if (metrics.total_metal_frame_chunk_swept_values < min_swept) return error.MetalFrameChunkSweptValueCountBelowThreshold;
    }
    if (options.min_graph_runtime_region_dispatch_count) |min_dispatches| {
        if (metrics.total_graph_runtime_region_dispatches < min_dispatches) return error.GraphRuntimeRegionDispatchCountBelowThreshold;
    }
    if (options.max_graph_runtime_region_fallback_count) |max_fallbacks| {
        if (metrics.total_graph_runtime_region_fallbacks > max_fallbacks) return error.GraphRuntimeRegionFallbackCountAboveThreshold;
    }
    if (options.min_graph_runtime_region_elided_node_count) |min_nodes| {
        if (metrics.total_graph_runtime_region_elided_nodes < min_nodes) return error.GraphRuntimeRegionElidedNodeCountBelowThreshold;
    }
    if (options.min_metal_deberta_ffn_forward_region_count) |min_regions| {
        if (metrics.total_metal_deberta_ffn_forward_regions < min_regions) return error.MetalDebertaFfnForwardRegionCountBelowThreshold;
    }
    if (options.min_metal_deberta_encoder_lora_layer_region_count) |min_regions| {
        if (metrics.total_metal_deberta_encoder_lora_layer_regions < min_regions) return error.MetalDebertaEncoderLoraLayerRegionCountBelowThreshold;
    }
    if (options.min_metal_deberta_encoder_lora_residual_layernorm_region_count) |min_regions| {
        if (metrics.total_metal_deberta_encoder_lora_residual_layernorm_regions < min_regions) return error.MetalDebertaEncoderLoraResidualLayerNormRegionCountBelowThreshold;
    }
    if (options.max_metal_deberta_encoder_lora_layer_scaffold_count) |max_regions| {
        if (metrics.total_metal_deberta_encoder_lora_layer_scaffold_regions > max_regions) return error.MetalDebertaEncoderLoraLayerScaffoldCountAboveThreshold;
    }
    if (options.max_metal_deberta_encoder_lora_layer_fallback_count) |max_fallbacks| {
        if (metrics.total_metal_deberta_encoder_lora_layer_fallbacks > max_fallbacks) return error.MetalDebertaEncoderLoraLayerFallbackCountAboveThreshold;
    }
    if (options.min_metal_deberta_attention_flash_call_count) |min_calls| {
        if (metrics.total_metal_deberta_attention_flash_calls < min_calls) return error.MetalDebertaAttentionFlashCallCountBelowThreshold;
    }
    if (options.max_metal_deberta_attention_gemm_fallback_count) |max_fallbacks| {
        if (metrics.total_metal_deberta_attention_gemm_fallbacks > max_fallbacks) return error.MetalDebertaAttentionGemmFallbackCountAboveThreshold;
    }
    if (options.min_metal_deberta_encoder_layer_success_count) |min_successes| {
        if (metrics.total_metal_deberta_encoder_layer_successes < min_successes) return error.MetalDebertaEncoderLayerSuccessCountBelowThreshold;
    }
    if (options.min_metal_deberta_ffn_fused_call_count) |min_calls| {
        if (metrics.total_metal_deberta_ffn_fused_calls < min_calls) return error.MetalDebertaFfnFusedCallCountBelowThreshold;
    }
    if (options.max_metal_deberta_ffn_fused_fallback_count) |max_fallbacks| {
        if (metrics.total_metal_deberta_ffn_fused_fallbacks > max_fallbacks) return error.MetalDebertaFfnFusedFallbackCountAboveThreshold;
    }
    if (options.max_runtime_frame_ineligible_missing_model_metadata) |max_count| {
        if (metrics.total_graph_runtime_frame_ineligible_missing_model_metadata > max_count) return error.RuntimeFrameIneligibleMissingModelMetadataAboveThreshold;
    }
    if (options.require_loss_decrease and !metrics.loss_decreased) return error.LossDidNotDecrease;

    const adapter_file_count = try countAdapterParameterFiles(out_dir);
    if (adapter_file_count == 0) return error.NoAdapterParameterFiles;
    const peft_adapter_tensor_count = try validatePeftAdapterBundle(allocator, peft_adapter_checkpoint_path, peft_adapter_config_path, manifest);
    if (peft_adapter_tensor_count == 0) return error.NoPeftAdapterTensors;
    const task_head = try validateTaskHeadCheckpoint(allocator, task_head_checkpoint_path, .{
        .num_classes = manifest.num_classes,
        .hidden_size = manifest.hidden_size,
    });
    const task_head_tensor_count = task_head.tensor_count;
    if (task_head_tensor_count < 2) return error.MissingTaskHeadTensors;
    if (manifest.adapter_parameter_file_count != adapter_file_count) return error.TrainingManifestArtifactMismatch;
    if (manifest.peft_adapter_tensor_count != peft_adapter_tensor_count) return error.TrainingManifestArtifactMismatch;
    if (manifest.regular_trainable_tensor_count != task_head_tensor_count) return error.TrainingManifestArtifactMismatch;

    return .{
        .output_dir = try allocator.dupe(u8, out_dir),
        .manifest_path = manifest_path,
        .metrics_path = metrics_path,
        .peft_adapter_checkpoint_path = peft_adapter_checkpoint_path,
        .peft_adapter_config_path = peft_adapter_config_path,
        .task_head_checkpoint_path = task_head_checkpoint_path,
        .adapter_file_count = adapter_file_count,
        .peft_adapter_tensor_count = peft_adapter_tensor_count,
        .task_head_tensor_count = task_head_tensor_count,
        .task_head_num_classes = task_head.num_classes,
        .task_head_hidden_size = task_head.hidden_size,
        .manifest_adapter_file_count = manifest.adapter_parameter_file_count,
        .manifest_peft_adapter_tensor_count = manifest.peft_adapter_tensor_count,
        .manifest_task_head_tensor_count = manifest.regular_trainable_tensor_count,
        .manifest_epochs = manifest.epochs,
        .manifest_example_count = manifest.example_count,
        .manifest_total_steps = manifest.total_steps,
        .manifest_optimizer_steps = manifest.optimizer_steps,
        .manifest_batch_size = manifest.batch_size,
        .manifest_seq_len = manifest.seq_len,
        .manifest_entity_label_count = manifest.entity_label_count,
        .manifest_backend = try allocator.dupe(u8, manifest.backend),
        .manifest_objective = try allocator.dupe(u8, manifest.objective),
        .metric_record_count = metrics.metric_record_count,
        .step_record_count = metrics.step_record_count,
        .epoch_record_count = metrics.epoch_record_count,
        .max_optimizer_step = metrics.max_optimizer_step,
        .supervised_token_count = metrics.supervised_token_count,
        .entity_token_count = metrics.entity_token_count,
        .ignored_token_count = metrics.ignored_token_count,
        .total_step_wall_ms = metrics.total_step_wall_ms,
        .avg_step_wall_ms = metrics.avg_step_wall_ms,
        .supervised_tokens_per_second = metrics.supervised_tokens_per_second,
        .total_graph_build_ms = metrics.total_graph_build_ms,
        .total_runtime_input_ms = metrics.total_runtime_input_ms,
        .total_autodiff_ms = metrics.total_autodiff_ms,
        .total_execute_ms = metrics.total_execute_ms,
        .total_extract_ms = metrics.total_extract_ms,
        .total_optimizer_update_ms = metrics.total_optimizer_update_ms,
        .total_device_optimizer_ms = metrics.total_device_optimizer_ms,
        .optimizer_backend = if (metrics.optimizer_backend) |backend| try allocator.dupe(u8, backend) else null,
        .optimizer_backend_mismatch = metrics.optimizer_backend_mismatch,
        .max_device_trainable_transfer_count = metrics.max_device_trainable_transfer_count,
        .max_device_resident_transfer_count = metrics.max_device_resident_transfer_count,
        .max_device_trainable_bytes = metrics.max_device_trainable_bytes,
        .max_peak_resident_bytes = metrics.max_peak_resident_bytes,
        .max_metal_tensor_device_owned_live_bytes = metrics.max_metal_tensor_device_owned_live_bytes,
        .max_metal_tensor_device_owned_peak_live_bytes = metrics.max_metal_tensor_device_owned_peak_live_bytes,
        .max_metal_runtime_total_bytes = metrics.max_metal_runtime_total_bytes,
        .max_metal_runtime_frame_retained_bytes = metrics.max_metal_runtime_frame_retained_bytes,
        .max_metal_runtime_reuse_pool_bytes = metrics.max_metal_runtime_reuse_pool_bytes,
        .max_metal_runtime_reuse_pool_slots = metrics.max_metal_runtime_reuse_pool_slots,
        .max_metal_runtime_reuse_pool_peak_slots = metrics.max_metal_runtime_reuse_pool_peak_slots,
        .total_metal_runtime_reuse_alloc_delta = metrics.total_metal_runtime_reuse_alloc_delta,
        .total_metal_runtime_reuse_hit_delta = metrics.total_metal_runtime_reuse_hit_delta,
        .metal_runtime_reuse_hit_rate = metrics.metal_runtime_reuse_hit_rate,
        .max_metal_eager_arena_peak_bytes = metrics.max_metal_eager_arena_peak_bytes,
        .max_metal_eager_arena_live_bytes = metrics.max_metal_eager_arena_live_bytes,
        .total_metal_eager_arena_reuse_hits = metrics.total_metal_eager_arena_reuse_hits,
        .total_metal_eager_arena_allocations = metrics.total_metal_eager_arena_allocations,
        .total_metal_eager_arena_spill_bytes = metrics.total_metal_eager_arena_spill_bytes,
        .total_metal_eager_arena_hazard_declines = metrics.total_metal_eager_arena_hazard_declines,
        .total_metal_eager_arena_alias_conflicts = metrics.total_metal_eager_arena_alias_conflicts,
        .total_metal_eager_arena_alias_reclaims = metrics.total_metal_eager_arena_alias_reclaims,
        .total_metal_eager_arena_alias_reclaim_bytes = metrics.total_metal_eager_arena_alias_reclaim_bytes,
        .max_metal_chunk_local_output_peak_bytes = metrics.max_metal_chunk_local_output_peak_bytes,
        .max_metal_chunk_local_output_live_bytes = metrics.max_metal_chunk_local_output_live_bytes,
        .total_metal_chunk_local_output_allocations = metrics.total_metal_chunk_local_output_allocations,
        .total_metal_chunk_local_output_reuse_hits = metrics.total_metal_chunk_local_output_reuse_hits,
        .total_metal_chunk_local_output_consumed_hints = metrics.total_metal_chunk_local_output_consumed_hints,
        .total_metal_chunk_local_output_unconsumed_hints = metrics.total_metal_chunk_local_output_unconsumed_hints,
        .total_metal_chunk_local_output_spill_bytes = metrics.total_metal_chunk_local_output_spill_bytes,
        .total_metal_chunk_local_output_alias_conflicts = metrics.total_metal_chunk_local_output_alias_conflicts,
        .total_metal_chunk_local_output_resets = metrics.total_metal_chunk_local_output_resets,
        .total_metal_chunk_local_output_reset_freed_bytes = metrics.total_metal_chunk_local_output_reset_freed_bytes,
        .total_metal_chunk_local_output_discard_freed_bytes = metrics.total_metal_chunk_local_output_discard_freed_bytes,
        .total_metal_chunk_local_output_reset_live_carry_values = metrics.total_metal_chunk_local_output_reset_live_carry_values,
        .total_graph_command_dispatches = metrics.total_graph_command_dispatches,
        .total_graph_planned_dispatches = metrics.total_graph_planned_dispatches,
        .total_graph_host_outputs = metrics.total_graph_host_outputs,
        .total_graph_true_host_outputs = metrics.total_graph_true_host_outputs,
        .total_graph_parameter_materializations = metrics.total_graph_parameter_materializations,
        .total_graph_device_parameter_outputs = metrics.total_graph_device_parameter_outputs,
        .max_metal_frame_gpu_ms = metrics.max_metal_frame_gpu_ms,
        .max_metal_last_frame_compute_encoders = metrics.max_metal_last_frame_compute_encoders,
        .total_metal_frame_chunk_boundaries = metrics.total_metal_frame_chunk_boundaries,
        .total_metal_frame_chunk_promoted_values = metrics.total_metal_frame_chunk_promoted_values,
        .total_metal_frame_chunk_swept_values = metrics.total_metal_frame_chunk_swept_values,
        .total_graph_runtime_region_dispatches = metrics.total_graph_runtime_region_dispatches,
        .total_graph_runtime_region_fallbacks = metrics.total_graph_runtime_region_fallbacks,
        .total_graph_runtime_region_active_regions = metrics.total_graph_runtime_region_active_regions,
        .total_graph_runtime_region_covered_nodes = metrics.total_graph_runtime_region_covered_nodes,
        .total_graph_runtime_region_elided_nodes = metrics.total_graph_runtime_region_elided_nodes,
        .total_graph_runtime_region_plan_compiles = metrics.total_graph_runtime_region_plan_compiles,
        .total_graph_runtime_region_plan_reuses = metrics.total_graph_runtime_region_plan_reuses,
        .total_graph_runtime_frame_metadata_ready = metrics.total_graph_runtime_frame_metadata_ready,
        .total_graph_runtime_frame_ineligible_no_regions = metrics.total_graph_runtime_frame_ineligible_no_regions,
        .total_graph_runtime_frame_ineligible_missing_qkv = metrics.total_graph_runtime_frame_ineligible_missing_qkv,
        .total_graph_runtime_frame_ineligible_missing_attention = metrics.total_graph_runtime_frame_ineligible_missing_attention,
        .total_graph_runtime_frame_ineligible_missing_ffn = metrics.total_graph_runtime_frame_ineligible_missing_ffn,
        .total_graph_runtime_frame_ineligible_missing_ple = metrics.total_graph_runtime_frame_ineligible_missing_ple,
        .total_graph_runtime_frame_ineligible_single_row = metrics.total_graph_runtime_frame_ineligible_single_row,
        .total_graph_runtime_frame_ineligible_non_layer_order = metrics.total_graph_runtime_frame_ineligible_non_layer_order,
        .total_graph_runtime_frame_ineligible_shape_mismatch = metrics.total_graph_runtime_frame_ineligible_shape_mismatch,
        .total_graph_runtime_frame_ineligible_missing_model_metadata = metrics.total_graph_runtime_frame_ineligible_missing_model_metadata,
        .total_metal_deberta_ffn_forward_regions = metrics.total_metal_deberta_ffn_forward_regions,
        .total_metal_deberta_encoder_lora_layer_regions = metrics.total_metal_deberta_encoder_lora_layer_regions,
        .total_metal_deberta_encoder_lora_residual_layernorm_regions = metrics.total_metal_deberta_encoder_lora_residual_layernorm_regions,
        .total_metal_deberta_encoder_lora_layer_scaffold_regions = metrics.total_metal_deberta_encoder_lora_layer_scaffold_regions,
        .total_metal_deberta_encoder_lora_layer_fallbacks = metrics.total_metal_deberta_encoder_lora_layer_fallbacks,
        .total_metal_deberta_encoder_layer_successes = metrics.total_metal_deberta_encoder_layer_successes,
        .total_metal_deberta_attention_flash_calls = metrics.total_metal_deberta_attention_flash_calls,
        .total_metal_deberta_attention_gemm_calls = metrics.total_metal_deberta_attention_gemm_calls,
        .total_metal_deberta_attention_gemm_fallbacks = metrics.total_metal_deberta_attention_gemm_fallbacks,
        .total_metal_deberta_attention_legacy_calls = metrics.total_metal_deberta_attention_legacy_calls,
        .total_metal_deberta_ffn_fused_calls = metrics.total_metal_deberta_ffn_fused_calls,
        .total_metal_deberta_ffn_fused_mps_matmuls = metrics.total_metal_deberta_ffn_fused_mps_matmuls,
        .total_metal_deberta_ffn_fused_fallbacks = metrics.total_metal_deberta_ffn_fused_fallbacks,
        .first_step_loss = metrics.first_step_loss,
        .final_step_loss = metrics.final_step_loss,
        .all_step_losses_finite = metrics.all_step_losses_finite,
        .loss_decreased = metrics.loss_decreased,
    };
}

pub fn freeRunValidationSummary(allocator: std.mem.Allocator, summary: *RunValidationSummary) void {
    allocator.free(summary.output_dir);
    allocator.free(summary.manifest_path);
    allocator.free(summary.metrics_path);
    allocator.free(summary.manifest_backend);
    allocator.free(summary.manifest_objective);
    if (summary.optimizer_backend) |backend| allocator.free(backend);
    allocator.free(summary.peft_adapter_checkpoint_path);
    allocator.free(summary.peft_adapter_config_path);
    allocator.free(summary.task_head_checkpoint_path);
    summary.* = undefined;
}

const MetricsInspection = struct {
    metric_record_count: usize = 0,
    step_record_count: usize = 0,
    epoch_record_count: usize = 0,
    max_optimizer_step: u64 = 0,
    step_loss_sum: f64 = 0,
    supervised_token_count: usize = 0,
    entity_token_count: usize = 0,
    ignored_token_count: usize = 0,
    total_step_wall_ms: f64 = 0,
    total_graph_build_ms: f64 = 0,
    total_runtime_input_ms: f64 = 0,
    total_autodiff_ms: f64 = 0,
    total_execute_ms: f64 = 0,
    total_extract_ms: f64 = 0,
    total_optimizer_update_ms: f64 = 0,
    total_device_optimizer_ms: f64 = 0,
    max_device_trainable_transfer_count: u64 = 0,
    max_device_resident_transfer_count: u64 = 0,
    max_device_trainable_bytes: usize = 0,
    max_peak_resident_bytes: usize = 0,
    max_metal_tensor_device_owned_live_bytes: u64 = 0,
    max_metal_tensor_device_owned_peak_live_bytes: u64 = 0,
    max_metal_runtime_total_bytes: u64 = 0,
    max_metal_runtime_frame_retained_bytes: u64 = 0,
    max_metal_runtime_reuse_pool_bytes: u64 = 0,
    max_metal_runtime_reuse_pool_slots: u64 = 0,
    max_metal_runtime_reuse_pool_peak_slots: u64 = 0,
    total_metal_runtime_reuse_alloc_delta: u64 = 0,
    total_metal_runtime_reuse_hit_delta: u64 = 0,
    max_metal_eager_arena_peak_bytes: u64 = 0,
    max_metal_eager_arena_live_bytes: u64 = 0,
    total_metal_eager_arena_reuse_hits: u64 = 0,
    total_metal_eager_arena_allocations: u64 = 0,
    total_metal_eager_arena_spill_bytes: u64 = 0,
    total_metal_eager_arena_hazard_declines: u64 = 0,
    total_metal_eager_arena_alias_conflicts: u64 = 0,
    total_metal_eager_arena_alias_reclaims: u64 = 0,
    total_metal_eager_arena_alias_reclaim_bytes: u64 = 0,
    max_metal_chunk_local_output_peak_bytes: u64 = 0,
    max_metal_chunk_local_output_live_bytes: u64 = 0,
    total_metal_chunk_local_output_allocations: u64 = 0,
    total_metal_chunk_local_output_reuse_hits: u64 = 0,
    total_metal_chunk_local_output_consumed_hints: u64 = 0,
    total_metal_chunk_local_output_unconsumed_hints: u64 = 0,
    total_metal_chunk_local_output_spill_bytes: u64 = 0,
    total_metal_chunk_local_output_alias_conflicts: u64 = 0,
    total_metal_chunk_local_output_resets: u64 = 0,
    total_metal_chunk_local_output_reset_freed_bytes: u64 = 0,
    total_metal_chunk_local_output_discard_freed_bytes: u64 = 0,
    total_metal_chunk_local_output_reset_live_carry_values: u64 = 0,
    total_graph_command_dispatches: u64 = 0,
    total_graph_planned_dispatches: u64 = 0,
    total_graph_host_outputs: u64 = 0,
    total_graph_true_host_outputs: u64 = 0,
    total_graph_parameter_materializations: u64 = 0,
    total_graph_device_parameter_outputs: u64 = 0,
    max_metal_frame_gpu_ms: f64 = 0,
    max_metal_last_frame_compute_encoders: u64 = 0,
    total_metal_frame_chunk_boundaries: u64 = 0,
    total_metal_frame_chunk_promoted_values: u64 = 0,
    total_metal_frame_chunk_swept_values: u64 = 0,
    total_graph_runtime_region_dispatches: u64 = 0,
    total_graph_runtime_region_fallbacks: u64 = 0,
    total_graph_runtime_region_active_regions: u64 = 0,
    total_graph_runtime_region_covered_nodes: u64 = 0,
    total_graph_runtime_region_elided_nodes: u64 = 0,
    total_graph_runtime_region_plan_compiles: u64 = 0,
    total_graph_runtime_region_plan_reuses: u64 = 0,
    total_graph_runtime_frame_metadata_ready: u64 = 0,
    total_graph_runtime_frame_ineligible_no_regions: u64 = 0,
    total_graph_runtime_frame_ineligible_missing_qkv: u64 = 0,
    total_graph_runtime_frame_ineligible_missing_attention: u64 = 0,
    total_graph_runtime_frame_ineligible_missing_ffn: u64 = 0,
    total_graph_runtime_frame_ineligible_missing_ple: u64 = 0,
    total_graph_runtime_frame_ineligible_single_row: u64 = 0,
    total_graph_runtime_frame_ineligible_non_layer_order: u64 = 0,
    total_graph_runtime_frame_ineligible_shape_mismatch: u64 = 0,
    total_graph_runtime_frame_ineligible_missing_model_metadata: u64 = 0,
    total_metal_deberta_ffn_forward_regions: u64 = 0,
    total_metal_deberta_encoder_lora_layer_regions: u64 = 0,
    total_metal_deberta_encoder_lora_residual_layernorm_regions: u64 = 0,
    total_metal_deberta_encoder_lora_layer_scaffold_regions: u64 = 0,
    total_metal_deberta_encoder_lora_layer_fallbacks: u64 = 0,
    total_metal_deberta_encoder_layer_successes: u64 = 0,
    total_metal_deberta_attention_flash_calls: u64 = 0,
    total_metal_deberta_attention_gemm_calls: u64 = 0,
    total_metal_deberta_attention_gemm_fallbacks: u64 = 0,
    total_metal_deberta_attention_legacy_calls: u64 = 0,
    total_metal_deberta_ffn_fused_calls: u64 = 0,
    total_metal_deberta_ffn_fused_mps_matmuls: u64 = 0,
    total_metal_deberta_ffn_fused_fallbacks: u64 = 0,
    first_step_loss: ?f64 = null,
    final_step_loss: ?f64 = null,
    first_loss_window_sum: f64 = 0,
    first_loss_window_count: usize = 0,
    final_epoch_avg_loss: ?f64 = null,
    recent_loss_window: [loss_trend_window]f64 = [_]f64{0} ** loss_trend_window,
    recent_loss_count: usize = 0,
    recent_loss_index: usize = 0,
    all_step_losses_finite: bool = true,
    optimizer_backend: ?[]const u8 = null,
    optimizer_backend_mismatch: bool = false,

    fn lossDecreased(self: MetricsInspection) bool {
        const first = self.first_step_loss orelse return false;
        const final = self.final_step_loss orelse return false;
        if (self.final_epoch_avg_loss) |epoch_avg| {
            if (self.first_loss_window_count > 0) {
                const first_window_avg = self.first_loss_window_sum / @as(f64, @floatFromInt(self.first_loss_window_count));
                if (epoch_avg < first_window_avg) return true;
            }
        }
        if (self.step_record_count > loss_trend_window and self.first_loss_window_count == loss_trend_window) {
            const count = @min(self.recent_loss_count, loss_trend_window);
            if (count == loss_trend_window) {
                var recent_sum: f64 = 0;
                for (self.recent_loss_window[0..count]) |loss| recent_sum += loss;
                return recent_sum / @as(f64, @floatFromInt(count)) <
                    self.first_loss_window_sum / @as(f64, @floatFromInt(self.first_loss_window_count));
            }
        }
        return final < first;
    }

    fn avgStepWallMs(self: MetricsInspection) f64 {
        if (self.step_record_count == 0) return 0;
        return self.total_step_wall_ms / @as(f64, @floatFromInt(self.step_record_count));
    }

    fn supervisedTokensPerSecond(self: MetricsInspection) f64 {
        if (self.supervised_token_count == 0 or self.total_step_wall_ms <= 0) return 0;
        const seconds = self.total_step_wall_ms / 1000.0;
        return @as(f64, @floatFromInt(self.supervised_token_count)) / seconds;
    }

    fn metalRuntimeReuseHitRate(self: MetricsInspection) f64 {
        if (self.total_metal_runtime_reuse_alloc_delta == 0) return 0;
        return @as(f64, @floatFromInt(self.total_metal_runtime_reuse_hit_delta)) /
            @as(f64, @floatFromInt(self.total_metal_runtime_reuse_alloc_delta));
    }
};

const ManifestInspection = struct {
    adapter_parameter_file_count: usize,
    peft_adapter_tensor_count: usize,
    regular_trainable_tensor_count: usize,
    num_classes: usize,
    schema_slot_count: usize,
    hidden_size: usize,
    model_dir: []const u8,
    lora_rank: usize,
    lora_alpha: f64,
    lora_dropout: f64,
    lora_targets: []const u8,
    resolved_lora_targets: []const std.json.Value,
    epochs: usize,
    batch_size: usize,
    seq_len: usize,
    example_count: usize,
    total_steps: usize,
    optimizer_steps: usize,
    final_avg_loss: f64,
    entity_label_count: usize,
    backend: []const u8,
    objective: []const u8,
};

fn inspectManifest(obj: std.json.ObjectMap) !ManifestInspection {
    const schema_version = jsonString(obj.get("schema_version")) orelse return error.InvalidTrainingManifest;
    if (!std.mem.eql(u8, schema_version, expected_manifest_schema_version)) return error.InvalidTrainingManifest;
    const artifact_family = jsonString(obj.get("artifact_family_version")) orelse return error.InvalidTrainingManifest;
    if (!std.mem.eql(u8, artifact_family, expected_artifact_family_version)) return error.InvalidTrainingManifest;

    const metrics_file = jsonString(obj.get("metrics_file")) orelse return error.InvalidTrainingManifest;
    if (!std.mem.eql(u8, metrics_file, metrics_file_name)) return error.InvalidTrainingManifest;
    const peft_checkpoint = jsonString(obj.get("peft_adapter_checkpoint")) orelse return error.InvalidTrainingManifest;
    if (!std.mem.eql(u8, peft_checkpoint, gliner2.adapter_checkpoint_file_name)) return error.InvalidTrainingManifest;
    const peft_config = jsonString(obj.get("peft_adapter_config")) orelse return error.InvalidTrainingManifest;
    if (!std.mem.eql(u8, peft_config, gliner2.adapter_config_file_name)) return error.InvalidTrainingManifest;
    const task_head_checkpoint = jsonString(obj.get("regular_trainable_checkpoint")) orelse return error.InvalidTrainingManifest;
    if (!std.mem.eql(u8, task_head_checkpoint, gliner2.task_head_checkpoint_file_name)) return error.InvalidTrainingManifest;
    const task_head_role = jsonString(obj.get("regular_trainable_checkpoint_role")) orelse return error.InvalidTrainingManifest;
    if (!std.mem.eql(u8, task_head_role, "trained_task_head") and
        !std.mem.eql(u8, task_head_role, "inference_graph_compatibility_only")) return error.InvalidTrainingManifest;
    const model_dropout = jsonString(obj.get("model_dropout")) orelse return error.InvalidTrainingManifest;
    if (!std.mem.eql(u8, model_dropout, "disabled")) return error.InvalidTrainingManifest;
    const sampling_config = jsonString(obj.get("sampling_config")) orelse return error.InvalidTrainingManifest;
    if (!std.mem.eql(u8, sampling_config, "disabled")) return error.InvalidTrainingManifest;
    const schema_conditioning_policy = jsonString(obj.get("schema_conditioning_policy")) orelse return error.InvalidTrainingManifest;
    if (!std.mem.eql(u8, schema_conditioning_policy, "deterministic-eval-form")) return error.InvalidTrainingManifest;
    _ = jsonBool(obj.get("deterministic")) orelse return error.InvalidTrainingManifest;
    _ = jsonBool(obj.get("train_shuffle")) orelse return error.InvalidTrainingManifest;

    const num_classes = jsonUsize(obj.get("num_classes")) orelse return error.InvalidTrainingManifest;
    const hidden_size = jsonUsize(obj.get("hidden_size")) orelse return error.InvalidTrainingManifest;
    if (num_classes == 0 or hidden_size == 0) return error.InvalidTrainingManifest;
    const model_dir = jsonString(obj.get("model_dir")) orelse return error.InvalidTrainingManifest;
    if (std.mem.trim(u8, model_dir, " \t\r\n").len == 0) return error.InvalidTrainingManifest;
    const lora_rank = jsonUsize(obj.get("lora_rank")) orelse return error.InvalidTrainingManifest;
    const lora_alpha = jsonF64(obj.get("lora_alpha")) orelse return error.InvalidTrainingManifest;
    const lora_dropout = jsonF64(obj.get("lora_dropout")) orelse 0.0;
    const span_negative_mask_rate = jsonF64(obj.get("span_negative_mask_rate")) orelse return error.InvalidTrainingManifest;
    const lora_targets = jsonString(obj.get("lora_targets")) orelse return error.InvalidTrainingManifest;
    const resolved_lora_targets_value = obj.get("resolved_lora_targets") orelse return error.InvalidTrainingManifest;
    if (resolved_lora_targets_value != .array) return error.InvalidTrainingManifest;
    const resolved_lora_targets = resolved_lora_targets_value.array.items;
    if (lora_rank == 0 or !std.math.isFinite(lora_alpha) or lora_alpha <= 0) return error.InvalidTrainingManifest;
    if (!std.math.isFinite(lora_dropout) or lora_dropout < 0.0 or lora_dropout >= 1.0) return error.InvalidTrainingManifest;
    if (!std.math.isFinite(span_negative_mask_rate) or span_negative_mask_rate < 0.0 or span_negative_mask_rate > 1.0) return error.InvalidTrainingManifest;
    if (countCsvTargets(lora_targets) == 0 or hasEmptyCsvTarget(lora_targets)) return error.InvalidTrainingManifest;
    if (resolved_lora_targets.len == 0) return error.InvalidTrainingManifest;
    for (resolved_lora_targets, 0..) |target, idx| {
        if (target != .string or std.mem.trim(u8, target.string, " \t\r\n").len == 0) return error.InvalidTrainingManifest;
        for (resolved_lora_targets[0..idx]) |previous| {
            if (std.mem.eql(u8, previous.string, target.string)) return error.InvalidTrainingManifest;
        }
    }
    const epochs = jsonUsize(obj.get("epochs")) orelse return error.InvalidTrainingManifest;
    const batch_size = jsonUsize(obj.get("batch_size")) orelse return error.InvalidTrainingManifest;
    const seq_len = jsonUsize(obj.get("seq_len")) orelse return error.InvalidTrainingManifest;
    const example_count = jsonUsize(obj.get("example_count")) orelse return error.InvalidTrainingManifest;
    const total_steps = jsonUsize(obj.get("total_steps")) orelse return error.InvalidTrainingManifest;
    const micro_batch_steps = jsonUsize(obj.get("micro_batch_steps")) orelse total_steps;
    const optimizer_steps = jsonUsize(obj.get("optimizer_steps")) orelse total_steps;
    const final_avg_loss = jsonF64(obj.get("final_avg_loss")) orelse return error.InvalidTrainingManifest;
    const entity_label_count = try inspectEntityLabels(obj.get("entity_labels"));
    const backend = jsonString(obj.get("backend")) orelse return error.InvalidTrainingManifest;
    if (std.mem.trim(u8, backend, " \t\r\n").len == 0) return error.InvalidTrainingManifest;
    const objective = jsonString(obj.get("objective")) orelse return error.InvalidTrainingManifest;
    if (!isSupportedObjective(objective)) return error.InvalidTrainingManifest;
    const is_total_loss = std.mem.eql(u8, objective, "gliner2-total-loss") or std.mem.eql(u8, objective, "gliner2_total_loss");
    const schema_slot_count = if (is_total_loss)
        jsonUsize(obj.get("schema_slot_count")) orelse return error.InvalidTrainingManifest
    else
        0;
    if (is_total_loss != std.mem.eql(u8, task_head_role, "inference_graph_compatibility_only")) return error.InvalidTrainingManifest;
    if (epochs == 0 or batch_size == 0 or seq_len == 0 or example_count == 0 or total_steps == 0 or micro_batch_steps != total_steps or optimizer_steps == 0 or optimizer_steps > total_steps) return error.InvalidTrainingManifest;
    if (!std.math.isFinite(final_avg_loss)) return error.InvalidTrainingManifest;
    if (is_total_loss) {
        if (schema_slot_count == 0 or schema_slot_count + 1 != num_classes) return error.InvalidTrainingManifest;
    } else if (entity_label_count == 0 or entity_label_count + 1 > num_classes) {
        return error.InvalidTrainingManifest;
    }
    if (jsonUsize(obj.get("entity_label_count"))) |recorded_count| {
        if (recorded_count != entity_label_count) return error.InvalidTrainingManifest;
    }

    return .{
        .adapter_parameter_file_count = jsonUsize(obj.get("adapter_parameter_file_count")) orelse return error.InvalidTrainingManifest,
        .peft_adapter_tensor_count = jsonUsize(obj.get("peft_adapter_tensor_count")) orelse return error.InvalidTrainingManifest,
        .regular_trainable_tensor_count = jsonUsize(obj.get("regular_trainable_tensor_count")) orelse return error.InvalidTrainingManifest,
        .num_classes = num_classes,
        .schema_slot_count = schema_slot_count,
        .hidden_size = hidden_size,
        .model_dir = model_dir,
        .lora_rank = lora_rank,
        .lora_alpha = lora_alpha,
        .lora_dropout = lora_dropout,
        .lora_targets = lora_targets,
        .resolved_lora_targets = resolved_lora_targets,
        .epochs = epochs,
        .batch_size = batch_size,
        .seq_len = seq_len,
        .example_count = example_count,
        .total_steps = total_steps,
        .optimizer_steps = optimizer_steps,
        .final_avg_loss = final_avg_loss,
        .entity_label_count = entity_label_count,
        .backend = backend,
        .objective = objective,
    };
}

fn isSupportedObjective(value: []const u8) bool {
    return std.mem.eql(u8, value, "token") or
        std.mem.eql(u8, value, "span-start") or
        std.mem.eql(u8, value, "gliner2-total-loss");
}

fn inspectEntityLabels(value: ?std.json.Value) !usize {
    const v = value orelse return error.InvalidTrainingManifest;
    if (v != .array) return error.InvalidTrainingManifest;
    const labels = v.array.items;
    if (labels.len == 0) return error.InvalidTrainingManifest;
    for (labels, 0..) |entry, idx| {
        if (entry != .string) return error.InvalidTrainingManifest;
        if (std.mem.trim(u8, entry.string, " \t\r\n").len == 0) return error.InvalidTrainingManifest;
        for (labels[0..idx]) |previous| {
            if (std.mem.eql(u8, previous.string, entry.string)) return error.InvalidTrainingManifest;
        }
    }
    return labels.len;
}

fn inspectMetricsJsonl(allocator: std.mem.Allocator, reader: *std.Io.Reader) !struct {
    metric_record_count: usize,
    step_record_count: usize,
    epoch_record_count: usize,
    max_optimizer_step: u64,
    step_loss_sum: f64,
    supervised_token_count: usize,
    entity_token_count: usize,
    ignored_token_count: usize,
    total_step_wall_ms: f64,
    avg_step_wall_ms: f64,
    supervised_tokens_per_second: f64,
    total_graph_build_ms: f64,
    total_runtime_input_ms: f64,
    total_autodiff_ms: f64,
    total_execute_ms: f64,
    total_extract_ms: f64,
    total_optimizer_update_ms: f64,
    total_device_optimizer_ms: f64,
    max_device_trainable_transfer_count: u64,
    max_device_resident_transfer_count: u64,
    max_device_trainable_bytes: usize,
    max_peak_resident_bytes: usize,
    max_metal_tensor_device_owned_live_bytes: u64,
    max_metal_tensor_device_owned_peak_live_bytes: u64,
    max_metal_runtime_total_bytes: u64,
    max_metal_runtime_frame_retained_bytes: u64,
    max_metal_runtime_reuse_pool_bytes: u64,
    max_metal_runtime_reuse_pool_slots: u64,
    max_metal_runtime_reuse_pool_peak_slots: u64,
    total_metal_runtime_reuse_alloc_delta: u64,
    total_metal_runtime_reuse_hit_delta: u64,
    metal_runtime_reuse_hit_rate: f64,
    max_metal_eager_arena_peak_bytes: u64,
    max_metal_eager_arena_live_bytes: u64,
    total_metal_eager_arena_reuse_hits: u64,
    total_metal_eager_arena_allocations: u64,
    total_metal_eager_arena_spill_bytes: u64,
    total_metal_eager_arena_hazard_declines: u64,
    total_metal_eager_arena_alias_conflicts: u64,
    total_metal_eager_arena_alias_reclaims: u64,
    total_metal_eager_arena_alias_reclaim_bytes: u64,
    max_metal_chunk_local_output_peak_bytes: u64,
    max_metal_chunk_local_output_live_bytes: u64,
    total_metal_chunk_local_output_allocations: u64,
    total_metal_chunk_local_output_reuse_hits: u64,
    total_metal_chunk_local_output_consumed_hints: u64,
    total_metal_chunk_local_output_unconsumed_hints: u64,
    total_metal_chunk_local_output_spill_bytes: u64,
    total_metal_chunk_local_output_alias_conflicts: u64,
    total_metal_chunk_local_output_resets: u64,
    total_metal_chunk_local_output_reset_freed_bytes: u64,
    total_metal_chunk_local_output_discard_freed_bytes: u64,
    total_metal_chunk_local_output_reset_live_carry_values: u64,
    total_graph_command_dispatches: u64,
    total_graph_planned_dispatches: u64,
    total_graph_host_outputs: u64,
    total_graph_true_host_outputs: u64,
    total_graph_parameter_materializations: u64,
    total_graph_device_parameter_outputs: u64,
    max_metal_frame_gpu_ms: f64,
    max_metal_last_frame_compute_encoders: u64,
    total_metal_frame_chunk_boundaries: u64,
    total_metal_frame_chunk_promoted_values: u64,
    total_metal_frame_chunk_swept_values: u64,
    total_graph_runtime_region_dispatches: u64,
    total_graph_runtime_region_fallbacks: u64,
    total_graph_runtime_region_active_regions: u64,
    total_graph_runtime_region_covered_nodes: u64,
    total_graph_runtime_region_elided_nodes: u64,
    total_graph_runtime_region_plan_compiles: u64,
    total_graph_runtime_region_plan_reuses: u64,
    total_graph_runtime_frame_metadata_ready: u64,
    total_graph_runtime_frame_ineligible_no_regions: u64,
    total_graph_runtime_frame_ineligible_missing_qkv: u64,
    total_graph_runtime_frame_ineligible_missing_attention: u64,
    total_graph_runtime_frame_ineligible_missing_ffn: u64,
    total_graph_runtime_frame_ineligible_missing_ple: u64,
    total_graph_runtime_frame_ineligible_single_row: u64,
    total_graph_runtime_frame_ineligible_non_layer_order: u64,
    total_graph_runtime_frame_ineligible_shape_mismatch: u64,
    total_graph_runtime_frame_ineligible_missing_model_metadata: u64,
    total_metal_deberta_ffn_forward_regions: u64,
    total_metal_deberta_encoder_lora_layer_regions: u64,
    total_metal_deberta_encoder_lora_residual_layernorm_regions: u64,
    total_metal_deberta_encoder_lora_layer_scaffold_regions: u64,
    total_metal_deberta_encoder_lora_layer_fallbacks: u64,
    total_metal_deberta_encoder_layer_successes: u64,
    total_metal_deberta_attention_flash_calls: u64,
    total_metal_deberta_attention_gemm_calls: u64,
    total_metal_deberta_attention_gemm_fallbacks: u64,
    total_metal_deberta_attention_legacy_calls: u64,
    total_metal_deberta_ffn_fused_calls: u64,
    total_metal_deberta_ffn_fused_mps_matmuls: u64,
    total_metal_deberta_ffn_fused_fallbacks: u64,
    first_step_loss: ?f64,
    final_step_loss: ?f64,
    all_step_losses_finite: bool,
    loss_decreased: bool,
    optimizer_backend: ?[]const u8,
    optimizer_backend_mismatch: bool,
} {
    var inspection = MetricsInspection{};
    errdefer if (inspection.optimizer_backend) |backend| allocator.free(backend);
    while (try reader.takeDelimiter('\n')) |raw_line| {
        const line = std.mem.trim(u8, raw_line, " \t\r\n");
        if (line.len == 0) continue;

        var parsed = try std.json.parseFromSlice(std.json.Value, allocator, line, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        if (parsed.value != .object) return error.InvalidMetricsRecord;
        const obj = parsed.value.object;
        inspection.metric_record_count += 1;

        const event = jsonString(obj.get("event")) orelse return error.InvalidMetricsRecord;
        if (std.mem.eql(u8, event, "step")) {
            inspection.step_record_count += 1;
            // Old metrics used the record ordinal as the step. Preserve that
            // read compatibility; accumulation runs still fail parity unless
            // they report their lower optimizer-step count explicitly.
            inspection.max_optimizer_step = @max(
                inspection.max_optimizer_step,
                jsonU64(obj.get("optimizer_step")) orelse
                    (jsonU64(obj.get("step")) orelse @as(u64, @intCast(inspection.step_record_count))),
            );
            const encoded_loss = jsonF64(obj.get("loss")) orelse return error.InvalidMetricsRecord;
            const loss = @as(f64, @as(f32, @floatCast(encoded_loss)));
            if (!std.math.isFinite(encoded_loss) or !std.math.isFinite(loss)) inspection.all_step_losses_finite = false;
            inspection.step_loss_sum += loss;
            if (inspection.first_step_loss == null) inspection.first_step_loss = loss;
            inspection.final_step_loss = loss;
            if (inspection.first_loss_window_count < loss_trend_window) {
                inspection.first_loss_window_sum += loss;
                inspection.first_loss_window_count += 1;
            }
            inspection.recent_loss_window[inspection.recent_loss_index] = loss;
            inspection.recent_loss_index = (inspection.recent_loss_index + 1) % loss_trend_window;
            if (inspection.recent_loss_count < loss_trend_window) inspection.recent_loss_count += 1;
            inspection.supervised_token_count += jsonUsize(obj.get("supervised_token_count")) orelse return error.InvalidMetricsRecord;
            inspection.entity_token_count += jsonUsize(obj.get("entity_token_count")) orelse return error.InvalidMetricsRecord;
            inspection.ignored_token_count += jsonUsize(obj.get("ignored_token_count")) orelse return error.InvalidMetricsRecord;
            const target_build_ms = jsonF64(obj.get("target_build_ms")) orelse return error.InvalidMetricsRecord;
            const train_step_ms = jsonF64(obj.get("train_step_ms")) orelse return error.InvalidMetricsRecord;
            const step_wall_ms = jsonF64(obj.get("step_wall_ms")) orelse return error.InvalidMetricsRecord;
            const graph_build_ms = jsonF64(obj.get("graph_build_ms")) orelse return error.InvalidMetricsRecord;
            const runtime_input_ms = jsonF64(obj.get("runtime_input_ms")) orelse return error.InvalidMetricsRecord;
            const compile_ms = jsonF64(obj.get("compile_ms")) orelse 0;
            const autodiff_ms = jsonF64(obj.get("autodiff_ms")) orelse return error.InvalidMetricsRecord;
            const execute_ms = jsonF64(obj.get("execute_ms")) orelse return error.InvalidMetricsRecord;
            const extract_ms = jsonF64(obj.get("extract_ms")) orelse return error.InvalidMetricsRecord;
            const optimizer_update_ms = jsonF64(obj.get("optimizer_update_ms")) orelse return error.InvalidMetricsRecord;
            const device_optimizer_ms = jsonF64(obj.get("device_optimizer_ms")) orelse 0;
            const optimizer_backend = jsonString(obj.get("optimizer_backend"));
            const device_trainable_transfer_count = jsonU64(obj.get("device_trainable_transfer_count")) orelse
                (jsonU64(obj.get("device_resident_transfer_count")) orelse 0);
            const device_resident_transfer_count = jsonU64(obj.get("device_resident_transfer_count")) orelse 0;
            const device_trainable_bytes = jsonUsize(obj.get("device_trainable_bytes")) orelse 0;
            const metal_tensor_device_owned_live_bytes = jsonU64(obj.get("metal_tensor_device_owned_live_bytes")) orelse 0;
            const metal_tensor_device_owned_peak_live_bytes = jsonU64(obj.get("metal_tensor_device_owned_peak_live_bytes")) orelse 0;
            const metal_runtime_total_bytes = jsonU64(obj.get("metal_runtime_total_bytes")) orelse 0;
            const metal_runtime_frame_retained_bytes = jsonU64(obj.get("metal_runtime_frame_retained_bytes")) orelse 0;
            const metal_runtime_reuse_pool_bytes = jsonU64(obj.get("metal_runtime_reuse_pool_bytes")) orelse 0;
            const metal_runtime_reuse_pool_slots = jsonU64(obj.get("metal_runtime_reuse_pool_slots")) orelse 0;
            const metal_runtime_reuse_pool_peak_slots = jsonU64(obj.get("metal_runtime_reuse_pool_peak_slots")) orelse 0;
            const metal_runtime_reuse_alloc_delta = jsonU64(obj.get("metal_runtime_reuse_alloc_delta")) orelse 0;
            const metal_runtime_reuse_hit_delta = jsonU64(obj.get("metal_runtime_reuse_hit_delta")) orelse 0;
            const metal_eager_arena_peak_bytes = jsonU64(obj.get("graph_executor_metal_eager_arena_peak_bytes")) orelse 0;
            const metal_eager_arena_live_bytes = jsonU64(obj.get("graph_executor_metal_eager_arena_live_bytes")) orelse 0;
            const metal_eager_arena_reuse_hits = jsonU64(obj.get("graph_executor_metal_eager_arena_reuse_hits")) orelse 0;
            const metal_eager_arena_allocations = jsonU64(obj.get("graph_executor_metal_eager_arena_allocations")) orelse 0;
            const metal_eager_arena_spill_bytes = jsonU64(obj.get("graph_executor_metal_eager_arena_spill_bytes")) orelse 0;
            const metal_eager_arena_hazard_declines = jsonU64(obj.get("graph_executor_metal_eager_arena_hazard_declines")) orelse 0;
            const metal_eager_arena_alias_conflicts = jsonU64(obj.get("graph_executor_metal_eager_arena_alias_conflicts")) orelse 0;
            const metal_eager_arena_alias_reclaims = jsonU64(obj.get("graph_executor_metal_eager_arena_alias_reclaims")) orelse 0;
            const metal_eager_arena_alias_reclaim_bytes = jsonU64(obj.get("graph_executor_metal_eager_arena_alias_reclaim_bytes")) orelse 0;
            const metal_chunk_local_output_peak_bytes = jsonU64(obj.get("graph_executor_metal_chunk_local_output_peak_bytes")) orelse 0;
            const metal_chunk_local_output_live_bytes = jsonU64(obj.get("graph_executor_metal_chunk_local_output_live_bytes")) orelse 0;
            const metal_chunk_local_output_allocations = jsonU64(obj.get("graph_executor_metal_chunk_local_output_allocations")) orelse 0;
            const metal_chunk_local_output_reuse_hits = jsonU64(obj.get("graph_executor_metal_chunk_local_output_reuse_hits")) orelse 0;
            const metal_chunk_local_output_consumed_hints = jsonU64(obj.get("graph_executor_metal_chunk_local_output_consumed_hints")) orelse 0;
            const metal_chunk_local_output_unconsumed_hints = jsonU64(obj.get("graph_executor_metal_chunk_local_output_unconsumed_hints")) orelse 0;
            const metal_chunk_local_output_spill_bytes = jsonU64(obj.get("graph_executor_metal_chunk_local_output_spill_bytes")) orelse 0;
            const metal_chunk_local_output_alias_conflicts = jsonU64(obj.get("graph_executor_metal_chunk_local_output_alias_conflicts")) orelse 0;
            const metal_chunk_local_output_resets = jsonU64(obj.get("graph_executor_metal_chunk_local_output_resets")) orelse 0;
            const metal_chunk_local_output_reset_freed_bytes = jsonU64(obj.get("graph_executor_metal_chunk_local_output_reset_freed_bytes")) orelse 0;
            const metal_chunk_local_output_discard_freed_bytes = jsonU64(obj.get("graph_executor_metal_chunk_local_output_discard_freed_bytes")) orelse 0;
            const metal_chunk_local_output_reset_live_carry_values = jsonU64(obj.get("graph_executor_metal_chunk_local_output_reset_live_carry_values")) orelse 0;
            const trainer_total_ms = jsonF64(obj.get("trainer_total_ms")) orelse return error.InvalidMetricsRecord;
            const peak_resident_bytes = jsonUsize(obj.get("peak_resident_bytes")) orelse return error.InvalidMetricsRecord;
            const supervised_tokens_per_second = jsonF64(obj.get("supervised_tokens_per_second")) orelse return error.InvalidMetricsRecord;
            if (!std.math.isFinite(target_build_ms) or target_build_ms < 0) return error.InvalidPerformanceMetrics;
            if (!std.math.isFinite(train_step_ms) or train_step_ms < 0) return error.InvalidPerformanceMetrics;
            if (!std.math.isFinite(step_wall_ms) or step_wall_ms <= 0) return error.InvalidPerformanceMetrics;
            if (!std.math.isFinite(graph_build_ms) or graph_build_ms < 0) return error.InvalidPerformanceMetrics;
            if (!std.math.isFinite(runtime_input_ms) or runtime_input_ms < 0) return error.InvalidPerformanceMetrics;
            if (!std.math.isFinite(compile_ms) or compile_ms < 0) return error.InvalidPerformanceMetrics;
            if (!std.math.isFinite(autodiff_ms) or (autodiff_ms <= 0 and compile_ms <= 0)) return error.InvalidPerformanceMetrics;
            if (!std.math.isFinite(execute_ms) or execute_ms <= 0) return error.InvalidPerformanceMetrics;
            if (!std.math.isFinite(extract_ms) or extract_ms < 0) return error.InvalidPerformanceMetrics;
            if (!std.math.isFinite(optimizer_update_ms) or optimizer_update_ms < 0) return error.InvalidPerformanceMetrics;
            if (!std.math.isFinite(device_optimizer_ms) or device_optimizer_ms < 0) return error.InvalidPerformanceMetrics;
            if (!std.math.isFinite(trainer_total_ms) or trainer_total_ms <= 0) return error.InvalidPerformanceMetrics;
            if (peak_resident_bytes == 0) return error.InvalidPerformanceMetrics;
            if (!std.math.isFinite(supervised_tokens_per_second) or supervised_tokens_per_second < 0) return error.InvalidPerformanceMetrics;
            inspection.total_step_wall_ms += step_wall_ms;
            inspection.total_graph_build_ms += graph_build_ms;
            inspection.total_runtime_input_ms += runtime_input_ms;
            inspection.total_autodiff_ms += if (autodiff_ms > 0) autodiff_ms else compile_ms;
            inspection.total_execute_ms += execute_ms;
            inspection.total_extract_ms += extract_ms;
            inspection.total_optimizer_update_ms += optimizer_update_ms;
            inspection.total_device_optimizer_ms += device_optimizer_ms;
            inspection.max_device_trainable_transfer_count = @max(inspection.max_device_trainable_transfer_count, device_trainable_transfer_count);
            inspection.max_device_resident_transfer_count = @max(inspection.max_device_resident_transfer_count, device_resident_transfer_count);
            inspection.max_device_trainable_bytes = @max(inspection.max_device_trainable_bytes, device_trainable_bytes);
            inspection.max_metal_tensor_device_owned_live_bytes = @max(inspection.max_metal_tensor_device_owned_live_bytes, metal_tensor_device_owned_live_bytes);
            inspection.max_metal_tensor_device_owned_peak_live_bytes = @max(inspection.max_metal_tensor_device_owned_peak_live_bytes, metal_tensor_device_owned_peak_live_bytes);
            inspection.max_metal_runtime_total_bytes = @max(inspection.max_metal_runtime_total_bytes, metal_runtime_total_bytes);
            inspection.max_metal_runtime_frame_retained_bytes = @max(inspection.max_metal_runtime_frame_retained_bytes, metal_runtime_frame_retained_bytes);
            inspection.max_metal_runtime_reuse_pool_bytes = @max(inspection.max_metal_runtime_reuse_pool_bytes, metal_runtime_reuse_pool_bytes);
            inspection.max_metal_runtime_reuse_pool_slots = @max(inspection.max_metal_runtime_reuse_pool_slots, metal_runtime_reuse_pool_slots);
            inspection.max_metal_runtime_reuse_pool_peak_slots = @max(inspection.max_metal_runtime_reuse_pool_peak_slots, metal_runtime_reuse_pool_peak_slots);
            inspection.total_metal_runtime_reuse_alloc_delta += metal_runtime_reuse_alloc_delta;
            inspection.total_metal_runtime_reuse_hit_delta += metal_runtime_reuse_hit_delta;
            inspection.max_metal_eager_arena_peak_bytes = @max(inspection.max_metal_eager_arena_peak_bytes, metal_eager_arena_peak_bytes);
            inspection.max_metal_eager_arena_live_bytes = @max(inspection.max_metal_eager_arena_live_bytes, metal_eager_arena_live_bytes);
            inspection.total_metal_eager_arena_reuse_hits += metal_eager_arena_reuse_hits;
            inspection.total_metal_eager_arena_allocations += metal_eager_arena_allocations;
            inspection.total_metal_eager_arena_spill_bytes += metal_eager_arena_spill_bytes;
            inspection.total_metal_eager_arena_hazard_declines += metal_eager_arena_hazard_declines;
            inspection.total_metal_eager_arena_alias_conflicts += metal_eager_arena_alias_conflicts;
            inspection.total_metal_eager_arena_alias_reclaims += metal_eager_arena_alias_reclaims;
            inspection.total_metal_eager_arena_alias_reclaim_bytes += metal_eager_arena_alias_reclaim_bytes;
            inspection.max_metal_chunk_local_output_peak_bytes = @max(inspection.max_metal_chunk_local_output_peak_bytes, metal_chunk_local_output_peak_bytes);
            inspection.max_metal_chunk_local_output_live_bytes = @max(inspection.max_metal_chunk_local_output_live_bytes, metal_chunk_local_output_live_bytes);
            inspection.total_metal_chunk_local_output_allocations += metal_chunk_local_output_allocations;
            inspection.total_metal_chunk_local_output_reuse_hits += metal_chunk_local_output_reuse_hits;
            inspection.total_metal_chunk_local_output_consumed_hints += metal_chunk_local_output_consumed_hints;
            inspection.total_metal_chunk_local_output_unconsumed_hints += metal_chunk_local_output_unconsumed_hints;
            inspection.total_metal_chunk_local_output_spill_bytes += metal_chunk_local_output_spill_bytes;
            inspection.total_metal_chunk_local_output_alias_conflicts += metal_chunk_local_output_alias_conflicts;
            inspection.total_metal_chunk_local_output_resets += metal_chunk_local_output_resets;
            inspection.total_metal_chunk_local_output_reset_freed_bytes += metal_chunk_local_output_reset_freed_bytes;
            inspection.total_metal_chunk_local_output_discard_freed_bytes += metal_chunk_local_output_discard_freed_bytes;
            inspection.total_metal_chunk_local_output_reset_live_carry_values += metal_chunk_local_output_reset_live_carry_values;
            inspection.total_graph_command_dispatches += jsonU64(obj.get("graph_executor_command_dispatches")) orelse 0;
            inspection.total_graph_planned_dispatches += jsonU64(obj.get("graph_executor_planned_dispatches")) orelse 0;
            inspection.total_graph_host_outputs += jsonU64(obj.get("graph_executor_host_outputs")) orelse 0;
            inspection.total_graph_true_host_outputs += jsonU64(obj.get("graph_executor_true_host_outputs")) orelse blk: {
                const command_outputs = jsonU64(obj.get("graph_executor_host_output_command")) orelse 0;
                const interpreter_outputs = jsonU64(obj.get("graph_executor_host_output_interpreter")) orelse 0;
                const constant_outputs = jsonU64(obj.get("graph_executor_host_output_pre_materialized_constant")) orelse 0;
                const runtime_region_outputs = jsonU64(obj.get("graph_executor_host_output_runtime_region")) orelse 0;
                const unattributed_outputs = jsonU64(obj.get("graph_executor_host_output_unattributed")) orelse 0;
                break :blk command_outputs + interpreter_outputs + constant_outputs + runtime_region_outputs + unattributed_outputs;
            };
            inspection.total_graph_parameter_materializations += jsonU64(obj.get("graph_executor_host_output_parameter")) orelse 0;
            inspection.total_graph_device_parameter_outputs += jsonU64(obj.get("graph_executor_device_output_parameter")) orelse 0;
            inspection.total_metal_frame_chunk_boundaries += jsonU64(obj.get("graph_executor_metal_frame_chunk_boundaries")) orelse 0;
            inspection.total_metal_frame_chunk_promoted_values += jsonU64(obj.get("graph_executor_metal_frame_chunk_promoted_values")) orelse 0;
            inspection.total_metal_frame_chunk_swept_values += jsonU64(obj.get("graph_executor_metal_frame_chunk_swept_values")) orelse 0;
            const metal_frame_gpu_ms = jsonF64(obj.get("metal_frame_gpu_ms")) orelse 0;
            if (!std.math.isFinite(metal_frame_gpu_ms) or metal_frame_gpu_ms < 0) return error.InvalidPerformanceMetrics;
            inspection.max_metal_frame_gpu_ms = @max(inspection.max_metal_frame_gpu_ms, metal_frame_gpu_ms);
            inspection.max_metal_last_frame_compute_encoders = @max(
                inspection.max_metal_last_frame_compute_encoders,
                jsonU64(obj.get("metal_last_frame_compute_encoders")) orelse 0,
            );
            inspection.total_graph_runtime_region_dispatches += jsonU64(obj.get("graph_executor_runtime_region_dispatches")) orelse 0;
            inspection.total_graph_runtime_region_fallbacks += jsonU64(obj.get("graph_executor_runtime_region_fallbacks")) orelse 0;
            inspection.total_graph_runtime_region_active_regions += jsonU64(obj.get("graph_executor_runtime_region_active_regions")) orelse 0;
            inspection.total_graph_runtime_region_covered_nodes += jsonU64(obj.get("graph_executor_runtime_region_covered_nodes")) orelse 0;
            inspection.total_graph_runtime_region_elided_nodes += jsonU64(obj.get("graph_executor_runtime_region_elided_nodes")) orelse 0;
            inspection.total_graph_runtime_region_plan_compiles += jsonU64(obj.get("graph_executor_runtime_region_plan_compiles")) orelse 0;
            inspection.total_graph_runtime_region_plan_reuses += jsonU64(obj.get("graph_executor_runtime_region_plan_reuses")) orelse 0;
            inspection.total_graph_runtime_frame_metadata_ready += jsonU64(obj.get("graph_executor_runtime_frame_metadata_ready")) orelse 0;
            inspection.total_graph_runtime_frame_ineligible_no_regions += jsonU64(obj.get("graph_executor_runtime_frame_ineligible_no_regions")) orelse 0;
            inspection.total_graph_runtime_frame_ineligible_missing_qkv += jsonU64(obj.get("graph_executor_runtime_frame_ineligible_missing_qkv")) orelse 0;
            inspection.total_graph_runtime_frame_ineligible_missing_attention += jsonU64(obj.get("graph_executor_runtime_frame_ineligible_missing_attention")) orelse 0;
            inspection.total_graph_runtime_frame_ineligible_missing_ffn += jsonU64(obj.get("graph_executor_runtime_frame_ineligible_missing_ffn")) orelse 0;
            inspection.total_graph_runtime_frame_ineligible_missing_ple += jsonU64(obj.get("graph_executor_runtime_frame_ineligible_missing_ple")) orelse 0;
            inspection.total_graph_runtime_frame_ineligible_single_row += jsonU64(obj.get("graph_executor_runtime_frame_ineligible_single_row")) orelse 0;
            inspection.total_graph_runtime_frame_ineligible_non_layer_order += jsonU64(obj.get("graph_executor_runtime_frame_ineligible_non_layer_order")) orelse 0;
            inspection.total_graph_runtime_frame_ineligible_shape_mismatch += jsonU64(obj.get("graph_executor_runtime_frame_ineligible_shape_mismatch")) orelse 0;
            inspection.total_graph_runtime_frame_ineligible_missing_model_metadata += jsonU64(obj.get("graph_executor_runtime_frame_ineligible_missing_model_metadata")) orelse 0;
            inspection.total_metal_deberta_ffn_forward_regions += jsonU64(obj.get("metal_deberta_ffn_forward_regions")) orelse 0;
            inspection.total_metal_deberta_encoder_lora_layer_regions += jsonU64(obj.get("metal_deberta_encoder_lora_layer_regions")) orelse 0;
            inspection.total_metal_deberta_encoder_lora_residual_layernorm_regions += jsonU64(obj.get("metal_deberta_encoder_lora_residual_layernorm_regions")) orelse 0;
            inspection.total_metal_deberta_encoder_lora_layer_scaffold_regions += jsonU64(obj.get("metal_deberta_encoder_lora_layer_scaffold_regions")) orelse 0;
            inspection.total_metal_deberta_encoder_lora_layer_fallbacks += jsonU64(obj.get("metal_deberta_encoder_lora_layer_fallbacks")) orelse 0;
            inspection.total_metal_deberta_encoder_layer_successes += jsonU64(obj.get("metal_deberta_encoder_layer_successes")) orelse 0;
            inspection.total_metal_deberta_attention_flash_calls += jsonU64(obj.get("metal_deberta_attention_flash_calls")) orelse 0;
            inspection.total_metal_deberta_attention_gemm_calls += jsonU64(obj.get("metal_deberta_attention_gemm_calls")) orelse 0;
            inspection.total_metal_deberta_attention_gemm_fallbacks += jsonU64(obj.get("metal_deberta_attention_gemm_fallbacks")) orelse 0;
            inspection.total_metal_deberta_attention_legacy_calls += jsonU64(obj.get("metal_deberta_attention_legacy_calls")) orelse 0;
            inspection.total_metal_deberta_ffn_fused_calls += jsonU64(obj.get("metal_deberta_ffn_fused_calls")) orelse 0;
            inspection.total_metal_deberta_ffn_fused_mps_matmuls += jsonU64(obj.get("metal_deberta_ffn_fused_mps_matmuls")) orelse 0;
            inspection.total_metal_deberta_ffn_fused_fallbacks += jsonU64(obj.get("metal_deberta_ffn_fused_fallbacks")) orelse 0;
            if (optimizer_backend) |backend| {
                if (inspection.optimizer_backend) |first_backend| {
                    if (!std.mem.eql(u8, first_backend, backend)) inspection.optimizer_backend_mismatch = true;
                } else {
                    inspection.optimizer_backend = try allocator.dupe(u8, backend);
                }
            }
            inspection.max_peak_resident_bytes = @max(inspection.max_peak_resident_bytes, peak_resident_bytes);
        } else if (std.mem.eql(u8, event, "optimizer_step")) {
            const optimizer_step = jsonU64(obj.get("optimizer_step")) orelse return error.InvalidMetricsRecord;
            if (optimizer_step == 0) return error.InvalidMetricsRecord;
            inspection.max_optimizer_step = @max(inspection.max_optimizer_step, optimizer_step);
            const grad_norm = jsonF64(obj.get("grad_norm")) orelse return error.InvalidMetricsRecord;
            const learning_rate = jsonF64(obj.get("learning_rate")) orelse return error.InvalidMetricsRecord;
            if (!std.math.isFinite(grad_norm) or grad_norm < 0 or !std.math.isFinite(learning_rate) or learning_rate < 0) return error.InvalidMetricsRecord;
        } else if (std.mem.eql(u8, event, "epoch")) {
            inspection.epoch_record_count += 1;
            const avg_loss = jsonF64(obj.get("avg_loss")) orelse return error.InvalidMetricsRecord;
            if (!std.math.isFinite(avg_loss)) inspection.all_step_losses_finite = false;
            inspection.final_epoch_avg_loss = avg_loss;
        }
    }

    return .{
        .metric_record_count = inspection.metric_record_count,
        .step_record_count = inspection.step_record_count,
        .epoch_record_count = inspection.epoch_record_count,
        .max_optimizer_step = inspection.max_optimizer_step,
        .step_loss_sum = inspection.step_loss_sum,
        .supervised_token_count = inspection.supervised_token_count,
        .entity_token_count = inspection.entity_token_count,
        .ignored_token_count = inspection.ignored_token_count,
        .total_step_wall_ms = inspection.total_step_wall_ms,
        .avg_step_wall_ms = inspection.avgStepWallMs(),
        .supervised_tokens_per_second = inspection.supervisedTokensPerSecond(),
        .total_graph_build_ms = inspection.total_graph_build_ms,
        .total_runtime_input_ms = inspection.total_runtime_input_ms,
        .total_autodiff_ms = inspection.total_autodiff_ms,
        .total_execute_ms = inspection.total_execute_ms,
        .total_extract_ms = inspection.total_extract_ms,
        .total_optimizer_update_ms = inspection.total_optimizer_update_ms,
        .total_device_optimizer_ms = inspection.total_device_optimizer_ms,
        .max_device_trainable_transfer_count = inspection.max_device_trainable_transfer_count,
        .max_device_resident_transfer_count = inspection.max_device_resident_transfer_count,
        .max_device_trainable_bytes = inspection.max_device_trainable_bytes,
        .max_peak_resident_bytes = inspection.max_peak_resident_bytes,
        .max_metal_tensor_device_owned_live_bytes = inspection.max_metal_tensor_device_owned_live_bytes,
        .max_metal_tensor_device_owned_peak_live_bytes = inspection.max_metal_tensor_device_owned_peak_live_bytes,
        .max_metal_runtime_total_bytes = inspection.max_metal_runtime_total_bytes,
        .max_metal_runtime_frame_retained_bytes = inspection.max_metal_runtime_frame_retained_bytes,
        .max_metal_runtime_reuse_pool_bytes = inspection.max_metal_runtime_reuse_pool_bytes,
        .max_metal_runtime_reuse_pool_slots = inspection.max_metal_runtime_reuse_pool_slots,
        .max_metal_runtime_reuse_pool_peak_slots = inspection.max_metal_runtime_reuse_pool_peak_slots,
        .total_metal_runtime_reuse_alloc_delta = inspection.total_metal_runtime_reuse_alloc_delta,
        .total_metal_runtime_reuse_hit_delta = inspection.total_metal_runtime_reuse_hit_delta,
        .metal_runtime_reuse_hit_rate = inspection.metalRuntimeReuseHitRate(),
        .max_metal_eager_arena_peak_bytes = inspection.max_metal_eager_arena_peak_bytes,
        .max_metal_eager_arena_live_bytes = inspection.max_metal_eager_arena_live_bytes,
        .total_metal_eager_arena_reuse_hits = inspection.total_metal_eager_arena_reuse_hits,
        .total_metal_eager_arena_allocations = inspection.total_metal_eager_arena_allocations,
        .total_metal_eager_arena_spill_bytes = inspection.total_metal_eager_arena_spill_bytes,
        .total_metal_eager_arena_hazard_declines = inspection.total_metal_eager_arena_hazard_declines,
        .total_metal_eager_arena_alias_conflicts = inspection.total_metal_eager_arena_alias_conflicts,
        .total_metal_eager_arena_alias_reclaims = inspection.total_metal_eager_arena_alias_reclaims,
        .total_metal_eager_arena_alias_reclaim_bytes = inspection.total_metal_eager_arena_alias_reclaim_bytes,
        .max_metal_chunk_local_output_peak_bytes = inspection.max_metal_chunk_local_output_peak_bytes,
        .max_metal_chunk_local_output_live_bytes = inspection.max_metal_chunk_local_output_live_bytes,
        .total_metal_chunk_local_output_allocations = inspection.total_metal_chunk_local_output_allocations,
        .total_metal_chunk_local_output_reuse_hits = inspection.total_metal_chunk_local_output_reuse_hits,
        .total_metal_chunk_local_output_consumed_hints = inspection.total_metal_chunk_local_output_consumed_hints,
        .total_metal_chunk_local_output_unconsumed_hints = inspection.total_metal_chunk_local_output_unconsumed_hints,
        .total_metal_chunk_local_output_spill_bytes = inspection.total_metal_chunk_local_output_spill_bytes,
        .total_metal_chunk_local_output_alias_conflicts = inspection.total_metal_chunk_local_output_alias_conflicts,
        .total_metal_chunk_local_output_resets = inspection.total_metal_chunk_local_output_resets,
        .total_metal_chunk_local_output_reset_freed_bytes = inspection.total_metal_chunk_local_output_reset_freed_bytes,
        .total_metal_chunk_local_output_discard_freed_bytes = inspection.total_metal_chunk_local_output_discard_freed_bytes,
        .total_metal_chunk_local_output_reset_live_carry_values = inspection.total_metal_chunk_local_output_reset_live_carry_values,
        .total_graph_command_dispatches = inspection.total_graph_command_dispatches,
        .total_graph_planned_dispatches = inspection.total_graph_planned_dispatches,
        .total_graph_host_outputs = inspection.total_graph_host_outputs,
        .total_graph_true_host_outputs = inspection.total_graph_true_host_outputs,
        .total_graph_parameter_materializations = inspection.total_graph_parameter_materializations,
        .total_graph_device_parameter_outputs = inspection.total_graph_device_parameter_outputs,
        .max_metal_frame_gpu_ms = inspection.max_metal_frame_gpu_ms,
        .max_metal_last_frame_compute_encoders = inspection.max_metal_last_frame_compute_encoders,
        .total_metal_frame_chunk_boundaries = inspection.total_metal_frame_chunk_boundaries,
        .total_metal_frame_chunk_promoted_values = inspection.total_metal_frame_chunk_promoted_values,
        .total_metal_frame_chunk_swept_values = inspection.total_metal_frame_chunk_swept_values,
        .total_graph_runtime_region_dispatches = inspection.total_graph_runtime_region_dispatches,
        .total_graph_runtime_region_fallbacks = inspection.total_graph_runtime_region_fallbacks,
        .total_graph_runtime_region_active_regions = inspection.total_graph_runtime_region_active_regions,
        .total_graph_runtime_region_covered_nodes = inspection.total_graph_runtime_region_covered_nodes,
        .total_graph_runtime_region_elided_nodes = inspection.total_graph_runtime_region_elided_nodes,
        .total_graph_runtime_region_plan_compiles = inspection.total_graph_runtime_region_plan_compiles,
        .total_graph_runtime_region_plan_reuses = inspection.total_graph_runtime_region_plan_reuses,
        .total_graph_runtime_frame_metadata_ready = inspection.total_graph_runtime_frame_metadata_ready,
        .total_graph_runtime_frame_ineligible_no_regions = inspection.total_graph_runtime_frame_ineligible_no_regions,
        .total_graph_runtime_frame_ineligible_missing_qkv = inspection.total_graph_runtime_frame_ineligible_missing_qkv,
        .total_graph_runtime_frame_ineligible_missing_attention = inspection.total_graph_runtime_frame_ineligible_missing_attention,
        .total_graph_runtime_frame_ineligible_missing_ffn = inspection.total_graph_runtime_frame_ineligible_missing_ffn,
        .total_graph_runtime_frame_ineligible_missing_ple = inspection.total_graph_runtime_frame_ineligible_missing_ple,
        .total_graph_runtime_frame_ineligible_single_row = inspection.total_graph_runtime_frame_ineligible_single_row,
        .total_graph_runtime_frame_ineligible_non_layer_order = inspection.total_graph_runtime_frame_ineligible_non_layer_order,
        .total_graph_runtime_frame_ineligible_shape_mismatch = inspection.total_graph_runtime_frame_ineligible_shape_mismatch,
        .total_graph_runtime_frame_ineligible_missing_model_metadata = inspection.total_graph_runtime_frame_ineligible_missing_model_metadata,
        .total_metal_deberta_ffn_forward_regions = inspection.total_metal_deberta_ffn_forward_regions,
        .total_metal_deberta_encoder_lora_layer_regions = inspection.total_metal_deberta_encoder_lora_layer_regions,
        .total_metal_deberta_encoder_lora_residual_layernorm_regions = inspection.total_metal_deberta_encoder_lora_residual_layernorm_regions,
        .total_metal_deberta_encoder_lora_layer_scaffold_regions = inspection.total_metal_deberta_encoder_lora_layer_scaffold_regions,
        .total_metal_deberta_encoder_lora_layer_fallbacks = inspection.total_metal_deberta_encoder_lora_layer_fallbacks,
        .total_metal_deberta_encoder_layer_successes = inspection.total_metal_deberta_encoder_layer_successes,
        .total_metal_deberta_attention_flash_calls = inspection.total_metal_deberta_attention_flash_calls,
        .total_metal_deberta_attention_gemm_calls = inspection.total_metal_deberta_attention_gemm_calls,
        .total_metal_deberta_attention_gemm_fallbacks = inspection.total_metal_deberta_attention_gemm_fallbacks,
        .total_metal_deberta_attention_legacy_calls = inspection.total_metal_deberta_attention_legacy_calls,
        .total_metal_deberta_ffn_fused_calls = inspection.total_metal_deberta_ffn_fused_calls,
        .total_metal_deberta_ffn_fused_mps_matmuls = inspection.total_metal_deberta_ffn_fused_mps_matmuls,
        .total_metal_deberta_ffn_fused_fallbacks = inspection.total_metal_deberta_ffn_fused_fallbacks,
        .first_step_loss = inspection.first_step_loss,
        .final_step_loss = inspection.final_step_loss,
        .all_step_losses_finite = inspection.all_step_losses_finite,
        .loss_decreased = inspection.lossDecreased(),
        .optimizer_backend = inspection.optimizer_backend,
        .optimizer_backend_mismatch = inspection.optimizer_backend_mismatch,
    };
}

fn countAdapterParameterFiles(out_dir: []const u8) !usize {
    var dir = try compat.cwd().openDir(compat.io(), out_dir, .{ .iterate = true });
    defer dir.close(compat.io());

    var count: usize = 0;
    var iter = dir.iterate();
    while (try iter.next(compat.io())) |entry| {
        if (entry.kind != .file) continue;
        if (std.mem.endsWith(u8, entry.name, ".lora_A" ++ adapter_file_suffix) or
            std.mem.endsWith(u8, entry.name, ".lora_B" ++ adapter_file_suffix)) count += 1;
    }
    return count;
}

fn validatePeftAdapterBundle(
    allocator: std.mem.Allocator,
    adapter_checkpoint_path: []const u8,
    adapter_config_path: []const u8,
    manifest: ManifestInspection,
) !usize {
    const config_bytes = try compat.cwd().readFileAlloc(compat.io(), adapter_config_path, allocator, .limited(4 * 1024 * 1024));
    defer allocator.free(config_bytes);
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, config_bytes, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidPeftAdapterConfig;
    try inspectPeftAdapterConfig(parsed.value.object, manifest);

    var reader = try safetensors.MMapReader.openFileAbsolute(allocator, adapter_checkpoint_path);
    defer reader.deinit();
    var tensors = reader.header.tensors.iterator();
    while (tensors.next()) |entry| {
        const name = entry.key_ptr.*;
        if (!std.mem.startsWith(u8, name, "base_model.model.") or
            (!std.mem.endsWith(u8, name, ".lora_A.weight") and !std.mem.endsWith(u8, name, ".lora_B.weight")))
        {
            return error.InvalidPeftAdapterTensorName;
        }
    }
    return reader.header.tensors.count();
}

fn inspectPeftAdapterConfig(obj: std.json.ObjectMap, manifest: ManifestInspection) !void {
    const peft_type = jsonString(obj.get("peft_type")) orelse return error.InvalidPeftAdapterConfig;
    if (!std.mem.eql(u8, peft_type, "LORA")) return error.InvalidPeftAdapterConfig;
    const task_type = obj.get("task_type") orelse return error.InvalidPeftAdapterConfig;
    if (task_type != .null) return error.InvalidPeftAdapterConfig;
    const auto_mapping = obj.get("auto_mapping") orelse return error.InvalidPeftAdapterConfig;
    if (auto_mapping != .object) return error.InvalidPeftAdapterConfig;
    const base_model_class = jsonString(auto_mapping.object.get("base_model_class")) orelse return error.InvalidPeftAdapterConfig;
    const parent_library = jsonString(auto_mapping.object.get("parent_library")) orelse return error.InvalidPeftAdapterConfig;
    if (!std.mem.eql(u8, base_model_class, "Extractor") or !std.mem.eql(u8, parent_library, "gliner2.model")) return error.InvalidPeftAdapterConfig;
    const bias = jsonString(obj.get("bias")) orelse return error.InvalidPeftAdapterConfig;
    if (!std.mem.eql(u8, bias, "none")) return error.InvalidPeftAdapterConfig;
    const base_model_name_or_path = jsonString(obj.get("base_model_name_or_path")) orelse return error.InvalidPeftAdapterConfig;
    if (!std.mem.eql(u8, base_model_name_or_path, manifest.model_dir)) return error.InvalidPeftAdapterConfig;
    const rank = jsonUsize(obj.get("r")) orelse return error.InvalidPeftAdapterConfig;
    if (rank != manifest.lora_rank) return error.InvalidPeftAdapterConfig;
    const lora_alpha = jsonF64(obj.get("lora_alpha")) orelse return error.InvalidPeftAdapterConfig;
    if (!std.math.isFinite(lora_alpha) or @abs(lora_alpha - manifest.lora_alpha) > 1e-6) return error.InvalidPeftAdapterConfig;
    const lora_dropout = jsonF64(obj.get("lora_dropout")) orelse 0.0;
    if (!std.math.isFinite(lora_dropout) or @abs(lora_dropout - manifest.lora_dropout) > 1e-6) return error.InvalidPeftAdapterConfig;
    const use_dora = jsonBool(obj.get("use_dora")) orelse return error.InvalidPeftAdapterConfig;
    if (use_dora) return error.InvalidPeftAdapterConfig;

    const target_modules_value = obj.get("target_modules") orelse return error.InvalidPeftAdapterConfig;
    if (target_modules_value != .array) return error.InvalidPeftAdapterConfig;
    const target_modules = target_modules_value.array.items;
    if (target_modules.len != manifest.resolved_lora_targets.len) return error.InvalidPeftAdapterConfig;
    for (target_modules, manifest.resolved_lora_targets) |target, expected| {
        if (target != .string or expected != .string) return error.InvalidPeftAdapterConfig;
        if (!std.mem.eql(u8, target.string, expected.string)) return error.InvalidPeftAdapterConfig;
    }
}

fn countCsvTargets(value: []const u8) usize {
    var count: usize = 0;
    var iter = std.mem.tokenizeScalar(u8, value, ',');
    while (iter.next()) |raw| {
        if (std.mem.trim(u8, raw, " \t\r\n").len > 0) count += 1;
    }
    return count;
}

fn hasEmptyCsvTarget(value: []const u8) bool {
    var iter = std.mem.splitScalar(u8, value, ',');
    while (iter.next()) |raw| {
        if (std.mem.trim(u8, raw, " \t\r\n").len == 0) return true;
    }
    return false;
}

const TaskHeadExpectedShape = struct {
    num_classes: usize,
    hidden_size: usize,
};

const TaskHeadInspection = struct {
    tensor_count: usize,
    num_classes: usize,
    hidden_size: usize,
};

fn validateTaskHeadCheckpoint(
    allocator: std.mem.Allocator,
    task_head_checkpoint_path: []const u8,
    expected: TaskHeadExpectedShape,
) !TaskHeadInspection {
    var reader = try safetensors.MMapReader.openFileAbsolute(allocator, task_head_checkpoint_path);
    defer reader.deinit();
    const weight = reader.header.tensors.get("classifier.weight") orelse return error.MissingTaskHeadTensors;
    const bias = reader.header.tensors.get("classifier.bias") orelse return error.MissingTaskHeadTensors;
    if (weight.dtype != .f32 or bias.dtype != .f32) return error.InvalidTaskHeadTensorDType;
    if (weight.shape.len != 2 or bias.shape.len != 1) return error.InvalidTaskHeadTensorShape;

    const weight_classes = positiveShapeDim(weight.shape[0]) orelse return error.InvalidTaskHeadTensorShape;
    const weight_hidden = positiveShapeDim(weight.shape[1]) orelse return error.InvalidTaskHeadTensorShape;
    const bias_classes = positiveShapeDim(bias.shape[0]) orelse return error.InvalidTaskHeadTensorShape;
    if (weight_classes != bias_classes) return error.InvalidTaskHeadTensorShape;
    if (weight_classes != expected.num_classes or weight_hidden != expected.hidden_size) return error.InvalidTaskHeadTensorShape;

    return .{
        .tensor_count = reader.header.tensors.count(),
        .num_classes = weight_classes,
        .hidden_size = weight_hidden,
    };
}

fn positiveShapeDim(dim: i64) ?usize {
    if (dim <= 0) return null;
    return @intCast(dim);
}

fn jsonString(value: ?std.json.Value) ?[]const u8 {
    const v = value orelse return null;
    return switch (v) {
        .string => |s| s,
        else => null,
    };
}

fn jsonF64(value: ?std.json.Value) ?f64 {
    const v = value orelse return null;
    return switch (v) {
        .integer => |i| @floatFromInt(i),
        .float => |f| f,
        else => null,
    };
}

fn jsonUsize(value: ?std.json.Value) ?usize {
    const v = value orelse return null;
    return switch (v) {
        .integer => |i| if (i >= 0) @intCast(i) else null,
        else => null,
    };
}

fn jsonU64(value: ?std.json.Value) ?u64 {
    const v = value orelse return null;
    return switch (v) {
        .integer => |i| if (i >= 0) @intCast(i) else null,
        else => null,
    };
}

fn jsonBool(value: ?std.json.Value) ?bool {
    const v = value orelse return null;
    return switch (v) {
        .bool => |b| b,
        else => null,
    };
}
