// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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
const platform = @import("antfly_platform");
const metal_runtime = @import("../backends/metal_runtime.zig");
const generation = @import("../pipelines/generation.zig");
const gpt_model_mod = @import("../models/gpt.zig");
const qwen3vl_projector = @import("../architectures/qwen3vl_projector.zig");
const runtime = @import("../runtime/root.zig");

/// One immutable serving configuration shared by admission and execution.
pub fn Features(comptime Hooks: type) type {
    return struct {
        prepared: bool = false,
        prepared_rows: u8 = 16,
        scheduling_rows: u8 = 0,
        cpu_preprocess: bool = false,
        vision_batch_items: u8 = 1,
        prefill_batch_items: u8 = 1,
        vision_frame_blocks: u8 = 1,
        pipeline_vision: bool = false,
        prefill_stream_layers: u8 = 0,
        prefill_decode_streak: u8 = 4,
        activation_pool: bool = false,
        prefill_activation_pool: bool = false,
        worker: bool = false,
        epoch_auxiliary: bool = false,
        single_frame: bool = false,
        gpu_token_burst: bool = false,
        gpu_token_burst_width: u8 = 2,
        cuda_burst_extra_bytes: usize = 0,
        gpu_greedy_stream: bool = false,
        stream_chunks: bool = false,
        vision_workspace_cache: bool = false,
        vision_late_seal: bool = false,
        queued_phase_priority: bool = false,
        resident_vision_budget: bool = false,
        prefill_late_seal: bool = false,
        shared_core_weights: bool = false,
        stream_first_layers: u8 = 0,
        batch_attention: bool = false,
        prefill_batch: bool = false,
        packed_mixed: bool = false,
        packed_prefill_items: u8 = 1,
        prepared_prefill_chunk_rows: u16 = 0,
        packed_shared_frame: bool = false,
        shared_decoder_scratch: bool = false,

        pub fn fromEnvironment() @This() {
            const epoch_auxiliary = metal_runtime.lightOnThroughputFeatureEnabled("ANTFLY_LIGHTONOCR3_EPOCH_AUX");
            const prepared = metal_runtime.lightOnThroughputFeatureEnabled("ANTFLY_LIGHTONOCR3_PREPARED_DECODE");
            const prefill_batch = platform.env.getenvBool("ANTFLY_LIGHTONOCR3_PREFILL_BATCH");
            const physical_rows = Hooks.effectiveLightOnReadRows();
            return .{
                .cpu_preprocess = metal_runtime.lightOnThroughputFeatureEnabled("ANTFLY_LIGHTONOCR3_CPU_PREPROCESS"),
                .vision_batch_items = if (platform.env.getenvBool("ANTFLY_LIGHTONOCR3_VISION_BATCH")) @intCast(@min(@max(platform.env.getenvUsize("ANTFLY_LIGHTONOCR3_VISION_BATCH_ITEMS") orelse 4, 1), 4)) else 1,
                .prefill_batch_items = if (prefill_batch) @intCast(@min(@max(platform.env.getenvUsize("ANTFLY_LIGHTONOCR3_PREFILL_BATCH_ITEMS") orelse 4, 1), generation.NativeGenerationPipeline.default_prepared_qwen_prefill_items)) else 1,
                .vision_frame_blocks = if (metal_runtime.qualifiedM4DeviceDefault() and metal_runtime.lightOnThroughputFeatureEnabled("ANTFLY_LIGHTONOCR3_CPU_PREPROCESS")) @intCast(@min(@max(platform.env.getenvUsize("ANTFLY_LIGHTONOCR3_VISION_FRAME_BLOCKS") orelse 1, 1), 4)) else 1,
                .prepared = prepared,
                .prefill_batch = prefill_batch,
                .queued_phase_priority = epoch_auxiliary and platform.env.getenvBool("ANTFLY_LIGHTONOCR3_QUEUED_PHASE_PRIORITY"),
                .vision_late_seal = epoch_auxiliary and metal_runtime.qualifiedM4DeviceDefault() and platform.env.getenvBool("ANTFLY_LIGHTONOCR3_VISION_LATE_SEAL"),
                .prefill_late_seal = epoch_auxiliary and metal_runtime.qualifiedM4DeviceDefault() and platform.env.getenvBool("ANTFLY_LIGHTONOCR3_PREFILL_LATE_SEAL"),
                .shared_decoder_scratch = metal_runtime.qualifiedM4DeviceDefault() and epoch_auxiliary and prepared and platform.env.getenvBool("ANTFLY_LIGHTONOCR3_SHARED_DECODER_SCRATCH"),
                .prepared_rows = if (physical_rows > 64) 128 else if (physical_rows > 32) 64 else if (physical_rows > 16) 32 else 16,
                .scheduling_rows = @intCast(physical_rows),
                .activation_pool = metal_runtime.lightOnThroughputFeatureEnabled("ANTFLY_LIGHTONOCR3_ACTIVATION_POOL"),
                .worker = metal_runtime.lightOnThroughputFeatureEnabled("ANTFLY_LIGHTONOCR3_EXECUTOR"),
                .epoch_auxiliary = epoch_auxiliary,
                .batch_attention = epoch_auxiliary and metal_runtime.lightOnThroughputFeatureEnabled("ANTFLY_LIGHTONOCR3_BATCH_ATTENTION"),
                .shared_core_weights = epoch_auxiliary and platform.env.getenvBool("ANTFLY_LIGHTONOCR3_SHARED_CORE_WEIGHTS"),
                .stream_first_layers = if (epoch_auxiliary and metal_runtime.lightOnThroughputFeatureEnabled("ANTFLY_LIGHTONOCR3_PREPARED_DECODE")) @intCast(@min(platform.env.getenvUsize("ANTFLY_LIGHTONOCR3_STREAM_FIRST_LAYERS") orelse 0, 16)) else 0,
                .single_frame = platform.env.getenvBool("ANTFLY_LIGHTONOCR3_SINGLE_FRAME"),
                .prefill_decode_streak = @intCast(@min(platform.env.getenvUsize("ANTFLY_LIGHTONOCR3_PREFILL_DECODE_STREAK") orelse 4, 16)),
            };
        }
        pub fn fromEnvironmentForModel(cfg: gpt_model_mod.Config) @This() {
            var features = fromEnvironment();
            features.resident_vision_budget = cfg.qwen35_metal_resident_vision;
            if (Hooks.cudaLightOnBatchProfileEnabled() and cfg.hasQwen35VisionArchitectureContract() and cfg.qwen35_device_only_state) {
                // CUDA shares the portable epoch, stage brokers and worker.
                // Metal encoder, prepared-tail and activation-pool flags
                // remain independently qualified backend features.
                features.worker = platform.env.getenvBoolDefault("ANTFLY_LIGHTONOCR3_EXECUTOR", true);
                // CUDA's portable packed path uses the same decode
                // workspace, without requiring a separate Metal preset.
                // Preserve an explicit prepared-workspace disable.
                if ((platform.env.getenvBool("ANTFLY_CUDA_ENABLE_LIGHTON_PACKED_MIXED") and
                    platform.env.getenvBool("ANTFLY_LIGHTONOCR3_PACKED_MIXED")) or platform.env.getenvBool("ANTFLY_LIGHTONOCR3_GPU_TOKEN_BURST"))
                    features.prepared = platform.env.getenvBoolDefault("ANTFLY_LIGHTONOCR3_PREPARED_DECODE", true);
                features.vision_batch_items = if (platform.env.getenvBool("ANTFLY_LIGHTONOCR3_VISION_BATCH")) @intCast(@min(@max(platform.env.getenvUsize("ANTFLY_LIGHTONOCR3_VISION_BATCH_ITEMS") orelse 4, 1), qwen3vl_projector.maxResidentVisionBatchItems(.cuda))) else 1;
                if (features.prefill_batch and platform.env.getenvBool("ANTFLY_LIGHTONOCR3_CUDA_WIDE_MICROBATCHES"))
                    features.prefill_batch_items = @intCast(@min(@max(platform.env.getenvUsize("ANTFLY_LIGHTONOCR3_PREFILL_BATCH_ITEMS") orelse 4, 1), generation.NativeGenerationPipeline.max_prepared_qwen_prefill_items));
                features.epoch_auxiliary = true;
                features.shared_decoder_scratch = true;
                features.cpu_preprocess = platform.env.getenvBoolDefault("ANTFLY_LIGHTONOCR3_CPU_PREPROCESS", true);
                features.queued_phase_priority = platform.env.getenvBool("ANTFLY_LIGHTONOCR3_QUEUED_PHASE_PRIORITY");
                features.vision_late_seal = platform.env.getenvBool("ANTFLY_LIGHTONOCR3_VISION_LATE_SEAL");
                features.prefill_late_seal = platform.env.getenvBool("ANTFLY_LIGHTONOCR3_PREFILL_LATE_SEAL");
            }
            features.shared_decoder_scratch = features.shared_decoder_scratch and cfg.qwen35_device_only_state;
            features.gpu_greedy_stream = platform.env.getenvBool("ANTFLY_LIGHTONOCR3_GPU_GREEDY_STREAM") and
                metal_runtime.qualifiedM4DeviceDefault() and cfg.hasQwen35VisionArchitectureContract() and
                cfg.qwen35_device_only_state and features.epoch_auxiliary and features.prepared and
                features.activation_pool and features.batch_attention and features.stream_first_layers != 0 and
                !features.single_frame;
            features.gpu_token_burst = platform.env.getenvBool("ANTFLY_LIGHTONOCR3_GPU_TOKEN_BURST") and
                metal_runtime.qualifiedM4DeviceDefault() and cfg.hasQwen35VisionArchitectureContract() and
                cfg.qwen35_device_only_state and features.shared_decoder_scratch and features.epoch_auxiliary and
                features.prepared and features.worker and features.activation_pool and features.batch_attention and
                features.single_frame and features.stream_first_layers == 0 and
                !platform.env.getenvBool("TERMITE_METAL_DISABLE_PIPELINED_DECODE_FRAME");
            if (Hooks.cudaLightOnBatchProfileEnabled() and cfg.hasQwen35VisionArchitectureContract() and cfg.qwen35_device_only_state) {
                features.gpu_token_burst = platform.env.getenvBool("ANTFLY_LIGHTONOCR3_GPU_TOKEN_BURST") and
                    features.shared_decoder_scratch and features.epoch_auxiliary and features.prepared and features.worker and
                    !features.gpu_greedy_stream and features.stream_first_layers == 0;
                if (features.gpu_token_burst) {
                    // A conservative second full F32 head plus all token
                    // IDs remains charged under the original byte caps.
                    features.cuda_burst_extra_bytes = @as(usize, cfg.vocab_size) * features.prepared_rows * @sizeOf(f32) +
                        generation.NativeGenerationPipeline.max_gpu_token_burst_width * 64 * @sizeOf(u32);
                }
            }
            features.gpu_token_burst_width = @intCast(std.math.clamp(platform.env.getenvUsize("ANTFLY_LIGHTONOCR3_GPU_TOKEN_BURST_WIDTH") orelse 2, 2, generation.NativeGenerationPipeline.max_gpu_token_burst_width));
            features.pipeline_vision = platform.env.getenvBool("ANTFLY_LIGHTONOCR3_PIPELINE_VISION") and
                metal_runtime.qualifiedM4DeviceDefault() and cfg.hasQwen35VisionArchitectureContract() and
                features.resident_vision_budget and features.cpu_preprocess and features.epoch_auxiliary and
                features.vision_batch_items == 1 and features.vision_frame_blocks <= 2 and
                !platform.env.getenvBool("TERMITE_METAL_DISABLE_PIPELINED_DECODE_FRAME") and
                !platform.env.getenvBool("TERMITE_METAL_DISABLE_QWEN35_VISION_FRAME") and
                !platform.env.getenvBool("TERMITE_QWEN3VL_PROFILE") and
                platform.env.getenv("ANTFLY_QWEN3VL_QUALIFICATION_TRACE_LAYER") == null;
            features.stream_chunks = features.gpu_greedy_stream and platform.env.getenvBool("ANTFLY_LIGHTONOCR3_STREAM_CHUNKS");
            features.vision_workspace_cache = features.epoch_auxiliary and metal_runtime.qualifiedM4DeviceDefault() and cfg.hasQwen35VisionArchitectureContract() and
                !platform.env.getenvBool("ANTFLY_LIGHTONOCR3_DISABLE_SHARED_VISION_PEAK") and platform.env.getenvBool("ANTFLY_LIGHTONOCR3_KEEP_VISION_WORKSPACE");
            features.shared_core_weights = features.epoch_auxiliary and platform.env.getenvBoolDefault("ANTFLY_LIGHTONOCR3_SHARED_CORE_WEIGHTS", metal_runtime.lightOnThroughputProfileEnabled() and cfg.hidden_size == 2560);
            if (metal_runtime.qualifiedM4DeviceDefault() and cfg.hasQwen35VisionArchitectureContract() and
                cfg.qwen35_device_only_state and features.shared_decoder_scratch and features.shared_core_weights and
                features.epoch_auxiliary and features.prepared and features.worker and features.activation_pool and
                features.prefill_batch_items == 1 and
                !platform.env.getenvBool("TERMITE_METAL_DISABLE_LIGHTONOCR3_PREFILL_PACK_REUSE") and
                !platform.env.getenvBool("TERMITE_METAL_ENABLE_CONCURRENT_PLANNED_DISPATCH") and
                !platform.env.getenvBool("TERMITE_METAL_DISABLE_PIPELINED_DECODE_FRAME"))
                features.prefill_stream_layers = @intCast(@min(platform.env.getenvUsize("ANTFLY_LIGHTONOCR3_PREFILL_STREAM_LAYERS") orelse 0, 16));
            features.packed_mixed = platform.env.getenvBool("ANTFLY_LIGHTONOCR3_PACKED_MIXED") and
                (metal_runtime.qualifiedM4DeviceDefault() or
                    (Hooks.cudaLightOnBatchProfileEnabled() and platform.env.getenvBool("ANTFLY_CUDA_ENABLE_LIGHTON_PACKED_MIXED"))) and cfg.hasQwen35VisionArchitectureContract() and
                cfg.qwen35_device_only_state and features.shared_decoder_scratch and features.prepared and
                features.worker and features.epoch_auxiliary and features.prefill_batch_items == 1 and
                !features.gpu_greedy_stream;
            features.prefill_activation_pool = platform.env.getenvBool("ANTFLY_LIGHTONOCR3_PREFILL_ACTIVATION_POOL") and
                metal_runtime.qualifiedM4DeviceDefault() and cfg.hasQwen35VisionArchitectureContract() and
                features.epoch_auxiliary and features.activation_pool and features.prepared and
                features.worker and cfg.qwen35_device_only_state and features.shared_decoder_scratch;
            if (features.packed_mixed) features.packed_prefill_items = @intCast(@min(@max(platform.env.getenvUsize("ANTFLY_LIGHTONOCR3_PACKED_PREFILL_ITEMS") orelse 1, 1), generation.NativeGenerationPipeline.default_prepared_qwen_prefill_items));
            if (features.packed_mixed) {
                const rows = platform.env.getenvUsize("ANTFLY_LIGHTONOCR3_PREFILL_QUERY_CHUNK_ROWS") orelse 0;
                features.prepared_prefill_chunk_rows = if (rows == 0) 0 else @intCast(@min(@max(rows, 64), 1024));
            }
            features.packed_shared_frame = platform.env.getenvBool("ANTFLY_LIGHTONOCR3_PACKED_SHARED_FRAME") and
                metal_runtime.qualifiedM4DeviceDefault() and features.packedSharedFrameEligible(cfg) and
                !platform.env.getenvBool("TERMITE_QWEN3VL_PROFILE") and
                platform.env.getenv("ANTFLY_QWEN3VL_QUALIFICATION_TRACE_LAYER") == null and
                !platform.env.getenvBool("TERMITE_METAL_ENABLE_CONCURRENT_PLANNED_DISPATCH");
            return features;
        }
        pub fn packedSharedFrameEligible(features: @This(), cfg: gpt_model_mod.Config) bool {
            return features.packed_mixed and features.single_frame and
                features.shared_decoder_scratch and cfg.qwen35_device_only_state and
                cfg.hasQwen35VisionArchitectureContract() and
                features.prefill_stream_layers == 0 and features.stream_first_layers == 0 and
                !features.prefill_activation_pool and !features.gpu_greedy_stream;
        }
        pub fn visionRetainedBlocks(features: @This()) usize {
            return @as(usize, features.vision_frame_blocks) * (if (features.pipeline_vision) @as(usize, 2) else 1);
        }
        pub fn auxiliaryAmounts(features: @This()) runtime.tier.memory.AdmissionAmounts {
            var amounts = runtime.tier.memory.AdmissionAmounts{};
            // The same conservative envelopes as page admission. Only
            // their owner changes: these allocations exist once per epoch.
            if (features.prepared) {
                amounts.backend_scratch_bytes += (if (features.prepared_rows > 64) @as(usize, 48) else if (features.prepared_rows > 32) @as(usize, 24) else if (features.prepared_rows > 16) @as(usize, 12) else 8) * 1024 * 1024;
                amounts.host_scratch_bytes += 1024 * 1024;
            }
            if (features.activation_pool) {
                amounts.backend_scratch_bytes += (if (features.prefill_activation_pool) @as(usize, 64) else 8) * 1024 * 1024;
                amounts.host_scratch_bytes += 1024 * 1024;
            }
            if (features.worker) amounts.host_scratch_bytes += 32 * 1024 * 1024;
            if (features.shared_core_weights) {
                amounts.backend_scratch_bytes += 8 * 1024 * 1024;
                amounts.host_scratch_bytes += 1024 * 1024;
            }
            if (features.batch_attention) {
                // Two 9 MiB banks permit one submitted prefix plus one
                // active suffix without reusing in-flight attention scratch.
                amounts.backend_scratch_bytes += 18 * 1024 * 1024;
                amounts.host_scratch_bytes += 1024 * 1024;
            }
            if (features.stream_first_layers != 0 or features.gpu_token_burst) {
                // Bound a prefix in flight while the suffix is encoded.
                amounts.backend_scratch_bytes += (if (features.prepared_rows > 64) @as(usize, 48) else if (features.prepared_rows > 32) @as(usize, 24) else if (features.prepared_rows > 16) @as(usize, 12) else 8) * 1024 * 1024;
                amounts.host_scratch_bytes += 1024 * 1024;
            }
            if (features.gpu_token_burst and features.gpu_token_burst_width > 2) amounts.backend_scratch_bytes += 4 * 1024 * 1024;
            amounts.backend_scratch_bytes += features.cuda_burst_extra_bytes;
            return amounts;
        }
        pub fn needsAuxiliary(features: @This()) bool {
            return features.epoch_auxiliary and (features.prepared or features.activation_pool or features.worker or features.shared_core_weights or features.stream_first_layers != 0 or features.batch_attention);
        }
    };
}
