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
const build_options = @import("build_options");
const model_manager_mod = @import("model_manager.zig");
const gpt_model_mod = @import("../models/gpt.zig");
const runtime = @import("../runtime/root.zig");
const ops = @import("../ops/ops.zig");
const reader_execution_pool = @import("reader_execution_pool.zig");
const execution_control_mod = @import("../execution_control.zig");
const executor_microbatch = @import("executor_microbatch.zig");
const generation = @import("../pipelines/generation.zig");
const session_factory = @import("../architectures/session_factory.zig");
const metal_runtime = @import("../backends/metal_runtime.zig");
const qwen3vl_projector = @import("../architectures/qwen3vl_projector.zig");

const InferenceExecutionControl = execution_control_mod.InferenceExecutionControl;
var lighton_stream_execution_id: std.atomic.Value(u64) = .init(1);

/// Owns a reader epoch, its worker, page controls and all shared leases.
pub fn Epoch(comptime Node: type, comptime Hooks: type) type {
    return struct {
        const LightOnStreamContext = @This();
        allocator: std.mem.Allocator,
        model: *model_manager_mod.LoadedModel,
        cfg: gpt_model_mod.Config,
        features: ExecutionFeatures = .{},
        auxiliary_lease: ?model_manager_mod.GenerationAdmissionLease = null,
        auxiliary_ready: std.atomic.Value(bool) = .init(false),
        auxiliary_acquisitions: usize = 0,
        budget: runtime.tier.memory.RunBudget,
        lane: execution_control_mod.ExecutionLane.Lease,
        coordinator: runtime.scheduler.native_generate.NativeGenerateCoordinator,
        manager: runtime.kv.manager.KvManager,
        storage: runtime.kv.storage_runtime.KvStorageRuntime = undefined,
        pool: runtime.kv.block.KvPoolId = undefined,
        cb: ?ops.ComputeBackend = null,
        execution_gate_wait_ns: u64 = 0,
        phase_wait_ns: u64 = 0,
        phase_fair: bool = false,
        phase_waiters: std.atomic.Value(usize) = .init(0),
        phase_priority_yields: usize = 0,
        published_vision_peak: std.atomic.Value(usize) = .init(0),
        published_vision_host_peak: std.atomic.Value(usize) = .init(0),
        published_decoder_peak: std.atomic.Value(usize) = .init(0),
        step_mutex: std.atomic.Mutex = .unlocked,
        step_gate: execution_control_mod.ExecutionLane = .{},
        gate_events: bool = false,
        cpu_preprocess_items: usize = 0,
        cpu_preprocess_ns: u64 = 0,
        kv_mutex: std.atomic.Mutex = .unlocked,
        controls_mutex: std.atomic.Mutex = .unlocked,
        controls: [reader_execution_pool.Pool.max_capacity]?InferenceExecutionControl = @splat(null),
        registered_pages: usize = 0,
        registered_window_peak: usize = 0,
        execution_id: u64,
        completed: usize = 0,
        recurrent_cache_pages: usize = 0,
        host_recurrent_bytes_total: usize = 0,
        vision_broker: executor_microbatch.Broker,
        prefill_broker: executor_microbatch.Broker,
        vision_seal_mutex: std.Io.Mutex = .init,
        prefill_seal_mutex: std.Io.Mutex = .init,
        prefill_await_groups: usize = 0,
        prefill_await_events: bool = false,
        prefill_host_scratch_acquisitions: usize = 0,
        prefill_host_scratch_releases: usize = 0,
        prefill_host_scratch_peak_bytes: usize = 0,
        vision_peak: reader_execution_pool.SharedPeak(model_manager_mod.GenerationAdmissionLease) = .{},
        vision_host_peak: reader_execution_pool.SharedPeak(model_manager_mod.GenerationAdmissionLease) = .{},
        decoder_peak: reader_execution_pool.SharedPeak(model_manager_mod.GenerationAdmissionLease) = .{},
        worker: ?std.Thread = null,
        worker_io: ?std.Io.Threaded = null,
        worker_wake: std.Io.Event = .unset,
        worker_stop: std.atomic.Value(bool) = .init(false),
        worker_control: InferenceExecutionControl = .{},
        worker_kv_dtype: runtime.kv.pool.KvDType = undefined,
        worker_steps: usize = 0,
        worker_tail_steps: usize = 0,

        pub const ExecutionFeatures = @import("reader_execution_config.zig").Features(Hooks);
        pub const Factory = struct {
            node: *Node,
            model: *model_manager_mod.LoadedModel,
            cfg: gpt_model_mod.Config,
            limits: runtime.tier.memory.Limits,
            physical_rows_limit: usize,
            epoch_page_limit: usize = 64,
            decoder_peak: usize = 0,
            features: ExecutionFeatures = .{},
            control: InferenceExecutionControl,
        };
        pub fn create(raw: *anyopaque) !*anyopaque {
            const factory: *Factory = @ptrCast(@alignCast(raw));
            const lane = try factory.model.native_reader_execution_lane.acquire(factory.control);
            errdefer lane.release();
            try factory.control.lock(factory.model.nativeGenerationMutex());
            errdefer factory.model.unlockNativeGeneration();
            const a = std.heap.smp_allocator;
            const ctx = try a.create(LightOnStreamContext);
            ctx.* = .{ .allocator = a, .model = factory.model, .cfg = factory.cfg, .features = factory.features, .auxiliary_ready = .init(!factory.features.needsAuxiliary()), .budget = runtime.tier.memory.RunBudget.init(factory.limits), .lane = lane, .coordinator = runtime.scheduler.native_generate.NativeGenerateCoordinator.init(a), .manager = runtime.kv.manager.KvManager.init(a), .vision_peak = .{ .max_increments = factory.epoch_page_limit }, .vision_host_peak = .{ .max_increments = factory.epoch_page_limit }, .decoder_peak = .{ .max_increments = factory.epoch_page_limit }, .vision_broker = executor_microbatch.Broker.init(a), .prefill_broker = executor_microbatch.Broker.init(a), .execution_id = lighton_stream_execution_id.fetchAdd(1, .monotonic) };
            if (factory.features.vision_late_seal) ctx.vision_broker.execution_mutex = &ctx.vision_seal_mutex;
            if (factory.features.prefill_late_seal) ctx.prefill_broker.execution_mutex = &ctx.prefill_seal_mutex;
            ctx.coordinator.setPolicy(factory.node.lightOnOcrPolicy(factory.physical_rows_limit));
            if (factory.features.packed_mixed) {
                ctx.coordinator.policy.max_step_prefill_items = factory.features.packed_prefill_items;
                ctx.coordinator.policy.max_step_query_tokens = generation.NativeGenerationPipeline.max_prepared_qwen_prefill_rows;
            }
            ctx.execution_gate_wait_ns = if (platform.env.getenvBool("ANTFLY_LIGHTONOCR3_DISABLE_GATE_PARK")) 0 else @as(u64, @min(ctx.coordinator.policy.contention_wait_us, 1000)) * std.time.ns_per_us;
            ctx.phase_wait_ns = if ((Hooks.cudaLightOnBatchProfileEnabled() and platform.env.getenvBool("ANTFLY_LIGHTONOCR3_PHASE_PARK")) or metal_runtime.lightOnThroughputFeatureEnabled("ANTFLY_LIGHTONOCR3_PHASE_PARK")) ctx.execution_gate_wait_ns else 0;
            ctx.phase_fair = (Hooks.cudaLightOnBatchProfileEnabled() and platform.env.getenvBool("ANTFLY_LIGHTONOCR3_PHASE_FAIR")) or metal_runtime.lightOnThroughputFeatureEnabled("ANTFLY_LIGHTONOCR3_PHASE_FAIR");
            ctx.gate_events = ctx.coordinator.usesStepEvents() and platform.env.getenvBoolDefault("ANTFLY_LIGHTONOCR3_GATE_EVENTS", true);
            // Keep the latency-oriented default; bulk-throughput profiles can
            // prioritize runnable prefills to fill the physical decode batch.
            ctx.coordinator.max_decode_streak_before_prefill = factory.features.prefill_decode_streak;
            return ctx;
        }
        pub fn destroy(raw: *anyopaque) void {
            const ctx: *LightOnStreamContext = @ptrCast(@alignCast(raw));
            std.debug.assert(ctx.registered_pages == 0);
            // Published results can let the last caller retire while the
            // executor is still freeing its own output. Join before the CB,
            // coordinator, model lane, or worker-owned Io can be destroyed.
            if (ctx.worker) |worker| {
                ctx.worker_stop.store(true, .release);
                ctx.worker_wake.set(ctx.worker_io.?.io());
                worker.join();
                ctx.coordinator.unregisterExecutor();
                ctx.worker_io.?.deinit();
                ctx.worker_io = null;
            }
            if (platform.env.getenvBool("ANTFLY_BENCH_LIGHTONOCR3")) {
                std.debug.print("LIGHTONOCR3 device_recurrent_state={d} recurrent_cache_pages={d} host_recurrent_bytes_total={d}\n", .{ @intFromBool(ctx.cfg.qwen35_device_only_state), ctx.recurrent_cache_pages, ctx.host_recurrent_bytes_total });
                std.debug.print("LIGHTONOCR3 prefill_await_groups={d} prefill_await_events={d} prefill_decode_streak={d}\n", .{ ctx.prefill_await_groups, @intFromBool(ctx.prefill_await_events), ctx.features.prefill_decode_streak });
                std.debug.print("LIGHTONOCR3 cpu_preprocess={d} cpu_preprocess_items={d} cpu_preprocess_ns={d} vision_batch_limit={d}\n", .{ @intFromBool(ctx.features.cpu_preprocess), ctx.cpu_preprocess_items, ctx.cpu_preprocess_ns, ctx.features.vision_batch_items });
                std.debug.print("LIGHTONOCR3 gate_events={d} gate_event_acquisitions={d} gate_event_waits={d}\n", .{ @intFromBool(ctx.gate_events), ctx.step_gate.acquisitions, ctx.step_gate.event_waits });
                std.debug.print("LIGHTONOCR3 executor_steps={d}\n", .{ctx.worker_steps});
                std.debug.print("LIGHTONOCR3 executor_tail_steps={d}\n", .{ctx.worker_tail_steps});
                std.debug.print("LIGHTONOCR3 phase_priority_yields={d} phase_fair={d}\n", .{ ctx.phase_priority_yields, @intFromBool(ctx.phase_fair) });
                const auxiliary = ctx.features.auxiliaryAmounts();
                std.debug.print("LIGHTONOCR3 epoch_auxiliary={d} auxiliary_acquisitions={d} auxiliary_host_scratch={d} auxiliary_backend_scratch={d}\n", .{ @intFromBool(ctx.features.epoch_auxiliary), ctx.auxiliary_acquisitions, auxiliary.host_scratch_bytes, auxiliary.backend_scratch_bytes });
                std.debug.print("LIGHTONOCR3 phase_gate_wait_ns={d}\n", .{ctx.phase_wait_ns});
                const s = ctx.coordinator.stats;
                if (ctx.cb) |cb| {
                    const t = cb.debugTimingSnapshot().provider;
                    const pool_stats = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnActivationPoolStats(&cb) else @as([4]u64, @splat(0));
                    std.debug.print("LIGHTONOCR3 activation_pool_hits={d}\nLIGHTONOCR3 activation_pool_peak_bytes={d}\nLIGHTONOCR3 activation_pool_duplicate_retirements={d}\n", .{ pool_stats[0], pool_stats[1], pool_stats[3] });
                    const prefill_pool_stats = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnPrefillActivationPoolStats(&cb) else @as([2]u64, @splat(0));
                    std.debug.print("LIGHTONOCR3 prefill_activation_pool={d} prefill_activation_pool_hits={d} prefill_activation_pool_hit_bytes={d}\n", .{ @intFromBool(ctx.features.prefill_activation_pool), prefill_pool_stats[0], prefill_pool_stats[1] });
                    const prepared_blocks = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnPreparedBlockCount(&cb) else 0;
                    std.debug.print("LIGHTONOCR3 prepared_blocks={d}\n", .{prepared_blocks});
                    const wide_tiles = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnWideProjectionTileCount(&cb) else 0;
                    std.debug.print("LIGHTONOCR3 wide_projection_tiles={d}\n", .{wide_tiles});
                    const prefill_pack = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnPrefillPackStats(&cb) else @as([2]u64, @splat(0));
                    std.debug.print("LIGHTONOCR3 prefill_pack_calls={d} prefill_pack_reuse_hits={d}\n", .{ prefill_pack[0], prefill_pack[1] });
                    const decode_pack = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnDecodePackStats(&cb) else @as([2]u64, @splat(0));
                    std.debug.print("LIGHTONOCR3 decode_pack_calls={d} decode_pack_reuse_hits={d}\n", .{ decode_pack[0], decode_pack[1] });
                    const vision_pack = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnVisionPackStats(&cb) else @as([2]u64, @splat(0));
                    std.debug.print("LIGHTONOCR3 vision_pack_calls={d} vision_pack_reuse_hits={d}\n", .{ vision_pack[0], vision_pack[1] });
                    const mpp_encoder = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnMppEncoderStats(&cb) else @as([2]u64, @splat(0));
                    std.debug.print("LIGHTONOCR3 mpp_planned_calls={d} mpp_dedicated_calls={d}\n", .{ mpp_encoder[0], mpp_encoder[1] });
                    const attention_batch = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnBatchAttentionStats(&cb) else @as([3]u64, @splat(0));
                    std.debug.print("LIGHTONOCR3 attention_batch_calls={d} attention_batch_rows={d}\n", .{ attention_batch[0], attention_batch[1] });
                    std.debug.print("LIGHTONOCR3 attention_batch_kernel_calls={d}\n", .{attention_batch[2]});
                    const stream_splits = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnStreamSplits(&cb) else 0;
                    std.debug.print("LIGHTONOCR3 stream_forward_splits={d} stream_first_layers={d}\n", .{ stream_splits, ctx.features.stream_first_layers });
                    const prefill_splits = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnPrefillStreamSplits(&cb) else 0;
                    std.debug.print("LIGHTONOCR3 prefill_stream_layers={d} prefill_stream_splits={d}\n", .{ ctx.features.prefill_stream_layers, prefill_splits });
                    const prefill_ffn_calls = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnPrefillFfnCalls(&cb) else 0;
                    std.debug.print("LIGHTONOCR3 prefill_fused_ffn_calls={d}\n", .{prefill_ffn_calls});
                    const core_weights = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnSharedCoreWeightStats(&cb) else @as([3]usize, @splat(0));
                    std.debug.print("LIGHTONOCR3 shared_core_weight_hits={d} shared_core_weight_misses={d} shared_core_weight_bytes={d}\n", .{ core_weights[0], core_weights[1], core_weights[2] });
                    const wide_mpp = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnWideMppStats(&cb) else @as([2]u64, @splat(0));
                    std.debug.print("LIGHTONOCR3 wide_mpp_projections={d}\nLIGHTONOCR3 wide_mpp_heads={d}\n", .{ wide_mpp[0], wide_mpp[1] });
                    const wide_gate_up = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnWideGateUpCalls(&cb) else 0;
                    std.debug.print("LIGHTONOCR3 wide_gate_up_calls={d}\n", .{wide_gate_up});
                    const full64_gate_up = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnFull64GateUpCalls(&cb) else 0;
                    std.debug.print("LIGHTONOCR3 full64_gate_up_calls={d}\n", .{full64_gate_up});
                    const short_prefill = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnShortPrefillStats(&cb) else @as([2]u64, @splat(0));
                    std.debug.print("LIGHTONOCR3 short_prefill_core_dispatches={d} short_direct_attention_dispatches={d}\n", .{ short_prefill[0], short_prefill[1] });
                    const wide_recurrent = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnWideRecurrentStats(&cb) else @as([3]u64, @splat(0));
                    std.debug.print("LIGHTONOCR3 wide_recurrent_dispatches={d} wide_recurrent_lifetime_max_rows={d} wide_recurrent_staged_bytes={d}\n", .{ wide_recurrent[0], wide_recurrent[1], wide_recurrent[2] });
                    const batch_argmax = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnBatchedArgmaxStats(&cb) else @as([2]u64, @splat(0));
                    std.debug.print("LIGHTONOCR3 batch_argmax_calls={d}\nLIGHTONOCR3 batch_argmax_rows={d}\n", .{ batch_argmax[0], batch_argmax[1] });
                    const vision_workspace = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnVisionWorkspaceStats(&cb) else @as([3]u64, @splat(0));
                    const large_vision_pack = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnLargeVisionPackStats(&cb) else @as([2]u64, @splat(0));
                    std.debug.print("LIGHTONOCR3 vision_workspace_cache={d}\nLIGHTONOCR3 vision_workspace_allocations={d}\nLIGHTONOCR3 vision_workspace_reuses={d}\nLIGHTONOCR3 vision_workspace_bytes={d}\n", .{ @intFromBool(ctx.features.vision_workspace_cache), vision_workspace[0], vision_workspace[1], vision_workspace[2] });
                    std.debug.print("LIGHTONOCR3 large_vision_pack_calls={d}\nLIGHTONOCR3 large_vision_pack_reuse_hits={d}\n", .{ large_vision_pack[0], large_vision_pack[1] });
                    std.debug.print("LIGHTONOCR3 resident_vision_budget={d}\n", .{@intFromBool(ctx.features.resident_vision_budget)});
                    std.debug.print("LIGHTONOCR3 vision_late_seal={d}\nLIGHTONOCR3 prefill_late_seal={d}\nLIGHTONOCR3 queued_phase_priority={d}\n", .{ @intFromBool(ctx.features.vision_late_seal), @intFromBool(ctx.features.prefill_late_seal), @intFromBool(ctx.features.queued_phase_priority) });
                    const standalone_heads = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnStandaloneWideHeads(&cb) else 0;
                    std.debug.print("LIGHTONOCR3 standalone_wide_heads={d}\n", .{standalone_heads});
                    var burst = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnBurstStats(&cb) else @as([5]u64, @splat(0));
                    if (comptime build_options.enable_cuda) if (cb.kind() == .cuda) {
                        burst = @import("../ops/cuda/cuda_compute.zig").CudaCompute.lightOnCudaBurstStats(&cb);
                    };
                    std.debug.print("LIGHTONOCR3 gpu_token_burst_enabled={d} gpu_token_burst_steps={d} gpu_token_burst_topology_mismatches={d}\n", .{ @intFromBool(ctx.features.gpu_token_burst), burst[0], burst[1] });
                    std.debug.print("LIGHTONOCR3 gpu_token_burst_extra_steps={d} gpu_token_burst_extra_rows={d} gpu_token_burst_max_width={d}\n", .{ burst[2], burst[3], burst[4] });
                    const single_frame = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnSingleFrameStats(&cb) else @as([2]u64, @splat(0));
                    std.debug.print("LIGHTONOCR3 single_frame_steps={d} single_frame_topology_mismatches={d}\n", .{ single_frame[0], single_frame[1] });
                    const greedy_stream = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnGpuGreedyStreamStats(&cb) else @as([3]u64, @splat(0));
                    std.debug.print("LIGHTONOCR3 gpu_greedy_stream_steps={d}\nLIGHTONOCR3 gpu_greedy_stream_two_frame_steps={d}\nLIGHTONOCR3 gpu_greedy_stream_topology_mismatches={d}\n", .{ greedy_stream[0], greedy_stream[1], greedy_stream[2] });
                    const stream_frames: [2]u64 = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnStreamFrameStats(&cb) else @splat(0);
                    std.debug.print("LIGHTONOCR3 gpu_greedy_stream_multi_frame_steps={d}\nLIGHTONOCR3 gpu_greedy_stream_frame_submits={d}\n", .{ stream_frames[0], stream_frames[1] });
                    const token_scopes = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnTokenEncoderScopeSteps(&cb) else 0;
                    std.debug.print("LIGHTONOCR3 token_encoder_scope_steps={d}\n", .{token_scopes});
                    const encoder_scopes = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.ocrEncoderScopeOpens(&cb) else @as([3]u64, @splat(0));
                    std.debug.print("LIGHTONOCR3 vision_encoder_scope_opens={d} prefill_encoder_scope_opens={d}\n", .{ encoder_scopes[0], encoder_scopes[1] });
                    const snapshots = if (comptime build_options.enable_metal) @import("../ops/metal_compute.zig").MetalCompute.lightOnFramePolicySnapshots(&cb) else 0;
                    std.debug.print("LIGHTONOCR3 frame_policy_snapshots={d}\n", .{snapshots});
                    std.debug.print("LIGHTONOCR3 runtime_frames={d} frame_gpu_ns={d} frame_wait_ns={d}\n", .{ t.decoder_runtime_frame_submits, t.decoder_runtime_frame_gpu_nanos, t.decoder_runtime_frame_wait_nanos });
                }
                std.debug.print("LIGHTONOCR3 execution_gate_wait_ns={d}\n", .{ctx.execution_gate_wait_ns});
                std.debug.print("LIGHTONOCR3 kv_storage_layers={d} model_layers={d}\n", .{ ctx.cfg.kvStorageLayerCount(), ctx.cfg.num_hidden_layers });
                std.debug.print("LIGHTONOCR3 registered_window_peak={d} physical_rows_limit={d}\n", .{ ctx.registered_window_peak, ctx.coordinator.policy.max_step_items });
                std.debug.print("LIGHTONOCR3 cohort_items={d} decode_steps={d} batched_steps={d} singleton_steps={d} decode_items={d}\n", .{ ctx.completed, s.step_batches_total, s.step_batch_size_2_total + s.step_batch_size_3_4_total + s.step_batch_size_5_8_total, s.step_singleton_batches_total, s.step_decode_items_total });
                std.debug.print("LIGHTONOCR3 turn_coalescing={d} coalesce_turn_boundaries={d} coalesce_rebased_items={d} coalesce_waits={d} coalesce_wait_us={d} kv_block_skips={d}\n", .{ @intFromBool(ctx.coordinator.policy.decode_coalesce_after_turn), s.decode_coalesce_turn_boundaries_total, s.decode_coalesce_rebased_items_total, s.decode_coalesce_waits_total, s.decode_coalesce_wait_us_total, s.step_kv_block_skips_total });
            }
            ctx.vision_broker.deinit();
            ctx.prefill_broker.deinit();
            if (ctx.cb != null) ctx.storage.deinit();
            ctx.manager.deinit();
            if (ctx.cb) |*cb| cb.deinit();
            // The worker has joined and the backend has released plans and
            // cached activations. Admission waiters may have kept this epoch
            // alive after its last admitted page retired; retain its own lease
            // through that interval, independently of every page's lease.
            if (ctx.auxiliary_lease) |*auxiliary| {
                auxiliary.release();
                ctx.auxiliary_lease = null;
                if (platform.env.getenvBool("ANTFLY_BENCH_LIGHTONOCR3")) std.debug.print("LIGHTONOCR3 auxiliary_release_after_backend=1\n", .{});
            }
            ctx.coordinator.deinit();
            if (platform.env.getenvBool("ANTFLY_BENCH_LIGHTONOCR3")) std.debug.print("LIGHTONOCR3 shared_vision_peak_bytes={d} peak_segments={d}\n", .{ ctx.vision_peak.bytes, ctx.vision_peak.count });
            if (platform.env.getenvBool("ANTFLY_BENCH_LIGHTONOCR3")) std.debug.print("LIGHTONOCR3 shared_vision_host_peak_bytes={d} peak_segments={d}\n", .{ ctx.vision_host_peak.bytes, ctx.vision_host_peak.count });
            ctx.vision_peak.deinit();
            ctx.vision_host_peak.deinit();
            const decoder_bytes = ctx.decoder_peak.bytes;
            if (platform.env.getenvBool("ANTFLY_BENCH_LIGHTONOCR3")) std.debug.print("LIGHTONOCR3 shared_decoder_scratch={d} shared_decoder_peak_bytes={d} peak_segments={d}\n", .{ @intFromBool(ctx.features.shared_decoder_scratch), decoder_bytes, ctx.decoder_peak.count });
            if (platform.env.getenvBool("ANTFLY_BENCH_LIGHTONOCR3")) std.debug.print("LIGHTONOCR3 prefill_host_scratch_acquisitions={d} prefill_host_scratch_releases={d} prefill_host_scratch_peak_bytes={d}\n", .{ ctx.prefill_host_scratch_acquisitions, ctx.prefill_host_scratch_releases, ctx.prefill_host_scratch_peak_bytes });
            // The worker joined, frames drained and backend caches retired above.
            ctx.decoder_peak.deinit();
            if (decoder_bytes != 0 and platform.env.getenvBool("ANTFLY_BENCH_LIGHTONOCR3")) std.debug.print("LIGHTONOCR3 decoder_scratch_release_after_backend=1\n", .{});
            ctx.model.unlockNativeGeneration();
            ctx.lane.release();
            ctx.allocator.destroy(ctx);
        }
        pub fn acquireVisionPeak(factory: *Factory, bytes: usize) !model_manager_mod.GenerationAdmissionLease {
            return factory.node.model_manager.acquireGenerationResources(&.{.{ .session = factory.model.session, .resources = .{ .backend_class = .gpu, .limits = factory.limits, .amounts = .{ .backend_scratch_bytes = bytes } } }});
        }
        pub fn acquireVisionHostPeak(factory: *Factory, bytes: usize) !model_manager_mod.GenerationAdmissionLease {
            return factory.node.model_manager.acquireGenerationResources(&.{.{ .session = factory.model.session, .resources = .{ .backend_class = .gpu, .limits = factory.limits, .amounts = .{ .host_scratch_bytes = bytes } } }});
        }
        pub fn finishPhaseWait(ctx: *LightOnStreamContext) void {
            if (ctx.phase_waiters.fetchSub(1, .acq_rel) == 1) if (ctx.worker_io) |*owner| {
                ctx.worker_wake.set(owner.io());
            };
        }
        // Keep admitted, mergeable broker work visible to the decode worker.
        // A prefill that requires decode progress must never park that worker.
        pub fn beginQueuedPhase(ctx: *LightOnStreamContext, vision: bool) bool {
            const enabled = ctx.phase_fair and ctx.features.queued_phase_priority and (if (vision)
                ctx.features.vision_late_seal
            else
                ctx.features.prefill_late_seal and ctx.features.prefill_decode_streak == 0);
            if (enabled) _ = ctx.phase_waiters.fetchAdd(1, .acq_rel);
            return enabled;
        }
        pub fn lockStep(ctx: *LightOnStreamContext, control: InferenceExecutionControl) !void {
            if (ctx.phase_fair) _ = ctx.phase_waiters.fetchAdd(1, .acq_rel);
            defer if (ctx.phase_fair) {
                ctx.finishPhaseWait();
            };
            const queued = if (ctx.gate_events) try ctx.step_gate.acquireWithPriority(control, true) else null;
            errdefer if (queued) |lease| lease.release();
            try control.lockWithPoll(&ctx.step_mutex, ctx.phase_wait_ns);
            ctx.invalidateCudaDecodeGraph();
        }
        fn invalidateCudaDecodeGraph(ctx: *LightOnStreamContext) void {
            // Vision and other foreground phases may resize shared CUDA
            // scratch. Retire captured addresses before that phase executes.
            if (comptime build_options.enable_cuda) if (ctx.cb) |*cb|
                @import("../ops/cuda/cuda_compute.zig").CudaCompute.invalidateLightOnDecodeGraph(cb);
        }
        pub fn lockStepForCleanup(ctx: *LightOnStreamContext) void {
            if (ctx.phase_fair) _ = ctx.phase_waiters.fetchAdd(1, .acq_rel);
            defer if (ctx.phase_fair) {
                ctx.finishPhaseWait();
            };
            if (ctx.gate_events) {
                _ = ctx.step_gate.acquireForCleanup(if (ctx.worker_io) |*owner| owner.io() else null, true);
            }
            if (ctx.phase_wait_ns == 0) {
                platform.sync.lockYielding(&ctx.step_mutex);
            } else {
                // Cleanup retains stack-backed state until its owner releases
                // the backend; an empty control cannot cancel acquisition.
                (InferenceExecutionControl{}).lockWithPoll(&ctx.step_mutex, ctx.phase_wait_ns) catch unreachable;
            }
            ctx.invalidateCudaDecodeGraph();
        }
        pub fn unlockStep(ctx: *LightOnStreamContext) void {
            ctx.step_mutex.unlock();
            if (ctx.gate_events) (execution_control_mod.ExecutionLane.Lease{ .lane = &ctx.step_gate }).release();
        }
        pub fn admitPage(ctx: *LightOnStreamContext, factory: *Factory, peak: ?usize, host_peak: usize, amounts: runtime.tier.memory.AdmissionAmounts) !model_manager_mod.GenerationAdmissionLease {
            // Resource-bearing choices are immutable for a shared backend.
            if (!std.meta.eql(ctx.features, factory.features) or ctx.cfg.compact_qwen35_kv != factory.cfg.compact_qwen35_kv or ctx.cfg.qwen35_device_only_state != factory.cfg.qwen35_device_only_state) return error.InvalidBatchDecodeState;
            const needs_auxiliary = !ctx.auxiliary_ready.load(.acquire);
            const needs_peak = if (peak) |bytes| ctx.phase_wait_ns == 0 or bytes > ctx.published_vision_peak.load(.acquire) else false;
            const needs_host_peak = host_peak > 0 and (ctx.phase_wait_ns == 0 or host_peak > ctx.published_vision_host_peak.load(.acquire));
            const needs_decoder_peak = factory.decoder_peak != 0 and (ctx.phase_wait_ns == 0 or factory.decoder_peak > ctx.published_decoder_peak.load(.acquire));
            if (needs_auxiliary or needs_peak or needs_host_peak or needs_decoder_peak) {
                try ctx.lockStep(factory.control);
                defer ctx.unlockStep();
                if (!ctx.auxiliary_ready.load(.acquire)) {
                    ctx.auxiliary_lease = try factory.node.model_manager.acquireGenerationResources(&.{.{ .session = factory.model.session, .resources = .{ .backend_class = .gpu, .limits = factory.limits, .amounts = ctx.features.auxiliaryAmounts() } }});
                    ctx.auxiliary_acquisitions += 1;
                    ctx.auxiliary_ready.store(true, .release);
                }
                if (peak) |bytes| {
                    try ctx.vision_peak.grow(bytes, factory, acquireVisionPeak);
                    if (ctx.phase_wait_ns != 0) ctx.published_vision_peak.store(ctx.vision_peak.bytes, .release);
                }
                if (factory.decoder_peak != 0) {
                    try ctx.decoder_peak.grow(factory.decoder_peak, factory, acquireVisionPeak);
                    if (ctx.phase_wait_ns != 0) ctx.published_decoder_peak.store(ctx.decoder_peak.bytes, .release);
                }
                if (host_peak != 0) {
                    try ctx.vision_host_peak.grow(host_peak, factory, acquireVisionHostPeak);
                    if (ctx.phase_wait_ns != 0) ctx.published_vision_host_peak.store(ctx.vision_host_peak.bytes, .release);
                }
            }
            return factory.node.model_manager.acquireGenerationResources(&.{.{ .session = factory.model.session, .resources = .{ .backend_class = .gpu, .limits = factory.limits, .amounts = amounts } }});
        }

        pub fn register(ctx: *LightOnStreamContext, control: InferenceExecutionControl) usize {
            platform.sync.lockYielding(&ctx.controls_mutex);
            defer ctx.controls_mutex.unlock();
            for (&ctx.controls, 0..) |*slot, index| if (slot.* == null) {
                slot.* = control;
                ctx.registered_pages += 1;
                ctx.registered_window_peak = @max(ctx.registered_window_peak, ctx.registered_pages);
                return index;
            };
            unreachable; // Pool capacity and the control table share this bound.
        }
        pub fn unregister(ctx: *LightOnStreamContext, index: usize) void {
            platform.sync.lockYielding(&ctx.controls_mutex);
            defer ctx.controls_mutex.unlock();
            std.debug.assert(ctx.controls[index] != null and ctx.registered_pages > 0);
            ctx.controls[index] = null;
            ctx.registered_pages -= 1;
        }
        pub fn check(raw: ?*anyopaque) !void {
            const ctx: *LightOnStreamContext = @ptrCast(@alignCast(raw.?));
            platform.sync.lockYielding(&ctx.controls_mutex);
            defer ctx.controls_mutex.unlock();
            var timeout = false;
            for (ctx.controls) |slot| if (slot) |control| {
                control.check() catch |err| {
                    timeout = timeout or err == error.Timeout or err == error.DeadlineExceeded;
                    continue;
                };
                return;
            };
            return if (timeout) error.Timeout else error.Cancelled;
        }
        /// Called under step_mutex before this epoch publishes its first page.
        pub fn startExecutor(ctx: *LightOnStreamContext, node: *Node, kv_dtype: runtime.kv.pool.KvDType) !void {
            if (ctx.worker != null or !ctx.features.worker) return;
            ctx.worker_io = std.Io.Threaded.init(ctx.allocator, .{});
            errdefer {
                ctx.worker_io.?.deinit();
                ctx.worker_io = null;
            }
            ctx.worker_control = node.bindExecutionControl(ctx.worker_io.?.io(), .{ .ptr = ctx, .check_fn = check });
            ctx.worker_kv_dtype = kv_dtype;
            ctx.coordinator.registerExecutor(&ctx.worker_wake, ctx.worker_io.?.io());
            errdefer ctx.coordinator.unregisterExecutor();
            ctx.worker = try std.Thread.spawn(.{}, runExecutor, .{ctx});
        }

        pub fn runExecutor(ctx: *LightOnStreamContext) void {
            const io = ctx.worker_io.?.io();
            // This pipeline borrows only epoch-owned objects. Request-local
            // pipelines may disappear immediately after result publication.
            var pipeline = generation.NativeGenerationPipeline{
                .allocator = ctx.allocator,
                .io = io,
                .cb = ctx.cb.?,
                .session = ctx.model.session,
                .gpt_config = ctx.cfg,
                .kv_dtype = ctx.worker_kv_dtype,
                .tokenizer = ctx.model.getTokenizer(),
                .execution_lock = &ctx.step_mutex,
                .execution_gate = if (ctx.gate_events) &ctx.step_gate else null,
                .execution_contention_wait_ns = ctx.execution_gate_wait_ns,
                .execution_control = ctx.worker_control,
                .prepared_lighton_decode = ctx.features.prepared,
                .packed_prefill_items = ctx.features.packed_prefill_items,
                .prepared_qwen_prefill_chunk_rows = ctx.features.prepared_prefill_chunk_rows,
                .packed_shared_frame = ctx.features.packed_shared_frame,
                .single_frame_lighton_decode = ctx.features.single_frame,
                .gpu_token_burst_lighton_decode = ctx.features.gpu_token_burst,
                .gpu_token_burst_width = ctx.features.gpu_token_burst_width,
                .gpu_greedy_stream_lighton_decode = ctx.features.gpu_greedy_stream,
                .lighton_cuda_execution_id = ctx.execution_id,
            };
            defer pipeline.deinit();
            var storage: [reader_execution_pool.Pool.max_capacity]runtime.scheduler.native_generate.StepItem = undefined;
            var step = std.ArrayListUnmanaged(runtime.scheduler.native_generate.StepItem).empty;
            while (!ctx.worker_stop.load(.acquire)) {
                // Request phases cannot enqueue decode until vision/prompt
                // preparation obtains this same GPU gate. Do not immediately
                // reacquire it forever just because continuations stay queued.
                // Reset before observing phase/queue predicates. A phase
                // releasing its last waiter always signals this same event.
                ctx.worker_wake.reset();
                if (ctx.phase_fair and ctx.phase_waiters.load(.acquire) != 0) {
                    ctx.phase_priority_yields += 1;
                    if (ctx.gate_events) {
                        if (!ctx.worker_stop.load(.acquire)) ctx.worker_wake.waitTimeout(io, .{ .duration = .{ .raw = .fromMilliseconds(10), .clock = .awake } }) catch {};
                        continue;
                    }
                    // Match the admitted epoch's gate parking interval. The
                    // old fixed 100us loop kept waking during long prefill
                    // phases even when the configured interval was 1ms.
                    const wait_us = @max(@as(u64, 100), @min(ctx.execution_gate_wait_ns / std.time.ns_per_us, 1000));
                    io.sleep(std.Io.Duration.fromMicroseconds(@intCast(wait_us)), .awake) catch {};
                    continue;
                }
                // Both predicates have now been observed after the reset.
                var budget = ctx.coordinator.defaultStepBudget();
                platform.sync.lockYielding(&ctx.kv_mutex);
                if (ctx.manager.poolAvailableBlocks(ctx.pool)) |available|
                    budget.max_kv_blocks = if (available > 1) available - 1 else 0;
                ctx.kv_mutex.unlock();
                if (ctx.coordinator.claimNextStep(budget, &storage, &step)) |claimed_lease| {
                    var lease = claimed_lease;
                    pipeline.executeClaimedStep(&ctx.coordinator, &lease, step.items);
                    ctx.worker_steps += 1;
                    continue;
                }
                if (!ctx.worker_stop.load(.acquire)) ctx.worker_wake.waitTimeout(io, .{ .duration = .{ .raw = .fromMicroseconds(ctx.coordinator.eventWaitUs()), .clock = .awake } }) catch {};
            }
            ctx.worker_tail_steps = pipeline.executor_tail_steps;
        }

        pub fn prepare(ctx: *LightOnStreamContext, kv_dtype: runtime.kv.pool.KvDType) !void {
            if (ctx.cb != null) return;
            var cb = try session_factory.getComputeBackendWithBudget(ctx.model.session, ctx.allocator, &ctx.budget);
            errdefer cb.deinit();
            // The device hook owns CUDA KV bytes. Both epoch pools retain
            // logical pages without allocating a second pair of host mirrors.
            const config = generation.requestKvManagerPoolConfig(generation.kvPoolConfig(if (ctx.model.session.backend() == .cuda) .cuda else .metal, kv_dtype, ctx.cfg, Hooks.generationKvSlidingTrimForced()));
            ctx.pool = try ctx.manager.addPool(config);
            ctx.storage = try runtime.kv.storage_runtime.KvStorageRuntime.init(ctx.allocator, config);
            errdefer ctx.storage.deinit();
            try cb.provisionKvDeviceWriteHook(&ctx.storage);
            if (comptime build_options.enable_metal) {
                if (cb.kind() == .metal) {
                    const M = @import("../ops/metal_compute.zig").MetalCompute;
                    try M.setLightOnPreparedRows(&cb, ctx.features.prepared_rows);
                    try M.setLightOnActivationPoolConfigured(&cb, ctx.features.activation_pool, ctx.features.prefill_activation_pool);
                    try M.setLightOnSharedCoreWeights(&cb, ctx.features.epoch_auxiliary and ctx.features.shared_core_weights);
                    try M.setLightOnStreamFirstLayers(&cb, ctx.features.stream_first_layers);
                    try M.setLightOnBatchAttention(&cb, ctx.features.batch_attention);
                    try M.setLightOnGpuGreedyStream(&cb, ctx.features.gpu_greedy_stream);
                    try M.setLightOnStreamChunks(&cb, ctx.features.stream_chunks);
                    try M.setLightOnPrefillStreamLayers(&cb, ctx.features.prefill_stream_layers);
                    // admitPage has already acquired the shared vision peak.
                    // It outlives cb.deinit and its explicit workspace clear.
                    try M.setLightOnVisionWorkspaceCache(&cb, ctx.features.vision_workspace_cache);
                }
            }
            ctx.cb = cb;
        }
    };
}
