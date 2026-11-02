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

//! Physical scheduler forward execution. Work/state/context are borrowed until
//! completion; tokenization, stopping and task publication stay with adapters.
const std = @import("std");
const platform = @import("antfly_platform");
const build_options = @import("build_options");
const execution_control = @import("../execution_control.zig");
const runtime = @import("../runtime/root.zig");
const gpt_arch = @import("../architectures/gpt.zig");
const gpt_mod = @import("../models/gpt.zig");
const ops = @import("../ops/ops.zig");
const ComputeBackend = ops.ComputeBackend;
const decoder_gated_runtime = @import("../backends/decoder_gated_runtime.zig");
const execution = @import("execution.zig");
const work_mod = @import("decoder_work.zig");
const PendingDecodeBatchWork = work_mod.Decode;
const PendingPrefillBatchWork = work_mod.Prefill;
const state_runtime = @import("decode_state_runtime.zig");
const NativeDecodeState = state_runtime.NativeDecodeState;
const BorrowedDecodeStateRuntime = state_runtime.BorrowedDecodeStateRuntime;
const decode_batch = @import("decode_batch.zig");
const MixedBatchDecodeItem = decode_batch.Item;
const buildOwnedMixedBatchDecodeContext = decode_batch.buildMixed;
const buildOwnedMixedBatchDecodeContextLayout = decode_batch.buildLayout;
const buildOwnedLightOnGraphDecodeContext = decode_batch.buildPackedDecode;
pub const packed_head_rows = @import("packed_head_rows.zig");
const max_gpu_token_burst_width = 8;
pub const Result = Executor.StepForwardResult;

fn packedQwenPrefillItemLimit(backend: ops.BackendKind, wide: bool) usize {
    return if (backend == .cuda and wide) 16 else 8;
}

fn cudaReplayLastLogitsEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_REPLAY_LAST_LOGITS", true);
}

fn debugStage(comptime format: []const u8, args: anytype) void {
    if (platform.env.getenvBool("TERMITE_GEN_STAGE_DEBUG")) std.debug.print("gen_debug: " ++ format ++ "\n", args);
}

pub fn singletonScheduledGemmaPrefillFrameEnabled() bool {
    return !platform.env.getenvBool("TERMITE_METAL_DISABLE_SINGLETON_SCHEDULED_PREFILL_FRAME");
}

pub fn shouldUseSingletonScheduledGemmaPrefillFrame(
    kind: ops.BackendKind,
    config: *const gpt_mod.Config,
    item_count: usize,
    phase: runtime.scheduler.native_generate.Phase,
    query_sequence_len: usize,
    wants_logits: bool,
) bool {
    return kind == .metal and
        config.family == .gemma and
        !config.usesMoe() and
        config.hasPle() and
        item_count == 1 and
        phase == .prefill and
        query_sequence_len > 1 and
        !wants_logits;
}

pub fn runWithDecoderRuntimeResetBoundary(
    cb: *const ComputeBackend,
    context: *anyopaque,
    run: *const fn (context: *anyopaque) anyerror!bool,
) !bool {
    try cb.decoderRuntimeResetState();
    const result = run(context) catch |err| {
        cb.decoderRuntimeResetState() catch {};
        return err;
    };
    try cb.decoderRuntimeResetState();
    return result;
}

/// Common eager/replay/compiled forward for sampled and scheduled decoding.
pub fn forwardAll(
    context: *const execution.Context,
    graph_cache: ?*@import("cache.zig").GraphCache,
    input_ids: []const i64,
    batch_size: usize,
    seq_len: usize,
    decode_context: *const gpt_arch.DecodeContext,
    replay_fallbacks: *u64,
) ![]f32 {
    if (NativeDecodeState.requiresDeepSeekV4CompressedCache(context.gpt_config.*) and
        (graph_cache != null or context.compiled_partition_backend != null)) return error.DeepSeekV4CompressedGraphModeNotSupported;
    if (graph_cache) |cache| return execution.graphForwardAll(context, cache, input_ids, batch_size, seq_len, decode_context);
    if (context.compiled_partition_backend != null) return error.MissingGraphCacheForCompiledPartitionBackend;
    if (context.cb.kind() == .cuda and batch_size == 1 and
        decode_context.query_sequence_len == 1 and cudaReplayLastLogitsEnabled())
    {
        if (gpt_arch.forwardLastLogitsWithCudaReplay(context.cb, context.allocator, context.gpt_config.*, input_ids, batch_size, seq_len, decode_context, "gpt.standard_last_logits")) |logits| {
            return logits;
        } else |err| {
            replay_fallbacks.* += 1;
            debugStage("forwardAllLogits replay path failed err={s}; falling back to interpreter", .{@errorName(err)});
        }
    }
    return gpt_arch.forward(context.cb, context.allocator, context.gpt_config.*, input_ids, batch_size, seq_len, decode_context);
}

pub fn endDecodeEpoch(cb: *const ComputeBackend, execution_id: u64, attempts: u64, declines: [8]u64) void {
    if (platform.env.getenvBool("ANTFLY_BENCH_LIGHTONOCR3") and execution_id != 0 and
        platform.env.getenvBool("ANTFLY_CUDA_ENABLE_LIGHTON_MODEL_GRAPH"))
        std.debug.print("LIGHTONOCR3 cuda_model_graph_attempts={d} declines={any}\n", .{ attempts, declines });
    if (comptime build_options.enable_cuda) {
        if (execution_id != 0) @import("../ops/cuda/cuda_compute.zig").CudaCompute.endLightOnDecodeGraphEpoch(cb, execution_id);
    }
}

pub const Executor = struct {
    context: execution.Context,
    graph_cache: ?*@import("cache.zig").GraphCache = null,
    pad_token: i64 = 0,
    continuation: ?execution_control.InferenceExecutionControl = null,
    access: execution_control.ExecutionAccess = .{},
    prepared_lighton_decode: bool = false,
    single_frame_lighton_decode: bool = false,
    gpu_token_burst_lighton_decode: bool = false,
    gpu_token_burst_width: u8 = 2,
    gpu_greedy_stream_lighton_decode: bool = false,
    lighton_cuda_execution_id: u64 = 0,
    packed_prefill_items: u8 = 1,
    packed_shared_frame: bool = false,
    prepared_qwen_prefill_chunk_rows: u16 = 0,
    lighton_graph_attempts: u64 = 0,
    lighton_graph_declines: [8]u64 = @splat(0),
    replay_last_logits_fallbacks: u64 = 0,
    tail_steps: usize = 0,

    /// Result publication executes under the same lane and autorelease scope
    /// as the completed forward. Publication may requeue borrowed work.
    pub const Publication = struct {
        ptr: *anyopaque,
        publish_fn: *const fn (*anyopaque, []const runtime.scheduler.native_generate.StepItem, anyerror!Result) void,

        fn publish(self: Publication, claimed: []const runtime.scheduler.native_generate.StepItem, result: anyerror!Result) void {
            self.publish_fn(self.ptr, claimed, result);
        }
    };

    pub fn execute(self: *Executor, claimed: []const runtime.scheduler.native_generate.StepItem, step_events: bool, publication: Publication) void {
        if (self.access.mutex) |mutex| {
            if (step_events or self.access.gate != null) {
                self.access.acquire() catch |err| {
                    publication.publish(claimed, err);
                    return;
                };
            } else platform.sync.lockYielding(mutex);
        }
        defer self.access.release();
        if (comptime build_options.enable_metal) {
            if (self.context.cb.kind() == .metal and self.context.gpt_config.hasQwen35VisionArchitectureContract() and
                !platform.env.getenvBool("TERMITE_METAL_DISABLE_QWEN35_STEP_POOL"))
            {
                const Scope = struct {
                    executor: *Executor,
                    claimed: []const runtime.scheduler.native_generate.StepItem,
                    publication: Publication,
                    fn run(raw: *anyopaque) callconv(.c) void {
                        const scope: *@This() = @ptrCast(@alignCast(raw));
                        scope.executor.executePooled(scope.claimed, scope.publication);
                    }
                };
                var scope = Scope{ .executor = self, .claimed = claimed, .publication = publication };
                @import("../backends/metal_runtime.zig").termite_metal_with_autorelease_pool(&scope, Scope.run);
                return;
            }
        }
        self.executePooled(claimed, publication);
    }

    fn executePooled(self: *Executor, claimed: []const runtime.scheduler.native_generate.StepItem, publication: Publication) void {
        self.context.cb.drainPrefetchBudget(4);
        for (claimed) |item| if (item.phase == .decode) {
            const work: *PendingDecodeBatchWork = @ptrCast(@alignCast(item.work_ptr));
            if (work.continuation_context != null) self.tail_steps += 1;
        };
        var result = self.forward(claimed) catch |err| {
            publication.publish(claimed, err);
            return;
        };
        defer result.deinit(self.context.allocator);
        publication.publish(claimed, result);
    }

    pub fn endEpoch(self: *Executor) void {
        endDecodeEpoch(self.context.cb, self.lighton_cuda_execution_id, self.lighton_graph_attempts, self.lighton_graph_declines);
    }

    fn graphWeight(self: *Executor, name: []const u8) !ops.CT {
        return gpt_arch.getTracedWeight(self.context.allocator, self.context.cb, self.context.gpt_config, name);
    }

    fn forwardAllLogits(self: *Executor, input_ids: []const i64, batch_size: usize, seq_len: usize, decode_context: *const gpt_arch.DecodeContext) ![]f32 {
        var context = self.context;
        const Adapter = struct {
            fn get(raw: *anyopaque, name: []const u8) !ops.CT {
                const executor: *Executor = @ptrCast(@alignCast(raw));
                return executor.graphWeight(name);
            }
        };
        context.weights = .{ .ptr = self, .get_fn = Adapter.get };
        return forwardAll(&context, self.graph_cache, input_ids, batch_size, seq_len, decode_context, &self.replay_last_logits_fallbacks);
    }

    const PrefixLogitsLayout = packed_head_rows.PrefixLogitsLayout;

    pub fn prefillHasHostRow(work: *const PendingPrefillBatchWork, layout: PrefixLogitsLayout) bool {
        return !work.wants_greedy and (layout == .all_host_rows or work.wants_last_logits);
    }

    pub const StepForwardResult = struct {
        greedy_tokens: ?[]u32 = null,
        packed_greedy_tokens: ?[]u32 = null,
        prefix_logits_layout: PrefixLogitsLayout = .all_host_rows,
        // Additional outputs, step-major: (steps - 1) * cohort rows.
        second_greedy_tokens: ?[]u32 = null,
        logits: ?[]f32,
        max_query_seq_len: usize,
        last_rows_only: bool = false,
        host_permit: ?@import("../backends/session.zig").RunPermit = null,

        pub fn deinit(self: *StepForwardResult, allocator: std.mem.Allocator) void {
            if (self.second_greedy_tokens) |values| allocator.free(values);
            if (self.packed_greedy_tokens) |values| allocator.free(values);
            if (self.greedy_tokens) |values| allocator.free(values);
            if (self.logits) |values| allocator.free(values);
            if (self.host_permit) |*permit| permit.deinit();
            self.* = undefined;
        }
    };

    fn scheduledStepNeedsLogits(claimed: []const runtime.scheduler.native_generate.StepItem) bool {
        for (claimed) |item| {
            switch (item.phase) {
                .decode => return true,
                .prefill => {
                    const work: *PendingPrefillBatchWork = @ptrCast(@alignCast(item.work_ptr));
                    if (work.wants_last_logits) return true;
                },
                .waiting => return true,
            }
        }
        return false;
    }

    fn tryRunSingletonScheduledGemmaPrefillFrame(
        self: *Executor,
        claimed: []const runtime.scheduler.native_generate.StepItem,
        decode_context: *const gpt_arch.DecodeContext,
    ) !bool {
        if (!singletonScheduledGemmaPrefillFrameEnabled()) return false;
        if (claimed.len != 1 or claimed[0].phase != .prefill) return false;
        const item = claimed[0];
        const work: *PendingPrefillBatchWork = @ptrCast(@alignCast(item.work_ptr));
        if (!shouldUseSingletonScheduledGemmaPrefillFrame(
            self.context.cb.kind(),
            &self.context.gpt_config.*,
            claimed.len,
            item.phase,
            item.query_sequence_len,
            work.wants_last_logits,
        )) return false;

        const Attempt = struct {
            pipeline: *Executor,
            work: *PendingPrefillBatchWork,
            decode_context: *const gpt_arch.DecodeContext,

            fn run(context: *anyopaque) !bool {
                const attempt: *@This() = @ptrCast(@alignCast(context));
                const pipeline = attempt.pipeline;
                const prepared = try pipeline.context.cb.decoderRuntimePrepareOrReuseFamily(
                    pipeline.context.allocator,
                    pipeline.context.gpt_config.*,
                    attempt.decode_context.kv_sequence_len,
                    pipeline.context.gpt_config.num_hidden_layers,
                );
                if (!prepared.prepared) return false;

                const tail = (try decoder_gated_runtime.forwardPrefillLastPreparedTail(
                    pipeline.context.cb,
                    pipeline.context.allocator,
                    pipeline.context.gpt_config.*,
                    pipeline.context.gpt_config.num_hidden_layers,
                    attempt.work.token_ids,
                    attempt.work.seq_len,
                    attempt.decode_context,
                    false,
                )) orelse return false;
                pipeline.context.cb.free(tail.final_hidden);
                return true;
            }
        };

        var attempt = Attempt{
            .pipeline = self,
            .work = work,
            .decode_context = decode_context,
        };
        return runWithDecoderRuntimeResetBoundary(self.context.cb, @ptrCast(&attempt), Attempt.run);
    }

    /// Run a step's forward pass: allocate scratch, populate per-item context,
    /// reserve prefill state, build the mixed-batch decode context, and invoke
    /// the model. On any error before the forward pass returns successfully,
    /// any prefill reservations made so far are rolled back. Once the forward
    /// completes, the prefill state is committed and rollback is suppressed.
    /// All GPU steps drain before publication. Only one submitted and one
    /// active frame exist at a time. Two logits banks are reused in order;
    /// the cohort's independent page states stay borrowed for the whole burst.
    fn finishLightOnGpuTokenBurst(
        self: *Executor,
        claimed: []const runtime.scheduler.native_generate.StepItem,
        items: []MixedBatchDecodeItem,
        first_logits: ops.CT,
        head: ops.CT,
        embedding_slot: usize,
        steps: usize,
        before_frames: u64,
        before_argmax: u64,
    ) !StepForwardResult {
        if (comptime !build_options.enable_metal) return error.UnsupportedOperation;
        if (steps < 2 or steps > max_gpu_token_burst_width) return error.InvalidBatchDecodeState;
        const M = @import("../ops/metal_compute.zig").MetalCompute;
        const first = try self.context.allocator.alloc(u32, claimed.len);
        errdefer self.context.allocator.free(first);
        const rest = try self.context.allocator.alloc(u32, (steps - 1) * claimed.len);
        errdefer self.context.allocator.free(rest);
        var second_logits: ?ops.CT = null;
        var previous_hidden: ?ops.CT = null;
        defer {
            // No resource or page borrow can retire before its producer.
            self.context.cb.decoderRuntimeWaitSubmittedFrame() catch {};
            if (previous_hidden) |hidden| self.context.cb.free(hidden);
            if (second_logits) |logits| self.context.cb.free(logits);
        }
        try M.encodeLightOnBurstArgmax(self.context.cb, first_logits, claimed.len, self.context.gpt_config.vocab_size, self.context.gpt_config.suppressTokenIds());
        try self.context.cb.decoderRuntimeSubmitFrame();
        for (1..steps) |step| {
            if (!try self.context.cb.decoderRuntimeBeginFrame()) return error.StreamedForwardFrameUnavailable;
            try self.context.cb.decoderRuntimeSetActiveFrameRegime(.decode);
            if (!try self.context.cb.decoderRuntimeBeginPlannedComputeScope()) return error.StreamedForwardFrameUnavailable;
            M.noteLightOnTokenEncoderScope(self.context.cb);
            for (items) |*item| {
                // Eligibility reserved all outputs in the request's original
                // max_tokens allowance. Only metadata is advanced on the CPU.
                try item.state.appendGeneratedToken();
                item.total_sequence_len += 1;
                var decode = BorrowedDecodeStateRuntime.init(item.state);
                const next = decode.makeDecodeContext(item.total_sequence_len, 1);
                item.kv_sequence_len = next.kv_sequence_len;
                item.kv_position_offset = next.kv_position_offset;
            }
            const metadata_bytes = 64 * (@sizeOf(gpt_arch.DecodeContext.KvBatchView) + @sizeOf(?*gpt_arch.Qwen35LinearCache) + 3 * @sizeOf(u32)) + 128;
            var metadata: [metadata_bytes]u8 align(@alignOf(gpt_arch.DecodeContext.KvBatchView)) = undefined;
            var fba = std.heap.FixedBufferAllocator.init(&metadata);
            var owned = try buildOwnedMixedBatchDecodeContext(fba.allocator(), items);
            defer owned.deinit();
            owned.context.prepared_lighton_decode = true;
            try self.context.cb.checkExecutionControl();
            const embedded = try M.lightOnBurstEmbedding(self.context.cb, embedding_slot, claimed.len, self.context.gpt_config.hidden_size);
            const hidden = try gpt_arch.forwardFinalHiddenTensorFromEmbeddingsWithLayer0Overrides(self.context.cb, self.context.allocator, self.context.gpt_config.*, embedded, .{}, claimed.len, 1, &owned.context, null);
            // The current hidden must survive an encoding error too. The C
            // frame retains encoded resources until cancel/drain.
            var hidden_owned = true;
            defer if (hidden_owned) self.context.cb.free(hidden.hidden);
            if (second_logits == null) {
                second_logits = try self.context.cb.linearNoBias(hidden.hidden, head, claimed.len, self.context.gpt_config.hidden_size, self.context.gpt_config.vocab_size);
            } else {
                // The bank from step-2 has completed before this frame began.
                // No additional vocabulary-sized allocation as width grows.
                const output = if (step % 2 == 0) first_logits else second_logits.?;
                try M.lightOnBurstHeadInto(self.context.cb, hidden.hidden, head, output, claimed.len, self.context.gpt_config.hidden_size, self.context.gpt_config.vocab_size);
            }
            const logits = if (step % 2 == 0) first_logits else second_logits.?;
            try M.encodeLightOnBurstArgmax(self.context.cb, logits, claimed.len, self.context.gpt_config.vocab_size, self.context.gpt_config.suppressTokenIds());
            // Copy the previous token buffer before the next producer may
            // overwrite it. GPU embedding already captured its dependency.
            try self.context.cb.decoderRuntimeWaitSubmittedFrame();
            const output_tokens = if (step == 1) first else rest[(step - 2) * claimed.len ..][0..claimed.len];
            try M.readLightOnBurstTokens(self.context.cb, output_tokens);
            if (previous_hidden) |old| self.context.cb.free(old);
            previous_hidden = null;
            try self.context.cb.decoderRuntimeSubmitFrame();
            previous_hidden = hidden.hidden;
            hidden_owned = false;
        }
        try self.context.cb.decoderRuntimeWaitSubmittedFrame();
        try M.readLightOnBurstTokens(self.context.cb, rest[(steps - 2) * claimed.len ..][0..claimed.len]);
        try self.context.cb.checkExecutionControl();
        M.noteLightOnBurst(self.context.cb, self.context.cb.debugTimingSnapshot().provider.decoder_runtime_frame_submits -| before_frames, M.argmaxOnFrameCount(self.context.cb) -| before_argmax, steps, claimed.len);
        return .{ .logits = null, .greedy_tokens = first, .second_greedy_tokens = rest, .max_query_seq_len = 1 };
    }

    fn tryLightOnCudaTokenBurst(
        self: *Executor,
        claimed: []const runtime.scheduler.native_generate.StepItem,
        items: []MixedBatchDecodeItem,
        input_ids: []const i64,
        first_context: *gpt_arch.DecodeContext,
    ) !?StepForwardResult {
        if (!self.gpu_token_burst_lighton_decode or self.access.mutex == null) return null;

        var steps: usize = std.math.clamp(@as(usize, self.gpu_token_burst_width), 2, max_gpu_token_burst_width);
        const eligible = eligible: {
            for (claimed) |item| {
                const work: *PendingDecodeBatchWork = @ptrCast(@alignCast(item.work_ptr));
                if (work.continuation_context == null or work.remaining_tokens < 2) break :eligible false;
                steps = @min(steps, work.remaining_tokens);
            }
            break :eligible true;
        };
        if (eligible) {
            const embedding = try gpt_arch.getEmbeddingWeight(self.context.cb, self.context.gpt_config.*);
            defer self.context.cb.free(embedding);
            const head = try gpt_arch.getLmHeadWeight(self.context.cb, self.context.gpt_config.*);
            defer self.context.cb.free(head);
            if (try @import("../ops/cuda/cuda_compute.zig").CudaCompute.beginLightOnTokenBurst(self.context.cb, embedding, claimed.len, steps, self.context.gpt_config.hidden_size, self.context.gpt_config.vocab_size, self.context.gpt_config.suppressTokenIds())) |prepared| {
                var burst = prepared;
                defer burst.deinit();
                // No fallback is permitted after this first forward
                // has changed any independent recurrent/KV state.
                return try self.finishLightOnCudaTokenBurst(claimed, items, input_ids, first_context, head, &burst);
            }
        }

        return null;
    }

    /// Queue a bounded chain of dependent CUDA tokens without a host read
    /// between forwards. Existing publication handles EOS, cancellation and
    /// the reserved KV count only after the entire claimed chain has drained.
    fn finishLightOnCudaTokenBurst(
        self: *Executor,
        claimed: []const runtime.scheduler.native_generate.StepItem,
        items: []MixedBatchDecodeItem,
        input_ids: []const i64,
        first_context: *gpt_arch.DecodeContext,
        head: ops.CT,
        burst: anytype,
    ) !StepForwardResult {
        errdefer for (items) |item| {
            // A partial queued forward is never replayable or reusable. Keep
            // failed recurrent allocations out of the healthy state pool.
            if (item.state.qwen35_linear_cache) |cache| for (cache.layers) |*layer| {
                layer.cuda_device.failed = true;
            };
        };
        const first = try self.context.allocator.alloc(u32, claimed.len);
        errdefer self.context.allocator.free(first);
        const rest = try self.context.allocator.alloc(u32, (burst.steps - 1) * claimed.len);
        errdefer self.context.allocator.free(rest);
        const wide = claimed.len > 64;
        var recurrent_groups: usize = 0;
        var recurrent_rows: usize = 0;
        var attention_groups: usize = 0;
        var attention_rows: usize = 0;
        const original_recurrent_groups = first_context.packed_recurrent_groups;
        const original_recurrent_rows = first_context.packed_recurrent_rows;
        const original_attention_groups = first_context.packed_attention_groups;
        const original_attention_rows = first_context.packed_attention_rows;
        if (wide) {
            first_context.packed_recurrent_groups = &recurrent_groups;
            first_context.packed_recurrent_rows = &recurrent_rows;
            first_context.packed_attention_groups = &attention_groups;
            first_context.packed_attention_rows = &attention_rows;
        }
        defer {
            first_context.packed_recurrent_groups = original_recurrent_groups;
            first_context.packed_recurrent_rows = original_recurrent_rows;
            first_context.packed_attention_groups = original_attention_groups;
            first_context.packed_attention_rows = original_attention_rows;
        }
        try self.context.cb.checkExecutionControl();
        {
            const hidden = try gpt_arch.forwardHiddenTensorWithCudaReplay(self.context.cb, self.context.allocator, self.context.gpt_config.*, input_ids, if (wide) 1 else claimed.len, if (wide) claimed.len else 1, first_context, "gpt.cuda_token_burst");
            defer self.context.cb.free(hidden);
            try burst.projectArgmax(hidden, head, 0);
        }
        for (1..burst.steps) |step| {
            try self.context.cb.checkExecutionControl();
            for (items) |*item| {
                // Only positions and pre-admitted KV capacity advance here;
                // the generated ID remains in the device token bank.
                try item.state.appendGeneratedToken();
                item.total_sequence_len += 1;
                var decode = BorrowedDecodeStateRuntime.init(item.state);
                const context = decode.makeDecodeContext(item.total_sequence_len, 1);
                item.kv_sequence_len = context.kv_sequence_len;
                item.kv_position_offset = context.kv_position_offset;
            }
            const metadata_bytes = 128 * (@sizeOf(gpt_arch.DecodeContext.KvBatchView) + @sizeOf(?*gpt_arch.Qwen35LinearCache) + 3 * @sizeOf(u32)) + 128;
            var metadata: [metadata_bytes]u8 align(@alignOf(gpt_arch.DecodeContext.KvBatchView)) = undefined;
            var fba = std.heap.FixedBufferAllocator.init(&metadata);
            const lengths: [128]usize = @splat(1);
            var context = try buildOwnedLightOnGraphDecodeContext(fba.allocator(), items, lengths[0..items.len]);
            defer context.deinit();
            context.context.prepared_lighton_decode = true;
            if (wide) {
                context.context.packed_recurrent_groups = &recurrent_groups;
                context.context.packed_recurrent_rows = &recurrent_rows;
                context.context.packed_attention_groups = &attention_groups;
                context.context.packed_attention_rows = &attention_rows;
            }
            const embedded = try burst.embedding(step - 1);
            const hidden = try gpt_arch.forwardFinalHiddenTensorFromEmbeddingsWithLayer0Overrides(self.context.cb, self.context.allocator, self.context.gpt_config.*, embedded, .{}, if (wide) 1 else claimed.len, if (wide) claimed.len else 1, &context.context, null);
            defer self.context.cb.free(hidden.hidden);
            try burst.projectArgmax(hidden.hidden, head, step);
        }
        var host: [max_gpu_token_burst_width * 128]u32 = undefined;
        try self.readLightOnCudaBurst(burst, host[0 .. burst.steps * claimed.len]);
        try self.context.cb.checkExecutionControl();
        @memcpy(first, host[0..claimed.len]);
        @memcpy(rest, host[claimed.len .. burst.steps * claimed.len]);
        burst.noteCompleted();
        if (platform.env.getenvBool("ANTFLY_BENCH_LIGHTONOCR3")) {
            std.debug.print("LIGHTONOCR3 cuda_token_burst_rows={d} steps={d}\n", .{ claimed.len, burst.steps });
            if (wide) {
                std.debug.print("LIGHTONOCR3 packed_step_prefill_items=0 packed_step_decode_items={d} packed_step_query_rows={d}\n", .{ claimed.len, claimed.len });
                std.debug.print("LIGHTONOCR3 packed_step_prefill_starts=0 packed_step_prefill_finishes=0 prepared_prefill_chunk_rows={d}\n", .{self.prepared_qwen_prefill_chunk_rows});
                std.debug.print("LIGHTONOCR3 packed_recurrent_groups={d} packed_recurrent_rows={d}\n", .{ recurrent_groups, recurrent_rows });
                std.debug.print("LIGHTONOCR3 packed_attention_groups={d} packed_attention_rows={d}\n", .{ attention_groups, attention_rows });
            }
        }
        return .{ .logits = null, .greedy_tokens = first, .second_greedy_tokens = rest, .max_query_seq_len = 1 };
    }

    /// Every queued consumer and its claimed pages remain owned while the
    /// lane admits independent vision/prompt work. Cleanup reacquires the lane
    /// even for cancellation or a CUDA error before retiring any resource.
    pub fn waitOutsideLightOnExecution(self: *Executor, pending: anytype) !void {
        if (self.access.mutex == null) return error.InvalidBatchDecodeState;
        self.access.release();
        const waited = pending.wait();
        self.access.acquireForCleanup();
        try waited;
    }

    /// Mixed prefix/decode choices use the same independently owned token
    /// bank and cancellation-safe pinned readback as multi-token decoding.
    /// The scheduler retains all claimed pages until this forward publishes.
    fn tryReadLightOnCudaPackedChoices(self: *Executor, logits: ops.CT, rows: usize) !?[]u32 {
        if (comptime !build_options.enable_cuda) return null;
        if (!self.gpu_token_burst_lighton_decode or self.access.mutex == null or self.lighton_cuda_execution_id == 0) return null;
        const C = @import("../ops/cuda/cuda_compute.zig").CudaCompute;
        const embedding = try gpt_arch.getEmbeddingWeight(self.context.cb, self.context.gpt_config.*);
        defer self.context.cb.free(embedding);
        var choices = (try C.beginLightOnTokenBurst(self.context.cb, embedding, rows, 1, self.context.gpt_config.hidden_size, self.context.gpt_config.vocab_size, self.context.gpt_config.suppressTokenIds())) orelse return null;
        defer choices.deinit();
        try choices.argmax(logits, 0);
        const output = try self.context.allocator.alloc(u32, rows);
        errdefer self.context.allocator.free(output);
        try self.readLightOnCudaBurst(&choices, output);
        try self.context.cb.checkExecutionControl();
        return output;
    }

    fn readLightOnCudaBurst(self: *Executor, burst: anytype, output: []u32) !void {
        if (try burst.beginRead(output.len)) |queued| {
            var pending = queued;
            defer pending.deinit();
            const before = burst.owner.ctx.stats.kernel_launches;
            try self.waitOutsideLightOnExecution(&pending);
            try pending.copy(std.mem.sliceAsBytes(output));
            burst.noteReadCompleted(before);
        } else try burst.read(output);
    }

    /// Execute only inside the reader epoch's model lane. The two-step graph
    /// returns through the existing bounded-burst publication contract.
    fn declineLightOnGraph(self: *Executor, reason: u3) ?StepForwardResult {
        self.lighton_graph_declines[reason] += 1;
        return null;
    }

    fn tryLightOnCudaDecodeGraph(
        self: *Executor,
        claimed: []const runtime.scheduler.native_generate.StepItem,
        items: []MixedBatchDecodeItem,
        input_ids: []const i64,
        first_context: *gpt_arch.DecodeContext,
    ) !?StepForwardResult {
        const C = @import("../ops/cuda/cuda_compute.zig").CudaCompute;
        if (!C.lightOnDecodeGraphEnabled(self.context.cb) or claimed.len < 2 or claimed.len > (if (platform.env.getenvBool("ANTFLY_LIGHTONOCR3_PHYSICAL128")) @as(usize, 128) else 64) or first_context.mrope_positions == null)
            return self.tryOrderedLightOnCudaDecodeGraph(claimed, items, input_ids, first_context);
        std.debug.assert(items.len == claimed.len and input_ids.len == claimed.len);
        var sequences: [128]u64 = undefined;
        var order: [128]usize = undefined;
        for (items, 0..) |item, i| sequences[i] = item.state.sequence_id orelse
            return self.tryOrderedLightOnCudaDecodeGraph(claimed, items, input_ids, first_context);
        try @import("../ops/cuda/lighton_decode_graph.zig").canonicalOrder(sequences[0..claimed.len], order[0..claimed.len]);
        var identity = true;
        for (order[0..claimed.len], 0..) |original, row| identity = identity and original == row;
        if (identity) return self.tryOrderedLightOnCudaDecodeGraph(claimed, items, input_ids, first_context);
        var ordered_claims: [128]runtime.scheduler.native_generate.StepItem = undefined;
        var ordered_items: [128]MixedBatchDecodeItem = undefined;
        var ordered_ids: [128]i64 = undefined;
        for (order[0..claimed.len], 0..) |original, row| {
            ordered_claims[row] = claimed[original];
            ordered_items[row] = items[original];
            ordered_ids[row] = input_ids[original];
        }
        const metadata_bytes = 128 * (@sizeOf(gpt_arch.DecodeContext.KvBatchView) + @sizeOf(?*gpt_arch.Qwen35LinearCache) + 3 * @sizeOf(u32)) + 128;
        var metadata: [metadata_bytes]u8 align(@alignOf(gpt_arch.DecodeContext.KvBatchView)) = undefined;
        var fba = std.heap.FixedBufferAllocator.init(&metadata);
        const lengths: [128]usize = @splat(1);
        var context = try buildOwnedLightOnGraphDecodeContext(fba.allocator(), ordered_items[0..claimed.len], lengths[0..claimed.len]);
        defer context.deinit();
        context.context.prepared_lighton_decode = true;
        const result = (try self.tryOrderedLightOnCudaDecodeGraph(ordered_claims[0..claimed.len], ordered_items[0..claimed.len], ordered_ids[0..claimed.len], &context.context)) orelse return null;
        const first = result.greedy_tokens.?;
        const second = result.second_greedy_tokens.?;
        std.debug.assert(first.len == claimed.len and second.len == claimed.len);
        var first_original: [128]u32 = undefined;
        var second_original: [128]u32 = undefined;
        for (order[0..claimed.len], 0..) |original, row| {
            first_original[original] = first[row];
            second_original[original] = second[row];
        }
        @memcpy(first, first_original[0..claimed.len]);
        @memcpy(second, second_original[0..claimed.len]);
        return result;
    }

    fn tryOrderedLightOnCudaDecodeGraph(
        self: *Executor,
        claimed: []const runtime.scheduler.native_generate.StepItem,
        items: []MixedBatchDecodeItem,
        input_ids: []const i64,
        first_context: *gpt_arch.DecodeContext,
    ) !?StepForwardResult {
        const C = @import("../ops/cuda/cuda_compute.zig").CudaCompute;
        self.lighton_graph_attempts += 1;
        if (self.lighton_cuda_execution_id == 0 or self.access.mutex == null or !C.lightOnDecodeGraphEnabled(self.context.cb) or
            claimed.len < 2 or claimed.len > (if (platform.env.getenvBool("ANTFLY_LIGHTONOCR3_PHYSICAL128")) @as(usize, 128) else 64) or self.context.gpt_config.suppressTokenIds().len != 0) return self.declineLightOnGraph(0);
        if (claimed.len > 64 and claimed.len % 64 == 1) return self.declineLightOnGraph(0);
        try self.context.cb.checkExecutionControl();
        const Key = struct {
            words: [24576]u64 = undefined,
            len: usize = 0,
            fn add(key: *@This(), word: u64) !void {
                if (key.len == key.words.len) return error.InvalidShape;
                key.words[key.len] = word;
                key.len += 1;
            }
        };
        var key = Key{};
        const storage = items[0].state.kv_storage orelse return self.declineLightOnGraph(1);
        const manager = items[0].state.kv_manager orelse return self.declineLightOnGraph(1);
        const positions = first_context.mrope_positions orelse return self.declineLightOnGraph(1);
        if (positions.len != claimed.len * 3) return self.declineLightOnGraph(1);
        const kv_lock = items[0].state.kv_lock orelse return self.declineLightOnGraph(1);
        for (items) |item| if (item.state.kv_lock != kv_lock) return self.declineLightOnGraph(1);
        platform.sync.lockYielding(kv_lock);
        defer kv_lock.unlock();
        var future: [128]usize = undefined;
        var max_future: usize = 0;
        for (claimed, items, 0..) |item, mixed, i| {
            const work: *PendingDecodeBatchWork = @ptrCast(@alignCast(item.work_ptr));
            if (item.phase != .decode or item.query_sequence_len != 1 or !work.wants_greedy or
                work.continuation_context == null or work.remaining_tokens < 2 or
                mixed.state.kv_manager != manager or mixed.state.kv_storage != storage or mixed.state.sequence_id == null or
                mixed.state.force_full_recompute or mixed.state.kv_compacted or mixed.state.allow_swa_ring or
                mixed.kv_position_offset != 0 or mixed.kv_sequence_len != mixed.total_sequence_len or
                mixed.state.qwen35_linear_cache == null) return self.declineLightOnGraph(2);
            const remaining = work.remaining_tokens;
            future[i] = try std.math.add(usize, item.total_sequence_len, remaining);
            if (future[i] > @import("../ops/cuda/paged_attention_bf16.zig").max_native_context_tokens) return self.declineLightOnGraph(3);
            if (storage.storage.config.sliding_window_size) |window|
                if (future[i] > window) return self.declineLightOnGraph(3);
            max_future = @max(max_future, future[i]);
        }
        const embedding = try gpt_arch.getEmbeddingWeight(self.context.cb, self.context.gpt_config.*);
        defer self.context.cb.free(embedding);
        const head = try gpt_arch.getLmHeadWeight(self.context.cb, self.context.gpt_config.*);
        defer self.context.cb.free(head);
        const embedding_pointer = C.lightOnGraphWeightPointer(self.context.cb, embedding) orelse return self.declineLightOnGraph(4);
        const head_pointer = C.lightOnGraphWeightPointer(self.context.cb, head) orelse return self.declineLightOnGraph(4);
        try key.add(self.lighton_cuda_execution_id);
        try key.add(claimed.len);
        try key.add(self.context.gpt_config.hidden_size);
        try key.add(self.context.gpt_config.vocab_size);
        try key.add(embedding_pointer);
        try key.add(head_pointer);
        try key.add(C.lightOnGraphWorkspacePointer(self.context.cb));
        var attention_layers: [128]usize = undefined;
        var attention_count: usize = 0;
        const first_cache = &items[0].state.qwen35_linear_cache.?;
        for (first_cache.layers, 0..) |*layer, layer_index| {
            if (!self.context.gpt_config.layerUsesQwen35LinearAttention(layer_index)) {
                if (attention_count == attention_layers.len) return self.declineLightOnGraph(5);
                // Native attention addresses the compact full-attention KV
                // layers; recurrent decoder layers have no KV storage slot.
                attention_layers[attention_count] = self.context.gpt_config.compactQwen35KvStorageLayerIndex(layer_index) orelse return self.declineLightOnGraph(5);
                attention_count += 1;
                continue;
            }
            for (items, 0..) |item, row| {
                const cache = &item.state.qwen35_linear_cache.?;
                if (cache.layers.len != first_cache.layers.len) return self.declineLightOnGraph(5);
                const state = &cache.layers[layer_index].cuda_device;
                if (!C.lightOnGraphStateOwned(self.context.cb, state)) return self.declineLightOnGraph(5);
                if (row % 64 == 0) for (state.weights) |weight| try key.add(weight.ptr);
                // BF16 convolution filters are widened into independently
                // owned page copies. The batch kernel already uses the first
                // page's copy for all peers; prove the immutable source IDs,
                // while the key above retains the actual captured pointers.
                for (state.weight_sources, layer.cuda_device.weight_sources) |source, expected|
                    if (source == 0 or source != expected) return self.declineLightOnGraph(6);
                try key.add(state.recurrent.ptr);
                try key.add(state.history[0].ptr);
                try key.add(state.history[1].ptr);
            }
        }
        // Reserve only the originally authorized remaining generation budget.
        // This adds physical capacity without making a future token visible.
        // Each captured launch snapshots a bounded flat block table, so long
        // pages share its capacity instead of each being capped at 64 blocks.
        var packet_blocks: usize = 0;
        for (items, 0..) |item, i| {
            if (i % 64 == 0) packet_blocks = 0;
            const state = item.state;
            const sequence = state.sequence_id.?;
            const table = storage.blockTable(sequence) orelse return self.declineLightOnGraph(1);
            if (table.shared_prefix_blocks != 0) return self.declineLightOnGraph(3);
            try manager.reserveTokenCapacity(sequence, future[i]);
            try storage.reserveDeviceTokenCapacity(sequence, future[i], future[i]);
            const blocks = try storage.logicalBlocksWithReservations(sequence);
            const packet_limit = @import("../ops/cuda/paged_attention_bf16.zig").max_blocks;
            if (blocks.len == 0 or blocks.len > packet_limit - packet_blocks) return self.declineLightOnGraph(3);
            if (key.len > key.words.len - 3 or blocks.len > key.words.len - key.len - 3) return self.declineLightOnGraph(3);
            packet_blocks += blocks.len;
            try key.add(sequence);
            try key.add(future[i]);
            try key.add(blocks.len);
            for (blocks) |block| try key.add(block);
        }
        var kv_words: [193]u64 = undefined;
        const kv_count = try C.lightOnGraphKvWords(self.context.cb, storage, items[0].state.sequence_id.?, max_future, attention_layers[0..attention_count], &kv_words);
        for (kv_words[0..kv_count]) |word| try key.add(word);
        const prepared = (try C.beginLightOnDecodeGraph(self.context.cb, self.lighton_cuda_execution_id, key.words[0..key.len], positions, claimed.len, self.context.gpt_config.hidden_size, self.context.gpt_config.vocab_size, max_future, attention_count, self.context.gpt_config.num_hidden_layers - attention_count)) orelse return self.declineLightOnGraph(7);
        var graph = prepared;
        defer graph.deinit();
        errdefer {
            for (items) |item| if (item.state.qwen35_linear_cache) |cache| for (cache.layers) |*layer| {
                layer.cuda_device.failed = true;
            };
            graph.compute.lighton_decode_graph.clear(&graph.compute.ctx) catch {};
        }
        var burst = try graph.burst(embedding, self.context.gpt_config.vocab_size);
        defer burst.deinit();
        const first = try self.context.allocator.alloc(u32, claimed.len);
        errdefer self.context.allocator.free(first);
        const second = try self.context.allocator.alloc(u32, claimed.len);
        errdefer self.context.allocator.free(second);
        const embedded = try self.context.cb.embeddingLookup(embedding, input_ids, claimed.len, self.context.gpt_config.hidden_size);
        defer self.context.cb.free(embedded);
        try graph.setInput(embedded);
        const Render = struct {
            pipeline: *Executor,
            items: []MixedBatchDecodeItem,
            max_tokens: usize = 0,
            burst: *C.LightOnTokenBurst,
            head: ops.CT,
            fn advanceCpu(render: *@This()) !void {
                for (render.items) |*item| {
                    try item.state.appendGeneratedTokenWithKvLockHeld();
                    item.total_sequence_len += 1;
                    var decode = BorrowedDecodeStateRuntime.init(item.state);
                    const context = decode.makeDecodeContext(item.total_sequence_len, 1);
                    item.kv_sequence_len = context.kv_sequence_len;
                    item.kv_position_offset = context.kv_position_offset;
                    render.max_tokens = @max(render.max_tokens, item.kv_sequence_len);
                }
            }
            fn run(raw: *anyopaque, claim: *C.LightOnGraphClaim) !void {
                const render: *@This() = @ptrCast(@alignCast(raw));
                const pipeline = render.pipeline;
                for (0..2) |step| {
                    try pipeline.context.cb.checkExecutionControl();
                    if (step == 1) try render.advanceCpu();
                    const metadata_bytes = 128 * (@sizeOf(gpt_arch.DecodeContext.KvBatchView) + @sizeOf(?*gpt_arch.Qwen35LinearCache) + 3 * @sizeOf(u32)) + 128;
                    var metadata: [metadata_bytes]u8 align(@alignOf(gpt_arch.DecodeContext.KvBatchView)) = undefined;
                    var fba = std.heap.FixedBufferAllocator.init(&metadata);
                    const lengths: [128]usize = @splat(1);
                    var context = try buildOwnedLightOnGraphDecodeContext(fba.allocator(), render.items, lengths[0..render.items.len]);
                    defer context.deinit();
                    context.context.prepared_lighton_decode = true;
                    claim.beginStep(step);
                    const input = if (step == 0) try claim.input() else try render.burst.embedding(0);
                    const hidden = try gpt_arch.forwardFinalHiddenTensorFromEmbeddingsWithLayer0Overrides(pipeline.context.cb, pipeline.context.allocator, pipeline.context.gpt_config.*, input, .{}, if (render.items.len > 64) 1 else render.items.len, if (render.items.len > 64) render.items.len else 1, &context.context, null);
                    defer pipeline.context.cb.free(hidden.hidden);
                    const logits = try claim.projectHead(pipeline.context.cb, hidden.hidden, render.head);
                    defer pipeline.context.cb.free(logits);
                    try render.burst.argmax(logits, step);
                    try claim.advance();
                }
            }
        };
        var render = Render{ .pipeline = self, .items = items, .burst = &burst, .head = head };
        if (graph.compute.lighton_decode_graph.phase == .replay) try render.advanceCpu();
        try self.context.cb.checkExecutionControl();
        try graph.run(&render, Render.run);
        graph.noteKvTokens(storage, render.max_tokens);
        var host: [256]u32 = undefined;
        try graph.read(&burst, host[0 .. 2 * claimed.len]);
        try self.context.cb.checkExecutionControl();
        @memcpy(first, host[0..claimed.len]);
        @memcpy(second, host[claimed.len .. 2 * claimed.len]);
        burst.noteCompleted();
        return .{ .logits = null, .greedy_tokens = first, .second_greedy_tokens = second, .max_query_seq_len = 1 };
    }

    fn checkPreparedQwenContinuation(self: *const Executor) !void {
        if (self.continuation) |control| try control.check();
    }

    /// Validate every chunk against its original immutable visual prompt before
    /// reserving any peer KV or modifying any recurrent history.
    pub fn preparedQwenPrefillStart(work: *const PendingPrefillBatchWork, item: runtime.scheduler.native_generate.StepItem) !usize {
        return work_mod.preparedPrefixStart(work, item);
    }

    fn copyPreparedQwenChunkPositions(work: *const PendingPrefillBatchWork, start: usize, output: []u32, total: usize, offset: usize) void {
        work_mod.copyPrefixPositions(work, start, output, total, offset);
    }

    fn forwardPackedScheduledQwenStep(self: *Executor, claimed: []const runtime.scheduler.native_generate.StepItem) !StepForwardResult {
        const prefill_limit = packedQwenPrefillItemLimit(self.context.cb.kind(), platform.env.getenvBool("ANTFLY_LIGHTONOCR3_CUDA_WIDE_MICROBATCHES"));
        if (claimed.len == 0 or claimed.len > (if (self.context.cb.kind() == .cuda and platform.env.getenvBool("ANTFLY_LIGHTONOCR3_PHYSICAL128")) @as(usize, 128) else 64) or self.packed_prefill_items == 0 or self.packed_prefill_items > prefill_limit or !self.context.gpt_config.hasQwen35VisionArchitectureContract()) return error.InvalidPackedQueryShape;
        const a = self.context.allocator;
        const cb = self.context.cb;
        const cfg = self.context.gpt_config.*;
        var lengths: [gpt_arch.max_packed_qwen_items]usize = undefined;
        var offsets: [gpt_arch.max_packed_qwen_items]usize = undefined;
        var prefill_starts: [gpt_arch.max_packed_qwen_items]usize = @splat(0);
        var prefix_starts: usize = 0;
        var prefix_finishes: usize = 0;
        var mixed: [gpt_arch.max_packed_qwen_items]MixedBatchDecodeItem = undefined;
        var reserved: [gpt_arch.max_packed_qwen_items]usize = @splat(0);
        var total: usize = 0;
        var prefill_count: usize = 0;
        const cuda_packed = cb.kind() == .cuda;
        var all_decode_greedy = cb.kind() == .metal or cuda_packed;
        // Validate the entire claim before reserving or updating any page.
        for (claimed, 0..) |item, index| {
            lengths[index] = item.query_sequence_len;
            offsets[index] = total;
            total = try std.math.add(usize, total, item.query_sequence_len);
            switch (item.phase) {
                .prefill => {
                    prefill_count += 1;
                    const work: *PendingPrefillBatchWork = @ptrCast(@alignCast(item.work_ptr));
                    const prompt = work.prepared_prefix orelse return error.InvalidPreparedPrompt;
                    prefill_starts[index] = try preparedQwenPrefillStart(work, item);
                    prefix_starts += @intFromBool(prefill_starts[index] == 0);
                    prefix_finishes += @intFromBool(work.seq_len == prompt.token_count);
                    mixed[index] = .{ .state = work.decode_state, .total_sequence_len = item.total_sequence_len, .query_sequence_len = item.query_sequence_len, .kv_sequence_len = item.kv_sequence_len, .kv_position_offset = item.kv_position_offset, .attention_mode = .paged_prefill };
                },
                .decode => {
                    if (item.query_sequence_len != 1) return error.InvalidPackedQueryShape;
                    const work: *PendingDecodeBatchWork = @ptrCast(@alignCast(item.work_ptr));
                    all_decode_greedy = all_decode_greedy and work.wants_greedy;
                    mixed[index] = .{ .state = work.decode_state, .total_sequence_len = item.total_sequence_len, .query_sequence_len = 1, .kv_sequence_len = item.kv_sequence_len, .kv_position_offset = item.kv_position_offset, .attention_mode = .paged_decode };
                },
                .waiting => return error.InvalidPackedQueryShape,
            }
            for (mixed[0..index]) |prior| {
                if (prior.state == mixed[index].state) return error.InvalidPackedQueryShape;
                if (prior.state.kv_manager == mixed[index].state.kv_manager and prior.state.sequence_id != null and
                    prior.state.sequence_id == mixed[index].state.sequence_id) return error.InvalidPackedQueryShape;
            }
        }
        // Admission covers the complete configured prefill group and all
        // remaining physical decode rows, including unequal page lengths.
        const wide_cuda_decode = cuda_packed and claimed.len > 64 and
            platform.env.getenvBool("ANTFLY_LIGHTONOCR3_PHYSICAL128");
        if ((prefill_count == 0 and !wide_cuda_decode) or prefill_count > self.packed_prefill_items or try gpt_arch.packedQueryRowCount(lengths[0..claimed.len]) != total) return error.InvalidPackedQueryShape;
        if (comptime build_options.enable_cuda) {
            // all_decode_greedy describes decode peers even in a mixed
            // prefill claim. Only a completely decoded cohort may replay.
            if (wide_cuda_decode and prefill_count == 0 and all_decode_greedy and self.lighton_cuda_execution_id != 0) {
                var ids: [gpt_arch.max_packed_qwen_items]i64 = undefined;
                for (claimed, 0..) |item, i| {
                    const work: *PendingDecodeBatchWork = @ptrCast(@alignCast(item.work_ptr));
                    ids[i] = work.token_id;
                }
                const metadata_bytes = 128 * (@sizeOf(gpt_arch.DecodeContext.KvBatchView) + @sizeOf(?*gpt_arch.Qwen35LinearCache) + 3 * @sizeOf(u32)) + 128;
                var metadata: [metadata_bytes]u8 align(@alignOf(gpt_arch.DecodeContext.KvBatchView)) = undefined;
                var fba = std.heap.FixedBufferAllocator.init(&metadata);
                var context = try buildOwnedLightOnGraphDecodeContext(fba.allocator(), mixed[0..claimed.len], lengths[0..claimed.len]);
                defer context.deinit();
                context.context.prepared_lighton_decode = true;
                if (try self.tryLightOnCudaDecodeGraph(claimed, mixed[0..claimed.len], ids[0..claimed.len], &context.context)) |result| return result;
                @import("../ops/cuda/cuda_compute.zig").CudaCompute.invalidateLightOnDecodeGraph(self.context.cb);
                // Eager decode introduces shapes absent from a mixed prefix
                // frame; reserve bounded plan slots before enqueueing them.
                try @import("../ops/cuda/cuda_compute.zig").CudaCompute.prepareLightOnMatmulFrame(self.context.cb);
                if (try self.tryLightOnCudaTokenBurst(claimed, mixed[0..claimed.len], ids[0..claimed.len], &context.context)) |result| return result;
            }
        }
        // The combined CPU logits and independent result copies coexist during
        // publication. Admit that complete lifetime before allocating metadata.
        var gpu_choices = false;
        if (comptime build_options.enable_metal) {
            if (all_decode_greedy and prefill_count < claimed.len and !cb.decoderRuntimeHasActiveFrame())
                gpu_choices = @import("../ops/metal_compute.zig").MetalCompute.reserveLightOnFrameArgmax(cb, claimed.len, cfg.vocab_size);
        }
        var greedy_prefills: usize = 0;
        for (claimed) |item| if (item.phase == .prefill) {
            const work: *PendingPrefillBatchWork = @ptrCast(@alignCast(item.work_ptr));
            if (work.wants_greedy) greedy_prefills += 1;
        };
        if (cuda_packed) gpu_choices = all_decode_greedy and (prefill_count < claimed.len or greedy_prefills > 0);
        const prefix_logits_layout: PrefixLogitsLayout = if (cuda_packed and gpu_choices) .final_host_rows else .all_host_rows;
        var host_prefills: usize = 0;
        var active_heads: [packed_head_rows.capacity]bool = @splat(true);
        for (claimed, 0..) |item, index| if (item.phase == .prefill) {
            const work: *PendingPrefillBatchWork = @ptrCast(@alignCast(item.work_ptr));
            host_prefills += @intFromBool(prefillHasHostRow(work, prefix_logits_layout));
            if (cuda_packed and gpu_choices) active_heads[index] = work.wants_last_logits;
        };
        const head_rows = try packed_head_rows.Plan.init(active_heads[0..claimed.len]);
        const needs_logits = scheduledStepNeedsLogits(claimed);
        const shared_frame = !cuda_packed and self.packed_shared_frame and gpu_choices;
        const trace_frame = platform.env.getenvBool("ANTFLY_BENCH_LIGHTONOCR3");
        const before_submits = if (trace_frame) cb.debugTimingSnapshot().provider.decoder_runtime_frame_submits else 0;
        // Device-greedy rows return token IDs. Only host-sampled prefixes
        // retain vocabulary rows and their independent publication copies.
        const host_rows = if (!needs_logits) 0 else if (gpu_choices) host_prefills else claimed.len;
        const logits_bytes = try std.math.mul(usize, try std.math.mul(usize, host_rows, cfg.vocab_size), 2 * @sizeOf(f32));
        const positions_bytes = try std.math.mul(usize, total, 3 * @sizeOf(u32));
        const head_metadata_bytes = @sizeOf(packed_head_rows.Plan) + @sizeOf(@TypeOf(active_heads)) + claimed.len * @sizeOf(u32);
        const host_bytes = try std.math.add(usize, try std.math.add(usize, logits_bytes, positions_bytes), (if (claimed.len > 64) @as(usize, 64) else 32) * 1024 + head_metadata_bytes);
        var recurrent_groups: usize = 0;
        var recurrent_rows: usize = 0;
        var attention_groups: usize = 0;
        var attention_rows: usize = 0;
        var forward_completed = false;
        defer if (forward_completed) {
            if (platform.env.getenvBool("ANTFLY_BENCH_LIGHTONOCR3")) std.debug.print("LIGHTONOCR3 packed_step_prefill_items={d} packed_step_decode_items={d} packed_step_query_rows={d}\n", .{ prefill_count, claimed.len - prefill_count, total });
            if (platform.env.getenvBool("ANTFLY_BENCH_LIGHTONOCR3")) std.debug.print("LIGHTONOCR3 packed_step_prefill_starts={d} packed_step_prefill_finishes={d} prepared_prefill_chunk_rows={d}\n", .{ prefix_starts, prefix_finishes, self.prepared_qwen_prefill_chunk_rows });
            if (platform.env.getenvBool("ANTFLY_BENCH_LIGHTONOCR3")) std.debug.print("LIGHTONOCR3 packed_recurrent_groups={d} packed_recurrent_rows={d}\n", .{ recurrent_groups, recurrent_rows });
            if (platform.env.getenvBool("ANTFLY_BENCH_LIGHTONOCR3")) std.debug.print("LIGHTONOCR3 packed_attention_groups={d} packed_attention_rows={d}\n", .{ attention_groups, attention_rows });
            if (platform.env.getenvBool("ANTFLY_BENCH_LIGHTONOCR3")) std.debug.print("LIGHTONOCR3 packed_input_frames={d} packed_cuda_steps={d}\n", .{ @intFromBool(!cuda_packed), @intFromBool(cuda_packed) });
            if (platform.env.getenvBool("ANTFLY_BENCH_LIGHTONOCR3")) std.debug.print("LIGHTONOCR3 packed_gpu_decode_rows={d} packed_cpu_logit_rows={d} packed_standalone_wide_heads={d}\n", .{ if (needs_logits and gpu_choices) claimed.len - prefill_count else 0, host_rows, @intFromBool(needs_logits and !gpu_choices and claimed.len > 16) });
            if (trace_frame) std.debug.print("LIGHTONOCR3 packed_shared_frame={d} packed_forward_submissions={d}\n", .{ @intFromBool(shared_frame), cb.debugTimingSnapshot().provider.decoder_runtime_frame_submits -| before_submits });
            if (platform.env.getenvBool("ANTFLY_BENCH_LIGHTONOCR3")) std.debug.print("LIGHTONOCR3 packed_gpu_prefill_rows={d} packed_skipped_prefix_heads={d}\n", .{ if (needs_logits and gpu_choices) greedy_prefills else 0, if (!needs_logits) prefill_count else claimed.len - head_rows.count });
        };
        const session = self.context.session orelse return error.InvalidBatchDecodeState;
        var host_lease = try session.admitHostPreprocess(host_bytes);
        errdefer host_lease.deinit();
        const positions = try a.alloc(u32, try std.math.mul(usize, total, 3));
        defer a.free(positions);
        var reserved_count: usize = 0;
        errdefer rollbackPrefillReservations(claimed, &reserved, reserved_count);
        for (claimed, 0..) |item, index| {
            if (item.phase == .prefill) {
                const work: *PendingPrefillBatchWork = @ptrCast(@alignCast(item.work_ptr));
                reserved[index] = try reserveAndRefreshScheduledPrefill(&mixed[index], work.seq_len);
                reserved_count = index + 1;
                copyPreparedQwenChunkPositions(work, prefill_starts[index], positions, total, offsets[index]);
            } else {
                var state = BorrowedDecodeStateRuntime.init(mixed[index].state);
                const context = state.makeDecodeContext(item.total_sequence_len, 1);
                const axes = context.mrope_positions orelse return error.Qwen3VlMropePositionsRequired;
                for (0..3) |axis| positions[axis * total + offsets[index]] = axes[axis];
            }
        }
        var owned = try buildOwnedMixedBatchDecodeContextLayout(a, mixed[0..claimed.len], positions, lengths[0..claimed.len]);
        defer owned.deinit();
        owned.context.packed_recurrent_groups = &recurrent_groups;
        owned.context.packed_recurrent_rows = &recurrent_rows;
        owned.context.packed_attention_groups = &attention_groups;
        owned.context.packed_attention_rows = &attention_rows;
        // Input embeddings and row copies are device operations too. Capture
        // them in one bounded producer frame. The qualified shared-frame path
        // lends that frame to the model and closes it after the F32 head.
        try cb.decoderRuntimeWaitSubmittedFrame();
        var input_frame_active = if (cuda_packed) false else try cb.decoderRuntimeBeginFrame();
        if (!cuda_packed and !input_frame_active) return error.ResidentImageOperationUnavailable;
        defer if (input_frame_active) cb.decoderRuntimeCancelFrame() catch {};
        try cb.decoderRuntimeSetActiveFrameRegime(.prefill);
        const combined = (try cb.allocUninitF32Shape(&.{ @intCast(total), @intCast(cfg.hidden_size) })) orelse return error.UnsupportedTensorType;
        var consumed = false;
        defer if (!consumed) cb.free(combined);
        var embedding_weight: ?ops.CT = null;
        defer if (embedding_weight) |weight| cb.free(weight);
        var input_index: usize = 0;
        while (input_index < claimed.len) {
            const item = claimed[input_index];
            if (item.phase == .prefill) {
                const work: *PendingPrefillBatchWork = @ptrCast(@alignCast(item.work_ptr));
                if (!try cb.copyRows2D(a, combined, offsets[input_index], work.prepared_prefix.?.embeddings.?, prefill_starts[input_index], lengths[input_index], cfg.hidden_size)) return error.UnsupportedTensorType;
                input_index += 1;
            } else {
                var count: usize = 1;
                while (input_index + count < claimed.len and claimed[input_index + count].phase == .decode) : (count += 1) {}
                var ids: [gpt_arch.max_packed_qwen_items]i64 = undefined;
                for (claimed[input_index..][0..count], 0..) |decode, row| {
                    const work: *PendingDecodeBatchWork = @ptrCast(@alignCast(decode.work_ptr));
                    ids[row] = work.token_id;
                }
                if (embedding_weight == null) embedding_weight = try gpt_arch.getEmbeddingWeight(cb, cfg);
                const embedding = try cb.embeddingLookup(embedding_weight.?, ids[0..count], count, cfg.hidden_size);
                defer cb.free(embedding);
                if (!try cb.copyRows2D(a, combined, offsets[input_index], embedding, 0, count, cfg.hidden_size)) return error.UnsupportedTensorType;
                input_index += count;
            }
        }
        if (input_frame_active and !shared_frame) {
            try cb.decoderRuntimeSubmitAndWaitFrame();
            input_frame_active = false;
        }
        consumed = true;
        const hidden = try gpt_arch.forwardFinalHiddenTensorFromEmbeddingsWithLayer0Overrides(cb, a, cfg, combined, .{}, 1, total, &owned.context, null);
        defer cb.free(hidden.hidden);
        if (hidden.total_rows != total) return error.InvalidTensorShape;
        forward_completed = true;
        if (!needs_logits) return .{ .logits = null, .max_query_seq_len = total, .last_rows_only = true, .host_permit = host_lease };

        // Intermediate prefix rows update KV/state but never produce a
        // choice. Exclude them from the head and restore scheduler order
        // after argmax. Metal retains its established complete-row layout.
        if (head_rows.count == 0) return error.InvalidBatchDecodeState;
        var last_indices: [gpt_arch.max_packed_qwen_items]i64 = undefined;
        for (head_rows.items[0..head_rows.count], 0..) |item, row| last_indices[row] = @intCast(offsets[item] + lengths[item] - 1);
        const last = try cb.embeddingLookup(hidden.hidden, last_indices[0..head_rows.count], head_rows.count, cfg.hidden_size);
        defer cb.free(last);
        const head = try gpt_arch.getLmHeadWeight(cb, cfg);
        defer cb.free(head);
        // Decode regime preserves the established F32 head inputs. The shared
        // path transfers ownership instead of opening a separate head frame.
        var head_frame_active = false;
        defer if (head_frame_active) cb.decoderRuntimeCancelFrame() catch {};
        if (gpu_choices and !cuda_packed) {
            if (shared_frame) {
                // Transfer the producer/model frame's sole cancellation owner.
                // The head still uses F32 inputs under the decode regime.
                head_frame_active = true;
                input_frame_active = false;
            } else {
                head_frame_active = try cb.decoderRuntimeBeginFrame();
                if (!head_frame_active) return error.ResidentImageOperationUnavailable;
            }
            try cb.decoderRuntimeSetActiveFrameRegime(.decode);
        }
        const logits = try cb.linearNoBias(last, head, head_rows.count, cfg.hidden_size, cfg.vocab_size);
        defer cb.free(logits);
        // Gather prefix logits before argmax closes the borrowed frame. Only
        // host arrays escape this forward; all device views retire below.
        var gathered_prefix_logits: ?ops.CT = null;
        defer if (gathered_prefix_logits) |tensor| cb.free(tensor);
        if (shared_frame and host_rows != 0) {
            var rows: [gpt_arch.max_packed_qwen_items]i64 = undefined;
            var count: usize = 0;
            for (claimed, 0..) |item, index| if (item.phase == .prefill and prefillHasHostRow(@ptrCast(@alignCast(item.work_ptr)), prefix_logits_layout)) {
                rows[count] = @intCast(head_rows.row_by_item[index]);
                count += 1;
            };
            gathered_prefix_logits = try cb.embeddingLookup(logits, rows[0..count], count, cfg.vocab_size);
        }
        var tokens: ?[]u32 = null;
        errdefer if (tokens) |choices| a.free(choices);
        if (comptime build_options.enable_metal) {
            if (gpu_choices) {
                try @import("../ops/metal_compute.zig").MetalCompute.ensureFrameArgmaxScope(cb);
                tokens = try cb.decoderRuntimeArgmaxRowsSuppress(logits, 0, claimed.len, cfg.vocab_size, cfg.suppressTokenIds(), a) orelse return error.ResidentImageOperationUnavailable;
                head_frame_active = false;
                if (tokens.?.len != claimed.len) return error.InvalidBatchDecodeState;
            }
        }
        if (cuda_packed and gpu_choices) {
            // Greedy prefix and decode rows share the existing suppressed
            // device argmax. Sampled prefixes retain admitted F32 logits.
            const compact = blk: {
                if (host_rows == 0) if (try self.tryReadLightOnCudaPackedChoices(logits, head_rows.count)) |choices| break :blk choices;
                break :blk try cb.argmaxRowsSuppress(logits, 0, head_rows.count, cfg.vocab_size, cfg.suppressTokenIds(), a) orelse return error.ResidentImageOperationUnavailable;
            };
            if (head_rows.count == claimed.len) {
                tokens = compact;
                if (compact.len != claimed.len) return error.InvalidBatchDecodeState;
            } else {
                defer a.free(compact);
                tokens = try head_rows.scatter(a, compact, cfg.vocab_size);
            }
        }
        // A fully device-greedy packet has no vocabulary rows for the CPU.
        // Keep an owned empty slice for the existing publication contract;
        // do not dispatch an embedding gather with zero rows.
        const host = if (gpu_choices and host_rows == 0) try a.alloc(f32, 0) else if (gathered_prefix_logits) |tensor| try cb.toFloat32(tensor, a) else if (gpu_choices) blk: {
            var rows: [gpt_arch.max_packed_qwen_items]i64 = undefined;
            var count: usize = 0;
            for (claimed, 0..) |item, index| if (item.phase == .prefill and prefillHasHostRow(@ptrCast(@alignCast(item.work_ptr)), prefix_logits_layout)) {
                rows[count] = @intCast(head_rows.row_by_item[index]);
                count += 1;
            };
            const prefix_logits = try cb.embeddingLookup(logits, rows[0..count], count, cfg.vocab_size);
            defer cb.free(prefix_logits);
            break :blk try cb.toFloat32(prefix_logits, a);
        } else try cb.toFloat32(logits, a);
        gpt_arch.applyFinalLogitSoftcapInPlace(cfg, host);
        return .{ .logits = host, .packed_greedy_tokens = tokens, .prefix_logits_layout = prefix_logits_layout, .max_query_seq_len = total, .last_rows_only = true, .host_permit = host_lease };
    }

    pub fn forward(
        self: *Executor,
        claimed: []const runtime.scheduler.native_generate.StepItem,
    ) !StepForwardResult {
        if (comptime build_options.enable_cuda) {
            if (self.lighton_cuda_execution_id != 0) {
                var pure_decode = claimed.len >= 2 and claimed.len <= (if (platform.env.getenvBool("ANTFLY_LIGHTONOCR3_PHYSICAL128")) @as(usize, 128) else 64);
                for (claimed) |item| {
                    pure_decode = pure_decode and item.phase == .decode and item.query_sequence_len == 1;
                    if (item.phase == .decode) {
                        const work: *PendingDecodeBatchWork = @ptrCast(@alignCast(item.work_ptr));
                        pure_decode = pure_decode and work.wants_greedy;
                    }
                }
                if (!pure_decode) {
                    const C = @import("../ops/cuda/cuda_compute.zig").CudaCompute;
                    C.invalidateLightOnDecodeGraph(self.context.cb);
                    try C.prepareLightOnMatmulFrame(self.context.cb);
                }
            }
        }
        for (claimed) |item| if (item.phase == .prefill) {
            const work: *PendingPrefillBatchWork = @ptrCast(@alignCast(item.work_ptr));
            if (work.prepared_prefix != null) return self.forwardPackedScheduledQwenStep(claimed);
        };
        // Keep the large CUDA cohort in compact layout for both recurrent
        // packets and the existing per-page paged-attention partitions.
        if (claimed.len > 64 and claimed.len <= gpt_arch.max_packed_qwen_items and
            self.context.cb.kind() == .cuda and self.graph_cache == null and self.context.compiled_partition_backend == null and
            self.context.gpt_config.hasQwen35VisionArchitectureContract() and platform.env.getenvBool("ANTFLY_LIGHTONOCR3_PACKED_MIXED") and
            platform.env.getenvBool("ANTFLY_LIGHTONOCR3_PHYSICAL128"))
            return self.forwardPackedScheduledQwenStep(claimed);
        // Canonical reader epochs own the CUDA execution lifetime too. Requiring
        // the diagnostic flag here silently downloaded full vocabulary logits
        // instead of selecting greedy tokens on the device for those epochs.
        const cuda_lighton = self.context.cb.kind() == .cuda and self.graph_cache == null and self.context.compiled_partition_backend == null and self.context.gpt_config.hasQwen35VisionArchitectureContract() and
            (self.lighton_cuda_execution_id != 0 or platform.env.getenvBool("ANTFLY_LIGHTONOCR3_CUDA_BATCH")) and @import("../architectures/qwen35_execution.zig").cudaCoreEnabled();
        const prepared_decode = self.context.gpt_config.hasQwen35VisionArchitectureContract() and claimed.len <= 64 and
            platform.env.getenvBoolDefault("ANTFLY_LIGHTONOCR3_DECODE_WORKSPACE", self.prepared_lighton_decode or cuda_lighton) and blk: {
            for (claimed) |item| if (item.phase != .decode or item.query_sequence_len != 1) break :blk false;
            break :blk true;
        };
        // Bounded decode geometry never needs heap-backed host metadata. The
        // forward completes before these views retire, exactly like the owned
        // context it replaces. Mixed/prefill and other families keep their path.
        var item_slots: [64]MixedBatchDecodeItem = undefined;
        const items = if (prepared_decode) item_slots[0..claimed.len] else try self.context.allocator.alloc(MixedBatchDecodeItem, claimed.len);
        defer if (!prepared_decode) self.context.allocator.free(items);

        var max_query_seq_len: usize = 0;
        for (claimed) |item| max_query_seq_len = @max(max_query_seq_len, item.query_sequence_len);

        const pad_id: i64 = self.pad_token;
        var input_slots: [64]i64 = undefined;
        const input_ids = if (prepared_decode) input_slots[0..claimed.len] else try self.context.allocator.alloc(i64, claimed.len * max_query_seq_len);
        defer if (!prepared_decode) self.context.allocator.free(input_ids);
        @memset(input_ids, pad_id);

        for (claimed, 0..) |item, idx| {
            switch (item.phase) {
                .decode => {
                    const work: *PendingDecodeBatchWork = @ptrCast(@alignCast(item.work_ptr));
                    items[idx] = .{
                        .state = work.decode_state,
                        .total_sequence_len = item.total_sequence_len,
                        .query_sequence_len = item.query_sequence_len,
                        .kv_sequence_len = item.kv_sequence_len,
                        .kv_position_offset = item.kv_position_offset,
                        .attention_mode = .paged_decode,
                    };
                    input_ids[idx * max_query_seq_len] = work.token_id;
                },
                .prefill => {
                    const work: *PendingPrefillBatchWork = @ptrCast(@alignCast(item.work_ptr));
                    items[idx] = .{
                        .state = work.decode_state,
                        .total_sequence_len = item.total_sequence_len,
                        .query_sequence_len = item.query_sequence_len,
                        .kv_sequence_len = item.kv_sequence_len,
                        .kv_position_offset = item.kv_position_offset,
                        .attention_mode = .paged_prefill,
                    };
                    @memcpy(input_ids[idx * max_query_seq_len ..][0..work.query_seq_len], work.token_ids);
                },
                .waiting => return error.InvalidBatchDecodeState,
            }
        }

        var reservation_slots: [64]usize = undefined;
        const reserved = if (prepared_decode) reservation_slots[0..claimed.len] else try self.context.allocator.alloc(usize, claimed.len);
        defer if (!prepared_decode) self.context.allocator.free(reserved);
        @memset(reserved, 0);

        // Track how many prefill items have been reserved so the errdefer
        // rollback only undoes reserves that were actually performed. The
        // counter is updated immediately after each successful reserve.
        var reserved_count: usize = 0;
        errdefer rollbackPrefillReservations(claimed, reserved, reserved_count);

        for (claimed, 0..) |item, idx| {
            if (item.phase != .prefill) continue;
            const work: *PendingPrefillBatchWork = @ptrCast(@alignCast(item.work_ptr));
            reserved[idx] = try reserveAndRefreshScheduledPrefill(&items[idx], work.seq_len);
            reserved_count = idx + 1;
        }

        // Preserve the scheduler's turn, execution lock, and reservation
        // rollback. Only the originating request can authorize pruning, and
        // only a singleton KV-only prefill may use the direct KV context.
        // Mixed batches and query/hidden-state consumers retain the full path.
        if (claimed.len == 1 and claimed[0].phase == .prefill) {
            const work: *PendingPrefillBatchWork = @ptrCast(@alignCast(claimed[0].work_ptr));
            if (work.allow_shared_kv_prefill and !work.wants_last_logits) {
                var decode_runtime = BorrowedDecodeStateRuntime.init(work.decode_state);
                const context = decode_runtime.makeDecodeContext(work.seq_len, work.query_seq_len);
                if (try gpt_arch.tryCudaSharedKvPrefill(self.context.cb, self.context.allocator, self.context.gpt_config.*, work.token_ids, work.seq_len, &context, false)) |_| {
                    return .{ .logits = null, .max_query_seq_len = max_query_seq_len };
                }
            }
        }

        const metadata_bytes = 64 * (@sizeOf(gpt_arch.DecodeContext.KvBatchView) + @sizeOf(?*gpt_arch.Qwen35LinearCache) + 3 * @sizeOf(u32)) + 128;
        var metadata: [metadata_bytes]u8 align(@alignOf(gpt_arch.DecodeContext.KvBatchView)) = undefined;
        var metadata_allocator = std.heap.FixedBufferAllocator.init(&metadata);
        var owned_ctx = try buildOwnedMixedBatchDecodeContext(if (prepared_decode) metadata_allocator.allocator() else self.context.allocator, items);
        defer owned_ctx.deinit();
        if (self.prepared_lighton_decode and max_query_seq_len == 1) {
            var only_decode = true;
            for (claimed) |item| only_decode = only_decode and item.phase == .decode;
            owned_ctx.context.prepared_lighton_decode = only_decode;
        }

        if (!scheduledStepNeedsLogits(claimed)) {
            if (try self.tryRunSingletonScheduledGemmaPrefillFrame(claimed, &owned_ctx.context)) {
                return .{ .logits = null, .max_query_seq_len = max_query_seq_len };
            }
            try gpt_arch.forwardHiddenOnly(self.context.cb, self.context.allocator, self.context.gpt_config.*, input_ids, claimed.len, max_query_seq_len, &owned_ctx.context);
            return .{ .logits = null, .max_query_seq_len = max_query_seq_len };
        }

        var all_greedy = (self.context.cb.kind() == .metal or cuda_lighton) and max_query_seq_len == 1;
        for (claimed) |item| {
            if (item.phase != .decode) {
                all_greedy = false;
                break;
            }
            const work: *PendingDecodeBatchWork = @ptrCast(@alignCast(item.work_ptr));
            all_greedy = all_greedy and work.wants_greedy;
        }
        if (all_greedy) {
            if (comptime build_options.enable_cuda) {
                if (cuda_lighton and self.lighton_cuda_execution_id != 0) {
                    if (try self.tryLightOnCudaDecodeGraph(claimed, items, input_ids, &owned_ctx.context)) |result| return result;
                    @import("../ops/cuda/cuda_compute.zig").CudaCompute.invalidateLightOnDecodeGraph(self.context.cb);
                }
                if (cuda_lighton) try @import("../ops/cuda/cuda_compute.zig").CudaCompute.prepareLightOnMatmulFrame(self.context.cb);
                if (cuda_lighton) if (try self.tryLightOnCudaTokenBurst(claimed, items, input_ids, &owned_ctx.context)) |result| return result;
            }
            if (comptime build_options.enable_metal) {
                const M = @import("../ops/metal_compute.zig").MetalCompute;
                if ((self.single_frame_lighton_decode or self.gpu_greedy_stream_lighton_decode) and self.context.gpt_config.hasQwen35VisionArchitectureContract() and claimed.len <= 64 and
                    !self.context.cb.decoderRuntimeHasActiveFrame() and M.reserveLightOnFrameArgmax(self.context.cb, claimed.len, self.context.gpt_config.vocab_size))
                {
                    const head = try self.graphWeight("lm_head.weight");
                    defer self.context.cb.free(head);
                    var burst_slot: ?usize = null;
                    var burst_steps: usize = std.math.clamp(@as(usize, self.gpu_token_burst_width), 2, max_gpu_token_burst_width);
                    if (self.gpu_token_burst_lighton_decode and self.single_frame_lighton_decode and
                        !self.gpu_greedy_stream_lighton_decode and self.access.mutex != null and
                        M.lightOnPreparedStreamReady(self.context.cb, self.context.gpt_config.*, claimed.len) and eligible: {
                        for (claimed) |item| {
                            const work: *PendingDecodeBatchWork = @ptrCast(@alignCast(item.work_ptr));
                            if (work.continuation_context == null or work.remaining_tokens < 2) break :eligible false;
                            burst_steps = @min(burst_steps, work.remaining_tokens);
                        }
                        break :eligible true;
                    }) {
                        const embedding = try self.graphWeight("model.embed_tokens.weight");
                        defer self.context.cb.free(embedding);
                        burst_slot = try M.lightOnBurstEmbeddingSlot(self.context.cb, embedding, self.context.gpt_config.hidden_size, self.context.gpt_config.vocab_size);
                    }
                    if (try self.context.cb.decoderRuntimeBeginFrame()) {
                        defer self.context.cb.decoderRuntimeCancelFrame() catch {};
                        try self.context.cb.decoderRuntimeSetActiveFrameRegime(.decode);
                        M.setLightOnExpectedStreamFrames(self.context.cb, 1);
                        const before_frames = self.context.cb.debugTimingSnapshot().provider.decoder_runtime_frame_submits;
                        const before_argmax = M.argmaxOnFrameCount(self.context.cb);
                        // Keep the existing serial encoder across prepared blocks.
                        // The backend retains concurrent hazard barriers and resets
                        // scope suppression when this owned frame completes/cancels.
                        if (platform.env.getenvBoolDefault("ANTFLY_LIGHTONOCR3_TOKEN_ENCODER", true) and
                            try self.context.cb.decoderRuntimeBeginPlannedComputeScope())
                            M.noteLightOnTokenEncoderScope(self.context.cb);
                        try self.context.cb.checkExecutionControl();
                        const hidden = try gpt_arch.forwardHiddenTensorWithCudaReplay(self.context.cb, self.context.allocator, self.context.gpt_config.*, input_ids, claimed.len, 1, &owned_ctx.context, "gpt.metal_batch_decode");
                        defer self.context.cb.free(hidden);
                        const logits = try self.context.cb.linearNoBias(hidden, head, claimed.len, self.context.gpt_config.hidden_size, self.context.gpt_config.vocab_size);
                        defer self.context.cb.free(logits);
                        if (burst_slot) |slot| return self.finishLightOnGpuTokenBurst(claimed, items, logits, head, slot, burst_steps, before_frames, before_argmax);
                        try M.ensureFrameArgmaxScope(self.context.cb);
                        // Never replay a forward after it has updated recurrent/KV
                        // state. A failure must fail this claimed step instead.
                        const tokens = try self.context.cb.decoderRuntimeArgmaxRowsSuppress(logits, 0, claimed.len, self.context.gpt_config.vocab_size, self.context.gpt_config.suppressTokenIds(), self.context.allocator) orelse return error.ResidentImageOperationUnavailable;
                        errdefer self.context.allocator.free(tokens);
                        if (tokens.len != claimed.len) return error.InvalidBatchDecodeState;
                        const frames = self.context.cb.debugTimingSnapshot().provider.decoder_runtime_frame_submits -| before_frames;
                        const on_frame = M.argmaxOnFrameCount(self.context.cb) -| before_argmax;
                        if (self.gpu_greedy_stream_lighton_decode)
                            M.noteLightOnGpuGreedyStream(self.context.cb, frames, on_frame)
                        else
                            M.noteLightOnSingleFrame(self.context.cb, frames, on_frame);
                        if (platform.env.getenvBool("TERMITE_METAL_STAGE_TIMING_LOG_FRAMES"))
                            std.debug.print("LIGHTONOCR3 profile_decode_rows={d}\n", .{claimed.len});
                        try self.context.cb.checkExecutionControl();
                        return .{ .logits = null, .greedy_tokens = tokens, .max_query_seq_len = 1 };
                    }
                }
            }

            if (self.context.cb.kind() == .metal and !platform.env.getenvBoolDefault("ANTFLY_GEMMA4_METAL_OCR_EAGER", false) and claimed.len <= 4) {
                var contexts: [4]gpt_arch.DecodeContext = undefined;
                var native_items: [4]decoder_gated_runtime.GreedyBatchItem = undefined;
                for (items, 0..) |item, row| {
                    var decode_runtime = BorrowedDecodeStateRuntime.init(item.state);
                    contexts[row] = decode_runtime.makeDecodeContext(item.total_sequence_len, 1);
                    native_items[row] = .{ .token_id = input_ids[row], .seq_len = item.total_sequence_len, .decode_context = &contexts[row] };
                }
                if (try decoder_gated_runtime.forwardGreedyTokensBatch(self.context.cb, self.context.allocator, self.context.gpt_config.*, self.context.gpt_config.num_hidden_layers, native_items[0..claimed.len])) |choices| {
                    if (platform.env.getenvBoolDefault("ANTFLY_GEMMA4_OCR_PROFILE", false)) std.log.info("metal_ocr_native_decode_rows={d}", .{claimed.len});
                    return .{ .logits = null, .greedy_tokens = choices, .max_query_seq_len = 1 };
                }
            }

            const hidden = try gpt_arch.forwardHiddenTensorWithCudaReplay(self.context.cb, self.context.allocator, self.context.gpt_config.*, input_ids, claimed.len, max_query_seq_len, &owned_ctx.context, "gpt.metal_batch_decode");
            defer self.context.cb.free(hidden);
            const weight = try self.graphWeight("lm_head.weight");
            defer self.context.cb.free(weight);
            const choices = if (cuda_lighton) blk: {
                const logits = try self.context.cb.linearNoBias(hidden, weight, claimed.len, self.context.gpt_config.hidden_size, self.context.gpt_config.vocab_size);
                defer self.context.cb.free(logits);
                break :blk try self.context.cb.argmaxRowsSuppress(logits, 0, claimed.len, self.context.gpt_config.vocab_size, self.context.gpt_config.suppressTokenIds(), self.context.allocator) orelse return error.ResidentImageOperationUnavailable;
            } else try self.context.cb.linearNoBiasArgmaxRowsSuppress(hidden, weight, claimed.len, self.context.gpt_config.hidden_size, self.context.gpt_config.vocab_size, self.context.gpt_config.suppressTokenIds(), self.context.allocator) orelse return error.ResidentImageOperationUnavailable;
            if (choices.len != claimed.len) {
                self.context.allocator.free(choices);
                return error.InvalidBatchDecodeState;
            }
            if (platform.env.getenvBoolDefault("ANTFLY_GEMMA4_OCR_PROFILE", false)) std.log.info("metal_ocr_decode_rows={d}", .{claimed.len});
            return .{ .logits = null, .greedy_tokens = choices, .max_query_seq_len = 1 };
        }

        if (cuda_lighton and claimed.len == 1 and claimed[0].phase == .prefill) {
            // Only the final prefix row is sampled. Avoid constructing and
            // downloading a vocabulary distribution for every image token.
            const logits = try gpt_arch.forwardLastLogitsWithCudaReplay(self.context.cb, self.context.allocator, self.context.gpt_config.*, input_ids, 1, max_query_seq_len, &owned_ctx.context, "gpt.cuda_lighton_prefill");
            return .{ .logits = logits, .max_query_seq_len = max_query_seq_len, .last_rows_only = true };
        }
        const logits = try self.forwardAllLogits(input_ids, claimed.len, max_query_seq_len, &owned_ctx.context);
        // Past this point the forward has committed: the prefill state is
        // valid and rollback would corrupt it. Returning normally cancels the
        // errdefer above.
        return .{ .logits = logits, .max_query_seq_len = max_query_seq_len };
    }

    /// Drive a claimed step end-to-end. Always calls `scheduler.completeStep`
    pub fn reserveScheduledPrefillState(decode_state: *NativeDecodeState, target_total_seq_len: usize) !usize {
        var decode_runtime = BorrowedDecodeStateRuntime.init(decode_state);
        return decode_runtime.reservePrefillTo(target_total_seq_len);
    }

    pub fn reserveAndRefreshScheduledPrefill(item: *MixedBatchDecodeItem, target_total_seq_len: usize) !usize {
        const reserved = try reserveScheduledPrefillState(item.state, target_total_seq_len);
        // The scheduler item captures KV geometry before this chunk is
        // reserved. Refresh it from the now-current state so paged KV writes
        // append the chunk instead of reusing the prior chunk's suffix range.
        refreshReservedPrefillGeometry(item);
        return reserved;
    }

    pub fn refreshReservedPrefillGeometry(item: *MixedBatchDecodeItem) void {
        var decode_runtime = BorrowedDecodeStateRuntime.init(item.state);
        const decode_ctx = decode_runtime.makeDecodeContext(item.total_sequence_len, item.query_sequence_len);
        item.kv_sequence_len = decode_ctx.kv_sequence_len;
        item.kv_position_offset = decode_ctx.kv_position_offset;
    }

    pub fn rollbackScheduledPrefillState(decode_state: *NativeDecodeState, reserved: usize) !void {
        var decode_runtime = BorrowedDecodeStateRuntime.init(decode_state);
        try decode_runtime.rollbackReservedPrefill(reserved);
    }

    /// Roll back any prefill reservations that have been completed so far.
    /// `reserved_count` is the number of items at the front of `claimed` that
    /// were processed by the reservation loop; reservations beyond that index
    /// were never made and must not be undone. Errors during rollback are
    /// swallowed — the goal is to leave decode state in the most consistent
    /// shape we can on partial failure.
    pub fn rollbackPrefillReservations(
        claimed: []const runtime.scheduler.native_generate.StepItem,
        reserved: []const usize,
        reserved_count: usize,
    ) void {
        if (reserved_count == 0) return;
        const limit = @min(reserved_count, claimed.len);
        var idx: usize = 0;
        while (idx < limit) : (idx += 1) {
            if (claimed[idx].phase != .prefill) continue;
            const work: *PendingPrefillBatchWork = @ptrCast(@alignCast(claimed[idx].work_ptr));
            rollbackScheduledPrefillState(work.decode_state, reserved[idx]) catch {};
        }
    }
};

test "physical decoder step cancellation publishes once before borrowing backend or work" {
    const Probe = struct {
        mutex: *std.atomic.Mutex,
        calls: usize = 0,
        failure: ?anyerror = null,
        unlocked: bool = false,

        fn cancel(_: ?*anyopaque) !void {
            return error.Cancelled;
        }

        fn publish(raw: *anyopaque, _: []const runtime.scheduler.native_generate.StepItem, outcome: anyerror!Result) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            _ = outcome catch |err| {
                self.failure = err;
                self.unlocked = self.mutex.tryLock();
                if (self.unlocked) self.mutex.unlock();
                return;
            };
        }
    };
    for ([_]bool{ false, true }) |events| {
        var mutex: std.atomic.Mutex = .unlocked;
        var gate = execution_control.ExecutionLane{};
        var waiters: std.atomic.Value(usize) = .init(0);
        var executor = Executor{
            // Cancellation must precede every backend/context/work borrow.
            .context = undefined,
            .access = .{
                .mutex = &mutex,
                .gate = if (events) &gate else null,
                .control = .{ .check_fn = Probe.cancel },
                .phase_waiters = &waiters,
            },
        };
        var probe = Probe{ .mutex = &mutex };
        executor.execute(&.{}, true, .{ .ptr = &probe, .publish_fn = Probe.publish });
        try std.testing.expectEqual(@as(usize, 1), probe.calls);
        try std.testing.expectEqual(error.Cancelled, probe.failure.?);
        try std.testing.expect(probe.unlocked);
        try std.testing.expectEqual(@as(usize, 0), waiters.load(.acquire));
        try std.testing.expect(!gate.owned and gate.head == null and gate.tail == null);
    }
}
