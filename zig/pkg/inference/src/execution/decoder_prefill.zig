// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Physical prefill requests. Scheduling, prompt preparation and task output
//! policy stay outside this owner; model route/capacity helpers remain canonical.
const std = @import("std");
const platform = @import("antfly_platform");
const build_options = @import("build_options");
const ops = @import("../ops/ops.zig");
const ComputeBackend = ops.ComputeBackend;
const gpt_arch = @import("../architectures/gpt.zig");
const graph_mod = @import("../graph/root.zig");
const decoder_forward = @import("decoder_forward.zig");
const decoder_routes = @import("decoder_routes.zig");
const decoder_gated_runtime = @import("../backends/decoder_gated_runtime.zig");
const decoder_tail_runtime = @import("../backends/decoder_tail_runtime.zig");
const control = @import("../execution_control.zig");
const runtime = @import("../runtime/root.zig");
const cuda_compute = if (build_options.enable_cuda) @import("../ops/cuda/cuda_compute.zig") else struct {};
const BorrowedDecodeStateRuntime = graph_mod.decode_state_runtime.BorrowedDecodeStateRuntime;

pub const Output = struct {
    last_logits: ?[]f32 = null,
    greedy_token: ?usize = null,
    last_hidden: ?ops.CT = null,
    last_hidden_rows: usize = 0,
};

pub const ChunkRequest = struct {
    token_ids: []const i64,
    total_chunk_end: usize,
    prompt_tokens: usize,
    already_prefilled: usize,
    processed_rows: usize,
    output_intent: decoder_routes.PrefillOutputIntent,
    capture_hidden: bool,
    shared_kv_pruning: bool,
    prepared_metal_whole_prompt: bool,
    metal_target_last_logits: bool,
    cuda_direct_last_logits: bool,
    typed_outputs: bool,
    attempted_chunk_size: usize,
    allow_resize: bool,
    compact_selected: bool = false,
    selection_gpu_oracle: bool = false,
    exact_repeat_probe: bool = false,
    exact_repeat_calls: ?*usize = null,
};
pub const ChunkOutcome = union(enum) { complete: Output, retry_chunk_size: usize };

pub const Runtime = struct {
    forward: decoder_forward.Runtime,
    state: *graph_mod.decode_state_runtime.NativeDecodeState,
    execution: control.ExecutionAccess = .{},
    work_control: runtime.kv.attention_match.Control = .{},

    fn checkWork(raw: ?*anyopaque) !void {
        const self: *Runtime = @ptrCast(@alignCast(raw.?));
        try self.work_control.check();
    }

    pub fn runChunk(self: *Runtime, request: ChunkRequest) !ChunkOutcome {
        try self.execution.acquire();
        defer self.execution.release();
        var repeat: if (build_options.enable_cuda) ?cuda_compute.CudaCompute.AttentionMatchRepeatScope else ?void = null;
        if (comptime build_options.enable_cuda) {
            if (request.exact_repeat_probe and request.token_ids.len > 1) {
                if (request.token_ids.len > 512) return error.UnsupportedCompactionQueryRoute;
                const compute: *cuda_compute.CudaCompute = @ptrCast(@alignCast(self.forward.context.cb.ptr));
                repeat = try compute.beginAttentionMatchRepeat(true, request.token_ids.len);
                compute.kernels.attention_match_repeat_check_ctx = self;
                compute.kernels.attention_match_repeat_check_fn = checkWork;
            }
        }
        defer if (comptime build_options.enable_cuda) {
            if (repeat) |*scope| {
                if (request.exact_repeat_calls) |calls| calls.* += scope.compute.kernels.attention_match_repeat_split_launches;
                scope.deinit();
            }
        };
        const allocator = self.forward.context.allocator;
        const chunk = request.token_ids;
        const total_chunk_end = request.total_chunk_end;
        const seq_len = request.prompt_tokens;
        const prefilled_tokens = request.already_prefilled;
        const processed = request.processed_rows;
        const output_intent = request.output_intent;
        const capture_last_hidden = request.capture_hidden;
        const allow_shared_kv_prefill = request.shared_kv_pruning;
        const use_metal_target_last_logits = request.metal_target_last_logits;
        const prefer_cuda_direct_last_logits = request.cuda_direct_last_logits;
        const can_use_typed_prefill_outputs = request.typed_outputs;
        var decode_runtime = BorrowedDecodeStateRuntime.init(self.state);
        var resized_chunk_size = request.attempted_chunk_size;
        var prepared_runtime = Forward{ .forward = self.forward };
        var prefill_last_logits: ?[]f32 = null;
        var prefill_greedy_token: ?usize = null;
        var prefill_last_hidden: ?ops.CT = null;
        var prefill_last_hidden_rows: usize = 0;
        errdefer if (prefill_last_logits) |values| allocator.free(values);
        errdefer if (prefill_last_hidden) |tensor| self.forward.context.cb.free(tensor);
        try decode_runtime.appendPrefillChunk(chunk.len);
        const decode_context = decode_runtime.makeDecodeContext(total_chunk_end, chunk.len);
        // Shared-KV pruning is only valid for these explicit output
        // intents. Checkpoint fitting collects queries from every
        // consumer; hidden-row/MTP consumers also retain the full path.
        const shared_prefill = if (allow_shared_kv_prefill and
            (output_intent == .kv_only or output_intent == .last_logits or output_intent == .greedy_token))
            try gpt_arch.tryCudaSharedKvPrefill(self.forward.context.cb, allocator, self.forward.context.gpt_config.*, chunk, total_chunk_end, &decode_context, output_intent != .kv_only)
        else
            null;
        const metal_prepared: ?Output = if (shared_prefill == null and request.prepared_metal_whole_prompt)
            try prepared_runtime.tryMetalPreparedTailPrefillGreedy(chunk, total_chunk_end, &decode_context, capture_last_hidden)
        else
            null;
        if (shared_prefill) |prepared| {
            prefill_last_logits = prepared.last_logits;
            debugGenerationStage("executePrefill shared_kv_pruned rows={d} output={s}", .{ chunk.len, @tagName(output_intent) });
        } else if (metal_prepared) |prepared| {
            prefill_last_logits = prepared.last_logits;
            prefill_greedy_token = prepared.greedy_token;
            prefill_last_hidden = prepared.last_hidden;
            prefill_last_hidden_rows = prepared.last_hidden_rows;
        } else if (output_intent == .greedy_token or output_intent == .greedy_token_and_hidden) {
            if (try prepared_runtime.tryCudaPreparedTailPrefillGreedy(chunk, total_chunk_end, &decode_context, capture_last_hidden)) |prepared| {
                prefill_greedy_token = prepared.greedy_token;
                prefill_last_hidden = prepared.last_hidden;
                prefill_last_hidden_rows = prepared.last_hidden_rows;
            } else if (capture_last_hidden) {
                var greedy_hidden = gpt_arch.forwardGreedyLastTokenWithFinalHidden(self.forward.context.cb, allocator, self.forward.context.gpt_config.*, chunk, 1, total_chunk_end, &decode_context) catch |err| {
                    if (try retryAfterMemoryBudgetExceeded(&decode_runtime, chunk.len, request.attempted_chunk_size, &resized_chunk_size, request.allow_resize, err)) return .{ .retry_chunk_size = resized_chunk_size };
                    return err;
                };
                prefill_greedy_token = greedy_hidden.token_id;
                if (greedy_hidden.token_tensor) |token| {
                    self.forward.context.cb.free(token);
                    greedy_hidden.token_tensor = null;
                }
                prefill_last_hidden = greedy_hidden.hidden;
                prefill_last_hidden_rows = greedy_hidden.rows;
            } else {
                prefill_greedy_token = gpt_arch.forwardGreedyLastToken(self.forward.context.cb, allocator, self.forward.context.gpt_config.*, chunk, 1, total_chunk_end, &decode_context) catch |err| {
                    if (try retryAfterMemoryBudgetExceeded(&decode_runtime, chunk.len, request.attempted_chunk_size, &resized_chunk_size, request.allow_resize, err)) return .{ .retry_chunk_size = resized_chunk_size };
                    return err;
                };
            }
            debugGenerationStage(
                "executePrefill captured greedy token={d}",
                .{prefill_greedy_token.?},
            );
        } else if (output_intent == .last_logits_and_hidden or
            (output_intent == .last_logits and use_metal_target_last_logits))
        {
            var target_hidden = self.forward.forwardFinalHiddenDevice(
                chunk,
                1,
                total_chunk_end,
                prefilled_tokens,
                &decode_context,
            ) catch |err| {
                if (try retryAfterMemoryBudgetExceeded(&decode_runtime, chunk.len, request.attempted_chunk_size, &resized_chunk_size, request.allow_resize, err)) return .{ .retry_chunk_size = resized_chunk_size };
                return err;
            };
            var keep_target_hidden = false;
            defer {
                if (!keep_target_hidden) self.forward.context.cb.free(target_hidden.hidden);
                if (target_hidden.pre_norm_hidden) |pre_norm| self.forward.context.cb.free(pre_norm);
                if (target_hidden.prepared_tail_choices) |choices| target_hidden.choices_allocator.?.free(choices);
            }
            const last_hidden = try self.forward.context.cb.sliceRows2D(
                allocator,
                target_hidden.hidden,
                target_hidden.rows - 1,
                1,
                self.forward.context.gpt_config.*.hidden_size,
            );
            defer self.forward.context.cb.free(last_hidden);
            const lm_w = try gpt_arch.getLmHeadWeight(self.forward.context.cb, self.forward.context.gpt_config.*);
            defer self.forward.context.cb.free(lm_w);
            const last_logits_device = try self.forward.context.cb.linearNoBias(
                last_hidden,
                lm_w,
                1,
                self.forward.context.gpt_config.*.hidden_size,
                self.forward.context.gpt_config.*.vocab_size,
            );
            defer self.forward.context.cb.free(last_logits_device);
            const last_logits = try self.forward.context.cb.toFloat32(last_logits_device, allocator);
            gpt_arch.applyFinalLogitSoftcapInPlace(self.forward.context.gpt_config.*, last_logits);
            prefill_last_logits = last_logits;
            if (capture_last_hidden) {
                prefill_last_hidden = target_hidden.hidden;
                prefill_last_hidden_rows = target_hidden.rows;
                keep_target_hidden = true;
            }
            if (capture_last_hidden) {
                debugGenerationStage(
                    "executePrefill captured final logits and MTP hidden vocab_size={d}",
                    .{self.forward.context.gpt_config.*.vocab_size},
                );
            } else {
                debugGenerationStage(
                    "executePrefill captured target final-row logits vocab_size={d}",
                    .{self.forward.context.gpt_config.*.vocab_size},
                );
            }
        } else if (output_intent == .last_logits and prefer_cuda_direct_last_logits) {
            prefill_last_logits = self.forward.forwardLastLogits(chunk, 1, total_chunk_end, &decode_context) catch |err| {
                if (try retryAfterMemoryBudgetExceeded(&decode_runtime, chunk.len, request.attempted_chunk_size, &resized_chunk_size, request.allow_resize, err)) return .{ .retry_chunk_size = resized_chunk_size };
                return err;
            };
            debugGenerationStage(
                "executePrefill captured CUDA final-row logits vocab_size={d}",
                .{self.forward.context.gpt_config.*.vocab_size},
            );
        } else if (output_intent == .kv_only and can_use_typed_prefill_outputs) {
            const hidden_only_result = if (self.forward.context.cb.kind() == .cuda and cudaReplayLastLogitsEnabled())
                gpt_arch.forwardHiddenOnlyWithCudaReplay(
                    self.forward.context.cb,
                    allocator,
                    self.forward.context.gpt_config.*,
                    chunk,
                    1,
                    total_chunk_end,
                    &decode_context,
                    "gpt.prefill_hidden_only",
                )
            else
                gpt_arch.forwardHiddenOnly(
                    self.forward.context.cb,
                    allocator,
                    self.forward.context.gpt_config.*,
                    chunk,
                    1,
                    total_chunk_end,
                    &decode_context,
                );
            hidden_only_result catch |err| {
                if (try retryAfterMemoryBudgetExceeded(&decode_runtime, chunk.len, request.attempted_chunk_size, &resized_chunk_size, request.allow_resize, err)) return .{ .retry_chunk_size = resized_chunk_size };
                return err;
            };
            debugGenerationStage(
                "executePrefill chunk complete processed={d} total_chunk_end={d} output=kv_only",
                .{ processed, total_chunk_end },
            );
        } else {
            const logits = self.forwardAllLogits(chunk, 1, total_chunk_end, &decode_context) catch |err| {
                if (try retryAfterMemoryBudgetExceeded(&decode_runtime, chunk.len, request.attempted_chunk_size, &resized_chunk_size, request.allow_resize, err)) return .{ .retry_chunk_size = resized_chunk_size };
                return err;
            };
            defer allocator.free(logits);
            debugGenerationStage(
                "executePrefill chunk complete processed={d} total_chunk_end={d} logits_len={d}",
                .{ processed, total_chunk_end, logits.len },
            );
            if (total_chunk_end == seq_len) {
                prefill_last_logits = try allocator.dupe(f32, logits[(chunk.len - 1) * self.forward.context.gpt_config.*.vocab_size ..][0..self.forward.context.gpt_config.*.vocab_size]);
                debugGenerationStage(
                    "executePrefill captured last logits vocab_size={d}",
                    .{self.forward.context.gpt_config.*.vocab_size},
                );
            }
        }
        // The chunk's KV writes and query captures are committed at
        // this point. Selection never runs inside an active forward.
        if (request.compact_selected) try self.state.compactSelectedGlobalDonors(self.forward.context.gpt_config.*, self.forward.context.execution_control, request.selection_gpu_oracle);
        return .{ .complete = .{ .last_logits = prefill_last_logits, .greedy_token = prefill_greedy_token, .last_hidden = prefill_last_hidden, .last_hidden_rows = prefill_last_hidden_rows } };
    }

    fn forwardAllLogits(self: *Runtime, ids: []const i64, batch: usize, total: usize, decode: *const gpt_arch.DecodeContext) ![]f32 {
        var fallback_count: u64 = 0;
        return graph_mod.decoder_step_executor.forwardAll(&self.forward.context, self.forward.graph_cache, ids, batch, total, decode, self.forward.replay_fallbacks orelse &fallback_count);
    }
    pub const PromptStateRequest = struct {
        prompt_tokens: usize,
        generated_tokens: i32,
        fresh_cuda_capacity: bool = false,
        prefilled_tokens: usize = 0,
        prompt_cache_enabled: bool = false,
        has_prompt_cache: bool = false,
        has_draft: bool = false,
        speculation_requested: bool = false,
    };

    /// Preserve the existing capacity route and recurrent reset order. Metadata
    /// preparation is separate from model execution lock acquisition because
    /// prepared embedding callers already hold a scheduler turn at this phase.
    pub fn preparePromptState(self: *Runtime, request: PromptStateRequest) !void {
        const config = self.forward.context.gpt_config.*;
        const cb = self.forward.context.cb;
        if (request.fresh_cuda_capacity and cb.kind() == .cuda and self.forward.context.kv_dtype == .f16 and
            !request.has_prompt_cache and !request.prompt_cache_enabled and !request.has_draft and
            !request.speculation_requested and request.prefilled_tokens == 0 and self.state.total_tokens == 0 and
            self.state.sequence_id == null and self.state.isPaged() and self.state.kvSlidingWindowTokens() == null and
            self.state.cudaPagedKvReplayCapacityTokens() == null and
            platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_RESERVE_PREFILL_KV", true))
        {
            if (decoder_routes.cudaE4bPrefillKvCapacity(config, request.prompt_tokens, request.generated_tokens)) |capacity| {
                const limit = std.mem.alignForward(usize, request.prompt_tokens + @as(usize, @intCast(request.generated_tokens)), 16);
                try self.state.reserveFreshCudaKvCapacity(capacity, limit);
                std.log.info("cuda_prefill_kv_reservation prompt_tokens={d} capacity_tokens={d}", .{ request.prompt_tokens, capacity });
            }
        }
        try self.resetRecurrentPrompt();
    }

    pub fn resetRecurrentPrompt(self: *Runtime) !void {
        const config = self.forward.context.gpt_config.*;
        if (config.isQwen35() and self.state.isPaged()) {
            try self.state.ensureQwen35LinearCache(config);
            self.state.resetQwen35LinearCache();
        }
    }

    pub fn prepareWholePrompt(self: *Runtime, sequence_length: usize) !void {
        var state = BorrowedDecodeStateRuntime.init(self.state);
        _ = try state.preparePrefill(sequence_length, sequence_length);
    }
    pub const EmbeddingRequest = struct {
        /// Ownership transfers under the execution lane, after successful
        /// acquisition. Failed admission/locking leaves the original slot live.
        owner: *?ops.CT,
        token_ids: []const i64,
        sequence_length: usize,
        capture_hidden: bool = false,
        ensure_device_resident: bool = true,
        route: enum { prepared_last_row, ordinary_last_row } = .prepared_last_row,
        attention_mask: ?[]const u8 = null,
        positions: ?[]const u32 = null,
        visual_mask: ?[]const bool = null,
        deepstack_embeddings: ?[]const f32 = null,
        deepstack_layer_count: usize = 0,
    };

    /// The caller owns its admitted scheduler turn. Physical state was prepared
    /// before acquiring the model lane, preserving prepared-prefix lock order.
    pub fn runEmbeddingsOwned(self: *Runtime, request: EmbeddingRequest) !Output {
        try self.execution.acquire();
        defer self.execution.release();
        const original = request.owner.* orelse return error.InvalidPreparedPrompt;
        request.owner.* = null;
        const cb = self.forward.context.cb;
        const embeddings = if (request.ensure_device_resident) try cb.ensureDeviceResidentOwned(original) else original;
        var owned = true;
        errdefer if (owned) cb.free(embeddings);
        var state = BorrowedDecodeStateRuntime.init(self.state);
        var decode = state.makeDecodeContext(request.sequence_length, request.sequence_length);
        decode.attn_or_mask = request.attention_mask;
        decode.mrope_positions = request.positions;
        decode.qwen3vl_visual_mask = request.visual_mask;
        decode.qwen3vl_deepstack_embeddings = request.deepstack_embeddings;
        decode.qwen3vl_deepstack_layer_count = request.deepstack_layer_count;
        var numerical = Forward{ .forward = self.forward };
        owned = false;
        return switch (request.route) {
            .prepared_last_row => try numerical.forwardPreparedEmbeddingsLastLogits(embeddings, request.token_ids, request.sequence_length, &decode, request.capture_hidden),
            .ordinary_last_row => .{ .last_logits = try gpt_arch.forwardLastLogitsLastRowFromEmbeddingsWithLayer0Overrides(cb, self.forward.context.allocator, self.forward.context.gpt_config.*, embeddings, .{}, 1, request.sequence_length, &decode, null) },
        };
    }
};
pub const PositionedEmbeddingBatchItem = struct {
    allocator: std.mem.Allocator,
    backend: *const ComputeBackend,
    model: *const @import("../models/gpt.zig").Config,
    state: *graph_mod.decode_state_runtime.NativeDecodeState,
    embeddings_owner: *?ops.CT,
    geometry_tokens: usize,
    position_ids: []const u32,
    deepstack_layer_count: usize,
    sequence_length: usize,
};

pub fn forwardPositionedEmbeddingBatch(items: []const PositionedEmbeddingBatchItem) ![][]f32 {
    const capacity = if (platform.env.getenvBool("ANTFLY_LIGHTONOCR3_CUDA_WIDE_MICROBATCHES")) decoder_routes.max_positioned_prefill_items else decoder_routes.default_positioned_prefill_items;
    if (items.len == 0 or items.len > capacity) return error.InvalidBatchDecodeState;
    const first = items[0];
    const cb = first.backend;
    const a = first.allocator;
    const seq = first.sequence_length;
    const cfg = first.model.*;
    if (seq == 0 or !cfg.hasQwen35VisionArchitectureContract()) return error.UnsupportedBatchedPrefill;
    const total = try std.math.mul(usize, items.len, seq);
    if (try decoder_routes.positionedPrefillBatchLimitForBackend(cb.kind(), items.len, seq) != items.len) return error.UnsupportedBatchedPrefill;
    const positions = try a.alloc(u32, try std.math.mul(usize, total, 3));
    defer a.free(positions);
    var mixed: [decoder_routes.max_positioned_prefill_items]graph_mod.decode_batch.Item = undefined;
    var reserved: [decoder_routes.max_positioned_prefill_items]usize = @splat(0);
    var prepared_count: usize = 0;
    errdefer for (0..prepared_count) |index| {
        var decode = BorrowedDecodeStateRuntime.init(items[index].state);
        decode.rollbackReservedPrefill(reserved[index]) catch {};
    };
    for (items, 0..) |item, index| {
        if (item.backend.ptr != cb.ptr or item.sequence_length != seq or
            item.geometry_tokens != seq or item.position_ids.len != 3 * seq or
            item.deepstack_layer_count != 0 or item.embeddings_owner.* == null or
            item.state.total_tokens != 0) return error.InvalidPreparedPrompt;
        mixed[index] = .{ .state = item.state, .total_sequence_len = seq, .query_sequence_len = seq, .kv_sequence_len = 0, .kv_position_offset = 0, .attention_mode = .paged_prefill };
        reserved[index] = try graph_mod.decoder_step_executor.Executor.reserveAndRefreshScheduledPrefill(&mixed[index], seq);
        prepared_count += 1;
        for (0..3) |axis| @memcpy(positions[axis * total + index * seq ..][0..seq], item.position_ids[axis * seq ..][0..seq]);
    }
    var owned = try graph_mod.decode_batch.buildWithPositions(a, mixed[0..items.len], positions);
    defer owned.deinit();
    const combined = (try cb.allocUninitF32Shape(&.{ @intCast(total), @intCast(cfg.hidden_size) })) orelse return error.UnsupportedBatchedPrefill;
    var consumed = false;
    defer if (!consumed) cb.free(combined);
    for (items, 0..) |item, index| {
        if (!try cb.copyRows2D(a, combined, index * seq, item.embeddings_owner.*.?, 0, seq, cfg.hidden_size)) return error.UnsupportedBatchedPrefill;
    }
    for (items) |item| {
        cb.free(item.embeddings_owner.*.?);
        item.embeddings_owner.* = null;
    }
    consumed = true;
    const hidden = try gpt_arch.forwardFinalHiddenTensorFromEmbeddingsWithLayer0Overrides(cb, a, cfg, combined, .{}, items.len, seq, &owned.context, null);
    defer cb.free(hidden.hidden);
    if (hidden.total_rows != total) return error.InvalidTensorShape;
    const last = (try cb.allocUninitF32Shape(&.{ @intCast(items.len), @intCast(cfg.hidden_size) })) orelse return error.UnsupportedBatchedPrefill;
    defer cb.free(last);
    for (0..items.len) |index| if (!try cb.copyRows2D(a, last, index, hidden.hidden, (index + 1) * seq - 1, 1, cfg.hidden_size)) return error.UnsupportedBatchedPrefill;
    const weight = try gpt_arch.getLmHeadWeight(cb, cfg);
    defer cb.free(weight);
    const logits = try cb.linearNoBias(last, weight, items.len, cfg.hidden_size, cfg.vocab_size);
    defer cb.free(logits);
    const host = try cb.toFloat32(logits, a);
    defer a.free(host);
    gpt_arch.applyFinalLogitSoftcapInPlace(cfg, host);
    const output = try a.alloc([]f32, items.len);
    var copied: usize = 0;
    errdefer {
        for (output[0..copied]) |row| a.free(row);
        a.free(output);
    }
    for (output, 0..) |*row, index| {
        row.* = try a.dupe(f32, host[index * cfg.vocab_size ..][0..cfg.vocab_size]);
        copied += 1;
    }
    if (platform.env.getenvBool("TERMITE_METAL_STAGE_TIMING_LOG_FRAMES"))
        std.debug.print("LIGHTONOCR3 profile_prefill_items={d} profile_prefill_query_tokens={d}\n", .{ items.len, seq });
    return output;
}

pub const Forward = struct {
    forward: decoder_forward.Runtime,
    pub fn capturePreparedTailPostNormHidden(
        cb: *const ComputeBackend,
        capture: bool,
        pre_norm_hidden: ops.CT,
        final_norm_slot: usize,
        hidden_size: usize,
        eps: f32,
    ) !?ops.CT {
        if (!capture) return null;
        return cb.decoderRuntimeApplyRmsNorm(&.{
            .slot = final_norm_slot,
            .input = pre_norm_hidden,
            .hidden_size = hidden_size,
            .eps = eps,
        });
    }

    pub fn tryCudaPreparedTailPrefillGreedy(
        self: *Forward,
        input_ids: []const i64,
        seq_len: usize,
        decode_context: *const gpt_arch.DecodeContext,
        capture_last_hidden: bool,
    ) !?Output {
        if (!decoder_routes.cudaPreparedPrefillGreedyEnabled()) return null;
        if (self.forward.context.cb.kind() != .cuda) return null;
        if (decode_context.attention_mode != .paged_prefill or decode_context.query_sequence_len != input_ids.len) return null;

        const tail = (try decoder_gated_runtime.forwardPrefillLastPreparedTail(
            self.forward.context.cb,
            self.forward.context.allocator,
            self.forward.context.gpt_config.*,
            self.forward.context.gpt_config.*.num_hidden_layers,
            input_ids,
            seq_len,
            decode_context,
            false,
        )) orelse return null;
        var owns_hidden = true;
        errdefer if (owns_hidden) self.forward.context.cb.free(tail.final_hidden);

        var greedy = (try decoder_tail_runtime.forwardGreedyTokenTensorFromFinalHidden(
            self.forward.context.cb,
            self.forward.context.allocator,
            self.forward.context.gpt_config.*,
            tail.final_hidden,
            .rms,
            tail.final_norm_slot,
        )) orelse {
            self.forward.context.cb.free(tail.final_hidden);
            return null;
        };
        if (greedy.token_tensor) |token_tensor| {
            self.forward.context.cb.free(token_tensor);
            greedy.token_tensor = null;
        }

        const last_hidden = try capturePreparedTailPostNormHidden(
            self.forward.context.cb,
            capture_last_hidden,
            tail.final_hidden,
            tail.final_norm_slot,
            self.forward.context.gpt_config.*.hidden_size,
            self.forward.context.gpt_config.*.norm_eps,
        );
        if (capture_last_hidden and last_hidden == null) {
            self.forward.context.cb.free(tail.final_hidden);
            owns_hidden = false;
            return null;
        }
        self.forward.context.cb.free(tail.final_hidden);
        owns_hidden = false;
        debugGenerationStage(
            "executePrefill prepared_tail_greedy token={d} capture_hidden={}",
            .{ greedy.token_id, capture_last_hidden },
        );
        return .{
            .greedy_token = greedy.token_id,
            .last_hidden = last_hidden,
            .last_hidden_rows = if (last_hidden != null) 1 else 0,
        };
    }

    pub fn metalPreparedTailOwnsWholePrefill(
        input_len: usize,
        seq_len: usize,
        decode_context: *const gpt_arch.DecodeContext,
    ) bool {
        return decode_context.attention_mode == .paged_prefill and
            decode_context.total_sequence_len == seq_len and
            decode_context.query_sequence_len == input_len and
            decode_context.kv_sequence_len == input_len and
            decode_context.kv_position_offset == 0 and
            input_len == seq_len;
    }

    /// Metal analog of the CUDA prepared-tail prefill: the planned prefill
    /// frame plus a prepared-slot greedy tail, capturing the last POST-final-
    /// norm hidden row for Gemma4 MTP seeding. Without this the MTP seed path
    /// re-runs the whole prompt through the generic per-op forward (a second
    /// prompt-length forward that scales with prompt size). Returns null on
    /// any miss so the caller falls back to the plain logits prefill.
    pub fn tryMetalPreparedTailPrefillGreedy(
        self: *Forward,
        input_ids: []const i64,
        seq_len: usize,
        decode_context: *const gpt_arch.DecodeContext,
        capture_last_hidden: bool,
    ) !?Output {
        if (self.forward.context.cb.kind() != .metal) return null;
        if (!decoder_routes.metalPreparedPrefillHiddenCaptureEnabled()) return null;
        if (!metalPreparedTailOwnsWholePrefill(input_ids.len, seq_len, decode_context)) return null;

        const prepared = self.forward.context.cb.decoderRuntimePrepareOrReuseFamily(
            self.forward.context.allocator,
            self.forward.context.gpt_config.*,
            seq_len,
            self.forward.context.gpt_config.*.num_hidden_layers,
        ) catch return null;
        if (!prepared.prepared) return null;

        const tail = (try decoder_gated_runtime.forwardPrefillLastPreparedTail(
            self.forward.context.cb,
            self.forward.context.allocator,
            self.forward.context.gpt_config.*,
            self.forward.context.gpt_config.*.num_hidden_layers,
            input_ids,
            seq_len,
            decode_context,
            true,
        )) orelse return null;
        defer self.forward.context.cb.free(tail.final_hidden);

        const last_hidden = try capturePreparedTailPostNormHidden(
            self.forward.context.cb,
            capture_last_hidden,
            tail.final_hidden,
            tail.final_norm_slot,
            self.forward.context.gpt_config.*.hidden_size,
            self.forward.context.gpt_config.*.norm_eps,
        );
        errdefer if (last_hidden) |hidden| self.forward.context.cb.free(hidden);
        if (capture_last_hidden and last_hidden == null) return null;

        if (tail.greedy_token_id) |greedy_token| {
            debugGenerationStage(
                "executePrefill metal_prepared_tail_greedy token={d} capture_hidden={}",
                .{ greedy_token, capture_last_hidden },
            );
            return .{
                .greedy_token = @intCast(greedy_token),
                .last_hidden = last_hidden,
                .last_hidden_rows = if (last_hidden != null) 1 else 0,
            };
        }

        const logits = if (try decoder_tail_runtime.forwardPreparedLogitsTensorFromFinalHidden(
            self.forward.context.cb,
            self.forward.context.gpt_config.*,
            tail.final_hidden,
            .rms,
            tail.final_norm_slot,
            tail.final_lm_head_slot,
        )) |prepared_logits|
            prepared_logits
        else
            (try decoder_tail_runtime.forwardLogitsTensorFromFinalHidden(
                self.forward.context.cb,
                self.forward.context.gpt_config.*,
                tail.final_hidden,
                .rms,
                tail.final_norm_slot,
            )) orelse {
                if (last_hidden) |hidden| self.forward.context.cb.free(hidden);
                return null;
            };
        defer self.forward.context.cb.free(logits);
        const last_logits = try self.forward.context.cb.toFloat32(logits, self.forward.context.allocator);
        gpt_arch.applyFinalLogitSoftcapInPlace(self.forward.context.gpt_config.*, last_logits);
        debugGenerationStage(
            "executePrefill metal_prepared_tail_logits capture_hidden={}",
            .{capture_last_hidden},
        );
        return .{
            .last_logits = last_logits,
            .last_hidden = last_hidden,
            .last_hidden_rows = if (last_hidden != null) 1 else 0,
        };
    }

    pub fn forwardPreparedEmbeddingsLastLogits(
        self: *Forward,
        input_embeddings: ops.CT,
        ple_token_ids: []const i64,
        seq_len: usize,
        decode_context: *const gpt_arch.DecodeContext,
        capture_last_hidden: bool,
    ) !Output {
        var owns_input = true;
        defer if (owns_input) self.forward.context.cb.free(input_embeddings);
        if (try self.tryMetalPreparedMultimodalPrefill(input_embeddings, ple_token_ids, seq_len, decode_context, capture_last_hidden)) |logits| {
            return logits;
        }
        if (capture_last_hidden) return error.MultimodalMtpPrefillUnavailable;
        const ple_vectors = try gpt_arch.computePleVectors(self.forward.context.cb, self.forward.context.allocator, self.forward.context.gpt_config.*, ple_token_ids, input_embeddings, seq_len);
        defer if (ple_vectors) |vectors| self.forward.context.cb.free(vectors);
        owns_input = false;
        return .{ .last_logits = try gpt_arch.forwardLastLogitsLastRowFromEmbeddingsWithLayer0Overrides(
            self.forward.context.cb,
            self.forward.context.allocator,
            self.forward.context.gpt_config.*,
            input_embeddings,
            .{},
            1,
            seq_len,
            decode_context,
            ple_vectors,
        ) };
    }

    /// The planned Gemma prefill frame for a media prompt: the same route
    /// the token-id prefill takes, fed with the prepared embeddings, so the
    /// whole layer stack runs as one command buffer instead of one
    /// submission per op. Null leaves the embeddings untouched for the
    /// generic forward.
    pub fn tryMetalPreparedMultimodalPrefill(
        self: *Forward,
        input_embeddings: ops.CT,
        ple_token_ids: []const i64,
        seq_len: usize,
        decode_context: *const gpt_arch.DecodeContext,
        capture_last_hidden: bool,
    ) !?Output {
        if (self.forward.context.cb.kind() != .metal) return null;
        if (decode_context.attention_mode != .paged_prefill or decode_context.attn_or_mask != null) return null;
        if (!metalPreparedTailOwnsWholePrefill(ple_token_ids.len, seq_len, decode_context)) return null;
        const prepared = self.forward.context.cb.decoderRuntimePrepareOrReuseFamily(
            self.forward.context.allocator,
            self.forward.context.gpt_config.*,
            seq_len,
            self.forward.context.gpt_config.*.num_hidden_layers,
        ) catch return null;
        if (!prepared.prepared) return null;
        var rows = (try decoder_gated_runtime.forwardFinalHiddenRowsFromEmbeddings(
            self.forward.context.cb,
            self.forward.context.allocator,
            self.forward.context.gpt_config.*,
            self.forward.context.gpt_config.*.num_hidden_layers,
            input_embeddings,
            ple_token_ids,
            seq_len,
            decode_context,
        )) orelse return null;
        defer rows.deinit(self.forward.context.cb);
        const last_hidden = try self.forward.context.cb.sliceRows2D(self.forward.context.allocator, rows.final_hidden, rows.rows - 1, 1, self.forward.context.gpt_config.*.hidden_size);
        var owns_last_hidden = true;
        defer if (owns_last_hidden) self.forward.context.cb.free(last_hidden);
        const lm_w = try gpt_arch.getLmHeadWeight(self.forward.context.cb, self.forward.context.gpt_config.*);
        defer self.forward.context.cb.free(lm_w);
        const last_logits_device = try self.forward.context.cb.linearNoBias(last_hidden, lm_w, 1, self.forward.context.gpt_config.*.hidden_size, self.forward.context.gpt_config.*.vocab_size);
        defer self.forward.context.cb.free(last_logits_device);
        const last_logits = try self.forward.context.cb.toFloat32(last_logits_device, self.forward.context.allocator);
        gpt_arch.applyFinalLogitSoftcapInPlace(self.forward.context.gpt_config.*, last_logits);
        debugGenerationStage("multimodal prefill metal_prepared_frame rows={d}", .{rows.rows});
        if (capture_last_hidden) owns_last_hidden = false;
        return .{ .last_logits = last_logits, .last_hidden = if (capture_last_hidden) last_hidden else null, .last_hidden_rows = if (capture_last_hidden) 1 else 0 };
    }
};
pub fn retryAfterMemoryBudgetExceeded(
    decode_runtime: *BorrowedDecodeStateRuntime,
    reserved_tokens: usize,
    attempted_chunk_size: usize,
    current_chunk_size: *usize,
    allow_resize: bool,
    err: anyerror,
) !bool {
    if (err != error.MemoryBudgetExceeded or attempted_chunk_size <= 1) return false;
    try decode_runtime.rollbackReservedPrefill(reserved_tokens);
    if (!allow_resize) return false;
    current_chunk_size.* = @max(attempted_chunk_size / 2, 1);
    return true;
}

const cudaReplayLastLogitsEnabled = decoder_forward.cudaReplayLastLogitsEnabled;
fn debugGenerationStage(comptime fmt: []const u8, args: anytype) void {
    if (!platform.env.getenvBool("TERMITE_GEN_STAGE_DEBUG")) return;
    std.debug.print("gen_debug: " ++ fmt ++ "\n", args);
}
