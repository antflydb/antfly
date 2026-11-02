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

//! Synchronous physical decoder execution, independent of task publication.
//! Contexts borrow immutable configuration and the real backend owner. Returned
//! resident values retain that backend pointer, never this temporary adapter.
const std = @import("std");
const platform = @import("antfly_platform");
const ops = @import("../ops/ops.zig");
const ComputeBackend = ops.ComputeBackend;
const gpt_arch = @import("../architectures/gpt.zig");
const graph_mod = @import("../graph/root.zig");
const metal_runtime = @import("../backends/metal_runtime.zig");
const decoder_gated_runtime = @import("../backends/decoder_gated_runtime.zig");
const BorrowedDecodeStateRuntime = graph_mod.decode_state_runtime.BorrowedDecodeStateRuntime;
const NativeDecodeState = @import("../graph/decode_state_runtime.zig").NativeDecodeState;
const runWithDecoderRuntimeResetBoundary = graph_mod.decoder_step_executor.runWithDecoderRuntimeResetBoundary;

pub const ForwardAllWithHiddenHost = struct {
    allocator: std.mem.Allocator,
    logits: []f32,
    hidden: []f32,
    pre_norm_hidden: []f32,
    rows: usize,

    pub fn deinit(self: *ForwardAllWithHiddenHost) void {
        self.allocator.free(self.logits);
        self.allocator.free(self.hidden);
        self.allocator.free(self.pre_norm_hidden);
        self.* = undefined;
    }
};

pub const ForwardAllWithHiddenDevice = struct {
    cb: *const ComputeBackend,
    logits: ops.CT,
    hidden: ops.CT,
    pre_norm_hidden: ops.CT,
    rows: usize,

    pub fn deinit(self: *ForwardAllWithHiddenDevice) void {
        self.cb.free(self.logits);
        self.cb.free(self.hidden);
        self.cb.free(self.pre_norm_hidden);
        self.* = undefined;
    }
};

pub const ForwardHiddenDevice = struct {
    cb: *const ComputeBackend,
    hidden: ops.CT,
    pre_norm_hidden: ?ops.CT,
    rows: usize,
    greedy_token_id: ?u32 = null,
    prepared_tail_choices: ?[]u32 = null,
    choices_allocator: ?std.mem.Allocator = null,

    pub fn deinit(self: *ForwardHiddenDevice) void {
        self.cb.free(self.hidden);
        if (self.pre_norm_hidden) |pre_norm| self.cb.free(pre_norm);
        if (self.prepared_tail_choices) |choices| self.choices_allocator.?.free(choices);
        self.* = undefined;
    }
};

pub const HiddenSource = enum {
    final,
    pre_norm,

    pub fn name(self: HiddenSource) []const u8 {
        return switch (self) {
            .final => "final",
            .pre_norm => "pre_norm",
        };
    }
};

pub const DeviceTokenOutcome = struct { token: usize, token_tensor: ?ops.CT = null };
pub const DeviceStepEvents = struct { handoff_attempts: usize = 0, handoff_hits: usize = 0, handoff_fallbacks: usize = 0 };
pub const DeviceStepRequest = struct {
    token_ids: []const i64,
    sequence_length: usize,
    input_token_tensor: ?ops.CT = null,
    sampling: ?graph_mod.model_runtime.SamplingConfig = null,
    prefetch_drain_budget: usize = 4,
};
pub const DeviceStepResult = union(enum) { token: DeviceTokenOutcome, logits: []f32, declined };

pub fn deviceTokenHandoffEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_GREEDY_DEVICE_TOKEN_HANDOFF", true);
}
pub fn gatedTokenTensorEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_GATED_TOKEN_TENSOR_DECODE", false);
}
pub fn metalDirectTailDefault() bool {
    return true;
}
pub fn metalDirectTailEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_GEMMA4_METAL_DIRECT_GREEDY", metalDirectTailDefault());
}

pub const Runtime = struct {
    context: graph_mod.execution.Context,
    graph_cache: ?*graph_mod.cache.GraphCache = null,
    io: ?std.Io = null,
    replay_fallbacks: ?*u64 = null,

    pub const MtpMaterializedDeviceActivation = struct {
        activation: ?ops.CT,
        next_target_choice: ?u32 = null,
    };

    pub const MtpMaterializedHostActivation = struct {
        activation: []f32,
        next_target_choice: ?u32 = null,
    };

    pub fn forwardDeviceStep(self: *Runtime, state: *NativeDecodeState, request: DeviceStepRequest, events: *DeviceStepEvents) !DeviceStepResult {
        const cb = self.context.cb;
        const allocator = self.context.allocator;
        const model = self.context.gpt_config.*;
        const kind = cb.kind();
        var physical = BorrowedDecodeStateRuntime.init(state);
        if (request.sampling) |sampling| {
            if (kind != .metal or model.family != .gemma or !model.hasPle() or !metalDirectTailEnabled()) return .declined;
            cb.drainPrefetchBudget(request.prefetch_drain_budget);
            const decode = physical.makeDecodeContext(request.sequence_length, 1);
            if (try decoder_gated_runtime.forwardSampledToken(cb, allocator, model, model.num_hidden_layers, request.token_ids[request.sequence_length - 1], request.sequence_length, &decode, sampling, request.token_ids[0..request.sequence_length])) |token| {
                if (token < 0) return error.InvalidModelOutput;
                return .{ .token = .{ .token = @intCast(token) } };
            }
            if (try decoder_gated_runtime.forwardLastLogits(cb, allocator, model, model.num_hidden_layers, request.token_ids[request.sequence_length - 1], request.sequence_length, &decode)) |logits|
                return .{ .logits = logits };
            return .declined;
        }
        cb.drainPrefetchBudget(request.prefetch_drain_budget);
        const decode = physical.makeDecodeContext(request.sequence_length, 1);
        if (kind == .metal and model.family == .gemma and model.hasPle() and metalDirectTailEnabled()) {
            if (try decoder_gated_runtime.forwardGreedyToken(cb, allocator, model, model.num_hidden_layers, request.token_ids[request.sequence_length - 1], request.sequence_length, &decode)) |token| {
                if (token < 0) return error.InvalidModelOutput;
                return .{ .token = .{ .token = @intCast(token) } };
            }
        }
        if (request.input_token_tensor) |tensor| {
            if (deviceTokenHandoffEnabled()) {
                events.handoff_attempts += 1;
                if (gatedTokenTensorEnabled() and (kind != .cuda or !gpt_arch.cudaDecodeGraphReplayRequired())) {
                    if (try decoder_gated_runtime.forwardGreedyTokenFromTokenTensor(cb, allocator, model, model.num_hidden_layers, request.token_ids[request.sequence_length - 1], tensor, request.sequence_length, &decode)) |result| {
                        events.handoff_hits += 1;
                        return .{ .token = .{ .token = result.token_id, .token_tensor = result.token_tensor } };
                    }
                }
                if (try gpt_arch.forwardGreedyLastTokenFromTokenTensor(cb, allocator, model, tensor, 1, request.sequence_length, &decode)) |result| {
                    events.handoff_hits += 1;
                    return .{ .token = .{ .token = result.token_id, .token_tensor = result.token_tensor } };
                }
                events.handoff_fallbacks += 1;
            }
        }
        const ids = request.token_ids[request.sequence_length - 1 .. request.sequence_length];
        return .{ .token = .{ .token = try gpt_arch.forwardGreedyLastToken(cb, allocator, model, ids, 1, request.sequence_length, &decode) } };
    }

    pub fn forwardDeviceTokenTensor(self: *Runtime, state: *NativeDecodeState, sequence_length: usize, input: ops.CT, drain_budget: usize, events: *DeviceStepEvents) !?ops.CT {
        const cb = self.context.cb;
        const model = self.context.gpt_config.*;
        cb.drainPrefetchBudget(drain_budget);
        var physical = BorrowedDecodeStateRuntime.init(state);
        const decode = physical.makeDecodeContext(sequence_length, 1);
        events.handoff_attempts += 1;
        if (gatedTokenTensorEnabled() and !gpt_arch.cudaDecodeGraphReplayRequired()) {
            if (try decoder_gated_runtime.forwardGreedyTokenTensorOnlyFromTokenTensor(cb, self.context.allocator, model, model.num_hidden_layers, 0, input, sequence_length, &decode)) |token| {
                events.handoff_hits += 1;
                return token;
            }
        }
        if (try gpt_arch.forwardGreedyLastTokenTensorOnlyFromTokenTensor(cb, self.context.allocator, model, input, 1, sequence_length, &decode)) |token| {
            events.handoff_hits += 1;
            return token;
        }
        events.handoff_fallbacks += 1;
        return null;
    }

    pub fn materializeAcceptedTokenKv(
        self: *Runtime,
        token_ids: []const i64,
        total_seq_len: usize,
        decode_state: *NativeDecodeState,
    ) !void {
        var decode_runtime = BorrowedDecodeStateRuntime.init(decode_state);
        if (decode_state.requiresFullRecompute()) {
            _ = try decode_runtime.appendGeneratedToken();
            return;
        }
        // Run a forward pass to populate the KV cache for the correction/bonus
        // token. Paged CUDA writes the KV suffix at decode_context.kv_sequence_len,
        // so advance first; otherwise the device KV layer stays one token short.
        _ = try decode_runtime.appendGeneratedToken();
        errdefer decode_runtime.truncateGeneratedTokens(1) catch {};
        const decode_context = decode_runtime.makeDecodeContext(total_seq_len, 1);
        const logits = try self.forwardAllLogits(
            token_ids[total_seq_len - 1 .. total_seq_len],
            1,
            total_seq_len,
            &decode_context,
        );
        self.context.allocator.free(logits);
    }

    pub fn greedyTargetChoiceFromFinalHiddenDevice(
        self: *Runtime,
        final_hidden: ops.CT,
    ) !?u32 {
        const lm_w = try self.context.graphWeight("lm_head.weight");
        defer self.context.cb.free(lm_w);
        const hidden_size: usize = @intCast(self.context.gpt_config.*.hidden_size);
        const vocab_size: usize = @intCast(self.context.gpt_config.*.vocab_size);
        const suppress_token_ids = self.context.gpt_config.*.suppressTokenIds();
        if (suppress_token_ids.len == 0) {
            if (try self.context.cb.linearNoBiasArgmaxLastRow(
                final_hidden,
                lm_w,
                1,
                hidden_size,
                vocab_size,
            )) |token| {
                return token;
            }
        }
        if (try self.context.cb.linearNoBiasArgmaxRowsSuppress(
            final_hidden,
            lm_w,
            1,
            hidden_size,
            vocab_size,
            suppress_token_ids,
            self.context.allocator,
        )) |choices| {
            defer self.context.allocator.free(choices);
            if (choices.len == 0) return error.InvalidModelOutput;
            return choices[0];
        }
        return null;
    }

    pub fn materializeMtpCanonicalHidden(
        self: *Runtime,
        token_ids: []const i64,
        total_seq_len: usize,
        decode_context: *const gpt_arch.DecodeContext,
        hidden_source: HiddenSource,
    ) !ForwardHiddenDevice {
        if (total_seq_len == 0 or token_ids.len < total_seq_len) return error.InvalidTensorShape;
        if (try self.materializeAcceptedTokenKvActiveHiddenForMtp(token_ids, total_seq_len, decode_context, hidden_source)) |result| return result;
        return self.forwardTargetHiddenDeviceNoReplay(token_ids[total_seq_len - 1 .. total_seq_len], 1, total_seq_len, decode_context, hidden_source);
    }

    pub fn materializeAcceptedTokenKvActiveHiddenForMtp(
        self: *Runtime,
        token_ids: []const i64,
        total_seq_len: usize,
        decode_context: *const gpt_arch.DecodeContext,
        hidden_source: HiddenSource,
    ) !?ForwardHiddenDevice {
        if (!activeMaterializationEnabled()) return null;
        if (hidden_source != .final) return null;
        if (self.context.cb.kind() != .metal) return null;
        if (!activeMaterializationSupported(self.context.cb.kind(), self.context.gpt_config.*, total_seq_len) and
            !getenvBool("ANTFLY_GEMMA4_MTP_LONG_CONTEXT_FRAME")) return null;
        if (total_seq_len == 0 or token_ids.len < total_seq_len) return error.InvalidTensorShape;
        var result = (try decoder_gated_runtime.forwardGreedyTokenAndFinalHidden(
            self.context.cb,
            self.context.allocator,
            self.context.gpt_config.*,
            self.context.gpt_config.*.num_hidden_layers,
            token_ids[total_seq_len - 1],
            total_seq_len,
            decode_context,
        )) orelse return null;
        errdefer result.deinit(self.context.cb);
        var greedy_token_id: ?u32 = null;
        if (result.token_id >= 0 and result.token_id <= std.math.maxInt(u32)) {
            const raw_token: u32 = @intCast(result.token_id);
            if (!isSuppressedToken(raw_token, self.context.gpt_config.*.suppressTokenIds())) {
                greedy_token_id = raw_token;
            }
        }
        return .{
            .cb = self.context.cb,
            .hidden = result.final_hidden,
            .pre_norm_hidden = null,
            .rows = 1,
            .greedy_token_id = greedy_token_id,
        };
    }

    pub fn materializeAcceptedTokenKvForMtp(
        self: *Runtime,
        token_ids: []const i64,
        total_seq_len: usize,
        decode_state: *NativeDecodeState,
        hidden_source: HiddenSource,
    ) !?u32 {
        var decode_runtime = BorrowedDecodeStateRuntime.init(decode_state);
        if (decode_state.requiresFullRecompute()) {
            _ = try decode_runtime.appendGeneratedToken();
            return null;
        }
        _ = try decode_runtime.appendGeneratedToken();
        errdefer decode_runtime.truncateGeneratedTokens(1) catch {};
        const decode_context = decode_runtime.makeDecodeContext(total_seq_len, 1);
        if (platform.env.getenvBoolDefault("ANTFLY_GEMMA4_MTP_RECOVERY_CHOICE_ONLY", false) and
            activeMaterializationEnabled() and hidden_source == .final and self.context.cb.kind() == .metal and
            (activeMaterializationSupported(self.context.cb.kind(), self.context.gpt_config.*, total_seq_len) or getenvBool("ANTFLY_GEMMA4_MTP_LONG_CONTEXT_FRAME")))
        {
            if (try decoder_gated_runtime.forwardGreedyTokenPreparedOnly(self.context.cb, self.context.allocator, self.context.gpt_config.*, self.context.gpt_config.*.num_hidden_layers, token_ids[total_seq_len - 1], total_seq_len, &decode_context)) |choice| {
                if (choice < 0 or choice >= self.context.gpt_config.*.vocab_size) return error.InvalidModelOutput;
                const token: u32 = @intCast(choice);
                if (isSuppressedToken(token, self.context.gpt_config.*.suppressTokenIds())) return error.InvalidModelOutput;
                return token;
            }
        }

        if (try self.materializeAcceptedTokenKvActiveHiddenForMtp(
            token_ids,
            total_seq_len,
            &decode_context,
            hidden_source,
        )) |active_result| {
            var result = active_result;
            defer result.deinit();
            return result.greedy_token_id orelse try self.greedyTargetChoiceFromFinalHiddenDevice(result.hidden);
        }
        if (hiddenOnlyMaterializationEnabled()) {
            var result = if (materializationReplayEnabled())
                try self.forwardTargetHiddenDevice(
                    token_ids[total_seq_len - 1 .. total_seq_len],
                    1,
                    total_seq_len,
                    &decode_context,
                    hidden_source,
                    "gpt.mtp_materialize_choice_hidden",
                )
            else
                try self.forwardTargetHiddenDeviceNoReplay(
                    token_ids[total_seq_len - 1 .. total_seq_len],
                    1,
                    total_seq_len,
                    &decode_context,
                    hidden_source,
                );
            defer result.deinit();
            return try self.greedyTargetChoiceFromFinalHiddenDevice(result.hidden);
        }
        var result = try self.forwardAllLogitsAndHiddenDevice(
            token_ids[total_seq_len - 1 .. total_seq_len],
            1,
            total_seq_len,
            &decode_context,
        );
        defer result.deinit();
        return try self.greedyTargetChoiceFromFinalHiddenDevice(result.hidden);
    }

    pub fn materializeAcceptedTokenKvAndReturnHidden(
        self: *Runtime,
        token_ids: []const i64,
        total_seq_len: usize,
        decode_state: *NativeDecodeState,
        hidden_source: HiddenSource,
    ) !MtpMaterializedHostActivation {
        var decode_runtime = BorrowedDecodeStateRuntime.init(decode_state);
        if (decode_state.requiresFullRecompute()) return error.MissingMaterializedHiddenState;
        _ = try decode_runtime.appendGeneratedToken();
        errdefer decode_runtime.truncateGeneratedTokens(1) catch {};
        const decode_context = decode_runtime.makeDecodeContext(total_seq_len, 1);
        if (try self.materializeAcceptedTokenKvActiveHiddenForMtp(
            token_ids,
            total_seq_len,
            &decode_context,
            hidden_source,
        )) |active_result| {
            var result = active_result;
            defer result.deinit();
            const activation = try self.dupeTargetHiddenRowFromHiddenDevice(&result, 0, hidden_source);
            errdefer self.context.allocator.free(activation);
            return .{
                .activation = activation,
                .next_target_choice = result.greedy_token_id orelse try self.greedyTargetChoiceFromFinalHiddenDevice(result.hidden),
            };
        }
        if (hiddenOnlyMaterializationEnabled()) {
            var result = if (materializationReplayEnabled())
                try self.forwardTargetHiddenDevice(
                    token_ids[total_seq_len - 1 .. total_seq_len],
                    1,
                    total_seq_len,
                    &decode_context,
                    hidden_source,
                    "gpt.mtp_materialize_host_hidden",
                )
            else
                try self.forwardTargetHiddenDeviceNoReplay(
                    token_ids[total_seq_len - 1 .. total_seq_len],
                    1,
                    total_seq_len,
                    &decode_context,
                    hidden_source,
                );
            defer result.deinit();
            const activation = try self.dupeTargetHiddenRowFromHiddenDevice(&result, 0, hidden_source);
            errdefer self.context.allocator.free(activation);
            return .{
                .activation = activation,
                .next_target_choice = try self.greedyTargetChoiceFromFinalHiddenDevice(result.hidden),
            };
        }
        var result = try self.forwardAllLogitsAndHiddenDevice(
            token_ids[total_seq_len - 1 .. total_seq_len],
            1,
            total_seq_len,
            &decode_context,
        );
        defer result.deinit();
        const activation = try self.dupeTargetHiddenRowFromDevice(&result, 0, hidden_source);
        errdefer self.context.allocator.free(activation);
        return .{
            .activation = activation,
            .next_target_choice = try self.greedyTargetChoiceFromFinalHiddenDevice(result.hidden),
        };
    }

    pub fn materializeAcceptedTokenKvAndCopyHiddenToBackend(
        self: *Runtime,
        token_ids: []const i64,
        total_seq_len: usize,
        decode_state: *NativeDecodeState,
        hidden_source: HiddenSource,
        dst_cb: *const ComputeBackend,
    ) !MtpMaterializedDeviceActivation {
        var decode_runtime = BorrowedDecodeStateRuntime.init(decode_state);
        if (decode_state.requiresFullRecompute()) return .{ .activation = null };
        _ = try decode_runtime.appendGeneratedToken();
        errdefer decode_runtime.truncateGeneratedTokens(1) catch {};
        const decode_context = decode_runtime.makeDecodeContext(total_seq_len, 1);
        if (try self.materializeAcceptedTokenKvActiveHiddenForMtp(
            token_ids,
            total_seq_len,
            &decode_context,
            hidden_source,
        )) |active_result| {
            var result = active_result;
            defer result.deinit();
            const activation = try self.copyTargetHiddenRowFromHiddenToBackend(&result, 0, hidden_source, dst_cb);
            errdefer if (activation) |tensor| dst_cb.free(tensor);
            return .{
                .activation = activation,
                .next_target_choice = result.greedy_token_id orelse try self.greedyTargetChoiceFromFinalHiddenDevice(result.hidden),
            };
        }
        if (hiddenOnlyMaterializationEnabled()) {
            var result = if (materializationReplayEnabled())
                try self.forwardTargetHiddenDevice(
                    token_ids[total_seq_len - 1 .. total_seq_len],
                    1,
                    total_seq_len,
                    &decode_context,
                    hidden_source,
                    "gpt.mtp_materialize_device_hidden",
                )
            else
                try self.forwardTargetHiddenDeviceNoReplay(
                    token_ids[total_seq_len - 1 .. total_seq_len],
                    1,
                    total_seq_len,
                    &decode_context,
                    hidden_source,
                );
            defer result.deinit();
            const activation = try self.copyTargetHiddenRowFromHiddenToBackend(&result, 0, hidden_source, dst_cb);
            errdefer if (activation) |tensor| dst_cb.free(tensor);
            return .{
                .activation = activation,
                .next_target_choice = try self.greedyTargetChoiceFromFinalHiddenDevice(result.hidden),
            };
        }
        var result = try self.forwardAllLogitsAndHiddenDevice(
            token_ids[total_seq_len - 1 .. total_seq_len],
            1,
            total_seq_len,
            &decode_context,
        );
        defer result.deinit();
        const activation = try self.copyTargetHiddenRowToBackend(&result, 0, hidden_source, dst_cb);
        errdefer if (activation) |tensor| dst_cb.free(tensor);
        return .{
            .activation = activation,
            .next_target_choice = try self.greedyTargetChoiceFromFinalHiddenDevice(result.hidden),
        };
    }

    fn forwardAllLogits(self: *Runtime, input_ids: []const i64, batch: usize, seq_len: usize, decode_context: *const gpt_arch.DecodeContext) ![]f32 {
        var fallback_count: u64 = 0;
        return graph_mod.decoder_step_executor.forwardAll(&self.context, self.graph_cache, input_ids, batch, seq_len, decode_context, self.replay_fallbacks orelse &fallback_count);
    }

    pub fn rejectUnsupportedDeepSeekV4GraphMode(self: *const Runtime) !void {
        if (!NativeDecodeState.requiresDeepSeekV4CompressedCache(self.context.gpt_config.*)) return;
        if (self.graph_cache != null or self.context.compiled_partition_backend != null)
            return error.DeepSeekV4CompressedGraphModeNotSupported;
    }

    pub fn forwardLastLogits(
        self: *Runtime,
        input_ids: []const i64,
        batch: usize,
        seq_len: usize,
        decode_context: *const gpt_arch.DecodeContext,
    ) ![]f32 {
        const query_seq_len = decode_context.query_sequence_len;
        try self.rejectUnsupportedDeepSeekV4GraphMode();
        if (self.graph_cache) |cache| {
            return self.graphForward(cache, input_ids, batch, seq_len, decode_context);
        }
        if (self.context.compiled_partition_backend != null) return error.MissingGraphCacheForCompiledPartitionBackend;

        // CUDA fast path: route the layer stack through the decoder-runtime
        // forward (graph-replay capable, fused kernels, paged attention).
        // This is what keeps sampled (non-greedy) decoding near greedy
        // throughput; the interpreter below runs one op at a time with heavy
        // per-op host overhead. Escape hatch:
        // ANTFLY_INFERENCE_CUDA_REPLAY_LAST_LOGITS=0.
        if (self.context.cb.kind() == .cuda and cudaReplayLastLogitsEnabled()) {
            if (gpt_arch.forwardLastLogitsWithCudaReplay(
                self.context.cb,
                self.context.allocator,
                self.context.gpt_config.*,
                input_ids,
                batch,
                seq_len,
                decode_context,
                "gpt.standard_last_logits",
            )) |logits| {
                return logits;
            } else |err| {
                if (self.replay_fallbacks) |counter| counter.* += 1;
                debugGenerationStage(
                    "forwardLastLogits replay path failed err={s}; falling back to interpreter",
                    .{@errorName(err)},
                );
            }
        }

        const logits = try gpt_arch.forward(self.context.cb, self.context.allocator, self.context.gpt_config.*, input_ids, batch, seq_len, decode_context);
        defer self.context.allocator.free(logits);
        const last_pos_offset = (query_seq_len - 1) * @as(usize, @intCast(self.context.gpt_config.*.vocab_size));
        return try self.context.allocator.dupe(f32, logits[last_pos_offset..][0..@intCast(self.context.gpt_config.*.vocab_size)]);
    }

    pub fn forwardCompiledGreedy(self: *Runtime, input_ids: []const i64, batch: usize, seq_len: usize, decode_context: *const gpt_arch.DecodeContext) !?usize {
        try self.rejectUnsupportedDeepSeekV4GraphMode();
        const cache = self.graph_cache orelse return null;
        if (self.context.compiled_partition_backend == null or self.context.compiled_attachment_target != .whole_model) return null;

        const token_id = (try graph_mod.execution.graphForwardCompiledModelGreedyToken(
            &self.context,
            cache,
            input_ids,
            batch,
            seq_len,
            decode_context,
            self.context.gpt_config.*.vocab_size,
        )) orelse return null;
        if (token_id < 0) return error.InvalidModelOutput;
        return @intCast(token_id);
    }

    pub fn forwardCompiledSampled(self: *Runtime, input_ids: []const i64, seq_len: usize, decode_context: *const gpt_arch.DecodeContext, sampling: graph_mod.model_runtime.SamplingConfig, token_history: []const i64, eligible: bool) !?usize {
        if (self.context.cb.kind() != .metal or !compiledSampledDecodeEnabled(self.context.gpt_config.*) or !eligible) return null;
        try self.rejectUnsupportedDeepSeekV4GraphMode();
        const cache = self.graph_cache orelse return null;
        if (self.context.compiled_partition_backend == null or self.context.compiled_attachment_target != .whole_model) return null;
        if (input_ids.len != 1 or decode_context.attention_mode != .paged_decode) return null;

        const token_id = (try graph_mod.execution.graphForwardCompiledModelSampledToken(
            &self.context,
            cache,
            .{
                .decode = .{
                    .execution_control = self.context.execution_control,
                    .token_id = input_ids[0],
                    .position = seq_len - 1,
                    .attention_mode = .paged_decode,
                },
                .sampling = sampling,
                .token_history = token_history,
            },
        )) orelse return null;
        if (token_id < 0) return error.InvalidModelOutput;
        return @intCast(token_id);
    }

    pub fn prepareCompiledGenerationRuntime(
        self: *Runtime,
        kv_tokens_hint: usize,
    ) !bool {
        const cache = self.graph_cache orelse return false;
        try self.rejectUnsupportedDeepSeekV4GraphMode();
        if (self.context.compiled_partition_backend == null or self.context.compiled_attachment_target != .whole_model) return false;
        // ponytail: Metal prefill prepares lazily; eager prepare is only a warmup and can be skipped.
        if (self.context.compiled_partition_backend == .metal) return false;
        return graph_mod.execution.prepareCompiledModelRuntime(&self.context, cache, kv_tokens_hint);
    }

    pub fn forwardAllLogitsAndHiddenHost(
        self: *Runtime,
        input_ids: []const i64,
        batch: usize,
        seq_len: usize,
        decode_context: *const gpt_arch.DecodeContext,
    ) !ForwardAllWithHiddenHost {
        try self.rejectUnsupportedDeepSeekV4GraphMode();
        if (self.context.compiled_partition_backend != null) return error.MissingGraphCacheForCompiledPartitionBackend;
        const allocator = self.context.allocator;
        const query_seq_len = decode_context.query_sequence_len;
        const total = batch * query_seq_len;
        const hidden_size: usize = @intCast(self.context.gpt_config.*.hidden_size);
        if (input_ids.len != total) return error.InvalidTensorShape;

        const embed_w = try gpt_arch.getEmbeddingWeight(self.context.cb, self.context.gpt_config.*);
        defer self.context.cb.free(embed_w);
        const embedded = try self.context.cb.embeddingLookup(embed_w, input_ids, total, hidden_size);
        const hidden_input = try gpt_arch.maybeScaleTokenEmbeddings(self.context.cb, allocator, self.context.gpt_config.*, embedded, total, hidden_size);

        const ple_vectors = try gpt_arch.computePleVectors(self.context.cb, allocator, self.context.gpt_config.*, input_ids, hidden_input, total);
        defer if (ple_vectors) |pv| self.context.cb.free(pv);

        const hidden_result = try gpt_arch.forwardFinalAndPreNormHiddenTensorFromEmbeddingsWithLayer0Overrides(
            self.context.cb,
            allocator,
            self.context.gpt_config.*,
            hidden_input,
            .{},
            batch,
            seq_len,
            decode_context,
            ple_vectors,
        );
        defer self.context.cb.free(hidden_result.final_hidden);
        defer self.context.cb.free(hidden_result.pre_norm_hidden);

        const lm_w = try gpt_arch.getLmHeadWeight(self.context.cb, self.context.gpt_config.*);
        defer self.context.cb.free(lm_w);

        const logits_ct = try self.context.cb.linearNoBias(
            hidden_result.final_hidden,
            lm_w,
            hidden_result.total_rows,
            self.context.gpt_config.*.hidden_size,
            self.context.gpt_config.*.vocab_size,
        );
        defer self.context.cb.free(logits_ct);

        const logits_host = try self.context.cb.toFloat32(logits_ct, allocator);
        gpt_arch.applyFinalLogitSoftcapInPlace(self.context.gpt_config.*, logits_host);
        errdefer allocator.free(logits_host);
        const hidden_host = try self.context.cb.toFloat32(hidden_result.final_hidden, allocator);
        errdefer allocator.free(hidden_host);
        const pre_norm_hidden_host = try self.context.cb.toFloat32(hidden_result.pre_norm_hidden, allocator);
        errdefer allocator.free(pre_norm_hidden_host);
        return .{
            .allocator = allocator,
            .logits = logits_host,
            .hidden = hidden_host,
            .pre_norm_hidden = pre_norm_hidden_host,
            .rows = hidden_result.total_rows,
        };
    }

    pub fn forwardHiddenDevice(
        self: *Runtime,
        input_ids: []const i64,
        batch: usize,
        seq_len: usize,
        decode_context: *const gpt_arch.DecodeContext,
    ) !ForwardHiddenDevice {
        try self.rejectUnsupportedDeepSeekV4GraphMode();
        if (self.context.compiled_partition_backend != null) return error.MissingGraphCacheForCompiledPartitionBackend;
        const allocator = self.context.allocator;
        const query_seq_len = decode_context.query_sequence_len;
        const total = batch * query_seq_len;
        const hidden_size: usize = @intCast(self.context.gpt_config.*.hidden_size);
        if (input_ids.len != total) return error.InvalidTensorShape;

        const embed_w = try gpt_arch.getEmbeddingWeight(self.context.cb, self.context.gpt_config.*);
        defer self.context.cb.free(embed_w);
        const embedded = try self.context.cb.embeddingLookup(embed_w, input_ids, total, hidden_size);
        const hidden_input = try gpt_arch.maybeScaleTokenEmbeddings(self.context.cb, allocator, self.context.gpt_config.*, embedded, total, hidden_size);

        const ple_vectors = try gpt_arch.computePleVectors(self.context.cb, allocator, self.context.gpt_config.*, input_ids, hidden_input, total);
        defer if (ple_vectors) |pv| self.context.cb.free(pv);

        const hidden_result = try gpt_arch.forwardFinalAndPreNormHiddenTensorFromEmbeddingsWithLayer0Overrides(
            self.context.cb,
            allocator,
            self.context.gpt_config.*,
            hidden_input,
            .{},
            batch,
            seq_len,
            decode_context,
            ple_vectors,
        );
        errdefer self.context.cb.free(hidden_result.final_hidden);
        errdefer self.context.cb.free(hidden_result.pre_norm_hidden);

        return .{
            .cb = self.context.cb,
            .hidden = hidden_result.final_hidden,
            .pre_norm_hidden = hidden_result.pre_norm_hidden,
            .rows = hidden_result.total_rows,
        };
    }

    pub fn tryMetalFinalHiddenFrame(
        self: *Runtime,
        input_ids: []const i64,
        batch: usize,
        seq_len: usize,
        retained_prefix_tokens: usize,
        decode_context: *const gpt_arch.DecodeContext,
    ) !?ForwardHiddenDevice {
        if (self.context.compiled_partition_backend != null or
            self.context.cb.kind() != .metal or
            self.context.gpt_config.*.family != .gemma or
            self.context.gpt_config.*.usesMoe() or
            !self.context.gpt_config.*.hasPle() or
            batch != 1 or
            input_ids.len == 0 or
            // Prompt-cache hits attach retained host blocks to a fresh
            // sequence. Until those rows have been materialized into the new
            // Metal slot, the direct frame may only own uncached prefixes.
            !metalFinalHiddenFrameOwnsResidentPrefix(retained_prefix_tokens) or
            decode_context.attention_mode != .paged_prefill or
            decode_context.query_sequence_len != input_ids.len)
        {
            return null;
        }

        const Attempt = struct {
            decoder: *Runtime,
            input_ids: []const i64,
            seq_len: usize,
            decode_context: *const gpt_arch.DecodeContext,
            result: ?ForwardHiddenDevice = null,

            fn run(context: *anyopaque) !bool {
                const attempt: *@This() = @ptrCast(@alignCast(context));
                const decoder = attempt.decoder;
                const prepared = try decoder.context.cb.decoderRuntimePrepareOrReuseFamily(
                    decoder.context.allocator,
                    decoder.context.gpt_config.*,
                    attempt.decode_context.kv_sequence_len,
                    decoder.context.gpt_config.*.num_hidden_layers,
                );
                if (!prepared.prepared) return false;

                const direct = (try decoder_gated_runtime.forwardFinalHiddenRows(
                    decoder.context.cb,
                    decoder.context.allocator,
                    decoder.context.gpt_config.*,
                    decoder.context.gpt_config.*.num_hidden_layers,
                    attempt.input_ids,
                    attempt.seq_len,
                    attempt.decode_context,
                )) orelse return false;
                attempt.result = .{
                    .cb = decoder.context.cb,
                    .hidden = direct.final_hidden,
                    .pre_norm_hidden = null,
                    .rows = direct.rows,
                    .prepared_tail_choices = direct.prepared_tail_choices,
                    .choices_allocator = direct.choices_allocator,
                };
                return true;
            }
        };

        var attempt = Attempt{
            .decoder = self,
            .input_ids = input_ids,
            .seq_len = seq_len,
            .decode_context = decode_context,
        };
        errdefer if (attempt.result) |result_value| {
            var result = result_value;
            result.deinit();
        };
        if (!try runWithDecoderRuntimeResetBoundary(self.context.cb, @ptrCast(&attempt), Attempt.run)) return null;
        const result = attempt.result orelse return null;
        attempt.result = null;
        return result;
    }

    pub fn forwardFinalHiddenDevice(
        self: *Runtime,
        input_ids: []const i64,
        batch: usize,
        seq_len: usize,
        retained_prefix_tokens: usize,
        decode_context: *const gpt_arch.DecodeContext,
    ) !ForwardHiddenDevice {
        return (try self.tryMetalFinalHiddenFrame(input_ids, batch, seq_len, retained_prefix_tokens, decode_context)) orelse
            self.forwardHiddenDevice(input_ids, batch, seq_len, decode_context);
    }

    pub fn forwardTargetHiddenDevice(
        self: *Runtime,
        input_ids: []const i64,
        batch: usize,
        seq_len: usize,
        decode_context: *const gpt_arch.DecodeContext,
        hidden_source: HiddenSource,
        replay_label: []const u8,
    ) !ForwardHiddenDevice {
        return self.forwardTargetHiddenDeviceReplayMode(
            input_ids,
            batch,
            seq_len,
            decode_context,
            hidden_source,
            replay_label,
            unsafeTargetReplayEnabled(),
            null,
        );
    }

    pub fn forwardTargetHiddenDevicePreparedArgmax(
        self: *Runtime,
        input_ids: []const i64,
        batch: usize,
        seq_len: usize,
        decode_context: *const gpt_arch.DecodeContext,
        hidden_source: HiddenSource,
        replay_label: []const u8,
        suppress_token_ids: []const i32,
    ) !ForwardHiddenDevice {
        return self.forwardTargetHiddenDeviceReplayMode(
            input_ids,
            batch,
            seq_len,
            decode_context,
            hidden_source,
            replay_label,
            unsafeTargetReplayEnabled(),
            suppress_token_ids,
        );
    }

    pub fn forwardTargetHiddenDeviceNoReplay(
        self: *Runtime,
        input_ids: []const i64,
        batch: usize,
        seq_len: usize,
        decode_context: *const gpt_arch.DecodeContext,
        hidden_source: HiddenSource,
    ) !ForwardHiddenDevice {
        return self.forwardTargetHiddenDeviceReplayMode(
            input_ids,
            batch,
            seq_len,
            decode_context,
            hidden_source,
            "",
            false,
            null,
        );
    }

    pub fn forwardTargetHiddenDeviceReplayMode(
        self: *Runtime,
        input_ids: []const i64,
        batch: usize,
        seq_len: usize,
        decode_context: *const gpt_arch.DecodeContext,
        hidden_source: HiddenSource,
        replay_label: []const u8,
        allow_replay: bool,
        prepared_tail_suppress_token_ids: ?[]const i32,
    ) !ForwardHiddenDevice {
        if (hidden_source != .final) {
            return self.forwardHiddenDevice(input_ids, batch, seq_len, decode_context);
        }
        try self.rejectUnsupportedDeepSeekV4GraphMode();
        if (self.context.compiled_partition_backend != null) return error.MissingGraphCacheForCompiledPartitionBackend;
        const allocator = self.context.allocator;
        const query_seq_len = decode_context.query_sequence_len;
        const total = batch * query_seq_len;
        const hidden_size: usize = @intCast(self.context.gpt_config.*.hidden_size);
        if (input_ids.len != total) return error.InvalidTensorShape;

        if (self.context.cb.kind() == .metal) {
            const verify_trace = hiddenRowsTraceEnabled();
            const trace_stats_before = if (verify_trace) decoder_gated_runtime.getTimingStats() else undefined;
            const trace_started_at = profileTimestamp(verify_trace, self.io);
            const direct_result = if (prepared_tail_suppress_token_ids) |suppress_token_ids|
                try decoder_gated_runtime.forwardFinalHiddenRowsPreparedArgmax(
                    self.context.cb,
                    allocator,
                    self.context.gpt_config.*,
                    self.context.gpt_config.*.num_hidden_layers,
                    input_ids,
                    seq_len,
                    decode_context,
                    suppress_token_ids,
                )
            else
                try decoder_gated_runtime.forwardFinalHiddenRows(
                    self.context.cb,
                    allocator,
                    self.context.gpt_config.*,
                    self.context.gpt_config.*.num_hidden_layers,
                    input_ids,
                    seq_len,
                    decode_context,
                );
            if (direct_result) |direct| {
                if (verify_trace) {
                    const wall_ns = profileElapsedNs(true, self.io, trace_started_at);
                    const after = decoder_gated_runtime.getTimingStats();
                    std.debug.print(
                        "mtp-verify-trace: rows={d} wall_us={d} embed_us={d} ple_us={d} frame_begin_us={d} layer_spec_us={d} plan_us={d} execute_us={d} block_us={d} finish_us={d}\n",
                        .{
                            direct.rows,
                            @divTrunc(wall_ns, std.time.ns_per_us),
                            @as(u64, @intCast(@divTrunc(after.prefill_embed_nanos - trace_stats_before.prefill_embed_nanos, std.time.ns_per_us))),
                            @as(u64, @intCast(@divTrunc(after.prefill_ple_prepare_nanos - trace_stats_before.prefill_ple_prepare_nanos, std.time.ns_per_us))),
                            @as(u64, @intCast(@divTrunc(after.prefill_frame_begin_nanos - trace_stats_before.prefill_frame_begin_nanos, std.time.ns_per_us))),
                            @as(u64, @intCast(@divTrunc(after.prefill_frame_layer_spec_nanos - trace_stats_before.prefill_frame_layer_spec_nanos, std.time.ns_per_us))),
                            @as(u64, @intCast(@divTrunc(after.prefill_frame_plan_nanos - trace_stats_before.prefill_frame_plan_nanos, std.time.ns_per_us))),
                            @as(u64, @intCast(@divTrunc(after.prefill_frame_execute_nanos - trace_stats_before.prefill_frame_execute_nanos, std.time.ns_per_us))),
                            @as(u64, @intCast(@divTrunc(after.prefill_block_nanos - trace_stats_before.prefill_block_nanos, std.time.ns_per_us))),
                            @as(u64, @intCast(@divTrunc(after.prefill_frame_finish_nanos - trace_stats_before.prefill_frame_finish_nanos, std.time.ns_per_us))),
                        },
                    );
                }
                return .{
                    .cb = self.context.cb,
                    .hidden = direct.final_hidden,
                    .pre_norm_hidden = null,
                    .rows = direct.rows,
                    .prepared_tail_choices = direct.prepared_tail_choices,
                    .choices_allocator = direct.choices_allocator,
                };
            }
        }

        const embed_w = try gpt_arch.getEmbeddingWeight(self.context.cb, self.context.gpt_config.*);
        defer self.context.cb.free(embed_w);
        const embedded = try self.context.cb.embeddingLookup(embed_w, input_ids, total, hidden_size);
        const hidden_input = try gpt_arch.maybeScaleTokenEmbeddings(self.context.cb, allocator, self.context.gpt_config.*, embedded, total, hidden_size);

        const ple_vectors = try gpt_arch.computePleVectors(self.context.cb, allocator, self.context.gpt_config.*, input_ids, hidden_input, total);
        defer if (ple_vectors) |pv| self.context.cb.free(pv);

        const hidden_result = if (allow_replay)
            try gpt_arch.forwardFinalHiddenTensorFromEmbeddingsWithLayer0OverridesCudaReplay(
                self.context.cb,
                allocator,
                self.context.gpt_config.*,
                hidden_input,
                .{},
                batch,
                seq_len,
                decode_context,
                ple_vectors,
                replay_label,
            )
        else
            try gpt_arch.forwardFinalHiddenTensorFromEmbeddingsWithLayer0Overrides(
                self.context.cb,
                allocator,
                self.context.gpt_config.*,
                hidden_input,
                .{},
                batch,
                seq_len,
                decode_context,
                ple_vectors,
            );
        errdefer self.context.cb.free(hidden_result.hidden);

        return .{
            .cb = self.context.cb,
            .hidden = hidden_result.hidden,
            .pre_norm_hidden = null,
            .rows = hidden_result.total_rows,
        };
    }

    pub fn forwardAllLogitsAndHiddenDevice(
        self: *Runtime,
        input_ids: []const i64,
        batch: usize,
        seq_len: usize,
        decode_context: *const gpt_arch.DecodeContext,
    ) !ForwardAllWithHiddenDevice {
        try self.rejectUnsupportedDeepSeekV4GraphMode();
        if (self.context.compiled_partition_backend != null) return error.MissingGraphCacheForCompiledPartitionBackend;
        const allocator = self.context.allocator;
        const query_seq_len = decode_context.query_sequence_len;
        const total = batch * query_seq_len;
        const hidden_size: usize = @intCast(self.context.gpt_config.*.hidden_size);
        if (input_ids.len != total) return error.InvalidTensorShape;

        const embed_w = try gpt_arch.getEmbeddingWeight(self.context.cb, self.context.gpt_config.*);
        defer self.context.cb.free(embed_w);
        const embedded = try self.context.cb.embeddingLookup(embed_w, input_ids, total, hidden_size);
        const hidden_input = try gpt_arch.maybeScaleTokenEmbeddings(self.context.cb, allocator, self.context.gpt_config.*, embedded, total, hidden_size);

        const ple_vectors = try gpt_arch.computePleVectors(self.context.cb, allocator, self.context.gpt_config.*, input_ids, hidden_input, total);
        defer if (ple_vectors) |pv| self.context.cb.free(pv);

        const hidden_result = try gpt_arch.forwardFinalAndPreNormHiddenTensorFromEmbeddingsWithLayer0Overrides(
            self.context.cb,
            allocator,
            self.context.gpt_config.*,
            hidden_input,
            .{},
            batch,
            seq_len,
            decode_context,
            ple_vectors,
        );
        errdefer self.context.cb.free(hidden_result.final_hidden);
        errdefer self.context.cb.free(hidden_result.pre_norm_hidden);

        const lm_w = try gpt_arch.getLmHeadWeight(self.context.cb, self.context.gpt_config.*);
        defer self.context.cb.free(lm_w);

        const logits_ct = try self.context.cb.linearNoBias(
            hidden_result.final_hidden,
            lm_w,
            hidden_result.total_rows,
            self.context.gpt_config.*.hidden_size,
            self.context.gpt_config.*.vocab_size,
        );
        errdefer self.context.cb.free(logits_ct);

        return .{
            .cb = self.context.cb,
            .logits = logits_ct,
            .hidden = hidden_result.final_hidden,
            .pre_norm_hidden = hidden_result.pre_norm_hidden,
            .rows = hidden_result.total_rows,
        };
    }

    pub fn dupeTargetHiddenRow(
        self: *Runtime,
        result: *const ForwardAllWithHiddenHost,
        row: usize,
        source: HiddenSource,
    ) ![]f32 {
        const hidden_size: usize = @intCast(self.context.gpt_config.*.hidden_size);
        if (row >= result.rows) return error.InvalidTensorShape;
        const hidden = switch (source) {
            .final => result.hidden,
            .pre_norm => result.pre_norm_hidden,
        };
        if (hidden.len != result.rows * hidden_size) return error.InvalidTensorShape;
        return try self.context.allocator.dupe(f32, hidden[row * hidden_size ..][0..hidden_size]);
    }

    pub fn dupeTargetHiddenRowFromDevice(
        self: *Runtime,
        result: *const ForwardAllWithHiddenDevice,
        row: usize,
        source: HiddenSource,
    ) ![]f32 {
        const hidden_size: usize = @intCast(self.context.gpt_config.*.hidden_size);
        if (row >= result.rows) return error.InvalidTensorShape;
        const hidden = switch (source) {
            .final => result.hidden,
            .pre_norm => result.pre_norm_hidden,
        };
        const row_ct = try self.context.cb.sliceRows2D(self.context.allocator, hidden, row, 1, hidden_size);
        defer self.context.cb.free(row_ct);
        const row_host = try self.context.cb.toFloat32(row_ct, self.context.allocator);
        errdefer self.context.allocator.free(row_host);
        if (row_host.len != hidden_size) return error.InvalidTensorShape;
        return row_host;
    }

    pub fn dupeTargetHiddenRowFromHiddenDevice(
        self: *Runtime,
        result: *const ForwardHiddenDevice,
        row: usize,
        source: HiddenSource,
    ) ![]f32 {
        const hidden_size: usize = @intCast(self.context.gpt_config.*.hidden_size);
        if (row >= result.rows) return error.InvalidTensorShape;
        const hidden = switch (source) {
            .final => result.hidden,
            .pre_norm => result.pre_norm_hidden orelse return error.MissingMaterializedHiddenState,
        };
        const row_ct = try self.context.cb.sliceRows2D(self.context.allocator, hidden, row, 1, hidden_size);
        defer self.context.cb.free(row_ct);
        const row_host = try self.context.cb.toFloat32(row_ct, self.context.allocator);
        errdefer self.context.allocator.free(row_host);
        if (row_host.len != hidden_size) return error.InvalidTensorShape;
        return row_host;
    }

    pub fn copyTargetHiddenRowToBackend(
        self: *Runtime,
        result: *const ForwardAllWithHiddenDevice,
        row: usize,
        source: HiddenSource,
        dst_cb: *const ComputeBackend,
    ) !?ops.CT {
        const hidden_size: usize = @intCast(self.context.gpt_config.*.hidden_size);
        if (row >= result.rows) return error.InvalidTensorShape;
        const hidden = switch (source) {
            .final => result.hidden,
            .pre_norm => result.pre_norm_hidden,
        };
        const row_ct = try self.context.cb.sliceRows2D(self.context.allocator, hidden, row, 1, hidden_size);
        defer self.context.cb.free(row_ct);
        return try dst_cb.copyTensorFromBackend(self.context.cb, row_ct);
    }

    pub fn copyTargetHiddenRowFromHiddenToBackend(
        self: *Runtime,
        result: *const ForwardHiddenDevice,
        row: usize,
        source: HiddenSource,
        dst_cb: *const ComputeBackend,
    ) !?ops.CT {
        const hidden_size: usize = @intCast(self.context.gpt_config.*.hidden_size);
        if (row >= result.rows) return error.InvalidTensorShape;
        const hidden = switch (source) {
            .final => result.hidden,
            .pre_norm => result.pre_norm_hidden orelse return error.MissingMaterializedHiddenState,
        };
        const row_ct = try self.context.cb.sliceRows2D(self.context.allocator, hidden, row, 1, hidden_size);
        defer self.context.cb.free(row_ct);
        return try dst_cb.copyTensorFromBackend(self.context.cb, row_ct);
    }

    pub fn graphForward(
        self: *Runtime,
        cache: *graph_mod.cache.GraphCache,
        input_ids: []const i64,
        batch: usize,
        seq_len: usize,
        decode_context: *const gpt_arch.DecodeContext,
    ) ![]f32 {
        const allocator = self.context.allocator;
        if (try graph_mod.execution.graphForwardCompiledModelLast(&self.context, cache, input_ids, batch, seq_len, decode_context)) |last_logits| {
            return last_logits;
        }
        if (self.context.compiled_partition_backend != null and self.context.compiled_attachment_target == .whole_model) {
            return error.MissingCompiledModelRuntime;
        }

        const logits = try graph_mod.execution.graphForwardAll(&self.context, cache, input_ids, batch, seq_len, decode_context);
        defer allocator.free(logits);
        const query_seq_len = decode_context.query_sequence_len;
        const vocab_size = self.context.gpt_config.*.vocab_size;

        if (query_seq_len == 0 or vocab_size == 0) return error.InvalidTensorShape;
        const width: usize = @intCast(vocab_size);
        const end = try std.math.mul(usize, query_seq_len, width);
        if (end > logits.len) return error.InvalidTensorShape;
        return allocator.dupe(f32, logits[end - width .. end]);
    }
};

pub fn cudaReplayLastLogitsEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_REPLAY_LAST_LOGITS", true);
}

pub fn metalFinalHiddenFrameOwnsResidentPrefix(retained_prefix_tokens: usize) bool {
    return retained_prefix_tokens == 0;
}

pub fn unsafeTargetReplayEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_GEMMA4_MTP_UNSAFE_TARGET_REPLAY", false);
}

pub fn profileTimestamp(enabled: bool, io_opt: ?std.Io) std.Io.Timestamp {
    if (!enabled) return std.Io.Timestamp.zero;
    return if (io_opt) |io| std.Io.Timestamp.now(io, .awake) else std.Io.Timestamp.zero;
}

pub fn profileElapsedNs(enabled: bool, io_opt: ?std.Io, started_at: std.Io.Timestamp) u64 {
    if (!enabled) return 0;
    if (std.meta.eql(started_at, std.Io.Timestamp.zero)) return 0;
    const finished_at = if (io_opt) |io| std.Io.Timestamp.now(io, .awake) else return 0;
    const elapsed = started_at.durationTo(finished_at).nanoseconds;
    if (elapsed <= 0) return 0;
    return @intCast(elapsed);
}

pub fn hiddenRowsTraceEnabled() bool {
    return platform.env.getenvBool("ANTFLY_GEMMA4_MTP_VERIFY_TRACE");
}

fn debugGenerationStage(comptime fmt: []const u8, args: anytype) void {
    if (!platform.env.getenvBool("TERMITE_GEN_STAGE_DEBUG")) return;
    std.debug.print("gen_debug: " ++ fmt ++ "\n", args);
}

test "physical decoder forward runs without a task and retains the actual backend owner" {
    const path = platform.env.getenv("ANTFLY_UNIFIED_DECODER_FIXTURE") orelse return error.SkipZigTest;
    const factory = @import("../architectures/session_factory.zig");
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |metal| {
        if (metal and !@import("build_options").enable_metal) continue;
        var session = if (metal) try factory.createMetalSession(a, path) else try factory.createNativeSession(a, path);
        defer session.close();
        factory.attachIo(session, std.testing.io);
        var backend = try factory.getComputeBackend(session, a);
        defer backend.deinit();
        const config = factory.getGptConfig(session).?;
        const ids = [_]i64{ 1, 2, 3, 4 };
        const decode = gpt_arch.DecodeContext{
            .attention_mode = .full_recompute,
            .total_sequence_len = ids.len,
            .query_sequence_len = ids.len,
            .kv_sequence_len = ids.len,
        };
        const reference = try gpt_arch.forward(&backend, a, config, &ids, 1, ids.len, &decode);
        defer a.free(reference);
        var decoder = Runtime{ .context = .{ .allocator = a, .cb = &backend, .gpt_config = &config } };
        const logits = try decoder.forwardLastLogits(&ids, 1, ids.len, &decode);
        defer a.free(logits);
        const width: usize = @intCast(config.vocab_size);
        try std.testing.expectEqualSlices(f32, reference[(ids.len - 1) * width ..], logits);
        var hidden = try decoder.forwardHiddenDevice(&ids, 1, ids.len, &decode);
        defer hidden.deinit();
        try std.testing.expect(hidden.cb == &backend);
        // Retire the temporary runtime adapter before materializing/freeing its
        // resident result. Both operations must still use the real backend.
        decoder = undefined;
        const values = try hidden.cb.toFloat32(hidden.hidden, a);
        defer a.free(values);
        try std.testing.expectEqual(ids.len * config.hidden_size, values.len);
        for (values) |value| try std.testing.expect(std.math.isFinite(value));
    }
}

pub fn compiledSampledDecodeDefaultEligible(cfg: @import("../models/gpt.zig").Config) bool {
    // Default promotion covers Gemma4 E2B. Other model geometries retain their
    // explicit opt-in, and the exact sampling contract is checked separately.
    return cfg.family == .gemma and cfg.gemma4_channel_protocol and
        !cfg.gemma4_mtp_assistant and cfg.hidden_size == 1536 and
        cfg.num_hidden_layers == 35 and cfg.num_attention_heads == 8 and
        cfg.num_key_value_heads == 1 and cfg.attention_head_dim == 256 and
        cfg.global_head_dim == 512 and cfg.sliding_window == 512 and
        cfg.intermediate_size == 6144 and cfg.num_kv_shared_layers == 20;
}

pub fn compiledSampledDecodeEnabled(cfg: @import("../models/gpt.zig").Config) bool {
    return metal_runtime.appleSimdgroupFeatureEnabled(
        "ANTFLY_INFERENCE_METAL_RESIDENT_NUCLEUS",
        compiledSampledDecodeDefaultEligible(cfg),
    );
}

pub fn hiddenOnlyMaterializationEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_GEMMA4_MTP_HIDDEN_ONLY_MATERIALIZE", true);
}

pub fn activeMaterializationEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_GEMMA4_MTP_ACTIVE_MATERIALIZE", true);
}

pub fn activeMaterializationSupported(
    kind: ops.BackendKind,
    config: @import("../models/gpt.zig").Config,
    total_seq_len: usize,
) bool {
    // Without a whole-model generated route, the Metal active frame can hand
    // a mixed SWA/global model back to generic attention after device-only KV
    // has been written. Past the local window that fallback cannot reconstruct
    // the complete KV span. Use the existing hidden-only materialization path,
    // which owns the full one-token forward, for long-context MTP instead.
    return !(kind == .metal and
        config.supportsSplitSwaGlobalKvRing() and
        total_seq_len > @as(usize, config.sliding_window));
}

pub fn materializationReplayEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_GEMMA4_MTP_MATERIALIZE_REPLAY", false);
}

pub fn isSuppressedToken(token_id: u32, token_ids: []const i32) bool {
    for (token_ids) |candidate| {
        if (candidate >= 0 and @as(u32, @intCast(candidate)) == token_id) return true;
    }
    return false;
}

fn getenvBool(comptime name: [*:0]const u8) bool {
    return platform.env.getenvBool(name);
}

fn populateDeviceStepWeights(store: *@import("../ops/native_compute.zig").WeightStore) !void {
    const fixture = @import("../ops/media_residency_test.zig");
    try fixture.putWeight(store, "model.embed_tokens.weight", &.{ 4, 4 }, null);
    try fixture.putWeight(store, "model.norm.weight", &.{4}, 1);
}

fn checkActualDeviceStep(cb_value: ComputeBackend, reference_value: ComputeBackend) !void {
    const a = std.testing.allocator;
    const cb = &cb_value;
    const reference = &reference_value;
    const config = @import("../models/gpt.zig").Config{ .family = .llama, .hidden_size = 4, .num_hidden_layers = 0, .num_attention_heads = 1, .vocab_size = 4, .weight_tying = true, .norm_type = .rms_norm, .position_encoding = .rope };
    var state = NativeDecodeState.initContiguous(a);
    defer state.deinit();
    try state.notePrefill(2);
    var borrowed = BorrowedDecodeStateRuntime.init(&state);
    const decode = borrowed.makeDecodeContext(2, 1);
    const expected = try gpt_arch.forwardGreedyLastToken(reference, a, config, &.{1}, 1, 2, &decode);
    var numerical = Runtime{ .context = .{ .allocator = a, .cb = cb, .gpt_config = &config, .weight_semantics = .traced_decoder } };
    var events = DeviceStepEvents{};
    const result = try numerical.forwardDeviceStep(&state, .{ .token_ids = &.{ 0, 1 }, .sequence_length = 2 }, &events);
    try std.testing.expect(result == .token);
    defer if (result.token.token_tensor) |tensor| cb.free(tensor);
    try std.testing.expectEqual(expected, result.token.token);
    try std.testing.expectEqual(@as(usize, 2), state.total_tokens);
    try std.testing.expectEqualDeep(DeviceStepEvents{}, events);
    // An unsupported physical sampled frame is a pre-numerical decline; the
    // task retains its canonical sampler and full-logits compatibility route.
    const declined = try numerical.forwardDeviceStep(&state, .{ .token_ids = &.{ 0, 1 }, .sequence_length = 2, .sampling = .{ .temperature = 0.5 } }, &events);
    try std.testing.expect(declined == .declined);
    try std.testing.expectEqual(@as(usize, 2), state.total_tokens);
    try std.testing.expectEqualDeep(DeviceStepEvents{}, events);
}

test "decoder device step matches independent Native greedy output without advancing task state" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    try @import("../ops/media_residency_test.zig").onNative(populateDeviceStepWeights, checkActualDeviceStep);
}

test "decoder device step preserves actual Metal greedy output and pre-numerical sampled decline" {
    try @import("../ops/media_residency_test.zig").onMetal(populateDeviceStepWeights, checkActualDeviceStep);
}
