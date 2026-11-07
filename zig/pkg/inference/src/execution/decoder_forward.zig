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

/// Owns one resident token across physical steps. The backend and optional lane
/// are borrowed from their real owners, never from a temporary Runtime adapter.
/// Moving a token empties the source; cancellation cannot prevent retirement.
pub const DeviceToken = struct {
    backend: *const ComputeBackend,
    access: ?@import("../execution_control.zig").ExecutionAccess = null,
    tensor: ?ops.CT = null,

    pub fn clear(self: *DeviceToken) void {
        const tensor = self.take() orelse return;
        var scope = if (self.access) |access| access.acquireReleaseScope() else null;
        defer if (scope) |*owned| owned.deinit();
        self.backend.free(tensor);
    }

    pub fn take(self: *DeviceToken) ?ops.CT {
        const tensor = self.tensor;
        self.tensor = null;
        return tensor;
    }

    /// Transfers a newly owned handle, retiring the previous step's handle.
    pub fn replace(self: *DeviceToken, tensor: ?ops.CT) void {
        self.clear();
        self.tensor = tensor;
    }

    pub fn resolve(self: *const DeviceToken, allocator: std.mem.Allocator) !usize {
        const tensor = self.tensor orelse return error.MissingDeviceToken;
        var scope = if (self.access) |access| try access.acquireScope() else null;
        defer if (scope) |*owned| owned.deinit();
        return gpt_arch.resolveGreedyDeviceTokenTensor(self.backend, allocator, tensor);
    }

    pub fn seed(self: *DeviceToken, token: usize) !void {
        if (self.tensor != null) return error.DeviceTokenAlreadyOwned;
        var scope = if (self.access) |access| try access.acquireScope() else null;
        defer if (scope) |*owned| owned.deinit();
        const data = [_]i32{@intCast(token)};
        const tensor = try self.backend.fromInt32Shape(&data, &.{1});
        self.tensor = tensor;
    }
};

pub const DeviceTokenOutcome = struct { token: usize, token_tensor: ?ops.CT = null };
pub const DeviceStepEvents = struct { handoff_attempts: usize = 0, handoff_hits: usize = 0, handoff_fallbacks: usize = 0 };
pub const PendingCandidateRequest = struct {
    token_ids: []const i64,
    sequence_length: usize,
    generated_tokens: usize,
    input_token_tensor: ops.CT,
    pure_greedy: bool,
    grammar_active: bool,
    prefetch_drain_budget: usize = 4,
};
pub const PendingCandidateEvents = struct {
    tensor_route: ?DeviceRoute = null,
    scalar_route: ?DeviceRoute = null,
    attempts: usize = 0,
    handoff: DeviceStepEvents = .{},
};
pub const PendingCandidate = union(enum) { tensor: ops.CT, scalar: DeviceTokenOutcome, declined };

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

/// Task semantics that affect physical device-token placement. The runtime
/// inspects its actual backend/cache/state; no grammar or sampler is retained.
pub const DeviceRouteRequest = struct {
    mode: enum { scalar, tensor, seed },
    generated_tokens: usize,
    pure_greedy: bool,
    grammar_active: bool,
};
pub const DeviceRoute = enum {
    eligible,
    backend_not_device,
    graph_blocked,
    first_token_blocked,
    kv_missing,
    non_greedy,
    grammar_blocked,
    handoff_disabled,
};

/// A synchronous hidden-row/head binding. The source hidden value remains
/// borrowed; only a sliced row view and the resolved head handle are owned.
/// No pointer to the temporary Runtime adapter escapes preparation.
pub const HeadRows = struct {
    backend: *const ComputeBackend,
    input: ops.CT,
    owns_input: bool,
    weight: ops.CT,

    pub fn deinit(self: *HeadRows) void {
        self.backend.free(self.weight);
        if (self.owns_input) self.backend.free(self.input);
        self.* = undefined;
    }
};

pub const HeadPreparationTrace = struct {
    started_at: std.Io.Timestamp,
    slice_us: u64 = 0,
    weight_us: u64 = 0,
};

/// Task semantics are resolved before this physical dispatch. Scheduling,
/// cached-prefill reuse and token acceptance remain with their task owner.
pub const EagerStepRequest = struct {
    token_history: []const i64,
    sequence_length: usize,
    generated_tokens: usize,
    has_prefill_logits: bool,
    compiled_sampling: ?graph_mod.model_runtime.SamplingConfig = null,
    compiled_sampling_eligible: bool = false,
    compiled_greedy_eligible: bool = false,
    prefetch_drain_budget: usize = 4,
    continuation: ?graph_mod.model_runtime.DecodeContinuation = null,
};

/// A token is a completed selection; logits are owned by the caller's runtime
/// allocator and retain the ordinary task sampler/grammar path.
pub const EagerStepResult = union(enum) { token: usize, logits: []f32 };

/// A synchronous host logit window owned by the actual invocation allocator.
/// Task code may sample rows without reconstructing the physical query layout.
pub const SpeculativeLogits = struct {
    allocator: std.mem.Allocator,
    values: []f32,
    query_rows: usize,
    vocabulary_size: usize,

    pub fn last(self: *const SpeculativeLogits) []const f32 {
        return self.values[(self.query_rows - 1) * self.vocabulary_size ..][0..self.vocabulary_size];
    }

    pub fn deinit(self: *SpeculativeLogits) void {
        self.allocator.free(self.values);
        self.* = undefined;
    }
};

pub fn reserveSpeculativeTargetVerification(
    decode_runtime: *BorrowedDecodeStateRuntime,
    backend: ops.BackendKind,
    target_config: @import("../models/gpt.zig").Config,
    query_len: usize,
    draft_count: usize,
) !void {
    // Qualified A4B Metal currently owns qLen=1 only. Reject the actual
    // multi-row shape before changing target KV; never replay it sequentially.
    if (backend == .metal and query_len > 1 and
        @import("../architectures/gemma4_runtime.zig").isQualifiedA4bArchitecture(target_config))
        return error.Gemma4A4bMetalSpeculativeVerificationNotQualified;
    _ = try decode_runtime.appendGeneratedTokens(draft_count);
}

/// The physical target suffix for a predictor verification round. Context
/// fields borrow the actual persistent state; no temporary adapter is retained.
pub const PredictorVerificationWindow = struct {
    state: *NativeDecodeState,
    sequence_length: usize,
    query_rows: usize,
    forward_rows: usize,
    logical_rows: usize,
    input_start: usize,
    draft_count: usize,
    context: gpt_arch.DecodeContext,

    pub fn rollbackForwardFailure(self: *const @This(), pending_count: usize) void {
        var physical = BorrowedDecodeStateRuntime.init(self.state);
        physical.truncateGeneratedTokens(self.draft_count + pending_count) catch {};
    }
};

pub const Runtime = struct {
    fn classifyDeviceStep(result: DeviceStepResult) @import("invocation_observation.zig").ResultClassification {
        return .{ .unsupported = result == .declined };
    }

    context: graph_mod.execution.Context,
    /// Explicitly owned by the whole-model caller, prepared cold from the
    /// original forward trace. No Session allocator implies this capability.
    native_program: ?*@import("native_decoder_program.zig").Program = null,
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

    pub fn deviceRoute(self: *const Runtime, state: *NativeDecodeState, request: DeviceRouteRequest) DeviceRoute {
        const kind = self.context.cb.kind();
        // Preserve the seed route's existing opt-out-before-probe behavior.
        if (request.mode == .seed and !deviceTokenHandoffEnabled()) return .handoff_disabled;
        if (if (request.mode == .scalar) kind != .cuda and kind != .metal else kind != .cuda) return .backend_not_device;
        if (self.graph_cache != null or self.context.compiled_partition_backend != null) return .graph_blocked;
        var decode = BorrowedDecodeStateRuntime.init(state);
        if (request.mode != .seed and request.generated_tokens == 0 and decode.kvView() == null) return .first_token_blocked;
        if (decode.kvView() == null) return .kv_missing;
        if (request.mode != .scalar and !request.pure_greedy) return .non_greedy;
        if (request.grammar_active) return .grammar_blocked;
        if (request.mode == .tensor and !deviceTokenHandoffEnabled()) return .handoff_disabled;
        return .eligible;
    }

    /// A pending greedy step keeps the original tensor-only -> scalar device
    /// fallback order. Each accepted callback retains its own observation;
    /// this selector neither wraps a second invocation nor publishes a token.
    pub fn pendingCandidate(self: *Runtime, state: *NativeDecodeState, request: PendingCandidateRequest, events: *PendingCandidateEvents) !PendingCandidate {
        if (!request.pure_greedy or request.grammar_active) return .declined;
        if (try self.pendingTensorCandidate(state, request, events)) |tensor| return .{ .tensor = tensor };
        events.attempts += 1;
        events.scalar_route = self.deviceRoute(state, .{ .mode = .scalar, .generated_tokens = request.generated_tokens, .pure_greedy = request.pure_greedy, .grammar_active = request.grammar_active });
        if (events.scalar_route.? != .eligible) return .declined;
        return switch (try self.forwardDeviceStep(state, .{ .token_ids = request.token_ids, .sequence_length = request.sequence_length, .input_token_tensor = request.input_token_tensor, .prefetch_drain_budget = request.prefetch_drain_budget }, &events.handoff)) {
            .token => |value| .{ .scalar = value },
            .declined => .declined,
            .logits => |values| {
                // Only sampled device steps return logits. This operation has
                // no sampling request and cannot replay a submitted fallback.
                self.context.allocator.free(values);
                return error.InvalidModelOutput;
            },
        };
    }

    /// Tensor-only lookahead must never resolve a scalar or select another
    /// physical forward after the pending position has been reserved. The
    /// regular pending selector shares this first attempt before its scalar
    /// fallback; actual route and partial callback events remain caller-owned.
    pub fn pendingTensorCandidate(self: *Runtime, state: *NativeDecodeState, request: PendingCandidateRequest, events: *PendingCandidateEvents) !?ops.CT {
        events.attempts += 1;
        events.tensor_route = self.deviceRoute(state, .{ .mode = .tensor, .generated_tokens = request.generated_tokens, .pure_greedy = request.pure_greedy, .grammar_active = request.grammar_active });
        if (events.tensor_route.? != .eligible) return null;
        return self.forwardDeviceTokenTensor(state, request.sequence_length, request.input_token_tensor, request.prefetch_drain_budget, &events.handoff);
    }

    pub fn forwardDeviceStep(self: *Runtime, state: *NativeDecodeState, request: DeviceStepRequest, events: *DeviceStepEvents) !DeviceStepResult {
        return @import("invocation_observation.zig").callContextOperationClassified(&self.context, .decode_step, classifyDeviceStep, @This().forwardDeviceStepUnobserved, .{ self, state, request, events });
    }

    fn forwardDeviceStepUnobserved(self: *Runtime, state: *NativeDecodeState, request: DeviceStepRequest, events: *DeviceStepEvents) !DeviceStepResult {
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
        return @import("invocation_observation.zig").callContextOperation(&self.context, .decode_step, @This().forwardDeviceTokenTensorUnobserved, .{ self, state, sequence_length, input, drain_budget, events });
    }

    fn forwardDeviceTokenTensorUnobserved(self: *Runtime, state: *NativeDecodeState, sequence_length: usize, input: ops.CT, drain_budget: usize, events: *DeviceStepEvents) !?ops.CT {
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
        return @import("invocation_observation.zig").callContextOperation(&self.context, .decode_step, @This().materializeAcceptedTokenKvUnobserved, .{ self, token_ids, total_seq_len, decode_state });
    }

    fn materializeAcceptedTokenKvUnobserved(
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

    /// Select only the requested resident vocabulary rows. Null preserves the
    /// backend's existing scalar fallback, with no eager host-logits export.
    /// The caller owns returned token IDs; acceptance, EOS and grammar remain
    /// task policy. Both paths use the same model suppression semantics.
    pub fn greedyLogitRows(self: *Runtime, logits: ops.CT, row_start: usize, row_count: usize) !?[]u32 {
        return @import("invocation_observation.zig").callContextOperation(&self.context, .decode_step, @This().greedyLogitRowsUnobserved, .{ self, logits, row_start, row_count });
    }

    fn greedyLogitRowsUnobserved(self: *Runtime, logits: ops.CT, row_start: usize, row_count: usize) !?[]u32 {
        const width: usize = @intCast(self.context.gpt_config.*.vocab_size);
        const suppress = self.context.gpt_config.*.suppressTokenIds();
        return if (suppress.len == 0)
            self.context.cb.argmaxRows(logits, row_start, row_count, width, self.context.allocator)
        else
            self.context.cb.argmaxRowsSuppress(logits, row_start, row_count, width, suppress, self.context.allocator);
    }

    pub fn greedyLogitRow(self: *Runtime, result: *const ForwardAllWithHiddenDevice, row: usize) !usize {
        return @import("invocation_observation.zig").callContextOperation(&self.context, .decode_step, @This().greedyLogitRowUnobserved, .{ self, result, row });
    }

    fn greedyLogitRowUnobserved(
        self: *Runtime,
        result: *const ForwardAllWithHiddenDevice,
        row: usize,
    ) !usize {
        const vocab_size: usize = @intCast(self.context.gpt_config.*.vocab_size);
        if (row >= result.rows) return error.InvalidTensorShape;
        const row_logits = try self.context.cb.sliceRows2D(self.context.allocator, result.logits, row, 1, vocab_size);
        defer self.context.cb.free(row_logits);
        const suppress_token_ids = self.context.gpt_config.*.suppressTokenIds();
        if (suppress_token_ids.len > 0) {
            const token_tensor = (try self.context.cb.argmaxLastRowSuppressTensor(row_logits, 1, vocab_size, suppress_token_ids)) orelse return error.UnsupportedBackend;
            defer self.context.cb.free(token_tensor);
            const token_host = try self.context.cb.toFloat32(token_tensor, self.context.allocator);
            defer self.context.allocator.free(token_host);
            if (token_host.len != 1 or token_host[0] < 0) return error.InvalidModelOutput;
            return @intFromFloat(token_host[0]);
        }
        const token = (try self.context.cb.argmaxLastRow(row_logits, 1, vocab_size)) orelse return error.UnsupportedBackend;
        return @intCast(token);
    }

    /// Bind the exact selected hidden rows and resolve the existing architecture
    /// weight adapter before trying either head implementation. This preserves
    /// the original load/error order even when a prepared tail is already ready.
    pub fn prepareHeadRows(self: *Runtime, source: *const ForwardHiddenDevice, start: usize, rows: usize, trace: ?*HeadPreparationTrace) !HeadRows {
        var input = source.hidden;
        var owns_input = false;
        errdefer if (owns_input) self.context.cb.free(input);
        if (start != 0 or source.rows != rows) {
            input = try self.context.cb.sliceRows2D(self.context.allocator, source.hidden, start, rows, @intCast(self.context.gpt_config.*.hidden_size));
            owns_input = true;
        }
        if (trace) |capture| capture.slice_us = @divTrunc(profileElapsedNs(true, self.io, capture.started_at), std.time.ns_per_us);
        const weight = try self.context.graphWeight("lm_head.weight");
        if (trace) |capture| capture.weight_us = @divTrunc(profileElapsedNs(true, self.io, capture.started_at), std.time.ns_per_us);
        return .{ .backend = self.context.cb, .input = input, .owns_input = owns_input, .weight = weight };
    }

    /// Prefer the same prepared quantized head used by decode frames; preserve
    /// a precomputed tail and leave a clean decline available to the caller.
    pub fn prepareHeadChoices(self: *Runtime, head: *const HeadRows, rows: usize, suppress: []const i32, prepared: *?[]u32) !void {
        if (prepared.* == null and self.context.cb.kind() == .metal) {
            prepared.* = try @import("invocation_observation.zig").callContextOperation(&self.context, .decode_step, decoder_gated_runtime.forwardPreparedLmHeadArgmaxRows, .{ self.context.cb, self.context.gpt_config.*, self.context.gpt_config.*.num_hidden_layers, head.input, rows, suppress, self.context.allocator });
        }
    }

    /// Transfer a ready tail exactly once, otherwise invoke the unchanged
    /// resident linear/argmax callback. Returned IDs belong to the caller.
    pub fn headChoices(self: *Runtime, head: *const HeadRows, rows: usize, suppress: []const i32, prepared: *?[]u32) !?[]u32 {
        if (prepared.*) |choices| {
            prepared.* = null;
            return choices;
        }
        return @import("invocation_observation.zig").callContextOperation(&self.context, .decode_step, @This().headChoicesUnobserved, .{ self, head, rows, suppress });
    }

    fn headChoicesUnobserved(self: *Runtime, head: *const HeadRows, rows: usize, suppress: []const i32) !?[]u32 {
        return self.context.cb.linearNoBiasArgmaxRowsSuppress(head.input, head.weight, rows, @intCast(self.context.gpt_config.*.hidden_size), @intCast(self.context.gpt_config.*.vocab_size), suppress, self.context.allocator);
    }

    pub fn requireHeadChoices(self: *Runtime, head: *const HeadRows, rows: usize, suppress: []const i32, prepared: *?[]u32) ![]u32 {
        return (try self.headChoices(head, rows, suppress, prepared)) orelse {
            const model = self.context.gpt_config.*;
            std.log.err(
                "gemma4_mtp_target_tail_argmax_unsupported: backend={s} rows={d} hidden={d} vocab={d} suppress={d} weight_tying={}",
                .{ @tagName(self.context.cb.kind()), rows, model.hidden_size, model.vocab_size, suppress.len, model.weight_tying },
            );
            return error.UnsupportedBackend;
        };
    }

    /// The task supplies acceptance semantics; the physical owner binds the
    /// already selected rows and head to the existing specialized region.
    pub fn verifyHeadChoices(self: *Runtime, head: *const HeadRows, rows: usize, suppress: []const i32, draft_tokens: []const i64, eos_token_ids: []const i32, accept_bonus: bool) !?ops.Gemma4MtpVerifyCommitResult {
        return @import("invocation_observation.zig").callContextOperation(&self.context, .decode_step, @This().verifyHeadChoicesUnobserved, .{ self, head, rows, suppress, draft_tokens, eos_token_ids, accept_bonus });
    }

    fn verifyHeadChoicesUnobserved(self: *Runtime, head: *const HeadRows, rows: usize, suppress: []const i32, draft_tokens: []const i64, eos_token_ids: []const i32, accept_bonus: bool) !?ops.Gemma4MtpVerifyCommitResult {
        return self.context.cb.gemma4MtpVerifyCommit(&.{ .input = head.input, .weight = head.weight, .rows = rows, .in_dim = @intCast(self.context.gpt_config.*.hidden_size), .out_dim = @intCast(self.context.gpt_config.*.vocab_size), .suppress_token_ids = suppress, .draft_tokens = draft_tokens, .eos_token_ids = eos_token_ids, .accept_bonus = accept_bonus, .allocator = self.context.allocator });
    }

    pub fn greedyTargetChoiceFromFinalHiddenDevice(
        self: *Runtime,
        final_hidden: ops.CT,
    ) !?u32 {
        return @import("invocation_observation.zig").callContextOperation(&self.context, .decode_step, @This().greedyTargetChoiceFromFinalHiddenDeviceUnobserved, .{ self, final_hidden });
    }

    fn greedyTargetChoiceFromFinalHiddenDeviceUnobserved(
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
        return @import("invocation_observation.zig").callContextOperation(&self.context, .forward, @This().materializeMtpCanonicalHiddenUnobserved, .{ self, token_ids, total_seq_len, decode_context, hidden_source });
    }

    fn materializeMtpCanonicalHiddenUnobserved(
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
        return @import("invocation_observation.zig").callContextOperation(&self.context, .forward, @This().materializeAcceptedTokenKvActiveHiddenForMtpUnobserved, .{ self, token_ids, total_seq_len, decode_context, hidden_source });
    }

    fn materializeAcceptedTokenKvActiveHiddenForMtpUnobserved(
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
        return @import("invocation_observation.zig").callContextOperation(&self.context, .decode_step, @This().materializeAcceptedTokenKvForMtpUnobserved, .{ self, token_ids, total_seq_len, decode_state, hidden_source });
    }

    fn materializeAcceptedTokenKvForMtpUnobserved(
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
        return @import("invocation_observation.zig").callContextOperation(&self.context, .decode_step, @This().materializeAcceptedTokenKvAndReturnHiddenUnobserved, .{ self, token_ids, total_seq_len, decode_state, hidden_source });
    }

    fn materializeAcceptedTokenKvAndReturnHiddenUnobserved(
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
        return @import("invocation_observation.zig").callContextOperation(&self.context, .decode_step, @This().materializeAcceptedTokenKvAndCopyHiddenToBackendUnobserved, .{ self, token_ids, total_seq_len, decode_state, hidden_source, dst_cb });
    }

    fn materializeAcceptedTokenKvAndCopyHiddenToBackendUnobserved(
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

    /// Reserve the real predictor verify suffix before deriving its query.
    /// Cached first choices omit the already verified row; pending tokens
    /// remain explicit supplied state facts and are never hidden in a cache key.
    pub fn beginPredictorVerification(self: *Runtime, state: *NativeDecodeState, sequence_length: usize, draft_count: usize, entry_pending: usize, cached_first: bool) !PredictorVerificationWindow {
        _ = self;
        if (draft_count == 0 or draft_count > 16) return error.InvalidSpeculativeK;
        if (entry_pending > 1 or (cached_first and entry_pending != 0)) return error.InvalidSpeculativeState;
        var physical = BorrowedDecodeStateRuntime.init(state);
        const appended = try std.math.add(usize, draft_count, entry_pending);
        if (try std.math.add(usize, state.total_tokens, appended) != sequence_length or sequence_length <= draft_count) return error.InvalidSpeculativeState;
        _ = try physical.appendGeneratedTokens(appended);
        errdefer physical.truncateGeneratedTokens(appended) catch {};
        if (state.total_tokens != sequence_length) return error.InvalidSpeculativeState;
        const forward_rows = if (cached_first) draft_count else draft_count + 1;
        const query_rows = if (physical.kvView() != null) forward_rows else sequence_length;
        return .{
            .state = state,
            .sequence_length = sequence_length,
            .query_rows = query_rows,
            .forward_rows = forward_rows,
            .logical_rows = draft_count + 1,
            .input_start = if (query_rows == sequence_length) 0 else if (cached_first) sequence_length - draft_count else sequence_length - query_rows,
            .draft_count = draft_count,
            .context = physical.makeDecodeContext(sequence_length, query_rows),
        };
    }

    /// Select the prepared hidden/head tail only when the real physical query
    /// covers exactly the requested projection rows. Historical full-history
    /// recomputation retains its original generic hidden producer.
    pub fn forwardPredictorHidden(self: *Runtime, window: *const PredictorVerificationWindow, history: []const i64, source: HiddenSource, suppress: []const i32) !ForwardHiddenDevice {
        const input = history[window.input_start..window.sequence_length];
        if (window.query_rows == window.forward_rows) return self.forwardTargetHiddenDevicePreparedArgmax(input, 1, window.sequence_length, &window.context, source, "gpt.mtp_verify_final_hidden", suppress);
        return self.forwardTargetHiddenDevice(input, 1, window.sequence_length, &window.context, source, "gpt.mtp_verify_final_hidden");
    }

    /// Draft KV is already positioned by the task's previous accepted proposal.
    /// Cached execution consumes one token; uncached execution recomputes history.
    pub fn speculativeDraftLogits(self: *Runtime, history: []const i64, sequence_length: usize, state: *NativeDecodeState) !SpeculativeLogits {
        var physical = BorrowedDecodeStateRuntime.init(state);
        const query_rows: usize = if (physical.kvView() != null) 1 else sequence_length;
        const decode = physical.makeDecodeContext(sequence_length, query_rows);
        const input = history[sequence_length - query_rows .. sequence_length];
        return .{ .allocator = self.context.allocator, .values = try self.forwardAllLogits(input, 1, sequence_length, &decode), .query_rows = query_rows, .vocabulary_size = self.context.gpt_config.vocab_size };
    }

    /// Reserve and forward the true target verification window. Failed physical
    /// execution restores both target and draft reservations in the original order;
    /// successful acceptance and prefix rollback remain task decisions.
    pub fn speculativeVerificationLogits(self: *Runtime, history: []const i64, sequence_length: usize, draft_count: usize, state: *NativeDecodeState, draft_state: *NativeDecodeState) !SpeculativeLogits {
        var physical = BorrowedDecodeStateRuntime.init(state);
        var draft = BorrowedDecodeStateRuntime.init(draft_state);
        const query_rows: usize = if (physical.kvView() != null) draft_count + 1 else sequence_length;
        // Draft proposals already reserved these tokens. Even an admission or
        // capability failure before target reservation must retire that suffix.
        errdefer draft.truncateGeneratedTokens(draft_count) catch {};
        try reserveSpeculativeTargetVerification(&physical, self.context.cb.kind(), self.context.gpt_config.*, query_rows, draft_count);
        errdefer physical.truncateGeneratedTokens(draft_count) catch {};
        const decode = physical.makeDecodeContext(sequence_length, query_rows);
        const input = history[sequence_length - query_rows .. sequence_length];
        const values = try self.forwardAllLogits(input, 1, sequence_length, &decode);
        return .{ .allocator = self.context.allocator, .values = values, .query_rows = query_rows, .vocabulary_size = self.context.gpt_config.vocab_size };
    }

    pub fn rejectUnsupportedDeepSeekV4GraphMode(self: *const Runtime) !void {
        if (!NativeDecodeState.requiresDeepSeekV4CompressedCache(self.context.gpt_config.*)) return;
        if (self.graph_cache != null or self.context.compiled_partition_backend != null)
            return error.DeepSeekV4CompressedGraphModeNotSupported;
    }

    /// Resolve eager query geometry from the actual state, then retain the
    /// original sampled-compiled, greedy-compiled and logits dispatch order.
    /// This borrows the caller's existing numerical lane; no state advances or
    /// fallback replay are added, and existing forward observations stay scoped
    /// to the actual selected callback.
    pub fn eagerStep(self: *Runtime, state: *NativeDecodeState, request: EagerStepRequest) !EagerStepResult {
        self.context.cb.drainPrefetchBudget(request.prefetch_drain_budget);
        var physical = BorrowedDecodeStateRuntime.init(state);
        const query_rows = if (physical.kvView() != null and (request.generated_tokens > 0 or !request.has_prefill_logits)) 1 else request.sequence_length;
        const decode = physical.makeDecodeContext(request.sequence_length, query_rows);
        const input_ids = if (query_rows == request.sequence_length)
            request.token_history[0..request.sequence_length]
        else
            request.token_history[request.sequence_length - query_rows .. request.sequence_length];
        debugGenerationStage(
            "standardDecode iter={d} requesting logits query_seq_len={d} input_len={d}",
            .{ request.generated_tokens, query_rows, input_ids.len },
        );
        // The borrowed stop-token view cannot escape into the persistent
        // adapter/context or cached ModelRuntime. Only these synchronous
        // compiled receivers see the current task decision.
        var scoped: Runtime = undefined;
        const compiled: *Runtime = if (request.continuation != null) receiver: {
            scoped = self.*;
            scoped.context.decode_continuation = request.continuation;
            break :receiver &scoped;
        } else self;
        if (request.compiled_sampling) |sampling| {
            if (try compiled.forwardCompiledSampled(input_ids, request.sequence_length, &decode, sampling, request.token_history[0..request.sequence_length], request.compiled_sampling_eligible)) |token| return .{ .token = token };
        }
        if (request.compiled_greedy_eligible) {
            if (try compiled.forwardCompiledGreedy(input_ids, 1, request.sequence_length, &decode)) |token| return .{ .token = token };
        }
        return .{ .logits = try self.forwardLastLogits(input_ids, 1, request.sequence_length, &decode) };
    }

    pub fn forwardLastLogits(
        self: *Runtime,
        input_ids: []const i64,
        batch: usize,
        seq_len: usize,
        decode_context: *const gpt_arch.DecodeContext,
    ) ![]f32 {
        return @import("invocation_observation.zig").callContextOperation(&self.context, .forward, @This().forwardLastLogitsUnobserved, .{ self, input_ids, batch, seq_len, decode_context });
    }

    fn forwardLastLogitsUnobserved(
        self: *Runtime,
        input_ids: []const i64,
        batch: usize,
        seq_len: usize,
        decode_context: *const gpt_arch.DecodeContext,
    ) ![]f32 {
        const query_seq_len = decode_context.query_sequence_len;
        try self.rejectUnsupportedDeepSeekV4GraphMode();
        const observation = @import("runtime_observation.zig");
        if (self.graph_cache == null) if (self.native_program) |program| {
            if (program.supportsCall(input_ids, batch, decode_context)) {
                const NativeCall = @import("native_decoder_program.zig").Call;
                var selected: ?NativeCall = program.beginCall(input_ids, decode_context, null) catch |err| switch (err) {
                    error.UnsupportedAuthoredNativeCall, error.UnsupportedAuthoredPagedAttention, error.AuthoredPagedWriteRequiresExclusiveBlock => declined: {
                        observation.nativeProgramFallback(.begin_call_declined, err);
                        break :declined null;
                    },
                    else => return err,
                };
                if (selected) |*call| {
                    const current = @import("cleanup_context.zig").Context{ .boundary = self.context.cleanupEpoch(), .operation = if (self.context.cb.call_context) |invocation| invocation.cleanup.operation else null };
                    defer call.close(current);
                    try call.prepareInto(self.context.allocator, null, current);
                    try call.run(self.context.cb, self.context.gpt_config.*, decode_context, current);
                    return try call.takeOutput();
                }
            } else observation.nativeProgramFallback(.unsupported_call, null);
        } else if (observation.kernelEnabled() and self.context.cb.kind() == .native) {
            observation.nativeProgramFallback(.no_prepared_native_program, null);
        };
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
        return @import("invocation_observation.zig").callContextOperation(&self.context, .forward, @This().forwardCompiledGreedyUnobserved, .{ self, input_ids, batch, seq_len, decode_context });
    }

    fn forwardCompiledGreedyUnobserved(self: *Runtime, input_ids: []const i64, batch: usize, seq_len: usize, decode_context: *const gpt_arch.DecodeContext) !?usize {
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
        return @import("invocation_observation.zig").callContextOperation(&self.context, .forward, @This().forwardCompiledSampledUnobserved, .{ self, input_ids, seq_len, decode_context, sampling, token_history, eligible });
    }

    fn forwardCompiledSampledUnobserved(self: *Runtime, input_ids: []const i64, seq_len: usize, decode_context: *const gpt_arch.DecodeContext, sampling: graph_mod.model_runtime.SamplingConfig, token_history: []const i64, eligible: bool) !?usize {
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
        return @import("invocation_observation.zig").callContextOperation(&self.context, .forward, @This().forwardAllLogitsAndHiddenHostUnobserved, .{ self, input_ids, batch, seq_len, decode_context });
    }

    fn forwardAllLogitsAndHiddenHostUnobserved(
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
        return @import("invocation_observation.zig").callContextOperation(&self.context, .forward, @This().forwardHiddenDeviceUnobserved, .{ self, input_ids, batch, seq_len, decode_context });
    }

    fn forwardHiddenDeviceUnobserved(
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
        return @import("invocation_observation.zig").callContextOperation(&self.context, .forward, @This().tryMetalFinalHiddenFrameUnobserved, .{ self, input_ids, batch, seq_len, retained_prefix_tokens, decode_context });
    }

    fn tryMetalFinalHiddenFrameUnobserved(
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
        return @import("invocation_observation.zig").callContextOperation(&self.context, .forward, @This().forwardFinalHiddenDeviceUnobserved, .{ self, input_ids, batch, seq_len, retained_prefix_tokens, decode_context });
    }

    fn forwardFinalHiddenDeviceUnobserved(
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
        return @import("invocation_observation.zig").callContextOperation(&self.context, .forward, @This().forwardTargetHiddenDeviceUnobserved, .{ self, input_ids, batch, seq_len, decode_context, hidden_source, replay_label });
    }

    fn forwardTargetHiddenDeviceUnobserved(
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
        return @import("invocation_observation.zig").callContextOperation(&self.context, .forward, @This().forwardTargetHiddenDevicePreparedArgmaxUnobserved, .{ self, input_ids, batch, seq_len, decode_context, hidden_source, replay_label, suppress_token_ids });
    }

    fn forwardTargetHiddenDevicePreparedArgmaxUnobserved(
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
        return @import("invocation_observation.zig").callContextOperation(&self.context, .forward, @This().forwardTargetHiddenDeviceNoReplayUnobserved, .{ self, input_ids, batch, seq_len, decode_context, hidden_source });
    }

    fn forwardTargetHiddenDeviceNoReplayUnobserved(
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
        return @import("invocation_observation.zig").callContextOperation(&self.context, .forward, @This().forwardTargetHiddenDeviceReplayModeUnobserved, .{ self, input_ids, batch, seq_len, decode_context, hidden_source, replay_label, allow_replay, prepared_tail_suppress_token_ids });
    }

    fn forwardTargetHiddenDeviceReplayModeUnobserved(
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
        return @import("invocation_observation.zig").callContextOperation(&self.context, .forward, @This().forwardAllLogitsAndHiddenDeviceUnobserved, .{ self, input_ids, batch, seq_len, decode_context });
    }

    fn forwardAllLogitsAndHiddenDeviceUnobserved(
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
        return @import("invocation_observation.zig").callContextOperation(&self.context, .forward, @This().dupeTargetHiddenRowUnobserved, .{ self, result, row, source });
    }

    fn dupeTargetHiddenRowUnobserved(
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
        return @import("invocation_observation.zig").callContextOperation(&self.context, .forward, @This().dupeTargetHiddenRowFromDeviceUnobserved, .{ self, result, row, source });
    }

    fn dupeTargetHiddenRowFromDeviceUnobserved(
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
        return @import("invocation_observation.zig").callContextOperation(&self.context, .forward, @This().dupeTargetHiddenRowFromHiddenDeviceUnobserved, .{ self, result, row, source });
    }

    fn dupeTargetHiddenRowFromHiddenDeviceUnobserved(
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
        return @import("invocation_observation.zig").callContextOperation(&self.context, .forward, @This().copyTargetHiddenRowToBackendUnobserved, .{ self, result, row, source, dst_cb });
    }

    fn copyTargetHiddenRowToBackendUnobserved(
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
        return @import("invocation_observation.zig").callContextOperation(&self.context, .forward, @This().copyTargetHiddenRowFromHiddenToBackendUnobserved, .{ self, result, row, source, dst_cb });
    }

    fn copyTargetHiddenRowFromHiddenToBackendUnobserved(
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
        return @import("invocation_observation.zig").callContextOperation(&self.context, .forward, @This().graphForwardUnobserved, .{ self, cache, input_ids, batch, seq_len, decode_context });
    }

    fn graphForwardUnobserved(
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

const DecoderObservationFixture = struct {
    const session_mod = @import("../backends/session.zig");
    const helper = @import("../ops/media_residency_test.zig");
    const weights = @import("testing/decoder.zig");
    const embedding_fn = @FieldType(ops.ComputeBackend.VTable, "embeddingLookup");
    var original_embedding: embedding_fn = undefined;
    var original_snapshot: @FieldType(ops.ComputeBackend.VTable, "debugTimingSnapshot") = null;
    var expected_gate: ?*std.atomic.Mutex = null;
    var snapshot_calls: usize = 0;
    fn snapshot(raw: *anyopaque) ops.BackendDebugTimingSnapshot {
        if (expected_gate) |gate| {
            const unlocked = gate.tryLock();
            if (unlocked) gate.unlock();
            std.debug.assert(!unlocked);
        }
        snapshot_calls += 1;
        return if (original_snapshot) |sample| sample(raw) else .{};
    }
    var expected_context: *anyopaque = undefined;
    var expect_registry: bool = false;
    var calls: usize = 0;
    var failure: ?anyerror = null;
    fn embedding(raw: *anyopaque, weight: ops.CT, ids: []const i64, total: usize, width: usize) !ops.CT {
        if (raw != expected_context) return error.DecoderObservationChangedBackend;
        if (@import("allocation_registry.zig").enabled() != expect_registry) return error.DecoderObservationChangedDisabledPath;
        calls += 1;
        if (failure) |err| return err;
        return original_embedding(raw, weight, ids, total, width);
    }
    fn info(_: *anyopaque) []const @import("../backends/tensor.zig").TensorInfo {
        return &.{};
    }
    fn kind(raw: *anyopaque) @import("../backends/backends.zig").BackendType {
        const backend: *const ComputeBackend = @ptrCast(@alignCast(raw));
        return if (backend.kind() == .metal) .metal else .native;
    }
    fn close(_: *anyopaque) void {}
    const table = session_mod.Session.VTable{ .run = undefined, .inputInfo = info, .outputInfo = info, .backend = kind, .close = close };
    fn check(actual: ComputeBackend, reference: ComputeBackend) !void {
        const a = std.testing.allocator;
        const cfg = weights.config();
        const ids = [_]i64{ 0, 1 };
        const expected = try weights.eager(reference, cfg, &ids);
        defer a.free(expected);
        var cb = actual;
        var backend_table = actual.vtable.*;
        original_embedding = actual.vtable.embeddingLookup;
        original_snapshot = actual.vtable.debugTimingSnapshot;
        backend_table.debugTimingSnapshot = snapshot;
        expected_gate = null;
        snapshot_calls = 0;
        expected_context = actual.ptr;
        backend_table.embeddingLookup = embedding;
        cb.vtable = &backend_table;
        calls = 0;
        failure = null;
        expect_registry = false;
        var diagnostics = @import("diagnostics.zig").Diagnostics{};
        const session = session_mod.Session{ .ptr = &cb, .vtable = &table, .invocation_diagnostics = .{ .diagnostics = &diagnostics, .stable_backend = actual } };
        var decoder = Runtime{ .context = .{ .allocator = a, .cb = &cb, .gpt_config = &cfg, .session = session } };
        const decode = gpt_arch.DecodeContext{ .attention_mode = .full_recompute, .total_sequence_len = ids.len, .query_sequence_len = ids.len, .kv_sequence_len = ids.len };
        const disabled = try decoder.forwardLastLogits(&ids, 1, ids.len, &decode);
        defer a.free(disabled);
        try std.testing.expect(diagnostics.snapshot() == null);
        const disabled_calls = calls;
        try std.testing.expect(disabled_calls != 0);
        for (expected, disabled) |want, got| try std.testing.expectApproxEqAbs(want, got, 0.001);
        diagnostics.configureBackend(true, &actual); // cold owner configuration
        expect_registry = true;
        const enabled = try decoder.forwardLastLogits(&ids, 1, ids.len, &decode);
        defer a.free(enabled);
        try std.testing.expectEqual(disabled_calls * 2, calls);
        for (disabled, enabled) |want, got| try std.testing.expectApproxEqAbs(want, got, 0.001);
        var measured = diagnostics.snapshot() orelse return error.MissingDecoderObservation;
        try std.testing.expect(measured.outcome == .success and measured.operation == .forward);
        try std.testing.expectEqual(actual.kind(), measured.context_backend.?);
        try std.testing.expect(measured.context_runtime_scope == .borrowed_execution_backend_context);
        try std.testing.expect(measured.execution == null); // eager has no program plan
        try std.testing.expect(measured.runtime.submission != null);
        if (actual.kind() == .metal) try std.testing.expect(measured.runtime.submission.?.metal_encoded_dispatches.? != 0);
        if (actual.kind() == .native) {
            var cache = graph_mod.cache.GraphCache.init(a);
            defer cache.deinit();
            decoder.graph_cache = &cache;
            defer decoder.graph_cache = null;
            const programmed = try decoder.forwardLastLogits(&ids, 1, ids.len, &decode);
            defer a.free(programmed);
            measured = diagnostics.snapshot().?;
            try std.testing.expect(measured.execution != null);
            try std.testing.expect(measured.execution_scope == .last_published_program);
            for (expected, programmed) |want, got| try std.testing.expectApproxEqAbs(want, got, 0.001);
        }
        // A backend's actual cancellation/failure result must replace success.
        failure = error.Cancelled;
        const single = gpt_arch.DecodeContext{ .attention_mode = .full_recompute, .total_sequence_len = 1, .query_sequence_len = 1, .kv_sequence_len = 1 };
        try std.testing.expectError(error.Cancelled, decoder.forwardLastLogits(ids[0..1], 1, 1, &single));
        measured = diagnostics.snapshot().?;
        try std.testing.expect(measured.outcome == .failed and measured.operation == .forward);
        try std.testing.expectEqualStrings("Cancelled", measured.error_name.?.view());
        try std.testing.expect(measured.execution == null);
        failure = error.ObservedDecoderFailure;
        try std.testing.expectError(error.ObservedDecoderFailure, decoder.forwardLastLogits(ids[0..1], 1, 1, &single));
        try std.testing.expectEqualStrings("ObservedDecoderFailure", diagnostics.snapshot().?.error_name.?.view());
        failure = null;
        // Real prefill transaction, including its early cancellation and clean
        // resize outcome, uses the same actual backend and publication owner.
        const prefill = @import("decoder_prefill.zig");
        var state = NativeDecodeState.initContiguous(a);
        defer state.deinit();
        var gate: std.atomic.Mutex = .unlocked;
        expected_gate = &gate;
        defer expected_gate = null;
        var owner = prefill.Runtime{ .forward = decoder, .state = &state, .execution = .{ .mutex = &gate } };
        const request = prefill.ChunkRequest{ .token_ids = &ids, .total_chunk_end = ids.len, .prompt_tokens = ids.len, .already_prefilled = 0, .processed_rows = 0, .output_intent = .last_logits, .capture_hidden = false, .shared_kv_pruning = false, .prepared_metal_whole_prompt = false, .metal_target_last_logits = false, .cuda_direct_last_logits = false, .typed_outputs = false, .attempted_chunk_size = 2, .allow_resize = true };
        const output = try owner.runChunk(request);
        defer if (output.complete.last_logits) |values| a.free(values);
        measured = diagnostics.snapshot().?;
        try std.testing.expect(measured.operation == .prefill and measured.outcome == .success);
        for (expected, output.complete.last_logits.?) |want, got| try std.testing.expectApproxEqAbs(want, got, 0.001);
        const Cancel = struct {
            fn check(_: ?*anyopaque) !void {
                if (@import("allocation_registry.zig").enabled()) return error.ObservationStartedBeforeLaneAcquisition;
                return error.Cancelled;
            }
        };
        owner.execution.control = .{ .check_fn = Cancel.check };
        const before_cancel = calls;
        const snapshots_before_cancel = snapshot_calls;
        try std.testing.expectError(error.Cancelled, owner.runChunk(request));
        try std.testing.expectEqual(before_cancel, calls);
        try std.testing.expectEqual(snapshots_before_cancel, snapshot_calls);
        measured = diagnostics.snapshot().?;
        try std.testing.expect(measured.outcome == .failed and measured.operation == .prefill);
        try std.testing.expectEqualStrings("Cancelled", measured.error_name.?.view());
        try std.testing.expect(measured.context_runtime_scope == .unavailable);
        try std.testing.expect(measured.runtime.submission == null and measured.runtime.cuda == null);
        expected_gate = null;
        diagnostics.configure(false);
        expect_registry = false;
        const after_disable = try decoder.forwardLastLogits(&ids, 1, ids.len, &decode);
        defer a.free(after_disable);
        try std.testing.expect(diagnostics.snapshot() == null);
        try std.testing.expect(cb.ptr == actual.ptr and cb.vtable == &backend_table);
        // Backend pointer/table and every callback result remain unchanged.
    }
};

test "physical decoder publishes actual Native prefill decode failures and disabled callback parity" {
    try DecoderObservationFixture.helper.onNative(DecoderObservationFixture.weights.populate, DecoderObservationFixture.check);
}

test "physical decoder publishes actual Metal prefill decode and explicit submission scopes" {
    try DecoderObservationFixture.helper.onMetal(DecoderObservationFixture.weights.populate, DecoderObservationFixture.check);
}

fn checkDevicePlacementOwner(cb: ComputeBackend, _: ComputeBackend) !void {
    const a = std.testing.allocator;
    const cfg = @import("testing/decoder.zig").config();
    var state = NativeDecodeState.initContiguous(a);
    defer state.deinit();
    const physical = Runtime{ .context = .{ .allocator = a, .cb = &cb, .gpt_config = &cfg } };
    const route = physical.deviceRoute(&state, .{ .mode = .scalar, .generated_tokens = 0, .pure_greedy = true, .grammar_active = false });
    try std.testing.expectEqual(if (cb.kind() == .native) DeviceRoute.backend_not_device else DeviceRoute.first_token_blocked, route);
    try std.testing.expectEqual(@as(usize, 0), state.total_tokens);
    // The same real backend and lane are used for placement and cancellation.
    // No copied callback table is accepted as evidence of a device route.
    const ctl = @import("../execution_control.zig");
    var mutex: std.atomic.Mutex = .unlocked;
    var lane: ctl.ExecutionLane = .{};
    const Cancel = struct {
        cancelled: bool = false,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (self.cancelled) return error.Cancelled;
        }
    };
    var cancel = Cancel{};
    const access = ctl.ExecutionAccess{ .mutex = &mutex, .gate = &lane, .control = .{ .ptr = &cancel, .check_fn = Cancel.check } };
    var owner = DeviceToken{ .backend = &cb, .access = access };
    defer owner.clear();
    cancel.cancelled = true;
    try std.testing.expectError(error.Cancelled, owner.seed(1));
    try @import("testing/decoder.zig").checkUnlocked(&mutex, &lane);
    cancel.cancelled = false;
    try owner.seed(1);
    try std.testing.expectError(error.DeviceTokenAlreadyOwned, owner.seed(2));
    const token = owner.tensor.?;
    try std.testing.expectEqual(@as(usize, 1), try owner.resolve(a));
    try @import("testing/decoder.zig").checkUnlocked(&mutex, &lane);
    try std.testing.expectEqual(@import("../backends/tensor.zig").DType.i32, try cb.tensorDType(token));
    const shape = try cb.tensorShape(token, a);
    defer a.free(shape);
    try std.testing.expectEqualSlices(i64, &.{1}, shape);
    cancel.cancelled = true;
    try std.testing.expectError(error.Cancelled, owner.resolve(a));
    owner.clear();
    try @import("testing/decoder.zig").checkUnlocked(&mutex, &lane);
    var current = DeviceToken{ .backend = &cb, .access = access };
    defer current.clear();
    var next = DeviceToken{ .backend = &cb, .access = access };
    defer next.clear();
    try std.testing.expectError(error.Cancelled, next.seed(2));
    try std.testing.expect(next.tensor == null);
    cancel.cancelled = false;
    try current.seed(2);
    try next.seed(3);
    const moved = next.tensor.?;
    // Handoff retires the prior input even after cancellation. The transferred
    // handle stays live until its receiving slot is cleared, exactly once.
    cancel.cancelled = true;
    current.replace(next.take());
    try std.testing.expect(next.tensor == null);
    try std.testing.expectEqual(moved, current.tensor.?);
    try std.testing.expectEqual(@import("../backends/tensor.zig").DType.i32, try cb.tensorDType(current.tensor.?));
    current.clear();
    current.clear();
    try std.testing.expect(current.tensor == null);
    try @import("testing/decoder.zig").checkUnlocked(&mutex, &lane);
    try std.testing.expectEqual(@as(usize, 0), state.total_tokens);
}

test "physical token placement actual Native cancellation and cleanup keep lane and decode state valid" {
    try @import("../ops/media_residency_test.zig").onNative(@import("../ops/media_residency_test.zig").noWeights, checkDevicePlacementOwner);
}

test "physical token placement actual Metal cancellation and cleanup keep lane and decode state valid" {
    try @import("../ops/media_residency_test.zig").onMetal(@import("../ops/media_residency_test.zig").noWeights, checkDevicePlacementOwner);
}

test "physical token placement actual Native allocation faults leave no physical token or lane" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const Case = struct {
        fn run(a: std.mem.Allocator) !void {
            const native = @import("../ops/native_compute.zig");
            var weights = native.WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
            defer weights.deinitOwned();
            var compute = native.NativeCompute.init(a, &weights, null);
            defer compute.deinit();
            const cb = compute.computeBackend();
            const ctl = @import("../execution_control.zig");
            var mutex: std.atomic.Mutex = .unlocked;
            var lane: ctl.ExecutionLane = .{};
            const access = ctl.ExecutionAccess{ .mutex = &mutex, .gate = &lane };
            defer @import("testing/decoder.zig").checkUnlocked(&mutex, &lane) catch @panic("leaked physical token lane");
            var current = DeviceToken{ .backend = &cb, .access = access };
            defer current.clear();
            var next = DeviceToken{ .backend = &cb, .access = access };
            defer next.clear();
            try current.seed(2);
            try next.seed(3);
            current.replace(next.take());
        }
    };
    try Case.run(std.testing.allocator);
    try platform.allocator.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "Native actual decoder without prepared Program records legacy attempt before unchanged missing weight failure" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const a = std.testing.allocator;
    const Native = @import("../ops/native_compute.zig");
    var store = Native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var compute = Native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    const cfg = @import("native_decoder_program.zig").Testing.config();
    const decode = gpt_arch.DecodeContext{ .total_sequence_len = 1, .query_sequence_len = 1, .kv_sequence_len = 1 };
    var original_error: ?anyerror = null;
    if (gpt_arch.forward(&cb, a, cfg, &.{2}, 1, 1, &decode)) |values| {
        a.free(values);
        return error.ExpectedMissingWeight;
    } else |err| original_error = err;
    var diagnostics = @import("diagnostics.zig").Diagnostics{};
    const binding = @import("invocation_observation.zig").Binding{ .diagnostics = &diagnostics };
    try std.testing.expect(binding.configure(true));
    var runtime = Runtime{ .context = .{ .allocator = a, .cb = &cb, .gpt_config = &cfg } };
    try std.testing.expectError(original_error.?, @import("invocation_observation.zig").call(binding, Runtime.forwardLastLogits, .{ &runtime, @as([]const i64, &.{2}), @as(usize, 1), @as(usize, 1), &decode }));
    const report = diagnostics.snapshot().?.report.?;
    try std.testing.expect(report.native_definition_templates == null);
    try std.testing.expectEqual(@as(u64, 1), report.authored_native_programs.?.fallback_no_prepared_native_program);
    try std.testing.expectEqual(@as(u8, 0), report.authored_native_programs.?.count);
}

fn checkPhysicalLogitRowSelection(backend_value: ops.ComputeBackend, _: ops.ComputeBackend) !void {
    const a = std.testing.allocator;
    const backend = &backend_value;
    var config = @import("../models/gpt.zig").Config{ .family = .llama, .hidden_size = 4, .num_hidden_layers = 0, .num_attention_heads = 1, .vocab_size = 4, .weight_tying = true, .norm_type = .rms_norm, .position_encoding = .rope };
    const logits = try backend.fromFloat32Shape(&.{ 9, 0, 1, 0, 0, 5, 4, 0, 0, 3, 0, 7 }, &.{ 3, 4 });
    defer backend.free(logits);
    var numerical = Runtime{ .context = .{ .allocator = a, .cb = backend, .gpt_config = &config } };
    const rows = ForwardAllWithHiddenDevice{ .cb = backend, .logits = logits, .hidden = logits, .pre_norm_hidden = logits, .rows = 3 };
    const choices = (try numerical.greedyLogitRows(logits, 1, 2)) orelse return error.MissingLogitChoices;
    defer a.free(choices);
    try std.testing.expectEqualSlices(u32, &.{ 1, 3 }, choices);
    for (choices, 1..) |choice, row| try std.testing.expectEqual(@as(usize, choice), try numerical.greedyLogitRow(&rows, row));
    try std.testing.expectError(error.InvalidTensorShape, numerical.greedyLogitRow(&rows, rows.rows));
    config.suppress_token_ids_len = 1;
    config.suppress_token_ids[0] = 3;
    const suppressed = (try numerical.greedyLogitRows(logits, 1, 2)) orelse return error.MissingLogitChoices;
    defer a.free(suppressed);
    try std.testing.expectEqualSlices(u32, &.{ 1, 1 }, suppressed);
    for (suppressed, 1..) |choice, row| {
        if (backend.vtable.argmaxLastRowSuppressTensor != null) {
            try std.testing.expectEqual(@as(usize, choice), try numerical.greedyLogitRow(&rows, row));
        } else {
            // Native has the original batched suppression callback but no
            // scalar-tensor callback. Preserve its explicit fallback refusal.
            try std.testing.expectError(error.UnsupportedBackend, numerical.greedyLogitRow(&rows, row));
        }
    }
}

test "physical decoder Native resident row selection preserves offset suppression and scalar fallback" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const helper = @import("../ops/media_residency_test.zig");
    try helper.onNative(helper.noWeights, checkPhysicalLogitRowSelection);
}

test "physical decoder Metal resident row selection preserves offset suppression and scalar fallback" {
    const helper = @import("../ops/media_residency_test.zig");
    try helper.onMetal(helper.noWeights, checkPhysicalLogitRowSelection);
}

fn checkPhysicalHeadRows(backend_value: ops.ComputeBackend, _: ops.ComputeBackend) !void {
    const a = std.testing.allocator;
    const backend = &backend_value;
    const config = @import("../models/gpt.zig").Config{ .family = .llama, .hidden_size = 4, .num_hidden_layers = 0, .num_attention_heads = 1, .vocab_size = 4, .weight_tying = true, .norm_type = .rms_norm, .position_encoding = .rope };
    const values = [_]f32{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };
    const hidden = try backend.fromFloat32Shape(&values, &.{ 3, 4 });
    defer backend.free(hidden);
    const source = ForwardHiddenDevice{ .cb = backend, .hidden = hidden, .pre_norm_hidden = null, .rows = 3 };
    var numerical = Runtime{ .context = .{ .allocator = a, .cb = backend, .gpt_config = &config, .weight_semantics = .traced_decoder } };
    var full = try numerical.prepareHeadRows(&source, 0, 3, null);
    defer full.deinit();
    try std.testing.expect(!full.owns_input);
    try std.testing.expect(full.input == hidden);
    var selected = try numerical.prepareHeadRows(&source, 1, 2, null);
    defer selected.deinit();
    try std.testing.expect(selected.owns_input);
    try std.testing.expect(selected.backend == backend);
    const selected_values = try backend.toFloat32(selected.input, a);
    defer a.free(selected_values);
    try std.testing.expectEqualSlices(f32, values[4..], selected_values);
    const ready = try a.dupe(u32, &.{ 1, 3 });
    var prepared: ?[]u32 = ready;
    defer if (prepared) |ids| a.free(ids);
    try numerical.prepareHeadChoices(&selected, 2, &.{}, &prepared);
    try std.testing.expect(prepared.?.ptr == ready.ptr);
    const taken = (try numerical.headChoices(&selected, 2, &.{}, &prepared)) orelse return error.MissingHeadChoices;
    defer a.free(taken);
    try std.testing.expect(taken.ptr == ready.ptr);
    try std.testing.expect(prepared == null);
    const expected = try backend.linearNoBiasArgmaxRowsSuppress(selected.input, selected.weight, 2, 4, 4, &.{}, a);
    defer if (expected) |ids| a.free(ids);
    const actual = try numerical.headChoices(&selected, 2, &.{}, &prepared);
    defer if (actual) |ids| a.free(ids);
    try std.testing.expectEqual(expected != null, actual != null);
    if (actual) |ids| try std.testing.expectEqualSlices(u32, expected.?, ids);
}

test "physical decoder Native head rows preserve borrowed source selected slice and one-time prepared tail transfer" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    try @import("../ops/media_residency_test.zig").onNative(populateDeviceStepWeights, checkPhysicalHeadRows);
}

test "physical decoder Metal head rows preserve borrowed source selected slice and one-time prepared tail transfer" {
    try @import("../ops/media_residency_test.zig").onMetal(populateDeviceStepWeights, checkPhysicalHeadRows);
}

fn checkActualEagerStep(backend_value: ComputeBackend, _: ComputeBackend) !void {
    const a = std.testing.allocator;
    const backend = &backend_value;
    const config = @import("../models/gpt.zig").Config{ .family = .llama, .hidden_size = 4, .num_hidden_layers = 0, .num_attention_heads = 1, .vocab_size = 4, .weight_tying = true, .norm_type = .rms_norm, .position_encoding = .rope };
    const ids = [_]i64{ 0, 1 };
    const kv = @import("../runtime/root.zig").kv;
    for ([_]bool{ false, true }) |paged| {
        var manager = kv.manager.KvManager.init(a);
        defer manager.deinit();
        const pool = try manager.addPool(.{ .backend = .native, .dtype = .f32, .page_size_tokens = 4, .num_kv_heads = 1, .head_dim = 4 });
        var state = if (paged) NativeDecodeState.initPaged(a, &manager, pool, null) else NativeDecodeState.initContiguous(a);
        defer state.deinit();
        try state.notePrefill(ids.len);
        for ([_]bool{ false, true }) |has_prefill| {
            const query_rows: usize = if (paged and !has_prefill) 1 else ids.len;
            var borrowed = BorrowedDecodeStateRuntime.init(&state);
            const decode = borrowed.makeDecodeContext(ids.len, query_rows);
            const expected = try gpt_arch.forward(backend, a, config, ids[ids.len - query_rows ..], 1, ids.len, &decode);
            defer a.free(expected);
            const before = state.total_tokens;
            var numerical = Runtime{ .context = .{ .allocator = a, .cb = backend, .gpt_config = &config, .weight_semantics = .traced_decoder } };
            const actual = try numerical.eagerStep(&state, .{
                .token_history = &ids,
                .sequence_length = ids.len,
                .generated_tokens = if (has_prefill) 0 else 1,
                .has_prefill_logits = has_prefill,
                // Both compiled variants must cleanly decline on this actual
                // uncompiled model and preserve the original logits path.
                .compiled_sampling = if (has_prefill) .{ .temperature = 0.7 } else null,
                .compiled_sampling_eligible = has_prefill,
                .compiled_greedy_eligible = !has_prefill,
            });
            const logits = switch (actual) {
                .token => return error.UnexpectedEagerToken,
                .logits => |values| values,
            };
            defer a.free(logits);
            try std.testing.expectEqualSlices(f32, expected[(query_rows - 1) * config.vocab_size ..], logits);
            try std.testing.expectEqual(before, state.total_tokens);
        }
    }
}

test "physical decoder Native eager step keeps actual KV query geometry compiled declines and task state" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    try @import("../ops/media_residency_test.zig").onNative(populateDeviceStepWeights, checkActualEagerStep);
}

test "physical decoder Metal eager step keeps actual KV query geometry compiled declines and task state" {
    try @import("../ops/media_residency_test.zig").onMetal(populateDeviceStepWeights, checkActualEagerStep);
}

fn checkActualPendingCandidate(cb_value: ComputeBackend, _: ComputeBackend) !void {
    const a = std.testing.allocator;
    const cb = &cb_value;
    const config = @import("../models/gpt.zig").Config{ .family = .llama, .hidden_size = 4, .num_hidden_layers = 0, .num_attention_heads = 1, .vocab_size = 4, .weight_tying = true, .norm_type = .rms_norm, .position_encoding = .rope };
    const kv = @import("../runtime/root.zig").kv;
    var manager = kv.manager.KvManager.init(a);
    defer manager.deinit();
    const pool = try manager.addPool(.{ .backend = if (cb.kind() == .metal) .metal else .native, .dtype = .f32, .page_size_tokens = 4, .num_layers_packed = 1, .num_kv_heads = 1, .head_dim = 4, .store_cpu_bytes = false });
    var state = NativeDecodeState.initPaged(a, &manager, pool, null);
    defer state.deinit();
    try state.notePrefill(2);
    var numerical = Runtime{ .context = .{ .allocator = a, .cb = cb, .gpt_config = &config, .weight_semantics = .traced_decoder } };
    const input = (try cb.fromInt32Shape(&.{1}, &.{ 1, 1 })) orelse return error.UnsupportedBackend;
    defer cb.free(input);
    var events = PendingCandidateEvents{};
    const request = PendingCandidateRequest{ .token_ids = &.{ 0, 1 }, .sequence_length = 2, .generated_tokens = 1, .input_token_tensor = input, .pure_greedy = true, .grammar_active = false };
    var tensor_events = PendingCandidateEvents{};
    try std.testing.expect(try numerical.pendingTensorCandidate(&state, request, &tensor_events) == null);
    try std.testing.expectEqual(@as(usize, 1), tensor_events.attempts);
    try std.testing.expectEqual(DeviceRoute.backend_not_device, tensor_events.tensor_route.?);
    try std.testing.expect(tensor_events.scalar_route == null);
    try std.testing.expectEqual(@as(usize, 0), tensor_events.handoff.handoff_attempts);
    try std.testing.expectEqual(@as(usize, 2), state.total_tokens);
    if (cb.kind() == .metal) {
        // This actual backend declines the CUDA-only tensor route, then runs
        // the original scalar callback against its real model/current KV view.
        var expected_events = DeviceStepEvents{};
        const expected = try numerical.forwardDeviceStep(&state, .{ .token_ids = request.token_ids, .sequence_length = 2, .input_token_tensor = input }, &expected_events);
        defer if (expected == .token) if (expected.token.token_tensor) |tensor| cb.free(tensor);
        try std.testing.expect(expected == .token);
        const actual = try numerical.pendingCandidate(&state, request, &events);
        defer if (actual == .scalar) if (actual.scalar.token_tensor) |tensor| cb.free(tensor);
        try std.testing.expect(actual == .scalar);
        try std.testing.expectEqual(expected.token.token, actual.scalar.token);
        try std.testing.expectEqual(DeviceRoute.backend_not_device, events.tensor_route.?);
        try std.testing.expectEqual(DeviceRoute.eligible, events.scalar_route.?);
        try std.testing.expectEqualDeep(expected_events, events.handoff);
    } else {
        try std.testing.expect(try numerical.pendingCandidate(&state, request, &events) == .declined);
        try std.testing.expectEqual(DeviceRoute.backend_not_device, events.tensor_route.?);
        try std.testing.expectEqual(DeviceRoute.backend_not_device, events.scalar_route.?);
        try std.testing.expectEqualDeep(DeviceStepEvents{}, events.handoff);
    }
    try std.testing.expectEqual(@as(usize, 2), events.attempts);
    try std.testing.expectEqual(@as(usize, 2), state.total_tokens);
    var grammar_events = PendingCandidateEvents{};
    var grammar_request = request;
    grammar_request.grammar_active = true;
    try std.testing.expect(try numerical.pendingCandidate(&state, grammar_request, &grammar_events) == .declined);
    try std.testing.expectEqualDeep(PendingCandidateEvents{}, grammar_events);
}

test "pending candidate preserves actual Native pre-numerical decline and mapped attempts" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    try @import("../ops/media_residency_test.zig").onNative(populateDeviceStepWeights, checkActualPendingCandidate);
}

test "pending candidate keeps actual Metal tensor decline scalar fallback output and handoff counts" {
    try @import("../ops/media_residency_test.zig").onMetal(populateDeviceStepWeights, checkActualPendingCandidate);
}

fn checkActualPredictorWindow(cb_value: ComputeBackend, _: ComputeBackend) !void {
    const a = std.testing.allocator;
    const cb = &cb_value;
    const config = @import("../models/gpt.zig").Config{ .family = .llama, .hidden_size = 4, .num_hidden_layers = 0, .num_attention_heads = 1, .vocab_size = 4, .weight_tying = true, .norm_type = .rms_norm, .position_encoding = .rope };
    var numerical = Runtime{ .context = .{ .allocator = a, .cb = cb, .gpt_config = &config, .weight_semantics = .traced_decoder } };
    const kv = @import("../runtime/root.zig").kv;
    var manager = kv.manager.KvManager.init(a);
    defer manager.deinit();
    const pool = try manager.addPool(.{ .backend = .native, .dtype = .f32, .page_size_tokens = 2, .num_layers_packed = 1, .num_kv_heads = 1, .head_dim = 4, .store_cpu_bytes = false });
    const history = [_]i64{ 0, 1, 2, 3, 1 };
    for ([_]bool{ false, true }) |paged| for ([_]usize{ 0, 1 }) |pending| for ([_]bool{ false, true }) |cached| {
        if (!paged and cached) continue;
        var state = if (paged) NativeDecodeState.initPaged(a, &manager, pool, null) else NativeDecodeState.initContiguous(a);
        defer state.deinit();
        try state.notePrefill(3 - pending);
        if (cached and pending != 0) {
            try std.testing.expectError(error.InvalidSpeculativeState, numerical.beginPredictorVerification(&state, 5, 2, pending, cached));
            try std.testing.expectEqual(3 - pending, state.total_tokens);
            continue;
        }
        const window = try numerical.beginPredictorVerification(&state, 5, 2, pending, cached);
        const expected_rows: usize = if (!paged) 5 else if (cached) 2 else 3;
        const expected_start: usize = if (!paged) 0 else if (cached) 3 else 2;
        try std.testing.expectEqual(expected_rows, window.query_rows);
        try std.testing.expectEqual(expected_start, window.input_start);
        try std.testing.expectEqual(@as(usize, 3), window.logical_rows);
        try std.testing.expectEqual(@as(usize, if (cached) 2 else 3), window.forward_rows);
        var original_context_owner = BorrowedDecodeStateRuntime.init(&state);
        const original_context = original_context_owner.makeDecodeContext(5, expected_rows);
        var expected = if (expected_rows == window.forward_rows)
            try numerical.forwardTargetHiddenDevicePreparedArgmax(history[expected_start..], 1, 5, &original_context, .final, "gpt.mtp_verify_final_hidden", &.{})
        else
            try numerical.forwardTargetHiddenDevice(history[expected_start..], 1, 5, &original_context, .final, "gpt.mtp_verify_final_hidden");
        defer expected.deinit();
        var actual = try numerical.forwardPredictorHidden(&window, &history, .final, &.{});
        defer actual.deinit();
        const expected_values = try cb.toFloat32(expected.hidden, a);
        defer a.free(expected_values);
        const actual_values = try cb.toFloat32(actual.hidden, a);
        defer a.free(actual_values);
        try std.testing.expectEqualSlices(f32, expected_values, actual_values);
        try std.testing.expectEqual(expected.rows, actual.rows);
        try std.testing.expectEqual(@as(usize, 5), state.total_tokens);
        window.rollbackForwardFailure(pending);
        try std.testing.expectEqual(3 - pending, state.total_tokens);
        if (paged) try std.testing.expectEqual(3 - pending, manager.tokenCount(state.sequence_id.?).?);
    };
}

test "predictor verification window preserves actual Native mapped rows cached first pending suffix and rollback" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    try @import("../ops/media_residency_test.zig").onNative(populateDeviceStepWeights, checkActualPredictorWindow);
}

fn checkActualCudaPendingCandidates(cb_value: ComputeBackend, _: ComputeBackend) !void {
    const a = std.testing.allocator;
    const cb = &cb_value;
    const config = @import("../models/gpt.zig").Config{ .family = .llama, .hidden_size = 4, .num_hidden_layers = 0, .num_attention_heads = 1, .vocab_size = 4, .weight_tying = true, .norm_type = .rms_norm, .position_encoding = .rope };
    const kv = @import("../runtime/root.zig").kv;
    var manager = kv.manager.KvManager.init(a);
    defer manager.deinit();
    const pool = try manager.addPool(.{ .backend = .cuda, .dtype = .f32, .page_size_tokens = 2, .num_layers_packed = 1, .num_kv_heads = 1, .head_dim = 4, .store_cpu_bytes = false });
    var state = NativeDecodeState.initPaged(a, &manager, pool, null);
    defer state.deinit();
    try state.notePrefill(2);
    var numerical = Runtime{ .context = .{ .allocator = a, .cb = cb, .gpt_config = &config, .weight_semantics = .traced_decoder } };
    const input = (try cb.fromInt32Shape(&.{1}, &.{ 1, 1 })) orelse return error.UnsupportedBackend;
    defer cb.free(input);
    const request = PendingCandidateRequest{ .token_ids = &.{ 0, 1 }, .sequence_length = 2, .generated_tokens = 1, .input_token_tensor = input, .pure_greedy = true, .grammar_active = false };
    var physical = BorrowedDecodeStateRuntime.init(&state);
    const decode = physical.makeDecodeContext(2, 1);
    if (decode.kv_cache == null) return error.SkipZigTest;
    try std.testing.expectEqual(@as(usize, 1), decode.query_sequence_len);

    const Check = struct {
        fn clear(backend: *const ComputeBackend, value: PendingCandidate) void {
            switch (value) {
                .tensor => |tensor| backend.free(tensor),
                .scalar => |token| if (token.token_tensor) |tensor| backend.free(tensor),
                .declined => {},
            }
        }
        fn resolve(backend: *const ComputeBackend, allocator: std.mem.Allocator, value: PendingCandidate) !?usize {
            return switch (value) {
                .tensor => |tensor| try (DeviceToken{ .backend = backend, .tensor = tensor }).resolve(allocator),
                .scalar => |token| token.token,
                .declined => null,
            };
        }
    };

    // Reproduce the old lookahead adapter directly: route metadata followed
    // by the SAME tensor callback, with no scalar fallback or host selection.
    var original_tensor_events = PendingCandidateEvents{ .attempts = 1 };
    original_tensor_events.tensor_route = numerical.deviceRoute(&state, .{ .mode = .tensor, .generated_tokens = 1, .pure_greedy = true, .grammar_active = false });
    const original_tensor = if (original_tensor_events.tensor_route.? == .eligible)
        try numerical.forwardDeviceTokenTensor(&state, 2, input, request.prefetch_drain_budget, &original_tensor_events.handoff)
    else
        null;
    defer if (original_tensor) |tensor| cb.free(tensor);
    var lookahead_events = PendingCandidateEvents{};
    const lookahead = try numerical.pendingTensorCandidate(&state, request, &lookahead_events);
    defer if (lookahead) |tensor| cb.free(tensor);
    try std.testing.expectEqualDeep(original_tensor_events, lookahead_events);
    try std.testing.expectEqual(@as(usize, 1), lookahead_events.attempts);
    try std.testing.expect(lookahead_events.scalar_route == null);
    try std.testing.expectEqual(original_tensor == null, lookahead == null);
    if (original_tensor) |tensor| {
        try std.testing.expectEqual(try (DeviceToken{ .backend = cb, .tensor = tensor }).resolve(a), try (DeviceToken{ .backend = cb, .tensor = lookahead.? }).resolve(a));
        try std.testing.expectEqual(@as(usize, 1), lookahead_events.handoff.handoff_hits);
    }

    // Independently reproduce the old candidate's tensor-then-scalar adapter
    // sequence. Actual default/opt-out policy controls every eligibility check;
    // this fixture never forces a route or changes a backend table.
    var original_events = PendingCandidateEvents{ .attempts = 1 };
    original_events.tensor_route = numerical.deviceRoute(&state, .{ .mode = .tensor, .generated_tokens = 1, .pure_greedy = true, .grammar_active = false });
    var original: PendingCandidate = .declined;
    if (original_events.tensor_route.? == .eligible) {
        if (try numerical.forwardDeviceTokenTensor(&state, 2, input, request.prefetch_drain_budget, &original_events.handoff)) |tensor| original = .{ .tensor = tensor };
    }
    if (original == .declined) {
        original_events.attempts += 1;
        original_events.scalar_route = numerical.deviceRoute(&state, .{ .mode = .scalar, .generated_tokens = 1, .pure_greedy = true, .grammar_active = false });
        if (original_events.scalar_route.? == .eligible) {
            const result = try numerical.forwardDeviceStep(&state, .{ .token_ids = request.token_ids, .sequence_length = 2, .input_token_tensor = input, .prefetch_drain_budget = request.prefetch_drain_budget }, &original_events.handoff);
            original = switch (result) {
                .token => |token| .{ .scalar = token },
                .declined => .declined,
                .logits => |values| {
                    a.free(values);
                    return error.UnexpectedSampledPendingResult;
                },
            };
        }
    }
    defer Check.clear(cb, original);
    var actual_events = PendingCandidateEvents{};
    const actual = try numerical.pendingCandidate(&state, request, &actual_events);
    defer Check.clear(cb, actual);
    try std.testing.expectEqual(std.meta.activeTag(original), std.meta.activeTag(actual));
    try std.testing.expectEqualDeep(original_events, actual_events);
    try std.testing.expectEqual(try Check.resolve(cb, a, original), try Check.resolve(cb, a, actual));
    try std.testing.expect(actual != .declined);
    if (actual == .tensor) try std.testing.expectEqual(@as(usize, 1), actual_events.attempts);
    if (actual == .scalar) try std.testing.expectEqual(@as(usize, 2), actual_events.attempts);
    try std.testing.expectEqual(@as(usize, 2), state.total_tokens);
    try std.testing.expectEqual(@as(usize, 2), manager.tokenCount(state.sequence_id.?).?);
    var guarded = request;
    guarded.grammar_active = true;
    var guarded_events = PendingCandidateEvents{};
    try std.testing.expect(try numerical.pendingCandidate(&state, guarded, &guarded_events) == .declined);
    try std.testing.expectEqualDeep(PendingCandidateEvents{}, guarded_events);
    guarded = request;
    guarded.pure_greedy = false;
    try std.testing.expect(try numerical.pendingCandidate(&state, guarded, &guarded_events) == .declined);
    try std.testing.expectEqualDeep(PendingCandidateEvents{}, guarded_events);
}

test "CUDA pending candidates preserve original tensor scalar callback sequence and tensor only lookahead" {
    try @import("../ops/media_residency_test.zig").onCuda(populateDeviceStepWeights, checkActualCudaPendingCandidates);
}
