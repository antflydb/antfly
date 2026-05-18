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
const cache_mod = @import("cache.zig");
const activations = @import("../backends/activations.zig");
const ops = @import("../ops/ops.zig");
const contracts = @import("backend_contracts.zig");

/// Backend-owned model runtime state.
///
/// This is intentionally separate from PartitionExecutor. A PartitionExecutor
/// fills node values inside a host-managed graph execution. A ModelRuntime is
/// the mutable per-session state for a compiled model that owns the whole
/// model execution path, including backend-side cache/KV state as backends grow
/// that capability.
pub const ModelRuntime = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        capabilities: ?*const fn (ctx: *anyopaque) RuntimeCapabilities = null,
        prepare: ?*const fn (
            ctx: *anyopaque,
            allocator: std.mem.Allocator,
            request: PrepareRequest,
        ) anyerror!bool = null,
        prefill: *const fn (
            ctx: *anyopaque,
            allocator: std.mem.Allocator,
            request: PrefillRequest,
        ) anyerror!ModelOutput,
        decode: ?*const fn (
            ctx: *anyopaque,
            allocator: std.mem.Allocator,
            request: DecodeRequest,
        ) anyerror!ModelOutput = null,
        decode_sample: ?*const fn (
            ctx: *anyopaque,
            allocator: std.mem.Allocator,
            request: SampledDecodeRequest,
        ) anyerror!SampledDecodeOutput = null,
        decode_greedy: ?*const fn (
            ctx: *anyopaque,
            allocator: std.mem.Allocator,
            request: DecodeRequest,
        ) anyerror!GreedyDecodeOutput = null,
        deinit: *const fn (ctx: *anyopaque) void,
        reset: ?*const fn (ctx: *anyopaque) anyerror!void = null,
        debug_timing_stats: ?*const fn (ctx: *anyopaque) RuntimeDebugTimingStats = null,
        reset_debug_timing_stats: ?*const fn (ctx: *anyopaque) void = null,
        print_debug_timing: ?*const fn (ctx: *anyopaque) void = null,
    };

    pub fn capabilities(self: *const ModelRuntime) RuntimeCapabilities {
        const caps_fn = self.vtable.capabilities orelse return .{
            .supports_decode = self.vtable.decode != null,
            .supports_sample_decode = self.vtable.decode_sample != null,
            .supports_greedy_decode = self.vtable.decode_greedy != null,
        };
        var caps = caps_fn(self.ptr);
        caps.supports_decode = caps.supports_decode and self.vtable.decode != null;
        caps.supports_sample_decode = caps.supports_sample_decode and self.vtable.decode_sample != null;
        caps.supports_greedy_decode = caps.supports_greedy_decode and self.vtable.decode_greedy != null;
        return caps;
    }

    pub fn reset(self: *ModelRuntime) !void {
        if (self.vtable.reset) |reset_fn| return reset_fn(self.ptr);
    }

    pub fn prepare(
        self: *ModelRuntime,
        allocator: std.mem.Allocator,
        request: PrepareRequest,
    ) !bool {
        const prepare_fn = self.vtable.prepare orelse return false;
        return prepare_fn(self.ptr, allocator, request);
    }

    pub fn debugTimingStats(self: *const ModelRuntime) RuntimeDebugTimingStats {
        if (self.vtable.debug_timing_stats) |stats_fn| {
            return stats_fn(self.ptr);
        }
        return .{};
    }

    pub fn resetDebugTimingStats(self: *ModelRuntime) void {
        if (self.vtable.reset_debug_timing_stats) |reset_fn| {
            reset_fn(self.ptr);
        }
    }

    pub fn printDebugTiming(self: *const ModelRuntime) void {
        if (self.vtable.print_debug_timing) |print_fn| {
            print_fn(self.ptr);
        }
    }

    pub fn prefill(
        self: *ModelRuntime,
        allocator: std.mem.Allocator,
        request: PrefillRequest,
    ) !ModelOutput {
        return self.vtable.prefill(self.ptr, allocator, request);
    }

    pub fn decode(
        self: *ModelRuntime,
        allocator: std.mem.Allocator,
        request: DecodeRequest,
    ) !ModelOutput {
        const decode_fn = self.vtable.decode orelse return error.UnsupportedDecode;
        return decode_fn(self.ptr, allocator, request);
    }

    pub fn decodeGreedy(
        self: *ModelRuntime,
        allocator: std.mem.Allocator,
        request: DecodeRequest,
    ) !GreedyDecodeOutput {
        const decode_greedy_fn = self.vtable.decode_greedy orelse return error.UnsupportedGreedyDecode;
        return decode_greedy_fn(self.ptr, allocator, request);
    }

    pub fn decodeSample(
        self: *ModelRuntime,
        allocator: std.mem.Allocator,
        request: SampledDecodeRequest,
    ) !SampledDecodeOutput {
        const decode_sample_fn = self.vtable.decode_sample orelse return error.UnsupportedSampleDecode;
        return decode_sample_fn(self.ptr, allocator, request);
    }

    pub fn deinit(self: *ModelRuntime) void {
        self.vtable.deinit(self.ptr);
        self.* = undefined;
    }
};

pub const RuntimeStateOwnership = enum {
    /// Runtime execution still materializes graph inputs from host state.
    /// This can use ModelRuntime orchestration, but the backend does not own
    /// persistent model/KV state yet.
    host_assisted_inputs,
    /// Runtime owns persistent KV/cache tensors, but they remain host-side
    /// tensors fed back into each backend call.
    runtime_owned_host_cache,
    /// Runtime execution owns persistent backend-side model/KV/cache state.
    backend_owned,
};

pub const RuntimeCapabilities = struct {
    supports_decode: bool = false,
    supports_sample_decode: bool = false,
    supports_greedy_decode: bool = false,
    state_ownership: RuntimeStateOwnership = .host_assisted_inputs,
};

pub const RuntimeDebugTimingStats = struct {
    backend: ops.BackendDebugTimingSnapshot = .{},
    decoder_runtime_ready: bool = false,
    decoder_runtime_absolute_embeddings_prepared: bool = false,
    runtime_prepare_calls: u64 = 0,
    runtime_prepare_nanos: u128 = 0,
    runtime_prepare_family_nanos: u128 = 0,
    runtime_prepare_greedy_nanos: u128 = 0,
    runtime_prepare_fast_hits: u64 = 0,
    prefill_calls: u64 = 0,
    prefill_prepare_nanos: u128 = 0,
    prefill_direct_last_logits_nanos: u128 = 0,
    prefill_direct_family_nanos: u128 = 0,
    prefill_direct_family_project_nanos: u128 = 0,
    prefill_direct_family_span_prep_nanos: u128 = 0,
    prefill_direct_family_quant_attn_nanos: u128 = 0,
    prefill_direct_family_block_apply_nanos: u128 = 0,
    prefill_direct_family_frame_wait_nanos: u128 = 0,
    prefill_direct_family_frame_gpu_nanos: u128 = 0,
    prefill_fallback_logits_nanos: u128 = 0,
    decode_begin_step_nanos: u128 = 0,
    decode_sample_calls: u64 = 0,
    decode_sample_direct_nanos: u128 = 0,
    decode_sample_fallback_nanos: u128 = 0,
    decode_greedy_calls: u64 = 0,
    decode_greedy_direct_nanos: u128 = 0,
    decode_greedy_fallback_nanos: u128 = 0,
    ensure_prepared_calls: u64 = 0,
    ensure_prepared_nanos: u128 = 0,
    ensure_prepared_sync_nanos: u128 = 0,
    ensure_prepared_family_nanos: u128 = 0,
    ensure_prepared_greedy_nanos: u128 = 0,
    ensure_prepared_fast_hits: u64 = 0,
};

pub const SamplingConfig = struct {
    temperature: f32 = 0,
    top_p: f32 = 0,
    top_k: i32 = 0,
    min_p: f32 = 0,
    repetition_penalty: f32 = 1.0,
    frequency_penalty: f32 = 0,
    presence_penalty: f32 = 0,

    pub fn isPureGreedy(self: SamplingConfig) bool {
        return self.temperature <= 0 and
            self.repetition_penalty == 1.0 and
            self.frequency_penalty == 0 and
            self.presence_penalty == 0;
    }
};

pub const ModelOutput = struct {
    pub const PreparedTailNorm = enum {
        layer,
        rms,
    };

    pub const PreparedTail = struct {
        final_hidden: contracts.CT,
        backend: ops.ComputeBackend,
        norm: PreparedTailNorm,
        norm_slot: usize,
        lm_head_slot: usize,
        hidden_size: usize,
        vocab_size: usize,
        eps: f32,
        final_logit_softcap: f32 = 0.0,

        fn logits(self: *PreparedTail) !?contracts.CT {
            return switch (self.norm) {
                .layer => self.backend.decoderRuntimeApplyLayerNormLinear(&.{
                    .input = self.final_hidden,
                    .norm_slot = self.norm_slot,
                    .linear_slot = self.lm_head_slot,
                    .hidden_size = self.hidden_size,
                    .eps = self.eps,
                    .out_dim = self.vocab_size,
                }),
                .rms => self.backend.decoderRuntimeApplyRmsNormLinear(&.{
                    .input = self.final_hidden,
                    .norm_slot = self.norm_slot,
                    .linear_slot = self.lm_head_slot,
                    .hidden_size = self.hidden_size,
                    .eps = self.eps,
                    .out_dim = self.vocab_size,
                }),
            };
        }

        fn greedyToken(self: *PreparedTail) !?i64 {
            // Final logit softcap is monotonic, so it changes sampling
            // probabilities but not the greedy argmax ordering.
            const token = switch (self.norm) {
                .layer => try self.backend.decoderRuntimeApplyLayerNormLinearArgmax(&.{
                    .input = self.final_hidden,
                    .norm_slot = self.norm_slot,
                    .linear_slot = self.lm_head_slot,
                    .hidden_size = self.hidden_size,
                    .eps = self.eps,
                    .out_dim = self.vocab_size,
                }),
                .rms => try self.backend.decoderRuntimeApplyRmsNormLinearArgmax(&.{
                    .input = self.final_hidden,
                    .norm_slot = self.norm_slot,
                    .linear_slot = self.lm_head_slot,
                    .hidden_size = self.hidden_size,
                    .eps = self.eps,
                    .out_dim = self.vocab_size,
                }),
            };
            return if (token) |token_id| @intCast(token_id) else null;
        }

        fn deinit(self: *PreparedTail) void {
            self.backend.free(self.final_hidden);
            self.* = undefined;
        }
    };

    /// Last-step logits owned by the caller.
    logits: ?[]f32 = null,
    device_logits: ?contracts.CT = null,
    device_logits_backend: ?ops.ComputeBackend = null,
    prepared_tail: ?PreparedTail = null,
    final_logit_softcap: f32 = 0.0,

    pub fn hostLogits(self: *ModelOutput, allocator: std.mem.Allocator) ![]const f32 {
        if (self.logits) |logits| return logits;
        if (self.prepared_tail) |*tail| {
            const device_logits = (try tail.logits()) orelse return error.InvalidModelOutput;
            const backend = tail.backend;
            const logits = try backend.toFloat32(device_logits, allocator);
            backend.free(device_logits);
            applyFinalLogitSoftcapInPlace(logits, tail.final_logit_softcap);
            tail.deinit();
            self.prepared_tail = null;
            self.logits = logits;
            return logits;
        }
        if (self.device_logits) |device_logits| {
            const backend = self.device_logits_backend orelse return error.InvalidModelOutput;
            const logits = try backend.toFloat32(device_logits, allocator);
            backend.free(device_logits);
            self.device_logits = null;
            self.device_logits_backend = null;
            applyFinalLogitSoftcapInPlace(logits, self.final_logit_softcap);
            self.logits = logits;
            return logits;
        }
        return error.InvalidModelOutput;
    }

    pub fn takeHostLogits(self: *ModelOutput, allocator: std.mem.Allocator) ![]f32 {
        _ = try self.hostLogits(allocator);
        const logits = self.logits orelse return error.InvalidModelOutput;
        self.logits = null;
        return logits;
    }

    pub fn greedyToken(self: *ModelOutput, allocator: std.mem.Allocator, vocab_size: usize) !i64 {
        if (vocab_size == 0) return error.InvalidModelOutput;
        if (self.prepared_tail) |*tail| {
            if (tail.vocab_size == vocab_size) {
                if (try tail.greedyToken()) |token_id| return token_id;
            }
        }
        if (self.device_logits) |device_logits| {
            const backend = self.device_logits_backend orelse return error.InvalidModelOutput;
            if (try backend.argmaxLastRow(device_logits, 1, vocab_size)) |token_id| {
                return @intCast(token_id);
            }
        }

        const logits = try self.hostLogits(allocator);
        if (logits.len < vocab_size) return error.InvalidModelOutput;
        var best_idx: usize = 0;
        for (logits[1..vocab_size], 1..) |value, idx| {
            if (value > logits[best_idx]) best_idx = idx;
        }
        return @intCast(best_idx);
    }

    pub fn deinit(self: *ModelOutput, allocator: std.mem.Allocator) void {
        if (self.logits) |logits| allocator.free(logits);
        if (self.device_logits) |device_logits| {
            if (self.device_logits_backend) |backend| backend.free(device_logits);
        }
        if (self.prepared_tail) |*tail| tail.deinit();
        self.* = undefined;
    }
};

fn applyFinalLogitSoftcapInPlace(logits: []f32, softcap: f32) void {
    if (softcap <= 0.0) return;
    for (logits) |*value| {
        value.* = std.math.tanh(value.* / softcap) * softcap;
    }
}

pub const PrepareRequest = struct {
    /// Best-effort prompt/KV length hint. Backends may use this to prebuild
    /// shape-specific resources without mutating logical generation state.
    kv_tokens_hint: usize = 0,
};

pub const PrefillRequest = struct {
    input_ids: []const i64,
    seq_len: usize,
    query_seq_len: usize,
    attention_mode: cache_mod.AttentionMode,
};

pub const DecodeRequest = struct {
    token_id: i64,
    position: usize,
    attention_mode: cache_mod.AttentionMode = .paged_decode,
};

pub const SampledDecodeRequest = struct {
    decode: DecodeRequest,
    sampling: SamplingConfig,
    token_history: []const i64,
};

pub const GreedyDecodeOutput = struct {
    token_id: i64,
};

pub const SampledDecodeOutput = struct {
    token_id: i64,
};

pub const ForwardRequest = union(enum) {
    prefill: PrefillRequest,
    decode: DecodeRequest,
};

const SamplingPenaltyState = struct {
    counts: std.AutoHashMapUnmanaged(u32, u32) = .empty,

    fn deinit(self: *SamplingPenaltyState, allocator: std.mem.Allocator) void {
        self.counts.deinit(allocator);
        self.* = .{};
    }

    fn seedFromHistory(self: *SamplingPenaltyState, allocator: std.mem.Allocator, token_history: []const i64) !void {
        for (token_history) |token_id| try self.noteToken(allocator, token_id);
    }

    fn noteToken(self: *SamplingPenaltyState, allocator: std.mem.Allocator, token_id: i64) !void {
        if (token_id < 0) return;
        const entry = try self.counts.getOrPut(allocator, @intCast(token_id));
        if (!entry.found_existing) entry.value_ptr.* = 0;
        entry.value_ptr.* += 1;
    }

    fn isEmpty(self: *const SamplingPenaltyState) bool {
        return self.counts.count() == 0;
    }
};

pub fn sampleTokenFromLogits(
    allocator: std.mem.Allocator,
    logits: []const f32,
    config: SamplingConfig,
    token_history: []const i64,
) usize {
    var penalty_state = SamplingPenaltyState{};
    defer penalty_state.deinit(allocator);
    penalty_state.seedFromHistory(allocator, token_history) catch {};
    return sample(@constCast(logits), config, &penalty_state, allocator);
}

fn sample(logits: []const f32, config: SamplingConfig, penalty_state: *const SamplingPenaltyState, allocator: std.mem.Allocator) usize {
    const has_penalties = config.repetition_penalty != 1.0 or
        config.frequency_penalty != 0 or
        config.presence_penalty != 0;
    if (config.temperature <= 0 and !has_penalties) {
        return activations.argmax(logits);
    }

    const working = allocator.alloc(f32, logits.len) catch return activations.argmax(logits);
    defer allocator.free(working);
    @memcpy(working, logits);

    if (has_penalties and !penalty_state.isEmpty()) {
        applyRepetitionPenalties(working, penalty_state, config);
    }
    if (config.temperature <= 0) {
        return activations.argmax(working);
    }

    const inv_temp = 1.0 / config.temperature;
    for (working) |*v| v.* *= inv_temp;
    activations.softmax(working, working.len);

    if (config.top_k > 0 and @as(usize, @intCast(config.top_k)) < working.len) {
        activations.topK(working, @intCast(config.top_k), allocator);
    }
    if (config.top_p > 0 and config.top_p < 1.0) {
        activations.topP(working, config.top_p, allocator);
    }
    if (config.min_p > 0 and config.min_p < 1.0) {
        applyMinP(working, config.min_p);
    }
    return activations.sampleFromProbs(working);
}

fn applyRepetitionPenalties(logits: []f32, penalty_state: *const SamplingPenaltyState, config: SamplingConfig) void {
    var it = penalty_state.counts.iterator();
    while (it.next()) |entry| {
        const token_id = entry.key_ptr.*;
        const count = entry.value_ptr.*;
        if (token_id >= logits.len) continue;
        if (config.repetition_penalty != 1.0) {
            const logit = logits[token_id];
            if (logit > 0) {
                logits[token_id] = logit / config.repetition_penalty;
            } else {
                logits[token_id] = logit * config.repetition_penalty;
            }
        }
        if (config.frequency_penalty != 0) {
            logits[token_id] -= config.frequency_penalty * @as(f32, @floatFromInt(count));
        }
        if (config.presence_penalty != 0) {
            logits[token_id] -= config.presence_penalty;
        }
    }
}

fn applyMinP(probs: []f32, min_p: f32) void {
    if (probs.len == 0) return;
    var max_prob: f32 = 0;
    for (probs) |p| max_prob = @max(max_prob, p);
    const threshold = max_prob * min_p;
    for (probs) |*p| {
        if (p.* < threshold) p.* = 0;
    }
}

/// Type-erased whole-model executor.
///
/// Implementations own immutable compiled model/session resources. They create
/// ModelRuntime values for per-request mutable state. Runtime values then
/// execute prefill/decode so cache/KV ownership has one place to live.
pub const ModelExecutor = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        create_runtime: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator) anyerror!ModelRuntime,
        deinit: *const fn (ctx: *anyopaque) void,
    };

    pub fn createRuntime(self: *const ModelExecutor, allocator: std.mem.Allocator) !ModelRuntime {
        return self.vtable.create_runtime(self.ptr, allocator);
    }

    pub fn deinit(self: *ModelExecutor) void {
        self.vtable.deinit(self.ptr);
        self.* = undefined;
    }
};

const MockModel = struct {
    runtime_resets: usize = 0,
    runtime_prepares: usize = 0,
    last_prepare_hint: usize = 0,
    runtime_deinits: usize = 0,
    executor_deinits: usize = 0,

    const runtime_vtable = ModelRuntime.VTable{
        .prepare = prepare,
        .prefill = prefill,
        .decode = decode,
        .decode_sample = decodeSample,
        .decode_greedy = decodeGreedy,
        .deinit = runtimeDeinit,
        .reset = runtimeReset,
    };

    const executor_vtable = ModelExecutor.VTable{
        .create_runtime = createRuntime,
        .deinit = executorDeinit,
    };

    fn runtimeDeinit(ctx: *anyopaque) void {
        const self: *MockModel = @ptrCast(@alignCast(ctx));
        self.runtime_deinits += 1;
    }

    fn runtimeReset(ctx: *anyopaque) !void {
        const self: *MockModel = @ptrCast(@alignCast(ctx));
        self.runtime_resets += 1;
    }

    fn prepare(
        ctx: *anyopaque,
        _: std.mem.Allocator,
        request: PrepareRequest,
    ) !bool {
        const self: *MockModel = @ptrCast(@alignCast(ctx));
        self.runtime_prepares += 1;
        self.last_prepare_hint = request.kv_tokens_hint;
        return true;
    }

    fn createRuntime(ctx: *anyopaque, _: std.mem.Allocator) !ModelRuntime {
        return .{ .ptr = ctx, .vtable = &runtime_vtable };
    }

    fn prefill(
        _: *anyopaque,
        allocator: std.mem.Allocator,
        request: PrefillRequest,
    ) !ModelOutput {
        const logits = try allocator.alloc(f32, 2);
        logits[0] = @floatFromInt(request.input_ids.len);
        logits[1] = @floatFromInt(request.seq_len + request.query_seq_len);
        return .{ .logits = logits };
    }

    fn decode(
        _: *anyopaque,
        allocator: std.mem.Allocator,
        request: DecodeRequest,
    ) !ModelOutput {
        const logits = try allocator.alloc(f32, 1);
        logits[0] = @floatFromInt(request.token_id + @as(i64, @intCast(request.position)));
        return .{ .logits = logits };
    }

    fn decodeGreedy(
        _: *anyopaque,
        _: std.mem.Allocator,
        request: DecodeRequest,
    ) !GreedyDecodeOutput {
        return .{ .token_id = request.token_id + @as(i64, @intCast(request.position)) };
    }

    fn decodeSample(
        _: *anyopaque,
        allocator: std.mem.Allocator,
        request: SampledDecodeRequest,
    ) !SampledDecodeOutput {
        const logits = [_]f32{
            @floatFromInt(request.decode.token_id),
            @floatFromInt(request.decode.position + request.token_history.len),
        };
        return .{ .token_id = @intCast(sampleTokenFromLogits(allocator, logits[0..], request.sampling, request.token_history)) };
    }

    fn executorDeinit(ctx: *anyopaque) void {
        const self: *MockModel = @ptrCast(@alignCast(ctx));
        self.executor_deinits += 1;
    }
};

test "ModelExecutor and ModelRuntime dispatch through vtables" {
    const allocator = std.testing.allocator;

    var mock = MockModel{};
    var executor = ModelExecutor{ .ptr = &mock, .vtable = &MockModel.executor_vtable };
    var runtime = try executor.createRuntime(allocator);
    const caps = runtime.capabilities();
    try std.testing.expect(caps.supports_decode);
    try std.testing.expect(caps.supports_sample_decode);
    try std.testing.expect(caps.supports_greedy_decode);
    try std.testing.expectEqual(RuntimeStateOwnership.host_assisted_inputs, caps.state_ownership);

    try runtime.reset();
    try std.testing.expectEqual(@as(usize, 1), mock.runtime_resets);

    try std.testing.expect(try runtime.prepare(allocator, .{ .kv_tokens_hint = 17 }));
    try std.testing.expectEqual(@as(usize, 1), mock.runtime_prepares);
    try std.testing.expectEqual(@as(usize, 17), mock.last_prepare_hint);

    var output = try runtime.prefill(allocator, .{
        .input_ids = &.{ 1, 2, 3 },
        .seq_len = 3,
        .query_seq_len = 3,
        .attention_mode = .paged_prefill,
    });
    defer output.deinit(allocator);
    try std.testing.expectEqualSlices(f32, &.{ 3, 6 }, try output.hostLogits(allocator));

    var decoded = try runtime.decode(allocator, .{
        .token_id = 4,
        .position = 5,
    });
    defer decoded.deinit(allocator);
    try std.testing.expectEqualSlices(f32, &.{9}, try decoded.hostLogits(allocator));
    try std.testing.expectEqual(@as(i64, 0), try decoded.greedyToken(allocator, 1));

    const greedy = try runtime.decodeGreedy(allocator, .{
        .token_id = 4,
        .position = 5,
    });
    try std.testing.expectEqual(@as(i64, 9), greedy.token_id);

    const sampled = try runtime.decodeSample(allocator, .{
        .decode = .{
            .token_id = 4,
            .position = 5,
        },
        .sampling = .{},
        .token_history = &.{ 1, 2, 3 },
    });
    try std.testing.expectEqual(@as(i64, 1), sampled.token_id);

    runtime.deinit();
    executor.deinit();
    try std.testing.expectEqual(@as(usize, 1), mock.runtime_deinits);
    try std.testing.expectEqual(@as(usize, 1), mock.executor_deinits);
}

test "ModelExecutor reports unsupported decode when backend has no decode path" {
    const allocator = std.testing.allocator;

    const NoDecode = struct {
        const runtime_vtable = ModelRuntime.VTable{
            .prefill = MockModel.prefill,
            .deinit = MockModel.runtimeDeinit,
        };

        const executor_vtable = ModelExecutor.VTable{
            .create_runtime = createRuntime,
            .deinit = MockModel.executorDeinit,
        };

        fn createRuntime(ctx: *anyopaque, _: std.mem.Allocator) !ModelRuntime {
            return .{ .ptr = ctx, .vtable = &runtime_vtable };
        }
    };

    var mock = MockModel{};
    var executor = ModelExecutor{ .ptr = &mock, .vtable = &NoDecode.executor_vtable };
    defer executor.deinit();
    var runtime = try executor.createRuntime(allocator);
    defer runtime.deinit();
    const caps = runtime.capabilities();
    try std.testing.expect(!caps.supports_decode);
    try std.testing.expect(!caps.supports_sample_decode);
    try std.testing.expect(!caps.supports_greedy_decode);

    try std.testing.expectError(error.UnsupportedDecode, runtime.decode(allocator, .{
        .token_id = 1,
        .position = 0,
    }));
    try std.testing.expectError(error.UnsupportedGreedyDecode, runtime.decodeGreedy(allocator, .{
        .token_id = 1,
        .position = 0,
    }));
    try std.testing.expectError(error.UnsupportedSampleDecode, runtime.decodeSample(allocator, .{
        .decode = .{
            .token_id = 1,
            .position = 0,
        },
        .sampling = .{},
        .token_history = &.{},
    }));
}
