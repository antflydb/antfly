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
const cache_mod = @import("cache.zig");
const activations = @import("../backends/activations.zig");
const InferenceExecutionControl = @import("../execution_control.zig").InferenceExecutionControl;
const Interruption = @import("../execution_control.zig").Interruption;
const UninterruptibleGuard = @import("../execution_control.zig").UninterruptibleGuard;
const ops = @import("../ops/ops.zig");
const contracts = @import("backend_contracts.zig");
const CloseScope = @import("../backends/session.zig").Session.CloseScope;
const CleanupContext = @import("../execution/cleanup_context.zig").Context;

/// Backend-owned model runtime state.
///
/// This is intentionally separate from PartitionExecutor. A PartitionExecutor
/// fills node values inside a host-managed graph execution. A ModelRuntime is
/// the mutable per-session state for a compiled model that owns the whole
/// model execution path, including backend-side cache/KV state as backends grow
/// that capability.
pub const ModelRuntime = struct {
    /// A copied table changing ordinary destruction must explicitly opt in.
    /// Acceptance precedes every provider/context cast; fallback preserves it.
    pub const ColdCleanupServices = struct {
        accepts: *const fn (*const VTable) bool,
        deinit: *const fn (*anyopaque, CleanupContext) void,
        reset: ?*const fn (*anyopaque, CleanupContext) anyerror!void = null,
    };

    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        capabilities: ?*const fn (ctx: *anyopaque) RuntimeCapabilities = null,
        /// Borrow actual numerical owner for provider operation exclusion.
        /// Target-specific schedules/weights/caches retain their original ABI.
        operation_backend: ?*const fn (ctx: *anyopaque) *const ops.ComputeBackend = null,
        accepts_operation_backend: ?*const fn (*const VTable) bool = null,
        /// Side-effect-free decline before any state reset or submission.
        supports_forward_request: ?*const fn (ctx: *anyopaque, request: ForwardRequest, operation: ForwardOperation) bool = null,
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
        /// Returns true only when `decode_sample` can execute this exact
        /// request without falling back to a second model forward. Callers
        /// must check this before submitting a decode frame.
        supports_sample_decode_request: ?*const fn (
            ctx: *anyopaque,
            request: SampledDecodeRequest,
        ) bool = null,
        decode_greedy: ?*const fn (
            ctx: *anyopaque,
            allocator: std.mem.Allocator,
            request: DecodeRequest,
        ) anyerror!GreedyDecodeOutput = null,
        /// Retain resources needed by resident outputs independently of cache
        /// ownership. Host-only outputs do not pin the runtime.
        retain_output_owner: ?*const fn (ctx: *anyopaque) OutputOwner = null,
        deinit: *const fn (ctx: *anyopaque) void,
        cold_cleanup_services: ?*const ColdCleanupServices = null,
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

    pub fn supportsForwardRequest(self: *const ModelRuntime, request: ForwardRequest, operation: ForwardOperation) bool {
        switch (request) {
            .prefill => |prefill_request| {
                if (prefill_request.query_seq_len == 0 or prefill_request.input_ids.len != prefill_request.query_seq_len or prefill_request.query_seq_len > prefill_request.seq_len) return false;
            },
            .decode => {
                const caps = self.capabilities();
                if (operation == .greedy and !caps.supports_greedy_decode) return false;
                if (operation == .logits and !caps.supports_decode) return false;
            },
        }
        if (self.vtable.supports_forward_request) |supports| return supports(self.ptr, request, operation);
        return true;
    }

    fn retainOutputOwner(self: *const ModelRuntime, output: *ModelOutput) void {
        // Both prefill and decode export owned value headers. Their runtime
        // owner/affinity remains unchanged; synchronous request borrows do not.
        if (output.device_logits_backend) |backend| output.device_logits_backend = backend.withoutSynchronousBorrows();
        if (output.prepared_tail) |*tail| tail.backend = tail.backend.withoutSynchronousBorrows();
        if (output.owner == null and (output.device_logits != null or output.prepared_tail != null)) {
            if (self.vtable.retain_output_owner) |retain| output.owner = retain(self.ptr);
        }
    }

    fn operationBackend(self: *const ModelRuntime) ?ops.ComputeBackend {
        const accepts = self.vtable.accepts_operation_backend orelse return null;
        if (!accepts(self.vtable)) return null;
        const getter = self.vtable.operation_backend orelse return null;
        return getter(self.ptr).*;
    }
    pub fn resetWithCleanup(self: *ModelRuntime, current: CleanupContext) !void {
        if (self.vtable.cold_cleanup_services) |services| {
            if (services.accepts(self.vtable)) if (services.reset) |reset_fn| {
                return @import("../execution/backend_operation.zig").callOpaque(self.operationBackend(), current.operation, reset_fn, .{ self.ptr, current });
            };
        }
        return self.resetWithOperation(current.operation);
    }
    pub fn reset(self: *ModelRuntime) !void {
        return self.resetWithOperation(null);
    }
    pub fn resetWithOperation(self: *ModelRuntime, borrowed: ?@import("../execution/backend_operation.zig").Borrowed) !void {
        if (self.vtable.reset) |reset_fn| try @import("../execution/backend_operation.zig").callOpaque(self.operationBackend(), borrowed, reset_fn, .{self.ptr});
    }

    pub fn prepare(
        self: *ModelRuntime,
        allocator: std.mem.Allocator,
        request: PrepareRequest,
    ) !bool {
        try request.check();
        const prepare_fn = self.vtable.prepare orelse return false;
        var hard_cancellation = if (request.execution_control) |control|
            try control.enterUninterruptible(self.capabilities().interruption)
        else
            UninterruptibleGuard{};
        defer hard_cancellation.deinit();
        const prepared = try @import("../execution/backend_operation.zig").callOpaque(self.operationBackend(), request.backend_operation, prepare_fn, .{ self.ptr, allocator, request });
        try request.check();
        return prepared;
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
        try request.check();
        var hard_cancellation = if (request.execution_control) |control|
            try control.enterUninterruptible(self.capabilities().interruption)
        else
            UninterruptibleGuard{};
        defer hard_cancellation.deinit();
        var output = try @import("../execution/backend_operation.zig").callOpaque(self.operationBackend(), request.backend_operation, self.vtable.prefill, .{ self.ptr, allocator, request });
        self.retainOutputOwner(&output);
        errdefer output.deinitWithCleanup(allocator, .{ .boundary = request.cleanup_epoch, .operation = request.backend_operation });
        try request.check();
        return output;
    }

    pub fn decode(
        self: *ModelRuntime,
        allocator: std.mem.Allocator,
        request: DecodeRequest,
    ) !ModelOutput {
        try request.check();
        const decode_fn = self.vtable.decode orelse return error.UnsupportedDecode;
        var hard_cancellation = if (request.execution_control) |control|
            try control.enterUninterruptible(self.capabilities().interruption)
        else
            UninterruptibleGuard{};
        defer hard_cancellation.deinit();
        var output = try @import("../execution/backend_operation.zig").callOpaque(self.operationBackend(), request.backend_operation, decode_fn, .{ self.ptr, allocator, request });
        self.retainOutputOwner(&output);
        errdefer output.deinitWithCleanup(allocator, .{ .boundary = request.cleanup_epoch, .operation = request.backend_operation });
        try request.check();
        return output;
    }

    pub fn decodeGreedy(
        self: *ModelRuntime,
        allocator: std.mem.Allocator,
        request: DecodeRequest,
    ) !GreedyDecodeOutput {
        try request.check();
        const decode_greedy_fn = self.vtable.decode_greedy orelse return error.UnsupportedGreedyDecode;
        var hard_cancellation = if (request.execution_control) |control|
            try control.enterUninterruptible(self.capabilities().interruption)
        else
            UninterruptibleGuard{};
        defer hard_cancellation.deinit();
        var output = try @import("../execution/backend_operation.zig").callOpaque(self.operationBackend(), request.backend_operation, decode_greedy_fn, .{ self.ptr, allocator, request });
        if (output.token_tensor != null and output.owner == null) {
            if (self.vtable.retain_output_owner) |retain| output.owner = retain(self.ptr);
        }
        errdefer output.deinitWithCleanup(.{ .boundary = request.cleanup_epoch, .operation = request.backend_operation });
        try request.check();
        return output;
    }

    pub fn decodeSample(
        self: *ModelRuntime,
        allocator: std.mem.Allocator,
        request: SampledDecodeRequest,
    ) !SampledDecodeOutput {
        try request.decode.check();
        const decode_sample_fn = self.vtable.decode_sample orelse return error.UnsupportedSampleDecode;
        var hard_cancellation = if (request.decode.execution_control) |control|
            try control.enterUninterruptible(self.capabilities().interruption)
        else
            UninterruptibleGuard{};
        defer hard_cancellation.deinit();
        const output = try @import("../execution/backend_operation.zig").callOpaque(self.operationBackend(), request.decode.backend_operation, decode_sample_fn, .{ self.ptr, allocator, request });
        try request.decode.check();
        return output;
    }

    pub fn supportsSampleDecodeRequest(
        self: *const ModelRuntime,
        request: SampledDecodeRequest,
    ) bool {
        if (self.vtable.decode_sample == null) return false;
        const supports_fn = self.vtable.supports_sample_decode_request orelse return false;
        return supports_fn(self.ptr, request);
    }

    pub fn deinit(self: *ModelRuntime) void {
        self.deinitWithCleanup(.{ .outside_execution = true });
    }
    pub fn deinitWithBoundary(self: *ModelRuntime, boundary: ?*const @import("../execution/retirement_sink.zig").Epoch) void {
        self.deinitWithCleanup(.{ .boundary = boundary });
    }
    pub fn deinitWithCleanup(self: *ModelRuntime, cleanup_context: CleanupContext) void {
        if (self.vtable.cold_cleanup_services) |services| {
            if (services.accepts(self.vtable)) {
                services.deinit(self.ptr, cleanup_context);
                self.* = undefined;
                return;
            }
        }
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

pub const ForwardOperation = enum { logits, greedy };

pub const RuntimeCapabilities = struct {
    supports_decode: bool = false,
    supports_sample_decode: bool = false,
    supports_greedy_decode: bool = false,
    state_ownership: RuntimeStateOwnership = .host_assisted_inputs,
    interruption: Interruption = .cooperative,
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
    decode_greedy_device_token_handoff_attempts: u64 = 0,
    decode_greedy_device_token_handoff_hits: u64 = 0,
    decode_greedy_device_token_handoff_fallbacks: u64 = 0,
    decode_greedy_device_token_seeds: u64 = 0,
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

/// One retained dependency of a resident result. The result releases it after
/// its tensors, or as soon as explicit materialization leaves only host values.
/// The provider supplies the reference; retaining requires no wrapper allocation.
pub const OutputOwner = struct {
    ptr: *anyopaque,
    release: *const fn (*anyopaque) void,
    begin_cleanup: ?*const fn (*anyopaque) CloseScope = null,
    cleanup_services: ?*const CleanupServices = null,
    pub const CleanupServices = struct {
        accepts: *const fn (*const OutputOwner) bool,
        release: *const fn (*anyopaque, CleanupContext) void,
    };

    /// Explicit owned capability; no epoch is retained for a later release.
    pub fn releaseWithBoundary(self: OutputOwner, boundary: ?*const @import("../execution/retirement_sink.zig").Epoch) void {
        self.releaseWithCleanup(.{ .boundary = boundary });
    }
    pub fn releaseWithCleanup(self: OutputOwner, cleanup_context: CleanupContext) void {
        if (self.cleanup_services) |services| {
            if (services.accepts(&self)) {
                services.release(self.ptr, cleanup_context);
                return;
            }
        }
        self.release(self.ptr);
    }

    pub fn beginCleanup(self: OutputOwner) CloseScope {
        return if (self.begin_cleanup) |begin| begin(self.ptr) else .{};
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
        /// Diagnostic-only request used by the fail-closed LM-head repack
        /// quality campaign. Production outputs leave this false and retain
        /// checkpoint-format full logits.
        use_transformed_lm_head: bool = false,
        greedy_token_id: ?i64 = null,

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
                    .use_transformed_lm_head = self.use_transformed_lm_head,
                }),
            };
        }

        fn greedyToken(self: *PreparedTail) !?i64 {
            if (self.greedy_token_id) |token_id| return token_id;
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

        pub fn deinit(self: *PreparedTail) void {
            self.deinitWithCleanup(.{ .outside_execution = true });
        }
        pub fn deinitWithBoundary(self: *PreparedTail, boundary: ?*const @import("../execution/retirement_sink.zig").Epoch) void {
            self.deinitWithCleanup(.{ .boundary = boundary });
        }
        pub fn deinitWithCleanup(self: *PreparedTail, cleanup_context: CleanupContext) void {
            self.backend.freeWithCleanup(self.final_hidden, cleanup_context);
            self.* = undefined;
        }
    };

    /// Last-step logits owned by the caller.
    logits: ?[]f32 = null,
    device_logits: ?contracts.CT = null,
    device_logits_backend: ?ops.ComputeBackend = null,
    prepared_tail: ?PreparedTail = null,
    final_logit_softcap: f32 = 0.0,
    owner: ?OutputOwner = null,

    fn releaseOwnerWithCleanup(self: *ModelOutput, cleanup_context: CleanupContext) void {
        if (self.owner) |owner| {
            self.owner = null;
            owner.releaseWithCleanup(cleanup_context);
        }
    }

    pub fn hostLogits(self: *ModelOutput, allocator: std.mem.Allocator) ![]const f32 {
        return self.hostLogitsWithCleanup(allocator, .{ .outside_execution = true });
    }
    pub fn hostLogitsWithBoundary(self: *ModelOutput, allocator: std.mem.Allocator, boundary: ?*const @import("../execution/retirement_sink.zig").Epoch) ![]const f32 {
        return self.hostLogitsWithCleanup(allocator, .{ .boundary = boundary });
    }
    pub fn hostLogitsWithCleanup(self: *ModelOutput, allocator: std.mem.Allocator, cleanup_context: CleanupContext) ![]const f32 {
        if (self.logits) |logits| return logits;
        if (self.prepared_tail) |*tail| {
            const device_logits = (try tail.logits()) orelse return error.InvalidModelOutput;
            const backend = tail.backend;
            const logits = materialized: {
                defer {
                    var cleanup = if (self.owner) |owner| owner.beginCleanup() else CloseScope{};
                    defer cleanup.deinit();
                    backend.freeWithCleanup(device_logits, cleanup_context);
                }
                break :materialized try backend.toFloat32(device_logits, allocator);
            };
            applyFinalLogitSoftcapInPlace(logits, tail.final_logit_softcap);
            var cleanup = if (self.owner) |owner| owner.beginCleanup() else CloseScope{};
            defer cleanup.deinit();
            tail.deinitWithCleanup(cleanup_context);
            self.prepared_tail = null;
            self.logits = logits;
            cleanup.deinit();
            self.releaseOwnerWithCleanup(cleanup_context);
            return logits;
        }
        if (self.device_logits) |device_logits| {
            const backend = self.device_logits_backend orelse return error.InvalidModelOutput;
            const logits = try backend.toFloat32(device_logits, allocator);
            var cleanup = if (self.owner) |owner| owner.beginCleanup() else CloseScope{};
            defer cleanup.deinit();
            backend.freeWithCleanup(device_logits, cleanup_context);
            self.device_logits = null;
            self.device_logits_backend = null;
            applyFinalLogitSoftcapInPlace(logits, self.final_logit_softcap);
            self.logits = logits;
            cleanup.deinit();
            self.releaseOwnerWithCleanup(cleanup_context);
            return logits;
        }
        return error.InvalidModelOutput;
    }

    pub fn takeHostLogits(self: *ModelOutput, allocator: std.mem.Allocator) ![]f32 {
        return self.takeHostLogitsWithCleanup(allocator, .{ .outside_execution = true });
    }
    pub fn takeHostLogitsWithBoundary(self: *ModelOutput, allocator: std.mem.Allocator, boundary: ?*const @import("../execution/retirement_sink.zig").Epoch) ![]f32 {
        return self.takeHostLogitsWithCleanup(allocator, .{ .boundary = boundary });
    }
    pub fn takeHostLogitsWithCleanup(self: *ModelOutput, allocator: std.mem.Allocator, cleanup_context: CleanupContext) ![]f32 {
        _ = try self.hostLogitsWithCleanup(allocator, cleanup_context);
        const logits = self.logits orelse return error.InvalidModelOutput;
        self.logits = null;
        return logits;
    }

    pub fn greedyToken(self: *ModelOutput, allocator: std.mem.Allocator, vocab_size: usize) !i64 {
        return self.greedyTokenWithCleanup(allocator, vocab_size, .{ .outside_execution = true });
    }
    pub fn greedyTokenWithBoundary(self: *ModelOutput, allocator: std.mem.Allocator, vocab_size: usize, boundary: ?*const @import("../execution/retirement_sink.zig").Epoch) !i64 {
        return self.greedyTokenWithCleanup(allocator, vocab_size, .{ .boundary = boundary });
    }
    pub fn greedyTokenWithCleanup(self: *ModelOutput, allocator: std.mem.Allocator, vocab_size: usize, cleanup_context: CleanupContext) !i64 {
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

        const logits = try self.hostLogitsWithCleanup(allocator, cleanup_context);
        if (logits.len < vocab_size) return error.InvalidModelOutput;
        var best_idx: usize = 0;
        for (logits[1..vocab_size], 1..) |value, idx| {
            if (value > logits[best_idx]) best_idx = idx;
        }
        return @intCast(best_idx);
    }

    pub fn deinit(self: *ModelOutput, allocator: std.mem.Allocator) void {
        self.deinitWithCleanup(allocator, .{ .outside_execution = true });
    }
    pub fn deinitWithBoundary(self: *ModelOutput, allocator: std.mem.Allocator, boundary: ?*const @import("../execution/retirement_sink.zig").Epoch) void {
        self.deinitWithCleanup(allocator, .{ .boundary = boundary });
    }
    pub fn deinitWithCleanup(self: *ModelOutput, allocator: std.mem.Allocator, cleanup_context: CleanupContext) void {
        var cleanup = if (self.owner) |owner| owner.beginCleanup() else CloseScope{};
        defer cleanup.deinit();
        if (self.logits) |logits| allocator.free(logits);
        if (self.device_logits) |device_logits| {
            if (self.device_logits_backend) |backend| backend.freeWithCleanup(device_logits, cleanup_context);
        }
        if (self.prepared_tail) |*tail| tail.deinitWithCleanup(cleanup_context);
        cleanup.deinit();
        self.releaseOwnerWithCleanup(cleanup_context);
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
    cleanup_epoch: ?*const @import("../execution/retirement_sink.zig").Epoch = null,
    backend_operation: ?@import("../execution/backend_operation.zig").Borrowed = null,
    execution_control: ?InferenceExecutionControl = null,
    /// Best-effort prompt/KV length hint. Backends may use this to prebuild
    /// shape-specific resources without mutating logical generation state.
    kv_tokens_hint: usize = 0,

    pub fn check(self: PrepareRequest) !void {
        if (self.execution_control) |control| try control.check();
    }
};

pub const PrefillRequest = struct {
    cleanup_epoch: ?*const @import("../execution/retirement_sink.zig").Epoch = null,
    backend_operation: ?@import("../execution/backend_operation.zig").Borrowed = null,
    execution_control: ?InferenceExecutionControl = null,
    input_ids: []const i64,
    seq_len: usize,
    query_seq_len: usize,
    attention_mode: cache_mod.AttentionMode,
    /// Largest KV suffix this request may write at once. Device runtimes use
    /// this with `allow_swa_ring` to size sliding-window rings before the first
    /// prefill write.
    max_inflight_tokens: usize = 0,
    allow_swa_ring: bool = false,
    force_host_logits: bool = false,
    prefer_greedy_token: bool = false,

    pub fn check(self: PrefillRequest) !void {
        if (self.execution_control) |control| try control.check();
    }
};

pub const DecodeRequest = struct {
    cleanup_epoch: ?*const @import("../execution/retirement_sink.zig").Epoch = null,
    backend_operation: ?@import("../execution/backend_operation.zig").Borrowed = null,
    execution_control: ?InferenceExecutionControl = null,
    token_id: i64,
    input_token_tensor: ?ops.CT = null,
    position: usize,
    attention_mode: cache_mod.AttentionMode = .paged_decode,

    pub fn check(self: DecodeRequest) !void {
        if (self.execution_control) |control| try control.check();
    }
};

pub const SampledDecodeRequest = struct {
    decode: DecodeRequest,
    sampling: SamplingConfig,
    token_history: []const i64,
};

pub const GreedyDecodeOutput = struct {
    token_id: i64,
    /// Optional backend-owned token tensor for the returned token. Callers that
    /// pass it back as DecodeRequest.input_token_tensor own it and must either
    /// transfer it again or call deinit.
    token_tensor: ?ops.CT = null,
    token_tensor_backend: ?*const ops.ComputeBackend = null,
    owner: ?OutputOwner = null,

    pub fn deinit(self: *GreedyDecodeOutput) void {
        self.deinitWithCleanup(.{ .outside_execution = true });
    }
    pub fn deinitWithBoundary(self: *GreedyDecodeOutput, boundary: ?*const @import("../execution/retirement_sink.zig").Epoch) void {
        self.deinitWithCleanup(.{ .boundary = boundary });
    }
    pub fn deinitWithCleanup(self: *GreedyDecodeOutput, cleanup_context: CleanupContext) void {
        var cleanup = if (self.owner) |owner| owner.beginCleanup() else CloseScope{};
        defer cleanup.deinit();
        if (self.token_tensor) |tensor| {
            if (self.token_tensor_backend) |backend| backend.freeWithCleanup(tensor, cleanup_context);
        }
        cleanup.deinit();
        if (self.owner) |owner| owner.releaseWithCleanup(cleanup_context);
        self.* = undefined;
    }
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

    pub fn deinit(self: *SamplingPenaltyState, allocator: std.mem.Allocator) void {
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
    /// A copied table changing ordinary destruction must explicitly opt in.
    /// Acceptance precedes every provider/context cast; fallback preserves it.
    pub const ColdCleanupServices = struct {
        accepts: *const fn (*const VTable) bool,
        deinit: *const fn (*anyopaque, CleanupContext) void,
    };

    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        create_runtime: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator) anyerror!ModelRuntime,
        deinit: *const fn (ctx: *anyopaque) void,
        cold_cleanup_services: ?*const ColdCleanupServices = null,
    };

    pub fn createRuntime(self: *const ModelExecutor, allocator: std.mem.Allocator) !ModelRuntime {
        return self.vtable.create_runtime(self.ptr, allocator);
    }

    pub fn deinit(self: *ModelExecutor) void {
        self.deinitWithCleanup(.{ .outside_execution = true });
    }
    pub fn deinitWithBoundary(self: *ModelExecutor, boundary: ?*const @import("../execution/retirement_sink.zig").Epoch) void {
        self.deinitWithCleanup(.{ .boundary = boundary });
    }
    pub fn deinitWithCleanup(self: *ModelExecutor, cleanup_context: CleanupContext) void {
        if (self.vtable.cold_cleanup_services) |services| {
            if (services.accepts(self.vtable)) {
                services.deinit(self.ptr, cleanup_context);
                self.* = undefined;
                return;
            }
        }
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
    saw_prefill_control: bool = false,

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
        ctx: *anyopaque,
        allocator: std.mem.Allocator,
        request: PrefillRequest,
    ) !ModelOutput {
        const self: *MockModel = @ptrCast(@alignCast(ctx));
        self.saw_prefill_control = request.execution_control != null;
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

test "ModelRuntime propagates control and releases output when the exit check cancels" {
    const CancelAfterCall = struct {
        checks: usize = 0,

        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.checks += 1;
            if (self.checks >= 2) return error.Cancelled;
        }
    };

    var state = CancelAfterCall{};
    var mock = MockModel{};
    var runtime = ModelRuntime{ .ptr = &mock, .vtable = &MockModel.runtime_vtable };
    try std.testing.expectError(error.Cancelled, runtime.prefill(std.testing.allocator, .{
        .execution_control = .{ .ptr = &state, .check_fn = CancelAfterCall.check },
        .input_ids = &.{ 1, 2, 3 },
        .seq_len = 3,
        .query_seq_len = 3,
        .attention_mode = .paged_prefill,
    }));
    try std.testing.expect(mock.saw_prefill_control);
    try std.testing.expectEqual(@as(usize, 2), state.checks);
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

test "resident model outputs survive runtime eviction and release owners after materialization or failure" {
    const a = std.testing.allocator;
    var weights = @import("../ops/native_compute.zig").WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer weights.resident_weights.deinit(a);
    defer weights.lazy_weights.deinit(a);
    var compute = @import("../ops/native_compute.zig").NativeCompute.init(a, &weights, null);
    defer compute.deinit();
    const Probe = struct {
        backend: ops.ComputeBackend,
        references: usize = 1,
        closed: bool = false,
        host_only: bool = false,
        active_cleanup: usize = 0,
        cleanup_begins: usize = 0,
        fn beginCleanup(ctx: *anyopaque) CloseScope {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            std.debug.assert(!self.closed);
            self.active_cleanup += 1;
            self.cleanup_begins += 1;
            return .{ .ptr = ctx, .release_fn = endCleanup };
        }
        fn endCleanup(ctx: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            std.debug.assert(self.active_cleanup != 0);
            self.active_cleanup -= 1;
        }
        fn retain(ctx: *anyopaque) OutputOwner {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.references += 1;
            return .{ .ptr = ctx, .release = release, .begin_cleanup = beginCleanup };
        }
        fn release(ctx: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            std.debug.assert(!self.closed);
            self.references -= 1;
            if (self.references == 0) {
                if (!self.host_only) std.debug.assert(self.active_cleanup != 0);
                self.closed = true;
            }
        }
        fn prefill(ctx: *anyopaque, allocator: std.mem.Allocator, _: PrefillRequest) !ModelOutput {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (self.host_only) return .{ .logits = try allocator.dupe(f32, &.{ 3, 5 }) };
            return .{ .device_logits = try self.backend.fromFloat32Shape(&.{ 3, 5 }, &.{ 1, 2 }), .device_logits_backend = self.backend };
        }
        const vtable = ModelRuntime.VTable{ .prefill = prefill, .deinit = release, .retain_output_owner = retain };
    };
    for ([_]enum { materialize, fail_materialization, discard, host }{ .materialize, .fail_materialization, .discard, .host }) |mode| {
        var probe = Probe{ .backend = compute.computeBackend(), .host_only = mode == .host };
        var runtime_instance = ModelRuntime{ .ptr = &probe, .vtable = &Probe.vtable };
        var output = try runtime_instance.prefill(a, .{ .input_ids = &.{1}, .seq_len = 1, .query_seq_len = 1, .attention_mode = .paged_prefill });
        runtime_instance.deinit();
        var output_live = true;
        defer if (output_live) output.deinit(a);
        try std.testing.expectEqual(mode == .host, probe.closed);
        switch (mode) {
            .materialize, .host => {
                try std.testing.expectEqualSlices(f32, &.{ 3, 5 }, try output.hostLogits(a));
                try std.testing.expect(probe.closed);
                try std.testing.expectEqualSlices(f32, &.{ 3, 5 }, try output.hostLogits(a));
            },
            .fail_materialization => {
                var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
                try std.testing.expectError(error.OutOfMemory, output.hostLogits(failing.allocator()));
                try std.testing.expect(!probe.closed);
                try std.testing.expect(output.owner != null);
                // Failed numerical materialization neither arms cleanup nor
                // retires the original resident tensor/owner.
                try std.testing.expectEqual(@as(usize, 0), probe.cleanup_begins);
                try std.testing.expectEqual(@as(usize, 0), probe.active_cleanup);
            },
            .discard => {},
        }
        output.deinit(a);
        output_live = false;
        try std.testing.expect(probe.closed);
        try std.testing.expectEqual(@as(usize, 0), probe.references);
        try std.testing.expectEqual(@as(usize, 0), probe.active_cleanup);
        try std.testing.expectEqual(@as(usize, if (mode == .host) 0 else 1), probe.cleanup_begins);
    }
}

test "ModelRuntime and ModelExecutor cold cleanup services preserve copied wrapper deinit policy" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const Probe = struct {
        bounded: usize = 0,
        wrapped: usize = 0,
        raw: usize = 0,
        nonce: u64 = 0,
        const runtime_services: ModelRuntime.ColdCleanupServices = .{ .accepts = acceptsRuntime, .deinit = releaseBounded };
        const executor_services: ModelExecutor.ColdCleanupServices = .{ .accepts = acceptsExecutor, .deinit = releaseBounded };
        const runtime_table: ModelRuntime.VTable = table: {
            var value = MockModel.runtime_vtable;
            value.deinit = releaseRaw;
            value.cold_cleanup_services = &runtime_services;
            break :table value;
        };
        const executor_table: ModelExecutor.VTable = table: {
            var value = MockModel.executor_vtable;
            value.deinit = releaseRaw;
            value.cold_cleanup_services = &executor_services;
            break :table value;
        };
        fn acceptsRuntime(table: *const ModelRuntime.VTable) bool {
            return table == &runtime_table;
        }
        fn acceptsExecutor(table: *const ModelExecutor.VTable) bool {
            return table == &executor_table;
        }
        fn releaseBounded(ctx: *anyopaque, cleanup_context: CleanupContext) void {
            const boundary = cleanup_context.boundary;
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.bounded += 1;
            self.nonce = if (boundary) |value| value.nonce else 0;
        }
        fn releaseWrapped(ctx: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.wrapped += 1;
        }
        fn releaseRaw(ctx: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.raw += 1;
        }
    };
    var sink = try @import("../execution/retirement_sink.zig").Ref.create();
    defer sink.release();
    var guard = try sink.enter();
    defer guard.finish();
    var epoch = try guard.retainEpoch();
    defer epoch.release();
    var probe = Probe{};
    var runtime = ModelRuntime{ .ptr = &probe, .vtable = &Probe.runtime_table };
    runtime.deinitWithBoundary(&epoch);
    var executor = ModelExecutor{ .ptr = &probe, .vtable = &Probe.executor_table };
    executor.deinitWithBoundary(&epoch);
    try std.testing.expectEqual(@as(usize, 2), probe.bounded);
    try std.testing.expectEqual(epoch.nonce, probe.nonce);
    var runtime_wrapper = Probe.runtime_table;
    runtime_wrapper.deinit = Probe.releaseWrapped;
    var wrapped_runtime = ModelRuntime{ .ptr = &probe, .vtable = &runtime_wrapper };
    wrapped_runtime.deinitWithBoundary(&epoch);
    var executor_wrapper = Probe.executor_table;
    executor_wrapper.deinit = Probe.releaseWrapped;
    var wrapped_executor = ModelExecutor{ .ptr = &probe, .vtable = &executor_wrapper };
    wrapped_executor.deinitWithBoundary(&epoch);
    try std.testing.expectEqual(@as(usize, 2), probe.wrapped);
    try std.testing.expectEqual(@as(usize, 2), probe.bounded);
    var legacy_table = Probe.runtime_table;
    legacy_table.cold_cleanup_services = null;
    var legacy = ModelRuntime{ .ptr = &probe, .vtable = &legacy_table };
    legacy.deinit();
    try std.testing.expectEqual(@as(usize, 1), probe.raw);
}

test "OutputOwner forwards exact current cleanup epoch with ordinary release fallback" {
    const Probe = struct {
        ordinary: usize = 0,
        bounded: usize = 0,
        nonce: u64 = 0,
        const owner_services: OutputOwner.CleanupServices = .{ .accepts = acceptsOwner, .release = releaseBounded };
        fn acceptsOwner(value: *const OutputOwner) bool {
            return value.release == release and value.begin_cleanup == null;
        }
        fn releaseCustom(ctx: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.ordinary += 10;
        }
        fn customBegin(_: *anyopaque) CloseScope {
            return .{};
        }
        fn release(ctx: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.ordinary += 1;
        }
        fn releaseBounded(ctx: *anyopaque, cleanup_context: CleanupContext) void {
            const boundary = cleanup_context.boundary;
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.bounded += 1;
            self.nonce = if (boundary) |value| value.nonce else 0;
        }
    };
    var sink = try @import("../execution/retirement_sink.zig").Ref.create();
    defer sink.release();
    var guard = try sink.enter();
    defer guard.finish();
    var epoch = try guard.retainEpoch();
    defer epoch.release();
    var probe = Probe{};
    const plain = OutputOwner{ .ptr = &probe, .release = Probe.release };
    plain.releaseWithBoundary(&epoch);
    const managed = OutputOwner{ .ptr = &probe, .release = Probe.release, .cleanup_services = &Probe.owner_services };
    managed.releaseWithBoundary(&epoch);
    try std.testing.expectEqual(@as(usize, 1), probe.ordinary);
    try std.testing.expectEqual(@as(usize, 1), probe.bounded);
    try std.testing.expectEqual(epoch.nonce, probe.nonce);
    var custom_release = managed;
    custom_release.release = Probe.releaseCustom;
    custom_release.releaseWithCleanup(.{ .boundary = &epoch });
    try std.testing.expectEqual(@as(usize, 11), probe.ordinary);
    try std.testing.expectEqual(@as(usize, 1), probe.bounded);
    var custom_scope = managed;
    custom_scope.begin_cleanup = Probe.customBegin;
    custom_scope.releaseWithCleanup(.{ .boundary = &epoch });
    try std.testing.expectEqual(@as(usize, 12), probe.ordinary);
    try std.testing.expectEqual(@as(usize, 1), probe.bounded);
}

fn modelOutputBoundaryCase(allocator: std.mem.Allocator) !void {
    const Probe = struct {
        scopes: usize = 0,
        frees: usize = 0,
        releases: usize = 0,
        free_nonce: u64 = 0,
        owner_nonce: u64 = 0,
        owner_inside_scope: bool = false,
        free_outside_scope: bool = false,
        const services: ops.ColdCleanupServices = .{ .accepts = accepts, .free = freeBounded };
        const owner_services: OutputOwner.CleanupServices = .{ .accepts = acceptsOwner, .release = releaseBounded };
        fn acceptsOwner(value: *const OutputOwner) bool {
            return value.release == releaseRaw and value.begin_cleanup == begin;
        }

        const table: ops.ComputeBackend.VTable = value: {
            var result = @import("../ops/native_compute.zig").vtable_impl;
            result.freeTensor = freeRaw;
            result.cold_cleanup_services = &services;
            result.toFloat32 = materialize;
            break :value result;
        };
        fn accepts(value: *const ops.ComputeBackend.VTable) bool {
            return value == &table;
        }
        fn materialize(_: *anyopaque, _: ops.CT, a: std.mem.Allocator) ![]f32 {
            return a.dupe(f32, &.{ 1, 3 });
        }
        fn freeRaw(ctx: *anyopaque, tensor: ops.CT) void {
            freeBounded(ctx, tensor, .{});
        }
        fn freeBounded(ctx: *anyopaque, _: ops.CT, cleanup_context: CleanupContext) void {
            const epoch = cleanup_context.boundary;
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.frees += 1;
            self.free_outside_scope = self.free_outside_scope or self.scopes == 0;
            self.free_nonce = if (epoch) |value| value.nonce else 0;
        }
        fn begin(ctx: *anyopaque) CloseScope {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.scopes += 1;
            return .{ .ptr = self, .release_fn = end };
        }
        fn end(ctx: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.scopes -= 1;
        }
        fn releaseRaw(ctx: *anyopaque) void {
            releaseBounded(ctx, .{});
        }
        fn releaseBounded(ctx: *anyopaque, cleanup_context: CleanupContext) void {
            const epoch = cleanup_context.boundary;
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.releases += 1;
            self.owner_inside_scope = self.owner_inside_scope or self.scopes != 0;
            self.owner_nonce = if (epoch) |value| value.nonce else 0;
        }
        fn owner(self: *@This()) OutputOwner {
            return .{ .ptr = self, .release = releaseRaw, .cleanup_services = &owner_services, .begin_cleanup = begin };
        }
    };
    var sink = try @import("../execution/retirement_sink.zig").Ref.create();
    defer sink.release();
    var old = try sink.enter();
    old.finish();
    var current = try sink.enter();
    defer current.finish();
    var epoch = try current.retainEpoch();
    defer epoch.release();
    var probe = Probe{};
    const backend = ops.ComputeBackend{ .ptr = &probe, .vtable = &Probe.table };
    const token: ops.CT = @ptrFromInt(4);
    {
        var output = ModelOutput{ .device_logits = token, .device_logits_backend = backend, .owner = probe.owner() };
        defer output.deinitWithBoundary(allocator, &epoch);
        const values = try output.hostLogitsWithBoundary(allocator, &epoch);
        try std.testing.expectEqualSlices(f32, &.{ 1, 3 }, values);
        try std.testing.expect(output.owner == null and output.device_logits == null);
    }
    {
        var output = ModelOutput{ .prepared_tail = .{ .final_hidden = token, .backend = backend, .norm = .layer, .norm_slot = 0, .lm_head_slot = 0, .hidden_size = 2, .vocab_size = 2, .eps = 0.00001 }, .owner = probe.owner() };
        output.deinitWithBoundary(allocator, &epoch);
    }
    {
        var output = GreedyDecodeOutput{ .token_id = 3, .token_tensor = token, .token_tensor_backend = &backend, .owner = probe.owner() };
        output.deinitWithBoundary(&epoch);
    }
    try std.testing.expectEqual(@as(usize, 3), probe.frees);
    try std.testing.expectEqual(@as(usize, 3), probe.releases);
    try std.testing.expectEqual(epoch.nonce, probe.free_nonce);
    try std.testing.expectEqual(epoch.nonce, probe.owner_nonce);
    try std.testing.expectEqual(@as(usize, 0), probe.scopes);
    try std.testing.expect(!probe.owner_inside_scope and !probe.free_outside_scope);
}

test "model retained output cleanup and materialization forward current epoch after SDK protection ends" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    try modelOutputBoundaryCase(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, modelOutputBoundaryCase, .{});
}

test "Model runtime exported logits sanitize request header before parent stack dies" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const a = std.testing.allocator;
    var endpoint = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
    defer endpoint.deinit(a);
    const canonical = endpoint.computeBackend();
    const Parent = struct {
        backend: ops.ComputeBackend,
        var callbacks: usize = 0;
        fn bind(_: *anyopaque, _: ?*@import("../runtime/tier/memory.zig").RunBudget) void {
            callbacks += 1;
        }
        fn prefill(raw: *anyopaque, _: std.mem.Allocator, _: PrefillRequest) !ModelOutput {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return .{ .device_logits = try self.backend.fromFloat32Shape(&.{ 3, 5 }, &.{ 1, 2 }), .device_logits_backend = self.backend };
        }
        fn close(_: *anyopaque) void {}
        fn execute(allocator: std.mem.Allocator, backend: ops.ComputeBackend) !ModelOutput {
            var budget = @import("../runtime/tier/memory.zig").RunBudget.init(.{});
            const context = @import("../execution/invocation_context.zig").Context{ .cleanup = .{} };
            var parent = @This(){ .backend = backend };
            parent.backend.call_context = &context;
            parent.backend.execution_budget = &budget;
            parent.backend.execution_budget_bind = bind;
            parent.backend.execution_owner = 42;
            var model = ModelRuntime{ .ptr = &parent, .vtable = &.{ .prefill = prefill, .deinit = close } };
            return model.prefill(allocator, .{ .input_ids = &.{1}, .seq_len = 1, .query_seq_len = 1, .attention_mode = .paged_prefill });
        }
    };
    Parent.callbacks = 0;
    var output = try Parent.execute(a, canonical);
    var owner = try @import("../execution/cleanup_owner.zig").Owner.beginOutsideCleanup();
    defer owner.finishAfterUnlock();
    const current = CleanupContext{ .boundary = owner.epoch() };
    defer output.deinitWithCleanup(a, current);
    const retained = output.device_logits_backend.?;
    try std.testing.expect(retained.ptr == canonical.ptr and retained.vtable == canonical.vtable);
    try std.testing.expect(retained.call_context == null and retained.execution_budget == null and retained.execution_budget_bind == null and retained.execution_owner == 0);
    retained.bindExecutionBudget();
    retained.clearExecutionBudget();
    const values = try output.hostLogitsWithCleanup(a, current);
    try std.testing.expectEqualSlices(f32, &.{ 3, 5 }, values);
    try std.testing.expectEqual(@as(usize, 0), Parent.callbacks);
}
