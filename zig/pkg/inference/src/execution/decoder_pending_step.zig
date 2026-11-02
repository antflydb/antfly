// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Physical pending decode submission and readback ownership. Task code owns
//! sampling, token publication, EOS, grammar, streaming and scheduler progress.
const std = @import("std");
const ops = @import("../ops/ops.zig");
const state_mod = @import("../graph/decode_state_runtime.zig");
const gated = @import("../backends/decoder_gated_runtime.zig");
const model_mod = @import("../models/gpt.zig");
const control_mod = @import("../execution_control.zig");

pub const Context = struct {
    allocator: std.mem.Allocator,
    backend: *const ops.ComputeBackend,
    model: *const model_mod.Config,
    state: *state_mod.NativeDecodeState,
    control: ?control_mod.InferenceExecutionControl = null,
    /// Actual model lane capability; callers must not substitute a raw mutex.
    execution: control_mod.ExecutionAccess = .{},
    prefetch_drain_budget: usize = 4,

    fn runtime(self: Context) state_mod.BorrowedDecodeStateRuntime {
        return state_mod.BorrowedDecodeStateRuntime.init(self.state);
    }
};

pub const Readback = struct {
    context: Context,
    pending: ?ops.PendingI32ScalarDownload,
    reserved_position: bool = false,
    lookahead: ?ops.CT = null,

    pub fn begin(context: Context, candidate: ops.CT) !?Readback {
        const pending = (try context.backend.beginI32ScalarDownload(candidate)) orelse return null;
        return .{ .context = context, .pending = pending };
    }

    /// Match the existing placeholder reservation ordering. An errored append
    /// is cleaned up by the same bounded truncate used by the legacy route.
    pub fn reservePosition(self: *Readback) !usize {
        if (self.reserved_position) return error.InvalidPendingDecodeStep;
        self.reserved_position = true;
        var runtime = self.context.runtime();
        return runtime.appendGeneratedToken();
    }

    pub fn retainLookahead(self: *Readback, tensor: ops.CT) !void {
        if (self.lookahead != null) return error.InvalidPendingDecodeStep;
        self.lookahead = tensor;
    }

    pub fn resolve(self: *Readback) !usize {
        const pending = self.pending orelse return error.InvalidPendingDecodeStep;
        const token = try self.context.backend.finishI32ScalarDownload(pending);
        self.pending = null;
        if (token < 0) return error.InvalidTensorShape;
        return @intCast(token);
    }

    pub fn commit(self: *Readback) !void {
        if (self.pending != null or !self.reserved_position) return error.InvalidPendingDecodeStep;
        self.reserved_position = false;
    }

    pub fn takeLookahead(self: *Readback) ?ops.CT {
        const tensor = self.lookahead;
        self.lookahead = null;
        return tensor;
    }

    pub fn discardLookahead(self: *Readback) void {
        const tensor = self.lookahead orelse return;
        self.lookahead = null;
        // A speculative write must finish before its KV rows can be truncated.
        self.context.backend.evalTensor(tensor) catch {};
        self.context.backend.free(tensor);
    }

    pub fn rollback(self: *Readback) !void {
        self.discardLookahead();
        if (!self.reserved_position) return;
        self.reserved_position = false;
        var runtime = self.context.runtime();
        try runtime.truncateGeneratedTokens(1);
    }

    pub fn deinit(self: *Readback) void {
        // Preserve the legacy error order: drop speculative output, truncate
        // its unpublished position, then cancel the outstanding readback.
        // Explicit EOS/stream-stop discard uses discardLookahead's fence.
        if (self.lookahead) |tensor| self.context.backend.free(tensor);
        self.lookahead = null;
        if (self.reserved_position) {
            self.reserved_position = false;
            var runtime = self.context.runtime();
            runtime.truncateGeneratedTokens(1) catch {};
        }
        if (self.pending) |pending| self.context.backend.cancelI32ScalarDownload(pending);
        self.pending = null;
    }
};

/// Driver-owned pipelined frame. KV advancement is explicit because task
/// acceptance can differ from numerical lookahead; no backend token is replayed.
pub const GreedyFrame = struct {
    context: Context,
    in_flight: bool = false,
    encoded: bool = false,
    reserved_position: bool = false,
    execution_owned: bool = false,

    pub fn init(context: Context) !GreedyFrame {
        try context.execution.acquire();
        return .{ .context = context, .execution_owned = true };
    }

    pub fn deinit(self: *GreedyFrame) void {
        if (!self.execution_owned) return;
        self.execution_owned = false;
        self.context.execution.release();
    }

    pub fn drainPrefetch(self: *GreedyFrame) void {
        self.context.backend.drainPrefetchBudget(self.context.prefetch_drain_budget);
    }

    pub fn arm(self: *GreedyFrame, token: i64, sequence_length: usize) !bool {
        if (!self.execution_owned or self.in_flight or self.encoded) return error.InvalidPendingDecodeStep;
        var runtime = self.context.runtime();
        const decode = runtime.makeDecodeContext(sequence_length, 1);
        const armed = try gated.forwardGreedyTokenPipelinedArm(self.context.backend, self.context.allocator, self.context.model.*, self.context.model.num_hidden_layers, token, sequence_length, &decode);
        self.in_flight = armed;
        return armed;
    }

    pub fn reserveLookahead(self: *GreedyFrame) !usize {
        if (self.reserved_position) return error.InvalidPendingDecodeStep;
        self.reserved_position = true;
        var runtime = self.context.runtime();
        return runtime.appendGeneratedToken();
    }

    pub fn commitPosition(self: *GreedyFrame) !void {
        if (!self.reserved_position) return error.InvalidPendingDecodeStep;
        self.reserved_position = false;
    }

    pub fn step(self: *GreedyFrame, sequence_length: usize) !?i64 {
        if (!self.in_flight or self.encoded) return error.InvalidPendingDecodeStep;
        self.context.backend.drainPrefetchBudget(self.context.prefetch_drain_budget);
        var runtime = self.context.runtime();
        const decode = runtime.makeDecodeContext(sequence_length, 1);
        const token = try gated.forwardGreedyTokenPipelinedStep(self.context.backend, self.context.allocator, self.context.model.*, self.context.model.num_hidden_layers, sequence_length, &decode);
        if (token != null) {
            self.in_flight = false;
            self.encoded = true;
        }
        return token;
    }

    pub fn awaitToken(self: *GreedyFrame) !?i64 {
        const token = try gated.decoderRuntimePipelinedControl(self.context.backend, .await_only);
        self.in_flight = false;
        return token;
    }

    pub fn submit(self: *GreedyFrame) !bool {
        if (!self.encoded) return error.InvalidPendingDecodeStep;
        const submitted = (try gated.decoderRuntimePipelinedControl(self.context.backend, .submit_pending)) != null;
        if (submitted) {
            self.encoded = false;
            self.in_flight = true;
        }
        return submitted;
    }

    pub fn cancelPending(self: *GreedyFrame) void {
        _ = gated.decoderRuntimePipelinedControl(self.context.backend, .cancel_pending) catch {};
        self.encoded = false;
    }

    pub fn abort(self: *GreedyFrame) void {
        self.cancelPending();
        _ = gated.decoderRuntimePipelinedControl(self.context.backend, .await_only) catch {};
        self.in_flight = false;
        // The legacy pipelined route abandons the entire request on error;
        // retain its metadata arithmetic rather than inventing a replay.
        self.reserved_position = false;
    }
};

test "pending readback owns download lookahead and real KV reservation across acceptance rollback and errors" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const Event = enum { begin, finish, evaluate, free, cancel };
    const Probe = struct {
        state: *state_mod.NativeDecodeState,
        trace: [16]Event = undefined,
        count: usize = 0,
        fail_finish: bool = false,
        unsupported: bool = false,
        fn note(self: *@This(), event: Event) void {
            self.trace[self.count] = event;
            self.count += 1;
        }
        fn begin(raw: *anyopaque, _: ops.CT) !?ops.PendingI32ScalarDownload {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.state.total_tokens != 4) return error.InvalidTestReservationOrder;
            self.note(.begin);
            return if (self.unsupported) null else .{ .handle = 11 };
        }
        fn finish(raw: *anyopaque, pending: ops.PendingI32ScalarDownload) !i32 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (pending.handle != 11 or self.state.total_tokens != 5) return error.InvalidTestReservationOrder;
            self.note(.finish);
            if (self.fail_finish) return error.InjectedReadbackFailure;
            return 7;
        }
        fn cancel(raw: *anyopaque, pending: ops.PendingI32ScalarDownload) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(pending.handle == 11 and self.state.total_tokens == 4);
            self.note(.cancel);
        }
        fn evaluate(raw: *anyopaque, tensor: ops.CT) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (@intFromPtr(tensor) != 16 or self.state.total_tokens != 5) return error.InvalidTestReservationOrder;
            self.note(.evaluate);
        }
        fn free(raw: *anyopaque, tensor: ops.CT) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(@intFromPtr(tensor) == 16 and self.state.total_tokens == 5);
            self.note(.free);
        }
    };
    for (0..4) |mode| {
        var state = state_mod.NativeDecodeState.initContiguous(std.testing.allocator);
        defer state.deinit();
        try state.notePrefill(4);
        var probe = Probe{ .state = &state, .fail_finish = mode == 2 };
        var table = @import("../ops/native_compute.zig").vtable_impl;
        table.beginI32ScalarDownload = Probe.begin;
        table.finishI32ScalarDownload = Probe.finish;
        table.cancelI32ScalarDownload = Probe.cancel;
        table.evalTensor = Probe.evaluate;
        table.freeTensor = Probe.free;
        const backend = ops.ComputeBackend{ .ptr = &probe, .vtable = &table };
        const config = model_mod.Config{};
        var step = (try Readback.begin(.{ .allocator = std.testing.allocator, .backend = &backend, .model = &config, .state = &state }, @ptrFromInt(8))).?;
        defer step.deinit();
        try std.testing.expectEqual(@as(usize, 5), try step.reservePosition());
        try step.retainLookahead(@ptrFromInt(16));
        if (mode == 0) {
            try std.testing.expectEqual(@as(usize, 7), try step.resolve());
            try step.commit();
            const lookahead = step.takeLookahead().?;
            backend.free(lookahead);
            step.deinit();
            try std.testing.expectEqual(@as(usize, 5), state.total_tokens);
            try std.testing.expectEqualSlices(Event, &.{ .begin, .finish, .free }, probe.trace[0..probe.count]);
        } else if (mode == 1) {
            _ = try step.resolve();
            try step.rollback();
            step.deinit();
            try std.testing.expectEqual(@as(usize, 4), state.total_tokens);
            try std.testing.expectEqualSlices(Event, &.{ .begin, .finish, .evaluate, .free }, probe.trace[0..probe.count]);
        } else {
            if (mode == 2) try std.testing.expectError(error.InjectedReadbackFailure, step.resolve());
            step.deinit();
            try std.testing.expectEqual(@as(usize, 4), state.total_tokens);
            const expected: []const Event = if (mode == 2) &.{ .begin, .finish, .free, .cancel } else &.{ .begin, .free, .cancel };
            try std.testing.expectEqualSlices(Event, expected, probe.trace[0..probe.count]);
        }
    }
}

test "pending greedy frame retains exact encoded and submitted phases without replay" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const contracts = @import("../graph/backend_contracts.zig");
    const Probe = struct {
        phases: [8]contracts.DecoderRuntimeDecodePhase = undefined,
        count: usize = 0,
        drains: usize = 0,
        fn kind(_: *anyopaque) ops.BackendKind {
            return .metal;
        }
        fn weight(_: *anyopaque, _: []const u8) !ops.CT {
            return @ptrFromInt(8);
        }
        fn free(_: *anyopaque, _: ops.CT) void {}
        fn drain(raw: *anyopaque, count: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(count == 4);
            self.drains += 1;
        }
        fn decode(raw: *anyopaque, request: *const ops.DecoderRuntimeDecodeRequest) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.phases[self.count] = request.phase;
            self.count += 1;
            request.output_token_ids[0] = 7;
            return true;
        }
    };
    var probe = Probe{};
    var table = @import("../ops/native_compute.zig").vtable_impl;
    table.backendKind = Probe.kind;
    table.getWeight = Probe.weight;
    table.getEmbeddingWeight = Probe.weight;
    table.freeTensor = Probe.free;
    table.drainPrefetchBudget = Probe.drain;
    table.regions.decoder.decode = Probe.decode;
    const backend = ops.ComputeBackend{ .ptr = &probe, .vtable = &table };
    const config = model_mod.Config{ .family = .gemma, .hidden_size = 4, .num_hidden_layers = 0, .num_attention_heads = 1, .intermediate_size = 8, .vocab_size = 16, .ple_hidden_size = 1 };
    var state = state_mod.NativeDecodeState.initContiguous(std.testing.allocator);
    defer state.deinit();
    try state.notePrefill(2);
    state.kv_view = .{ .sequence_id = 0, .pool_id = 0, .logical_block_count = 1, .tail_tokens = 2, .token_count = 2, .position_offset = 0 };
    var frame = try GreedyFrame.init(.{ .allocator = std.testing.allocator, .backend = &backend, .model = &config, .state = &state });
    defer frame.deinit();
    frame.drainPrefetch();
    try std.testing.expect(try frame.arm(2, 2));
    try std.testing.expect(frame.in_flight and !frame.encoded);
    _ = try frame.reserveLookahead();
    try std.testing.expectEqual(@as(?i64, 7), try frame.step(3));
    try std.testing.expect(!frame.in_flight and frame.encoded);
    try std.testing.expect(try frame.submit());
    try frame.commitPosition();
    try std.testing.expect(frame.in_flight and !frame.encoded and !frame.reserved_position);
    try std.testing.expectEqual(@as(?i64, 7), try frame.awaitToken());
    try std.testing.expect(!frame.in_flight);
    try std.testing.expectEqual(@as(usize, 3), state.total_tokens);
    try std.testing.expectEqual(@as(usize, 2), probe.drains);
    try std.testing.expectEqualSlices(contracts.DecoderRuntimeDecodePhase, &.{ .submit_only, .step, .submit_pending, .await_only }, probe.phases[0..probe.count]);
}

/// The existing native decoder is already gated and owns its KV; this owner
/// keeps its submitted and encoded stages bounded to one task invocation.
/// Explicit .encoded is the only successful submission proof.
pub fn PipelinedStage(comptime Decoder: type) type {
    return struct {
        const Self = @This();
        decoder: *Decoder,
        enabled: bool = false,
        slot: usize = 0,
        encoded: bool = false,
        submitted: bool = false,

        pub fn begin(decoder: *Decoder, grammar: *const ops.WhisperGrammarState) ?Self {
            if (!decoder.setPipelined(true)) return null;
            if (!decoder.seedGrammar(grammar)) {
                decoder.drain();
                _ = decoder.setPipelined(false);
                return null;
            }
            return .{ .decoder = decoder, .enabled = true };
        }

        pub fn deinit(self: *Self) void {
            if (!self.enabled) return;
            self.decoder.drain();
            _ = self.decoder.setPipelined(false);
            self.* = undefined;
        }

        pub fn encode(self: *Self, tokens: []const i64, base: ops.WhisperLogitsParams, suppress: []const i32, device_slot: ?usize) !bool {
            var params = base;
            const slot = if (device_slot != null) 1 - self.slot else self.slot;
            params.token_slot = @intCast(slot);
            params.stats_slot = @intCast(slot);
            const result = try self.decoder.stepWith(tokens, .{ .pipelined = .{ .params = params, .suppress = suppress, .device_token_slot = device_slot } });
            self.encoded = result == .encoded;
            return self.encoded;
        }

        pub fn encodePrefix(self: *Self, prefix: []const i64, params: ops.WhisperLogitsParams, suppressed: []const i32) !bool {
            const position = self.decoder.positions();
            if (position > prefix.len) return error.InvalidPendingDecodeStep;
            return self.encode(prefix[position..], params, suppressed, null);
        }

        pub fn encodeLookahead(self: *Self, params: ops.WhisperLogitsParams, suppressed: []const i32) !bool {
            return self.encode(&.{0}, params, suppressed, self.slot);
        }

        pub fn submit(self: *Self) !void {
            if (!self.encoded) return error.InvalidPendingDecodeStep;
            try self.decoder.submit();
            self.encoded = false;
            self.submitted = true;
        }

        pub fn awaitStats(self: *Self) !ops.WhisperLogitsStatsRaw {
            const stats = (try self.decoder.awaitStats(self.slot)) orelse return error.NoDecoderOutput;
            self.submitted = false;
            return stats;
        }

        pub fn discardLookahead(self: *Self) void {
            if (!self.encoded) return;
            self.decoder.discard();
            self.encoded = false;
        }

        pub fn submitLookahead(self: *Self) !void {
            try self.submit();
            self.slot = 1 - self.slot;
        }
    };
}
