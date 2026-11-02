// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Physical incremental encoder/decoder state. Tasks own token histories,
//! suppression/acceptance, EOS, original-page mappings and publication.
const std = @import("std");
const ops = @import("../ops/ops.zig");
const platform = @import("antfly_platform");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const CT = ops.CT;
const ComputeBackend = ops.ComputeBackend;

/// Compile-time architecture binding preserves its tuned scalar/batched
/// implementations without a backend wrapper or dispatch allocation.
pub fn CachedDecoder(comptime Architecture: type) type {
    return struct {
        const Self = @This();
        pub const Mode = enum { scalar, batched };
        pub const Context = struct {
            allocator: std.mem.Allocator,
            backend: *const ComputeBackend,
            model: *const Architecture.Config,
            control: ?Control = null,
            profile_stages: bool = false,
            replay_key: []const u8 = "encoder_decoder.lm_head_argmax",
            replay_input_label: []const u8 = "encoder_decoder.lm_head_input",
            unsupported_suppression: anyerror = error.UnsupportedOperation,
        };
        pub const FrameStats = struct {
            consumed_steps: usize = 0,
            api_fallbacks: usize = 0,
            topology_mismatches: usize = 0,
        };

        pub const HeadEvents = struct {
            pub const Fallback = enum { capture_unavailable, prepare_unavailable, dynamic_suppression };
            pub const Stage = enum { fused_argmax, fused_unavailable, token_download };
            pub const Timing = struct { stage: Stage, elapsed_ns: u64 };
            cuda_graph_replay: bool = false,
            cuda_graph_replay_steps: usize = 0,
            cuda_graph_capture_steps: usize = 0,
            cuda_graph_fallback_reason: ?Fallback = null,
            lm_head_path: ?[]const u8 = null,
            timings: [2]Timing = undefined,
            timing_count: usize = 0,
        };

        context: Context,
        mode: Mode,
        cache: Architecture.DecoderIncrementalCache,
        hidden: ?CT = null,
        owns_frame: bool = false,
        frame_start: u64 = 0,
        frame_stats: FrameStats = .{},
        compaction_supported: bool = true,
        head_events: HeadEvents = .{},

        pub fn init(context: Context, mode: Mode, encoder_hidden: CT, batch: usize, encoder_length: usize, capacity: usize) !Self {
            return .{
                .context = context,
                .mode = mode,
                .cache = try Architecture.buildDecoderIncrementalCache(context.backend, context.allocator, context.model.*, encoder_hidden, batch, encoder_length, capacity),
            };
        }

        pub fn deinit(self: *Self) void {
            // Driver cancellation precedes storage retirement on every exit.
            if (self.owns_frame) self.context.backend.decoderRuntimeCancelFrame() catch {};
            if (self.hidden) |hidden| self.context.backend.free(hidden);
            self.cache.deinit(self.context.backend, self.context.allocator);
            self.* = undefined;
        }

        pub fn preallocated(self: *const Self) bool {
            return self.cache.self.preallocated;
        }

        pub fn position(self: *const Self) usize {
            return self.cache.self.len;
        }

        /// A new forward is never replayed after an error. Existing hidden rows
        /// retire before the next step; decoder cache commit remains canonical.
        pub fn step(self: *Self, token_ids: []const i64, single_frame: bool) !void {
            if (self.context.control) |control| try control.check();
            if (self.owns_frame) return error.DecoderStepFrameStillOwned;
            if (self.hidden) |old| self.context.backend.free(old);
            self.hidden = null;
            self.owns_frame = try beginStepFrame(self.context.backend, single_frame);
            if (self.owns_frame)
                self.frame_start = self.context.backend.debugTimingSnapshot().provider.decoder_runtime_frame_submits;
            self.hidden = switch (self.mode) {
                .scalar => blk: {
                    if (token_ids.len != 1) return error.InvalidInputShape;
                    break :blk try Architecture.decoderForwardIncrementalStepFinalHiddenTensor(self.context.backend, self.context.allocator, self.context.model.*, token_ids[0], &self.cache);
                },
                .batched => try Architecture.decoderForwardIncrementalBatchStepFinalHiddenTensor(self.context.backend, self.context.allocator, self.context.model.*, token_ids, &self.cache),
            };
        }

        /// Ownership is explicit: no task structure stores a second hidden owner.
        pub fn takeHidden(self: *Self) !CT {
            const hidden = self.hidden orelse return error.InvalidInputShape;
            self.hidden = null;
            return hidden;
        }

        pub fn fusedTokens(self: *Self, suppressed: []const i32) !?[]u32 {
            const hidden = self.hidden orelse return error.InvalidInputShape;
            const selected = try Architecture.decoderFusedTokensFromFinalHiddenTensor(self.context.backend, self.context.allocator, self.context.model.*, hidden, self.cache.batch, suppressed);
            if (selected != null) {
                self.context.backend.free(hidden);
                self.hidden = null;
            }
            return selected;
        }

        /// The canonical projection consumes hidden on success; retain our
        /// ownership on failure so aggregate cleanup remains authoritative.
        pub fn projectRows(self: *Self) !CT {
            const hidden = self.hidden orelse return error.InvalidInputShape;
            const logits = try Architecture.decoderLmHeadLogitsRowsFromFinalHiddenTensor(self.context.backend, self.context.allocator, self.context.model.*, hidden, self.cache.batch);
            self.hidden = null;
            return logits;
        }

        pub fn argmaxRows(self: *Self, logits: CT) !?[]u32 {
            const prior_steps = self.frame_stats.consumed_steps;
            const tokens = try stepArgmaxRows(self.context.backend, logits, self.cache.batch, self.context.model.vocab_size, self.context.allocator, &self.owns_frame, &self.frame_stats.consumed_steps, &self.frame_stats.api_fallbacks);
            if (self.frame_stats.consumed_steps > prior_steps and
                self.context.backend.debugTimingSnapshot().provider.decoder_runtime_frame_submits -| self.frame_start != 1)
                self.frame_stats.topology_mismatches += 1;
            return tokens;
        }

        /// Task-supplied suppression is already resolved. This owner selects
        /// graph replay/fused projection/reduction without retaining task policy.
        pub fn selectScalar(self: *Self, suppress_tokens: []const i32, fused_lm_head_allowed: bool, allow_cuda_graph: bool) !u32 {
            const allocator = self.context.allocator;
            const cb = self.context.backend;
            const model = self.context.model.*;
            self.head_events = .{};
            var owned_hidden = try self.takeHidden();
            var owned_live = true;
            errdefer if (owned_live) cb.free(owned_hidden);
            const hidden = &owned_hidden;
            const hidden_live = &owned_live;

            if (fused_lm_head_allowed) {
                var capture_graph = false;
                errdefer if (capture_graph) cb.debugCudaGraphCaptureEnd(false) catch {};
                if (allow_cuda_graph) {
                    if (suppress_tokens.len == 0) {
                        if (try cb.debugCudaGraphPrepareGreedyTokenReplayInput(self.context.replay_key, hidden.*, 0)) |prepared| {
                            cb.free(hidden.*);
                            hidden.* = prepared;
                            hidden_live.* = true;
                            _ = try cb.debugCudaGraphPrepareDecodeScalars(0, 0, 0, 0, 0);
                            if (try cb.debugCudaGraphReplayFinalHidden(hidden.*)) |token_tensor| {
                                defer cb.free(token_tensor);
                                cb.free(hidden.*);
                                hidden_live.* = false;
                                self.head_events.cuda_graph_replay = true;
                                self.head_events.cuda_graph_replay_steps += 1;
                                self.head_events.lm_head_path = "fused_argmax_graph";
                                return try self.downloadToken(token_tensor);
                            }
                            capture_graph = try cb.debugCudaGraphCaptureBegin(self.context.replay_key);
                            if (capture_graph) {
                                try cb.debugCudaTraceTensor(self.context.replay_input_label, hidden.*);
                                try cb.debugCudaGraphRegisterFinalHiddenReplayInput(hidden.*);
                            } else if (self.head_events.cuda_graph_fallback_reason == null) {
                                self.head_events.cuda_graph_fallback_reason = .capture_unavailable;
                            }
                        } else if (self.head_events.cuda_graph_fallback_reason == null) {
                            self.head_events.cuda_graph_fallback_reason = .prepare_unavailable;
                        }
                    } else if (self.head_events.cuda_graph_fallback_reason == null) {
                        self.head_events.cuda_graph_fallback_reason = .dynamic_suppression;
                    }
                }
                const fused_start = platform.time.monotonicNs();
                if (try Architecture.decoderFusedTokenFromFinalHiddenTensor(cb, model, hidden.*, suppress_tokens)) |token_tensor| {
                    self.recordHeadTiming(.fused_argmax, fused_start);
                    defer cb.free(token_tensor);
                    if (capture_graph) {
                        try cb.debugCudaGraphRegisterFinalHiddenReplayBoundary(hidden.*, token_tensor);
                        try cb.debugCudaGraphCaptureEnd(true);
                        self.head_events.cuda_graph_capture_steps += 1;
                        capture_graph = false;
                    }
                    cb.free(hidden.*);
                    hidden_live.* = false;
                    if (self.head_events.lm_head_path == null) self.head_events.lm_head_path = "fused_argmax";
                    return try self.downloadToken(token_tensor);
                }
                self.recordHeadTiming(.fused_unavailable, fused_start);
                if (capture_graph) {
                    try cb.debugCudaGraphCaptureEnd(false);
                    capture_graph = false;
                }
                if (self.head_events.lm_head_path == null) self.head_events.lm_head_path = "full_logits_fused_unavailable";
            }

            const logits = try Architecture.decoderLmHeadLogitsFromFinalHiddenTensor(cb, allocator, model, hidden.*);
            hidden_live.* = false;
            defer cb.free(logits);

            if (suppress_tokens.len == 0) {
                if (self.head_events.lm_head_path == null) self.head_events.lm_head_path = "full_logits";
                const token = (try cb.argmaxLastRow(logits, 1, model.vocab_size)) orelse return error.UnsupportedOperation;
                return token;
            }

            if (try cb.argmaxRowsSuppress(logits, 0, 1, model.vocab_size, suppress_tokens, allocator)) |tokens| {
                defer allocator.free(tokens);
                if (tokens.len != 1) return error.InvalidTensorShape;
                if (self.head_events.lm_head_path == null) self.head_events.lm_head_path = "full_logits";
                return tokens[0];
            }

            if (try cb.argmaxLastRowSuppressTensor(logits, 1, model.vocab_size, suppress_tokens)) |token_tensor| {
                defer cb.free(token_tensor);
                if (self.head_events.lm_head_path == null) self.head_events.lm_head_path = "full_logits";
                return try self.downloadToken(token_tensor);
            }

            return self.context.unsupported_suppression;
        }

        fn recordHeadTiming(self: *Self, stage: HeadEvents.Stage, start: u64) void {
            if (!self.context.profile_stages) return;
            self.head_events.timings[self.head_events.timing_count] = .{ .stage = stage, .elapsed_ns = platform.time.monotonicNs() - start };
            self.head_events.timing_count += 1;
        }

        fn downloadToken(self: *Self, tensor: CT) !u32 {
            const started = platform.time.monotonicNs();
            const values = try self.context.backend.toFloat32(tensor, self.context.allocator);
            self.recordHeadTiming(.token_download, started);
            defer self.context.allocator.free(values);
            if (values.len != 1 or values[0] < 0) return error.InvalidTensorShape;
            return @intFromFloat(values[0]);
        }

        /// Task supplies only retained cache indices, preserving its separate
        /// original-page map. Unsupported layouts disable later compaction.
        pub fn compactRows(self: *Self, retained_rows: []const u32) !bool {
            if (!self.compaction_supported) return false;
            if (!try Architecture.compactDecoderIncrementalCache(self.context.backend, self.context.allocator, self.context.model.*, &self.cache, retained_rows)) {
                self.compaction_supported = false;
                return false;
            }
            return true;
        }
    };
}

pub fn beginStepFrame(cb: *const ComputeBackend, enabled: bool) !bool {
    if (!enabled or cb.kind() != .metal or cb.decoderRuntimeHasActiveFrame()) return false;
    if (!try cb.decoderRuntimeBeginFrame()) return false;
    errdefer cb.decoderRuntimeCancelFrame() catch {};
    try cb.decoderRuntimeSetActiveFrameRegime(.decode);
    if (!try cb.decoderRuntimeBeginPlannedComputeScope()) {
        try cb.decoderRuntimeCancelFrame();
        return false;
    }
    return true;
}

pub fn stepArgmaxRows(cb: *const ComputeBackend, logits: CT, rows: usize, vocab: usize, allocator: std.mem.Allocator, owns_frame: *bool, steps: *usize, fallbacks: *usize) !?[]u32 {
    if (owns_frame.*) {
        _ = try cb.decoderRuntimeBeginPlannedComputeScope();
        if (try cb.decoderRuntimeArgmaxRowsSuppress(logits, 0, rows, vocab, &.{}, allocator)) |selected| {
            if (cb.decoderRuntimeHasActiveFrame()) {
                allocator.free(selected);
                return error.FlorenceArgmaxFrameNotConsumed;
            }
            owns_frame.* = false;
            steps.* += 1;
            return selected;
        }
        if (!cb.decoderRuntimeHasActiveFrame()) {
            owns_frame.* = false;
            return error.FlorenceArgmaxFrameFailed;
        }
        try cb.decoderRuntimeSubmitAndWaitFrame();
        owns_frame.* = false;
        fallbacks.* += 1;
    }
    return cb.argmaxRows(logits, 0, rows, vocab, allocator);
}

const TestArchitecture = @import("../architectures/florence.zig");
const TestDecoder = CachedDecoder(TestArchitecture);

fn populateTestWeights(store: *@import("../ops/native_compute.zig").WeightStore) !void {
    const fixture = @import("../ops/media_residency_test.zig");
    try fixture.putWeight(store, "language_model.model.shared.weight", &.{ 4, 4 }, null);
    try fixture.putWeight(store, "language_model.model.decoder.embed_positions.weight", &.{ 12, 4 }, null);
    try fixture.putWeight(store, "language_model.model.decoder.layernorm_embedding.weight", &.{4}, 1);
    try fixture.putWeight(store, "language_model.model.decoder.layernorm_embedding.bias", &.{4}, 0);
    var name_buf: [256]u8 = undefined;
    for ([_][]const u8{ "self_attn.q_proj", "self_attn.k_proj", "self_attn.v_proj", "self_attn.out_proj", "encoder_attn.q_proj", "encoder_attn.k_proj", "encoder_attn.v_proj", "encoder_attn.out_proj", "fc1", "fc2" }) |suffix| {
        const output: i64 = if (std.mem.eql(u8, suffix, "fc1")) 8 else 4;
        const input: i64 = if (std.mem.eql(u8, suffix, "fc2")) 8 else 4;
        try fixture.putWeight(store, try std.fmt.bufPrint(&name_buf, "language_model.model.decoder.layers.0.{s}.weight", .{suffix}), &.{ output, input }, null);
        try fixture.putWeight(store, try std.fmt.bufPrint(&name_buf, "language_model.model.decoder.layers.0.{s}.bias", .{suffix}), &.{output}, null);
    }
    for ([_][]const u8{ "self_attn_layer_norm", "encoder_attn_layer_norm", "final_layer_norm" }) |suffix| {
        try fixture.putWeight(store, try std.fmt.bufPrint(&name_buf, "language_model.model.decoder.layers.0.{s}.weight", .{suffix}), &.{4}, 1);
        try fixture.putWeight(store, try std.fmt.bufPrint(&name_buf, "language_model.model.decoder.layers.0.{s}.bias", .{suffix}), &.{4}, 0);
    }
}

fn checkActualCachedDecoder(cb_value: ComputeBackend, _: ComputeBackend) !void {
    const a = std.testing.allocator;
    const cb = &cb_value;
    const config = TestArchitecture.Config{ .d_model = 4, .decoder_layers = 1, .decoder_attention_heads = 1, .decoder_ffn_dim = 8, .vocab_size = 4 };
    const encoder = try cb.fromFloat32Shape(&.{ 1, 2, 3, 4, 5, 6, 7, 8 }, &.{ 2, 1, 4 });
    defer cb.free(encoder);
    var raw = try TestArchitecture.buildDecoderIncrementalCache(cb, a, config, encoder, 2, 1, 4);
    defer raw.deinit(cb, a);
    var owned = try TestDecoder.init(.{ .allocator = a, .backend = cb, .model = &config }, .batched, encoder, 2, 1, 4);
    defer owned.deinit();
    for ([_][2]i64{ .{ 1, 2 }, .{ 3, 1 } }) |tokens| {
        const expected_hidden = try TestArchitecture.decoderForwardIncrementalBatchStepFinalHiddenTensor(cb, a, config, &tokens, &raw);
        const expected_logits = try TestArchitecture.decoderLmHeadLogitsRowsFromFinalHiddenTensor(cb, a, config, expected_hidden, 2);
        defer cb.free(expected_logits);
        try owned.step(&tokens, false);
        const logits = try owned.projectRows();
        defer cb.free(logits);
        const expected = try cb.toFloat32(expected_logits, a);
        defer a.free(expected);
        const actual = try cb.toFloat32(logits, a);
        defer a.free(actual);
        try std.testing.expectEqualSlices(f32, expected, actual);
        try std.testing.expectEqual(raw.self.len, owned.position());
        for (raw.self.keys, raw.self.values, owned.cache.self.keys, owned.cache.self.values) |expected_key, expected_value, actual_key, actual_value| {
            const expected_k = try cb.toFloat32(expected_key.?, a);
            defer a.free(expected_k);
            const actual_k = try cb.toFloat32(actual_key.?, a);
            defer a.free(actual_k);
            const expected_v = try cb.toFloat32(expected_value.?, a);
            defer a.free(expected_v);
            const actual_v = try cb.toFloat32(actual_value.?, a);
            defer a.free(actual_v);
            try std.testing.expectEqualSlices(f32, expected_k, actual_k);
            try std.testing.expectEqualSlices(f32, expected_v, actual_v);
        }
    }
    const compacted = try owned.compactRows(&.{1});
    const raw_compacted = try TestArchitecture.compactDecoderIncrementalCache(cb, a, config, &raw, &.{1});
    try std.testing.expectEqual(raw_compacted, compacted);
    try std.testing.expectEqual(raw.batch, owned.cache.batch);
    try std.testing.expectEqualSlices(i64, raw.encoder_mask, owned.cache.encoder_mask);
    try std.testing.expectEqualSlices(i64, raw.self_mask, owned.cache.self_mask);
    const expected_hidden = try TestArchitecture.decoderForwardIncrementalBatchStepFinalHiddenTensor(cb, a, config, &.{2}, &raw);
    defer cb.free(expected_hidden);
    try owned.step(&.{2}, false);
    const actual_hidden = try owned.takeHidden();
    defer cb.free(actual_hidden);
    const expected = try cb.toFloat32(expected_hidden, a);
    defer a.free(expected);
    const actual = try cb.toFloat32(actual_hidden, a);
    defer a.free(actual);
    try std.testing.expectEqualSlices(f32, expected, actual);
    try std.testing.expectEqual(raw.self.len, owned.position());

    // Both entry modes preserve their actual architecture path. Cancellation
    // leaves the scalar cache position and its real retained tensor untouched.
    const Cancel = struct {
        cancelled: bool = false,
        fn check(raw_ptr: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw_ptr.?));
            if (self.cancelled) return error.Cancelled;
        }
    };
    const scalar_encoder = try cb.fromFloat32Shape(&.{ 1, 2, 3, 4 }, &.{ 1, 1, 4 });
    defer cb.free(scalar_encoder);
    var cancel = Cancel{};
    var scalar = try TestDecoder.init(.{ .allocator = a, .backend = cb, .model = &config, .control = .{ .ptr = &cancel, .check_fn = Cancel.check } }, .scalar, scalar_encoder, 1, 1, 4);
    defer scalar.deinit();
    try scalar.step(&.{1}, false);
    const retained = scalar.hidden.?;
    cancel.cancelled = true;
    try std.testing.expectError(error.Cancelled, scalar.step(&.{2}, false));
    try std.testing.expectEqual(@as(usize, 1), scalar.position());
    try std.testing.expectEqual(retained, scalar.hidden.?);

    var scalar_reference = try TestArchitecture.buildDecoderIncrementalCache(cb, a, config, scalar_encoder, 1, 1, 4);
    defer scalar_reference.deinit(cb, a);
    const reference_hidden = try TestArchitecture.decoderForwardIncrementalStepFinalHiddenTensor(cb, a, config, 1, &scalar_reference);
    const reference_logits = try TestArchitecture.decoderLmHeadLogitsFromFinalHiddenTensor(cb, a, config, reference_hidden);
    defer cb.free(reference_logits);
    const reference = try cb.toFloat32(reference_logits, a);
    defer a.free(reference);
    var top: usize = 0;
    for (reference, 0..) |value, index| if (value > reference[top]) {
        top = index;
    };
    var second: usize = if (top == 0) 1 else 0;
    for (reference, 0..) |value, index| if (index != top and value > reference[second]) {
        second = index;
    };
    scalar.context.control = null;
    const suppressed = [_]i32{@intCast(top)};
    const selected = try scalar.selectScalar(&suppressed, false, false);
    try std.testing.expectEqual(@as(u32, @intCast(second)), selected);
    try std.testing.expect(scalar.hidden == null);
    try std.testing.expectEqual(@as(usize, 1), scalar.position());
    try std.testing.expectEqualStrings("full_logits", scalar.head_events.lm_head_path.?);
    const Oom = struct {
        fn run(allocator: std.mem.Allocator, backend: *const ComputeBackend, model: *const TestArchitecture.Config, encoded: CT) !void {
            var decoder = try TestDecoder.init(.{ .allocator = allocator, .backend = backend, .model = model }, .batched, encoded, 2, 1, 4);
            defer decoder.deinit();
            try decoder.step(&.{ 1, 2 }, false);
            const projected = try decoder.projectRows();
            defer backend.free(projected);
            _ = try decoder.compactRows(&.{1});
            try decoder.step(&.{2}, false);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Oom.run, .{ cb, &config, encoder });
}

test "cached decoder owner matches actual native scalar batch and compacted state and preserves cancelled ownership" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    try @import("../ops/media_residency_test.zig").onNative(populateTestWeights, checkActualCachedDecoder);
}
