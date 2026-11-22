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

//! Request-owned decoder route and outputs. Tasks choose suppression/grammar,
//! temperature, language, timestamps, acceptance and token publication.
const std = @import("std");
const backends = @import("../backends/backends.zig");
const ops = @import("../ops/ops.zig");
const Native = architecture.IncrementalDecoderAdapter;
const architecture = @import("architecture_session.zig");
const stages = @import("encoder_decoder_runtime.zig");
const merged = @import("seq2seq_runtime.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;

pub const Row = struct {
    allocator: std.mem.Allocator,
    logits: []const f32 = &.{},
    vocab_size: usize = 0,
    stats: ?ops.WhisperLogitsStatsRaw = null,
    owned_logits: ?[]f32 = null,
    host_outputs: []backends.Tensor = &.{},
    host_logits: ?backends.Tensor = null,

    pub fn deinit(self: *Row) void {
        if (self.owned_logits) |values| self.allocator.free(values);
        if (self.host_logits) |*output| output.deinit();
        for (self.host_outputs) |*value| value.deinit();
        if (self.host_outputs.len != 0) self.allocator.free(self.host_outputs);
        self.* = undefined;
    }

    fn selectHostRow(self: *Row, output: backends.Tensor, full_prefix_rows: ?usize) !void {
        if (output.dtype != .f32 or output.shape.len < 3 or output.shape[2] <= 0) return error.InvalidLogitsShape;
        const logits = output.asFloat32();
        const vocab = std.math.cast(usize, output.shape[2]) orelse return error.InvalidLogitsShape;
        const end = if (full_prefix_rows) |rows| std.math.mul(usize, rows, vocab) catch return error.InvalidLogitsShape else logits.len;
        if (end < vocab or end > logits.len) return error.InvalidLogitsShape;
        self.vocab_size = vocab;
        self.logits = logits[end - vocab .. end];
    }
};

pub const RetainedEncoderSource = Native.Source;

/// The caller owns the mutable encoder holder during promotion. The source
/// owns its shape snapshot and an independent complete storage/provider pin.
pub fn retainIncrementalSource(allocator: std.mem.Allocator, encoded: *stages.EncoderOutputs, decoder: backends.Session) !?RetainedEncoderSource {
    const receiver = (try encoded.incrementalReceiverFor(decoder)) orelse return null;
    const resident = &encoded.resident;
    const pin = try resident.values.retainResources(allocator);
    return .{ .receiver = receiver, .value = resident.values.outputs[0], .backend = resident.values.backend, .shape = resident.shape, .resources = pin };
}

pub const Decoder = struct {
    pub const Options = struct {
        allow_native: bool = true,
        allow_merged: bool = true,
        compiled_cache: ?bool = null,
        output_rows: enum { final_available, full_prefix } = .final_available,
        expected_initial_query_rows: ?usize = null,
    };
    allocator: std.mem.Allocator,
    session: backends.Session,
    encoder: *const stages.EncoderOutputs,
    encoder_mask: []const i64,
    vocab_size: usize,
    control: ?Control,
    native: ?Native.State,
    options: Options,
    incremental: ?merged.State,
    full_prefix: ?stages.DecoderInputPlan,
    compiled: ?@import("resident_decoder.zig").Decoder = null,

    pub fn init(allocator: std.mem.Allocator, session: backends.Session, encoder: *const stages.EncoderOutputs, encoder_length: usize, encoder_mask: []const i64, vocab_size: usize, control: ?Control) !Decoder {
        return initWithOptions(allocator, session, encoder, encoder_length, encoder_mask, vocab_size, control, .{});
    }

    /// Promote a qualified resident encoder source under runtime ownership.
    /// The task supplies its initial-query contract and keeps only token policy.
    pub fn initForEncoder(allocator: std.mem.Allocator, session: backends.Session, encoder: *stages.EncoderOutputs, encoder_length: usize, encoder_mask: []const i64, vocab_size: usize, control: ?Control, options: Options) !Decoder {
        if (control) |active| active.check() catch |err| {
            observeRoute(session, encoder, options, .failed, err);
            return err;
        };
        var source = if (options.allow_native) retainIncrementalSource(allocator, encoder, session) catch |err| {
            observeRoute(session, encoder, options, .failed, err);
            return err;
        } else null;
        defer if (source) |*value| value.deinit();
        return initWithSource(allocator, session, encoder, encoder_length, encoder_mask, vocab_size, control, options, if (source) |*value| value else null);
    }

    pub fn initWithOptions(allocator: std.mem.Allocator, session: backends.Session, encoder: *const stages.EncoderOutputs, encoder_length: usize, encoder_mask: []const i64, vocab_size: usize, control: ?Control, options: Options) !Decoder {
        return initWithSource(allocator, session, encoder, encoder_length, encoder_mask, vocab_size, control, options, null);
    }

    pub fn initWithSource(allocator: std.mem.Allocator, session: backends.Session, encoder: *const stages.EncoderOutputs, encoder_length: usize, encoder_mask: []const i64, vocab_size: usize, control: ?Control, options: Options, source: ?*const Native.Source) !Decoder {
        return prepareSource(allocator, session, encoder, encoder_length, encoder_mask, vocab_size, control, options, source) catch |err| {
            observeRoute(session, encoder, options, .failed, err);
            return err;
        };
    }

    fn prepareSource(allocator: std.mem.Allocator, session: backends.Session, encoder: *const stages.EncoderOutputs, encoder_length: usize, encoder_mask: []const i64, vocab_size: usize, control: ?Control, options: Options, source: ?*const Native.Source) !Decoder {
        const host: ?backends.Tensor = switch (encoder.*) {
            .host => |outputs| if (outputs.len != 0) outputs[0] else return error.NoEncoderOutput,
            .resident => null,
        };
        var native: ?Native.State = if (source) |resident| blk: {
            if (!options.allow_native) return error.InvalidIncrementalSource;
            break :blk try Native.fromResidentPrepared(session, allocator, control, try resident.retained(), .{ .expected_initial_query_rows = options.expected_initial_query_rows });
        } else if (options.allow_native and host != null) try Native.fromHostPrepared(session, allocator, control, host.?.asFloat32(), encoder_length, .{ .expected_initial_query_rows = options.expected_initial_query_rows }) else null;
        errdefer if (native) |*value| value.deinit();
        var compiled = if (native == null and options.allow_native and host != null) try @import("resident_decoder.zig").Decoder.initEncoderDecoder(allocator, session, encoder_length, options.compiled_cache, control) else null;
        errdefer if (compiled) |*value| value.deinit();
        const incremental = if (options.allow_merged and native == null and compiled == null and host != null) try merged.State.init(allocator, session, host.?, encoder_mask, vocab_size) else null;
        const full_prefix = if (native == null and compiled == null and incremental == null)
            try stages.DecoderInputPlan.init(allocator, try session.inputInfoForStage(.decoder))
        else
            null;
        observeRoute(session, encoder, options, if (native != null) .native_incremental else if (compiled != null) .compiled_incremental else if (incremental != null) .merged_incremental else .full_prefix, null);
        return .{
            .allocator = allocator,
            .session = session,
            .encoder = encoder,
            .encoder_mask = encoder_mask,
            .vocab_size = vocab_size,
            .control = control,
            .native = native,
            .compiled = compiled,
            .options = options,
            .incremental = incremental,
            .full_prefix = full_prefix,
        };
    }

    fn observeRoute(session: backends.Session, encoder: *const stages.EncoderOutputs, options: Options, selected: @import("diagnostics.zig").DecoderRoute.Selection, failure: ?anyerror) void {
        const binding = session.invocation_diagnostics orelse return;
        if (!binding.diagnostics.isEnabled()) return;
        publishRoute(binding.diagnostics, .{
            .selected = selected,
            .backend = session.backend(),
            .encoder_storage = switch (encoder.*) {
                .host => .host,
                .resident => .resident,
            },
            .cache_custody = switch (selected) {
                .failed => .unavailable,
                .full_prefix => .none,
                .merged_incremental => .merged_resident,
                .native_incremental => .native_runtime,
                .compiled_incremental => .compiled_runtime,
            },
            .native_allowed = options.allow_native,
            .merged_allowed = options.allow_merged,
            .compiled_cache_requested = options.compiled_cache,
            .expected_initial_query_rows = options.expected_initial_query_rows,
            .requested_rows = if (options.output_rows == .full_prefix) .full_prefix else .final_available,
        }, failure);
    }
    noinline fn publishRoute(publisher: *@import("diagnostics.zig").Diagnostics, route: @import("diagnostics.zig").DecoderRoute, failure: ?anyerror) void {
        publisher.publish(.{
            .scope = .session_preparation,
            .operation = .decoder_init,
            .decoder_route = route,
            .outcome = if (failure != null) .failed else .success,
            .error_name = if (failure) |err| @import("diagnostics.zig").Text.init(@errorName(err)) else null,
        });
    }

    pub fn deinit(self: *Decoder) void {
        if (self.native) |*value| value.deinit();
        if (self.compiled) |*value| value.deinit();
        if (self.incremental) |*value| value.deinit();
        if (self.full_prefix) |*value| value.deinit();
        self.* = undefined;
    }

    /// Dispatch stays a synchronous borrowed capability; no task pointer or
    /// tokenizer is retained. Rows preserve provider ownership without copies.
    pub fn stepSuffix(self: *Decoder, suffix: @import("decoder_position.zig").Suffix, stats_request: ?Native.StatsRequest, dispatch: anytype) !Row {
        var row = Row{ .allocator = self.allocator, .vocab_size = self.vocab_size };
        errdefer row.deinit();
        if (self.native) |*decoder| {
            _ = try suffix.endAt(decoder.positions());
            const pending = suffix.tokens;
            if (stats_request) |request| {
                switch (try decoder.stepWith(pending, .{ .stats = request })) {
                    .stats => |stats| row.stats = stats,
                    .logits => |logits| {
                        row.owned_logits = logits;
                        row.logits = logits;
                    },
                    .none, .encoded => return error.NoDecoderOutput,
                }
            } else {
                row.owned_logits = try decoder.step(pending);
                row.logits = row.owned_logits.?;
            }
            return row;
        }
        if (self.compiled) |*decoder| {
            const outputs = self.encoder.host;
            row.owned_logits = try decoder.encoderDecoderSuffix(suffix, outputs[0], self.encoder_mask, self.allocator, self.control);
            row.logits = row.owned_logits.?;
            return row;
        }
        if (self.incremental) |*state| {
            const adapters = @import("tensor_dispatch.zig");
            const stage = adapters.stageDispatcher(dispatch);
            if (adapters.hasDispatcher(dispatch) and stage == null) return error.DecoderDispatchChanged;
            row.owned_logits = try state.stepResidentDispatched(suffix, self.control, stage);
            row.logits = row.owned_logits.?;
            return row;
        }
        return error.InvalidDecoderPosition;
    }

    /// Append-only callers already own their history. Full-prefix routes
    /// still need the whole slice, while incremental routes consume only
    /// the explicitly positioned suffix.
    pub fn step(self: *Decoder, prefix: []const i64, position: usize, stats_request: ?Native.StatsRequest, dispatch: anytype) !Row {
        if (position >= prefix.len) return error.InvalidDecoderPosition;
        if (self.full_prefix == null) return self.stepSuffix(.{ .position = position, .tokens = prefix[position..] }, stats_request, dispatch);
        return self.stepFullPrefix(prefix, dispatch);
    }

    pub fn stepFullPrefix(self: *Decoder, prefix: []const i64, dispatch: anytype) !Row {
        var row = Row{ .allocator = self.allocator, .vocab_size = self.vocab_size };
        errdefer row.deinit();
        var inputs: stages.DecoderInputs = undefined;
        const plan = if (self.full_prefix) |*value| value else return error.InvalidDecoderInput;
        const hidden = try stages.decoderHiddenInput(self.encoder, self.encoder_mask.len);
        try plan.bind(&inputs, prefix, self.encoder_mask, hidden);
        switch (hidden) {
            .resident => {
                const adapters = @import("tensor_dispatch.zig");
                const stage = adapters.stageDispatcher(dispatch);
                if (adapters.hasDispatcher(dispatch) and stage == null) return error.DecoderDispatchChanged;
                row.owned_logits = try stages.finalVocabularyRow(self.allocator, self.session, inputs.stageInputs(), prefix.len, self.vocab_size, self.control, stage);
                row.logits = row.owned_logits.?;
            },
            .host => {
                const host_inputs = try inputs.hostInputs();
                row.host_outputs = try @import("tensor_dispatch.zig").run(self.allocator, self.session, null, null, host_inputs, self.control, dispatch);
                if (row.host_outputs.len == 0) return error.NoDecoderOutput;
                try row.selectHostRow(row.host_outputs[0], if (self.options.output_rows == .full_prefix) prefix.len else null);
            },
        }
        return row;
    }
};

test "decoder route retains actual host output rows without copies and cancels before dispatch" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const a = std.testing.allocator;
    var temp = std.testing.tmpDir(.{});
    defer temp.cleanup();
    try @import("testing/encoder_decoder.zig").write(a, temp.dir, "decoder.onnx", false);
    const path = try std.fs.path.join(a, &.{ ".zig-cache", "tmp", temp.sub_path[0..], "decoder.onnx" });
    defer a.free(path);
    const imported = @import("../backends/imported_onnx_session.zig");
    var session = try imported.createSessionWithOptions(a, path, .native, .{ .graph_runtime_strategy = .compiled_preferred });
    var diagnostics = @import("diagnostics.zig").Diagnostics{};
    diagnostics.configure(true);
    session.invocation_diagnostics = .{ .diagnostics = &diagnostics };
    var session_open = true;
    defer if (session_open) session.close();
    var hidden = try backends.Tensor.initFloat32(a, "encoder_hidden_states", &.{ 1, 2, 4 }, &.{ -2, 1, 2, -1, 3, -1, 1, 0 });
    defer hidden.deinit();
    var tensors = [_]backends.Tensor{hidden};
    var encoder = stages.EncoderOutputs{ .host = &tensors };
    var decoder = try Decoder.init(a, session, &encoder, 2, &.{ 1, 1 }, 4, null);
    var decoder_open = true;
    defer if (decoder_open) decoder.deinit();
    try std.testing.expect(decoder.native == null and decoder.incremental == null);
    const route = diagnostics.snapshot().?.report.?.decoder_route.?;
    try std.testing.expectEqual(@import("diagnostics.zig").DecoderRoute.Selection.full_prefix, route.selected);
    try std.testing.expectEqual(backends.BackendType.native, route.backend);
    try std.testing.expectEqual(.none, route.cache_custody);
    try std.testing.expect(diagnostics.snapshot().?.execution == null);
    const missing = stages.EncoderOutputs{ .host = &.{} };
    try std.testing.expectError(error.NoEncoderOutput, Decoder.init(a, session, &missing, 2, &.{ 1, 1 }, 4, null));
    const failed = diagnostics.snapshot().?;
    try std.testing.expectEqual(@import("diagnostics.zig").DecoderRoute.Selection.failed, failed.report.?.decoder_route.?.selected);
    try std.testing.expectEqualStrings("NoEncoderOutput", failed.report.?.failure.bytes[0..failed.report.?.failure.len]);
    try std.testing.expect(failed.execution == null and failed.report.?.transfers == null);
    var row = try decoder.step(&.{ 0, 1 }, 0, null, null);
    defer row.deinit();
    try std.testing.expect(row.owned_logits == null);
    try std.testing.expectEqualSlices(f32, &.{ -3, 1, -1, 0 }, row.logits);
    const data = row.host_outputs[0].asFloat32();
    try std.testing.expect(row.logits.ptr == data[data.len - 4 ..].ptr);
    const Cancel = struct {
        fn check(_: ?*anyopaque) !void {
            return error.Cancelled;
        }
    };
    decoder.control = .{ .check_fn = Cancel.check };
    try std.testing.expectError(error.Cancelled, Decoder.initForEncoder(a, session, &encoder, 2, &.{ 1, 1 }, 4, decoder.control, .{}));
    const cancelled = diagnostics.snapshot().?;
    try std.testing.expectEqual(@import("diagnostics.zig").DecoderRoute.Selection.failed, cancelled.report.?.decoder_route.?.selected);
    try std.testing.expectEqualStrings("Cancelled", cancelled.report.?.failure.bytes[0..cancelled.report.?.failure.len]);
    try std.testing.expect(cancelled.execution == null and cancelled.report.?.transfers == null);
    try std.testing.expectError(error.Cancelled, decoder.step(&.{ 0, 1 }, 0, null, null));
    decoder.deinit();
    decoder_open = false;
    session.close();
    session_open = false;
    try std.testing.expectEqualSlices(f32, &.{ -3, 1, -1, 0 }, row.logits);
}
