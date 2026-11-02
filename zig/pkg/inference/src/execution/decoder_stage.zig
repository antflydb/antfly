// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Request-owned decoder route and outputs. Tasks choose suppression/grammar,
//! temperature, language, timestamps, acceptance and token publication.
const std = @import("std");
const backends = @import("../backends/backends.zig");
const ops = @import("../ops/ops.zig");
const native_arch = @import("../architectures/whisper.zig");
const architecture = @import("architecture_session.zig");
const stages = @import("encoder_decoder_runtime.zig");
const merged = @import("seq2seq_runtime.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;

pub const Row = struct {
    allocator: std.mem.Allocator,
    logits: []const f32 = &.{},
    stats: ?ops.WhisperLogitsStatsRaw = null,
    owned_logits: ?[]f32 = null,
    host_outputs: []backends.Tensor = &.{},

    pub fn deinit(self: *Row) void {
        if (self.owned_logits) |values| self.allocator.free(values);
        for (self.host_outputs) |*value| value.deinit();
        if (self.host_outputs.len != 0) self.allocator.free(self.host_outputs);
        self.* = undefined;
    }
};

pub const Decoder = struct {
    allocator: std.mem.Allocator,
    session: backends.Session,
    encoder: *const stages.EncoderOutputs,
    encoder_mask: []const i64,
    vocab_size: usize,
    control: ?Control,
    native: ?architecture.WhisperNativeDecoder,
    incremental: ?merged.State,

    pub fn init(allocator: std.mem.Allocator, session: backends.Session, encoder: *const stages.EncoderOutputs, encoder_length: usize, encoder_mask: []const i64, vocab_size: usize, control: ?Control) !Decoder {
        const host: ?backends.Tensor = switch (encoder.*) {
            .host => |outputs| if (outputs.len != 0) outputs[0] else return error.NoEncoderOutput,
            .resident => null,
        };
        var native = if (host) |value| try architecture.whisperNativeDecoder(session, allocator, control, value.asFloat32(), encoder_length) else null;
        errdefer if (native) |*value| value.deinit();
        return .{
            .allocator = allocator,
            .session = session,
            .encoder = encoder,
            .encoder_mask = encoder_mask,
            .vocab_size = vocab_size,
            .control = control,
            .native = native,
            .incremental = if (native == null and host != null) merged.State.init(allocator, session, host.?, encoder_mask, vocab_size) else null,
        };
    }

    pub fn deinit(self: *Decoder) void {
        if (self.native) |*value| value.deinit();
        if (self.incremental) |*value| value.deinit();
        self.* = undefined;
    }

    /// Dispatch stays a synchronous borrowed capability; no task pointer or
    /// tokenizer is retained. Rows preserve provider ownership without copies.
    pub fn step(self: *Decoder, prefix: []const i64, stats_request: ?native_arch.StatsRequest, dispatch: anytype) !Row {
        var row = Row{ .allocator = self.allocator };
        errdefer row.deinit();
        if (self.native) |*decoder| {
            const pending = prefix[decoder.positions()..];
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
        switch (self.encoder.*) {
            .resident => {
                row.owned_logits = try stages.finalVocabularyRow(self.allocator, self.session, self.encoder, prefix, self.encoder_mask, self.vocab_size, self.control);
                row.logits = row.owned_logits.?;
            },
            .host => |outputs| {
                if (self.incremental) |*state| {
                    row.host_outputs = try state.stepOutputs(prefix, dispatch, self.control);
                } else {
                    const shape = [_]i64{ 1, @intCast(prefix.len) };
                    var tokens = try backends.Tensor.initInt64(self.allocator, "input_ids", &shape, prefix);
                    defer tokens.deinit();
                    const hidden = outputs[0].borrowedView("encoder_hidden_states");
                    row.host_outputs = if (comptime @TypeOf(dispatch) == @TypeOf(null))
                        try self.session.runWithControl(&.{ tokens, hidden }, self.allocator, self.control)
                    else if (comptime @typeInfo(@TypeOf(dispatch)) == .optional)
                        if (dispatch) |provider| try provider.run(self.allocator, self.session, null, null, &.{ tokens, hidden }, self.control) else try self.session.runWithControl(&.{ tokens, hidden }, self.allocator, self.control)
                    else
                        try dispatch.run(self.allocator, self.session, null, null, &.{ tokens, hidden }, self.control);
                }
                if (row.host_outputs.len == 0) return error.NoDecoderOutput;
                const logits = row.host_outputs[0].asFloat32();
                const vocab = if (row.host_outputs[0].shape.len >= 3)
                    @as(usize, @intCast(row.host_outputs[0].shape[2]))
                else
                    return error.InvalidLogitsShape;
                if (vocab == 0 or logits.len < vocab) return error.InvalidLogitsShape;
                row.logits = logits[logits.len - vocab ..];
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
    const session = try imported.createSessionWithOptions(a, path, .native, .{ .graph_runtime_strategy = .compiled_preferred });
    var session_open = true;
    defer if (session_open) session.close();
    var hidden = try backends.Tensor.initFloat32(a, "encoder_hidden_states", &.{ 1, 2, 4 }, &.{ -2, 1, 2, -1, 3, -1, 1, 0 });
    defer hidden.deinit();
    var tensors = [_]backends.Tensor{hidden};
    const encoder = stages.EncoderOutputs{ .host = &tensors };
    var decoder = try Decoder.init(a, session, &encoder, 2, &.{ 1, 1 }, 4, null);
    var decoder_open = true;
    defer if (decoder_open) decoder.deinit();
    try std.testing.expect(decoder.native == null and decoder.incremental == null);
    var row = try decoder.step(&.{ 0, 1 }, null, null);
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
    try std.testing.expectError(error.Cancelled, decoder.step(&.{ 0, 1 }, null, null));
    decoder.deinit();
    decoder_open = false;
    session.close();
    session_open = false;
    try std.testing.expectEqualSlices(f32, &.{ -3, 1, -1, 0 }, row.logits);
}
