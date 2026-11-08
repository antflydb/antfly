// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Internal, full-sequence soft input boundary. Text framing and memory
//! composition happen before this call; no tokenizer or vocabulary is mutated.
const std = @import("std");
const ops = @import("../ops/ops.zig");
const model = @import("../models/gpt.zig");
const gpt = @import("gpt.zig");

pub const Input = struct {
    /// Borrowed ordinary prompt positions, including the replaced positions.
    input_ids: []const i64,
    /// Borrowed row-major [sequence, hidden] initial activations. Every row
    /// is visible. The caller retains both slices until prefill returns.
    input_embeddings: []const f32,

    pub fn validate(self: Input, config: model.Config) !void {
        // PLE, M-RoPE and mixed media require separately trained boundaries.
        if (config.family != .qwen3 or config.hasPle() or config.isMultimodal() or
            config.usesMoe() or config.mrope_interleaved) return error.UnsupportedSoftPromptArchitecture;
        if (self.input_ids.len == 0 or config.hidden_size == 0 or
            self.input_ids.len > config.max_position_embeddings or
            self.input_ids.len > std.math.maxInt(i32) or config.hidden_size > std.math.maxInt(i32))
            return error.InvalidSoftPromptShape;
        const elements = std.math.mul(usize, self.input_ids.len, config.hidden_size) catch
            return error.InvalidSoftPromptShape;
        if (self.input_embeddings.len != elements) return error.InvalidSoftPromptShape;
        for (self.input_ids) |id| if (id < 0 or id >= config.vocab_size) return error.InvalidSoftPromptToken;
        for (self.input_embeddings) |value| if (!std.math.isFinite(value)) return error.NonFiniteSoftPrompt;
    }
};

/// Returns caller-owned last-position logits. The backend owns every temporary
/// tensor and consumes its uploaded embedding input on success and error.
/// A supplied context must describe a fresh, complete prefill at position zero.
/// Session/execution leases and KV lifetime remain the invoking owner's job.
pub fn prefillLastLogits(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    config: model.Config,
    input: Input,
    context: ?*const gpt.DecodeContext,
) ![]f32 {
    try input.validate(config);
    if (context) |ctx| {
        if (ctx.total_sequence_len != input.input_ids.len or
            ctx.query_sequence_len != input.input_ids.len or ctx.kv_sequence_len != input.input_ids.len or
            ctx.attention_mode == .paged_decode or ctx.kv_position_offset != 0 or
            ctx.attn_or_mask != null) return error.InvalidSoftPromptContext;
    }
    const shape = [_]i32{ 1, @intCast(input.input_ids.len), @intCast(config.hidden_size) };
    const host = try cb.fromFloat32Shape(input.input_embeddings, &shape);
    const resident = try cb.ensureDeviceResidentOwned(host);
    // The GPT from-embeddings entry point consumes resident, including errors.
    return gpt.forwardLastLogitsLastRowFromEmbeddingsWithLayer0Overrides(
        cb,
        allocator,
        config,
        resident,
        .{},
        1,
        input.input_ids.len,
        context,
        null,
    );
}

test "soft prompt validates architecture shape token identity and finite rows before execution" {
    const cfg = model.Config{
        .family = .qwen3,
        .hidden_size = 2,
        .vocab_size = 8,
        .max_position_embeddings = 4,
    };
    const input = Input{ .input_ids = &.{ 0, 7 }, .input_embeddings = &.{ 1, 2, 3, 4 } };
    try input.validate(cfg);
    try std.testing.expectError(error.InvalidSoftPromptShape, (Input{ .input_ids = &.{}, .input_embeddings = &.{} }).validate(cfg));
    try std.testing.expectError(error.InvalidSoftPromptShape, (Input{ .input_ids = &.{1}, .input_embeddings = &.{1} }).validate(cfg));
    try std.testing.expectError(error.InvalidSoftPromptToken, (Input{ .input_ids = &.{8}, .input_embeddings = &.{ 1, 2 } }).validate(cfg));
    try std.testing.expectError(error.InvalidSoftPromptToken, (Input{ .input_ids = &.{-1}, .input_embeddings = &.{ 1, 2 } }).validate(cfg));
    try std.testing.expectError(error.NonFiniteSoftPrompt, (Input{ .input_ids = &.{1}, .input_embeddings = &.{ std.math.nan(f32), 2 } }).validate(cfg));
    var unsupported = cfg;
    unsupported.ple_hidden_size = 2;
    try std.testing.expectError(error.UnsupportedSoftPromptArchitecture, input.validate(unsupported));
    unsupported = cfg;
    unsupported.family = .gemma;
    try std.testing.expectError(error.UnsupportedSoftPromptArchitecture, input.validate(unsupported));
}
