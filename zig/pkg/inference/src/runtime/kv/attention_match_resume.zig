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
const checkpoint = @import("attention_match_checkpoint.zig");
const Control = @import("attention_match.zig").Control;

pub const Geometry = struct { layer: u32, window: u32, heads: u32, dim: u32 };
pub const Contract = struct {
    identity: checkpoint.Identity,
    vocab_size: u32,
    max_logical_tokens: usize,
    max_appended_tokens: usize = 8192,
    layer_donors: []const u32,
    donors: []const Geometry,
};

/// The contract is derived from the loaded model, never the attachment header.
/// Check identity and exact model geometry before any CUDA allocation or cache
/// publication. Resume requires an identical token prefix at original positions.
/// Empty append is valid: use the saved logits rather than replaying a token.
pub fn validate(state: checkpoint.State, contract: Contract, full_prompt: []const u32, limits: checkpoint.Limits) !usize {
    return validateWithControl(state, contract, full_prompt, limits, .{});
}

pub fn validateWithControl(state: checkpoint.State, contract: Contract, full_prompt: []const u32, limits: checkpoint.Limits, control: Control) !usize {
    try control.check();
    if (!state.identity.eql(contract.identity)) return error.KvCheckpointModelMismatch;
    if (contract.vocab_size == 0 or contract.max_logical_tokens == 0 or contract.max_logical_tokens > checkpoint.max_context_tokens or contract.max_appended_tokens > 8192 or
        contract.layer_donors.len == 0 or contract.donors.len == 0) return error.InvalidKvResumeContract;
    try state.validateWithControl(limits, control);
    if (state.logits.len != contract.vocab_size or state.logical_length > contract.max_logical_tokens or
        state.layer_donors.len != contract.layer_donors.len or state.donors.len != contract.donors.len or
        !std.mem.eql(u32, state.layer_donors, contract.layer_donors)) return error.KvCheckpointLayoutMismatch;
    for (state.donors, contract.donors) |donor, expected| {
        if (donor.layer != expected.layer or donor.window != expected.window or donor.heads.len != expected.heads or expected.dim == 0) return error.KvCheckpointLayoutMismatch;
        for (donor.heads) |head| if (head.dim != expected.dim) return error.KvCheckpointLayoutMismatch;
    }
    if (full_prompt.len < state.tokens.len or full_prompt.len > contract.max_logical_tokens or
        full_prompt.len - state.tokens.len > contract.max_appended_tokens) return error.KvCheckpointAppendLimit;
    if (!std.mem.eql(u32, full_prompt[0..state.tokens.len], state.tokens)) return error.KvCheckpointPrefixMismatch;
    for (full_prompt[state.tokens.len..]) |token| if (token >= contract.vocab_size) return error.InvalidKvCheckpoint;
    try control.check();
    return state.tokens.len;
}

const identity: checkpoint.Identity = .{ .weights = @splat(1), .tokenizer = @splat(2), .template = @splat(3), .layout = @splat(4) };
const global: checkpoint.Head = .{ .dim = 1, .compacted_until = 2, .positions = &.{ 0, 2, 3 }, .coverage_ends = &.{ 2, 3, 4 }, .keys = &.{ 0, 0, 0 }, .values = &.{ 2, 3, 4 }, .bias = &.{ @log(@as(f32, 2)), 0, 0 } };
const local: checkpoint.Head = .{ .dim = 1, .positions = &.{ 2, 3 }, .coverage_ends = &.{ 3, 4 }, .keys = &.{ 0, 0 }, .values = &.{ 3, 4 }, .bias = &.{ 0, 0 } };
const fixture_state: checkpoint.State = .{ .identity = identity, .logical_length = 4, .tokens = &.{ 0, 1, 2, 3 }, .logits = &.{ 0.1, 0.2, 0.3, 0.4 }, .layer_donors = &.{ 0, 1, 0 }, .donors = &.{ .{ .layer = 0, .heads = &.{global} }, .{ .layer = 1, .window = 2, .heads = &.{local} } } };
const fixture_contract: Contract = .{ .identity = identity, .vocab_size = 4, .max_logical_tokens = 16, .max_appended_tokens = 4, .layer_donors = &.{ 0, 1, 0 }, .donors = &.{ .{ .layer = 0, .window = 0, .heads = 1, .dim = 1 }, .{ .layer = 1, .window = 2, .heads = 1, .dim = 1 } } };

test "resume uses full logical prefix and keeps saved next-token logits for empty append" {
    try std.testing.expectEqual(@as(usize, 4), try validate(fixture_state, fixture_contract, &.{ 0, 1, 2, 3 }, .{}));
    try std.testing.expectEqual(@as(usize, 4), try validate(fixture_state, fixture_contract, &.{ 0, 1, 2, 3, 0, 1 }, .{}));
    const bytes = try checkpoint.encodeV2(std.testing.allocator, fixture_state, .{});
    defer std.testing.allocator.free(bytes);
    var restored = try checkpoint.decode(std.testing.allocator, bytes, identity, .{});
    defer restored.deinit();
    _ = try validate(restored.state, fixture_contract, fixture_state.tokens, .{});
    try std.testing.expectEqualSlices(f32, fixture_state.logits, restored.state.logits);
}

test "resume rejects a valid attachment with the wrong loaded-model geometry" {
    var changed = fixture_contract;
    changed.vocab_size = 5;
    try std.testing.expectError(error.KvCheckpointLayoutMismatch, validate(fixture_state, changed, fixture_state.tokens, .{}));
    changed = fixture_contract;
    changed.layer_donors = &.{ 0, 1, 1 };
    try std.testing.expectError(error.KvCheckpointLayoutMismatch, validate(fixture_state, changed, fixture_state.tokens, .{}));
    for (0..4) |field| {
        var donors = [_]Geometry{ fixture_contract.donors[0], fixture_contract.donors[1] };
        switch (field) {
            0 => donors[0].layer = 1,
            1 => donors[0].window = 2,
            2 => donors[0].heads = 2,
            else => donors[0].dim = 2,
        }
        changed = fixture_contract;
        changed.donors = &donors;
        try std.testing.expectError(error.KvCheckpointLayoutMismatch, validate(fixture_state, changed, fixture_state.tokens, .{}));
    }
    changed = fixture_contract;
    changed.identity.weights[0] ^= 1;
    try std.testing.expectError(error.KvCheckpointModelMismatch, validate(fixture_state, changed, fixture_state.tokens, .{}));
}

test "resume rejects prefix rewrites invalid appended IDs and both length limits" {
    try std.testing.expectError(error.KvCheckpointPrefixMismatch, validate(fixture_state, fixture_contract, &.{ 0, 1, 3, 2, 0 }, .{}));
    try std.testing.expectError(error.KvCheckpointAppendLimit, validate(fixture_state, fixture_contract, &.{ 0, 1, 2 }, .{}));
    try std.testing.expectError(error.InvalidKvCheckpoint, validate(fixture_state, fixture_contract, &.{ 0, 1, 2, 3, 4 }, .{}));
    try std.testing.expectError(error.KvCheckpointAppendLimit, validate(fixture_state, fixture_contract, &.{ 0, 1, 2, 3, 0, 0, 0, 0, 0 }, .{}));
    var changed = fixture_contract;
    changed.max_logical_tokens = 4;
    try std.testing.expectError(error.KvCheckpointAppendLimit, validate(fixture_state, changed, &.{ 0, 1, 2, 3, 0 }, .{}));
}

test "resume validation honors cancellation before returning the trusted boundary" {
    const Cancel = struct {
        calls: usize = 0,
        at: usize = std.math.maxInt(usize),
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            if (self.calls == self.at) return error.CompactionCancelled;
        }
    };
    var counter: Cancel = .{};
    try std.testing.expectEqual(@as(usize, 4), try validateWithControl(fixture_state, fixture_contract, fixture_state.tokens, .{}, .{ .context = &counter, .check_fn = Cancel.check }));
    for (1..counter.calls + 1) |at| {
        var cancel: Cancel = .{ .at = at };
        try std.testing.expectError(error.CompactionCancelled, validateWithControl(fixture_state, fixture_contract, fixture_state.tokens, .{}, .{ .context = &cancel, .check_fn = Cancel.check }));
    }
}

test "resume admits exactly 8192 appended prompt tokens independently of decode allowance" {
    var contract = fixture_contract;
    contract.max_logical_tokens = checkpoint.max_context_tokens;
    contract.max_appended_tokens = 8192;
    var tokens: [4 + 8192 + 1]u32 = @splat(0);
    @memcpy(tokens[0..4], fixture_state.tokens);
    try std.testing.expectEqual(@as(usize, 4), try validate(fixture_state, contract, tokens[0 .. 4 + 8192], .{}));
    try std.testing.expectError(error.KvCheckpointAppendLimit, validate(fixture_state, contract, &tokens, .{}));
}

test "resume rejects a model contract above the shared native context ceiling" {
    var contract = fixture_contract;
    contract.max_logical_tokens = checkpoint.max_context_tokens + 1;
    try std.testing.expectError(error.InvalidKvResumeContract, validate(fixture_state, contract, fixture_state.tokens, checkpoint.serving_limits));
}
