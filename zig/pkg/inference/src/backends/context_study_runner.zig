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

//! Bounded private study generation. The coordinator supplies a private replay
//! hook and provenance-validated prefix tokens. This helper owns no serving
//! sampler, grammar, callback, usage counter, or output stream.
const std = @import("std");
const context = @import("../runtime/kv/context_session.zig");
const control_mod = @import("../execution_control.zig");

pub const max_prefill_tokens: u32 = 8192;
pub const max_response_tokens: u32 = 8192;

pub const Forward = struct {
    ctx: *anyopaque,
    /// Append to the supplied private branch and return only the final row's
    /// logits, allocated by the provided allocator. No KV/query readback. The
    /// adapter must apply the run's execution control to native calls through
    /// its context; checks around a native call cannot interrupt that call.
    logits_fn: *const fn (*anyopaque, std.mem.Allocator, context.Hook, u32, []const i64) anyerror![]f32,
    hidden_fn: *const fn (*anyopaque, std.mem.Allocator, context.Hook, u32, []const i64) anyerror!void,
    /// Qualification-only prepared qLen=1 route. A null result is an explicit
    /// refusal and must never fall back after the private session may have
    /// been mutated. The callback returns the strict greedy token for the
    /// supplied input token. `require_output` is false only for the final
    /// append, whose next-token value is deliberately ignored like the legacy
    /// hidden-only path; every consumed result is range checked.
    prepared_token_fn: ?*const fn (*anyopaque, std.mem.Allocator, context.Hook, u32, i64, bool) anyerror!?i64 = null,
};

pub const Receipt = struct {
    session_identity: u64,
    prefill_tokens: u32,
    generated_tokens: u32,
    generated_tokens_sha256: [32]u8,
    /// Qualification-only wall buckets. Production calls leave this null.
    timing: ?StudyTiming = null,
};

pub const StudyTiming = struct {
    /// Hidden-only prefix chunks, excluding the final prefix token.
    prefill_hidden_ns: u64,
    /// The final prefix-token forward that produces the first logits row.
    initial_logits_ns: u64,
    /// Fixed-length greedy loop, including the final hidden-only append or
    /// strict prepared singleton whose next-token result is ignored.
    generated_loop_ns: u64,
};

fn monotonicNow() ?u64 {
    var ts: std.posix.timespec = undefined;
    if (std.posix.errno(std.posix.system.clock_gettime(.MONOTONIC, &ts)) != .SUCCESS or ts.sec < 0 or ts.nsec < 0)
        return null;
    return std.math.add(u64, std.math.mul(u64, @intCast(ts.sec), std.time.ns_per_s) catch return null, @intCast(ts.nsec)) catch null;
}

fn elapsed(start: ?u64, end: ?u64) ?u64 {
    return std.math.sub(u64, end orelse return null, start orelse return null) catch null;
}

fn check(control: ?*const control_mod.InferenceExecutionControl) !void {
    if (control) |value| try value.check();
}

fn greedy(logits: []const f32, vocab_size: u32) !i64 {
    if (vocab_size == 0 or logits.len != vocab_size) return error.InvalidStudyLogits;
    var best: usize = 0;
    for (logits, 0..) |value, index| {
        if (!std.math.isFinite(value)) return error.NonFiniteStudyLogits;
        // Strict comparison preserves the lowest-token-id tie break.
        if (value > logits[best]) best = index;
    }
    return @intCast(best);
}

fn preparedToken(
    allocator: std.mem.Allocator,
    forward: Forward,
    hook: context.Hook,
    position: u32,
    token: i64,
    vocab_size: u32,
    require_output: bool,
) !i64 {
    const callback = forward.prepared_token_fn orelse return error.PreparedStudyRouteUnsupported;
    const result = (try callback(forward.ctx, allocator, hook, position, token, require_output)) orelse
        return error.PreparedStudyRouteUnsupported;
    if (require_output and (result < 0 or result >= vocab_size)) return error.InvalidStudyToken;
    return result;
}

/// A failed run may already have appended a prefix of the private branch. The
/// caller must discard that branch instead of sealing or reusing it. Every
/// logits slice successfully returned by `logits_fn` is owned by this helper
/// until it is freed or replaced. A prepared callback refusal is terminal and
/// never falls back to `logits_fn`.
pub fn run(
    allocator: std.mem.Allocator,
    forward: Forward,
    hook: context.Hook,
    prefix: []const i64,
    response_tokens: u32,
    vocab_size: u32,
    control: ?*const control_mod.InferenceExecutionControl,
) !Receipt {
    return runWithCapture(allocator, forward, hook, prefix, response_tokens, vocab_size, control, null);
}

/// Test-only bounded output observation of the identical native generation loop.
/// On failure no partial token sequence is returned as successful evidence.
pub fn runWithCapture(
    allocator: std.mem.Allocator,
    forward: Forward,
    hook: context.Hook,
    prefix: []const i64,
    response_tokens: u32,
    vocab_size: u32,
    control: ?*const control_mod.InferenceExecutionControl,
    output_tokens: ?[]i64,
) !Receipt {
    if (output_tokens) |tokens| {
        if (!@import("builtin").is_test or tokens.len != response_tokens)
            return error.InvalidStudyCapture;
        @memset(tokens, -1);
    }
    errdefer if (output_tokens) |tokens| @memset(tokens, -1);
    if (hook.identity == 0 or prefix.len == 0 or prefix.len > max_prefill_tokens or
        response_tokens == 0 or response_tokens > max_response_tokens or vocab_size == 0)
        return error.InvalidStudyGeneration;
    const prefill: u32 = @intCast(prefix.len);
    const start = std.math.cast(u32, hook.plan.prompt_tokens) orelse return error.InvalidStudyGeneration;
    const response_start = try std.math.add(u32, start, prefill);
    const end = try std.math.add(u32, response_start, response_tokens);
    if (end != hook.plan.logical_token_limit) return error.InvalidStudyGeneration;
    for (prefix) |token| if (token < 0 or token >= vocab_size) return error.InvalidStudyToken;
    try check(control);

    // Wall timing is deliberately restricted to the qualification capture
    // path. Clock failure removes timing evidence but never changes generation.
    const timing_start = if (output_tokens != null) monotonicNow() else null;

    // Hidden-only chunks avoid materializing vocabulary-sized logits for every
    // prefill row. Process the final prefix token separately to obtain one row.
    var cursor: u32 = 0;
    while (cursor + 1 < prefill) {
        try check(control);
        const remaining = prefill - cursor - 1;
        const rows = try hook.nextChunk(start + cursor, remaining);
        if (rows == 0 or rows > remaining) return error.InvalidStudyChunk;
        try check(control);
        try forward.hidden_fn(forward.ctx, allocator, hook, start + cursor, prefix[cursor..][0..rows]);
        cursor += rows;
    }
    const hidden_end = if (output_tokens != null) monotonicNow() else null;
    if (try hook.nextChunk(response_start - 1, 1) != 1) return error.InvalidStudyChunk;
    try check(control);
    const use_prepared = forward.prepared_token_fn != null;
    var logits: ?[]f32 = if (use_prepared)
        null
    else
        try forward.logits_fn(forward.ctx, allocator, hook, response_start - 1, prefix[prefill - 1 ..]);
    var next_prepared_token: i64 = if (use_prepared)
        try preparedToken(allocator, forward, hook, response_start - 1, prefix[prefill - 1], vocab_size, true)
    else
        -1;
    const logits_end = if (output_tokens != null) monotonicNow() else null;
    defer if (logits) |owned| allocator.free(owned);
    var digest = std.crypto.hash.sha2.Sha256.init(.{});
    for (0..response_tokens) |index| {
        try check(control);
        const token = if (use_prepared)
            next_prepared_token
        else
            try greedy(logits.?, vocab_size);
        var encoded: [8]u8 = undefined;
        std.mem.writeInt(i64, &encoded, token, .little);
        digest.update(&encoded);
        const position = response_start + @as(u32, @intCast(index));
        if (try hook.nextChunk(position, 1) != 1) return error.InvalidStudyChunk;
        try check(control);
        if (use_prepared) {
            // Execute the final emitted token as well so the sealed response
            // source contains exactly response_tokens + 1 prepared singleton
            // forwards. Its next-token result is ignored, matching the legacy
            // final hidden-only append; only consumed outputs are validated.
            const require_output = index + 1 < response_tokens;
            const next = try preparedToken(allocator, forward, hook, position, token, vocab_size, require_output);
            if (index + 1 < response_tokens) next_prepared_token = next;
        } else if (index + 1 == response_tokens) {
            // The final emitted token must still pass through the model so its
            // query is part of the sealed generated-response source.
            try forward.hidden_fn(forward.ctx, allocator, hook, position, &.{token});
        } else {
            const next = try forward.logits_fn(forward.ctx, allocator, hook, position, &.{token});
            allocator.free(logits.?);
            logits = next;
        }
        if (output_tokens) |tokens| tokens[index] = token;
    }
    try check(control);
    const generated_end = if (output_tokens != null) monotonicNow() else null;
    var result: [32]u8 = undefined;
    digest.final(&result);
    const timing: ?StudyTiming = if (output_tokens != null) blk: {
        break :blk .{
            .prefill_hidden_ns = elapsed(timing_start, hidden_end) orelse break :blk null,
            .initial_logits_ns = elapsed(hidden_end, logits_end) orelse break :blk null,
            .generated_loop_ns = elapsed(logits_end, generated_end) orelse break :blk null,
        };
    } else null;
    return .{ .session_identity = hook.identity, .prefill_tokens = prefill, .generated_tokens = response_tokens, .generated_tokens_sha256 = result, .timing = timing };
}

test "context private study greedy is finite and deterministic" {
    try std.testing.expectEqual(@as(i64, 1), try greedy(&.{ 0, 2, 2 }, 3));
    try std.testing.expectError(error.InvalidStudyLogits, greedy(&.{0}, 3));
    try std.testing.expectError(error.NonFiniteStudyLogits, greedy(&.{ 0, std.math.nan(f32) }, 2));
    try std.testing.expectError(error.NonFiniteStudyLogits, greedy(&.{ 0, std.math.inf(f32) }, 2));
}

const TestCall = struct {
    kind: enum { next_chunk, hidden, logits, prepared_token },
    position: u32,
    count: u32,
    first_token: i64 = -1,
    requires_output: bool = false,
};

const TestState = struct {
    calls: [640]TestCall = undefined,
    call_count: usize = 0,
    forward_count: usize = 0,
    logits_count: usize = 0,
    max_chunk: u32 = 2,
    bad_chunk_at: ?usize = null,
    fail_forward_at: ?usize = null,
    nonfinite_logits_at: ?usize = null,
    prepared_count: usize = 0,
    prepared_null_at: ?usize = null,

    fn record(self: *@This(), call: TestCall) void {
        self.calls[self.call_count] = call;
        self.call_count += 1;
    }

    fn nextChunk(raw: *anyopaque, identity: u64, start: u32, requested: u32) !u32 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (identity != 77) return error.WrongStudyIdentity;
        self.record(.{ .kind = .next_chunk, .position = start, .count = requested });
        if (self.bad_chunk_at) |index| if (index == self.call_count - 1) return 0;
        return @min(requested, self.max_chunk);
    }

    fn attention(_: *anyopaque, _: u64, _: context.Attention, _: context.DeviceRef, _: context.DeviceRef, _: context.DeviceRef, _: context.DeviceRef) !void {
        return error.UnexpectedStudyAttention;
    }

    fn beforeForward(self: *@This()) !usize {
        const index = self.forward_count;
        self.forward_count += 1;
        if (self.fail_forward_at) |fail_at| if (fail_at == index) return error.InjectedStudyForwardFailure;
        return index;
    }

    fn hidden(raw: *anyopaque, _: std.mem.Allocator, session_hook: context.Hook, position: u32, tokens: []const i64) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (session_hook.identity != 77 or tokens.len == 0) return error.InvalidFakeStudyForward;
        _ = try self.beforeForward();
        self.record(.{ .kind = .hidden, .position = position, .count = @intCast(tokens.len), .first_token = tokens[0] });
    }

    fn logits(raw: *anyopaque, allocator: std.mem.Allocator, session_hook: context.Hook, position: u32, tokens: []const i64) ![]f32 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (session_hook.identity != 77 or tokens.len == 0) return error.InvalidFakeStudyForward;
        _ = try self.beforeForward();
        const index = self.logits_count;
        self.logits_count += 1;
        self.record(.{ .kind = .logits, .position = position, .count = @intCast(tokens.len), .first_token = tokens[0] });
        const result = try allocator.alloc(f32, 4);
        const fixtures = [_][4]f32{
            .{ 0, 5, 5, 1 },
            .{ 0, 0, 9, 9 },
            .{ 7, 6, 5, 4 },
        };
        @memcpy(result, &fixtures[@min(index, fixtures.len - 1)]);
        if (self.nonfinite_logits_at) |bad_at| {
            if (bad_at == index) result[0] = std.math.nan(f32);
        }
        return result;
    }

    fn preparedTokenFn(raw: *anyopaque, _: std.mem.Allocator, session_hook: context.Hook, position: u32, token: i64, require_output: bool) !?i64 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (session_hook.identity != 77) return error.InvalidFakeStudyForward;
        _ = try self.beforeForward();
        const index = self.prepared_count;
        self.prepared_count += 1;
        self.record(.{ .kind = .prepared_token, .position = position, .count = 1, .first_token = token, .requires_output = require_output });
        if (self.prepared_null_at) |null_at| if (null_at == index) return null;
        if (!require_output) return 99;
        return ([_]i64{ 1, 2, 0, 3 })[@min(index, 3)];
    }

    fn hook(self: *@This(), logical_limit: usize) context.Hook {
        return .{
            .ctx = self,
            .identity = 77,
            .plan = .{
                .policy = .{},
                .context_limit = logical_limit,
                .prompt_tokens = 10,
                .logical_token_limit = logical_limit,
                .retained_row_limit = logical_limit,
            },
            .vtable = &.{ .next_chunk = nextChunk, .attention = attention },
        };
    }

    fn forward(self: *@This()) Forward {
        return .{ .ctx = self, .logits_fn = logits, .hidden_fn = hidden };
    }

    fn preparedForward(self: *@This()) Forward {
        return .{ .ctx = self, .logits_fn = logits, .hidden_fn = hidden, .prepared_token_fn = preparedTokenFn };
    }
};

test "context private study runner records positions hash and final append" {
    var state = TestState{};
    const receipt = try run(std.testing.allocator, state.forward(), state.hook(18), &.{ 0, 1, 2, 3, 0 }, 3, 4, null);
    try std.testing.expectEqual(@as(u64, 77), receipt.session_identity);
    try std.testing.expectEqual(@as(u32, 5), receipt.prefill_tokens);
    try std.testing.expectEqual(@as(u32, 3), receipt.generated_tokens);

    var digest = std.crypto.hash.sha2.Sha256.init(.{});
    for ([_]i64{ 1, 2, 0 }) |token| {
        var encoded: [8]u8 = undefined;
        std.mem.writeInt(i64, &encoded, token, .little);
        digest.update(&encoded);
    }
    var expected: [32]u8 = undefined;
    digest.final(&expected);
    try std.testing.expectEqualSlices(u8, &expected, &receipt.generated_tokens_sha256);

    const expected_calls = [_]TestCall{
        .{ .kind = .next_chunk, .position = 10, .count = 4 },
        .{ .kind = .hidden, .position = 10, .count = 2, .first_token = 0 },
        .{ .kind = .next_chunk, .position = 12, .count = 2 },
        .{ .kind = .hidden, .position = 12, .count = 2, .first_token = 2 },
        .{ .kind = .next_chunk, .position = 14, .count = 1 },
        .{ .kind = .logits, .position = 14, .count = 1, .first_token = 0 },
        .{ .kind = .next_chunk, .position = 15, .count = 1 },
        .{ .kind = .logits, .position = 15, .count = 1, .first_token = 1 },
        .{ .kind = .next_chunk, .position = 16, .count = 1 },
        .{ .kind = .logits, .position = 16, .count = 1, .first_token = 2 },
        .{ .kind = .next_chunk, .position = 17, .count = 1 },
        .{ .kind = .hidden, .position = 17, .count = 1, .first_token = 0 },
    };
    try std.testing.expectEqualSlices(TestCall, &expected_calls, state.calls[0..state.call_count]);
}

test "context private study prepared route executes final token and never calls logits" {
    var state = TestState{};
    var tokens: [3]i64 = undefined;
    const receipt = try runWithCapture(
        std.testing.allocator,
        state.preparedForward(),
        state.hook(18),
        &.{ 0, 1, 2, 3, 0 },
        3,
        4,
        null,
        &tokens,
    );
    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 0 }, &tokens);
    try std.testing.expectEqual(@as(usize, 0), state.logits_count);
    try std.testing.expectEqual(@as(usize, 4), state.prepared_count);
    try std.testing.expectEqual(@as(u32, 3), receipt.generated_tokens);
    const expected_tail = [_]TestCall{
        .{ .kind = .next_chunk, .position = 14, .count = 1 },
        .{ .kind = .prepared_token, .position = 14, .count = 1, .first_token = 0, .requires_output = true },
        .{ .kind = .next_chunk, .position = 15, .count = 1 },
        .{ .kind = .prepared_token, .position = 15, .count = 1, .first_token = 1, .requires_output = true },
        .{ .kind = .next_chunk, .position = 16, .count = 1 },
        .{ .kind = .prepared_token, .position = 16, .count = 1, .first_token = 2, .requires_output = true },
        .{ .kind = .next_chunk, .position = 17, .count = 1 },
        .{ .kind = .prepared_token, .position = 17, .count = 1, .first_token = 0 },
    };
    try std.testing.expectEqualSlices(TestCall, &expected_tail, state.calls[state.call_count - expected_tail.len .. state.call_count]);
}

test "context private study prepared route refuses null without logits fallback" {
    var state = TestState{ .prepared_null_at = 2 };
    var tokens: [3]i64 = undefined;
    try std.testing.expectError(error.PreparedStudyRouteUnsupported, runWithCapture(
        std.testing.allocator,
        state.preparedForward(),
        state.hook(18),
        &.{ 0, 1, 2, 3, 0 },
        3,
        4,
        null,
        &tokens,
    ));
    try std.testing.expectEqual(@as(usize, 0), state.logits_count);
    try std.testing.expectEqual(@as(usize, 3), state.prepared_count);
    try std.testing.expectEqualSlices(i64, &.{ -1, -1, -1 }, &tokens);
}

test "context private study prepared route performs response plus one singleton forwards" {
    var state = TestState{};
    var tokens: [256]i64 = undefined;
    const receipt = try runWithCapture(
        std.testing.allocator,
        state.preparedForward(),
        state.hook(271),
        &.{ 0, 1, 2, 3, 0 },
        256,
        4,
        null,
        &tokens,
    );
    try std.testing.expectEqual(@as(u32, 256), receipt.generated_tokens);
    try std.testing.expectEqual(@as(usize, 257), state.prepared_count);
    try std.testing.expectEqual(@as(usize, 0), state.logits_count);
    try std.testing.expectEqual(TestCall{ .kind = .prepared_token, .position = 270, .count = 1, .first_token = 3 }, state.calls[state.call_count - 1]);
}

test "context private study runner fails closed on forward error and invalid chunk" {
    var forward_failure = TestState{ .fail_forward_at = 3 };
    try std.testing.expectError(error.InjectedStudyForwardFailure, run(
        std.testing.allocator,
        forward_failure.forward(),
        forward_failure.hook(18),
        &.{ 0, 1, 2, 3, 0 },
        3,
        4,
        null,
    ));

    var chunk_failure = TestState{ .bad_chunk_at = 2 };
    try std.testing.expectError(error.InvalidStudyChunk, run(
        std.testing.allocator,
        chunk_failure.forward(),
        chunk_failure.hook(18),
        &.{ 0, 1, 2, 3, 0 },
        3,
        4,
        null,
    ));
}

test "context private study runner propagates cancellation between reservation and forward" {
    const Cancel = struct {
        checks: usize = 0,
        cancel_at: usize,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.checks += 1;
            if (self.checks == self.cancel_at) return error.Cancelled;
        }
    };
    var state = TestState{};
    var cancel = Cancel{ .cancel_at = 5 };
    const control = control_mod.InferenceExecutionControl{ .ptr = &cancel, .check_fn = Cancel.check };
    try std.testing.expectError(error.Cancelled, run(
        std.testing.allocator,
        state.forward(),
        state.hook(18),
        &.{ 0, 1, 2, 3, 0 },
        3,
        4,
        &control,
    ));
    // The second chunk was reserved, but cancellation prevented its forward.
    try std.testing.expectEqual(@as(usize, 1), state.forward_count);
    try std.testing.expectEqual(TestCall{ .kind = .next_chunk, .position = 12, .count = 2 }, state.calls[state.call_count - 1]);
}

test "context private study runner frees live logits on allocation failure" {
    var state = TestState{};
    // Initial logits allocation succeeds; the next autoregressive allocation
    // fails. The runner must still release the initial live allocation.
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 1 });
    try std.testing.expectError(error.OutOfMemory, run(
        failing.allocator(),
        state.forward(),
        state.hook(18),
        &.{ 0, 1, 2, 3, 0 },
        3,
        4,
        null,
    ));
    try std.testing.expect(failing.has_induced_failure);
    try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
}

test "context private study runner rejects nonfinite forward output" {
    var state = TestState{ .nonfinite_logits_at = 1 };
    try std.testing.expectError(error.NonFiniteStudyLogits, run(
        std.testing.allocator,
        state.forward(),
        state.hook(18),
        &.{ 0, 1, 2, 3, 0 },
        3,
        4,
        null,
    ));
}

test "context private study captured output matches normal execution and clears failed evidence" {
    const a = std.testing.allocator;
    var ordinary = TestState{};
    const expected = try run(a, ordinary.forward(), ordinary.hook(18), &.{ 0, 1, 2, 3, 0 }, 3, 4, null);
    var captured = TestState{};
    var tokens: [3]i64 = undefined;
    const actual = try runWithCapture(a, captured.forward(), captured.hook(18), &.{ 0, 1, 2, 3, 0 }, 3, 4, null, &tokens);
    try std.testing.expectEqual(expected.session_identity, actual.session_identity);
    try std.testing.expectEqual(expected.prefill_tokens, actual.prefill_tokens);
    try std.testing.expectEqual(expected.generated_tokens, actual.generated_tokens);
    try std.testing.expectEqualSlices(u8, &expected.generated_tokens_sha256, &actual.generated_tokens_sha256);
    try std.testing.expect(expected.timing == null);
    const timing = actual.timing orelse return error.MissingStudyTiming;
    _ = try std.math.add(u64, try std.math.add(u64, timing.prefill_hidden_ns, timing.initial_logits_ns), timing.generated_loop_ns);
    try std.testing.expectEqualSlices(i64, &.{ 1, 2, 0 }, &tokens);
    try std.testing.expectEqualSlices(TestCall, ordinary.calls[0..ordinary.call_count], captured.calls[0..captured.call_count]);
    var failed = TestState{ .fail_forward_at = 4 };
    try std.testing.expectError(error.InjectedStudyForwardFailure, runWithCapture(a, failed.forward(), failed.hook(18), &.{ 0, 1, 2, 3, 0 }, 3, 4, null, &tokens));
    try std.testing.expectEqualSlices(i64, &.{ -1, -1, -1 }, &tokens);
    var invalid = TestState{};
    try std.testing.expectError(error.InvalidStudyCapture, runWithCapture(a, invalid.forward(), invalid.hook(18), &.{ 0, 1, 2, 3, 0 }, 3, 4, null, tokens[0..2]));
    try std.testing.expectEqual(@as(usize, 0), invalid.call_count);
}
