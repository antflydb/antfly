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

//! Prepared opaque whole-model candidates share request, output and commit
//! semantics while retaining each target's model executor and kernel schedules.
const std = @import("std");
const model_runtime = @import("../graph/model_runtime.zig");

pub const SessionExecutorOptions = struct {
    config: ?@import("../models/gpt.zig").Config = null,
    kv_dtype: ?@import("../runtime/kv/pool.zig").KvDType = null,
    shared_moe_cache: ?*@import("../runtime/moe/shared.zig").SharedExpertCache = null,
};

/// Existing specialized schedules remain ahead of the common live CPU path.
/// Selection belongs to execution, independently of serving publication.
pub fn prefersSpecializedSession(session: @import("../backends/backends.zig").Session) bool {
    return @import("build_options").enable_metal and session.backend() == .metal;
}

pub fn createSessionExecutor(allocator: std.mem.Allocator, session: @import("../backends/backends.zig").Session, options: SessionExecutorOptions) !?model_runtime.ModelExecutor {
    const config = options.config orelse @import("../architectures/session_factory.zig").getGptConfig(session) orelse return null;
    if (comptime @import("build_options").enable_metal) {
        const specialized = @import("../graph/metal_executor.zig");
        if (prefersSpecializedSession(session) and specialized.supportsSession(session))
            return try specialized.createModelExecutor(allocator, session, config, options.kv_dtype, options.shared_moe_cache);
    }
    const live = @import("../graph/live_model_executor.zig");
    if (!live.supportsSession(session)) return null;
    return try live.createModelExecutor(allocator, session, config, options.kv_dtype, options.shared_moe_cache);
}

pub const Candidate = struct {
    runtime: *model_runtime.ModelRuntime,
    pub fn prepare(self: Candidate, allocator: std.mem.Allocator, request: model_runtime.PrepareRequest) !bool {
        return self.runtime.prepare(allocator, request);
    }
    pub fn forward(
        self: Candidate,
        allocator: std.mem.Allocator,
        request: model_runtime.ForwardRequest,
    ) !?[]f32 {
        var result = (try self.output(allocator, request)) orelse return null;
        defer result.deinit(allocator);
        return try result.takeHostLogits(allocator);
    }

    pub fn output(
        self: Candidate,
        allocator: std.mem.Allocator,
        request: model_runtime.ForwardRequest,
    ) !?model_runtime.ModelOutput {
        switch (request) {
            inline else => |value| try value.check(),
        }
        if (!self.runtime.supportsForwardRequest(request, .logits)) return null;
        // Calling the runtime commits this attempt. An error may follow KV or
        // recurrent updates/submission, so it must never trigger another forward.
        const result = switch (request) {
            .prefill => |prefill_request| blk: {
                try resetRuntimeAtPrefillStart(self.runtime, prefill_request);
                break :blk try self.runtime.prefill(allocator, prefill_request);
            },
            .decode => |decode_request| try self.runtime.decode(allocator, decode_request),
        };
        return result;
    }

    pub fn greedy(
        self: Candidate,
        allocator: std.mem.Allocator,
        request: model_runtime.ForwardRequest,
        vocab_size: usize,
    ) !?i64 {
        return switch (request) {
            .prefill => blk: {
                var result = (try self.output(allocator, request)) orelse return null;
                defer result.deinit(allocator);
                break :blk try result.greedyToken(allocator, vocab_size);
            },
            .decode => |decode_request| blk: {
                try decode_request.check();
                if (!self.runtime.supportsForwardRequest(request, .greedy)) break :blk null;
                var selection = try self.runtime.decodeGreedy(allocator, decode_request);
                defer selection.deinit();
                break :blk selection.token_id;
            },
        };
    }

    pub fn sample(
        self: Candidate,
        allocator: std.mem.Allocator,
        request: model_runtime.SampledDecodeRequest,
    ) !?i64 {
        // This is the commit boundary. Returning null is allowed only before the
        // runtime submits a decode frame. Once decodeSample starts, every error is
        // propagated and the caller must not replay the token through another
        // forward path.
        if (!self.runtime.supportsSampleDecodeRequest(request)) return null;
        const sampled = try self.runtime.decodeSample(allocator, request);
        return sampled.token_id;
    }
};

fn resetRuntimeAtPrefillStart(runtime: *model_runtime.ModelRuntime, request: model_runtime.PrefillRequest) !void {
    // A fresh prompt starts at position zero. Later chunks must retain the
    // runtime-owned KV written by earlier chunks.
    if (request.seq_len == request.query_seq_len) {
        try request.check();
        try runtime.reset();
        try request.check();
    }
}

test "sampled runtime declines before submission when capability rejects request" {
    const Probe = struct {
        decode_calls: usize = 0,

        fn supports(_: *anyopaque, _: model_runtime.SampledDecodeRequest) bool {
            return false;
        }

        fn decode(ctx: *anyopaque, _: std.mem.Allocator, _: model_runtime.SampledDecodeRequest) !model_runtime.SampledDecodeOutput {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.decode_calls += 1;
            return .{ .token_id = 7 };
        }

        fn prefill(_: *anyopaque, _: std.mem.Allocator, _: model_runtime.PrefillRequest) !model_runtime.ModelOutput {
            return error.UnexpectedPrefill;
        }

        fn deinit(_: *anyopaque) void {}

        const vtable = model_runtime.ModelRuntime.VTable{
            .prefill = prefill,
            .decode_sample = decode,
            .supports_sample_decode_request = supports,
            .deinit = deinit,
        };
    };

    var probe = Probe{};
    var runtime = model_runtime.ModelRuntime{ .ptr = &probe, .vtable = &Probe.vtable };
    const result = try (Candidate{ .runtime = &runtime }).sample(std.testing.allocator, .{
        .decode = .{ .token_id = 1, .position = 1 },
        .sampling = .{ .temperature = 0.8, .top_p = 0.95 },
        .token_history = &.{1},
    });
    try std.testing.expectEqual(@as(?i64, null), result);
    try std.testing.expectEqual(@as(usize, 0), probe.decode_calls);
}

test "sampled runtime propagates post-commit error without replay" {
    const Probe = struct {
        decode_calls: usize = 0,

        fn supports(_: *anyopaque, _: model_runtime.SampledDecodeRequest) bool {
            return true;
        }

        fn decode(ctx: *anyopaque, _: std.mem.Allocator, _: model_runtime.SampledDecodeRequest) !model_runtime.SampledDecodeOutput {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.decode_calls += 1;
            return error.InjectedPostCommitFailure;
        }

        fn prefill(_: *anyopaque, _: std.mem.Allocator, _: model_runtime.PrefillRequest) !model_runtime.ModelOutput {
            return error.UnexpectedPrefill;
        }

        fn deinit(_: *anyopaque) void {}

        const vtable = model_runtime.ModelRuntime.VTable{
            .prefill = prefill,
            .decode_sample = decode,
            .supports_sample_decode_request = supports,
            .deinit = deinit,
        };
    };

    var probe = Probe{};
    var runtime = model_runtime.ModelRuntime{ .ptr = &probe, .vtable = &Probe.vtable };
    try std.testing.expectError(error.InjectedPostCommitFailure, (Candidate{ .runtime = &runtime }).sample(std.testing.allocator, .{
        .decode = .{ .token_id = 1, .position = 1 },
        .sampling = .{ .temperature = 0.8, .top_p = 0.95 },
        .token_history = &.{1},
    }));
    try std.testing.expectEqual(@as(usize, 1), probe.decode_calls);
}

test "opaque model candidate preserves chunked prefill state and releases discarded device tokens" {
    const ops = @import("../ops/ops.zig");
    const native = @import("../ops/native_compute.zig");
    const Probe = struct {
        resets: usize = 0,
        freed: usize = 0,
        backend: ops.ComputeBackend = undefined,
        fn reset(ctx: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.resets += 1;
        }
        fn prefill(_: *anyopaque, a: std.mem.Allocator, _: model_runtime.PrefillRequest) !model_runtime.ModelOutput {
            return .{ .logits = try a.dupe(f32, &.{ 0, 1 }) };
        }
        fn decode(ctx: *anyopaque, _: std.mem.Allocator, _: model_runtime.DecodeRequest) !model_runtime.GreedyDecodeOutput {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            return .{ .token_id = 1, .token_tensor = @ptrFromInt(4096), .token_tensor_backend = &self.backend };
        }
        fn free(ctx: *anyopaque, _: ops.CT) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.freed += 1;
        }
        fn deinit(_: *anyopaque) void {}
        const table = model_runtime.ModelRuntime.VTable{ .prefill = prefill, .decode_greedy = decode, .reset = reset, .deinit = deinit };
    };
    var probe = Probe{};
    var backend_table = native.vtable_impl;
    backend_table.freeTensor = Probe.free;
    probe.backend = .{ .ptr = &probe, .vtable = &backend_table };
    var runtime = model_runtime.ModelRuntime{ .ptr = &probe, .vtable = &Probe.table };
    const candidate = Candidate{ .runtime = &runtime };
    for ([_]usize{ 4, 8 }) |seq_len| {
        const logits = (try candidate.forward(std.testing.allocator, .{ .prefill = .{ .input_ids = &.{ 1, 2, 3, 4 }, .seq_len = seq_len, .query_seq_len = 4, .attention_mode = .paged_prefill } })).?;
        defer std.testing.allocator.free(logits);
        try std.testing.expectEqualSlices(f32, &.{ 0, 1 }, logits);
        try std.testing.expectEqual(@as(usize, 1), probe.resets);
    }
    try std.testing.expectEqual(@as(?i64, 1), try candidate.greedy(std.testing.allocator, .{ .decode = .{ .token_id = 1, .position = 8 } }, 2));
    try std.testing.expectEqual(@as(usize, 1), probe.freed);
}

test "opaque forward declines before reset and propagates post-commit fallback-shaped errors" {
    const Probe = struct {
        accepted: bool = false,
        calls: usize = 0,
        resets: usize = 0,
        fn supports(ctx: *anyopaque, _: model_runtime.ForwardRequest, _: model_runtime.ForwardOperation) bool {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            return self.accepted;
        }
        fn reset(ctx: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.resets += 1;
        }
        fn prefill(ctx: *anyopaque, _: std.mem.Allocator, _: model_runtime.PrefillRequest) !model_runtime.ModelOutput {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.calls += 1;
            return error.MissingValue;
        }
        fn decode(ctx: *anyopaque, _: std.mem.Allocator, _: model_runtime.DecodeRequest) !model_runtime.ModelOutput {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.calls += 1;
            return error.UnsupportedShape;
        }
        fn greedy(ctx: *anyopaque, _: std.mem.Allocator, _: model_runtime.DecodeRequest) !model_runtime.GreedyDecodeOutput {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.calls += 1;
            return error.UnsupportedGreedyDecode;
        }
        fn deinit(_: *anyopaque) void {}
        const table = model_runtime.ModelRuntime.VTable{ .prefill = prefill, .decode = decode, .decode_greedy = greedy, .supports_forward_request = supports, .reset = reset, .deinit = deinit };
    };
    var probe = Probe{};
    var runtime = model_runtime.ModelRuntime{ .ptr = &probe, .vtable = &Probe.table };
    const candidate = Candidate{ .runtime = &runtime };
    const prompt = model_runtime.ForwardRequest{ .prefill = .{ .input_ids = &.{1}, .seq_len = 1, .query_seq_len = 1, .attention_mode = .paged_prefill } };
    try std.testing.expect(try candidate.output(std.testing.allocator, prompt) == null);
    try std.testing.expectEqual(@as(usize, 0), probe.calls);
    try std.testing.expectEqual(@as(usize, 0), probe.resets);
    probe.accepted = true;
    try std.testing.expectError(error.MissingValue, candidate.output(std.testing.allocator, prompt));
    try std.testing.expectError(error.UnsupportedShape, candidate.output(std.testing.allocator, .{ .decode = .{ .token_id = 1, .position = 1 } }));
    try std.testing.expectError(error.UnsupportedGreedyDecode, candidate.greedy(std.testing.allocator, .{ .decode = .{ .token_id = 1, .position = 1 } }, 2));
    try std.testing.expectEqual(@as(usize, 3), probe.calls);
    try std.testing.expectEqual(@as(usize, 1), probe.resets);
}
