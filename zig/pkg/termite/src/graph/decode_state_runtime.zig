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
const gpt_arch = @import("../architectures/gpt.zig");
const generation = @import("../pipelines/generation.zig");
const runtime = @import("../runtime/root.zig");

pub const DecodeStateRuntime = struct {
    state: generation.NativeDecodeState,

    pub fn initPaged(
        allocator: std.mem.Allocator,
        kv_manager: *runtime.kv.manager.KvManager,
        pool_id: runtime.kv.block.KvPoolId,
        shared_moe_cache: ?*runtime.moe.shared.SharedExpertCache,
    ) DecodeStateRuntime {
        return .{
            .state = generation.NativeDecodeState.initPaged(allocator, kv_manager, pool_id, shared_moe_cache),
        };
    }

    pub fn deinit(self: *DecodeStateRuntime) void {
        self.state.deinit();
    }

    pub fn currentTokenCount(self: *const DecodeStateRuntime) usize {
        return self.state.total_tokens;
    }

    pub fn kvView(self: *const DecodeStateRuntime) ?generation.KvView {
        return self.state.kvView();
    }

    pub fn notePrefill(self: *DecodeStateRuntime, token_count: usize) !void {
        try self.state.notePrefill(token_count);
    }

    pub fn appendPrefillChunk(self: *DecodeStateRuntime, token_count: usize) !void {
        try self.state.appendPrefillChunk(token_count);
    }

    pub fn appendGeneratedToken(self: *DecodeStateRuntime) !usize {
        try self.state.appendGeneratedToken();
        return self.currentTokenCount();
    }

    pub fn appendGeneratedTokens(self: *DecodeStateRuntime, count: usize) !usize {
        try self.state.appendGeneratedTokens(count);
        return self.currentTokenCount();
    }

    pub fn truncateGeneratedTokens(self: *DecodeStateRuntime, count: usize) !void {
        try self.state.truncateTokens(count);
    }

    pub fn compactKvCache(self: *DecodeStateRuntime, config: runtime.kv.compaction.CompactionConfig) !void {
        _ = try self.state.compactKvCache(config);
    }

    pub fn validateDecodePosition(self: *const DecodeStateRuntime, position: usize) !void {
        if (self.currentTokenCount() != position) return error.InvalidDecodePosition;
    }

    pub fn makeDecodeContext(
        self: *DecodeStateRuntime,
        seq_len: usize,
        query_seq_len: usize,
        attention_mode: cache_mod.AttentionMode,
    ) gpt_arch.DecodeContext {
        var decode_context = self.state.gptDecodeContext(seq_len, query_seq_len);
        if (attention_mode == .full_recompute) decode_context.attention_mode = .full_recompute;
        return decode_context;
    }

    pub fn preparePrefill(
        self: *DecodeStateRuntime,
        seq_len: usize,
        query_seq_len: usize,
        attention_mode: cache_mod.AttentionMode,
    ) !gpt_arch.DecodeContext {
        if (self.currentTokenCount() == 0) {
            if (seq_len != query_seq_len) return error.UnsupportedShape;
            try self.notePrefill(query_seq_len);
        } else {
            const expected_prior = seq_len - query_seq_len;
            if (self.currentTokenCount() != expected_prior) return error.InvalidPrefillSequence;
            try self.appendPrefillChunk(query_seq_len);
        }
        return self.makeDecodeContext(seq_len, query_seq_len, attention_mode);
    }

    pub fn beginDecodeStep(
        self: *DecodeStateRuntime,
        position: usize,
        attention_mode: cache_mod.AttentionMode,
    ) !struct {
        seq_len: usize,
        decode_context: gpt_arch.DecodeContext,
    } {
        try self.validateDecodePosition(position);
        const seq_len = try self.appendGeneratedToken();
        return .{
            .seq_len = seq_len,
            .decode_context = self.makeDecodeContext(seq_len, 1, attention_mode),
        };
    }
};

test "decode state runtime paged prefill and decode step" {
    const allocator = std.testing.allocator;
    var manager = runtime.kv.manager.KvManager.init(allocator);
    defer manager.deinit();

    const pool_id = try manager.addPool(.{
        .backend = .native,
        .dtype = .f16,
        .page_size_tokens = 16,
        .num_layers_packed = 2,
        .num_kv_heads = 2,
        .head_dim = 8,
        .sliding_window_size = null,
    });

    var ds = DecodeStateRuntime.initPaged(allocator, &manager, pool_id, null);
    defer ds.deinit();

    const prefill_ctx = try ds.preparePrefill(4, 4, .paged_prefill);
    try std.testing.expectEqual(@as(usize, 4), ds.currentTokenCount());
    try std.testing.expectEqual(@as(usize, 4), prefill_ctx.kv_sequence_len);

    const step = try ds.beginDecodeStep(4, .paged_decode);
    try std.testing.expectEqual(@as(usize, 5), step.seq_len);
    try std.testing.expectEqual(@as(usize, 1), step.decode_context.query_sequence_len);
}
