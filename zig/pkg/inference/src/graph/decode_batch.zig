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

//! Runtime-owned batch metadata: task adapters supply typed state/geometry.
//! Uniform, mixed prefill/decode and packed queries share one assembly path.
const std = @import("std");
const gpt_arch = @import("../architectures/gpt.zig");
const state_runtime = @import("decode_state_runtime.zig");
const NativeDecodeState = state_runtime.NativeDecodeState;
const BorrowedDecodeStateRuntime = state_runtime.BorrowedDecodeStateRuntime;

pub const OwnedContext = struct {
    allocator: std.mem.Allocator,
    kv_batch: ?[]gpt_arch.DecodeContext.KvBatchView = null,
    mrope_positions: ?[]u32 = null,
    qwen35_linear_batch: ?[]?*gpt_arch.Qwen35LinearCache = null,
    context: gpt_arch.DecodeContext,

    pub fn deinit(self: *OwnedContext) void {
        if (self.kv_batch) |batch| self.allocator.free(batch);
        if (self.mrope_positions) |positions| self.allocator.free(positions);
        if (self.qwen35_linear_batch) |caches| self.allocator.free(caches);
        self.kv_batch = null;
        self.mrope_positions = null;
        self.qwen35_linear_batch = null;
    }
};

pub const Item = struct {
    state: *NativeDecodeState,
    total_sequence_len: usize,
    query_sequence_len: usize,
    kv_sequence_len: usize,
    kv_position_offset: usize,
    attention_mode: gpt_arch.DecodeContext.AttentionMode,
};

pub fn buildUniform(allocator: std.mem.Allocator, states: []const *NativeDecodeState, seq_len: usize, query_seq_len: usize) !OwnedContext {
    return assemble(false, allocator, states, seq_len, query_seq_len, null, null);
}

pub fn buildMixed(allocator: std.mem.Allocator, items: []const Item) !OwnedContext {
    return buildLayout(allocator, items, null, null);
}

pub fn buildWithPositions(allocator: std.mem.Allocator, items: []const Item, positions: ?[]const u32) !OwnedContext {
    return buildLayout(allocator, items, positions, null);
}

pub fn buildLayout(allocator: std.mem.Allocator, items: []const Item, positions: ?[]const u32, packed_lengths: ?[]const usize) !OwnedContext {
    return assemble(true, allocator, items, 0, 0, positions, packed_lengths);
}

/// Wide decode cohorts retain explicit axis-major position metadata. Lengths
/// are borrowed until the owned context retires; positions are copied here.
pub fn buildPackedDecode(allocator: std.mem.Allocator, items: []const Item, lengths: []const usize) !OwnedContext {
    if (items.len > gpt_arch.max_packed_qwen_items or lengths.len != items.len) return error.InvalidPackedQueryShape;
    for (items, lengths) |item, len| if (len != 1 or item.query_sequence_len != 1) return error.InvalidPackedQueryShape;
    if (items.len <= 64) return buildMixed(allocator, items);
    var positions: [3 * gpt_arch.max_packed_qwen_items]u32 = undefined;
    for (items, 0..) |item, row| {
        var decode = BorrowedDecodeStateRuntime.init(item.state);
        const context = decode.makeDecodeContext(item.total_sequence_len, 1);
        const axes = context.mrope_positions orelse return error.Qwen3VlMropePositionsRequired;
        for (0..3) |axis| positions[axis * items.len + row] = axes[axis];
    }
    return buildLayout(allocator, items, positions[0 .. 3 * items.len], lengths);
}

fn Source(comptime mixed: bool) type {
    return if (mixed) []const Item else []const *NativeDecodeState;
}

fn stateAt(comptime mixed: bool, source: Source(mixed), index: usize) *NativeDecodeState {
    return if (mixed) source[index].state else source[index];
}

fn contextAt(comptime mixed: bool, source: Source(mixed), index: usize, seq_len: usize, query_seq_len: usize) gpt_arch.DecodeContext {
    var cursor = BorrowedDecodeStateRuntime.init(stateAt(mixed, source, index));
    return if (mixed) cursor.makeDecodeContext(source[index].total_sequence_len, source[index].query_sequence_len) else cursor.makeDecodeContext(seq_len, query_seq_len);
}

fn assemble(comptime mixed: bool, allocator: std.mem.Allocator, source: Source(mixed), seq_len: usize, query_seq_len: usize, full_positions: ?[]const u32, packed_lengths: ?[]const usize) !OwnedContext {
    if (source.len == 0) return error.EmptyBatch;
    if (packed_lengths) |lengths| {
        if (lengths.len != source.len or full_positions == null) return error.InvalidPackedQueryShape;
        _ = try gpt_arch.packedQueryRowCount(lengths);
        if (mixed) for (source, lengths) |item, len| if (item.query_sequence_len != len) return error.InvalidPackedQueryShape;
    }
    // Reject malformed packed geometry before borrowing any sequence state.
    const first = if (mixed) undefined else contextAt(false, source, 0, seq_len, query_seq_len);
    if (!mixed and (source.len == 1 or !first.usesPagedKv())) return .{ .allocator = allocator, .context = first };
    if (mixed and source.len == 1 and packed_lengths == null) {
        const item = source[0];
        var context = contextAt(true, source, 0, seq_len, query_seq_len);
        if (!context.usesPagedKv()) return error.MixedBatchDecodeModes;
        if (full_positions) |positions| {
            if (positions.len != try std.math.mul(usize, item.query_sequence_len, 3)) return error.InvalidQwen3VlMropeShape;
            context.mrope_positions = positions;
        }
        context.attention_mode = item.attention_mode;
        context.total_sequence_len = item.total_sequence_len;
        context.query_sequence_len = item.query_sequence_len;
        context.kv_sequence_len = item.kv_sequence_len;
        context.kv_position_offset = item.kv_position_offset;
        return .{ .allocator = allocator, .context = context };
    }

    var max_query: usize = if (mixed) 0 else first.query_sequence_len;
    var max_total: usize = if (mixed) 0 else first.total_sequence_len;
    var max_kv: usize = if (mixed) 0 else first.kv_sequence_len;
    var has_caches = if (mixed) false else first.qwen35_linear_cache != null;
    var has_positions = if (mixed) false else first.mrope_positions != null;
    if (mixed) for (source) |item| {
        max_query = @max(max_query, item.query_sequence_len);
        max_total = @max(max_total, item.total_sequence_len);
        max_kv = @max(max_kv, item.kv_sequence_len);
        has_caches = has_caches or item.state.qwen35_linear_cache != null;
        has_positions = has_positions or item.state.qwen3vl_mrope_position_delta != null;
    };
    const physical_rows = if (packed_lengths) |lengths| try gpt_arch.packedQueryRowCount(lengths) else try std.math.mul(usize, source.len, max_query);
    if (mixed and full_positions == null and has_positions and max_query != 1) return error.Qwen3VlMixedBatchMropeUnsupported;
    if (full_positions) |positions| if (positions.len != try std.math.mul(usize, physical_rows, 3)) return error.InvalidQwen3VlMropeShape;
    const caches: ?[]?*gpt_arch.Qwen35LinearCache = if (has_caches) try allocator.alloc(?*gpt_arch.Qwen35LinearCache, source.len) else null;
    errdefer if (caches) |owned| allocator.free(owned);
    const batch = try allocator.alloc(gpt_arch.DecodeContext.KvBatchView, source.len);
    errdefer allocator.free(batch);
    const positions: ?[]u32 = if (full_positions) |values| try allocator.dupe(u32, values) else if (has_positions) try allocator.alloc(u32, try std.math.mul(usize, source.len, 3)) else null;
    errdefer if (positions) |owned| allocator.free(owned);
    for (source, 0..) |_, index| {
        const context = contextAt(mixed, source, index, seq_len, query_seq_len);
        if (!context.usesPagedKv()) return error.MixedBatchDecodeModes;
        if (!mixed) {
            if (context.total_sequence_len != first.total_sequence_len or context.query_sequence_len != first.query_sequence_len or context.kv_sequence_len != first.kv_sequence_len or context.kv_position_offset != first.kv_position_offset or
                (context.qwen35_linear_cache != null) != (caches != null) or (context.mrope_positions != null) != (positions != null)) return error.IncompatibleBatchDecodeContext;
        }
        if (full_positions == null) if (positions) |owned| {
            const axes = context.mrope_positions orelse return error.Qwen3VlMropePositionsRequired;
            for (0..3) |axis| owned[axis * source.len + index] = axes[axis];
        };
        if (caches) |owned| owned[index] = context.qwen35_linear_cache;
        // Copy the complete physical KV view once for every layout. Task
        // adapters never reconstruct pool, ring or storage ownership metadata.
        batch[index] = .{
            .kv_cache = context.kv_cache.?,
            .kv_manager = context.kv_manager.?,
            .kv_storage = context.kv_storage orelse context.kv_cache.?.kv_storage,
        };
        if (mixed) {
            const item = source[index];
            batch[index].per_item_query_len = item.query_sequence_len;
            batch[index].per_item_total_len = item.total_sequence_len;
            batch[index].per_item_kv_len = item.kv_sequence_len;
            batch[index].per_item_kv_position_offset = item.kv_position_offset;
            batch[index].per_item_mode = switch (item.attention_mode) {
                .full_recompute => .dense_causal,
                .paged_prefill => .paged_prefill,
                .paged_decode => .paged_decode,
            };
        }
    }
    return .{
        .allocator = allocator,
        .kv_batch = batch,
        .qwen35_linear_batch = caches,
        .mrope_positions = positions,
        .context = .{
            .qwen35_linear_batch = caches,
            .mrope_positions = positions,
            .attention_mode = if (mixed) .paged_prefill else first.attention_mode,
            .total_sequence_len = max_total,
            .query_sequence_len = if (packed_lengths != null) physical_rows else max_query,
            .packed_query_lengths = packed_lengths,
            .kv_sequence_len = max_kv,
            .kv_position_offset = if (mixed) 0 else first.kv_position_offset,
            .kv_storage = if (mixed) source[0].state.kv_storage else first.kv_storage,
            .kv_batch = batch,
            .moe_runtime = if (mixed) &source[0].state.moe_runtime else first.moe_runtime,
        },
    };
}

test "decode batch metadata unwinds allocations without retiring borrowed KV or recurrent state" {
    const a = std.testing.allocator;
    const runtime = @import("../runtime/root.zig");
    var manager = runtime.kv.manager.KvManager.init(a);
    defer manager.deinit();
    const pool = try manager.addPool(.{ .backend = .native, .dtype = .f32, .page_size_tokens = 4, .num_layers_packed = 1, .num_kv_heads = 1, .head_dim = 2 });
    const config = @import("../models/gpt.zig").Config{ .family = .qwen3_5, .hidden_size = 4, .num_hidden_layers = 1, .qwen35_has_linear_attention = true, .qwen35_full_attention_interval = 4, .qwen35_linear_num_key_heads = 1, .qwen35_linear_num_value_heads = 1, .qwen35_linear_key_head_dim = 2, .qwen35_linear_value_head_dim = 2, .qwen35_linear_conv_kernel_dim = 4 };
    var first = NativeDecodeState.initPaged(a, &manager, pool, null);
    defer first.deinit();
    var second = NativeDecodeState.initPaged(a, &manager, pool, null);
    defer second.deinit();
    try first.ensureQwen35LinearCache(config);
    try second.ensureQwen35LinearCache(config);
    try first.notePrefill(6);
    try second.notePrefill(6);
    first.qwen3vl_mrope_position_delta = -3;
    second.qwen3vl_mrope_position_delta = -2;
    second.qwen35_linear_cache.?.layers[0].recurrent[0] = 99;
    const Check = struct {
        fn run(allocator: std.mem.Allocator, states: []const *NativeDecodeState) !void {
            var uniform = try buildUniform(allocator, states, 7, 1);
            defer uniform.deinit();
            const items = [_]Item{
                .{ .state = states[0], .total_sequence_len = 7, .query_sequence_len = 1, .kv_sequence_len = 6, .kv_position_offset = 0, .attention_mode = .paged_decode },
                .{ .state = states[1], .total_sequence_len = 7, .query_sequence_len = 1, .kv_sequence_len = 6, .kv_position_offset = 0, .attention_mode = .paged_decode },
            };
            var positions = [_]u32{ 1, 2, 3, 4, 5, 6 };
            var packed_context = try buildLayout(allocator, &items, &positions, &.{ 1, 1 });
            defer packed_context.deinit();
            positions[0] = 999;
            try std.testing.expectEqual(@as(u32, 1), packed_context.context.mrope_positions.?[0]);
            try std.testing.expectEqual(states[1].sequence_id.?, packed_context.context.kv_batch.?[1].kv_cache.sequence_id);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Check.run, .{@as([]const *NativeDecodeState, &.{ &first, &second })});
    try std.testing.expectEqual(@as(f32, 99), second.qwen35_linear_cache.?.layers[0].recurrent[0]);
    try first.appendGeneratedToken();
    try std.testing.expectEqual(@as(usize, 7), first.total_tokens);
}

test "decode batch rejects packed geometry before borrowing sequence state" {
    const item: Item = .{ .state = undefined, .total_sequence_len = 1, .query_sequence_len = 1, .kv_sequence_len = 0, .kv_position_offset = 0, .attention_mode = .paged_prefill };
    try std.testing.expectError(error.InvalidPackedQueryShape, buildLayout(std.testing.failing_allocator, &.{ item, item }, &.{ 0, 0, 0, 0, 0, 0 }, &.{ 2, 2 }));
    try std.testing.expectError(error.InvalidPackedQueryShape, buildLayout(std.testing.failing_allocator, &.{ item, item }, null, &.{ 1, 1 }));
}
