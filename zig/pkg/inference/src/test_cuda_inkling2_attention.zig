// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const std = @import("std");
const checkpoint = @import("architectures/inkling2_checkpoint.zig");
const attention = @import("architectures/inkling2_attention_cuda.zig");
const cache = @import("ops/cuda/hybrid_paged_kv.zig");
const relative = @import("ops/cuda/paged_relative_attention.zig");
const Context = @import("ops/cuda/context.zig").CudaContext;
const Blas = @import("ops/cuda/cublaslt.zig").CublasLt;
const buffer = @import("ops/cuda/buffer.zig");
const Staging = @import("ops/cuda/pinned_input_arena.zig").Arena;

test "CUDA Inkling 2 runs resident local and global attention from checkpoint weights" {
    const allocator = std.testing.allocator;
    const dir = @import("antfly_platform").env.getenv("ANTFLY_INKLING2_MODEL_DIR") orelse return error.Inkling2FixtureRequired;
    var source = try checkpoint.Checkpoint.open(allocator, dir);
    defer source.deinit();
    var ctx = try Context.initDefault();
    defer ctx.deinit();
    var blas = try Blas.open(&ctx);
    defer blas.deinit();
    var staging: Staging = .{};
    defer staging.deinit(&ctx);
    var forward: attention.Forward = .{};
    defer forward.deinit(&ctx);
    const text = source.config.value.text_config;
    var state: cache.Cache = undefined;
    try state.init(allocator, &ctx, .{
        .layout = .{ .page_tokens = 16, .window_tokens = text.sliding_window_size, .max_tokens = 32, .max_append_tokens = 2 },
        .full_slots = 1,
        .sliding_slots = 1,
        .kv_heads = text.num_key_value_heads,
        .head_dim = text.head_dim,
    });
    defer state.deinit() catch @panic("unable to drain Inkling attention cache");

    const rows = 2;
    const row_bytes = @as(usize, text.hidden_size) * 2;
    var host = try buffer.HostBuffer.alloc(&ctx, row_bytes * rows);
    defer host.free(&ctx);
    const embeddings = try source.reader.rawTensorBytes("model.llm.embed.weight");
    @memcpy(host.bytes(), embeddings[42 * row_bytes ..][0 .. rows * row_bytes]);
    var input = try buffer.DeviceBuffer.alloc(&ctx, host.len);
    defer input.free(&ctx);
    try input.copyFromHost(&ctx, host.constBytes());
    try ctx.synchronize();
    var output = try buffer.DeviceBuffer.alloc(&ctx, host.len);
    defer output.free(&ctx);
    var page_ids_host = try buffer.HostBuffer.alloc(&ctx, 8);
    defer page_ids_host.free(&ctx);
    var page_ids_device = try buffer.DeviceBuffer.alloc(&ctx, page_ids_host.len);
    defer page_ids_device.free(&ctx);
    var workspace = try attention.Workspace.init(&ctx, text, rows);
    defer workspace.deinit(&ctx);
    var blas_space = try buffer.DeviceBuffer.alloc(&ctx, 8 * 1024 * 1024);
    defer blas_space.free(&ctx);
    var attention_space = try buffer.DeviceBuffer.alloc(&ctx, try relative.scratchBytes(.{
        .rows = rows,
        .start = 0,
        .q_heads = text.num_attention_heads,
        .kv_heads = text.num_key_value_heads,
        .relative_extent = text.rel_extent,
        .score_scale = 1,
    }, 0, rows));
    defer attention_space.free(&ctx);
    try state.prepareAppend(rows);
    for ([_]usize{ 0, 5 }, [_]cache.Group{ .sliding, .full }) |number, group| {
        var layer = try attention.Layer.load(allocator, &ctx, &source, &staging, number);
        defer layer.deinit(&ctx);
        const history_bytes = 3 * @as(usize, text.num_key_value_heads) * text.head_dim * 2;
        var key_history = try buffer.DeviceBuffer.alloc(&ctx, history_bytes);
        defer key_history.free(&ctx);
        var value_history = try buffer.DeviceBuffer.alloc(&ctx, history_bytes);
        defer value_history.free(&ctx);
        const aligned_ids: []align(@alignOf(u32)) u8 = @alignCast(page_ids_host.bytes());
        const count = try state.stagedPageIds(group, std.mem.bytesAsSlice(u32, aligned_ids));
        try page_ids_device.copyFromHost(&ctx, aligned_ids[0 .. count * @sizeOf(u32)]);
        try forward.run(&ctx, &blas, text, &layer, .{
            .input = .{ .q = input, .k = input, .v = input },
            .output = output,
            .workspace = workspace,
            .blas_space = blas_space,
            .attention_space = attention_space,
            .cache_owner = &state,
            .visit = .{ .group = group, .slot = 0 },
            .page_ids_device = page_ids_device,
            .old_history = .{ .key = .{}, .value = .{} },
            .next_history = .{ .key = key_history, .value = value_history },
        });
        try output.copyToHost(&ctx, host.bytes());
        try ctx.synchronize();
        var nonzero: usize = 0;
        for (0..rows * @as(usize, text.hidden_size)) |i| {
            const bits = std.mem.readInt(u16, host.constBytes()[i * 2 ..][0..2], .little);
            try std.testing.expect(bits & 0x7f80 != 0x7f80);
            nonzero += @intFromBool(bits & 0x7fff != 0);
        }
        try std.testing.expect(nonzero > @as(usize, text.hidden_size));
        std.debug.print("Inkling resident attention layer={d} group={s} nonzero={d}/{d}\n", .{
            number, @tagName(group), nonzero, rows * @as(usize, text.hidden_size),
        });
    }
    try state.commit();
    try std.testing.expectEqual(@as(usize, rows), state.committedTokens());
}
