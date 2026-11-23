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
const platform = @import("antfly_platform");
const checkpoint = @import("architectures/inkling2_checkpoint.zig");
const decoder = @import("architectures/inkling2_decoder_cuda.zig");
const vision = @import("architectures/inkling2_vision_cuda.zig");
const Context = @import("ops/cuda/context.zig").CudaContext;
const buffer = @import("ops/cuda/buffer.zig");

const head_tokens = [_]u32{ 42, 34851, 220, 110104, 314, 159, 9599, 42, 1883, 220, 481 };
const qualification_prompts = [_]struct { name: []const u8, tokens: []const u32 }{
    .{ .name = "code", .tokens = &.{ 1719, 152397, 7, 87, 8, 25, 198, 149862, 150583 } },
    .{ .name = "german", .tokens = &.{ 31814, 155465, 52015, 149993, 154307, 150012 } },
    .{ .name = "math", .tokens = &.{ 68107, 196987, 220, 68028, 196977 } },
    .{ .name = "holdout-product", .tokens = &.{ 68237, 196987, 220, 68429, 196977 } },
    .{ .name = "holdout-root", .tokens = &.{ 371, 152397, 151798, 149879, 220, 72916, 149884 } },
    .{ .name = "holdout-french", .tokens = &.{ 2658, 184184, 149877, 149902, 6, 127341, 149987 } },
    .{ .name = "holdout-code", .tokens = &.{ 1719, 150935, 888, 7, 77, 8, 25, 198, 149862, 150003, 149934, 196982, 220, 17, 25 } },
};

fn topToken(logits: []const f32, cap: f32) !u32 {
    var best: usize = 0;
    for (logits, 0..) |logit, i| {
        try std.testing.expect(std.math.isFinite(logit));
        try std.testing.expect(@abs(logit) <= cap);
        if (logit > logits[best]) best = i;
    }
    return @intCast(best);
}

fn verifyReplay(allocator: std.mem.Allocator, model: *decoder.Decoder, limit: u32) !void {
    const ctx = model.ctx;
    if (ctx.info.compute_major != 12 or ctx.info.compute_minor > 1) return;
    var eager: decoder.Request = undefined;
    try eager.init(allocator, model, limit);
    defer eager.deinit() catch @panic("unable to drain eager Inkling request");
    eager.replay_enabled = false;
    eager.use_separate_controllers = true;
    var captured: decoder.Request = undefined;
    try captured.init(allocator, model, limit);
    defer captured.deinit() catch @panic("unable to drain captured Inkling request");
    defer model.attn.attention.use_serial_chunks = false;
    const before = ctx.stats.graph_submissions;
    for (0..limit) |position| {
        const token: u32 = @intCast(42 + position % 5 * 313);
        model.attn.attention.use_serial_chunks = true;
        const reference: ?[]const f32 = if (position <= 4 or (position >= 15 and position <= 17) or
            (position >= 126 and position <= 129) or position + 4 >= limit)
            try eager.step(token)
        else blk: {
            try eager.append(token);
            break :blk null;
        };
        model.attn.attention.use_serial_chunks = false;
        if (reference) |expected| {
            const replayed = try captured.step(token);
            try std.testing.expectEqualSlices(f32, expected, replayed);
        } else {
            try captured.append(token);
        }
    }
    try std.testing.expectEqual(@as(usize, limit), captured.position());
    try std.testing.expectEqual(@as(usize, limit - 3), ctx.stats.graph_submissions - before);
    std.debug.print("Inkling CUDA Graph logits matched eager serial attention through position {d}, {d} graph submissions\n", .{ limit, ctx.stats.graph_submissions - before });
}

fn exportQualificationLogits(allocator: std.mem.Allocator, model: *decoder.Decoder, dir: std.Io.Dir) !void {
    for ([_]bool{ false, true }) |fast| {
        var request: decoder.Request = undefined;
        try request.init(allocator, model, head_tokens.len);
        defer request.deinit() catch @panic("unable to drain Inkling qualification request");
        request.use_dense = fast;
        for (head_tokens, 0..) |token, i| {
            if (i != 0 and i != 1 and i != 4 and i != head_tokens.len - 1) {
                try request.append(token);
                continue;
            }
            const logits = try request.step(token);
            var filename: [48]u8 = undefined;
            const name = try std.fmt.bufPrint(&filename, "cuda-{s}-{d}.f32", .{ if (fast) "fast" else "default", i });
            try dir.writeFile(std.testing.io, .{ .sub_path = name, .data = std.mem.sliceAsBytes(logits) });
        }
        for (qualification_prompts) |prompt| {
            var heldout: decoder.Request = undefined;
            try heldout.init(allocator, model, @intCast(prompt.tokens.len));
            defer heldout.deinit() catch @panic("unable to drain Inkling prompt");
            heldout.use_dense = fast;
            for (prompt.tokens[0 .. prompt.tokens.len - 1]) |token| try heldout.append(token);
            const logits = try heldout.step(prompt.tokens[prompt.tokens.len - 1]);
            var filename: [48]u8 = undefined;
            const name = try std.fmt.bufPrint(&filename, "cuda-{s}-{s}.f32", .{ if (fast) "fast" else "default", prompt.name });
            try dir.writeFile(std.testing.io, .{ .sub_path = name, .data = std.mem.sliceAsBytes(logits) });
        }
    }
}

fn exportQualificationImage(allocator: std.mem.Allocator, model: *decoder.Decoder, image: vision.Embeddings, dir: std.Io.Dir) !void {
    for ([_]bool{ false, true }) |fast| {
        var request: decoder.Request = undefined;
        try request.init(allocator, model, image.rows + 1);
        defer request.deinit() catch @panic("unable to drain Inkling probe image");
        request.use_dense = fast;
        for (0..image.rows) |i| try request.appendEmbedding(try image.row(@intCast(i)));
        const logits = try request.step(42);
        try dir.writeFile(std.testing.io, .{ .sub_path = if (fast) "cuda-fast-image.f32" else "cuda-image.f32", .data = std.mem.sliceAsBytes(logits) });
    }
}

fn verifyLongContext(allocator: std.mem.Allocator, model: *decoder.Decoder) !void {
    const window = model.text.sliding_window_size;
    const floor = model.text.log_scaling_n_floor;
    const limit = floor + 2;
    var request: decoder.Request = undefined;
    try request.init(allocator, model, limit);
    defer request.deinit() catch @panic("unable to drain long Inkling request");
    try std.testing.expect(!request.replay_enabled);
    request.use_dense = true;
    const start = platform.time.monotonicNs();
    for (0..limit) |i| {
        const token = head_tokens[i % head_tokens.len];
        const position = i + 1;
        if (position == window - 1 or position == window or position == window + 1 or
            position == floor - 1 or position == floor or position == floor + 1 or position == limit)
        {
            const logits = try request.step(token);
            const best = try topToken(logits, model.text.final_logit_softcapping);
            std.debug.print("Inkling long context through {d} tokens, argmax={d}, peak={d:.5}\n", .{ position, best, logits[best] });
        } else {
            try request.append(token);
        }
        try std.testing.expectEqual(position, request.position());
    }
    try std.testing.expectError(error.KvCapacityTooSmall, request.step(head_tokens[0]));
    try std.testing.expectEqual(@as(usize, limit), request.position());
    std.debug.print("Inkling long context completed {d} tokens in {d:.3}s\n", .{
        limit, @as(f64, @floatFromInt(platform.time.monotonicNs() - start)) / 1e9,
    });
}

fn verifyOutputHead(allocator: std.mem.Allocator, model: *decoder.Decoder) !void {
    if (model.ctx.info.compute_major != 12 or model.ctx.info.compute_minor > 1) return;
    const sequence = head_tokens;
    var reference: decoder.Request = undefined;
    try reference.init(allocator, model, sequence.len);
    defer reference.deinit() catch @panic("unable to drain Inkling cuBLAS output head");
    reference.use_reference_head = true;
    var native: decoder.Request = undefined;
    try native.init(allocator, model, sequence.len);
    defer native.deinit() catch @panic("unable to drain Inkling native output head");
    var max_difference: f32 = 0;
    for (sequence) |token| {
        const expected = try reference.step(token);
        const actual = try native.step(token);
        for (expected, actual) |a, b| max_difference = @max(max_difference, @abs(a - b));
        try std.testing.expectEqual(try topToken(expected, model.text.final_logit_softcapping), try topToken(actual, model.text.final_logit_softcapping));
    }
    std.debug.print("Inkling F32 output head max absolute error {d:.8} against cuBLAS, matching greedy tokens at {d} positions\n", .{ max_difference, sequence.len });
    try std.testing.expect(max_difference < 0.002);
}

fn verifyPackedWeights(allocator: std.mem.Allocator, model: *decoder.Decoder) !void {
    if (model.ctx.info.compute_major != 12 or model.ctx.info.compute_minor > 1) return;
    try std.testing.expect(model.packed_head.storage.len > 0);
    for (&model.layers) |*layer| {
        try std.testing.expect(layer.attn.packed_input_weights.storage.len > 0);
        try std.testing.expect(layer.attn.packed_output_weights.storage.len > 0);
    }
    const sequence = head_tokens;
    var raw: decoder.Request = undefined;
    try raw.init(allocator, model, sequence.len);
    defer raw.deinit() catch @panic("unable to drain raw BF16 decode request");
    raw.use_dense = true;
    raw.use_packed_weights = false;
    raw.use_separate_controllers = true;
    var compressed: decoder.Request = undefined;
    try compressed.init(allocator, model, sequence.len);
    defer compressed.deinit() catch @panic("unable to drain packed BF16 decode request");
    compressed.use_dense = true;
    for (sequence) |token| {
        const expected = try raw.step(token);
        const actual = try compressed.step(token);
        try std.testing.expectEqualSlices(f32, expected, actual);
    }
    std.debug.print("Inkling losslessly packed BF16 attention and output weights matched full native logits at {d} positions\n", .{sequence.len});
}

fn verifyGreedyShortlist(allocator: std.mem.Allocator, model: *decoder.Decoder) !void {
    const ctx = model.ctx;
    if (ctx.info.compute_major != 12 or ctx.info.compute_minor > 1) return;
    try std.testing.expect(model.head_shortlist.storage.len > 0);
    for ([_]bool{ false, true }) |fast| {
        var full: decoder.Request = undefined;
        try full.init(allocator, model, head_tokens.len);
        defer full.deinit() catch @panic("unable to drain full-logit request");
        full.use_dense = fast;
        var greedy: decoder.Request = undefined;
        try greedy.init(allocator, model, head_tokens.len);
        defer greedy.deinit() catch @panic("unable to drain greedy request");
        greedy.use_dense = fast;
        try std.testing.expect(greedy.shortlist_workspace.len > 0);
        for (head_tokens) |token| {
            const expected = try topToken(try full.step(token), model.text.final_logit_softcapping);
            try std.testing.expectEqual(expected, try greedy.stepGreedy(token));
        }
        try std.testing.expectEqual(@as(usize, head_tokens.len), greedy.position());
    }
    std.debug.print("Inkling packed greedy selection matched full capped logits at {d} positions in each decode mode\n", .{head_tokens.len});

    const k = 2304;
    const rows = 1025;
    var host_weights = try buffer.HostBuffer.alloc(ctx, k * rows * 2);
    defer host_weights.free(ctx);
    const weight_bytes: []align(2) u8 = @alignCast(host_weights.bytes());
    const weights = std.mem.bytesAsSlice(u16, weight_bytes);
    @memset(weights[0 .. k * (rows - 1)], 0x3d00);
    @memset(weights[k * (rows - 1) ..], 0x3d80);
    var raw = try buffer.DeviceBuffer.alloc(ctx, host_weights.len);
    defer raw.free(ctx);
    try raw.copyFromHost(ctx, host_weights.constBytes());
    var exact = try model.matvec.tryPack(ctx, raw, k, rows, .f32);
    defer exact.deinit(ctx);
    var shortlist = try model.matvec.tryPrepareShortlist(ctx, raw, exact);
    defer shortlist.deinit(ctx);
    try std.testing.expect(shortlist.rows == rows);
    var scratch = try buffer.DeviceBuffer.alloc(ctx, shortlist.workspaceBytes());
    defer scratch.free(ctx);
    var input = try buffer.DeviceBuffer.alloc(ctx, k * 2);
    defer input.free(ctx);
    var host_input = try buffer.HostBuffer.alloc(ctx, k * 2 + 4);
    defer host_input.free(ctx);
    const input_bytes: []align(2) u8 = @alignCast(host_input.bytes()[0 .. k * 2]);
    const values = std.mem.bytesAsSlice(u16, input_bytes);
    const output = host_input.bytes()[k * 2 ..][0..4];
    @memset(values, 0x4100);
    try input.copyFromHost(ctx, input_bytes);
    try model.matvec.runGreedy(ctx, input, exact, shortlist, scratch, output, 30);
    try ctx.synchronize();
    // Soft-capping saturates all rows, including the final partial block.
    // The larger raw score in the final row must lose the capped tie.
    try std.testing.expectEqual(@as(u32, 0), std.mem.readInt(u32, output, .little));

    values[0] = 0x7fc0;
    try input.copyFromHost(ctx, input_bytes);
    try model.matvec.runGreedy(ctx, input, exact, shortlist, scratch, output, 30);
    try ctx.synchronize();
    try std.testing.expectEqual(std.math.maxInt(u32), std.mem.readInt(u32, output, .little));

    @memset(values, 0);
    values[0] = 0x4800;
    try input.copyFromHost(ctx, input_bytes);
    try model.matvec.runGreedy(ctx, input, exact, shortlist, scratch, output, 65536);
    try ctx.synchronize();
    try std.testing.expectEqual(@as(u32, rows - 1), std.mem.readInt(u32, output, .little));
    std.debug.print("Packed greedy selection preserved soft-cap ties, non-finite errors and the out-of-range exact scan\n", .{});
}

test "CUDA Inkling 2 runs the fixed loop with all released text weights" {
    const allocator = std.testing.allocator;
    const dir = platform.env.getenv("ANTFLY_INKLING2_MODEL_DIR") orelse return error.Inkling2FixtureRequired;
    var source = try checkpoint.Checkpoint.open(allocator, dir);
    defer source.deinit();
    var ctx = try Context.initDefault();
    defer ctx.deinit();
    const load_start = platform.time.monotonicNs();
    var model = try decoder.Decoder.load(allocator, &ctx, &source, .{ .pack_attention_weights = true, .retain_raw_head = true, .prepare_greedy = true });
    defer model.deinit();
    std.debug.print("Inkling resident text model loaded in {d:.3}s, {d} weight layers, {d} visits\n", .{
        @as(f64, @floatFromInt(platform.time.monotonicNs() - load_start)) / 1e9, model.loaded_layers, model.schedule.len,
    });
    var request: decoder.Request = undefined;
    const context_limit = 18;
    try request.init(allocator, &model, context_limit);
    defer request.deinit() catch @panic("unable to drain Inkling decode request");
    try std.testing.expectError(error.InvalidInkling2Token, request.step(model.text.unpadded_vocab_size));
    try std.testing.expectEqual(@as(usize, 0), request.position());
    var token: u32 = 42;
    var first_peak: f32 = 0;
    for (1..context_limit + 1) |position| {
        const start = platform.time.monotonicNs();
        const logits = try request.step(token);
        const elapsed_ms = @as(f64, @floatFromInt(platform.time.monotonicNs() - start)) / 1e6;
        try std.testing.expectEqual(@as(usize, model.text.unpadded_vocab_size), logits.len);
        try std.testing.expectEqual(position, request.position());
        const next = try topToken(logits, model.text.final_logit_softcapping);
        try std.testing.expect(logits[next] > 0);
        if (position == 1) first_peak = logits[next] else try std.testing.expect(first_peak != logits[next]);
        if (position <= 3 or position >= 17) std.debug.print("Inkling text position={d} token={d} next={d} peak={d:.5} latency_ms={d:.3}\n", .{ position, token, next, logits[next], elapsed_ms });
        token = next;
    }
    try std.testing.expectError(error.KvCapacityTooSmall, request.step(token));
    try std.testing.expectEqual(@as(usize, context_limit), request.position());
    const qualification_dir = if (platform.env.getenv("ANTFLY_INKLING2_QUALIFICATION_DIR")) |path|
        try std.Io.Dir.openDirAbsolute(std.testing.io, path, .{})
    else
        null;
    const long_qualification = platform.env.getenv("ANTFLY_INKLING2_QUALIFICATION_LONG") != null;
    defer if (qualification_dir) |dir_handle| dir_handle.close(std.testing.io);
    try verifyReplay(allocator, &model, if (long_qualification) model.text.sliding_window_size else 130);
    try verifyOutputHead(allocator, &model);
    try verifyPackedWeights(allocator, &model);
    try verifyGreedyShortlist(allocator, &model);
    if (qualification_dir) |dir_handle| try exportQualificationLogits(allocator, &model, dir_handle);

    var encoder = try vision.Vision.load(&ctx, &source);
    defer encoder.deinit();
    const width = 57;
    const height = 62;
    var pixels: [width * height * 3]u8 = undefined;
    for (&pixels, 0..) |*value, i| value.* = @truncate(i * 17);
    var image = try encoder.encodeRgb(&model.blas, &pixels, width, height);
    defer image.deinit(&ctx);
    try std.testing.expectEqual(@as(u32, 4), image.rows);
    try std.testing.expectError(error.InvalidInkling2ImageRow, image.row(image.rows));
    var multimodal: decoder.Request = undefined;
    try multimodal.init(allocator, &model, image.rows + 1);
    defer multimodal.deinit() catch @panic("unable to drain Inkling multimodal request");
    for (0..image.rows) |i| try multimodal.appendEmbedding(try image.row(@intCast(i)));
    const image_logits = try multimodal.step(42);
    const image_next = try topToken(image_logits, model.text.final_logit_softcapping);
    try std.testing.expectEqual(@as(usize, 5), multimodal.position());
    try std.testing.expect(image_logits[image_next] != first_peak);
    std.debug.print("Inkling vision layers=23 image={d}x{d} projected_rows={d} next={d} peak={d:.5}\n", .{
        width, height, image.rows, image_next, image_logits[image_next],
    });
    if (qualification_dir) |dir_handle| try exportQualificationImage(allocator, &model, image, dir_handle);
    if (long_qualification) try verifyLongContext(allocator, &model);
}
