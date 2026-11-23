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

//! Single-request greedy generation from the local Inkling 2 checkpoint.
const std = @import("std");
const platform = @import("antfly_platform");
const tokenizer = @import("inference_hf_tokenizer").HfTokenizer;
const checkpoint = @import("architectures/inkling2_checkpoint.zig");
const decoder = @import("architectures/inkling2_decoder_cuda.zig");
const vision = @import("architectures/inkling2_vision_cuda.zig");
const image = @import("pipelines/image.zig");
const Context = @import("ops/cuda/context.zig").CudaContext;

pub fn main(init: std.process.Init) !void {
    const allocator = std.heap.c_allocator;
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.next();
    const model_dir = args.next() orelse return usage(init.io);
    if (std.mem.eql(u8, model_dir, "--help")) return usage(init.io);
    const prompt = args.next() orelse return error.MissingPrompt;
    var limit: u32 = 32;
    var arg = args.next();
    if (arg) |value| {
        if (!std.mem.startsWith(u8, value, "--")) {
            limit = try std.fmt.parseInt(u32, value, 10);
            arg = args.next();
        }
    }
    var image_path: ?[]const u8 = null;
    var fast_matvec = false;
    var quantized_experts = false;
    while (arg) |option| : (arg = args.next()) {
        if (std.mem.eql(u8, option, "--image") and image_path == null) {
            image_path = args.next() orelse return error.InvalidArguments;
        } else if (std.mem.eql(u8, option, "--fast-matvec") and !fast_matvec) {
            fast_matvec = true;
        } else if (std.mem.eql(u8, option, "--quantized-experts") and !quantized_experts) {
            quantized_experts = true;
        } else return error.InvalidArguments;
    }
    if (limit == 0) return error.InvalidArguments;
    const pixels = if (image_path) |path| try readImage(allocator, init.io, path) else null;
    defer if (pixels) |value| value.deinit(allocator);

    const tokenizer_path = try std.fs.path.join(allocator, &.{ model_dir, "tokenizer.json" });
    defer allocator.free(tokenizer_path);
    const tokenizer_bytes = try @import("util/c_file.zig").readFileMax(allocator, tokenizer_path, 64 * 1024 * 1024);
    defer allocator.free(tokenizer_bytes);
    const hf = try tokenizer.loadFromBytes(allocator, tokenizer_bytes);
    defer hf.deinitSelf();
    const tok = hf.tokenizer();
    const image_prefix = "<|message_user|><|content_image|>";
    const rendered = if (pixels != null) try std.fmt.allocPrint(allocator, "<|end_message|><|message_user|><|content_text|>{s}<|end_message|>" ++
        "<|message_system|><|content_text|>Thinking effort level: 0<|end_message|><|message_model|><|content_text|>", .{prompt}) else null;
    defer if (rendered) |value| allocator.free(value);
    const prefix_ids = if (pixels != null) try tok.encode(allocator, image_prefix) else null;
    defer if (prefix_ids) |value| allocator.free(value);
    const image_stop = if (pixels != null) blk: {
        const ids = try tok.encode(allocator, "<|end_message|>");
        defer allocator.free(ids);
        if (ids.len != 1) return error.InvalidInkling2Tokenizer;
        break :blk try inputToken(ids[0]);
    } else null;
    const prompt_ids = try tok.encode(allocator, rendered orelse prompt);
    defer allocator.free(prompt_ids);
    if (prompt_ids.len == 0) return error.MissingPrompt;
    var source = try checkpoint.Checkpoint.open(allocator, model_dir);
    defer source.deinit();
    const image_rows = if (pixels) |value| ((value.width + 39) / 40) * ((value.height + 39) / 40) else 0;
    const prefix_rows = if (prefix_ids) |value| value.len else 0;
    const prompt_count = try std.math.add(usize, prompt_ids.len, prefix_rows + image_rows);
    const tokens = try std.math.add(usize, prompt_count, limit);
    if (tokens - 1 > source.config.value.text_config.model_max_length) return error.InvalidInkling2Context;
    var ctx = try Context.initDefault();
    defer ctx.deinit();
    const load_start = platform.time.monotonicNs();
    var model = try decoder.Decoder.load(allocator, &ctx, &source, .{
        .pack_attention_weights = fast_matvec,
        .prepare_greedy = true,
        .quantized_expert_activations = quantized_experts,
    });
    defer model.deinit();
    var request: decoder.Request = undefined;
    try request.init(allocator, &model, @intCast(tokens - 1));
    request.use_dense = fast_matvec;
    defer request.deinit() catch @panic("unable to drain the Inkling CUDA stream");
    const prefill_start = platform.time.monotonicNs();
    if (pixels) |value| {
        var encoder = try vision.Vision.load(&ctx, &source);
        defer encoder.deinit();
        var embeddings = try encoder.encodeRgb(&model.blas, value.data, value.width, value.height);
        defer embeddings.deinit(&ctx);
        for (prefix_ids.?) |id| try request.append(try inputToken(id));
        for (0..embeddings.rows) |i| try request.appendEmbedding(try embeddings.row(@intCast(i)));
    }
    for (prompt_ids[0 .. prompt_ids.len - 1]) |id| try request.append(try inputToken(id));
    var last = try request.stepGreedy(try inputToken(prompt_ids[prompt_ids.len - 1]));
    const decode_start = platform.time.monotonicNs();
    var generated: std.ArrayList(i32) = .empty;
    defer generated.deinit(allocator);
    while (generated.items.len < limit) {
        if (last == source.config.value.eos_token_id or (image_stop != null and last == image_stop.?)) break;
        try generated.append(allocator, @intCast(last));
        if (generated.items.len == limit) break;
        last = try request.stepGreedy(last);
    }
    const done = platform.time.monotonicNs();
    const text = try tok.decodeText(allocator, generated.items);
    defer allocator.free(text);
    const out = std.Io.File.stdout();
    try out.writeStreamingAll(init.io, text);
    try out.writeStreamingAll(init.io, "\n");
    std.debug.print("load_ms={d:.2} prompt_tokens={d} prefill_ms={d:.2} generated_tokens={d} decode_ms={d:.2}\n", .{
        milliseconds(prefill_start - load_start), prompt_ids.len,                    milliseconds(decode_start - prefill_start),
        generated.items.len,                      milliseconds(done - decode_start),
    });
}

fn usage(io: std.Io) !void {
    try std.Io.File.stdout().writeStreamingAll(io, "Usage: antfly-inference-inkling2-cuda MODEL_DIR PROMPT [MAX_NEW_TOKENS] [--image PATH] [--fast-matvec] [--quantized-experts]\n");
}

fn inputToken(id: i32) !u32 {
    return std.math.cast(u32, id) orelse error.InvalidInkling2Token;
}

fn readImage(allocator: std.mem.Allocator, io: std.Io, path: []const u8) !image.Image {
    const data = try std.Io.Dir.cwd().readFileAlloc(io, path, allocator, .limited(64 * 1024 * 1024));
    defer allocator.free(data);
    _ = try image.inspectEncodedForInference(data, 4096);
    const rgba = try image.decodeRgba(allocator, data);
    defer rgba.deinit(allocator);
    const rgb = try allocator.alloc(u8, @as(usize, rgba.width) * rgba.height * 3);
    for (0..rgba.height) |y| for (0..rgba.width) |x| {
        const i = y * rgba.width + x;
        const alpha: u32 = rgba.data[4 * i + 3];
        const checker: u32 = if ((x / 8 + y / 8) % 2 == 0) 204 else 153;
        for (0..3) |channel| {
            const color: u32 = rgba.data[4 * i + channel];
            rgb[3 * i + channel] = @intCast((alpha * color + (255 - alpha) * checker + 127) / 255);
        }
    };
    return .{ .data = rgb, .width = rgba.width, .height = rgba.height, .channels = 3 };
}

fn milliseconds(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / 1e6;
}
