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

//! Native Moondream2 parity against the upstream float32 reference written by
//! `scripts/moondream/reference.py`. Skipped unless both are present:
//!
//!   ANTFLY_MOONDREAM_MODEL      pulled vikhyatk/moondream2@2024-03-04 directory
//!   ANTFLY_MOONDREAM_REFERENCE  reference.py --output directory
//!
//! Checks, in pipeline order: prompt token ids, preprocessed pixels, projected
//! image embeddings, first-step logits, and the greedy answer token ids.

const std = @import("std");
const platform = @import("antfly_platform");
const c_file = @import("../util/c_file.zig");
const moondream = @import("moondream.zig");
const arch = @import("../architectures/moondream.zig");

const Reference = struct {
    before_ids: []const i32,
    after_ids: []const i32,
    stop_ids: []const i32,
    prompt_rows: usize,
    question: []const u8,
    first_top_ids: []const i32,
    generated_ids: []const i32,
    answer: []const u8,
};

fn readF32(allocator: std.mem.Allocator, root: []const u8, name: []const u8) ![]f32 {
    const path = try std.fs.path.join(allocator, &.{ root, name });
    defer allocator.free(path);
    const bytes = try c_file.readFileMax(allocator, path, 64 * 1024 * 1024);
    defer allocator.free(bytes);
    if (bytes.len % 4 != 0) return error.InvalidReferenceFile;
    const out = try allocator.alloc(f32, bytes.len / 4);
    for (out, 0..) |*v, i| v.* = @bitCast(std.mem.readInt(u32, bytes[i * 4 ..][0..4], .little));
    return out;
}

const Diff = struct { max_abs: f32, mean_abs: f32, min_row_cosine: f32 };

fn compare(a: []const f32, b: []const f32, row: usize) Diff {
    var max_abs: f32 = 0;
    var sum_abs: f64 = 0;
    var min_cos: f32 = 1;
    const rows = a.len / row;
    for (0..rows) |r| {
        var dot: f64 = 0;
        var na: f64 = 0;
        var nb: f64 = 0;
        for (a[r * row ..][0..row], b[r * row ..][0..row]) |x, y| {
            const d = @abs(x - y);
            max_abs = @max(max_abs, d);
            sum_abs += d;
            dot += @as(f64, x) * y;
            na += @as(f64, x) * x;
            nb += @as(f64, y) * y;
        }
        const cos: f32 = if (na == 0 or nb == 0) 1 else @floatCast(dot / (@sqrt(na) * @sqrt(nb)));
        min_cos = @min(min_cos, cos);
    }
    return .{ .max_abs = max_abs, .mean_abs = @floatCast(sum_abs / @as(f64, @floatFromInt(a.len))), .min_row_cosine = min_cos };
}

test "native moondream matches the upstream float32 reference" {
    const model_dir = platform.env.getenv("ANTFLY_MOONDREAM_MODEL") orelse return error.SkipZigTest;
    const root = platform.env.getenv("ANTFLY_MOONDREAM_REFERENCE") orelse return error.SkipZigTest;
    const a = std.testing.allocator;

    const json_path = try std.fs.path.join(a, &.{ root, "reference.json" });
    defer a.free(json_path);
    const json_bytes = try c_file.readFile(a, json_path);
    defer a.free(json_bytes);
    const parsed = try std.json.parseFromSlice(Reference, a, json_bytes, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    const ref = parsed.value;

    var pipeline = try moondream.Pipeline.load(a, model_dir);
    defer pipeline.deinit();
    const config = pipeline.config;

    // Tokenizer: both halves and the stop ids.
    var tokens = try pipeline.tokenizePrompt(ref.question);
    defer tokens.deinit(a);
    try std.testing.expectEqualSlices(i32, ref.before_ids, tokens.before);
    try std.testing.expectEqualSlices(i32, ref.after_ids, tokens.after);
    for (ref.stop_ids) |id| {
        var found = false;
        for (pipeline.stop_token_ids) |stop| found = found or stop == id;
        try std.testing.expect(found);
    }
    try std.testing.expectEqual(ref.prompt_rows, tokens.textCount() + config.imageTokens());

    const image_path = try std.fs.path.join(a, &.{ root, "image.png" });
    defer a.free(image_path);
    const image = try c_file.readFile(a, image_path);
    defer a.free(image);

    // Preprocessing: Pillow bicubic, [-1, 1].
    {
        const want = try readF32(a, root, "pixels.f32");
        defer a.free(want);
        const got = try moondream.preprocessImage(a, config, image);
        defer a.free(got);
        try std.testing.expectEqual(want.len, got.len);
        const diff = compare(want, got, config.image_size);
        std.debug.print("moondream parity pixels: max_abs={d:.6} mean_abs={d:.6}\n", .{ diff.max_abs, diff.mean_abs });
        // One uint8 step is 2/255 after normalization.
        try std.testing.expect(diff.max_abs <= 2.0 / 255.0 + 1e-6);
    }

    // Vision tower + projector.
    const embeddings = try pipeline.encodeImage(image, null);
    defer a.free(embeddings);
    {
        const want = try readF32(a, root, "image_embeddings.f32");
        defer a.free(want);
        try std.testing.expectEqual(want.len, embeddings.len);
        const diff = compare(want, embeddings, config.hidden_size);
        std.debug.print("moondream parity image embeddings: max_abs={d:.6} mean_abs={d:.6} min_row_cos={d:.6}\n", .{ diff.max_abs, diff.mean_abs, diff.min_row_cosine });
        try std.testing.expect(diff.min_row_cosine >= 0.9995);
        try std.testing.expect(diff.mean_abs <= 5e-3);
    }

    // First-step logits over the full prompt.
    {
        const want = try readF32(a, root, "first_logits.f32");
        defer a.free(want);
        const prompt = try pipeline.promptEmbeddings(tokens, embeddings);
        defer a.free(prompt);
        var kv = try arch.KvState.init(a, config);
        defer kv.deinit();
        var cb = pipeline.compute.computeBackend();
        const logits = try arch.forwardLastLogits(&cb, a, config, prompt, prompt.len / config.hidden_size, &kv);
        defer a.free(logits);
        try std.testing.expectEqual(want.len, logits.len);
        const diff = compare(want, logits, logits.len);
        var argmax: usize = 0;
        for (logits, 0..) |v, i| {
            if (v > logits[argmax]) argmax = i;
        }
        std.debug.print("moondream parity first logits: max_abs={d:.6} mean_abs={d:.6} cos={d:.8} argmax={d} ref_argmax={d}\n", .{ diff.max_abs, diff.mean_abs, diff.min_row_cosine, argmax, ref.first_top_ids[0] });
        try std.testing.expectEqual(@as(usize, @intCast(ref.first_top_ids[0])), argmax);
        try std.testing.expect(diff.min_row_cosine >= 0.99999);
        try std.testing.expect(diff.max_abs <= 0.05);
    }

    // Greedy answer: token-for-token.
    {
        const max_new: i32 = @intCast(ref.generated_ids.len + 1);
        var result = try pipeline.answer(image, ref.question, .{ .max_tokens = max_new }, null);
        defer result.deinit();
        const got = result.token_ids.?;
        std.debug.print("moondream parity answer: native={s} | reference={s}\n", .{ result.text, ref.answer });
        // Upstream includes the stop token in its ids; native stops before it.
        var want = ref.generated_ids;
        if (want.len > 0) {
            for (ref.stop_ids) |stop| {
                if (want[want.len - 1] == stop) {
                    want = want[0 .. want.len - 1];
                    break;
                }
            }
        }
        try std.testing.expectEqualSlices(i32, want, got);
        try std.testing.expectEqualStrings(ref.answer, result.text);
    }
}
