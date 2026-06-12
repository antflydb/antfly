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

// NEFTune (Jain et al., NeurIPS 2023): add uniform noise to input embeddings
// during training to improve instruction-tuned model quality. Shared helper
// callable from any trainer; determinism is driven by the global step.

const std = @import("std");

pub const NEFTuneConfig = struct {
    alpha: f32 = 0.0, // 0 = disabled; typical values 5.0 - 15.0
};

pub fn applyInPlace(
    features: []f32,
    mask: ?[]const f32,
    num_tokens: usize,
    hidden_size: usize,
    alpha: f32,
    step: u64,
) void {
    if (alpha <= 0.0 or num_tokens == 0 or hidden_size == 0) return;
    if (features.len < num_tokens * hidden_size) return;

    const total_f = @as(f32, @floatFromInt(num_tokens * hidden_size));
    const noise_scale = alpha / @sqrt(total_f);

    // Mix step with buffer length so different tensors at the same step
    // don't collide on the same noise stream.
    const seed_hash: u64 = step ^ (@as(u64, features.len) *% 0x9E3779B97F4A7C15);
    var prng = std.Random.DefaultPrng.init(seed_hash);
    const rng = prng.random();

    var t: usize = 0;
    while (t < num_tokens) : (t += 1) {
        const keep = if (mask) |m| (m[t] > 0.5) else true;
        const base = t * hidden_size;
        var i: usize = 0;
        while (i < hidden_size) : (i += 1) {
            const u = rng.float(f32) * 2.0 - 1.0;
            if (keep) features[base + i] += u * noise_scale;
        }
    }
}

pub fn applyAlloc(
    allocator: std.mem.Allocator,
    features: []const f32,
    mask: ?[]const f32,
    num_tokens: usize,
    hidden_size: usize,
    alpha: f32,
    step: u64,
) ![]f32 {
    const out = try allocator.dupe(f32, features);
    if (alpha <= 0.0 or num_tokens == 0 or hidden_size == 0) return out;
    applyInPlace(out, mask, num_tokens, hidden_size, alpha, step);
    return out;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "neftune: alpha=0 is a no-op" {
    const allocator = std.testing.allocator;
    const num_tokens: usize = 4;
    const hidden: usize = 8;
    const n = num_tokens * hidden;

    const original = try allocator.alloc(f32, n);
    defer allocator.free(original);
    for (original, 0..) |*x, i| x.* = @as(f32, @floatFromInt(i)) * 0.125;

    const buf = try allocator.dupe(f32, original);
    defer allocator.free(buf);

    applyInPlace(buf, null, num_tokens, hidden, 0.0, 42);
    try std.testing.expectEqualSlices(f32, original, buf);

    const allocd = try applyAlloc(allocator, original, null, num_tokens, hidden, 0.0, 42);
    defer allocator.free(allocd);
    try std.testing.expectEqualSlices(f32, original, allocd);
}

test "neftune: noise bounded by noise_scale per element" {
    const allocator = std.testing.allocator;
    const num_tokens: usize = 16;
    const hidden: usize = 32;
    const n = num_tokens * hidden;
    const alpha: f32 = 10.0;

    const original = try allocator.alloc(f32, n);
    defer allocator.free(original);
    for (original) |*x| x.* = 0.0;

    const noised = try applyAlloc(allocator, original, null, num_tokens, hidden, alpha, 7);
    defer allocator.free(noised);

    const total_f = @as(f32, @floatFromInt(n));
    const noise_scale = alpha / @sqrt(total_f);
    for (noised) |v| {
        try std.testing.expect(@abs(v) <= noise_scale + 1e-6);
    }
}

test "neftune: deterministic for same step" {
    const allocator = std.testing.allocator;
    const num_tokens: usize = 8;
    const hidden: usize = 16;
    const n = num_tokens * hidden;

    const original = try allocator.alloc(f32, n);
    defer allocator.free(original);
    for (original, 0..) |*x, i| x.* = @sin(@as(f32, @floatFromInt(i)) * 0.1);

    const a = try applyAlloc(allocator, original, null, num_tokens, hidden, 8.0, 123);
    defer allocator.free(a);
    const b = try applyAlloc(allocator, original, null, num_tokens, hidden, 8.0, 123);
    defer allocator.free(b);
    try std.testing.expectEqualSlices(f32, a, b);

    const c = try applyAlloc(allocator, original, null, num_tokens, hidden, 8.0, 124);
    defer allocator.free(c);
    // Different step -> at least one element must differ.
    var any_diff = false;
    for (a, c) |x, y| {
        if (x != y) {
            any_diff = true;
            break;
        }
    }
    try std.testing.expect(any_diff);
}

test "neftune: masked tokens are untouched" {
    const allocator = std.testing.allocator;
    const num_tokens: usize = 6;
    const hidden: usize = 12;
    const n = num_tokens * hidden;

    const original = try allocator.alloc(f32, n);
    defer allocator.free(original);
    for (original, 0..) |*x, i| x.* = @as(f32, @floatFromInt(i));

    const mask = try allocator.alloc(f32, num_tokens);
    defer allocator.free(mask);
    // Only tokens 1 and 3 are valid; others are padding.
    for (mask) |*m| m.* = 0.0;
    mask[1] = 1.0;
    mask[3] = 1.0;

    const noised = try applyAlloc(allocator, original, mask, num_tokens, hidden, 10.0, 99);
    defer allocator.free(noised);

    var t: usize = 0;
    while (t < num_tokens) : (t += 1) {
        const base = t * hidden;
        if (mask[t] > 0.5) continue;
        var i: usize = 0;
        while (i < hidden) : (i += 1) {
            try std.testing.expectEqual(original[base + i], noised[base + i]);
        }
    }
}

test "neftune: sum perturbation on order of noise_scale * sqrt(N)" {
    const allocator = std.testing.allocator;
    const num_tokens: usize = 64;
    const hidden: usize = 64;
    const n = num_tokens * hidden;
    const alpha: f32 = 10.0;

    const original = try allocator.alloc(f32, n);
    defer allocator.free(original);
    for (original) |*x| x.* = 0.0;

    const noised = try applyAlloc(allocator, original, null, num_tokens, hidden, alpha, 321);
    defer allocator.free(noised);

    var sum: f64 = 0.0;
    for (noised) |v| sum += v;

    const total_f = @as(f32, @floatFromInt(n));
    const noise_scale = alpha / @sqrt(total_f);
    // Expected |sum| ~ noise_scale * sqrt(N) for a zero-mean uniform draw.
    // Allow a loose 10x slack to avoid flakiness.
    const bound: f64 = @as(f64, noise_scale) * @sqrt(@as(f64, total_f)) * 10.0;
    try std.testing.expect(@abs(sum) <= bound);
}
