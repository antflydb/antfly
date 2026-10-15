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

//! Metal segment training attention (ops/segment_training_attention.zig)
//! against the CPU reference (`inference_linalg`'s
//! segmentTrainingAttentionForwardHost/BackwardHost), including
//! attention-probability dropout. Without dropout this runs on the same
//! device kernels as `modernbert_training_attention_device_test.zig`; these
//! cases specifically exercise the dropout mask the flash kernels add.
const std = @import("std");
const build_options = @import("build_options");
const reference = @import("segment_training_attention.zig");
const ops = @import("ops.zig");
const Device = @import("../graph/resident_training_fixture.zig").Device;

fn fill(values: []f32, seed: u64) void {
    var prng = std.Random.DefaultPrng.init(seed);
    for (values) |*value| value.* = prng.random().floatNorm(f32) * 0.7;
}

fn upload(cb: *const ops.ComputeBackend, values: []const f32, rows: i64, columns: i64) !ops.CT {
    return cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = values, .shape = &.{ @intCast(rows), @intCast(columns) } } }, .{});
}

fn expectClose(expected: []const f32, actual: []const f32, tolerance: f32) !void {
    try std.testing.expectEqual(expected.len, actual.len);
    var worst: f32 = 0;
    for (expected, actual) |want, got| {
        try std.testing.expect(std.math.isFinite(got));
        worst = @max(worst, @abs(want - got));
    }
    if (worst > tolerance) {
        std.debug.print("segment training attention Metal vs CPU max error {d}\n", .{worst});
        return error.TestExpectedApproxEqAbs;
    }
}

/// Encodes a u64 as two little-endian i32 words, matching
/// `segment_training_attention.zig`'s `decodeU64`.
fn encodeU64(dst: []i32, value: u64) void {
    dst[0] = @bitCast(@as(u32, @truncate(value)));
    dst[1] = @bitCast(@as(u32, @truncate(value >> 32)));
}

const Case = struct {
    attrs: reference.Attrs,
    lengths: []const usize, // valid tokens per row; padded queries see the valid keys
    segments: bool = false, // split each row's valid prefix into two trees under a trunk
    apply_dropout: bool = true, // the control's runtime flag (eval binds this to false)
};

/// Physical control: `[seed(2) | micro_batch(2) | replica(2) | apply_dropout(1)
/// | positions(tokens) | ranges(tokens*6)]` (`segment_training_attention.validateControl`).
/// Unlike ModernBERT's device control, ranges and positions here are
/// row-local (0..seq_len-1), not absolute token indices.
fn testControl(a: std.mem.Allocator, case: Case, replay: reference.Replay) ![]i32 {
    const s = case.attrs.seq_len;
    const tokens = case.attrs.batch * s;
    const words = try a.alloc(i32, 7 + tokens + tokens * 6);
    @memset(words, 0);
    encodeU64(words[0..2], replay.seed);
    encodeU64(words[2..4], replay.micro_batch);
    encodeU64(words[4..6], replay.replica);
    words[6] = @intFromBool(case.apply_dropout);
    const positions = words[7..][0..tokens];
    const ranges = words[7 + tokens ..];
    for (0..case.attrs.batch) |b| {
        const len = case.lengths[b];
        for (0..s) |i| {
            const q = b * s + i;
            positions[q] = @intCast(i);
            const own = ranges[q * 6 ..][0..6];
            const trunk = len / 2;
            if (case.segments and i < len and len >= 4) {
                // Trunk [0, trunk) sees itself; branch A [trunk, trunk+1) and
                // branch B [trunk+1, len) see the trunk and themselves.
                own[0] = 0;
                own[1] = @intCast(trunk);
                if (i >= trunk) {
                    const own_lo: usize = if (i == trunk) trunk else trunk + 1;
                    const own_hi: usize = if (i == trunk) trunk + 1 else len;
                    own[2] = @intCast(own_lo);
                    own[3] = @intCast(own_hi);
                }
            } else {
                own[0] = 0;
                own[1] = @intCast(len);
            }
        }
    }
    return words;
}

test "segment training attention Metal forward and packed gradient match the CPU reference, with and without dropout" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!@import("../backends/metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    var device = try Device.init(a);
    defer device.deinit();
    const cb = device.backend.computeBackend();
    const cases = [_]Case{
        // No dropout: the same masking as the ModernBERT device path.
        .{ .attrs = .{ .batch = 2, .seq_len = 97, .num_heads = 2, .head_dim = 64, .dropout_probability = 0, .dropout_stream_id = 0 }, .lengths = &.{ 97, 65 }, .apply_dropout = false },
        // p=0.1, global attention, several batch rows, a row length off the
        // 32-row flash tile boundary.
        .{ .attrs = .{ .batch = 2, .seq_len = 97, .num_heads = 2, .head_dim = 64, .dropout_probability = 0.1, .dropout_stream_id = 7 }, .lengths = &.{ 97, 65 } },
        // p=0.1 with a local window and trunk/branch segments.
        .{ .attrs = .{ .batch = 1, .seq_len = 128, .num_heads = 2, .head_dim = 64, .window = 20, .dropout_probability = 0.1, .dropout_stream_id = 11 }, .lengths = &.{100}, .segments = true },
        // Graph-time dropout_probability > 0 but the control's apply_dropout
        // flag is off (the eval path): must come out bit-identical to p=0.
        .{ .attrs = .{ .batch = 1, .seq_len = 64, .num_heads = 1, .head_dim = 8, .dropout_probability = 0.1, .dropout_stream_id = 3 }, .lengths = &.{64}, .apply_dropout = false },
        // p=0.1 with head_dim 96: a multiple of 8 but over the flash
        // kernels' 64 cap, so this exercises the host bridge instead.
        .{ .attrs = .{ .batch = 1, .seq_len = 48, .num_heads = 1, .head_dim = 96, .dropout_probability = 0.1, .dropout_stream_id = 5 }, .lengths = &.{40} },
    };
    for (cases) |case| {
        errdefer std.debug.print("case batch={d} seq={d} heads={d} dim={d} window={d} dropout={d} apply={}\n", .{ case.attrs.batch, case.attrs.seq_len, case.attrs.num_heads, case.attrs.head_dim, case.attrs.window, case.attrs.dropout_probability, case.apply_dropout });
        const layout = try case.attrs.layout();
        const qkv = try a.alloc(f32, @intCast(layout.qkv_rows * layout.hidden));
        defer a.free(qkv);
        fill(qkv, 41);
        const dout = try a.alloc(f32, @intCast(layout.batch_tokens * layout.hidden));
        defer a.free(dout);
        fill(dout, 43);
        const replay = reference.Replay{ .seed = 0xC0FFEE, .micro_batch = 2, .replica = 0 };
        const words = try testControl(a, case, replay);
        defer a.free(words);
        const want_forward = try reference.forward(a, case.attrs, qkv, words, .{});
        defer a.free(want_forward);
        const want_backward = try reference.backward(a, case.attrs, qkv, words, dout, .{});
        defer a.free(want_backward);

        const qkv_ct = try upload(&cb, qkv, layout.qkv_rows, layout.hidden);
        defer cb.free(qkv_ct);
        const dout_ct = try upload(&cb, dout, layout.batch_tokens, layout.hidden);
        defer cb.free(dout_ct);
        const control_ct = (try cb.fromInt32Shape(words, &.{@intCast(words.len)})) orelse return error.UnsupportedTestBackend;
        defer cb.free(control_ct);
        const output = try cb.segmentTrainingAttentionV1(qkv_ct, control_ct, case.attrs);
        defer cb.free(output);
        const got_forward = try cb.toFloat32(output, a);
        defer a.free(got_forward);
        try expectClose(want_forward, got_forward, 2e-5);
        const gradient = try cb.segmentTrainingAttentionBackwardV1(qkv_ct, control_ct, dout_ct, case.attrs);
        defer cb.free(gradient);
        const got_backward = try cb.toFloat32(gradient, a);
        defer a.free(got_backward);
        var largest: f32 = 0;
        for (want_backward) |value| largest = @max(largest, @abs(value));
        try expectClose(want_backward, got_backward, 2e-5 + 2e-4 * largest);
    }
}
