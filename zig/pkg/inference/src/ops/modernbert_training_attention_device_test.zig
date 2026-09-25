// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Metal ModernBERT training attention against the CPU reference.
const std = @import("std");
const build_options = @import("build_options");
const reference = @import("modernbert_training_attention.zig");
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
        std.debug.print("ModernBERT training attention Metal vs CPU max error {d}\n", .{worst});
        return error.TestExpectedApproxEqAbs;
    }
}

test "modernbert training attention Metal forward and packed gradient match the CPU reference" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!@import("../backends/metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    var device = try Device.init(a);
    defer device.deinit();
    const cb = device.backend.computeBackend();
    const Case = struct { attrs: reference.Attrs, lengths: []const usize, segments: bool = false };
    const cases = [_]Case{
        .{ .attrs = .{ .batch = 2, .seq_len = 7, .num_heads = 2, .head_dim = 4 }, .lengths = &.{ 7, 4 } },
        .{ .attrs = .{ .batch = 2, .seq_len = 9, .num_heads = 3, .head_dim = 2, .window = 2 }, .lengths = &.{ 9, 5 } },
        .{ .attrs = .{ .batch = 1, .seq_len = 10, .num_heads = 2, .head_dim = 4 }, .lengths = &.{8}, .segments = true },
        .{ .attrs = .{ .batch = 2, .seq_len = 8, .num_heads = 1, .head_dim = 6, .window = 1 }, .lengths = &.{ 8, 3 }, .segments = true },
        // Several 256-key chunks, 64-wide heads, and a local window.
        .{ .attrs = .{ .batch = 1, .seq_len = 600, .num_heads = 2, .head_dim = 64, .window = 64 }, .lengths = &.{590} },
        .{ .attrs = .{ .batch = 2, .seq_len = 300, .num_heads = 2, .head_dim = 64 }, .lengths = &.{ 300, 120 } },
    };
    for (cases) |case| {
        errdefer std.debug.print("case batch={d} seq={d} heads={d} dim={d} window={d}\n", .{ case.attrs.batch, case.attrs.seq_len, case.attrs.num_heads, case.attrs.head_dim, case.attrs.window });
        const layout = try case.attrs.layout();
        const qkv = try a.alloc(f32, @intCast(layout.qkv_rows * layout.hidden));
        defer a.free(qkv);
        fill(qkv, 41);
        const dout = try a.alloc(f32, @intCast(layout.tokens * layout.hidden));
        defer a.free(dout);
        fill(dout, 43);
        const words = try reference.testControl(a, .{ .attrs = case.attrs, .lengths = case.lengths, .segments = case.segments });
        defer a.free(words);
        const want_forward = try reference.forward(a, case.attrs, qkv, words, null);
        defer a.free(want_forward);
        const want_backward = try reference.backward(a, case.attrs, qkv, words, dout, null);
        defer a.free(want_backward);

        const qkv_ct = try upload(&cb, qkv, layout.qkv_rows, layout.hidden);
        defer cb.free(qkv_ct);
        const dout_ct = try upload(&cb, dout, layout.tokens, layout.hidden);
        defer cb.free(dout_ct);
        const control_ct = (try cb.fromInt32Shape(words, &.{@intCast(words.len)})) orelse return error.UnsupportedTestBackend;
        defer cb.free(control_ct);
        const output = try cb.modernBertTrainingAttentionV1(qkv_ct, control_ct, case.attrs);
        defer cb.free(output);
        const got_forward = try cb.toFloat32(output, a);
        defer a.free(got_forward);
        try expectClose(want_forward, got_forward, 2e-5);
        const gradient = try cb.modernBertTrainingAttentionBackwardV1(qkv_ct, control_ct, dout_ct, case.attrs);
        defer cb.free(gradient);
        const got_backward = try cb.toFloat32(gradient, a);
        defer a.free(got_backward);
        var largest: f32 = 0;
        for (want_backward) |value| largest = @max(largest, @abs(value));
        try expectClose(want_backward, got_backward, 2e-5 + 2e-4 * largest);
        // Fixed reduction shapes: a second run is bit-identical.
        const again = try cb.modernBertTrainingAttentionBackwardV1(qkv_ct, control_ct, dout_ct, case.attrs);
        defer cb.free(again);
        const repeated = try cb.toFloat32(again, a);
        defer a.free(repeated);
        try std.testing.expectEqualSlices(u32, std.mem.bytesAsSlice(u32, std.mem.sliceAsBytes(got_backward)), std.mem.bytesAsSlice(u32, std.mem.sliceAsBytes(repeated)));
    }
}
