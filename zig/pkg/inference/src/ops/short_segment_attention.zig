// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Bounded dense attention for one short, contiguous segment on Accelerate.
//! Other topologies retain the general tiled implementation.
const std = @import("std");
const native = @import("../backends/native.zig");
const linalg = @import("inference_linalg");

pub fn execute(a: std.mem.Allocator, io: ?std.Io, q: []const f32, k: []const f32, v: []const f32, request: anytype) !?[]f32 {
    if (comptime @import("builtin").os.tag != .macos) return null;
    if (!native.useBlas()) return null;
    const n = request.queries;
    const d = request.head_dim;
    if (n == 0 or n > 198 or request.keys != n or d != 64 or request.num_heads == 0 or request.num_heads > 64) return null;
    if (request.ranges.len != n * 6 or request.query_positions.len != n or request.key_positions.len != n) return null;
    for (0..n) |row| {
        const ranges = request.ranges[row * 6 ..][0..6];
        if (ranges[0] != 0 or ranges[1] != n or ranges[2] != ranges[3] or ranges[4] != ranges[5] or
            ranges[2] > n or ranges[4] > n or request.query_positions[row] != row or request.key_positions[row] != row) return null;
    }
    const h = request.num_heads * d;
    if (q.len != n * h or k.len != n * h or v.len != n * h) return error.InvalidAttentionShape;
    const out = try a.alloc(f32, n * h);
    errdefer a.free(out);
    const qh = try a.alloc(f32, n * d);
    defer a.free(qh);
    const kh = try a.alloc(f32, n * d);
    defer a.free(kh);
    const vh = try a.alloc(f32, n * d);
    defer a.free(vh);
    const oh = try a.alloc(f32, n * d);
    defer a.free(oh);
    // At the 198-token ceiling this score matrix is only 156,816 bytes.
    const scores = try a.alloc(f32, n * n);
    defer a.free(scores);
    const scale = 1 / @sqrt(@as(f32, @floatFromInt(d)));
    for (0..request.num_heads) |head| {
        for (0..n) |row| {
            @memcpy(qh[row * d ..][0..d], q[row * h + head * d ..][0..d]);
            @memcpy(kh[row * d ..][0..d], k[row * h + head * d ..][0..d]);
            @memcpy(vh[row * d ..][0..d], v[row * h + head * d ..][0..d]);
        }
        if (io) |runtime_io|
            try native.sgemmTransB(runtime_io, n, n, d, scale, qh, kh, 0, scores)
        else
            native.sgemmTransBSync(n, n, d, scale, qh, kh, 0, scores);
        for (0..n) |row| {
            const values = scores[row * n ..][0..n];
            for (values, 0..) |*value, col|
                if ((if (row > col) row - col else col - row) > request.window) {
                    value.* = -std.math.inf(f32);
                };
            linalg.primitives.softmaxRow(values);
        }
        if (io) |runtime_io|
            try native.sgemm(runtime_io, n, d, n, 1, scores, vh, 0, oh)
        else
            native.sgemmSync(n, d, n, 1, scores, vh, 0, oh);
        for (0..n) |row| @memcpy(out[row * h + head * d ..][0..d], oh[row * d ..][0..d]);
    }
    return out;
}

test "short segment attention matches tiled oracle and rejects other topologies" {
    if (comptime @import("builtin").os.tag != .macos) return error.SkipZigTest;
    if (!native.useBlas()) return error.SkipZigTest;
    const a = std.testing.allocator;
    for ([_]usize{ 1, 7, 40, 87, 158, 198 }) |n| {
        const h = 2 * 64;
        const input = try a.alloc(f32, n * h);
        defer a.free(input);
        for (input, 0..) |*x, i| x.* = @sin(@as(f32, @floatFromInt(i)) * 0.13);
        const ranges = try a.alloc(u32, n * 6);
        defer a.free(ranges);
        @memset(ranges, 0);
        const positions = try a.alloc(i32, n);
        defer a.free(positions);
        for (positions, 0..) |*p, row| {
            p.* = @intCast(row);
            ranges[row * 6 + 1] = @intCast(n);
        }
        for ([_]u32{ 0, 64, std.math.maxInt(u32) }) |window| {
            const request = .{ .queries = n, .keys = n, .head_dim = @as(usize, 64), .num_heads = @as(usize, 2), .ranges = ranges, .query_positions = positions, .key_positions = positions, .window = window };
            const actual = (try execute(a, null, input, input, input, request)).?;
            defer a.free(actual);
            const expected = try linalg.segmentAttentionHost(a, input, input, input, ranges, positions, positions, window, n, n, 2, 64);
            defer a.free(expected);
            for (actual, expected) |x, y| try std.testing.expectApproxEqAbs(y, x, 2e-5);
            positions[0] = 1;
            try std.testing.expect(try execute(a, null, input, input, input, request) == null);
            positions[0] = 0;
            ranges[1] = 0;
            try std.testing.expect(try execute(a, null, input, input, input, request) == null);
            ranges[1] = @intCast(n);
        }
    }
}

test "short segment attention releases scratch on every allocation failure" {
    if (comptime @import("builtin").os.tag != .macos) return error.SkipZigTest;
    if (!native.useBlas()) return error.SkipZigTest;
    const Check = struct {
        fn run(a: std.mem.Allocator) !void {
            const input: [3 * 64]f32 = @splat(0.25);
            const ranges = [_]u32{ 0, 3, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0 };
            const positions = [_]i32{ 0, 1, 2 };
            const output = (try execute(a, null, &input, &input, &input, .{ .queries = @as(usize, 3), .keys = @as(usize, 3), .head_dim = @as(usize, 64), .num_heads = @as(usize, 1), .ranges = &ranges, .query_positions = &positions, .key_positions = &positions, .window = @as(u32, 1) })).?;
            defer a.free(output);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}
