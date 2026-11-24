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

//! Native OpenBLAS policy qualification run by verify_runtime_openblas.py.
const std = @import("std");
const native = @import("backends/native.zig");
const linalg = @import("inference_linalg");
test "OpenBLAS probe: native CPU budget honors the explicit worker ceiling even without BLAS" {
    const raw = std.c.getenv("ANTFLY_INFERENCE_CPU_THREADS") orelse return;
    const limit = try std.fmt.parseInt(usize, std.mem.span(raw), 10);
    try std.testing.expect(linalg.pool.cachedCpuCount() <= limit);
}
test "OpenBLAS probe: concurrent first use publishes one complete BLAS policy" {
    const expected = std.mem.eql(u8, std.mem.span(std.c.getenv("EXPECT_BLAS").?), "true");
    var runtime = std.Io.Threaded.init(std.testing.allocator, .{});
    defer runtime.deinit();
    var group: std.Io.Group = .init;
    defer group.cancel(runtime.io());
    var results: [16]bool = undefined;
    for (&results) |*result| group.async(runtime.io(), struct {
        fn run(out: *bool) void {
            out.* = native.useBlas();
        }
    }.run, .{result});
    try group.await(runtime.io());
    for (results) |result| try std.testing.expectEqual(expected, result);
    if (expected) {
        const count = try std.fmt.parseInt(usize, std.mem.span(std.c.getenv("EXPECT_THREADS").?), 10);
        if (count == 0) {
            try std.testing.expect(native.openblas.threadLimit() == null);
        } else try std.testing.expectEqual(@min(count, linalg.pool.cachedCpuCount()), native.openblas.threadLimit().?);
    }
}
test "OpenBLAS probe: concurrent matrix calls keep independent outputs" {
    var runtime = std.Io.Threaded.init(std.testing.allocator, .{});
    defer runtime.deinit();
    var group: std.Io.Group = .init;
    defer group.cancel(runtime.io());
    var results: [8]bool = undefined;
    for (&results) |*result| group.async(runtime.io(), struct {
        fn run(ok: *bool) void {
            const a: [64 * 128]f32 = @splat(1);
            const b: [128 * 64]f32 = @splat(1);
            var out: [64 * 64]f32 = undefined;
            native.sgemmSync(64, 64, 128, 1, &a, &b, 0, &out);
            ok.* = true;
            for (out) |value| if (value != 128) {
                ok.* = false;
            };
        }
    }.run, .{result});
    try group.await(runtime.io());
    for (results) |ok| try std.testing.expect(ok);
}
test "OpenBLAS probe: GEMM transpose variants preserve alpha beta and layout" {
    const a = [_]f32{ 1, 2, 3, 4, 5, 6 }; // 2 x 3
    const at = [_]f32{ 1, 4, 2, 5, 3, 6 };
    const b = [_]f32{ 7, 8, 9, 10, 11, 12 }; // 3 x 2
    const bt = [_]f32{ 7, 9, 11, 8, 10, 12 };
    const expected = [_]f32{ 29.5, 32.5, 70, 77.5 };
    var out: [4]f32 = @splat(2);
    native.sgemmSync(2, 2, 3, 0.5, &a, &b, 0.25, &out);
    for (out, expected) |actual, want| try std.testing.expectApproxEqAbs(want, actual, 1e-5);
    out = .{ 2, 2, 2, 2 };
    native.sgemmTransBSync(2, 2, 3, 0.5, &a, &bt, 0.25, &out);
    for (out, expected) |actual, want| try std.testing.expectApproxEqAbs(want, actual, 1e-5);
    out = .{ 2, 2, 2, 2 };
    native.sgemmTransA(2, 2, 3, 0.5, &at, &b, 0.25, &out);
    for (out, expected) |actual, want| try std.testing.expectApproxEqAbs(want, actual, 1e-5);
}
test "OpenBLAS probe: empty reductions scale output without reading inputs" {
    var out: [6]f32 = @splat(4);
    native.sgemmSync(2, 3, 0, 1, &.{}, &.{}, 0.5, &out);
    for (out) |v| try std.testing.expectEqual(@as(f32, 2), v);
    native.sgemmTransBSync(2, 3, 0, 1, &.{}, &.{}, 0.5, &out);
    for (out) |v| try std.testing.expectEqual(@as(f32, 1), v);
    native.sgemmTransA(2, 3, 0, 1, &.{}, &.{}, 0, &out);
    for (out) |v| try std.testing.expectEqual(@as(f32, 0), v);
    native.sgemmSync(0, 0, 0, 1, &.{}, &.{}, 0, &.{});
}
test "OpenBLAS probe: strided output preserves padding and empty reduction" {
    const a = [_]f32{ 1, -2, 3, 4, 5, -6 };
    const b = [_]f32{ 1, 2, 3, -4, 5, 6, 7, 8, -9 };
    var out: [13]f32 = @splat(17);
    try native.sgemmTransBStrided(null, 2, 3, 3, 0.5, &a, &b, 0.25, out[1..], 6);
    for (0..2) |row| for (0..3) |col| {
        var expected: f32 = 17 * 0.25;
        for (0..3) |k| expected += 0.5 * a[row * 3 + k] * b[col * 3 + k];
        try std.testing.expectApproxEqAbs(expected, out[1 + row * 6 + col], 1e-5);
    };
    for ([_]usize{ 0, 4, 5, 6, 10, 11, 12 }) |i| try std.testing.expectEqual(@as(f32, 17), out[i]);
    try native.sgemmTransBStrided(null, 2, 3, 0, 1, &.{}, &.{}, 0, out[1..], 6);
    for (0..2) |row| for (0..3) |col| {
        try std.testing.expectEqual(@as(f32, 0), out[1 + row * 6 + col]);
    };
}
