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

//! Hardware differentials. Explicit CUDA selection never skips missing hardware.
const std = @import("std");
const build_options = @import("build_options");
const support = @import("../util/laya_test_support.zig");
const native = @import("../ops/native_compute.zig");
const cuda = @import("../ops/cuda/cuda_compute.zig");

fn requireCuda() !void {
    if (try support.selectedBackend() != .cuda) return error.SkipZigTest;
    if (!build_options.enable_cuda) return error.CudaNotEnabled;
}

test "laya CUDA batched RoPE matches independent positions and native rotation" {
    if (comptime !build_options.enable_cuda) return requireCuda();
    try requireCuda();
    const a = std.testing.allocator;
    var gpu = try cuda.CudaCompute.init(a);
    defer gpu.deinit();
    const cb = gpu.computeBackend();
    const batch = 3;
    const seq = 7;
    const heads = 2;
    const dim = 8;
    var values: [batch * seq * heads * dim]f32 = undefined;
    for (&values, 0..) |*v, i| v.* = @sin(@as(f32, @floatFromInt(i)) * 0.13);
    const input = try cb.fromFloat32Shape(&values, &.{ batch * seq, heads * dim });
    defer cb.free(input);
    for ([_]bool{ false, true }) |interleaved| {
        for ([_]usize{ 0, 11 }) |offset| {
            var expected = values;
            var positions: [batch * seq * heads]usize = undefined;
            for (&positions, 0..) |*position, i| position.* = offset + (i / heads) % seq;
            native.ropeCore(&expected, &positions, dim, dim, 10000, 1, interleaved);
            const output = try cb.rope(input, seq, dim, dim, 10000, 1, offset, interleaved);
            defer cb.free(output);
            const host = try cb.toFloat32(output, a);
            defer a.free(host);
            for (expected, host) |want, got| try std.testing.expectApproxEqAbs(want, got, 2e-5);
        }
    }
    try std.testing.expectEqual(@as(usize, 0), gpu.snapshotStats().rope_host_fallbacks);
}

test "laya CUDA local attention matches explicit bidirectional window bias" {
    if (comptime !build_options.enable_cuda) return requireCuda();
    try requireCuda();
    const a = std.testing.allocator;
    var gpu = try cuda.CudaCompute.init(a);
    defer gpu.deinit();
    const cb = gpu.computeBackend();
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var cpu = native.NativeCompute.init(a, &store, null);
    defer cpu.deinit();
    const reference = cpu.computeBackend();
    for ([_]usize{ 7, 63, 64, 65, 127, 128, 129, 511, 512 }) |seq| {
        const batch = 2;
        const heads = 2;
        const dim = 8;
        const values = try a.alloc(f32, batch * seq * heads * dim);
        defer a.free(values);
        for (values, 0..) |*v, i| v.* = @sin(@as(f32, @floatFromInt(i)) * 0.1);
        const mask = try a.alloc(i64, batch * seq);
        defer a.free(mask);
        @memset(mask, 1);
        @memset(mask[mask.len - 3 ..], 0);
        const input = try cb.fromFloat32Shape(values, &.{ @intCast(batch * seq), heads * dim });
        defer cb.free(input);
        const cpu_input = try reference.fromFloat32Shape(values, &.{ @intCast(batch * seq), heads * dim });
        defer reference.free(cpu_input);
        for ([_]usize{ 0, 3, 64 }) |radius| {
            const bias = try a.alloc(f32, heads * seq * seq);
            defer a.free(bias);
            for (0..heads) |head| for (0..seq) |q| {
                for (0..seq) |k| bias[(head * seq + q) * seq + k] = if ((@max(q, k) - @min(q, k)) <= radius) 0 else -std.math.inf(f32);
            };
            const cpu_bias = try reference.fromFloat32(bias);
            defer reference.free(cpu_bias);
            const expected = try reference.scaledDotProductAttention(cpu_input, cpu_input, cpu_input, mask, cpu_bias, batch, seq, heads, dim);
            defer reference.free(expected);
            const output = (try cb.encoderLocalAttention(input, input, input, mask, batch, seq, heads, dim, radius)).?;
            defer cb.free(output);
            const host = try cb.toFloat32(output, a);
            defer a.free(host);
            const oracle = try reference.toFloat32(expected, a);
            defer a.free(oracle);
            for (host, oracle, 0..) |got, want, i| {
                // Fully masked padded queries are not consumed by Laya.
                if (mask[i / (heads * dim)] != 0) try std.testing.expectApproxEqAbs(want, got, 2e-4);
                try std.testing.expect(std.math.isFinite(got));
            }
        }
    }
}

test "laya CUDA marker gather and action features preserve padding ties and row boundaries" {
    if (comptime !build_options.enable_cuda) return requireCuda();
    try requireCuda();
    const a = std.testing.allocator;
    var gpu = try cuda.CudaCompute.init(a);
    defer gpu.deinit();
    const cb = gpu.computeBackend();
    for ([_]usize{ 1, 2, 127, 128, 129, 256 }) |batch| {
        for ([_]usize{ 2, 3, 5, 6, 10, 11, 20 }) |count| {
            for ([_]bool{ false, true }) |padded| {
                const seq = 3;
                const dim = 64;
                const values = try a.alloc(f32, batch * seq * dim);
                defer a.free(values);
                for (values, 0..) |*v, i| v.* = @floatFromInt(i);
                const markers = try a.alloc(i64, batch * count);
                defer a.free(markers);
                const indices = try a.alloc(i64, markers.len);
                defer a.free(indices);
                const scores = try a.alloc(f32, markers.len);
                defer a.free(scores);
                for (0..batch) |row| {
                    for (0..count) |option| {
                        const position: i64 = if (option == 0) 2 else if (option == 1) 1 else if (padded and option == count - 1) -1 else 0;
                        markers[row * count + option] = position;
                        indices[row * count + option] = @intCast(row * seq + @as(usize, @intCast(@max(position, 0))));
                        scores[row * count + option] = if (option < 2) 1000 else if (position == -1) 10000 else -1000;
                    }
                }
                const hidden = try cb.fromFloat32Shape(values, &.{ @intCast(batch * seq), dim });
                defer cb.free(hidden);
                const logits = try cb.fromFloat32Shape(scores, &.{ @intCast(batch * count), 1 });
                defer cb.free(logits);
                const gathered = try cb.embeddingLookup(hidden, indices, batch * count, dim);
                defer cb.free(gathered);
                const gathered_host = try cb.toFloat32(gathered, a);
                defer a.free(gathered_host);
                for (indices, 0..) |index, row| try std.testing.expectEqualSlices(f32, values[@as(usize, @intCast(index)) * dim ..][0..dim], gathered_host[row * dim ..][0..dim]);
                const features = (try cb.layaActionFeatures(&.{ .hidden = hidden, .logits = logits, .markers = markers, .batch = batch, .sequence = seq, .options = count, .hidden_size = dim })).?;
                defer cb.free(features);
                const host = try cb.toFloat32(features, a);
                defer a.free(host);
                for (0..batch) |row| {
                    const actual = host[row * (dim + 4) ..][0 .. dim + 4];
                    try std.testing.expectEqualSlices(f32, values[row * seq * dim ..][0..dim], actual[0..dim]);
                    for ([_]f32{ 0.5, 0, @log(@as(f32, 2)) / @log(@as(f32, @floatFromInt(if (padded and count > 2) count - 1 else count))), @as(f32, @floatFromInt(if (padded and count > 2) count - 1 else count)) / 255 }, actual[dim..]) |want, got| try std.testing.expectApproxEqAbs(want, got, 2e-6);
                }
                markers[0] = -2;
                try std.testing.expectError(error.InvalidLayaInputs, cb.layaActionFeatures(&.{ .hidden = hidden, .logits = logits, .markers = markers, .batch = batch, .sequence = seq, .options = count, .hidden_size = dim }));
            }
        }
    }
}

test "laya CUDA warp attention matches legacy at shape and window boundaries" {
    if (comptime !build_options.enable_cuda) return requireCuda();
    try requireCuda();
    const a = std.testing.allocator;
    var gpu = try cuda.CudaCompute.init(a);
    defer gpu.deinit();
    const cb = gpu.computeBackend();
    for ([_]usize{ 64, 128 }) |dim| {
        for ([_]usize{ 1, 31, 32, 33, 63, 64, 65, 127, 128, 129, 511, 512 }) |seq| {
            const batch = 2;
            const heads = 2;
            const values = try a.alloc(f32, batch * seq * heads * dim);
            defer a.free(values);
            for (values, 0..) |*v, i| v.* = @sin(@as(f32, @floatFromInt(i)) * 0.13);
            const q = try cb.fromFloat32Shape(values, &.{ @intCast(batch * seq), @intCast(heads * dim) });
            defer cb.free(q);
            for (values, 0..) |*v, i| v.* = @cos(@as(f32, @floatFromInt(i)) * 0.07);
            const k = try cb.fromFloat32Shape(values, &.{ @intCast(batch * seq), @intCast(heads * dim) });
            defer cb.free(k);
            for (values, 0..) |*v, i| v.* = @sin(@as(f32, @floatFromInt(i)) * 0.037);
            const v = try cb.fromFloat32Shape(values, &.{ @intCast(batch * seq), @intCast(heads * dim) });
            defer cb.free(v);
            const mask = try a.alloc(i64, batch * seq);
            defer a.free(mask);
            @memset(mask, 1);
            @memset(mask[seq..], 0);
            for ([_]usize{ 0, 1, 64, 512 }) |radius| {
                gpu.laya_optimizations = false;
                const legacy = (try cb.encoderLocalAttention(q, k, v, mask, batch, seq, heads, dim, radius)).?;
                defer cb.free(legacy);
                gpu.laya_optimizations = true;
                const before = gpu.snapshotStats().laya_warp_attention;
                const fast = if (radius == 512)
                    try cb.scaledDotProductAttention(q, k, v, mask, null, batch, seq, heads, dim)
                else
                    (try cb.encoderLocalAttention(q, k, v, mask, batch, seq, heads, dim, radius)).?;
                defer cb.free(fast);
                try std.testing.expectEqual(before + 1, gpu.snapshotStats().laya_warp_attention);
                const expected = try cb.toFloat32(legacy, a);
                defer a.free(expected);
                const actual = try cb.toFloat32(fast, a);
                defer a.free(actual);
                for (expected, actual) |want, got| try std.testing.expectApproxEqAbs(want, got, 2e-4);
                for (actual[seq * heads * dim ..]) |got| try std.testing.expectEqual(@as(f32, 0), got);
            }
        }
    }
}

test "laya CUDA packed exact GELU matches unfused projection slices" {
    if (comptime !build_options.enable_cuda) return requireCuda();
    try requireCuda();
    const a = std.testing.allocator;
    var gpu = try cuda.CudaCompute.init(a);
    defer gpu.deinit();
    const cb = gpu.computeBackend();
    for ([_]usize{ 1, 7, 128 }) |rows| {
        for ([_]usize{ 1, 31, 32, 33, 2624 }) |width| {
            const values = try a.alloc(f32, rows * width * 2);
            defer a.free(values);
            for (values, 0..) |*v, i| v.* = @sin(@as(f32, @floatFromInt(i)) * 0.13) * 9;
            values[0] = std.math.inf(f32);
            const input = try cb.fromFloat32Shape(values, &.{ @intCast(rows), @intCast(width * 2) });
            defer cb.free(input);
            const gate = try cb.sliceLastDim(input, 0, width);
            defer cb.free(gate);
            const up = try cb.sliceLastDim(input, width, 2 * width);
            defer cb.free(up);
            const activated = (try cb.geluExact(gate)).?;
            defer cb.free(activated);
            const reference = try cb.multiply(activated, up);
            defer cb.free(reference);
            gpu.laya_fusion = false;
            try std.testing.expect(try cb.packedGegluExact(input, rows, width) == null);
            gpu.laya_fusion = true;
            const before = gpu.snapshotStats().laya_packed_geglu;
            const output = (try cb.packedGegluExact(input, rows, width)).?;
            defer cb.free(output);
            try std.testing.expectEqual(before + 1, gpu.snapshotStats().laya_packed_geglu);
            const expected = try cb.toFloat32(reference, a);
            defer a.free(expected);
            const actual = try cb.toFloat32(output, a);
            defer a.free(actual);
            for (expected, actual) |want, got| try std.testing.expectApproxEqAbs(want, got, 1e-5);
        }
    }
}
