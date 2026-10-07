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

//! Hardware differentials. Explicit CUDA selection never skips missing hardware.
const std = @import("std");
const build_options = @import("build_options");
const support = @import("../util/laya_test_support.zig");
const native = @import("../ops/native_compute.zig");
const ops = @import("../ops/ops.zig");
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

test "GLiNER CUDA fused QKV RoPE preserves split-half rotations and batch positions" {
    if (comptime !build_options.enable_cuda) return requireCuda();
    try requireCuda();
    const a = std.testing.allocator;
    var gpu = try cuda.CudaCompute.init(a);
    defer gpu.deinit();
    const cb = gpu.computeBackend();
    const Shape = struct { batch: usize, seq: usize, dim: usize };
    for ([_]Shape{ .{ .batch = 3, .seq = 17, .dim = 64 }, .{ .batch = 2, .seq = 513, .dim = 128 }, .{ .batch = 1, .seq = 7999, .dim = 64 } }) |shape| {
        const heads = 2;
        const rows = shape.batch * shape.seq;
        const hidden = heads * shape.dim;
        const values = try a.alloc(f32, rows * 3 * hidden);
        defer a.free(values);
        for (values, 0..) |*value, i| value.* = @sin(@as(f32, @floatFromInt(i)) * 0.13);
        const input = try cb.fromFloat32Shape(values, &.{ @intCast(rows), @intCast(3 * hidden) });
        defer cb.free(input);
        gpu.gliner_encoder_attention = false;
        try std.testing.expect(try cb.splitQkvRope(input, shape.batch, shape.seq, heads, shape.dim, 160000) == null);
        gpu.gliner_encoder_attention = true;
        try std.testing.expectError(error.InvalidShape, cb.splitQkvRope(input, shape.batch, shape.seq, heads, shape.dim, 0));
        try std.testing.expectError(error.InvalidShape, cb.splitQkvRope(input, shape.batch, shape.seq, heads, shape.dim - 1, 160000));
        for ([_]f32{ 10000, 160000 }) |theta| {
            const parts = (try cb.splitQkvRope(input, shape.batch, shape.seq, heads, shape.dim, theta)).?;
            const actual_parts = [_]ops.CT{ parts.first, parts.second, parts.third };
            defer for (actual_parts) |part| cb.free(part);
            for (actual_parts, 0..) |part, index| {
                const slice = try cb.sliceLastDim(input, index * hidden, (index + 1) * hidden);
                defer cb.free(slice);
                const expected = if (index < 2) try cb.rope(slice, shape.seq, shape.dim, shape.dim, theta, 1, 0, false) else slice;
                defer if (index < 2) cb.free(expected);
                const want = try cb.toFloat32(expected, a);
                defer a.free(want);
                const got = try cb.toFloat32(part, a);
                defer a.free(got);
                try std.testing.expectEqualSlices(f32, want, got);
            }
        }
    }
}

test "GLiNER CUDA long attention respects global local and fully masked rows" {
    if (comptime !build_options.enable_cuda) return requireCuda();
    try requireCuda();
    const a = std.testing.allocator;
    var gpu = try cuda.CudaCompute.init(a);
    defer gpu.deinit();
    gpu.gliner_encoder_attention = true;
    const cb = gpu.computeBackend();
    for ([_]usize{ 513, 2048, 7999 }) |seq| {
        const dim = 64;
        const values = try a.alloc(f32, 2 * seq * dim);
        defer a.free(values);
        @memset(values, 0);
        const qk = try cb.fromFloat32Shape(values, &.{ @intCast(2 * seq), dim });
        defer cb.free(qk);
        for (values, 0..) |*value, i| value.* = @as(f32, @floatFromInt((i / dim) % seq)) * 0.001 + @as(f32, @floatFromInt(i % dim)) * 0.005;
        const v = try cb.fromFloat32Shape(values, &.{ @intCast(2 * seq), dim });
        defer cb.free(v);
        const mask = try a.alloc(i64, 2 * seq);
        defer a.free(mask);
        @memset(mask, 0);
        @memset(mask[0 .. seq - 17], 1);
        for ([_]usize{ 64, seq }) |radius| {
            const result = if (radius == seq)
                try cb.scaledDotProductAttention(qk, qk, v, mask, null, 2, seq, 1, dim)
            else
                (try cb.encoderLocalAttention(qk, qk, v, mask, 2, seq, 1, dim, radius)).?;
            defer cb.free(result);
            const actual = try cb.toFloat32(result, a);
            defer a.free(actual);
            for (0..seq) |row| {
                const begin = row -| radius;
                const end = @min(seq - 17, row + radius + 1);
                for (0..dim) |d| {
                    const expected: f32 = if (end > begin) @as(f32, @floatFromInt(begin + end - 1)) * 0.0005 + @as(f32, @floatFromInt(d)) * 0.005 else 0;
                    try std.testing.expectApproxEqAbs(expected, actual[row * dim + d], 2e-4);
                }
            }
            for (actual[seq * dim ..]) |value| try std.testing.expectEqual(@as(f32, 0), value);
        }
    }
}

test "GLiNER CUDA mixed attention matches masked FP32 reference across tile tails" {
    if (comptime !build_options.enable_cuda) return requireCuda();
    try requireCuda();
    const a = std.testing.allocator;
    var gpu = try cuda.CudaCompute.init(a);
    defer gpu.deinit();
    gpu.gliner_encoder_attention = true;
    gpu.gliner_mixed_attention = true;
    const cb = gpu.computeBackend();
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var cpu = native.NativeCompute.init(a, &store, null);
    defer cpu.deinit();
    const ref = cpu.computeBackend();
    for ([_]usize{ 1, 31, 33, 65, 129, 513, 2049 }) |seq| {
        const hidden = 2 * 64;
        const values = try a.alloc(f32, 3 * seq * hidden);
        defer a.free(values);
        for (values, 0..) |*value, i| value.* = @sin(@as(f32, @floatFromInt(i)) * 0.13);
        const q = try cb.fromFloat32Shape(values, &.{ @intCast(3 * seq), hidden });
        defer cb.free(q);
        const rq = try ref.fromFloat32Shape(values, &.{ @intCast(3 * seq), hidden });
        defer ref.free(rq);
        for (values, 0..) |*value, i| value.* = @cos(@as(f32, @floatFromInt(i)) * 0.17);
        const k = try cb.fromFloat32Shape(values, &.{ @intCast(3 * seq), hidden });
        defer cb.free(k);
        const rk = try ref.fromFloat32Shape(values, &.{ @intCast(3 * seq), hidden });
        defer ref.free(rk);
        for (values, 0..) |*value, i| value.* = @sin(@as(f32, @floatFromInt(i)) * 0.19);
        const v = try cb.fromFloat32Shape(values, &.{ @intCast(3 * seq), hidden });
        defer cb.free(v);
        const rv = try ref.fromFloat32Shape(values, &.{ @intCast(3 * seq), hidden });
        defer ref.free(rv);
        const mask = try a.alloc(i64, 3 * seq);
        defer a.free(mask);
        @memset(mask, 1);
        @memset(mask[seq -| 3 .. 2 * seq], 0);
        const result = try cb.scaledDotProductAttention(q, k, v, mask, null, 3, seq, 2, 64);
        defer cb.free(result);
        const expected = try ref.scaledDotProductAttention(rq, rk, rv, mask, null, 3, seq, 2, 64);
        defer ref.free(expected);
        const actual = try cb.toFloat32(result, a);
        defer a.free(actual);
        const oracle = try ref.toFloat32(expected, a);
        defer a.free(oracle);
        for (actual, oracle, 0..) |got, want, i| {
            try std.testing.expect(std.math.isFinite(got));
            if (mask[i / hidden] == 1) try std.testing.expectApproxEqAbs(want, got, 5e-4);
            if (i / hidden >= seq and i / hidden < 2 * seq) try std.testing.expectEqual(@as(f32, 0), got);
        }
        for ([_]usize{ 0, 1, 64, seq + 7 }) |radius| {
            const local = (try cb.encoderLocalAttention(q, k, v, mask, 3, seq, 2, 64, radius)).?;
            defer cb.free(local);
            // Compare with the separate FP32 CUDA kernel, including padded
            // queries whose local key window is completely masked.
            gpu.gliner_mixed_attention = false;
            const full_precision = (try cb.encoderLocalAttention(q, k, v, mask, 3, seq, 2, 64, radius)).?;
            defer cb.free(full_precision);
            gpu.gliner_mixed_attention = true;
            const got = try cb.toFloat32(local, a);
            defer a.free(got);
            const want = try cb.toFloat32(full_precision, a);
            defer a.free(want);
            for (got, want) |x, y| try std.testing.expectApproxEqAbs(y, x, 5e-4);
        }
    }
}

test "GLiNER CUDA boundary matrix mirrors preserve source identity and task heads" {
    if (comptime !build_options.enable_cuda) return requireCuda();
    try requireCuda();
    const a = std.testing.allocator;
    const Tensor = @import("../backends/tensor.zig").Tensor;
    var gpu = try cuda.CudaCompute.init(a);
    defer gpu.deinit();
    gpu.strict_f32_weights = true;
    gpu.gliner_boundary_inference = true;
    gpu.gliner_mixed_attention = true;
    const cb = gpu.computeBackend();
    var data: [16 * 16]f32 = @splat(0);
    for (0..16) |i| data[i * 16 + i] = 1.0001;
    var source = try Tensor.initFloat32(a, "", &.{ 16, 16 }, &data);
    defer source.deinit();
    const encoder = "encoder.layer.0.attention.self.query_proj.weight";
    const head = "classifier.0.weight";
    const embedding = "embeddings.word_embeddings.weight";
    for ([_][]const u8{ encoder, head, embedding }) |name| {
        try gpu.insertWeightFromTensor(try a.dupe(u8, name), &source);
        try gpu.prepareGlinerBoundaryF16Mirror(name, &source);
    }
    try std.testing.expectEqual(@as(u32, 1), gpu.gliner_boundary_f16_mirrors.count());
    try std.testing.expectError(error.DuplicateWeight, gpu.prepareGlinerBoundaryF16Mirror(encoder, &source));
    const input = try cb.fromFloat32Shape(&@as([32]f32, @splat(2)), &.{ 2, 16 });
    defer cb.free(input);
    const bias = try cb.fromFloat32Shape(&@as([16]f32, @splat(0.125)), &.{16});
    defer cb.free(bias);
    for ([_][]const u8{ encoder, head, embedding }) |name| {
        const resident = gpu.resident_weights.getPtr(name).?;
        try std.testing.expectEqual(@import("../backends/tensor.zig").DType.f32, resident.dtype);
        const expected: f32 = if (std.mem.eql(u8, name, encoder)) 2 else 2.0002;
        const output = try cb.linear(input, @ptrCast(resident), bias, 2, 16, 16);
        defer cb.free(output);
        const got = try cb.toFloat32(output, a);
        defer a.free(got);
        for (got) |value| try std.testing.expectApproxEqAbs(expected + 0.125, value, 2e-6);
        const no_bias = try cb.linearNoBias(input, @ptrCast(resident), 2, 16, 16);
        defer cb.free(no_bias);
        const plain = try cb.toFloat32(no_bias, a);
        defer a.free(plain);
        for (plain) |value| try std.testing.expectApproxEqAbs(expected, value, 2e-6);
    }
    // Reusing a mirrored projection must preserve each invocation's FP32 bias.
    const other_bias = try cb.fromFloat32Shape(&@as([16]f32, @splat(-0.03125)), &.{16});
    defer cb.free(other_bias);
    const refreshed = try cb.linear(input, @ptrCast(gpu.resident_weights.getPtr(encoder).?), other_bias, 2, 16, 16);
    defer cb.free(refreshed);
    const refreshed_values = try cb.toFloat32(refreshed, a);
    defer a.free(refreshed_values);
    for (refreshed_values) |value| try std.testing.expectApproxEqAbs(@as(f32, 1.96875), value, 2e-6);
    try std.testing.expectEqualSlices(f32, &data, source.asFloat32());
    gpu.gliner_mixed_attention = false;
    const original = try cb.linearNoBias(input, @ptrCast(gpu.resident_weights.getPtr(encoder).?), 2, 16, 16);
    defer cb.free(original);
    const original_values = try cb.toFloat32(original, a);
    defer a.free(original_values);
    for (original_values) |value| try std.testing.expectApproxEqAbs(@as(f32, 2.0002), value, 2e-6);
    try std.testing.expectError(error.InvalidCudaState, gpu.prepareGlinerBoundaryF16Mirror(encoder, &source));
    gpu.gliner_mixed_attention = true;
    var overflow = try Tensor.initFloat32(a, "", &.{ 1, 1 }, &.{70000});
    defer overflow.deinit();
    const bad_name = "encoder.layer.1.output.dense.weight";
    try gpu.insertWeightFromTensor(try a.dupe(u8, bad_name), &overflow);
    try std.testing.expectError(error.UnsupportedGlinerCudaPrecision, gpu.prepareGlinerBoundaryF16Mirror(bad_name, &overflow));
    try std.testing.expectEqual(@as(u32, 1), gpu.gliner_boundary_f16_mirrors.count());
}
