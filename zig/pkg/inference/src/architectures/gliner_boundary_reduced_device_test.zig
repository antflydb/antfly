// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const build_options = @import("build_options");
const ops = @import("../ops/ops.zig");
const math_mod = @import("gliner_boundary_device_math.zig");
const gpu_store = @import("../ops/gpu_hosted_store.zig");
const metal = @import("../ops/metal_compute.zig");
const tensor_mod = @import("../backends/tensor.zig");
const gguf = @import("../gguf/tensor_types.zig");
const codec = @import("../gguf/quant_codec.zig");
const parity = @import("gliner_boundary_parity_test.zig");
const Precision = ops.gliner_boundary_device.WeightPrecision;

test "gliner boundary device reduced weight ownership survives failed preparation" {
    const a = std.testing.allocator;
    const native = @import("../ops/native_compute.zig");
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var backend = native.NativeCompute.init(a, &store, null);
    defer backend.deinit();
    var cb = backend.computeBackend();
    const Fake = struct {
        fn execute(ctx: *anyopaque, request: *const ops.gliner_boundary_device.Request) anyerror!ops.CT {
            const native_backend: *native.NativeCompute = @ptrCast(@alignCast(ctx));
            const original = native_backend.computeBackend();
            return switch (request.*) {
                .load_matrix => original.fromFloat32Shape(&.{1}, &.{1}),
                .embedding_reduced => error.UnsupportedGlinerBoundaryDevice,
                else => error.UnexpectedDeviceOperation,
            };
        }
        fn check(allocator: std.mem.Allocator, backend_: *const ops.ComputeBackend) !void {
            const math = try math_mod.Context.create(allocator, backend_, .{}, null);
            defer math.destroy();
            math.encoder_precision = .q8_0;
            _ = math.embedding(&.{0}, 2, 32) catch |err| switch (err) {
                error.UnsupportedGlinerBoundaryDevice => return,
                else => return err,
            };
            return error.ExpectedUnsupportedDevice;
        }
    };
    var vt = cb.vtable.*;
    vt.glinerBoundaryDevice = Fake.execute;
    cb.vtable = &vt;
    try std.testing.checkAllAllocationFailures(a, Fake.check, .{&cb});
}

fn tensor(a: std.mem.Allocator, name: []const u8, data: []u8, dtype: tensor_mod.DType, shape: []const i64) tensor_mod.Tensor {
    return .{ .allocator = a, .name = name, .data = data, .dtype = dtype, .shape = shape, .owns_data = false, .owns_shape = false };
}

fn encoded(a: std.mem.Allocator, values: []const f32, precision: Precision) ![]u8 {
    return switch (precision) {
        .f16 => blk: {
            const out = try a.alloc(u8, values.len * 2);
            for (values, 0..) |value, i| std.mem.writeInt(u16, out[i * 2 ..][0..2], @bitCast(@as(f16, @floatCast(value))), .little);
            break :blk out;
        },
        .q8_0 => codec.quantizeQ8_0FromF32(a, values),
        .q4_0 => codec.quantizeQ4_0FromF32(a, values),
        .q4_k => codec.quantizeQ4_KFromF32(a, values),
        .f32 => error.InvalidInput,
    };
}

fn quantType(precision: Precision) gguf.TensorType {
    return .{ .known = switch (precision) {
        .q8_0 => .Q8_0,
        .q4_0 => .Q4_0,
        .q4_k => .Q4_K,
        else => unreachable,
    } };
}

test "gliner boundary device Metal reduced linear and embedding preserve native bytes" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!@import("../backends/metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    const width = 256;
    const out_dim = 64;
    const weight_name = "encoder.layer.0.attention.self.query_proj.weight";
    const bias_name = "encoder.layer.0.attention.self.query_proj.bias";
    const embedding_name = "embeddings.word_embeddings.weight";
    const values = try a.alloc(f32, out_dim * width);
    defer a.free(values);
    for (values, 0..) |*value, i| value.* = @sin(@as(f32, @floatFromInt(i % 499)) * 0.11) * 0.07;
    var biases: [out_dim]f32 = undefined;
    for (&biases, 0..) |*value, i| value.* = @as(f32, @floatFromInt(i % 7)) * 0.03;
    for ([_]Precision{ .f16, .q8_0, .q4_0, .q4_k }) |precision| {
        errdefer std.debug.print("reduced device precision: {s}\n", .{@tagName(precision)});
        const raw = try encoded(a, values, precision);
        defer a.free(raw);
        const reference_weight = try a.alloc(f32, values.len);
        defer a.free(reference_weight);
        if (precision == .f16) {
            for (reference_weight, 0..) |*value, i| value.* = @floatCast(@as(f16, @bitCast(std.mem.readInt(u16, raw[i * 2 ..][0..2], .little))));
        } else try codec.dequantizeToFloat32(quantType(precision), raw, reference_weight);
        var store = gpu_store.WeightStore{ .allocator = a, .prefix = "", .lazy_weights = .empty, .prefer_f32_dense_tensors = true };
        defer store.lazy_weights.deinit(a);
        for ([_][]const u8{ weight_name, embedding_name }) |name| {
            const entry: gpu_store.LazyWeightEntry = if (precision == .f16)
                .{ .tensor_ref = .{ .name = name }, .host_loaded = .{ .tensor = tensor(a, name, raw, .f16, &.{ out_dim, width }) }, .active_tier = .host }
            else
                .{ .tensor_ref = .{ .name = name }, .quantized_storage = .{ .allocator = a, .tensor_type = quantType(precision), .raw_bytes = raw, .shape = &.{ out_dim, width }, .raw_owned = false }, .active_tier = .host };
            try store.lazy_weights.put(a, name, entry);
        }
        try store.lazy_weights.put(a, bias_name, .{ .tensor_ref = .{ .name = bias_name }, .host_loaded = .{ .tensor = tensor(a, bias_name, std.mem.sliceAsBytes(&biases), .f32, &.{out_dim}) }, .active_tier = .host });
        metal.initPrefetchQueue(&store, a);
        defer metal.deinitPrefetchQueue(&store);
        defer metal.deinitSharedNativeProvider(&store);
        var backend = try metal.MetalCompute.init(a, &store, null);
        defer backend.deinit();
        const cb = backend.computeBackend();
        const math = try math_mod.Context.create(a, &cb, .{}, null);
        defer math.destroy();
        math.encoder_precision = precision;
        const wrong: Precision = if (precision == .f16) .q8_0 else .f16;
        try std.testing.expectError(error.InvalidGlinerBoundaryTensorPrecision, cb.glinerBoundaryDevice(&.{ .load_matrix = .{ .name = weight_name, .rows = out_dim, .columns = width, .precision = wrong } }));
        for ([_]usize{ 1, 3, 129 }) |rows| {
            errdefer std.debug.print("boundary reduced linear precision={s} rows={d} in={d} out={d}\n", .{ @tagName(precision), rows, width, out_dim });
            const input_values = try a.alloc(f32, rows * width);
            defer a.free(input_values);
            for (input_values, 0..) |*value, i| value.* = @cos(@as(f32, @floatFromInt(i % 277)) * 0.19) * 0.6;
            const input = try math.execute(.{ .upload_f32 = .{ .values = input_values, .shape = &.{ @intCast(rows), width } } }, input_values.len);
            defer math.drop(input);
            const result = try math.linear(input, rows, width, out_dim, "encoder.layer.0.attention.self.query_proj");
            defer math.drop(result);
            try std.testing.expectEqual(@as(usize, 0), math.stats.proposal_download_bytes);
            const actual = try math.download(result, rows * out_dim, false);
            defer a.free(actual);
            const expected = try a.alloc(f32, actual.len);
            defer a.free(expected);
            for (0..rows) |r| for (0..out_dim) |o| {
                var sum = biases[o];
                for (0..width) |k| sum += input_values[r * width + k] * reference_weight[o * width + k];
                expected[r * out_dim + o] = sum;
            };
            try parity.expectFloats(expected, actual, 3e-4, 3e-5);
        }
        const ids = [_]i64{ 0, 63, 17, 17, 1 };
        const embedded = try math.embedding(&ids, out_dim, width);
        defer math.drop(embedded);
        const actual = try math.download(embedded, ids.len * width, false);
        defer a.free(actual);
        for (ids, 0..) |id, row| try parity.expectFloats(reference_weight[@as(usize, @intCast(id)) * width ..][0..width], actual[row * width ..][0..width], 1e-7, 1e-7);
        try std.testing.expectEqual(2 * raw.len + 2 * out_dim * 4, math.stats.charged_weight_bytes);
        try std.testing.expectEqual(@as(usize, ids.len * 4), math.stats.metadata_upload_bytes);
        const checkpoint = math.stats.result_download_bytes;
        try std.testing.expectError(error.InvalidBoundaryRouting, math.embedding(&.{64}, out_dim, width));
        try std.testing.expectEqual(checkpoint, math.stats.result_download_bytes);
    }
}
