// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Public ComputeBackend regression coverage for instance-local activation
//! precision. References decode the identical packed weights, then accumulate
//! the original floating inputs independently in f64.
const std = @import("std");
const ops = @import("ops.zig");
const native = @import("native_compute.zig");
const weights = @import("../models/weight_source.zig");
const Tensor = @import("../backends/tensor.zig").Tensor;
const types = @import("../gguf/tensor_types.zig");
const codec = @import("../gguf/quant_codec.zig");
const quant_matmul = @import("../graph/quant_matmul.zig");

const name = "activation_policy.weight";
const width = 768;
const columns = 512;

fn addWeight(a: std.mem.Allocator, store: *native.WeightStore, kind: types.KnownTensorType, values: []const f32) !void {
    const raw = switch (kind) {
        .Q8_0 => try codec.quantizeQ8_0FromF32(a, values),
        .Q4_0 => try codec.quantizeQ4_0FromF32(a, values),
        .Q4_K => try codec.quantizeQ4_KFromF32(a, values),
        .Q5_K => try codec.quantizeQ5_KFromF32(a, values),
        else => unreachable,
    };
    errdefer a.free(raw);
    const shape = try a.dupe(i64, &.{ columns, width });
    errdefer a.free(shape);
    var storage = weights.QuantizedStorage{ .allocator = a, .tensor_type = .{ .known = kind }, .raw_bytes = raw, .shape = shape };
    errdefer storage.prepared.deinit(a);
    try native.prepareNativeQuantizedStorage(&storage);
    var tensor = try Tensor.initFloat32(a, name, &.{ columns, width }, &.{});
    errdefer tensor.deinit();
    const key = try a.dupe(u8, name);
    errdefer a.free(key);
    try store.resident_weights.put(a, key, .{ .tensor = tensor, .quantized = true, .quantized_storage = storage });
}

fn reference(a: std.mem.Allocator, input: []const f32, dense: []const f32, rows: usize) ![]f32 {
    const output = try a.alloc(f32, rows * columns);
    for (0..rows) |row| for (0..columns) |column| {
        var sum: f64 = 0;
        for (input[row * width ..][0..width], dense[column * width ..][0..width]) |x, w| sum += @as(f64, x) * @as(f64, w);
        output[row * columns + column] = @floatCast(sum);
    };
    return output;
}

fn expectProjection(cb: *const ops.ComputeBackend, tensor: ops.CT, expected: []const f32, bias: ?[]const f32, relu: bool) !void {
    const actual = try cb.toFloat32(tensor, std.testing.allocator);
    defer std.testing.allocator.free(actual);
    try std.testing.expectEqual(expected.len, actual.len);
    for (actual, expected, 0..) |got, raw, index| {
        const want = if (relu) @max(0, raw + if (bias) |values| values[index % columns] else 0) else raw + if (bias) |values| values[index % columns] else 0;
        if (!std.math.isFinite(got) or @abs(got - want) > 2e-5 + @abs(want) * 2e-5) {
            std.debug.print("strict native projection element {d}: expected {d}, actual {d}\n", .{ index, want, got });
            return error.TestExpectedEqual;
        }
    }
}

fn checkFormat(kind: types.KnownTensorType) !void {
    const a = std.testing.allocator;
    const source = try a.alloc(f32, columns * width);
    defer a.free(source);
    for (source, 0..) |*value, i| value.* = @sin(@as(f32, @floatFromInt(i % 499)) * 0.11) * 0.07;
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    try addWeight(a, &store, kind, source);
    var strict = native.NativeCompute.init(a, &store, null);
    defer strict.deinit();
    var automatic = native.NativeCompute.init(a, &store, null);
    defer automatic.deinit();
    strict.quantized_activation_policy = .strict_f32;
    const cb = strict.computeBackend();
    const automatic_cb = automatic.computeBackend();
    const weight = try cb.getWeight(name);
    defer cb.free(weight);
    const automatic_weight = try automatic_cb.getWeight(name);
    defer automatic_cb.free(automatic_weight);
    try std.testing.expectEqual(native.QuantizedActivationPolicy.automatic, automatic.quantized_activation_policy);
    const storage = &store.resident_weights.getPtr(name).?.quantized_storage.?;
    const dense = try a.alloc(f32, source.len);
    defer a.free(dense);
    try codec.dequantizeToFloat32(.{ .known = kind }, storage.raw_bytes, dense);
    var bias_values: [columns]f32 = undefined;
    for (&bias_values, 0..) |*value, i| value.* = @as(f32, @floatFromInt(i % 7)) * 0.03;
    const bias = try cb.fromFloat32(&bias_values);
    defer cb.free(bias);

    for ([_]usize{ 1, 3, 129, 197 }) |rows| {
        if (rows == 197 and kind != .Q4_K and kind != .Q5_K) continue;
        errdefer std.debug.print("strict native activation format={s} rows={d} in={d} out={d}\n", .{ @tagName(kind), rows, width, columns });
        const input_values = try a.alloc(f32, rows * width);
        defer a.free(input_values);
        for (input_values, 0..) |*value, i| value.* = @cos(@as(f32, @floatFromInt(i % 277)) * 0.19) * 0.6;
        const input = try cb.fromFloat32Shape(input_values, &.{ @intCast(rows), width });
        defer cb.free(input);
        const expected = try reference(a, input_values, dense, rows);
        defer a.free(expected);
        const single = try cb.linearNoBias(input, weight, rows, width, columns);
        defer cb.free(single);
        try expectProjection(&cb, single, expected, null, false);
        if (rows == 1) {
            // Changing one backend instance must leave another instance's
            // existing activation-quantized dispatch numerically unchanged.
            const other = try automatic_cb.linearNoBias(input, automatic_weight, rows, width, columns);
            defer automatic_cb.free(other);
            const actual_other = try automatic_cb.toFloat32(other, a);
            defer a.free(actual_other);
            var maximum_difference: f32 = 0;
            for (actual_other, expected) |value, want| maximum_difference = @max(maximum_difference, @abs(value - want));
            try std.testing.expect(maximum_difference > 1e-4);
            const repeated = try cb.linearNoBias(input, weight, rows, width, columns);
            defer cb.free(repeated);
            try expectProjection(&cb, repeated, expected, null, false);
        }
        if (rows != 3) continue;
        const biased = try cb.linear(input, weight, bias, rows, width, columns);
        defer cb.free(biased);
        try expectProjection(&cb, biased, expected, &bias_values, false);
        const pair = try cb.linearNoBiasPair(input, weight, weight, rows, width, columns);
        defer cb.free(pair.first);
        defer cb.free(pair.second);
        try expectProjection(&cb, pair.first, expected, null, false);
        try expectProjection(&cb, pair.second, expected, null, false);
        const pair_bias = try cb.linearPair(input, weight, bias, weight, bias, rows, width, columns);
        defer cb.free(pair_bias.first);
        defer cb.free(pair_bias.second);
        try expectProjection(&cb, pair_bias.first, expected, &bias_values, false);
        try expectProjection(&cb, pair_bias.second, expected, &bias_values, false);
        const triple = try cb.linearTriple(input, weight, bias, weight, bias, weight, bias, rows, width, columns);
        defer cb.free(triple.first);
        defer cb.free(triple.second);
        defer cb.free(triple.third);
        try expectProjection(&cb, triple.first, expected, &bias_values, false);
        try expectProjection(&cb, triple.second, expected, &bias_values, false);
        try expectProjection(&cb, triple.third, expected, &bias_values, false);
        const pair_relu = try cb.linearPairRelu(input, weight, bias, weight, bias, rows, width, columns);
        defer cb.free(pair_relu.first);
        defer cb.free(pair_relu.second);
        try expectProjection(&cb, pair_relu.first, expected, &bias_values, true);
        try expectProjection(&cb, pair_relu.second, expected, &bias_values, true);
        const format = @import("../graph/backend_contracts.zig").quantFormatFromGgufTensorType(.{ .known = kind }).?;
        const planned: ops.OperatorPlan = .{ .quant_matmul = quant_matmul.plan(.{ .rows = rows, .in_dim = width, .out_dim = columns, .format = format }) };
        const planned_plain = try cb.linearNoBiasWithPlan(input, weight, rows, width, columns, planned);
        defer cb.free(planned_plain);
        try expectProjection(&cb, planned_plain, expected, null, false);
        const planned_bias = try cb.linearWithPlan(input, weight, bias, rows, width, columns, planned);
        defer cb.free(planned_bias);
        try expectProjection(&cb, planned_bias, expected, &bias_values, false);
        // Exercise the actual fused adapter with a nonzero rank-two residual.
        var lora_a_values: [2 * width]f32 = undefined;
        var lora_b_values: [2 * columns]f32 = undefined;
        for (&lora_a_values, 0..) |*value, i| value.* = @as(f32, @floatFromInt(i % 13)) * 0.001;
        for (&lora_b_values, 0..) |*value, i| value.* = @as(f32, @floatFromInt(i % 11)) * 0.002;
        const lora_a = try cb.fromFloat32(&lora_a_values);
        defer cb.free(lora_a);
        const lora_b = try cb.fromFloat32(&lora_b_values);
        defer cb.free(lora_b);
        const adapted = try cb.linearLoRA(input, weight, bias, lora_a, lora_b, 0.5, 2, rows, width, columns);
        defer cb.free(adapted);
        const adapted_expected = try a.dupe(f32, expected);
        defer a.free(adapted_expected);
        for (0..rows) |row| for (0..columns) |column| {
            var residual: f64 = 0;
            for (0..2) |rank| {
                var activation: f64 = 0;
                for (input_values[row * width ..][0..width], lora_a_values[rank * width ..][0..width]) |x, w| activation += @as(f64, x) * @as(f64, w);
                residual += activation * @as(f64, lora_b_values[column * 2 + rank]);
            }
            adapted_expected[row * columns + column] += @floatCast(residual * 0.5);
        };
        try expectProjection(&cb, adapted, adapted_expected, &bias_values, false);
        const unchanged_input = try cb.toFloat32(input, a);
        defer a.free(unchanged_input);
        try std.testing.expectEqualSlices(f32, input_values, unchanged_input);
        try std.testing.expectEqual(@as(usize, 0), store.resident_weights.getPtr(name).?.tensor.data.len);
    }
}

test "native strict f32 activations preserve Q8 Q4 and K weight projections across shapes" {
    for ([_]types.KnownTensorType{ .Q8_0, .Q4_0, .Q4_K, .Q5_K }) |kind| try checkFormat(kind);
}
