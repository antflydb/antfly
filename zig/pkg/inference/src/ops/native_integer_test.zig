// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const native = @import("native_compute.zig");
const ops = @import("ops.zig");
const tensor = @import("../backends/tensor.zig");

fn expectIntegers(compute: *native.NativeCompute, value: ops.CT, expected: []const i64) !void {
    const actual = try compute.toInt64(value, std.testing.allocator);
    defer std.testing.allocator.free(actual);
    try std.testing.expectEqualSlices(i64, expected, actual);
}

test "native integer tensors preserve exact storage through clone reshape gather and export" {
    const a = std.testing.allocator;
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    const value = (try cb.fromInt32Shape(&.{ 16777217, -16777217, 2147483647, -2147483648 }, &.{ 2, 2 })).?;
    defer cb.free(value);
    try std.testing.expectEqual(tensor.DType.i32, try cb.tensorDType(value));
    const exported = (try cb.exportTensorData(value, a)).?;
    defer a.free(exported.payload.bytes);
    try std.testing.expectEqual(tensor.DType.i32, exported.dtype);
    const exported_values = std.mem.bytesAsSlice(i32, exported.payload.bytes);
    try std.testing.expectEqual(@as(usize, 4), exported_values.len);
    for ([_]i32{ 16777217, -16777217, 2147483647, -2147483648 }, exported_values) |expected, actual|
        try std.testing.expectEqual(expected, actual);
    const clone = (try cb.cloneTensorShape(value, &.{4})).?;
    defer cb.free(clone);
    const reshaped = try cb.primReshape(clone, &.{ 1, -1 });
    defer cb.free(reshaped);
    try std.testing.expectEqual(tensor.DType.i32, try cb.tensorDType(reshaped));
    try expectIntegers(&compute, reshaped, &.{ 16777217, -16777217, 2147483647, -2147483648 });
    const index = (try cb.fromInt32Shape(&.{ 1, 0 }, &.{2})).?;
    defer cb.free(index);
    const selected = try cb.primGather(value, index, 0, &.{ 2, 2 });
    defer cb.free(selected);
    try expectIntegers(&compute, selected, &.{ 2147483647, -2147483648, 16777217, -16777217 });
    // i64 payloads preserve integers beyond f64's exact range as well.
    const wide = try compute.importOwnedStaticTensor(try tensor.Tensor.initInt64(a, "", &.{2}, &.{ 9007199254740993, -9007199254740993 }));
    defer cb.free(wide);
    const wide_clone = (try cb.cloneTensorShape(wide, &.{ 1, 2 })).?;
    defer cb.free(wide_clone);
    try expectIntegers(&compute, wide_clone, &.{ 9007199254740993, -9007199254740993 });
    const empty = (try cb.fromInt32Shape(&.{}, &.{0})).?;
    defer cb.free(empty);
    try expectIntegers(&compute, empty, &.{});
    try std.testing.expectError(error.InvalidShape, cb.fromInt32Shape(&.{1}, &.{2}));
    try std.testing.expectError(error.InvalidShape, cb.fromInt32Shape(&.{1}, &.{-1}));
}

test "native integer gather distinguishes adjacent indices above the f32 exact range" {
    const a = std.testing.allocator;
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    // One64MiB owned table, no duplicate host mirror. Values at these adjacent
    // indices would become indistinguishable if indices traveled through f32.
    const count = 16777219;
    const values = try a.alloc(f32, count);
    @memset(values, 0);
    values[16777216] = 11;
    values[16777217] = 29;
    const host = tensor.Tensor.initFloat32Owned(a, "", &.{ count, 1 }, values) catch |err| {
        a.free(values);
        return err;
    };
    const input = try compute.importOwnedStaticTensor(host);
    defer cb.free(input);
    const indices = (try cb.fromInt32Shape(&.{ 16777216, 16777217 }, &.{2})).?;
    defer cb.free(indices);
    const selected = try cb.primGather(input, indices, 0, &.{ count, 1 });
    defer cb.free(selected);
    const actual = try cb.toFloat32(selected, a);
    defer a.free(actual);
    try std.testing.expectEqualSlices(f32, &.{ 11, 29 }, actual);
}

test "native integer gather bounds and repeated scatter gradients are exact" {
    const a = std.testing.allocator;
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    const input = try cb.fromFloat32Shape(&.{ 1, 2, 3, 4, 5, 6 }, &.{ 3, 2 });
    defer cb.free(input);
    const negative = (try cb.fromInt32Shape(&.{-1}, &.{1})).?;
    defer cb.free(negative);
    const selected = try cb.primGather(input, negative, 0, &.{ 3, 2 });
    defer cb.free(selected);
    const last = try cb.toFloat32(selected, a);
    defer a.free(last);
    try std.testing.expectEqualSlices(f32, &.{ 5, 6 }, last);
    const invalid = try compute.importOwnedStaticTensor(try tensor.Tensor.initInt64(a, "", &.{2}, &.{ -4, 9007199254740993 }));
    defer cb.free(invalid);
    try std.testing.expectError(error.IndexOutOfBounds, cb.primGather(input, invalid, 0, &.{ 3, 2 }));
    const repeated = (try cb.fromInt32Shape(&.{ 2, 0, 2, -1 }, &.{4})).?;
    defer cb.free(repeated);
    const cotangent = try cb.fromFloat32Shape(&.{ 1, 2, 3, 4, 5, 6, 7, 8 }, &.{ 4, 2 });
    defer cb.free(cotangent);
    const gradient = try cb.primScatterAdd(cotangent, repeated, &.{ 4, 2 }, &.{ 3, 2 }, 0);
    defer cb.free(gradient);
    const actual = try cb.toFloat32(gradient, a);
    defer a.free(actual);
    try std.testing.expectEqualSlices(f32, &.{ 3, 4, 0, 0, 13, 16 }, actual);
    const shape = try cb.tensorShape(gradient, a);
    defer a.free(shape);
    try std.testing.expectEqualSlices(i64, &.{ 3, 2 }, shape);
    try std.testing.expectError(error.UnsupportedPrimitiveOp, cb.primScatterAdd(cotangent, repeated, &.{ 4, 2 }, &.{ 3, 2 }, 1));
}

fn allocationCase(a: std.mem.Allocator) !void {
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    const input = (try cb.fromInt32Shape(&.{ 16777217, 16777219, -16777217 }, &.{3})).?;
    defer cb.free(input);
    const clone = (try cb.cloneTensorShape(input, &.{ 1, 3 })).?;
    defer cb.free(clone);
    const reshaped = try cb.primReshape(clone, &.{ 3, 1 });
    defer cb.free(reshaped);
    const indices = (try cb.fromInt32Shape(&.{ 2, 0 }, &.{2})).?;
    defer cb.free(indices);
    const gathered = try cb.primGather(reshaped, indices, 0, &.{ 3, 1 });
    defer cb.free(gathered);
    const exact = try compute.toInt64(gathered, a);
    defer a.free(exact);
    const exported = (try cb.exportTensorData(gathered, a)).?;
    defer a.free(exported.payload.bytes);
    const floats = try cb.fromFloat32Shape(&.{ 1, 2 }, &.{ 2, 1 });
    defer cb.free(floats);
    const gradient = try cb.primScatterAdd(floats, indices, &.{ 2, 1 }, &.{ 3, 1 }, 0);
    defer cb.free(gradient);
}

test "native integer tensor ownership survives allocation failures" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationCase, .{});
}
