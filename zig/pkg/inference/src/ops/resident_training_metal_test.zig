// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const build_options = @import("build_options");
const ops = @import("ops.zig");
const metal = @import("metal_compute.zig");
const gpu_store = @import("gpu_hosted_store.zig");
const tensor = @import("../backends/tensor.zig");
const metal_tensor = @import("../backends/metal_tensor.zig");
const metal_runtime = @import("../backends/metal_runtime.zig");
const interpreter = @import("../graph/interpreter.zig");
const ml = @import("ml").graph;

const Fixture = struct {
    allocator: std.mem.Allocator,
    store: *gpu_store.WeightStore,
    backend: *metal.MetalCompute,

    fn init(a: std.mem.Allocator) !Fixture {
        const store = try a.create(gpu_store.WeightStore);
        errdefer a.destroy(store);
        store.* = .{ .allocator = a, .prefix = "", .lazy_weights = .empty, .prefer_f32_dense_tensors = true };
        errdefer store.lazy_weights.deinit(a);
        metal.initPrefetchQueue(store, a);
        errdefer metal.deinitPrefetchQueue(store);
        errdefer metal.deinitSharedNativeProvider(store);
        const backend = try a.create(metal.MetalCompute);
        errdefer a.destroy(backend);
        backend.* = try metal.MetalCompute.init(a, store, null);
        return .{ .allocator = a, .store = store, .backend = backend };
    }

    fn deinit(self: *Fixture) void {
        self.backend.deinit();
        self.allocator.destroy(self.backend);
        metal.deinitSharedNativeProvider(self.store);
        metal.deinitPrefetchQueue(self.store);
        self.store.lazy_weights.deinit(self.allocator);
        self.allocator.destroy(self.store);
        self.* = undefined;
    }
};

fn upload(cb: *const ops.ComputeBackend, values: []const f32, shape: []const i32) !ops.CT {
    return cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = values, .shape = shape } }, .{});
}

fn expectFloats(cb: *const ops.ComputeBackend, value: ops.CT, expected: []const f32) !void {
    const output = try std.testing.allocator.alloc(f32, expected.len);
    defer std.testing.allocator.free(output);
    try cb.glinerBoundaryDownload(value, output);
    try std.testing.expectEqualSlices(f32, expected, output);
}

fn expectInts(cb: *const ops.ComputeBackend, value: ops.CT, expected: []const i32) !void {
    try std.testing.expectEqual(tensor.DType.i32, try cb.tensorDType(value));
    const exported = (try cb.exportTensorData(value, std.testing.allocator)).?;
    defer std.testing.allocator.free(exported.payload.bytes);
    try std.testing.expectEqual(tensor.DType.i32, exported.dtype);
    const actual = std.mem.bytesAsSlice(i32, exported.payload.bytes);
    try std.testing.expectEqual(expected.len, actual.len);
    for (expected, actual) |want, got| try std.testing.expectEqual(want, got);
}

test "resident training strict primitives reject unqualified backends without fallback" {
    const a = std.testing.allocator;
    const native = @import("native_compute.zig");
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var backend = native.NativeCompute.init(a, &store, null);
    defer backend.deinit();
    const cb = backend.computeBackend();
    try std.testing.expectError(error.UnsupportedResidentTrainingPrimitive, upload(&cb, &.{1}, &.{1}));
    const value = try cb.fromFloat32Shape(&.{1}, &.{1});
    defer cb.free(value);
    try std.testing.expectError(error.UnsupportedResidentTrainingCapture, cb.snapshotTensorShape(value, &.{1}));
    try std.testing.expectError(error.UnsupportedResidentTrainingPrimitive, cb.residentTrainingNorm(&.{.{ .tensor = value, .elem_count = 1 }}, .{}));
}

test "resident training Metal exact integer clone reshape and adjacent indices above 2pow24" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime.metalDeviceAvailable()) return error.SkipZigTest;
    var fixture = try Fixture.init(std.testing.allocator);
    defer fixture.deinit();
    const cb = fixture.backend.computeBackend();
    const integers = [_]i32{ 16_777_217, -16_777_217, std.math.maxInt(i32), std.math.minInt(i32) };
    const indices = (try cb.fromInt32Shape(&integers, &.{ 2, 2 })).?;
    defer cb.free(indices);
    const clone = (try cb.cloneTensorShape(indices, &.{4})).?;
    defer cb.free(clone);
    const reshaped = try cb.primReshape(clone, &.{ 1, -1 });
    defer cb.free(reshaped);
    try expectInts(&cb, indices, &integers);
    try expectInts(&cb, reshaped, &integers);
    try std.testing.expectError(error.UnsupportedTensorType, cb.toFloat32(indices, std.testing.allocator));
    try std.testing.expectError(error.UnsupportedTensorType, cb.trainingOverwriteF32(indices, &.{ 0, 0, 0, 0 }, &.{ 2, 2 }));
    try std.testing.expectError(error.UnsupportedResidentTrainingPrimitive, cb.tryConvertDType(indices, .i64));

    // A single64MiB device output, populated from two values without a host
    // table. Distinct adjacent IDs prove the MSL kernel itself reads i32.
    const high = (try cb.fromInt32Shape(&.{ 16_777_216, 16_777_217 }, &.{2})).?;
    defer cb.free(high);
    const values = try upload(&cb, &.{ 11, 29 }, &.{ 2, 1 });
    defer cb.free(values);
    const before = metal_tensor.memoryStatsSnapshot();
    const table = try cb.primScatterAdd(values, high, &.{ 2, 1 }, &.{ 16_777_218, 1 }, 0);
    defer cb.free(table);
    const result = try cb.primGather(table, high, 0, &.{ 16_777_218, 1 });
    defer cb.free(result);
    const after = metal_tensor.memoryStatsSnapshot();
    try std.testing.expectEqual(before.to_host_device_calls, after.to_host_device_calls);
    try std.testing.expectEqual(before.host_mirror_download_bytes, after.host_mirror_download_bytes);
    try expectFloats(&cb, result, &.{ 11, 29 });
}

test "resident training Metal retained graph captures are independent and typed scatter repeats exactly" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime.metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    var fixture = try Fixture.init(a);
    defer fixture.deinit();
    const cb = fixture.backend.computeBackend();
    const source = try upload(&cb, &.{ 1, 2, 3, 4, 5, 6 }, &.{ 3, 2 });
    defer cb.free(source);
    var graph = ml.Graph.init(a);
    defer graph.deinit();
    var builder = ml.Builder.init(&graph);
    const input = try builder.parameter("source", ml.Shape.init(.f32, &.{ 3, 2 }));
    try graph.markOutput(input);
    const before = metal_tensor.memoryStatsSnapshot();
    var captures = try interpreter.captureNodeValues(a, &graph, &cb, .{
        .runtime_inputs = &.{.{ .node_id = input, .value = source }},
        .require_resident_capture = true,
    }, &.{input});
    defer captures.deinit(&cb);
    try cb.trainingOverwriteF32(source, &.{ 7, 8, 9, 10, 11, 12 }, &.{ 3, 2 });
    const index = (try cb.fromInt32Shape(&.{ 1, -1, 1, 0 }, &.{ 2, 2 })).?;
    defer cb.free(index);
    const gathered = try cb.primGather(captures.values[0], index, 0, &.{ 3, 2 });
    defer cb.free(gathered);
    const gradient = try upload(&cb, &.{ 1, 2, 3, 4, 5, 6, 7, 8 }, &.{ 4, 2 });
    defer cb.free(gradient);
    const repeated = try cb.primScatterAdd(gradient, index, &.{ 4, 2 }, &.{ 3, 2 }, 0);
    defer cb.free(repeated);
    const after = metal_tensor.memoryStatsSnapshot();
    try std.testing.expectEqual(before.to_host_device_calls, after.to_host_device_calls);
    try std.testing.expectEqual(before.host_mirror_download_bytes, after.host_mirror_download_bytes);
    try expectFloats(&cb, source, &.{ 7, 8, 9, 10, 11, 12 });
    try expectFloats(&cb, captures.values[0], &.{ 1, 2, 3, 4, 5, 6 });
    try expectFloats(&cb, gathered, &.{ 3, 4, 5, 6, 3, 4, 1, 2 });
    try expectFloats(&cb, repeated, &.{ 7, 8, 6, 8, 3, 4 });

    const host = try cb.fromFloat32(&.{ 1, 2, 3, 4, 5, 6 });
    defer cb.free(host);
    try std.testing.expectError(error.ForeignResidentTrainingTensor, interpreter.captureNodeValues(a, &graph, &cb, .{
        .runtime_inputs = &.{.{ .node_id = input, .value = host }},
        .require_resident_capture = true,
    }, &.{input}));
}

test "resident training Metal admission rejects invalid indices storage shapes and external frames" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime.metalDeviceAvailable()) return error.SkipZigTest;
    var fixture = try Fixture.init(std.testing.allocator);
    defer fixture.deinit();
    const cb = fixture.backend.computeBackend();
    const source = try upload(&cb, &.{ 1, 2, 3, 4, 5, 6 }, &.{ 3, 2 });
    defer cb.free(source);
    const good = (try cb.fromInt32Shape(&.{ 0, 1, 2 }, &.{3})).?;
    defer cb.free(good);
    const bad = (try cb.fromInt32Shape(&.{-4}, &.{1})).?;
    defer cb.free(bad);
    try std.testing.expectError(error.IndexOutOfBounds, cb.primGather(source, bad, 0, &.{ 3, 2 }));
    try std.testing.expectError(error.UnsupportedResidentTrainingPrimitive, cb.primGather(source, good, 1, &.{ 3, 2 }));
    try std.testing.expectError(error.UnsupportedResidentTrainingPrimitive, cb.primTranspose(good, &.{0}, &.{3}));
    try std.testing.expectError(error.UnsupportedTensorType, cb.add(good, good));
    try std.testing.expectError(error.InvalidResidentTrainingShape, cb.fromInt32Shape(&.{1}, &.{2}));
    try std.testing.expectError(error.InvalidResidentTrainingShape, cb.snapshotTensorShape(source, &.{7}));
    try std.testing.expectError(error.ResourceLimitExceeded, cb.residentTrainingPrimitive(&.{ .scatter_add = .{
        .values = source,
        .indices = good,
        .input_shape = &.{ 3, 2 },
        .output_shape = &.{ 3, 2 },
    } }, .{ .max_scatter_work = 1 }));
    try std.testing.expectError(error.UnsupportedResidentTrainingPrimitive, cb.residentTrainingPrimitive(&.{ .gather = .{
        .input = source,
        .indices = source,
        .input_shape = &.{ 3, 2 },
    } }, .{}));
    const host = try cb.fromFloat32(&.{ 1, 2, 3, 4, 5, 6 });
    defer cb.free(host);
    try std.testing.expectError(error.ForeignResidentTrainingTensor, cb.snapshotTensorShape(host, &.{ 3, 2 }));
    try metal_runtime.beginFrame(fixture.backend.provider_impl.raw_decode_runtime);
    defer metal_runtime.cancelFrame(fixture.backend.provider_impl.raw_decode_runtime) catch {};
    try std.testing.expectError(error.ResidentTrainingExternalFrame, cb.snapshotTensorShape(source, &.{ 3, 2 }));
}

fn allocationCheck(a: std.mem.Allocator, backend: *metal.MetalCompute) !void {
    const original = backend.allocator;
    backend.allocator = a;
    defer backend.allocator = original;
    const cb = backend.computeBackend();
    const source = try upload(&cb, &.{ 1, 2, 3, 4, 5, 6 }, &.{ 3, 2 });
    defer cb.free(source);
    const indices = (try cb.fromInt32Shape(&.{ 2, 0, 2 }, &.{3})).?;
    defer cb.free(indices);
    const snapshot = try cb.snapshotTensorShape(source, &.{ 3, 2 });
    defer cb.free(snapshot);
    const gathered = try cb.primGather(snapshot, indices, 0, &.{ 3, 2 });
    defer cb.free(gathered);
    const result = try cb.primScatterAdd(gathered, indices, &.{ 3, 2 }, &.{ 3, 2 }, 0);
    defer cb.free(result);
    const norm = try cb.residentTrainingNorm(&.{.{ .tensor = result, .elem_count = 6 }}, .{});
    try std.testing.expect(norm.finite);
}

test "resident training Metal allocation failures release every strict tensor" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime.metalDeviceAvailable()) return error.SkipZigTest;
    var fixture = try Fixture.init(std.testing.allocator);
    defer fixture.deinit();
    const before = metal_tensor.memoryStatsSnapshot();
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationCheck, .{fixture.backend});
    const after = metal_tensor.memoryStatsSnapshot();
    try std.testing.expectEqual(before.device_owned_live_bytes, after.device_owned_live_bytes);
    try std.testing.expectEqual(before.host_mirror_download_bytes, after.host_mirror_download_bytes);
}

test "resident training Metal sparse grouped scatter preserves input order without quadratic scans" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime.metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    var fixture = try Fixture.init(a);
    defer fixture.deinit();
    const cb = fixture.backend.computeBackend();
    const n = 4097;
    const rows = 8003;
    const columns = 3;
    var indices: [n]i32 = undefined;
    var values: [n * columns]f32 = undefined;
    const expected = try a.alloc(f32, rows * columns);
    defer a.free(expected);
    @memset(expected, 0);
    for (&indices, 0..) |*index, i| {
        index.* = if (i % 3 == 0) -1 else @intCast((i * 19) % 13);
        const row: usize = @intCast(if (index.* < 0) index.* + rows else index.*);
        for (0..columns) |c| {
            const value: f32 = if (i % 4 == 0) 1e7 else if (i % 4 == 1) -1e7 else @as(f32, @floatFromInt((i + c) % 11)) * 0.125;
            values[i * columns + c] = value;
            expected[row * columns + c] += value;
        }
    }
    const copied_indices = blk: {
        const original = (try cb.fromInt32Shape(&indices, &.{n})).?;
        defer cb.free(original);
        break :blk try cb.snapshotTensorShape(original, &.{n});
    };
    defer cb.free(copied_indices);
    const source = try upload(&cb, &values, &.{ n, columns });
    defer cb.free(source);
    const before = metal_tensor.memoryStatsSnapshot();
    const result = try cb.residentTrainingPrimitive(&.{ .scatter_add = .{
        .values = source,
        .indices = copied_indices,
        .input_shape = &.{ n, columns },
        .output_shape = &.{ rows, columns },
    } }, .{ .max_scatter_work = (n + rows) * columns });
    defer cb.free(result);
    const after = metal_tensor.memoryStatsSnapshot();
    try std.testing.expectEqual(before.to_host_device_calls, after.to_host_device_calls);
    try std.testing.expectEqual(before.host_mirror_download_bytes, after.host_mirror_download_bytes);
    try expectFloats(&cb, result, expected);
}

test "resident training Metal parallel norm covers full models adapters tails and scaled extremes" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime.metalDeviceAvailable()) return error.SkipZigTest;
    var fixture = try Fixture.init(std.testing.allocator);
    defer fixture.deinit();
    const cb = fixture.backend.computeBackend();
    var inputs: [341]ops.resident_training.NormInput = undefined;
    var initialized: usize = 0;
    defer for (inputs[0..initialized]) |item| cb.free(item.tensor);
    const lengths = [_]usize{ 1, 17, 1023, 1024, 1025, 8193 };
    var values: [8193]f32 = undefined;
    var sum: f64 = 0;
    var partial_bytes: usize = 0;
    for (&inputs, 0..) |*input, slot| {
        const length = lengths[slot % lengths.len];
        for (values[0..length], 0..) |*value, i| {
            value.* = @as(f32, @floatFromInt(@as(i32, @intCast((i * 13 + slot) % 79)) - 39)) / 17;
            sum += @as(f64, value.*) * @as(f64, value.*);
        }
        input.* = .{ .tensor = try upload(&cb, values[0..length], &.{@intCast(length)}), .elem_count = length };
        initialized += 1;
        partial_bytes += ((length + 1023) / 1024) * 12;
    }
    const before = metal_tensor.memoryStatsSnapshot();
    const summary = try cb.residentTrainingNorm(&inputs, .{});
    const legacy = try cb.trainingSumSquaresManyF32(&inputs);
    const after = metal_tensor.memoryStatsSnapshot();
    try std.testing.expectEqual(before.to_host_device_calls, after.to_host_device_calls);
    try std.testing.expectEqual(before.host_mirror_download_bytes, after.host_mirror_download_bytes);
    try std.testing.expect(summary.finite);
    try std.testing.expectApproxEqRel(sum, summary.sum_squares, 2e-6);
    try std.testing.expectApproxEqRel(@sqrt(sum), summary.norm, 2e-6);
    try std.testing.expectApproxEqRel(@as(f32, @floatCast(sum)), legacy, 2e-6);
    try std.testing.expectEqual(@as(usize, inputs.len), summary.tensor_count);
    try std.testing.expectEqual(@as(usize, inputs.len * 12), summary.download_bytes);
    try std.testing.expectEqual(partial_bytes, summary.partial_bytes);
    try std.testing.expectError(error.ResourceLimitExceeded, cb.residentTrainingNorm(&inputs, .{ .max_tensors = 334 }));
    try std.testing.expectError(error.ResourceLimitExceeded, cb.residentTrainingNorm(&inputs, .{ .max_partial_bytes = partial_bytes - 1 }));

    inline for (.{ @as(f32, 1e20), @as(f32, 1e-20) }) |scale| {
        const value = try upload(&cb, &.{ scale, -scale }, &.{2});
        defer cb.free(value);
        const extreme = try cb.residentTrainingNorm(&.{.{ .tensor = value, .elem_count = 2 }}, .{});
        try std.testing.expect(extreme.finite);
        try std.testing.expectApproxEqRel(@as(f64, scale) * @as(f64, scale) * 2, extreme.sum_squares, 2e-6);
    }
    const invalid = try upload(&cb, &.{ 1, std.math.nan(f32), std.math.inf(f32) }, &.{3});
    defer cb.free(invalid);
    const invalid_summary = try cb.residentTrainingNorm(&.{.{ .tensor = invalid, .elem_count = 3 }}, .{});
    try std.testing.expect(!invalid_summary.finite);
    try std.testing.expectEqual(@as(f64, 1), invalid_summary.sum_squares);
}

const CancelAt = struct {
    calls: usize = 0,
    at: usize,
    fn check(raw: ?*anyopaque) anyerror!void {
        const self: *CancelAt = @ptrCast(@alignCast(raw.?));
        self.calls += 1;
        if (self.calls == self.at) return error.Cancelled;
    }
};

test "resident training Metal norm cancellation releases its frame and preserves source" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime.metalDeviceAvailable()) return error.SkipZigTest;
    var fixture = try Fixture.init(std.testing.allocator);
    defer fixture.deinit();
    var cb = fixture.backend.computeBackend();
    const source = try upload(&cb, &.{ 1, 2, 3 }, &.{3});
    defer cb.free(source);
    const before = metal_tensor.memoryStatsSnapshot();
    // The single input checks are wrapper, primitive, validation, dispatch,
    // completion and wrapper completion. Exercise both sides of submission.
    for ([_]usize{ 4, 5 }) |at| {
        var cancel = CancelAt{ .at = at };
        cb.execution_control = .{ .ptr = &cancel, .check_fn = CancelAt.check };
        try std.testing.expectError(error.Cancelled, cb.residentTrainingNorm(&.{.{ .tensor = source, .elem_count = 3 }}, .{}));
        cb.execution_control = null;
        try std.testing.expect(!metal_runtime.hasActiveFrame(fixture.backend.provider_impl.raw_decode_runtime));
        const recovered = try cb.residentTrainingNorm(&.{.{ .tensor = source, .elem_count = 3 }}, .{});
        try std.testing.expectApproxEqRel(@as(f64, 14), recovered.sum_squares, 2e-6);
    }
    const after = metal_tensor.memoryStatsSnapshot();
    try std.testing.expectEqual(before.device_owned_live_bytes, after.device_owned_live_bytes);
    try std.testing.expectEqual(before.host_mirror_download_bytes, after.host_mirror_download_bytes);
    try expectFloats(&cb, source, &.{ 1, 2, 3 });
    try metal_runtime.beginFrame(fixture.backend.provider_impl.raw_decode_runtime);
    defer metal_runtime.cancelFrame(fixture.backend.provider_impl.raw_decode_runtime) catch {};
    try std.testing.expectError(error.ResidentTrainingExternalFrame, cb.residentTrainingNorm(&.{.{ .tensor = source, .elem_count = 3 }}, .{}));
}

test "resident training Metal AdamW failed partial tensor preparation releases retained handles" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime.metalDeviceAvailable()) return error.SkipZigTest;
    var fixture = try Fixture.init(std.testing.allocator);
    defer fixture.deinit();
    const cb = fixture.backend.computeBackend();
    const before = metal_tensor.memoryStatsSnapshot();
    for (0..4) |position| {
        const good = try upload(&cb, &.{ 1, 2 }, &.{2});
        defer cb.free(good);
        const invalid = (try cb.fromInt32Shape(&.{ 1, 2 }, &.{2})).?;
        defer cb.free(invalid);
        const valid = ops.TrainingAdamWBatchInput{ .weight = good, .grad = good, .m = good, .v = good, .elem_count = 2, .bias_correction1 = 0.1, .bias_correction2 = 0.01 };
        var input = valid;
        switch (position) {
            0 => input.weight = invalid,
            1 => input.grad = invalid,
            2 => input.m = invalid,
            3 => input.v = invalid,
            else => unreachable,
        }
        try std.testing.expectError(error.UnsupportedTensorType, cb.trainingAdamWManyF32(&.{ valid, input }, .{
            .lr = 0.01,
            .beta1 = 0.9,
            .beta2 = 0.99,
            .eps = 1e-8,
            .weight_decay = 0.01,
        }));
        try expectFloats(&cb, good, &.{ 1, 2 });
    }
    const after = metal_tensor.memoryStatsSnapshot();
    try std.testing.expectEqual(before.device_owned_live_bytes, after.device_owned_live_bytes);
}
