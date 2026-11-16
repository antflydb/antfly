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

const std = @import("std");
const CudaCompute = @import("cuda_compute.zig").CudaCompute;
const buffer_mod = @import("buffer.zig");
const driver_mod = @import("driver.zig");
const tensor_mod = @import("../../backends/tensor.zig");
const weight_source_mod = @import("../../models/weight_source.zig");
const gguf_tensor_types = @import("../../gguf/tensor_types.zig");
const CT = @import("../ops.zig").CT;

fn tensorFromCt(value: CT) *@import("cuda_compute.zig").CudaTensor {
    return @ptrCast(@alignCast(value));
}

test "Gemma4 CUDA host PLE preserves prefill device tokens and captured fused decode" {
    if (comptime !@import("build_options").enable_cuda) return error.SkipZigTest;
    const a = std.testing.allocator;
    var compute = try CudaCompute.init(a);
    defer compute.deinit();
    const cb = compute.computeBackend();
    const width = 10752;
    const vocab = 7;
    const block_bytes = 210;
    const raw = try a.alloc(u8, vocab * width / 256 * block_bytes);
    defer a.free(raw);
    for (raw, 0..) |*byte, i| byte.* = @truncate(i *% 37 +% i / block_bytes);
    for (0..vocab * width / 256) |i|
        std.mem.writeInt(u16, raw[i * block_bytes + 208 ..][0..2], @bitCast(@as(f16, 0.00390625)), .little);
    const shape = [_]i64{ vocab, width };
    var loaded = weight_source_mod.LoadedWeight{
        .tensor = .{ .data = &.{}, .dtype = .f32, .shape = &shape, .name = "", .allocator = a, .owns_data = false, .owns_shape = false },
        .quantized = true,
        .quantized_storage = .{ .tensor_type = .{ .known = .Q6_K }, .raw_bytes = raw, .shape = &shape, .allocator = a },
    };
    // One binary, identical bytes and kernels; only weight placement changes.
    const key = "model.per_layer_input.per_layer_token_embd.weight";
    compute.gemma4_ple_host_enabled = false;
    try compute.insertWeightFromLoaded(try a.dupe(u8, "per_layer_token_embd.weight"), &loaded);
    const baseline_bytes = compute.stats.resident_weight_bytes;
    const physical_before = compute.ctx.device_allocations.snapshot().live;
    compute.gemma4_ple_host_enabled = true;
    // Exercise malformed storage before any host owner exists, rather than
    // accidentally testing the duplicate-owner guard instead of validation.
    loaded.quantized_storage.?.raw_bytes = raw[0 .. raw.len - 1];
    const invalid_key = try a.dupe(u8, key);
    defer a.free(invalid_key);
    try std.testing.expectError(error.InvalidShape, compute.insertWeightFromLoaded(invalid_key, &loaded));
    try std.testing.expect(compute.gemma4_ple_host.owner == null);
    try std.testing.expectEqual(physical_before, compute.ctx.device_allocations.snapshot().live);
    loaded.quantized_storage.?.raw_bytes = raw;
    try compute.insertWeightFromLoaded(try a.dupe(u8, key), &loaded);
    try std.testing.expectEqual(physical_before, compute.ctx.device_allocations.snapshot().live);
    try std.testing.expectEqual(baseline_bytes, compute.stats.resident_weight_bytes);
    try std.testing.expectEqual(raw.len, compute.stats.gemma4_ple_host_weight_bytes);
    try std.testing.expectEqualSlices(u8, raw, compute.gemma4_ple_host.host.constBytes());
    // The source can close/change without invalidating retained host storage.
    @memset(raw, 0);
    const device_weight = try cb.getWeight("per_layer_token_embd.weight");
    const host_weight = try cb.getWeight(key);
    try std.testing.expectEqual(raw.len, tensorFromCt(device_weight).buffer.len);
    try std.testing.expectEqual(gguf_tensor_types.KnownTensorType.Q6_K, tensorFromCt(device_weight).quant_type.?.known);
    for ([_][]const i64{ &.{0}, &.{ 6, 2, 0, 6 } }) |ids| {
        const expected = try cb.embeddingLookup(device_weight, ids, ids.len, width);
        defer cb.free(expected);
        const actual = try cb.embeddingLookup(host_weight, ids, ids.len, width);
        defer cb.free(actual);
        const want = try cb.toFloat32(expected, a);
        defer a.free(want);
        const got = try cb.toFloat32(actual, a);
        defer a.free(got);
        try std.testing.expectEqualSlices(f32, want, got);
    }
    const token = (try cb.fromInt32Shape(&.{0}, &.{1})).?;
    defer cb.free(token);
    const rhs_data = try a.alloc(f32, width);
    defer a.free(rhs_data);
    for (rhs_data, 0..) |*v, i| v.* = @sin(@as(f32, @floatFromInt(i)) * 0.031);
    const rhs = try cb.fromFloat32Shape(rhs_data, &.{ 1, width });
    defer cb.free(rhs);
    const norm = try cb.fromFloat32Shape(&@as([256]f32, @splat(1)), &.{256});
    defer cb.free(norm);
    var output = try buffer_mod.DeviceBuffer.alloc(&compute.ctx, width * 4);
    defer output.free(&compute.ctx);
    var reference = try buffer_mod.DeviceBuffer.alloc(&compute.ctx, width * 4);
    defer reference.free(&compute.ctx);
    const Launch = struct {
        fn fused(c: *CudaCompute, dst: buffer_mod.DeviceBuffer, w: CT, id: CT, x: CT, n: CT) !void {
            try c.kernels.launchRmsNormAddWeightedEmbeddingI32Q6KF32(&c.ctx, dst, tensorFromCt(x).buffer, tensorFromCt(n).buffer, tensorFromCt(w).buffer, tensorFromCt(id).buffer, 1, 42, 256, 1e-6, 0.7, 0.9);
        }
    };
    // Warm module loading before capture. Replay must read the updated device
    // token, rather than pinning the first selected row or reading it on CPU.
    try Launch.fused(&compute, output, host_weight, token, rhs, norm);
    try compute.ctx.synchronize();
    try compute.ctx.beginStreamCapture(driver_mod.CU_STREAM_CAPTURE_MODE_THREAD_LOCAL);
    Launch.fused(&compute, output, host_weight, token, rhs, norm) catch |err| {
        if (compute.ctx.endStreamCapture()) |g| compute.ctx.destroyGraph(g) else |_| {}
        return err;
    };
    const graph = try compute.ctx.endStreamCapture();
    defer compute.ctx.destroyGraph(graph);
    const exec = try compute.ctx.instantiateGraph(graph);
    defer compute.ctx.destroyGraphExec(exec);
    const want = try a.alloc(f32, width);
    defer a.free(want);
    const got = try a.alloc(f32, width);
    defer a.free(got);
    for ([_]i32{ 0, 6, 2, 0 }) |id| {
        try tensorFromCt(token).buffer.copyFromHost(&compute.ctx, std.mem.asBytes(&id));
        try Launch.fused(&compute, reference, device_weight, token, rhs, norm);
        try compute.ctx.launchGraph(exec);
        try reference.copyToHost(&compute.ctx, std.mem.sliceAsBytes(want));
        try output.copyToHost(&compute.ctx, std.mem.sliceAsBytes(got));
        try compute.ctx.synchronize();
        try std.testing.expectEqualSlices(f32, want, got);
        const lookup = (try cb.embeddingLookupTensor(host_weight, token, 1, width)).?;
        defer cb.free(lookup);
        const expected = try cb.embeddingLookup(device_weight, &.{id}, 1, width);
        defer cb.free(expected);
        const values = try cb.toFloat32(lookup, a);
        defer a.free(values);
        const expected_values = try cb.toFloat32(expected, a);
        defer a.free(expected_values);
        try std.testing.expectEqualSlices(f32, expected_values, values);
    }
    // A second owner is rejected independently of storage validation.
    const duplicate_key = try a.dupe(u8, key);
    defer a.free(duplicate_key);
    try std.testing.expectError(error.InvalidCudaState, compute.insertWeightFromLoaded(duplicate_key, &loaded));
}

test "Gemma4 CUDA host PLE failed physical free survives complete compute teardown" {
    if (comptime !@import("build_options").enable_cuda) return error.SkipZigTest;
    const memory = @import("../../runtime/tier/memory.zig");
    const retirement = @import("host_retirement.zig");
    const driver = @import("driver.zig");
    const FailOnce = struct {
        var original: *const fn (?*anyopaque) callconv(.c) driver.CUresult = undefined;
        var calls: usize = 0;
        fn free(ptr: ?*anyopaque) callconv(.c) driver.CUresult {
            calls += 1;
            return if (calls == 1) 1 else original(ptr);
        }
    };
    var controller = memory.AdmissionController{};
    var lease: ?memory.AdmissionLease = try controller.tryAcquire(.gpu, .{}, .{ .host_weight_bytes = 4096 }, false);
    defer if (lease) |*held| held.release();
    var compute = try CudaCompute.init(std.testing.allocator);
    var alive = true;
    defer if (alive) compute.deinit();
    compute.gemma4_ple_host = try buffer_mod.MappedHostBuffer.allocAdmitted(&compute.ctx, 4096, &lease);
    try std.testing.expect(lease == null);
    const owner = compute.gemma4_ple_host.owner.?;
    FailOnce.original = owner.driver.fns.cuMemFreeHost.?;
    FailOnce.calls = 0;
    owner.driver.fns.cuMemFreeHost = FailOnce.free;
    defer retirement.process.retry();
    compute.deinit(); // Exercise the complete production destructor, not just clear().
    alive = false;
    try std.testing.expect(compute.ctx.ctx == null);
    try std.testing.expect(compute.gemma4_ple_host.owner == null);
    try std.testing.expect(retirement.process.head == owner);
    try std.testing.expectEqual(@as(usize, 1), FailOnce.calls);
    try std.testing.expectEqual(@as(usize, 4096), controller.snapshot().host_weight_bytes);
    // Both driver functions and the exact primary remain usable after the
    // original compute/context/library wrappers have been destroyed.
    retirement.process.retry();
    try std.testing.expect(retirement.process.head == null);
    try std.testing.expectEqual(@as(usize, 2), FailOnce.calls);
    try std.testing.expectEqual(memory.AdmissionAmounts{}, controller.snapshot());
}
