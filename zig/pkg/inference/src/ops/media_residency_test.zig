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
const ops = @import("ops.zig");
const native = @import("native_compute.zig");

pub fn putWeight(store: *native.WeightStore, name: []const u8, shape: []const i64, constant: ?f32) !void {
    const allocator = store.allocator;
    var count: usize = 1;
    for (shape) |dim| count *= @intCast(dim);
    const data = try allocator.alloc(f32, count);
    defer allocator.free(data);
    for (data, 0..) |*v, i| v.* = constant orelse (@as(f32, @floatFromInt((i * 7 + name.len) % 23)) - 11) * 0.015;
    const key = try allocator.dupe(u8, name);
    errdefer allocator.free(key);
    var tensor = try @import("../backends/tensor.zig").Tensor.initFloat32(allocator, key, shape, data);
    errdefer tensor.deinit();
    try store.resident_weights.put(allocator, key, .{ .tensor = tensor });
}

pub fn noWeights(_: *native.WeightStore) !void {}

pub fn onNative(comptime populate: anytype, comptime check: anytype) !void {
    const allocator = std.testing.allocator;
    var weights = native.WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    defer weights.deinitOwned();
    try populate(&weights);
    var compute = native.NativeCompute.init(allocator, &weights, null);
    defer compute.deinit();
    try check(compute.computeBackend(), compute.computeBackend());
}

pub fn onMetal(comptime populate: anytype, comptime check: anytype) !void {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    if (!@import("../backends/metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    const metal = @import("metal_compute.zig");
    const allocator = std.testing.allocator;
    var weights = native.WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    defer weights.deinitOwned();
    try populate(&weights);
    var reference = native.NativeCompute.init(allocator, &weights, null);
    defer reference.deinit();
    var store = @import("gpu_hosted_store.zig").WeightStore{ .allocator = allocator, .prefix = "", .lazy_weights = .{} };
    defer store.lazy_weights.deinit(allocator);
    metal.initPrefetchQueue(&store, allocator);
    defer metal.deinitPrefetchQueue(&store);
    defer metal.deinitSharedNativeProvider(&store);
    var it = weights.resident_weights.iterator();
    while (it.next()) |entry| {
        var borrowed = entry.value_ptr.tensor;
        borrowed.owns_data = false;
        borrowed.owns_shape = false;
        try store.lazy_weights.put(allocator, entry.key_ptr.*, .{ .tensor_ref = .{ .name = entry.key_ptr.* }, .host_loaded = .{ .tensor = borrowed } });
    }
    var compute = try metal.MetalCompute.init(allocator, &store, null);
    defer compute.deinit();
    try check(compute.computeBackend(), reference.computeBackend());
}

pub fn onCuda(comptime populate: anytype, comptime check: anytype) !void {
    if (comptime !@import("build_options").enable_cuda) return error.SkipZigTest;
    const cuda = @import("cuda/cuda_compute.zig");
    const allocator = std.testing.allocator;
    var weights = native.WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    defer weights.deinitOwned();
    try populate(&weights);
    var reference = native.NativeCompute.init(allocator, &weights, null);
    defer reference.deinit();
    var compute = try cuda.CudaCompute.init(allocator);
    defer compute.deinit();
    var it = weights.resident_weights.iterator();
    while (it.next()) |entry| {
        const key = try allocator.dupe(u8, entry.key_ptr.*);
        try compute.insertWeightFromTensor(key, &entry.value_ptr.tensor);
    }
    try check(compute.computeBackend(), reference.computeBackend());
}

pub const Transfers = struct {
    downloads: u64,
    accesses: u64,
    cuda_bytes: u64,
    pub fn snapshot(cb: ops.ComputeBackend) Transfers {
        const stats = cb.debugTimingSnapshot();
        return .{ .downloads = @intCast(stats.provider.metal_tensor_host_mirror_download_bytes), .accesses = @intCast(stats.provider.metal_tensor_to_host_device_calls), .cuda_bytes = @intCast(cb.trainingRuntimeStats().d2h_bytes) };
    }
    pub fn expectUnchanged(self: Transfers, cb: ops.ComputeBackend) !void {
        const after = snapshot(cb);
        if (cb.kind() == .metal) {
            try std.testing.expectEqual(self.downloads, after.downloads);
            try std.testing.expectEqual(self.accesses, after.accesses);
        }
        if (cb.kind() == .cuda) try std.testing.expectEqual(self.cuda_bytes, after.cuda_bytes);
    }
};
