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

//! F32 weights for the small Memorilla components that do not own sessions.
const std = @import("std");
const build = @import("build_options");
const metal = @import("../ops/metal_compute.zig");
const hosted = @import("../ops/gpu_hosted_store.zig");
const Tensor = @import("../backends/tensor.zig").Tensor;
const ops = @import("../ops/ops.zig");

pub const Weights = struct {
    allocator: std.mem.Allocator,
    store: *hosted.WeightStore,

    pub fn init(a: std.mem.Allocator) !Weights {
        if (comptime !build.enable_metal) return error.MetalNotEnabled;
        const store = try a.create(hosted.WeightStore);
        store.* = .{
            .allocator = a,
            .prefix = "",
            .lazy_weights = .empty,
            .allow_direct_quant = false,
            .prefer_f32_dense_tensors = true,
            .allow_immutable_f32_weight_borrow = true,
        };
        metal.initPrefetchQueue(store, a);
        return .{ .allocator = a, .store = store };
    }

    /// Consumes the key on success. The caller retains the source tensor.
    pub fn insertWeightFromTensor(self: *Weights, key: []const u8, tensor: *const Tensor) !void {
        if (tensor.dtype != .f32) return error.UnsupportedMemorillaPrecision;
        var owned = try Tensor.initFloat32(self.allocator, key, tensor.shape, tensor.asFloat32());
        errdefer owned.deinit();
        try self.store.lazy_weights.putNoClobber(self.allocator, key, .{
            .tensor_ref = .{ .name = key, .byte_len = tensor.data.len },
            .host_loaded = .{ .tensor = owned },
        });
    }

    pub fn compute(self: *Weights, io: std.Io) !ops.ComputeBackend {
        if (comptime !build.enable_metal) return error.MetalNotEnabled;
        const context = try self.allocator.create(metal.MetalCompute);
        errdefer self.allocator.destroy(context);
        try context.initInPlaceWithKernelJitOptions(self.allocator, self.store, null, io, .{});
        context.require_f32_linears = true;
        return context.ownedComputeBackend();
    }

    pub fn adopt(self: *Weights, source: *@import("../ops/native_compute.zig").WeightStore, expected_bytes: usize) !void {
        std.debug.assert(self.store.lazy_weights.count() == 0);
        var bytes: usize = 0;
        var weights = source.resident_weights.valueIterator();
        while (weights.next()) |weight| {
            if (weight.tensor.dtype != .f32 or weight.quantized_storage != null) return error.UnsupportedMemorillaPrecision;
            bytes = try std.math.add(usize, bytes, weight.tensor.data.len);
        }
        if (bytes != expected_bytes) return error.UnsupportedMemorillaPrecision;
        try self.store.lazy_weights.ensureUnusedCapacity(self.allocator, source.resident_weights.count());
        var entries = source.resident_weights.iterator();
        while (entries.next()) |entry| {
            const key = entry.key_ptr.*;
            self.store.lazy_weights.putAssumeCapacityNoClobber(key, .{
                .tensor_ref = .{ .name = key, .byte_len = entry.value_ptr.tensor.data.len },
                .host_loaded = entry.value_ptr.*,
            });
        }
        source.resident_weights.clearRetainingCapacity();
    }

    pub fn deinit(self: *Weights) void {
        metal.deinitSharedNativeProvider(self.store);
        metal.deinitPrefetchQueue(self.store);
        var entries = self.store.lazy_weights.valueIterator();
        while (entries.next()) |entry| {
            if (entry.host_loaded) |*weight| weight.deinit();
            entry.tensor_ref.deinit(self.allocator);
        }
        self.store.lazy_weights.deinit(self.allocator);
        self.allocator.destroy(self.store);
        self.* = undefined;
    }
};
