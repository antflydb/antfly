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
const build_options = @import("build_options");
const native_mod = if (build_options.enable_native) @import("../ops/native_compute.zig") else struct {};
const wasm_compute_mod = if (build_options.enable_wasm) @import("../ops/wasm_compute.zig") else struct {};
const gpu_store_mod = if (build_options.enable_metal) @import("../ops/gpu_hosted_store.zig") else struct {};
const cuda_compute_mod = if (build_options.enable_cuda) @import("../ops/cuda/cuda_compute.zig") else struct {};
const CudaCompute = if (build_options.enable_cuda) cuda_compute_mod.CudaCompute else opaque {};
const metal_compute_mod = if (build_options.enable_metal) @import("../ops/metal_compute.zig") else struct {};
const Tensor = @import("tensor.zig").Tensor;
const DType = @import("tensor.zig").DType;
const BackendType = @import("backends.zig").BackendType;
const ops_mod = @import("../ops/ops.zig");
const NativeCompute = if (build_options.enable_native) native_mod.NativeCompute else opaque {};
const WeightStore = if (build_options.enable_native) native_mod.WeightStore else opaque {};
const GpuWeightStore = if (build_options.enable_metal) gpu_store_mod.WeightStore else opaque {};
const MetalCompute = if (build_options.enable_metal) metal_compute_mod.MetalCompute else opaque {};
const WasmCompute = if (build_options.enable_wasm) wasm_compute_mod.WasmCompute else opaque {};

pub const BackendContext = union(enum) {
    cuda: *CudaCompute,
    native: struct {
        compute: *NativeCompute,
        weight_store: *WeightStore,
    },
    metal_hosted: struct {
        compute: *MetalCompute,
        weight_store: *GpuWeightStore,
    },
    wasm: struct {
        compute: *WasmCompute,
    },

    pub fn init(allocator: std.mem.Allocator, requested: BackendType, io: ?std.Io) !BackendContext {
        return switch (requested) {
            .cuda => blk: {
                if (comptime !build_options.enable_cuda) return error.CudaNotEnabled;
                const compute = try allocator.create(CudaCompute);
                errdefer allocator.destroy(compute);
                compute.* = try CudaCompute.init(allocator);
                break :blk .{ .cuda = compute };
            },
            .native => blk: {
                if (comptime !build_options.enable_native) return error.NativeNotEnabled;
                const compute = try allocator.create(NativeCompute);
                errdefer allocator.destroy(compute);
                const weight_store = try allocator.create(WeightStore);
                errdefer allocator.destroy(weight_store);
                weight_store.* = .{
                    .allocator = allocator,
                    .resident_weights = .empty,
                    .lazy_weights = .empty,
                };
                compute.* = if (io) |runtime|
                    NativeCompute.initWithIo(allocator, weight_store, null, runtime)
                else
                    NativeCompute.init(allocator, weight_store, null);
                break :blk .{
                    .native = .{
                        .compute = compute,
                        .weight_store = weight_store,
                    },
                };
            },
            .wasm => blk: {
                if (comptime !build_options.enable_wasm) return error.WasmNotEnabled;
                const compute = try allocator.create(WasmCompute);
                errdefer allocator.destroy(compute);
                compute.* = wasm_compute_mod.WasmCompute.init(allocator);
                break :blk .{ .wasm = .{ .compute = compute } };
            },
            .metal => blk: {
                if (comptime !build_options.enable_metal) return error.MetalNotEnabled;
                const compute = try allocator.create(MetalCompute);
                errdefer allocator.destroy(compute);
                const weight_store = try allocator.create(GpuWeightStore);
                errdefer allocator.destroy(weight_store);
                weight_store.* = initGpuHostedWeightStore(allocator, .metal);
                errdefer deinitGpuHostedWeightStore(allocator, weight_store, .metal);
                metal_compute_mod.initPrefetchQueue(weight_store, allocator);
                compute.* = if (io) |runtime|
                    try metal_compute_mod.MetalCompute.initWithIo(allocator, weight_store, null, runtime)
                else
                    try metal_compute_mod.MetalCompute.init(allocator, weight_store, null);
                break :blk .{
                    .metal_hosted = .{
                        .compute = compute,
                        .weight_store = weight_store,
                    },
                };
            },
            else => error.UnsupportedOnnxGraphBackend,
        };
    }

    pub fn deinit(self: *BackendContext, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .cuda => |compute| {
                if (comptime build_options.enable_cuda) compute.deinit() else unreachable;
                allocator.destroy(compute);
            },
            .native => |*ctx| {
                if (comptime build_options.enable_native) {
                    ctx.compute.computeBackend().deinit();
                    ctx.weight_store.deinitOwned();
                    allocator.destroy(ctx.weight_store);
                } else {
                    unreachable;
                }
            },
            .metal_hosted => |*ctx| {
                if (comptime build_options.enable_metal) {
                    ctx.compute.computeBackend().deinit();
                } else {
                    unreachable;
                }
                allocator.destroy(ctx.compute);
                deinitGpuHostedWeightStore(allocator, ctx.weight_store, .metal);
                allocator.destroy(ctx.weight_store);
            },
            .wasm => |*ctx| {
                if (comptime build_options.enable_wasm) {
                    var cb = ctx.compute.computeBackend();
                    cb.deinit();
                    allocator.destroy(ctx.compute);
                } else {
                    unreachable;
                }
            },
        }
    }

    pub fn computeBackend(self: *BackendContext) ops_mod.ComputeBackend {
        return switch (self.*) {
            .cuda => |compute| if (comptime build_options.enable_cuda) compute.computeBackend() else unreachable,
            .native => |*ctx| if (comptime build_options.enable_native)
                ctx.compute.computeBackend()
            else
                unreachable,
            .metal_hosted => |*ctx| if (comptime build_options.enable_metal)
                ctx.compute.computeBackend()
            else
                unreachable,
            .wasm => |*ctx| if (comptime build_options.enable_wasm)
                ctx.compute.computeBackend()
            else
                unreachable,
        };
    }

    pub fn backendType(self: *const BackendContext) BackendType {
        return switch (self.*) {
            .cuda => .cuda,
            .native => .native,
            .metal_hosted => .metal,
            .wasm => .wasm,
        };
    }

    pub fn importHostTensor(self: *BackendContext, allocator: std.mem.Allocator, tensor: *const Tensor) !ops_mod.CT {
        return switch (self.*) {
            .cuda => {
                const cb = self.computeBackend();
                return importTensorToBackend(allocator, &cb, tensor);
            },
            .native => |*ctx| if (comptime build_options.enable_native)
                ctx.compute.importHostTensor(tensor)
            else
                unreachable,
            .metal_hosted => |*ctx| return importMetalTensor(allocator, ctx.compute, tensor),
            .wasm => |*ctx| {
                const cb = if (comptime build_options.enable_wasm)
                    ctx.compute.computeBackend()
                else
                    unreachable;
                return importTensorToBackend(allocator, &cb, tensor);
            },
        };
    }

    pub fn importStaticTensor(self: *BackendContext, allocator: std.mem.Allocator, tensor: Tensor) !ops_mod.CT {
        return switch (self.*) {
            .cuda => {
                var owned = tensor;
                defer owned.deinit();
                return self.importHostTensor(allocator, &owned);
            },
            .native => |*ctx| if (comptime build_options.enable_native)
                ctx.compute.importOwnedStaticTensor(tensor)
            else
                unreachable,
            .metal_hosted => |*ctx| {
                var owned_tensor = tensor;
                defer owned_tensor.deinit();
                return importMetalTensor(allocator, ctx.compute, &owned_tensor);
            },
            .wasm => |*ctx| {
                var owned_tensor = tensor;
                defer owned_tensor.deinit();
                const cb = if (comptime build_options.enable_wasm)
                    ctx.compute.computeBackend()
                else
                    unreachable;
                return importTensorToBackend(allocator, &cb, &owned_tensor);
            },
        };
    }

    pub fn importDenseStaticF32(
        self: *BackendContext,
        allocator: std.mem.Allocator,
        dtype: DType,
        shape: []const i64,
        values: []f32,
    ) !ops_mod.CT {
        return switch (self.*) {
            .cuda => {
                defer allocator.free(values);
                if (dtype != .f32) return error.UnsupportedTensorType;
                const cb = self.computeBackend();
                return (try cb.fromConstantBytes(std.mem.sliceAsBytes(values), .f32, shape)) orelse error.UnsupportedTensorType;
            },
            .native => |*ctx| if (comptime build_options.enable_native)
                ctx.compute.importDenseTensor("", dtype, shape, values)
            else
                unreachable,
            .metal_hosted => |*ctx| {
                defer allocator.free(values);
                const shape_i32 = try tensorShapeI32(allocator, shape);
                defer allocator.free(shape_i32);
                return if (comptime build_options.enable_metal)
                    ctx.compute.importResidentF32(values, shape_i32)
                else
                    unreachable;
            },
            .wasm => |*ctx| {
                defer allocator.free(values);
                const shape_i32 = try tensorShapeI32(allocator, shape);
                defer allocator.free(shape_i32);
                const cb = if (comptime build_options.enable_wasm)
                    ctx.compute.computeBackend()
                else
                    unreachable;
                return cb.fromFloat32Shape(values, shape_i32);
            },
        };
    }
};

pub const SharedBackendContext = struct {
    allocator: std.mem.Allocator,
    ctx: BackendContext,
    backend: ops_mod.ComputeBackend,
    ref_count: std.atomic.Value(usize) = std.atomic.Value(usize).init(1),

    pub fn init(allocator: std.mem.Allocator, requested: BackendType, io: ?std.Io) !*SharedBackendContext {
        const shared = try allocator.create(SharedBackendContext);
        errdefer allocator.destroy(shared);
        var context = try BackendContext.init(allocator, requested, io);
        shared.* = .{ .allocator = allocator, .backend = context.computeBackend(), .ctx = context };
        return shared;
    }

    pub fn retain(self: *SharedBackendContext) *SharedBackendContext {
        _ = self.ref_count.fetchAdd(1, .monotonic);
        return self;
    }

    pub fn release(self: *SharedBackendContext) void {
        if (self.ref_count.fetchSub(1, .acq_rel) != 1) return;
        self.ctx.deinit(self.allocator);
        self.allocator.destroy(self);
    }

    pub fn computeBackend(self: *SharedBackendContext) ops_mod.ComputeBackend {
        return self.backend;
    }
    pub fn stableBackend(self: *SharedBackendContext) *const ops_mod.ComputeBackend {
        return &self.backend;
    }

    pub fn backendType(self: *const SharedBackendContext) BackendType {
        return self.ctx.backendType();
    }

    pub fn importHostTensor(self: *SharedBackendContext, allocator: std.mem.Allocator, tensor: *const Tensor) !ops_mod.CT {
        return self.ctx.importHostTensor(allocator, tensor);
    }

    pub fn importStaticTensor(self: *SharedBackendContext, allocator: std.mem.Allocator, tensor: Tensor) !ops_mod.CT {
        return self.ctx.importStaticTensor(allocator, tensor);
    }

    pub fn importDenseStaticF32(
        self: *SharedBackendContext,
        allocator: std.mem.Allocator,
        dtype: DType,
        shape: []const i64,
        values: []f32,
    ) !ops_mod.CT {
        return self.ctx.importDenseStaticF32(allocator, dtype, shape, values);
    }
};

fn initGpuHostedWeightStore(allocator: std.mem.Allocator, requested: BackendType) GpuWeightStore {
    if (comptime !build_options.enable_metal) unreachable;
    std.debug.assert(requested == .metal);

    return .{
        .allocator = allocator,
        .prefix = "",
        .lazy_weights = .empty,
    };
}

fn deinitGpuHostedWeightStore(allocator: std.mem.Allocator, weight_store: *GpuWeightStore, hosted_backend: BackendType) void {
    if (comptime !build_options.enable_metal) {
        return;
    }
    std.debug.assert(hosted_backend == .metal);

    if (hosted_backend == .metal) {
        if (comptime build_options.enable_metal) {
            metal_compute_mod.deinitSharedNativeProvider(weight_store);
            metal_compute_mod.deinitPackedExpertViews(weight_store, allocator);
            metal_compute_mod.deinitPrefetchQueue(weight_store);
        }
    }
    weight_store.lazy_weights.deinit(allocator);
    if (weight_store.tensor_store) |*store| store.deinit();
}

test "backend context native uses native compute backend" {
    const allocator = std.testing.allocator;
    var ctx = try BackendContext.init(allocator, .native, null);
    defer ctx.deinit(allocator);

    try std.testing.expectEqual(BackendType.native, ctx.backendType());
    try std.testing.expectEqual(ops_mod.BackendKind.native, ctx.computeBackend().kind());
}

test "shared backend context retains exact compute backend identity" {
    const allocator = std.testing.allocator;
    const shared = try SharedBackendContext.init(allocator, .native, null);
    defer shared.release();

    const retained = shared.retain();
    defer retained.release();

    const first = shared.computeBackend();
    const second = retained.computeBackend();
    try std.testing.expectEqual(first.ptr, second.ptr);
    try std.testing.expectEqual(first.vtable, second.vtable);
    try std.testing.expectEqual(BackendType.native, shared.backendType());
}


fn expectSimpleAddRoundTrip(ctx: *BackendContext, allocator: std.mem.Allocator) !void {
    var cb = ctx.computeBackend();
    const shape = [_]i32{ 2, 2 };
    const lhs = try cb.fromFloat32Shape(&[_]f32{ 1, 2, 3, 4 }, &shape);
    defer cb.free(lhs);
    const rhs = try cb.fromFloat32Shape(&[_]f32{ 10, 20, 30, 40 }, &shape);
    defer cb.free(rhs);
    const sum = try cb.add(lhs, rhs);
    defer cb.free(sum);
    const host = try cb.toFloat32(sum, allocator);
    defer allocator.free(host);
    try std.testing.expectEqualSlices(f32, &[_]f32{ 11, 22, 33, 44 }, host);
}

test "backend context metal uses metal compute backend" {
    if (!build_options.enable_metal) return error.SkipZigTest;

    const allocator = std.testing.allocator;
    var ctx = try BackendContext.init(allocator, .metal, null);
    defer ctx.deinit(allocator);

    try std.testing.expectEqual(BackendType.metal, ctx.backendType());
    try std.testing.expectEqual(ops_mod.BackendKind.metal, ctx.computeBackend().kind());
}

test "backend context metal executes simple add" {
    if (!build_options.enable_metal) return error.SkipZigTest;

    const allocator = std.testing.allocator;
    var ctx = try BackendContext.init(allocator, .metal, null);
    defer ctx.deinit(allocator);

    try expectSimpleAddRoundTrip(&ctx, allocator);
}

test "tensorShapeI32 rejects oversized dimensions" {
    const allocator = std.testing.allocator;
    try std.testing.expectError(error.UnsupportedShape, tensorShapeI32(allocator, &.{ 1, std.math.maxInt(i64) }));
}

fn tensorToOwnedI32(allocator: std.mem.Allocator, tensor: *const Tensor) ![]i32 {
    const count = tensor.elementCount();
    const out = try allocator.alloc(i32, count);
    errdefer allocator.free(out);
    switch (tensor.dtype) {
        .i8 => {
            const src = tensor.asInt8();
            for (src, 0..) |value, i| out[i] = value;
        },
        .i16 => {
            const src_bytes: [*]const u8 = tensor.data.ptr;
            for (0..count) |i| {
                const offset = i * 2;
                out[i] = std.mem.readInt(i16, src_bytes[offset..][0..2], .little);
            }
        },
        .i32 => {
            const src_bytes: [*]const u8 = tensor.data.ptr;
            for (0..count) |i| {
                const offset = i * 4;
                out[i] = std.mem.readInt(i32, src_bytes[offset..][0..4], .little);
            }
        },
        .i64 => {
            const src_bytes: [*]const u8 = tensor.data.ptr;
            for (0..count) |i| {
                const offset = i * 8;
                const value = std.mem.readInt(i64, src_bytes[offset..][0..8], .little);
                out[i] = std.math.cast(i32, value) orelse return error.UnsupportedTensorDType;
            }
        },
        .u8 => {
            const src = std.mem.bytesAsSlice(u8, tensor.data);
            for (src, 0..) |value, i| out[i] = value;
        },
        .bool_ => {
            const src = std.mem.bytesAsSlice(u8, tensor.data);
            if (src.len == count) {
                for (src, 0..) |value, i| out[i] = if (value == 0) 0 else 1;
            } else if (src.len * 8 >= count) {
                for (0..count) |i| {
                    const byte = src[i / 8];
                    const bit = (byte >> @intCast(i % 8)) & 1;
                    out[i] = if (bit == 0) 0 else 1;
                }
            } else {
                return error.InvalidTensorData;
            }
        },
        else => return error.UnsupportedTensorDType,
    }
    return out;
}

fn tensorToOwnedF32(allocator: std.mem.Allocator, tensor: *const Tensor) ![]f32 {
    const count = tensor.elementCount();
    const out = try allocator.alloc(f32, count);
    switch (tensor.dtype) {
        .f32 => {
            if (tensor.asFloat32IfAligned()) |src| {
                @memcpy(out, src);
            } else {
                const src_bytes: [*]const u8 = tensor.data.ptr;
                for (0..count) |i| {
                    const offset = i * 4;
                    const bits: u32 = @bitCast([4]u8{
                        src_bytes[offset],
                        src_bytes[offset + 1],
                        src_bytes[offset + 2],
                        src_bytes[offset + 3],
                    });
                    out[i] = @bitCast(bits);
                }
            }
        },
        .f16 => {
            const src_bytes: [*]const u8 = tensor.data.ptr;
            for (0..count) |i| {
                const offset = i * 2;
                const half: f16 = @bitCast([2]u8{ src_bytes[offset], src_bytes[offset + 1] });
                out[i] = @floatCast(half);
            }
        },
        .bf16 => {
            const src_bytes: [*]const u8 = tensor.data.ptr;
            for (0..count) |i| {
                const offset = i * 2;
                const bits: u16 = @bitCast([2]u8{ src_bytes[offset], src_bytes[offset + 1] });
                out[i] = @bitCast(@as(u32, bits) << 16);
            }
        },
        .f64 => {
            const src_bytes: [*]const u8 = tensor.data.ptr;
            for (0..count) |i| {
                const offset = i * 8;
                const bits = std.mem.readInt(u64, src_bytes[offset..][0..8], .little);
                const value: f64 = @bitCast(bits);
                out[i] = @floatCast(value);
            }
        },
        .i8 => {
            const src = tensor.asInt8();
            for (src, 0..) |value, i| out[i] = @floatFromInt(value);
        },
        .i16 => {
            const src_bytes: [*]const u8 = tensor.data.ptr;
            for (0..count) |i| {
                const offset = i * 2;
                const value = std.mem.readInt(i16, src_bytes[offset..][0..2], .little);
                out[i] = @floatFromInt(value);
            }
        },
        .i32 => {
            const src_bytes: [*]const u8 = tensor.data.ptr;
            for (0..count) |i| {
                const offset = i * 4;
                const value = std.mem.readInt(i32, src_bytes[offset..][0..4], .little);
                out[i] = @floatFromInt(value);
            }
        },
        .i64 => {
            const src_bytes: [*]const u8 = tensor.data.ptr;
            for (0..count) |i| {
                const offset = i * 8;
                const value = std.mem.readInt(i64, src_bytes[offset..][0..8], .little);
                out[i] = @floatFromInt(value);
            }
        },
        .u8 => {
            const src = std.mem.bytesAsSlice(u8, tensor.data);
            for (src, 0..) |value, i| out[i] = @floatFromInt(value);
        },
        .bool_ => {
            const src = std.mem.bytesAsSlice(u8, tensor.data);
            if (src.len == count) {
                for (src, 0..) |value, i| out[i] = if (value == 0) 0.0 else 1.0;
            } else if (src.len * 8 >= count) {
                for (0..count) |i| {
                    const byte = src[i / 8];
                    const bit = (byte >> @intCast(i % 8)) & 1;
                    out[i] = if (bit == 0) 0.0 else 1.0;
                }
            } else {
                return error.InvalidTensorData;
            }
        },
    }
    return out;
}

pub fn tensorShapeI32(allocator: std.mem.Allocator, shape: []const i64) ![]i32 {
    const converted = try allocator.alloc(i32, shape.len);
    errdefer allocator.free(converted);
    for (shape, 0..) |dim, i| {
        converted[i] = std.math.cast(i32, dim) orelse return error.UnsupportedShape;
    }
    return converted;
}

pub fn importMetalTensor(allocator: std.mem.Allocator, compute: *MetalCompute, tensor: *const Tensor) !ops_mod.CT {
    if (comptime !build_options.enable_metal) unreachable;
    const shape = try tensorShapeI32(allocator, tensor.shape);
    defer allocator.free(shape);
    switch (tensor.dtype) {
        .i8, .i16, .i32, .i64, .u8, .bool_ => {
            const cb = compute.computeBackend();
            const dtype: ops_mod.GraphDType = switch (tensor.dtype) {
                inline else => |tag| @field(ops_mod.GraphDType, @tagName(tag)),
            };
            return (try cb.fromConstantBytes(tensor.data, dtype, tensor.shape)) orelse error.UnsupportedTensorType;
        },
        else => {},
    }
    if (tensor.dtype == .f32 and @intFromPtr(tensor.data.ptr) % @alignOf(f32) == 0)
        return compute.importResidentF32(std.mem.bytesAsSlice(f32, @as([]align(@alignOf(f32)) const u8, @alignCast(tensor.data))), shape);
    const values = try tensorToOwnedF32(allocator, tensor);
    defer allocator.free(values);
    return compute.importResidentF32(values, shape);
}

fn importTensorToBackend(allocator: std.mem.Allocator, cb: *const ops_mod.ComputeBackend, tensor: *const Tensor) !ops_mod.CT {
    const dtype: ops_mod.GraphDType = switch (tensor.dtype) {
        inline else => |tag| @field(ops_mod.GraphDType, @tagName(tag)),
    };
    if (try cb.fromConstantBytes(tensor.data, dtype, tensor.shape)) |value| return value;
    const shape = try tensorShapeI32(allocator, tensor.shape);
    defer allocator.free(shape);
    switch (tensor.dtype) {
        .i32 => {
            const values = try tensorToOwnedI32(allocator, tensor);
            defer allocator.free(values);
            return (try cb.fromInt32Shape(values, shape)) orelse error.UnsupportedTensorType;
        },
        .f32 => {
            const values = try tensorToOwnedF32(allocator, tensor);
            defer allocator.free(values);
            return cb.fromFloat32Shape(values, shape);
        },
        else => return error.UnsupportedTensorType,
    }
}

test "backend context CUDA preserves typed input bytes" {
    if (comptime !build_options.enable_cuda) return error.SkipZigTest;
    const a = std.testing.allocator;
    var context = BackendContext.init(a, .cuda, null) catch |err| switch (err) {
        error.CudaUnavailable, error.NoCudaDevices => return error.SkipZigTest,
        else => return err,
    };
    defer context.deinit(a);
    const cb = context.computeBackend();
    inline for (.{ i64, i32, u8, f32, f64 }) |T| {
        const values = [_]T{ if (T == i64) 9007199254740993 else 1, 2 };
        var tensor = Tensor{ .name = "input", .data = @constCast(std.mem.sliceAsBytes(&values)), .dtype = @field(DType, @typeName(T)), .shape = &.{2}, .allocator = a, .owns_data = false, .owns_shape = false };
        const device = try context.importHostTensor(a, &tensor);
        defer cb.free(device);
        const exported = (try cb.exportTensorData(device, a)).?;
        defer a.free(exported.payload.bytes);
        try std.testing.expectEqual(tensor.dtype, exported.dtype);
        try std.testing.expectEqualSlices(u8, tensor.data, exported.payload.bytes);
    }
}
