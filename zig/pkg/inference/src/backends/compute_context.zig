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

    pub const InitOptions = struct { observe_native_allocations: bool = false };
    pub fn init(allocator: std.mem.Allocator, requested: BackendType, io: ?std.Io) !BackendContext {
        return initWithOptions(allocator, requested, io, .{});
    }
    pub fn initWithOptions(allocator: std.mem.Allocator, requested: BackendType, io: ?std.Io, options: InitOptions) !BackendContext {
        if (requested == .native and options.observe_native_allocations) {
            if (@import("../execution/allocation_observer.zig").State.forAllocator(allocator) != null) return initImpl(allocator, requested, io);
            const observer = try @import("../execution/allocation_observer.zig").State.create(allocator, .from_owner_creation);
            defer observer.release();
            return initImpl(observer.allocator(), requested, io);
        }
        return initImpl(allocator, requested, io);
    }
    fn initImpl(allocator: std.mem.Allocator, requested: BackendType, io: ?std.Io) !BackendContext {
        return switch (requested) {
            .cuda => blk: {
                if (comptime !build_options.enable_cuda) return error.CudaNotEnabled;
                const compute = try allocator.create(CudaCompute);
                errdefer allocator.destroy(compute);
                try compute.initInPlace(allocator);
                break :blk .{ .cuda = compute };
            },
            .native => blk: {
                if (comptime !build_options.enable_native) return error.NativeNotEnabled;
                const weight_store = try allocator.create(WeightStore);
                errdefer allocator.destroy(weight_store);
                weight_store.* = .{
                    .allocator = allocator,
                    .resident_weights = .empty,
                    .lazy_weights = .empty,
                };
                const backend = try @import("compute_factory.zig").createNative(allocator, weight_store, null, io, .{});
                const compute: *NativeCompute = @ptrCast(@alignCast(backend.ptr));
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
                    const store_allocator = ctx.weight_store.allocator;
                    ctx.weight_store.deinitOwned();
                    store_allocator.destroy(ctx.weight_store);
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
                ctx.compute.importOwnedDenseTensor("", dtype, shape, .{ .values = values, .allocator = allocator })
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
    /// Stable cold provider metadata; no per-request/native-kernel heap owner.
    operations: @import("../execution/operation_owner.zig").Owner = .{},
    allocator: std.mem.Allocator,
    ctx: BackendContext,
    backend: ops_mod.ComputeBackend,
    ref_count: std.atomic.Value(usize) = std.atomic.Value(usize).init(1),

    pub fn init(allocator: std.mem.Allocator, requested: BackendType, io: ?std.Io) !*SharedBackendContext {
        return initWithOptions(allocator, requested, io, .{});
    }
    pub fn initWithOptions(allocator: std.mem.Allocator, requested: BackendType, io: ?std.Io, options: BackendContext.InitOptions) !*SharedBackendContext {
        if (requested == .native and options.observe_native_allocations) {
            if (@import("../execution/allocation_observer.zig").State.forAllocator(allocator) != null) return initImpl(allocator, requested, io);
            const observer = try @import("../execution/allocation_observer.zig").State.create(allocator, .from_owner_creation);
            defer observer.release();
            return initImpl(observer.allocator(), requested, io);
        }
        return initImpl(allocator, requested, io);
    }
    fn initImpl(allocator: std.mem.Allocator, requested: BackendType, io: ?std.Io) !*SharedBackendContext {
        const shared = try allocator.create(SharedBackendContext);
        errdefer allocator.destroy(shared);
        var context = try BackendContext.init(allocator, requested, io);
        shared.* = .{ .allocator = allocator, .backend = context.computeBackend(), .ctx = context };
        if (requested == .native) {
            if (comptime build_options.enable_native) shared.ctx.native.compute.shared_operation_owner = &shared.operations;
        } else if (requested == .cuda) {
            if (comptime build_options.enable_cuda) shared.ctx.cuda.shared_operation_owner = &shared.operations;
        }
        return shared;
    }

    pub fn retain(self: *SharedBackendContext) *SharedBackendContext {
        _ = self.ref_count.fetchAdd(1, .monotonic);
        return self;
    }

    pub fn release(self: *SharedBackendContext) void {
        if (self.ref_count.fetchSub(1, .acq_rel) != 1) return;
        self.operations.assertIdle();
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

    pub fn importHostTensor(self: *SharedBackendContext, allocator: std.mem.Allocator, tensor: *const Tensor) anyerror!ops_mod.CT {
        return self.importHostTensorWithOperation(allocator, tensor, null);
    }
    pub fn importHostTensorWithOperation(self: *SharedBackendContext, allocator: std.mem.Allocator, tensor: *const Tensor, token: ?@import("../execution/backend_operation.zig").Borrowed) anyerror!ops_mod.CT {
        var scope: @import("../execution/backend_operation.zig").Scope = undefined;
        try @import("../execution/backend_operation.zig").enterInto(&self.backend, null, token, .legacy_unknown_frame, &scope);
        var succeeded = false;
        defer {
            if (succeeded) scope.finish() catch @import("antfly_platform").inference_process_supervisor.restartWorker() else scope.deinitChecked() catch @import("antfly_platform").inference_process_supervisor.restartWorker();
        }
        const result = try self.ctx.importHostTensor(allocator, tensor);
        succeeded = true;
        return result;
    }

    pub fn importStaticTensor(self: *SharedBackendContext, allocator: std.mem.Allocator, tensor: Tensor) anyerror!ops_mod.CT {
        return self.importStaticTensorWithOperation(allocator, tensor, null);
    }
    pub fn importStaticTensorWithOperation(self: *SharedBackendContext, allocator: std.mem.Allocator, tensor: Tensor, token: ?@import("../execution/backend_operation.zig").Borrowed) anyerror!ops_mod.CT {
        var scope: @import("../execution/backend_operation.zig").Scope = undefined;
        @import("../execution/backend_operation.zig").enterInto(&self.backend, null, token, .legacy_unknown_frame, &scope) catch |err| {
            var rejected = tensor;
            rejected.deinit();
            return err;
        };
        var succeeded = false;
        defer {
            if (succeeded) scope.finish() catch @import("antfly_platform").inference_process_supervisor.restartWorker() else scope.deinitChecked() catch @import("antfly_platform").inference_process_supervisor.restartWorker();
        }
        const result = try self.ctx.importStaticTensor(allocator, tensor);
        succeeded = true;
        return result;
    }

    pub fn importDenseStaticF32(self: *SharedBackendContext, allocator: std.mem.Allocator, dtype: DType, shape: []const i64, values: []f32) anyerror!ops_mod.CT {
        return self.importDenseStaticF32WithOperation(allocator, dtype, shape, values, null);
    }
    pub fn importDenseStaticF32WithOperation(self: *SharedBackendContext, allocator: std.mem.Allocator, dtype: DType, shape: []const i64, values: []f32, token: ?@import("../execution/backend_operation.zig").Borrowed) anyerror!ops_mod.CT {
        var scope: @import("../execution/backend_operation.zig").Scope = undefined;
        @import("../execution/backend_operation.zig").enterInto(&self.backend, null, token, .legacy_unknown_frame, &scope) catch |err| {
            allocator.free(values);
            return err;
        };
        var succeeded = false;
        defer {
            if (succeeded) scope.finish() catch @import("antfly_platform").inference_process_supervisor.restartWorker() else scope.deinitChecked() catch @import("antfly_platform").inference_process_supervisor.restartWorker();
        }
        const result = try self.ctx.importDenseStaticF32(allocator, dtype, shape, values);
        succeeded = true;
        return result;
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
// Draft tests for compute_context.zig after the opt-in observer integration.
test "early native observations outlive their compute through a foreign dense view" {
    const a = std.testing.allocator;
    const observers = @import("../execution/allocation_observer.zig");
    var destination = try BackendContext.init(a, .native, null);
    defer destination.deinit(a);
    var source = try BackendContext.initWithOptions(a, .native, null, .{ .observe_native_allocations = true });
    var source_open = true;
    defer if (source_open) source.deinit(a);
    const owner = observers.State.forAllocator(source.native.compute.allocator).?.retain();
    defer owner.release();
    try std.testing.expectEqual(observers.Coverage.from_owner_creation, owner.coverage);
    const source_backend = source.computeBackend();
    const destination_backend = destination.computeBackend();
    const tensor = try source_backend.fromFloat32Shape(&.{ 1, 2, 3, 4 }, &.{ 2, 2 });
    var tensor_open = true;
    defer if (tensor_open) source_backend.free(tensor);
    const alias = try destination_backend.primReshape(tensor, &.{4});
    var alias_open = true;
    defer if (alias_open) destination_backend.free(alias);
    source_backend.free(tensor);
    tensor_open = false;
    source.deinit(a);
    source_open = false;
    try std.testing.expect(owner.snapshot().requested_live_bytes >= 4 * @sizeOf(f32));
    const actual = try destination_backend.toFloat32(alias, a);
    defer a.free(actual);
    try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3, 4 }, actual);
    destination_backend.free(alias);
    alias_open = false;
    try std.testing.expectEqual(@as(u64, 0), owner.snapshot().requested_live_bytes);
}

test "early native observed context construction unwinds every allocation failure" {
    const Case = struct {
        fn run(a: std.mem.Allocator) !void {
            const observer = try @import("../execution/allocation_observer.zig").State.createWithMetadata(a, a, .from_owner_creation);
            defer observer.release();
            var context = try BackendContext.initWithOptions(observer.allocator(), .native, null, .{ .observe_native_allocations = true });
            defer context.deinit(a);
            const backend = context.computeBackend();
            const input = try backend.fromFloat32Shape(&.{ -1, 2 }, &.{2});
            defer backend.free(input);
            const output = try backend.relu(input);
            defer backend.free(output);
            const values = try backend.toFloat32(output, a);
            defer a.free(values);
            try std.testing.expectEqualSlices(f32, &.{ 0, 2 }, values);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

const ObservedNativeContextFixture = struct {
    fn run(a: std.mem.Allocator) !void {
        var context = try BackendContext.init(a, .native, null);
        defer context.deinit(a);
        try std.testing.expect(context.native.compute.data == context.native.weight_store);
        try std.testing.expectEqual(@import("../execution/allocation_registry.zig").enabled(), @import("../execution/allocation_observer.zig").State.forAllocator(context.native.compute.allocator) != null);
        try expectSimpleAddRoundTrip(&context, a);
    }
    fn observed(a: std.mem.Allocator) !void {
        var registry: @import("../execution/allocation_registry.zig").Registry = undefined;
        registry.begin(std.testing.allocator);
        defer _ = registry.finish();
        try run(a);
    }
};

test "generic Native backend context uses canonical factory with unchanged disabled allocations and observed numerics" {
    if (comptime !build_options.enable_native) return error.SkipZigTest;
    try std.testing.expect(!@import("../execution/allocation_registry.zig").enabled());
    var backing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var context = try BackendContext.init(backing.allocator(), .native, null);
    defer context.deinit(backing.allocator());
    try std.testing.expectEqual(@as(usize, 2), backing.alloc_index);
    try ObservedNativeContextFixture.run(std.testing.allocator);
    try ObservedNativeContextFixture.observed(std.testing.allocator);
}

test "generic Native backend context observed and unobserved required allocations unwind all faults" {
    if (comptime !build_options.enable_native) return error.SkipZigTest;
    var backing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(backing.allocator(), ObservedNativeContextFixture.run, .{});
    try std.testing.checkAllAllocationFailures(backing.allocator(), ObservedNativeContextFixture.observed, .{});
}

const SharedNativeObservationFixture = struct {
    const operation = @import("../execution/backend_operation.zig");
    const invocation = @import("../execution/invocation_observation.zig");
    const Diagnostics = @import("../execution/diagnostics.zig").Diagnostics;
    const Session = @import("session.zig").Session;
    const Worker = struct {
        session: Session,
        binding: invocation.Binding,
        failed: bool = false,
        fn noop() !usize {
            return 7;
        }
        fn run(self: *@This()) void {
            for (0..128) |_| {
                // This model has a DIFFERENT Session gate. The shared provider
                // gate must still reject cold allocator installation/sampling.
                if (self.session.configureExecutionDiagnostics(true)) {
                    self.failed = true;
                    return;
                }
                const value = invocation.call(self.binding, noop, .{}) catch {
                    self.failed = true;
                    return;
                };
                if (value != 7) {
                    self.failed = true;
                    return;
                }
                const observed = self.binding.diagnostics.snapshot() orelse {
                    self.failed = true;
                    return;
                };
                if (observed.context_runtime_scope != .unavailable or observed.runtime.native_allocations != null or observed.runtime.submission == null) {
                    self.failed = true;
                    return;
                }
            }
        }
    };
};

test "actual shared Native provider excludes foreign model cold enable and snapshots until its real lane is free" {
    if (comptime !build_options.enable_native) return error.SkipZigTest;
    const F = SharedNativeObservationFixture;
    const shared = try SharedBackendContext.init(std.testing.allocator, .native, null);
    defer shared.release();
    const backend = shared.computeBackend();
    const original_allocator = shared.ctx.native.compute.allocator;
    var numeric = try F.operation.enter(backend, null, null, .managed);
    defer numeric.deinitChecked() catch unreachable;
    const token = numeric.token() orelse return error.MissingSharedNativeOperation;
    var cold = F.Diagnostics{};
    var sampled = F.Diagnostics{};
    sampled.configure(true); // observation only; deliberately no backend mutation
    var foreign_gate: std.atomic.Mutex = .unlocked;
    const session = F.Session{ .ptr = shared, .vtable = undefined, .execution_gate = &foreign_gate, .invocation_diagnostics = .{ .diagnostics = &cold, .stable_backend = backend, .context_sampling = .provider_metadata } };
    var worker = F.Worker{ .session = session, .binding = .{ .diagnostics = &sampled, .stable_backend = backend, .context_sampling = .provider_metadata } };
    const thread = try std.Thread.spawn(.{}, F.Worker.run, .{&worker});
    thread.join();
    try std.testing.expect(!worker.failed and !cold.enabled.load(.acquire));
    try std.testing.expect(shared.ctx.native.compute.allocator.ptr == original_allocator.ptr and shared.ctx.native.compute.allocator.vtable == original_allocator.vtable);
    try std.testing.expectError(error.QueueFull, F.operation.enter(backend, null, null, .legacy_unknown_frame));
    // Numerical exclusion does not also own the distinct metadata/index
    // capability. Nest only the actual managed operation; reject promotion.
    try std.testing.expectError(error.BackendOperationChanged, F.operation.enter(backend, null, token, .metadata));
    var nested = try F.operation.enter(backend, null, token, .managed);
    try nested.finish();
    try numeric.finish();
    try std.testing.expectError(error.BackendOperationConsumed, token.validate(.{ .implementation = .{ .context = backend.ptr, .table = backend.vtable }, .program = null, .mode = .managed }));
    try std.testing.expect(session.configureExecutionDiagnostics(true));
    try std.testing.expect(@import("../execution/allocation_observer.zig").State.forAllocator(shared.ctx.native.compute.allocator) != null);
    try std.testing.expectEqual(@as(usize, 7), try F.invocation.call(session.invocation_diagnostics, F.Worker.noop, .{}));
    const observed = cold.snapshot().?;
    try std.testing.expect(observed.context_runtime_scope == .shared_backend_context_interval);
    try std.testing.expect(observed.runtime.native_allocations.?.compute != null);
    var metadata = try F.operation.enter(backend, null, null, .metadata);
    defer metadata.deinitChecked() catch unreachable;
    try std.testing.expectError(error.BackendOperationChanged, F.operation.enter(backend, null, metadata.token(), .managed));
    try metadata.finish();
}

test "private Native computes keep no operation owner and cannot inherit shared scope through altered callbacks" {
    if (comptime !build_options.enable_native) return error.SkipZigTest;
    const operation = @import("../execution/backend_operation.zig");
    var context = try BackendContext.init(std.testing.allocator, .native, null);
    defer context.deinit(std.testing.allocator);
    const backend = context.computeBackend();
    var raw = try operation.enter(backend, null, null, .metadata);
    try std.testing.expect(raw.token() == null);
    try raw.finish();
    try std.testing.expect(context.native.compute.shared_operation_owner == null);
    const shared = try SharedBackendContext.init(std.testing.allocator, .native, null);
    defer shared.release();
    var copied = shared.computeBackend();
    var table = copied.vtable.*;
    copied.vtable = &table;
    var declined = try operation.enter(copied, null, null, .metadata);
    try std.testing.expect(declined.token() == null);
    try declined.finish();
}

test "actual shared CUDA provider operation accepts canonical scoped budget view with no success path wait" {
    if (comptime !build_options.enable_cuda) return error.SkipZigTest;
    const operation = @import("../execution/backend_operation.zig");
    const shared = SharedBackendContext.init(std.testing.allocator, .cuda, null) catch |err| switch (err) {
        error.CudaUnavailable, error.NoCudaDevices => return error.SkipZigTest,
        else => return err,
    };
    defer shared.release();
    const backend = shared.computeBackend();
    var scope = try operation.enter(backend, null, null, .managed);
    defer scope.deinitChecked() catch unreachable;
    const token = scope.token() orelse return error.MissingSharedCudaOperation;
    var budget = @import("../runtime/tier/memory.zig").RunBudget.init(.{});
    const scoped = shared.ctx.cuda.computeBackendWithScopedRunBudget(&budget);
    var scoped_live = true;
    defer if (scoped_live) scoped.deinit();
    try std.testing.expect(scoped.ptr == backend.ptr);
    try std.testing.expectError(error.BackendOperationChanged, operation.enter(scoped, null, token, .metadata));
    var nested = try operation.enter(scoped, null, token, .managed);
    try nested.finish();
    const input = try scoped.fromFloat32Shape(&.{ 1, 2 }, &.{2});
    defer backend.free(input);
    const output = try scoped.add(input, input);
    defer backend.free(output);
    const values = try scoped.toFloat32(output, std.testing.allocator);
    defer std.testing.allocator.free(values);
    try std.testing.expectEqualSlices(f32, &.{ 2, 4 }, values);
    const waits = shared.ctx.cuda.ctx.stats.stream_syncs;
    scoped.deinit();
    scoped_live = false;
    try scope.finish();
    try std.testing.expectEqual(waits, shared.ctx.cuda.ctx.stats.stream_syncs);
    var metadata = try operation.enter(backend, null, null, .metadata);
    const before_metadata = shared.ctx.cuda.ctx.stats.stream_syncs;
    try metadata.deinitChecked();
    try std.testing.expectEqual(before_metadata, shared.ctx.cuda.ctx.stats.stream_syncs);
    var unknown = try operation.enter(backend, null, null, .legacy_unknown_frame);
    try unknown.deinitChecked();
    try std.testing.expectEqual(before_metadata, shared.ctx.cuda.ctx.stats.stream_syncs);
    shared.ctx.cuda.debug_cuda_graph_capture_active = true;
    const rejected = operation.enter(backend, null, null, .managed);
    shared.ctx.cuda.debug_cuda_graph_capture_active = false;
    if (rejected) |unexpected_value| {
        var unexpected = unexpected_value;
        try unexpected.finish();
        return error.ExpectedForeignCaptureRejection;
    } else |err| try std.testing.expectEqual(error.UnknownExternalOperationFrame, err);
    try std.testing.expectEqual(before_metadata, shared.ctx.cuda.ctx.stats.stream_syncs);
    var uncertain = try operation.enter(backend, null, null, .managed);
    const before_cleanup = shared.ctx.cuda.ctx.stats.stream_syncs;
    try uncertain.deinitChecked();
    try std.testing.expectEqual(before_cleanup + 1, shared.ctx.cuda.ctx.stats.stream_syncs);
}
