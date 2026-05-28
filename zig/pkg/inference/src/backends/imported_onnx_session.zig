// Copyright 2026 Antfly, Inc.
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
const ml = @import("ml");
const onnx_graph = @import("onnx_graph");
const c_file = @import("../util/c_file.zig");
const native_mod = if (build_options.enable_native) @import("../ops/native_compute.zig") else struct {};
const wasm_compute_mod = if (build_options.enable_wasm) @import("../ops/wasm_compute.zig") else struct {};
const gpu_store_mod = if (build_options.enable_mlx or build_options.enable_metal) @import("../ops/gpu_hosted_store.zig") else struct {};
const mlx_compute_mod = if (build_options.enable_mlx) @import("../ops/mlx_compute.zig") else struct {};
const metal_compute_mod = if (build_options.enable_metal) @import("../ops/metal_compute.zig") else struct {};
const graph_interpreter = @import("../graph/interpreter.zig");
const graph_partition = @import("../graph/partition.zig");
const metal_capabilities = @import("../graph/metal_capabilities.zig");
const graph_contracts = @import("../graph/backend_contracts.zig");
const operator_plan = @import("../graph/operator_plan.zig");
const session_mod = @import("session.zig");
const Session = session_mod.Session;
const ResidentInput = session_mod.ResidentInput;
const ResidentOutputs = session_mod.ResidentOutputs;
const Tensor = @import("tensor.zig").Tensor;
const TensorInfo = @import("tensor.zig").TensorInfo;
const DType = @import("tensor.zig").DType;
const BackendType = @import("backends.zig").BackendType;
const ops_mod = @import("../ops/ops.zig");
const graph_runtime_mod = if (build_options.enable_wasm) WasmGraphRuntime else @import("../graph/runtime.zig");

const NativeCompute = if (build_options.enable_native) native_mod.NativeCompute else opaque {};
const WeightStore = if (build_options.enable_native) native_mod.WeightStore else opaque {};
const RuntimeInput = graph_interpreter.RuntimeInput;
const CachedAnalysis = graph_interpreter.CachedAnalysis;
const Graph = ml.graph.Graph;
const Shape = ml.graph.Shape;
const NodeId = ml.graph.NodeId;
const ConstFoldPass = ml.graph.passes.const_fold;
const FusePass = ml.graph.passes.fuse;
const CsePass = ml.graph.passes.cse;
const max_onnx_model_bytes = 2 * 1024 * 1024 * 1024;
const GpuWeightStore = if (build_options.enable_mlx or build_options.enable_metal) gpu_store_mod.WeightStore else opaque {};
const MlxCompute = if (build_options.enable_mlx) mlx_compute_mod.MlxCompute else opaque {};
const MetalCompute = if (build_options.enable_metal) metal_compute_mod.MetalCompute else opaque {};
const WasmCompute = if (build_options.enable_wasm) wasm_compute_mod.WasmCompute else opaque {};
const clipclap_audio_input_frames: i64 = 1001;

const WasmGraphRuntime = struct {
    pub const Strategy = enum {
        interpreter,
        partitioned,
        compiled_preferred,
        compiled_required,
    };

    pub fn strategyFromEnv() Strategy {
        return .interpreter;
    }

    pub const Runtime = struct {
        default_backend: *const ops_mod.ComputeBackend,

        pub fn init(
            _: std.mem.Allocator,
            _: *const Graph,
            compute_backend: *const ops_mod.ComputeBackend,
            requested_strategy: Strategy,
        ) !Runtime {
            if (requested_strategy != .interpreter) return error.UnsupportedCompiledGraphRuntime;
            return .{ .default_backend = compute_backend };
        }

        pub fn deinit(_: *Runtime) void {}

        pub fn execute(
            self: *Runtime,
            allocator: std.mem.Allocator,
            graph: *const Graph,
            options: graph_interpreter.ExecuteOptions,
        ) !Result {
            var result = try graph_interpreter.execute(allocator, graph, self.default_backend, options);
            errdefer result.deinit(self.default_backend);
            const outputs = result.outputs;
            result.outputs = &.{};
            return .{
                .outputs = outputs,
                .allocator = result.allocator,
            };
        }
    };

    pub const Result = struct {
        outputs: []ops_mod.CT,
        allocator: std.mem.Allocator,

        pub fn deinit(self: *Result, runtime: *const Runtime) void {
            for (self.outputs, 0..) |ct, idx| {
                if (containsCt(self.outputs[0..idx], ct)) continue;
                runtime.default_backend.free(ct);
            }
            self.allocator.free(self.outputs);
            self.outputs = &.{};
        }
    };

    fn containsCt(values: []const ops_mod.CT, value: ops_mod.CT) bool {
        for (values) |candidate| {
            if (candidate == value) return true;
        }
        return false;
    }
};

fn shouldSpecializeClipclapAudioInputTime(path: []const u8) bool {
    const base = std.fs.path.basename(path);
    return std.mem.eql(u8, base, "audio_model.onnx");
}

const BackendContext = union(enum) {
    native: struct {
        compute: *NativeCompute,
        weight_store: *WeightStore,
    },
    mlx_hosted: struct {
        compute: *MlxCompute,
        weight_store: *GpuWeightStore,
        reported_backend: BackendType,
    },
    metal_hosted: struct {
        compute: *MetalCompute,
        weight_store: *GpuWeightStore,
    },
    wasm: struct {
        compute: *WasmCompute,
    },

    fn init(allocator: std.mem.Allocator, requested: BackendType, io: ?std.Io) !BackendContext {
        return switch (requested) {
            .native, .onnx => blk: {
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
            .mlx => blk: {
                if (comptime !build_options.enable_mlx) return error.MlxNotEnabled;
                const compute = try allocator.create(MlxCompute);
                errdefer allocator.destroy(compute);
                const weight_store = try allocator.create(GpuWeightStore);
                errdefer allocator.destroy(weight_store);
                weight_store.* = initGpuHostedWeightStore(allocator, .mlx);
                errdefer deinitGpuHostedWeightStore(allocator, weight_store, .mlx);
                compute.* = if (io) |runtime|
                    try mlx_compute_mod.MlxCompute.initMlxHostedWithIo(allocator, weight_store, null, runtime)
                else
                    try mlx_compute_mod.MlxCompute.initMlxHosted(allocator, weight_store, null);
                break :blk .{
                    .mlx_hosted = .{
                        .compute = compute,
                        .weight_store = weight_store,
                        .reported_backend = .mlx,
                    },
                };
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

    fn deinit(self: *BackendContext, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .native => |*ctx| {
                if (comptime build_options.enable_native) {
                    ctx.compute.computeBackend().deinit();
                    ctx.weight_store.resident_weights.deinit(allocator);
                    ctx.weight_store.lazy_weights.deinit(allocator);
                    native_mod.deinitPrefetchQueue(ctx.weight_store);
                    allocator.destroy(ctx.weight_store);
                } else {
                    unreachable;
                }
            },
            .mlx_hosted => |*ctx| {
                if (comptime build_options.enable_mlx) {
                    const compute: *mlx_compute_mod.MlxCompute = @ptrCast(@alignCast(ctx.compute));
                    compute.computeBackend().deinit();
                } else {
                    unreachable;
                }
                deinitGpuHostedWeightStore(allocator, ctx.weight_store, .mlx);
                allocator.destroy(ctx.weight_store);
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

    fn computeBackend(self: *BackendContext) ops_mod.ComputeBackend {
        return switch (self.*) {
            .native => |*ctx| if (comptime build_options.enable_native)
                ctx.compute.computeBackend()
            else
                unreachable,
            .mlx_hosted => |*ctx| if (comptime build_options.enable_mlx)
                (@as(*mlx_compute_mod.MlxCompute, @ptrCast(@alignCast(ctx.compute)))).computeBackend()
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

    fn backendType(self: *const BackendContext) BackendType {
        return switch (self.*) {
            .native => .native,
            .mlx_hosted => |ctx| ctx.reported_backend,
            .metal_hosted => .metal,
            .wasm => .wasm,
        };
    }

    fn importHostTensor(self: *BackendContext, allocator: std.mem.Allocator, tensor: *const Tensor) !ops_mod.CT {
        return switch (self.*) {
            .native => |*ctx| if (comptime build_options.enable_native)
                ctx.compute.importHostTensor(tensor)
            else
                unreachable,
            .mlx_hosted => |*ctx| {
                const cb = if (comptime build_options.enable_mlx)
                    (@as(*mlx_compute_mod.MlxCompute, @ptrCast(@alignCast(ctx.compute)))).computeBackend()
                else
                    unreachable;
                return importTensorToBackend(allocator, &cb, tensor);
            },
            .metal_hosted => |*ctx| {
                const cb = if (comptime build_options.enable_metal)
                    ctx.compute.computeBackend()
                else
                    unreachable;
                return importTensorToBackend(allocator, &cb, tensor);
            },
            .wasm => |*ctx| {
                const cb = if (comptime build_options.enable_wasm)
                    ctx.compute.computeBackend()
                else
                    unreachable;
                return importTensorToBackend(allocator, &cb, tensor);
            },
        };
    }

    fn importStaticTensor(self: *BackendContext, allocator: std.mem.Allocator, tensor: Tensor) !ops_mod.CT {
        return switch (self.*) {
            .native => |*ctx| if (comptime build_options.enable_native)
                ctx.compute.importOwnedStaticTensor(tensor)
            else
                unreachable,
            .mlx_hosted => |*ctx| {
                var owned_tensor = tensor;
                defer owned_tensor.deinit();
                const cb = if (comptime build_options.enable_mlx)
                    (@as(*mlx_compute_mod.MlxCompute, @ptrCast(@alignCast(ctx.compute)))).computeBackend()
                else
                    unreachable;
                return importTensorToBackend(allocator, &cb, &owned_tensor);
            },
            .metal_hosted => |*ctx| {
                var owned_tensor = tensor;
                defer owned_tensor.deinit();
                const cb = if (comptime build_options.enable_metal)
                    ctx.compute.computeBackend()
                else
                    unreachable;
                return importTensorToBackend(allocator, &cb, &owned_tensor);
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

    fn importDenseStaticF32(
        self: *BackendContext,
        allocator: std.mem.Allocator,
        dtype: DType,
        shape: []const i64,
        values: []f32,
    ) !ops_mod.CT {
        return switch (self.*) {
            .native => |*ctx| if (comptime build_options.enable_native)
                ctx.compute.importDenseTensor("", dtype, shape, values)
            else
                unreachable,
            .mlx_hosted => |*ctx| {
                defer allocator.free(values);
                const shape_i32 = try tensorShapeI32(allocator, shape);
                defer allocator.free(shape_i32);
                const cb = if (comptime build_options.enable_mlx)
                    (@as(*mlx_compute_mod.MlxCompute, @ptrCast(@alignCast(ctx.compute)))).computeBackend()
                else
                    unreachable;
                return cb.fromFloat32Shape(values, shape_i32);
            },
            .metal_hosted => |*ctx| {
                defer allocator.free(values);
                const shape_i32 = try tensorShapeI32(allocator, shape);
                defer allocator.free(shape_i32);
                const cb = if (comptime build_options.enable_metal)
                    ctx.compute.computeBackend()
                else
                    unreachable;
                return cb.fromFloat32Shape(values, shape_i32);
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
    ref_count: std.atomic.Value(usize) = std.atomic.Value(usize).init(1),

    pub fn init(allocator: std.mem.Allocator, requested: BackendType, io: ?std.Io) !*SharedBackendContext {
        const shared = try allocator.create(SharedBackendContext);
        errdefer allocator.destroy(shared);
        shared.* = .{
            .allocator = allocator,
            .ctx = try BackendContext.init(allocator, requested, io),
        };
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

    fn computeBackend(self: *SharedBackendContext) ops_mod.ComputeBackend {
        return self.ctx.computeBackend();
    }

    pub fn backendType(self: *const SharedBackendContext) BackendType {
        return self.ctx.backendType();
    }

    fn importHostTensor(self: *SharedBackendContext, allocator: std.mem.Allocator, tensor: *const Tensor) !ops_mod.CT {
        return self.ctx.importHostTensor(allocator, tensor);
    }

    fn importStaticTensor(self: *SharedBackendContext, allocator: std.mem.Allocator, tensor: Tensor) !ops_mod.CT {
        return self.ctx.importStaticTensor(allocator, tensor);
    }

    fn importDenseStaticF32(
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
    if (comptime !(build_options.enable_mlx or build_options.enable_metal)) unreachable;
    if (requested == .mlx) {
        if (comptime build_options.enable_mlx) {
            const mlx_backend = @import("../backends/mlx.zig");
            return .{
                .allocator = allocator,
                .resident_weights = mlx_backend.c.mlx_map_string_to_array_new(),
                // Match the stream initialization used by the normal MLX session
                // constructors. Using gpuStream() here bypassed that path and left
                // ONNX graph execution with an invalid/empty stream on hosted runs.
                .stream = mlx_backend.openDefaultStream().stream,
                .prefix = "",
                .lazy_weights = .empty,
            };
        }
        unreachable;
    }

    if (comptime build_options.enable_mlx) {
        return .{
            .allocator = allocator,
            .resident_weights = std.mem.zeroes(@FieldType(GpuWeightStore, "resident_weights")),
            .stream = std.mem.zeroes(@FieldType(GpuWeightStore, "stream")),
            .prefix = "",
            .lazy_weights = .empty,
        };
    }

    return .{
        .allocator = allocator,
        .resident_weights = {},
        .stream = {},
        .prefix = "",
        .lazy_weights = .empty,
    };
}

fn deinitGpuHostedWeightStore(allocator: std.mem.Allocator, weight_store: *GpuWeightStore, hosted_backend: BackendType) void {
    if (comptime !(build_options.enable_mlx or build_options.enable_metal)) {
        return;
    }
    if (hosted_backend == .mlx) {
        if (comptime build_options.enable_mlx) {
            const mlx_backend = @import("../backends/mlx.zig");
            mlx_compute_mod.deinitPrefetchQueue(weight_store);
            mlx_compute_mod.deinitPackedExpertViews(weight_store, allocator);
            weight_store.lazy_weights.deinit(allocator);
            if (weight_store.tensor_store) |*store| store.deinit();
            _ = mlx_backend.c.mlx_stream_free(weight_store.stream);
            _ = mlx_backend.c.mlx_map_string_to_array_free(weight_store.resident_weights);
            weight_store.resident_transposed_weights.deinit(allocator);
            return;
        }
        unreachable;
    }

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

fn metalShaderValidationEnabledForTest() bool {
    const c_std = @cImport(@cInclude("stdlib.h"));
    return c_std.getenv("MTL_SHADER_VALIDATION") != null;
}

test "backend context mlx uses MLX compute backend" {
    if (!build_options.enable_mlx) return error.SkipZigTest;
    if (metalShaderValidationEnabledForTest()) return error.SkipZigTest;

    const allocator = std.testing.allocator;
    var ctx = try BackendContext.init(allocator, .mlx, null);
    defer ctx.deinit(allocator);

    try std.testing.expectEqual(BackendType.mlx, ctx.backendType());
    try std.testing.expectEqual(ops_mod.BackendKind.mlx, ctx.computeBackend().kind());
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

test "backend context mlx executes simple add" {
    if (!build_options.enable_mlx) return error.SkipZigTest;
    if (metalShaderValidationEnabledForTest()) return error.SkipZigTest;

    const allocator = std.testing.allocator;
    var ctx = try BackendContext.init(allocator, .mlx, null);
    defer ctx.deinit(allocator);

    try expectSimpleAddRoundTrip(&ctx, allocator);
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

fn tensorShapeI32(allocator: std.mem.Allocator, shape: []const i64) ![]i32 {
    const converted = try allocator.alloc(i32, shape.len);
    errdefer allocator.free(converted);
    for (shape, 0..) |dim, i| {
        converted[i] = std.math.cast(i32, dim) orelse return error.UnsupportedShape;
    }
    return converted;
}

fn importTensorToBackend(allocator: std.mem.Allocator, cb: *const ops_mod.ComputeBackend, tensor: *const Tensor) !ops_mod.CT {
    if (tensorToOwnedI32(allocator, tensor)) |values| {
        defer allocator.free(values);
        const shape = try tensorShapeI32(allocator, tensor.shape);
        defer allocator.free(shape);
        if (try cb.fromInt32Shape(values, shape)) |ct| return ct;
    } else |_| {}

    const values = try tensorToOwnedF32(allocator, tensor);
    defer allocator.free(values);
    const shape = try tensorShapeI32(allocator, tensor.shape);
    defer allocator.free(shape);
    return cb.fromFloat32Shape(values, shape);
}

pub const ImportedOnnxSession = struct {
    allocator: std.mem.Allocator,
    graph: Graph,
    cached_analysis: CachedAnalysis,
    input_node_ids: []NodeId,
    input_info: []TensorInfo,
    output_info: []TensorInfo,
    static_runtime_inputs: []RuntimeInput,
    shared_backend_ctx: *SharedBackendContext,
    cb: @import("../ops/ops.zig").ComputeBackend,
    runtime: graph_runtime_mod.Runtime,

    pub fn deinit(self: *ImportedOnnxSession) void {
        for (self.input_info) |info| {
            self.allocator.free(info.name);
            self.allocator.free(info.shape);
        }
        self.allocator.free(self.input_info);

        for (self.output_info) |info| {
            self.allocator.free(info.name);
            self.allocator.free(info.shape);
        }
        self.allocator.free(self.output_info);

        for (self.static_runtime_inputs) |ri| self.cb.free(ri.value);
        self.allocator.free(self.static_runtime_inputs);
        self.allocator.free(self.input_node_ids);
        self.cached_analysis.deinit(self.allocator);
        self.runtime.deinit();
        self.graph.deinit();

        self.shared_backend_ctx.release();
        self.allocator.destroy(self);
    }
};

pub fn createSession(allocator: std.mem.Allocator, onnx_path: []const u8, requested_backend: BackendType) !Session {
    return createSessionWithOptions(allocator, onnx_path, requested_backend, .{});
}

pub const ImportedOnnxSessionOptions = struct {
    graph_runtime_strategy: ?graph_runtime_mod.Strategy = null,
    shared_backend_ctx: ?*SharedBackendContext = null,
    dim_overrides: ?*const onnx_graph.DimOverrides = null,
    /// Caller's Io runtime, used by the compute backend for parallel GEMM
    /// dispatch via `linalg.sgemm*Io`.  When null, backends fall back to
    /// the void linalg API which uses a process-wide futex pool internally.
    io: ?std.Io = null,
};

pub fn createSessionWithOptions(
    allocator: std.mem.Allocator,
    onnx_path: []const u8,
    requested_backend: BackendType,
    options: ImportedOnnxSessionOptions,
) !Session {
    if (options.shared_backend_ctx) |shared| {
        if (shared.backendType() != requested_backend) return error.SharedBackendContextMismatch;
    }

    const model_bytes = try c_file.readFileMax(allocator, onnx_path, max_onnx_model_bytes);
    defer allocator.free(model_bytes);

    const model_dir = std.fs.path.dirname(onnx_path) orelse ".";
    var model = try onnx_graph.parseLazyAsModelWithBaseDir(allocator, model_bytes, model_dir);
    defer model.deinit();

    var clipclap_audio_dim_overrides: onnx_graph.DimOverrides = .empty;
    defer clipclap_audio_dim_overrides.deinit(allocator);
    var use_clipclap_audio_dim_overrides = false;
    if (options.dim_overrides == null and shouldSpecializeClipclapAudioInputTime(onnx_path)) {
        try clipclap_audio_dim_overrides.put(allocator, "time", clipclap_audio_input_frames);
        use_clipclap_audio_dim_overrides = true;
    }
    const dim_overrides = options.dim_overrides orelse if (use_clipclap_audio_dim_overrides) &clipclap_audio_dim_overrides else null;

    var converted = try model.convertToGraphWithDims(allocator, dim_overrides);
    errdefer converted.deinit(allocator);

    var folded = try ConstFoldPass.fold(allocator, &converted.graph);
    defer folded.deinit();
    try validateImportedGraph(&folded.graph, "const_fold");

    var fused = try FusePass.fuse(allocator, &folded.graph);
    defer fused.deinit();
    try validateImportedGraph(&fused.graph, "fuse");

    var deduped = try CsePass.eliminate(allocator, &fused.graph);
    defer deduped.deinit();
    try validateImportedGraph(&deduped.graph, "cse");

    const folded_to_fused = try composeNodeIdMaps(allocator, folded.id_map, fused.id_map);
    defer allocator.free(folded_to_fused);
    const final_id_map = try composeNodeIdMaps(allocator, folded_to_fused, deduped.id_map);
    defer allocator.free(final_id_map);

    const remapped_params = try remapReachableParameters(
        allocator,
        converted.parameter_names,
        converted.graph.parameters.items,
        final_id_map,
    );
    defer {
        allocator.free(remapped_params.names);
        allocator.free(remapped_params.node_ids);
    }

    converted.graph.deinit();
    converted.graph = Graph.init(allocator);

    const graph = deduped.graph;
    deduped.graph = Graph.init(allocator);
    const parameter_names = remapped_params.names;
    const parameter_node_ids = remapped_params.node_ids;
    if (parameter_names.len != parameter_node_ids.len) {
        std.log.err("onnx graph parameter mismatch names={} nodes={}", .{ parameter_names.len, parameter_node_ids.len });
        for (parameter_node_ids, 0..) |node_id, i| {
            const node = graph.node(node_id);
            const graph_name = if (std.meta.activeTag(node.op) == .parameter) graph.parameterName(node) else "<non-parameter>";
            const converted_name = if (i < parameter_names.len) parameter_names[i] else "<missing>";
            std.log.err("onnx graph parameter[{}] graph={s} converted={s}", .{ i, graph_name, converted_name });
        }
        return error.InvalidOnnxGraph;
    }

    const input_count = model.inputCount();
    if (input_count > parameter_names.len) {
        std.log.err("onnx graph input mismatch inputs={} params={}", .{ input_count, parameter_names.len });
        return error.InvalidOnnxGraph;
    }

    const input_node_ids = try remapModelInputBindings(
        allocator,
        &model,
        parameter_names,
        parameter_node_ids,
    );
    defer allocator.free(input_node_ids);

    const static_params = try filterStaticParameters(allocator, &model, parameter_names, parameter_node_ids);
    defer {
        allocator.free(static_params.names);
        allocator.free(static_params.node_ids);
    }

    const self = try allocator.create(ImportedOnnxSession);
    errdefer allocator.destroy(self);

    // Visibility on whether the GEMM backend is composing with the caller's
    // runtime or falling back to the process-wide futex pool.  The latter is
    // correct but bypasses any cooperative scheduling -- worth flagging once
    // per session so misconfigurations are visible without needing a debugger.
    if (options.io == null) {
        std.log.debug(
            "imported_onnx_session: created without Io; matmul will use lib/linalg's *Sync futex pool",
            .{},
        );
    } else {
        std.log.debug(
            "imported_onnx_session: created with Io; matmul will dispatch through caller's runtime",
            .{},
        );
    }

    self.* = .{
        .allocator = allocator,
        .graph = graph,
        .cached_analysis = undefined,
        .input_node_ids = try allocator.dupe(NodeId, input_node_ids),
        .input_info = undefined,
        .output_info = undefined,
        .static_runtime_inputs = undefined,
        .shared_backend_ctx = if (options.shared_backend_ctx) |shared| shared.retain() else try SharedBackendContext.init(allocator, requested_backend, options.io),
        .cb = undefined,
        .runtime = undefined,
    };
    errdefer {
        allocator.free(self.input_node_ids);
        self.graph.deinit();
        self.shared_backend_ctx.release();
    }

    self.cb = self.shared_backend_ctx.computeBackend();
    self.runtime = try graph_runtime_mod.Runtime.init(
        allocator,
        &self.graph,
        &self.cb,
        options.graph_runtime_strategy orelse graph_runtime_mod.strategyFromEnv(),
    );
    errdefer self.runtime.deinit();

    self.input_info = buildInputInfo(allocator, &model, &self.graph, self.input_node_ids) catch |err| {
        std.log.err("buildInputInfo failed: {}", .{err});
        return err;
    };
    errdefer freeTensorInfoList(allocator, self.input_info);

    self.output_info = buildOutputInfo(allocator, &model, &self.graph, self.graph.outputs.items) catch |err| {
        std.log.err("buildOutputInfo failed: {} outputs={} converted_outputs={}", .{
            err,
            if (model.graph()) |g| g.outputs.len else 0,
            self.graph.outputs.items.len,
        });
        return err;
    };
    errdefer freeTensorInfoList(allocator, self.output_info);

    self.static_runtime_inputs = buildStaticRuntimeInputs(
        allocator,
        &model,
        self.shared_backend_ctx,
        static_params.names,
        static_params.node_ids,
    ) catch |err| {
        std.log.err("buildStaticRuntimeInputs failed: {}", .{err});
        return err;
    };
    errdefer {
        for (self.static_runtime_inputs) |ri| self.cb.free(ri.value);
        allocator.free(self.static_runtime_inputs);
    }

    self.cached_analysis = CachedAnalysis.compute(allocator, &self.graph) catch |err| {
        std.log.err("CachedAnalysis.compute failed: {}", .{err});
        return err;
    };
    errdefer self.cached_analysis.deinit(allocator);

    logImportedGraphBindings(&self.graph, self.input_node_ids, self.graph.outputs.items);

    allocator.free(converted.output_ids);
    allocator.free(converted.parameter_names);

    return .{
        .ptr = self,
        .vtable = &imported_session_vtable,
    };
}

const imported_session_vtable = Session.VTable{
    .run = run,
    .inputInfo = inputInfo,
    .outputInfo = outputInfo,
    .backend = backend,
    .close = close,
    .runResident = runResident,
    .runResidentInputs = runResidentInputs,
};

pub fn sharedBackendContext(session: Session) ?*SharedBackendContext {
    if (session.vtable != &imported_session_vtable) return null;
    const self: *ImportedOnnxSession = @ptrCast(@alignCast(session.ptr));
    return self.shared_backend_ctx;
}

fn importedOnnxTraceEnabled() bool {
    if (comptime build_options.enable_wasm or !build_options.link_libc) return false;
    const value = std.c.getenv("TERMITE_ONNX_GRAPH_TRACE") orelse return false;
    return value[0] != 0 and value[0] != '0';
}

fn logImportedGraphBindings(graph: *const Graph, input_node_ids: []const NodeId, output_node_ids: []const NodeId) void {
    if (!importedOnnxTraceEnabled()) return;
    for (input_node_ids, 0..) |input_id, input_idx| {
        std.log.info("termite onnx graph input index={} node_id={d}", .{ input_idx, input_id });
        if (input_id == ml.graph.null_node) continue;
        var consumer_count: usize = 0;
        for (graph.nodes.items, 0..) |node, node_idx| {
            for (node.getInputs()) |dep| {
                if (dep != input_id) continue;
                std.log.info("termite onnx graph input consumer input_index={} input_node={d} consumer_node={d} op={s} shape={any}", .{
                    input_idx,
                    input_id,
                    node_idx,
                    @tagName(std.meta.activeTag(node.op)),
                    node.output_shape,
                });
                consumer_count += 1;
            }
        }
        if (consumer_count == 0) {
            std.log.warn("termite onnx graph input index={} node_id={d} has no direct consumers", .{ input_idx, input_id });
        }
    }
    for (output_node_ids, 0..) |output_id, output_idx| {
        const node = graph.node(output_id);
        std.log.info("termite onnx graph output index={} node_id={d} op={s} inputs={any} shape={any}", .{
            output_idx,
            output_id,
            @tagName(std.meta.activeTag(node.op)),
            node.getInputs(),
            node.output_shape,
        });
    }
}

fn run(ptr: *anyopaque, inputs: []const Tensor, allocator: std.mem.Allocator) anyerror![]Tensor {
    const self: *ImportedOnnxSession = @ptrCast(@alignCast(ptr));
    var resident_outputs = try runResidentImpl(self, inputs, null, allocator);
    defer resident_outputs.deinit();

    const outputs = try allocator.alloc(Tensor, resident_outputs.outputs.len);
    var output_count: usize = 0;
    errdefer {
        for (outputs[0..output_count]) |*tensor| tensor.deinit();
        allocator.free(outputs);
    }

    for (resident_outputs.outputs, 0..) |output_ct, i| {
        const info = self.output_info[i];
        const shape = self.cb.tensorShape(output_ct, allocator) catch try allocator.dupe(i64, info.shape);
        defer allocator.free(shape);
        if (try self.cb.exportTensorData(output_ct, allocator)) |exported| {
            outputs[i] = try tensorFromExportedData(allocator, info.name, shape, exported);
        } else {
            const values = try self.cb.toFloat32(output_ct, allocator);
            defer allocator.free(values);
            outputs[i] = try tensorFromNumericValues(allocator, info.name, info.dtype, shape, values);
        }
        output_count += 1;
    }

    return outputs;
}

fn runResident(ptr: *anyopaque, inputs: []const Tensor, allocator: std.mem.Allocator) anyerror!?ResidentOutputs {
    const self: *ImportedOnnxSession = @ptrCast(@alignCast(ptr));
    return try runResidentImpl(self, inputs, null, allocator);
}

fn runResidentInputs(ptr: *anyopaque, inputs: []const ResidentInput, allocator: std.mem.Allocator) anyerror!?ResidentOutputs {
    const self: *ImportedOnnxSession = @ptrCast(@alignCast(ptr));
    return try runResidentImpl(self, null, inputs, allocator);
}

fn runResidentImpl(
    self: *ImportedOnnxSession,
    host_inputs: ?[]const Tensor,
    resident_inputs: ?[]const ResidentInput,
    allocator: std.mem.Allocator,
) !ResidentOutputs {
    const input_len = if (host_inputs) |inputs| inputs.len else if (resident_inputs) |inputs| inputs.len else 0;
    if (input_len != self.input_info.len) return error.InputArityMismatch;

    var live_dynamic_count: usize = 0;
    for (self.input_node_ids) |node_id| {
        if (node_id != ml.graph.null_node) live_dynamic_count += 1;
    }

    const dynamic_inputs = try allocator.alloc(RuntimeInput, live_dynamic_count);
    defer allocator.free(dynamic_inputs);

    var imported_count: usize = 0;
    errdefer {
        if (resident_inputs == null) {
            for (dynamic_inputs[0..imported_count]) |ri| self.cb.free(ri.value);
        }
    }

    for (self.input_node_ids, 0..) |node_id, i| {
        if (node_id == ml.graph.null_node) continue;
        const value = if (host_inputs) |inputs|
            try self.shared_backend_ctx.importHostTensor(allocator, &inputs[i])
        else if (resident_inputs) |inputs| blk: {
            try validateResidentInput(self, inputs[i]);
            break :blk inputs[i].value;
        } else unreachable;
        dynamic_inputs[imported_count] = .{
            .node_id = node_id,
            .value = value,
        };
        imported_count += 1;
    }

    const runtime_inputs = try allocator.alloc(RuntimeInput, self.static_runtime_inputs.len + dynamic_inputs.len);
    defer allocator.free(runtime_inputs);
    @memcpy(runtime_inputs[0..self.static_runtime_inputs.len], self.static_runtime_inputs);
    @memcpy(runtime_inputs[self.static_runtime_inputs.len..], dynamic_inputs);
    const sdpa_mask = if (host_inputs) |inputs| findSdpaMask(inputs) else null;

    var exec_result = try self.runtime.execute(allocator, &self.graph, .{
        .runtime_inputs = runtime_inputs,
        .sdpa_mask = sdpa_mask,
        .cached_analysis = self.cached_analysis,
    });
    errdefer exec_result.deinit(&self.runtime);

    const outputs = exec_result.outputs;
    exec_result.outputs = &.{};
    for (dynamic_inputs[0..imported_count]) |ri| {
        if (resident_inputs == null) self.cb.free(ri.value);
    }
    return .{
        .outputs = outputs,
        .backend = &self.cb,
        .allocator = exec_result.allocator,
    };
}

fn validateResidentInput(self: *ImportedOnnxSession, input: ResidentInput) !void {
    if (input.backend.ptr == self.cb.ptr and input.backend.vtable == self.cb.vtable) return;
    if (input.backend.kind() == .native and self.cb.kind() == .native) return;
    return error.UnsupportedResidentInputBackend;
}

fn findSdpaMask(inputs: []const Tensor) ?[]const i64 {
    for (inputs) |*input| {
        if (!std.mem.eql(u8, input.name, "attention_mask")) continue;
        if (input.dtype != .i64) return null;
        return input.asInt64();
    }
    return null;
}

fn inputInfo(ptr: *anyopaque) []const TensorInfo {
    const self: *ImportedOnnxSession = @ptrCast(@alignCast(ptr));
    return self.input_info;
}

fn outputInfo(ptr: *anyopaque) []const TensorInfo {
    const self: *ImportedOnnxSession = @ptrCast(@alignCast(ptr));
    return self.output_info;
}

fn backend(ptr: *anyopaque) BackendType {
    const self: *ImportedOnnxSession = @ptrCast(@alignCast(ptr));
    return self.shared_backend_ctx.backendType();
}

fn close(ptr: *anyopaque) void {
    const self: *ImportedOnnxSession = @ptrCast(@alignCast(ptr));
    self.deinit();
}

fn validateImportedGraph(graph: *const Graph, stage: []const u8) !void {
    for (graph.nodes.items, 0..) |node, node_id| {
        for (0..node.num_inputs) |input_idx| {
            const input_id = node.inputs[input_idx];
            if (input_id == ml.graph.null_node) {
                std.log.err("onnx graph invalid after {s}: node_id={} op={s} has null input at index {}", .{
                    stage,
                    node_id,
                    @tagName(std.meta.activeTag(node.op)),
                    input_idx,
                });
                return error.InvalidOnnxGraph;
            }
            if (input_id >= node_id) {
                std.log.err("onnx graph invalid after {s}: node_id={} op={s} input_id={} is not topologically earlier", .{
                    stage,
                    node_id,
                    @tagName(std.meta.activeTag(node.op)),
                    input_id,
                });
                return error.InvalidOnnxGraph;
            }
        }
    }
}

fn composeNodeIdMaps(
    allocator: std.mem.Allocator,
    prior: []const NodeId,
    next: []const NodeId,
) ![]NodeId {
    const composed = try allocator.alloc(NodeId, prior.len);
    errdefer allocator.free(composed);

    for (prior, 0..) |mid, i| {
        if (mid == ml.graph.null_node or mid >= next.len) {
            composed[i] = ml.graph.null_node;
            continue;
        }
        composed[i] = next[mid];
    }
    return composed;
}

fn buildInputInfo(
    allocator: std.mem.Allocator,
    model: *onnx_graph.Model,
    graph: *const Graph,
    input_node_ids: []const NodeId,
) ![]TensorInfo {
    const onnx_model_graph = model.graph() orelse return error.InvalidOnnxGraph;
    const infos = try allocator.alloc(TensorInfo, input_node_ids.len);
    var initialized: usize = 0;
    errdefer {
        for (infos[0..initialized]) |info| {
            allocator.free(info.name);
            allocator.free(info.shape);
        }
        allocator.free(infos);
    }

    var out_idx: usize = 0;
    for (onnx_model_graph.inputs) |input| {
        if (input.name.len == 0 or !model.input_set.contains(input.name)) continue;
        const node_id = input_node_ids[out_idx];
        const shape = if (node_id != ml.graph.null_node) graph.node(node_id).output_shape else Shape.init(.f32, &.{});
        infos[out_idx] = try tensorInfoFromValueOrShape(
            allocator,
            input.name,
            input.type_proto,
            shape,
        );
        initialized += 1;
        out_idx += 1;
    }
    if (out_idx != infos.len) return error.InvalidOnnxGraph;
    return infos;
}

test "composeNodeIdMaps preserves reachable nodes across passes" {
    const allocator = std.testing.allocator;
    const prior = [_]NodeId{ 4, 2, ml.graph.null_node, 1 };
    const next = [_]NodeId{ 9, 7, 5, 3, 1 };

    const composed = try composeNodeIdMaps(allocator, &prior, &next);
    defer allocator.free(composed);

    try std.testing.expectEqualSlices(NodeId, &.{ 1, 5, ml.graph.null_node, 7 }, composed);
}

fn buildOutputInfo(
    allocator: std.mem.Allocator,
    model: *onnx_graph.Model,
    graph: *const Graph,
    output_node_ids: []const NodeId,
) ![]TensorInfo {
    const onnx_model_graph = model.graph() orelse return error.InvalidOnnxGraph;
    if (onnx_model_graph.outputs.len != output_node_ids.len) return error.InvalidOnnxGraph;

    const infos = try allocator.alloc(TensorInfo, output_node_ids.len);
    var initialized: usize = 0;
    errdefer {
        for (infos[0..initialized]) |info| {
            allocator.free(info.name);
            allocator.free(info.shape);
        }
        allocator.free(infos);
    }

    for (onnx_model_graph.outputs, output_node_ids, 0..) |output, node_id, i| {
        infos[i] = try tensorInfoFromValueOrShape(
            allocator,
            output.name,
            output.type_proto,
            graph.node(node_id).output_shape,
        );
        initialized += 1;
    }
    return infos;
}

fn buildStaticRuntimeInputs(
    allocator: std.mem.Allocator,
    model: *onnx_graph.Model,
    backend_ctx: *SharedBackendContext,
    parameter_names: [][]const u8,
    parameter_node_ids: []const NodeId,
) ![]RuntimeInput {
    if (parameter_names.len != parameter_node_ids.len) return error.InvalidOnnxGraph;
    const runtime_inputs = try allocator.alloc(RuntimeInput, parameter_names.len);
    var initialized: usize = 0;
    errdefer {
        for (runtime_inputs[0..initialized]) |ri| backend_ctx.computeBackend().free(ri.value);
        allocator.free(runtime_inputs);
    }

    for (parameter_names, parameter_node_ids, 0..) |name, node_id, i| {
        const init = model.getInitializer(name) orelse return error.MissingWeight;
        if (try maybeDirectStaticTensor(allocator, model, init)) |tensor| {
            runtime_inputs[i] = .{
                .node_id = node_id,
                .value = try backend_ctx.importStaticTensor(allocator, tensor),
            };
            initialized += 1;
            continue;
        }

        const init_dtype = dtypeFromShape(init.shape);
        const init_shape = try shapeSliceFromOwnedShape(allocator, init.shape);
        defer allocator.free(init_shape);
        const values = try model.loadInitializerData(allocator, name);
        const imported = backend_ctx.importDenseStaticF32(
            allocator,
            init_dtype,
            init_shape,
            values,
        ) catch |err| {
            std.log.err("importDenseStaticF32 failed name={s} dtype={s} shape={any} err={}", .{
                name,
                @tagName(init_dtype),
                init_shape,
                err,
            });
            return err;
        };
        runtime_inputs[i] = .{
            .node_id = node_id,
            .value = imported,
        };
        initialized += 1;
    }

    return runtime_inputs;
}

const RemappedParameters = struct {
    names: [][]const u8,
    node_ids: []NodeId,
};

fn remapModelInputBindings(
    allocator: std.mem.Allocator,
    model: *onnx_graph.Model,
    remapped_parameter_names: [][]const u8,
    remapped_parameter_node_ids: []const NodeId,
) ![]NodeId {
    const onnx_model_graph = model.graph() orelse return error.InvalidOnnxGraph;
    const input_count = model.inputCount();
    const input_node_ids = try allocator.alloc(NodeId, input_count);
    var out_idx: usize = 0;

    for (onnx_model_graph.inputs) |input| {
        if (input.name.len == 0 or !model.input_set.contains(input.name)) continue;
        input_node_ids[out_idx] = ml.graph.null_node;

        for (remapped_parameter_names, remapped_parameter_node_ids) |remapped_name, remapped_node_id| {
            if (std.mem.eql(u8, remapped_name, input.name)) {
                input_node_ids[out_idx] = remapped_node_id;
                break;
            }
        }

        out_idx += 1;
    }

    if (out_idx != input_count) {
        allocator.free(input_node_ids);
        return error.InvalidOnnxGraph;
    }
    return input_node_ids;
}

fn filterStaticParameters(
    allocator: std.mem.Allocator,
    model: *onnx_graph.Model,
    parameter_names: [][]const u8,
    parameter_node_ids: []const NodeId,
) !RemappedParameters {
    if (parameter_names.len != parameter_node_ids.len) return error.InvalidOnnxGraph;

    var count: usize = 0;
    for (parameter_names) |name| {
        if (!model.input_set.contains(name)) count += 1;
    }

    const names = try allocator.alloc([]const u8, count);
    errdefer allocator.free(names);
    const node_ids = try allocator.alloc(NodeId, count);
    errdefer allocator.free(node_ids);

    var out_idx: usize = 0;
    for (parameter_names, parameter_node_ids) |name, node_id| {
        if (model.input_set.contains(name)) continue;
        names[out_idx] = name;
        node_ids[out_idx] = node_id;
        out_idx += 1;
    }

    return .{ .names = names, .node_ids = node_ids };
}

fn remapReachableParameters(
    allocator: std.mem.Allocator,
    parameter_names: [][]const u8,
    parameter_node_ids: []const NodeId,
    id_map: []const NodeId,
) !RemappedParameters {
    if (parameter_names.len != parameter_node_ids.len) return error.InvalidOnnxGraph;

    var count: usize = 0;
    for (parameter_node_ids) |old_id| {
        if (old_id < id_map.len and id_map[old_id] != ml.graph.null_node) count += 1;
    }

    const names = try allocator.alloc([]const u8, count);
    errdefer allocator.free(names);
    const node_ids = try allocator.alloc(NodeId, count);
    errdefer allocator.free(node_ids);

    var out_idx: usize = 0;
    for (parameter_names, parameter_node_ids) |name, old_id| {
        if (old_id >= id_map.len) continue;
        const new_id = id_map[old_id];
        if (new_id == ml.graph.null_node) continue;
        names[out_idx] = name;
        node_ids[out_idx] = new_id;
        out_idx += 1;
    }

    return .{ .names = names, .node_ids = node_ids };
}

fn maybeDirectStaticTensor(
    allocator: std.mem.Allocator,
    model: *onnx_graph.Model,
    init: onnx_graph.InitializerData,
) !?Tensor {
    const dtype = dtypeFromShape(init.shape);
    if (init.shape.rank() != 2) return null;
    if (dtype != .f32 and dtype != .f16 and dtype != .bf16) return null;

    if (init.tensor.raw_data.len > 0) {
        return .{
            .data = try allocator.dupe(u8, init.tensor.raw_data),
            .dtype = dtype,
            .shape = try shapeSliceFromOwnedShape(allocator, init.shape),
            .name = init.name,
            .allocator = allocator,
            .owns_data = true,
            .owns_shape = true,
        };
    }

    if (init.external) |external| {
        const base_dir = model.base_dir orelse return null;
        if (external.location.len == 0) return null;
        if (std.fs.path.isAbsolute(external.location)) return error.InvalidExternalPath;
        if (std.mem.indexOf(u8, external.location, "..") != null) return error.InvalidExternalPath;

        const full_path = try std.fs.path.join(allocator, &.{ base_dir, external.location });
        defer allocator.free(full_path);

        if (external.offset < 0) return error.InvalidExternalOffset;
        const offset: u64 = @intCast(external.offset);
        const len: usize = if (external.length < 0) blk: {
            const size = try c_file.fileSize(allocator, full_path);
            if (offset > size) return error.ExternalRegionOutOfBounds;
            break :blk @intCast(size - offset);
        } else @intCast(external.length);

        return .{
            .data = try c_file.readRegion(allocator, full_path, offset, len),
            .dtype = dtype,
            .shape = try shapeSliceFromOwnedShape(allocator, init.shape),
            .name = init.name,
            .allocator = allocator,
            .owns_data = true,
            .owns_shape = true,
        };
    }

    return null;
}

fn tensorInfoFromValueOrShape(
    allocator: std.mem.Allocator,
    name: []const u8,
    type_proto: ?onnx_graph.proto.TypeProto,
    fallback_shape: Shape,
) !TensorInfo {
    const owned_name = try allocator.dupe(u8, name);
    errdefer allocator.free(owned_name);

    if (type_proto) |tp| {
        if (tp.tensor_type) |tt| {
            const dtype = try onnxDataTypeToBackend(tt.elem_type);
            const dims = if (tt.shape) |shape| try dimsFromTensorShapeProto(allocator, shape) else try allocator.dupe(i64, &.{});
            return .{ .name = owned_name, .dtype = dtype, .shape = dims };
        }
    }

    return .{
        .name = owned_name,
        .dtype = dtypeFromShape(fallback_shape),
        .shape = try shapeSliceFromOwnedShape(allocator, fallback_shape),
    };
}

fn dimsFromTensorShapeProto(
    allocator: std.mem.Allocator,
    shape: onnx_graph.proto.TensorShapeProto,
) ![]i64 {
    const dims = try allocator.alloc(i64, shape.dims.len);
    for (shape.dims, 0..) |dim, i| {
        dims[i] = dim.dim_value orelse -1;
    }
    return dims;
}

fn shapeSliceFromOwnedShape(allocator: std.mem.Allocator, shape: Shape) ![]i64 {
    const rank = shape.rank();
    const dims = try allocator.alloc(i64, rank);
    for (0..rank) |i| dims[i] = shape.dim(@intCast(i));
    return dims;
}

fn shapeSliceFromShape(shape: Shape) []const i64 {
    return shape.dims[0..shape.rank()];
}

fn dtypeFromShape(shape: Shape) DType {
    return switch (shape.dtype) {
        .f32 => .f32,
        .f16 => .f16,
        .bf16 => .bf16,
        .f64 => .f64,
        .i8 => .i8,
        .i16 => .i16,
        .i32 => .i32,
        .i64 => .i64,
        .u8 => .u8,
        .bool_ => .bool_,
    };
}

fn onnxDataTypeToBackend(dtype: onnx_graph.proto.DataType) !DType {
    return switch (dtype) {
        .float32 => .f32,
        .float16 => .f16,
        .bfloat16 => .bf16,
        .float64 => .f64,
        .int8 => .i8,
        .int16 => .i16,
        .int32 => .i32,
        .int64 => .i64,
        .uint8 => .u8,
        .bool_ => .bool_,
        else => error.UnsupportedDType,
    };
}

fn tensorFromNumericValues(
    allocator: std.mem.Allocator,
    name: []const u8,
    dtype: DType,
    shape: []const i64,
    values: []const f32,
) !Tensor {
    return switch (dtype) {
        .f32 => Tensor.initFloat32(allocator, name, shape, values),
        .f64 => blk: {
            const data = try allocator.alloc(f64, values.len);
            defer allocator.free(data);
            for (values, 0..) |value, i| data[i] = value;
            break :blk Tensor.initFloat64(allocator, name, shape, data);
        },
        .i64 => blk: {
            const data = try allocator.alloc(i64, values.len);
            defer allocator.free(data);
            for (values, 0..) |value, i| data[i] = @intFromFloat(@round(value));
            break :blk Tensor.initInt64(allocator, name, shape, data);
        },
        .i16 => blk: {
            const data = try allocator.alloc(i16, values.len);
            defer allocator.free(data);
            for (values, 0..) |value, i| data[i] = @intFromFloat(@round(value));
            break :blk Tensor.initInt16(allocator, name, shape, data);
        },
        .i8 => blk: {
            const data = try allocator.alloc(i8, values.len);
            defer allocator.free(data);
            for (values, 0..) |value, i| data[i] = @intFromFloat(@round(value));
            break :blk Tensor.initInt8(allocator, name, shape, data);
        },
        .i32 => initInt32Tensor(allocator, name, shape, values),
        .u8 => initU8Tensor(allocator, name, shape, values),
        .bool_ => initBoolTensor(allocator, name, shape, values),
        .f16 => initF16Tensor(allocator, name, shape, values),
        .bf16 => initBf16Tensor(allocator, name, shape, values),
    };
}

fn tensorFromExportedData(
    allocator: std.mem.Allocator,
    name: []const u8,
    shape: []const i64,
    exported: @import("../ops/ops.zig").ExportTensorData,
) !Tensor {
    errdefer freeExportedTensorData(allocator, exported);

    return switch (exported.payload) {
        .bytes => |bytes| .{
            .data = bytes,
            .dtype = exported.dtype,
            .shape = try allocator.dupe(i64, shape),
            .name = name,
            .allocator = allocator,
            .owns_data = true,
            .owns_shape = true,
        },
        .quantized_f32 => |quantized| blk: {
            const values = try allocator.alloc(f32, countElements(shape));
            errdefer allocator.free(values);
            try @import("../gguf/quant_codec.zig").dequantizeToFloat32(
                quantized.tensor_type,
                quantized.raw_bytes,
                values,
            );
            defer allocator.free(values);
            defer allocator.free(quantized.raw_bytes);
            defer allocator.free(quantized.shape);
            break :blk Tensor.initFloat32(allocator, name, shape, values);
        },
    };
}

fn freeExportedTensorData(
    allocator: std.mem.Allocator,
    exported: @import("../ops/ops.zig").ExportTensorData,
) void {
    switch (exported.payload) {
        .bytes => |bytes| allocator.free(bytes),
        .quantized_f32 => |quantized| {
            allocator.free(quantized.raw_bytes);
            allocator.free(quantized.shape);
        },
    }
}

fn countElements(shape: []const i64) usize {
    var total: usize = 1;
    for (shape) |dim| total *= @intCast(dim);
    return total;
}

fn initInt32Tensor(
    allocator: std.mem.Allocator,
    name: []const u8,
    shape: []const i64,
    values: []const f32,
) !Tensor {
    const bytes = try allocator.alloc(u8, values.len * @sizeOf(i32));
    errdefer allocator.free(bytes);
    for (values, 0..) |value, i| {
        const casted: i32 = @intFromFloat(@round(value));
        std.mem.writeInt(i32, bytes[i * 4 ..][0..4], casted, .little);
    }
    const owned_shape = try allocator.dupe(i64, shape);
    return .{
        .data = bytes,
        .dtype = .i32,
        .shape = owned_shape,
        .name = name,
        .allocator = allocator,
        .owns_data = true,
        .owns_shape = true,
    };
}

fn initU8Tensor(
    allocator: std.mem.Allocator,
    name: []const u8,
    shape: []const i64,
    values: []const f32,
) !Tensor {
    const bytes = try allocator.alloc(u8, values.len);
    errdefer allocator.free(bytes);
    for (values, 0..) |value, i| {
        const rounded = @round(value);
        const clamped = @max(@as(f32, 0), @min(@as(f32, 255), rounded));
        bytes[i] = @intFromFloat(clamped);
    }
    const owned_shape = try allocator.dupe(i64, shape);
    return .{
        .data = bytes,
        .dtype = .u8,
        .shape = owned_shape,
        .name = name,
        .allocator = allocator,
        .owns_data = true,
        .owns_shape = true,
    };
}

fn initBoolTensor(
    allocator: std.mem.Allocator,
    name: []const u8,
    shape: []const i64,
    values: []const f32,
) !Tensor {
    const bytes = try allocator.alloc(u8, values.len);
    errdefer allocator.free(bytes);
    for (values, 0..) |value, i| bytes[i] = if (@round(value) == 0) 0 else 1;
    const owned_shape = try allocator.dupe(i64, shape);
    return .{
        .data = bytes,
        .dtype = .bool_,
        .shape = owned_shape,
        .name = name,
        .allocator = allocator,
        .owns_data = true,
        .owns_shape = true,
    };
}

fn initF16Tensor(
    allocator: std.mem.Allocator,
    name: []const u8,
    shape: []const i64,
    values: []const f32,
) !Tensor {
    const bytes = try allocator.alloc(u8, values.len * @sizeOf(f16));
    errdefer allocator.free(bytes);
    for (values, 0..) |value, i| {
        const casted: f16 = @floatCast(value);
        const bits: u16 = @bitCast(casted);
        std.mem.writeInt(u16, bytes[i * 2 ..][0..2], bits, .little);
    }
    const owned_shape = try allocator.dupe(i64, shape);
    return .{
        .data = bytes,
        .dtype = .f16,
        .shape = owned_shape,
        .name = name,
        .allocator = allocator,
        .owns_data = true,
        .owns_shape = true,
    };
}

fn initBf16Tensor(
    allocator: std.mem.Allocator,
    name: []const u8,
    shape: []const i64,
    values: []const f32,
) !Tensor {
    const bytes = try allocator.alloc(u8, values.len * @sizeOf(u16));
    errdefer allocator.free(bytes);
    for (values, 0..) |value, i| {
        const bits: u32 = @bitCast(value);
        const bf16_bits: u16 = @truncate(bits >> 16);
        std.mem.writeInt(u16, bytes[i * 2 ..][0..2], bf16_bits, .little);
    }
    const owned_shape = try allocator.dupe(i64, shape);
    return .{
        .data = bytes,
        .dtype = .bf16,
        .shape = owned_shape,
        .name = name,
        .allocator = allocator,
        .owns_data = true,
        .owns_shape = true,
    };
}

fn freeTensorInfoList(allocator: std.mem.Allocator, infos: []TensorInfo) void {
    for (infos) |info| {
        allocator.free(info.name);
        allocator.free(info.shape);
    }
    allocator.free(infos);
}

test "imported onnx session runs simple add model" {
    const allocator = std.testing.allocator;

    var graph = Graph.init(allocator);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const x = try builder.parameter("x", Shape.init(.f32, &.{4}));
    const bias = try builder.tensorConst(&.{ 0.1, 0.2, 0.3, 0.4 }, Shape.init(.f32, &.{4}));
    const sum = try builder.add(x, bias);
    try graph.markOutput(sum);

    const model_bytes = try onnx_graph.exportGraph(allocator, &graph, .{});
    defer allocator.free(model_bytes);

    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try dir.dir.writeFile(std.testing.io, .{ .sub_path = "model.onnx", .data = model_bytes });

    const path = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", dir.sub_path[0..], "model.onnx" });
    defer allocator.free(path);

    var session = try createSessionWithOptions(allocator, path, .native, .{
        .graph_runtime_strategy = .partitioned,
    });
    defer session.close();

    const input = [_]f32{ 1.0, 2.0, 3.0, 4.0 };
    var input_tensor = try Tensor.initFloat32(allocator, "x", &.{4}, &input);
    defer input_tensor.deinit();

    var outputs = try session.run(&.{input_tensor}, allocator);
    defer {
        for (outputs) |*tensor| tensor.deinit();
        allocator.free(outputs);
    }

    try std.testing.expectEqual(@as(usize, 1), outputs.len);
    try std.testing.expectEqual(DType.f32, outputs[0].dtype);
    try std.testing.expectEqualSlices(i64, &.{4}, outputs[0].shape);
    const actual = outputs[0].asFloat32();
    const expected = [_]f32{ 1.1, 2.2, 3.3, 4.4 };
    for (expected, actual) |want, got| {
        try std.testing.expectApproxEqAbs(want, got, 1e-5);
    }
}

test "imported onnx l2 normalize tail plans to metal without device" {
    const allocator = std.testing.allocator;

    var graph = Graph.init(allocator);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);

    const batch: i64 = 4;
    const dim: i64 = 768;
    const x = try builder.parameter("projected", Shape.init(.f32, &.{ batch, dim }));
    const squared = try builder.mul(x, x);
    const sum_sq = try builder.reduceSum(squared, &.{1});
    const norm = try builder.sqrt(sum_sq);
    const norm_bc = try graph.addNode(.{
        .op = .{ .broadcast_in_dim = .{
            .target_shape = Shape.init(.f32, &.{ batch, dim }),
            .broadcast_axes = .{ 0, 1, 0, 0, 0, 0, 0, 0 },
            .num_axes = 2,
        } },
        .output_shape = Shape.init(.f32, &.{ batch, dim }),
        .inputs = .{ norm, ml.graph.null_node, ml.graph.null_node, ml.graph.null_node },
        .num_inputs = 1,
    });
    const normalized = try builder.div(x, norm_bc);
    try graph.markOutput(normalized);

    const model_bytes = try onnx_graph.exportGraph(allocator, &graph, .{});
    defer allocator.free(model_bytes);

    var model = try onnx_graph.parseLazyAsModel(allocator, model_bytes);
    defer model.deinit();
    var converted = try model.convertToGraph(allocator);
    defer converted.deinit(allocator);
    var folded = try ConstFoldPass.fold(allocator, &converted.graph);
    defer folded.deinit();
    var fused = try FusePass.fuse(allocator, &folded.graph);
    defer fused.deinit();
    var deduped = try CsePass.eliminate(allocator, &fused.graph);
    defer deduped.deinit();

    try expectNormalizeTailPlansForMetal(&deduped.graph);
}

fn defaultClipclapTextModelPath(allocator: std.mem.Allocator) ![]u8 {
    return defaultClipclapModelPath(allocator, "text_model.onnx");
}

fn defaultClipclapModelPath(allocator: std.mem.Allocator, file_name: []const u8) ![]u8 {
    const home = std.c.getenv("HOME") orelse return error.SkipZigTest;
    return std.fs.path.join(allocator, &.{ std.mem.span(home), ".termite", "models", "antflydb", "clipclap", file_name });
}

test "imported onnx session executes clipclap text model natively with padded mask" {
    const allocator = std.testing.allocator;
    const path = try defaultClipclapTextModelPath(allocator);
    defer allocator.free(path);

    var session = createSession(allocator, path, .native) catch |err| switch (err) {
        error.FileNotFound => return error.SkipZigTest,
        else => return err,
    };
    defer session.close();

    const seq_len = 77;
    var input_ids_data: [seq_len]i64 = undefined;
    var attention_mask_data: [seq_len]i64 = undefined;
    @memset(input_ids_data[0..], 1);
    @memset(attention_mask_data[0..], 0);
    @memset(attention_mask_data[0..9], 1);

    var input_ids = try Tensor.initInt64(allocator, "input_ids", &.{ 1, seq_len }, &input_ids_data);
    defer input_ids.deinit();
    var attention_mask = try Tensor.initInt64(allocator, "attention_mask", &.{ 1, seq_len }, &attention_mask_data);
    defer attention_mask.deinit();

    var outputs = try session.run(&.{ input_ids, attention_mask }, allocator);
    defer {
        for (outputs) |*tensor| tensor.deinit();
        allocator.free(outputs);
    }

    try std.testing.expect(outputs.len > 0);
    try std.testing.expectEqual(DType.f32, outputs[0].dtype);
    try std.testing.expect(outputs[0].elementCount() > 0);
    for (outputs[0].asFloat32()) |value| {
        try std.testing.expect(std.math.isFinite(value));
    }
}

test "imported onnx session loads clipclap audio model natively with cubic resize" {
    const allocator = std.testing.allocator;
    const path = try defaultClipclapModelPath(allocator, "audio_model.onnx");
    defer allocator.free(path);

    var session = createSession(allocator, path, .native) catch |err| switch (err) {
        error.FileNotFound => return error.SkipZigTest,
        else => return err,
    };
    defer session.close();

    const imported: *ImportedOnnxSession = @ptrCast(@alignCast(session.ptr));
    var found_input_features = false;
    for (session.inputInfo(), 0..) |info, i| {
        if (!std.mem.eql(u8, info.name, "input_features")) continue;
        found_input_features = true;
        const input_shape = imported.graph.node(imported.input_node_ids[i]).output_shape;
        try std.testing.expect(input_shape.rank() >= 3);
        try std.testing.expectEqual(@as(i64, clipclap_audio_input_frames), input_shape.dim(2));
    }
    try std.testing.expect(found_input_features);
}

test "imported onnx session loads clipclap text model with mlx" {
    if (!build_options.enable_mlx) return error.SkipZigTest;

    const allocator = std.testing.allocator;
    const path = try defaultClipclapTextModelPath(allocator);
    defer allocator.free(path);

    var session = createSession(allocator, path, .mlx) catch |err| switch (err) {
        error.FileNotFound => return error.SkipZigTest,
        else => return err,
    };
    defer session.close();
}

test "imported onnx session plans local clipclap text projections and transposes for metal without device" {
    try expectClipclapOnnxPlansForMetal("text_model.onnx", .{
        .require_sdpa = true,
        .require_transpose = true,
        .require_matmul = true,
    });
    try expectClipclapOnnxPlansForMetal("text_projection.onnx", .{
        .require_matmul = true,
    });
}

test "imported onnx session plans local clipclap visual model and projection for metal without device" {
    try expectClipclapOnnxPlansForMetal("visual_model.onnx", .{
        .require_transpose = true,
        .require_matmul = true,
    });
    try expectClipclapOnnxPlansForMetal("visual_projection.onnx", .{
        .require_matmul = true,
    });
}

test "imported onnx session plans local clipclap audio model and projection for metal without device" {
    try expectClipclapOnnxPlansForMetal("audio_model.onnx", .{
        .require_transpose = true,
        .require_matmul = true,
    });
    try expectClipclapOnnxPlansForMetal("audio_projection.onnx", .{
        .require_matmul = true,
    });
}

const ClipclapPlannerExpectations = struct {
    require_sdpa: bool = false,
    require_transpose: bool = false,
    require_matmul: bool = false,
};

fn expectNormalizeTailPlansForMetal(graph: *const Graph) !void {
    const allocator = std.testing.allocator;
    const seeds = try graph_partition.allocTensorDescriptorSeeds(allocator, graph);
    defer allocator.free(seeds);
    try seedF32GraphResidencyForMetal(seeds, graph);

    const caps = [_]graph_partition.Capability{
        .{ .backend = .metal, .priority = 20, .decide = &metal_capabilities.decideMetalEagerGraph },
        .{ .backend = .native, .priority = 0, .decide = &graph_partition.decideNative },
    };
    var diagnostics = graph_partition.CapabilityDiagnostics{};
    var plan = try graph_partition.partitionWithOptions(allocator, graph, &caps, .{
        .tensor_descs = seeds,
        .diagnostics = &diagnostics,
    });
    defer plan.deinit();

    var reduce_count: usize = 0;
    var metal_reduce_count: usize = 0;
    var unary_count: usize = 0;
    var metal_unary_count: usize = 0;
    var broadcast_count: usize = 0;
    var metal_broadcast_count: usize = 0;
    var normalize_binary_count: usize = 0;
    var metal_normalize_binary_count: usize = 0;

    for (0..graph.nodeCount()) |idx| {
        const node_id: NodeId = @intCast(idx);
        const node = graph.node(node_id);
        const planned_backend = plan.partitions[plan.node_assignment[node_id]].backend;
        switch (node.op) {
            .reduce_sum, .reduce_mean, .reduce_max => {
                if (!clipclapNodeIsLastDimF32Reduce(graph, node_id)) continue;
                reduce_count += 1;
                if (planned_backend == .metal) metal_reduce_count += 1;
            },
            .sqrt, .rsqrt => {
                if (node.output_shape.dtype != .f32) continue;
                unary_count += 1;
                if (planned_backend == .metal) metal_unary_count += 1;
            },
            .broadcast_in_dim => {
                if (!clipclapNodeIsNormalizeBroadcastTail(graph, node_id)) continue;
                broadcast_count += 1;
                if (planned_backend == .metal) metal_broadcast_count += 1;
            },
            .mul, .div => {
                if (!clipclapNodeConsumesNormalizeBroadcastTail(graph, node_id)) continue;
                normalize_binary_count += 1;
                if (planned_backend == .metal) metal_normalize_binary_count += 1;
            },
            else => {},
        }
    }

    if (reduce_count != metal_reduce_count or
        unary_count != metal_unary_count or
        broadcast_count != metal_broadcast_count or
        normalize_binary_count != metal_normalize_binary_count)
    {
        dumpNormalizePlannerMismatches(graph, &plan, seeds);
    }
    try std.testing.expect(reduce_count > 0);
    try std.testing.expect(unary_count > 0);
    try std.testing.expect(broadcast_count > 0);
    try std.testing.expect(normalize_binary_count > 0);
    try std.testing.expectEqual(reduce_count, metal_reduce_count);
    try std.testing.expectEqual(unary_count, metal_unary_count);
    try std.testing.expectEqual(broadcast_count, metal_broadcast_count);
    try std.testing.expectEqual(normalize_binary_count, metal_normalize_binary_count);
    try std.testing.expectEqual(@as(usize, 0), diagnostics.count(.missing_quant_kernel));
}

fn expectClipclapOnnxPlansForMetal(file_name: []const u8, expectations: ClipclapPlannerExpectations) !void {
    const allocator = std.testing.allocator;
    const path = try defaultClipclapModelPath(allocator, file_name);
    defer allocator.free(path);

    const model_bytes = c_file.readFileMax(allocator, path, max_onnx_model_bytes) catch |err| switch (err) {
        error.FileNotFound => return error.SkipZigTest,
        else => return err,
    };
    defer allocator.free(model_bytes);

    const model_dir = std.fs.path.dirname(path) orelse ".";
    var model = try onnx_graph.parseLazyAsModelWithBaseDir(allocator, model_bytes, model_dir);
    defer model.deinit();

    var clipclap_audio_dim_overrides: onnx_graph.DimOverrides = .empty;
    defer clipclap_audio_dim_overrides.deinit(allocator);
    var use_clipclap_audio_dim_overrides = false;
    if (shouldSpecializeClipclapAudioInputTime(path)) {
        try clipclap_audio_dim_overrides.put(allocator, "time", clipclap_audio_input_frames);
        use_clipclap_audio_dim_overrides = true;
    }
    const dim_overrides: ?*const onnx_graph.DimOverrides = if (use_clipclap_audio_dim_overrides) &clipclap_audio_dim_overrides else null;

    var converted = try model.convertToGraphWithDims(allocator, dim_overrides);
    defer converted.deinit(allocator);
    var folded = try ConstFoldPass.fold(allocator, &converted.graph);
    defer folded.deinit();
    var fused = try FusePass.fuse(allocator, &folded.graph);
    defer fused.deinit();
    var deduped = try CsePass.eliminate(allocator, &fused.graph);
    defer deduped.deinit();
    specializeGraphDynamicDimsForPlanner(&deduped.graph, 1);

    const graph = &deduped.graph;
    const seeds = try graph_partition.allocTensorDescriptorSeeds(allocator, graph);
    defer allocator.free(seeds);
    try seedF32GraphResidencyForMetal(seeds, graph);

    const caps = [_]graph_partition.Capability{
        .{ .backend = .metal, .priority = 20, .decide = &metal_capabilities.decideMetalEagerGraph },
        .{ .backend = .native, .priority = 0, .decide = &graph_partition.decideNative },
    };
    var diagnostics = graph_partition.CapabilityDiagnostics{};
    var plan = try graph_partition.partitionWithOptions(allocator, graph, &caps, .{
        .tensor_descs = seeds,
        .diagnostics = &diagnostics,
    });
    defer plan.deinit();

    var sdpa_count: usize = 0;
    var metal_sdpa_count: usize = 0;
    var transpose_count: usize = 0;
    var metal_transpose_count: usize = 0;
    var matmul_count: usize = 0;
    var metal_matmul_count: usize = 0;
    var equal_binary_count: usize = 0;
    var metal_equal_binary_count: usize = 0;
    for (0..graph.nodeCount()) |idx| {
        const node_id: NodeId = @intCast(idx);
        const node = graph.node(node_id);
        const planned_backend = plan.partitions[plan.node_assignment[node_id]].backend;
        switch (node.op) {
            .fused_sdpa => {
                sdpa_count += 1;
                if (planned_backend == .metal) {
                    metal_sdpa_count += 1;
                    try std.testing.expectEqual(operator_plan.Operator.attention_flash, plan.operatorPlanForNode(node_id).?.operator());
                }
            },
            .transpose => {
                if (node.output_shape.dtype != .f32) continue;
                transpose_count += 1;
                if (planned_backend == .metal) metal_transpose_count += 1;
            },
            .dot_general, .fused_linear, .fused_linear_no_bias => {
                if (node.output_shape.dtype != .f32) continue;
                matmul_count += 1;
                if (planned_backend == .metal) metal_matmul_count += 1;
            },
            .add, .mul, .sub, .div, .less_than, .fused_elem_add, .fused_elem_multiply => {
                if (!clipclapNodeHasEqualSizeF32BinaryInputs(graph, node_id)) continue;
                equal_binary_count += 1;
                if (planned_backend == .metal) metal_equal_binary_count += 1;
            },
            else => {},
        }
    }

    if (expectations.require_sdpa) try std.testing.expect(sdpa_count > 0);
    if (expectations.require_transpose) try std.testing.expect(transpose_count > 0);
    if (expectations.require_matmul) try std.testing.expect(matmul_count > 0);
    if (sdpa_count != metal_sdpa_count or
        transpose_count != metal_transpose_count or
        matmul_count != metal_matmul_count or
        equal_binary_count != metal_equal_binary_count)
    {
        dumpClipclapPlannerMismatches(file_name, graph, &plan, seeds);
    }
    try std.testing.expectEqual(sdpa_count, metal_sdpa_count);
    try std.testing.expectEqual(transpose_count, metal_transpose_count);
    try std.testing.expectEqual(matmul_count, metal_matmul_count);
    try std.testing.expectEqual(equal_binary_count, metal_equal_binary_count);
    try std.testing.expectEqual(@as(usize, 0), diagnostics.count(.missing_quant_kernel));
}

fn clipclapNodeHasEqualSizeF32BinaryInputs(graph: *const Graph, node_id: NodeId) bool {
    const node = graph.node(node_id);
    if (node.output_shape.dtype != .f32) return false;
    const inputs = node.getInputs();
    if (inputs.len < 2) return false;
    if (inputs[0] == ml.graph.null_node or inputs[1] == ml.graph.null_node) return false;
    const lhs_shape = graph.node(inputs[0]).output_shape;
    const rhs_shape = graph.node(inputs[1]).output_shape;
    if (lhs_shape.dtype != .f32 or rhs_shape.dtype != .f32) return false;
    const lhs_elems = lhs_shape.numElements() orelse return false;
    const rhs_elems = rhs_shape.numElements() orelse return false;
    const out_elems = node.output_shape.numElements() orelse return false;
    return lhs_elems > 0 and lhs_elems == rhs_elems and lhs_elems == out_elems;
}

fn clipclapNodeIsLastDimF32Reduce(graph: *const Graph, node_id: NodeId) bool {
    const node = graph.node(node_id);
    if (node.output_shape.dtype != .f32) return false;
    const attrs = switch (node.op) {
        .reduce_sum, .reduce_mean, .reduce_max => |attrs| attrs,
        else => return false,
    };
    if (attrs.num_axes != 1) return false;
    const inputs = node.getInputs();
    if (inputs.len == 0 or inputs[0] == ml.graph.null_node) return false;
    const input_shape = graph.node(inputs[0]).output_shape;
    if (input_shape.dtype != .f32 or input_shape.rank() == 0) return false;
    return attrs.axes[0] == input_shape.rank() - 1;
}

fn clipclapNodeIsNormalizeBroadcastTail(graph: *const Graph, node_id: NodeId) bool {
    const node = graph.node(node_id);
    const attrs = switch (node.op) {
        .broadcast_in_dim => |attrs| attrs,
        else => return false,
    };
    if (node.output_shape.dtype != .f32) return false;
    const inputs = node.getInputs();
    if (inputs.len == 0 or inputs[0] == ml.graph.null_node) return false;
    const input_shape = graph.node(inputs[0]).output_shape;
    const output_shape = node.output_shape;
    const rank = output_shape.rank();
    if (rank == 0 or input_shape.rank() != rank or attrs.num_axes != rank) return false;
    for (0..rank) |axis| {
        if (attrs.broadcast_axes[axis] != axis) return false;
        const in_dim = input_shape.dim(@intCast(axis));
        const out_dim = output_shape.dim(@intCast(axis));
        if (in_dim <= 0 or out_dim <= 0) return false;
        if (axis == rank - 1) {
            if (in_dim != 1 or out_dim <= 1) return false;
        } else if (in_dim != out_dim) return false;
    }
    return true;
}

fn clipclapNodeConsumesNormalizeBroadcastTail(graph: *const Graph, node_id: NodeId) bool {
    const node = graph.node(node_id);
    if (node.output_shape.dtype != .f32) return false;
    switch (node.op) {
        .mul, .div => {},
        else => return false,
    }
    for (node.getInputs()) |input_id| {
        if (input_id == ml.graph.null_node) continue;
        if (clipclapNodeIsNormalizeBroadcastTail(graph, input_id)) return true;
    }
    return false;
}

fn dumpClipclapPlannerMismatches(
    file_name: []const u8,
    graph: *const Graph,
    plan: *const graph_partition.PartitionPlan,
    seeds: []const ?graph_contracts.TensorDesc,
) void {
    std.debug.print("clipclap metal planner mismatches for {s}\n", .{file_name});
    var printed: usize = 0;
    for (0..graph.nodeCount()) |idx| {
        const node_id: NodeId = @intCast(idx);
        const node = graph.node(node_id);
        const planned_backend = plan.partitions[plan.node_assignment[node_id]].backend;
        const interesting = switch (node.op) {
            .fused_sdpa, .transpose, .dot_general, .fused_linear, .fused_linear_no_bias => node.output_shape.dtype == .f32,
            .reduce_sum, .reduce_mean, .reduce_max => clipclapNodeIsLastDimF32Reduce(graph, node_id),
            .sqrt, .rsqrt => node.output_shape.dtype == .f32,
            .broadcast_in_dim => clipclapNodeIsNormalizeBroadcastTail(graph, node_id),
            .mul, .div => clipclapNodeConsumesNormalizeBroadcastTail(graph, node_id) or clipclapNodeHasEqualSizeF32BinaryInputs(graph, node_id),
            .add, .sub, .less_than, .fused_elem_add, .fused_elem_multiply => clipclapNodeHasEqualSizeF32BinaryInputs(graph, node_id),
            else => false,
        };
        if (!interesting or planned_backend == .metal) continue;
        const decision = metal_capabilities.decideMetalEagerGraph(.{
            .graph = graph,
            .node_id = node_id,
            .op = node.op,
            .tensor_descs = seeds,
        });
        std.debug.print("  node={d} op={s} backend={s} reason={s} shape={any}\n", .{
            node_id,
            @tagName(std.meta.activeTag(node.op)),
            @tagName(planned_backend),
            @tagName(decision.reason),
            node.output_shape,
        });
        printed += 1;
        if (printed >= 16) break;
    }
}

fn dumpNormalizePlannerMismatches(
    graph: *const Graph,
    plan: *const graph_partition.PartitionPlan,
    seeds: []const ?graph_contracts.TensorDesc,
) void {
    std.debug.print("normalize tail metal planner mismatches\n", .{});
    var printed: usize = 0;
    for (0..graph.nodeCount()) |idx| {
        const node_id: NodeId = @intCast(idx);
        const node = graph.node(node_id);
        const planned_backend = plan.partitions[plan.node_assignment[node_id]].backend;
        const interesting = switch (node.op) {
            .reduce_sum, .reduce_mean, .reduce_max => clipclapNodeIsLastDimF32Reduce(graph, node_id),
            .sqrt, .rsqrt => node.output_shape.dtype == .f32,
            .broadcast_in_dim => clipclapNodeIsNormalizeBroadcastTail(graph, node_id),
            .mul, .div => clipclapNodeConsumesNormalizeBroadcastTail(graph, node_id),
            else => false,
        };
        if (!interesting or planned_backend == .metal) continue;
        const decision = metal_capabilities.decideMetalEagerGraph(.{
            .graph = graph,
            .node_id = node_id,
            .op = node.op,
            .tensor_descs = seeds,
        });
        std.debug.print("  node={d} op={s} backend={s} reason={s} shape={any}\n", .{
            node_id,
            @tagName(std.meta.activeTag(node.op)),
            @tagName(planned_backend),
            @tagName(decision.reason),
            node.output_shape,
        });
        printed += 1;
        if (printed >= 16) break;
    }
}

fn seedF32GraphResidencyForMetal(seeds: []?graph_contracts.TensorDesc, graph: *const Graph) !void {
    for (0..graph.nodeCount()) |idx| {
        const node_id: NodeId = @intCast(idx);
        const shape = graph.node(node_id).output_shape;
        if (shape.dtype != .f32) continue;
        var desc = graph_contracts.TensorDesc.init(shape, .metal_buffer);
        desc.resident_backend = .metal;
        try graph_partition.seedTensorDescriptor(seeds, graph, node_id, desc);
    }
}

fn specializeGraphDynamicDimsForPlanner(graph: *Graph, fallback_dim: i64) void {
    std.debug.assert(fallback_dim > 0);
    for (0..graph.nodeCount()) |idx| {
        const node_id: NodeId = @intCast(idx);
        const node = graph.nodeMut(node_id);
        specializeShapeDynamicDimsForPlanner(&node.output_shape, fallback_dim);
        switch (node.op) {
            .reshape => |*attrs| specializeShapeDynamicDimsForPlanner(&attrs.new_shape, fallback_dim),
            .broadcast_in_dim => |*attrs| specializeShapeDynamicDimsForPlanner(&attrs.target_shape, fallback_dim),
            .fused_sdpa => |*attrs| specializeSdpaAttrsForPlanner(graph, node_id, attrs),
            else => {},
        }
    }
    reconcileDotGeneralPlannerShapes(graph);
}

fn specializeShapeDynamicDimsForPlanner(shape: *ml.graph.Shape, fallback_dim: i64) void {
    for (0..shape.rank()) |axis| {
        if (shape.dims[axis] >= 0) continue;
        shape.dims[axis] = if (shape.bounds[axis] > 0) @min(shape.bounds[axis], fallback_dim) else fallback_dim;
        shape.bounds[axis] = 0;
    }
}

fn specializeSdpaAttrsForPlanner(graph: *const Graph, node_id: NodeId, attrs: *ml.graph.node.AttentionAttrs) void {
    const node = graph.node(node_id);
    if (node.num_inputs < 3) return;
    const q_shape = graph.node(node.inputs[0]).output_shape;
    const k_shape = graph.node(node.inputs[1]).output_shape;

    if (q_shape.rank() == 4) {
        const batch = positivePlannerDim(q_shape, 0) orelse return;
        const num_heads = positivePlannerDim(q_shape, 1) orelse return;
        const seq_len = positivePlannerDim(q_shape, 2) orelse return;
        const head_dim = positivePlannerDim(q_shape, 3) orelse return;
        attrs.batch = @intCast(batch);
        attrs.num_heads = @intCast(num_heads);
        attrs.seq_len = @intCast(seq_len);
        attrs.head_dim = @intCast(head_dim);
        if (k_shape.rank() == 4) {
            if (positivePlannerDim(k_shape, 1)) |num_kv_heads| attrs.num_kv_heads = @intCast(num_kv_heads);
            if (positivePlannerDim(k_shape, 2)) |kv_seq_len| attrs.kv_seq_len = @intCast(kv_seq_len);
        }
        return;
    }

    if (q_shape.rank() == 3) {
        const bh = positivePlannerDim(q_shape, 0) orelse return;
        const seq_len = positivePlannerDim(q_shape, 1) orelse return;
        const head_dim = positivePlannerDim(q_shape, 2) orelse return;
        const num_heads = if (attrs.num_heads > 0) attrs.num_heads else 1;
        attrs.num_heads = num_heads;
        attrs.batch = @intCast(@max(@as(usize, 1), bh / num_heads));
        attrs.seq_len = @intCast(seq_len);
        attrs.head_dim = @intCast(head_dim);
        if (k_shape.rank() == 3) {
            if (positivePlannerDim(k_shape, 1)) |kv_seq_len| attrs.kv_seq_len = @intCast(kv_seq_len);
        }
    }
}

fn positivePlannerDim(shape: ml.graph.Shape, axis: u8) ?usize {
    if (axis >= shape.rank()) return null;
    const dim = shape.dim(axis);
    if (dim <= 0) return null;
    return @intCast(dim);
}

fn reconcileDotGeneralPlannerShapes(graph: *Graph) void {
    for (0..graph.nodeCount()) |idx| {
        const node_id: NodeId = @intCast(idx);
        const node = graph.node(node_id);
        const attrs = switch (node.op) {
            .dot_general => |attrs| attrs,
            else => continue,
        };
        if (attrs.num_batch != 0 or attrs.num_contracting != 1 or node.num_inputs < 2) continue;

        const lhs_id = node.inputs[0];
        const rhs_id = node.inputs[1];
        if (lhs_id == ml.graph.null_node or rhs_id == ml.graph.null_node) continue;
        const lhs_shape = graph.node(lhs_id).output_shape;
        const rhs_shape = graph.node(rhs_id).output_shape;
        if (lhs_shape.rank() != 2 or rhs_shape.rank() != 2) continue;

        const lhs_axis = attrs.lhs_contracting[0];
        const rhs_axis = attrs.rhs_contracting[0];
        if (lhs_axis >= lhs_shape.rank() or rhs_axis >= rhs_shape.rank()) continue;
        const rhs_dim = rhs_shape.dim(rhs_axis);
        if (rhs_dim <= 0 or lhs_shape.dim(lhs_axis) == rhs_dim) continue;
        setPlannerShapeDim(graph, lhs_id, lhs_axis, rhs_dim);
    }
}

fn setPlannerShapeDim(graph: *Graph, node_id: NodeId, axis: u8, dim: i64) void {
    const node = graph.nodeMut(node_id);
    if (axis >= node.output_shape.rank()) return;
    node.output_shape.dims[axis] = dim;
    switch (node.op) {
        .reshape => |*attrs| {
            if (axis < attrs.new_shape.rank()) attrs.new_shape.dims[axis] = dim;
        },
        .broadcast_in_dim => |*attrs| {
            if (axis < attrs.target_shape.rank()) attrs.target_shape.dims[axis] = dim;
        },
        else => {},
    }
}

test "findSdpaMask returns borrowed attention_mask input" {
    const allocator = std.testing.allocator;
    const mask = [_]i64{ 1, 1, 0, 0 };
    var attention_mask = try Tensor.initInt64(allocator, "attention_mask", &.{ 1, 4 }, &mask);
    defer attention_mask.deinit();
    const token_ids = [_]i64{ 10, 11, 0, 0 };
    var input_ids = try Tensor.initInt64(allocator, "input_ids", &.{ 1, 4 }, &token_ids);
    defer input_ids.deinit();

    const found = findSdpaMask(&.{ input_ids, attention_mask }).?;
    try std.testing.expectEqualSlices(i64, &mask, found);
}
