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
const ops = @import("../ops.zig");
const tensor_mod = @import("../../backends/tensor.zig");
const buffer_mod = @import("buffer.zig");
const context_mod = @import("context.zig");
const kernels_mod = @import("kernels.zig");
const scratch_mod = @import("scratch.zig");
const weight_source_mod = @import("../../models/weight_source.zig");
const tensor_store_mod = @import("../../models/tensor_store.zig");
const gguf_tensor_types = @import("../../gguf/tensor_types.zig");
const quant_codec = @import("../../gguf/quant_codec.zig");
const native_compute = @import("../native_compute.zig");
const runtime_root = @import("../../runtime/root.zig");
const platform = @import("antfly_platform");

const CT = ops.CT;

pub const CudaTensor = struct {
    buffer: buffer_mod.DeviceBuffer,
    dtype: tensor_mod.DType,
    shape: []i64,
    elem_count: usize,
    quant_type: ?gguf_tensor_types.TensorType = null,
    owns_buffer: bool = true,
    owns_shape: bool = true,
    owned_by_tensor: bool = true,
};

const DeviceKvCacheKey = struct {
    sequence_id: u32,
    layer_index: usize,
};

const DeviceKvCacheEntry = struct {
    k: buffer_mod.DeviceBuffer = .{},
    v: buffer_mod.DeviceBuffer = .{},
    capacity_tokens: usize = 0,
    valid_tokens: usize = 0,
    kv_hidden: usize = 0,
};

const CudaKvDeviceWriteHook = struct {
    compute: *CudaCompute,

    fn deviceWriteHook(self: *CudaKvDeviceWriteHook) runtime_root.kv.storage_runtime.DeviceWriteHook {
        return .{
            .ctx = @ptrCast(self),
            .vtable = &cuda_kv_hook_vtable,
        };
    }

    fn writeLayerKvSuffix(
        _: *anyopaque,
        _: runtime_root.kv.storage_runtime.KvSuffixWrite,
        _: runtime_root.kv.storage_runtime.DeviceKvRef,
        _: runtime_root.kv.storage_runtime.DeviceKvRef,
    ) anyerror!void {
        return error.DeviceWriteUnsupported;
    }

    fn releaseSequence(ctx: *anyopaque, sequence_id: runtime_root.kv.storage_runtime.SequenceId) void {
        const self: *CudaKvDeviceWriteHook = @ptrCast(@alignCast(ctx));
        self.compute.releaseDeviceKvSequence(sequence_id);
    }

    fn hookDeinit(ctx: *anyopaque, allocator: std.mem.Allocator) void {
        const self: *CudaKvDeviceWriteHook = @ptrCast(@alignCast(ctx));
        allocator.destroy(self);
    }
};

const cuda_kv_hook_vtable: runtime_root.kv.storage_runtime.DeviceWriteHook.VTable = .{
    .writeLayerKvSuffix = CudaKvDeviceWriteHook.writeLayerKvSuffix,
    .reserveLayerKvDevice = null,
    .releaseSequence = CudaKvDeviceWriteHook.releaseSequence,
    .deinit = CudaKvDeviceWriteHook.hookDeinit,
};

const GqaGraphCacheKey = struct {
    batch: usize,
    q_seq_len: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    has_attn_or_mask: bool,
    bias_mode: u32,
};

pub const CudaCompute = struct {
    allocator: std.mem.Allocator,
    ctx: context_mod.CudaContext,
    kernels: kernels_mod.KernelModule,
    resident_weights: std.StringHashMapUnmanaged(CudaTensor) = .{},
    lazy_weights: std.StringHashMapUnmanaged(native_compute.LazyWeightEntry) = .{},
    device_kv_cache: std.AutoHashMapUnmanaged(DeviceKvCacheKey, DeviceKvCacheEntry) = .{},
    gqa_graph_cache: std.AutoHashMapUnmanaged(GqaGraphCacheKey, kernels_mod.KernelModule.GqaAttentionGraph) = .{},
    tensor_store: ?tensor_store_mod.TensorStore = null,
    temp_buffers: std.ArrayListUnmanaged(buffer_mod.DeviceBuffer) = .empty,
    temp_ids_masks: scratch_mod.DeviceScratch = .{},
    owned_by_backend: bool = false,
    allow_host_training_fallbacks: bool = true,
    allow_direct_quant: bool = true,
    pending_mmap_weight_uploads: bool = false,
    gqa_graph_disabled_for_session: bool = false,

    pub fn init(allocator: std.mem.Allocator) !CudaCompute {
        var ctx = context_mod.CudaContext.initDefault() catch |err| {
            std.debug.print("cuda compute init failed during context setup: {s}\n", .{@errorName(err)});
            return err;
        };
        errdefer ctx.deinit();
        const kernels = kernels_mod.KernelModule.load(&ctx) catch |err| {
            std.debug.print("cuda compute init failed during kernel module load: {s}\n", .{@errorName(err)});
            return err;
        };
        return .{
            .allocator = allocator,
            .ctx = ctx,
            .kernels = kernels,
            .allow_host_training_fallbacks = cudaHostTrainingFallbacksAllowed(),
        };
    }

    pub fn create(allocator: std.mem.Allocator) !*CudaCompute {
        const self = try allocator.create(CudaCompute);
        errdefer allocator.destroy(self);
        self.* = try CudaCompute.init(allocator);
        self.owned_by_backend = true;
        return self;
    }

    pub fn deinit(self: *CudaCompute) void {
        if (self.pending_mmap_weight_uploads) {
            self.ctx.synchronize() catch {};
            self.pending_mmap_weight_uploads = false;
        }
        var it = self.resident_weights.iterator();
        while (it.next()) |entry| {
            var tensor = entry.value_ptr.*;
            tensor.owns_buffer = true;
            tensor.owns_shape = true;
            freeCudaTensorStorage(self, &tensor);
            self.allocator.free(entry.key_ptr.*);
        }
        self.resident_weights.deinit(self.allocator);
        var lazy_it = self.lazy_weights.iterator();
        while (lazy_it.next()) |entry| {
            if (entry.value_ptr.loaded) |*loaded| loaded.deinit();
            entry.value_ptr.tensor_ref.deinit(self.allocator);
            self.allocator.free(entry.key_ptr.*);
        }
        self.lazy_weights.deinit(self.allocator);
        var kv_it = self.device_kv_cache.iterator();
        while (kv_it.next()) |entry| {
            entry.value_ptr.k.free(&self.ctx);
            entry.value_ptr.v.free(&self.ctx);
        }
        self.device_kv_cache.deinit(self.allocator);
        var graph_it = self.gqa_graph_cache.iterator();
        while (graph_it.next()) |entry| {
            entry.value_ptr.deinit(&self.ctx);
        }
        self.gqa_graph_cache.deinit(self.allocator);
        if (self.tensor_store) |store| store.deinit();
        for (self.temp_buffers.items) |*buffer| buffer.free(&self.ctx);
        self.temp_buffers.deinit(self.allocator);
        self.temp_ids_masks.deinit(&self.ctx);
        self.kernels.unload(&self.ctx);
        self.ctx.deinit();
    }

    fn releaseDeviceKvSequence(self: *CudaCompute, sequence_id: runtime_root.kv.storage_runtime.SequenceId) void {
        while (true) {
            var found_key: ?DeviceKvCacheKey = null;
            var it = self.device_kv_cache.iterator();
            while (it.next()) |entry| {
                if (entry.key_ptr.sequence_id != sequence_id) continue;
                found_key = entry.key_ptr.*;
                break;
            }
            const key = found_key orelse break;
            if (self.device_kv_cache.fetchRemove(key)) |removed| {
                var value = removed.value;
                value.k.free(&self.ctx);
                value.v.free(&self.ctx);
            }
        }
    }

    pub fn computeBackend(self: *CudaCompute) ops.ComputeBackend {
        return .{ .ptr = self, .vtable = &vtable };
    }

    pub fn adoptLazyWeights(
        self: *CudaCompute,
        lazy_weights: std.StringHashMapUnmanaged(native_compute.LazyWeightEntry),
        tensor_store: ?tensor_store_mod.TensorStore,
        allow_direct_quant: bool,
    ) void {
        self.lazy_weights = lazy_weights;
        self.tensor_store = tensor_store;
        self.allow_direct_quant = allow_direct_quant;
    }

    pub fn finishPendingWeightUploads(self: *CudaCompute) !void {
        if (!self.pending_mmap_weight_uploads) return;
        try self.ctx.synchronize();
        self.pending_mmap_weight_uploads = false;
    }

    pub fn insertWeightFromLoaded(self: *CudaCompute, owned_key: []const u8, loaded: *const weight_source_mod.LoadedWeight) !void {
        if (loaded.quantized_storage) |storage| {
            errdefer self.allocator.free(owned_key);
            if (cudaDequantizeQuantWeightsOnUpload()) {
                const elem_count = try elementCountFromShape(storage.shape);
                const data = try self.allocator.alloc(f32, elem_count);
                defer self.allocator.free(data);
                try quant_codec.dequantizeToFloat32(storage.tensor_type, storage.raw_bytes, data);

                const shape = try self.allocator.dupe(i64, storage.shape);
                errdefer self.allocator.free(shape);
                var device = try allocDeviceBuffer(self, data.len * @sizeOf(f32));
                errdefer device.free(&self.ctx);
                try device.copyFromHost(&self.ctx, std.mem.sliceAsBytes(data));
                try self.ctx.synchronize();
                try self.resident_weights.put(self.allocator, owned_key, .{
                    .buffer = device,
                    .dtype = .f32,
                    .shape = shape,
                    .elem_count = data.len,
                    .quant_type = null,
                    .owns_buffer = false,
                    .owns_shape = false,
                    .owned_by_tensor = false,
                });
                return;
            }
            const elem_count = try elementCountFromShape(storage.shape);
            const shape = try self.allocator.dupe(i64, storage.shape);
            errdefer self.allocator.free(shape);
            var device = try allocDeviceBuffer(self, storage.raw_bytes.len);
            errdefer device.free(&self.ctx);
            try device.copyFromHost(&self.ctx, storage.raw_bytes);
            if (storage.raw_mmap_backed) {
                self.pending_mmap_weight_uploads = true;
            } else {
                try self.ctx.synchronize();
            }
            try self.resident_weights.put(self.allocator, owned_key, .{
                .buffer = device,
                .dtype = .u8,
                .shape = shape,
                .elem_count = elem_count,
                .quant_type = storage.tensor_type,
                .owns_buffer = false,
                .owns_shape = false,
                .owned_by_tensor = false,
            });
            return;
        }
        if (loaded.quantized or loaded.tensor.dtype != .f32) {
            if (!loaded.quantized and (loaded.tensor.dtype == .f16 or loaded.tensor.dtype == .bf16)) {
                var converted = try weight_source_mod.convertToF32(self.allocator, &loaded.tensor);
                defer converted.deinit();
                try self.insertWeightFromTensor(owned_key, &converted);
                return;
            }
            self.allocator.free(owned_key);
            return error.UnsupportedTensorType;
        }
        try self.insertWeightFromTensor(owned_key, &loaded.tensor);
    }

    pub fn insertWeightFromTensor(self: *CudaCompute, owned_key: []const u8, tensor: *const tensor_mod.Tensor) !void {
        errdefer self.allocator.free(owned_key);
        if (tensor.dtype != .f32) return error.UnsupportedTensorType;
        if (tensor.data.len % @sizeOf(f32) != 0) return error.InvalidShape;
        const elem_count = tensor.data.len / @sizeOf(f32);
        if (try elementCountFromShape(tensor.shape) != elem_count) return error.InvalidShape;
        const shape = try self.allocator.dupe(i64, tensor.shape);
        errdefer self.allocator.free(shape);
        var device = try allocDeviceBuffer(self, tensor.data.len);
        errdefer device.free(&self.ctx);
        try device.copyFromHost(&self.ctx, tensor.data);
        try self.ctx.synchronize();
        try self.resident_weights.put(self.allocator, owned_key, .{
            .buffer = device,
            .dtype = .f32,
            .shape = shape,
            .elem_count = elem_count,
            .quant_type = null,
            .owns_buffer = false,
            .owns_shape = false,
            .owned_by_tensor = false,
        });
    }
};

fn cudaDequantizeQuantWeightsOnUpload() bool {
    return platform.env.getenvBoolDefault("TERMITE_CUDA_DEQUANTIZE_QUANT_WEIGHTS", false);
}

fn cudaHostTrainingFallbacksAllowed() bool {
    return platform.env.getenvBoolDefault("TERMITE_CUDA_ALLOW_HOST_TRAINING_FALLBACKS", true);
}

fn tensorFromCt(tensor: CT) *CudaTensor {
    return @ptrCast(@alignCast(tensor));
}

fn unsupportedOp(name: []const u8) anyerror!CT {
    if (platform.env.getenvBoolDefault("TERMITE_CUDA_UNSUPPORTED_DEBUG", false)) {
        std.log.err("cuda unsupported op: {s}", .{name});
    }
    return error.CudaOpUnsupported;
}

fn backendKind(_: *anyopaque) ops.BackendKind {
    return .cuda;
}

fn deinitBackend(ctx: *anyopaque) void {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const owned = self.owned_by_backend;
    if (owned) {
        self.deinit();
        self.allocator.destroy(self);
    }
}

fn provisionKvDeviceWriteHook(
    ctx: *anyopaque,
    storage: *runtime_root.kv.storage_runtime.KvStorageRuntime,
) anyerror!void {
    if (!pagedKvDeviceCacheEnabled()) return;
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const hook = try self.allocator.create(CudaKvDeviceWriteHook);
    hook.* = .{ .compute = self };
    storage.setDeviceWriteHook(hook.deviceWriteHook());
}

fn freeCudaTensorStorage(self: *CudaCompute, cuda_tensor: *CudaTensor) void {
    if (cuda_tensor.owns_buffer) releaseDeviceBuffer(self, &cuda_tensor.buffer);
    if (cuda_tensor.owns_shape) self.allocator.free(cuda_tensor.shape);
}

const max_temp_buffers = 256;

fn allocDeviceBuffer(self: *CudaCompute, len: usize) !buffer_mod.DeviceBuffer {
    if (len == 0) return .{};
    var best_index: ?usize = null;
    var best_len: usize = std.math.maxInt(usize);
    for (self.temp_buffers.items, 0..) |buffer, i| {
        if (buffer.len >= len and buffer.len < best_len) {
            best_index = i;
            best_len = buffer.len;
        }
    }
    if (best_index) |i| {
        const buffer = self.temp_buffers.swapRemove(i);
        return buffer;
    }
    return buffer_mod.DeviceBuffer.alloc(&self.ctx, len);
}

fn releaseDeviceBuffer(self: *CudaCompute, buffer: *buffer_mod.DeviceBuffer) void {
    if (buffer.ptr == 0) return;
    if (self.temp_buffers.items.len < max_temp_buffers) {
        self.temp_buffers.append(self.allocator, buffer.*) catch {
            buffer.free(&self.ctx);
            return;
        };
        buffer.* = .{};
        return;
    }
    buffer.free(&self.ctx);
}

fn freeTensor(ctx: *anyopaque, tensor: CT) void {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const cuda_tensor = tensorFromCt(tensor);
    if (!cuda_tensor.owned_by_tensor) return;
    freeCudaTensorStorage(self, cuda_tensor);
    self.allocator.destroy(cuda_tensor);
}

fn getWeight(ctx: *anyopaque, name: []const u8) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (self.resident_weights.getPtr(name)) |weight| return weight;
    if (self.lazy_weights.getPtr(name)) |entry| {
        try loadLazyWeightToDevice(self, name, entry);
        return self.resident_weights.getPtr(name) orelse error.WeightNotFound;
    }
    return error.WeightNotFound;
}

fn prefetchWeightHint(_: *anyopaque, _: []const u8, _: u32) void {}
fn drainPrefetchBudget(_: *anyopaque, _: usize) void {}

fn loadLazyWeightToDevice(self: *CudaCompute, name: []const u8, entry: *native_compute.LazyWeightEntry) !void {
    const store = self.tensor_store orelse return error.WeightNotFound;
    const owned_key = try self.allocator.dupe(u8, name);
    errdefer self.allocator.free(owned_key);

    if (self.allow_direct_quant) {
        const storage_opt = try store.loadQuantizedStorageRef(&entry.tensor_ref);
        if (storage_opt) |storage| {
            var loaded = weight_source_mod.LoadedWeight{
                .tensor = .{
                    .data = &.{},
                    .dtype = .f32,
                    .shape = &.{},
                    .name = name,
                    .allocator = self.allocator,
                    .owns_data = false,
                    .owns_shape = false,
                },
                .quantized = true,
                .quantized_storage = storage,
            };
            defer loaded.deinit();
            try self.insertWeightFromLoaded(owned_key, &loaded);
            return;
        }
    }

    var loaded = try store.loadTensorRef(&entry.tensor_ref);
    defer loaded.deinit();
    if (!self.allow_direct_quant) {
        if (loaded.quantized_storage) |*storage| {
            storage.deinit();
            loaded.quantized_storage = null;
            loaded.quantized = false;
        }
    }
    try self.insertWeightFromLoaded(owned_key, &loaded);
}

fn fromFloat32Op(ctx: *anyopaque, data: []const f32) anyerror!CT {
    var shape = [_]i32{@intCast(data.len)};
    return fromFloat32ShapeOp(ctx, data, &shape);
}

fn fromFloat32ShapeOp(ctx: *anyopaque, data: []const f32, shape: []const i32) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    var elem_count: usize = 1;
    for (shape) |dim| {
        if (dim < 0) return error.InvalidShape;
        elem_count = try std.math.mul(usize, elem_count, @intCast(dim));
    }
    if (elem_count != data.len) return error.InvalidShape;

    const shape_i64 = try self.allocator.alloc(i64, shape.len);
    errdefer self.allocator.free(shape_i64);
    for (shape, 0..) |dim, i| shape_i64[i] = dim;

    var device = try allocDeviceBuffer(self, data.len * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try device.copyFromHost(&self.ctx, std.mem.sliceAsBytes(data));
    try self.ctx.synchronize();

    const tensor = try self.allocator.create(CudaTensor);
    tensor.* = .{
        .buffer = device,
        .dtype = .f32,
        .shape = shape_i64,
        .elem_count = elem_count,
    };
    return tensor;
}

fn toFloat32Op(ctx: *anyopaque, tensor: CT, allocator: std.mem.Allocator) anyerror![]f32 {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const cuda_tensor = tensorFromCt(tensor);
    if (cuda_tensor.dtype != .f32) return error.UnsupportedTensorType;
    const out = try allocator.alloc(f32, cuda_tensor.elem_count);
    errdefer allocator.free(out);
    try cuda_tensor.buffer.copyToHost(&self.ctx, std.mem.sliceAsBytes(out));
    try self.ctx.synchronize();
    return out;
}

fn tensorDTypeOp(_: *anyopaque, tensor: CT) anyerror!tensor_mod.DType {
    return tensorFromCt(tensor).dtype;
}

fn tensorShapeOp(_: *anyopaque, tensor: CT, allocator: std.mem.Allocator) anyerror![]i64 {
    return allocator.dupe(i64, tensorFromCt(tensor).shape);
}

fn evalTensorOp(ctx: *anyopaque, _: CT) anyerror!void {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    try self.ctx.synchronize();
}

fn createTensor(
    self: *CudaCompute,
    device: buffer_mod.DeviceBuffer,
    shape: []i64,
    elem_count: usize,
) !CT {
    const tensor = try self.allocator.create(CudaTensor);
    tensor.* = .{
        .buffer = device,
        .dtype = .f32,
        .shape = shape,
        .elem_count = elem_count,
    };
    return tensor;
}

fn uploadOwnedHost(self: *CudaCompute, data: []f32, shape_src: []const i64) !CT {
    errdefer self.allocator.free(data);
    const elem_count = data.len;
    const shape = try self.allocator.dupe(i64, shape_src);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try device.copyFromHost(&self.ctx, std.mem.sliceAsBytes(data));
    try self.ctx.synchronize();
    self.allocator.free(data);
    return createTensor(self, device, shape, elem_count);
}

fn downloadAlloc(self: *CudaCompute, tensor: *const CudaTensor) ![]f32 {
    try ensureF32(tensor);
    const out = try self.allocator.alloc(f32, tensor.elem_count);
    errdefer self.allocator.free(out);
    try tensor.buffer.copyToHost(&self.ctx, std.mem.sliceAsBytes(out));
    try self.ctx.synchronize();
    return out;
}

fn broadcastShape2(allocator: std.mem.Allocator, a: []const i64, b: []const i64) !?[]i64 {
    const rank = @max(a.len, b.len);
    if (rank > 8) return error.InvalidShape;
    const out = try allocator.alloc(i64, rank);
    errdefer allocator.free(out);
    for (0..rank) |i| {
        const a_dim: i64 = if (i + a.len >= rank) a[i + a.len - rank] else 1;
        const b_dim: i64 = if (i + b.len >= rank) b[i + b.len - rank] else 1;
        if (a_dim <= 0 or b_dim <= 0) return error.InvalidShape;
        if (a_dim != b_dim and a_dim != 1 and b_dim != 1) {
            allocator.free(out);
            return null;
        }
        out[i] = @max(a_dim, b_dim);
    }
    return out;
}

fn broadcastShape3(allocator: std.mem.Allocator, a: []const i64, b: []const i64, c: []const i64) !?[]i64 {
    const ab = try broadcastShape2(allocator, a, b) orelse return null;
    defer allocator.free(ab);
    return broadcastShape2(allocator, ab, c);
}

fn u32FromShape(allocator: std.mem.Allocator, shape: []const i64) ![]u32 {
    const out = try allocator.alloc(u32, shape.len);
    errdefer allocator.free(out);
    for (shape, 0..) |dim, i| {
        if (dim < 0 or dim > std.math.maxInt(u32)) return error.InvalidShape;
        out[i] = @intCast(dim);
    }
    return out;
}

fn u32FromAxes(allocator: std.mem.Allocator, axes: []const u8) ![]u32 {
    const out = try allocator.alloc(u32, axes.len);
    errdefer allocator.free(out);
    for (axes, 0..) |axis, i| out[i] = axis;
    return out;
}

fn broadcastAxesForSuffix(allocator: std.mem.Allocator, input_shape: []const i64, target_shape: []const i64) ![]u32 {
    if (input_shape.len > target_shape.len) return error.InvalidShape;
    const axes = try allocator.alloc(u32, input_shape.len);
    errdefer allocator.free(axes);
    const offset = target_shape.len - input_shape.len;
    for (input_shape, 0..) |dim, i| {
        const target_dim = target_shape[offset + i];
        if (dim != target_dim and dim != 1) return error.InvalidShape;
        axes[i] = @intCast(offset + i);
    }
    return axes;
}

fn resolveBroadcastInputShape(allocator: std.mem.Allocator, tensor_shape: []const i64, declared_shape: []const i64, broadcast_axes_len: usize, elem_count: usize) ![]i64 {
    const source_shape = if (declared_shape.len == broadcast_axes_len)
        declared_shape
    else if (tensor_shape.len == broadcast_axes_len)
        tensor_shape
    else if (declared_shape.len > 0)
        declared_shape
    else
        tensor_shape;
    if (source_shape.len > 8) return error.InvalidShape;

    const out_len = if (source_shape.len != broadcast_axes_len) broadcast_axes_len else source_shape.len;
    const out = try allocator.alloc(i64, out_len);
    errdefer allocator.free(out);
    if (source_shape.len < broadcast_axes_len) {
        if (elem_count != 1) return error.InvalidShape;
        @memset(out, 1);
    } else if (source_shape.len > broadcast_axes_len) {
        var out_idx: usize = 0;
        for (source_shape, 0..) |raw_dim, src_idx| {
            const remaining_source = source_shape.len - src_idx;
            const remaining_slots = broadcast_axes_len - out_idx;
            if (raw_dim == 1 and remaining_source > remaining_slots) continue;
            if (out_idx >= out.len) return error.InvalidShape;
            out[out_idx] = raw_dim;
            out_idx += 1;
        }
        if (out_idx != out.len) return error.InvalidShape;
    } else for (source_shape, 0..) |dim, i| {
        if (dim > 0) {
            out[i] = dim;
        } else if (i < tensor_shape.len and tensor_shape[i] > 0) {
            out[i] = tensor_shape[i];
        } else {
            out[i] = 1;
        }
    }
    for (out, 0..) |dim, i| {
        if (dim > 0) continue;
        if (i < tensor_shape.len and tensor_shape[i] > 0) {
            out[i] = tensor_shape[i];
        } else {
            out[i] = 1;
        }
    }
    if (try elementCountFromShape(out) != elem_count) return error.InvalidShape;
    return out;
}

fn resolveBroadcastTargetShape(allocator: std.mem.Allocator, target_shape: []const i64, broadcast_axes: []const u32, input_shape: []const i64) ![]i64 {
    if (target_shape.len > 8) return error.InvalidShape;
    const out = try allocator.alloc(i64, target_shape.len);
    errdefer allocator.free(out);
    for (target_shape, 0..) |raw_dim, d| {
        var dim = raw_dim;
        if (dim <= 0) {
            for (broadcast_axes, 0..) |axis, in_d| {
                if (axis == @as(u32, @intCast(d))) {
                    dim = input_shape[in_d];
                    break;
                }
            }
        }
        if (dim <= 0) dim = 1;
        out[d] = dim;
    }
    return out;
}

fn cudaBroadcastMappingSupported(input_shape: []const i64, target_shape: []const i64, broadcast_axes: []const u32) bool {
    if (input_shape.len > 8 or target_shape.len > 8 or broadcast_axes.len != input_shape.len) return false;
    var seen = [_]bool{false} ** 8;
    for (broadcast_axes, 0..) |axis, in_d| {
        if (axis >= target_shape.len) return false;
        const axis_usize: usize = @intCast(axis);
        if (seen[axis_usize]) return false;
        seen[axis_usize] = true;
        const input_dim = input_shape[in_d];
        const target_dim = target_shape[axis_usize];
        if (input_dim <= 0 or target_dim < 0) return false;
        if (input_dim != 1 and input_dim != target_dim) return false;
    }
    return true;
}

fn broadcastMappingIsIdentity(input_shape: []const i64, target_shape: []const i64, broadcast_axes: []const u32) bool {
    if (!sameShape(input_shape, target_shape) or input_shape.len != broadcast_axes.len) return false;
    for (broadcast_axes, 0..) |axis, i| {
        if (axis != @as(u32, @intCast(i))) return false;
    }
    return true;
}

fn broadcastToShapeDeviceMapped(ctx: *anyopaque, input: CT, target_shape_raw: []const i64, input_shape_raw: []const i64, broadcast_axes: []const u32) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);
    const input_shape = try resolveBroadcastInputShape(self.allocator, input_tensor.shape, input_shape_raw, broadcast_axes.len, input_tensor.elem_count);
    defer self.allocator.free(input_shape);
    const target_shape = try resolveBroadcastTargetShape(self.allocator, target_shape_raw, broadcast_axes, input_shape);
    defer self.allocator.free(target_shape);
    if (!cudaBroadcastMappingSupported(input_shape, target_shape, broadcast_axes)) return error.UnsupportedShape;
    const out_count = try elementCountFromShape(target_shape);
    const shape = try dupeShape(self.allocator, target_shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    if (broadcastMappingIsIdentity(input_shape, target_shape, broadcast_axes)) {
        try device.copyFromDevice(&self.ctx, input_tensor.buffer, input_tensor.elem_count * @sizeOf(f32));
        return createTensor(self, device, shape, out_count);
    }

    const target_u32 = try u32FromShape(self.allocator, target_shape);
    defer self.allocator.free(target_u32);
    const input_u32 = try u32FromShape(self.allocator, input_shape);
    defer self.allocator.free(input_u32);

    const target_device = try uploadTempU32(self, target_u32);
    const input_device = try uploadTempU32(self, input_u32);
    const axes_device = try uploadTempU32(self, broadcast_axes);
    try self.kernels.launchBroadcastInDimF32(
        &self.ctx,
        device,
        input_tensor.buffer,
        out_count,
        input_tensor.elem_count,
        target_shape.len,
        input_shape.len,
        target_device,
        input_device,
        axes_device,
    );
    return createTensor(self, device, shape, out_count);
}

fn broadcastToShapeDevice(ctx: *anyopaque, input: CT, target_shape: []const i64) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const axes_u32 = try broadcastAxesForSuffix(self.allocator, input_tensor.shape, target_shape);
    defer self.allocator.free(axes_u32);
    return broadcastToShapeDeviceMapped(ctx, input, target_shape, input_tensor.shape, axes_u32);
}

fn nativeArgMaxFallback(ctx: *anyopaque, input: CT, axis: u8, keepdims: bool, input_shape: []const i64) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    var fallback: NativeFallback = undefined;
    try fallback.init(self, "argmax");
    defer fallback.deinit();

    const ni = try nativeCtFromCuda(self, &fallback, input);
    defer fallback.cb.free(ni);
    const out = try fallback.cb.primArgMax(ni, axis, keepdims, input_shape);
    defer fallback.cb.free(out);
    return cudaCtFromNative(self, &fallback, out, input_shape);
}

fn nativeReshapeFallback(ctx: *anyopaque, input: CT, new_shape: []const i64) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    var fallback: NativeFallback = undefined;
    try fallback.init(self, "reshape");
    defer fallback.deinit();

    const ni = try nativeCtFromCuda(self, &fallback, input);
    defer fallback.cb.free(ni);
    const out = try fallback.cb.primReshape(ni, new_shape);
    defer fallback.cb.free(out);
    return cudaCtFromNative(self, &fallback, out, new_shape);
}

fn nativeTransposeFallback(ctx: *anyopaque, input: CT, perm: []const u8, input_shape: []const i64) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    var fallback: NativeFallback = undefined;
    try fallback.init(self, "transpose");
    defer fallback.deinit();

    const ni = try nativeCtFromCuda(self, &fallback, input);
    defer fallback.cb.free(ni);
    const out = try fallback.cb.primTranspose(ni, perm, input_shape);
    defer fallback.cb.free(out);
    return cudaCtFromNative(self, &fallback, out, input_shape);
}

fn nativeBroadcastInDimFallback(ctx: *anyopaque, input: CT, target_shape: []const i64, broadcast_axes: []const u8, input_shape: []const i64) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    var fallback: NativeFallback = undefined;
    try fallback.init(self, "broadcast_in_dim");
    defer fallback.deinit();

    const ni = try nativeCtFromCuda(self, &fallback, input);
    defer fallback.cb.free(ni);
    const out = try fallback.cb.primBroadcastInDim(ni, target_shape, broadcast_axes, input_shape);
    defer fallback.cb.free(out);
    return cudaCtFromNative(self, &fallback, out, target_shape);
}

fn nativeDotGeneralFallback(ctx: *anyopaque, lhs: CT, rhs: CT, lhs_shape: []const i64, rhs_shape: []const i64, lhs_contracting: []const u8, rhs_contracting: []const u8, lhs_batch: []const u8, rhs_batch: []const u8) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    var fallback: NativeFallback = undefined;
    try fallback.init(self, "dot_general");
    defer fallback.deinit();

    const nl = try nativeCtFromCuda(self, &fallback, lhs);
    defer fallback.cb.free(nl);
    const nr = try nativeCtFromCuda(self, &fallback, rhs);
    defer fallback.cb.free(nr);
    const out = try fallback.cb.primDotGeneral(nl, nr, lhs_shape, rhs_shape, lhs_contracting, rhs_contracting, lhs_batch, rhs_batch);
    defer fallback.cb.free(out);
    return cudaCtFromNative(self, &fallback, out, tensorFromCt(lhs).shape);
}

const Linear2DPlan = struct {
    rows: usize,
    in_dim: usize,
    out_dim: usize,
};

fn planDotGeneral2DLinear(lhs_shape: []const i64, rhs_shape: []const i64, lhs_contracting: []const u8, rhs_contracting: []const u8, lhs_batch: []const u8, rhs_batch: []const u8) !?Linear2DPlan {
    if (lhs_batch.len != 0 or rhs_batch.len != 0) return null;
    if (lhs_contracting.len != 1 or rhs_contracting.len != 1) return null;
    if (lhs_shape.len != 2 or rhs_shape.len != 2) return null;
    if (lhs_contracting[0] != 1 or rhs_contracting[0] != 1) return null;
    if (lhs_shape[0] < 0 or lhs_shape[1] < 0 or rhs_shape[0] < 0 or rhs_shape[1] < 0) return error.InvalidShape;
    if (lhs_shape[1] != rhs_shape[1]) return error.InvalidShape;
    return .{
        .rows = @intCast(lhs_shape[0]),
        .in_dim = @intCast(lhs_shape[1]),
        .out_dim = @intCast(rhs_shape[0]),
    };
}

fn dotGeneral2DDevice(ctx: *anyopaque, lhs: CT, rhs: CT, lhs_shape: []const i64, rhs_shape: []const i64, lhs_contracting: []const u8, rhs_contracting: []const u8, lhs_batch: []const u8, rhs_batch: []const u8) anyerror!?CT {
    const plan = try planDotGeneral2DLinear(lhs_shape, rhs_shape, lhs_contracting, rhs_contracting, lhs_batch, rhs_batch) orelse return null;
    return try linearNoBias(ctx, lhs, rhs, plan.rows, plan.in_dim, plan.out_dim);
}

fn nativeScatterAddFallback(ctx: *anyopaque, input: CT, indices: CT, input_shape: []const i64, indices_shape: []const i64, axis: u8) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    var fallback: NativeFallback = undefined;
    try fallback.init(self, "scatter_add");
    defer fallback.deinit();

    const ni = try nativeCtFromCuda(self, &fallback, input);
    defer fallback.cb.free(ni);
    const nidx = try nativeCtFromCuda(self, &fallback, indices);
    defer fallback.cb.free(nidx);
    const out = try fallback.cb.primScatterAdd(ni, nidx, input_shape, indices_shape, axis);
    defer fallback.cb.free(out);
    return cudaCtFromNative(self, &fallback, out, input_shape);
}

fn nativeGatherFallback(ctx: *anyopaque, input: CT, indices: CT, axis: u8, input_shape: []const i64) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    var fallback: NativeFallback = undefined;
    try fallback.init(self, "gather");
    defer fallback.deinit();

    const ni = try nativeCtFromCuda(self, &fallback, input);
    defer fallback.cb.free(ni);
    const nidx = try nativeCtFromCuda(self, &fallback, indices);
    defer fallback.cb.free(nidx);
    const out = try fallback.cb.primGather(ni, nidx, axis, input_shape);
    defer fallback.cb.free(out);
    return cudaCtFromNative(self, &fallback, out, input_shape);
}

fn nativeSliceFallback(ctx: *anyopaque, input: CT, starts: []const i64, limits: []const i64, strides: []const i64, input_shape: []const i64) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    var fallback: NativeFallback = undefined;
    try fallback.init(self, "slice");
    defer fallback.deinit();

    const ni = try nativeCtFromCuda(self, &fallback, input);
    defer fallback.cb.free(ni);
    const out = try fallback.cb.primSlice(ni, starts, limits, strides, input_shape);
    defer fallback.cb.free(out);
    return cudaCtFromNative(self, &fallback, out, input_shape);
}

fn nativeConcatPrimFallback(ctx: *anyopaque, a: CT, b: CT, axis: u8, a_shape: []const i64, b_shape: []const i64) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    var fallback: NativeFallback = undefined;
    try fallback.init(self, "concat_prim");
    defer fallback.deinit();

    const na = try nativeCtFromCuda(self, &fallback, a);
    defer fallback.cb.free(na);
    const nb = try nativeCtFromCuda(self, &fallback, b);
    defer fallback.cb.free(nb);
    const out = try fallback.cb.primConcatPrim(na, nb, axis, a_shape, b_shape);
    defer fallback.cb.free(out);
    return cudaCtFromNative(self, &fallback, out, a_shape);
}

fn nativeSoftmaxFallback(ctx: *anyopaque, input: CT, dim: u32, comptime log_softmax: bool) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    var fallback: NativeFallback = undefined;
    try fallback.init(self, if (log_softmax) "log_softmax" else "softmax");
    defer fallback.deinit();

    const ni = try nativeCtFromCuda(self, &fallback, input);
    defer fallback.cb.free(ni);
    const out = if (log_softmax) try fallback.cb.primLogSoftmax(ni, dim) else try fallback.cb.primSoftmax(ni, dim);
    defer fallback.cb.free(out);
    return cudaCtFromNative(self, &fallback, out, tensorFromCt(input).shape);
}

fn resolveSoftmaxLastDimCuda(tensor: *const CudaTensor, dim: u32) ?usize {
    if (dim != 0) return @intCast(dim);
    if (tensor.shape.len > 0) {
        const raw_last = tensor.shape[tensor.shape.len - 1];
        if (raw_last > 0) return @intCast(raw_last);
        if (raw_last == -1) {
            var known_product: usize = 1;
            var infer_count: usize = 0;
            for (tensor.shape) |shape_dim| {
                if (shape_dim == -1) {
                    infer_count += 1;
                    continue;
                }
                if (shape_dim <= 0) return null;
                known_product = std.math.mul(usize, known_product, @intCast(shape_dim)) catch return null;
            }
            if (infer_count == 1 and known_product > 0 and tensor.elem_count % known_product == 0) {
                return tensor.elem_count / known_product;
            }
        }
    }
    if (tensor.elem_count > 0) return tensor.elem_count;
    return null;
}

fn deviceCopyWithShape(ctx: *anyopaque, input: CT, new_shape: []const i64) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);
    const elem_count = try elementCountFromShape(new_shape);
    if (elem_count != input_tensor.elem_count) return error.InvalidShape;

    const shape = try dupeShape(self.allocator, new_shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, input_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try device.copyFromDevice(&self.ctx, input_tensor.buffer, input_tensor.elem_count * @sizeOf(f32));
    return createTensor(self, device, shape, input_tensor.elem_count);
}

fn reshapeDeviceCopy(ctx: *anyopaque, input: CT, new_shape: []const i64) anyerror!CT {
    return deviceCopyWithShape(ctx, input, new_shape);
}

fn isIdentityBroadcast(target_shape: []const i64, broadcast_axes: []const u8, input_shape: []const i64) bool {
    if (target_shape.len != input_shape.len or broadcast_axes.len != input_shape.len) return false;
    for (input_shape, 0..) |dim, idx| {
        if (dim < 0 or target_shape[idx] < 0) return false;
        if (broadcast_axes[idx] != idx or target_shape[idx] != dim) return false;
    }
    return true;
}

const max_cuda_broadcast_rank = 8;

const BroadcastInDimPlan = struct {
    target_rank: usize,
    input_rank: usize,
    out_count: usize,
    input_count: usize,
    target_shape: [max_cuda_broadcast_rank]u32,
    input_shape: [max_cuda_broadcast_rank]u32,
    axes: [max_cuda_broadcast_rank]u32,
};

fn broadcastDimToU32(dim: i64) !u32 {
    if (dim < 0 or dim > std.math.maxInt(u32)) return error.InvalidShape;
    return @intCast(dim);
}

fn planBroadcastInDim(
    target_shape: []const i64,
    broadcast_axes: []const u8,
    input_shape: []const i64,
    input_elem_count: usize,
) !?BroadcastInDimPlan {
    if (target_shape.len > max_cuda_broadcast_rank or input_shape.len > max_cuda_broadcast_rank) return null;
    if (broadcast_axes.len != input_shape.len) return error.InvalidShape;

    var plan = BroadcastInDimPlan{
        .target_rank = target_shape.len,
        .input_rank = input_shape.len,
        .out_count = 1,
        .input_count = 1,
        .target_shape = [_]u32{0} ** max_cuda_broadcast_rank,
        .input_shape = [_]u32{0} ** max_cuda_broadcast_rank,
        .axes = [_]u32{0} ** max_cuda_broadcast_rank,
    };

    for (target_shape, 0..) |dim, idx| {
        plan.target_shape[idx] = try broadcastDimToU32(dim);
        plan.out_count = try checkedMul(plan.out_count, @intCast(plan.target_shape[idx]));
    }
    if (plan.out_count > std.math.maxInt(u32)) return null;

    for (input_shape, 0..) |dim, idx| {
        plan.input_shape[idx] = try broadcastDimToU32(dim);
        plan.input_count = try checkedMul(plan.input_count, @intCast(plan.input_shape[idx]));
    }
    if (plan.input_count != input_elem_count) return error.InvalidShape;

    var seen = [_]bool{false} ** max_cuda_broadcast_rank;
    for (broadcast_axes, 0..) |axis_u8, idx| {
        const axis: usize = axis_u8;
        if (axis >= target_shape.len or seen[axis]) return error.InvalidShape;
        seen[axis] = true;
        plan.axes[idx] = axis_u8;
        const input_dim = plan.input_shape[idx];
        const target_dim = plan.target_shape[axis];
        if (input_dim != 1 and input_dim != target_dim) return error.InvalidShape;
    }

    return plan;
}

fn metadataBufferSlice(buffer: buffer_mod.DeviceBuffer, offset_u32: usize, count_u32: usize) buffer_mod.DeviceBuffer {
    if (count_u32 == 0) return .{};
    return .{
        .ptr = buffer.ptr + @as(u64, @intCast(offset_u32 * @sizeOf(u32))),
        .len = count_u32 * @sizeOf(u32),
    };
}

fn launchBroadcastInDimPlan(
    self: *CudaCompute,
    dst: buffer_mod.DeviceBuffer,
    input_tensor: *const CudaTensor,
    plan: *const BroadcastInDimPlan,
) !void {
    var metadata = [_]u32{0} ** (max_cuda_broadcast_rank * 3);
    var metadata_len: usize = 0;
    for (plan.target_shape[0..plan.target_rank]) |value| {
        metadata[metadata_len] = value;
        metadata_len += 1;
    }
    for (plan.input_shape[0..plan.input_rank]) |value| {
        metadata[metadata_len] = value;
        metadata_len += 1;
    }
    for (plan.axes[0..plan.input_rank]) |value| {
        metadata[metadata_len] = value;
        metadata_len += 1;
    }
    const metadata_device = try uploadTempU32(self, metadata[0..metadata_len]);
    const target_device = metadataBufferSlice(metadata_device, 0, plan.target_rank);
    const input_shape_device = metadataBufferSlice(metadata_device, plan.target_rank, plan.input_rank);
    const axes_device = metadataBufferSlice(metadata_device, plan.target_rank + plan.input_rank, plan.input_rank);
    try self.kernels.launchBroadcastInDimF32(
        &self.ctx,
        dst,
        input_tensor.buffer,
        plan.out_count,
        plan.input_count,
        plan.target_rank,
        plan.input_rank,
        target_device,
        input_shape_device,
        axes_device,
    );
}

fn broadcastInDimDeviceSupported(ctx: *anyopaque, input: CT, target_shape: []const i64, broadcast_axes: []const u8, input_shape: []const i64) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);
    const plan = try planBroadcastInDim(target_shape, broadcast_axes, input_shape, input_tensor.elem_count) orelse return null;

    const shape = try dupeShape(self.allocator, target_shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, plan.out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try launchBroadcastInDimPlan(self, device, input_tensor, &plan);
    return createTensor(self, device, shape, plan.out_count);
}

fn broadcastScalarToBuffer(
    self: *CudaCompute,
    dst: buffer_mod.DeviceBuffer,
    scalar_tensor: *const CudaTensor,
    target_shape: []const i64,
    out_count: usize,
) !bool {
    if (scalar_tensor.elem_count != 1) return false;
    const plan = try planBroadcastInDim(target_shape, &[_]u8{}, &[_]i64{}, scalar_tensor.elem_count) orelse return false;
    if (plan.out_count != out_count) return error.InvalidShape;
    try launchBroadcastInDimPlan(self, dst, scalar_tensor, &plan);
    return true;
}

const BroadcastShapePlan = struct {
    rank: usize,
    count: usize,
    shape: [max_cuda_broadcast_rank]i64,
};

fn planBroadcastShape3(
    a_shape: []const i64,
    b_shape: []const i64,
    c_shape: []const i64,
    a_count: usize,
    b_count: usize,
    c_count: usize,
) !?BroadcastShapePlan {
    const rank = @max(a_shape.len, @max(b_shape.len, c_shape.len));
    if (rank > max_cuda_broadcast_rank) return null;
    if ((try elementCountFromShape(a_shape)) != a_count) return error.InvalidShape;
    if ((try elementCountFromShape(b_shape)) != b_count) return error.InvalidShape;
    if ((try elementCountFromShape(c_shape)) != c_count) return error.InvalidShape;

    var plan = BroadcastShapePlan{
        .rank = rank,
        .count = 1,
        .shape = [_]i64{0} ** max_cuda_broadcast_rank,
    };
    for (0..rank) |axis| {
        const dims = [_]i64{
            broadcastDimFromRight(a_shape, rank, axis),
            broadcastDimFromRight(b_shape, rank, axis),
            broadcastDimFromRight(c_shape, rank, axis),
        };
        var out_dim: i64 = 1;
        for (dims) |dim| {
            if (dim < 0) return null;
            if (dim == 1) continue;
            if (out_dim == 1) {
                out_dim = dim;
            } else if (out_dim != dim) {
                return null;
            }
        }
        plan.shape[axis] = out_dim;
        plan.count = try checkedMul(plan.count, @intCast(out_dim));
    }
    if (plan.count > std.math.maxInt(u32)) return null;
    return plan;
}

fn broadcastDimFromRight(shape: []const i64, out_rank: usize, out_axis: usize) i64 {
    const leading = out_rank - shape.len;
    if (out_axis < leading) return 1;
    return shape[out_axis - leading];
}

fn suffixBroadcastAxes(input_rank: usize, out_rank: usize, axes: *[max_cuda_broadcast_rank]u8) []const u8 {
    const leading = out_rank - input_rank;
    for (0..input_rank) |idx| axes[idx] = @intCast(leading + idx);
    return axes[0..input_rank];
}

fn materializeBroadcastOperand(
    self: *CudaCompute,
    tensor: *const CudaTensor,
    out_shape: []const i64,
    out_count: usize,
    temp: *buffer_mod.DeviceBuffer,
) !?buffer_mod.DeviceBuffer {
    if (tensor.elem_count == out_count and sameShape(tensor.shape, out_shape)) return tensor.buffer;
    var axes_buf: [max_cuda_broadcast_rank]u8 = undefined;
    const axes = suffixBroadcastAxes(tensor.shape.len, out_shape.len, &axes_buf);
    const plan = try planBroadcastInDim(out_shape, axes, tensor.shape, tensor.elem_count) orelse return null;
    if (plan.out_count != out_count) return error.InvalidShape;
    temp.* = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    try launchBroadcastInDimPlan(self, temp.*, tensor, &plan);
    return temp.*;
}

const ConcatLastDimPlan = struct {
    rows: usize,
    dim_a: usize,
    dim_b: usize,
    out_dim: usize,
    out_count: usize,
};

fn planConcatLastDim(a_shape: []const i64, b_shape: []const i64, axis: u8) !?ConcatLastDimPlan {
    if (a_shape.len == 0 or a_shape.len != b_shape.len) return null;
    const axis_index: usize = @intCast(axis);
    if (axis_index + 1 != a_shape.len) return null;

    var rows: usize = 1;
    for (a_shape[0..axis_index], 0..) |dim, idx| {
        if (dim < 0 or b_shape[idx] < 0) return error.InvalidShape;
        if (dim != b_shape[idx]) return error.InvalidShape;
        rows = try checkedMul(rows, @intCast(dim));
    }
    if (a_shape[axis_index] < 0 or b_shape[axis_index] < 0) return error.InvalidShape;
    const dim_a: usize = @intCast(a_shape[axis_index]);
    const dim_b: usize = @intCast(b_shape[axis_index]);
    const out_dim = try checkedAdd(dim_a, dim_b);
    return .{
        .rows = rows,
        .dim_a = dim_a,
        .dim_b = dim_b,
        .out_dim = out_dim,
        .out_count = try checkedMul(rows, out_dim),
    };
}

fn concatLastDimDevice(ctx: *anyopaque, a: CT, b: CT, axis: u8, a_shape: []const i64, b_shape: []const i64) anyerror!?CT {
    const plan = try planConcatLastDim(a_shape, b_shape, axis) orelse return null;
    const axis_index: usize = @intCast(axis);

    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const a_tensor = tensorFromCt(a);
    const b_tensor = tensorFromCt(b);
    try ensureF32(a_tensor);
    try ensureF32(b_tensor);
    try ensureCount(a_tensor, try checkedMul(plan.rows, plan.dim_a));
    try ensureCount(b_tensor, try checkedMul(plan.rows, plan.dim_b));

    const shape = try dupeShape(self.allocator, a_shape);
    errdefer self.allocator.free(shape);
    shape[axis_index] = @intCast(plan.out_dim);
    var device = try allocDeviceBuffer(self, plan.out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchConcatLastDimF32(&self.ctx, device, a_tensor.buffer, b_tensor.buffer, plan.rows, plan.dim_a, plan.dim_b);
    return try createTensor(self, device, shape, plan.out_count);
}

fn toFloat32BatchOp(ctx: *anyopaque, cts: []const CT, allocator: std.mem.Allocator) anyerror![][]f32 {
    const out = try allocator.alloc([]f32, cts.len);
    errdefer allocator.free(out);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |buf| allocator.free(buf);
    }
    for (cts, 0..) |ct, idx| {
        out[idx] = try toFloat32Op(ctx, ct, allocator);
        initialized += 1;
    }
    return out;
}

fn allocShape2(allocator: std.mem.Allocator, rows: usize, cols: usize) ![]i64 {
    const shape = try allocator.alloc(i64, 2);
    shape[0] = @intCast(rows);
    shape[1] = @intCast(cols);
    return shape;
}

fn dupeShape(allocator: std.mem.Allocator, shape: []const i64) ![]i64 {
    return allocator.dupe(i64, shape);
}

fn ensureF32(tensor: *const CudaTensor) !void {
    if (tensor.dtype != .f32 or tensor.quant_type != null) {
        if (platform.env.getenvBoolDefault("TERMITE_CUDA_UNSUPPORTED_DEBUG", false)) {
            if (tensor.quant_type) |quant_type| {
                std.log.err("cuda expected f32 tensor but got quantized type={s} shape_rank={d} elems={d}", .{ quantTypeName(quant_type), tensor.shape.len, tensor.elem_count });
            } else {
                std.log.err("cuda expected f32 tensor but got dtype={s} shape_rank={d} elems={d}", .{ @tagName(tensor.dtype), tensor.shape.len, tensor.elem_count });
            }
        }
        return error.UnsupportedTensorType;
    }
}

fn ensureF32OrQuantized(tensor: *const CudaTensor) !void {
    if (tensor.quant_type != null) return;
    try ensureF32(tensor);
}

fn isKnownQuant(tensor: *const CudaTensor, known: gguf_tensor_types.KnownTensorType) bool {
    const quant_type = tensor.quant_type orelse return false;
    return switch (quant_type) {
        .known => |actual| actual == known,
        else => false,
    };
}

fn quantTypeName(quant_type: gguf_tensor_types.TensorType) []const u8 {
    return switch (quant_type) {
        .known => |known| @tagName(known),
        else => "unknown",
    };
}

fn ensureCount(tensor: *const CudaTensor, expected: usize) !void {
    if (tensor.elem_count != expected) return error.InvalidShape;
}

fn checkedMul(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b) catch error.InvalidShape;
}

fn checkedAdd(a: usize, b: usize) !usize {
    return std.math.add(usize, a, b) catch error.InvalidShape;
}

fn checkedSub(a: usize, b: usize) !usize {
    return std.math.sub(usize, a, b) catch error.InvalidShape;
}

fn elementCountFromShape(shape: []const i64) !usize {
    var count: usize = 1;
    for (shape) |dim| {
        if (dim < 0) return error.InvalidShape;
        count = try checkedMul(count, @intCast(dim));
    }
    return count;
}

fn sameShape(a: []const i64, b: []const i64) bool {
    return std.mem.eql(i64, a, b);
}

const NativeUnaryOp = enum { negate, sqrt, rsqrt, exp, log, sin, cos, tanh_prim, erf, abs };
const NativeBinaryOp = enum { add, multiply, subtract, divide, less_than };

const NativeFallback = struct {
    compute: *CudaCompute,
    ws: native_compute.WeightStore = undefined,
    engine: native_compute.NativeCompute = undefined,
    cb: ops.ComputeBackend = undefined,

    fn init(self: *NativeFallback, compute: *CudaCompute, op_name: []const u8) !void {
        if (!compute.allow_host_training_fallbacks) {
            if (op_name.len > 0) {
                std.debug.print("error: CUDA host training fallback disabled for {s}\n", .{op_name});
            }
            return error.CudaHostTrainingFallbackDisabled;
        }
        self.compute = compute;
        self.ws = .{
            .allocator = compute.allocator,
            .resident_weights = .{},
            .lazy_weights = .{},
        };
        self.engine = native_compute.NativeCompute.init(compute.allocator, &self.ws, null);
        self.cb = self.engine.computeBackend();
    }

    fn deinit(self: *NativeFallback) void {
        native_compute.deinitPrefetchQueue(&self.ws);
        self.ws.resident_weights.deinit(self.compute.allocator);
        self.ws.lazy_weights.deinit(self.compute.allocator);
    }
};

fn shapeI32FromI64(allocator: std.mem.Allocator, shape: []const i64) ![]i32 {
    const out = try allocator.alloc(i32, shape.len);
    errdefer allocator.free(out);
    for (shape, 0..) |dim, i| {
        if (dim < 0 or dim > std.math.maxInt(i32)) return error.InvalidShape;
        out[i] = @intCast(dim);
    }
    return out;
}

fn nativeCtFromCuda(self: *CudaCompute, fallback: *NativeFallback, input: CT) !CT {
    const tensor = tensorFromCt(input);
    const data = try downloadAlloc(self, tensor);
    defer self.allocator.free(data);
    const shape_i32 = try shapeI32FromI64(self.allocator, tensor.shape);
    defer self.allocator.free(shape_i32);
    return fallback.cb.fromFloat32Shape(data, shape_i32);
}

fn cudaCtFromNative(self: *CudaCompute, fallback: *NativeFallback, input: CT, fallback_shape: []const i64) !CT {
    const shape = fallback.cb.tensorShape(input, self.allocator) catch try self.allocator.dupe(i64, fallback_shape);
    defer self.allocator.free(shape);
    const data = try fallback.cb.toFloat32(input, self.allocator);
    return uploadOwnedHost(self, data, shape);
}

fn nativeUnaryFallback(ctx: *anyopaque, a: CT, op: NativeUnaryOp) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    var fallback: NativeFallback = undefined;
    try fallback.init(self, @tagName(op));
    defer fallback.deinit();

    const na = try nativeCtFromCuda(self, &fallback, a);
    defer fallback.cb.free(na);
    const out = switch (op) {
        .negate => try fallback.cb.primNegate(na),
        .sqrt => try fallback.cb.primSqrt(na),
        .rsqrt => try fallback.cb.primRsqrt(na),
        .exp => try fallback.cb.primExp(na),
        .log => try fallback.cb.primLog(na),
        .sin => try fallback.cb.primSin(na),
        .cos => try fallback.cb.primCos(na),
        .tanh_prim => try fallback.cb.primTanh(na),
        .erf => try fallback.cb.primErf(na),
        .abs => try fallback.cb.primAbs(na),
    };
    defer fallback.cb.free(out);
    return cudaCtFromNative(self, &fallback, out, tensorFromCt(a).shape);
}

fn nativeBinaryFallback(ctx: *anyopaque, a: CT, b: CT, op: NativeBinaryOp) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    var fallback: NativeFallback = undefined;
    try fallback.init(self, @tagName(op));
    defer fallback.deinit();

    const na = try nativeCtFromCuda(self, &fallback, a);
    defer fallback.cb.free(na);
    const nb = try nativeCtFromCuda(self, &fallback, b);
    defer fallback.cb.free(nb);
    const out = switch (op) {
        .add => try fallback.cb.add(na, nb),
        .multiply => try fallback.cb.multiply(na, nb),
        .subtract => try fallback.cb.primSubtract(na, nb),
        .divide => try fallback.cb.primDivide(na, nb),
        .less_than => try fallback.cb.primLessThan(na, nb),
    };
    defer fallback.cb.free(out);
    const out_shape = fallback.cb.tensorShape(out, self.allocator) catch tensorFromCt(a).shape;
    const out_shape_owned = out_shape.ptr != tensorFromCt(a).shape.ptr;
    defer if (out_shape_owned) self.allocator.free(out_shape);
    return cudaCtFromNative(self, &fallback, out, out_shape);
}

fn nativeWhereSelectFallback(ctx: *anyopaque, cond: CT, on_true: CT, on_false: CT) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    var fallback: NativeFallback = undefined;
    try fallback.init(self, "where_select");
    defer fallback.deinit();

    const nc = try nativeCtFromCuda(self, &fallback, cond);
    defer fallback.cb.free(nc);
    const nt = try nativeCtFromCuda(self, &fallback, on_true);
    defer fallback.cb.free(nt);
    const nf = try nativeCtFromCuda(self, &fallback, on_false);
    defer fallback.cb.free(nf);
    const out = try fallback.cb.primWhereSelect(nc, nt, nf);
    defer fallback.cb.free(out);
    const out_shape = fallback.cb.tensorShape(out, self.allocator) catch tensorFromCt(on_true).shape;
    const out_shape_owned = out_shape.ptr != tensorFromCt(on_true).shape.ptr;
    defer if (out_shape_owned) self.allocator.free(out_shape);
    return cudaCtFromNative(self, &fallback, out, out_shape);
}

fn nativeReduceFallback(ctx: *anyopaque, input: CT, axes: []const u8, input_shape: []const i64, comptime mode: enum { sum, max, mean }) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    var fallback: NativeFallback = undefined;
    try fallback.init(self, "reduce_" ++ @tagName(mode));
    defer fallback.deinit();

    const ni = try nativeCtFromCuda(self, &fallback, input);
    defer fallback.cb.free(ni);
    const out = switch (mode) {
        .sum => try fallback.cb.primReduceSum(ni, axes, input_shape),
        .max => try fallback.cb.primReduceMax(ni, axes, input_shape),
        .mean => try fallback.cb.primReduceMean(ni, axes, input_shape),
    };
    defer fallback.cb.free(out);
    return cudaCtFromNative(self, &fallback, out, input_shape);
}

fn uploadTempI64(self: *CudaCompute, data: []const i64) !buffer_mod.DeviceBuffer {
    const device = try self.temp_ids_masks.acquire(&self.ctx, data.len * @sizeOf(i64));
    try device.copyFromHost(&self.ctx, std.mem.sliceAsBytes(data));
    return device;
}

fn uploadTempU32(self: *CudaCompute, data: []const u32) !buffer_mod.DeviceBuffer {
    const device = try self.temp_ids_masks.acquire(&self.ctx, data.len * @sizeOf(u32));
    try device.copyFromHost(&self.ctx, std.mem.sliceAsBytes(data));
    return device;
}

fn uploadTempU8(self: *CudaCompute, data: []const u8) !buffer_mod.DeviceBuffer {
    const device = try self.temp_ids_masks.acquire(&self.ctx, data.len);
    try device.copyFromHost(&self.ctx, data);
    return device;
}

fn cudaGqaDebugEnabled() bool {
    return platform.env.getenvBool("TERMITE_CUDA_GQA_DEBUG");
}

fn embeddingLookup(ctx: *anyopaque, weight: CT, ids: []const i64, total: usize, dim: usize) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const weight_tensor = tensorFromCt(weight);
    try ensureF32OrQuantized(weight_tensor);
    if (ids.len != total) return error.InvalidShape;
    if (dim == 0 or weight_tensor.elem_count % dim != 0) return error.InvalidShape;
    const vocab = weight_tensor.elem_count / dim;
    for (ids) |raw_id| {
        if (raw_id < 0) return error.InvalidTokenId;
        const id: usize = @intCast(raw_id);
        if (id >= vocab) return error.InvalidTokenId;
    }
    const ids_device = try uploadTempI64(self, ids);
    const out_count = try checkedMul(total, dim);
    const shape = try allocShape2(self.allocator, total, dim);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    if (weight_tensor.quant_type) |quant_type| {
        switch (quant_type) {
            .known => |known| switch (known) {
                .Q4_K => try self.kernels.launchEmbeddingLookupQ4KF32(&self.ctx, device, weight_tensor.buffer, ids_device, total, dim),
                .Q5_K => try self.kernels.launchEmbeddingLookupQ5KF32(&self.ctx, device, weight_tensor.buffer, ids_device, total, dim),
                .Q6_K => try self.kernels.launchEmbeddingLookupQ6KF32(&self.ctx, device, weight_tensor.buffer, ids_device, total, dim),
                else => {
                    if (platform.env.getenvBoolDefault("TERMITE_CUDA_UNSUPPORTED_DEBUG", false)) {
                        std.log.err("cuda embedding unsupported quantized weight type={s}", .{@tagName(known)});
                    }
                    return error.UnsupportedTensorType;
                },
            },
            else => {
                if (platform.env.getenvBoolDefault("TERMITE_CUDA_UNSUPPORTED_DEBUG", false)) {
                    std.log.err("cuda embedding unsupported quantized weight type=unknown", .{});
                }
                return error.UnsupportedTensorType;
            },
        }
    } else {
        try self.kernels.launchEmbeddingLookupF32(&self.ctx, device, weight_tensor.buffer, ids_device, total, dim);
    }
    return createTensor(self, device, shape, out_count);
}

fn takeRows(ctx: *anyopaque, request: *const ops.TakeRowsRequest) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(request.input);
    try ensureF32(input_tensor);
    if (request.dim == 0 or request.rows != request.row_ids.len) return error.InvalidShape;
    if (input_tensor.elem_count % request.dim != 0) return error.InvalidShape;
    const source_rows = input_tensor.elem_count / request.dim;
    for (request.row_ids) |row_id| {
        if (row_id >= source_rows) return error.InvalidShape;
    }
    const row_ids_device = try uploadTempU32(self, request.row_ids);

    const out_count = try checkedMul(request.rows, request.dim);
    const shape = try allocShape2(self.allocator, request.rows, request.dim);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchTakeRowsF32(&self.ctx, device, input_tensor.buffer, row_ids_device, source_rows, request.rows, request.dim);
    return try createTensor(self, device, shape, out_count);
}

fn glinerWordEmbeddings(ctx: *anyopaque, request: *const ops.GlinerWordEmbeddingsRequest) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const hidden_tensor = tensorFromCt(request.hidden);
    try ensureF32(hidden_tensor);
    if (request.batch == 0 or request.seq_len == 0 or request.hidden_size == 0) return error.InvalidShape;
    const token_count = try checkedMul(request.batch, request.seq_len);
    if (request.words_mask.len < token_count) return error.InvalidShape;
    try ensureCount(hidden_tensor, try checkedMul(token_count, request.hidden_size));

    const out_rows = try checkedMul(request.batch, request.num_words);
    const out_count = try checkedMul(out_rows, request.hidden_size);
    const shape = try allocShape2(self.allocator, out_rows, request.hidden_size);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    if (out_count == 0) return try createTensor(self, device, shape, out_count);

    const mask_device = try uploadTempI64(self, request.words_mask[0..token_count]);
    try self.kernels.launchGlinerWordEmbeddingsF32(
        &self.ctx,
        device,
        hidden_tensor.buffer,
        mask_device,
        request.batch,
        request.seq_len,
        request.hidden_size,
        request.num_words,
    );
    return try createTensor(self, device, shape, out_count);
}

fn glinerLabelGruCombined(ctx: *anyopaque, request: *const ops.GlinerLabelGruCombinedRequest) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const label_tensor = tensorFromCt(request.label_embeddings);
    try ensureF32(label_tensor);
    if (request.num_labels == 0 or request.hidden_size == 0) return error.InvalidShape;
    try ensureCount(label_tensor, try checkedMul(request.num_labels, request.hidden_size));

    const pos_w = try getWeight(ctx, "count_embed.pos_embedding.weight");
    const pos_tensor = tensorFromCt(pos_w);
    if (pos_tensor.dtype != .f32 or pos_tensor.quant_type != null) return null;
    if (pos_tensor.elem_count < request.hidden_size) return error.InvalidShape;

    const label_count = try checkedMul(request.num_labels, request.hidden_size);
    const gate_dim = try checkedMul(request.hidden_size, 3);

    const pos_shape = try allocShape2(self.allocator, request.num_labels, request.hidden_size);
    var pos_shape_owned = false;
    errdefer if (!pos_shape_owned) self.allocator.free(pos_shape);
    var pos_device = try allocDeviceBuffer(self, label_count * @sizeOf(f32));
    var pos_device_owned = false;
    errdefer if (!pos_device_owned) pos_device.free(&self.ctx);
    try self.kernels.launchRepeatFirstRowF32(&self.ctx, pos_device, pos_tensor.buffer, request.num_labels, request.hidden_size);
    const pos_ct = try createTensor(self, pos_device, pos_shape, label_count);
    pos_shape_owned = true;
    pos_device_owned = true;
    defer freeTensor(ctx, pos_ct);

    const w_ih = try getWeight(ctx, "count_embed.gru.weight_ih_l0");
    const b_ih = try getWeight(ctx, "count_embed.gru.bias_ih_l0");
    const gi = try linear(ctx, pos_ct, w_ih, b_ih, request.num_labels, request.hidden_size, gate_dim);
    defer freeTensor(ctx, gi);

    const w_hh = try getWeight(ctx, "count_embed.gru.weight_hh_l0");
    const b_hh = try getWeight(ctx, "count_embed.gru.bias_hh_l0");
    const gh = try linear(ctx, request.label_embeddings, w_hh, b_hh, request.num_labels, request.hidden_size, gate_dim);
    defer freeTensor(ctx, gh);

    const gi_tensor = tensorFromCt(gi);
    const gh_tensor = tensorFromCt(gh);
    const shape = try allocShape2(self.allocator, request.num_labels, request.hidden_size);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, label_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchGlinerGruCombineF32(
        &self.ctx,
        device,
        label_tensor.buffer,
        gi_tensor.buffer,
        gh_tensor.buffer,
        request.num_labels,
        request.hidden_size,
    );
    return try createTensor(self, device, shape, label_count);
}

fn linear(ctx: *anyopaque, input: CT, weight: CT, bias: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_tensor = tensorFromCt(weight);
    const bias_tensor = tensorFromCt(bias);
    try ensureF32(input_tensor);
    try ensureF32OrQuantized(weight_tensor);
    try ensureF32(bias_tensor);
    try ensureCount(input_tensor, try checkedMul(rows, in_dim));
    try ensureCount(weight_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(bias_tensor, out_dim);

    const out_count = try checkedMul(rows, out_dim);
    const shape = try allocShape2(self.allocator, rows, out_dim);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    if (weight_tensor.quant_type) |quant_type| {
        switch (quant_type) {
            .known => |known| switch (known) {
                .Q4_K => if (rows >= 2)
                    try self.kernels.launchLinearQ4KBiasTile4Rows2F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim)
                else
                    try self.kernels.launchLinearQ4KBiasTile4F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim),
                .Q5_K => try self.kernels.launchLinearQ5KBiasF32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim),
                .Q6_K => try self.kernels.launchLinearQ6KBiasF32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim),
                else => {
                    if (platform.env.getenvBoolDefault("TERMITE_CUDA_UNSUPPORTED_DEBUG", false)) {
                        std.log.err("cuda linear_bias unsupported quantized weight type={s}", .{@tagName(known)});
                    }
                    return error.UnsupportedTensorType;
                },
            },
            else => {
                if (platform.env.getenvBoolDefault("TERMITE_CUDA_UNSUPPORTED_DEBUG", false)) {
                    std.log.err("cuda linear_bias unsupported quantized weight type=unknown", .{});
                }
                return error.UnsupportedTensorType;
            },
        }
    } else {
        if (rows >= 2 and in_dim >= 256 and out_dim >= 4) {
            try self.kernels.launchLinearBiasTile4Rows2F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
        } else {
            try self.kernels.launchLinearBiasF32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
        }
    }
    return createTensor(self, device, shape, out_count);
}

fn linearQuickGelu(ctx: *anyopaque, input: CT, weight: CT, bias: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_tensor = tensorFromCt(weight);
    const bias_tensor = tensorFromCt(bias);
    if (!isKnownQuant(weight_tensor, .Q4_K)) return null;
    try ensureF32(input_tensor);
    try ensureF32(bias_tensor);
    try ensureCount(input_tensor, try checkedMul(rows, in_dim));
    try ensureCount(weight_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(bias_tensor, out_dim);

    const out_count = try checkedMul(rows, out_dim);
    const shape = try allocShape2(self.allocator, rows, out_dim);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchLinearQ4KBiasQuickGeluTile4F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
    return try createTensor(self, device, shape, out_count);
}

fn linearRelu(ctx: *anyopaque, input: CT, weight: CT, bias: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_tensor = tensorFromCt(weight);
    const bias_tensor = tensorFromCt(bias);
    try ensureF32(input_tensor);
    try ensureF32(bias_tensor);
    const use_q4 = isKnownQuant(weight_tensor, .Q4_K);
    const use_dense = weight_tensor.quant_type == null and rows >= 2 and in_dim >= 256 and out_dim >= 4;
    if (!use_q4 and !use_dense) return null;
    if (use_dense) try ensureF32(weight_tensor);
    try ensureCount(input_tensor, try checkedMul(rows, in_dim));
    try ensureCount(weight_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(bias_tensor, out_dim);

    const out_count = try checkedMul(rows, out_dim);
    const shape = try allocShape2(self.allocator, rows, out_dim);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    if (use_q4) {
        if (rows >= 2) {
            try self.kernels.launchLinearQ4KBiasReluTile4Rows2F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
        } else {
            try self.kernels.launchLinearQ4KBiasReluTile4F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
        }
    } else {
        try self.kernels.launchLinearBiasReluTile4Rows2F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
    }
    return try createTensor(self, device, shape, out_count);
}

fn linearGelu(ctx: *anyopaque, input: CT, weight: CT, bias: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_tensor = tensorFromCt(weight);
    const bias_tensor = tensorFromCt(bias);
    if (weight_tensor.quant_type != null) return null;
    if (rows < 2 or in_dim < 256 or out_dim < 4) return null;
    try ensureF32(input_tensor);
    try ensureF32(weight_tensor);
    try ensureF32(bias_tensor);
    try ensureCount(input_tensor, try checkedMul(rows, in_dim));
    try ensureCount(weight_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(bias_tensor, out_dim);

    const out_count = try checkedMul(rows, out_dim);
    const shape = try allocShape2(self.allocator, rows, out_dim);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchLinearBiasGeluTile4Rows2F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
    return try createTensor(self, device, shape, out_count);
}

fn linearAdd(ctx: *anyopaque, input: CT, weight: CT, bias: CT, residual: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_tensor = tensorFromCt(weight);
    const bias_tensor = tensorFromCt(bias);
    const residual_tensor = tensorFromCt(residual);
    try ensureF32(input_tensor);
    try ensureF32(bias_tensor);
    try ensureF32(residual_tensor);
    const use_q4 = isKnownQuant(weight_tensor, .Q4_K);
    const use_dense = weight_tensor.quant_type == null and rows >= 2 and in_dim >= 256 and out_dim >= 4;
    if (!use_q4 and !use_dense) return null;
    if (use_dense) try ensureF32(weight_tensor);
    try ensureCount(input_tensor, try checkedMul(rows, in_dim));
    try ensureCount(weight_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(bias_tensor, out_dim);
    const out_count = try checkedMul(rows, out_dim);
    try ensureCount(residual_tensor, out_count);

    const shape = try allocShape2(self.allocator, rows, out_dim);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    if (use_q4) {
        try self.kernels.launchLinearQ4KBiasAddTile4F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, residual_tensor.buffer, rows, in_dim, out_dim);
    } else {
        try self.kernels.launchLinearBiasAddTile4Rows2F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, residual_tensor.buffer, rows, in_dim, out_dim);
    }
    return try createTensor(self, device, shape, out_count);
}

fn linearNoBiasWithShape(ctx: *anyopaque, input: CT, weight: CT, rows: usize, in_dim: usize, out_dim: usize, shape: []i64) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    errdefer self.allocator.free(shape);
    const input_tensor = tensorFromCt(input);
    const weight_tensor = tensorFromCt(weight);
    try ensureF32(input_tensor);
    try ensureF32OrQuantized(weight_tensor);
    try ensureCount(input_tensor, try checkedMul(rows, in_dim));
    try ensureCount(weight_tensor, try checkedMul(out_dim, in_dim));

    const out_count = try checkedMul(rows, out_dim);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    if (weight_tensor.quant_type) |quant_type| {
        switch (quant_type) {
            .known => |known| switch (known) {
                .Q8_0 => try self.kernels.launchLinearQ8_0F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim),
                .Q4_0 => try self.kernels.launchLinearQ4_0F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim),
                .Q4_K => try self.kernels.launchLinearQ4KTile4F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim),
                .Q5_K => try self.kernels.launchLinearQ5KF32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim),
                .Q6_K => try self.kernels.launchLinearQ6KF32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim),
                else => {
                    if (platform.env.getenvBoolDefault("TERMITE_CUDA_UNSUPPORTED_DEBUG", false)) {
                        std.log.err("cuda linear unsupported quantized weight type={s}", .{@tagName(known)});
                    }
                    return error.UnsupportedTensorType;
                },
            },
            else => {
                if (platform.env.getenvBoolDefault("TERMITE_CUDA_UNSUPPORTED_DEBUG", false)) {
                    std.log.err("cuda linear unsupported quantized weight type=unknown", .{});
                }
                return error.UnsupportedTensorType;
            },
        }
    } else {
        try self.kernels.launchLinearF32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim);
    }
    return createTensor(self, device, shape, out_count);
}

fn linearNoBias(ctx: *anyopaque, input: CT, weight: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const shape = try allocShape2(self.allocator, rows, out_dim);
    return linearNoBiasWithShape(ctx, input, weight, rows, in_dim, out_dim, shape);
}

fn linearTriple(ctx: *anyopaque, input: CT, weight_a: CT, bias_a: CT, weight_b: CT, bias_b: CT, weight_c: CT, bias_c: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!ops.LinearTripleResult {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_a_tensor = tensorFromCt(weight_a);
    const weight_b_tensor = tensorFromCt(weight_b);
    const weight_c_tensor = tensorFromCt(weight_c);
    const bias_a_tensor = tensorFromCt(bias_a);
    const bias_b_tensor = tensorFromCt(bias_b);
    const bias_c_tensor = tensorFromCt(bias_c);

    try ensureF32(input_tensor);
    try ensureF32(bias_a_tensor);
    try ensureF32(bias_b_tensor);
    try ensureF32(bias_c_tensor);
    try ensureCount(input_tensor, try checkedMul(rows, in_dim));
    try ensureCount(weight_a_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(weight_b_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(weight_c_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(bias_a_tensor, out_dim);
    try ensureCount(bias_b_tensor, out_dim);
    try ensureCount(bias_c_tensor, out_dim);

    if (isKnownQuant(weight_a_tensor, .Q4_K) and isKnownQuant(weight_b_tensor, .Q4_K) and isKnownQuant(weight_c_tensor, .Q4_K)) {
        const out_count = try checkedMul(rows, out_dim);
        const shape_a = try allocShape2(self.allocator, rows, out_dim);
        var shape_a_owned = false;
        errdefer if (!shape_a_owned) self.allocator.free(shape_a);
        const shape_b = try allocShape2(self.allocator, rows, out_dim);
        var shape_b_owned = false;
        errdefer if (!shape_b_owned) self.allocator.free(shape_b);
        const shape_c = try allocShape2(self.allocator, rows, out_dim);
        var shape_c_owned = false;
        errdefer if (!shape_c_owned) self.allocator.free(shape_c);
        var device_a = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
        var device_a_owned = false;
        errdefer if (!device_a_owned) device_a.free(&self.ctx);
        var device_b = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
        var device_b_owned = false;
        errdefer if (!device_b_owned) device_b.free(&self.ctx);
        var device_c = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
        var device_c_owned = false;
        errdefer if (!device_c_owned) device_c.free(&self.ctx);
        try self.kernels.launchLinearQ4KTripleBiasTiledF32(
            &self.ctx,
            device_a,
            device_b,
            device_c,
            input_tensor.buffer,
            weight_a_tensor.buffer,
            bias_a_tensor.buffer,
            weight_b_tensor.buffer,
            bias_b_tensor.buffer,
            weight_c_tensor.buffer,
            bias_c_tensor.buffer,
            rows,
            in_dim,
            out_dim,
        );
        const first = try createTensor(self, device_a, shape_a, out_count);
        shape_a_owned = true;
        device_a_owned = true;
        errdefer freeTensor(ctx, first);
        const second = try createTensor(self, device_b, shape_b, out_count);
        shape_b_owned = true;
        device_b_owned = true;
        errdefer freeTensor(ctx, second);
        const third = try createTensor(self, device_c, shape_c, out_count);
        shape_c_owned = true;
        device_c_owned = true;
        return .{ .first = first, .second = second, .third = third };
    }

    const first = try linear(ctx, input, weight_a, bias_a, rows, in_dim, out_dim);
    errdefer freeTensor(ctx, first);
    const second = try linear(ctx, input, weight_b, bias_b, rows, in_dim, out_dim);
    errdefer freeTensor(ctx, second);
    const third = try linear(ctx, input, weight_c, bias_c, rows, in_dim, out_dim);
    return .{ .first = first, .second = second, .third = third };
}

fn linearPair(ctx: *anyopaque, input: CT, weight_a: CT, bias_a: CT, weight_b: CT, bias_b: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!ops.LinearPairResult {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_a_tensor = tensorFromCt(weight_a);
    const weight_b_tensor = tensorFromCt(weight_b);
    const bias_a_tensor = tensorFromCt(bias_a);
    const bias_b_tensor = tensorFromCt(bias_b);

    try ensureF32(input_tensor);
    try ensureF32(bias_a_tensor);
    try ensureF32(bias_b_tensor);
    try ensureCount(input_tensor, try checkedMul(rows, in_dim));
    try ensureCount(weight_a_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(weight_b_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(bias_a_tensor, out_dim);
    try ensureCount(bias_b_tensor, out_dim);

    if (!(isKnownQuant(weight_a_tensor, .Q4_K) and isKnownQuant(weight_b_tensor, .Q4_K))) {
        const first = try linear(ctx, input, weight_a, bias_a, rows, in_dim, out_dim);
        errdefer freeTensor(ctx, first);
        const second = try linear(ctx, input, weight_b, bias_b, rows, in_dim, out_dim);
        return .{ .first = first, .second = second };
    }

    const out_count = try checkedMul(rows, out_dim);
    const shape_a = try allocShape2(self.allocator, rows, out_dim);
    var shape_a_owned = false;
    errdefer if (!shape_a_owned) self.allocator.free(shape_a);
    const shape_b = try allocShape2(self.allocator, rows, out_dim);
    var shape_b_owned = false;
    errdefer if (!shape_b_owned) self.allocator.free(shape_b);
    var device_a = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    var device_a_owned = false;
    errdefer if (!device_a_owned) device_a.free(&self.ctx);
    var device_b = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    var device_b_owned = false;
    errdefer if (!device_b_owned) device_b.free(&self.ctx);
    try self.kernels.launchLinearQ4KPairBiasTiledF32(
        &self.ctx,
        device_a,
        device_b,
        input_tensor.buffer,
        weight_a_tensor.buffer,
        bias_a_tensor.buffer,
        weight_b_tensor.buffer,
        bias_b_tensor.buffer,
        rows,
        in_dim,
        out_dim,
    );
    const first = try createTensor(self, device_a, shape_a, out_count);
    shape_a_owned = true;
    device_a_owned = true;
    errdefer freeTensor(ctx, first);
    const second = try createTensor(self, device_b, shape_b, out_count);
    shape_b_owned = true;
    device_b_owned = true;
    return .{ .first = first, .second = second };
}

fn layerNorm(ctx: *anyopaque, input: CT, gamma: CT, beta: CT, dim: usize, eps: f32) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const gamma_tensor = tensorFromCt(gamma);
    const beta_tensor = tensorFromCt(beta);
    try ensureF32(input_tensor);
    try ensureF32(gamma_tensor);
    try ensureF32(beta_tensor);
    if (dim == 0 or input_tensor.elem_count % dim != 0) return error.InvalidShape;
    try ensureCount(gamma_tensor, dim);
    try ensureCount(beta_tensor, dim);
    const shape = try dupeShape(self.allocator, input_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, input_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchLayerNormF32(&self.ctx, device, input_tensor.buffer, gamma_tensor.buffer, beta_tensor.buffer, input_tensor.elem_count / dim, dim, eps);
    return createTensor(self, device, shape, input_tensor.elem_count);
}

fn addLayerNorm(ctx: *anyopaque, a: CT, b: CT, gamma: CT, beta: CT, dim: usize, eps: f32) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const a_tensor = tensorFromCt(a);
    const b_tensor = tensorFromCt(b);
    const gamma_tensor = tensorFromCt(gamma);
    const beta_tensor = tensorFromCt(beta);
    try ensureF32(a_tensor);
    try ensureF32(b_tensor);
    try ensureF32(gamma_tensor);
    try ensureF32(beta_tensor);
    if (a_tensor.elem_count != b_tensor.elem_count or !sameShape(a_tensor.shape, b_tensor.shape)) return null;
    if (dim == 0 or a_tensor.elem_count % dim != 0) return null;
    try ensureCount(gamma_tensor, dim);
    try ensureCount(beta_tensor, dim);

    const shape = try dupeShape(self.allocator, a_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, a_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchAddLayerNormF32(&self.ctx, device, a_tensor.buffer, b_tensor.buffer, gamma_tensor.buffer, beta_tensor.buffer, a_tensor.elem_count / dim, dim, eps);
    return try createTensor(self, device, shape, a_tensor.elem_count);
}

fn rmsNorm(ctx: *anyopaque, input: CT, weight: CT, dim: usize, eps: f32) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_tensor = tensorFromCt(weight);
    try ensureF32(input_tensor);
    try ensureF32(weight_tensor);
    if (dim == 0 or input_tensor.elem_count % dim != 0) return error.InvalidShape;
    try ensureCount(weight_tensor, dim);

    const shape = try dupeShape(self.allocator, input_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, input_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchRmsNormF32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, input_tensor.elem_count / dim, dim, eps);
    return createTensor(self, device, shape, input_tensor.elem_count);
}
const UnaryOp = enum { silu, gelu, relu, quick_gelu, sigmoid, tanh, exp, log, sqrt, rsqrt, abs, sin, cos, erf };

fn unaryHost(ctx: *anyopaque, input: CT, op: UnaryOp) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);
    const shape = try dupeShape(self.allocator, input_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, input_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    const kernel_op: kernels_mod.ElementwiseOp = switch (op) {
        .silu => .silu,
        .gelu => .gelu,
        .relu => .relu,
        .quick_gelu => .quick_gelu,
        .sigmoid => .sigmoid,
        .tanh => .tanh,
        .exp => .exp,
        .log => .log,
        .sqrt => .sqrt,
        .rsqrt => .rsqrt,
        .abs => .abs,
        .sin => .sin,
        .cos => .cos,
        .erf => .erf,
    };
    try self.kernels.launchElementwiseF32(&self.ctx, device, input_tensor.buffer, .{}, input_tensor.elem_count, kernel_op);
    return createTensor(self, device, shape, input_tensor.elem_count);
}

fn silu(ctx: *anyopaque, input: CT) anyerror!CT {
    return unaryHost(ctx, input, .silu);
}

fn gelu(ctx: *anyopaque, input: CT) anyerror!CT {
    return unaryHost(ctx, input, .gelu);
}

fn relu(ctx: *anyopaque, input: CT) anyerror!CT {
    return unaryHost(ctx, input, .relu);
}

fn quickGelu(ctx: *anyopaque, input: CT) anyerror!CT {
    return unaryHost(ctx, input, .quick_gelu);
}

fn sigmoid(ctx: *anyopaque, input: CT) anyerror!CT {
    return unaryHost(ctx, input, .sigmoid);
}

fn tanhAct(ctx: *anyopaque, input: CT) anyerror!CT {
    return unaryHost(ctx, input, .tanh);
}
fn concat(ctx: *anyopaque, a: CT, b: CT, total: usize, dim_a: usize, dim_b: usize) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const a_tensor = tensorFromCt(a);
    const b_tensor = tensorFromCt(b);
    try ensureF32(a_tensor);
    try ensureF32(b_tensor);
    try ensureCount(a_tensor, try checkedMul(total, dim_a));
    try ensureCount(b_tensor, try checkedMul(total, dim_b));
    const out_dim = try checkedAdd(dim_a, dim_b);
    const out_count = try checkedMul(total, out_dim);
    const shape = try allocShape2(self.allocator, total, out_dim);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchConcatLastDimF32(&self.ctx, device, a_tensor.buffer, b_tensor.buffer, total, dim_a, dim_b);
    return createTensor(self, device, shape, out_count);
}

fn concatRows2D(ctx: *anyopaque, a: CT, b: CT, rows_a: usize, rows_b: usize, cols: usize) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const a_tensor = tensorFromCt(a);
    const b_tensor = tensorFromCt(b);
    try ensureF32(a_tensor);
    try ensureF32(b_tensor);
    try ensureCount(a_tensor, try checkedMul(rows_a, cols));
    try ensureCount(b_tensor, try checkedMul(rows_b, cols));

    const out_rows = try checkedAdd(rows_a, rows_b);
    const out_count = try checkedMul(out_rows, cols);
    const shape = try allocShape2(self.allocator, out_rows, cols);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    const a_bytes = a_tensor.elem_count * @sizeOf(f32);
    const b_bytes = b_tensor.elem_count * @sizeOf(f32);
    try device.copyFromDeviceOffset(&self.ctx, 0, a_tensor.buffer, 0, a_bytes);
    try device.copyFromDeviceOffset(&self.ctx, a_bytes, b_tensor.buffer, 0, b_bytes);
    return createTensor(self, device, shape, out_count);
}

fn sliceRows2D(ctx: *anyopaque, input: CT, start_row: usize, row_count: usize, cols: usize) anyerror!CT {
    if (cols == 0) return error.InvalidShape;
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);
    if (input_tensor.elem_count % cols != 0) return error.UnexpectedOutputShape;
    const total_rows = input_tensor.elem_count / cols;
    if (start_row > total_rows or row_count > total_rows - start_row) return error.UnexpectedOutputShape;

    const out_count = try checkedMul(row_count, cols);
    const shape = try allocShape2(self.allocator, row_count, cols);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    const src_offset = try checkedMul(try checkedMul(start_row, cols), @sizeOf(f32));
    const byte_count = try checkedMul(out_count, @sizeOf(f32));
    try device.copyFromDeviceOffset(&self.ctx, 0, input_tensor.buffer, src_offset, byte_count);
    return createTensor(self, device, shape, out_count);
}

fn copyLastDimSliceDevice(ctx: *anyopaque, input: CT, input_shape: []const i64, start: usize, stop: usize) anyerror!CT {
    if (input_shape.len == 0 or start > stop) return error.InvalidShape;
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);

    const last_dim_i64 = input_shape[input_shape.len - 1];
    if (last_dim_i64 <= 0) return error.InvalidShape;
    const last_dim: usize = @intCast(last_dim_i64);
    if (stop > last_dim) return error.OutOfBounds;

    var rows: usize = 1;
    for (input_shape[0 .. input_shape.len - 1]) |dim| {
        if (dim <= 0) return error.InvalidShape;
        rows = try checkedMul(rows, @intCast(dim));
    }
    try ensureCount(input_tensor, try checkedMul(rows, last_dim));

    const out_dim = stop - start;
    const out_count = try checkedMul(rows, out_dim);
    const output_shape = try dupeShape(self.allocator, input_shape);
    errdefer self.allocator.free(output_shape);
    output_shape[output_shape.len - 1] = @intCast(out_dim);

    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    const row_bytes = try checkedMul(out_dim, @sizeOf(f32));
    for (0..rows) |row| {
        const src_elem = try checkedAdd(try checkedMul(row, last_dim), start);
        const dst_elem = try checkedMul(row, out_dim);
        try device.copyFromDeviceOffset(
            &self.ctx,
            try checkedMul(dst_elem, @sizeOf(f32)),
            input_tensor.buffer,
            try checkedMul(src_elem, @sizeOf(f32)),
            row_bytes,
        );
    }
    return createTensor(self, device, output_shape, out_count);
}

fn splitLastDim3(ctx: *anyopaque, input: CT, rows: usize, dim: usize) anyerror!ops.SplitLastDim3Result {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);
    const part_count = try checkedMul(rows, dim);
    try ensureCount(input_tensor, try checkedMul(part_count, 3));
    const shape_first = try allocShape2(self.allocator, rows, dim);
    var shape_first_owned = false;
    errdefer if (!shape_first_owned) self.allocator.free(shape_first);
    const shape_second = try allocShape2(self.allocator, rows, dim);
    var shape_second_owned = false;
    errdefer if (!shape_second_owned) self.allocator.free(shape_second);
    const shape_third = try allocShape2(self.allocator, rows, dim);
    var shape_third_owned = false;
    errdefer if (!shape_third_owned) self.allocator.free(shape_third);
    var first_device = try allocDeviceBuffer(self, part_count * @sizeOf(f32));
    var first_device_owned = false;
    errdefer if (!first_device_owned) first_device.free(&self.ctx);
    var second_device = try allocDeviceBuffer(self, part_count * @sizeOf(f32));
    var second_device_owned = false;
    errdefer if (!second_device_owned) second_device.free(&self.ctx);
    var third_device = try allocDeviceBuffer(self, part_count * @sizeOf(f32));
    var third_device_owned = false;
    errdefer if (!third_device_owned) third_device.free(&self.ctx);
    try self.kernels.launchSplitLastDim3F32(&self.ctx, first_device, second_device, third_device, input_tensor.buffer, rows, dim);
    const first = try createTensor(self, first_device, shape_first, part_count);
    first_device_owned = true;
    shape_first_owned = true;
    errdefer freeTensor(ctx, first);
    const second = try createTensor(self, second_device, shape_second, part_count);
    second_device_owned = true;
    shape_second_owned = true;
    errdefer freeTensor(ctx, second);
    const third = try createTensor(self, third_device, shape_third, part_count);
    third_device_owned = true;
    shape_third_owned = true;
    return .{ .first = first, .second = second, .third = third };
}

fn sliceLastDim(ctx: *anyopaque, input: CT, start: usize, stop: usize) anyerror!CT {
    if (start > stop) return error.OutOfBounds;
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);
    if (input_tensor.shape.len == 2 and input_tensor.shape[0] > 0 and input_tensor.shape[1] > 0) {
        const rows: usize = @intCast(input_tensor.shape[0]);
        const cols: usize = @intCast(input_tensor.shape[1]);
        if (stop <= cols) {
            const dim = stop - start;
            if (dim > 0 and cols == dim * 3 and (start == 0 or start == dim or start == dim * 2)) {
                const parts = try splitLastDim3(ctx, input, rows, dim);
                switch (start / dim) {
                    0 => {
                        defer freeTensor(ctx, parts.second);
                        defer freeTensor(ctx, parts.third);
                        return parts.first;
                    },
                    1 => {
                        defer freeTensor(ctx, parts.first);
                        defer freeTensor(ctx, parts.third);
                        return parts.second;
                    },
                    2 => {
                        defer freeTensor(ctx, parts.first);
                        defer freeTensor(ctx, parts.second);
                        return parts.third;
                    },
                    else => unreachable,
                }
            }
        }
    }
    if (input_tensor.shape.len >= 1) {
        const maybe: ?CT = copyLastDimSliceDevice(ctx, input, input_tensor.shape, start, stop) catch |err| switch (err) {
            error.InvalidShape, error.OutOfBounds => null,
            else => return err,
        };
        if (maybe) |out| return out;
    }

    var fallback: NativeFallback = undefined;
    try fallback.init(self, "slice_last_dim");
    defer fallback.deinit();
    const ni = try nativeCtFromCuda(self, &fallback, input);
    defer fallback.cb.free(ni);
    const out = try fallback.cb.sliceLastDim(ni, start, stop);
    defer fallback.cb.free(out);
    return cudaCtFromNative(self, &fallback, out, input_tensor.shape);
}

fn binaryElementwise(ctx: *anyopaque, a: CT, b: CT, op: kernels_mod.ElementwiseOp) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const a_tensor = tensorFromCt(a);
    const b_tensor = tensorFromCt(b);
    try ensureF32(a_tensor);
    try ensureF32(b_tensor);
    if (a_tensor.elem_count != b_tensor.elem_count or !sameShape(a_tensor.shape, b_tensor.shape)) {
        if (try binaryElementwiseBroadcast(ctx, a_tensor, b_tensor, op)) |out| return out;
        return nativeBinaryFallback(ctx, a, b, switch (op) {
            .add => .add,
            .multiply => .multiply,
            .less_than => .less_than,
            .divide => .divide,
            .subtract => .subtract,
            else => return error.InvalidShape,
        });
    }

    const shape = try dupeShape(self.allocator, a_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, a_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchElementwiseF32(&self.ctx, device, a_tensor.buffer, b_tensor.buffer, a_tensor.elem_count, op);
    return createTensor(self, device, shape, a_tensor.elem_count);
}

fn binaryElementwiseBroadcast(ctx: *anyopaque, a_tensor: *const CudaTensor, b_tensor: *const CudaTensor, op: kernels_mod.ElementwiseOp) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const out_plan = try planBroadcastShape3(a_tensor.shape, b_tensor.shape, &[_]i64{}, a_tensor.elem_count, b_tensor.elem_count, 1) orelse return null;
    const out_shape = out_plan.shape[0..out_plan.rank];
    const shape = try dupeShape(self.allocator, out_shape);
    errdefer self.allocator.free(shape);

    var a_temp: buffer_mod.DeviceBuffer = .{};
    defer releaseDeviceBuffer(self, &a_temp);
    var b_temp: buffer_mod.DeviceBuffer = .{};
    defer releaseDeviceBuffer(self, &b_temp);
    const a_buffer = (try materializeBroadcastOperand(self, a_tensor, out_shape, out_plan.count, &a_temp)) orelse {
        self.allocator.free(shape);
        return null;
    };
    const b_buffer = (try materializeBroadcastOperand(self, b_tensor, out_shape, out_plan.count, &b_temp)) orelse {
        self.allocator.free(shape);
        return null;
    };

    var device = try allocDeviceBuffer(self, out_plan.count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchElementwiseF32(&self.ctx, device, a_buffer, b_buffer, out_plan.count, op);
    return createTensor(self, device, shape, out_plan.count);
}

fn negateDevice(ctx: *anyopaque, input: CT) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);

    const shape = try dupeShape(self.allocator, input_tensor.shape);
    errdefer self.allocator.free(shape);
    var scale = try allocDeviceBuffer(self, input_tensor.elem_count * @sizeOf(f32));
    defer releaseDeviceBuffer(self, &scale);
    var device = try allocDeviceBuffer(self, input_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchFillF32(&self.ctx, scale, input_tensor.elem_count, -1.0);
    try self.kernels.launchElementwiseF32(&self.ctx, device, input_tensor.buffer, scale, input_tensor.elem_count, .multiply);
    return createTensor(self, device, shape, input_tensor.elem_count);
}

fn subtractDeviceSameShape(ctx: *anyopaque, a: CT, b: CT) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const a_tensor = tensorFromCt(a);
    const b_tensor = tensorFromCt(b);
    try ensureF32(a_tensor);
    try ensureF32(b_tensor);
    if (a_tensor.elem_count != b_tensor.elem_count or !sameShape(a_tensor.shape, b_tensor.shape)) return null;

    const shape = try dupeShape(self.allocator, a_tensor.shape);
    errdefer self.allocator.free(shape);
    var scale = try allocDeviceBuffer(self, b_tensor.elem_count * @sizeOf(f32));
    defer releaseDeviceBuffer(self, &scale);
    var neg_b = try allocDeviceBuffer(self, b_tensor.elem_count * @sizeOf(f32));
    defer releaseDeviceBuffer(self, &neg_b);
    var device = try allocDeviceBuffer(self, a_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchFillF32(&self.ctx, scale, b_tensor.elem_count, -1.0);
    try self.kernels.launchElementwiseF32(&self.ctx, neg_b, b_tensor.buffer, scale, b_tensor.elem_count, .multiply);
    try self.kernels.launchElementwiseF32(&self.ctx, device, a_tensor.buffer, neg_b, a_tensor.elem_count, .add);
    return createTensor(self, device, shape, a_tensor.elem_count);
}

fn subtractDeviceBroadcast(ctx: *anyopaque, a: CT, b: CT) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const a_tensor = tensorFromCt(a);
    const b_tensor = tensorFromCt(b);
    try ensureF32(a_tensor);
    try ensureF32(b_tensor);
    const out_plan = try planBroadcastShape3(a_tensor.shape, b_tensor.shape, &[_]i64{}, a_tensor.elem_count, b_tensor.elem_count, 1) orelse return null;
    const out_shape = out_plan.shape[0..out_plan.rank];
    const shape = try dupeShape(self.allocator, out_shape);
    errdefer self.allocator.free(shape);

    var a_temp: buffer_mod.DeviceBuffer = .{};
    defer releaseDeviceBuffer(self, &a_temp);
    var b_temp: buffer_mod.DeviceBuffer = .{};
    defer releaseDeviceBuffer(self, &b_temp);
    var neg_one: buffer_mod.DeviceBuffer = .{};
    defer releaseDeviceBuffer(self, &neg_one);
    var neg_b: buffer_mod.DeviceBuffer = .{};
    defer releaseDeviceBuffer(self, &neg_b);
    const a_buffer = (try materializeBroadcastOperand(self, a_tensor, out_shape, out_plan.count, &a_temp)) orelse {
        self.allocator.free(shape);
        return null;
    };
    const b_buffer = (try materializeBroadcastOperand(self, b_tensor, out_shape, out_plan.count, &b_temp)) orelse {
        self.allocator.free(shape);
        return null;
    };

    neg_one = try allocDeviceBuffer(self, out_plan.count * @sizeOf(f32));
    neg_b = try allocDeviceBuffer(self, out_plan.count * @sizeOf(f32));
    var device = try allocDeviceBuffer(self, out_plan.count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchFillF32(&self.ctx, neg_one, out_plan.count, -1.0);
    try self.kernels.launchElementwiseF32(&self.ctx, neg_b, b_buffer, neg_one, out_plan.count, .multiply);
    try self.kernels.launchElementwiseF32(&self.ctx, device, a_buffer, neg_b, out_plan.count, .add);
    return createTensor(self, device, shape, out_plan.count);
}

fn whereSelectDeviceBroadcast(ctx: *anyopaque, cond: CT, on_true: CT, on_false: CT) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const cond_tensor = tensorFromCt(cond);
    const true_tensor = tensorFromCt(on_true);
    const false_tensor = tensorFromCt(on_false);
    try ensureF32(cond_tensor);
    try ensureF32(true_tensor);
    try ensureF32(false_tensor);

    const out_plan = try planBroadcastShape3(
        cond_tensor.shape,
        true_tensor.shape,
        false_tensor.shape,
        cond_tensor.elem_count,
        true_tensor.elem_count,
        false_tensor.elem_count,
    ) orelse return null;
    const out_shape = out_plan.shape[0..out_plan.rank];

    const shape = try dupeShape(self.allocator, out_shape);
    errdefer self.allocator.free(shape);
    var cond_temp: buffer_mod.DeviceBuffer = .{};
    defer releaseDeviceBuffer(self, &cond_temp);
    var true_temp: buffer_mod.DeviceBuffer = .{};
    defer releaseDeviceBuffer(self, &true_temp);
    var false_temp: buffer_mod.DeviceBuffer = .{};
    defer releaseDeviceBuffer(self, &false_temp);
    const cond_buffer = (try materializeBroadcastOperand(self, cond_tensor, out_shape, out_plan.count, &cond_temp)) orelse {
        self.allocator.free(shape);
        return null;
    };
    const true_buffer = (try materializeBroadcastOperand(self, true_tensor, out_shape, out_plan.count, &true_temp)) orelse {
        self.allocator.free(shape);
        return null;
    };
    const false_buffer = (try materializeBroadcastOperand(self, false_tensor, out_shape, out_plan.count, &false_temp)) orelse {
        self.allocator.free(shape);
        return null;
    };

    var true_part = try allocDeviceBuffer(self, out_plan.count * @sizeOf(f32));
    defer releaseDeviceBuffer(self, &true_part);
    var neg_one = try allocDeviceBuffer(self, out_plan.count * @sizeOf(f32));
    defer releaseDeviceBuffer(self, &neg_one);
    var neg_cond = try allocDeviceBuffer(self, out_plan.count * @sizeOf(f32));
    defer releaseDeviceBuffer(self, &neg_cond);
    var one = try allocDeviceBuffer(self, out_plan.count * @sizeOf(f32));
    defer releaseDeviceBuffer(self, &one);
    var inv_cond = try allocDeviceBuffer(self, out_plan.count * @sizeOf(f32));
    defer releaseDeviceBuffer(self, &inv_cond);
    var false_part = try allocDeviceBuffer(self, out_plan.count * @sizeOf(f32));
    defer releaseDeviceBuffer(self, &false_part);
    var device = try allocDeviceBuffer(self, out_plan.count * @sizeOf(f32));
    errdefer device.free(&self.ctx);

    try self.kernels.launchElementwiseF32(&self.ctx, true_part, cond_buffer, true_buffer, out_plan.count, .multiply);
    try self.kernels.launchFillF32(&self.ctx, neg_one, out_plan.count, -1.0);
    try self.kernels.launchElementwiseF32(&self.ctx, neg_cond, cond_buffer, neg_one, out_plan.count, .multiply);
    try self.kernels.launchFillF32(&self.ctx, one, out_plan.count, 1.0);
    try self.kernels.launchElementwiseF32(&self.ctx, inv_cond, one, neg_cond, out_plan.count, .add);
    try self.kernels.launchElementwiseF32(&self.ctx, false_part, inv_cond, false_buffer, out_plan.count, .multiply);
    try self.kernels.launchElementwiseF32(&self.ctx, device, true_part, false_part, out_plan.count, .add);
    return createTensor(self, device, shape, out_plan.count);
}

fn whereSelectDeviceSameShape(ctx: *anyopaque, cond: CT, on_true: CT, on_false: CT) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const cond_tensor = tensorFromCt(cond);
    const true_tensor = tensorFromCt(on_true);
    const false_tensor = tensorFromCt(on_false);
    try ensureF32(cond_tensor);
    try ensureF32(true_tensor);
    try ensureF32(false_tensor);
    if (cond_tensor.elem_count != true_tensor.elem_count or cond_tensor.elem_count != false_tensor.elem_count) return null;
    if (!sameShape(cond_tensor.shape, true_tensor.shape) or !sameShape(cond_tensor.shape, false_tensor.shape)) return null;

    const shape = try dupeShape(self.allocator, true_tensor.shape);
    errdefer self.allocator.free(shape);
    var true_part = try allocDeviceBuffer(self, true_tensor.elem_count * @sizeOf(f32));
    defer releaseDeviceBuffer(self, &true_part);
    var neg_one = try allocDeviceBuffer(self, cond_tensor.elem_count * @sizeOf(f32));
    defer releaseDeviceBuffer(self, &neg_one);
    var neg_cond = try allocDeviceBuffer(self, cond_tensor.elem_count * @sizeOf(f32));
    defer releaseDeviceBuffer(self, &neg_cond);
    var one = try allocDeviceBuffer(self, cond_tensor.elem_count * @sizeOf(f32));
    defer releaseDeviceBuffer(self, &one);
    var inv_cond = try allocDeviceBuffer(self, cond_tensor.elem_count * @sizeOf(f32));
    defer releaseDeviceBuffer(self, &inv_cond);
    var false_part = try allocDeviceBuffer(self, false_tensor.elem_count * @sizeOf(f32));
    defer releaseDeviceBuffer(self, &false_part);
    var device = try allocDeviceBuffer(self, true_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);

    try self.kernels.launchElementwiseF32(&self.ctx, true_part, cond_tensor.buffer, true_tensor.buffer, cond_tensor.elem_count, .multiply);
    try self.kernels.launchFillF32(&self.ctx, neg_one, cond_tensor.elem_count, -1.0);
    try self.kernels.launchElementwiseF32(&self.ctx, neg_cond, cond_tensor.buffer, neg_one, cond_tensor.elem_count, .multiply);
    try self.kernels.launchFillF32(&self.ctx, one, cond_tensor.elem_count, 1.0);
    try self.kernels.launchElementwiseF32(&self.ctx, inv_cond, one, neg_cond, cond_tensor.elem_count, .add);
    try self.kernels.launchElementwiseF32(&self.ctx, false_part, inv_cond, false_tensor.buffer, cond_tensor.elem_count, .multiply);
    try self.kernels.launchElementwiseF32(&self.ctx, device, true_part, false_part, cond_tensor.elem_count, .add);
    return createTensor(self, device, shape, true_tensor.elem_count);
}

fn add(ctx: *anyopaque, a: CT, b: CT) anyerror!CT {
    return binaryElementwise(ctx, a, b, .add);
}

fn multiply(ctx: *anyopaque, a: CT, b: CT) anyerror!CT {
    return binaryElementwise(ctx, a, b, .multiply);
}

fn subtractOp(ctx: *anyopaque, a: CT, b: CT) anyerror!CT {
    if (try subtractDeviceSameShape(ctx, a, b)) |out| return out;
    if (try subtractDeviceBroadcast(ctx, a, b)) |out| return out;
    return nativeBinaryFallback(ctx, a, b, .subtract);
}

fn divideOp(ctx: *anyopaque, a: CT, b: CT) anyerror!CT {
    return binaryElementwise(ctx, a, b, .divide);
}

fn negateOp(ctx: *anyopaque, a: CT) anyerror!CT {
    return negateDevice(ctx, a);
}

fn primSqrtOp(ctx: *anyopaque, a: CT) anyerror!CT {
    return unaryHost(ctx, a, .sqrt);
}

fn primRsqrtOp(ctx: *anyopaque, a: CT) anyerror!CT {
    return unaryHost(ctx, a, .rsqrt);
}

fn primExpOp(ctx: *anyopaque, a: CT) anyerror!CT {
    return unaryHost(ctx, a, .exp);
}

fn primLogOp(ctx: *anyopaque, a: CT) anyerror!CT {
    return unaryHost(ctx, a, .log);
}

fn primSinOp(ctx: *anyopaque, a: CT) anyerror!CT {
    return unaryHost(ctx, a, .sin);
}

fn primCosOp(ctx: *anyopaque, a: CT) anyerror!CT {
    return unaryHost(ctx, a, .cos);
}

fn primTanhOp(ctx: *anyopaque, a: CT) anyerror!CT {
    return unaryHost(ctx, a, .tanh);
}

fn primErfOp(ctx: *anyopaque, a: CT) anyerror!CT {
    return unaryHost(ctx, a, .erf);
}

fn primAbsOp(ctx: *anyopaque, a: CT) anyerror!CT {
    return unaryHost(ctx, a, .abs);
}

fn lessThanOp(ctx: *anyopaque, a: CT, b: CT) anyerror!CT {
    return binaryElementwise(ctx, a, b, .less_than);
}

fn whereSelectOp(ctx: *anyopaque, cond: CT, on_true: CT, on_false: CT) anyerror!CT {
    if (try whereSelectDeviceSameShape(ctx, cond, on_true, on_false)) |out| return out;
    if (try whereSelectDeviceBroadcast(ctx, cond, on_true, on_false)) |out| return out;
    return nativeWhereSelectFallback(ctx, cond, on_true, on_false);
}

const ReduceMode = enum { sum, max, mean };

fn reduceOutputShape(allocator: std.mem.Allocator, input_shape: []const i64, axes: []const u8) ![]i64 {
    const out = try allocator.dupe(i64, input_shape);
    errdefer allocator.free(out);
    for (axes) |axis| {
        if (axis >= out.len) return error.InvalidShape;
        out[axis] = 1;
    }
    return out;
}

fn isAllAxesReduction(axes: []const u8, rank: usize) bool {
    if (axes.len != rank) return false;
    var seen = [_]bool{false} ** 8;
    for (axes) |axis| {
        if (axis >= rank or seen[axis]) return false;
        seen[axis] = true;
    }
    return true;
}

fn reduceDeviceSupported(ctx: *anyopaque, input: CT, axes: []const u8, input_shape: []const i64, mode: ReduceMode) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);
    if (input_shape.len == 0 or input_shape.len > 8) return null;
    const input_count = try elementCountFromShape(input_shape);
    try ensureCount(input_tensor, input_count);

    var rows: usize = 0;
    var dim: usize = 0;
    if (axes.len == 1 and axes[0] == input_shape.len - 1) {
        dim = @intCast(input_shape[input_shape.len - 1]);
        if (dim == 0) return error.InvalidShape;
        rows = input_count / dim;
    } else if (isAllAxesReduction(axes, input_shape.len)) {
        dim = input_count;
        rows = 1;
    } else {
        return null;
    }

    const output_shape = try reduceOutputShape(self.allocator, input_shape, axes);
    errdefer self.allocator.free(output_shape);
    var device = try allocDeviceBuffer(self, rows * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    const kernel_mode: kernels_mod.ReduceOp = switch (mode) {
        .sum => .sum,
        .max => .max,
        .mean => .mean,
    };
    try self.kernels.launchReduceLastDimF32(&self.ctx, device, input_tensor.buffer, rows, dim, kernel_mode);
    return createTensor(self, device, output_shape, rows);
}

fn primReduceSumOp(ctx: *anyopaque, input: CT, axes: []const u8, input_shape: []const i64) anyerror!CT {
    if (axes.len == 0) return deviceCopyWithShape(ctx, input, input_shape);
    if (try reduceDeviceSupported(ctx, input, axes, input_shape, .sum)) |out| return out;
    return nativeReduceFallback(ctx, input, axes, input_shape, .sum);
}

fn primReduceMaxOp(ctx: *anyopaque, input: CT, axes: []const u8, input_shape: []const i64) anyerror!CT {
    if (axes.len == 0) return deviceCopyWithShape(ctx, input, input_shape);
    if (try reduceDeviceSupported(ctx, input, axes, input_shape, .max)) |out| return out;
    return nativeReduceFallback(ctx, input, axes, input_shape, .max);
}

fn primReduceMeanOp(ctx: *anyopaque, input: CT, axes: []const u8, input_shape: []const i64) anyerror!CT {
    if (axes.len == 0) return deviceCopyWithShape(ctx, input, input_shape);
    if (try reduceDeviceSupported(ctx, input, axes, input_shape, .mean)) |out| return out;
    return nativeReduceFallback(ctx, input, axes, input_shape, .mean);
}

fn primReshapeOp(ctx: *anyopaque, input: CT, new_shape: []const i64) anyerror!CT {
    return deviceCopyWithShape(ctx, input, new_shape);
}

fn isAliasableTranspose(input_shape: []const i64, perm: []const u8) bool {
    if (perm.len != input_shape.len) return false;

    var expected_non_singleton: [16]usize = undefined;
    if (input_shape.len > expected_non_singleton.len) return false;
    var expected_len: usize = 0;
    for (input_shape, 0..) |dim, axis| {
        if (dim < 0) return false;
        if (dim != 1) {
            expected_non_singleton[expected_len] = axis;
            expected_len += 1;
        }
    }

    var seen = [_]bool{false} ** 16;
    var seen_non_singleton: usize = 0;
    for (perm) |axis_u8| {
        const axis: usize = axis_u8;
        if (axis >= input_shape.len or seen[axis]) return false;
        seen[axis] = true;
        if (input_shape[axis] != 1) {
            if (seen_non_singleton >= expected_len or expected_non_singleton[seen_non_singleton] != axis) return false;
            seen_non_singleton += 1;
        }
    }
    return seen_non_singleton == expected_len;
}

fn transposeAliasShape(allocator: std.mem.Allocator, input_shape: []const i64, perm: []const u8) ![]i64 {
    const out = try allocator.alloc(i64, input_shape.len);
    errdefer allocator.free(out);
    for (perm, 0..) |src_axis, dst_axis| out[dst_axis] = input_shape[src_axis];
    return out;
}

fn deviceTranspose2D(ctx: *anyopaque, input: CT, rows: usize, cols: usize) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);
    try ensureCount(input_tensor, try checkedMul(rows, cols));

    const output_shape = try allocShape2(self.allocator, cols, rows);
    errdefer self.allocator.free(output_shape);
    var device = try allocDeviceBuffer(self, input_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchTranspose2DF32(&self.ctx, device, input_tensor.buffer, rows, cols);
    return createTensor(self, device, output_shape, input_tensor.elem_count);
}

fn computeU32Strides(shape: []const u32, out: []u32) void {
    var stride: u32 = 1;
    var rev_idx: usize = shape.len;
    while (rev_idx > 0) {
        rev_idx -= 1;
        out[rev_idx] = stride;
        stride *= shape[rev_idx];
    }
}

fn deviceTransposeND(ctx: *anyopaque, input: CT, perm: []const u8, input_shape: []const i64) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);
    if (input_shape.len == 0 or input_shape.len > 8 or perm.len != input_shape.len) return error.UnsupportedShape;

    const rank = input_shape.len;
    var input_shape_u32 = [_]u32{0} ** 8;
    var output_shape_u32 = [_]u32{0} ** 8;
    var input_strides = [_]u32{0} ** 8;
    var output_strides = [_]u32{0} ** 8;
    var perm_u32 = [_]u32{0} ** 8;
    var seen = [_]bool{false} ** 8;
    var output_count: usize = 1;

    for (input_shape, 0..) |dim, idx| {
        if (dim <= 0 or dim > std.math.maxInt(u32)) return error.UnsupportedShape;
        input_shape_u32[idx] = @intCast(dim);
    }
    for (perm, 0..) |axis_u8, dst_axis| {
        const axis: usize = axis_u8;
        if (axis >= rank or seen[axis]) return error.InvalidShape;
        seen[axis] = true;
        perm_u32[dst_axis] = axis_u8;
        output_shape_u32[dst_axis] = input_shape_u32[axis];
        output_count = try checkedMul(output_count, output_shape_u32[dst_axis]);
    }
    if (output_count != input_tensor.elem_count or output_count > std.math.maxInt(u32)) return error.InvalidShape;

    computeU32Strides(input_shape_u32[0..rank], input_strides[0..rank]);
    computeU32Strides(output_shape_u32[0..rank], output_strides[0..rank]);
    const output_shape = try transposeAliasShape(self.allocator, input_shape, perm);
    errdefer self.allocator.free(output_shape);
    var device = try allocDeviceBuffer(self, input_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);

    var metadata = [_]u32{0} ** (8 * 4);
    var metadata_len: usize = 0;
    for (input_shape_u32[0..rank]) |value| {
        metadata[metadata_len] = value;
        metadata_len += 1;
    }
    for (input_strides[0..rank]) |value| {
        metadata[metadata_len] = value;
        metadata_len += 1;
    }
    for (output_strides[0..rank]) |value| {
        metadata[metadata_len] = value;
        metadata_len += 1;
    }
    for (perm_u32[0..rank]) |value| {
        metadata[metadata_len] = value;
        metadata_len += 1;
    }

    const metadata_device = try uploadTempU32(self, metadata[0..metadata_len]);
    const input_shape_device = metadataBufferSlice(metadata_device, 0, rank);
    const input_strides_device = metadataBufferSlice(metadata_device, rank, rank);
    const output_strides_device = metadataBufferSlice(metadata_device, rank * 2, rank);
    const perm_device = metadataBufferSlice(metadata_device, rank * 3, rank);
    try self.kernels.launchTransposeNDF32(
        &self.ctx,
        device,
        input_tensor.buffer,
        input_tensor.elem_count,
        rank,
        input_shape_device,
        input_strides_device,
        output_strides_device,
        perm_device,
    );
    return createTensor(self, device, output_shape, input_tensor.elem_count);
}

fn tryDeviceTranspose2D(ctx: *anyopaque, input: CT, perm: []const u8, input_shape: []const i64) anyerror!?CT {
    if (input_shape.len != 2 or perm.len != 2 or perm[0] != 1 or perm[1] != 0) return null;
    if (input_shape[0] <= 0 or input_shape[1] <= 0) return null;
    return try deviceTranspose2D(ctx, input, @intCast(input_shape[0]), @intCast(input_shape[1]));
}

fn primTransposeOp(ctx: *anyopaque, input: CT, perm: []const u8, input_shape: []const i64) anyerror!CT {
    if (isAliasableTranspose(input_shape, perm)) {
        const self: *CudaCompute = @ptrCast(@alignCast(ctx));
        const output_shape = try transposeAliasShape(self.allocator, input_shape, perm);
        defer self.allocator.free(output_shape);
        return deviceCopyWithShape(ctx, input, output_shape);
    }
    if (try tryDeviceTranspose2D(ctx, input, perm, input_shape)) |out| return out;
    if (input_shape.len > 2) return deviceTransposeND(ctx, input, perm, input_shape);
    var fallback: NativeFallback = undefined;
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    try fallback.init(self, "transpose");
    defer fallback.deinit();
    const ni = try nativeCtFromCuda(self, &fallback, input);
    defer fallback.cb.free(ni);
    const out = try fallback.cb.primTranspose(ni, perm, input_shape);
    defer fallback.cb.free(out);
    return cudaCtFromNative(self, &fallback, out, input_shape);
}

fn primBroadcastInDimOp(ctx: *anyopaque, input: CT, target_shape: []const i64, broadcast_axes: []const u8, input_shape: []const i64) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const axes_u32 = try u32FromAxes(self.allocator, broadcast_axes);
    defer self.allocator.free(axes_u32);
    return broadcastToShapeDeviceMapped(ctx, input, target_shape, input_shape, axes_u32) catch |err| switch (err) {
        error.UnsupportedShape, error.InvalidShape => return nativeBroadcastInDimFallback(ctx, input, target_shape, broadcast_axes, input_shape),
        else => return err,
    };
}

fn axesArePrefix(axes: []const u8) bool {
    for (axes, 0..) |axis, i| {
        if (axis != i) return false;
    }
    return true;
}

fn sharedLinearDotOutputShape(
    allocator: std.mem.Allocator,
    lhs_shape: []const i64,
    lhs_contract_axis: usize,
    out_dim: i64,
) ![]i64 {
    const output_shape = try allocator.alloc(i64, lhs_shape.len);
    errdefer allocator.free(output_shape);
    if (lhs_contract_axis > 0) @memcpy(output_shape[0..lhs_contract_axis], lhs_shape[0..lhs_contract_axis]);
    output_shape[lhs_contract_axis] = out_dim;
    return output_shape;
}

fn tryDeviceRank2DotGeneral(
    ctx: *anyopaque,
    lhs: CT,
    rhs: CT,
    lhs_shape: []const i64,
    rhs_shape: []const i64,
    lhs_contracting: []const u8,
    rhs_contracting: []const u8,
    lhs_batch: []const u8,
    rhs_batch: []const u8,
) anyerror!?CT {
    if (lhs_batch.len != 0 or rhs_batch.len != 0) return null;
    if (lhs_contracting.len != 1 or rhs_contracting.len != 1) return null;
    if (lhs_shape.len != 2 or rhs_shape.len != 2) return null;
    const lhs_contract_axis: usize = lhs_contracting[0];
    const rhs_contract_axis: usize = rhs_contracting[0];
    if (lhs_contract_axis > 1 or rhs_contract_axis > 1) return null;

    const lhs_k = lhs_shape[lhs_contract_axis];
    const rhs_k = rhs_shape[rhs_contract_axis];
    const lhs_out_axis: usize = 1 - lhs_contract_axis;
    const rhs_out_axis: usize = 1 - rhs_contract_axis;
    const rows_i64 = lhs_shape[lhs_out_axis];
    const out_dim_i64 = rhs_shape[rhs_out_axis];
    if (lhs_k <= 0 or rhs_k <= 0 or lhs_k != rhs_k or rows_i64 <= 0 or out_dim_i64 <= 0) return null;

    var lhs_linear = lhs;
    var lhs_tmp: ?CT = null;
    defer if (lhs_tmp) |tmp| freeTensor(ctx, tmp);
    if (lhs_contract_axis == 0) {
        lhs_tmp = try deviceTranspose2D(ctx, lhs, @intCast(lhs_shape[0]), @intCast(lhs_shape[1]));
        lhs_linear = lhs_tmp.?;
    }

    var rhs_linear = rhs;
    var rhs_tmp: ?CT = null;
    defer if (rhs_tmp) |tmp| freeTensor(ctx, tmp);
    if (rhs_contract_axis == 0) {
        rhs_tmp = try deviceTranspose2D(ctx, rhs, @intCast(rhs_shape[0]), @intCast(rhs_shape[1]));
        rhs_linear = rhs_tmp.?;
    }

    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const output_shape = try allocShape2(self.allocator, @intCast(rows_i64), @intCast(out_dim_i64));
    var output_shape_passed_to_linear = false;
    errdefer if (!output_shape_passed_to_linear) self.allocator.free(output_shape);
    output_shape_passed_to_linear = true;
    return try linearNoBiasWithShape(
        ctx,
        lhs_linear,
        rhs_linear,
        @intCast(rows_i64),
        @intCast(lhs_k),
        @intCast(out_dim_i64),
        output_shape,
    );
}

fn tryDeviceSharedLinearDotGeneral(
    ctx: *anyopaque,
    lhs: CT,
    rhs: CT,
    lhs_shape: []const i64,
    rhs_shape: []const i64,
    lhs_contracting: []const u8,
    rhs_contracting: []const u8,
    lhs_batch: []const u8,
    rhs_batch: []const u8,
) anyerror!?CT {
    if (lhs_contracting.len != 1 or rhs_contracting.len != 1) return null;
    if (rhs_batch.len != 0) return null;
    if (lhs_shape.len < 2 or rhs_shape.len != 2) return null;
    if (!axesArePrefix(lhs_batch)) return null;

    const lhs_contract_axis: usize = lhs_contracting[0];
    if (lhs_contract_axis != lhs_shape.len - 1) return null;
    if (lhs_batch.len > lhs_contract_axis) return null;
    const in_dim_i64 = lhs_shape[lhs_contract_axis];
    const rhs_contract_axis: usize = rhs_contracting[0];
    if (rhs_contract_axis > 1) return null;
    const rhs_in_dim_i64 = rhs_shape[rhs_contract_axis];
    const rhs_out_axis: usize = 1 - rhs_contract_axis;
    const out_dim_i64 = rhs_shape[rhs_out_axis];
    if (in_dim_i64 <= 0 or rhs_in_dim_i64 <= 0 or out_dim_i64 <= 0) return null;
    if (in_dim_i64 != rhs_in_dim_i64) return null;

    var rows: usize = 1;
    for (lhs_shape[0..lhs_contract_axis]) |dim| {
        if (dim <= 0) return null;
        rows = try checkedMul(rows, @intCast(dim));
    }

    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const output_shape = try sharedLinearDotOutputShape(self.allocator, lhs_shape, lhs_contract_axis, out_dim_i64);
    var output_shape_passed_to_linear = false;
    errdefer if (!output_shape_passed_to_linear) self.allocator.free(output_shape);
    if (rhs_contract_axis == 0) {
        const rhs_transposed = try deviceTranspose2D(ctx, rhs, @intCast(rhs_shape[0]), @intCast(rhs_shape[1]));
        defer freeTensor(ctx, rhs_transposed);
        output_shape_passed_to_linear = true;
        return try linearNoBiasWithShape(
            ctx,
            lhs,
            rhs_transposed,
            rows,
            @intCast(in_dim_i64),
            @intCast(out_dim_i64),
            output_shape,
        );
    }
    output_shape_passed_to_linear = true;
    return try linearNoBiasWithShape(
        ctx,
        lhs,
        rhs,
        rows,
        @intCast(in_dim_i64),
        @intCast(out_dim_i64),
        output_shape,
    );
}

fn tryDeviceBatchedMatmulDotGeneral(
    ctx: *anyopaque,
    lhs: CT,
    rhs: CT,
    lhs_shape: []const i64,
    rhs_shape: []const i64,
    lhs_contracting: []const u8,
    rhs_contracting: []const u8,
    lhs_batch: []const u8,
    rhs_batch: []const u8,
) anyerror!?CT {
    if (lhs_shape.len < 3 or rhs_shape.len != lhs_shape.len) return null;
    const rank = lhs_shape.len;
    if (lhs_contracting.len != 1 or rhs_contracting.len != 1) return null;
    if (lhs_contracting[0] != rank - 1) return null;
    const rhs_contract_axis: usize = rhs_contracting[0];
    if (rhs_contract_axis != rank - 1 and rhs_contract_axis != rank - 2) return null;
    if (lhs_batch.len != rank - 2 or rhs_batch.len != rank - 2) return null;
    if (!axesArePrefix(lhs_batch) or !axesArePrefix(rhs_batch)) return null;

    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    var batches: usize = 1;
    for (0..rank - 2) |idx| {
        if (lhs_shape[idx] <= 0 or rhs_shape[idx] <= 0 or lhs_shape[idx] != rhs_shape[idx]) return null;
        batches = try checkedMul(batches, @intCast(lhs_shape[idx]));
    }
    const m_i64 = lhs_shape[rank - 2];
    const k_i64 = lhs_shape[rank - 1];
    const rhs_out_axis: usize = if (rhs_contract_axis == rank - 1) rank - 2 else rank - 1;
    const rhs_k_i64 = rhs_shape[rhs_contract_axis];
    const n_i64 = rhs_shape[rhs_out_axis];
    if (m_i64 <= 0 or k_i64 <= 0 or rhs_k_i64 <= 0 or n_i64 <= 0 or k_i64 != rhs_k_i64) return null;

    const shape = try self.allocator.alloc(i64, rank);
    errdefer self.allocator.free(shape);
    for (0..rank - 2) |idx| shape[idx] = lhs_shape[idx];
    shape[rank - 2] = m_i64;
    shape[rank - 1] = n_i64;

    const lhs_tensor = tensorFromCt(lhs);
    var rhs_linear = rhs;
    var rhs_tmp: ?CT = null;
    defer if (rhs_tmp) |tmp| freeTensor(ctx, tmp);
    if (rhs_contract_axis == rank - 1) {
        if (rank > 8) return null;
        var perm_buf = [_]u8{0} ** 8;
        for (0..rank - 2) |idx| perm_buf[idx] = @intCast(idx);
        perm_buf[rank - 2] = @intCast(rank - 1);
        perm_buf[rank - 1] = @intCast(rank - 2);
        rhs_tmp = try deviceTransposeND(ctx, rhs, perm_buf[0..rank], rhs_shape);
        rhs_linear = rhs_tmp.?;
    }
    const rhs_tensor = tensorFromCt(rhs_linear);
    try ensureF32(lhs_tensor);
    try ensureF32(rhs_tensor);
    const m: usize = @intCast(m_i64);
    const k: usize = @intCast(k_i64);
    const n: usize = @intCast(n_i64);
    const out_count = try checkedMul(try checkedMul(batches, m), n);
    try ensureCount(lhs_tensor, try checkedMul(try checkedMul(batches, m), k));
    try ensureCount(rhs_tensor, try checkedMul(try checkedMul(batches, k), n));
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchBatchedMatmulF32(&self.ctx, device, lhs_tensor.buffer, rhs_tensor.buffer, batches, m, k, n);
    return createTensor(self, device, shape, out_count);
}

fn primDotGeneralOp(ctx: *anyopaque, lhs: CT, rhs: CT, lhs_shape: []const i64, rhs_shape: []const i64, lhs_contracting: []const u8, rhs_contracting: []const u8, lhs_batch: []const u8, rhs_batch: []const u8) anyerror!CT {
    if (try tryDeviceRank2DotGeneral(ctx, lhs, rhs, lhs_shape, rhs_shape, lhs_contracting, rhs_contracting, lhs_batch, rhs_batch)) |out| return out;
    if (try tryDeviceSharedLinearDotGeneral(ctx, lhs, rhs, lhs_shape, rhs_shape, lhs_contracting, rhs_contracting, lhs_batch, rhs_batch)) |out| return out;
    if (try tryDeviceBatchedMatmulDotGeneral(ctx, lhs, rhs, lhs_shape, rhs_shape, lhs_contracting, rhs_contracting, lhs_batch, rhs_batch)) |out| return out;
    var fallback: NativeFallback = undefined;
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    try fallback.init(self, "dot_general");
    defer fallback.deinit();
    const nl = try nativeCtFromCuda(self, &fallback, lhs);
    defer fallback.cb.free(nl);
    const nr = try nativeCtFromCuda(self, &fallback, rhs);
    defer fallback.cb.free(nr);
    const out = try fallback.cb.primDotGeneral(nl, nr, lhs_shape, rhs_shape, lhs_contracting, rhs_contracting, lhs_batch, rhs_batch);
    defer fallback.cb.free(out);
    return cudaCtFromNative(self, &fallback, out, tensorFromCt(lhs).shape);
}

fn isFullSlice(starts: []const i64, limits: []const i64, strides: []const i64, input_shape: []const i64) bool {
    if (starts.len != input_shape.len or limits.len != input_shape.len or strides.len != input_shape.len) return false;
    for (input_shape, 0..) |dim, i| {
        if (starts[i] != 0 or strides[i] != 1) return false;
        if (limits[i] != dim and !(limits[i] < 0 and dim >= 0)) return false;
    }
    return true;
}

fn contiguousLeadingSliceDevice(ctx: *anyopaque, input: CT, starts: []const i64, limits: []const i64, strides: []const i64, input_shape: []const i64) anyerror!?CT {
    if (input_shape.len == 0 or starts.len != input_shape.len or limits.len != input_shape.len or strides.len != input_shape.len) return null;
    if (input_shape[0] <= 0 or starts[0] < 0 or strides[0] != 1) return null;
    const dim0 = input_shape[0];
    const limit0 = if (limits[0] < 0) dim0 else limits[0];
    if (limit0 < starts[0] or limit0 > dim0) return null;

    var inner_count: usize = 1;
    for (input_shape[1..], 1..) |dim, axis| {
        if (dim <= 0) return null;
        if (starts[axis] != 0 or strides[axis] != 1) return null;
        if (limits[axis] != dim and !(limits[axis] < 0 and dim >= 0)) return null;
        inner_count = try checkedMul(inner_count, @intCast(dim));
    }

    const row_count: usize = @intCast(limit0 - starts[0]);
    const start_row: usize = @intCast(starts[0]);
    const cols = inner_count;
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);
    try ensureCount(input_tensor, try elementCountFromShape(input_shape));
    const output_shape = try dupeShape(self.allocator, input_shape);
    errdefer self.allocator.free(output_shape);
    output_shape[0] = @intCast(row_count);

    const out_count = try checkedMul(row_count, cols);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    const src_offset = try checkedMul(try checkedMul(start_row, cols), @sizeOf(f32));
    const byte_count = try checkedMul(out_count, @sizeOf(f32));
    try device.copyFromDeviceOffset(&self.ctx, 0, input_tensor.buffer, src_offset, byte_count);
    return createTensor(self, device, output_shape, out_count);
}

fn contiguousLastDimSliceDevice(ctx: *anyopaque, input: CT, starts: []const i64, limits: []const i64, strides: []const i64, input_shape: []const i64) anyerror!?CT {
    if (input_shape.len == 0 or starts.len != input_shape.len or limits.len != input_shape.len or strides.len != input_shape.len) return null;
    const last_axis = input_shape.len - 1;
    for (input_shape[0..last_axis], 0..) |dim, axis| {
        if (dim <= 0) return null;
        if (starts[axis] != 0 or strides[axis] != 1) return null;
        if (limits[axis] != dim and !(limits[axis] < 0 and dim >= 0)) return null;
    }
    if (starts[last_axis] < 0 or strides[last_axis] != 1) return null;
    const last_dim = input_shape[last_axis];
    if (last_dim <= 0) return null;
    const limit = if (limits[last_axis] < 0) last_dim else limits[last_axis];
    if (limit < starts[last_axis] or limit > last_dim) return null;
    return try copyLastDimSliceDevice(ctx, input, input_shape, @intCast(starts[last_axis]), @intCast(limit));
}

fn primSliceOp(ctx: *anyopaque, input: CT, starts: []const i64, limits: []const i64, strides: []const i64, input_shape: []const i64) anyerror!CT {
    if (isFullSlice(starts, limits, strides, input_shape)) return deviceCopyWithShape(ctx, input, input_shape);
    if (try contiguousLeadingSliceDevice(ctx, input, starts, limits, strides, input_shape)) |out| return out;
    if (try contiguousLastDimSliceDevice(ctx, input, starts, limits, strides, input_shape)) |out| return out;
    var fallback: NativeFallback = undefined;
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    try fallback.init(self, "slice");
    defer fallback.deinit();
    const ni = try nativeCtFromCuda(self, &fallback, input);
    defer fallback.cb.free(ni);
    const out = try fallback.cb.primSlice(ni, starts, limits, strides, input_shape);
    defer fallback.cb.free(out);
    return cudaCtFromNative(self, &fallback, out, input_shape);
}

fn primConcatPrimOp(ctx: *anyopaque, a: CT, b: CT, axis: u8, a_shape: []const i64, b_shape: []const i64) anyerror!CT {
    if (a_shape.len == b_shape.len and a_shape.len > 0 and axis == a_shape.len - 1) {
        var total: usize = 1;
        var prefix_match = true;
        for (a_shape[0 .. a_shape.len - 1], 0..) |dim, i| {
            if (dim != b_shape[i]) {
                prefix_match = false;
                break;
            }
            total = try checkedMul(total, @intCast(dim));
        }
        if (prefix_match) {
            return concat(ctx, a, b, total, @intCast(a_shape[a_shape.len - 1]), @intCast(b_shape[b_shape.len - 1]));
        }
    }
    if (a_shape.len == 2 and b_shape.len == 2 and axis == 0 and a_shape[1] == b_shape[1]) {
        if (a_shape[0] < 0 or b_shape[0] < 0 or a_shape[1] < 0) return error.InvalidShape;
        return concatRows2D(ctx, a, b, @intCast(a_shape[0]), @intCast(b_shape[0]), @intCast(a_shape[1]));
    }
    var fallback: NativeFallback = undefined;
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    try fallback.init(self, "concat_prim");
    defer fallback.deinit();
    const na = try nativeCtFromCuda(self, &fallback, a);
    defer fallback.cb.free(na);
    const nb = try nativeCtFromCuda(self, &fallback, b);
    defer fallback.cb.free(nb);
    const out = try fallback.cb.primConcatPrim(na, nb, axis, a_shape, b_shape);
    defer fallback.cb.free(out);
    return cudaCtFromNative(self, &fallback, out, a_shape);
}

fn primSoftmaxOp(ctx: *anyopaque, input: CT, dim: u32) anyerror!CT {
    return softmaxLastDimDevice(ctx, input, dim, false);
}

fn primLogSoftmaxOp(ctx: *anyopaque, input: CT, dim: u32) anyerror!CT {
    return softmaxLastDimDevice(ctx, input, dim, true);
}

fn softmaxLastDimDevice(ctx: *anyopaque, input: CT, dim: u32, log_softmax: bool) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const t = tensorFromCt(input);
    try ensureF32(t);
    const last_dim: usize = @intCast(dim);
    if (last_dim == 0 or t.elem_count % last_dim != 0) return error.InvalidShape;
    const shape = try dupeShape(self.allocator, t.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, t.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchSoftmaxLastDimF32(&self.ctx, device, t.buffer, t.elem_count / last_dim, last_dim, log_softmax);
    return createTensor(self, device, shape, t.elem_count);
}

fn argMaxOutputShape(allocator: std.mem.Allocator, input_shape: []const i64, axis: usize, keepdims: bool) ![]i64 {
    if (axis >= input_shape.len or input_shape.len > 8) return error.InvalidShape;
    const out_rank = if (keepdims) input_shape.len else input_shape.len - 1;
    const out_shape = try allocator.alloc(i64, out_rank);
    errdefer allocator.free(out_shape);
    if (keepdims) {
        @memcpy(out_shape, input_shape);
        out_shape[axis] = 1;
    } else {
        var out_i: usize = 0;
        for (input_shape, 0..) |dim, i| {
            if (i == axis) continue;
            out_shape[out_i] = dim;
            out_i += 1;
        }
    }
    return out_shape;
}

fn argMaxLastDimDevice(ctx: *anyopaque, input: CT, axis: u8, keepdims: bool, input_shape: []const i64) anyerror!?CT {
    if (input_shape.len == 0 or axis != input_shape.len - 1) return null;
    const last_dim_i64 = input_shape[input_shape.len - 1];
    if (last_dim_i64 <= 0) return null;
    var rows: usize = 1;
    for (input_shape[0 .. input_shape.len - 1]) |dim| {
        if (dim <= 0) return null;
        rows = try checkedMul(rows, @intCast(dim));
    }
    const dim: usize = @intCast(last_dim_i64);
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);
    try ensureCount(input_tensor, try checkedMul(rows, dim));

    const output_shape = try argMaxOutputShape(self.allocator, input_shape, input_shape.len - 1, keepdims);
    errdefer self.allocator.free(output_shape);
    var device = try allocDeviceBuffer(self, rows * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchArgMaxLastDimF32(&self.ctx, device, input_tensor.buffer, rows, dim);
    return createTensor(self, device, output_shape, rows);
}

fn primArgMaxOp(ctx: *anyopaque, input: CT, axis: u8, keepdims: bool, input_shape: []const i64) anyerror!CT {
    if (try argMaxLastDimDevice(ctx, input, axis, keepdims, input_shape)) |out| return out;
    var fallback: NativeFallback = undefined;
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    try fallback.init(self, "argmax");
    defer fallback.deinit();
    const ni = try nativeCtFromCuda(self, &fallback, input);
    defer fallback.cb.free(ni);
    const out = try fallback.cb.primArgMax(ni, axis, keepdims, input_shape);
    defer fallback.cb.free(out);
    return cudaCtFromNative(self, &fallback, out, input_shape);
}

fn scatterAddRowsDevice(ctx: *anyopaque, input: CT, indices: CT, input_shape: []const i64, indices_shape: []const i64, axis: u8) anyerror!?CT {
    if (axis != 0 or input_shape.len != 2) return null;
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const indices_tensor = tensorFromCt(indices);
    try ensureF32(input_tensor);
    try ensureF32(indices_tensor);
    if (input_shape[0] < 0 or input_shape[1] <= 0) return null;

    const rows: usize = @intCast(input_shape[0]);
    const dim: usize = @intCast(input_shape[1]);
    try ensureCount(input_tensor, try checkedMul(rows, dim));
    if (indices_tensor.elem_count < rows) return error.ShapeMismatch;

    const index_values = try downloadAlloc(self, indices_tensor);
    defer self.allocator.free(index_values);

    var out_rows: usize = 0;
    if (indices_shape.len > 0) {
        if (indices_shape[0] <= 0) return null;
        out_rows = @intCast(indices_shape[0]);
    } else {
        if (index_values.len == 0) return null;
        for (index_values) |value| {
            if (value < 0) return error.IndexOutOfBounds;
            const row: usize = @intFromFloat(value);
            out_rows = @max(out_rows, try checkedAdd(row, 1));
        }
    }

    const row_ids = try self.allocator.alloc(u32, rows);
    defer self.allocator.free(row_ids);
    for (row_ids, 0..) |*row_id, i| {
        const value = index_values[i];
        if (value < 0) return error.IndexOutOfBounds;
        const row: usize = @intFromFloat(value);
        if (row >= out_rows or row > std.math.maxInt(u32)) return error.IndexOutOfBounds;
        row_id.* = @intCast(row);
    }

    const output_shape = try allocShape2(self.allocator, out_rows, dim);
    errdefer self.allocator.free(output_shape);
    const out_count = try checkedMul(out_rows, dim);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchFillF32(&self.ctx, device, out_count, 0.0);
    const row_ids_device = try uploadTempU32(self, row_ids);
    try self.kernels.launchScatterAddRowsF32(&self.ctx, device, input_tensor.buffer, row_ids_device, out_rows, rows, dim);
    return createTensor(self, device, output_shape, out_count);
}

fn scatterAddIntoRowsDevice(ctx: *anyopaque, dest: CT, values: CT, indices: CT, dest_shape: []const i64, values_shape: []const i64, axis: u8) anyerror!?CT {
    if (axis != 0 or dest_shape.len != 2 or values_shape.len != 2) return null;
    if (dest_shape[0] < 0 or dest_shape[1] <= 0 or values_shape[0] < 0 or values_shape[1] != dest_shape[1]) return null;

    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const dest_tensor = tensorFromCt(dest);
    const values_tensor = tensorFromCt(values);
    const indices_tensor = tensorFromCt(indices);
    try ensureF32(dest_tensor);
    try ensureF32(values_tensor);
    try ensureF32(indices_tensor);

    const out_rows: usize = @intCast(dest_shape[0]);
    const rows: usize = @intCast(values_shape[0]);
    const dim: usize = @intCast(dest_shape[1]);
    const out_count = try checkedMul(out_rows, dim);
    try ensureCount(dest_tensor, out_count);
    try ensureCount(values_tensor, try checkedMul(rows, dim));
    if (indices_tensor.elem_count < rows) return error.ShapeMismatch;

    const index_values = try downloadAlloc(self, indices_tensor);
    defer self.allocator.free(index_values);

    const row_ids = try self.allocator.alloc(u32, rows);
    defer self.allocator.free(row_ids);
    for (row_ids, 0..) |*row_id, i| {
        const value = index_values[i];
        if (value < 0) return error.IndexOutOfBounds;
        const row: usize = @intFromFloat(value);
        if (row >= out_rows or row > std.math.maxInt(u32)) return error.IndexOutOfBounds;
        row_id.* = @intCast(row);
    }

    const output_shape = try dupeShape(self.allocator, dest_shape);
    errdefer self.allocator.free(output_shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try device.copyFromDevice(&self.ctx, dest_tensor.buffer, out_count * @sizeOf(f32));
    const row_ids_device = try uploadTempU32(self, row_ids);
    try self.kernels.launchScatterAddRowsF32(&self.ctx, device, values_tensor.buffer, row_ids_device, out_rows, rows, dim);
    return createTensor(self, device, output_shape, out_count);
}

fn primScatterAddOp(ctx: *anyopaque, input: CT, indices: CT, input_shape: []const i64, indices_shape: []const i64, axis: u8) anyerror!CT {
    if (try scatterAddRowsDevice(ctx, input, indices, input_shape, indices_shape, axis)) |out| return out;
    var fallback: NativeFallback = undefined;
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    try fallback.init(self, "scatter_add");
    defer fallback.deinit();
    const ni = try nativeCtFromCuda(self, &fallback, input);
    defer fallback.cb.free(ni);
    const nidx = try nativeCtFromCuda(self, &fallback, indices);
    defer fallback.cb.free(nidx);
    const out = try fallback.cb.primScatterAdd(ni, nidx, input_shape, indices_shape, axis);
    defer fallback.cb.free(out);
    return cudaCtFromNative(self, &fallback, out, input_shape);
}

fn primScatterAddIntoOp(ctx: *anyopaque, dest: CT, values: CT, indices: CT, dest_shape: []const i64, values_shape: []const i64, indices_shape: []const i64, axis: u8) anyerror!CT {
    _ = indices_shape;
    if (try scatterAddIntoRowsDevice(ctx, dest, values, indices, dest_shape, values_shape, axis)) |out| return out;
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (!self.allow_host_training_fallbacks) {
        std.debug.print("error: CUDA host training fallback disabled for scatter_add_into\n", .{});
        return error.CudaHostTrainingFallbackDisabled;
    }
    return error.UnsupportedPrimitiveOp;
}

fn allDimsAreOne(shape: []const i64) bool {
    for (shape) |dim| {
        if (dim != 1) return false;
    }
    return true;
}

fn gatherOutputShape(allocator: std.mem.Allocator, input_shape: []const i64, axis_index: usize, indices_shape: []const i64, index_count: usize) ![]i64 {
    if (axis_index >= input_shape.len or input_shape.len > 8 or indices_shape.len > 8) return error.InvalidShape;

    const scalar_index = indices_shape.len == 0 and index_count == 1;
    const normalized_indices_len: usize = if (scalar_index)
        0
    else if (indices_shape.len == 0)
        1
    else if (indices_shape.len > 1 and index_count == 1 and allDimsAreOne(indices_shape))
        1
    else
        indices_shape.len;

    const out_rank = input_shape.len - 1 + normalized_indices_len;
    if (out_rank > 8) return error.InvalidShape;

    const out_shape = try allocator.alloc(i64, out_rank);
    errdefer allocator.free(out_shape);
    var out_i: usize = 0;
    for (input_shape[0..axis_index]) |dim| {
        out_shape[out_i] = dim;
        out_i += 1;
    }
    if (!scalar_index) {
        if (indices_shape.len == 0) {
            out_shape[out_i] = @intCast(index_count);
            out_i += 1;
        } else if (normalized_indices_len == 1 and indices_shape.len != 1) {
            out_shape[out_i] = 1;
            out_i += 1;
        } else {
            @memcpy(out_shape[out_i..][0..indices_shape.len], indices_shape);
            out_i += indices_shape.len;
        }
    }
    for (input_shape[axis_index + 1 ..]) |dim| {
        out_shape[out_i] = dim;
        out_i += 1;
    }
    return out_shape;
}

fn gatherWithHostIndicesDevice(ctx: *anyopaque, input: CT, indices: CT, axis: u8, input_shape: []const i64) anyerror!?CT {
    const axis_index: usize = axis;
    if (input_shape.len == 0 or axis_index >= input_shape.len or input_shape.len > 8) return null;

    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const indices_tensor = tensorFromCt(indices);
    try ensureF32(input_tensor);
    try ensureF32(indices_tensor);
    if (indices_tensor.shape.len > 8) return error.InvalidShape;

    var prefix_count: usize = 1;
    for (input_shape[0..axis_index]) |dim| {
        if (dim <= 0) return null;
        prefix_count = try checkedMul(prefix_count, @intCast(dim));
    }

    const axis_extent_i64 = input_shape[axis_index];
    if (axis_extent_i64 <= 0) return null;
    const axis_extent: usize = @intCast(axis_extent_i64);

    var suffix_size: usize = 1;
    for (input_shape[axis_index + 1 ..]) |dim| {
        if (dim <= 0) return null;
        suffix_size = try checkedMul(suffix_size, @intCast(dim));
    }

    try ensureCount(input_tensor, try checkedMul(try checkedMul(prefix_count, axis_extent), suffix_size));

    const index_count = indices_tensor.elem_count;
    try ensureCount(indices_tensor, try elementCountFromShape(indices_tensor.shape));
    const out_count = try checkedMul(try checkedMul(prefix_count, index_count), suffix_size);
    const output_shape = try gatherOutputShape(self.allocator, input_shape, axis_index, indices_tensor.shape, index_count);
    errdefer self.allocator.free(output_shape);

    const index_values = try downloadAlloc(self, indices_tensor);
    defer self.allocator.free(index_values);

    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    const slice_bytes = try checkedMul(suffix_size, @sizeOf(f32));
    for (0..prefix_count) |prefix_idx| {
        for (index_values, 0..) |value, idx_pos| {
            var gather_index = @as(i64, @intFromFloat(value));
            if (gather_index < 0) gather_index += @as(i64, @intCast(axis_extent));
            if (gather_index < 0 or gather_index >= @as(i64, @intCast(axis_extent))) return error.IndexOutOfBounds;
            const gather_pos: usize = @intCast(gather_index);
            const src_elem = try checkedMul(try checkedAdd(try checkedMul(prefix_idx, axis_extent), gather_pos), suffix_size);
            const dst_elem = try checkedMul(try checkedAdd(try checkedMul(prefix_idx, index_count), idx_pos), suffix_size);
            try device.copyFromDeviceOffset(
                &self.ctx,
                try checkedMul(dst_elem, @sizeOf(f32)),
                input_tensor.buffer,
                try checkedMul(src_elem, @sizeOf(f32)),
                slice_bytes,
            );
        }
    }
    return createTensor(self, device, output_shape, out_count);
}

fn gatherAxis0RowsDevice(ctx: *anyopaque, input: CT, indices: CT, input_shape: []const i64) anyerror!?CT {
    if (input_shape.len != 2) return null;
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const indices_tensor = tensorFromCt(indices);
    try ensureF32(input_tensor);
    try ensureF32(indices_tensor);
    if (input_shape[0] <= 0 or input_shape[1] <= 0) return null;

    const source_rows: usize = @intCast(input_shape[0]);
    const dim: usize = @intCast(input_shape[1]);
    try ensureCount(input_tensor, try checkedMul(source_rows, dim));

    const index_count = indices_tensor.elem_count;
    const index_values = try downloadAlloc(self, indices_tensor);
    defer self.allocator.free(index_values);
    const row_ids = try self.allocator.alloc(u32, index_count);
    defer self.allocator.free(row_ids);
    for (index_values, 0..) |value, i| {
        var row_i64 = @as(i64, @intFromFloat(value));
        if (row_i64 < 0) row_i64 += @as(i64, @intCast(source_rows));
        if (row_i64 < 0 or row_i64 >= @as(i64, @intCast(source_rows))) return error.IndexOutOfBounds;
        row_ids[i] = @intCast(row_i64);
    }

    const out_count = try checkedMul(index_count, dim);
    const shape = try gatherOutputShape(self.allocator, input_shape, 0, indices_tensor.shape, index_count);
    errdefer self.allocator.free(shape);

    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    const row_ids_device = try uploadTempU32(self, row_ids);
    try self.kernels.launchTakeRowsF32(&self.ctx, device, input_tensor.buffer, row_ids_device, source_rows, index_count, dim);
    return createTensor(self, device, shape, out_count);
}

fn primGatherOp(ctx: *anyopaque, input: CT, indices: CT, axis: u8, input_shape: []const i64) anyerror!CT {
    if (axis == 0) {
        if (try gatherAxis0RowsDevice(ctx, input, indices, input_shape)) |out| return out;
    }
    if (try gatherWithHostIndicesDevice(ctx, input, indices, axis, input_shape)) |out| return out;
    var fallback: NativeFallback = undefined;
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    try fallback.init(self, "gather");
    defer fallback.deinit();
    const ni = try nativeCtFromCuda(self, &fallback, input);
    defer fallback.cb.free(ni);
    const nidx = try nativeCtFromCuda(self, &fallback, indices);
    defer fallback.cb.free(nidx);
    const out = try fallback.cb.primGather(ni, nidx, axis, input_shape);
    defer fallback.cb.free(out);
    return cudaCtFromNative(self, &fallback, out, input_shape);
}

fn sdpaLaunch(ctx: *anyopaque, q_ct: CT, k_ct: CT, v_ct: CT, mask: ?[]const i64, attn_bias_ct: ?CT, batch: usize, seq_len: usize, num_heads: usize, head_dim: usize) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const q_tensor = tensorFromCt(q_ct);
    const k_tensor = tensorFromCt(k_ct);
    const v_tensor = tensorFromCt(v_ct);
    try ensureF32(q_tensor);
    try ensureF32(k_tensor);
    try ensureF32(v_tensor);
    const hidden = try checkedMul(num_heads, head_dim);
    const count = try checkedMul(try checkedMul(batch, seq_len), hidden);
    try ensureCount(q_tensor, count);
    try ensureCount(k_tensor, count);
    try ensureCount(v_tensor, count);
    const token_count = try checkedMul(batch, seq_len);
    const has_mask = mask != null;
    if (mask) |mask_values| {
        if (mask_values.len < token_count) return error.InvalidShape;
    }

    const mask_device = if (mask) |mask_values| try uploadTempI64(self, mask_values) else buffer_mod.DeviceBuffer{};
    const bias_tensor: ?*CudaTensor = if (attn_bias_ct) |bct| tensorFromCt(bct) else null;
    const bias_buffer = if (bias_tensor) |bt| bt.buffer else buffer_mod.DeviceBuffer{};
    const bias_mode: u32 = if (bias_tensor) |bt| blk: {
        const shared = try checkedMul(num_heads, try checkedMul(seq_len, seq_len));
        const batched = try checkedMul(batch, shared);
        break :blk if (bt.elem_count == batched) 2 else if (bt.elem_count == shared) 1 else return error.InvalidShape;
    } else 0;

    const shape = try dupeShape(self.allocator, q_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchAttentionF32(&self.ctx, device, q_tensor.buffer, k_tensor.buffer, v_tensor.buffer, mask_device, bias_buffer, batch, seq_len, num_heads, head_dim, false, has_mask, bias_mode, true);
    return createTensor(self, device, shape, count);
}

fn sdpa(ctx: *anyopaque, q_ct: CT, k_ct: CT, v_ct: CT, mask: []const i64, attn_bias_ct: ?CT, batch: usize, seq_len: usize, num_heads: usize, head_dim: usize) anyerror!CT {
    return sdpaLaunch(ctx, q_ct, k_ct, v_ct, mask, attn_bias_ct, batch, seq_len, num_heads, head_dim);
}

fn sdpaFull(ctx: *anyopaque, q_ct: CT, k_ct: CT, v_ct: CT, attn_bias_ct: ?CT, batch: usize, seq_len: usize, num_heads: usize, head_dim: usize) anyerror!?CT {
    return try sdpaLaunch(ctx, q_ct, k_ct, v_ct, null, attn_bias_ct, batch, seq_len, num_heads, head_dim);
}

fn causalSelfAttention(ctx: *anyopaque, q_ct: CT, k_ct: CT, v_ct: CT, attn_bias_ct: ?CT, batch: usize, seq_len: usize, num_heads: usize, head_dim: usize) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const q_tensor = tensorFromCt(q_ct);
    const k_tensor = tensorFromCt(k_ct);
    const v_tensor = tensorFromCt(v_ct);
    try ensureF32(q_tensor);
    try ensureF32(k_tensor);
    try ensureF32(v_tensor);
    const hidden = try checkedMul(num_heads, head_dim);
    const count = try checkedMul(try checkedMul(batch, seq_len), hidden);
    try ensureCount(q_tensor, count);
    try ensureCount(k_tensor, count);
    try ensureCount(v_tensor, count);
    const bias_tensor: ?*CudaTensor = if (attn_bias_ct) |bct| tensorFromCt(bct) else null;
    const bias_buffer = if (bias_tensor) |bt| bt.buffer else buffer_mod.DeviceBuffer{};
    const bias_mode: u32 = if (bias_tensor) |bt| blk: {
        const shared = try checkedMul(num_heads, try checkedMul(seq_len, seq_len));
        const batched = try checkedMul(batch, shared);
        break :blk if (bt.elem_count == batched) 2 else if (bt.elem_count == shared) 1 else return error.InvalidShape;
    } else 0;

    const shape = try dupeShape(self.allocator, q_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchAttentionF32(&self.ctx, device, q_tensor.buffer, k_tensor.buffer, v_tensor.buffer, .{}, bias_buffer, batch, seq_len, num_heads, head_dim, true, false, bias_mode, false);
    return createTensor(self, device, shape, count);
}
fn crossAttention(_: *anyopaque, _: CT, _: CT, _: CT, _: []const i64, _: usize, _: usize, _: usize, _: usize, _: usize) anyerror!CT {
    return unsupportedOp("cross_attention");
}
fn relativePositionBias(_: *anyopaque, _: CT, _: usize, _: usize, _: usize, _: usize, _: usize, _: bool) anyerror!CT {
    return unsupportedOp("relative_position_bias");
}
fn debertaDisentangledAttention(ctx: *anyopaque, q_ct: CT, k_ct: CT, v_ct: CT, q_r_ct: CT, k_r_ct: CT, mask: []const i64, batch: usize, seq_len: usize, num_heads: usize, head_dim: usize) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const q_tensor = tensorFromCt(q_ct);
    const k_tensor = tensorFromCt(k_ct);
    const v_tensor = tensorFromCt(v_ct);
    const q_r_tensor = tensorFromCt(q_r_ct);
    const k_r_tensor = tensorFromCt(k_r_ct);
    try ensureF32(q_tensor);
    try ensureF32(k_tensor);
    try ensureF32(v_tensor);
    try ensureF32(q_r_tensor);
    try ensureF32(k_r_tensor);
    if (seq_len == 0) return error.InvalidShape;
    const hidden = try checkedMul(num_heads, head_dim);
    const count = try checkedMul(try checkedMul(batch, seq_len), hidden);
    const rel_positions = try checkedSub(try checkedMul(2, seq_len), 1);
    const rel_count = try checkedMul(rel_positions, hidden);
    try ensureCount(q_tensor, count);
    try ensureCount(k_tensor, count);
    try ensureCount(v_tensor, count);
    try ensureCount(q_r_tensor, rel_count);
    try ensureCount(k_r_tensor, rel_count);
    if (mask.len < try checkedMul(batch, seq_len)) return error.InvalidShape;

    const mask_device = try uploadTempI64(self, mask);
    const shape = try dupeShape(self.allocator, q_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchDebertaAttentionF32(&self.ctx, device, q_tensor.buffer, k_tensor.buffer, v_tensor.buffer, q_r_tensor.buffer, k_r_tensor.buffer, mask_device, batch, seq_len, num_heads, head_dim);
    return createTensor(self, device, shape, count);
}
fn windowedSelfAttention(
    _: *anyopaque,
    _: CT,
    _: CT,
    _: CT,
    _: CT,
    _: CT,
    _: CT,
    _: CT,
    _: usize,
    _: usize,
    _: usize,
    _: usize,
    _: usize,
    _: usize,
) anyerror!CT {
    return unsupportedOp("windowed_self_attention");
}
fn channelSelfAttention(
    _: *anyopaque,
    _: CT,
    _: CT,
    _: CT,
    _: CT,
    _: CT,
    _: CT,
    _: CT,
    _: usize,
    _: usize,
    _: usize,
    _: usize,
) anyerror!CT {
    return unsupportedOp("channel_self_attention");
}
fn tokenGridConv2d(
    _: *anyopaque,
    _: CT,
    _: CT,
    _: CT,
    _: usize,
    _: usize,
    _: usize,
    _: usize,
    _: usize,
    _: usize,
    _: usize,
    _: usize,
    _: usize,
    _: usize,
    _: usize,
    _: usize,
) anyerror!CT {
    return unsupportedOp("token_grid_conv2d");
}
fn conv1d(_: *anyopaque, _: CT, _: CT, _: CT, _: usize, _: usize, _: usize, _: usize, _: usize, _: usize, _: usize) anyerror!CT {
    return unsupportedOp("conv1d");
}
fn conv2d(
    ctx: *anyopaque,
    input: CT,
    weight: CT,
    bias: CT,
    batch: usize,
    in_channels: usize,
    out_channels: usize,
    height: usize,
    width: usize,
    kernel_h: usize,
    kernel_w: usize,
    stride_h: usize,
    stride_w: usize,
    padding_h: usize,
    padding_w: usize,
    groups: usize,
) anyerror!CT {
    if (groups == 0 or in_channels % groups != 0 or out_channels % groups != 0) return error.InvalidShape;
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_tensor = tensorFromCt(weight);
    const bias_tensor = tensorFromCt(bias);
    try ensureF32(input_tensor);
    try ensureF32(weight_tensor);
    try ensureF32(bias_tensor);
    const out_h = (height + 2 * padding_h - kernel_h) / stride_h + 1;
    const out_w = (width + 2 * padding_w - kernel_w) / stride_w + 1;
    const out_count = try checkedMul(try checkedMul(batch, out_channels), try checkedMul(out_h, out_w));
    try ensureCount(input_tensor, try checkedMul(try checkedMul(batch, in_channels), try checkedMul(height, width)));
    try ensureCount(weight_tensor, try checkedMul(try checkedMul(out_channels, in_channels / groups), try checkedMul(kernel_h, kernel_w)));
    try ensureCount(bias_tensor, out_channels);
    const shape = try self.allocator.dupe(i64, &.{ @as(i64, @intCast(batch)), @as(i64, @intCast(out_channels)), @as(i64, @intCast(out_h)), @as(i64, @intCast(out_w)) });
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchConv2dF32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, batch, in_channels, out_channels, height, width, kernel_h, kernel_w, stride_h, stride_w, padding_h, padding_w, groups, out_h, out_w);
    return createTensor(self, device, shape, out_count);
}
fn rope(ctx: *anyopaque, input: CT, seq_len: usize, head_dim: usize, rope_dim: usize, theta: f32, freq_scale: f32, position_offset: usize, consecutive_pairs: bool) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);
    if (seq_len == 0 or head_dim == 0 or rope_dim == 0 or rope_dim > head_dim or rope_dim % 2 != 0) return error.InvalidRoPEInput;
    if (input_tensor.elem_count % head_dim != 0) return error.InvalidRoPEInput;
    const total_chunks = input_tensor.elem_count / head_dim;
    if (total_chunks % seq_len != 0 or total_chunks / seq_len == 0) return error.InvalidRoPEInput;

    const shape = try dupeShape(self.allocator, input_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, input_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchRopeF32(&self.ctx, device, input_tensor.buffer, input_tensor.elem_count, seq_len, head_dim, rope_dim, theta, freq_scale, position_offset, consecutive_pairs);
    return createTensor(self, device, shape, input_tensor.elem_count);
}
fn ropePerItem(_: *anyopaque, _: CT, _: usize, _: usize, _: usize, _: usize, _: f32, _: f32, _: []const usize, _: []const usize, _: bool) anyerror!CT {
    return unsupportedOp("rope_per_item");
}

const GatheredPagedKv = struct {
    k: buffer_mod.DeviceBuffer,
    v: buffer_mod.DeviceBuffer,
};

fn pagedKvDeviceCacheEnabled() bool {
    return !platform.env.getenvBool("TERMITE_CUDA_DISABLE_DEVICE_KV");
}

fn cudaGqaGraphEnabled() bool {
    return platform.env.getenvBool("TERMITE_CUDA_GRAPH_GQA");
}

fn gqaGraphDebugEnabled() bool {
    return platform.env.getenvBool("TERMITE_CUDA_GRAPH_DEBUG");
}

fn getOrCreateGqaGraph(
    self: *CudaCompute,
    key: GqaGraphCacheKey,
    dst: buffer_mod.DeviceBuffer,
    q: buffer_mod.DeviceBuffer,
    k: buffer_mod.DeviceBuffer,
    v: buffer_mod.DeviceBuffer,
    attn_or_mask: buffer_mod.DeviceBuffer,
    bias: buffer_mod.DeviceBuffer,
    kv_seq_len: usize,
    total_sequence_len: usize,
    query_position_offset: usize,
    kv_position_offset: usize,
    sliding_window: usize,
) !*kernels_mod.KernelModule.GqaAttentionGraph {
    const gop = try self.gqa_graph_cache.getOrPut(self.allocator, key);
    if (!gop.found_existing) {
        if (gqaGraphDebugEnabled()) std.debug.print("cuda-gqa-graph capture batch={d} q={d} heads={d}/{d} dim={d} mask={} bias={d}\n", .{ key.batch, key.q_seq_len, key.num_heads, key.num_kv_heads, key.head_dim, key.has_attn_or_mask, key.bias_mode });
        gop.value_ptr.* = self.kernels.captureGqaAttentionF32Graph(
            &self.ctx,
            dst,
            q,
            k,
            v,
            attn_or_mask,
            bias,
            key.batch,
            key.q_seq_len,
            kv_seq_len,
            total_sequence_len,
            query_position_offset,
            kv_position_offset,
            key.num_heads,
            key.num_kv_heads,
            key.head_dim,
            sliding_window,
            key.has_attn_or_mask,
            key.bias_mode,
        ) catch |err| {
            _ = self.gqa_graph_cache.remove(key);
            return err;
        };
    }
    return gop.value_ptr;
}

fn deviceKvTokenCapacity(attention: ops.AttentionContext) usize {
    if (attention.kv_manager) |manager| {
        if (attention.kv_cache) |kv| {
            if (manager.getPoolMut(kv.pool_id)) |pool| {
                const block_count = if (kv.logical_blocks) |blocks| blocks.len else blk: {
                    const table = manager.blockTable(kv.sequence_id) orelse break :blk kv.logical_block_count;
                    break :blk table.blocks.items.len;
                };
                return @max(attention.kv_sequence_len, block_count * pool.config.page_size_tokens);
            }
        }
    }
    return attention.kv_sequence_len;
}

fn ensureDeviceKvCacheEntry(
    self: *CudaCompute,
    key: DeviceKvCacheKey,
    capacity_tokens: usize,
    kv_hidden: usize,
) !*DeviceKvCacheEntry {
    const gop = try self.device_kv_cache.getOrPut(self.allocator, key);
    if (!gop.found_existing) {
        gop.value_ptr.* = .{};
    }
    const entry = gop.value_ptr;
    if (entry.kv_hidden != 0 and entry.kv_hidden != kv_hidden) {
        entry.k.free(&self.ctx);
        entry.v.free(&self.ctx);
        entry.* = .{};
    }
    if (entry.capacity_tokens < capacity_tokens or entry.k.ptr == 0 or entry.v.ptr == 0) {
        const old_k = entry.k;
        const old_v = entry.v;
        var new_k = try buffer_mod.DeviceBuffer.alloc(&self.ctx, capacity_tokens * kv_hidden * @sizeOf(f32));
        errdefer new_k.free(&self.ctx);
        var new_v = try buffer_mod.DeviceBuffer.alloc(&self.ctx, capacity_tokens * kv_hidden * @sizeOf(f32));
        errdefer new_v.free(&self.ctx);
        if (entry.valid_tokens != 0 and old_k.ptr != 0 and old_v.ptr != 0) {
            const copy_bytes = entry.valid_tokens * kv_hidden * @sizeOf(f32);
            try new_k.copyFromDevice(&self.ctx, old_k, copy_bytes);
            try new_v.copyFromDevice(&self.ctx, old_v, copy_bytes);
        }
        var old_k_mut = old_k;
        var old_v_mut = old_v;
        old_k_mut.free(&self.ctx);
        old_v_mut.free(&self.ctx);
        entry.k = new_k;
        entry.v = new_v;
        entry.capacity_tokens = capacity_tokens;
        entry.kv_hidden = kv_hidden;
    }
    return entry;
}

fn tryDevicePagedKv(
    self: *CudaCompute,
    k_tensor: *CudaTensor,
    v_tensor: *CudaTensor,
    attention: ops.AttentionContext,
    batch: usize,
    kv_hidden: usize,
    kv_count: usize,
    suffix_kv_count: usize,
) !?struct { k: buffer_mod.DeviceBuffer, v: buffer_mod.DeviceBuffer } {
    if (!pagedKvDeviceCacheEnabled() or batch != 1) return null;
    const kv = attention.kv_cache orelse return null;
    const key: DeviceKvCacheKey = .{ .sequence_id = kv.sequence_id, .layer_index = attention.layer_index };
    const capacity_tokens = @max(attention.kv_sequence_len, deviceKvTokenCapacity(attention));
    var entry = try ensureDeviceKvCacheEntry(self, key, capacity_tokens, kv_hidden);

    if (k_tensor.elem_count == kv_count and v_tensor.elem_count == kv_count) {
        const bytes = kv_count * @sizeOf(f32);
        try entry.k.copyFromDevice(&self.ctx, k_tensor.buffer, bytes);
        try entry.v.copyFromDevice(&self.ctx, v_tensor.buffer, bytes);
        entry.valid_tokens = attention.kv_sequence_len;
        return .{ .k = entry.k, .v = entry.v };
    }

    if (k_tensor.elem_count != suffix_kv_count or v_tensor.elem_count != suffix_kv_count) return null;
    if (attention.query_sequence_len > attention.kv_sequence_len) return error.InvalidShape;
    const start_token = attention.kv_sequence_len - attention.query_sequence_len;
    if (entry.valid_tokens < start_token) return null;
    const suffix_bytes = suffix_kv_count * @sizeOf(f32);
    const dst_offset = start_token * kv_hidden * @sizeOf(f32);
    try entry.k.copyFromDeviceOffset(&self.ctx, dst_offset, k_tensor.buffer, 0, suffix_bytes);
    try entry.v.copyFromDeviceOffset(&self.ctx, dst_offset, v_tensor.buffer, 0, suffix_bytes);
    entry.valid_tokens = @max(entry.valid_tokens, attention.kv_sequence_len);
    return .{ .k = entry.k, .v = entry.v };
}

fn writePagedKvSuffixFromDevice(
    self: *CudaCompute,
    k_tensor: *CudaTensor,
    v_tensor: *CudaTensor,
    attention: ops.AttentionContext,
    kv_hidden: usize,
) !void {
    const kv = attention.kv_cache orelse return;
    if (attention.query_sequence_len == 0) return;
    const suffix_count = try checkedMul(attention.query_sequence_len, kv_hidden);
    try ensureCount(k_tensor, suffix_count);
    try ensureCount(v_tensor, suffix_count);

    const k_host = try self.allocator.alloc(f32, suffix_count);
    defer self.allocator.free(k_host);
    const v_host = try self.allocator.alloc(f32, suffix_count);
    defer self.allocator.free(v_host);
    try k_tensor.buffer.copyToHost(&self.ctx, std.mem.sliceAsBytes(k_host));
    try v_tensor.buffer.copyToHost(&self.ctx, std.mem.sliceAsBytes(v_host));
    try self.ctx.synchronize();

    if (attention.kv_manager) |manager| {
        try manager.writeLayerKvSuffix(kv.sequence_id, attention.layer_index, attention.kv_sequence_len, attention.query_sequence_len, k_host, v_host);
    } else if (attention.kv_storage) |storage| {
        try storage.writeLayerKvSuffix(kv.sequence_id, attention.layer_index, attention.kv_sequence_len, attention.query_sequence_len, k_host, v_host);
    } else {
        if (cudaGqaDebugEnabled()) std.debug.print("cuda unsupported op: gqa_paged_attention_cache_write\n", .{});
        return error.CudaOpUnsupported;
    }
}

fn gatherPagedKvToDevice(
    self: *CudaCompute,
    attention: ops.AttentionContext,
    num_kv_heads: usize,
    head_dim: usize,
) !GatheredPagedKv {
    const kv = attention.kv_cache orelse {
        if (cudaGqaDebugEnabled()) std.debug.print("cuda unsupported op: gqa_paged_attention_cache\n", .{});
        return error.CudaOpUnsupported;
    };
    const manager = attention.kv_manager orelse {
        if (cudaGqaDebugEnabled()) std.debug.print("cuda unsupported op: gqa_paged_attention_storage\n", .{});
        return error.CudaOpUnsupported;
    };
    const block_ids = if (kv.logical_blocks) |blocks|
        blocks
    else blk: {
        const table = manager.blockTable(kv.sequence_id) orelse return error.InvalidSequenceId;
        break :blk table.blocks.items;
    };
    const pool = manager.getPoolMut(kv.pool_id) orelse return error.InvalidPoolId;
    if (!pool.config.store_cpu_bytes) return error.KvBytesUnavailable;
    if (pool.config.num_kv_heads < num_kv_heads or pool.config.head_dim < head_dim) {
        if (cudaGqaDebugEnabled()) {
            std.debug.print(
                "cuda-gqa gather invalid pool layer={d} pool_heads={d} pool_dim={d} want_heads={d} want_dim={d} blocks={d} kv_len={d} page={d}\n",
                .{ attention.layer_index, pool.config.num_kv_heads, pool.config.head_dim, num_kv_heads, head_dim, block_ids.len, attention.kv_sequence_len, pool.config.page_size_tokens },
            );
        }
        return error.InvalidPagedKvState;
    }

    const kv_hidden = try checkedMul(num_kv_heads, head_dim);
    const kv_count = try checkedMul(attention.kv_sequence_len, kv_hidden);
    const k_host = try self.allocator.alloc(f32, kv_count);
    defer self.allocator.free(k_host);
    const v_host = try self.allocator.alloc(f32, kv_count);
    defer self.allocator.free(v_host);

    for (0..attention.kv_sequence_len) |token_idx| {
        const block_idx = token_idx / pool.config.page_size_tokens;
        const token_offset = token_idx % pool.config.page_size_tokens;
        if (block_idx >= block_ids.len) {
            if (cudaGqaDebugEnabled()) std.debug.print("cuda-gqa gather missing block token={d} block_idx={d} blocks={d}\n", .{ token_idx, block_idx, block_ids.len });
            return error.InvalidPagedKvState;
        }
        const row = try pool.readToken(block_ids[block_idx], attention.layer_index, token_offset);
        if (row.k.len < kv_hidden or row.v.len < kv_hidden) {
            if (cudaGqaDebugEnabled()) std.debug.print("cuda-gqa gather short row k={d} v={d} want={d}\n", .{ row.k.len, row.v.len, kv_hidden });
            return error.InvalidPagedKvState;
        }
        @memcpy(k_host[token_idx * kv_hidden ..][0..kv_hidden], row.k[0..kv_hidden]);
        @memcpy(v_host[token_idx * kv_hidden ..][0..kv_hidden], row.v[0..kv_hidden]);
    }

    var k_device = try allocDeviceBuffer(self, kv_count * @sizeOf(f32));
    errdefer k_device.free(&self.ctx);
    var v_device = try allocDeviceBuffer(self, kv_count * @sizeOf(f32));
    errdefer v_device.free(&self.ctx);
    try k_device.copyFromHost(&self.ctx, std.mem.sliceAsBytes(k_host));
    try v_device.copyFromHost(&self.ctx, std.mem.sliceAsBytes(v_host));
    try self.ctx.synchronize();
    return .{ .k = k_device, .v = v_device };
}

fn gqaAttentionLaunch(
    self: *CudaCompute,
    q_ct: CT,
    k_ct: CT,
    v_ct: CT,
    attn_bias_ct: ?CT,
    attention: ops.AttentionContext,
    batch: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
) anyerror!CT {
    if (num_kv_heads == 0 or num_heads % num_kv_heads != 0 or head_dim == 0) return error.InvalidShape;
    if (attention.total_sequence_len < attention.query_sequence_len) return error.InvalidShape;
    if (attention.attention_sink.hasMetadata()) return unsupportedOp("gqa_attention_sink");
    if (attention.kv_batch != null) return unsupportedOp("gqa_paged_attention_batch");

    const q_tensor = tensorFromCt(q_ct);
    const k_tensor = tensorFromCt(k_ct);
    const v_tensor = tensorFromCt(v_ct);
    try ensureF32(q_tensor);
    try ensureF32(k_tensor);
    try ensureF32(v_tensor);

    const q_hidden = try checkedMul(num_heads, head_dim);
    const kv_hidden = try checkedMul(num_kv_heads, head_dim);
    const q_count = try checkedMul(try checkedMul(batch, attention.query_sequence_len), q_hidden);
    const kv_count = try checkedMul(try checkedMul(batch, attention.kv_sequence_len), kv_hidden);
    const suffix_kv_count = try checkedMul(try checkedMul(batch, attention.query_sequence_len), kv_hidden);
    if (cudaGqaDebugEnabled()) {
        std.debug.print(
            "cuda-gqa mode={s} batch={d} q_len={d} kv_len={d} total={d} kv_pos={d} heads={d}/{d} dim={d} q_count={d}/{d} k_count={d}/{d} v_count={d}/{d} cache={} manager={} storage={} skip={}\n",
            .{
                @tagName(attention.mode),
                batch,
                attention.query_sequence_len,
                attention.kv_sequence_len,
                attention.total_sequence_len,
                attention.kv_position_offset,
                num_heads,
                num_kv_heads,
                head_dim,
                q_tensor.elem_count,
                q_count,
                k_tensor.elem_count,
                kv_count,
                v_tensor.elem_count,
                kv_count,
                attention.kv_cache != null,
                attention.kv_manager != null,
                attention.kv_storage != null,
                attention.skip_kv_write,
            },
        );
    }
    try ensureCount(q_tensor, q_count);
    if (k_tensor.elem_count != kv_count or v_tensor.elem_count != kv_count) {
        if (k_tensor.elem_count != suffix_kv_count or v_tensor.elem_count != suffix_kv_count or batch != 1) return error.InvalidShape;
    }

    const bias_tensor: ?*CudaTensor = if (attn_bias_ct) |bct| tensorFromCt(bct) else null;
    const bias_buffer = if (bias_tensor) |bt| bt.buffer else buffer_mod.DeviceBuffer{};
    const bias_mode: u32 = if (bias_tensor) |bt| blk: {
        const shared = try checkedMul(num_heads, try checkedMul(attention.query_sequence_len, attention.kv_sequence_len));
        const batched = try checkedMul(batch, shared);
        break :blk if (bt.elem_count == batched) 2 else if (bt.elem_count == shared) 1 else return error.InvalidShape;
    } else 0;

    const attn_or_mask_device = if (attention.attn_or_mask) |mask| blk: {
        const expected = try checkedMul(attention.total_sequence_len, attention.total_sequence_len);
        if (mask.len < expected) return error.InvalidShape;
        break :blk try uploadTempU8(self, mask[0..expected]);
    } else buffer_mod.DeviceBuffer{};

    const query_position_offset = attention.total_sequence_len - attention.query_sequence_len;
    var gathered_k = buffer_mod.DeviceBuffer{};
    var gathered_v = buffer_mod.DeviceBuffer{};
    defer releaseDeviceBuffer(self, &gathered_k);
    defer releaseDeviceBuffer(self, &gathered_v);
    const attention_k, const attention_v = blk: {
        if (try tryDevicePagedKv(self, k_tensor, v_tensor, attention, batch, kv_hidden, kv_count, suffix_kv_count)) |device_kv| {
            break :blk .{ device_kv.k, device_kv.v };
        }
        if (k_tensor.elem_count == kv_count and v_tensor.elem_count == kv_count) {
            break :blk .{ k_tensor.buffer, v_tensor.buffer };
        } else {
            if (!attention.skip_kv_write) {
                try writePagedKvSuffixFromDevice(self, k_tensor, v_tensor, attention, kv_hidden);
            }
            const gathered = try gatherPagedKvToDevice(self, attention, num_kv_heads, head_dim);
            gathered_k = gathered.k;
            gathered_v = gathered.v;
            break :blk .{ gathered_k, gathered_v };
        }
    };

    const shape = try dupeShape(self.allocator, q_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, q_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    if (cudaGqaGraphEnabled() and !self.gqa_graph_disabled_for_session) graph_path: {
        const graph_key: GqaGraphCacheKey = .{
            .batch = batch,
            .q_seq_len = attention.query_sequence_len,
            .num_heads = num_heads,
            .num_kv_heads = num_kv_heads,
            .head_dim = head_dim,
            .has_attn_or_mask = attention.attn_or_mask != null,
            .bias_mode = bias_mode,
        };
        const graph = getOrCreateGqaGraph(
            self,
            graph_key,
            device,
            q_tensor.buffer,
            attention_k,
            attention_v,
            attn_or_mask_device,
            bias_buffer,
            attention.kv_sequence_len,
            attention.total_sequence_len,
            query_position_offset,
            attention.kv_position_offset,
            attention.sliding_window,
        ) catch |err| {
            if (gqaGraphDebugEnabled()) std.debug.print("cuda-gqa-graph disabled after capture failure: {s}\n", .{@errorName(err)});
            self.gqa_graph_disabled_for_session = true;
            break :graph_path;
        };
        self.kernels.launchGqaAttentionF32Captured(
            &self.ctx,
            graph,
            device,
            q_tensor.buffer,
            attention_k,
            attention_v,
            attn_or_mask_device,
            bias_buffer,
            batch,
            attention.query_sequence_len,
            attention.kv_sequence_len,
            attention.total_sequence_len,
            query_position_offset,
            attention.kv_position_offset,
            num_heads,
            num_kv_heads,
            head_dim,
            attention.sliding_window,
            attention.attn_or_mask != null,
            bias_mode,
        ) catch |err| {
            if (gqaGraphDebugEnabled()) std.debug.print("cuda-gqa-graph disabled after replay failure: {s}\n", .{@errorName(err)});
            self.gqa_graph_disabled_for_session = true;
            break :graph_path;
        };
        return createTensor(self, device, shape, q_count);
    }
    try self.kernels.launchGqaAttentionF32(
        &self.ctx,
        device,
        q_tensor.buffer,
        attention_k,
        attention_v,
        attn_or_mask_device,
        bias_buffer,
        batch,
        attention.query_sequence_len,
        attention.kv_sequence_len,
        attention.total_sequence_len,
        query_position_offset,
        attention.kv_position_offset,
        num_heads,
        num_kv_heads,
        head_dim,
        attention.sliding_window,
        attention.attn_or_mask != null,
        bias_mode,
    );
    return createTensor(self, device, shape, q_count);
}

fn gqaCausalAttention(ctx: *anyopaque, q_ct: CT, k_ct: CT, v_ct: CT, attn_bias_ct: ?CT, batch: usize, seq_len: usize, num_heads: usize, num_kv_heads: usize, head_dim: usize) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const attention: ops.AttentionContext = .{
        .mode = .dense_causal,
        .total_sequence_len = seq_len,
        .query_sequence_len = seq_len,
        .kv_sequence_len = seq_len,
    };
    return gqaAttentionLaunch(self, q_ct, k_ct, v_ct, attn_bias_ct, attention, batch, num_heads, num_kv_heads, head_dim);
}

fn gqaPagedAttention(ctx: *anyopaque, q_ct: CT, k_ct: CT, v_ct: CT, attn_bias_ct: ?CT, attention: ops.AttentionContext, batch: usize, num_heads: usize, num_kv_heads: usize, head_dim: usize) anyerror!CT {
    return gqaAttentionLaunch(@ptrCast(@alignCast(ctx)), q_ct, k_ct, v_ct, attn_bias_ct, attention, batch, num_heads, num_kv_heads, head_dim);
}

const vtable = ops.ComputeBackend.VTable{
    .backendKind = &backendKind,
    .deinitBackend = &deinitBackend,
    .freeTensor = &freeTensor,
    .provisionKvDeviceWriteHook = &provisionKvDeviceWriteHook,
    .getWeight = &getWeight,
    .prefetchWeightHint = &prefetchWeightHint,
    .drainPrefetchBudget = &drainPrefetchBudget,
    .embeddingLookup = &embeddingLookup,
    .takeRows = &takeRows,
    .glinerWordEmbeddings = &glinerWordEmbeddings,
    .glinerLabelGruCombined = &glinerLabelGruCombined,
    .linear = &linear,
    .linearQuickGelu = &linearQuickGelu,
    .linearRelu = &linearRelu,
    .linearGelu = &linearGelu,
    .linearAdd = &linearAdd,
    .linearNoBias = &linearNoBias,
    .linearPair = &linearPair,
    .linearTriple = &linearTriple,
    .layerNorm = &layerNorm,
    .addLayerNorm = &addLayerNorm,
    .rmsNorm = &rmsNorm,
    .gelu = &gelu,
    .relu = &relu,
    .silu = &silu,
    .quickGelu = &quickGelu,
    .sigmoid = &sigmoid,
    .tanh_act = &tanhAct,
    .splitLastDim3 = &splitLastDim3,
    .sliceLastDim = &sliceLastDim,
    .concat = &concat,
    .concatRows2D = &concatRows2D,
    .sliceRows2D = &sliceRows2D,
    .add = &add,
    .scaledDotProductAttention = &sdpa,
    .scaledDotProductAttentionFull = &sdpaFull,
    .causalSelfAttention = &causalSelfAttention,
    .crossAttention = &crossAttention,
    .relativePositionBias = &relativePositionBias,
    .disentangledRelativeAttention = &debertaDisentangledAttention,
    .windowedSelfAttention = &windowedSelfAttention,
    .channelSelfAttention = &channelSelfAttention,
    .tokenGridConv2d = &tokenGridConv2d,
    .multiply = &multiply,
    .conv1d = &conv1d,
    .conv2d = &conv2d,
    .rope = &rope,
    .ropePerItem = &ropePerItem,
    .gqaCausalAttention = &gqaCausalAttention,
    .gqaPagedAttention = &gqaPagedAttention,
    .fromFloat32 = &fromFloat32Op,
    .fromFloat32Shape = &fromFloat32ShapeOp,
    .toFloat32 = &toFloat32Op,
    .toFloat32Batch = &toFloat32BatchOp,
    .tensorDType = &tensorDTypeOp,
    .tensorShape = &tensorShapeOp,
    .evalTensor = &evalTensorOp,
    .subtract = &subtractOp,
    .divide = &divideOp,
    .negate = &negateOp,
    .sqrtOp = &primSqrtOp,
    .rsqrtOp = &primRsqrtOp,
    .expOp = &primExpOp,
    .logOp = &primLogOp,
    .sinOp = &primSinOp,
    .cosOp = &primCosOp,
    .tanhOp = &primTanhOp,
    .erfOp = &primErfOp,
    .absOp = &primAbsOp,
    .lessThan = &lessThanOp,
    .whereSelect = &whereSelectOp,
    .reduceSumOp = &primReduceSumOp,
    .reduceMaxOp = &primReduceMaxOp,
    .reduceMeanOp = &primReduceMeanOp,
    .argmaxOp = &primArgMaxOp,
    .reshapeOp = &primReshapeOp,
    .transposeOp = &primTransposeOp,
    .broadcastInDimOp = &primBroadcastInDimOp,
    .dotGeneralOp = &primDotGeneralOp,
    .scatterAddOp = &primScatterAddOp,
    .scatterAddIntoOp = &primScatterAddIntoOp,
    .gatherOp = &primGatherOp,
    .sliceOp = &primSliceOp,
    .concatPrimOp = &primConcatPrimOp,
    .softmaxOp = &primSoftmaxOp,
    .logSoftmaxOp = &primLogSoftmaxOp,
};

test "cuda compute vtable is type checked" {
    const backend_kind_fn: *const fn (*anyopaque) ops.BackendKind = &backendKind;
    const linear_fn: *const fn (*anyopaque, CT, CT, CT, usize, usize, usize) anyerror!CT = &linear;
    const linear_no_bias_fn: *const fn (*anyopaque, CT, CT, usize, usize, usize) anyerror!CT = &linearNoBias;
    const rms_norm_fn: *const fn (*anyopaque, CT, CT, usize, f32) anyerror!CT = &rmsNorm;
    const rope_per_item_fn: *const fn (*anyopaque, CT, usize, usize, usize, usize, f32, f32, []const usize, []const usize, bool) anyerror!CT = &ropePerItem;
    const subtract_fn: *const fn (*anyopaque, CT, CT) anyerror!CT = &subtractOp;
    const divide_fn: *const fn (*anyopaque, CT, CT) anyerror!CT = &divideOp;
    const exp_fn: *const fn (*anyopaque, CT) anyerror!CT = &primExpOp;
    const log_fn: *const fn (*anyopaque, CT) anyerror!CT = &primLogOp;
    const less_than_fn: *const fn (*anyopaque, CT, CT) anyerror!CT = &lessThanOp;
    const where_select_fn: *const fn (*anyopaque, CT, CT, CT) anyerror!CT = &whereSelectOp;
    const reshape_fn: *const fn (*anyopaque, CT, []const i64) anyerror!CT = &primReshapeOp;
    const dot_general_fn: *const fn (*anyopaque, CT, CT, []const i64, []const i64, []const u8, []const u8, []const u8, []const u8) anyerror!CT = &primDotGeneralOp;
    const to_float_batch_fn: *const fn (*anyopaque, []const CT, std.mem.Allocator) anyerror![][]f32 = &toFloat32BatchOp;
    _ = backend_kind_fn;
    _ = linear_fn;
    _ = linear_no_bias_fn;
    _ = rms_norm_fn;
    _ = rope_per_item_fn;
    _ = subtract_fn;
    _ = divide_fn;
    _ = exp_fn;
    _ = log_fn;
    _ = less_than_fn;
    _ = where_select_fn;
    _ = reshape_fn;
    _ = dot_general_fn;
    _ = to_float_batch_fn;
    _ = vtable;
}

test "cuda shape helpers reject incompatible shapes" {
    try std.testing.expect(try checkedMul(2, 3) == 6);
    try std.testing.expect(sameShape(&.{ 2, 3 }, &.{ 2, 3 }));
    try std.testing.expect(!sameShape(&.{ 2, 3 }, &.{ 3, 2 }));
    try std.testing.expect(axesArePrefix(&.{ 0, 1 }));
    try std.testing.expect(!axesArePrefix(&.{ 1, 0 }));
    try std.testing.expect(isFullSlice(&.{ 0, 0 }, &.{ 2, 3 }, &.{ 1, 1 }, &.{ 2, 3 }));
    try std.testing.expect(!isFullSlice(&.{ 1, 0 }, &.{ 2, 3 }, &.{ 1, 1 }, &.{ 2, 3 }));

    const shape = try sharedLinearDotOutputShape(std.testing.allocator, &.{ 2, 3, 4 }, 2, 7);
    defer std.testing.allocator.free(shape);
    try std.testing.expectEqualSlices(i64, &.{ 2, 3, 7 }, shape);

    const gather_shape = try gatherOutputShape(std.testing.allocator, &.{ 2, 3, 4 }, 1, &.{ 5, 6 }, 30);
    defer std.testing.allocator.free(gather_shape);
    try std.testing.expectEqualSlices(i64, &.{ 2, 5, 6, 4 }, gather_shape);

    const scalar_gather_shape = try gatherOutputShape(std.testing.allocator, &.{ 2, 3, 4 }, 1, &.{}, 1);
    defer std.testing.allocator.free(scalar_gather_shape);
    try std.testing.expectEqualSlices(i64, &.{ 2, 4 }, scalar_gather_shape);

    const all_ones_gather_shape = try gatherOutputShape(std.testing.allocator, &.{ 2, 3, 4 }, 1, &.{ 1, 1 }, 1);
    defer std.testing.allocator.free(all_ones_gather_shape);
    try std.testing.expectEqualSlices(i64, &.{ 2, 1, 4 }, all_ones_gather_shape);

    const argmax_keep_shape = try argMaxOutputShape(std.testing.allocator, &.{ 2, 3, 4 }, 2, true);
    defer std.testing.allocator.free(argmax_keep_shape);
    try std.testing.expectEqualSlices(i64, &.{ 2, 3, 1 }, argmax_keep_shape);

    const argmax_drop_shape = try argMaxOutputShape(std.testing.allocator, &.{ 2, 3, 4 }, 2, false);
    defer std.testing.allocator.free(argmax_drop_shape);
    try std.testing.expectEqualSlices(i64, &.{ 2, 3 }, argmax_drop_shape);
}

test "cuda reduction shape helpers cover last-dim and all-axes patterns" {
    try std.testing.expect(isAllAxesReduction(&.{ 0, 1 }, 2));
    try std.testing.expect(isAllAxesReduction(&.{ 1, 0 }, 2));
    try std.testing.expect(!isAllAxesReduction(&.{1}, 2));
    try std.testing.expect(!isAllAxesReduction(&.{ 0, 0 }, 2));

    const last_dim = try reduceOutputShape(std.testing.allocator, &.{ 2, 3, 4 }, &.{2});
    defer std.testing.allocator.free(last_dim);
    try std.testing.expect(sameShape(last_dim, &.{ 2, 3, 1 }));

    const all_axes = try reduceOutputShape(std.testing.allocator, &.{ 2, 3 }, &.{ 0, 1 });
    defer std.testing.allocator.free(all_axes);
    try std.testing.expect(sameShape(all_axes, &.{ 1, 1 }));
}

test "cuda shape-only planners identify safe device copies" {
    try std.testing.expect(isAliasableTranspose(&.{ 2, 3 }, &.{ 0, 1 }));
    try std.testing.expect(isAliasableTranspose(&.{ 2, 1, 3 }, &.{ 0, 2, 1 }));
    try std.testing.expect(!isAliasableTranspose(&.{ 2, 3 }, &.{ 1, 0 }));

    const transposed = try transposeAliasShape(std.testing.allocator, &.{ 2, 1, 3 }, &.{ 0, 2, 1 });
    defer std.testing.allocator.free(transposed);
    try std.testing.expect(sameShape(transposed, &.{ 2, 3, 1 }));

    try std.testing.expect(isIdentityBroadcast(&.{ 2, 3 }, &.{ 0, 1 }, &.{ 2, 3 }));
    try std.testing.expect(!isIdentityBroadcast(&.{ 2, 3 }, &.{1}, &.{3}));
    try std.testing.expect(!isIdentityBroadcast(&.{ 2, 3 }, &.{ 0, 1 }, &.{ 1, 3 }));

    const broadcast = (try planBroadcastInDim(&.{ 2, 2, 3 }, &.{ 0, 2 }, &.{ 2, 3 }, 6)).?;
    try std.testing.expectEqual(@as(usize, 3), broadcast.target_rank);
    try std.testing.expectEqual(@as(usize, 2), broadcast.input_rank);
    try std.testing.expectEqual(@as(usize, 12), broadcast.out_count);
    try std.testing.expectEqual(@as(usize, 6), broadcast.input_count);

    const scalar_broadcast = (try planBroadcastInDim(&.{ 2, 3 }, &.{}, &.{}, 1)).?;
    try std.testing.expectEqual(@as(usize, 6), scalar_broadcast.out_count);
    try std.testing.expectEqual(@as(usize, 0), scalar_broadcast.input_rank);

    try std.testing.expectError(error.InvalidShape, planBroadcastInDim(&.{ 2, 3 }, &.{ 0, 0 }, &.{ 2, 3 }, 6));
    try std.testing.expectError(error.InvalidShape, planBroadcastInDim(&.{ 2, 3 }, &.{1}, &.{2}, 2));

    const where_broadcast = (try planBroadcastShape3(&.{ 1, 4 }, &.{ 2, 4 }, &.{}, 4, 8, 1)).?;
    try std.testing.expectEqual(@as(usize, 2), where_broadcast.rank);
    try std.testing.expectEqual(@as(usize, 8), where_broadcast.count);
    try std.testing.expectEqualSlices(i64, &.{ 2, 4 }, where_broadcast.shape[0..where_broadcast.rank]);
    try std.testing.expect((try planBroadcastShape3(&.{ 2, 3 }, &.{ 3, 2 }, &.{}, 6, 6, 1)) == null);

    try std.testing.expect(isFullSlice(&.{ 0, 0 }, &.{ 2, 3 }, &.{ 1, 1 }, &.{ 2, 3 }));
    try std.testing.expect(isFullSlice(&.{ 0, 0 }, &.{ -1, -1 }, &.{ 1, 1 }, &.{ 2, 3 }));
    try std.testing.expect(!isFullSlice(&.{ 1, 0 }, &.{ 2, 3 }, &.{ 1, 1 }, &.{ 2, 3 }));
    try std.testing.expect(!isFullSlice(&.{ 0, 0 }, &.{ 2, 3 }, &.{ 2, 1 }, &.{ 2, 3 }));
}

test "cuda softmax last-dimension resolver handles explicit and inferred dims" {
    var explicit = CudaTensor{
        .buffer = .{},
        .dtype = .f32,
        .shape = @constCast(&[_]i64{ 2, 3 }),
        .elem_count = 6,
    };
    try std.testing.expectEqual(@as(?usize, 3), resolveSoftmaxLastDimCuda(&explicit, 0));
    try std.testing.expectEqual(@as(?usize, 2), resolveSoftmaxLastDimCuda(&explicit, 2));

    var inferred = CudaTensor{
        .buffer = .{},
        .dtype = .f32,
        .shape = @constCast(&[_]i64{ 2, -1 }),
        .elem_count = 10,
    };
    try std.testing.expectEqual(@as(?usize, 5), resolveSoftmaxLastDimCuda(&inferred, 0));
}

test "cuda strict mode blocks host training fallback boundary" {
    var fake = CudaCompute{
        .allocator = std.testing.allocator,
        .ctx = undefined,
        .kernels = undefined,
        .allow_host_training_fallbacks = false,
    };
    var fallback: NativeFallback = undefined;
    try std.testing.expectError(error.CudaHostTrainingFallbackDisabled, fallback.init(&fake, ""));
}

test "cuda dot general planner maps folded 2d matmul to linear dims" {
    const plan = (try planDotGeneral2DLinear(
        &.{ 4, 8 },
        &.{ 16, 8 },
        &.{1},
        &.{1},
        &.{},
        &.{},
    )).?;
    try std.testing.expectEqual(@as(usize, 4), plan.rows);
    try std.testing.expectEqual(@as(usize, 8), plan.in_dim);
    try std.testing.expectEqual(@as(usize, 16), plan.out_dim);

    try std.testing.expect((try planDotGeneral2DLinear(
        &.{ 4, 8 },
        &.{ 8, 16 },
        &.{1},
        &.{0},
        &.{},
        &.{},
    )) == null);
    try std.testing.expectError(error.InvalidShape, planDotGeneral2DLinear(
        &.{ 4, 8 },
        &.{ 16, 7 },
        &.{1},
        &.{1},
        &.{},
        &.{},
    ));
}

test "cuda concat planner only accepts last-dimension concat" {
    const plan = (try planConcatLastDim(&.{ 2, 3, 4 }, &.{ 2, 3, 5 }, 2)).?;
    try std.testing.expectEqual(@as(usize, 6), plan.rows);
    try std.testing.expectEqual(@as(usize, 4), plan.dim_a);
    try std.testing.expectEqual(@as(usize, 5), plan.dim_b);
    try std.testing.expectEqual(@as(usize, 9), plan.out_dim);
    try std.testing.expectEqual(@as(usize, 54), plan.out_count);

    try std.testing.expect((try planConcatLastDim(&.{ 2, 3, 4 }, &.{ 2, 3, 5 }, 1)) == null);
    try std.testing.expectError(error.InvalidShape, planConcatLastDim(&.{ 2, 3, 4 }, &.{ 2, 4, 5 }, 2));
}
