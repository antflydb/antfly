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

// WASM ComputeBackend: SIMD-accelerated CPU inference for browser environments.
//
// Uses activations.zig (@Vector → WASM SIMD) for norms/activations and
// inference_linalg for pure Zig matmul kernels. Optionally
// delegates heavy ops to WebGPU via JS extern imports.

const std = @import("std");
const build_options = @import("build_options");
const ops = @import("ops.zig");
const CT = ops.CT;
const ComputeBackend = ops.ComputeBackend;
const activations = @import("../backends/activations.zig");
const tensor_mod = @import("../backends/tensor.zig");
const linalg = @import("inference_linalg");
const quant_codec = @import("../gguf/quant_codec.zig");
const tensor_types = @import("../gguf/tensor_types.zig");
const turboquant = @import("../runtime/kv/turboquant.zig");
const model_runtime = @import("../graph/model_runtime.zig");
const quant_matmul = @import("../graph/quant_matmul.zig");
const web_profile = @import("../web/profile.zig");
const wasm_extern = if (build_options.enable_webgpu) @import("wasm_extern.zig") else struct {
    pub const GpuBufferId = u32;
    pub const invalid_buffer: GpuBufferId = 0;
    pub const GqaCachedKeyFormat = enum(u32) {
        f32 = 0,
        polar4 = 1,
        turbo3 = 2,
    };
    pub const GqaCachedValueFormat = enum(u32) {
        f32 = 0,
        int8_per_head = 1,
    };

    pub fn isAvailable() bool {
        return false;
    }

    pub fn createBuffer(_: u32) GpuBufferId {
        return 0;
    }

    pub fn upload(_: GpuBufferId, _: []const u8) void {}

    pub fn download(_: GpuBufferId, _: []u8) void {}

    pub fn freeBuffer(_: GpuBufferId) void {}

    pub fn add(_: GpuBufferId, _: GpuBufferId, _: GpuBufferId, _: u32) void {}

    pub fn addBroadcast(_: GpuBufferId, _: GpuBufferId, _: GpuBufferId, _: u32, _: u32, _: u32) void {}

    pub fn mul(_: GpuBufferId, _: GpuBufferId, _: GpuBufferId, _: u32, _: u32, _: u32) void {}

    pub fn sub(_: GpuBufferId, _: GpuBufferId, _: GpuBufferId, _: u32, _: u32, _: u32) void {}

    pub fn div(_: GpuBufferId, _: GpuBufferId, _: GpuBufferId, _: u32, _: u32, _: u32) void {}

    pub fn lessThan(_: GpuBufferId, _: GpuBufferId, _: GpuBufferId, _: u32, _: u32, _: u32) void {}

    pub fn whereSelect(_: GpuBufferId, _: GpuBufferId, _: GpuBufferId, _: GpuBufferId, _: u32, _: u32, _: u32) void {}

    pub fn neg(_: GpuBufferId, _: GpuBufferId, _: u32) void {}

    pub fn sqrt(_: GpuBufferId, _: GpuBufferId, _: u32) void {}

    pub fn rsqrt(_: GpuBufferId, _: GpuBufferId, _: u32) void {}

    pub fn exp(_: GpuBufferId, _: GpuBufferId, _: u32) void {}

    pub fn log(_: GpuBufferId, _: GpuBufferId, _: u32) void {}

    pub fn sin(_: GpuBufferId, _: GpuBufferId, _: u32) void {}

    pub fn cos(_: GpuBufferId, _: GpuBufferId, _: u32) void {}

    pub fn tanh(_: GpuBufferId, _: GpuBufferId, _: u32) void {}

    pub fn abs(_: GpuBufferId, _: GpuBufferId, _: u32) void {}

    pub fn erf(_: GpuBufferId, _: GpuBufferId, _: u32) void {}

    pub fn gelu(_: GpuBufferId, _: GpuBufferId, _: u32) void {}

    pub fn softmax(_: GpuBufferId, _: GpuBufferId, _: u32, _: u32) void {}

    pub fn logSoftmax(_: GpuBufferId, _: GpuBufferId, _: u32, _: u32) void {}

    pub fn reduceSumLastDim(_: GpuBufferId, _: GpuBufferId, _: u32, _: u32) void {}

    pub fn reduceMaxLastDim(_: GpuBufferId, _: GpuBufferId, _: u32, _: u32) void {}

    pub fn reduceMeanLastDim(_: GpuBufferId, _: GpuBufferId, _: u32, _: u32) void {}

    pub fn reduceSum(_: GpuBufferId, _: GpuBufferId, _: u32, _: u32, _: u32, _: u32, _: []const u32, _: []const u32, _: []const u32, _: []const u32, _: []const u32, _: []const u32, _: []const u32) void {}

    pub fn reduceMax(_: GpuBufferId, _: GpuBufferId, _: u32, _: u32, _: u32, _: u32, _: []const u32, _: []const u32, _: []const u32, _: []const u32, _: []const u32, _: []const u32, _: []const u32) void {}

    pub fn reduceMean(_: GpuBufferId, _: GpuBufferId, _: u32, _: u32, _: u32, _: u32, _: []const u32, _: []const u32, _: []const u32, _: []const u32, _: []const u32, _: []const u32, _: []const u32) void {}

    pub fn broadcastInDim(_: GpuBufferId, _: GpuBufferId, _: u32, _: u32, _: u32, _: []const u32, _: []const u32, _: []const u32, _: []const u32, _: []const u32) void {}

    pub fn disentangledRelativeAttention(_: GpuBufferId, _: GpuBufferId, _: GpuBufferId, _: GpuBufferId, _: GpuBufferId, _: GpuBufferId, _: GpuBufferId, _: u32, _: u32, _: u32, _: u32) void {}

    pub fn layerNorm(_: GpuBufferId, _: GpuBufferId, _: GpuBufferId, _: GpuBufferId, _: u32, _: u32, _: u32) void {}

    pub fn rmsNorm(_: GpuBufferId, _: GpuBufferId, _: GpuBufferId, _: u32, _: u32, _: u32) void {}
};

pub const GpuKvKeyFormat = wasm_extern.GqaCachedKeyFormat;
pub const GpuKvValueFormat = wasm_extern.GqaCachedValueFormat;

// Minimum elements in a matmul output to justify GPU dispatch overhead.
// Below this, WASM SIMD is faster due to no upload/download cost.
const WEBGPU_MATMUL_THRESHOLD: usize = 64 * 768; // ~50K elements

// Minimum output elements for GPU attention dispatch.
const WEBGPU_ATTN_THRESHOLD: usize = 64 * 768; // ~50K elements

// Max seq_len the attention shader supports (shared memory limit).
const WEBGPU_ATTN_MAX_SEQ: usize = 512;

// Max kv_len the cached attention shader supports (shared memory limit).
const WEBGPU_CACHED_ATTN_MAX_KV: usize = 2048;

const WasmBuf = struct {
    data: []f32,
    len: usize, // logical element count
    owned: bool,
    allocator: std.mem.Allocator,
    shape: ?[]i64 = null,
    i32_data: ?[]i32 = null,
    f16_data: ?[]f16 = null,
    quant_raw: ?[]const u8 = null, // raw quantized bytes (owned)
    quant_type: ?tensor_types.TensorType = null,
    gpu_tensor: ?wasm_extern.GpuBufferId = null,
    gpu_tensor_owned: bool = false,
    host_data_valid: bool = true,

    fn fromSlice(allocator: std.mem.Allocator, data: []f32, owned: bool) *WasmBuf {
        return fromSliceWithGpu(allocator, data, owned, null, false);
    }

    fn fromSliceWithGpu(
        allocator: std.mem.Allocator,
        data: []f32,
        owned: bool,
        gpu_tensor: ?wasm_extern.GpuBufferId,
        gpu_tensor_owned: bool,
    ) *WasmBuf {
        const buf = allocator.create(WasmBuf) catch @panic("OOM");
        buf.* = .{
            .data = data,
            .len = data.len,
            .owned = owned,
            .allocator = allocator,
            .gpu_tensor = gpu_tensor,
            .gpu_tensor_owned = gpu_tensor_owned,
            .host_data_valid = gpu_tensor == null,
        };
        return buf;
    }

    fn fromF16Slice(allocator: std.mem.Allocator, data: []f16) *WasmBuf {
        const buf = allocator.create(WasmBuf) catch @panic("OOM");
        const empty = allocator.alloc(f32, 0) catch @panic("OOM");
        buf.* = .{
            .data = empty,
            .len = data.len,
            .owned = false,
            .allocator = allocator,
            .f16_data = data,
        };
        return buf;
    }

    fn fromI32Slice(allocator: std.mem.Allocator, data: []i32) *WasmBuf {
        const buf = allocator.create(WasmBuf) catch @panic("OOM");
        const empty = allocator.alloc(f32, 0) catch @panic("OOM");
        buf.* = .{
            .data = empty,
            .len = data.len,
            .owned = false,
            .allocator = allocator,
            .i32_data = data,
        };
        return buf;
    }

    fn fromQuantized(alloc: std.mem.Allocator, raw: []const u8, qtype: tensor_types.TensorType, n_elements: usize) *WasmBuf {
        const buf = alloc.create(WasmBuf) catch @panic("OOM");
        const empty = alloc.alloc(f32, 0) catch @panic("OOM");
        buf.* = .{
            .data = empty,
            .len = n_elements,
            .owned = false,
            .allocator = alloc,
            .quant_raw = raw,
            .quant_type = qtype,
        };
        return buf;
    }

    fn fromQuantizedGpuResident(alloc: std.mem.Allocator, qtype: tensor_types.TensorType, n_elements: usize) *WasmBuf {
        const buf = alloc.create(WasmBuf) catch @panic("OOM");
        const empty = alloc.alloc(f32, 0) catch @panic("OOM");
        buf.* = .{
            .data = empty,
            .len = n_elements,
            .owned = false,
            .allocator = alloc,
            .quant_type = qtype,
        };
        return buf;
    }

    fn deinit(self: *WasmBuf) void {
        if (self.owned) {
            self.allocator.free(self.data);
        }
        if (self.shape) |shape| {
            self.allocator.free(shape);
        }
        if (self.i32_data) |data| {
            self.allocator.free(data);
        }
        if (self.f16_data) |f16d| {
            self.allocator.free(f16d);
        }
        if (self.quant_raw) |raw| {
            self.allocator.free(raw);
        }
        if (self.gpu_tensor_owned) {
            if (self.gpu_tensor) |gpu_tensor| {
                wasm_extern.freeBuffer(gpu_tensor);
            }
        }
        self.allocator.destroy(self);
    }

    fn dropHostQuantizedCopy(self: *WasmBuf) void {
        if (self.quant_raw) |raw| {
            self.allocator.free(raw);
            self.quant_raw = null;
        }
    }

    fn setShapeOwned(self: *WasmBuf, shape: []i64) void {
        if (self.shape) |existing| {
            self.allocator.free(existing);
        }
        self.shape = shape;
    }

    fn cloneShape(self: *WasmBuf) !?[]i64 {
        if (self.shape) |shape| {
            return try self.allocator.dupe(i64, shape);
        }
        return null;
    }

    fn setShape2D(self: *WasmBuf, rows: usize, cols: usize) !void {
        const shape = try self.allocator.alloc(i64, 2);
        shape[0] = @intCast(rows);
        shape[1] = @intCast(cols);
        self.setShapeOwned(shape);
    }

    /// Returns f32 data, dequanting from f16/quantized if needed. Caller must free if allocated is true.
    const F32View = struct { data: []const f32, allocated: bool };
    fn viewF32(self: *WasmBuf, alloc: std.mem.Allocator) !F32View {
        if (self.quant_type != null) {
            if (self.quant_raw) |raw| {
                const out = try alloc.alloc(f32, self.len);
                try quant_codec.dequantizeToFloat32(self.quant_type.?, raw, out);
                return .{ .data = out, .allocated = true };
            }
            return error.HostQuantizedWeightUnavailable;
        }
        if (self.f16_data) |f16d| {
            const out = try alloc.alloc(f32, f16d.len);
            for (f16d, 0..) |v, i| out[i] = @floatCast(v);
            return .{ .data = out, .allocated = true };
        }
        try self.ensureHostData();
        return .{ .data = self.data, .allocated = false };
    }

    fn ensureHostData(self: *WasmBuf) !void {
        if (self.host_data_valid) return;
        const gpu_tensor = self.gpu_tensor orelse return error.HostTensorUnavailable;
        if (self.data.len < self.len) return error.HostTensorUnavailable;
        const byte_len: usize = self.len * @sizeOf(f32);
        wasm_extern.download(gpu_tensor, @as([*]u8, @ptrCast(self.data.ptr))[0..byte_len]);
        self.host_data_valid = true;
    }
};

const GpuWeightStore = struct {
    allocator: std.mem.Allocator,
    buffers: std.AutoHashMap(*WasmBuf, wasm_extern.GpuBufferId),

    fn init(allocator: std.mem.Allocator) GpuWeightStore {
        return .{
            .allocator = allocator,
            .buffers = std.AutoHashMap(*WasmBuf, wasm_extern.GpuBufferId).init(allocator),
        };
    }

    fn deinit(self: *GpuWeightStore) void {
        var it = self.buffers.valueIterator();
        while (it.next()) |gpu_buf| {
            wasm_extern.freeBuffer(gpu_buf.*);
        }
        self.buffers.deinit();
    }

    fn hasResident(self: *GpuWeightStore, weight: *WasmBuf) bool {
        return self.buffers.contains(weight);
    }

    fn ensureResident(self: *GpuWeightStore, weight: *WasmBuf) !wasm_extern.GpuBufferId {
        if (!build_options.enable_webgpu) return error.WebGpuUnavailable;
        if (self.buffers.get(weight)) |gpu_buf| return gpu_buf;
        if (weight.quant_type != null and weight.quant_raw == null) {
            return error.HostQuantizedWeightUnavailable;
        }

        const gpu_buf = if (weight.quant_raw) |raw| blk: {
            const id = try createUploadedQuantStorageBuffer(self.allocator, raw);
            break :blk id;
        } else if (weight.f16_data) |f16d| blk: {
            const out = try self.allocator.alloc(f32, f16d.len);
            defer self.allocator.free(out);
            for (f16d, 0..) |v, i| out[i] = @floatCast(v);
            const byte_len: usize = out.len * @sizeOf(f32);
            const id = wasm_extern.createBuffer(@intCast(byte_len));
            wasm_extern.upload(id, @as([*]const u8, @ptrCast(out.ptr))[0..byte_len]);
            break :blk id;
        } else blk: {
            const byte_len: usize = weight.data.len * @sizeOf(f32);
            const id = wasm_extern.createBuffer(@intCast(byte_len));
            wasm_extern.upload(id, @as([*]const u8, @ptrCast(weight.data.ptr))[0..byte_len]);
            break :blk id;
        };

        try self.buffers.put(weight, gpu_buf);
        return gpu_buf;
    }

    fn putResident(self: *GpuWeightStore, weight: *WasmBuf, gpu_buf: wasm_extern.GpuBufferId) !void {
        try self.buffers.put(weight, gpu_buf);
    }
};

fn quantStorageByteLen(raw_len: usize) !usize {
    const with_padding = std.math.add(usize, raw_len, 3) catch return error.OutOfMemory;
    return with_padding & ~@as(usize, 3);
}

fn createUploadedQuantStorageBuffer(allocator: std.mem.Allocator, raw: []const u8) !wasm_extern.GpuBufferId {
    // Quant kernels bind raw bytes as array<u32>, so the final partial word must exist.
    const padded_len = try quantStorageByteLen(raw.len);
    const id = wasm_extern.createBuffer(std.math.cast(u32, padded_len) orelse return error.OutOfMemory);
    errdefer wasm_extern.freeBuffer(id);

    if (padded_len == raw.len) {
        wasm_extern.upload(id, raw);
        return id;
    }

    const padded = try allocator.alloc(u8, padded_len);
    defer allocator.free(padded);
    @memset(padded, 0);
    @memcpy(padded[0..raw.len], raw);
    wasm_extern.upload(id, padded);
    return id;
}

const GpuTensor = struct {
    id: wasm_extern.GpuBufferId,

    fn create(byte_len: usize) GpuTensor {
        return .{ .id = wasm_extern.createBuffer(@intCast(byte_len)) };
    }

    fn fromF32(data: []const f32) GpuTensor {
        const tensor = create(data.len * @sizeOf(f32));
        tensor.uploadF32(data);
        return tensor;
    }

    fn uploadF32(self: GpuTensor, data: []const f32) void {
        const byte_len: usize = data.len * @sizeOf(f32);
        wasm_extern.upload(self.id, @as([*]const u8, @ptrCast(data.ptr))[0..byte_len]);
    }

    fn downloadF32(self: GpuTensor, out: []f32) void {
        const byte_len: usize = out.len * @sizeOf(f32);
        wasm_extern.download(self.id, @as([*]u8, @ptrCast(out.ptr))[0..byte_len]);
    }

    fn deinit(self: *GpuTensor) void {
        wasm_extern.freeBuffer(self.id);
        self.id = 0;
    }

    fn detach(self: *GpuTensor) wasm_extern.GpuBufferId {
        const id = self.id;
        self.id = 0;
        return id;
    }
};

const GpuInputTensor = struct {
    id: wasm_extern.GpuBufferId,
    owned: bool,

    fn fromBuf(buf: *WasmBuf) GpuInputTensor {
        if (buf.gpu_tensor) |gpu_tensor| {
            return .{ .id = gpu_tensor, .owned = false };
        }
        var tensor = GpuTensor.fromF32(buf.data);
        return .{ .id = tensor.detach(), .owned = true };
    }

    fn deinit(self: *GpuInputTensor) void {
        if (self.owned) {
            wasm_extern.freeBuffer(self.id);
        }
        self.id = 0;
    }
};

fn cloneDenseBufPreservingGpu(self: *WasmCompute, buf: *WasmBuf) !CT {
    const output = try self.allocator.dupe(f32, buf.data);
    const out_buf = if (build_options.enable_webgpu and self.use_gpu) blk: {
        if (buf.gpu_tensor) |src_gpu| {
            var out_gpu = GpuTensor.create(buf.len * @sizeOf(f32));
            wasm_extern.copyBufferToBuffer(src_gpu, 0, out_gpu.id, 0, @intCast(buf.len * @sizeOf(f32)));
            break :blk WasmBuf.fromSliceWithGpu(self.allocator, output, true, out_gpu.detach(), true);
        }
        break :blk WasmBuf.fromSlice(self.allocator, output, true);
    } else WasmBuf.fromSlice(self.allocator, output, true);
    if (try buf.cloneShape()) |shape| {
        out_buf.setShapeOwned(shape);
    }
    return fromBuf(out_buf);
}

fn dupSliceWithGpu(self: *WasmCompute, buf: *WasmBuf, start_elem: usize, elem_count: usize) !CT {
    const output = try self.allocator.dupe(f32, buf.data[start_elem .. start_elem + elem_count]);
    const out_buf = if (build_options.enable_webgpu and self.use_gpu) blk: {
        if (buf.gpu_tensor) |src_gpu| {
            var out_gpu = GpuTensor.create(elem_count * @sizeOf(f32));
            wasm_extern.copyBufferToBuffer(
                src_gpu,
                @intCast(start_elem * @sizeOf(f32)),
                out_gpu.id,
                0,
                @intCast(elem_count * @sizeOf(f32)),
            );
            break :blk WasmBuf.fromSliceWithGpu(self.allocator, output, true, out_gpu.detach(), true);
        }
        break :blk WasmBuf.fromSlice(self.allocator, output, true);
    } else WasmBuf.fromSlice(self.allocator, output, true);
    return fromBuf(out_buf);
}

fn setBufShape2D(buf: *WasmBuf, rows: usize, cols: usize) !*WasmBuf {
    try buf.setShape2D(rows, cols);
    return buf;
}

fn copyBufShape(dst: *WasmBuf, src: *WasmBuf) !*WasmBuf {
    if (try src.cloneShape()) |shape| {
        dst.setShapeOwned(shape);
    }
    return dst;
}

fn supportsGpuQuantMatmul(tensor_type: tensor_types.TensorType) bool {
    return switch (tensor_type) {
        .known => |k| switch (k) {
            .Q4_0, .Q4_1, .Q5_0, .Q5_1, .Q8_0, .Q8_1, .IQ4_NL, .IQ4_XS, .Q2_K, .Q3_K, .Q4_K, .Q5_K, .Q6_K, .Q8_K, .I2_S, .I8_S, .Q1_0, .TQ1_0, .TQ2_0, .MXFP4, .NVFP4, .IQ1_S, .IQ1_M, .IQ2_XXS, .IQ2_XS, .IQ2_S, .IQ3_XXS, .IQ3_S => true,
            else => false,
        },
        else => false,
    };
}

fn quantFormatFromTensorType(tensor_type: tensor_types.TensorType) ?quant_matmul.Format {
    return switch (tensor_type) {
        .known => |known| switch (known) {
            .Q4_0 => .q4_0,
            .Q4_1 => .q4_1,
            .Q5_0 => .q5_0,
            .Q5_1 => .q5_1,
            .Q8_0 => .q8_0,
            .Q8_1 => .q8_1,
            .Q2_K => .q2_k,
            .Q3_K => .q3_k,
            .Q4_K => .q4_k,
            .Q5_K => .q5_k,
            .Q6_K => .q6_k,
            .Q8_K => .q8_k,
            .IQ4_NL => .iq4_nl,
            .IQ4_XS => .iq4_xs,
            .I2_S => .i2_s,
            // Extra formats added by the WebGPU GGUF kernel work; without
            // these the planned-quant-linear path validates them as
            // unsupported and the MMV dispatch always falls back to GEMM.
            .I8_S => .i8_s,
            .Q1_0 => .q1_0,
            .TQ1_0 => .tq1_0,
            .TQ2_0 => .tq2_0,
            .MXFP4 => .mxfp4,
            .NVFP4 => .nvfp4,
            .IQ1_S => .iq1_s,
            .IQ1_M => .iq1_m,
            .IQ2_XXS => .iq2_xxs,
            .IQ2_XS => .iq2_xs,
            .IQ2_S => .iq2_s,
            .IQ3_XXS => .iq3_xxs,
            .IQ3_S => .iq3_s,
            else => null,
        },
        else => null,
    };
}

fn toBuf(ct: CT) *WasmBuf {
    return @ptrCast(@alignCast(ct));
}

fn fromBuf(buf: *WasmBuf) CT {
    return @ptrCast(buf);
}

fn resolveSoftmaxLastDim(buf: *WasmBuf, dim: u32) ?usize {
    if (dim != 0) return @intCast(dim);
    if (buf.shape) |shape| {
        if (shape.len > 0 and shape[shape.len - 1] > 0) {
            return @intCast(shape[shape.len - 1]);
        }
    }
    if (buf.len > 0) return buf.len;
    return null;
}

fn isModuloBroadcastCompatible(a_len: usize, b_len: usize) bool {
    const out_len = @max(a_len, b_len);
    if (out_len == 0) return a_len == b_len;
    if (a_len == 0 or b_len == 0) return false;
    return out_len % a_len == 0 and out_len % b_len == 0;
}

fn isWhereSelectGpuCompatible(cond_len: usize, true_len: usize, false_len: usize) bool {
    if (cond_len == 0) return true;
    return (true_len == cond_len or true_len == 1) and (false_len == cond_len or false_len == 1);
}

fn isGpuReduceCompatible(axes: []const u8, input_shape: []const i64) bool {
    const rank = input_shape.len;
    if (rank == 0 or rank > 8 or axes.len == 0) return false;
    for (input_shape) |dim| {
        if (dim <= 0) return false;
    }
    var seen = @as([8]bool, @splat(false));
    for (axes) |ax| {
        if (ax >= rank or seen[ax]) return false;
        seen[ax] = true;
    }
    return true;
}

fn isGpuReduceLastDimCompatible(axes: []const u8, input_shape: []const i64) bool {
    return isGpuReduceCompatible(axes, input_shape) and axes.len == 1 and axes[0] == input_shape.len - 1;
}

fn isGpuBroadcastInDimCompatible(target_shape: []const i64, broadcast_axes: []const u8, input_shape: []const i64) bool {
    const out_rank = target_shape.len;
    const in_rank = input_shape.len;
    if (out_rank == 0 or out_rank > 8 or in_rank > 8 or broadcast_axes.len != in_rank) return false;
    var seen = @as([8]bool, @splat(false));
    for (target_shape) |dim| {
        if (dim <= 0) return false;
    }
    for (input_shape, 0..) |dim, in_d| {
        if (dim <= 0) return false;
        const ax = broadcast_axes[in_d];
        if (ax >= out_rank or seen[ax]) return false;
        seen[ax] = true;
        if (dim != 1 and dim != target_shape[ax]) return false;
    }
    return true;
}

/// Contiguous per-layer KV cache for autoregressive generation.
/// Pre-allocates [max_len * kv_dim] per layer. Tokens are appended
/// each step; attention reads from 0..total_len.
pub const WasmKvCache = struct {
    k_cache: [][]f32, // [num_layers]
    v_cache: [][]f32,
    cached_len: usize, // tokens from prior steps
    step_tokens: usize, // tokens in current forward step
    max_len: usize,
    num_layers: u32,
    num_kv_heads: u32,
    head_dim: u32,
    alloc: std.mem.Allocator,

    pub fn init(a: std.mem.Allocator, num_layers: u32, num_kv_heads: u32, head_dim: u32, max_len: usize) !WasmKvCache {
        const kv_dim = @as(usize, num_kv_heads) * head_dim;
        const layer_size = max_len * kv_dim;
        const k = try a.alloc([]f32, num_layers);
        const v = try a.alloc([]f32, num_layers);
        for (0..num_layers) |i| {
            k[i] = try a.alloc(f32, layer_size);
            @memset(k[i], 0);
            v[i] = try a.alloc(f32, layer_size);
            @memset(v[i], 0);
        }
        return .{
            .k_cache = k,
            .v_cache = v,
            .cached_len = 0,
            .step_tokens = 0,
            .max_len = max_len,
            .num_layers = num_layers,
            .num_kv_heads = num_kv_heads,
            .head_dim = head_dim,
            .alloc = a,
        };
    }

    pub fn deinit(self: *WasmKvCache) void {
        for (0..self.num_layers) |i| {
            self.alloc.free(self.k_cache[i]);
            self.alloc.free(self.v_cache[i]);
        }
        self.alloc.free(self.k_cache);
        self.alloc.free(self.v_cache);
    }

    pub fn reset(self: *WasmKvCache) void {
        self.cached_len = 0;
        self.step_tokens = 0;
    }

    /// Write new K/V for a layer at the current write offset (cached_len).
    pub fn appendKv(self: *WasmKvCache, layer: usize, k_data: []const f32, v_data: []const f32) void {
        const kv_dim = @as(usize, self.num_kv_heads) * self.head_dim;
        const off = self.cached_len * kv_dim;
        @memcpy(self.k_cache[layer][off .. off + k_data.len], k_data);
        @memcpy(self.v_cache[layer][off .. off + v_data.len], v_data);
    }

    /// Advance cached_len after a full forward pass completes.
    pub fn commitStep(self: *WasmKvCache) void {
        self.cached_len += self.step_tokens;
        self.step_tokens = 0;
    }

    /// Total K/V length for attention (cached + current step).
    pub fn totalLen(self: WasmKvCache) usize {
        return self.cached_len + self.step_tokens;
    }

    /// Truncate cache to new_len tokens (for speculative decoding rollback).
    pub fn truncateTo(self: *WasmKvCache, new_len: usize) void {
        self.cached_len = @min(new_len, self.cached_len);
        self.step_tokens = 0;
    }
};

/// GPU-resident KV cache. K/V buffers live on the GPU; only new tokens
/// are uploaded each decode step via writeBufferAtOffset.
pub const GpuKvCache = struct {
    k_gpu: []u32, // [num_layers] GPU buffer IDs
    v_gpu: []u32,
    key_format: GpuKvKeyFormat = .f32,
    value_format: GpuKvValueFormat = .f32,
    key_row_bytes: u32 = 0,
    value_row_bytes: u32 = 0,
    num_layers: u32,
    num_kv_heads: u32,
    head_dim: u32,
    max_len: usize,
    cached_len: usize,
    alloc: std.mem.Allocator,

    pub fn init(a: std.mem.Allocator, num_layers: u32, num_kv_heads: u32, head_dim: u32, max_len: usize) !GpuKvCache {
        return initWithFormats(a, num_layers, num_kv_heads, head_dim, max_len, .f32, .f32);
    }

    pub fn initWithFormats(a: std.mem.Allocator, num_layers: u32, num_kv_heads: u32, head_dim: u32, max_len: usize, key_format: GpuKvKeyFormat, value_format: GpuKvValueFormat) !GpuKvCache {
        if (!build_options.enable_webgpu) unreachable;

        const key_row_bytes = try gpuKvKeyRowBytes(key_format, num_kv_heads, head_dim);
        const value_row_bytes = try gpuKvValueRowBytes(value_format, num_kv_heads, head_dim);
        const key_layer_bytes: u32 = @intCast(alignForward4(max_len * key_row_bytes));
        const value_layer_bytes: u32 = @intCast(alignForward4(max_len * value_row_bytes));
        const k = try a.alloc(wasm_extern.GpuBufferId, num_layers);
        const v = try a.alloc(wasm_extern.GpuBufferId, num_layers);
        for (0..num_layers) |i| {
            k[i] = wasm_extern.createBuffer(key_layer_bytes);
            v[i] = wasm_extern.createBuffer(value_layer_bytes);
        }
        return .{
            .k_gpu = k,
            .v_gpu = v,
            .key_format = key_format,
            .value_format = value_format,
            .key_row_bytes = @intCast(key_row_bytes),
            .value_row_bytes = @intCast(value_row_bytes),
            .num_layers = num_layers,
            .num_kv_heads = num_kv_heads,
            .head_dim = head_dim,
            .max_len = max_len,
            .cached_len = 0,
            .alloc = a,
        };
    }

    pub fn deinit(self: *GpuKvCache) void {
        if (!build_options.enable_webgpu) unreachable;
        for (0..self.num_layers) |i| {
            wasm_extern.freeBuffer(self.k_gpu[i]);
            wasm_extern.freeBuffer(self.v_gpu[i]);
        }
        self.alloc.free(self.k_gpu);
        self.alloc.free(self.v_gpu);
    }

    pub fn reset(self: *GpuKvCache) void {
        self.cached_len = 0;
    }

    /// Truncate cache to new_len tokens (for speculative decoding rollback).
    pub fn truncateTo(self: *GpuKvCache, new_len: usize) void {
        self.cached_len = @min(new_len, self.cached_len);
    }

    /// Upload new K/V data for a layer at the current cached_len offset.
    pub fn appendKv(self: *GpuKvCache, layer: usize, k_data: []const f32, v_data: []const f32) !void {
        if (!build_options.enable_webgpu) unreachable;
        const kv_dim: usize = @as(usize, self.num_kv_heads) * self.head_dim;
        if (k_data.len != v_data.len or k_data.len % kv_dim != 0) return error.InvalidKvRowWidth;
        const rows = k_data.len / kv_dim;
        const key_row_bytes: usize = @intCast(self.key_row_bytes);
        const value_row_bytes: usize = @intCast(self.value_row_bytes);
        const key_offset_bytes: u32 = @intCast(self.cached_len * key_row_bytes);
        const value_offset_bytes: u32 = @intCast(self.cached_len * value_row_bytes);

        switch (self.key_format) {
            .f32 => {
                const k_bytes = @as([*]const u8, @ptrCast(k_data.ptr))[0 .. k_data.len * @sizeOf(f32)];
                wasm_extern.writeBufferAtOffset(self.k_gpu[layer], key_offset_bytes, k_bytes);
            },
            .polar4 => {
                const encoded = try self.alloc.alloc(u8, rows * key_row_bytes);
                defer self.alloc.free(encoded);
                for (0..rows) |row| {
                    try turboquant.encodePolar4Key(
                        k_data[row * kv_dim ..][0..kv_dim],
                        encoded[row * key_row_bytes ..][0..key_row_bytes],
                        self.num_kv_heads,
                        self.head_dim,
                    );
                }
                wasm_extern.writeBufferAtOffset(self.k_gpu[layer], key_offset_bytes, encoded);
            },
            .turbo3 => {
                const encoded = try self.alloc.alloc(u8, rows * key_row_bytes);
                defer self.alloc.free(encoded);
                const base_bytes = turboquant.turbo3KeyBytes(self.num_kv_heads, self.head_dim);
                const residual_bytes = turboquant.turbo3ResidualBytes(self.num_kv_heads, self.head_dim);
                for (0..rows) |row| {
                    const row_dst = encoded[row * key_row_bytes ..][0..key_row_bytes];
                    try turboquant.encodeTurbo3Key(
                        k_data[row * kv_dim ..][0..kv_dim],
                        row_dst[0..base_bytes],
                        self.num_kv_heads,
                        self.head_dim,
                    );
                    try turboquant.encodeTurbo3ResidualSketch(
                        k_data[row * kv_dim ..][0..kv_dim],
                        row_dst[0..base_bytes],
                        row_dst[base_bytes..][0..residual_bytes],
                        self.num_kv_heads,
                        self.head_dim,
                    );
                }
                wasm_extern.writeBufferAtOffset(self.k_gpu[layer], key_offset_bytes, encoded);
            },
        }

        switch (self.value_format) {
            .f32 => {
                const v_bytes = @as([*]const u8, @ptrCast(v_data.ptr))[0 .. v_data.len * @sizeOf(f32)];
                wasm_extern.writeBufferAtOffset(self.v_gpu[layer], value_offset_bytes, v_bytes);
            },
            .int8_per_head => return error.UnsupportedKvDType,
        }
    }

    pub fn appendKvFromGpu(self: *GpuKvCache, layer: usize, k_src_gpu: wasm_extern.GpuBufferId, v_src_gpu: wasm_extern.GpuBufferId, src_offset_elems: usize, elem_count: usize) !void {
        if (!build_options.enable_webgpu) unreachable;
        const kv_dim: usize = @as(usize, self.num_kv_heads) * self.head_dim;
        if (elem_count % kv_dim != 0) return error.InvalidKvRowWidth;
        if (self.key_format != .f32 or self.value_format != .f32) return error.UnsupportedKvDType;

        const rows = elem_count / kv_dim;
        const key_row_bytes: usize = @intCast(self.key_row_bytes);
        const value_row_bytes: usize = @intCast(self.value_row_bytes);
        const key_offset_bytes: u32 = @intCast(self.cached_len * key_row_bytes);
        const value_offset_bytes: u32 = @intCast(self.cached_len * value_row_bytes);
        const src_offset_bytes: u32 = @intCast(src_offset_elems * @sizeOf(f32));
        const copy_bytes: u32 = @intCast(rows * kv_dim * @sizeOf(f32));

        wasm_extern.copyBufferToBuffer(k_src_gpu, src_offset_bytes, self.k_gpu[layer], key_offset_bytes, copy_bytes);
        wasm_extern.copyBufferToBuffer(v_src_gpu, src_offset_bytes, self.v_gpu[layer], value_offset_bytes, copy_bytes);
    }
};

fn alignForward4(value: usize) usize {
    return (value + 3) & ~@as(usize, 3);
}

fn gpuKvKeyRowBytes(format: GpuKvKeyFormat, num_kv_heads: u32, head_dim: u32) !usize {
    const kv_dim: usize = @as(usize, num_kv_heads) * head_dim;
    return switch (format) {
        .f32 => kv_dim * @sizeOf(f32),
        .polar4 => blk: {
            const bytes = turboquant.polar4KeyBytes(num_kv_heads, head_dim);
            if (bytes == 0) return error.UnsupportedKvHeadDim;
            break :blk bytes;
        },
        .turbo3 => blk: {
            const base_bytes = turboquant.turbo3KeyBytes(num_kv_heads, head_dim);
            const residual_bytes = turboquant.turbo3ResidualBytes(num_kv_heads, head_dim);
            if (base_bytes == 0 or residual_bytes == 0) return error.UnsupportedKvHeadDim;
            break :blk base_bytes + residual_bytes;
        },
    };
}

fn gpuKvValueRowBytes(format: GpuKvValueFormat, num_kv_heads: u32, head_dim: u32) !usize {
    const kv_dim: usize = @as(usize, num_kv_heads) * head_dim;
    return switch (format) {
        .f32 => kv_dim * @sizeOf(f32),
        .int8_per_head => return error.UnsupportedKvDType,
    };
}

pub const WasmCompute = struct {
    const GraphPlanBuffer = struct {
        id: wasm_extern.GpuBufferId,
        bytes: usize,
    };

    const DecoderRuntimeAbsoluteEmbeddings = struct {
        token_embedding: *WasmBuf,
        position_embedding: *WasmBuf,
        vocab_size: usize,
        max_position_embeddings: usize,
        hidden_size: usize,
    };

    const DecoderRuntimeLayerNormSlot = struct {
        weight: *WasmBuf,
        bias: *WasmBuf,
        hidden_size: usize,
    };

    const DecoderRuntimeRmsNormSlot = struct {
        weight: *WasmBuf,
        hidden_size: usize,
    };

    const DecoderRuntimeLinearSlot = struct {
        weight: *WasmBuf,
        bias: *WasmBuf,
        in_dim: usize,
        out_dim: usize,
    };

    allocator: std.mem.Allocator,
    weights: std.StringHashMap(*WasmBuf),
    gpu_weights: GpuWeightStore,
    use_gpu: bool,
    active_kv_cache: ?*WasmKvCache = null,
    active_gpu_kv_cache: ?*GpuKvCache = null,
    graph_plan_buffers: std.AutoHashMap(usize, GraphPlanBuffer),
    decoder_runtime_embeddings: ?DecoderRuntimeAbsoluteEmbeddings = null,
    decoder_runtime_layer_norm_slots: std.AutoHashMap(usize, DecoderRuntimeLayerNormSlot),
    decoder_runtime_rms_norm_slots: std.AutoHashMap(usize, DecoderRuntimeRmsNormSlot),
    decoder_runtime_linear_slots: std.AutoHashMap(usize, DecoderRuntimeLinearSlot),

    pub fn init(allocator: std.mem.Allocator) WasmCompute {
        return .{
            .allocator = allocator,
            .weights = std.StringHashMap(*WasmBuf).init(allocator),
            .gpu_weights = GpuWeightStore.init(allocator),
            .use_gpu = wasm_extern.isAvailable(),
            .graph_plan_buffers = std.AutoHashMap(usize, GraphPlanBuffer).init(allocator),
            .decoder_runtime_layer_norm_slots = std.AutoHashMap(usize, DecoderRuntimeLayerNormSlot).init(allocator),
            .decoder_runtime_rms_norm_slots = std.AutoHashMap(usize, DecoderRuntimeRmsNormSlot).init(allocator),
            .decoder_runtime_linear_slots = std.AutoHashMap(usize, DecoderRuntimeLinearSlot).init(allocator),
        };
    }

    pub fn registerWeight(self: *WasmCompute, name: []const u8, data: []f32) void {
        const buf = WasmBuf.fromSlice(self.allocator, data, false);
        if (build_options.enable_webgpu and self.use_gpu) {
            _ = self.gpu_weights.ensureResident(buf) catch @panic("OOM");
        }
        self.weights.put(name, buf) catch @panic("OOM");
    }

    pub fn registerF16Weight(self: *WasmCompute, name: []const u8, data: []f16) void {
        const buf = WasmBuf.fromF16Slice(self.allocator, data);
        if (build_options.enable_webgpu and self.use_gpu) {
            _ = self.gpu_weights.ensureResident(buf) catch @panic("OOM");
        }
        self.weights.put(name, buf) catch @panic("OOM");
    }

    pub fn registerQuantizedWeight(self: *WasmCompute, name: []const u8, raw: []const u8, qtype: tensor_types.TensorType, n_elements: usize) void {
        const buf = WasmBuf.fromQuantized(self.allocator, raw, qtype, n_elements);
        if (build_options.enable_webgpu and self.use_gpu) {
            _ = self.gpu_weights.ensureResident(buf) catch @panic("OOM");
            if (supportsGpuQuantMatmul(qtype)) {
                buf.dropHostQuantizedCopy();
            }
        }
        self.weights.put(name, buf) catch @panic("OOM");
    }

    pub fn registerGpuQuantizedWeight(self: *WasmCompute, name: []const u8, gpu_buf: wasm_extern.GpuBufferId, qtype: tensor_types.TensorType, n_elements: usize) void {
        const buf = WasmBuf.fromQuantizedGpuResident(self.allocator, qtype, n_elements);
        self.gpu_weights.putResident(buf, gpu_buf) catch @panic("OOM");
        self.weights.put(name, buf) catch @panic("OOM");
    }

    pub fn computeBackend(self: *WasmCompute) ComputeBackend {
        return .{ .ptr = @ptrCast(self), .vtable = &vtable };
    }

    fn getWeightOp(ctx: *anyopaque, name: []const u8) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const buf = self.weights.get(name) orelse return error.WeightNotFound;
        return fromBuf(buf);
    }

    fn freeTensorOp(ctx: *anyopaque, tensor: CT) void {
        _ = ctx;
        const buf = toBuf(tensor);
        // Don't free weight tensors (not owned)
        if (buf.owned) {
            buf.deinit();
        }
    }

    fn embeddingLookupOp(ctx: *anyopaque, weight: CT, ids: []const i64, total: usize, dim: usize) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const w = toBuf(weight);
        const out = try self.allocator.alloc(f32, total * dim);
        if (w.f16_data) |f16d| {
            for (0..total) |i| {
                const idx: usize = @intCast(ids[i]);
                const src = f16d[idx * dim ..][0..dim];
                const dst = out[i * dim ..][0..dim];
                for (src, 0..) |v, j| dst[j] = @floatCast(v);
            }
        } else {
            for (0..total) |i| {
                const idx: usize = @intCast(ids[i]);
                @memcpy(out[i * dim ..][0..dim], w.data[idx * dim ..][0..dim]);
            }
        }
        return fromBuf(try setBufShape2D(WasmBuf.fromSlice(self.allocator, out, true), total, dim));
    }

    fn embeddingLookupTensorOp(ctx: *anyopaque, weight: CT, ids_ct: CT, total: usize, dim: usize) anyerror!?CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const ids_buf = toBuf(ids_ct);
        const ids_i32 = ids_buf.i32_data orelse return null;
        if (ids_i32.len != total) return error.UnexpectedOutputShape;

        const ids = try self.allocator.alloc(i64, total);
        defer self.allocator.free(ids);
        for (ids_i32, 0..) |id, i| ids[i] = id;
        return try embeddingLookupOp(ctx, weight, ids, total, dim);
    }

    fn takeRowsOp(ctx: *anyopaque, request: *const ops.TakeRowsRequest) anyerror!?CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        if (request.rows == 0 or request.dim == 0) {
            const out = try self.allocator.alloc(f32, request.rows * request.dim);
            return fromBuf(try setBufShape2D(WasmBuf.fromSlice(self.allocator, out, true), request.rows, request.dim));
        }

        const input = toBuf(request.input);
        const total_rows = @divExact(input.len, request.dim);
        if (total_rows * request.dim != input.len) return error.UnexpectedOutputShape;
        if (request.row_ids.len != request.rows) return error.UnexpectedOutputShape;

        const out = try self.allocator.alloc(f32, request.rows * request.dim);
        for (request.row_ids, 0..) |row_id, out_row| {
            const src_row: usize = row_id;
            if (src_row >= total_rows) return error.UnexpectedOutputShape;
            @memcpy(
                out[out_row * request.dim ..][0..request.dim],
                input.data[src_row * request.dim ..][0..request.dim],
            );
        }
        return fromBuf(try setBufShape2D(WasmBuf.fromSlice(self.allocator, out, true), request.rows, request.dim));
    }

    const wasm_vec_len = web_profile.simd_f32_lanes;

    fn shouldPreferGpuLinear(self: *WasmCompute, inp: *WasmBuf, weight: *WasmBuf, rows: usize, out_dim: usize) bool {
        if (!(build_options.enable_webgpu and self.use_gpu)) return false;
        return inp.gpu_tensor != null or self.gpu_weights.hasResident(weight) or rows * out_dim >= WEBGPU_MATMUL_THRESHOLD;
    }

    fn shouldPreferGpuNorm(self: *WasmCompute, inp: *WasmBuf, dim: usize) bool {
        if (!(build_options.enable_webgpu and self.use_gpu)) return false;
        return inp.gpu_tensor != null or (dim >= 4096 and inp.len >= 65536);
    }

    fn shouldPreferGpuElementwise(self: *WasmCompute, a: *WasmBuf, b: ?*WasmBuf, len: usize) bool {
        if (!(build_options.enable_webgpu and self.use_gpu)) return false;
        if (len == 0) return false;
        if (a.gpu_tensor != null) return true;
        if (b != null and b.?.gpu_tensor != null) return true;
        return len >= WEBGPU_MATMUL_THRESHOLD;
    }

    fn linearOp(ctx: *anyopaque, input: CT, weight: CT, bias_ct: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const inp = toBuf(input);
        const w = toBuf(weight);
        const b = toBuf(bias_ct);
        const out = try self.allocator.alloc(f32, rows * out_dim);
        // Y = X @ W^T + bias
        if (shouldPreferGpuLinear(self, inp, w, rows, out_dim)) {
            var inp_gpu = GpuInputTensor.fromBuf(inp);
            defer inp_gpu.deinit();
            if (w.quant_type) |qt| {
                if (supportsGpuQuantMatmul(qt)) {
                    switch (qt) {
                        .known => |k| {
                            const w_gpu_buf = try self.gpu_weights.ensureResident(w);
                            var out_gpu = gpuSgemmTransBQuant(rows, out_dim, in_dim, inp_gpu.id, w_gpu_buf, out, k);
                            defer out_gpu.deinit();
                            var bias_gpu = GpuInputTensor.fromBuf(b);
                            defer bias_gpu.deinit();
                            var biased_gpu = gpuBinaryBroadcast(out_gpu.id, bias_gpu.id, out.len, b.len, out, .add);
                            return fromBuf(try setBufShape2D(WasmBuf.fromSliceWithGpu(self.allocator, out, true, biased_gpu.detach(), true), rows, out_dim));
                        },
                        else => {
                            const w_view = try w.viewF32(self.allocator);
                            defer if (w_view.allocated) self.allocator.free(@constCast(w_view.data));
                            linalg.sgemmTransBSync(rows, out_dim, in_dim, 1.0, inp.data, w_view.data, 0.0, out);
                        },
                    }
                } else {
                    const w_view = try w.viewF32(self.allocator);
                    defer if (w_view.allocated) self.allocator.free(@constCast(w_view.data));
                    linalg.sgemmTransBSync(rows, out_dim, in_dim, 1.0, inp.data, w_view.data, 0.0, out);
                }
            } else {
                const w_gpu_buf = try self.gpu_weights.ensureResident(w);
                var out_gpu = gpuSgemmTransB(rows, out_dim, in_dim, inp_gpu.id, w_gpu_buf, out);
                defer out_gpu.deinit();
                var bias_gpu = GpuInputTensor.fromBuf(b);
                defer bias_gpu.deinit();
                var biased_gpu = gpuBinaryBroadcast(out_gpu.id, bias_gpu.id, out.len, b.len, out, .add);
                return fromBuf(try setBufShape2D(WasmBuf.fromSliceWithGpu(self.allocator, out, true, biased_gpu.detach(), true), rows, out_dim));
            }
        } else {
            const w_view = try w.viewF32(self.allocator);
            defer if (w_view.allocated) self.allocator.free(@constCast(w_view.data));
            linalg.sgemmTransBSync(rows, out_dim, in_dim, 1.0, inp.data, w_view.data, 0.0, out);
        }
        // Add bias
        const b_view = try b.viewF32(self.allocator);
        defer if (b_view.allocated) self.allocator.free(@constCast(b_view.data));
        const VEC = wasm_vec_len;
        for (0..rows) |r| {
            const row = out[r * out_dim ..][0..out_dim];
            var j: usize = 0;
            while (j + VEC <= out_dim) : (j += VEC) {
                const v: @Vector(VEC, f32) = row[j..][0..VEC].*;
                const bv: @Vector(VEC, f32) = b_view.data[j..][0..VEC].*;
                row[j..][0..VEC].* = v + bv;
            }
            while (j < out_dim) : (j += 1) {
                row[j] += b_view.data[j];
            }
        }
        return fromBuf(try setBufShape2D(WasmBuf.fromSlice(self.allocator, out, true), rows, out_dim));
    }

    fn linearNoBiasOp(ctx: *anyopaque, input: CT, weight: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const inp = toBuf(input);
        const w = toBuf(weight);
        const out = try self.allocator.alloc(f32, rows * out_dim);

        // GPU path for quantized weights (keep quantized in GPU store and dequant in shader).
        if (shouldPreferGpuLinear(self, inp, w, rows, out_dim)) {
            var inp_gpu = GpuInputTensor.fromBuf(inp);
            defer inp_gpu.deinit();
            if (w.quant_type) |qt| {
                if (supportsGpuQuantMatmul(qt)) {
                    switch (qt) {
                        .known => |k| {
                            const w_gpu_buf = try self.gpu_weights.ensureResident(w);
                            var out_gpu = gpuSgemmTransBQuant(rows, out_dim, in_dim, inp_gpu.id, w_gpu_buf, out, k);
                            return fromBuf(try setBufShape2D(WasmBuf.fromSliceWithGpu(self.allocator, out, true, out_gpu.detach(), true), rows, out_dim));
                        },
                        else => {},
                    }
                }
            }
        }

        const w_view = try w.viewF32(self.allocator);
        defer if (w_view.allocated) self.allocator.free(@constCast(w_view.data));

        if (shouldPreferGpuLinear(self, inp, w, rows, out_dim)) {
            const w_gpu_buf = try self.gpu_weights.ensureResident(w);
            var inp_gpu = GpuInputTensor.fromBuf(inp);
            defer inp_gpu.deinit();
            var out_gpu = gpuSgemmTransB(rows, out_dim, in_dim, inp_gpu.id, w_gpu_buf, out);
            return fromBuf(try setBufShape2D(WasmBuf.fromSliceWithGpu(self.allocator, out, true, out_gpu.detach(), true), rows, out_dim));
        } else {
            linalg.sgemmTransBSync(rows, out_dim, in_dim, 1.0, inp.data, w_view.data, 0.0, out);
        }
        return fromBuf(try setBufShape2D(WasmBuf.fromSlice(self.allocator, out, true), rows, out_dim));
    }

    fn validatePlannedQuantLinear(
        weight: CT,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
        plan: ops.OperatorPlan,
    ) !void {
        switch (plan) {
            .quant_matmul => |quant| {
                if (quant.operator == .fallback or
                    quant.rows != rows or
                    quant.in_dim != in_dim or
                    quant.out_dim != out_dim)
                {
                    return error.InvalidPartitionPlan;
                }
                const weight_buf = toBuf(weight);
                const qtype = weight_buf.quant_type orelse return error.InvalidPartitionPlan;
                if (!supportsGpuQuantMatmul(qtype)) return error.UnsupportedTensorType;
                const format = quantFormatFromTensorType(qtype) orelse return error.InvalidPartitionPlan;
                if (format != quant.format) return error.InvalidPartitionPlan;
            },
            else => return error.InvalidPartitionPlan,
        }
    }

    fn linearNoBiasPlannedOp(
        ctx: *anyopaque,
        request: *const ops.LinearNoBiasPlannedRequest,
    ) anyerror!CT {
        try validatePlannedQuantLinear(
            request.weight,
            request.rows,
            request.in_dim,
            request.out_dim,
            request.operator_plan,
        );
        return linearNoBiasOp(
            ctx,
            request.input,
            request.weight,
            request.rows,
            request.in_dim,
            request.out_dim,
        );
    }

    fn layerNormOp(ctx: *anyopaque, input: CT, gamma: CT, beta: CT, dim: usize, eps: f32) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const inp = toBuf(input);
        const g = toBuf(gamma);
        const b = toBuf(beta);

        // GPU path: dispatch when dim is large enough to amortize overhead
        if (shouldPreferGpuNorm(self, inp, dim)) {
            const total_rows: u32 = @intCast(inp.len / dim);

            var inp_gpu = GpuInputTensor.fromBuf(inp);
            defer inp_gpu.deinit();
            const g_buf = try self.gpu_weights.ensureResident(g);
            const b_buf = try self.gpu_weights.ensureResident(b);
            var out_buf = GpuTensor.create(inp.len * @sizeOf(f32));

            wasm_extern.layerNorm(inp_gpu.id, g_buf, b_buf, out_buf.id, total_rows, @intCast(dim), @bitCast(eps));

            const out = try self.allocator.alloc(f32, inp.len);
            return fromBuf(try copyBufShape(WasmBuf.fromSliceWithGpu(self.allocator, out, true, out_buf.detach(), true), inp));
        }

        const g_view = try g.viewF32(self.allocator);
        defer if (g_view.allocated) self.allocator.free(@constCast(g_view.data));
        const b_view = try b.viewF32(self.allocator);
        defer if (b_view.allocated) self.allocator.free(@constCast(b_view.data));
        const out = try self.allocator.alloc(f32, inp.len);
        @memcpy(out, inp.data);
        activations.layerNorm(out, g_view.data, b_view.data, dim, eps);
        return fromBuf(try copyBufShape(WasmBuf.fromSlice(self.allocator, out, true), inp));
    }

    fn rmsNormOp(ctx: *anyopaque, input: CT, weight: CT, dim: usize, eps: f32) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const inp = toBuf(input);
        const w = toBuf(weight);

        // GPU path: dispatch when dim is large enough to amortize overhead
        if (shouldPreferGpuNorm(self, inp, dim)) {
            const total_rows: u32 = @intCast(inp.len / dim);

            var inp_gpu = GpuInputTensor.fromBuf(inp);
            defer inp_gpu.deinit();
            const w_buf = try self.gpu_weights.ensureResident(w);
            var out_buf = GpuTensor.create(inp.len * @sizeOf(f32));

            wasm_extern.rmsNorm(inp_gpu.id, w_buf, out_buf.id, total_rows, @intCast(dim), @bitCast(eps));

            const out = try self.allocator.alloc(f32, inp.len);
            return fromBuf(try copyBufShape(WasmBuf.fromSliceWithGpu(self.allocator, out, true, out_buf.detach(), true), inp));
        }

        const w_view = try w.viewF32(self.allocator);
        defer if (w_view.allocated) self.allocator.free(@constCast(w_view.data));
        const out = try self.allocator.alloc(f32, inp.len);
        @memcpy(out, inp.data);
        activations.rmsNorm(out, w_view.data, dim, eps);
        return fromBuf(try copyBufShape(WasmBuf.fromSlice(self.allocator, out, true), inp));
    }

    fn geluOp(ctx: *anyopaque, input: CT) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const inp = toBuf(input);
        if (shouldPreferGpuElementwise(self, inp, null, inp.len)) {
            const out = try self.allocator.alloc(f32, inp.len);
            var inp_gpu = GpuInputTensor.fromBuf(inp);
            defer inp_gpu.deinit();
            var out_gpu = gpuGelu(inp_gpu.id, out);
            return fromBuf(try copyBufShape(WasmBuf.fromSliceWithGpu(self.allocator, out, true, out_gpu.detach(), true), inp));
        }
        const out = try self.allocator.alloc(f32, inp.len);
        @memcpy(out, inp.data);
        activations.gelu(out);
        return fromBuf(try copyBufShape(WasmBuf.fromSlice(self.allocator, out, true), inp));
    }

    fn geluNewOp(ctx: *anyopaque, input: CT) anyerror!CT {
        return geluOp(ctx, input);
    }

    fn reluOp(ctx: *anyopaque, input: CT) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const inp = toBuf(input);
        const out = try self.allocator.alloc(f32, inp.len);
        @memcpy(out, inp.data);
        activations.relu(out);
        return fromBuf(try copyBufShape(WasmBuf.fromSlice(self.allocator, out, true), inp));
    }

    fn siluOp(ctx: *anyopaque, input: CT) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const inp = toBuf(input);
        const out = try self.allocator.alloc(f32, inp.len);
        @memcpy(out, inp.data);
        activations.silu(out);
        return fromBuf(try copyBufShape(WasmBuf.fromSlice(self.allocator, out, true), inp));
    }

    fn quickGeluOp(ctx: *anyopaque, input: CT) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const inp = toBuf(input);
        const out = try self.allocator.alloc(f32, inp.len);
        @memcpy(out, inp.data);
        activations.quickGelu(out);
        return fromBuf(WasmBuf.fromSlice(self.allocator, out, true));
    }

    fn sigmoidOp(ctx: *anyopaque, input: CT) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const inp = toBuf(input);
        const out = try self.allocator.alloc(f32, inp.len);
        @memcpy(out, inp.data);
        activations.sigmoid(out);
        return fromBuf(WasmBuf.fromSlice(self.allocator, out, true));
    }

    fn tanhOp(ctx: *anyopaque, input: CT) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const inp = toBuf(input);
        const out = try self.allocator.alloc(f32, inp.len);
        @memcpy(out, inp.data);
        for (out) |*x| x.* = std.math.tanh(x.*);
        return fromBuf(WasmBuf.fromSlice(self.allocator, out, true));
    }

    fn addOp(ctx: *anyopaque, a: CT, b_ct: CT) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const a_buf = toBuf(a);
        const b_buf = toBuf(b_ct);
        const out_len = @max(a_buf.len, b_buf.len);
        if (shouldPreferGpuElementwise(self, a_buf, b_buf, out_len) and isModuloBroadcastCompatible(a_buf.len, b_buf.len)) {
            const out = try self.allocator.alloc(f32, out_len);
            var a_gpu = GpuInputTensor.fromBuf(a_buf);
            defer a_gpu.deinit();
            var b_gpu = GpuInputTensor.fromBuf(b_buf);
            defer b_gpu.deinit();
            var out_gpu = gpuBinaryBroadcast(a_gpu.id, b_gpu.id, a_buf.len, b_buf.len, out, .add);
            const out_buf = WasmBuf.fromSliceWithGpu(self.allocator, out, true, out_gpu.detach(), true);
            return fromBuf(if (a_buf.len >= b_buf.len) try copyBufShape(out_buf, a_buf) else try copyBufShape(out_buf, b_buf));
        }
        const out = try primBinaryBroadcast(self.allocator, a_buf.data, b_buf.data, .add);
        const out_buf = WasmBuf.fromSlice(self.allocator, out, true);
        return fromBuf(if (a_buf.len >= b_buf.len) try copyBufShape(out_buf, a_buf) else try copyBufShape(out_buf, b_buf));
    }

    fn multiplyOp(ctx: *anyopaque, a: CT, b_ct: CT) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const a_buf = toBuf(a);
        const b_buf = toBuf(b_ct);
        const out_len = @max(a_buf.len, b_buf.len);
        if (shouldPreferGpuElementwise(self, a_buf, b_buf, out_len) and isModuloBroadcastCompatible(a_buf.len, b_buf.len)) {
            const out = try self.allocator.alloc(f32, out_len);
            var a_gpu = GpuInputTensor.fromBuf(a_buf);
            defer a_gpu.deinit();
            var b_gpu = GpuInputTensor.fromBuf(b_buf);
            defer b_gpu.deinit();
            var out_gpu = gpuBinaryBroadcast(a_gpu.id, b_gpu.id, a_buf.len, b_buf.len, out, .mul);
            const out_buf = WasmBuf.fromSliceWithGpu(self.allocator, out, true, out_gpu.detach(), true);
            return fromBuf(if (a_buf.len >= b_buf.len) try copyBufShape(out_buf, a_buf) else try copyBufShape(out_buf, b_buf));
        }
        const out = try primBinaryBroadcast(self.allocator, a_buf.data, b_buf.data, .mul);
        const out_buf = WasmBuf.fromSlice(self.allocator, out, true);
        return fromBuf(if (a_buf.len >= b_buf.len) try copyBufShape(out_buf, a_buf) else try copyBufShape(out_buf, b_buf));
    }

    fn concatOp(ctx: *anyopaque, a: CT, b_ct: CT, total: usize, dim_a: usize, dim_b: usize) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const a_buf = toBuf(a);
        const b_buf = toBuf(b_ct);
        const out_dim = dim_a + dim_b;
        const out = try self.allocator.alloc(f32, total * out_dim);
        for (0..total) |row| {
            @memcpy(out[row * out_dim ..][0..dim_a], a_buf.data[row * dim_a ..][0..dim_a]);
            @memcpy(out[row * out_dim + dim_a ..][0..dim_b], b_buf.data[row * dim_b ..][0..dim_b]);
        }
        return fromBuf(WasmBuf.fromSlice(self.allocator, out, true));
    }

    fn scaledDotProductAttentionOp(
        ctx: *anyopaque,
        Q_ct: CT,
        K_ct: CT,
        V_ct: CT,
        mask: []const i64,
        attn_bias_ct: ?CT,
        batch: usize,
        seq_len: usize,
        num_heads: usize,
        head_dim: usize,
    ) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const Q = toBuf(Q_ct);
        const K = toBuf(K_ct);
        const V = toBuf(V_ct);
        const total = batch * seq_len;
        const out_len = total * num_heads * head_dim;
        const scale: f32 = 1.0 / @sqrt(@as(f32, @floatFromInt(head_dim)));
        const attn_bias_view = if (attn_bias_ct) |ab| try toBuf(ab).viewF32(self.allocator) else null;
        defer if (attn_bias_view) |v| {
            if (v.allocated) self.allocator.free(v.data);
        };
        const attn_bias: ?[]const f32 = if (attn_bias_view) |v| v.data else null;

        const out = try self.allocator.alloc(f32, out_len);

        // GPU path: fused attention shader (avoids intermediate transfers)
        if (build_options.enable_webgpu and self.use_gpu and
            out_len >= WEBGPU_ATTN_THRESHOLD and seq_len <= WEBGPU_ATTN_MAX_SEQ and
            attn_bias == null)
        {
            var q_gpu = GpuInputTensor.fromBuf(Q);
            defer q_gpu.deinit();
            var k_gpu = GpuInputTensor.fromBuf(K);
            defer k_gpu.deinit();
            var v_gpu = GpuInputTensor.fromBuf(V);
            defer v_gpu.deinit();
            var out_gpu = gpuAttention(batch, seq_len, num_heads, head_dim, scale, q_gpu.id, k_gpu.id, v_gpu.id, mask, out);
            return fromBuf(try setBufShape2D(WasmBuf.fromSliceWithGpu(self.allocator, out, true, out_gpu.detach(), true), total, num_heads * head_dim));
        }

        // SIMD path: per-head attention on CPU
        const scores = try self.allocator.alloc(f32, seq_len * seq_len);
        defer self.allocator.free(scores);

        for (0..batch) |b| {
            for (0..num_heads) |h| {
                // scores = Q_head @ K_head^T, scaled
                for (0..seq_len) |qi| {
                    for (0..seq_len) |ki| {
                        var dot: f32 = 0.0;
                        for (0..head_dim) |d| {
                            const q_val = Q.data[(b * seq_len + qi) * num_heads * head_dim + h * head_dim + d];
                            const k_val = K.data[(b * seq_len + ki) * num_heads * head_dim + h * head_dim + d];
                            dot += q_val * k_val;
                        }
                        var s = dot * scale;
                        if (attn_bias) |bias| {
                            s += bias[h * seq_len * seq_len + qi * seq_len + ki];
                        }
                        scores[qi * seq_len + ki] = s;
                    }
                }

                // Apply mask: set masked positions to -inf
                for (0..seq_len) |qi| {
                    for (0..seq_len) |ki| {
                        if (mask[b * seq_len + ki] == 0) {
                            scores[qi * seq_len + ki] = -std.math.inf(f32);
                        }
                    }
                }

                // Softmax per row
                activations.softmax(scores, seq_len);

                // output = scores @ V_head
                for (0..seq_len) |qi| {
                    for (0..head_dim) |d| {
                        var sum: f32 = 0.0;
                        for (0..seq_len) |vi| {
                            sum += scores[qi * seq_len + vi] *
                                V.data[(b * seq_len + vi) * num_heads * head_dim + h * head_dim + d];
                        }
                        out[(b * seq_len + qi) * num_heads * head_dim + h * head_dim + d] = sum;
                    }
                }
            }
        }

        return fromBuf(try setBufShape2D(WasmBuf.fromSlice(self.allocator, out, true), total, num_heads * head_dim));
    }

    fn fromFloat32Op(ctx: *anyopaque, data: []const f32) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const out = try self.allocator.alloc(f32, data.len);
        @memcpy(out, data);
        return fromBuf(WasmBuf.fromSlice(self.allocator, out, true));
    }

    fn fromFloat32ShapeOp(ctx: *anyopaque, data: []const f32, shape_i32: []const i32) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const out = try self.allocator.alloc(f32, data.len);
        @memcpy(out, data);
        const buf = WasmBuf.fromSlice(self.allocator, out, true);
        const shape = try self.allocator.alloc(i64, shape_i32.len);
        for (shape_i32, 0..) |dim, i| shape[i] = dim;
        buf.setShapeOwned(shape);
        return fromBuf(buf);
    }

    fn fromInt32ShapeOp(ctx: *anyopaque, data: []const i32, shape_i32: []const i32) anyerror!?CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const out = try self.allocator.dupe(i32, data);
        const buf = WasmBuf.fromI32Slice(self.allocator, out);
        const shape = try self.allocator.alloc(i64, shape_i32.len);
        for (shape_i32, 0..) |dim, i| shape[i] = dim;
        buf.setShapeOwned(shape);
        return fromBuf(buf);
    }

    fn exportTensorDataOp(_: *anyopaque, tensor: CT, allocator: std.mem.Allocator) anyerror!?ops.ExportTensorData {
        const buf = toBuf(tensor);
        if (buf.i32_data) |data| {
            return .{
                .dtype = .i32,
                .payload = .{ .bytes = try allocator.dupe(u8, std.mem.sliceAsBytes(data)) },
            };
        }
        if (buf.quant_type != null) return null;
        if (buf.f16_data != null) return null;
        try buf.ensureHostData();
        return .{
            .dtype = .f32,
            .payload = .{ .bytes = try allocator.dupe(u8, std.mem.sliceAsBytes(buf.data)) },
        };
    }

    fn tensorDTypeOp(_: *anyopaque, tensor: CT) anyerror!tensor_mod.DType {
        const buf = toBuf(tensor);
        if (buf.i32_data != null) return .i32;
        if (buf.f16_data != null) return .f16;
        if (buf.quant_type != null) return .f32;
        return .f32;
    }

    fn evalTensorOp(_: *anyopaque, _: CT) anyerror!void {}

    fn tensorShapeOp(ctx: *anyopaque, tensor: CT, allocator: std.mem.Allocator) anyerror![]i64 {
        _ = ctx;
        const buf = toBuf(tensor);
        if (buf.shape) |shape| {
            return allocator.dupe(i64, shape);
        }
        return error.UnsupportedShape;
    }

    fn zeroTensorOp(ctx: *anyopaque, rows: usize, dim: usize) anyerror!?CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const out = try self.allocator.alloc(f32, rows * dim);
        @memset(out, 0.0);
        return fromBuf(try setBufShape2D(WasmBuf.fromSlice(self.allocator, out, true), rows, dim));
    }

    fn argmaxLastRowOp(ctx: *anyopaque, tensor: CT, rows: usize, dim: usize) anyerror!?u32 {
        _ = ctx;
        if (rows == 0 or dim == 0) return null;
        const buf = toBuf(tensor);
        if (buf.len != rows * dim) return error.UnexpectedOutputShape;
        const last_row = buf.data[(rows - 1) * dim ..][0..dim];
        var best_idx: u32 = 0;
        var best_val = last_row[0];
        for (last_row[1..], 1..) |v, i| {
            if (v > best_val) {
                best_val = v;
                best_idx = @intCast(i);
            }
        }
        return best_idx;
    }

    fn sampleLastRowOp(ctx: *anyopaque, request: *const ops.SampleLastRowRequest) anyerror!?u32 {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        if (request.rows == 0 or request.dim == 0) return null;
        const buf = toBuf(request.tensor);
        if (buf.len != request.rows * request.dim) return error.UnexpectedOutputShape;
        const last_row = buf.data[(request.rows - 1) * request.dim ..][0..request.dim];
        return @intCast(model_runtime.sampleTokenFromLogits(
            self.allocator,
            last_row,
            .{
                .temperature = request.temperature,
                .top_p = request.top_p,
                .top_k = @intCast(@min(request.top_k, @as(usize, @intCast(std.math.maxInt(i32))))),
                .min_p = request.min_p,
                .repetition_penalty = request.repetition_penalty,
                .frequency_penalty = request.frequency_penalty,
                .presence_penalty = request.presence_penalty,
            },
            request.token_history,
        ));
    }

    fn linearNoBiasArgmaxLastRowOp(ctx: *anyopaque, input: CT, weight: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!?u32 {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        if (rows == 0 or in_dim == 0 or out_dim == 0) return null;
        const inp = toBuf(input);
        const w = toBuf(weight);
        if (inp.len != rows * in_dim) return error.UnexpectedOutputShape;

        const w_view = try w.viewF32(self.allocator);
        defer if (w_view.allocated) self.allocator.free(@constCast(w_view.data));
        const last_row = inp.data[(rows - 1) * in_dim ..][0..in_dim];

        var best_idx: u32 = 0;
        var best_val = -std.math.inf(f32);
        for (0..out_dim) |out_idx| {
            const row = w_view.data[out_idx * in_dim ..][0..in_dim];
            var dot: f32 = 0.0;
            for (0..in_dim) |j| {
                dot += last_row[j] * row[j];
            }
            if (dot > best_val) {
                best_val = dot;
                best_idx = @intCast(out_idx);
            }
        }
        return best_idx;
    }

    fn linearNoBiasArgmaxLastRowTensorOp(ctx: *anyopaque, input: CT, weight: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!?CT {
        const token_id = (try linearNoBiasArgmaxLastRowOp(ctx, input, weight, rows, in_dim, out_dim)) orelse return null;
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const out = try self.allocator.alloc(f32, 1);
        out[0] = @floatFromInt(token_id);
        const buf = WasmBuf.fromSlice(self.allocator, out, true);
        const shape = try self.allocator.alloc(i64, 1);
        shape[0] = 1;
        buf.setShapeOwned(shape);
        return fromBuf(buf);
    }

    fn toFloat32Op(_: *anyopaque, tensor: CT, alloc: std.mem.Allocator) anyerror![]f32 {
        const buf = toBuf(tensor);
        if (buf.quant_type != null) {
            const view = try buf.viewF32(alloc);
            if (!view.allocated) {
                const out = try alloc.dupe(f32, view.data);
                return out;
            }
            return @constCast(view.data);
        }
        if (buf.i32_data) |i32d| {
            const out = try alloc.alloc(f32, i32d.len);
            for (i32d, 0..) |v, i| out[i] = @floatFromInt(v);
            return out;
        }
        if (buf.f16_data) |f16d| {
            const out = try alloc.alloc(f32, f16d.len);
            for (f16d, 0..) |v, i| out[i] = @floatCast(v);
            return out;
        }
        try buf.ensureHostData();
        const out = try alloc.alloc(f32, buf.len);
        @memcpy(out, buf.data);
        return out;
    }

    fn backendKindOp(_: *anyopaque) ops.BackendKind {
        return .wasm;
    }

    fn deinitBackendOp(ctx: *anyopaque) void {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        self.gpu_weights.deinit();
        var graph_plan_it = self.graph_plan_buffers.valueIterator();
        while (graph_plan_it.next()) |buffer| {
            if (buffer.id != wasm_extern.invalid_buffer) wasm_extern.freeBuffer(buffer.id);
        }
        self.graph_plan_buffers.deinit();
        self.decoder_runtime_layer_norm_slots.deinit();
        self.decoder_runtime_rms_norm_slots.deinit();
        self.decoder_runtime_linear_slots.deinit();
        var it = self.weights.valueIterator();
        while (it.next()) |buf| {
            buf.*.deinit();
        }
        self.weights.deinit();
    }

    fn noopPrefetch(_: *anyopaque, _: []const u8, _: u32) void {}
    fn noopDrain(_: *anyopaque, _: usize) void {}

    fn reserveGraphPlanSlotsOp(ctx: *anyopaque, slots: []const ops.GraphPlanSlot) anyerror!bool {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        if (!(build_options.enable_webgpu and self.use_gpu)) return false;

        for (slots) |slot| {
            const byte_len = if (slot.bytes == 0) @as(usize, @sizeOf(f32)) else slot.bytes;
            if (self.graph_plan_buffers.getPtr(slot.slot)) |existing| {
                if (existing.bytes >= byte_len and existing.id != wasm_extern.invalid_buffer) continue;
                if (existing.id != wasm_extern.invalid_buffer) wasm_extern.freeBuffer(existing.id);
                existing.* = .{
                    .id = wasm_extern.createBuffer(std.math.cast(u32, byte_len) orelse return error.OutOfMemory),
                    .bytes = byte_len,
                };
                continue;
            }
            try self.graph_plan_buffers.put(slot.slot, .{
                .id = wasm_extern.createBuffer(std.math.cast(u32, byte_len) orelse return error.OutOfMemory),
                .bytes = byte_len,
            });
        }
        return true;
    }

    fn decoderRuntimePrepareGreedyOp(ctx: *anyopaque, request: *const ops.DecoderRuntimeGreedyRequest) anyerror!bool {
        _ = ctx;
        return request.hidden_size != 0 and request.num_layers != 0 and request.vocab_size != 0;
    }

    fn decoderRuntimeResetStateOp(ctx: *anyopaque) anyerror!void {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        _ = self;
    }

    fn decoderRuntimePrepareAbsoluteEmbeddingsOp(ctx: *anyopaque, request: *const ops.DecoderRuntimePrepareAbsoluteEmbeddingsRequest) anyerror!bool {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        self.decoder_runtime_embeddings = .{
            .token_embedding = toBuf(request.token_embedding),
            .position_embedding = toBuf(request.position_embedding),
            .vocab_size = request.vocab_size,
            .max_position_embeddings = request.max_position_embeddings,
            .hidden_size = request.hidden_size,
        };
        return true;
    }

    fn decoderRuntimeEmbedAbsolutePositionOp(ctx: *anyopaque, request: *const ops.DecoderRuntimeEmbedAbsolutePositionRequest) anyerror!?CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const embeddings = self.decoder_runtime_embeddings orelse return null;
        if (request.hidden_size != embeddings.hidden_size) return error.UnexpectedOutputShape;
        if (request.token_id >= embeddings.vocab_size or request.position_id >= embeddings.max_position_embeddings) {
            return error.UnexpectedOutputShape;
        }

        const token = [_]i64{@intCast(request.token_id)};
        const position = [_]i64{@intCast(request.position_id)};
        const token_emb = try embeddingLookupOp(ctx, fromBuf(embeddings.token_embedding), &token, 1, request.hidden_size);
        defer freeTensorOp(ctx, token_emb);
        const position_emb = try embeddingLookupOp(ctx, fromBuf(embeddings.position_embedding), &position, 1, request.hidden_size);
        defer freeTensorOp(ctx, position_emb);
        return try addOp(ctx, token_emb, position_emb);
    }

    fn decoderRuntimePrepareLayerNormOp(ctx: *anyopaque, request: *const ops.DecoderRuntimePrepareLayerNormRequest) anyerror!bool {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        try self.decoder_runtime_layer_norm_slots.put(request.slot, .{
            .weight = toBuf(request.weight),
            .bias = toBuf(request.bias),
            .hidden_size = request.hidden_size,
        });
        return true;
    }

    fn decoderRuntimeApplyLayerNormOp(ctx: *anyopaque, request: *const ops.DecoderRuntimeApplyLayerNormRequest) anyerror!?CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const slot = self.decoder_runtime_layer_norm_slots.get(request.slot) orelse return null;
        if (slot.hidden_size != request.hidden_size) return error.UnexpectedOutputShape;
        return try layerNormOp(ctx, request.input, fromBuf(slot.weight), fromBuf(slot.bias), request.hidden_size, request.eps);
    }

    fn decoderRuntimePrepareRmsNormOp(ctx: *anyopaque, request: *const ops.DecoderRuntimePrepareRmsNormRequest) anyerror!bool {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        try self.decoder_runtime_rms_norm_slots.put(request.slot, .{
            .weight = toBuf(request.weight),
            .hidden_size = request.hidden_size,
        });
        return true;
    }

    fn decoderRuntimeApplyRmsNormOp(ctx: *anyopaque, request: *const ops.DecoderRuntimeApplyRmsNormRequest) anyerror!?CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const slot = self.decoder_runtime_rms_norm_slots.get(request.slot) orelse return null;
        if (slot.hidden_size != request.hidden_size) return error.UnexpectedOutputShape;
        return try rmsNormOp(ctx, request.input, fromBuf(slot.weight), request.hidden_size, request.eps);
    }

    fn decoderRuntimePrepareLinearOp(ctx: *anyopaque, request: *const ops.DecoderRuntimePrepareLinearRequest) anyerror!bool {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        _ = request.retain_dense_fallback;
        try self.decoder_runtime_linear_slots.put(request.slot, .{
            .weight = toBuf(request.weight),
            .bias = toBuf(request.bias),
            .in_dim = request.in_dim,
            .out_dim = request.out_dim,
        });
        return true;
    }

    fn decoderRuntimeApplyLinearOp(ctx: *anyopaque, request: *const ops.DecoderRuntimeApplyLinearRequest) anyerror!?CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const slot = self.decoder_runtime_linear_slots.get(request.slot) orelse return null;
        if (slot.in_dim != request.in_dim or slot.out_dim != request.out_dim) return error.UnexpectedOutputShape;
        return try linearOp(ctx, request.input, fromBuf(slot.weight), fromBuf(slot.bias), 1, request.in_dim, request.out_dim);
    }

    fn decoderRuntimeApplyLinearArgmaxOp(ctx: *anyopaque, request: *const ops.DecoderRuntimeApplyLinearArgmaxRequest) anyerror!?usize {
        const logits = (try decoderRuntimeApplyLinearOp(ctx, &.{
            .slot = request.slot,
            .input = request.input,
            .in_dim = request.in_dim,
            .out_dim = request.out_dim,
        })) orelse return null;
        defer freeTensorOp(ctx, logits);
        const token = (try argmaxLastRowOp(ctx, logits, 1, request.out_dim)) orelse return null;
        return token;
    }

    fn decoderRuntimeApplyLinearPairOp(ctx: *anyopaque, request: *const ops.DecoderRuntimeApplyLinearPairRequest) anyerror!?ops.LinearNoBiasPairResult {
        const first = (try decoderRuntimeApplyLinearOp(ctx, &.{
            .slot = request.slot_a,
            .input = request.input,
            .in_dim = request.in_dim,
            .out_dim = request.out_dim,
        })) orelse return null;
        errdefer freeTensorOp(ctx, first);

        const second = (try decoderRuntimeApplyLinearOp(ctx, &.{
            .slot = request.slot_b,
            .input = request.input,
            .in_dim = request.in_dim,
            .out_dim = request.out_dim,
        })) orelse return null;
        return .{ .first = first, .second = second };
    }

    fn decoderRuntimeApplyLinearQkvOp(ctx: *anyopaque, request: *const ops.DecoderRuntimeApplyLinearQkvRequest) anyerror!?ops.LinearNoBiasTripleResult {
        const first = (try decoderRuntimeApplyLinearOp(ctx, &.{
            .slot = request.q_slot,
            .input = request.input,
            .in_dim = request.in_dim,
            .out_dim = request.q_out_dim,
        })) orelse return null;
        errdefer freeTensorOp(ctx, first);

        const second = (try decoderRuntimeApplyLinearOp(ctx, &.{
            .slot = request.k_slot,
            .input = request.input,
            .in_dim = request.in_dim,
            .out_dim = request.kv_out_dim,
        })) orelse return null;
        errdefer freeTensorOp(ctx, second);

        const third = (try decoderRuntimeApplyLinearOp(ctx, &.{
            .slot = request.v_slot,
            .input = request.input,
            .in_dim = request.in_dim,
            .out_dim = request.kv_out_dim,
        })) orelse return null;
        return .{ .first = first, .second = second, .third = third };
    }

    fn decoderRuntimeApplyActivationOp(ctx: *anyopaque, request: *const ops.DecoderRuntimeApplyActivationRequest) anyerror!?CT {
        _ = request.dim;
        return switch (request.kind) {
            .gelu, .gelu_new => try geluOp(ctx, request.input),
            .silu => try siluOp(ctx, request.input),
            .relu => try reluOp(ctx, request.input),
            .quick_gelu => try quickGeluOp(ctx, request.input),
            .relu_squared => blk: {
                const relu = try reluOp(ctx, request.input);
                errdefer freeTensorOp(ctx, relu);
                break :blk try multiplyOp(ctx, relu, relu);
            },
        };
    }

    fn decoderRuntimeApplyAddOp(ctx: *anyopaque, request: *const ops.DecoderRuntimeApplyAddRequest) anyerror!?CT {
        _ = request.dim;
        return try addOp(ctx, request.lhs, request.rhs);
    }

    fn decoderRuntimeApplyLayerNormLinearOp(ctx: *anyopaque, request: *const ops.DecoderRuntimeApplyLayerNormLinearRequest) anyerror!?CT {
        const normed = (try decoderRuntimeApplyLayerNormOp(ctx, &.{
            .slot = request.norm_slot,
            .input = request.input,
            .hidden_size = request.hidden_size,
            .eps = request.eps,
        })) orelse return null;
        defer freeTensorOp(ctx, normed);
        return try decoderRuntimeApplyLinearOp(ctx, &.{
            .slot = request.linear_slot,
            .input = normed,
            .in_dim = request.hidden_size,
            .out_dim = request.out_dim,
        });
    }

    fn decoderRuntimeApplyLayerNormLinearArgmaxOp(ctx: *anyopaque, request: *const ops.DecoderRuntimeApplyLayerNormLinearArgmaxRequest) anyerror!?usize {
        const logits = (try decoderRuntimeApplyLayerNormLinearOp(ctx, &.{
            .norm_slot = request.norm_slot,
            .linear_slot = request.linear_slot,
            .input = request.input,
            .hidden_size = request.hidden_size,
            .eps = request.eps,
            .out_dim = request.out_dim,
        })) orelse return null;
        defer freeTensorOp(ctx, logits);
        const token = (try argmaxLastRowOp(ctx, logits, 1, request.out_dim)) orelse return null;
        return token;
    }

    fn decoderRuntimeApplyLayerNormLinearSampleOp(ctx: *anyopaque, request: *const ops.DecoderRuntimeApplyLayerNormLinearSampleRequest) anyerror!?usize {
        const logits = (try decoderRuntimeApplyLayerNormLinearOp(ctx, &.{
            .norm_slot = request.norm_slot,
            .linear_slot = request.linear_slot,
            .input = request.input,
            .hidden_size = request.hidden_size,
            .eps = request.eps,
            .out_dim = request.out_dim,
        })) orelse return null;
        defer freeTensorOp(ctx, logits);
        const token = (try sampleLastRowOp(ctx, &.{
            .tensor = logits,
            .rows = 1,
            .dim = request.out_dim,
            .temperature = request.temperature,
            .top_k = request.top_k,
            .top_p = request.top_p,
            .min_p = request.min_p,
            .repetition_penalty = request.repetition_penalty,
            .frequency_penalty = request.frequency_penalty,
            .presence_penalty = request.presence_penalty,
            .token_history = request.token_history,
        })) orelse return null;
        return token;
    }

    fn decoderRuntimeApplyRmsNormLinearOp(ctx: *anyopaque, request: *const ops.DecoderRuntimeApplyRmsNormLinearRequest) anyerror!?CT {
        const normed = (try decoderRuntimeApplyRmsNormOp(ctx, &.{
            .slot = request.norm_slot,
            .input = request.input,
            .hidden_size = request.hidden_size,
            .eps = request.eps,
        })) orelse return null;
        defer freeTensorOp(ctx, normed);
        return try decoderRuntimeApplyLinearOp(ctx, &.{
            .slot = request.linear_slot,
            .input = normed,
            .in_dim = request.hidden_size,
            .out_dim = request.out_dim,
        });
    }

    fn decoderRuntimeApplyRmsNormLinearArgmaxOp(ctx: *anyopaque, request: *const ops.DecoderRuntimeApplyRmsNormLinearArgmaxRequest) anyerror!?usize {
        const logits = (try decoderRuntimeApplyRmsNormLinearOp(ctx, &.{
            .norm_slot = request.norm_slot,
            .linear_slot = request.linear_slot,
            .input = request.input,
            .hidden_size = request.hidden_size,
            .eps = request.eps,
            .out_dim = request.out_dim,
        })) orelse return null;
        defer freeTensorOp(ctx, logits);
        const token = (try argmaxLastRowOp(ctx, logits, 1, request.out_dim)) orelse return null;
        return token;
    }

    fn decoderRuntimeApplyRmsNormLinearSampleOp(ctx: *anyopaque, request: *const ops.DecoderRuntimeApplyRmsNormLinearSampleRequest) anyerror!?usize {
        const logits = (try decoderRuntimeApplyRmsNormLinearOp(ctx, &.{
            .norm_slot = request.norm_slot,
            .linear_slot = request.linear_slot,
            .input = request.input,
            .hidden_size = request.hidden_size,
            .eps = request.eps,
            .out_dim = request.out_dim,
        })) orelse return null;
        defer freeTensorOp(ctx, logits);
        const token = (try sampleLastRowOp(ctx, &.{
            .tensor = logits,
            .rows = 1,
            .dim = request.out_dim,
            .temperature = request.temperature,
            .top_k = request.top_k,
            .top_p = request.top_p,
            .min_p = request.min_p,
            .repetition_penalty = request.repetition_penalty,
            .frequency_penalty = request.frequency_penalty,
            .presence_penalty = request.presence_penalty,
            .token_history = request.token_history,
        })) orelse return null;
        return token;
    }

    fn runDenseFfnResidualOp(ctx: *anyopaque, request: *const ops.RunDenseFfnResidualRequest) anyerror!?CT {
        const first = (try decoderRuntimeApplyLinearOp(ctx, &.{
            .slot = request.first_linear_slot,
            .input = request.input,
            .in_dim = request.hidden_size,
            .out_dim = request.intermediate_size,
        })) orelse return null;
        defer freeTensorOp(ctx, first);

        const activated = (try decoderRuntimeApplyActivationOp(ctx, &.{
            .input = first,
            .kind = request.activation,
            .dim = request.intermediate_size,
        })) orelse return null;
        defer freeTensorOp(ctx, activated);

        const projected = (try decoderRuntimeApplyLinearOp(ctx, &.{
            .slot = request.second_linear_slot,
            .input = activated,
            .in_dim = request.intermediate_size,
            .out_dim = request.hidden_size,
        })) orelse return null;
        defer freeTensorOp(ctx, projected);

        return try decoderRuntimeApplyAddOp(ctx, &.{
            .lhs = projected,
            .rhs = request.residual,
            .dim = request.hidden_size,
        });
    }

    fn runGatedFfnResidualOp(ctx: *anyopaque, request: *const ops.RunGatedFfnResidualRequest) anyerror!?CT {
        const pair = (try decoderRuntimeApplyLinearPairOp(ctx, &.{
            .slot_a = request.gate_linear_slot,
            .slot_b = request.up_linear_slot,
            .input = request.input,
            .in_dim = request.hidden_size,
            .out_dim = request.intermediate_size,
        })) orelse return null;
        defer freeTensorOp(ctx, pair.first);
        defer freeTensorOp(ctx, pair.second);

        const activated = (try decoderRuntimeApplyActivationOp(ctx, &.{
            .input = pair.first,
            .kind = request.activation,
            .dim = request.intermediate_size,
        })) orelse return null;
        defer freeTensorOp(ctx, activated);

        const gated = try multiplyOp(ctx, activated, pair.second);
        defer freeTensorOp(ctx, gated);

        const down_input = if (request.post_gate_rms_norm_slot) |slot| blk: {
            const normed = (try decoderRuntimeApplyRmsNormOp(ctx, &.{
                .slot = slot,
                .input = gated,
                .hidden_size = request.intermediate_size,
                .eps = 0.0,
            })) orelse return null;
            break :blk normed;
        } else gated;
        const down_input_owned = request.post_gate_rms_norm_slot != null;
        defer if (down_input_owned) freeTensorOp(ctx, down_input);

        const projected = (try decoderRuntimeApplyLinearOp(ctx, &.{
            .slot = request.down_linear_slot,
            .input = down_input,
            .in_dim = request.intermediate_size,
            .out_dim = request.hidden_size,
        })) orelse return null;
        defer freeTensorOp(ctx, projected);

        const residual_input = if (request.post_down_rms_norm_slot) |slot| blk: {
            const normed = (try decoderRuntimeApplyRmsNormOp(ctx, &.{
                .slot = slot,
                .input = projected,
                .hidden_size = request.hidden_size,
                .eps = 0.0,
            })) orelse return null;
            break :blk normed;
        } else projected;
        const residual_input_owned = request.post_down_rms_norm_slot != null;
        defer if (residual_input_owned) freeTensorOp(ctx, residual_input);

        return try decoderRuntimeApplyAddOp(ctx, &.{
            .lhs = residual_input,
            .rhs = request.residual,
            .dim = request.hidden_size,
        });
    }

    fn runAttentionOp(ctx: *anyopaque, request: *const ops.RunAttentionRequest) anyerror!?CT {
        var attention = request.attention;
        if (request.attention_sink.hasMetadata()) attention.attention_sink = request.attention_sink;
        const q = toBuf(request.q);
        const q_width = request.num_heads * request.head_dim;
        if (attention.query_sequence_len == 0 or q_width == 0) return null;
        if (q.len % q_width != 0) return error.UnexpectedOutputShape;
        if (q.len / q_width % attention.query_sequence_len != 0) return error.UnexpectedOutputShape;
        const batch = (q.len / q_width) / attention.query_sequence_len;

        if (attention.mode == .dense_causal and attention.sliding_window == 0 and attention.attn_or_mask == null) {
            const sink_scores = try attentionSinkScores(attention.attention_sink, request.num_heads);
            return try gqaCausalAttentionWithSink(
                ctx,
                request.q,
                request.k,
                request.v,
                null,
                sink_scores,
                batch,
                attention.query_sequence_len,
                request.num_heads,
                request.num_kv_heads,
                request.head_dim,
            );
        }

        return try gqaPagedAttentionOp(
            ctx,
            request.q,
            request.k,
            request.v,
            null,
            attention,
            batch,
            request.num_heads,
            request.num_kv_heads,
            request.head_dim,
        );
    }

    fn runAttentionResidualOp(ctx: *anyopaque, request: *const ops.RunAttentionResidualRequest) anyerror!?CT {
        const attention_input_size = request.num_heads * request.head_dim;
        var current = (try runAttentionOp(ctx, &.{
            .q = request.q,
            .k = request.k,
            .v = request.v,
            .attention = request.attention,
            .attention_sink = request.attention_sink,
            .num_heads = request.num_heads,
            .num_kv_heads = request.num_kv_heads,
            .head_dim = request.head_dim,
        })) orelse return null;
        errdefer freeTensorOp(ctx, current);

        if (request.pre_linear_rms_norm_slot) |slot| {
            const normed = (try decoderRuntimeApplyRmsNormOp(ctx, &.{
                .slot = slot,
                .input = current,
                .hidden_size = attention_input_size,
                .eps = request.eps,
            })) orelse return null;
            freeTensorOp(ctx, current);
            current = normed;
        }

        const projected = (try decoderRuntimeApplyLinearOp(ctx, &.{
            .slot = request.linear_slot,
            .input = current,
            .in_dim = attention_input_size,
            .out_dim = request.hidden_size,
        })) orelse return null;
        freeTensorOp(ctx, current);
        current = projected;

        if (request.post_linear_rms_norm_slot) |slot| {
            const normed = (try decoderRuntimeApplyRmsNormOp(ctx, &.{
                .slot = slot,
                .input = current,
                .hidden_size = request.hidden_size,
                .eps = request.eps,
            })) orelse return null;
            freeTensorOp(ctx, current);
            current = normed;
        }

        const result = try addOp(ctx, current, request.residual);
        freeTensorOp(ctx, current);
        return result;
    }

    fn decoderRuntimeApplyBlockFfnNormOp(
        ctx: *anyopaque,
        input: CT,
        layer_norm_slot: ?usize,
        rms_norm_slot: ?usize,
        hidden_size: usize,
        eps: f32,
    ) anyerror!?CT {
        if (layer_norm_slot) |slot| {
            return decoderRuntimeApplyLayerNormOp(ctx, &.{
                .slot = slot,
                .input = input,
                .hidden_size = hidden_size,
                .eps = eps,
            });
        }
        if (rms_norm_slot) |slot| {
            return decoderRuntimeApplyRmsNormOp(ctx, &.{
                .slot = slot,
                .input = input,
                .hidden_size = hidden_size,
                .eps = eps,
            });
        }
        return input;
    }

    fn runDenseDecoderBlockOp(ctx: *anyopaque, request: *const ops.RunDenseDecoderBlockRequest) anyerror!?CT {
        // The dense decoder block accepts either pre-projected q/k/v or a raw
        // attention_input that must be projected through fused_qkv_linear_slot.
        // Mirror the MLX backend's contract — see ops/mlx_compute.zig
        // runDenseDecoderBlockOp — so callers can use the same request shape.
        var q_tensor = request.q;
        var k_tensor = request.k;
        var v_tensor = request.v;
        var owns_qkv = false;
        defer if (owns_qkv) {
            if (q_tensor) |t| freeTensorOp(ctx, t);
            if (k_tensor) |t| freeTensorOp(ctx, t);
            if (v_tensor) |t| freeTensorOp(ctx, t);
        };
        if (q_tensor == null and request.attention_input != null and request.fused_qkv_linear_slot != null) {
            const q_dim = request.num_heads * request.head_dim;
            const kv_dim = request.num_kv_heads * request.head_dim;
            const fused_qkv = (try decoderRuntimeApplyLinearOp(ctx, &.{
                .slot = request.fused_qkv_linear_slot.?,
                .input = request.attention_input.?,
                .in_dim = request.hidden_size,
                .out_dim = q_dim + kv_dim * 2,
            })) orelse return null;
            defer freeTensorOp(ctx, fused_qkv);
            q_tensor = try sliceLastDimOp(ctx, fused_qkv, 0, q_dim);
            errdefer freeTensorOp(ctx, q_tensor.?);
            k_tensor = try sliceLastDimOp(ctx, fused_qkv, q_dim, q_dim + kv_dim);
            errdefer freeTensorOp(ctx, k_tensor.?);
            v_tensor = try sliceLastDimOp(ctx, fused_qkv, q_dim + kv_dim, q_dim + kv_dim * 2);
            owns_qkv = true;
        }
        const q_ct = q_tensor orelse return null;
        const k_ct = k_tensor orelse return null;
        const v_ct = v_tensor orelse return null;

        const attn_res = (try runAttentionResidualOp(ctx, &.{
            .q = q_ct,
            .k = k_ct,
            .v = v_ct,
            .residual = request.residual,
            .attention = request.attention,
            .attention_sink = request.attention.attention_sink,
            .num_heads = request.num_heads,
            .num_kv_heads = request.num_kv_heads,
            .head_dim = request.head_dim,
            .linear_slot = request.attention_linear_slot,
            .pre_linear_rms_norm_slot = request.attention_pre_linear_rms_norm_slot,
            .post_linear_rms_norm_slot = request.attention_post_linear_rms_norm_slot,
            .hidden_size = request.hidden_size,
            .eps = request.eps,
        })) orelse return null;
        defer freeTensorOp(ctx, attn_res);

        const ffn_normed = (try decoderRuntimeApplyBlockFfnNormOp(
            ctx,
            attn_res,
            request.ffn_layer_norm_slot,
            request.ffn_rms_norm_slot,
            request.hidden_size,
            request.eps,
        )) orelse return null;
        defer if (ffn_normed != attn_res) freeTensorOp(ctx, ffn_normed);

        return runDenseFfnResidualOp(ctx, &.{
            .first_linear_slot = request.first_ffn_linear_slot,
            .second_linear_slot = request.second_ffn_linear_slot,
            .input = ffn_normed,
            .residual = attn_res,
            .hidden_size = request.hidden_size,
            .intermediate_size = request.intermediate_size,
            .activation = request.activation,
        });
    }

    fn runGatedDecoderBlockOp(ctx: *anyopaque, request: *const ops.RunGatedDecoderBlockRequest) anyerror!?CT {
        var q_tensor = request.q;
        var k_tensor = request.k;
        var v_tensor = request.v;
        var owns_q = false;
        var owns_k = false;
        var owns_v = false;
        defer if (owns_q and q_tensor != null) freeTensorOp(ctx, q_tensor.?);
        defer if (owns_k and k_tensor != null) freeTensorOp(ctx, k_tensor.?);
        defer if (owns_v and v_tensor != null) freeTensorOp(ctx, v_tensor.?);

        const can_project_from_attention_input = request.attention_input != null and
            request.q_linear_slot != null and
            request.k_linear_slot != null and
            request.v_linear_slot != null;

        if (can_project_from_attention_input) {
            const qkv_projected = (try decoderRuntimeApplyLinearQkvOp(ctx, &.{
                .q_slot = request.q_linear_slot.?,
                .k_slot = request.k_linear_slot.?,
                .v_slot = request.v_linear_slot.?,
                .input = request.attention_input.?,
                .in_dim = request.hidden_size,
                .q_out_dim = request.num_heads * request.head_dim,
                .kv_out_dim = request.num_kv_heads * request.head_dim,
            })) orelse return null;
            q_tensor = qkv_projected.first;
            k_tensor = qkv_projected.second;
            v_tensor = qkv_projected.third;
            owns_q = true;
            owns_k = true;
            owns_v = true;
        }

        const q = q_tensor orelse return null;
        const k = k_tensor orelse return null;
        const v = v_tensor orelse return null;

        const attn_res = (try runAttentionResidualOp(ctx, &.{
            .q = q,
            .k = k,
            .v = v,
            .residual = request.residual,
            .attention = request.attention,
            .attention_sink = request.attention.attention_sink,
            .num_heads = request.num_heads,
            .num_kv_heads = request.num_kv_heads,
            .head_dim = request.head_dim,
            .linear_slot = request.attention_linear_slot,
            .pre_linear_rms_norm_slot = request.attention_pre_linear_rms_norm_slot,
            .post_linear_rms_norm_slot = request.attention_post_linear_rms_norm_slot,
            .hidden_size = request.hidden_size,
            .eps = request.eps,
        })) orelse return null;
        defer freeTensorOp(ctx, attn_res);

        const ffn_normed = (try decoderRuntimeApplyBlockFfnNormOp(
            ctx,
            attn_res,
            request.ffn_layer_norm_slot,
            request.ffn_rms_norm_slot,
            request.hidden_size,
            request.eps,
        )) orelse return null;
        defer if (ffn_normed != attn_res) freeTensorOp(ctx, ffn_normed);

        return runGatedFfnResidualOp(ctx, &.{
            .gate_linear_slot = request.gate_ffn_linear_slot,
            .up_linear_slot = request.up_ffn_linear_slot,
            .down_linear_slot = request.down_ffn_linear_slot,
            .input = ffn_normed,
            .residual = attn_res,
            .post_gate_rms_norm_slot = request.ffn_post_gate_rms_norm_slot,
            .hidden_size = request.hidden_size,
            .intermediate_size = request.intermediate_size,
            .activation = request.activation,
        });
    }

    /// Causal (decoder) self-attention: Q @ K^T with causal mask (future positions = -inf).
    /// Optional additive attn_bias [num_heads, seq_len, seq_len].
    fn causalSelfAttentionOp(
        ctx: *anyopaque,
        Q_ct: CT,
        K_ct: CT,
        V_ct: CT,
        attn_bias_ct: ?CT,
        batch: usize,
        seq_len: usize,
        num_heads: usize,
        head_dim: usize,
    ) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const Q = toBuf(Q_ct);
        const K = toBuf(K_ct);
        const V = toBuf(V_ct);
        const H = num_heads * head_dim;
        const attn_bias_view = if (attn_bias_ct) |ab| try toBuf(ab).viewF32(self.allocator) else null;
        defer if (attn_bias_view) |v| {
            if (v.allocated) self.allocator.free(v.data);
        };
        const attn_bias: ?[]const f32 = if (attn_bias_view) |v| v.data else null;

        // GPU path: causal attention shader (no mask buffer needed)
        if (build_options.enable_webgpu and self.use_gpu and
            batch * seq_len * H >= WEBGPU_ATTN_THRESHOLD and seq_len <= WEBGPU_ATTN_MAX_SEQ and
            attn_bias == null)
        {
            const out = try self.allocator.alloc(f32, batch * seq_len * H);
            var q_gpu = GpuInputTensor.fromBuf(Q);
            defer q_gpu.deinit();
            var k_gpu = GpuInputTensor.fromBuf(K);
            defer k_gpu.deinit();
            var v_gpu = GpuInputTensor.fromBuf(V);
            defer v_gpu.deinit();
            var out_gpu = gpuCausalAttention(batch, seq_len, num_heads, head_dim, q_gpu.id, k_gpu.id, v_gpu.id, out);
            return fromBuf(try setBufShape2D(WasmBuf.fromSliceWithGpu(self.allocator, out, true, out_gpu.detach(), true), batch * seq_len, H));
        }
        const output = try linalg.flashCausalAttentionHost(self.allocator, Q.data, K.data, V.data, attn_bias, null, 0, batch, seq_len, seq_len, 0, 0, num_heads, num_heads, head_dim);
        return fromBuf(try setBufShape2D(WasmBuf.fromSlice(self.allocator, output, true), batch * seq_len, H));
    }

    /// Cross-attention: Q from decoder [batch, dec_seq, H], K/V from encoder [batch, enc_seq, H].
    /// Score matrix is [dec_seq, enc_seq] per head, masked by enc_mask.
    fn crossAttentionOp(
        ctx: *anyopaque,
        Q_ct: CT,
        K_ct: CT,
        V_ct: CT,
        enc_mask: []const i64,
        batch: usize,
        dec_seq: usize,
        enc_seq: usize,
        num_heads: usize,
        head_dim: usize,
    ) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const Q = toBuf(Q_ct);
        const K = toBuf(K_ct);
        const V = toBuf(V_ct);
        const H = num_heads * head_dim;

        // GPU path: cross-attention shader
        if (build_options.enable_webgpu and self.use_gpu and
            batch * dec_seq * H >= WEBGPU_ATTN_THRESHOLD and enc_seq <= WEBGPU_ATTN_MAX_SEQ)
        {
            const out = try self.allocator.alloc(f32, batch * dec_seq * H);
            var q_gpu = GpuInputTensor.fromBuf(Q);
            defer q_gpu.deinit();
            var k_gpu = GpuInputTensor.fromBuf(K);
            defer k_gpu.deinit();
            var v_gpu = GpuInputTensor.fromBuf(V);
            defer v_gpu.deinit();
            var out_gpu = gpuCrossAttention(batch, dec_seq, enc_seq, num_heads, head_dim, q_gpu.id, k_gpu.id, v_gpu.id, enc_mask, out);
            return fromBuf(try setBufShape2D(WasmBuf.fromSliceWithGpu(self.allocator, out, true, out_gpu.detach(), true), batch * dec_seq, H));
        }

        const output = try linalg.crossAttentionHost(self.allocator, Q.data, K.data, V.data, enc_mask, batch, dec_seq, enc_seq, num_heads, head_dim);
        return fromBuf(try setBufShape2D(WasmBuf.fromSlice(self.allocator, output, true), batch * dec_seq, H));
    }

    fn relativePositionBiasOp(ctx: *anyopaque, weight_ct: CT, q_len: usize, k_len: usize, num_heads: usize, num_buckets: usize, max_distance: usize, bidirectional: bool) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const table_view = try toBuf(weight_ct).viewF32(self.allocator);
        defer if (table_view.allocated) self.allocator.free(table_view.data);
        const table = table_view.data; // [num_heads, num_buckets]

        const output = try self.allocator.alloc(f32, num_heads * q_len * k_len);

        for (0..q_len) |qi| {
            for (0..k_len) |ki| {
                const bucket = linalg.t5RelativePositionBucket(@as(i64, @intCast(ki)) - @as(i64, @intCast(qi)), num_buckets, max_distance, bidirectional);
                for (0..num_heads) |h| {
                    output[h * q_len * k_len + qi * k_len + ki] = table[h * num_buckets + bucket];
                }
            }
        }

        return fromBuf(try setBufShape2D(WasmBuf.fromSlice(self.allocator, output, true), num_heads * q_len, k_len));
    }

    /// DeBERTa disentangled attention with relative position encoding.
    /// Three score components: content-content (Q·K), content-position (Q·K_r),
    /// and position-content (Q_r·K), scaled by 1/sqrt(head_dim * 3).
    fn disentangledRelativeAttentionOp(
        ctx: *anyopaque,
        Q_ct: CT,
        K_ct: CT,
        V_ct: CT,
        Q_r_ct: CT,
        K_r_ct: CT,
        mask: []const i64,
        batch: usize,
        seq_len: usize,
        num_heads: usize,
        head_dim: usize,
    ) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const Q = toBuf(Q_ct);
        const K = toBuf(K_ct);
        const V = toBuf(V_ct);
        const Q_r = toBuf(Q_r_ct);
        const K_r = toBuf(K_r_ct);

        if (build_options.enable_webgpu and self.use_gpu and
            seq_len <= WEBGPU_ATTN_MAX_SEQ and
            (Q.gpu_tensor != null or K.gpu_tensor != null or V.gpu_tensor != null or batch * seq_len * num_heads * head_dim >= (WEBGPU_ATTN_THRESHOLD / 4)))
        {
            const out = try self.allocator.alloc(f32, batch * seq_len * num_heads * head_dim);
            var q_gpu = GpuInputTensor.fromBuf(Q);
            defer q_gpu.deinit();
            var k_gpu = GpuInputTensor.fromBuf(K);
            defer k_gpu.deinit();
            var v_gpu = GpuInputTensor.fromBuf(V);
            defer v_gpu.deinit();
            var q_r_gpu = GpuInputTensor.fromBuf(Q_r);
            defer q_r_gpu.deinit();
            var k_r_gpu = GpuInputTensor.fromBuf(K_r);
            defer k_r_gpu.deinit();
            var out_gpu = gpuDebertaDisentangledAttention(batch, seq_len, num_heads, head_dim, q_gpu.id, k_gpu.id, v_gpu.id, q_r_gpu.id, k_r_gpu.id, mask, out);
            return fromBuf(try setBufShape2D(WasmBuf.fromSliceWithGpu(self.allocator, out, true, out_gpu.detach(), true), batch * seq_len, num_heads * head_dim));
        }

        const out = try linalg.debertaDisentangledAttentionHost(self.allocator, Q.data, K.data, V.data, Q_r.data, K_r.data, mask, batch, seq_len, num_heads, head_dim);
        return fromBuf(try setBufShape2D(WasmBuf.fromSlice(self.allocator, out, true), batch * seq_len, num_heads * head_dim));
    }

    /// Windowed self-attention (Florence DaViT spatial blocks):
    /// LayerNorm → pad to windows → fused QKV linear → per-window multi-head attention → project → unpad.
    fn windowedSelfAttentionOp(
        ctx: *anyopaque,
        input_ct: CT,
        norm_weight_ct: CT,
        norm_bias_ct: CT,
        qkv_weight_ct: CT,
        qkv_bias_ct: CT,
        proj_weight_ct: CT,
        proj_bias_ct: CT,
        batch: usize,
        height: usize,
        width: usize,
        dim: usize,
        num_heads: usize,
        window_size: usize,
    ) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const head_dim = dim / num_heads;

        // 1. LayerNorm
        const normed_ct = try layerNormOp(ctx, input_ct, norm_weight_ct, norm_bias_ct, dim, 1e-5);
        defer freeTensorOp(ctx, normed_ct);
        const normed = toBuf(normed_ct).data;

        // 2. Pad tokens into windows
        const window_pack = try linalg.padTokensToWindows(self.allocator, normed, batch, height, width, dim, window_size);
        defer self.allocator.free(window_pack.data);

        // 3. Fused QKV linear: [window_count * window_area, dim] → [window_count * window_area, dim*3]
        const rows = window_pack.window_count * window_pack.window_area;
        const qkv_w_view = try toBuf(qkv_weight_ct).viewF32(self.allocator);
        defer if (qkv_w_view.allocated) self.allocator.free(qkv_w_view.data);
        const qkv_b_view = try toBuf(qkv_bias_ct).viewF32(self.allocator);
        defer if (qkv_b_view.allocated) self.allocator.free(qkv_b_view.data);
        const qkv_weight = qkv_w_view.data;
        const qkv_bias = qkv_b_view.data;
        const qkv = try self.allocator.alloc(f32, rows * dim * 3);
        defer self.allocator.free(qkv);
        // bias init
        for (0..rows) |r| {
            @memcpy(qkv[r * dim * 3 ..][0 .. dim * 3], qkv_bias[0 .. dim * 3]);
        }
        linalg.sgemmTransBSync(rows, dim * 3, dim, 1.0, window_pack.data, qkv_weight, 1.0, qkv);

        // 4. Per-window multi-head attention
        const attn = try self.allocator.alloc(f32, rows * dim);
        defer self.allocator.free(attn);
        @memset(attn, 0.0);

        const scores = try self.allocator.alloc(f32, window_pack.window_area * window_pack.window_area);
        defer self.allocator.free(scores);
        const scale: f32 = 1.0 / @sqrt(@as(f32, @floatFromInt(head_dim)));

        const VEC = wasm_vec_len;
        for (0..window_pack.window_count) |window_idx| {
            const window_base = window_idx * window_pack.window_area;
            for (0..num_heads) |head| {
                const head_offset = head * head_dim;

                // Compute scores: Q @ K^T
                for (0..window_pack.window_area) |qi| {
                    const q_src = (window_base + qi) * dim * 3 + head_offset;
                    for (0..window_pack.window_area) |ki| {
                        const k_src = (window_base + ki) * dim * 3 + dim + head_offset;
                        var acc: @Vector(VEC, f32) = @splat(0.0);
                        var d: usize = 0;
                        while (d + VEC <= head_dim) : (d += VEC) {
                            const qv: @Vector(VEC, f32) = qkv[q_src + d ..][0..VEC].*;
                            const kv: @Vector(VEC, f32) = qkv[k_src + d ..][0..VEC].*;
                            acc += qv * kv;
                        }
                        var dot = @reduce(.Add, acc);
                        while (d < head_dim) : (d += 1) {
                            dot += qkv[q_src + d] * qkv[k_src + d];
                        }
                        scores[qi * window_pack.window_area + ki] = dot * scale;
                    }
                }

                // Softmax per row
                activations.softmax(scores, window_pack.window_area);

                // Weighted sum: out = scores @ V
                for (0..window_pack.window_area) |qi| {
                    const dst_base = (window_base + qi) * dim + head_offset;
                    for (0..window_pack.window_area) |vi| {
                        const w = scores[qi * window_pack.window_area + vi];
                        if (w == 0.0) continue;
                        const v_src = (window_base + vi) * dim * 3 + 2 * dim + head_offset;
                        const w_splat: @Vector(VEC, f32) = @splat(w);
                        var d: usize = 0;
                        while (d + VEC <= head_dim) : (d += VEC) {
                            const ov: @Vector(VEC, f32) = attn[dst_base + d ..][0..VEC].*;
                            const vv: @Vector(VEC, f32) = qkv[v_src + d ..][0..VEC].*;
                            attn[dst_base + d ..][0..VEC].* = ov + w_splat * vv;
                        }
                        while (d < head_dim) : (d += 1) {
                            attn[dst_base + d] += w * qkv[v_src + d];
                        }
                    }
                }
            }
        }

        // 5. Output projection: [rows, dim] → [rows, dim]
        const proj_w_view = try toBuf(proj_weight_ct).viewF32(self.allocator);
        defer if (proj_w_view.allocated) self.allocator.free(proj_w_view.data);
        const proj_b_view = try toBuf(proj_bias_ct).viewF32(self.allocator);
        defer if (proj_b_view.allocated) self.allocator.free(proj_b_view.data);
        const proj_weight = proj_w_view.data;
        const proj_bias = proj_b_view.data;
        const projected = try self.allocator.alloc(f32, rows * dim);
        defer self.allocator.free(projected);
        for (0..rows) |r| {
            @memcpy(projected[r * dim ..][0..dim], proj_bias[0..dim]);
        }
        linalg.sgemmTransBSync(rows, dim, dim, 1.0, attn, proj_weight, 1.0, projected);

        // 6. Unpad windows back to original spatial layout
        const result = try linalg.unpadWindowTokens(self.allocator, projected, window_pack, batch, height, width, dim);
        return fromBuf(WasmBuf.fromSlice(self.allocator, result, true));
    }

    /// Channel self-attention (Florence DaViT channel blocks):
    /// LayerNorm → fused QKV linear → attention over channel dim (transposed) → project.
    fn channelSelfAttentionOp(
        ctx: *anyopaque,
        input_ct: CT,
        norm_weight_ct: CT,
        norm_bias_ct: CT,
        qkv_weight_ct: CT,
        qkv_bias_ct: CT,
        proj_weight_ct: CT,
        proj_bias_ct: CT,
        batch: usize,
        seq_len: usize,
        dim: usize,
        groups: usize,
    ) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));

        // 1. LayerNorm
        const normed_ct = try layerNormOp(ctx, input_ct, norm_weight_ct, norm_bias_ct, dim, 1e-5);
        defer freeTensorOp(ctx, normed_ct);

        // 2. Fused QKV linear: [batch * seq_len, dim] → [batch * seq_len, dim*3]
        const normed = toBuf(normed_ct).data;
        const ch_qkv_w_view = try toBuf(qkv_weight_ct).viewF32(self.allocator);
        defer if (ch_qkv_w_view.allocated) self.allocator.free(ch_qkv_w_view.data);
        const ch_qkv_b_view = try toBuf(qkv_bias_ct).viewF32(self.allocator);
        defer if (ch_qkv_b_view.allocated) self.allocator.free(ch_qkv_b_view.data);
        const qkv_weight = ch_qkv_w_view.data;
        const qkv_bias = ch_qkv_b_view.data;
        const total_rows = batch * seq_len;
        const qkv = try self.allocator.alloc(f32, total_rows * dim * 3);
        defer self.allocator.free(qkv);
        for (0..total_rows) |r| {
            @memcpy(qkv[r * dim * 3 ..][0 .. dim * 3], qkv_bias[0 .. dim * 3]);
        }
        linalg.sgemmTransBSync(total_rows, dim * 3, dim, 1.0, normed, qkv_weight, 1.0, qkv);

        const attended = try self.allocator.alloc(f32, total_rows * dim);
        defer self.allocator.free(attended);
        try linalg.channelAttention(self.allocator, attended, qkv, batch, seq_len, dim, groups);

        // 4. Output projection: [batch * seq_len, dim] → [batch * seq_len, dim]
        const ch_proj_w_view = try toBuf(proj_weight_ct).viewF32(self.allocator);
        defer if (ch_proj_w_view.allocated) self.allocator.free(ch_proj_w_view.data);
        const ch_proj_b_view = try toBuf(proj_bias_ct).viewF32(self.allocator);
        defer if (ch_proj_b_view.allocated) self.allocator.free(ch_proj_b_view.data);
        const proj_weight = ch_proj_w_view.data;
        const proj_bias = ch_proj_b_view.data;
        const projected = try self.allocator.alloc(f32, total_rows * dim);
        for (0..total_rows) |r| {
            @memcpy(projected[r * dim ..][0..dim], proj_bias[0..dim]);
        }
        linalg.sgemmTransBSync(total_rows, dim, dim, 1.0, attended, proj_weight, 1.0, projected);

        return fromBuf(WasmBuf.fromSlice(self.allocator, projected, true));
    }

    /// TokenGrid Conv2d: input is [batch*H*W, channels] token layout.
    /// Depthwise fast-path when groups == in_channels == out_channels (avoids reshape+conv2d).
    /// General path: reshape to NCHW → conv2d → reshape back to token layout.
    fn tokenGridConv2dOp(
        ctx: *anyopaque,
        input_ct: CT,
        weight_ct: CT,
        bias_ct: CT,
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
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const input_view = try toBuf(input_ct).viewF32(self.allocator);
        defer if (input_view.allocated) self.allocator.free(input_view.data);
        const input_tokens = input_view.data;
        const out_h = (height + 2 * padding_h - kernel_h) / stride_h + 1;
        const out_w = (width + 2 * padding_w - kernel_w) / stride_w + 1;

        // Depthwise fast-path: operate directly on token layout
        if (groups == in_channels and groups == out_channels) {
            const kernel_size = kernel_h * kernel_w;
            const w_view = try toBuf(weight_ct).viewF32(self.allocator);
            defer if (w_view.allocated) self.allocator.free(w_view.data);
            const b_view = try toBuf(bias_ct).viewF32(self.allocator);
            defer if (b_view.allocated) self.allocator.free(b_view.data);
            const weight_data = w_view.data;
            const bias_data = b_view.data;
            const out_tokens = try self.allocator.alloc(f32, batch * out_h * out_w * out_channels);

            for (0..batch) |b| {
                for (0..out_h) |oy| {
                    const in_y_origin = oy * stride_h;
                    for (0..out_w) |ox| {
                        const in_x_origin = ox * stride_w;
                        const out_base = ((b * out_h + oy) * out_w + ox) * out_channels;
                        for (0..out_channels) |c| {
                            const weight_channel = weight_data[c * kernel_size ..][0..kernel_size];
                            var acc = bias_data[c];
                            for (0..kernel_h) |ky| {
                                const in_y_signed: i64 = @as(i64, @intCast(in_y_origin + ky)) - @as(i64, @intCast(padding_h));
                                if (in_y_signed < 0 or in_y_signed >= @as(i64, @intCast(height))) continue;
                                const in_y: usize = @intCast(in_y_signed);
                                const row_base = ((b * height + in_y) * width) * in_channels;
                                const weight_row_base = ky * kernel_w;
                                for (0..kernel_w) |kx| {
                                    const in_x_signed: i64 = @as(i64, @intCast(in_x_origin + kx)) - @as(i64, @intCast(padding_w));
                                    if (in_x_signed < 0 or in_x_signed >= @as(i64, @intCast(width))) continue;
                                    const in_x: usize = @intCast(in_x_signed);
                                    acc += input_tokens[row_base + in_x * in_channels + c] * weight_channel[weight_row_base + kx];
                                }
                            }
                            out_tokens[out_base + c] = acc;
                        }
                    }
                }
            }
            return fromBuf(WasmBuf.fromSlice(self.allocator, out_tokens, true));
        }

        // General path: reshape tokens [B*H*W, C] → NCHW image
        const image = try self.allocator.alloc(f32, batch * in_channels * height * width);
        defer self.allocator.free(image);
        for (0..batch) |b| {
            for (0..height) |y| {
                for (0..width) |x| {
                    const token_idx = (b * height * width + y * width + x) * in_channels;
                    for (0..in_channels) |c| {
                        image[((b * in_channels + c) * height + y) * width + x] = input_tokens[token_idx + c];
                    }
                }
            }
        }

        // Run conv2d on NCHW image
        const image_ct = fromBuf(WasmBuf.fromSlice(self.allocator, image, false));
        defer freeTensorOp(ctx, image_ct);
        const out_ct = try conv2dOp(ctx, image_ct, weight_ct, bias_ct, batch, in_channels, out_channels, height, width, kernel_h, kernel_w, stride_h, stride_w, padding_h, padding_w, groups);
        defer freeTensorOp(ctx, out_ct);
        const out_image = toBuf(out_ct).data;

        // Reshape NCHW back to token layout [B*out_H*out_W, out_C]
        const out_tokens = try self.allocator.alloc(f32, batch * out_h * out_w * out_channels);
        for (0..batch) |b| {
            for (0..out_h) |y| {
                for (0..out_w) |x| {
                    const token_idx = (b * out_h * out_w + y * out_w + x) * out_channels;
                    for (0..out_channels) |c| {
                        out_tokens[token_idx + c] = out_image[((b * out_channels + c) * out_h + y) * out_w + x];
                    }
                }
            }
        }
        return fromBuf(WasmBuf.fromSlice(self.allocator, out_tokens, true));
    }

    /// Conv1d: output[b, oc, t] = bias[oc] + sum_{ic,k} input[b, ic, t*stride+k-padding] * weight[oc, ic, k]
    fn conv1dOp(
        ctx: *anyopaque,
        input_ct: CT,
        weight_ct: CT,
        bias_ct: CT,
        batch: usize,
        in_channels: usize,
        out_channels: usize,
        time_steps: usize,
        kernel_size: usize,
        stride: usize,
        padding: usize,
    ) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const in_view = try toBuf(input_ct).viewF32(self.allocator);
        defer if (in_view.allocated) self.allocator.free(in_view.data);
        const w_view = try toBuf(weight_ct).viewF32(self.allocator);
        defer if (w_view.allocated) self.allocator.free(w_view.data);
        const b_view = try toBuf(bias_ct).viewF32(self.allocator);
        defer if (b_view.allocated) self.allocator.free(b_view.data);
        const in_data = in_view.data;
        const w_data = w_view.data;
        const b_data = b_view.data;

        const out_time = (time_steps + 2 * padding - kernel_size) / stride + 1;
        const output = try self.allocator.alloc(f32, batch * out_channels * out_time);

        // Initialize with bias
        for (0..batch) |b| {
            for (0..out_channels) |oc| {
                const bias_val = b_data[oc];
                const out_base = (b * out_channels + oc) * out_time;
                for (0..out_time) |t| {
                    output[out_base + t] = bias_val;
                }
            }
        }

        // Convolution
        for (0..batch) |b| {
            for (0..out_channels) |oc| {
                for (0..out_time) |t| {
                    var sum: f32 = 0.0;
                    for (0..in_channels) |ic| {
                        for (0..kernel_size) |k| {
                            const in_t_signed: i64 = @as(i64, @intCast(t * stride + k)) - @as(i64, @intCast(padding));
                            if (in_t_signed >= 0 and in_t_signed < @as(i64, @intCast(time_steps))) {
                                const in_t: usize = @intCast(in_t_signed);
                                const in_idx = (b * in_channels + ic) * time_steps + in_t;
                                const w_idx = (oc * in_channels + ic) * kernel_size + k;
                                sum += in_data[in_idx] * w_data[w_idx];
                            }
                        }
                    }
                    output[(b * out_channels + oc) * out_time + t] += sum;
                }
            }
        }

        return fromBuf(WasmBuf.fromSlice(self.allocator, output, true));
    }

    /// Conv2d with three paths: groups=1 (im2col + matmul), depthwise, general grouped.
    fn conv2dOp(
        ctx: *anyopaque,
        input_ct: CT,
        weight_ct: CT,
        bias_ct: CT,
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
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        if (groups == 0 or in_channels % groups != 0 or out_channels % groups != 0) return error.InvalidInputShape;

        const input_view = try toBuf(input_ct).viewF32(self.allocator);
        defer if (input_view.allocated) self.allocator.free(input_view.data);
        const weight_view = try toBuf(weight_ct).viewF32(self.allocator);
        defer if (weight_view.allocated) self.allocator.free(weight_view.data);
        const bias_view = try toBuf(bias_ct).viewF32(self.allocator);
        defer if (bias_view.allocated) self.allocator.free(bias_view.data);
        const input_data = input_view.data;
        const weight_data = weight_view.data;
        const bias_data = bias_view.data;

        const out_h = (height + 2 * padding_h - kernel_h) / stride_h + 1;
        const out_w = (width + 2 * padding_w - kernel_w) / stride_w + 1;
        const output = try self.allocator.alloc(f32, batch * out_channels * out_h * out_w);

        if (groups == 1) {
            // im2col + matmul path
            const rows = out_h * out_w;
            const k_dim = in_channels * kernel_h * kernel_w;
            const cols = try self.allocator.alloc(f32, rows * k_dim);
            defer self.allocator.free(cols);

            for (0..batch) |b| {
                // Build im2col matrix
                for (0..out_h) |oy| {
                    for (0..out_w) |ox| {
                        const row = oy * out_w + ox;
                        const row_base = row * k_dim;
                        for (0..in_channels) |ic| {
                            for (0..kernel_h) |ky| {
                                for (0..kernel_w) |kx| {
                                    const col_idx = ((ic * kernel_h + ky) * kernel_w) + kx;
                                    const in_y_signed: i64 = @as(i64, @intCast(oy * stride_h + ky)) - @as(i64, @intCast(padding_h));
                                    const in_x_signed: i64 = @as(i64, @intCast(ox * stride_w + kx)) - @as(i64, @intCast(padding_w));
                                    cols[row_base + col_idx] = if (in_y_signed < 0 or in_x_signed < 0 or
                                        in_y_signed >= @as(i64, @intCast(height)) or in_x_signed >= @as(i64, @intCast(width)))
                                        0.0
                                    else blk: {
                                        const in_y: usize = @intCast(in_y_signed);
                                        const in_x: usize = @intCast(in_x_signed);
                                        break :blk input_data[((b * in_channels + ic) * height + in_y) * width + in_x];
                                    };
                                }
                            }
                        }
                    }
                }

                // matmul: [rows, k_dim] @ [out_channels, k_dim]^T → [rows, out_channels]
                const batch_out = output[b * out_channels * rows ..][0 .. out_channels * rows];
                for (0..rows) |row| {
                    @memcpy(batch_out[row * out_channels ..][0..out_channels], bias_data[0..out_channels]);
                }
                if (build_options.enable_webgpu and self.use_gpu and rows * out_channels >= WEBGPU_MATMUL_THRESHOLD) {
                    // GPU matmul: im2col cols and weights live on the host
                    // (im2col is rebuilt per-batch on CPU and weights aren't
                    // pinned), so upload both per call. gpuSgemmTransB
                    // overwrites C, so accumulate into a scratch buffer and
                    // add the bias-prefilled batch_out separately.
                    const scratch = try self.allocator.alloc(f32, rows * out_channels);
                    defer self.allocator.free(scratch);
                    var a_gpu = GpuTensor.fromF32(cols);
                    defer @constCast(&a_gpu).deinit();
                    var b_gpu = GpuTensor.fromF32(weight_data);
                    defer @constCast(&b_gpu).deinit();
                    var out_gpu = gpuSgemmTransB(rows, out_channels, k_dim, a_gpu.id, b_gpu.id, scratch);
                    defer out_gpu.deinit();
                    out_gpu.downloadF32(scratch);
                    for (0..rows * out_channels) |i| {
                        batch_out[i] += scratch[i];
                    }
                } else {
                    linalg.sgemmTransBSync(rows, out_channels, k_dim, 1.0, cols, weight_data, 1.0, batch_out);
                }
            }

            // Transpose from [batch, out_h*out_w, out_channels] to [batch, out_channels, out_h*out_w]
            const rows_total = out_h * out_w;
            const transposed = try self.allocator.alloc(f32, output.len);
            defer self.allocator.free(transposed);
            for (0..batch) |b| {
                const src_batch = output[b * out_channels * rows_total ..][0 .. out_channels * rows_total];
                const dst_batch = transposed[b * out_channels * rows_total ..][0 .. out_channels * rows_total];
                for (0..rows_total) |row| {
                    for (0..out_channels) |oc| {
                        dst_batch[(oc * rows_total) + row] = src_batch[row * out_channels + oc];
                    }
                }
            }
            @memcpy(output, transposed);
            return fromBuf(WasmBuf.fromSlice(self.allocator, output, true));
        }

        if (groups == in_channels and groups == out_channels) {
            // Depthwise path
            const kernel_size = kernel_h * kernel_w;
            for (0..batch) |b| {
                for (0..out_channels) |c| {
                    const weight_channel = weight_data[c * kernel_size ..][0..kernel_size];
                    const input_base = (b * in_channels + c) * height * width;
                    const output_base = (b * out_channels + c) * out_h * out_w;
                    for (0..out_h) |oy| {
                        const in_y_origin = oy * stride_h;
                        for (0..out_w) |ox| {
                            const in_x_origin = ox * stride_w;
                            var acc = bias_data[c];
                            for (0..kernel_h) |ky| {
                                const in_y_signed: i64 = @as(i64, @intCast(in_y_origin + ky)) - @as(i64, @intCast(padding_h));
                                if (in_y_signed < 0 or in_y_signed >= @as(i64, @intCast(height))) continue;
                                const in_y: usize = @intCast(in_y_signed);
                                const row_base = input_base + in_y * width;
                                const weight_row_base = ky * kernel_w;
                                for (0..kernel_w) |kx| {
                                    const in_x_signed: i64 = @as(i64, @intCast(in_x_origin + kx)) - @as(i64, @intCast(padding_w));
                                    if (in_x_signed < 0 or in_x_signed >= @as(i64, @intCast(width))) continue;
                                    const in_x: usize = @intCast(in_x_signed);
                                    acc += input_data[row_base + in_x] * weight_channel[weight_row_base + kx];
                                }
                            }
                            output[output_base + oy * out_w + ox] = acc;
                        }
                    }
                }
            }
            return fromBuf(WasmBuf.fromSlice(self.allocator, output, true));
        }

        // General grouped convolution: im2col per group + matmul
        const in_per_group = in_channels / groups;
        const out_per_group = out_channels / groups;
        const rows = out_h * out_w;
        const k_dim = in_per_group * kernel_h * kernel_w;
        const cols = try self.allocator.alloc(f32, rows * k_dim);
        defer self.allocator.free(cols);
        const group_out = try self.allocator.alloc(f32, rows * out_per_group);
        defer self.allocator.free(group_out);
        const transposed_buf = try self.allocator.alloc(f32, rows * out_per_group);
        defer self.allocator.free(transposed_buf);

        for (0..batch) |b| {
            for (0..groups) |group| {
                const ic_base = group * in_per_group;
                const oc_base = group * out_per_group;

                for (0..out_h) |oy| {
                    for (0..out_w) |ox| {
                        const row = oy * out_w + ox;
                        const row_base = row * k_dim;
                        for (0..in_per_group) |ic_group| {
                            const ic = ic_base + ic_group;
                            for (0..kernel_h) |ky| {
                                for (0..kernel_w) |kx| {
                                    const col_idx = ((ic_group * kernel_h + ky) * kernel_w) + kx;
                                    const in_y_signed: i64 = @as(i64, @intCast(oy * stride_h + ky)) - @as(i64, @intCast(padding_h));
                                    const in_x_signed: i64 = @as(i64, @intCast(ox * stride_w + kx)) - @as(i64, @intCast(padding_w));
                                    cols[row_base + col_idx] = if (in_y_signed < 0 or in_x_signed < 0 or
                                        in_y_signed >= @as(i64, @intCast(height)) or in_x_signed >= @as(i64, @intCast(width)))
                                        0.0
                                    else blk: {
                                        const in_y: usize = @intCast(in_y_signed);
                                        const in_x: usize = @intCast(in_x_signed);
                                        break :blk input_data[((b * in_channels + ic) * height + in_y) * width + in_x];
                                    };
                                }
                            }
                        }
                    }
                }

                const group_weight = weight_data[oc_base * k_dim ..][0 .. out_per_group * k_dim];
                for (0..rows) |row| {
                    @memcpy(group_out[row * out_per_group ..][0..out_per_group], bias_data[oc_base..][0..out_per_group]);
                }
                linalg.sgemmTransBSync(rows, out_per_group, k_dim, 1.0, cols, group_weight, 1.0, group_out);

                // Transpose [rows, out_per_group] → [out_per_group, rows] and scatter into output
                for (0..rows) |row| {
                    for (0..out_per_group) |oc| {
                        output[((b * out_channels + oc_base + oc) * out_h * out_w) + row] = group_out[row * out_per_group + oc];
                    }
                }
            }
        }

        return fromBuf(WasmBuf.fromSlice(self.allocator, output, true));
    }

    // --- RoPE (Rotary Position Embeddings) ---

    fn ropeOp(ctx: *anyopaque, input: CT, seq_len: usize, head_dim: usize, rope_dim: usize, theta: f32, freq_scale: f32, position_offset: usize, consecutive_pairs: bool) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const data = toBuf(input).data;
        const total_chunks = data.len / head_dim;
        if (seq_len == 0) return error.InvalidRoPEInput;
        if (total_chunks % seq_len != 0) return error.InvalidRoPEInput;
        const chunks_per_position = total_chunks / seq_len;
        if (chunks_per_position == 0) return error.InvalidRoPEInput;
        const output = try self.allocator.dupe(f32, data);
        errdefer self.allocator.free(output);

        // Build flat position array: one position per head-sized chunk.
        const positions = try self.allocator.alloc(usize, total_chunks);
        defer self.allocator.free(positions);
        for (0..total_chunks) |tok| {
            positions[tok] = position_offset + ((tok / chunks_per_position) % seq_len);
        }

        linalg.ropeCore(output, positions, head_dim, rope_dim, theta, freq_scale, consecutive_pairs);
        return fromBuf(WasmBuf.fromSlice(self.allocator, output, true));
    }

    fn ropePerItemOp(
        ctx: *anyopaque,
        input: CT,
        batch: usize,
        max_seq_len: usize,
        head_dim: usize,
        rope_dim: usize,
        theta: f32,
        freq_scale: f32,
        query_lengths: []const usize,
        position_offsets: []const usize,
        consecutive_pairs: bool,
    ) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        if (query_lengths.len != batch or position_offsets.len != batch) return error.InvalidRoPEInput;

        const data = toBuf(input).data;
        const row_count = batch * max_seq_len;
        if (row_count == 0) return fromBuf(try copyBufShape(WasmBuf.fromSlice(self.allocator, try self.allocator.dupe(f32, data), true), toBuf(input)));
        if (data.len % row_count != 0) return error.InvalidRoPEInput;

        const row_dim = data.len / row_count;
        if (row_dim % head_dim != 0) return error.InvalidRoPEInput;
        const num_heads = row_dim / head_dim;
        const output = try self.allocator.dupe(f32, data);
        errdefer self.allocator.free(output);

        // Build flat position array: one entry per head-sized chunk.
        // Padded positions (pos >= query_lengths[b]) get 0 = identity rotation.
        const total_tokens = row_count * num_heads;
        const positions = try self.allocator.alloc(usize, total_tokens);
        defer self.allocator.free(positions);
        @memset(positions, 0);

        for (0..batch) |b| {
            if (query_lengths[b] > max_seq_len) return error.InvalidRoPEInput;
            for (0..query_lengths[b]) |pos| {
                const absolute_pos = position_offsets[b] + pos;
                const row_base = (b * max_seq_len + pos) * num_heads;
                for (0..num_heads) |h| {
                    positions[row_base + h] = absolute_pos;
                }
            }
        }

        linalg.ropeCore(output, positions, head_dim, rope_dim, theta, freq_scale, consecutive_pairs);
        return fromBuf(try copyBufShape(WasmBuf.fromSlice(self.allocator, output, true), toBuf(input)));
    }

    // --- GQA (Grouped-Query Attention) ---

    fn attentionSinkScores(sink: ops.AttentionSinkMetadata, num_heads: usize) !?[]const f32 {
        if (sink.per_head_tensor) |tensor| {
            const scores = toBuf(tensor).data;
            if (scores.len < num_heads) return error.InvalidAttentionSinkShape;
            return scores[0..num_heads];
        }
        if (sink.slot != null) return error.UnsupportedAttentionSink;
        return null;
    }

    fn softmaxWithOptionalSink(scores: []f32, sink_score: ?f32) void {
        const sink = sink_score orelse {
            activations.softmax(scores, scores.len);
            return;
        };

        var max_score = sink;
        for (scores) |score| max_score = @max(max_score, score);
        if (max_score == -std.math.inf(f32)) {
            @memset(scores, 0.0);
            return;
        }

        var denom: f32 = @exp(sink - max_score);
        for (scores) |score| {
            if (score != -std.math.inf(f32)) denom += @exp(score - max_score);
        }

        const inv_denom = 1.0 / denom;
        for (scores) |*score| {
            score.* = if (score.* == -std.math.inf(f32)) 0.0 else @exp(score.* - max_score) * inv_denom;
        }
    }

    /// GQA causal self-attention. Like causalSelfAttentionOp but supports
    /// num_kv_heads < num_heads — each KV head is shared across
    /// heads_per_group = num_heads / num_kv_heads query heads.
    fn gqaCausalAttentionOp(
        ctx: *anyopaque,
        Q_ct: CT,
        K_ct: CT,
        V_ct: CT,
        attn_bias_ct: ?CT,
        batch: usize,
        seq_len: usize,
        num_heads: usize,
        num_kv_heads: usize,
        head_dim: usize,
    ) anyerror!CT {
        return gqaCausalAttentionWithSink(ctx, Q_ct, K_ct, V_ct, attn_bias_ct, null, batch, seq_len, num_heads, num_kv_heads, head_dim);
    }

    fn gqaCausalAttentionWithSink(
        ctx: *anyopaque,
        Q_ct: CT,
        K_ct: CT,
        V_ct: CT,
        attn_bias_ct: ?CT,
        sink_scores: ?[]const f32,
        batch: usize,
        seq_len: usize,
        num_heads: usize,
        num_kv_heads: usize,
        head_dim: usize,
    ) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));

        // MHA fast-path: when num_kv_heads == num_heads, delegate to existing causal attention
        if (num_kv_heads == num_heads and sink_scores == null) {
            return causalSelfAttentionOp(ctx, Q_ct, K_ct, V_ct, attn_bias_ct, batch, seq_len, num_heads, head_dim);
        }

        const Q = toBuf(Q_ct);
        const K = toBuf(K_ct);
        const V = toBuf(V_ct);
        const H_q = num_heads * head_dim;
        const H_kv = num_kv_heads * head_dim;
        const heads_per_group = num_heads / num_kv_heads;
        const scale: f32 = 1.0 / @sqrt(@as(f32, @floatFromInt(head_dim)));
        const gqa_bias_view = if (attn_bias_ct) |ab| try toBuf(ab).viewF32(self.allocator) else null;
        defer if (gqa_bias_view) |v| {
            if (v.allocated) self.allocator.free(v.data);
        };
        const attn_bias: ?[]const f32 = if (gqa_bias_view) |v| v.data else null;

        const out = try self.allocator.alloc(f32, batch * seq_len * H_q);

        // GPU path: GQA causal attention shader
        if (build_options.enable_webgpu and self.use_gpu and sink_scores == null and
            batch * seq_len * H_q >= WEBGPU_ATTN_THRESHOLD and seq_len <= WEBGPU_ATTN_MAX_SEQ and
            attn_bias == null)
        {
            var q_gpu = GpuInputTensor.fromBuf(Q);
            defer q_gpu.deinit();
            var k_gpu = GpuInputTensor.fromBuf(K);
            defer k_gpu.deinit();
            var v_gpu = GpuInputTensor.fromBuf(V);
            defer v_gpu.deinit();
            var out_gpu = gpuGqaCausalAttention(batch, seq_len, num_heads, num_kv_heads, head_dim, q_gpu.id, k_gpu.id, v_gpu.id, out);
            return fromBuf(try setBufShape2D(WasmBuf.fromSliceWithGpu(self.allocator, out, true, out_gpu.detach(), true), batch * seq_len, H_q));
        }

        @memset(out, 0.0);

        const scores = try self.allocator.alloc(f32, seq_len * seq_len);
        defer self.allocator.free(scores);

        const VEC = wasm_vec_len;

        for (0..batch) |b| {
            for (0..num_heads) |h| {
                const kv_h = h / heads_per_group;
                const q_head_off = h * head_dim;
                const kv_head_off = kv_h * head_dim;

                // scores = Q_head @ K_head^T, scaled, with causal mask
                for (0..seq_len) |qi| {
                    const q_base = (b * seq_len + qi) * H_q + q_head_off;
                    for (0..seq_len) |ki| {
                        if (ki > qi) {
                            scores[qi * seq_len + ki] = -std.math.inf(f32);
                            continue;
                        }
                        const k_base = (b * seq_len + ki) * H_kv + kv_head_off;
                        var acc: @Vector(VEC, f32) = @splat(0.0);
                        var d: usize = 0;
                        while (d + VEC <= head_dim) : (d += VEC) {
                            const qv: @Vector(VEC, f32) = Q.data[q_base + d ..][0..VEC].*;
                            const kv: @Vector(VEC, f32) = K.data[k_base + d ..][0..VEC].*;
                            acc += qv * kv;
                        }
                        var dot = @reduce(.Add, acc);
                        while (d < head_dim) : (d += 1) {
                            dot += Q.data[q_base + d] * K.data[k_base + d];
                        }
                        var s = dot * scale;
                        if (attn_bias) |bias| {
                            s += bias[h * seq_len * seq_len + qi * seq_len + ki];
                        }
                        scores[qi * seq_len + ki] = s;
                    }
                }

                // Softmax per row
                const sink_score: ?f32 = if (sink_scores) |sink| sink[h] else null;
                for (0..seq_len) |qi| {
                    softmaxWithOptionalSink(scores[qi * seq_len ..][0..seq_len], sink_score);
                }

                // output = scores @ V_head (V uses kv_head_off)
                for (0..seq_len) |qi| {
                    const out_base = (b * seq_len + qi) * H_q + q_head_off;
                    for (0..seq_len) |vi| {
                        const w = scores[qi * seq_len + vi];
                        if (w == 0.0) continue;
                        const v_base = (b * seq_len + vi) * H_kv + kv_head_off;
                        const w_splat: @Vector(VEC, f32) = @splat(w);
                        var d: usize = 0;
                        while (d + VEC <= head_dim) : (d += VEC) {
                            const ov: @Vector(VEC, f32) = out[out_base + d ..][0..VEC].*;
                            const vv: @Vector(VEC, f32) = V.data[v_base + d ..][0..VEC].*;
                            out[out_base + d ..][0..VEC].* = ov + w_splat * vv;
                        }
                        while (d < head_dim) : (d += 1) {
                            out[out_base + d] += w * V.data[v_base + d];
                        }
                    }
                }
            }
        }

        return fromBuf(try setBufShape2D(WasmBuf.fromSlice(self.allocator, out, true), batch * seq_len, H_q));
    }

    /// Paged/streaming GQA attention. When a WasmKvCache is active,
    /// appends K/V to the per-layer cache and attends against the full
    /// history. Otherwise delegates to dense causal attention.
    fn gqaPagedAttentionOp(
        ctx: *anyopaque,
        Q_ct: CT,
        K_ct: CT,
        V_ct: CT,
        attn_bias_ct: ?CT,
        attention: ops.AttentionContext,
        batch: usize,
        num_heads: usize,
        num_kv_heads: usize,
        head_dim: usize,
    ) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const sink_scores = try attentionSinkScores(attention.attention_sink, num_heads);
        if (sink_scores != null and self.active_gpu_kv_cache != null and self.active_kv_cache == null) return error.UnsupportedAttentionSink;

        // GPU-resident KV cache path
        if (build_options.enable_webgpu and self.use_gpu and sink_scores == null) {
            if (self.active_gpu_kv_cache) |gpu_cache| {
                const Q = toBuf(Q_ct);
                const K = toBuf(K_ct);
                const V = toBuf(V_ct);
                const q_len = attention.query_sequence_len;
                const kv_dim = @as(usize, num_kv_heads) * head_dim;
                const layer = attention.layer_index;

                // Also append to CPU cache for consistency
                if (self.active_kv_cache) |cpu_cache| {
                    if (!attention.skip_kv_write) {
                        const new_kv_count = batch * q_len * kv_dim;
                        cpu_cache.appendKv(layer, K.data[0..new_kv_count], V.data[0..new_kv_count]);
                    }
                }

                // Append to GPU cache
                if (!attention.skip_kv_write) {
                    const new_kv_count = batch * q_len * kv_dim;
                    if (K.gpu_tensor) |k_gpu_tensor| {
                        if (V.gpu_tensor) |v_gpu_tensor| {
                            try gpu_cache.appendKvFromGpu(layer, k_gpu_tensor, v_gpu_tensor, 0, new_kv_count);
                        } else {
                            try gpu_cache.appendKv(layer, K.data[0..new_kv_count], V.data[0..new_kv_count]);
                        }
                    } else {
                        try gpu_cache.appendKv(layer, K.data[0..new_kv_count], V.data[0..new_kv_count]);
                    }
                }

                const total_kv_len = if (self.active_kv_cache) |c| c.totalLen() else gpu_cache.cached_len + q_len;

                // Only use GPU cached attention when kv_len fits in shader shared memory
                if (total_kv_len <= WEBGPU_CACHED_ATTN_MAX_KV and attn_bias_ct == null) {
                    const H_q = num_heads * head_dim;
                    const out = try self.allocator.alloc(f32, batch * q_len * H_q);
                    var q_gpu = GpuInputTensor.fromBuf(Q);
                    defer q_gpu.deinit();

                    var out_gpu = gpuGqaCachedAttention(
                        batch,
                        q_len,
                        total_kv_len,
                        num_heads,
                        num_kv_heads,
                        head_dim,
                        q_gpu.id,
                        gpu_cache.k_gpu[layer],
                        0,
                        gpu_cache.v_gpu[layer],
                        gpu_cache.key_format,
                        gpu_cache.value_format,
                        gpu_cache.key_row_bytes,
                        gpu_cache.value_row_bytes,
                        out,
                    );

                    return fromBuf(try setBufShape2D(WasmBuf.fromSliceWithGpu(self.allocator, out, true, out_gpu.detach(), true), batch * q_len, H_q));
                }

                // Fallback to CPU path with CPU cache data
            }
        }

        const cache = self.active_kv_cache orelse {
            return gqaCausalAttentionWithSink(ctx, Q_ct, K_ct, V_ct, attn_bias_ct, sink_scores, batch, attention.query_sequence_len, num_heads, num_kv_heads, head_dim);
        };

        const Q = toBuf(Q_ct);
        const K = toBuf(K_ct);
        const V = toBuf(V_ct);
        const q_len = attention.query_sequence_len;
        const kv_dim = @as(usize, num_kv_heads) * head_dim;
        const layer = attention.layer_index;

        // Append this step's K/V to the cache for this layer.
        if (!attention.skip_kv_write) {
            const new_kv_count = batch * q_len * kv_dim;
            cache.appendKv(layer, K.data[0..new_kv_count], V.data[0..new_kv_count]);
        }

        const total_kv_len = cache.totalLen();

        // Create WasmBuf views into the full cache for this layer.
        const k_full = WasmBuf.fromSlice(self.allocator, cache.k_cache[layer][0 .. batch * total_kv_len * kv_dim], false);
        defer self.allocator.destroy(k_full);
        const v_full = WasmBuf.fromSlice(self.allocator, cache.v_cache[layer][0 .. batch * total_kv_len * kv_dim], false);
        defer self.allocator.destroy(v_full);

        return gqaCachedAttention(self, Q, k_full, v_full, attn_bias_ct, sink_scores, batch, q_len, total_kv_len, num_heads, num_kv_heads, head_dim);
    }

    /// GQA attention with different Q length and KV length.
    /// Q: [batch * q_len, num_heads * head_dim]
    /// K/V: [batch * kv_len, num_kv_heads * head_dim]
    /// Each query at position i (0..q_len) corresponds to absolute position
    /// (kv_len - q_len + i), and can attend to KV positions 0..kv_len-q_len+i.
    fn gqaCachedAttention(
        self: *WasmCompute,
        Q: *WasmBuf,
        K: *WasmBuf,
        V: *WasmBuf,
        attn_bias_ct: ?CT,
        sink_scores: ?[]const f32,
        batch: usize,
        q_len: usize,
        kv_len: usize,
        num_heads: usize,
        num_kv_heads: usize,
        head_dim: usize,
    ) anyerror!CT {
        // Fast path: if q_len == kv_len, use standard causal attention
        if (q_len == kv_len) {
            return gqaCausalAttentionWithSink(@ptrCast(self), @ptrCast(Q), @ptrCast(K), @ptrCast(V), attn_bias_ct, sink_scores, batch, q_len, num_heads, num_kv_heads, head_dim);
        }

        const H_q = num_heads * head_dim;
        const H_kv = num_kv_heads * head_dim;
        const heads_per_group = num_heads / num_kv_heads;
        const scale: f32 = 1.0 / @sqrt(@as(f32, @floatFromInt(head_dim)));
        const kv_offset = kv_len - q_len; // absolute position of first query token

        const out = try self.allocator.alloc(f32, batch * q_len * H_q);
        @memset(out, 0.0);

        const scores = try self.allocator.alloc(f32, kv_len);
        defer self.allocator.free(scores);

        const VEC = wasm_vec_len;

        for (0..batch) |b| {
            for (0..num_heads) |h| {
                const kv_h = h / heads_per_group;
                const q_head_off = h * head_dim;
                const kv_head_off = kv_h * head_dim;

                for (0..q_len) |qi| {
                    const abs_pos = kv_offset + qi;
                    const q_base = (b * q_len + qi) * H_q + q_head_off;

                    // Score against all KV positions up to abs_pos
                    for (0..kv_len) |ki| {
                        if (ki > abs_pos) {
                            scores[ki] = -std.math.inf(f32);
                            continue;
                        }
                        const k_base = (b * kv_len + ki) * H_kv + kv_head_off;

                        var acc: @Vector(VEC, f32) = @splat(0.0);
                        var d: usize = 0;
                        while (d + VEC <= head_dim) : (d += VEC) {
                            const qv: @Vector(VEC, f32) = Q.data[q_base + d ..][0..VEC].*;
                            const kv: @Vector(VEC, f32) = K.data[k_base + d ..][0..VEC].*;
                            acc += qv * kv;
                        }
                        var dot = @reduce(.Add, acc);
                        while (d < head_dim) : (d += 1) {
                            dot += Q.data[q_base + d] * K.data[k_base + d];
                        }
                        scores[ki] = dot * scale;
                    }

                    // Softmax over scores[0..kv_len]
                    const sink_score: ?f32 = if (sink_scores) |sink| sink[h] else null;
                    softmaxWithOptionalSink(scores[0..kv_len], sink_score);

                    // Weighted sum of V
                    const out_base = (b * q_len + qi) * H_q + q_head_off;
                    for (0..kv_len) |vi| {
                        const w = scores[vi];
                        if (w == 0.0) continue;
                        const v_base = (b * kv_len + vi) * H_kv + kv_head_off;

                        const w_splat: @Vector(VEC, f32) = @splat(w);
                        var d: usize = 0;
                        while (d + VEC <= head_dim) : (d += VEC) {
                            const ov: @Vector(VEC, f32) = out[out_base + d ..][0..VEC].*;
                            const vv: @Vector(VEC, f32) = V.data[v_base + d ..][0..VEC].*;
                            out[out_base + d ..][0..VEC].* = ov + w_splat * vv;
                        }
                        while (d < head_dim) : (d += 1) {
                            out[out_base + d] += w * V.data[v_base + d];
                        }
                    }
                }
            }
        }

        return fromBuf(try setBufShape2D(WasmBuf.fromSlice(self.allocator, out, true), batch * q_len, H_q));
    }

    /// Dispatch matmul C = A @ B^T to WebGPU compute shader.
    fn gpuSgemmTransB(m: usize, n: usize, k: usize, a_buf: wasm_extern.GpuBufferId, b_buf: wasm_extern.GpuBufferId, out: []f32) GpuTensor {
        if (!build_options.enable_webgpu) unreachable;

        const out_buf = GpuTensor.create(out.len * @sizeOf(f32));

        wasm_extern.matmulTransB(a_buf, b_buf, out_buf.id, @intCast(m), @intCast(n), @intCast(k));
        return out_buf;
    }

    /// Decide whether a given (rows, qtype, k) call to gpuSgemmTransBQuant
    /// goes through the MMV (qLen=1) shader family or the tiled GEMM family.
    /// Delegates to the shared `graph/quant_matmul.select` shape selector so
    /// the WebGPU and Metal backends share one source of truth, then layers
    /// on a WebGPU-specific carve-out for I2_S (whose GEMM does BitNet-style
    /// per-row int8 activation pre-quantization that the MMV variant doesn't
    /// replicate yet — see web/shaders/matmul_transb_i2_s.wgsl).
    fn shouldUseMmv(rows: usize, in_dim: usize, out_dim: usize, qtype: tensor_types.KnownTensorType) bool {
        if (qtype == .I2_S) return false;
        const format = quantFormatFromTensorType(.{ .known = qtype }) orelse return false;
        const dispatch = quant_matmul.select(.{
            .rows = rows,
            .in_dim = in_dim,
            .out_dim = out_dim,
            .format = format,
        });
        return dispatch == .mmv;
    }

    /// Dispatch quantized matmul C = A @ dequant(B_quant)^T to WebGPU.
    /// B_quant is raw quantized bytes uploaded as-is; the shader dequantizes.
    fn gpuSgemmTransBQuant(m: usize, n: usize, k: usize, a_buf: wasm_extern.GpuBufferId, b_buf: wasm_extern.GpuBufferId, out: []f32, qtype: tensor_types.KnownTensorType) GpuTensor {
        if (!build_options.enable_webgpu) unreachable;

        const out_buf = GpuTensor.create(out.len * @sizeOf(f32));
        const m32: u32 = @intCast(m);
        const n32: u32 = @intCast(n);
        const k32: u32 = @intCast(k);
        const out_id = out_buf.id;

        if (shouldUseMmv(m, k, n, qtype)) {
            switch (qtype) {
                .Q4_0 => wasm_extern.matmulTransBQ4_0Mmv(a_buf, b_buf, out_id, m32, n32, k32),
                .Q4_1 => wasm_extern.matmulTransBQ4_1Mmv(a_buf, b_buf, out_id, m32, n32, k32),
                .Q5_0 => wasm_extern.matmulTransBQ5_0Mmv(a_buf, b_buf, out_id, m32, n32, k32),
                .Q5_1 => wasm_extern.matmulTransBQ5_1Mmv(a_buf, b_buf, out_id, m32, n32, k32),
                .Q8_0 => wasm_extern.matmulTransBQ8_0Mmv(a_buf, b_buf, out_id, m32, n32, k32),
                .Q8_1 => wasm_extern.matmulTransBQ8_1Mmv(a_buf, b_buf, out_id, m32, n32, k32),
                .IQ4_NL => wasm_extern.matmulTransBIQ4_NLMmv(a_buf, b_buf, out_id, m32, n32, k32),
                .IQ4_XS => wasm_extern.matmulTransBIQ4_XSMmv(a_buf, b_buf, out_id, m32, n32, k32),
                .Q2_K => wasm_extern.matmulTransBQ2_KMmv(a_buf, b_buf, out_id, m32, n32, k32),
                .Q3_K => wasm_extern.matmulTransBQ3_KMmv(a_buf, b_buf, out_id, m32, n32, k32),
                .Q4_K => wasm_extern.matmulTransBQ4_KMmv(a_buf, b_buf, out_id, m32, n32, k32),
                .Q5_K => wasm_extern.matmulTransBQ5_KMmv(a_buf, b_buf, out_id, m32, n32, k32),
                .Q6_K => wasm_extern.matmulTransBQ6_KMmv(a_buf, b_buf, out_id, m32, n32, k32),
                .Q8_K => wasm_extern.matmulTransBQ8_KMmv(a_buf, b_buf, out_id, m32, n32, k32),
                .I8_S => wasm_extern.matmulTransBI8_SMmv(a_buf, b_buf, out_id, m32, n32, k32),
                .Q1_0 => wasm_extern.matmulTransBQ1_0Mmv(a_buf, b_buf, out_id, m32, n32, k32),
                .TQ1_0 => wasm_extern.matmulTransBTQ1_0Mmv(a_buf, b_buf, out_id, m32, n32, k32),
                .TQ2_0 => wasm_extern.matmulTransBTQ2_0Mmv(a_buf, b_buf, out_id, m32, n32, k32),
                .MXFP4 => wasm_extern.matmulTransBMXFP4Mmv(a_buf, b_buf, out_id, m32, n32, k32),
                .NVFP4 => wasm_extern.matmulTransBNVFP4Mmv(a_buf, b_buf, out_id, m32, n32, k32),
                .IQ1_S => wasm_extern.matmulTransBIQ1_SMmv(a_buf, b_buf, out_id, m32, n32, k32),
                .IQ1_M => wasm_extern.matmulTransBIQ1_MMmv(a_buf, b_buf, out_id, m32, n32, k32),
                .IQ2_XXS => wasm_extern.matmulTransBIQ2_XXSMmv(a_buf, b_buf, out_id, m32, n32, k32),
                .IQ2_XS => wasm_extern.matmulTransBIQ2_XSMmv(a_buf, b_buf, out_id, m32, n32, k32),
                .IQ2_S => wasm_extern.matmulTransBIQ2_SMmv(a_buf, b_buf, out_id, m32, n32, k32),
                .IQ3_XXS => wasm_extern.matmulTransBIQ3_XXSMmv(a_buf, b_buf, out_id, m32, n32, k32),
                .IQ3_S => wasm_extern.matmulTransBIQ3_SMmv(a_buf, b_buf, out_id, m32, n32, k32),
                else => unreachable,
            }
        } else {
            switch (qtype) {
                .Q4_0 => wasm_extern.matmulTransBQ4_0(a_buf, b_buf, out_id, m32, n32, k32),
                .Q4_1 => wasm_extern.matmulTransBQ4_1(a_buf, b_buf, out_id, m32, n32, k32),
                .Q5_0 => wasm_extern.matmulTransBQ5_0(a_buf, b_buf, out_id, m32, n32, k32),
                .Q5_1 => wasm_extern.matmulTransBQ5_1(a_buf, b_buf, out_id, m32, n32, k32),
                .Q8_0 => wasm_extern.matmulTransBQ8_0(a_buf, b_buf, out_id, m32, n32, k32),
                .Q8_1 => wasm_extern.matmulTransBQ8_1(a_buf, b_buf, out_id, m32, n32, k32),
                .IQ4_NL => wasm_extern.matmulTransBIQ4_NL(a_buf, b_buf, out_id, m32, n32, k32),
                .IQ4_XS => wasm_extern.matmulTransBIQ4_XS(a_buf, b_buf, out_id, m32, n32, k32),
                .Q2_K => wasm_extern.matmulTransBQ2_K(a_buf, b_buf, out_id, m32, n32, k32),
                .Q3_K => wasm_extern.matmulTransBQ3_K(a_buf, b_buf, out_id, m32, n32, k32),
                .Q4_K => wasm_extern.matmulTransBQ4_K(a_buf, b_buf, out_id, m32, n32, k32),
                .Q5_K => wasm_extern.matmulTransBQ5_K(a_buf, b_buf, out_id, m32, n32, k32),
                .Q6_K => wasm_extern.matmulTransBQ6_K(a_buf, b_buf, out_id, m32, n32, k32),
                .Q8_K => wasm_extern.matmulTransBQ8_K(a_buf, b_buf, out_id, m32, n32, k32),
                .I2_S => wasm_extern.matmulTransBI2_S(a_buf, b_buf, out_id, m32, n32, k32),
                .I8_S => wasm_extern.matmulTransBI8_S(a_buf, b_buf, out_id, m32, n32, k32),
                .Q1_0 => wasm_extern.matmulTransBQ1_0(a_buf, b_buf, out_id, m32, n32, k32),
                .TQ1_0 => wasm_extern.matmulTransBTQ1_0(a_buf, b_buf, out_id, m32, n32, k32),
                .TQ2_0 => wasm_extern.matmulTransBTQ2_0(a_buf, b_buf, out_id, m32, n32, k32),
                .MXFP4 => wasm_extern.matmulTransBMXFP4(a_buf, b_buf, out_id, m32, n32, k32),
                .NVFP4 => wasm_extern.matmulTransBNVFP4(a_buf, b_buf, out_id, m32, n32, k32),
                .IQ1_S => wasm_extern.matmulTransBIQ1_S(a_buf, b_buf, out_id, m32, n32, k32),
                .IQ1_M => wasm_extern.matmulTransBIQ1_M(a_buf, b_buf, out_id, m32, n32, k32),
                .IQ2_XXS => wasm_extern.matmulTransBIQ2_XXS(a_buf, b_buf, out_id, m32, n32, k32),
                .IQ2_XS => wasm_extern.matmulTransBIQ2_XS(a_buf, b_buf, out_id, m32, n32, k32),
                .IQ2_S => wasm_extern.matmulTransBIQ2_S(a_buf, b_buf, out_id, m32, n32, k32),
                .IQ3_XXS => wasm_extern.matmulTransBIQ3_XXS(a_buf, b_buf, out_id, m32, n32, k32),
                .IQ3_S => wasm_extern.matmulTransBIQ3_S(a_buf, b_buf, out_id, m32, n32, k32),
                else => unreachable,
            }
        }

        return out_buf;
    }

    /// Dispatch fused attention to WebGPU compute shader.
    /// Uploads Q, K, V, mask; runs fused QK^T+softmax+V shader; downloads result.
    fn gpuAttention(
        batch: usize,
        seq_len: usize,
        num_heads: usize,
        head_dim: usize,
        scale: f32,
        q_buf: wasm_extern.GpuBufferId,
        k_buf: wasm_extern.GpuBufferId,
        v_buf: wasm_extern.GpuBufferId,
        mask: []const i64,
        out: []f32,
    ) GpuTensor {
        if (!build_options.enable_webgpu) unreachable;

        const total = batch * seq_len;
        const mask_elems = total;
        const mask_bytes: u32 = @intCast(mask_elems * 4);
        const out_bytes: u32 = @intCast(total * num_heads * head_dim * 4);

        const mask_buf = wasm_extern.createBuffer(mask_bytes);
        const out_buf = GpuTensor.create(out_bytes);

        // Convert i64 mask to u32 for GPU (reuse out buffer as scratch since it's uninitialized)
        const mask_u32: [*]u32 = @ptrCast(@alignCast(out.ptr));
        for (0..mask_elems) |i| {
            mask_u32[i] = if (mask[i] != 0) 1 else 0;
        }
        wasm_extern.upload(mask_buf, @as([*]const u8, @ptrCast(mask_u32))[0..mask_bytes]);

        // Scale is baked into the params uniform on the JS side
        _ = scale;
        wasm_extern.attention(q_buf, k_buf, v_buf, mask_buf, out_buf.id, @intCast(batch), @intCast(seq_len), @intCast(num_heads), @intCast(head_dim));

        out_buf.downloadF32(out);

        wasm_extern.freeBuffer(mask_buf);
        return out_buf;
    }

    fn gpuAdd(
        a_buf: wasm_extern.GpuBufferId,
        b_buf: wasm_extern.GpuBufferId,
        out: []f32,
    ) GpuTensor {
        if (!build_options.enable_webgpu) unreachable;
        const out_bytes: u32 = @intCast(out.len * @sizeOf(f32));
        const out_buf = GpuTensor.create(out_bytes);
        wasm_extern.add(a_buf, b_buf, out_buf.id, @intCast(out.len));
        return out_buf;
    }

    fn gpuGelu(
        inp_buf: wasm_extern.GpuBufferId,
        out: []f32,
    ) GpuTensor {
        if (!build_options.enable_webgpu) unreachable;
        const out_bytes: u32 = @intCast(out.len * @sizeOf(f32));
        const out_buf = GpuTensor.create(out_bytes);
        wasm_extern.gelu(inp_buf, out_buf.id, @intCast(out.len));
        return out_buf;
    }

    fn gpuBinaryBroadcast(
        a_buf: wasm_extern.GpuBufferId,
        b_buf: wasm_extern.GpuBufferId,
        a_len: usize,
        b_len: usize,
        out: []f32,
        comptime op: enum { add, mul, sub, div, lt },
    ) GpuTensor {
        if (!build_options.enable_webgpu) unreachable;
        const out_bytes: u32 = @intCast(out.len * @sizeOf(f32));
        const out_buf = GpuTensor.create(out_bytes);
        switch (op) {
            .add => wasm_extern.addBroadcast(a_buf, b_buf, out_buf.id, @intCast(out.len), @intCast(a_len), @intCast(b_len)),
            .mul => wasm_extern.mul(a_buf, b_buf, out_buf.id, @intCast(out.len), @intCast(a_len), @intCast(b_len)),
            .sub => wasm_extern.sub(a_buf, b_buf, out_buf.id, @intCast(out.len), @intCast(a_len), @intCast(b_len)),
            .div => wasm_extern.div(a_buf, b_buf, out_buf.id, @intCast(out.len), @intCast(a_len), @intCast(b_len)),
            .lt => wasm_extern.lessThan(a_buf, b_buf, out_buf.id, @intCast(out.len), @intCast(a_len), @intCast(b_len)),
        }
        return out_buf;
    }

    fn gpuUnary(
        inp_buf: wasm_extern.GpuBufferId,
        out: []f32,
        comptime op: enum { neg, sqrt, rsqrt, exp_op, log_op, sin_op, cos_op, tanh_prim, abs_op, erf },
    ) GpuTensor {
        if (!build_options.enable_webgpu) unreachable;
        const out_bytes: u32 = @intCast(out.len * @sizeOf(f32));
        const out_buf = GpuTensor.create(out_bytes);
        switch (op) {
            .neg => wasm_extern.neg(inp_buf, out_buf.id, @intCast(out.len)),
            .sqrt => wasm_extern.sqrt(inp_buf, out_buf.id, @intCast(out.len)),
            .rsqrt => wasm_extern.rsqrt(inp_buf, out_buf.id, @intCast(out.len)),
            .exp_op => wasm_extern.exp(inp_buf, out_buf.id, @intCast(out.len)),
            .log_op => wasm_extern.log(inp_buf, out_buf.id, @intCast(out.len)),
            .sin_op => wasm_extern.sin(inp_buf, out_buf.id, @intCast(out.len)),
            .cos_op => wasm_extern.cos(inp_buf, out_buf.id, @intCast(out.len)),
            .tanh_prim => wasm_extern.tanh(inp_buf, out_buf.id, @intCast(out.len)),
            .abs_op => wasm_extern.abs(inp_buf, out_buf.id, @intCast(out.len)),
            .erf => wasm_extern.erf(inp_buf, out_buf.id, @intCast(out.len)),
        }
        return out_buf;
    }

    fn gpuWhereSelect(
        cond_buf: wasm_extern.GpuBufferId,
        true_buf: wasm_extern.GpuBufferId,
        false_buf: wasm_extern.GpuBufferId,
        true_len: usize,
        false_len: usize,
        out: []f32,
    ) GpuTensor {
        if (!build_options.enable_webgpu) unreachable;
        const out_bytes: u32 = @intCast(out.len * @sizeOf(f32));
        const out_buf = GpuTensor.create(out_bytes);
        wasm_extern.whereSelect(cond_buf, true_buf, false_buf, out_buf.id, @intCast(out.len), @intCast(true_len), @intCast(false_len));
        return out_buf;
    }

    fn gpuSoftmax(
        inp_buf: wasm_extern.GpuBufferId,
        rows: usize,
        dim: usize,
        out: []f32,
        comptime log_mode: bool,
    ) GpuTensor {
        if (!build_options.enable_webgpu) unreachable;
        const out_bytes: u32 = @intCast(out.len * @sizeOf(f32));
        const out_buf = GpuTensor.create(out_bytes);
        if (log_mode) {
            wasm_extern.logSoftmax(inp_buf, out_buf.id, @intCast(rows), @intCast(dim));
        } else {
            wasm_extern.softmax(inp_buf, out_buf.id, @intCast(rows), @intCast(dim));
        }
        return out_buf;
    }

    fn gpuReduceLastDim(
        inp_buf: wasm_extern.GpuBufferId,
        rows: usize,
        dim: usize,
        out: []f32,
        comptime mode: enum { sum, max, mean },
    ) GpuTensor {
        if (!build_options.enable_webgpu) unreachable;
        const out_bytes: u32 = @intCast(out.len * @sizeOf(f32));
        const out_buf = GpuTensor.create(out_bytes);
        switch (mode) {
            .sum => wasm_extern.reduceSumLastDim(inp_buf, out_buf.id, @intCast(rows), @intCast(dim)),
            .max => wasm_extern.reduceMaxLastDim(inp_buf, out_buf.id, @intCast(rows), @intCast(dim)),
            .mean => wasm_extern.reduceMeanLastDim(inp_buf, out_buf.id, @intCast(rows), @intCast(dim)),
        }
        return out_buf;
    }

    fn gpuReduce(
        inp_buf: wasm_extern.GpuBufferId,
        out: []f32,
        input_shape: []const i64,
        axes: []const u8,
        out_shape: []const i64,
        comptime mode: enum { sum, max, mean },
    ) GpuTensor {
        if (!build_options.enable_webgpu) unreachable;

        var input_u32 = @as([8]u32, @splat(1));
        var output_u32 = @as([8]u32, @splat(1));
        var reduced = @as([8]u32, @splat(0));
        var in_strides = @as([8]u32, @splat(1));
        var out_strides = @as([8]u32, @splat(1));
        var kept_axes = @as([8]u32, @splat(0));
        var reduced_axes = @as([8]u32, @splat(0));

        for (input_shape, 0..) |dim, i| input_u32[i] = @intCast(dim);
        for (out_shape, 0..) |dim, i| output_u32[i] = @intCast(dim);
        for (axes, 0..) |axis, i| {
            reduced[axis] = 1;
            reduced_axes[i] = axis;
        }
        var kept_idx: usize = 0;
        for (0..input_shape.len) |axis| {
            if (reduced[axis] == 0) {
                kept_axes[kept_idx] = @intCast(axis);
                kept_idx += 1;
            }
        }

        var in_stride_usize: [8]usize = undefined;
        var out_stride_usize: [8]usize = undefined;
        computeStrides(input_shape, in_stride_usize[0..input_shape.len]);
        if (out_shape.len > 0) computeStrides(out_shape, out_stride_usize[0..out_shape.len]);
        for (0..input_shape.len) |i| in_strides[i] = @intCast(in_stride_usize[i]);
        for (0..out_shape.len) |i| out_strides[i] = @intCast(out_stride_usize[i]);

        var reduce_count: usize = 1;
        for (axes) |axis| reduce_count *= @intCast(input_shape[axis]);

        const out_bytes: u32 = @intCast(out.len * @sizeOf(f32));
        const out_buf = GpuTensor.create(out_bytes);
        switch (mode) {
            .sum => wasm_extern.reduceSum(inp_buf, out_buf.id, @intCast(out.len), @intCast(reduce_count), @intCast(input_shape.len), @intCast(out_shape.len), input_u32[0..], output_u32[0..], reduced[0..], in_strides[0..], out_strides[0..], kept_axes[0..], reduced_axes[0..]),
            .max => wasm_extern.reduceMax(inp_buf, out_buf.id, @intCast(out.len), @intCast(reduce_count), @intCast(input_shape.len), @intCast(out_shape.len), input_u32[0..], output_u32[0..], reduced[0..], in_strides[0..], out_strides[0..], kept_axes[0..], reduced_axes[0..]),
            .mean => wasm_extern.reduceMean(inp_buf, out_buf.id, @intCast(out.len), @intCast(reduce_count), @intCast(input_shape.len), @intCast(out_shape.len), input_u32[0..], output_u32[0..], reduced[0..], in_strides[0..], out_strides[0..], kept_axes[0..], reduced_axes[0..]),
        }
        return out_buf;
    }

    fn gpuBroadcastInDim(
        inp_buf: wasm_extern.GpuBufferId,
        out: []f32,
        target_shape: []const i64,
        broadcast_axes: []const u8,
        input_shape: []const i64,
    ) GpuTensor {
        if (!build_options.enable_webgpu) unreachable;

        var target_u32 = @as([8]u32, @splat(1));
        var input_u32 = @as([8]u32, @splat(1));
        var axes_u32 = @as([8]u32, @splat(0));
        var out_strides = @as([8]u32, @splat(1));
        var in_strides = @as([8]u32, @splat(1));

        for (target_shape, 0..) |dim, i| target_u32[i] = @intCast(dim);
        for (input_shape, 0..) |dim, i| input_u32[i] = @intCast(dim);
        for (broadcast_axes, 0..) |axis, i| axes_u32[i] = axis;

        var out_stride_usize: [8]usize = undefined;
        var in_stride_usize: [8]usize = undefined;
        computeStrides(target_shape, out_stride_usize[0..target_shape.len]);
        computeStrides(input_shape, in_stride_usize[0..input_shape.len]);
        for (0..target_shape.len) |i| out_strides[i] = @intCast(out_stride_usize[i]);
        for (0..input_shape.len) |i| in_strides[i] = @intCast(in_stride_usize[i]);

        const out_bytes: u32 = @intCast(out.len * @sizeOf(f32));
        const out_buf = GpuTensor.create(out_bytes);
        wasm_extern.broadcastInDim(
            inp_buf,
            out_buf.id,
            @intCast(out.len),
            @intCast(target_shape.len),
            @intCast(input_shape.len),
            target_u32[0..],
            input_u32[0..],
            axes_u32[0..],
            out_strides[0..],
            in_strides[0..],
        );
        return out_buf;
    }

    /// Dispatch DeBERTa disentangled attention to WebGPU.
    /// Q/K/V are [batch*seq_len, H], Q_r/K_r are [2*seq_len-1, H], mask is [batch*seq_len].
    fn gpuDebertaDisentangledAttention(
        batch: usize,
        seq_len: usize,
        num_heads: usize,
        head_dim: usize,
        q_buf: wasm_extern.GpuBufferId,
        k_buf: wasm_extern.GpuBufferId,
        v_buf: wasm_extern.GpuBufferId,
        q_r_buf: wasm_extern.GpuBufferId,
        k_r_buf: wasm_extern.GpuBufferId,
        mask: []const i64,
        out: []f32,
    ) GpuTensor {
        if (!build_options.enable_webgpu) unreachable;

        const total = batch * seq_len;
        const mask_bytes: u32 = @intCast(total * @sizeOf(u32));
        const out_bytes: u32 = @intCast(total * num_heads * head_dim * @sizeOf(f32));

        const mask_buf = wasm_extern.createBuffer(mask_bytes);
        const out_buf = GpuTensor.create(out_bytes);

        const mask_u32: [*]u32 = @ptrCast(@alignCast(out.ptr));
        for (0..total) |i| {
            mask_u32[i] = if (mask[i] != 0) 1 else 0;
        }
        wasm_extern.upload(mask_buf, @as([*]const u8, @ptrCast(mask_u32))[0..mask_bytes]);

        wasm_extern.disentangledRelativeAttention(
            q_buf,
            k_buf,
            v_buf,
            q_r_buf,
            k_r_buf,
            mask_buf,
            out_buf.id,
            @intCast(batch),
            @intCast(seq_len),
            @intCast(num_heads),
            @intCast(head_dim),
        );

        out_buf.downloadF32(out);
        wasm_extern.freeBuffer(mask_buf);
        return out_buf;
    }

    /// Dispatch causal self-attention to WebGPU compute shader.
    /// No mask buffer — causal masking is built into the shader.
    fn gpuCausalAttention(
        batch: usize,
        seq_len: usize,
        num_heads: usize,
        head_dim: usize,
        q_buf: wasm_extern.GpuBufferId,
        k_buf: wasm_extern.GpuBufferId,
        v_buf: wasm_extern.GpuBufferId,
        out: []f32,
    ) GpuTensor {
        if (!build_options.enable_webgpu) unreachable;

        const total = batch * seq_len;
        const out_bytes: u32 = @intCast(total * num_heads * head_dim * 4);
        const out_buf = GpuTensor.create(out_bytes);

        wasm_extern.causalAttention(q_buf, k_buf, v_buf, out_buf.id, @intCast(batch), @intCast(seq_len), @intCast(num_heads), @intCast(head_dim));

        out_buf.downloadF32(out);

        return out_buf;
    }

    /// Dispatch GQA causal attention to WebGPU compute shader.
    /// Q is [batch*seq_len, num_heads*head_dim], K/V are [batch*seq_len, num_kv_heads*head_dim].
    fn gpuGqaCausalAttention(
        batch: usize,
        seq_len: usize,
        num_heads: usize,
        num_kv_heads: usize,
        head_dim: usize,
        q_buf: wasm_extern.GpuBufferId,
        k_buf: wasm_extern.GpuBufferId,
        v_buf: wasm_extern.GpuBufferId,
        out: []f32,
    ) GpuTensor {
        if (!build_options.enable_webgpu) unreachable;

        const total = batch * seq_len;
        const out_bytes: u32 = @intCast(total * num_heads * head_dim * 4);
        const out_buf = GpuTensor.create(out_bytes);

        wasm_extern.gqaCausalAttention(q_buf, k_buf, v_buf, out_buf.id, @intCast(batch), @intCast(seq_len), @intCast(num_heads), @intCast(num_kv_heads), @intCast(head_dim));

        out_buf.downloadF32(out);

        return out_buf;
    }

    /// Dispatch GQA cached attention to WebGPU. Q is [batch*q_len, H_q],
    /// K/V are GPU-resident buffers with [batch*kv_len, H_kv] data.
    fn gpuGqaCachedAttention(
        batch: usize,
        q_len: usize,
        kv_len: usize,
        num_heads: usize,
        num_kv_heads: usize,
        head_dim: usize,
        q_buf: wasm_extern.GpuBufferId,
        k_gpu_buf: wasm_extern.GpuBufferId,
        k_aux_gpu_buf: wasm_extern.GpuBufferId,
        v_gpu_buf: wasm_extern.GpuBufferId,
        key_format: GpuKvKeyFormat,
        value_format: GpuKvValueFormat,
        key_row_bytes: u32,
        value_row_bytes: u32,
        out: []f32,
    ) GpuTensor {
        if (!build_options.enable_webgpu) unreachable;

        const H_q = num_heads * head_dim;
        const out_bytes: u32 = @intCast(batch * q_len * H_q * 4);
        const out_buf = GpuTensor.create(out_bytes);

        wasm_extern.gqaCachedAttentionEx(
            q_buf,
            k_gpu_buf,
            k_aux_gpu_buf,
            v_gpu_buf,
            out_buf.id,
            @intCast(batch),
            @intCast(q_len),
            @intCast(kv_len),
            @intCast(num_heads),
            @intCast(num_kv_heads),
            @intCast(head_dim),
            key_format,
            value_format,
            key_row_bytes,
            value_row_bytes,
            0,
        );

        out_buf.downloadF32(out);
        return out_buf;
    }

    /// Dispatch cross-attention to WebGPU compute shader.
    /// Q is [batch, dec_seq, H], K/V are [batch, enc_seq, H].
    fn gpuCrossAttention(
        batch: usize,
        dec_seq: usize,
        enc_seq: usize,
        num_heads: usize,
        head_dim: usize,
        q_buf: wasm_extern.GpuBufferId,
        k_buf: wasm_extern.GpuBufferId,
        v_buf: wasm_extern.GpuBufferId,
        enc_mask: []const i64,
        out: []f32,
    ) GpuTensor {
        if (!build_options.enable_webgpu) unreachable;

        const H = num_heads * head_dim;
        const mask_elems = batch * enc_seq;
        const mask_bytes: u32 = @intCast(mask_elems * 4);
        const out_bytes: u32 = @intCast(batch * dec_seq * H * 4);

        const mask_buf = wasm_extern.createBuffer(mask_bytes);
        const out_buf = GpuTensor.create(out_bytes);

        // Convert i64 mask to u32 for GPU (reuse out buffer as scratch)
        const mask_u32: [*]u32 = @ptrCast(@alignCast(out.ptr));
        for (0..mask_elems) |i| {
            mask_u32[i] = if (enc_mask[i] != 0) 1 else 0;
        }
        wasm_extern.upload(mask_buf, @as([*]const u8, @ptrCast(mask_u32))[0..mask_bytes]);

        wasm_extern.crossAttention(q_buf, k_buf, v_buf, mask_buf, out_buf.id, @intCast(batch), @intCast(dec_seq), @intCast(enc_seq), @intCast(num_heads), @intCast(head_dim));

        out_buf.downloadF32(out);

        wasm_extern.freeBuffer(mask_buf);
        return out_buf;
    }

    // ── Primitive ops for training ─────────────────────────────────────────

    /// Binary op helper that handles implicit trailing-dimension broadcasting.
    /// When one operand is shorter and evenly divides the longer, it repeats cyclically.
    fn primBinaryBroadcast(allocator: std.mem.Allocator, a_data: []const f32, b_data: []const f32, comptime op: enum { add, mul, sub, div, lt }) ![]f32 {
        const big = if (a_data.len >= b_data.len) a_data else b_data;
        const small = if (a_data.len >= b_data.len) b_data else a_data;
        const a_is_big = a_data.len >= b_data.len;
        const output = try allocator.alloc(f32, big.len);

        if (small.len == big.len) {
            for (big, small, 0..) |va, vb, i| {
                const a_val = if (a_is_big) va else vb;
                const b_val = if (a_is_big) vb else va;
                output[i] = switch (op) {
                    .add => a_val + b_val,
                    .mul => a_val * b_val,
                    .sub => a_val - b_val,
                    .div => a_val / b_val,
                    .lt => if (a_val < b_val) @as(f32, 1.0) else @as(f32, 0.0),
                };
            }
        } else {
            for (0..big.len) |i| {
                const bi = i % small.len;
                const a_val = if (a_is_big) big[i] else small[bi];
                const b_val = if (a_is_big) small[bi] else big[i];
                output[i] = switch (op) {
                    .add => a_val + b_val,
                    .mul => a_val * b_val,
                    .sub => a_val - b_val,
                    .div => a_val / b_val,
                    .lt => if (a_val < b_val) @as(f32, 1.0) else @as(f32, 0.0),
                };
            }
        }
        return output;
    }

    fn primSubtractOp(ctx: *anyopaque, a: CT, b: CT) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const a_buf = toBuf(a);
        const b_buf = toBuf(b);
        const out_len = @max(a_buf.len, b_buf.len);
        if (shouldPreferGpuElementwise(self, a_buf, b_buf, out_len) and isModuloBroadcastCompatible(a_buf.len, b_buf.len)) {
            const output = try self.allocator.alloc(f32, out_len);
            var a_gpu = GpuInputTensor.fromBuf(a_buf);
            defer a_gpu.deinit();
            var b_gpu = GpuInputTensor.fromBuf(b_buf);
            defer b_gpu.deinit();
            var out_gpu = gpuBinaryBroadcast(a_gpu.id, b_gpu.id, a_buf.len, b_buf.len, output, .sub);
            const out_buf = WasmBuf.fromSliceWithGpu(self.allocator, output, true, out_gpu.detach(), true);
            return fromBuf(if (a_buf.len >= b_buf.len) try copyBufShape(out_buf, a_buf) else try copyBufShape(out_buf, b_buf));
        }
        const output = try primBinaryBroadcast(self.allocator, a_buf.data, b_buf.data, .sub);
        const out_buf = WasmBuf.fromSlice(self.allocator, output, true);
        return fromBuf(if (a_buf.len >= b_buf.len) try copyBufShape(out_buf, a_buf) else try copyBufShape(out_buf, b_buf));
    }

    fn primDivideOp(ctx: *anyopaque, a: CT, b: CT) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const a_buf = toBuf(a);
        const b_buf = toBuf(b);
        const out_len = @max(a_buf.len, b_buf.len);
        if (shouldPreferGpuElementwise(self, a_buf, b_buf, out_len) and isModuloBroadcastCompatible(a_buf.len, b_buf.len)) {
            const output = try self.allocator.alloc(f32, out_len);
            var a_gpu = GpuInputTensor.fromBuf(a_buf);
            defer a_gpu.deinit();
            var b_gpu = GpuInputTensor.fromBuf(b_buf);
            defer b_gpu.deinit();
            var out_gpu = gpuBinaryBroadcast(a_gpu.id, b_gpu.id, a_buf.len, b_buf.len, output, .div);
            const out_buf = WasmBuf.fromSliceWithGpu(self.allocator, output, true, out_gpu.detach(), true);
            return fromBuf(if (a_buf.len >= b_buf.len) try copyBufShape(out_buf, a_buf) else try copyBufShape(out_buf, b_buf));
        }
        const output = try primBinaryBroadcast(self.allocator, a_buf.data, b_buf.data, .div);
        const out_buf = WasmBuf.fromSlice(self.allocator, output, true);
        return fromBuf(if (a_buf.len >= b_buf.len) try copyBufShape(out_buf, a_buf) else try copyBufShape(out_buf, b_buf));
    }

    fn primNegateOp(ctx: *anyopaque, a: CT) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const a_buf = toBuf(a);
        const a_data = a_buf.data;
        const output = try self.allocator.alloc(f32, a_data.len);
        if (shouldPreferGpuElementwise(self, a_buf, null, a_buf.len)) {
            var a_gpu = GpuInputTensor.fromBuf(a_buf);
            defer a_gpu.deinit();
            var out_gpu = gpuUnary(a_gpu.id, output, .neg);
            return fromBuf(try copyBufShape(WasmBuf.fromSliceWithGpu(self.allocator, output, true, out_gpu.detach(), true), a_buf));
        }
        const VEC = wasm_vec_len;
        var i: usize = 0;
        while (i + VEC <= a_data.len) : (i += VEC) {
            const va: @Vector(VEC, f32) = a_data[i..][0..VEC].*;
            output[i..][0..VEC].* = -va;
        }
        while (i < a_data.len) : (i += 1) output[i] = -a_data[i];
        return fromBuf(try copyBufShape(WasmBuf.fromSlice(self.allocator, output, true), a_buf));
    }

    fn primUnaryOp(comptime op: enum { sqrt, rsqrt, exp_op, log_op, sin_op, cos_op, tanh_prim, abs_op }, ctx: *anyopaque, a: CT) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const a_buf = toBuf(a);
        const a_data = a_buf.data;
        const output = try self.allocator.alloc(f32, a_data.len);
        if (shouldPreferGpuElementwise(self, a_buf, null, a_buf.len)) {
            var a_gpu = GpuInputTensor.fromBuf(a_buf);
            defer a_gpu.deinit();
            var out_gpu = switch (op) {
                .sqrt => gpuUnary(a_gpu.id, output, .sqrt),
                .rsqrt => gpuUnary(a_gpu.id, output, .rsqrt),
                .exp_op => gpuUnary(a_gpu.id, output, .exp_op),
                .log_op => gpuUnary(a_gpu.id, output, .log_op),
                .sin_op => gpuUnary(a_gpu.id, output, .sin_op),
                .cos_op => gpuUnary(a_gpu.id, output, .cos_op),
                .tanh_prim => gpuUnary(a_gpu.id, output, .tanh_prim),
                .abs_op => gpuUnary(a_gpu.id, output, .abs_op),
            };
            return fromBuf(try copyBufShape(WasmBuf.fromSliceWithGpu(self.allocator, output, true, out_gpu.detach(), true), a_buf));
        }
        for (a_data, 0..) |v, i| {
            output[i] = switch (op) {
                .sqrt => @sqrt(v),
                .rsqrt => 1.0 / @sqrt(v),
                .exp_op => @exp(v),
                .log_op => @log(v),
                .sin_op => @sin(v),
                .cos_op => @cos(v),
                .tanh_prim => std.math.tanh(v),
                .abs_op => @abs(v),
            };
        }
        return fromBuf(try copyBufShape(WasmBuf.fromSlice(self.allocator, output, true), a_buf));
    }

    fn primSqrtOp(ctx: *anyopaque, a: CT) anyerror!CT {
        return primUnaryOp(.sqrt, ctx, a);
    }
    fn primRsqrtOp(ctx: *anyopaque, a: CT) anyerror!CT {
        return primUnaryOp(.rsqrt, ctx, a);
    }
    fn primExpOp(ctx: *anyopaque, a: CT) anyerror!CT {
        return primUnaryOp(.exp_op, ctx, a);
    }
    fn primLogOp(ctx: *anyopaque, a: CT) anyerror!CT {
        return primUnaryOp(.log_op, ctx, a);
    }
    fn primSinOp(ctx: *anyopaque, a: CT) anyerror!CT {
        return primUnaryOp(.sin_op, ctx, a);
    }
    fn primCosOp(ctx: *anyopaque, a: CT) anyerror!CT {
        return primUnaryOp(.cos_op, ctx, a);
    }
    fn primTanhPrimOp(ctx: *anyopaque, a: CT) anyerror!CT {
        return primUnaryOp(.tanh_prim, ctx, a);
    }
    fn primAbsOp(ctx: *anyopaque, a: CT) anyerror!CT {
        return primUnaryOp(.abs_op, ctx, a);
    }

    fn primErfOp(ctx: *anyopaque, a: CT) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const a_buf = toBuf(a);
        const a_data = a_buf.data;
        const output = try self.allocator.alloc(f32, a_data.len);
        if (shouldPreferGpuElementwise(self, a_buf, null, a_buf.len)) {
            var a_gpu = GpuInputTensor.fromBuf(a_buf);
            defer a_gpu.deinit();
            var out_gpu = gpuUnary(a_gpu.id, output, .erf);
            return fromBuf(try copyBufShape(WasmBuf.fromSliceWithGpu(self.allocator, output, true, out_gpu.detach(), true), a_buf));
        }
        // Abramowitz & Stegun approximation (max error ~1.5e-7)
        for (a_data, 0..) |v, i| {
            const x = @abs(v);
            const t = 1.0 / (1.0 + 0.3275911 * x);
            const poly = t * (0.254829592 + t * (-0.284496736 + t * (1.421413741 + t * (-1.453152027 + t * 1.061405429))));
            const result = 1.0 - poly * @exp(-x * x);
            output[i] = if (v >= 0) result else -result;
        }
        return fromBuf(try copyBufShape(WasmBuf.fromSlice(self.allocator, output, true), a_buf));
    }

    fn primLessThanOp(ctx: *anyopaque, a: CT, b: CT) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const a_buf = toBuf(a);
        const b_buf = toBuf(b);
        const out_len = @max(a_buf.len, b_buf.len);
        if (shouldPreferGpuElementwise(self, a_buf, b_buf, out_len) and isModuloBroadcastCompatible(a_buf.len, b_buf.len)) {
            const output = try self.allocator.alloc(f32, out_len);
            var a_gpu = GpuInputTensor.fromBuf(a_buf);
            defer a_gpu.deinit();
            var b_gpu = GpuInputTensor.fromBuf(b_buf);
            defer b_gpu.deinit();
            var out_gpu = gpuBinaryBroadcast(a_gpu.id, b_gpu.id, a_buf.len, b_buf.len, output, .lt);
            const out_buf = WasmBuf.fromSliceWithGpu(self.allocator, output, true, out_gpu.detach(), true);
            return fromBuf(if (a_buf.len >= b_buf.len) try copyBufShape(out_buf, a_buf) else try copyBufShape(out_buf, b_buf));
        }
        const output = try primBinaryBroadcast(self.allocator, a_buf.data, b_buf.data, .lt);
        const out_buf = WasmBuf.fromSlice(self.allocator, output, true);
        return fromBuf(if (a_buf.len >= b_buf.len) try copyBufShape(out_buf, a_buf) else try copyBufShape(out_buf, b_buf));
    }

    fn primWhereSelectOp(ctx: *anyopaque, cond: CT, on_true: CT, on_false: CT) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const c_buf = toBuf(cond);
        const t_buf = toBuf(on_true);
        const f_buf = toBuf(on_false);
        const c_data = c_buf.data;
        const t_data = t_buf.data;
        const f_data = f_buf.data;

        if (shouldPreferGpuElementwise(self, c_buf, null, c_buf.len) and isWhereSelectGpuCompatible(c_buf.len, t_buf.len, f_buf.len)) {
            const output = try self.allocator.alloc(f32, c_buf.len);
            var c_gpu = GpuInputTensor.fromBuf(c_buf);
            defer c_gpu.deinit();
            var t_gpu = GpuInputTensor.fromBuf(t_buf);
            defer t_gpu.deinit();
            var f_gpu = GpuInputTensor.fromBuf(f_buf);
            defer f_gpu.deinit();
            var out_gpu = gpuWhereSelect(c_gpu.id, t_gpu.id, f_gpu.id, t_buf.len, f_buf.len, output);
            return fromBuf(try copyBufShape(WasmBuf.fromSliceWithGpu(self.allocator, output, true, out_gpu.detach(), true), c_buf));
        }

        if (t_data.len != c_data.len or f_data.len != c_data.len) {
            // Handle scalar broadcast.
            const out_len = c_data.len;
            const output = try self.allocator.alloc(f32, out_len);
            for (0..out_len) |i| {
                const c = c_data[i];
                const t = if (t_data.len == 1) t_data[0] else if (i < t_data.len) t_data[i] else return error.ShapeMismatch;
                const f = if (f_data.len == 1) f_data[0] else if (i < f_data.len) f_data[i] else return error.ShapeMismatch;
                output[i] = if (c != 0.0) t else f;
            }
            return fromBuf(try copyBufShape(WasmBuf.fromSlice(self.allocator, output, true), c_buf));
        }

        const output = try self.allocator.alloc(f32, c_data.len);
        for (c_data, t_data, f_data, 0..) |c, t, f, i| {
            output[i] = if (c != 0.0) t else f;
        }
        return fromBuf(try copyBufShape(WasmBuf.fromSlice(self.allocator, output, true), c_buf));
    }

    /// Compute strides for a given shape (row-major).
    fn computeStrides(shape: []const i64, out: []usize) void {
        if (shape.len == 0) return;
        out[shape.len - 1] = 1;
        var j: usize = shape.len - 1;
        while (j > 0) {
            j -= 1;
            out[j] = out[j + 1] * @as(usize, @intCast(shape[j + 1]));
        }
    }

    /// Compute total number of elements from shape.
    fn shapeNumel(shape: []const i64) usize {
        var n: usize = 1;
        for (shape) |d| n *= @as(usize, @intCast(d));
        return n;
    }

    fn primReduceOp(self: *WasmCompute, input: CT, axes: []const u8, input_shape: []const i64, mode: enum { sum, max, mean }) anyerror!CT {
        const in_buf = toBuf(input);
        const in_data = in_buf.data;
        const rank = input_shape.len;

        // Compute output shape (remove reduced axes).
        var out_shape_buf: [8]i64 = undefined;
        var out_rank: usize = 0;
        var is_reduced = @as([8]bool, @splat(false));
        for (axes) |ax| is_reduced[ax] = true;
        for (0..rank) |d| {
            if (!is_reduced[d]) {
                out_shape_buf[out_rank] = input_shape[d];
                out_rank += 1;
            }
        }
        const out_numel = if (out_rank == 0) @as(usize, 1) else shapeNumel(out_shape_buf[0..out_rank]);

        const output = try self.allocator.alloc(f32, out_numel);
        const in_numel = shapeNumel(input_shape);
        if (shouldPreferGpuElementwise(self, in_buf, null, in_numel) and isGpuReduceCompatible(axes, input_shape)) {
            var inp_gpu = GpuInputTensor.fromBuf(in_buf);
            defer inp_gpu.deinit();
            var out_gpu = switch (mode) {
                .sum => gpuReduce(inp_gpu.id, output, input_shape, axes, out_shape_buf[0..out_rank], .sum),
                .max => gpuReduce(inp_gpu.id, output, input_shape, axes, out_shape_buf[0..out_rank], .max),
                .mean => gpuReduce(inp_gpu.id, output, input_shape, axes, out_shape_buf[0..out_rank], .mean),
            };
            return fromBuf(WasmBuf.fromSliceWithGpu(self.allocator, output, true, out_gpu.detach(), true));
        }

        switch (mode) {
            .sum, .mean => @memset(output, 0.0),
            .max => @memset(output, -std.math.inf(f32)),
        }

        // Compute input strides.
        var in_strides: [8]usize = undefined;
        computeStrides(input_shape, in_strides[0..rank]);

        // For each input element, compute its output index by dropping reduced dims.
        for (0..in_numel) |flat_in| {
            var remaining = flat_in;

            var out_flat: usize = 0;
            var cur_out_stride: usize = if (out_rank > 0) blk2: {
                var s: usize = 1;
                for (1..out_rank) |d| s *= @as(usize, @intCast(out_shape_buf[d]));
                break :blk2 s;
            } else 0;
            var out_dim_idx: usize = 0;
            for (0..rank) |d| {
                const coord = remaining / in_strides[d];
                remaining %= in_strides[d];
                if (!is_reduced[d]) {
                    out_flat += coord * cur_out_stride;
                    out_dim_idx += 1;
                    if (out_dim_idx < out_rank) {
                        cur_out_stride /= @as(usize, @intCast(out_shape_buf[out_dim_idx]));
                    }
                }
            }

            switch (mode) {
                .sum, .mean => output[out_flat] += in_data[flat_in],
                .max => output[out_flat] = @max(output[out_flat], in_data[flat_in]),
            }
        }

        if (mode == .mean) {
            var reduce_count: usize = 1;
            for (axes) |ax| reduce_count *= @as(usize, @intCast(input_shape[ax]));
            const scale: f32 = 1.0 / @as(f32, @floatFromInt(reduce_count));
            for (output) |*v| v.* *= scale;
        }

        return fromBuf(WasmBuf.fromSlice(self.allocator, output, true));
    }

    fn primReduceSumOp(ctx: *anyopaque, input: CT, axes: []const u8, input_shape: []const i64) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        return self.primReduceOp(input, axes, input_shape, .sum);
    }

    fn primReduceMaxOp(ctx: *anyopaque, input: CT, axes: []const u8, input_shape: []const i64) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        return self.primReduceOp(input, axes, input_shape, .max);
    }

    fn primReduceMeanOp(ctx: *anyopaque, input: CT, axes: []const u8, input_shape: []const i64) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        return self.primReduceOp(input, axes, input_shape, .mean);
    }

    fn reshape2dOp(ctx: *anyopaque, input: CT, rows: usize, cols: usize) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const buf = toBuf(input);
        if (rows * cols != buf.len) return error.ShapeMismatch;
        const result = try cloneDenseBufPreservingGpu(self, buf);
        try toBuf(result).setShape2D(rows, cols);
        return result;
    }

    fn splitLastDim3Op(ctx: *anyopaque, input: CT, rows: usize, dim: usize) anyerror!ops.SplitLastDim3Result {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const buf = toBuf(input);
        if (buf.len != rows * dim * 3) return error.UnexpectedOutputShape;

        const total = rows * dim;
        const first = try self.allocator.alloc(f32, total);
        errdefer self.allocator.free(first);
        const second = try self.allocator.alloc(f32, total);
        errdefer self.allocator.free(second);
        const third = try self.allocator.alloc(f32, total);
        errdefer self.allocator.free(third);

        for (0..rows) |row| {
            const src = row * dim * 3;
            const dst = row * dim;
            @memcpy(first[dst..][0..dim], buf.data[src..][0..dim]);
            @memcpy(second[dst..][0..dim], buf.data[src + dim ..][0..dim]);
            @memcpy(third[dst..][0..dim], buf.data[src + dim * 2 ..][0..dim]);
        }

        const gpu_enabled = build_options.enable_webgpu and self.use_gpu and buf.gpu_tensor != null;
        var first_gpu: ?wasm_extern.GpuBufferId = null;
        var second_gpu: ?wasm_extern.GpuBufferId = null;
        var third_gpu: ?wasm_extern.GpuBufferId = null;
        var gpu_owned = false;
        errdefer if (gpu_owned) {
            if (first_gpu) |id| wasm_extern.freeBuffer(id);
            if (second_gpu) |id| wasm_extern.freeBuffer(id);
            if (third_gpu) |id| wasm_extern.freeBuffer(id);
        };

        if (gpu_enabled) {
            const src_gpu = buf.gpu_tensor.?;
            var first_tensor = GpuTensor.create(total * @sizeOf(f32));
            var second_tensor = GpuTensor.create(total * @sizeOf(f32));
            var third_tensor = GpuTensor.create(total * @sizeOf(f32));
            gpu_owned = true;
            first_gpu = first_tensor.detach();
            second_gpu = second_tensor.detach();
            third_gpu = third_tensor.detach();

            for (0..rows) |row| {
                const src_base: usize = row * dim * 3;
                const dst_base: usize = row * dim;
                const copy_bytes: u32 = @intCast(dim * @sizeOf(f32));
                wasm_extern.copyBufferToBuffer(src_gpu, @intCast(src_base * @sizeOf(f32)), first_gpu.?, @intCast(dst_base * @sizeOf(f32)), copy_bytes);
                wasm_extern.copyBufferToBuffer(src_gpu, @intCast((src_base + dim) * @sizeOf(f32)), second_gpu.?, @intCast(dst_base * @sizeOf(f32)), copy_bytes);
                wasm_extern.copyBufferToBuffer(src_gpu, @intCast((src_base + dim * 2) * @sizeOf(f32)), third_gpu.?, @intCast(dst_base * @sizeOf(f32)), copy_bytes);
            }
        }

        const first_buf = WasmBuf.fromSliceWithGpu(self.allocator, first, true, first_gpu, gpu_owned);
        const second_buf = WasmBuf.fromSliceWithGpu(self.allocator, second, true, second_gpu, gpu_owned);
        const third_buf = WasmBuf.fromSliceWithGpu(self.allocator, third, true, third_gpu, gpu_owned);
        try first_buf.setShape2D(rows, dim);
        try second_buf.setShape2D(rows, dim);
        try third_buf.setShape2D(rows, dim);

        return .{
            .first = fromBuf(first_buf),
            .second = fromBuf(second_buf),
            .third = fromBuf(third_buf),
        };
    }

    fn reshape2DOp(ctx: *anyopaque, input: CT, old_rows: usize, old_cols: usize, new_rows: usize, new_cols: usize) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const buf = toBuf(input);
        if (old_rows * old_cols != buf.len) return error.UnexpectedOutputShape;
        if (new_rows * new_cols != buf.len) return error.UnexpectedOutputShape;
        const result = try cloneDenseBufPreservingGpu(self, buf);
        try toBuf(result).setShape2D(new_rows, new_cols);
        return result;
    }

    fn concatRows2DOp(ctx: *anyopaque, a: CT, b: CT, rows_a: usize, rows_b: usize, cols: usize) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const a_buf = toBuf(a);
        const b_buf = toBuf(b);
        const a_len = rows_a * cols;
        const b_len = rows_b * cols;
        if (a_buf.len != a_len or b_buf.len != b_len) return error.UnexpectedOutputShape;

        const output = try self.allocator.alloc(f32, a_len + b_len);
        @memcpy(output[0..a_len], a_buf.data[0..a_len]);
        @memcpy(output[a_len..][0..b_len], b_buf.data[0..b_len]);

        if (build_options.enable_webgpu and self.use_gpu) {
            if (a_buf.gpu_tensor) |a_gpu| {
                if (b_buf.gpu_tensor) |b_gpu| {
                    var out_gpu = GpuTensor.create((a_len + b_len) * @sizeOf(f32));
                    wasm_extern.copyBufferToBuffer(a_gpu, 0, out_gpu.id, 0, @intCast(a_len * @sizeOf(f32)));
                    wasm_extern.copyBufferToBuffer(b_gpu, 0, out_gpu.id, @intCast(a_len * @sizeOf(f32)), @intCast(b_len * @sizeOf(f32)));
                    const out_buf = WasmBuf.fromSliceWithGpu(self.allocator, output, true, out_gpu.detach(), true);
                    try out_buf.setShape2D(rows_a + rows_b, cols);
                    return fromBuf(out_buf);
                }
            }
        }

        const out_buf = WasmBuf.fromSlice(self.allocator, output, true);
        try out_buf.setShape2D(rows_a + rows_b, cols);
        return fromBuf(out_buf);
    }

    fn sliceRows2DOp(ctx: *anyopaque, input: CT, start_row: usize, row_count: usize, cols: usize) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const buf = toBuf(input);
        const total_rows = @divExact(buf.len, cols);
        if (total_rows * cols != buf.len) return error.UnexpectedOutputShape;
        if (start_row + row_count > total_rows) return error.UnexpectedOutputShape;
        const result = try dupSliceWithGpu(self, buf, start_row * cols, row_count * cols);
        try toBuf(result).setShape2D(row_count, cols);
        return result;
    }

    fn sliceLastDimOp(ctx: *anyopaque, input: CT, start: usize, stop: usize) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const buf = toBuf(input);
        const shape = buf.shape orelse return error.UnsupportedShape;
        if (shape.len != 2) return error.UnsupportedShape;
        const rows: usize = @intCast(shape[0]);
        const cols: usize = @intCast(shape[1]);
        if (rows * cols != buf.len) return error.UnexpectedOutputShape;
        if (start > stop or stop > cols) return error.UnexpectedOutputShape;

        const out_cols = stop - start;
        const output = try self.allocator.alloc(f32, rows * out_cols);
        for (0..rows) |row| {
            const src_base = row * cols + start;
            const dst_base = row * out_cols;
            @memcpy(output[dst_base..][0..out_cols], buf.data[src_base..][0..out_cols]);
        }

        if (build_options.enable_webgpu and self.use_gpu) {
            if (buf.gpu_tensor) |src_gpu| {
                var out_gpu = GpuTensor.create(rows * out_cols * @sizeOf(f32));
                for (0..rows) |row| {
                    const src_base = row * cols + start;
                    const dst_base = row * out_cols;
                    wasm_extern.copyBufferToBuffer(
                        src_gpu,
                        @intCast(src_base * @sizeOf(f32)),
                        out_gpu.id,
                        @intCast(dst_base * @sizeOf(f32)),
                        @intCast(out_cols * @sizeOf(f32)),
                    );
                }
                const out_buf = WasmBuf.fromSliceWithGpu(self.allocator, output, true, out_gpu.detach(), true);
                try out_buf.setShape2D(rows, out_cols);
                return fromBuf(out_buf);
            }
        }

        const out_buf = WasmBuf.fromSlice(self.allocator, output, true);
        try out_buf.setShape2D(rows, out_cols);
        return fromBuf(out_buf);
    }

    fn primReshapeOp(ctx: *anyopaque, input: CT, target_shape: []const i64) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const buf = toBuf(input);
        if (shapeNumel(target_shape) != buf.len) return error.ShapeMismatch;
        const result = try cloneDenseBufPreservingGpu(self, buf);
        const shape = try self.allocator.dupe(i64, target_shape);
        toBuf(result).setShapeOwned(shape);
        return result;
    }

    fn primTransposeOp(ctx: *anyopaque, input: CT, perm: []const u8, input_shape: []const i64) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const in_data = toBuf(input).data;
        const rank = input_shape.len;
        const numel = shapeNumel(input_shape);

        var in_strides: [8]usize = undefined;
        computeStrides(input_shape, in_strides[0..rank]);

        // Compute output shape and strides.
        var out_shape: [8]i64 = undefined;
        for (0..rank) |d| out_shape[d] = input_shape[perm[d]];
        var out_strides: [8]usize = undefined;
        computeStrides(out_shape[0..rank], out_strides[0..rank]);

        const output = try self.allocator.alloc(f32, numel);
        for (0..numel) |flat_out| {
            var remaining = flat_out;
            var flat_in: usize = 0;
            for (0..rank) |d| {
                const coord = remaining / out_strides[d];
                remaining %= out_strides[d];
                flat_in += coord * in_strides[perm[d]];
            }
            output[flat_out] = in_data[flat_in];
        }
        return fromBuf(WasmBuf.fromSlice(self.allocator, output, true));
    }

    fn primBroadcastInDimOp(ctx: *anyopaque, input: CT, target_shape: []const i64, broadcast_axes: []const u8, input_shape: []const i64) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const in_buf = toBuf(input);
        const in_data = in_buf.data;
        const out_rank = target_shape.len;
        const in_rank = input_shape.len;
        const out_numel = shapeNumel(target_shape);
        if (shouldPreferGpuElementwise(self, in_buf, null, out_numel) and isGpuBroadcastInDimCompatible(target_shape, broadcast_axes, input_shape)) {
            const output = try self.allocator.alloc(f32, out_numel);
            var inp_gpu = GpuInputTensor.fromBuf(in_buf);
            defer inp_gpu.deinit();
            var out_gpu = gpuBroadcastInDim(inp_gpu.id, output, target_shape, broadcast_axes, input_shape);
            const out_buf = WasmBuf.fromSliceWithGpu(self.allocator, output, true, out_gpu.detach(), true);
            if (target_shape.len > 0) {
                const shape = try self.allocator.dupe(i64, target_shape);
                out_buf.setShapeOwned(shape);
            }
            return fromBuf(out_buf);
        }

        var out_strides: [8]usize = undefined;
        computeStrides(target_shape, out_strides[0..out_rank]);

        // Build input strides in the output coordinate system.
        var in_strides: [8]usize = undefined;
        computeStrides(input_shape, in_strides[0..in_rank]);

        const output = try self.allocator.alloc(f32, out_numel);
        for (0..out_numel) |flat_out| {
            var remaining = flat_out;
            var flat_in: usize = 0;
            for (0..out_rank) |d| {
                const coord = remaining / out_strides[d];
                remaining %= out_strides[d];
                // Check if this output dim corresponds to an input dim.
                for (broadcast_axes[0..in_rank], 0..) |ax, in_d| {
                    if (ax == d) {
                        // If the input dim is 1, broadcast (coord stays 0).
                        if (input_shape[in_d] > 1) {
                            flat_in += coord * in_strides[in_d];
                        }
                        break;
                    }
                }
            }
            output[flat_out] = in_data[flat_in];
        }
        const out_buf = WasmBuf.fromSlice(self.allocator, output, true);
        if (target_shape.len > 0) {
            const shape = try self.allocator.dupe(i64, target_shape);
            out_buf.setShapeOwned(shape);
        }
        return fromBuf(out_buf);
    }

    fn primDotGeneralOp(ctx: *anyopaque, lhs: CT, rhs: CT, lhs_shape: []const i64, rhs_shape: []const i64, lhs_contracting: []const u8, rhs_contracting: []const u8, lhs_batch: []const u8, rhs_batch: []const u8) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const lhs_data = toBuf(lhs).data;
        const rhs_data = toBuf(rhs).data;

        // Handle common case: 2D matmul (no batch dims, one contracting dim each).
        if (lhs_batch.len == 0 and lhs_contracting.len == 1 and rhs_contracting.len == 1) {
            const lhs_rank = lhs_shape.len;
            const rhs_rank = rhs_shape.len;

            if (lhs_rank == 2 and rhs_rank == 2) {
                const lc = lhs_contracting[0];
                const rc = rhs_contracting[0];
                const k = @as(usize, @intCast(lhs_shape[lc]));

                const m = @as(usize, @intCast(lhs_shape[1 - lc]));
                const n = @as(usize, @intCast(rhs_shape[1 - rc]));

                const output = try self.allocator.alloc(f32, m * n);
                @memset(output, 0.0);

                for (0..m) |i| {
                    for (0..n) |j| {
                        var acc: f64 = 0.0;
                        for (0..k) |ki| {
                            const a_idx = if (lc == 1) i * k + ki else ki * m + i;
                            const b_idx = if (rc == 0) ki * n + j else j * k + ki;
                            acc += @as(f64, lhs_data[a_idx]) * @as(f64, rhs_data[b_idx]);
                        }
                        output[i * n + j] = @floatCast(acc);
                    }
                }
                return fromBuf(WasmBuf.fromSlice(self.allocator, output, true));
            }
        }

        // Batched case: lhs_batch.len >= 1, 1 contracting dim.
        if (lhs_batch.len >= 1 and lhs_contracting.len == 1 and rhs_contracting.len == 1) {
            var batch_size: usize = 1;
            for (lhs_batch) |bd| batch_size *= @as(usize, @intCast(lhs_shape[bd]));

            const lc = lhs_contracting[0];
            const rc = rhs_contracting[0];
            const k = @as(usize, @intCast(lhs_shape[lc]));

            // Find the non-batch, non-contracting dims.
            var lhs_free: usize = 0;
            for (0..lhs_shape.len) |d| {
                var is_batch = false;
                for (lhs_batch) |bd| {
                    if (bd == d) {
                        is_batch = true;
                        break;
                    }
                }
                if (!is_batch and d != lc) {
                    lhs_free = @as(usize, @intCast(lhs_shape[d]));
                    break;
                }
            }
            var rhs_free: usize = 0;
            for (0..rhs_shape.len) |d| {
                var is_batch = false;
                for (rhs_batch) |bd| {
                    if (bd == d) {
                        is_batch = true;
                        break;
                    }
                }
                if (!is_batch and d != rc) {
                    rhs_free = @as(usize, @intCast(rhs_shape[d]));
                    break;
                }
            }

            const m = lhs_free;
            const n = rhs_free;
            const output = try self.allocator.alloc(f32, batch_size * m * n);
            @memset(output, 0.0);

            for (0..batch_size) |b_idx| {
                const lhs_off = b_idx * m * k;
                const rhs_off = b_idx * k * n;
                const out_off = b_idx * m * n;
                linalg.sgemmSync(m, n, k, 1.0, lhs_data[lhs_off..][0 .. m * k], rhs_data[rhs_off..][0 .. k * n], 0.0, output[out_off..][0 .. m * n]);
            }
            return fromBuf(WasmBuf.fromSlice(self.allocator, output, true));
        }

        return error.UnsupportedPrimitiveOp;
    }

    fn primScatterAddOp(ctx: *anyopaque, input: CT, indices: CT, input_shape: []const i64, indices_shape: []const i64, axis: u8) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const in_data = toBuf(input).data;
        const idx_data = toBuf(indices).data;
        _ = axis; // Currently only axis=0 supported.

        if (input_shape.len == 2) {
            const n = @as(usize, @intCast(input_shape[0]));
            const d = @as(usize, @intCast(input_shape[1]));
            const out_rows: usize = if (indices_shape.len > 0)
                @as(usize, @intCast(indices_shape[0]))
            else blk: {
                var max_idx: usize = 0;
                for (idx_data) |v| {
                    const idx = @as(usize, @intFromFloat(v));
                    if (idx > max_idx) max_idx = idx;
                }
                break :blk max_idx + 1;
            };
            const output = try self.allocator.alloc(f32, out_rows * d);
            @memset(output, 0.0);
            for (0..n) |i| {
                const row = @as(usize, @intFromFloat(idx_data[i]));
                if (row >= out_rows) return error.IndexOutOfBounds;
                for (0..d) |j| {
                    output[row * d + j] += in_data[i * d + j];
                }
            }
            return fromBuf(WasmBuf.fromSlice(self.allocator, output, true));
        }

        return error.UnsupportedPrimitiveOp;
    }

    fn primGatherOp(ctx: *anyopaque, input: CT, indices: CT, axis: u8, input_shape: []const i64) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const in_data = toBuf(input).data;
        const idx_data = toBuf(indices).data;
        _ = axis; // Currently axis=0 only.

        if (input_shape.len == 2) {
            const d = @as(usize, @intCast(input_shape[1]));
            const n = idx_data.len;
            const output = try self.allocator.alloc(f32, n * d);
            for (0..n) |i| {
                const row = @as(usize, @intFromFloat(idx_data[i]));
                @memcpy(output[i * d ..][0..d], in_data[row * d ..][0..d]);
            }
            return fromBuf(WasmBuf.fromSlice(self.allocator, output, true));
        }

        return error.UnsupportedPrimitiveOp;
    }

    fn primSliceOp(ctx: *anyopaque, input: CT, starts: []const i64, limits: []const i64, strides_param: []const i64, input_shape: []const i64) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const in_data = toBuf(input).data;
        const rank = input_shape.len;

        // Compute output shape.
        var out_shape: [8]i64 = undefined;
        var out_numel: usize = 1;
        for (0..rank) |d| {
            const size = @divTrunc(limits[d] - starts[d] + strides_param[d] - 1, strides_param[d]);
            out_shape[d] = size;
            out_numel *= @as(usize, @intCast(size));
        }

        var in_strides: [8]usize = undefined;
        computeStrides(input_shape, in_strides[0..rank]);
        var out_strides: [8]usize = undefined;
        computeStrides(out_shape[0..rank], out_strides[0..rank]);

        const output = try self.allocator.alloc(f32, out_numel);
        for (0..out_numel) |flat_out| {
            var remaining = flat_out;
            var flat_in: usize = 0;
            for (0..rank) |d| {
                const out_coord = remaining / out_strides[d];
                remaining %= out_strides[d];
                const in_coord = @as(usize, @intCast(starts[d])) + out_coord * @as(usize, @intCast(strides_param[d]));
                flat_in += in_coord * in_strides[d];
            }
            output[flat_out] = in_data[flat_in];
        }
        return fromBuf(WasmBuf.fromSlice(self.allocator, output, true));
    }

    fn primConcatPrimOp(ctx: *anyopaque, a: CT, b: CT, axis: u8, a_shape: []const i64, b_shape: []const i64) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const a_data = toBuf(a).data;
        const b_data = toBuf(b).data;
        const rank = a_shape.len;

        // Compute output shape.
        var out_shape: [8]i64 = undefined;
        for (0..rank) |d| {
            out_shape[d] = if (d == axis) a_shape[d] + b_shape[d] else a_shape[d];
        }
        const out_numel = shapeNumel(out_shape[0..rank]);

        var outer: usize = 1;
        for (0..axis) |d| outer *= @as(usize, @intCast(a_shape[d]));
        var inner: usize = 1;
        for (axis + 1..rank) |d| inner *= @as(usize, @intCast(a_shape[d]));
        const a_axis = @as(usize, @intCast(a_shape[axis]));
        const b_axis = @as(usize, @intCast(b_shape[axis]));

        const output = try self.allocator.alloc(f32, out_numel);
        for (0..outer) |o| {
            const a_chunk = a_axis * inner;
            const b_chunk = b_axis * inner;
            const out_chunk = (a_axis + b_axis) * inner;
            @memcpy(output[o * out_chunk ..][0..a_chunk], a_data[o * a_chunk ..][0..a_chunk]);
            @memcpy(output[o * out_chunk + a_chunk ..][0..b_chunk], b_data[o * b_chunk ..][0..b_chunk]);
        }
        return fromBuf(WasmBuf.fromSlice(self.allocator, output, true));
    }

    fn primSoftmaxOp(ctx: *anyopaque, input: CT, dim: u32) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const in_buf = toBuf(input);
        const data = in_buf.data;
        const d = resolveSoftmaxLastDim(in_buf, dim) orelse return error.InvalidTensorShape;
        if (d == 0 or data.len % d != 0) return error.InvalidTensorShape;
        const rows = data.len / d;
        const output = try self.allocator.alloc(f32, data.len);

        if (shouldPreferGpuElementwise(self, in_buf, null, data.len)) {
            var inp_gpu = GpuInputTensor.fromBuf(in_buf);
            defer inp_gpu.deinit();
            var out_gpu = gpuSoftmax(inp_gpu.id, rows, d, output, false);
            return fromBuf(try copyBufShape(WasmBuf.fromSliceWithGpu(self.allocator, output, true, out_gpu.detach(), true), in_buf));
        }

        for (0..rows) |r| {
            const row = data[r * d ..][0..d];
            const out = output[r * d ..][0..d];

            var max_val: f32 = -std.math.inf(f32);
            for (row) |v| max_val = @max(max_val, v);

            var sum: f32 = 0;
            for (out, row) |*o, v| {
                o.* = @exp(v - max_val);
                sum += o.*;
            }

            const inv_sum = 1.0 / sum;
            for (out) |*o| o.* *= inv_sum;
        }
        const out_buf = WasmBuf.fromSlice(self.allocator, output, true);
        if (try in_buf.cloneShape()) |shape| out_buf.setShapeOwned(shape);
        return fromBuf(out_buf);
    }

    fn primLogSoftmaxOp(ctx: *anyopaque, input: CT, dim: u32) anyerror!CT {
        const self: *WasmCompute = @ptrCast(@alignCast(ctx));
        const in_buf = toBuf(input);
        const data = in_buf.data;
        const d = resolveSoftmaxLastDim(in_buf, dim) orelse return error.InvalidTensorShape;
        if (d == 0 or data.len % d != 0) return error.InvalidTensorShape;
        const rows = data.len / d;
        const output = try self.allocator.alloc(f32, data.len);

        if (shouldPreferGpuElementwise(self, in_buf, null, data.len)) {
            var inp_gpu = GpuInputTensor.fromBuf(in_buf);
            defer inp_gpu.deinit();
            var out_gpu = gpuSoftmax(inp_gpu.id, rows, d, output, true);
            return fromBuf(try copyBufShape(WasmBuf.fromSliceWithGpu(self.allocator, output, true, out_gpu.detach(), true), in_buf));
        }

        for (0..rows) |r| {
            const row = data[r * d ..][0..d];
            const out = output[r * d ..][0..d];

            var max_val: f32 = -std.math.inf(f32);
            for (row) |v| max_val = @max(max_val, v);

            var sum_exp: f32 = 0;
            for (row) |v| sum_exp += @exp(v - max_val);
            const lse = @log(sum_exp);

            for (out, row) |*o, v| {
                o.* = (v - max_val) - lse;
            }
        }
        const out_buf = WasmBuf.fromSlice(self.allocator, output, true);
        if (try in_buf.cloneShape()) |shape| out_buf.setShapeOwned(shape);
        return fromBuf(out_buf);
    }

    const vtable = ComputeBackend.VTable{
        .backendKind = backendKindOp,
        .deinitBackend = deinitBackendOp,
        .freeTensor = freeTensorOp,
        .reserveGraphPlanSlots = reserveGraphPlanSlotsOp,
        .getWeight = getWeightOp,
        .prefetchWeightHint = noopPrefetch,
        .drainPrefetchBudget = noopDrain,
        .embeddingLookup = embeddingLookupOp,
        .embeddingLookupTensor = embeddingLookupTensorOp,
        .takeRows = takeRowsOp,
        .splitLastDim3 = splitLastDim3Op,
        .reshape2D = reshape2DOp,
        .concatRows2D = concatRows2DOp,
        .sliceRows2D = sliceRows2DOp,
        .linear = linearOp,
        .linearNoBias = linearNoBiasOp,
        .linearNoBiasPlanned = linearNoBiasPlannedOp,
        .layerNorm = layerNormOp,
        .rmsNorm = rmsNormOp,
        .gelu = geluOp,
        .geluNew = geluNewOp,
        .relu = reluOp,
        .silu = siluOp,
        .quickGelu = quickGeluOp,
        .sigmoid = sigmoidOp,
        .tanh_act = tanhOp,
        .concat = concatOp,
        .add = addOp,
        .multiply = multiplyOp,
        .scaledDotProductAttention = scaledDotProductAttentionOp,
        .causalSelfAttention = causalSelfAttentionOp,
        .crossAttention = crossAttentionOp,
        .relativePositionBias = relativePositionBiasOp,
        .disentangledRelativeAttention = disentangledRelativeAttentionOp,
        .windowedSelfAttention = windowedSelfAttentionOp,
        .channelSelfAttention = channelSelfAttentionOp,
        .tokenGridConv2d = tokenGridConv2dOp,
        .conv1d = conv1dOp,
        .conv2d = conv2dOp,
        .rope = ropeOp,
        .ropePerItem = ropePerItemOp,
        .reshape2d = reshape2dOp,
        .gqaCausalAttention = gqaCausalAttentionOp,
        .gqaPagedAttention = gqaPagedAttentionOp,
        .fromFloat32 = fromFloat32Op,
        .fromFloat32Shape = fromFloat32ShapeOp,
        .fromInt32Shape = fromInt32ShapeOp,
        .toFloat32 = toFloat32Op,
        .exportTensorData = exportTensorDataOp,
        .tensorDType = tensorDTypeOp,
        .tensorShape = tensorShapeOp,
        .evalTensor = evalTensorOp,
        .zeroTensor = zeroTensorOp,
        .argmaxLastRow = argmaxLastRowOp,
        .sampleLastRow = sampleLastRowOp,
        .linearNoBiasArgmaxLastRow = linearNoBiasArgmaxLastRowOp,
        .linearNoBiasArgmaxLastRowTensor = linearNoBiasArgmaxLastRowTensorOp,
        .decoderRuntimePrepareGreedy = decoderRuntimePrepareGreedyOp,
        .decoderRuntimeResetState = decoderRuntimeResetStateOp,
        .decoderRuntimePrepareAbsoluteEmbeddings = decoderRuntimePrepareAbsoluteEmbeddingsOp,
        .decoderRuntimeEmbedAbsolutePosition = decoderRuntimeEmbedAbsolutePositionOp,
        .decoderRuntimePrepareLayerNorm = decoderRuntimePrepareLayerNormOp,
        .decoderRuntimeApplyLayerNorm = decoderRuntimeApplyLayerNormOp,
        .decoderRuntimePrepareRmsNorm = decoderRuntimePrepareRmsNormOp,
        .decoderRuntimeApplyRmsNorm = decoderRuntimeApplyRmsNormOp,
        .decoderRuntimeApplyLayerNormLinear = decoderRuntimeApplyLayerNormLinearOp,
        .decoderRuntimeApplyLayerNormLinearArgmax = decoderRuntimeApplyLayerNormLinearArgmaxOp,
        .decoderRuntimeApplyLayerNormLinearSample = decoderRuntimeApplyLayerNormLinearSampleOp,
        .decoderRuntimeApplyRmsNormLinear = decoderRuntimeApplyRmsNormLinearOp,
        .decoderRuntimeApplyRmsNormLinearArgmax = decoderRuntimeApplyRmsNormLinearArgmaxOp,
        .decoderRuntimeApplyRmsNormLinearSample = decoderRuntimeApplyRmsNormLinearSampleOp,
        .decoderRuntimePrepareLinear = decoderRuntimePrepareLinearOp,
        .decoderRuntimeApplyLinear = decoderRuntimeApplyLinearOp,
        .decoderRuntimeApplyLinearArgmax = decoderRuntimeApplyLinearArgmaxOp,
        .decoderRuntimeApplyLinearPair = decoderRuntimeApplyLinearPairOp,
        .decoderRuntimeApplyLinearQkv = decoderRuntimeApplyLinearQkvOp,
        .decoderRuntimeApplyActivation = decoderRuntimeApplyActivationOp,
        .decoderRuntimeApplyAdd = decoderRuntimeApplyAddOp,
        .runDenseFfnResidual = runDenseFfnResidualOp,
        .runGatedFfnResidual = runGatedFfnResidualOp,
        .runAttention = runAttentionOp,
        .runAttentionResidual = runAttentionResidualOp,
        .runDenseDecoderBlock = runDenseDecoderBlockOp,
        .runGatedDecoderBlock = runGatedDecoderBlockOp,
        // Primitive ops for training
        .subtract = primSubtractOp,
        .divide = primDivideOp,
        .negate = primNegateOp,
        .sqrtOp = primSqrtOp,
        .rsqrtOp = primRsqrtOp,
        .expOp = primExpOp,
        .logOp = primLogOp,
        .sinOp = primSinOp,
        .cosOp = primCosOp,
        .tanhOp = primTanhPrimOp,
        .erfOp = primErfOp,
        .absOp = primAbsOp,
        .lessThan = primLessThanOp,
        .whereSelect = primWhereSelectOp,
        .reduceSumOp = primReduceSumOp,
        .reduceMaxOp = primReduceMaxOp,
        .reduceMeanOp = primReduceMeanOp,
        .reshapeOp = primReshapeOp,
        .transposeOp = primTransposeOp,
        .broadcastInDimOp = primBroadcastInDimOp,
        .dotGeneralOp = primDotGeneralOp,
        .scatterAddOp = primScatterAddOp,
        .gatherOp = primGatherOp,
        .sliceOp = primSliceOp,
        .concatPrimOp = primConcatPrimOp,
        .softmaxOp = primSoftmaxOp,
        .logSoftmaxOp = primLogSoftmaxOp,
        .sliceLastDim = sliceLastDimOp,
    };
};

test {
    _ = @import("wasm_compute_test.zig");
    _ = @import("wasm_e2e_test.zig");
}

test "wasm compute graph plan reservation reports unavailable without webgpu" {
    const allocator = std.testing.allocator;
    var compute = WasmCompute.init(allocator);
    defer compute.deinitBackendOp(&compute);
    var cb = compute.computeBackend();

    const reserved = try cb.reserveGraphPlanSlots(&.{.{ .slot = 0, .bytes = 4096 }});
    if (!build_options.enable_webgpu) {
        try std.testing.expect(!reserved);
    }
}

test "wasm_compute: runAttention includes attention sink probability mass" {
    const allocator = std.testing.allocator;
    var compute = WasmCompute.init(allocator);
    var cb = compute.computeBackend();
    defer cb.deinit();

    const q_data = [_]f32{ 1.0, 1.0 };
    const k_data = [_]f32{ 0.0, 0.0 };
    const v_data = [_]f32{ 2.0, 4.0 };
    const sink_data = [_]f32{0.0};

    const q = try cb.fromFloat32(&q_data);
    defer cb.free(q);
    const k = try cb.fromFloat32(&k_data);
    defer cb.free(k);
    const v = try cb.fromFloat32(&v_data);
    defer cb.free(v);
    const sink = try cb.fromFloat32(&sink_data);
    defer cb.free(sink);

    const result = (try cb.runAttention(&.{
        .q = q,
        .k = k,
        .v = v,
        .attention = .{
            .total_sequence_len = 2,
            .query_sequence_len = 2,
            .kv_sequence_len = 2,
        },
        .attention_sink = .{ .per_head_tensor = sink },
        .num_heads = 1,
        .num_kv_heads = 1,
        .head_dim = 1,
    })) orelse return error.TestUnexpectedNull;
    defer cb.free(result);

    const out = try cb.toFloat32(result, allocator);
    defer allocator.free(out);
    try std.testing.expectEqual(@as(usize, 2), out.len);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), out[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 2.0), out[1], 1e-6);
}

test "wasm_compute: cached attention includes attention sink probability mass" {
    const allocator = std.testing.allocator;
    var compute = WasmCompute.init(allocator);
    defer compute.deinitBackendOp(&compute);

    var q_data = [_]f32{0.0};
    var k_data = [_]f32{ 0.0, 0.0, 0.0 };
    var v_data = [_]f32{ 2.0, 4.0, 8.0 };
    const sink_scores = [_]f32{0.0};

    const q = WasmBuf.fromSlice(allocator, q_data[0..], false);
    defer q.deinit();
    const k = WasmBuf.fromSlice(allocator, k_data[0..], false);
    defer k.deinit();
    const v = WasmBuf.fromSlice(allocator, v_data[0..], false);
    defer v.deinit();

    const result = try compute.gqaCachedAttention(q, k, v, null, sink_scores[0..], 1, 1, 3, 1, 1, 1);
    defer toBuf(result).deinit();

    const out = toBuf(result).data;
    try std.testing.expectEqual(@as(usize, 1), out.len);
    try std.testing.expectApproxEqAbs(@as(f32, 3.5), out[0], 1e-6);
}

test "wasm compute quant gpu storage length is padded to u32 words" {
    try std.testing.expectEqual(@as(usize, 0), try quantStorageByteLen(0));
    try std.testing.expectEqual(@as(usize, 4), try quantStorageByteLen(1));
    try std.testing.expectEqual(@as(usize, 4), try quantStorageByteLen(2));
    try std.testing.expectEqual(@as(usize, 4), try quantStorageByteLen(3));
    try std.testing.expectEqual(@as(usize, 4), try quantStorageByteLen(4));
    try std.testing.expectEqual(@as(usize, 8), try quantStorageByteLen(5));
    try std.testing.expectEqual(@as(usize, 1024), try quantStorageByteLen(1024));
    try std.testing.expectError(error.OutOfMemory, quantStorageByteLen(std.math.maxInt(usize) - 1));
}

test "wasm compute webgpu elementwise shape guards" {
    try std.testing.expect(isModuloBroadcastCompatible(8, 8));
    try std.testing.expect(isModuloBroadcastCompatible(8, 1));
    try std.testing.expect(isModuloBroadcastCompatible(8, 4));
    try std.testing.expect(!isModuloBroadcastCompatible(8, 3));
    try std.testing.expect(!isModuloBroadcastCompatible(8, 0));

    try std.testing.expect(isWhereSelectGpuCompatible(8, 8, 1));
    try std.testing.expect(isWhereSelectGpuCompatible(8, 1, 8));
    try std.testing.expect(!isWhereSelectGpuCompatible(8, 4, 8));
    try std.testing.expect(!isWhereSelectGpuCompatible(8, 8, 4));

    try std.testing.expect(isGpuReduceLastDimCompatible(&.{1}, &.{ 2, 4 }));
    try std.testing.expect(isGpuReduceLastDimCompatible(&.{ 0, 1 }, &.{ 2, 4 }));
    try std.testing.expect(!isGpuReduceLastDimCompatible(&.{0}, &.{ 2, 4 }));

    try std.testing.expect(isGpuBroadcastInDimCompatible(&.{ 2, 3, 4 }, &.{ 0, 1, 2 }, &.{ 1, 1, 4 }));
    try std.testing.expect(isGpuBroadcastInDimCompatible(&.{ 2, 3, 4 }, &.{2}, &.{4}));
    try std.testing.expect(!isGpuBroadcastInDimCompatible(&.{ 2, 3, 4 }, &.{ 0, 1, 2 }, &.{ 2, 4, 4 }));
}
