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

// MLX backend helpers for Apple Silicon.
//
// Wraps mlx-c (the official C bindings for MLX) with Zig-friendly helpers.
// Only compiled when -Dmlx=true is passed to the build.

const std = @import("std");
const tensor_mod = @import("tensor.zig");
const runtime = @import("../runtime/root.zig");

pub const c = @cImport({
    @cInclude("mlx/c/mlx.h");
});

extern fn termite_metal_device_available() c_int;
extern "c" fn free(ptr: ?*anyopaque) void;
fn noop_free(_: ?*anyopaque) callconv(.c) void {}

pub fn metalDeviceAvailable() bool {
    return termite_metal_device_available() != 0;
}

pub const StreamKind = enum {
    cpu,
    gpu,
};

pub const Stream = c.mlx_stream;
pub const DistributedGroup = c.mlx_distributed_group;

pub const DistributedContext = struct {
    group: DistributedGroup,
    rank: usize,
    world_size: usize,
    backend: []const u8,
    strict: bool,
};

pub const StreamHandle = struct {
    stream: Stream,

    pub fn deinit(self: StreamHandle) void {
        _ = c.mlx_stream_free(self.stream);
    }
};

pub const ShardRange = struct {
    start: usize,
    len: usize,

    pub fn end(self: @This()) usize {
        return self.start + self.len;
    }
};

pub const ShardedMatrix = struct {
    data: []f32,
    range: ShardRange,
};

pub fn allowCpuStreamWithoutMetal() bool {
    const libc = @cImport(@cInclude("stdlib.h"));
    const value = libc.getenv("TERMITE_MLX_ALLOW_CPU_STREAM_WITHOUT_METAL") orelse return false;
    const slice = std.mem.span(value);
    return std.mem.eql(u8, slice, "1") or
        std.ascii.eqlIgnoreCase(slice, "true") or
        std.ascii.eqlIgnoreCase(slice, "yes") or
        std.ascii.eqlIgnoreCase(slice, "on");
}

pub fn defaultStreamKind() StreamKind {
    if (distributedEnabled()) return .cpu;
    return if (metalDeviceAvailable()) .gpu else .cpu;
}

pub fn distributedConfig() runtime.distributed.Config {
    return runtime.distributed.configFromEnv();
}

pub fn distributedEnabled() bool {
    const cfg = distributedConfig();
    return cfg.enabled and cfg.world_size > 1 and cfg.mode != .none;
}

pub fn distributedAvailable(backend_name: ?[]const u8) bool {
    const name_z = if (backend_name) |name|
        std.heap.page_allocator.dupeZ(u8, name) catch return false
    else
        null;
    defer if (name_z) |buf| std.heap.page_allocator.free(buf);
    return c.mlx_distributed_is_available(if (name_z) |buf| buf.ptr else null);
}

pub fn openDefaultStream() StreamHandle {
    return .{ .stream = if (defaultStreamKind() == .gpu) c.mlx_default_gpu_stream_new() else c.mlx_default_cpu_stream_new() };
}

pub fn initDistributed(strict: bool, backend_name: ?[]const u8) !DistributedContext {
    if (!distributedEnabled()) return error.MlxDistributedDisabled;
    if (!distributedAvailable(backend_name)) return error.MlxDistributedUnavailable;

    const name_z = if (backend_name) |name| try std.heap.page_allocator.dupeZ(u8, name) else null;
    defer if (name_z) |buf| std.heap.page_allocator.free(buf);

    var group = c.mlx_distributed_group_new();
    errdefer _ = c.mlx_distributed_group_free(group);
    if (c.mlx_distributed_init(&group, strict, if (name_z) |buf| buf.ptr else null) != 0) {
        return error.MlxDistributedInitFailed;
    }
    if (group.ctx == null) {
        return error.MlxDistributedInitFailed;
    }
    const rank = c.mlx_distributed_group_rank(group);
    const world_size = c.mlx_distributed_group_size(group);
    if (rank < 0 or world_size <= 0) return error.MlxDistributedInitFailed;
    return .{
        .group = group,
        .rank = @intCast(rank),
        .world_size = @intCast(world_size),
        .backend = if (backend_name) |name| name else switch (distributedConfig().backend) {
            .ring => "ring",
            .mpi => "mpi",
        },
        .strict = strict,
    };
}

fn shardRange(total: usize, rank: usize, world_size: usize) ShardRange {
    const base = total / world_size;
    const rem = total % world_size;
    const extra = if (rank < rem) @as(usize, 1) else 0;
    const start = rank * base + @min(rank, rem);
    return .{ .start = start, .len = base + extra };
}

/// GPU stream for Metal acceleration.
pub fn gpuStream() c.mlx_stream {
    if (metalDeviceAvailable()) {
        return c.mlx_default_gpu_stream_new();
    }
    return c.mlx_default_cpu_stream_new();
}

/// Create an MLX array from a Zig f32 slice.
pub fn arrayFromFloat32(data: []const f32, shape: []const i32) c.mlx_array {
    const owned = std.heap.c_allocator.alloc(f32, data.len) catch unreachable;
    @memcpy(owned, data);
    return c.mlx_array_new_data_managed(
        @ptrCast(owned.ptr),
        shape.ptr,
        @intCast(shape.len),
        c.MLX_FLOAT32,
        &freeManagedCBuffer,
    );
}

pub fn arrayFromBorrowedFloat32(data: []const f32, shape: []const i32) c.mlx_array {
    return c.mlx_array_new_data_managed(
        @ptrCast(@constCast(data.ptr)),
        shape.ptr,
        @intCast(shape.len),
        c.MLX_FLOAT32,
        &noop_free,
    );
}

fn freeManagedCBuffer(ptr: ?*anyopaque) callconv(.c) void {
    free(ptr);
}

/// Create an MLX array that takes ownership of a c-allocated float32 buffer.
pub fn arrayFromOwnedFloat32(data: []f32, shape: []const i32) c.mlx_array {
    return c.mlx_array_new_data_managed(
        @ptrCast(data.ptr),
        shape.ptr,
        @intCast(shape.len),
        c.MLX_FLOAT32,
        &freeManagedCBuffer,
    );
}

pub fn arrayFromOwnedInt32(data: []i32, shape: []const i32) c.mlx_array {
    return c.mlx_array_new_data_managed(
        @ptrCast(data.ptr),
        shape.ptr,
        @intCast(shape.len),
        c.MLX_INT32,
        &freeManagedCBuffer,
    );
}

pub fn arrayFromBytes(data: []const u8, shape: []const i32, dtype: c.mlx_dtype) c.mlx_array {
    return c.mlx_array_new_data(
        @ptrCast(data.ptr),
        shape.ptr,
        @intCast(shape.len),
        dtype,
    );
}

pub fn arrayFromBorrowedBytes(data: []const u8, shape: []const i32, dtype: c.mlx_dtype) c.mlx_array {
    return c.mlx_array_new_data_managed(
        @ptrCast(@constCast(data.ptr)),
        shape.ptr,
        @intCast(shape.len),
        dtype,
        &noop_free,
    );
}

pub fn arrayFromTensor(allocator: std.mem.Allocator, tensor: *const tensor_mod.Tensor, force_f32: bool) !c.mlx_array {
    const shape = try allocator.alloc(i32, tensor.shape.len);
    defer allocator.free(shape);
    for (tensor.shape, 0..) |dim, i| shape[i] = @intCast(dim);
    const owned = try std.heap.c_allocator.alloc(u8, tensor.data.len);
    errdefer std.heap.c_allocator.free(owned);
    @memcpy(owned, tensor.data);
    var arr = c.mlx_array_new_data_managed(
        @ptrCast(owned.ptr),
        shape.ptr,
        @intCast(shape.len),
        mlxDType(tensor.dtype),
        &freeManagedCBuffer,
    );
    if (force_f32 and (tensor.dtype == .f16 or tensor.dtype == .bf16)) {
        var casted = c.mlx_array_new();
        try check(c.mlx_astype(&casted, arr, c.MLX_FLOAT32, gpuStream()));
        _ = c.mlx_array_free(arr);
        arr = casted;
    }
    return arr;
}

/// Create an MLX array from a Zig i32 slice.
pub fn arrayFromInt32(data: []const i32, shape: []const i32) c.mlx_array {
    const owned = std.heap.c_allocator.alloc(i32, data.len) catch unreachable;
    @memcpy(owned, data);
    return c.mlx_array_new_data_managed(
        @ptrCast(owned.ptr),
        shape.ptr,
        @intCast(shape.len),
        c.MLX_INT32,
        &freeManagedCBuffer,
    );
}

/// Load all tensors from a SafeTensors file into an MLX map.
pub fn loadSafetensors(path: []const u8, allocator: std.mem.Allocator, stream: c.mlx_stream) !c.mlx_map_string_to_array {
    _ = stream;
    const path_z = try allocator.dupeZ(u8, path);
    defer allocator.free(path_z);

    var weights = c.mlx_map_string_to_array_new();
    var metadata = c.mlx_map_string_to_string_new();
    defer _ = c.mlx_map_string_to_string_free(metadata);

    if (c.mlx_load_safetensors(&weights, &metadata, path_z.ptr, c.mlx_default_cpu_stream_new()) != 0) {
        return error.SafetensorsLoadFailed;
    }

    return weights;
}

pub fn insertWeight(weights: c.mlx_map_string_to_array, allocator: std.mem.Allocator, name: []const u8, arr: c.mlx_array) !void {
    const name_z = try allocator.dupeZ(u8, name);
    defer allocator.free(name_z);
    if (c.mlx_map_string_to_array_insert(weights, name_z.ptr, arr) != 0) {
        return error.MlxMapInsertFailed;
    }
}

/// Look up a weight by name from the MLX weight map.
/// Returns null if not found. Caller must free the returned array.
pub fn getWeight(weights: c.mlx_map_string_to_array, name: [:0]const u8) ?c.mlx_array {
    var arr = c.mlx_array_new();
    if (c.mlx_map_string_to_array_get(&arr, weights, name.ptr) != 0) {
        _ = c.mlx_array_free(arr);
        return null;
    }
    if (arr.ctx == null) {
        _ = c.mlx_array_free(arr);
        return null;
    }
    return arr;
}

/// Read the float32 data out of an evaluated MLX array into a Zig slice.
/// Caller owns the returned slice.
pub fn readFloat32(arr: c.mlx_array, allocator: std.mem.Allocator) ![]f32 {
    var read_arr = arr;
    var casted: ?c.mlx_array = null;
    defer {
        if (casted) |tmp| _ = c.mlx_array_free(tmp);
    }

    if (c.mlx_array_dtype(arr) != c.MLX_FLOAT32) {
        var tmp = c.mlx_array_new();
        try check(c.mlx_astype(&tmp, arr, c.MLX_FLOAT32, gpuStream()));
        casted = tmp;
        read_arr = tmp;
    }

    // Eval first to materialize
    const vec = c.mlx_vector_array_new_value(read_arr);
    defer _ = c.mlx_vector_array_free(vec);
    if (c.mlx_eval(vec) != 0) return error.MlxEvalFailed;

    const size = c.mlx_array_size(read_arr);
    const ptr = c.mlx_array_data_float32(read_arr);
    if (ptr == null) return error.MlxDataNull;

    const result = try allocator.alloc(f32, size);
    @memcpy(result, ptr[0..size]);
    return result;
}

pub fn readInt32(arr: c.mlx_array, allocator: std.mem.Allocator) ![]i32 {
    const vec = c.mlx_vector_array_new_value(arr);
    defer _ = c.mlx_vector_array_free(vec);
    if (c.mlx_eval(vec) != 0) return error.MlxEvalFailed;

    const size = c.mlx_array_size(arr);
    const ptr = c.mlx_array_data_int32(arr);
    if (ptr == null) return error.MlxDataNull;

    const result = try allocator.alloc(i32, size);
    @memcpy(result, ptr[0..size]);
    return result;
}

/// Borrow a materialized float32 view of an MLX array without copying.
/// The returned slice remains valid only while the MLX array stays alive.
pub fn borrowFloat32(arr: c.mlx_array) ![]const f32 {
    try evalArray(arr);

    const size = c.mlx_array_size(arr);
    const ptr = c.mlx_array_data_float32(arr);
    if (ptr == null) return error.MlxDataNull;
    return ptr[0..size];
}

pub fn evalArray(arr: c.mlx_array) !void {
    const vec = c.mlx_vector_array_new_value(arr);
    defer _ = c.mlx_vector_array_free(vec);
    if (c.mlx_eval(vec) != 0) return error.MlxEvalFailed;
}

pub fn optionalInt(value: ?i32) c.mlx_optional_int {
    return .{
        .value = value orelse 0,
        .has_value = value != null,
    };
}

pub fn argmaxAxis(arr: c.mlx_array, axis: i32, keepdims: bool) !c.mlx_array {
    var result = c.mlx_array_new();
    errdefer _ = c.mlx_array_free(result);
    try check(c.mlx_argmax_axis(&result, arr, axis, keepdims, gpuStream()));
    return result;
}

pub fn argpartitionAxis(arr: c.mlx_array, kth: i32, axis: i32) !c.mlx_array {
    var result = c.mlx_array_new();
    errdefer _ = c.mlx_array_free(result);
    try check(c.mlx_argpartition_axis(&result, arr, kth, axis, gpuStream()));
    return result;
}

pub fn takeAlongAxis(arr: c.mlx_array, indices: c.mlx_array, axis: i32) !c.mlx_array {
    var result = c.mlx_array_new();
    errdefer _ = c.mlx_array_free(result);
    try check(c.mlx_take_along_axis(&result, arr, indices, axis, gpuStream()));
    return result;
}

pub fn topkAxis(arr: c.mlx_array, k: i32, axis: i32) !c.mlx_array {
    var result = c.mlx_array_new();
    errdefer _ = c.mlx_array_free(result);
    try check(c.mlx_topk_axis(&result, arr, k, axis, gpuStream()));
    return result;
}

pub const QuantizeResult = struct {
    weight: c.mlx_array,
    scales: c.mlx_array,
    biases: ?c.mlx_array = null,
};

pub fn gatherMm(
    lhs: c.mlx_array,
    rhs: c.mlx_array,
    lhs_indices: ?c.mlx_array,
    rhs_indices: ?c.mlx_array,
    sorted_indices: bool,
) !c.mlx_array {
    var result = c.mlx_array_new();
    errdefer _ = c.mlx_array_free(result);
    try check(c.mlx_gather_mm(
        &result,
        lhs,
        rhs,
        lhs_indices orelse c.mlx_array_null(),
        rhs_indices orelse c.mlx_array_null(),
        sorted_indices,
        gpuStream(),
    ));
    return result;
}

pub fn qqmm(
    lhs: c.mlx_array,
    rhs: c.mlx_array,
    rhs_scales: ?c.mlx_array,
    group_size: ?i32,
    bits: ?i32,
    mode: [:0]const u8,
    global_scale_lhs: ?c.mlx_array,
    global_scale_rhs: ?c.mlx_array,
) !c.mlx_array {
    var result = c.mlx_array_new();
    errdefer _ = c.mlx_array_free(result);
    try check(c.mlx_qqmm(
        &result,
        lhs,
        rhs,
        rhs_scales orelse c.mlx_array_new(),
        optionalInt(group_size),
        optionalInt(bits),
        mode.ptr,
        global_scale_lhs orelse c.mlx_array_new(),
        global_scale_rhs orelse c.mlx_array_new(),
        gpuStream(),
    ));
    return result;
}

pub fn quantize(
    weight: c.mlx_array,
    group_size: ?i32,
    bits: ?i32,
    mode: [:0]const u8,
    global_scale: ?c.mlx_array,
) !QuantizeResult {
    var result_vec = c.mlx_vector_array_new();
    errdefer _ = c.mlx_vector_array_free(result_vec);
    try check(c.mlx_quantize(
        &result_vec,
        weight,
        optionalInt(group_size),
        optionalInt(bits),
        mode.ptr,
        global_scale orelse c.mlx_array_new(),
        gpuStream(),
    ));

    const count = c.mlx_vector_array_size(result_vec);
    if (count < 2 or count > 3) {
        _ = c.mlx_vector_array_free(result_vec);
        return error.MlxUnexpectedVectorSize;
    }

    var quantized_weight = c.mlx_array_new();
    errdefer _ = c.mlx_array_free(quantized_weight);
    try check(c.mlx_vector_array_get(&quantized_weight, result_vec, 0));

    var scales = c.mlx_array_new();
    errdefer _ = c.mlx_array_free(scales);
    try check(c.mlx_vector_array_get(&scales, result_vec, 1));

    var biases: ?c.mlx_array = null;
    errdefer {
        if (biases) |arr| _ = c.mlx_array_free(arr);
    }
    if (count == 3) {
        var arr = c.mlx_array_new();
        try check(c.mlx_vector_array_get(&arr, result_vec, 2));
        biases = arr;
    }
    _ = c.mlx_vector_array_free(result_vec);

    return .{
        .weight = quantized_weight,
        .scales = scales,
        .biases = biases,
    };
}

pub fn dequantize(
    weight: c.mlx_array,
    scales: c.mlx_array,
    biases: ?c.mlx_array,
    group_size: ?i32,
    bits: ?i32,
    mode: [:0]const u8,
    global_scale: ?c.mlx_array,
    dtype: ?c.mlx_dtype,
) !c.mlx_array {
    var result = c.mlx_array_new();
    errdefer _ = c.mlx_array_free(result);
    try check(c.mlx_dequantize(
        &result,
        weight,
        scales,
        biases orelse c.mlx_array_new(),
        optionalInt(group_size),
        optionalInt(bits),
        mode.ptr,
        global_scale orelse c.mlx_array_new(),
        .{
            .value = dtype orelse c.MLX_FLOAT32,
            .has_value = dtype != null,
        },
        gpuStream(),
    ));
    return result;
}

pub fn quantizedMatmul(
    lhs: c.mlx_array,
    quantized_weight: c.mlx_array,
    scales: c.mlx_array,
    biases: ?c.mlx_array,
    transpose: bool,
    group_size: ?i32,
    bits: ?i32,
    mode: [:0]const u8,
) !c.mlx_array {
    var result = c.mlx_array_new();
    errdefer _ = c.mlx_array_free(result);
    try check(c.mlx_quantized_matmul(
        &result,
        lhs,
        quantized_weight,
        scales,
        biases orelse c.mlx_array_new(),
        transpose,
        optionalInt(group_size),
        optionalInt(bits),
        mode.ptr,
        gpuStream(),
    ));
    return result;
}

pub fn gatherQmm(
    lhs: c.mlx_array,
    quantized_weight: c.mlx_array,
    scales: c.mlx_array,
    biases: ?c.mlx_array,
    lhs_indices: ?c.mlx_array,
    rhs_indices: ?c.mlx_array,
    transpose: bool,
    group_size: ?i32,
    bits: ?i32,
    mode: [:0]const u8,
    sorted_indices: bool,
) !c.mlx_array {
    var result = c.mlx_array_new();
    errdefer _ = c.mlx_array_free(result);
    try check(c.mlx_gather_qmm(
        &result,
        lhs,
        quantized_weight,
        scales,
        if (biases) |arr| arr else c.mlx_array_null(),
        if (lhs_indices) |arr| arr else c.mlx_array_null(),
        if (rhs_indices) |arr| arr else c.mlx_array_null(),
        transpose,
        optionalInt(group_size),
        optionalInt(bits),
        mode.ptr,
        sorted_indices,
        gpuStream(),
    ));
    return result;
}

/// Check result and return error on failure.
pub fn check(result: c_int) !void {
    if (result != 0) return error.MlxOpFailed;
}

fn mlxDType(dtype: tensor_mod.DType) c.mlx_dtype {
    return switch (dtype) {
        .f32 => c.MLX_FLOAT32,
        .f16 => c.MLX_FLOAT16,
        .bf16 => c.MLX_BFLOAT16,
        .f64 => c.MLX_FLOAT64,
        .i8 => c.MLX_INT8,
        .i16 => c.MLX_INT16,
        .i32 => c.MLX_INT32,
        .i64 => c.MLX_INT64,
        .u8 => c.MLX_UINT8,
        .bool_ => c.MLX_BOOL,
    };
}

pub fn readFloat32Into(arr: c.mlx_array, out: []f32) !void {
    var read_arr = arr;
    var casted: ?c.mlx_array = null;
    defer {
        if (casted) |tmp| _ = c.mlx_array_free(tmp);
    }

    if (c.mlx_array_dtype(arr) != c.MLX_FLOAT32) {
        var tmp = c.mlx_array_new();
        try check(c.mlx_astype(&tmp, arr, c.MLX_FLOAT32, gpuStream()));
        casted = tmp;
        read_arr = tmp;
    }

    const vec = c.mlx_vector_array_new_value(read_arr);
    defer _ = c.mlx_vector_array_free(vec);
    if (c.mlx_eval(vec) != 0) return error.MlxEvalFailed;

    const size = c.mlx_array_size(read_arr);
    if (out.len != size) return error.ShapeMismatch;
    const ptr = c.mlx_array_data_float32(read_arr);
    if (ptr == null) return error.MlxDataNull;
    @memcpy(out, ptr[0..size]);
}

pub fn allSumFloat32InPlaceOnStream(values: []f32, stream: Stream, group: DistributedGroup) !void {
    const shape = [_]i32{@intCast(values.len)};
    const input_arr = arrayFromFloat32(values, &shape);
    defer _ = c.mlx_array_free(input_arr);
    var result = c.mlx_array_new();
    defer _ = c.mlx_array_free(result);
    try check(c.mlx_distributed_all_sum(&result, input_arr, group, stream));
    try readFloat32Into(result, values);
}

pub fn allGatherFloat32OnStream(allocator: std.mem.Allocator, stream: Stream, input: []const f32, group: DistributedGroup) ![]f32 {
    const shape = [_]i32{@intCast(input.len)};
    const input_arr = arrayFromFloat32(input, &shape);
    defer _ = c.mlx_array_free(input_arr);
    var result = c.mlx_array_new();
    defer _ = c.mlx_array_free(result);
    try check(c.mlx_distributed_all_gather(&result, input_arr, group, stream));
    return try readFloat32(result, allocator);
}

pub fn shardMatrixColumnsFloat32(allocator: std.mem.Allocator, matrix: []const f32, rows: usize, cols: usize, rank: usize, world_size: usize) !ShardedMatrix {
    if (matrix.len != rows * cols) return error.ShapeMismatch;
    const range = shardRange(cols, rank, world_size);
    const shard = try allocator.alloc(f32, rows * range.len);
    errdefer allocator.free(shard);
    for (0..rows) |row| {
        const src = matrix[row * cols + range.start ..][0..range.len];
        const dst = shard[row * range.len ..][0..range.len];
        @memcpy(dst, src);
    }
    return .{ .data = shard, .range = range };
}

pub fn shardMatrixRowsFloat32(allocator: std.mem.Allocator, matrix: []const f32, rows: usize, cols: usize, rank: usize, world_size: usize) !ShardedMatrix {
    if (matrix.len != rows * cols) return error.ShapeMismatch;
    const range = shardRange(rows, rank, world_size);
    const shard = try allocator.alloc(f32, range.len * cols);
    errdefer allocator.free(shard);
    @memcpy(shard, matrix[range.start * cols ..][0 .. range.len * cols]);
    return .{ .data = shard, .range = range };
}

pub fn shardVectorFloat32(allocator: std.mem.Allocator, values: []const f32, rank: usize, world_size: usize) !ShardedMatrix {
    const range = shardRange(values.len, rank, world_size);
    return .{ .data = try allocator.dupe(f32, values[range.start..range.end()]), .range = range };
}

pub fn matmul2DFloat32IntoOnStream(out: []f32, stream: Stream, lhs: []const f32, lhs_rows: usize, lhs_cols: usize, rhs: []const f32, rhs_rows: usize, rhs_cols: usize) !void {
    if (lhs_cols != rhs_rows) return error.ShapeMismatch;
    if (lhs.len != lhs_rows * lhs_cols) return error.ShapeMismatch;
    if (rhs.len != rhs_rows * rhs_cols) return error.ShapeMismatch;
    if (out.len != lhs_rows * rhs_cols) return error.ShapeMismatch;

    const lhs_shape = [_]i32{ @intCast(lhs_rows), @intCast(lhs_cols) };
    const rhs_shape = [_]i32{ @intCast(rhs_rows), @intCast(rhs_cols) };
    const lhs_arr = arrayFromFloat32(lhs, &lhs_shape);
    defer _ = c.mlx_array_free(lhs_arr);
    const rhs_arr = arrayFromFloat32(rhs, &rhs_shape);
    defer _ = c.mlx_array_free(rhs_arr);
    var result = c.mlx_array_new();
    defer _ = c.mlx_array_free(result);
    try check(c.mlx_matmul(&result, lhs_arr, rhs_arr, stream));
    try readFloat32Into(result, out);
}

pub fn matmul2DFloat32WithBiasIntoOnStream(out: []f32, stream: Stream, lhs: []const f32, lhs_rows: usize, lhs_cols: usize, rhs: []const f32, rhs_rows: usize, rhs_cols: usize, bias: []const f32) !void {
    try matmul2DFloat32IntoOnStream(out, stream, lhs, lhs_rows, lhs_cols, rhs, rhs_rows, rhs_cols);
    if (bias.len == 0) return;
    if (bias.len != rhs_cols) return error.ShapeMismatch;
    for (0..lhs_rows) |row| {
        const out_row = out[row * rhs_cols ..][0..rhs_cols];
        for (0..rhs_cols) |col| out_row[col] += bias[col];
    }
}

pub fn linearReplicatedInputToShardedOutputOnStream(out: []f32, stream: Stream, input: []const f32, rows: usize, in_dim: usize, sharded_weight: []const f32, sharded_out_dim: usize, sharded_bias: []const f32) !void {
    if (input.len != rows * in_dim) return error.ShapeMismatch;
    if (sharded_weight.len != sharded_out_dim * in_dim) return error.ShapeMismatch;
    if (out.len != rows * sharded_out_dim) return error.ShapeMismatch;
    if (sharded_bias.len != 0 and sharded_bias.len != sharded_out_dim) return error.ShapeMismatch;

    const input_shape = [_]i32{ @intCast(rows), @intCast(in_dim) };
    const weight_shape = [_]i32{ @intCast(sharded_out_dim), @intCast(in_dim) };
    const input_arr = arrayFromBorrowedFloat32(input, &input_shape);
    defer _ = c.mlx_array_free(input_arr);
    const weight_arr = arrayFromBorrowedFloat32(sharded_weight, &weight_shape);
    defer _ = c.mlx_array_free(weight_arr);

    var weight_t = c.mlx_array_new();
    defer _ = c.mlx_array_free(weight_t);
    try check(c.mlx_transpose(&weight_t, weight_arr, stream));

    var result = c.mlx_array_new();
    defer _ = c.mlx_array_free(result);
    try check(c.mlx_matmul(&result, input_arr, weight_t, stream));
    try readFloat32Into(result, out);

    if (sharded_bias.len == 0) return;
    for (0..rows) |row| {
        const out_row = out[row * sharded_out_dim ..][0..sharded_out_dim];
        for (0..sharded_out_dim) |col| out_row[col] += sharded_bias[col];
    }
}

pub fn linearShardedInputToReplicatedOutputOnStream(out: []f32, stream: Stream, input_shard: []const f32, rows: usize, sharded_in_dim: usize, sharded_weight: []const f32, out_dim: usize, full_bias: []const f32, group: DistributedGroup) !void {
    if (input_shard.len != rows * sharded_in_dim) return error.ShapeMismatch;
    if (sharded_weight.len != out_dim * sharded_in_dim) return error.ShapeMismatch;
    if (out.len != rows * out_dim) return error.ShapeMismatch;
    if (full_bias.len != 0 and full_bias.len != out_dim) return error.ShapeMismatch;

    const input_shape = [_]i32{ @intCast(rows), @intCast(sharded_in_dim) };
    const weight_shape = [_]i32{ @intCast(out_dim), @intCast(sharded_in_dim) };
    const input_arr = arrayFromBorrowedFloat32(input_shard, &input_shape);
    defer _ = c.mlx_array_free(input_arr);
    const weight_arr = arrayFromBorrowedFloat32(sharded_weight, &weight_shape);
    defer _ = c.mlx_array_free(weight_arr);

    var weight_t = c.mlx_array_new();
    defer _ = c.mlx_array_free(weight_t);
    try check(c.mlx_transpose(&weight_t, weight_arr, stream));

    var result = c.mlx_array_new();
    defer _ = c.mlx_array_free(result);
    try check(c.mlx_matmul(&result, input_arr, weight_t, stream));
    try readFloat32Into(result, out);
    try allSumFloat32InPlaceOnStream(out, stream, group);
    if (full_bias.len == 0) return;
    for (0..rows) |row| {
        const out_row = out[row * out_dim ..][0..out_dim];
        for (0..out_dim) |col| out_row[col] += full_bias[col];
    }
}
