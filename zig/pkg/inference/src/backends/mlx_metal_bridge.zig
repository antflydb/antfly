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

// MLX ↔ MetalTensor bridge.
//
// Compiled when `enable_mlx` is true. Lets
// MLX-oriented call sites invoke the Metal runtime by converting at the
// boundary rather than threading `MetalTensor` through every layer.

const std = @import("std");
const build_options = @import("build_options");
const metal_tensor = @import("metal_tensor.zig");
const mlx = @import("mlx.zig");

pub const MetalTensor = metal_tensor.MetalTensor;
const c = mlx.c;

/// Materialize an MLX array and return a borrowed MetalTensor view over its
/// float32 data. The caller must keep `arr` alive for the tensor's lifetime.
/// `shape_buf` receives a freshly-populated i32 shape; its storage must
/// outlive the returned tensor (usually stack-allocated at the call site).
pub fn borrowMlxArrayAsMetalTensor(
    arr: c.mlx_array,
    shape_buf: []i32,
) !MetalTensor {
    if (!build_options.enable_mlx) {
        @compileError("mlx_metal_bridge requires -Dmlx=true");
    }
    if (c.mlx_array_dtype(arr) != c.MLX_FLOAT32) return error.MlxDtypeMismatch;
    const nd = c.mlx_array_ndim(arr);
    if (shape_buf.len < nd) return error.ShapeBufferTooSmall;

    try mlx.evalArray(arr);
    const ptr = c.mlx_array_data_float32(arr) orelse return error.MlxDataNull;
    const size: usize = @intCast(c.mlx_array_size(arr));

    for (0..nd) |i| {
        shape_buf[i] = @intCast(c.mlx_array_dim(arr, @intCast(i)));
    }
    return MetalTensor.borrowed(@constCast(ptr), size, shape_buf[0..nd]);
}

/// Wrap an owned MetalTensor output (allocated with c_allocator) in an MLX
/// array. Transfers ownership of the buffer to MLX. `shape_i32` must outlive
/// the call; MLX copies it internally.
pub fn adoptMetalTensorAsMlxArray(tensor: MetalTensor) c.mlx_array {
    if (!build_options.enable_mlx) {
        @compileError("mlx_metal_bridge requires -Dmlx=true");
    }
    std.debug.assert(tensor.owned_by_c_allocator);
    return mlx.arrayFromOwnedFloat32(tensor.slice(), tensor.shape());
}

/// Like `adoptMetalTensorAsMlxArray` but borrows the buffer — caller retains
/// ownership. Useful when the Metal output is short-lived scratch that the
/// caller wants to free itself.
pub fn borrowMetalTensorAsMlxArray(tensor: MetalTensor) c.mlx_array {
    if (!build_options.enable_mlx) {
        @compileError("mlx_metal_bridge requires -Dmlx=true");
    }
    return mlx.arrayFromBorrowedFloat32(tensor.slice(), tensor.shape());
}

/// Bridge a slice of mlx_arrays to borrowed MetalTensors. The returned slice
/// and its shape-buffer storage are both allocated with `allocator` — caller
/// must free both (tensor slice first, then shapes buffer). All tensors view
/// the source arrays' backing memory, so the arrays must outlive the tensors.
pub fn borrowMlxArraysAsMetalTensors(
    allocator: std.mem.Allocator,
    arrays: []const c.mlx_array,
) !struct { tensors: []MetalTensor, shapes: [][metal_tensor.max_dims]i32 } {
    if (!build_options.enable_mlx) {
        @compileError("mlx_metal_bridge requires -Dmlx=true");
    }
    const shapes = try allocator.alloc([metal_tensor.max_dims]i32, arrays.len);
    errdefer allocator.free(shapes);
    const tensors = try allocator.alloc(MetalTensor, arrays.len);
    errdefer allocator.free(tensors);
    for (arrays, 0..) |arr, i| {
        tensors[i] = try borrowMlxArrayAsMetalTensor(arr, shapes[i][0..]);
    }
    return .{ .tensors = tensors, .shapes = shapes };
}

/// Bridge an optional mlx_array to an optional borrowed MetalTensor.
/// The shape_buf must outlive the returned tensor (typically stack-allocated).
pub fn borrowOptionalMlxArrayAsMetalTensor(
    arr_opt: ?c.mlx_array,
    shape_buf: []i32,
) !?MetalTensor {
    if (!build_options.enable_mlx) {
        @compileError("mlx_metal_bridge requires -Dmlx=true");
    }
    const arr = arr_opt orelse return null;
    return try borrowMlxArrayAsMetalTensor(arr, shape_buf);
}
