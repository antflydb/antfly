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
const ml = @import("ml");
const build_options = @import("build_options");
const platform = @import("antfly_platform");
const ops = @import("../ops/ops.zig");
const CT = ops.CT;
const ComputeBackend = ops.ComputeBackend;

/// Transfer a tensor without changing dtype. Try direct device copy first;
/// otherwise stage its exact bytes (f32 may use the numeric host path).
pub fn copy(
    allocator: std.mem.Allocator,
    value: CT,
    from: *const ComputeBackend,
    to: *const ComputeBackend,
) !CT {
    return copyWithKnownShape(allocator, value, from, to, null);
}

pub fn copyWithKnownShape(
    allocator: std.mem.Allocator,
    value: CT,
    from: *const ComputeBackend,
    to: *const ComputeBackend,
    known_shape: ?[]const i64,
) !CT {
    return copyImpl(allocator, value, from, to, known_shape, false);
}

/// Partition callbacks historically accept providers that only expose numeric
/// readback. Sources with typed metadata use the exact transfer contract.
pub fn copyRuntimeInput(allocator: std.mem.Allocator, value: CT, from: *const ComputeBackend, to: *const ComputeBackend) !CT {
    return copyImpl(allocator, value, from, to, null, true);
}

fn copyImpl(
    allocator: std.mem.Allocator,
    value: CT,
    from: *const ComputeBackend,
    to: *const ComputeBackend,
    known_shape: ?[]const i64,
    allow_untyped_float32: bool,
) !CT {
    const trace = platform.env.getenvBoolDefault("TERMITE_GRAPH_EXECUTOR_TRACE_TRANSFERS", false);
    if (trace) std.debug.print("graph_executor_transfer_stage: begin from={s} to={s}\n", .{ @tagName(from.kind()), @tagName(to.kind()) });
    if (try to.copyTensorFromBackend(from, value)) |copied| return copied;
    const runtime_shape_needed = if (known_shape) |shape| blk: {
        for (shape) |dim| if (dim < 0) break :blk true;
        break :blk false;
    } else true;
    const owned_shape_i64 = if (runtime_shape_needed) try from.tensorShape(value, allocator) else null;
    defer if (owned_shape_i64) |shape| allocator.free(shape);
    const shape_i64 = owned_shape_i64 orelse known_shape.?;
    if (trace) std.debug.print("graph_executor_transfer_stage: shape_i64={any}\n", .{shape_i64});
    const shape_i32 = try tensorShapeI32(allocator, shape_i64);
    defer allocator.free(shape_i32);
    if (trace) std.debug.print("graph_executor_transfer_stage: shape_i32={any}\n", .{shape_i32});
    const dtype = if (allow_untyped_float32 and from.vtable.tensorDType == null) .f32 else try from.tensorDType(value);
    switch (dtype) {
        inline .i8, .i16, .i32, .i64, .u8, .bool_, .f16, .bf16, .f64 => |tag| {
            const exported = (try from.exportTensorData(value, allocator)) orelse return error.UnsupportedTensorType;
            const bytes = switch (exported.payload) {
                .bytes => |bytes| bytes,
                .quantized_f32 => |quant| {
                    allocator.free(quant.raw_bytes);
                    allocator.free(quant.shape);
                    return error.UnsupportedTensorType;
                },
            };
            defer allocator.free(bytes);
            if (exported.dtype != dtype) return error.UnsupportedTensorType;
            if (try to.fromConstantBytes(bytes, @field(ml.graph.DType, @tagName(tag)), shape_i64)) |exact| return exact;
            if (dtype != .i32 or bytes.len % 4 != 0) return error.UnsupportedTensorType;
            const integers = try allocator.alloc(i32, bytes.len / 4);
            defer allocator.free(integers);
            for (integers, 0..) |*integer, i| integer.* = std.mem.readInt(i32, bytes[i * 4 ..][0..4], .little);
            return (try to.fromInt32Shape(integers, shape_i32)) orelse return error.UnsupportedTensorType;
        },
        else => {},
    }
    const transferred = try copyFloat32(allocator, value, from, to, shape_i32);
    errdefer to.free(transferred);
    if (trace) std.debug.print("graph_executor_transfer_stage: from_float32_shape_done\n", .{});
    if (comptime build_options.enable_metal) if (to.kind() == .metal) {
        if (try @import("../ops/metal_compute.zig").MetalCompute.makeDeviceResident(to, transferred)) |device_transferred| {
            if (device_transferred != transferred) to.free(transferred);
            if (trace) std.debug.print("graph_executor_transfer_stage: made_metal_device_resident\n", .{});
            return device_transferred;
        }
    };
    if (trace) std.debug.print("graph_executor_transfer_stage: end host_resident\n", .{});
    return transferred;
}

/// Stage the host F32 operands required by the Metal dynamic projection path.
pub fn copyHostFloat32(allocator: std.mem.Allocator, value: CT, from: *const ComputeBackend, to: *const ComputeBackend) !CT {
    const dimensions = try from.tensorShape(value, allocator);
    defer allocator.free(dimensions);
    const shape_i32 = try tensorShapeI32(allocator, dimensions);
    defer allocator.free(shape_i32);
    return copyFloat32(allocator, value, from, to, shape_i32);
}

fn copyFloat32(allocator: std.mem.Allocator, value: CT, from: *const ComputeBackend, to: *const ComputeBackend, shape_i32: []const i32) !CT {
    const data = try from.toFloat32(value, allocator);
    defer allocator.free(data);
    return to.fromFloat32Shape(data, shape_i32);
}

fn tensorShapeI32(allocator: std.mem.Allocator, shape: []const i64) ![]i32 {
    const out = try allocator.alloc(i32, shape.len);
    errdefer allocator.free(out);
    for (shape, 0..) |dim, i| {
        out[i] = std.math.cast(i32, dim) orelse return error.UnsupportedShape;
    }
    return out;
}
