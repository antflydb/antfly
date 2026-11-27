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
        break :blk !(try from.tensorShapeMatches(value, shape) orelse true);
    } else true;
    const owned_shape_i64 = if (runtime_shape_needed) try from.tensorShape(value, allocator) else null;
    defer if (owned_shape_i64) |shape| allocator.free(shape);
    const shape_i64 = owned_shape_i64 orelse known_shape.?;
    if (trace) std.debug.print("graph_executor_transfer_stage: shape_i64={any}\n", .{shape_i64});
    const dtype = if (allow_untyped_float32 and from.vtable.tensorDType == null) .f32 else try from.tensorDType(value);
    switch (dtype) {
        inline .i8, .i16, .i32, .i64, .u8, .bool_, .f16, .bf16, .f64 => |tag| {
            const borrowed: ?[]const u8 = if (comptime build_options.enable_native)
                @import("../ops/native_compute.zig").borrowDenseTransferBytes(from, value, dtype, shape_i64)
            else
                null;
            var owned: ?[]u8 = null;
            defer if (owned) |data| allocator.free(data);
            const bytes = borrowed orelse exact: {
                const exported = (try from.exportTensorData(value, allocator)) orelse return error.UnsupportedTensorType;
                const data = switch (exported.payload) {
                    .bytes => |bytes| bytes,
                    .quantized_f32 => |quant| {
                        allocator.free(quant.raw_bytes);
                        allocator.free(quant.shape);
                        return error.UnsupportedTensorType;
                    },
                };
                owned = data;
                if (exported.dtype != dtype) return error.UnsupportedTensorType;
                break :exact data;
            };
            if (try to.fromConstantBytes(bytes, @field(ml.graph.DType, @tagName(tag)), shape_i64)) |exact| return exact;
            if (dtype != .i32 or bytes.len % 4 != 0) return error.UnsupportedTensorType;
            var inline_dimensions: [ml.graph.shape.max_rank]i32 = undefined;
            const shape_i32 = try tensorShapeI32(allocator, shape_i64, &inline_dimensions);
            defer if (shape_i64.len > inline_dimensions.len) allocator.free(shape_i32);
            const integers = try allocator.alloc(i32, bytes.len / 4);
            defer allocator.free(integers);
            for (integers, 0..) |*integer, i| integer.* = std.mem.readInt(i32, bytes[i * 4 ..][0..4], .little);
            return (try to.fromInt32Shape(integers, shape_i32)) orelse return error.UnsupportedTensorType;
        },
        else => {},
    }
    var inline_dimensions: [ml.graph.shape.max_rank]i32 = undefined;
    const shape_i32 = try tensorShapeI32(allocator, shape_i64, &inline_dimensions);
    defer if (shape_i64.len > inline_dimensions.len) allocator.free(shape_i32);
    if (trace) std.debug.print("graph_executor_transfer_stage: shape_i32={any}\n", .{shape_i32});
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
    var inline_dimensions: [ml.graph.shape.max_rank]i32 = undefined;
    const shape_i32 = try tensorShapeI32(allocator, dimensions, &inline_dimensions);
    defer if (dimensions.len > inline_dimensions.len) allocator.free(shape_i32);
    return copyFloat32(allocator, value, from, to, shape_i32);
}

fn copyFloat32(allocator: std.mem.Allocator, value: CT, from: *const ComputeBackend, to: *const ComputeBackend, shape_i32: []const i32) !CT {
    if (try from.hostFloat32Read(value)) |data| return to.fromFloat32Shape(data, shape_i32);
    const data = try from.toFloat32(value, allocator);
    defer allocator.free(data);
    return to.fromFloat32Shape(data, shape_i32);
}

fn tensorShapeI32(allocator: std.mem.Allocator, shape: []const i64, inline_dimensions: []i32) ![]i32 {
    const borrowed = shape.len <= inline_dimensions.len;
    const out = if (borrowed) inline_dimensions[0..shape.len] else try allocator.alloc(i32, shape.len);
    errdefer if (!borrowed) allocator.free(out);
    for (shape, 0..) |dim, i| {
        out[i] = std.math.cast(i32, dim) orelse return error.UnsupportedShape;
    }
    return out;
}

test "transfer dimensions borrow inline storage and retain checked overflow fallback" {
    var inline_dimensions: [2]i32 = undefined;
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    const borrowed = try tensorShapeI32(failing.allocator(), &.{ 3, 5 }, &inline_dimensions);
    try std.testing.expectEqualSlices(i32, &.{ 3, 5 }, borrowed);
    try std.testing.expect(!failing.has_induced_failure);
    try std.testing.expectError(error.OutOfMemory, tensorShapeI32(failing.allocator(), &.{ 3, 5, 7 }, &inline_dimensions));
    const owned = try tensorShapeI32(std.testing.allocator, &.{ 3, 5, 7 }, &inline_dimensions);
    defer std.testing.allocator.free(owned);
    try std.testing.expectEqualSlices(i32, &.{ 3, 5, 7 }, owned);
    try std.testing.expectError(error.UnsupportedShape, tensorShapeI32(std.testing.allocator, &.{ 3, 5, std.math.maxInt(i64) }, &inline_dimensions));
}

test "exact Native integer transfer borrows bytes without allocating temporary storage" {
    if (comptime !build_options.enable_native) return error.SkipZigTest;
    const allocator = std.testing.allocator;
    const Context = @import("../backends/compute_context.zig").BackendContext;
    var source_context = try Context.init(allocator, .native, std.testing.io);
    defer source_context.deinit(allocator);
    var target_context = try Context.init(allocator, .native, std.testing.io);
    defer target_context.deinit(allocator);
    const source = source_context.computeBackend();
    const target = target_context.computeBackend();
    const shape = &[_]i64{2};
    var input: [16]u8 = undefined;
    std.mem.writeInt(i64, input[0..8], (1 << 56) + 17, .little);
    std.mem.writeInt(i64, input[8..16], -(1 << 58) + 5, .little);
    const original = (try source.fromConstantBytes(&input, .i64, shape)) orelse return error.UnsupportedTensorType;
    var original_owned = true;
    defer if (original_owned) source.free(original);
    var failing = std.testing.FailingAllocator.init(allocator, .{ .fail_index = 0 });
    const copied = try copyWithKnownShape(failing.allocator(), original, &source, &target, shape);
    defer target.free(copied);
    try std.testing.expect(!failing.has_induced_failure);
    source.free(original);
    original_owned = false;
    const exported = (try target.exportTensorData(copied, allocator)) orelse return error.UnsupportedTensorType;
    const bytes = exported.payload.bytes;
    defer allocator.free(bytes);
    try std.testing.expectEqual(.i64, exported.dtype);
    try std.testing.expectEqualSlices(u8, &input, bytes);

    const copied_with_stale_shape = try copyWithKnownShape(allocator, copied, &target, &source, &.{ 1, 2 });
    defer source.free(copied_with_stale_shape);
    try std.testing.expect((try source.tensorShapeMatches(copied_with_stale_shape, shape)).?);

    const floats = [_]f32{ -2, 3 };
    const float_copy = imported: {
        const float_input = try source.fromFloat32Shape(&floats, &.{2});
        defer source.free(float_input);
        break :imported try copyWithKnownShape(failing.allocator(), float_input, &source, &target, shape);
    };
    defer target.free(float_copy);
    try std.testing.expect(!failing.has_induced_failure);
    const float_output = try target.toFloat32(float_copy, allocator);
    defer allocator.free(float_output);
    try std.testing.expectEqualSlices(f32, &floats, float_output);

    const reduced_bits: [4]u8 = .{ 0x01, 0x80, 0x55, 0x7e };
    inline for (.{ .f16, .bf16 }) |dtype| {
        const reduced_copy = imported: {
            const reduced = (try source.fromConstantBytes(&reduced_bits, dtype, shape)).?;
            defer source.free(reduced);
            break :imported try copyWithKnownShape(failing.allocator(), reduced, &source, &target, shape);
        };
        defer target.free(reduced_copy);
        const reduced_data = (try target.exportTensorData(reduced_copy, allocator)).?;
        defer allocator.free(reduced_data.payload.bytes);
        try std.testing.expectEqual(dtype, reduced_data.dtype);
        try std.testing.expectEqualSlices(u8, &reduced_bits, reduced_data.payload.bytes);
    }
    try std.testing.expect(!failing.has_induced_failure);

    const wide_empty_shape = &[_]i64{ 0, 1 << 31 };
    const empty = (try source.fromConstantBytes(&.{}, .i64, wide_empty_shape)).?;
    defer source.free(empty);
    const empty_copy = try copyWithKnownShape(failing.allocator(), empty, &source, &target, wide_empty_shape);
    defer target.free(empty_copy);
    try std.testing.expect((try target.tensorShapeMatches(empty_copy, wide_empty_shape)).?);
}
