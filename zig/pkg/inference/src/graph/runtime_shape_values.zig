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
const ops = @import("../ops/ops.zig");
const ComputeBackend = ops.ComputeBackend;
const CT = ops.CT;

/// Shape tensors are control data: transfer only their small payload, preserving
/// integer widths instead of converting dimensions through floating point.
pub fn read(cb: *const ComputeBackend, input: CT, out: *[8]i64) ![]const i64 {
    const allocator = std.heap.page_allocator;
    const shape = try cb.tensorShape(input, allocator);
    defer allocator.free(shape);
    var count: usize = 1;
    for (shape) |dim| {
        if (dim < 0) return error.InvalidTensorShape;
        count = try std.math.mul(usize, count, @intCast(dim));
    }
    if (count > out.len) return error.InvalidTensorShape;
    switch (try cb.tensorDType(input)) {
        .i8, .i16, .i32, .i64, .u8, .bool_ => {
            const exported = (try cb.exportTensorData(input, allocator)) orelse return error.UnsupportedTensorType;
            defer allocator.free(exported.payload.bytes);
            const bytes = exported.payload.bytes;
            if (bytes.len != count * exported.dtype.byteSize()) return error.InvalidTensorShape;
            for (out[0..count], 0..) |*dim, i| dim.* = switch (exported.dtype) {
                .i8 => @as(i8, @bitCast(bytes[i])),
                .u8, .bool_ => bytes[i],
                .i16 => std.mem.readInt(i16, bytes[i * 2 ..][0..2], .little),
                .i32 => std.mem.readInt(i32, bytes[i * 4 ..][0..4], .little),
                .i64 => std.mem.readInt(i64, bytes[i * 8 ..][0..8], .little),
                else => return error.UnsupportedTensorType,
            };
        },
        else => {
            const values = try cb.toFloat32(input, allocator);
            defer allocator.free(values);
            if (values.len != count) return error.InvalidTensorShape;
            for (values, 0..) |value, i| {
                if (!std.math.isFinite(value) or @trunc(value) != value or value < -9223372036854775808.0 or value >= 9223372036854775808.0) return error.InvalidTensorShape;
                out[i] = @intFromFloat(value);
            }
        },
    }
    return out[0..count];
}
