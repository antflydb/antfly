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

pub const DType = enum {
    f32,
    f16,
    bf16,
    f64,
    i8,
    i16,
    i32,
    i64,
    u8,
    bool_,

    pub fn byteSize(self: DType) usize {
        return switch (self) {
            .f32, .i32 => 4,
            .f16, .bf16, .i16 => 2,
            .f64, .i64 => 8,
            .i8, .u8, .bool_ => 1,
        };
    }
};

pub const TensorInfo = struct {
    name: []const u8,
    dtype: DType,
    shape: []const i64,
};

/// A multi-dimensional tensor backed by a flat buffer.
pub const Tensor = struct {
    data: []u8,
    dtype: DType,
    shape: []const i64,
    name: []const u8,
    allocator: std.mem.Allocator,
    owns_data: bool,
    owns_shape: bool,

    pub fn initFloat32(allocator: std.mem.Allocator, name: []const u8, shape: []const i64, data: []const f32) !Tensor {
        const bytes = std.mem.sliceAsBytes(data);
        const owned_bytes = try allocator.dupe(u8, bytes);
        const owned_shape = try allocator.dupe(i64, shape);
        return .{
            .data = owned_bytes,
            .dtype = .f32,
            .shape = owned_shape,
            .name = name,
            .allocator = allocator,
            .owns_data = true,
            .owns_shape = true,
        };
    }

    pub fn initInt64(allocator: std.mem.Allocator, name: []const u8, shape: []const i64, data: []const i64) !Tensor {
        const bytes = std.mem.sliceAsBytes(data);
        const owned_bytes = try allocator.dupe(u8, bytes);
        const owned_shape = try allocator.dupe(i64, shape);
        return .{
            .data = owned_bytes,
            .dtype = .i64,
            .shape = owned_shape,
            .name = name,
            .allocator = allocator,
            .owns_data = true,
            .owns_shape = true,
        };
    }

    pub fn initInt8(allocator: std.mem.Allocator, name: []const u8, shape: []const i64, data: []const i8) !Tensor {
        const bytes = std.mem.sliceAsBytes(data);
        const owned_bytes = try allocator.dupe(u8, bytes);
        const owned_shape = try allocator.dupe(i64, shape);
        return .{
            .data = owned_bytes,
            .dtype = .i8,
            .shape = owned_shape,
            .name = name,
            .allocator = allocator,
            .owns_data = true,
            .owns_shape = true,
        };
    }

    pub fn initInt16(allocator: std.mem.Allocator, name: []const u8, shape: []const i64, data: []const i16) !Tensor {
        const bytes = std.mem.sliceAsBytes(data);
        const owned_bytes = try allocator.dupe(u8, bytes);
        const owned_shape = try allocator.dupe(i64, shape);
        return .{
            .data = owned_bytes,
            .dtype = .i16,
            .shape = owned_shape,
            .name = name,
            .allocator = allocator,
            .owns_data = true,
            .owns_shape = true,
        };
    }

    pub fn initFloat64(allocator: std.mem.Allocator, name: []const u8, shape: []const i64, data: []const f64) !Tensor {
        const bytes = std.mem.sliceAsBytes(data);
        const owned_bytes = try allocator.dupe(u8, bytes);
        const owned_shape = try allocator.dupe(i64, shape);
        return .{
            .data = owned_bytes,
            .dtype = .f64,
            .shape = owned_shape,
            .name = name,
            .allocator = allocator,
            .owns_data = true,
            .owns_shape = true,
        };
    }

    pub fn initBool(allocator: std.mem.Allocator, name: []const u8, shape: []const i64, data: []const u8) !Tensor {
        const owned_bytes = try allocator.dupe(u8, data);
        const owned_shape = try allocator.dupe(i64, shape);
        return .{
            .data = owned_bytes,
            .dtype = .bool_,
            .shape = owned_shape,
            .name = name,
            .allocator = allocator,
            .owns_data = true,
            .owns_shape = true,
        };
    }

    pub fn asFloat32(self: *const Tensor) []const f32 {
        const aligned: []align(@alignOf(f32)) const u8 = @alignCast(self.data);
        return std.mem.bytesAsSlice(f32, aligned);
    }

    pub fn isAlignedFor(self: *const Tensor, comptime T: type) bool {
        return (@intFromPtr(self.data.ptr) % @alignOf(T)) == 0;
    }

    pub fn asFloat32IfAligned(self: *const Tensor) ?[]const f32 {
        if (!self.isAlignedFor(f32)) return null;
        return self.asFloat32();
    }

    pub fn asFloat32Mut(self: *Tensor) []f32 {
        const aligned: []align(@alignOf(f32)) u8 = @alignCast(self.data);
        return std.mem.bytesAsSlice(f32, aligned);
    }

    pub fn asInt64(self: *const Tensor) []const i64 {
        const aligned: []align(@alignOf(i64)) const u8 = @alignCast(self.data);
        return std.mem.bytesAsSlice(i64, aligned);
    }

    pub fn asFloat16IfAligned(self: *const Tensor) ?[]const f16 {
        if (!self.isAlignedFor(f16)) return null;
        const aligned: []align(@alignOf(f16)) const u8 = @alignCast(self.data);
        return std.mem.bytesAsSlice(f16, aligned);
    }

    pub fn asInt8(self: *const Tensor) []const i8 {
        return std.mem.bytesAsSlice(i8, self.data);
    }

    pub fn asInt16(self: *const Tensor) []const i16 {
        const aligned: []align(@alignOf(i16)) const u8 = @alignCast(self.data);
        return std.mem.bytesAsSlice(i16, aligned);
    }

    pub fn asFloat64(self: *const Tensor) []const f64 {
        const aligned: []align(@alignOf(f64)) const u8 = @alignCast(self.data);
        return std.mem.bytesAsSlice(f64, aligned);
    }

    pub fn elementCount(self: *const Tensor) usize {
        var count: usize = 1;
        for (self.shape) |dim| {
            count *= @intCast(dim);
        }
        return count;
    }

    pub fn deinit(self: *Tensor) void {
        if (self.owns_data) self.allocator.free(self.data);
        if (self.owns_shape) self.allocator.free(self.shape);
    }
};

test "tensor f32 round-trip" {
    const allocator = std.testing.allocator;
    const data = [_]f32{ 1.0, 2.0, 3.0, 4.0 };
    var t = try Tensor.initFloat32(allocator, "test", &.{ 2, 2 }, &data);
    defer t.deinit();

    try std.testing.expectEqual(@as(usize, 4), t.elementCount());
    const slice = t.asFloat32();
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), slice[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 4.0), slice[3], 1e-6);
}

test "tensor scalar dtype sizes" {
    try std.testing.expectEqual(@as(usize, 1), DType.i8.byteSize());
    try std.testing.expectEqual(@as(usize, 2), DType.i16.byteSize());
    try std.testing.expectEqual(@as(usize, 8), DType.f64.byteSize());
}
