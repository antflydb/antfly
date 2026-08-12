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

/// Optional ownership hook for resources whose lifetime must cover the tensor.
/// Session output admission uses one shared ref-counted hook across a returned
/// tensor batch, without coupling this low-level type to the memory controller.
pub const Lifetime = struct {
    context: *anyopaque,
    release: *const fn (*anyopaque) void,
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
    /// When set, `data` is a slice inside this stable mmap-backed byte range.
    mmap_source_bytes: ?[]const u8 = null,
    lifetime: ?Lifetime = null,

    fn initOwned(
        comptime T: type,
        allocator: std.mem.Allocator,
        name: []const u8,
        shape: []const i64,
        data: []const T,
        dtype: DType,
    ) !Tensor {
        const owned_bytes = try allocator.dupe(u8, std.mem.sliceAsBytes(data));
        errdefer allocator.free(owned_bytes);
        const owned_shape = try allocator.dupe(i64, shape);
        return .{
            .data = owned_bytes,
            .dtype = dtype,
            .shape = owned_shape,
            .name = name,
            .allocator = allocator,
            .owns_data = true,
            .owns_shape = true,
        };
    }

    pub fn initFloat32(allocator: std.mem.Allocator, name: []const u8, shape: []const i64, data: []const f32) !Tensor {
        return initOwned(f32, allocator, name, shape, data, .f32);
    }

    pub fn initInt64(allocator: std.mem.Allocator, name: []const u8, shape: []const i64, data: []const i64) !Tensor {
        return initOwned(i64, allocator, name, shape, data, .i64);
    }

    pub fn initInt8(allocator: std.mem.Allocator, name: []const u8, shape: []const i64, data: []const i8) !Tensor {
        return initOwned(i8, allocator, name, shape, data, .i8);
    }

    pub fn initInt16(allocator: std.mem.Allocator, name: []const u8, shape: []const i64, data: []const i16) !Tensor {
        return initOwned(i16, allocator, name, shape, data, .i16);
    }

    pub fn initFloat64(allocator: std.mem.Allocator, name: []const u8, shape: []const i64, data: []const f64) !Tensor {
        return initOwned(f64, allocator, name, shape, data, .f64);
    }

    pub fn initBool(allocator: std.mem.Allocator, name: []const u8, shape: []const i64, data: []const u8) !Tensor {
        return initOwned(u8, allocator, name, shape, data, .bool_);
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

    /// Return a non-owning view suitable for passing an existing tensor under
    /// a different input name. The source tensor remains responsible for its
    /// buffers and any attached lifetime hook; deinitializing the view is a
    /// no-op. Shallow-copying a Tensor is not sufficient because it would copy
    /// the lifetime hook without retaining it and can therefore release a
    /// shared admission lease more than once.
    pub fn borrowedView(self: *const Tensor, name: []const u8) Tensor {
        var view = self.*;
        view.name = name;
        view.owns_data = false;
        view.owns_shape = false;
        view.lifetime = null;
        return view;
    }

    pub fn deinit(self: *Tensor) void {
        if (self.owns_data) self.allocator.free(self.data);
        if (self.owns_shape) self.allocator.free(self.shape);
        if (self.lifetime) |lifetime| {
            self.lifetime = null;
            lifetime.release(lifetime.context);
        }
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

test "tensor constructor frees copied data when shape allocation fails" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 1 });
    const data = [_]f32{ 1.0, 2.0 };

    try std.testing.expectError(
        error.OutOfMemory,
        Tensor.initFloat32(failing.allocator(), "test", &.{2}, &data),
    );
    try std.testing.expect(failing.has_induced_failure);
    try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
}

test "tensor scalar dtype sizes" {
    try std.testing.expectEqual(@as(usize, 1), DType.i8.byteSize());
    try std.testing.expectEqual(@as(usize, 2), DType.i16.byteSize());
    try std.testing.expectEqual(@as(usize, 8), DType.f64.byteSize());
}

test "borrowed tensor view does not release source lifetime" {
    const ReleaseCounter = struct {
        count: usize = 0,

        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.count += 1;
        }
    };

    var counter = ReleaseCounter{};
    var source = try Tensor.initFloat32(std.testing.allocator, "source", &.{1}, &.{1.0});
    source.lifetime = .{ .context = &counter, .release = ReleaseCounter.release };

    var view = source.borrowedView("renamed");
    try std.testing.expectEqualStrings("renamed", view.name);
    try std.testing.expect(!view.owns_data);
    try std.testing.expect(!view.owns_shape);
    try std.testing.expect(view.lifetime == null);

    view.deinit();
    try std.testing.expectEqual(@as(usize, 0), counter.count);
    source.deinit();
    try std.testing.expectEqual(@as(usize, 1), counter.count);
}
