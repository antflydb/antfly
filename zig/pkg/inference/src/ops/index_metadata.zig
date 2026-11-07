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

//! Exact, non-owning index/control metadata. Integer payloads never pass
//! through floating point. Legacy F32 indices preserve truncation toward zero,
//! checked conversion and one negative-axis wrap before bounds validation.
const std = @import("std");
const DType = @import("../backends/tensor.zig").DType;

pub const Reader = struct {
    dtype: DType,
    bytes: []const u8,
    len: usize,

    pub fn init(dtype: DType, bytes: []const u8) !Reader {
        switch (dtype) {
            .f32, .i8, .i16, .u8, .bool_, .i32, .i64 => {},
            else => return error.UnsupportedTensorType,
        }
        const width = dtype.byteSize();
        if (bytes.len % width != 0) return error.InvalidTensorShape;
        return .{ .dtype = dtype, .bytes = bytes, .len = bytes.len / width };
    }
    pub fn at(self: Reader, index: usize) !i64 {
        if (index >= self.len) return error.IndexOutOfBounds;
        return switch (self.dtype) {
            .i8 => std.mem.bytesAsSlice(i8, self.bytes)[index],
            .i16 => std.mem.bytesAsSlice(i16, self.bytes)[index],
            .u8, .bool_ => self.bytes[index],
            .i32 => std.mem.bytesAsSlice(i32, self.bytes)[index],
            .i64 => std.mem.bytesAsSlice(i64, self.bytes)[index],
            .f32 => blk: {
                const value: f64 = std.mem.bytesAsSlice(f32, self.bytes)[index];
                if (!std.math.isFinite(value) or value < -9223372036854775808.0 or value >= 9223372036854775808.0)
                    return error.InvalidIndex;
                break :blk @intFromFloat(value);
            },
            else => unreachable,
        };
    }
    pub fn normalized(self: Reader, index: usize, extent: usize) !usize {
        const signed_extent = std.math.cast(i64, extent) orelse return error.InvalidTensorShape;
        var value = try self.at(index);
        if (value < 0) value += signed_extent;
        if (value < 0 or value >= signed_extent) return error.IndexOutOfBounds;
        return @intCast(value);
    }
    pub fn validate(self: Reader, extent: usize) !void {
        for (0..self.len) |index| _ = try self.normalized(index, extent);
    }
};

test "index metadata preserves unaligned full-width integers and legacy float errors" {
    const values = [_]i64{ 16_777_217, -1, std.math.minInt(i64), std.math.maxInt(i64) };
    var unaligned: [1 + @sizeOf(@TypeOf(values))]u8 = undefined;
    @memcpy(unaligned[1..], std.mem.sliceAsBytes(&values));
    const exact = try Reader.init(.i64, unaligned[1..]);
    try std.testing.expectEqual(@as(i64, 16_777_217), try exact.at(0));
    try std.testing.expectEqual(@as(usize, 16_777_217), try exact.normalized(0, 16_777_218));
    try std.testing.expectEqual(@as(usize, 4), try exact.normalized(1, 5));
    try std.testing.expectError(error.IndexOutOfBounds, exact.normalized(2, 5));
    try std.testing.expectError(error.IndexOutOfBounds, exact.normalized(3, 5));
    const floats = [_]f32{ 1.9, -1.9, std.math.inf(f32), std.math.nan(f32), 9223372036854775808.0 };
    const legacy = try Reader.init(.f32, std.mem.sliceAsBytes(&floats));
    try std.testing.expectEqual(@as(i64, 1), try legacy.at(0));
    try std.testing.expectEqual(@as(usize, 4), try legacy.normalized(1, 5));
    for (2..floats.len) |index| try std.testing.expectError(error.InvalidIndex, legacy.at(index));
    try std.testing.expectError(error.InvalidTensorShape, Reader.init(.i64, unaligned[0..3]));
}
