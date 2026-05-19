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

//! Protobuf encoder/decoder for RaBitQ messages.
//!
//! Wire-compatible with antfly/lib/vector/quantize/quantize.proto (edition 2023).
//! Uses the generic comptime encode/decode runtime from lib/protobuf — each
//! struct declares a `_pb_field_map` and delegates to `message.encode` /
//! `message.decode`. Domain helpers (`at`, `getCount`, `clone`) stay local.
//!
//! Note: `RaBitQCodeSet.data` is declared as `repeated uint64` in the proto
//! but encoded as packed fixed64 here for performance (matches the Go
//! implementation in antfly/lib/vector/quantize).

const std = @import("std");
const Allocator = std.mem.Allocator;
const vector = @import("vector.zig");
const protobuf = @import("protobuf");
const message = protobuf.message;

const FieldDesc = message.FieldDesc;

/// RaBitQCodeSet - wire-compatible with quantize.proto field numbers.
///   field 1: count (int64, varint)
///   field 2: width (int64, varint)
///   field 3: data (repeated uint64, packed fixed64)
pub const RaBitQCodeSet = struct {
    count: i64 = 0,
    width: i64 = 0,
    data: []u64 = &.{},

    pub const _pb_field_map = [_]FieldDesc{
        .{ .field_num = 1, .name = "count", .encoding = .varint },
        .{ .field_num = 2, .name = "width", .encoding = .varint },
        .{ .field_num = 3, .name = "data", .encoding = .repeated_fixed64 },
    };

    pub fn at(self: *const RaBitQCodeSet, offset: usize) []u64 {
        const w: usize = @intCast(self.width);
        const start = offset * w;
        return @constCast(self.data[start .. start + w]);
    }

    pub fn atConst(self: *const RaBitQCodeSet, offset: usize) []const u64 {
        const w: usize = @intCast(self.width);
        const start = offset * w;
        return self.data[start .. start + w];
    }

    pub fn encode(self: *const RaBitQCodeSet, alloc: Allocator) ![]u8 {
        return message.encode(RaBitQCodeSet, alloc, self);
    }

    pub fn encodedLen(self: *const RaBitQCodeSet) usize {
        return message.encodedLen(RaBitQCodeSet, self);
    }

    pub fn encodeInto(self: *const RaBitQCodeSet, alloc: Allocator, buf: *protobuf.wire.Buf) !void {
        return message.encodeInto(RaBitQCodeSet, alloc, buf, self);
    }

    pub fn decode(alloc: Allocator, bytes: []const u8) !RaBitQCodeSet {
        return message.decode(RaBitQCodeSet, alloc, bytes);
    }

    pub fn clone(self: *const RaBitQCodeSet, alloc: Allocator) !RaBitQCodeSet {
        return .{
            .count = self.count,
            .width = self.width,
            .data = try alloc.dupe(u64, self.data),
        };
    }

    pub fn deinit(self: *RaBitQCodeSet, alloc: Allocator) void {
        message.deinit(RaBitQCodeSet, alloc, self);
        self.* = .{};
    }
};

pub const VectorSet = struct {
    dims: i64 = 0,
    count: i64 = 0,
    data: []f32 = &.{},

    pub const _pb_field_map = [_]FieldDesc{
        .{ .field_num = 1, .name = "dims", .encoding = .varint },
        .{ .field_num = 2, .name = "count", .encoding = .varint },
        .{ .field_num = 3, .name = "data", .encoding = .repeated_fixed32 },
    };

    pub fn encode(self: *const VectorSet, alloc: Allocator) ![]u8 {
        return message.encode(VectorSet, alloc, self);
    }

    pub fn encodedLen(self: *const VectorSet) usize {
        return message.encodedLen(VectorSet, self);
    }

    pub fn encodeInto(self: *const VectorSet, alloc: Allocator, buf: *protobuf.wire.Buf) !void {
        return message.encodeInto(VectorSet, alloc, buf, self);
    }

    pub fn decode(alloc: Allocator, bytes: []const u8) !VectorSet {
        return message.decode(VectorSet, alloc, bytes);
    }

    pub fn clone(self: *const VectorSet, alloc: Allocator) !VectorSet {
        return .{
            .dims = self.dims,
            .count = self.count,
            .data = try alloc.dupe(f32, self.data),
        };
    }

    pub fn deinit(self: *VectorSet, alloc: Allocator) void {
        message.deinit(VectorSet, alloc, self);
        self.* = .{};
    }
};

/// RaBitQuantizedVectorSet - wire-compatible with quantize.proto.
pub const RaBitQuantizedVectorSet = struct {
    metric: vector.DistanceMetric = .l2_squared,
    centroid: []f32 = &.{},
    codes: RaBitQCodeSet = .{},
    code_counts: []u32 = &.{},
    centroid_distances: []f32 = &.{},
    quantized_dot_products: []f32 = &.{},
    centroid_dot_products: []f32 = &.{},
    centroid_norm: f32 = 0,

    pub const _pb_field_map = [_]FieldDesc{
        .{ .field_num = 1, .name = "metric", .encoding = .varint },
        .{ .field_num = 2, .name = "centroid", .encoding = .repeated_fixed32 },
        .{ .field_num = 3, .name = "codes", .encoding = .submessage },
        .{ .field_num = 4, .name = "code_counts", .encoding = .repeated_varint },
        .{ .field_num = 5, .name = "centroid_distances", .encoding = .repeated_fixed32 },
        .{ .field_num = 6, .name = "quantized_dot_products", .encoding = .repeated_fixed32 },
        .{ .field_num = 7, .name = "centroid_dot_products", .encoding = .repeated_fixed32 },
        .{ .field_num = 8, .name = "centroid_norm", .encoding = .fixed32 },
    };

    pub fn getCount(self: *const RaBitQuantizedVectorSet) usize {
        return self.code_counts.len;
    }

    pub fn encode(self: *const RaBitQuantizedVectorSet, alloc: Allocator) ![]u8 {
        return message.encode(RaBitQuantizedVectorSet, alloc, self);
    }

    pub fn encodedLen(self: *const RaBitQuantizedVectorSet) usize {
        return message.encodedLen(RaBitQuantizedVectorSet, self);
    }

    pub fn encodeInto(self: *const RaBitQuantizedVectorSet, alloc: Allocator, buf: *protobuf.wire.Buf) !void {
        return message.encodeInto(RaBitQuantizedVectorSet, alloc, buf, self);
    }

    pub fn decode(alloc: Allocator, bytes: []const u8) !RaBitQuantizedVectorSet {
        return message.decode(RaBitQuantizedVectorSet, alloc, bytes);
    }

    pub fn clone(self: *const RaBitQuantizedVectorSet, alloc: Allocator) !RaBitQuantizedVectorSet {
        return .{
            .metric = self.metric,
            .centroid = try alloc.dupe(f32, self.centroid),
            .codes = try self.codes.clone(alloc),
            .code_counts = try alloc.dupe(u32, self.code_counts),
            .centroid_distances = try alloc.dupe(f32, self.centroid_distances),
            .quantized_dot_products = try alloc.dupe(f32, self.quantized_dot_products),
            .centroid_dot_products = try alloc.dupe(f32, self.centroid_dot_products),
            .centroid_norm = self.centroid_norm,
        };
    }

    pub fn deinit(self: *RaBitQuantizedVectorSet, alloc: Allocator) void {
        message.deinit(RaBitQuantizedVectorSet, alloc, self);
        self.* = .{};
    }
};

pub const NonQuantizedVectorSet = struct {
    vectors: VectorSet = .{},

    pub const _pb_field_map = [_]FieldDesc{
        .{ .field_num = 1, .name = "vectors", .encoding = .submessage },
    };

    pub fn getCount(self: *const NonQuantizedVectorSet) usize {
        return @intCast(self.vectors.count);
    }

    pub fn encode(self: *const NonQuantizedVectorSet, alloc: Allocator) ![]u8 {
        return message.encode(NonQuantizedVectorSet, alloc, self);
    }

    pub fn encodedLen(self: *const NonQuantizedVectorSet) usize {
        return message.encodedLen(NonQuantizedVectorSet, self);
    }

    pub fn encodeInto(self: *const NonQuantizedVectorSet, alloc: Allocator, buf: *protobuf.wire.Buf) !void {
        return message.encodeInto(NonQuantizedVectorSet, alloc, buf, self);
    }

    pub fn decode(alloc: Allocator, bytes: []const u8) !NonQuantizedVectorSet {
        return message.decode(NonQuantizedVectorSet, alloc, bytes);
    }

    pub fn clone(self: *const NonQuantizedVectorSet, alloc: Allocator) !NonQuantizedVectorSet {
        return .{
            .vectors = try self.vectors.clone(alloc),
        };
    }

    pub fn deinit(self: *NonQuantizedVectorSet, alloc: Allocator) void {
        message.deinit(NonQuantizedVectorSet, alloc, self);
        self.* = .{};
    }
};

// --- Tests ---

test "RaBitQCodeSet roundtrip" {
    const alloc = std.testing.allocator;

    var original = RaBitQCodeSet{
        .count = 2,
        .width = 1,
        .data = @constCast(&[_]u64{ 0xDEADBEEF, 0xCAFEBABE }),
    };

    const encoded = try original.encode(alloc);
    defer alloc.free(encoded);

    var decoded = try RaBitQCodeSet.decode(alloc, encoded);
    defer decoded.deinit(alloc);

    try std.testing.expectEqual(@as(i64, 2), decoded.count);
    try std.testing.expectEqual(@as(i64, 1), decoded.width);
    try std.testing.expectEqual(@as(usize, 2), decoded.data.len);
    try std.testing.expectEqual(@as(u64, 0xDEADBEEF), decoded.data[0]);
    try std.testing.expectEqual(@as(u64, 0xCAFEBABE), decoded.data[1]);
}

test "RaBitQuantizedVectorSet roundtrip" {
    const alloc = std.testing.allocator;

    const codes = RaBitQCodeSet{
        .count = 1,
        .width = 1,
        .data = @constCast(&[_]u64{0xFF}),
    };

    var original = RaBitQuantizedVectorSet{
        .metric = .cosine,
        .centroid = @constCast(&[_]f32{ 1.0, 2.0, 3.0 }),
        .codes = codes,
        .code_counts = @constCast(&[_]u32{8}),
        .centroid_distances = @constCast(&[_]f32{1.5}),
        .quantized_dot_products = @constCast(&[_]f32{0.9}),
        .centroid_dot_products = @constCast(&[_]f32{0.8}),
        .centroid_norm = 3.7416,
    };

    const encoded = try original.encode(alloc);
    defer alloc.free(encoded);

    var decoded = try RaBitQuantizedVectorSet.decode(alloc, encoded);
    defer decoded.deinit(alloc);

    try std.testing.expectEqual(vector.DistanceMetric.cosine, decoded.metric);
    try std.testing.expectEqual(@as(usize, 3), decoded.centroid.len);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), decoded.centroid[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 2.0), decoded.centroid[1], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 3.0), decoded.centroid[2], 1e-6);
    try std.testing.expectEqual(@as(i64, 1), decoded.codes.count);
    try std.testing.expectEqual(@as(u64, 0xFF), decoded.codes.data[0]);
    try std.testing.expectEqual(@as(u32, 8), decoded.code_counts[0]);
    try std.testing.expectApproxEqAbs(@as(f32, 1.5), decoded.centroid_distances[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.9), decoded.quantized_dot_products[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0.8), decoded.centroid_dot_products[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 3.7416), decoded.centroid_norm, 1e-4);
}

test "NonQuantizedVectorSet roundtrip" {
    const alloc = std.testing.allocator;

    var original = NonQuantizedVectorSet{
        .vectors = .{
            .dims = 2,
            .count = 3,
            .data = try alloc.dupe(f32, &[_]f32{
                1.0, 2.0,
                3.0, 4.0,
                5.0, 6.0,
            }),
        },
    };
    defer original.deinit(alloc);

    const encoded = try original.encode(alloc);
    defer alloc.free(encoded);

    var decoded = try NonQuantizedVectorSet.decode(alloc, encoded);
    defer decoded.deinit(alloc);

    try std.testing.expectEqual(@as(i64, 2), decoded.vectors.dims);
    try std.testing.expectEqual(@as(i64, 3), decoded.vectors.count);
    try std.testing.expectEqualSlices(f32, original.vectors.data, decoded.vectors.data);
}

test "NonQuantizedVectorSet roundtrip with 64KiB packed f32 payload" {
    const alloc = std.testing.allocator;
    const dims: usize = 128;
    const count: usize = 128;
    const data = try alloc.alloc(f32, dims * count);
    defer alloc.free(data);
    for (data, 0..) |*value, i| value.* = @floatFromInt(i);

    var original = NonQuantizedVectorSet{
        .vectors = .{
            .dims = @intCast(dims),
            .count = @intCast(count),
            .data = data,
        },
    };
    const encoded = try original.encode(alloc);
    defer alloc.free(encoded);

    var decoded = try NonQuantizedVectorSet.decode(alloc, encoded);
    defer decoded.deinit(alloc);

    try std.testing.expectEqual(@as(i64, @intCast(dims)), decoded.vectors.dims);
    try std.testing.expectEqual(@as(i64, @intCast(count)), decoded.vectors.count);
    try std.testing.expectEqualSlices(f32, data, decoded.vectors.data);
}

test "RaBitQuantizedVectorSet roundtrip with HBC internal node shape" {
    const alloc = std.testing.allocator;
    const dims: usize = 128;
    const count: usize = 79;
    const width: usize = 2;

    const centroid = try alloc.alloc(f32, dims);
    defer alloc.free(centroid);
    for (centroid, 0..) |*value, i| value.* = @floatFromInt(i);
    const code_data = try alloc.alloc(u64, count * width);
    defer alloc.free(code_data);
    for (code_data, 0..) |*value, i| value.* = i;
    const code_counts = try alloc.alloc(u32, count);
    defer alloc.free(code_counts);
    const centroid_distances = try alloc.alloc(f32, count);
    defer alloc.free(centroid_distances);
    const quantized_dot_products = try alloc.alloc(f32, count);
    defer alloc.free(quantized_dot_products);
    const centroid_dot_products = try alloc.alloc(f32, count);
    defer alloc.free(centroid_dot_products);
    for (0..count) |i| {
        code_counts[i] = @intCast(i % 16);
        centroid_distances[i] = @floatFromInt(i);
        quantized_dot_products[i] = @floatFromInt(i + 1);
        centroid_dot_products[i] = @floatFromInt(i + 2);
    }

    var original = RaBitQuantizedVectorSet{
        .metric = .cosine,
        .centroid = centroid,
        .codes = .{
            .count = @intCast(count),
            .width = @intCast(width),
            .data = code_data,
        },
        .code_counts = code_counts,
        .centroid_distances = centroid_distances,
        .quantized_dot_products = quantized_dot_products,
        .centroid_dot_products = centroid_dot_products,
        .centroid_norm = 1,
    };
    const encoded = try original.encode(alloc);
    defer alloc.free(encoded);

    var decoded = try RaBitQuantizedVectorSet.decode(alloc, encoded);
    defer decoded.deinit(alloc);

    try std.testing.expectEqual(vector.DistanceMetric.cosine, decoded.metric);
    try std.testing.expectEqualSlices(f32, centroid, decoded.centroid);
    try std.testing.expectEqual(@as(i64, @intCast(count)), decoded.codes.count);
    try std.testing.expectEqual(@as(i64, @intCast(width)), decoded.codes.width);
    try std.testing.expectEqualSlices(u64, code_data, decoded.codes.data);
    try std.testing.expectEqualSlices(u32, code_counts, decoded.code_counts);
    try std.testing.expectEqualSlices(f32, centroid_distances, decoded.centroid_distances);
    try std.testing.expectEqualSlices(f32, quantized_dot_products, decoded.quantized_dot_products);
    try std.testing.expectEqualSlices(f32, centroid_dot_products, decoded.centroid_dot_products);
}
