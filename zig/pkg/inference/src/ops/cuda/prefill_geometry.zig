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

//! Values-only metadata for the optional recurrent-prefill DSO. No fake
//! packet, tensor, dtype, page address or SDK endpoint enters a cold query.
const std = @import("std");
pub const Geometry = extern struct {
    key_heads: i32,
    value_heads: i32,
    items: i32,
    offsets: [64]i32 = @splat(0),
    lengths: [64]i32 = @splat(0),
    pub const abi: u64 = @as(u64, 1) << 32 | @sizeOf(Geometry);
    pub fn fromLengths(key_heads: usize, value_heads: usize, lengths: []const usize) !Geometry {
        if (key_heads != 16 or (value_heads != 16 and value_heads != 32) or lengths.len == 0 or lengths.len > 64) return error.InvalidTensorShape;
        var value: Geometry = .{ .key_heads = @intCast(key_heads), .value_heads = @intCast(value_heads), .items = @intCast(lengths.len) };
        var rows: usize = 0;
        for (lengths, 0..) |length, index| {
            if (length == 0 or length > 4096 - rows) return error.InvalidTensorShape;
            value.offsets[index] = @intCast(rows);
            value.lengths[index] = @intCast(length);
            rows += length;
        }
        try value.validate();
        return value;
    }
    pub fn fromPacket(packet: anytype) !Geometry {
        var value = Geometry{ .key_heads = std.math.cast(i32, packet.base.key_heads) orelse return error.InvalidTensorShape, .value_heads = std.math.cast(i32, packet.base.value_heads) orelse return error.InvalidTensorShape, .items = std.math.cast(i32, packet.base.items) orelse return error.InvalidTensorShape };
        for (0..64) |index| {
            value.offsets[index] = std.math.cast(i32, packet.offsets[index]) orelse return error.InvalidTensorShape;
            value.lengths[index] = std.math.cast(i32, packet.lengths[index]) orelse return error.InvalidTensorShape;
        }
        try value.validate();
        return value;
    }
    pub fn validate(self: *const Geometry) !void {
        if (self.key_heads != 16 or (self.value_heads != 16 and self.value_heads != 32) or self.items < 1 or self.items > 64) return error.InvalidTensorShape;
        var rows: i32 = 0;
        var prefixes: usize = 0;
        for (0..@intCast(self.items)) |index| {
            const length = self.lengths[index];
            if (self.offsets[index] != rows or length < 1 or length > 4096 - rows) return error.InvalidTensorShape;
            rows += length;
            prefixes += @intFromBool(length > 1);
        }
        if (prefixes > 16) return error.InvalidTensorShape;
    }
};
comptime {
    if (@sizeOf(Geometry) != 524) @compileError("optional prefill metadata ABI changed");
}

test "optional prefill cold geometry carries exact ordered packet metadata without device addresses" {
    const value = try Geometry.fromLengths(16, 32, &.{ 1, 512, 1, 7 });
    try std.testing.expectEqualSlices(i32, &.{ 0, 1, 513, 514 }, value.offsets[0..4]);
    try std.testing.expectEqualSlices(i32, &.{ 1, 512, 1, 7 }, value.lengths[0..4]);
    var bad = value;
    bad.offsets[2] += 1;
    try std.testing.expectError(error.InvalidTensorShape, bad.validate());
    try std.testing.expectError(error.InvalidTensorShape, Geometry.fromLengths(1, 16, &.{2}));
    try std.testing.expectError(error.InvalidTensorShape, Geometry.fromLengths(16, 16, &.{4097}));
    try std.testing.expectError(error.InvalidTensorShape, Geometry.fromLengths(16, 16, &@as([17]usize, @splat(2))));
    _ = try Geometry.fromLengths(16, 16, &@as([64]usize, @splat(1)));
}

test "optional prefill actual packet metadata agrees with cold values without reading page pointers" {
    var packet = @import("gated_delta.zig").RaggedParams{ .base = .{ .key_heads = 16, .value_heads = 16, .items = 2 } };
    packet.offsets[1] = 3;
    packet.lengths[0] = 3;
    packet.lengths[1] = 1;
    const actual = try Geometry.fromPacket(&packet);
    try std.testing.expectEqualDeep(try Geometry.fromLengths(16, 16, &.{ 3, 1 }), actual);
    packet.base.items = std.math.maxInt(u32);
    try std.testing.expectError(error.InvalidTensorShape, Geometry.fromPacket(&packet));
}
