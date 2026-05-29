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

pub const KnownTensorType = enum(u32) {
    F32 = 0,
    F16 = 1,
    Q4_0 = 2,
    Q4_1 = 3,
    Q5_0 = 6,
    Q5_1 = 7,
    Q8_0 = 8,
    Q8_1 = 9,
    Q2_K = 10,
    Q3_K = 11,
    Q4_K = 12,
    Q5_K = 13,
    Q6_K = 14,
    Q8_K = 15,
    IQ2_XXS = 16,
    IQ2_XS = 17,
    IQ3_XXS = 18,
    IQ1_S = 19,
    IQ4_NL = 20,
    IQ3_S = 21,
    IQ2_S = 22,
    IQ4_XS = 23,
    I8 = 24,
    I16 = 25,
    I32 = 26,
    I64 = 27,
    F64 = 28,
    IQ1_M = 29,
    BF16 = 30,
    TQ1_0 = 34,
    TQ2_0 = 35,
    I2_S = 36,
    I8_S = 37,
    TL1 = 38,
    MXFP4 = 39,
    NVFP4 = 40,
    Q1_0 = 41,
};

pub const TensorType = union(enum) {
    known: KnownTensorType,
    bitnet_tl2,
    unknown: u32,

    pub const Dialect = enum {
        ggml_org,
        bitnet,
    };

    pub fn fromRaw(raw_value: u32) TensorType {
        return fromRawForDialect(raw_value, .ggml_org);
    }

    pub fn fromRawForDialect(raw_value: u32, dialect: Dialect) TensorType {
        if (dialect == .bitnet and raw_value == 39) return .bitnet_tl2;
        return switch (raw_value) {
            @intFromEnum(KnownTensorType.F32) => .{ .known = .F32 },
            @intFromEnum(KnownTensorType.F16) => .{ .known = .F16 },
            @intFromEnum(KnownTensorType.Q4_0) => .{ .known = .Q4_0 },
            @intFromEnum(KnownTensorType.Q4_1) => .{ .known = .Q4_1 },
            @intFromEnum(KnownTensorType.Q5_0) => .{ .known = .Q5_0 },
            @intFromEnum(KnownTensorType.Q5_1) => .{ .known = .Q5_1 },
            @intFromEnum(KnownTensorType.Q8_0) => .{ .known = .Q8_0 },
            @intFromEnum(KnownTensorType.Q8_1) => .{ .known = .Q8_1 },
            @intFromEnum(KnownTensorType.Q2_K) => .{ .known = .Q2_K },
            @intFromEnum(KnownTensorType.Q3_K) => .{ .known = .Q3_K },
            @intFromEnum(KnownTensorType.Q4_K) => .{ .known = .Q4_K },
            @intFromEnum(KnownTensorType.Q5_K) => .{ .known = .Q5_K },
            @intFromEnum(KnownTensorType.Q6_K) => .{ .known = .Q6_K },
            @intFromEnum(KnownTensorType.Q8_K) => .{ .known = .Q8_K },
            @intFromEnum(KnownTensorType.IQ2_XXS) => .{ .known = .IQ2_XXS },
            @intFromEnum(KnownTensorType.IQ2_XS) => .{ .known = .IQ2_XS },
            @intFromEnum(KnownTensorType.IQ3_XXS) => .{ .known = .IQ3_XXS },
            @intFromEnum(KnownTensorType.IQ1_S) => .{ .known = .IQ1_S },
            @intFromEnum(KnownTensorType.IQ4_NL) => .{ .known = .IQ4_NL },
            @intFromEnum(KnownTensorType.IQ3_S) => .{ .known = .IQ3_S },
            @intFromEnum(KnownTensorType.IQ2_S) => .{ .known = .IQ2_S },
            @intFromEnum(KnownTensorType.IQ4_XS) => .{ .known = .IQ4_XS },
            @intFromEnum(KnownTensorType.I8) => .{ .known = .I8 },
            @intFromEnum(KnownTensorType.I16) => .{ .known = .I16 },
            @intFromEnum(KnownTensorType.I32) => .{ .known = .I32 },
            @intFromEnum(KnownTensorType.I64) => .{ .known = .I64 },
            @intFromEnum(KnownTensorType.F64) => .{ .known = .F64 },
            @intFromEnum(KnownTensorType.IQ1_M) => .{ .known = .IQ1_M },
            @intFromEnum(KnownTensorType.BF16) => .{ .known = .BF16 },
            @intFromEnum(KnownTensorType.TQ1_0) => .{ .known = .TQ1_0 },
            @intFromEnum(KnownTensorType.TQ2_0) => .{ .known = .TQ2_0 },
            @intFromEnum(KnownTensorType.I2_S) => .{ .known = .I2_S },
            @intFromEnum(KnownTensorType.I8_S) => .{ .known = .I8_S },
            @intFromEnum(KnownTensorType.TL1) => .{ .known = .TL1 },
            @intFromEnum(KnownTensorType.MXFP4) => .{ .known = .MXFP4 },
            @intFromEnum(KnownTensorType.NVFP4) => .{ .known = .NVFP4 },
            @intFromEnum(KnownTensorType.Q1_0) => .{ .known = .Q1_0 },
            else => .{ .unknown = raw_value },
        };
    }

    pub fn raw(self: TensorType) u32 {
        return switch (self) {
            .known => |known| @intFromEnum(known),
            .bitnet_tl2 => 39,
            .unknown => |raw_value| raw_value,
        };
    }

    pub fn name(self: TensorType) []const u8 {
        return switch (self) {
            .known => |known| @tagName(known),
            .bitnet_tl2 => "TL2",
            .unknown => "UNKNOWN",
        };
    }

    pub fn eql(a: TensorType, b: TensorType) bool {
        return switch (a) {
            .known => |known_a| switch (b) {
                .known => |known_b| known_a == known_b,
                else => false,
            },
            .bitnet_tl2 => switch (b) {
                .bitnet_tl2 => true,
                else => false,
            },
            .unknown => |raw_a| switch (b) {
                .unknown => |raw_b| raw_a == raw_b,
                else => false,
            },
        };
    }

    pub fn isQuantized(self: TensorType) bool {
        return switch (self) {
            .known => |known| switch (known) {
                .F32, .F16, .BF16, .I8, .I16, .I32, .I64, .F64 => false,
                else => true,
            },
            .bitnet_tl2 => true,
            .unknown => true,
        };
    }
};

pub fn valuesPerBlock(tensor_type: TensorType) ?u32 {
    return switch (tensor_type) {
        .known => |known| switch (known) {
            .F32, .F16, .BF16, .I8, .I16, .I32, .I64, .F64, .Q4_0, .Q4_1, .Q5_0, .Q5_1, .Q8_0, .Q8_1 => 32,
            .Q1_0 => 128,
            .I2_S => 128,
            .I8_S, .TL1 => 1,
            .Q2_K, .Q3_K, .Q4_K, .Q5_K, .Q6_K, .Q8_K, .TQ1_0, .TQ2_0 => 256,
            .IQ4_NL => 32,
            .IQ2_XXS, .IQ2_XS, .IQ3_XXS, .IQ1_S, .IQ3_S, .IQ2_S, .IQ4_XS, .IQ1_M => 256,
            .MXFP4 => 32,
            .NVFP4 => 64,
        },
        .bitnet_tl2 => null,
        .unknown => null,
    };
}

pub fn bytesPerBlock(tensor_type: TensorType) ?u32 {
    return switch (tensor_type) {
        .known => |known| switch (known) {
            .F32 => 128,
            .F16, .BF16 => 64,
            .I8 => 32,
            .I16 => 64,
            .I32 => 128,
            .I64, .F64 => 256,
            .Q4_0 => 18,
            .Q4_1 => 20,
            .Q1_0 => 18,
            .I2_S => 32,
            .I8_S, .TL1 => 1,
            .Q5_0 => 22,
            .Q5_1 => 24,
            .Q8_0 => 34,
            .Q8_1 => 36,
            .Q2_K => 84,
            .Q3_K => 110,
            .Q4_K => 144,
            .Q5_K => 176,
            .Q6_K => 210,
            .Q8_K => 292,
            .IQ2_XXS => 66,
            .IQ2_XS => 74,
            .IQ3_XXS => 98,
            .IQ1_S => 50,
            .IQ4_NL => 18,
            .IQ3_S => 110,
            .IQ2_S => 82,
            .IQ4_XS => 136,
            .IQ1_M => 56,
            .TQ1_0 => 54,
            .TQ2_0 => 66,
            .MXFP4 => 17,
            .NVFP4 => 36,
        },
        .bitnet_tl2 => null,
        .unknown => null,
    };
}

pub fn elementCount(dimensions: []const u64) ?u64 {
    var total: u64 = 1;
    for (dimensions) |dim| {
        total = std.math.mul(u64, total, dim) catch return null;
    }
    return total;
}

pub fn elementCountI64(dimensions: []const i64) ?u64 {
    var total: u64 = 1;
    for (dimensions) |dim| {
        if (dim < 0) return null;
        total = std.math.mul(u64, total, @intCast(dim)) catch return null;
    }
    return total;
}

pub fn byteLen(tensor_type: TensorType, dimensions: []const u64) ?u64 {
    const total = elementCount(dimensions) orelse return null;
    return switch (tensor_type) {
        .known => |known| switch (known) {
            .F32 => std.math.mul(u64, total, @sizeOf(f32)) catch null,
            .F16, .BF16 => std.math.mul(u64, total, 2) catch null,
            .I8 => total,
            .I16 => std.math.mul(u64, total, 2) catch null,
            .I32 => std.math.mul(u64, total, 4) catch null,
            .I64, .F64 => std.math.mul(u64, total, 8) catch null,
            .I2_S, .TL1 => (std.math.divCeil(u64, total, 4) catch return null) + 32,
            .I8_S => total,
            else => blk: {
                const values_per_block = valuesPerBlock(tensor_type) orelse break :blk null;
                const bytes_per_block = bytesPerBlock(tensor_type) orelse break :blk null;
                const blocks = std.math.divCeil(u64, total, values_per_block) catch return null;
                break :blk std.math.mul(u64, blocks, bytes_per_block) catch null;
            },
        },
        .bitnet_tl2 => byteLenBitnetTL2(dimensions),
        .unknown => null,
    };
}

fn byteLenBitnetTL2(dimensions: []const u64) ?u64 {
    if (dimensions.len < 2) return null;
    const cols = dimensions[0];
    if (cols < 256) return null;
    var rows: u64 = 1;
    for (dimensions[1..]) |dim| {
        rows = std.math.mul(u64, rows, dim) catch return null;
    }

    const ternary_cols = cols - 256;
    const ternary_bytes = (((std.math.mul(u64, ternary_cols, rows) catch return null) / 3) * 5) / 8;
    const dense_prefix_bytes = (((std.math.mul(u64, 256, rows) catch return null) / 2) * 4) / 8;
    const unaligned = std.math.add(u64, ternary_bytes, dense_prefix_bytes) catch return null;
    const aligned = std.mem.alignForward(u64, unaligned, 32);
    return std.math.add(u64, aligned, 32) catch null;
}

test "tensor type round trip" {
    const q5k = TensorType.fromRaw(13);
    try std.testing.expectEqualStrings("Q5_K", q5k.name());
    try std.testing.expectEqual(@as(u32, 13), q5k.raw());
    try std.testing.expect(q5k.isQuantized());

    const unknown = TensorType.fromRaw(999);
    try std.testing.expectEqualStrings("UNKNOWN", unknown.name());
    try std.testing.expectEqual(@as(u32, 999), unknown.raw());
    try std.testing.expect(valuesPerBlock(unknown) == null);

    const nvfp4 = TensorType.fromRaw(40);
    try std.testing.expectEqualStrings("NVFP4", nvfp4.name());
    try std.testing.expect(nvfp4.isQuantized());

    const int64_type = TensorType.fromRaw(27);
    try std.testing.expectEqualStrings("I64", int64_type.name());
    try std.testing.expect(!int64_type.isQuantized());

    const bitnet_i2 = TensorType.fromRaw(36);
    try std.testing.expectEqualStrings("I2_S", bitnet_i2.name());
    try std.testing.expect(bitnet_i2.isQuantized());

    const bitnet_tl1 = TensorType.fromRaw(38);
    try std.testing.expectEqualStrings("TL1", bitnet_tl1.name());
    try std.testing.expect(bitnet_tl1.isQuantized());

    const upstream_39 = TensorType.fromRaw(39);
    try std.testing.expectEqualStrings("MXFP4", upstream_39.name());

    const bitnet_39 = TensorType.fromRawForDialect(39, .bitnet);
    try std.testing.expectEqualStrings("TL2", bitnet_39.name());
    try std.testing.expectEqual(@as(u32, 39), bitnet_39.raw());
    try std.testing.expect(bitnet_39.isQuantized());
}

test "tensor byte length for dense and k-quants" {
    try std.testing.expectEqual(@as(?u64, 16), byteLen(.{ .known = .F32 }, &.{4}));
    try std.testing.expectEqual(@as(?u64, 8), byteLen(.{ .known = .F16 }, &.{4}));
    try std.testing.expectEqual(@as(?u64, 4), byteLen(.{ .known = .I8 }, &.{4}));
    try std.testing.expectEqual(@as(?u64, 8), byteLen(.{ .known = .I16 }, &.{4}));
    try std.testing.expectEqual(@as(?u64, 16), byteLen(.{ .known = .I32 }, &.{4}));
    try std.testing.expectEqual(@as(?u64, 32), byteLen(.{ .known = .I64 }, &.{4}));
    try std.testing.expectEqual(@as(?u64, 32), byteLen(.{ .known = .F64 }, &.{4}));
    try std.testing.expectEqual(@as(?u64, 18), byteLen(.{ .known = .Q1_0 }, &.{128}));
    try std.testing.expectEqual(@as(?u64, 96), byteLen(.{ .known = .I2_S }, &.{256}));
    try std.testing.expectEqual(@as(?u64, 256), byteLen(.{ .known = .I8_S }, &.{256}));
    try std.testing.expectEqual(@as(?u64, 96), byteLen(.{ .known = .TL1 }, &.{256}));
    try std.testing.expectEqual(@as(?u64, 5856), byteLen(.bitnet_tl2, &.{ 6912, 4 }));
    try std.testing.expectEqual(@as(?u64, 176), byteLen(.{ .known = .Q5_K }, &.{256}));
    try std.testing.expectEqual(@as(?u64, 18), byteLen(.{ .known = .IQ4_NL }, &.{32}));
    try std.testing.expectEqual(@as(?u64, 136), byteLen(.{ .known = .IQ4_XS }, &.{256}));
    try std.testing.expectEqual(@as(?u64, 66), byteLen(.{ .known = .IQ2_XXS }, &.{256}));
    try std.testing.expectEqual(@as(?u64, 74), byteLen(.{ .known = .IQ2_XS }, &.{256}));
    try std.testing.expectEqual(@as(?u64, 98), byteLen(.{ .known = .IQ3_XXS }, &.{256}));
    try std.testing.expectEqual(@as(?u64, 50), byteLen(.{ .known = .IQ1_S }, &.{256}));
    try std.testing.expectEqual(@as(?u64, 110), byteLen(.{ .known = .IQ3_S }, &.{256}));
    try std.testing.expectEqual(@as(?u64, 82), byteLen(.{ .known = .IQ2_S }, &.{256}));
    try std.testing.expectEqual(@as(?u64, 56), byteLen(.{ .known = .IQ1_M }, &.{256}));
    try std.testing.expectEqual(@as(?u64, 54), byteLen(.{ .known = .TQ1_0 }, &.{256}));
    try std.testing.expectEqual(@as(?u64, 66), byteLen(.{ .known = .TQ2_0 }, &.{256}));
    try std.testing.expectEqual(@as(?u64, 17), byteLen(.{ .known = .MXFP4 }, &.{32}));
    try std.testing.expectEqual(@as(?u64, 36), byteLen(.{ .known = .NVFP4 }, &.{64}));
}
