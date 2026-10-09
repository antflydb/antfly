// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Views address woven frame storage without copying field samples.
const std = @import("std");
pub fn Sample(comptime Plane: type) type {
    return if (@typeInfo(Plane) == .pointer) std.meta.Child(Plane) else Plane.Sample;
}
pub fn get(plane: anytype, i: usize) Sample(@TypeOf(plane)) {
    return if (@typeInfo(@TypeOf(plane)) == .pointer) plane[i] else plane.get(i);
}
pub fn put(plane: anytype, i: usize, value: Sample(@TypeOf(plane))) void {
    if (@typeInfo(@TypeOf(plane)) == .pointer) plane[i] = value else plane.put(i, value);
}
pub fn length(plane: anytype) usize {
    return if (@typeInfo(@TypeOf(plane)) == .pointer) plane.len else plane.width * plane.height;
}
pub fn View(comptime T: type) type {
    return struct {
        pub const Sample = T;
        data: []T,
        width: usize,
        height: usize,
        field: bool = false,
        parity: usize = 0,
        pub fn get(self: @This(), i: usize) T {
            return self.data[self.index(i)];
        }
        pub fn put(self: @This(), i: usize, value: T) void {
            self.data[self.index(i)] = value;
        }
        fn index(self: @This(), i: usize) usize {
            return if (self.field) ((i / self.width) * 2 + self.parity) * self.width + i % self.width else i;
        }
    };
}
pub const Cell = struct { mb: usize, index: usize };
/// Macroblock storage remains a raster grid. Field/frame addressing translates
/// physical row locations back to the owning macroblock and its 4x4 cell.
pub fn cell(meta: []const @import("h264_entropy.zig").Meta, width: usize, chroma_format: u8, paired: bool, field: bool, parity: usize, plane: usize, x: i32, y: i32) ?Cell {
    return cellSample(meta, width, chroma_format, paired, field, parity, plane, x * 4, y * 4);
}
pub fn cellSample(meta: []const @import("h264_entropy.zig").Meta, width: usize, chroma_format: u8, paired: bool, field: bool, parity: usize, plane: usize, x: i32, y: i32) ?Cell {
    const sub_x: usize = if (plane == 0 or chroma_format == 3) 1 else 2;
    const sub_y: usize = if (plane == 0 or chroma_format != 1) 1 else 2;
    const stride = width / (sub_x * 4);
    const height = meta.len / (width / 16) * 16 / sub_y;
    if (x < 0 or y < 0 or x >= stride * 4 or @as(usize, @intCast(y)) >= height / (if (field) @as(usize, 2) else 1)) return null;
    const px: usize = @as(usize, @intCast(x));
    const py: usize = @as(usize, @intCast(y)) * (if (field) @as(usize, 2) else 1) + (if (field) parity else 0);
    const mw = width / 16;
    const mbh = 16 / sub_y;
    const mbx = px / (16 / sub_x);
    var mby = py / mbh;
    var local_y = py % mbh;
    if (paired) {
        const pair_row = py / (2 * mbh);
        const top = pair_row * 2 * mw + mbx;
        if (meta[top].field) {
            mby = pair_row * 2 + py % 2;
            local_y = py % (2 * mbh) / 2;
        }
    }
    return .{ .mb = mby * mw + mbx, .index = (mby * (mbh / 4) + local_y / 4) * stride + @as(usize, @intCast(x)) / 4 };
}
pub fn raster(address: usize, width_mbs: usize) usize {
    return address / 2 / width_mbs * 2 * width_mbs + address / 2 % width_mbs + address % 2 * width_mbs;
}
pub fn physicalCell(meta: []const @import("h264_entropy.zig").Meta, width: usize, paired: bool, x: usize, y: usize) Cell {
    const mw = width / 16;
    const col = x / 16;
    var row = y / 16;
    var local = y % 16;
    if (paired and meta[y / 32 * 2 * mw + col].field) {
        row = y / 32 * 2 + y % 2;
        local = y % 32 / 2;
    }
    return .{ .mb = row * mw + col, .index = (row * 4 + local / 4) * (width / 4) + x / 4 };
}
