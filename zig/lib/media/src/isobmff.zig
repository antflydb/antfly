// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
pub const Box = struct {
    typ: u32,
    data: []const u8,
    payload: []const u8,
    end: usize,
};

pub fn readBox(bytes: []const u8, start: usize) !Box {
    if (start > bytes.len or bytes.len - start < 8) return error.MalformedMedia;
    const size32 = readU32(bytes[start..][0..4]);
    const typ = readU32(bytes[start + 4 ..][0..4]);

    var header_len: usize = 8;
    var box_size: u64 = size32;
    if (size32 == 1) {
        if (bytes.len - start < 16) return error.MalformedMedia;
        box_size = readU64(bytes[start + 8 ..][0..8]);
        header_len = 16;
    } else if (size32 == 0) {
        box_size = bytes.len - start;
    }

    const end_u64 = std.math.add(u64, start, box_size) catch return error.MalformedMedia;
    const end = std.math.cast(usize, end_u64) orelse return error.MalformedMedia;
    if (box_size < header_len or end > bytes.len) return error.MalformedMedia;

    return .{
        .typ = typ,
        .data = bytes[start..end],
        .payload = bytes[start + header_len .. end],
        .end = end,
    };
}

pub fn fourcc(tag: *const [4:0]u8) u32 {
    return std.mem.readInt(u32, tag, .big);
}

fn readU32(bytes: []const u8) u32 {
    return std.mem.readInt(u32, bytes[0..4], .big);
}
fn readU64(bytes: []const u8) u64 {
    return std.mem.readInt(u64, bytes[0..8], .big);
}
