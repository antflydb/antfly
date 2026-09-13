// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//! Overflow-safe byte-region validation shared by immutable native formats.
//! Persistent metadata is untrusted at recovery boundaries: no reader may
//! form a slice until casts, multiplication, addition, and the enclosing
//! file bound have all succeeded.

const std = @import("std");

pub const Region = struct {
    offset: usize,
    len: usize,
    end: usize,

    pub fn slice(self: Region, bytes: []const u8) []const u8 {
        std.debug.assert(self.end <= bytes.len);
        return bytes[self.offset..self.end];
    }
};

pub fn fromLength(limit: usize, offset_raw: u64, len_raw: u64) !Region {
    const offset = std.math.cast(usize, offset_raw) orelse return error.InvalidFileRegion;
    const len = std.math.cast(usize, len_raw) orelse return error.InvalidFileRegion;
    const end = std.math.add(usize, offset, len) catch return error.InvalidFileRegion;
    if (end > limit) return error.InvalidFileRegion;
    return .{ .offset = offset, .len = len, .end = end };
}

pub fn fromCount(limit: usize, offset_raw: u64, count_raw: u64, item_size: usize) !Region {
    const count = std.math.cast(usize, count_raw) orelse return error.InvalidFileRegion;
    const len = std.math.mul(usize, count, item_size) catch return error.InvalidFileRegion;
    return fromLength(limit, offset_raw, @intCast(len));
}

pub fn exactTail(
    data_len: usize,
    footer_len: usize,
    minimum_offset: usize,
    offset_raw: u64,
    count_raw: u64,
    item_size: usize,
) !Region {
    if (footer_len > data_len) return error.InvalidFileRegion;
    const payload_end = data_len - footer_len;
    const region = try fromCount(payload_end, offset_raw, count_raw, item_size);
    if (region.offset < minimum_offset or region.end != payload_end) return error.InvalidFileRegion;
    return region;
}

test "checked regions reject wrapped and out-of-file metadata" {
    try std.testing.expectError(error.InvalidFileRegion, fromLength(64, std.math.maxInt(u64) - 7, 16));
    try std.testing.expectError(error.InvalidFileRegion, fromCount(64, 8, std.math.maxInt(u64), 64));
    try std.testing.expectError(error.InvalidFileRegion, exactTail(128, 32, 16, 96, 1, 64));
    const valid = try exactTail(128, 32, 16, 32, 2, 32);
    try std.testing.expectEqual(@as(usize, 32), valid.offset);
    try std.testing.expectEqual(@as(usize, 96), valid.end);
}
