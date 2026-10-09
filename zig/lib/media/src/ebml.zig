// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
pub const VintResult = struct { value: u64, len: usize };
pub const SizeResult = struct { value: u64, len: usize, is_unknown: bool };

pub const Element = struct {
    id: u32,
    size: ?u64,
    data_start: usize,
    data_end: ?usize,
};

pub fn vintLength(first_byte: u8) !usize {
    if (first_byte == 0) return error.MalformedMedia;
    var mask: u8 = 0x80;
    var len: usize = 1;
    while ((first_byte & mask) == 0) : (len += 1) {
        mask >>= 1;
    }
    if (len > 8) return error.MalformedMedia;
    return len;
}

/// Reads an EBML element ID. The marker bit(s) stay part of the returned
/// value, matching how element IDs are canonically compared.
pub fn readElementId(bytes: []const u8, start: usize) !VintResult {
    if (start >= bytes.len) return error.MalformedMedia;
    const len = try vintLength(bytes[start]);
    if (len > 4) return error.MalformedMedia;
    if (len > bytes.len - start) return error.MalformedMedia;
    var value: u64 = 0;
    for (bytes[start .. start + len]) |b| value = (value << 8) | b;
    return .{ .value = value, .len = len };
}

/// Reads an EBML variable-size integer (used for element sizes, block track
/// numbers, and lace sizes). The marker bit is stripped from the returned
/// value. `is_unknown` reports the reserved all-ones encoding used for
/// streamed Segment/Cluster elements.
pub fn readVint(bytes: []const u8, start: usize) !SizeResult {
    if (start >= bytes.len) return error.MalformedMedia;
    const len = try vintLength(bytes[start]);
    if (len > bytes.len - start) return error.MalformedMedia;

    const shift: u4 = @intCast(len);
    const mask: u8 = @intCast(@as(u16, 0xFF) >> shift);
    var value: u64 = bytes[start] & mask;
    for (bytes[start + 1 .. start + len]) |b| value = (value << 8) | b;

    const max_value = (@as(u64, 1) << @intCast(7 * len)) - 1;
    return .{ .value = value, .len = len, .is_unknown = value == max_value };
}

pub fn readElementHeader(bytes: []const u8, start: usize) !Element {
    const id_result = try readElementId(bytes, start);
    const id = std.math.cast(u32, id_result.value) orelse return error.MalformedMedia;

    const size_start = start + id_result.len;
    const size_result = try readVint(bytes, size_start);
    const data_start = size_start + size_result.len;
    if (data_start > bytes.len) return error.MalformedMedia;

    if (size_result.is_unknown) {
        return .{ .id = id, .size = null, .data_start = data_start, .data_end = null };
    }
    const data_end_u64 = std.math.add(u64, data_start, size_result.value) catch return error.MalformedMedia;
    const data_end = std.math.cast(usize, data_end_u64) orelse return error.MalformedMedia;
    if (data_end > bytes.len) return error.MalformedMedia;

    return .{ .id = id, .size = size_result.value, .data_start = data_start, .data_end = data_end };
}

pub fn elementPayload(bytes: []const u8, elem: Element) ![]const u8 {
    const end = elem.data_end orelse return error.MalformedMedia;
    if (elem.data_start > end or end > bytes.len) return error.MalformedMedia;
    return bytes[elem.data_start..end];
}

test "EBML payload rejects invalid and truncated bounds" {
    try std.testing.expectError(error.MalformedMedia, elementPayload("abc", .{ .id = 1, .size = 2, .data_start = 2, .data_end = 4 }));
    try std.testing.expectError(error.MalformedMedia, elementPayload("abc", .{ .id = 1, .size = 0, .data_start = 2, .data_end = 1 }));
    try std.testing.expectError(error.MalformedMedia, readVint(&.{0x01}, 0));
}

pub fn readUint(bytes: []const u8) !u64 {
    if (bytes.len == 0 or bytes.len > 8) return error.MalformedMedia;
    var value: u64 = 0;
    for (bytes) |b| value = (value << 8) | b;
    return value;
}

pub fn readSignedInt(bytes: []const u8) !i64 {
    if (bytes.len == 0 or bytes.len > 8) return error.MalformedMedia;
    var value: u64 = 0;
    for (bytes) |b| value = (value << 8) | b;
    if (bytes.len == 8) return @bitCast(value);

    const bit_width: u8 = @intCast(bytes.len * 8);
    const shift: u6 = @intCast(64 - bit_width);
    const widened: i64 = @bitCast(value << shift);
    return widened >> shift;
}

pub fn readFloat(bytes: []const u8) !f64 {
    if (bytes.len == 4) {
        const bits = std.mem.readInt(u32, bytes[0..4], .big);
        return @as(f32, @bitCast(bits));
    }
    if (bytes.len == 8) {
        const bits = std.mem.readInt(u64, bytes[0..8], .big);
        return @bitCast(bits);
    }
    return error.MalformedMedia;
}
