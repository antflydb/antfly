// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

const std = @import("std");

/// PostgreSQL accepts optional braces and a dash after each four hex digits.
/// Persist and return only the canonical lower-case 8-4-4-4-12 spelling.
pub fn parse(input: []const u8) error{InvalidUuid}![16]u8 {
    const wrapped = input.len >= 2 and input[0] == '{';
    const text = if (wrapped) blk: {
        if (input[input.len - 1] != '}') return error.InvalidUuid;
        break :blk input[1 .. input.len - 1];
    } else input;
    var bytes: [16]u8 = undefined;
    var offset: usize = 0;
    for (&bytes, 0..) |*byte, index| {
        if (offset + 2 > text.len) return error.InvalidUuid;
        const high = std.fmt.charToDigit(text[offset], 16) catch return error.InvalidUuid;
        const low = std.fmt.charToDigit(text[offset + 1], 16) catch return error.InvalidUuid;
        byte.* = (high << 4) | low;
        offset += 2;
        if (index % 2 == 1 and index != 15 and offset < text.len and text[offset] == '-') offset += 1;
    }
    if (offset != text.len) return error.InvalidUuid;
    return bytes;
}

pub fn format(bytes: [16]u8) [36]u8 {
    const hex = "0123456789abcdef";
    var result: [36]u8 = undefined;
    var offset: usize = 0;
    for (bytes, 0..) |byte, index| {
        if (index == 4 or index == 6 or index == 8 or index == 10) {
            result[offset] = '-';
            offset += 1;
        }
        result[offset] = hex[byte >> 4];
        result[offset + 1] = hex[byte & 15];
        offset += 2;
    }
    return result;
}

pub fn canonicalAlloc(alloc: std.mem.Allocator, input: []const u8) ![]u8 {
    const value = format(try parse(input));
    return alloc.dupe(u8, &value);
}

test "UUID input variants share one canonical value" {
    const canonical = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11";
    for ([_][]const u8{
        canonical,
        "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11",
        "{a0eebc999c0b4ef8bb6d6bb9bd380a11}",
        "a0ee-bc99-9c0b-4ef8-bb6d-6bb9-bd38-0a11",
    }) |input| {
        const value = format(try parse(input));
        try std.testing.expectEqualStrings(canonical, &value);
    }
    for ([_][]const u8{ "", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a1z", "a0eebc99--9c0b-4ef8-bb6d-6bb9bd380a11", "{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11" }) |input| {
        try std.testing.expectError(error.InvalidUuid, parse(input));
    }
}
