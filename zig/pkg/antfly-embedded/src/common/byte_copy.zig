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

const std = @import("std");

pub fn copyPossiblyAliased(dest: []u8, src: []const u8) void {
    std.debug.assert(dest.len == src.len);
    if (dest.len == 0 or dest.ptr == src.ptr) return;
    const dest_start = @intFromPtr(dest.ptr);
    const dest_end = dest_start + dest.len;
    const src_start = @intFromPtr(src.ptr);
    const src_end = src_start + src.len;
    if (dest_end <= src_start or src_end <= dest_start) {
        @memcpy(dest, src);
    } else if (dest_start < src_start) {
        std.mem.copyForwards(u8, dest, src);
    } else {
        std.mem.copyBackwards(u8, dest, src);
    }
}

pub fn appendSlicePossiblyAliased(list: *std.ArrayListUnmanaged(u8), allocator: std.mem.Allocator, bytes: []const u8) !void {
    if (sliceOffsetInBuffer(list.items, bytes)) |source_offset| {
        const source_len = bytes.len;
        const old_len = list.items.len;
        try list.ensureUnusedCapacity(allocator, source_len);
        list.items.len = old_len + source_len;
        copyPossiblyAliased(list.items[old_len..][0..source_len], list.items[source_offset..][0..source_len]);
        return;
    }
    try list.appendSlice(allocator, bytes);
}

fn sliceOffsetInBuffer(buffer: []const u8, slice: []const u8) ?usize {
    if (slice.len == 0) return buffer.len;
    if (buffer.len == 0) return null;
    const buffer_start = @intFromPtr(buffer.ptr);
    const buffer_end = buffer_start + buffer.len;
    const slice_start = @intFromPtr(slice.ptr);
    const slice_end = slice_start + slice.len;
    if (slice_start < buffer_start or slice_end > buffer_end) return null;
    return slice_start - buffer_start;
}

test "copyPossiblyAliased supports overlapping ranges" {
    var bytes = [_]u8{ 'a', 'b', 'c', 'd', 'e', 'f' };

    copyPossiblyAliased(bytes[2..6], bytes[0..4]);
    try std.testing.expectEqualStrings("ababcd", &bytes);

    copyPossiblyAliased(bytes[0..4], bytes[2..6]);
    try std.testing.expectEqualStrings("abcdcd", &bytes);
}

test "appendSlicePossiblyAliased preserves self-referential sources across growth" {
    var list: std.ArrayListUnmanaged(u8) = .empty;
    defer list.deinit(std.testing.allocator);

    try list.appendSlice(std.testing.allocator, "abcdef");
    try appendSlicePossiblyAliased(&list, std.testing.allocator, list.items[1..5]);

    try std.testing.expectEqualStrings("abcdefbcde", list.items);
}

/// Compare long binary keys in fixed-width chunks, including in checked builds.
/// Slice loads keep unaligned input safe; the first differing lane determines
/// exactly the same unsigned lexicographic order as std.mem.order.
pub fn orderBytes(lhs: []const u8, rhs: []const u8) std.math.Order {
    const length = @min(lhs.len, rhs.len);
    if (lhs.ptr == rhs.ptr) return std.math.order(lhs.len, rhs.len);
    const width = 16;
    var offset: usize = 0;
    while (length - offset >= width) : (offset += width) {
        const left: @Vector(width, u8) = lhs[offset..][0..width].*;
        const right: @Vector(width, u8) = rhs[offset..][0..width].*;
        if (std.simd.firstTrue(left != right)) |different| {
            return std.math.order(lhs[offset + different], rhs[offset + different]);
        }
    }
    const tail = std.mem.order(u8, lhs[offset..length], rhs[offset..length]);
    return if (tail == .eq) std.math.order(lhs.len, rhs.len) else tail;
}

test "privacy binary key order matches scalar order across vector lanes alignment and tails" {
    var lhs: [160]u8 = undefined;
    var rhs: [160]u8 = undefined;
    for (&lhs, 0..) |*byte, i| byte.* = @truncate(i * 37 + 11);
    rhs = lhs;
    for (0..17) |alignment| {
        for (0..81) |length| {
            const a = lhs[alignment..][0..length];
            const b = rhs[alignment..][0..length];
            try std.testing.expectEqual(std.mem.order(u8, a, b), orderBytes(a, b));
            for (0..length) |different| {
                rhs[alignment + different] ^= 0xff;
                try std.testing.expectEqual(std.mem.order(u8, a, b), orderBytes(a, b));
                try std.testing.expectEqual(std.mem.order(u8, b, a), orderBytes(b, a));
                rhs[alignment + different] ^= 0xff;
            }
            for (0..length + 1) |prefix| {
                try std.testing.expectEqual(std.mem.order(u8, a, a[0..prefix]), orderBytes(a, a[0..prefix]));
                try std.testing.expectEqual(std.mem.order(u8, a, b[0..prefix]), orderBytes(a, b[0..prefix]));
                try std.testing.expectEqual(std.mem.order(u8, b[0..prefix], a), orderBytes(b[0..prefix], a));
            }
        }
    }
}

/// Exact shared-prefix length for unaligned binary slices. Checked builds
/// inspect fixed-width chunks instead of issuing a branch for every byte.
pub fn commonPrefixBytes(lhs: []const u8, rhs: []const u8) usize {
    const length = @min(lhs.len, rhs.len);
    if (lhs.ptr == rhs.ptr) return length;
    const width = 16;
    var offset: usize = 0;
    while (length - offset >= width) : (offset += width) {
        const left: @Vector(width, u8) = lhs[offset..][0..width].*;
        const right: @Vector(width, u8) = rhs[offset..][0..width].*;
        if (std.simd.firstTrue(left != right)) |different| return offset + different;
    }
    while (offset < length and lhs[offset] == rhs[offset]) : (offset += 1) {}
    return offset;
}

test "privacy binary common prefix preserves lanes alignment tails and prefix slices" {
    var lhs: [160]u8 = undefined;
    for (&lhs, 0..) |*byte, i| byte.* = @truncate(i * 37 + 11);
    var rhs = lhs;
    for (0..17) |alignment| {
        for (0..81) |length| {
            const a = lhs[alignment..][0..length];
            const b = rhs[alignment..][0..length];
            try std.testing.expectEqual(length, commonPrefixBytes(a, b));
            for (0..length) |different| {
                rhs[alignment + different] ^= 0xff;
                try std.testing.expectEqual(different, commonPrefixBytes(a, b));
                rhs[alignment + different] ^= 0xff;
            }
            for (0..length + 1) |prefix| {
                try std.testing.expectEqual(prefix, commonPrefixBytes(a, b[0..prefix]));
                try std.testing.expectEqual(prefix, commonPrefixBytes(a[0..prefix], a));
            }
        }
    }
}
