// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

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
