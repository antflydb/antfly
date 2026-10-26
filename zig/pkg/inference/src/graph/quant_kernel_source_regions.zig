// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Compose all generated regions against one immutable source snapshot.
//! Bounds and ownership are checked before a caller writes the final file.
const std = @import("std");

pub const Change = struct {
    start: usize,
    end: usize,
    expected: []const u8,
    owner: []const u8 = "",
};

pub fn sortAndValidate(changes: []Change, source_len: usize) !void {
    std.mem.sort(Change, changes, {}, struct {
        fn less(_: void, a: Change, b: Change) bool {
            return a.start < b.start;
        }
    }.less);
    var previous_end: usize = 0;
    var previous_start: ?usize = null;
    for (changes) |change| {
        if (change.end < change.start or change.end > source_len) return error.InvalidSourceRegionBounds;
        if (change.start < previous_end or previous_start == change.start) return error.OverlappingSourceRegionOwners;
        previous_start = change.start;
        previous_end = change.end;
    }
}

pub fn merge(allocator: std.mem.Allocator, source: []const u8, changes: []Change) ![]u8 {
    try sortAndValidate(changes, source.len);
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    var cursor: usize = 0;
    for (changes) |change| {
        try out.appendSlice(allocator, source[cursor..change.start]);
        try out.appendSlice(allocator, change.expected);
        cursor = change.end;
    }
    try out.appendSlice(allocator, source[cursor..]);
    return out.toOwnedSlice(allocator);
}

test "source composition preserves surrounding text and original offsets" {
    var changes = [_]Change{
        .{ .start = 4, .end = 6, .expected = "second replacement" },
        .{ .start = 1, .end = 3, .expected = "first" },
    };
    const source = "aXYbUVc";
    const result = try merge(std.testing.allocator, source, &changes);
    defer std.testing.allocator.free(result);
    try std.testing.expectEqualStrings("afirstbsecond replacementc", result);
    try std.testing.expectEqualStrings("aXYbUVc", source);
}

test "source composition rejects overlapping duplicate and invalid ownership" {
    var overlap = [_]Change{
        .{ .start = 1, .end = 4, .expected = "a" },
        .{ .start = 3, .end = 5, .expected = "b" },
    };
    try std.testing.expectError(error.OverlappingSourceRegionOwners, merge(std.testing.allocator, "abcdef", &overlap));
    var duplicate = [_]Change{
        .{ .start = 2, .end = 2, .expected = "a" },
        .{ .start = 2, .end = 2, .expected = "b" },
    };
    try std.testing.expectError(error.OverlappingSourceRegionOwners, merge(std.testing.allocator, "abcdef", &duplicate));
    var outside = [_]Change{.{ .start = 2, .end = 7, .expected = "a" }};
    try std.testing.expectError(error.InvalidSourceRegionBounds, merge(std.testing.allocator, "abcdef", &outside));
    var reversed = [_]Change{.{ .start = 4, .end = 2, .expected = "a" }};
    try std.testing.expectError(error.InvalidSourceRegionBounds, merge(std.testing.allocator, "abcdef", &reversed));
}
