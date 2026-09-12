// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Fixture-local metadata sharing. Values borrow the parsed capture's arena;
//! lookup never allocates, follows another reference, or changes tensor data.
const std = @import("std");

pub const max_entries = 64;

pub fn validate(version: u32, lengths: []const usize) !void {
    if (version != 1) return error.InvalidTrainingFixtureMetadata;
    for (lengths) |length| if (length > max_entries) return error.InvalidTrainingFixtureMetadata;
}

pub fn resolve(comptime T: type, local: ?T, index: ?usize, table: []const T) !T {
    if (table.len > max_entries) return error.InvalidTrainingFixtureMetadata;
    if (index) |i| {
        if (local != null or i >= table.len) return error.InvalidTrainingFixtureMetadata;
        return table[i];
    }
    return local orelse error.InvalidTrainingFixtureMetadata;
}

test "boundary training fixture metadata rejects missing ambiguous and unbounded references" {
    const a: []const u8 = "first";
    const b: []const u8 = "second";
    const table = [_][]const u8{ a, b };
    const borrowed = try resolve([]const u8, null, 1, &table);
    try std.testing.expect(borrowed.ptr == b.ptr);
    try std.testing.expectEqualStrings(a, try resolve([]const u8, a, null, &table));
    try std.testing.expectError(error.InvalidTrainingFixtureMetadata, resolve([]const u8, null, null, &table));
    try std.testing.expectError(error.InvalidTrainingFixtureMetadata, resolve([]const u8, a, 0, &table));
    try std.testing.expectError(error.InvalidTrainingFixtureMetadata, resolve([]const u8, null, 2, &table));
    try std.testing.expectError(error.InvalidTrainingFixtureMetadata, resolve(u8, null, 0, &([_]u8{0} ** (max_entries + 1))));
    try validate(1, &.{max_entries});
    try std.testing.expectError(error.InvalidTrainingFixtureMetadata, validate(2, &.{0}));
    try std.testing.expectError(error.InvalidTrainingFixtureMetadata, validate(1, &.{max_entries + 1}));
}
