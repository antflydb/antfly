// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

const std = @import("std");

pub const StableId = u64;

/// Stable IDs are part of the replay ABI. Keep this algorithm independent of
/// std.hash implementation details and never derive an ID from source position.
pub fn stable(namespace: []const u8, name: []const u8) StableId {
    var hash: u64 = 0xcbf29ce484222325;
    for (namespace) |byte| {
        hash ^= byte;
        hash *%= 0x100000001b3;
    }
    hash ^= 0;
    hash *%= 0x100000001b3;
    for (name) |byte| {
        hash ^= byte;
        hash *%= 0x100000001b3;
    }
    return hash;
}

pub fn lessThan(_: void, lhs: StableId, rhs: StableId) bool {
    return lhs < rhs;
}

pub fn sort(ids: []StableId) void {
    std.mem.sort(StableId, ids, {}, lessThan);
}

pub fn validateCanonical(ids: []const StableId) !void {
    for (ids, 0..) |value, index| {
        if (index == 0) continue;
        if (ids[index - 1] == value) return error.DuplicateStableId;
        if (ids[index - 1] > value) return error.NonCanonicalOrder;
    }
}

test "stable IDs are namespaced and fixed" {
    try std.testing.expectEqual(@as(u64, 0xf5510053f570d145), stable("choice", "toy.command"));
    try std.testing.expect(stable("choice", "toy.command") != stable("property", "toy.command"));
}

test "canonical ID validation rejects duplicates and disorder" {
    try validateCanonical(&.{ 1, 2, 3 });
    try std.testing.expectError(error.DuplicateStableId, validateCanonical(&.{ 1, 1 }));
    try std.testing.expectError(error.NonCanonicalOrder, validateCanonical(&.{ 2, 1 }));
}
