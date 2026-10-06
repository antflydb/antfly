// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Opaque native user-row keys. Identity is independent of mutable SQL unique
//! constraints. Generate once during preparation and retain across admission;
//! the native expected-absent predicate is still mandatory at commit.
const std = @import("std");

pub fn generate(alloc: std.mem.Allocator, io: std.Io) ![]const u8 {
    const output = try alloc.alloc(u8, 32);
    errdefer alloc.free(output);
    var entropy: [16]u8 = undefined;
    try io.randomSecure(&entropy);
    const encoded = std.fmt.bytesToHex(entropy, .lower);
    @memcpy(output, &encoded);
    return output;
}

test "native generated row identities are opaque owned keys" {
    const first = try generate(std.testing.allocator, std.testing.io);
    defer std.testing.allocator.free(first);
    const second = try generate(std.testing.allocator, std.testing.io);
    defer std.testing.allocator.free(second);
    try std.testing.expectEqual(@as(usize, 32), first.len);
    try std.testing.expect(!std.mem.eql(u8, first, second));
    for (first) |byte| try std.testing.expect(std.ascii.isHex(byte));
}
