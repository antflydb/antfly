// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License at https://www.antfly.io/licensing/ELv2-license.

const std = @import("std");

/// Canonical lowercase-hex identity of one metadata cluster. Unlike the fixed
/// metadata Raft group id, this value distinguishes independently bootstrapped
/// clusters. The text representation keeps status and internal JSON APIs valid
/// UTF-8 while retaining 128 bits of random entropy.
pub const MetadataClusterIncarnation = [32]u8;

pub const zero: MetadataClusterIncarnation = "00000000000000000000000000000000".*;

pub fn generate(io: std.Io) !MetadataClusterIncarnation {
    var entropy: [16]u8 = undefined;
    try io.randomSecure(&entropy);
    var value = std.fmt.bytesToHex(entropy, .lower);
    // A zero value is reserved to make absent/uninitialized identities fail
    // closed at API boundaries. The loop is effectively single-pass.
    while (!isValid(value)) {
        try io.randomSecure(&entropy);
        value = std.fmt.bytesToHex(entropy, .lower);
    }
    return value;
}

pub fn isValid(value: MetadataClusterIncarnation) bool {
    if (std.mem.eql(u8, &value, &zero)) return false;
    for (value) |char| {
        if (!std.ascii.isDigit(char) and !(char >= 'a' and char <= 'f')) return false;
    }
    return true;
}

test "metadata cluster incarnation has one canonical JSON representation" {
    const value: MetadataClusterIncarnation = "0123456789abcdef0123456789abcdef".*;
    try std.testing.expect(isValid(value));
    try std.testing.expect(!isValid(zero));
    try std.testing.expect(!isValid("0123456789ABCDEF0123456789ABCDEF".*));

    const encoded = try std.json.Stringify.valueAlloc(std.testing.allocator, value, .{});
    defer std.testing.allocator.free(encoded);
    try std.testing.expectEqualStrings("\"0123456789abcdef0123456789abcdef\"", encoded);
    const decoded = try std.json.parseFromSlice(MetadataClusterIncarnation, std.testing.allocator, encoded, .{});
    defer decoded.deinit();
    try std.testing.expectEqual(value, decoded.value);
}
