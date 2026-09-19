// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Allocation-free recognition shared by DATA and the native codec. Matching
//! this family grants no authority and does not validate the encoded payload.
const std = @import("std");
pub const magic = "AFCENTRY";
pub const version: u16 = 1;
pub const profile: u16 = 1;
pub const max_wire_bytes = 512 * 1024;
pub const raft_batch_protocol_version: u16 = 7;

/// Durable progress matches the complete original Raft Entry.data bytes.
/// Envelope checksums are validated separately and are included in this hash.
pub fn payloadDigest(bytes: []const u8) [32]u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    return digest;
}

/// Truncated magic is still reserved: dispatch must reject it through the
/// canonical validator, never silently classify it as an ignored normal entry.
pub fn looksLike(bytes: []const u8) bool {
    if (bytes.len == 0) return false;
    const length = @min(bytes.len, magic.len);
    return std.mem.eql(u8, bytes[0..length], magic[0..length]);
}

test "workload admission completion entry recognition reserves malformed prefixes" {
    try std.testing.expect(!looksLike(""));
    try std.testing.expect(!looksLike("{\"table\":\"docs\",\"batch\":{}}"));
    try std.testing.expect(!looksLike("put:key=value"));
    for (1..magic.len + 1) |length| try std.testing.expect(looksLike(magic[0..length]));
    try std.testing.expect(looksLike(magic ++ "invalid header"));
}
