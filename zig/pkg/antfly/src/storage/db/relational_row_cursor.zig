// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Stateless ordered-row continuation. The identity binds the immutable schema
//! and logical index/comparison identity; the suffix orders tuples and primary-key
//! ties. Owner-local physical generations may differ after restore/reconciliation.
//! This is not a retained snapshot or an authorization capability.
const std = @import("std");
pub const identity_len = 48;
pub const max_key_bytes = 64 * 1024;

pub fn identity(schema_version: u32, index_name: []const u8, fingerprint: [32]u8) [identity_len]u8 {
    var result: [identity_len]u8 = undefined;
    std.mem.writeInt(u32, result[0..4], schema_version, .big);
    @memcpy(result[4..16], "AROW-CURSOR1");
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly.relational-row-cursor.logical-index.v1");
    hash.update(&fingerprint);
    hash.update(index_name);
    hash.final(result[16..48]);
    return result;
}

pub fn encode(alloc: std.mem.Allocator, header: [identity_len]u8, key: []const u8) ![]u8 {
    if (key.len == 0 or key.len > max_key_bytes) return error.RelationalRowsOutputBudgetExceeded;
    const result = try alloc.alloc(u8, (identity_len + key.len) * 2);
    const alphabet = "0123456789abcdef";
    for (header, 0..) |byte, i| {
        result[i * 2] = alphabet[byte >> 4];
        result[i * 2 + 1] = alphabet[byte & 15];
    }
    for (key, identity_len..) |byte, i| {
        result[i * 2] = alphabet[byte >> 4];
        result[i * 2 + 1] = alphabet[byte & 15];
    }
    return result;
}

pub fn validate(encoded: []const u8) !void {
    if (encoded.len <= identity_len * 2 or encoded.len > (identity_len + max_key_bytes) * 2 or encoded.len % 2 != 0)
        return error.InvalidRelationalRowsRequest;
    for (encoded) |byte| if (!std.ascii.isDigit(byte) and !(byte >= 'a' and byte <= 'f')) return error.InvalidRelationalRowsRequest;
}

pub fn decode(alloc: std.mem.Allocator, encoded: []const u8, expected: [identity_len]u8) ![]u8 {
    try validate(encoded);
    var header: [identity_len]u8 = undefined;
    _ = std.fmt.hexToBytes(&header, encoded[0 .. identity_len * 2]) catch return error.InvalidRelationalRowsRequest;
    if (!std.mem.eql(u8, &header, &expected)) return error.PreparedGenerationChanged;
    const key = try alloc.alloc(u8, encoded.len / 2 - identity_len);
    errdefer alloc.free(key);
    _ = std.fmt.hexToBytes(key, encoded[identity_len * 2 ..]) catch return error.InvalidRelationalRowsRequest;
    return key;
}

test "relational row query cursors preserve binary order and fence logical index comparison and schema" {
    const alloc = std.testing.allocator;
    const header = identity(7, "by_id", .{3} ** 32);
    const encoded = try encode(alloc, header, "\x00\xff\x80");
    defer alloc.free(encoded);
    const decoded = try decode(alloc, encoded, header);
    defer alloc.free(decoded);
    try std.testing.expectEqualStrings("\x00\xff\x80", decoded);
    try std.testing.expectError(error.PreparedGenerationChanged, decode(alloc, encoded, identity(8, "by_id", .{3} ** 32)));
    try std.testing.expectError(error.PreparedGenerationChanged, decode(alloc, encoded, identity(7, "by_other_id", .{3} ** 32)));
    try std.testing.expectError(error.PreparedGenerationChanged, decode(alloc, encoded, identity(7, "by_id", .{4} ** 32)));
    try std.testing.expectError(error.InvalidRelationalRowsRequest, validate("not-a-cursor"));
}
