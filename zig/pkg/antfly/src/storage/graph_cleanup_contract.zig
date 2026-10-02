// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Authoritative cleanup job incarnations, independent of local directories.
const std = @import("std");
const keys = @import("internal_keys.zig");
pub const Guard = struct { endpoint: []const u8, generation: u64 };
const magic = "GEC2";
pub fn matchesKey(key: []const u8, endpoint: []const u8) bool {
    if (!std.mem.startsWith(u8, key, keys.graph_endpoint_cleanup_prefix) or key.len != keys.graph_endpoint_cleanup_prefix.len + 64) return false;
    var digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(endpoint, &digest, .{});
    const hex = std.fmt.bytesToHex(digest, .lower);
    return std.mem.eql(u8, key[keys.graph_endpoint_cleanup_prefix.len..], &hex);
}
pub fn decode(key: []const u8, value: []const u8) !Guard {
    // Legacy values are arbitrary endpoint bytes; test their hash first so
    // an endpoint beginning with the version magic cannot be misinterpreted.
    if (matchesKey(key, value)) return .{ .endpoint = value, .generation = 0 };
    if (value.len < 12 or !std.mem.startsWith(u8, value, magic)) return error.InvalidGraphSegment;
    const generation = std.mem.readInt(u64, value[4..12], .little);
    if (generation == 0 or !matchesKey(key, value[12..])) return error.InvalidGraphSegment;
    return .{ .endpoint = value[12..], .generation = generation };
}
pub fn encodeAlloc(alloc: std.mem.Allocator, endpoint: []const u8, generation: u64) ![]u8 {
    if (generation == 0) return error.InvalidGraphSegment;
    const value = try alloc.alloc(u8, 12 + endpoint.len);
    @memcpy(value[0..4], magic);
    std.mem.writeInt(u64, value[4..12], generation, .little);
    @memcpy(value[12..], endpoint);
    return value;
}
