// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

const std = @import("std");
const Allocator = std.mem.Allocator;

const version_4_magic = "AGS4";
const header_len = version_4_magic.len + @sizeOf(u64);
pub const hard_max_edges_per_document: usize = 1_000_000;
pub const hard_max_relation_items_per_artifact: usize = 1_000_000;
pub const hard_max_manifest_bytes: usize = 64 * 1024 * 1024;

pub fn effectiveEdgeLimit(configured: u32) usize {
    return if (configured == 0) hard_max_edges_per_document else @min(@as(usize, configured), hard_max_edges_per_document);
}

pub const Format = enum { v4 };

pub fn freeKeys(alloc: Allocator, keys: [][]u8) void {
    for (keys) |key| alloc.free(key);
    if (keys.len > 0) alloc.free(keys);
}

pub fn format(raw: []const u8) !Format {
    if (raw.len > hard_max_manifest_bytes) return error.ResourceLimitExceeded;
    if (!std.mem.startsWith(u8, raw, version_4_magic) or raw.len < header_len) return error.UnsupportedGraphAssetStateVersion;
    var pos: usize = header_len;
    const count = readU32Big(raw, &pos) catch return error.InvalidGraphAssetState;
    if (@as(usize, count) > hard_max_edges_per_document) return error.ResourceLimitExceeded;
    if (@as(usize, count) > (raw.len - pos) / @sizeOf(u32)) return error.InvalidGraphAssetState;
    for (0..count) |_| {
        const key_len = readU32Big(raw, &pos) catch return error.InvalidGraphAssetState;
        if (key_len > raw.len - pos) return error.InvalidGraphAssetState;
        pos += key_len;
    }
    if (pos != raw.len) return error.InvalidGraphAssetState;
    return .v4;
}

pub fn decodeKeysAlloc(alloc: Allocator, raw: []const u8) ![][]u8 {
    _ = try format(raw);
    var pos: usize = header_len;
    const count = readU32Big(raw, &pos) catch return error.InvalidGraphAssetState;
    const keys = if (count > 0) try alloc.alloc([]u8, count) else return &.{};
    var initialized: usize = 0;
    errdefer {
        for (keys[0..initialized]) |key| alloc.free(key);
        alloc.free(keys);
    }
    for (keys) |*key| {
        const key_len = readU32Big(raw, &pos) catch return error.InvalidGraphAssetState;
        if (key_len > raw.len - pos) return error.InvalidGraphAssetState;
        key.* = try alloc.dupe(u8, raw[pos..][0..key_len]);
        pos += key_len;
        initialized += 1;
    }
    if (pos != raw.len) return error.InvalidGraphAssetState;
    return keys;
}

/// Stores the owning graph generation and edge keys. Complete payloads are
/// retained by logical-edge-global contender records, keeping this deletion
/// manifest compact even when edge metadata is large.
pub fn encodeAlloc(alloc: Allocator, generation: u64, writes: anytype) ![]u8 {
    if (writes.len > hard_max_edges_per_document) return error.ResourceLimitExceeded;
    var encoded_len: usize = header_len + @sizeOf(u32);
    for (writes) |write| {
        encoded_len = std.math.add(usize, encoded_len, @sizeOf(u32)) catch return error.ResourceLimitExceeded;
        encoded_len = std.math.add(usize, encoded_len, write.key.len) catch return error.ResourceLimitExceeded;
        if (encoded_len > hard_max_manifest_bytes) return error.ResourceLimitExceeded;
    }
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.ensureTotalCapacityPrecise(alloc, encoded_len);
    try out.appendSlice(alloc, version_4_magic);
    var generation_bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &generation_bytes, generation, .big);
    try out.appendSlice(alloc, &generation_bytes);
    try appendLength(&out, alloc, writes.len);
    for (writes) |write| {
        try appendLength(&out, alloc, write.key.len);
        try out.appendSlice(alloc, write.key);
    }
    return try out.toOwnedSlice(alloc);
}

pub fn containsKey(raw: []const u8, target_key: []const u8) !bool {
    _ = try format(raw);
    var pos: usize = header_len;
    const count = readU32Big(raw, &pos) catch return error.InvalidGraphAssetState;
    if (@as(usize, count) > hard_max_edges_per_document) return error.ResourceLimitExceeded;
    const minimum_entry_bytes: usize = @sizeOf(u32);
    if (@as(usize, count) > (raw.len - pos) / minimum_entry_bytes) return error.InvalidGraphAssetState;
    var found = false;
    for (0..count) |_| {
        const key_len = readU32Big(raw, &pos) catch return error.InvalidGraphAssetState;
        if (key_len > raw.len - pos) return error.InvalidGraphAssetState;
        const matches = std.mem.eql(u8, raw[pos..][0..key_len], target_key);
        pos += key_len;
        found = found or matches;
    }
    if (pos != raw.len) return error.InvalidGraphAssetState;
    return found;
}

pub fn coverageGeneration(raw: []const u8) !u64 {
    _ = try format(raw);
    return std.mem.readInt(u64, raw[version_4_magic.len..][0..@sizeOf(u64)], .big);
}

fn readU32Big(bytes: []const u8, pos: *usize) !u32 {
    if (bytes.len - pos.* < @sizeOf(u32)) return error.EndOfStream;
    const value = std.mem.readInt(u32, bytes[pos.*..][0..@sizeOf(u32)], .big);
    pos.* += @sizeOf(u32);
    return value;
}

fn appendU32Big(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, value: u32) !void {
    const be = std.mem.nativeToBig(u32, value);
    try out.appendSlice(alloc, std.mem.asBytes(&be));
}

fn appendLength(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, value: usize) !void {
    if (value > std.math.maxInt(u32)) return error.GraphAssetStateTooLarge;
    try appendU32Big(out, alloc, @intCast(value));
}

test "graph asset state v4 round trip preserves generation and keys without payload duplication" {
    const alloc = std.testing.allocator;
    const Pair = struct { key: []const u8, value: []const u8 };
    const raw = try encodeAlloc(alloc, 42, &[_]Pair{
        .{ .key = "edge:a", .value = "payload:a" },
        .{ .key = "edge:b", .value = "payload:b" },
    });
    defer alloc.free(raw);
    try std.testing.expectEqual(Format.v4, try format(raw));
    const keys = try decodeKeysAlloc(alloc, raw);
    defer freeKeys(alloc, keys);
    try std.testing.expectEqualStrings("edge:a", keys[0]);
    try std.testing.expect(std.mem.indexOf(u8, raw, "payload:a") == null);
    try std.testing.expect(try containsKey(raw, "edge:b"));
    try std.testing.expectEqual(@as(u64, 42), try coverageGeneration(raw));
}

test "graph asset state rejects excessive entry counts before allocation" {
    const raw = version_4_magic ++ [_]u8{0} ** 8 ++ [_]u8{ 0xff, 0xff, 0xff, 0xff };
    try std.testing.expectError(error.ResourceLimitExceeded, containsKey(raw, "edge:a"));
}

test "graph asset state applies a finite safety limit when zero is configured" {
    try std.testing.expectEqual(hard_max_edges_per_document, effectiveEdgeLimit(0));
    try std.testing.expectEqual(@as(usize, 25), effectiveEdgeLimit(25));
}
