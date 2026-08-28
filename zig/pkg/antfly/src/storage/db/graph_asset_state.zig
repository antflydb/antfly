// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

const std = @import("std");
const Allocator = std.mem.Allocator;

const version_4_magic = "AGS4";
const version_5_magic = "AGS5";
const segment_magic = "AGB1";
const header_len = version_4_magic.len + @sizeOf(u64);
pub const hard_max_edges_per_document: usize = 1_000_000;
pub const hard_max_relation_items_per_artifact: usize = 1_000_000;
/// Graph source artifacts are replayed during live apply, repair, split, and
/// restore. A shared byte ceiling keeps every path within the same admission
/// contract instead of making restore uniquely reject data accepted live.
pub const hard_max_relation_artifact_bytes: usize = 16 * 1024 * 1024;
pub const hard_max_manifest_bytes: usize = 64 * 1024 * 1024;

pub fn effectiveEdgeLimit(configured: u32) usize {
    return if (configured == 0) hard_max_edges_per_document else @min(@as(usize, configured), hard_max_edges_per_document);
}

pub const Format = enum { v4, v5 };

pub const SegmentedRoot = struct {
    generation: u64,
    segment_count: u32,
    key_count: u32,
};

pub fn freeKeys(alloc: Allocator, keys: [][]u8) void {
    for (keys) |key| alloc.free(key);
    if (keys.len > 0) alloc.free(keys);
}

pub fn format(raw: []const u8) !Format {
    if (raw.len > hard_max_manifest_bytes) return error.ResourceLimitExceeded;
    if (std.mem.startsWith(u8, raw, version_5_magic)) {
        _ = try segmentedRoot(raw);
        return .v5;
    }
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
    if (try format(raw) != .v4) return error.SegmentedGraphAssetState;
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
    if (try format(raw) != .v4) return error.SegmentedGraphAssetState;
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

/// A v5 root is intentionally small and is published only after every
/// deterministic segment is durable. The root therefore acts as the commit
/// record for a resumable restore while ordinary readers can load segments in
/// bounded store reads.
pub fn encodeSegmentedRootAlloc(
    alloc: Allocator,
    generation: u64,
    segment_count: usize,
    key_count: usize,
) ![]u8 {
    if (segment_count > std.math.maxInt(u32) or key_count > hard_max_edges_per_document) {
        return error.ResourceLimitExceeded;
    }
    const out = try alloc.alloc(u8, header_len + 2 * @sizeOf(u32));
    @memcpy(out[0..version_5_magic.len], version_5_magic);
    std.mem.writeInt(u64, out[version_5_magic.len..header_len], generation, .big);
    std.mem.writeInt(u32, out[header_len..][0..4], @intCast(segment_count), .big);
    std.mem.writeInt(u32, out[header_len + 4 ..][0..4], @intCast(key_count), .big);
    return out;
}

pub fn segmentedRoot(raw: []const u8) !SegmentedRoot {
    const expected_len = header_len + 2 * @sizeOf(u32);
    if (raw.len != expected_len or !std.mem.startsWith(u8, raw, version_5_magic)) {
        return error.InvalidGraphAssetState;
    }
    const root = SegmentedRoot{
        .generation = std.mem.readInt(u64, raw[version_5_magic.len..header_len], .big),
        .segment_count = std.mem.readInt(u32, raw[header_len..][0..4], .big),
        .key_count = std.mem.readInt(u32, raw[header_len + 4 ..][0..4], .big),
    };
    if (@as(usize, root.key_count) > hard_max_edges_per_document) return error.ResourceLimitExceeded;
    if (root.key_count == 0 and root.segment_count != 0) return error.InvalidGraphAssetState;
    if (root.key_count > 0 and root.segment_count == 0) return error.InvalidGraphAssetState;
    if (root.segment_count > root.key_count) return error.InvalidGraphAssetState;
    return root;
}

pub fn encodeSegmentAlloc(alloc: Allocator, generation: u64, writes: anytype) ![]u8 {
    if (writes.len == 0 or writes.len > hard_max_edges_per_document) return error.ResourceLimitExceeded;
    var encoded_len: usize = header_len + @sizeOf(u32);
    for (writes) |write| {
        encoded_len = std.math.add(usize, encoded_len, @sizeOf(u32)) catch return error.ResourceLimitExceeded;
        encoded_len = std.math.add(usize, encoded_len, write.key.len) catch return error.ResourceLimitExceeded;
        if (encoded_len > hard_max_manifest_bytes) return error.ResourceLimitExceeded;
    }
    const out = try alloc.alloc(u8, encoded_len);
    errdefer alloc.free(out);
    @memcpy(out[0..segment_magic.len], segment_magic);
    std.mem.writeInt(u64, out[segment_magic.len..header_len], generation, .big);
    std.mem.writeInt(u32, out[header_len..][0..4], @intCast(writes.len), .big);
    var pos = header_len + @sizeOf(u32);
    for (writes) |write| {
        if (write.key.len > std.math.maxInt(u32)) return error.ResourceLimitExceeded;
        std.mem.writeInt(u32, out[pos..][0..4], @intCast(write.key.len), .big);
        pos += 4;
        @memcpy(out[pos..][0..write.key.len], write.key);
        pos += write.key.len;
    }
    return out;
}

pub fn decodeSegmentKeysAlloc(alloc: Allocator, raw: []const u8, expected_generation: u64) ![][]u8 {
    if (raw.len > hard_max_manifest_bytes) return error.ResourceLimitExceeded;
    if (raw.len < header_len + @sizeOf(u32) or !std.mem.startsWith(u8, raw, segment_magic)) {
        return error.InvalidGraphAssetState;
    }
    if (std.mem.readInt(u64, raw[segment_magic.len..header_len], .big) != expected_generation) {
        return error.InvalidGraphAssetState;
    }
    var pos: usize = header_len;
    const count = try readU32Big(raw, &pos);
    if (count == 0 or @as(usize, count) > hard_max_edges_per_document) return error.ResourceLimitExceeded;
    if (@as(usize, count) > (raw.len - pos) / @sizeOf(u32)) return error.InvalidGraphAssetState;
    const keys = try alloc.alloc([]u8, count);
    var initialized: usize = 0;
    errdefer {
        for (keys[0..initialized]) |key| alloc.free(key);
        alloc.free(keys);
    }
    for (keys) |*key| {
        const key_len = try readU32Big(raw, &pos);
        if (key_len > raw.len - pos) return error.InvalidGraphAssetState;
        key.* = try alloc.dupe(u8, raw[pos..][0..key_len]);
        pos += key_len;
        initialized += 1;
    }
    if (pos != raw.len) return error.InvalidGraphAssetState;
    return keys;
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

test "graph asset state segmented root and blocks round trip" {
    const alloc = std.testing.allocator;
    const Pair = struct { key: []const u8 };
    const root_raw = try encodeSegmentedRootAlloc(alloc, 42, 2, 3);
    defer alloc.free(root_raw);
    try std.testing.expectEqual(Format.v5, try format(root_raw));
    const root = try segmentedRoot(root_raw);
    try std.testing.expectEqual(@as(u64, 42), root.generation);
    try std.testing.expectEqual(@as(u32, 2), root.segment_count);
    try std.testing.expectEqual(@as(u32, 3), root.key_count);

    const segment_raw = try encodeSegmentAlloc(alloc, 42, &[_]Pair{
        .{ .key = "edge:a" },
        .{ .key = "edge:b" },
    });
    defer alloc.free(segment_raw);
    const keys = try decodeSegmentKeysAlloc(alloc, segment_raw, 42);
    defer freeKeys(alloc, keys);
    try std.testing.expectEqualStrings("edge:a", keys[0]);
    try std.testing.expectEqualStrings("edge:b", keys[1]);
}
