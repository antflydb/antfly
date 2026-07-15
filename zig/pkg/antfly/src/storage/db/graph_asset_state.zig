// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

const std = @import("std");
const Allocator = std.mem.Allocator;

const version_2_magic = "AGS2";

pub const Entry = struct {
    key: []u8,
    value: []u8,

    pub fn deinit(self: *Entry, alloc: Allocator) void {
        alloc.free(self.key);
        alloc.free(self.value);
        self.* = undefined;
    }
};

pub fn freeEntries(alloc: Allocator, entries: []Entry) void {
    for (entries) |*entry| entry.deinit(alloc);
    if (entries.len > 0) alloc.free(entries);
}

/// Version 2 stores the materialized payload alongside each edge key. This is
/// required to deterministically restore a surviving source when two graph
/// sources emit the same logical edge with different metadata or weights.
pub fn encodeAlloc(alloc: Allocator, writes: anytype) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, version_2_magic);
    try appendLength(&out, alloc, writes.len);
    for (writes) |write| {
        try appendLength(&out, alloc, write.key.len);
        try out.appendSlice(alloc, write.key);
        try appendLength(&out, alloc, write.value.len);
        try out.appendSlice(alloc, write.value);
    }
    return try out.toOwnedSlice(alloc);
}

pub fn decodeAlloc(alloc: Allocator, raw: []const u8) ![]Entry {
    if (!std.mem.startsWith(u8, raw, version_2_magic)) return error.UnsupportedGraphAssetStateVersion;
    var pos: usize = version_2_magic.len;
    const count = readU32Big(raw, &pos) catch return error.InvalidGraphAssetState;
    const minimum_entry_bytes: usize = 2 * @sizeOf(u32);
    if (@as(usize, count) > (raw.len - pos) / minimum_entry_bytes) return error.InvalidGraphAssetState;
    const entries = if (count > 0) try alloc.alloc(Entry, count) else return &.{};
    var initialized: usize = 0;
    errdefer {
        for (entries[0..initialized]) |*entry| entry.deinit(alloc);
        alloc.free(entries);
    }

    for (entries) |*entry| {
        const key_len = readU32Big(raw, &pos) catch return error.InvalidGraphAssetState;
        if (key_len > raw.len - pos) return error.InvalidGraphAssetState;
        const key = try alloc.dupe(u8, raw[pos..][0..key_len]);
        pos += key_len;
        errdefer alloc.free(key);

        const value_len = readU32Big(raw, &pos) catch return error.InvalidGraphAssetState;
        if (value_len > raw.len - pos) return error.InvalidGraphAssetState;
        const value = try alloc.dupe(u8, raw[pos..][0..value_len]);
        pos += value_len;
        entry.* = .{ .key = key, .value = value };
        initialized += 1;
    }
    if (pos != raw.len) return error.InvalidGraphAssetState;
    return entries;
}

pub fn containsKey(raw: []const u8, target_key: []const u8) !bool {
    if (!std.mem.startsWith(u8, raw, version_2_magic)) return error.UnsupportedGraphAssetStateVersion;
    var pos: usize = version_2_magic.len;
    const count = readU32Big(raw, &pos) catch return error.InvalidGraphAssetState;
    const minimum_entry_bytes: usize = 2 * @sizeOf(u32);
    if (@as(usize, count) > (raw.len - pos) / minimum_entry_bytes) return error.InvalidGraphAssetState;
    var found = false;
    for (0..count) |_| {
        const key_len = readU32Big(raw, &pos) catch return error.InvalidGraphAssetState;
        if (key_len > raw.len - pos) return error.InvalidGraphAssetState;
        const matches = std.mem.eql(u8, raw[pos..][0..key_len], target_key);
        pos += key_len;
        const value_len = readU32Big(raw, &pos) catch return error.InvalidGraphAssetState;
        if (value_len > raw.len - pos) return error.InvalidGraphAssetState;
        pos += value_len;
        found = found or matches;
    }
    if (pos != raw.len) return error.InvalidGraphAssetState;
    return found;
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

test "graph asset state v2 round trip preserves payloads" {
    const alloc = std.testing.allocator;
    const Pair = struct { key: []const u8, value: []const u8 };
    const raw = try encodeAlloc(alloc, &[_]Pair{
        .{ .key = "edge:a", .value = "payload:a" },
        .{ .key = "edge:b", .value = "payload:b" },
    });
    defer alloc.free(raw);
    const entries = try decodeAlloc(alloc, raw);
    defer freeEntries(alloc, entries);
    try std.testing.expectEqualStrings("edge:a", entries[0].key);
    try std.testing.expectEqualStrings("payload:a", entries[0].value);
    try std.testing.expect(try containsKey(raw, "edge:b"));
}

test "graph asset state rejects legacy key-only manifests" {
    const alloc = std.testing.allocator;
    const raw = &[_]u8{ 0, 0, 0, 1, 0, 0, 0, 6 } ++ "edge:a";
    try std.testing.expectError(error.UnsupportedGraphAssetStateVersion, decodeAlloc(alloc, raw));
    try std.testing.expectError(error.UnsupportedGraphAssetStateVersion, containsKey(raw, "edge:a"));
}

test "graph asset state rejects impossible entry counts before allocation" {
    const alloc = std.testing.allocator;
    const raw = version_2_magic ++ [_]u8{ 0xff, 0xff, 0xff, 0xff };
    try std.testing.expectError(error.InvalidGraphAssetState, decodeAlloc(alloc, raw));
    try std.testing.expectError(error.InvalidGraphAssetState, containsKey(raw, "edge:a"));
}
