// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

const std = @import("std");
const Allocator = std.mem.Allocator;

const version_3_magic = "AGS3";
const header_len = version_3_magic.len + @sizeOf(u64);
pub const hard_max_edges_per_document: usize = 1_000_000;
pub const hard_max_relation_items_per_artifact: usize = 1_000_000;
pub const hard_max_manifest_bytes: usize = 64 * 1024 * 1024;
/// Aggregate guardrails for one precedence-reconciliation pass. A visible-edge
/// limit alone cannot bound work when many sources contain the same identities.
pub const hard_max_reconcile_entries: usize = 4_000_000;
pub const hard_max_reconcile_bytes: usize = 256 * 1024 * 1024;

pub fn effectiveEdgeLimit(configured: u32) usize {
    return if (configured == 0) hard_max_edges_per_document else @min(@as(usize, configured), hard_max_edges_per_document);
}

pub const Format = enum {
    /// v0.2.0 persisted only an edge count followed by length-prefixed keys.
    v0_2_0,
    v3,
};

pub const Entry = struct {
    key: []u8,
    value: []u8,

    pub fn deinit(self: *Entry, alloc: Allocator) void {
        alloc.free(self.key);
        alloc.free(self.value);
        self.* = undefined;
    }
};

pub const EntryView = struct {
    key: []const u8,
    value: []const u8,
};

pub const EntryIterator = struct {
    raw: []const u8,
    pos: usize,
    remaining: u32,

    pub fn next(self: *EntryIterator) !?EntryView {
        if (self.remaining == 0) {
            if (self.pos != self.raw.len) return error.InvalidGraphAssetState;
            return null;
        }
        const key_len = readU32Big(self.raw, &self.pos) catch return error.InvalidGraphAssetState;
        if (key_len > self.raw.len - self.pos) return error.InvalidGraphAssetState;
        const key = self.raw[self.pos..][0..key_len];
        self.pos += key_len;
        const value_len = readU32Big(self.raw, &self.pos) catch return error.InvalidGraphAssetState;
        if (value_len > self.raw.len - self.pos) return error.InvalidGraphAssetState;
        const value = self.raw[self.pos..][0..value_len];
        self.pos += value_len;
        self.remaining -= 1;
        if (self.remaining == 0 and self.pos != self.raw.len) return error.InvalidGraphAssetState;
        return .{ .key = key, .value = value };
    }
};

pub const ReconcileBudget = struct {
    max_entries: usize = hard_max_reconcile_entries,
    max_bytes: usize = hard_max_reconcile_bytes,
    entries: usize = 0,
    bytes: usize = 0,

    pub fn charge(self: *ReconcileBudget, raw: []const u8) !void {
        const count = try v3EntryCount(raw);
        const next_entries = std.math.add(usize, self.entries, count) catch return error.ResourceLimitExceeded;
        const next_bytes = std.math.add(usize, self.bytes, raw.len) catch return error.ResourceLimitExceeded;
        if (next_entries > self.max_entries or next_bytes > self.max_bytes) return error.ResourceLimitExceeded;
        self.entries = next_entries;
        self.bytes = next_bytes;
    }
};

pub fn freeEntries(alloc: Allocator, entries: []Entry) void {
    for (entries) |*entry| entry.deinit(alloc);
    if (entries.len > 0) alloc.free(entries);
}

pub fn freeKeys(alloc: Allocator, keys: [][]u8) void {
    for (keys) |key| alloc.free(key);
    if (keys.len > 0) alloc.free(keys);
}

pub fn format(raw: []const u8) !Format {
    if (raw.len > hard_max_manifest_bytes) return error.ResourceLimitExceeded;
    if (std.mem.startsWith(u8, raw, version_3_magic)) {
        if (raw.len < header_len) return error.InvalidGraphAssetState;
        return .v3;
    }
    // The only released predecessor is v0.2.0's unversioned key-only
    // encoding. Validate the whole payload before treating arbitrary bytes as
    // that format so corrupt state fails closed instead of becoming migration
    // input.
    var pos: usize = 0;
    const count = readU32Big(raw, &pos) catch return error.InvalidGraphAssetState;
    if (@as(usize, count) > hard_max_edges_per_document) return error.ResourceLimitExceeded;
    const minimum_entry_bytes: usize = @sizeOf(u32);
    if (@as(usize, count) > (raw.len - pos) / minimum_entry_bytes) return error.InvalidGraphAssetState;
    for (0..count) |_| {
        const key_len = readU32Big(raw, &pos) catch return error.InvalidGraphAssetState;
        if (key_len > raw.len - pos) return error.InvalidGraphAssetState;
        pos += key_len;
    }
    if (pos != raw.len) return error.InvalidGraphAssetState;
    return .v0_2_0;
}

pub fn decodeV020KeysAlloc(alloc: Allocator, raw: []const u8) ![][]u8 {
    if (try format(raw) != .v0_2_0) return error.UnsupportedGraphAssetStateVersion;
    var pos: usize = 0;
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

/// Stores the owning graph generation and the complete payload for each edge.
/// Payload retention is required to restore the next-precedence source without
/// rescanning when several sources emit the same logical edge key.
pub fn encodeAlloc(alloc: Allocator, generation: u64, writes: anytype) ![]u8 {
    if (writes.len > hard_max_edges_per_document) return error.ResourceLimitExceeded;
    var encoded_len: usize = header_len + @sizeOf(u32);
    for (writes) |write| {
        encoded_len = std.math.add(usize, encoded_len, 2 * @sizeOf(u32)) catch return error.ResourceLimitExceeded;
        encoded_len = std.math.add(usize, encoded_len, write.key.len) catch return error.ResourceLimitExceeded;
        encoded_len = std.math.add(usize, encoded_len, write.value.len) catch return error.ResourceLimitExceeded;
        if (encoded_len > hard_max_manifest_bytes) return error.ResourceLimitExceeded;
    }
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.ensureTotalCapacityPrecise(alloc, encoded_len);
    try out.appendSlice(alloc, version_3_magic);
    var generation_bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &generation_bytes, generation, .big);
    try out.appendSlice(alloc, &generation_bytes);
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
    var iterator = try entryIterator(raw);
    const count = iterator.remaining;
    const entries = if (count > 0) try alloc.alloc(Entry, count) else return &.{};
    var initialized: usize = 0;
    errdefer {
        for (entries[0..initialized]) |*entry| entry.deinit(alloc);
        alloc.free(entries);
    }

    for (entries) |*entry| {
        const view = (try iterator.next()) orelse return error.InvalidGraphAssetState;
        const key = try alloc.dupe(u8, view.key);
        errdefer alloc.free(key);
        const value = try alloc.dupe(u8, view.value);
        entry.* = .{ .key = key, .value = value };
        initialized += 1;
    }
    if (try iterator.next() != null) return error.InvalidGraphAssetState;
    return entries;
}

pub fn entryIterator(raw: []const u8) !EntryIterator {
    if (!std.mem.startsWith(u8, raw, version_3_magic) or raw.len < header_len) return error.UnsupportedGraphAssetStateVersion;
    if (raw.len > hard_max_manifest_bytes) return error.ResourceLimitExceeded;
    var pos: usize = header_len;
    const count = readU32Big(raw, &pos) catch return error.InvalidGraphAssetState;
    if (@as(usize, count) > hard_max_edges_per_document) return error.ResourceLimitExceeded;
    const minimum_entry_bytes: usize = 2 * @sizeOf(u32);
    if (@as(usize, count) > (raw.len - pos) / minimum_entry_bytes) return error.InvalidGraphAssetState;
    return .{ .raw = raw, .pos = pos, .remaining = count };
}

fn v3EntryCount(raw: []const u8) !usize {
    const iterator = try entryIterator(raw);
    return iterator.remaining;
}

pub fn containsKey(raw: []const u8, target_key: []const u8) !bool {
    const state_format = try format(raw);
    var pos: usize = if (state_format == .v3) header_len else 0;
    const count = readU32Big(raw, &pos) catch return error.InvalidGraphAssetState;
    if (@as(usize, count) > hard_max_edges_per_document) return error.ResourceLimitExceeded;
    const minimum_entry_bytes: usize = if (state_format == .v3) 2 * @sizeOf(u32) else @sizeOf(u32);
    if (@as(usize, count) > (raw.len - pos) / minimum_entry_bytes) return error.InvalidGraphAssetState;
    var found = false;
    for (0..count) |_| {
        const key_len = readU32Big(raw, &pos) catch return error.InvalidGraphAssetState;
        if (key_len > raw.len - pos) return error.InvalidGraphAssetState;
        const matches = std.mem.eql(u8, raw[pos..][0..key_len], target_key);
        pos += key_len;
        if (state_format == .v3) {
            const value_len = readU32Big(raw, &pos) catch return error.InvalidGraphAssetState;
            if (value_len > raw.len - pos) return error.InvalidGraphAssetState;
            pos += value_len;
        }
        found = found or matches;
    }
    if (pos != raw.len) return error.InvalidGraphAssetState;
    return found;
}

pub fn coverageGeneration(raw: []const u8) !?u64 {
    return switch (try format(raw)) {
        .v0_2_0 => null,
        .v3 => std.mem.readInt(u64, raw[version_3_magic.len..][0..@sizeOf(u64)], .big),
    };
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

test "graph asset state v3 round trip preserves generation and payloads" {
    const alloc = std.testing.allocator;
    const Pair = struct { key: []const u8, value: []const u8 };
    const raw = try encodeAlloc(alloc, 42, &[_]Pair{
        .{ .key = "edge:a", .value = "payload:a" },
        .{ .key = "edge:b", .value = "payload:b" },
    });
    defer alloc.free(raw);
    const entries = try decodeAlloc(alloc, raw);
    defer freeEntries(alloc, entries);
    try std.testing.expectEqualStrings("edge:a", entries[0].key);
    try std.testing.expectEqualStrings("payload:a", entries[0].value);
    try std.testing.expect(try containsKey(raw, "edge:b"));
    try std.testing.expectEqual(@as(u64, 42), (try coverageGeneration(raw)).?);
}

test "graph asset state reads v0.2.0 key-only manifests" {
    const alloc = std.testing.allocator;
    const raw = [_]u8{ 0, 0, 0, 1, 0, 0, 0, 6 } ++ "edge:a";
    try std.testing.expectEqual(Format.v0_2_0, try format(raw));
    try std.testing.expect((try coverageGeneration(raw)) == null);
    const keys = try decodeV020KeysAlloc(alloc, raw);
    defer freeKeys(alloc, keys);
    try std.testing.expectEqual(@as(usize, 1), keys.len);
    try std.testing.expectEqualStrings("edge:a", keys[0]);
    try std.testing.expect(try containsKey(raw, "edge:a"));
    try std.testing.expectError(error.UnsupportedGraphAssetStateVersion, decodeAlloc(alloc, raw));
}

test "graph asset state rejects excessive entry counts before allocation" {
    const alloc = std.testing.allocator;
    const raw = version_3_magic ++ [_]u8{0} ** 8 ++ [_]u8{ 0xff, 0xff, 0xff, 0xff };
    try std.testing.expectError(error.ResourceLimitExceeded, decodeAlloc(alloc, raw));
    try std.testing.expectError(error.ResourceLimitExceeded, containsKey(raw, "edge:a"));
}

test "graph asset state applies a finite safety limit when zero is configured" {
    try std.testing.expectEqual(hard_max_edges_per_document, effectiveEdgeLimit(0));
    try std.testing.expectEqual(@as(usize, 25), effectiveEdgeLimit(25));
}

test "graph reconciliation budget bounds overlapping manifest work" {
    const alloc = std.testing.allocator;
    const Pair = struct { key: []const u8, value: []const u8 };
    const raw = try encodeAlloc(alloc, 42, &[_]Pair{
        .{ .key = "edge:a", .value = "payload:a" },
        .{ .key = "edge:b", .value = "payload:b" },
    });
    defer alloc.free(raw);

    var budget = ReconcileBudget{ .max_entries = 3, .max_bytes = raw.len * 2 };
    try budget.charge(raw);
    try std.testing.expectError(error.ResourceLimitExceeded, budget.charge(raw));
}
