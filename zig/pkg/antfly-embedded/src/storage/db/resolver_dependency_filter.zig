// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Adaptive dependency summaries. Small roots fit in one compact row; larger
//! roots split sorted fingerprint leaves with bounded native writes. Sixty-four
//! SHA-derived bits are only a negative proof: every hit requires an exact
//! full-SHA dependency lookup. Reference counts preserve collisions on deletion.
const std = @import("std");
const records = @import("resolver_authority.zig");
const A = std.mem.Allocator;
pub const leaf_capacity = 128;
const read_page_leaves = 128;
const Entry = struct { fingerprint: u64, refs: u32 };
const Entries = struct {
    len: usize = 0,
    items: [leaf_capacity + 1]Entry = undefined,
    fn find(self: *const Entries, fp: u64) usize {
        var lo: usize = 0;
        var hi = self.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (self.items[mid].fingerprint < fp) lo = mid + 1 else hi = mid;
        }
        return lo;
    }
    fn mutate(self: *Entries, fp: u64, add: bool) !bool {
        const i = self.find(fp);
        if (i < self.len and self.items[i].fingerprint == fp) {
            if (add) {
                self.items[i].refs = try std.math.add(u32, self.items[i].refs, 1);
                return false;
            }
            self.items[i].refs -= 1;
            if (self.items[i].refs != 0) return false;
            std.mem.copyForwards(Entry, self.items[i .. self.len - 1], self.items[i + 1 .. self.len]);
            self.len -= 1;
            return true;
        }
        if (!add or self.len > leaf_capacity) return error.InvalidCatalogRecord;
        std.mem.copyBackwards(Entry, self.items[i + 1 .. self.len + 1], self.items[i..self.len]);
        self.items[i] = .{ .fingerprint = fp, .refs = 1 };
        self.len += 1;
        return true;
    }
    fn append(self: *Entries, other: Entries) !void {
        if (self.len + other.len > leaf_capacity) return error.InvalidCatalogRecord;
        if (self.len != 0 and other.len != 0 and self.items[self.len - 1].fingerprint >= other.items[0].fingerprint) return error.InvalidCatalogRecord;
        @memcpy(self.items[self.len..][0..other.len], other.items[0..other.len]);
        self.len += other.len;
    }
};
const Meta = struct { refs: u64 = 0, distinct: u32 = 0, leaves: u32 = 0, entries: Entries = .{} };
fn fingerprint(digest: *const [32]u8) u64 {
    return std.mem.readInt(u64, digest[0..8], .big);
}
fn decodeEntries(raw: []const u8, count: usize) !Entries {
    if (count > leaf_capacity or raw.len != count * 12) return error.InvalidCatalogRecord;
    var value: Entries = .{};
    for (0..count) |i| {
        const e: Entry = .{ .fingerprint = std.mem.readInt(u64, raw[i * 12 ..][0..8], .big), .refs = std.mem.readInt(u32, raw[i * 12 + 8 ..][0..4], .little) };
        if (e.refs == 0 or (i != 0 and value.items[i - 1].fingerprint >= e.fingerprint)) return error.InvalidCatalogRecord;
        value.items[i] = e;
        value.len += 1;
    }
    return value;
}
fn writeEntries(raw: []u8, entries: Entries) void {
    for (entries.items[0..entries.len], 0..) |e, i| {
        std.mem.writeInt(u64, raw[i * 12 ..][0..8], e.fingerprint, .big);
        std.mem.writeInt(u32, raw[i * 12 + 8 ..][0..4], e.refs, .little);
    }
}
fn decodeMeta(raw: []const u8) !Meta {
    if (raw.len < 24 or raw[0] != 1 or raw[1] > 1 or !std.mem.allEqual(u8, raw[12..16], 0)) return error.InvalidCatalogRecord;
    const len = std.mem.readInt(u16, raw[2..4], .little);
    const m: Meta = .{ .distinct = std.mem.readInt(u32, raw[4..8], .little), .leaves = std.mem.readInt(u32, raw[8..12], .little), .refs = std.mem.readInt(u64, raw[16..24], .little), .entries = try decodeEntries(raw[24..], len) };
    if (m.distinct == 0 or m.refs < m.distinct) return error.InvalidCatalogRecord;
    if (raw[1] == 0) {
        if (m.leaves != 0 or m.distinct != len) return error.InvalidCatalogRecord;
        var refs: u64 = 0;
        for (m.entries.items[0..m.entries.len]) |e| refs += e.refs;
        if (refs != m.refs) return error.InvalidCatalogRecord;
    } else if (len != 0 or m.leaves == 0 or @as(u64, m.leaves) > @as(u64, m.distinct) + 1) return error.InvalidCatalogRecord;
    return m;
}
fn encodeMeta(a: A, m: Meta) ![]u8 {
    const out = try a.alloc(u8, 24 + m.entries.len * 12);
    @memset(out, 0);
    out[0] = 1;
    out[1] = if (m.leaves == 0) 0 else 1;
    std.mem.writeInt(u16, out[2..4], @intCast(m.entries.len), .little);
    std.mem.writeInt(u32, out[4..8], m.distinct, .little);
    std.mem.writeInt(u32, out[8..12], m.leaves, .little);
    std.mem.writeInt(u64, out[16..24], m.refs, .little);
    writeEntries(out[24..], m.entries);
    return out;
}
fn decodeLeaf(raw: []const u8, upper: u64) !Entries {
    if (raw.len < 4 or raw[0] != 2 or raw[1] != 0) return error.InvalidCatalogRecord;
    const value = try decodeEntries(raw[4..], std.mem.readInt(u16, raw[2..4], .little));
    if ((value.len == 0 and upper != std.math.maxInt(u64)) or (value.len != 0 and value.items[value.len - 1].fingerprint > upper)) return error.InvalidCatalogRecord;
    return value;
}
fn encodeLeaf(a: A, e: Entries) ![]u8 {
    const out = try a.alloc(u8, 4 + e.len * 12);
    out[0] = 2;
    out[1] = 0;
    std.mem.writeInt(u16, out[2..4], @intCast(e.len), .little);
    writeEntries(out[4..], e);
    return out;
}
pub fn validateRoot(raw: []const u8) !void {
    _ = try decodeMeta(raw);
}
pub fn validateLeaf(raw: []const u8, upper: u64) !void {
    _ = try decodeLeaf(raw, upper);
}
const Leaf = struct { key: []const u8, upper: u64, entries: Entries };
fn parseLeaf(prefix: []const u8, row: records.Row) !Leaf {
    if (!std.mem.startsWith(u8, row.key, prefix) or row.key.len != prefix.len + 16) return error.InvalidCatalogRecord;
    const upper = std.fmt.parseInt(u64, row.key[prefix.len..], 16) catch return error.InvalidCatalogRecord;
    return .{ .key = row.key, .upper = upper, .entries = try decodeLeaf(row.value, upper) };
}
fn ceiling(backend: records.Backend, a: A, prefix: []const u8, fp: u64) !Leaf {
    const after = if (fp == 0) null else try std.fmt.allocPrint(a, "{s}{x:0>16}", .{ prefix, fp - 1 });
    const page = try backend.scan(backend.ptr, a, prefix, after, 1);
    if (page.rows.len != 1) return error.InvalidCatalogRecord;
    return parseLeaf(prefix, page.rows[0]);
}
const CachedLeaf = struct { upper: u64, entries: Entries };
pub const Reader = struct {
    backend: records.Backend,
    prefix: []const u8,
    meta: Meta,
    pages: []CachedLeaf = &.{},
    loaded: usize = 0,
    cursor: usize = 0,
    last: ?u64 = null,
    /// Sorted full-SHA inputs stream bounded leaf pages. Scalar cached entries
    /// survive reuse of the caller's I/O scratch between successive digests.
    pub fn contains(self: *Reader, a: A, digest: *const [32]u8) !bool {
        const fp = fingerprint(digest);
        if (self.meta.leaves == 0) {
            const i = self.meta.entries.find(fp);
            return i < self.meta.entries.len and self.meta.entries.items[i].fingerprint == fp;
        }
        if (self.loaded == 0 or fp > self.pages[self.loaded - 1].upper or (self.last != null and fp < self.last.?)) {
            const after = if (fp == 0) null else try std.fmt.allocPrint(a, "{s}{x:0>16}", .{ self.prefix, fp - 1 });
            const page = try self.backend.scan(self.backend.ptr, a, self.prefix, after, @intCast(self.pages.len));
            if (page.rows.len == 0 or page.rows.len > self.pages.len) return error.InvalidCatalogRecord;
            for (page.rows, 0..) |row, i| {
                const leaf = try parseLeaf(self.prefix, row);
                if (i != 0 and self.pages[i - 1].upper >= leaf.upper) return error.InvalidCatalogRecord;
                self.pages[i] = .{ .upper = leaf.upper, .entries = leaf.entries };
            }
            self.loaded = page.rows.len;
            self.cursor = 0;
        }
        while (fp > self.pages[self.cursor].upper) self.cursor += 1;
        self.last = fp;
        const entries = &self.pages[self.cursor].entries;
        const i = entries.find(fp);
        return i < entries.len and entries.items[i].fingerprint == fp;
    }
};
pub const Index = struct {
    a: A,
    backend: records.Backend,
    root: []const u8,
    fn put(self: Index, key: []const u8, raw: []const u8) !void {
        try self.backend.put(self.backend.ptr, self.a, try self.a.dupe(u8, key), raw);
    }
    fn remove(self: Index, key: []const u8) !void {
        try self.backend.remove(self.backend.ptr, self.a, try self.a.dupe(u8, key));
    }
    fn leafKey(a: A, prefix: []const u8, upper: u64) ![]const u8 {
        return std.fmt.allocPrint(a, "{s}{x:0>16}", .{ prefix, upper });
    }
    fn putLeaf(self: Index, prefix: []const u8, upper: u64, entries: Entries) !void {
        try self.put(try leafKey(self.a, prefix, upper), try encodeLeaf(self.a, entries));
    }
    /// At most four native writes. Splits, local merges and small-root collapse
    /// are atomic within the caller's writer transaction; no whole-root rebuild.
    pub fn update(self: Index, root_digest: []const u8, digest: *const [32]u8, add: bool) !void {
        var bytes: [16 * 1024]u8 = undefined;
        var frame: std.heap.FixedBufferAllocator = .init(&bytes);
        const a = frame.allocator();
        const key = try std.fmt.allocPrint(a, "{s}dep-summary/{s}", .{ self.root, root_digest });
        const prefix = try std.fmt.allocPrint(a, "{s}dep-leaf/{s}/", .{ self.root, root_digest });
        const raw = try self.backend.get(self.backend.ptr, a, key);
        var m: Meta = if (raw) |value| try decodeMeta(value) else .{};
        if (!add and raw == null) return error.InvalidCatalogRecord;
        const fp = fingerprint(digest);
        var leaf: Leaf = if (m.leaves == 0) .{ .key = key, .upper = std.math.maxInt(u64), .entries = m.entries } else try ceiling(self.backend, a, prefix, fp);
        const changed = try leaf.entries.mutate(fp, add);
        if (add) m.refs = try std.math.add(u64, m.refs, 1) else m.refs = try std.math.sub(u64, m.refs, 1);
        if (changed) m.distinct = if (add) try std.math.add(u32, m.distinct, 1) else try std.math.sub(u32, m.distinct, 1);
        if (m.distinct == 0) {
            if (m.refs != 0 or m.leaves > 2) return error.InvalidCatalogRecord;
            if (m.leaves != 0) {
                try self.remove(leaf.key);
                if (leaf.upper != std.math.maxInt(u64)) try self.remove(try leafKey(a, prefix, std.math.maxInt(u64)));
            }
            try self.remove(key);
            return;
        }
        if (m.refs < m.distinct) return error.InvalidCatalogRecord;
        if (m.leaves == 0) {
            if (leaf.entries.len <= leaf_capacity) {
                m.entries = leaf.entries;
                try self.put(key, try encodeMeta(self.a, m));
                return;
            }
            m.leaves = 2;
        } else if (m.distinct <= leaf_capacity and m.leaves <= 3) {
            const page = try self.backend.scan(self.backend.ptr, a, prefix, null, 4);
            if (page.rows.len != m.leaves) return error.InvalidCatalogRecord;
            var merged: Entries = .{};
            for (page.rows) |row| {
                const old = try parseLeaf(prefix, row);
                try merged.append(if (old.upper == leaf.upper) leaf.entries else old.entries);
            }
            if (merged.len != m.distinct) return error.InvalidCatalogRecord;
            m.leaves = 0;
            m.entries = merged;
            try self.put(key, try encodeMeta(self.a, m));
            for (page.rows) |row| try self.remove(row.key);
            return;
        } else if (leaf.entries.len <= leaf_capacity) {
            if (leaf.entries.len == 0 and leaf.upper != std.math.maxInt(u64)) {
                try self.remove(leaf.key);
                m.leaves -= 1;
            } else {
                var merged = false;
                if (!add and leaf.entries.len < leaf_capacity / 2) {
                    const next = try self.backend.scan(self.backend.ptr, a, prefix, leaf.key, 1);
                    if (next.rows.len != 0) {
                        const sibling = try parseLeaf(prefix, next.rows[0]);
                        if (leaf.entries.len + sibling.entries.len <= leaf_capacity) {
                            try leaf.entries.append(sibling.entries);
                            try self.putLeaf(prefix, sibling.upper, leaf.entries);
                            try self.remove(leaf.key);
                            m.leaves -= 1;
                            merged = true;
                        }
                    }
                }
                if (!merged) try self.putLeaf(prefix, leaf.upper, leaf.entries);
            }
            try self.put(key, try encodeMeta(self.a, m));
            return;
        } else m.leaves = try std.math.add(u32, m.leaves, 1);
        // A full leaf splits into two sorted intervals. Reuse its old upper
        // bound for the right sibling, so a split requires only two page puts.
        const half = leaf.entries.len / 2;
        var left: Entries = .{};
        left.len = half;
        @memcpy(left.items[0..half], leaf.entries.items[0..half]);
        var right: Entries = .{};
        right.len = leaf.entries.len - half;
        @memcpy(right.items[0..right.len], leaf.entries.items[half..leaf.entries.len]);
        try self.putLeaf(prefix, left.items[half - 1].fingerprint, left);
        try self.putLeaf(prefix, leaf.upper, right);
        m.entries = .{};
        try self.put(key, try encodeMeta(self.a, m));
    }
    pub fn reader(self: Index, a: A, root_digest: []const u8) !?Reader {
        const key = try std.fmt.allocPrint(a, "{s}dep-summary/{s}", .{ self.root, root_digest });
        const raw = (try self.backend.get(self.backend.ptr, a, key)) orelse return null;
        const meta = try decodeMeta(raw);
        return .{ .backend = self.backend, .prefix = try std.fmt.allocPrint(a, "{s}dep-leaf/{s}/", .{ self.root, root_digest }), .meta = meta, .pages = if (meta.leaves == 0) &.{} else try a.alloc(CachedLeaf, @min(read_page_leaves, meta.leaves)) };
    }
};
