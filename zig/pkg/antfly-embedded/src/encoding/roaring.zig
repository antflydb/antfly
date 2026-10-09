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

//! Roaring Bitmap: compressed bitmap for document ID sets.
//!
//! Compatible with the standard roaring bitmap serialization; used for posting lists.
//! Uses SIMD (@Vector(8, u64)) for bulk bitwise operations on bitmap containers.
//!
//! Roaring bitmaps partition the 32-bit space into 16-bit "chunks" (high 16 bits).
//! Each chunk uses one of two container types:
//!   - Array container: sorted list of u16 values (sparse, < 4096 elements)
//!   - Bitmap container: 1024 u64 words = 65536 bits (dense, >= 4096 elements)
//!
//! Threshold: 4096 elements (same memory footprint for both representations).

const std = @import("std");
const Allocator = std.mem.Allocator;

/// Container threshold: switch from array to bitmap at this cardinality.
const array_max = 4096;

/// Number of u64 words in a bitmap container (65536 bits / 64).
const bitmap_words = 1024;

/// A single container holding values for one 16-bit chunk.
const Container = union(enum) {
    array: std.ArrayListUnmanaged(u16),
    bitmap: []u64, // always bitmap_words long

    pub fn deinit(self: *Container, alloc: Allocator) void {
        switch (self.*) {
            .array => |*a| a.deinit(alloc),
            .bitmap => |b| alloc.free(b),
        }
    }

    fn cardinality(self: *const Container) usize {
        return switch (self.*) {
            .array => |*a| a.items.len,
            .bitmap => |b| bitmapPopcount(b),
        };
    }

    fn contains(self: *const Container, val: u16) bool {
        return switch (self.*) {
            .array => |*a| arrayContains(a.items, val),
            .bitmap => |b| bitmapGet(b, val),
        };
    }

    /// Number of members strictly less than `val`. O(log n) for arrays
    /// (binary search), O(bitmap_words) for bitmap containers (popcount over
    /// a fixed-size prefix; constant work in practice).
    fn rankBelow(self: *const Container, val: u16) usize {
        return switch (self.*) {
            .array => |*a| arraySearchPos(a.items, val),
            .bitmap => |b| bitmapPopcountBelow(b, val),
        };
    }

    fn add(self: *Container, alloc: Allocator, val: u16) !void {
        switch (self.*) {
            .array => |*a| {
                // Binary search for insertion point
                const pos = arraySearchPos(a.items, val);
                if (pos < a.items.len and a.items[pos] == val) return; // already present

                try a.insert(alloc, pos, val);

                // Convert to bitmap if threshold exceeded
                if (a.items.len > array_max) {
                    const bm = try alloc.alloc(u64, bitmap_words);
                    @memset(bm, 0);
                    for (a.items) |v| bitmapSet(bm, v);
                    a.deinit(alloc);
                    self.* = .{ .bitmap = bm };
                }
            },
            .bitmap => |b| bitmapSet(b, val),
        }
    }

    /// Append a strictly-ascending run of values that share the same high-16 key.
    /// `run` carries the high-16 redundantly in the upper bits; we only use
    /// the low 16 bits per element. Caller asserts `run` is sorted ascending and
    /// internally unique. May collide with existing items, which are deduped.
    fn appendSortedAscending(self: *Container, alloc: Allocator, run: []const u32) !void {
        if (run.len == 0) return;
        switch (self.*) {
            .array => |*a| {
                // Hot path: container empty (most posting-list terms in a sorted
                // build land here exactly once per chunk). Fill array directly.
                if (a.items.len == 0) {
                    if (run.len <= array_max) {
                        try a.ensureTotalCapacity(alloc, run.len);
                        for (run) |v| a.appendAssumeCapacity(@truncate(v));
                        return;
                    }
                    // Run alone exceeds array threshold → go straight to bitmap.
                    const bm = try alloc.alloc(u64, bitmap_words);
                    @memset(bm, 0);
                    for (run) |v| bitmapSet(bm, @truncate(v));
                    a.deinit(alloc);
                    self.* = .{ .bitmap = bm };
                    return;
                }
                // Existing array — keep it sorted. Each new value either extends
                // the tail (when last < new) or falls back to the regular path.
                var last_low: u16 = a.items[a.items.len - 1];
                var k: usize = 0;
                // Fast tail-append run.
                while (k < run.len) {
                    const new_low: u16 = @truncate(run[k]);
                    if (new_low <= last_low) break;
                    try a.append(alloc, new_low);
                    last_low = new_low;
                    k += 1;
                    if (a.items.len > array_max) {
                        // Promote to bitmap and finish remaining via bitmapSet.
                        const bm = try alloc.alloc(u64, bitmap_words);
                        @memset(bm, 0);
                        for (a.items) |v| bitmapSet(bm, v);
                        a.deinit(alloc);
                        self.* = .{ .bitmap = bm };
                        for (run[k..]) |v| bitmapSet(bm, @truncate(v));
                        return;
                    }
                }
                // Anything remaining could collide with existing items; defer to
                // the per-value path which handles dedup + insertion order.
                while (k < run.len) : (k += 1) {
                    try self.add(alloc, @truncate(run[k]));
                }
            },
            .bitmap => |b| {
                for (run) |v| bitmapSet(b, @truncate(v));
            },
        }
    }

    fn remove(self: *Container, alloc: Allocator, val: u16) !void {
        switch (self.*) {
            .array => |*a| {
                const pos = arraySearchPos(a.items, val);
                if (pos < a.items.len and a.items[pos] == val) {
                    _ = a.orderedRemove(pos);
                }
            },
            .bitmap => |b| {
                bitmapUnset(b, val);
                // Convert back to array if below threshold
                if (bitmapPopcount(b) <= array_max) {
                    var arr: std.ArrayListUnmanaged(u16) = .empty;
                    errdefer arr.deinit(alloc);
                    try arr.ensureTotalCapacity(alloc, bitmapPopcount(b));
                    var iter = bitmapIterator(b);
                    while (iter.next()) |v| {
                        arr.appendAssumeCapacity(v);
                    }
                    alloc.free(b);
                    self.* = .{ .array = arr };
                }
            },
        }
    }
};

// ============================================================================
// Array helpers
// ============================================================================

fn arrayContains(items: []const u16, val: u16) bool {
    return arrayContainsSimdQuad(items, val);
}

fn arrayContainsSimdQuad(items: []const u16, val: u16) bool {
    const gap: usize = 16;
    if (items.len < gap) {
        for (items) |item| {
            if (item == val) return true;
            if (item > val) return false;
        }
        return false;
    }

    const num_blocks = items.len / gap;
    var base: usize = 0;
    var n: usize = num_blocks;
    while (n > 3) {
        const quarter = n >> 2;
        const k1 = items[(base + quarter + 1) * gap - 1];
        const k2 = items[(base + 2 * quarter + 1) * gap - 1];
        const k3 = items[(base + 3 * quarter + 1) * gap - 1];

        base += @as(usize, @intFromBool(k1 < val)) * quarter;
        base += @as(usize, @intFromBool(k2 < val)) * quarter;
        base += @as(usize, @intFromBool(k3 < val)) * quarter;
        n -= 3 * quarter;
    }

    while (n > 1) {
        const half = n >> 1;
        if (items[(base + half + 1) * gap - 1] < val) {
            base += half;
        }
        n -= half;
    }

    const block_index = if (items[(base + 1) * gap - 1] < val) base + 1 else base;
    if (block_index < num_blocks) {
        const block: @Vector(gap, u16) = items[block_index * gap ..][0..gap].*;
        const needle: @Vector(gap, u16) = @splat(val);
        return @reduce(.Or, block == needle);
    }

    for (items[num_blocks * gap ..]) |item| {
        if (item == val) return true;
        if (item > val) return false;
    }
    return false;
}

fn arrayContainsBinary(items: []const u16, val: u16) bool {
    var lo: usize = 0;
    var hi: usize = items.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (items[mid] < val) {
            lo = mid + 1;
        } else if (items[mid] > val) {
            hi = mid;
        } else {
            return true;
        }
    }
    return false;
}

/// Returns the insertion point for val in sorted items.
fn arraySearchPos(items: []const u16, val: u16) usize {
    var lo: usize = 0;
    var hi: usize = items.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (items[mid] < val) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    return lo;
}

// ============================================================================
// Bitmap helpers (SIMD-accelerated)
// ============================================================================

fn bitmapGet(words: []const u64, val: u16) bool {
    const word_idx = val >> 6;
    const bit_idx: u6 = @truncate(val);
    return (words[word_idx] & (@as(u64, 1) << bit_idx)) != 0;
}

fn bitmapSet(words: []u64, val: u16) void {
    const word_idx = val >> 6;
    const bit_idx: u6 = @truncate(val);
    words[word_idx] |= @as(u64, 1) << bit_idx;
}

fn bitmapUnset(words: []u64, val: u16) void {
    const word_idx = val >> 6;
    const bit_idx: u6 = @truncate(val);
    words[word_idx] &= ~(@as(u64, 1) << bit_idx);
}

/// Number of set bits in `words` strictly below bit index `target`. Used by
/// rank queries during posting iterator seeks. Walks whole words below the
/// target word, then masks the last word to count only bits before `target`.
fn bitmapPopcountBelow(words: []const u64, target: u16) usize {
    const word_idx: usize = target >> 6;
    const bit_idx: u6 = @truncate(target);
    var count: usize = 0;
    var i: usize = 0;
    while (i < word_idx) : (i += 1) count += @popCount(words[i]);
    if (word_idx < bitmap_words and bit_idx != 0) {
        const mask: u64 = (~@as(u64, 0)) >> @intCast(64 - @as(u32, bit_idx));
        count += @popCount(words[word_idx] & mask);
    }
    return count;
}

/// SIMD popcount using @Vector(8, u64) (512-bit).
fn bitmapPopcount(words: []const u64) usize {
    var count: usize = 0;
    var i: usize = 0;
    while (i + 8 <= bitmap_words) : (i += 8) {
        const v: @Vector(8, u64) = words[i..][0..8].*;
        const popcounts: @Vector(8, usize) = @intCast(@popCount(v));
        count += @reduce(.Add, popcounts);
    }
    while (i < bitmap_words) : (i += 1) {
        count += @popCount(words[i]);
    }
    return count;
}

const BitmapIter = struct {
    words: []const u64,
    word_idx: usize,
    current: u64,

    fn next(self: *BitmapIter) ?u16 {
        while (self.current == 0) {
            self.word_idx += 1;
            if (self.word_idx >= bitmap_words) return null;
            self.current = self.words[self.word_idx];
        }
        const bit: u6 = @intCast(@ctz(self.current));
        self.current &= self.current - 1;
        return @as(u16, @intCast(self.word_idx)) * 64 + bit;
    }
};

fn bitmapIterator(words: []const u64) BitmapIter {
    return .{ .words = words, .word_idx = 0, .current = words[0] };
}

// ============================================================================
// SIMD bulk bitmap operations
// ============================================================================

fn bitmapAndSimd(dst: []u64, src: []const u64) void {
    var i: usize = 0;
    while (i + 8 <= bitmap_words) : (i += 8) {
        const a: @Vector(8, u64) = dst[i..][0..8].*;
        const b: @Vector(8, u64) = src[i..][0..8].*;
        dst[i..][0..8].* = a & b;
    }
    while (i < bitmap_words) : (i += 1) dst[i] &= src[i];
}

fn bitmapOrSimd(dst: []u64, src: []const u64) void {
    var i: usize = 0;
    while (i + 8 <= bitmap_words) : (i += 8) {
        const a: @Vector(8, u64) = dst[i..][0..8].*;
        const b: @Vector(8, u64) = src[i..][0..8].*;
        dst[i..][0..8].* = a | b;
    }
    while (i < bitmap_words) : (i += 1) dst[i] |= src[i];
}

fn bitmapAndNotSimd(dst: []u64, src: []const u64) void {
    var i: usize = 0;
    while (i + 8 <= bitmap_words) : (i += 8) {
        const a: @Vector(8, u64) = dst[i..][0..8].*;
        const b: @Vector(8, u64) = src[i..][0..8].*;
        dst[i..][0..8].* = a & ~b;
    }
    while (i < bitmap_words) : (i += 1) dst[i] &= ~src[i];
}

// ============================================================================
// Container-level set operations
// ============================================================================

fn andContainers(alloc: Allocator, self: *Container, other: *const Container) void {
    switch (self.*) {
        .bitmap => |sb| switch (other.*) {
            .bitmap => |ob| bitmapAndSimd(sb, ob),
            .array => |*oa| {
                // bitmap & array -> filter array values present in bitmap
                var arr: std.ArrayListUnmanaged(u16) = .empty;
                for (oa.items) |v| {
                    if (bitmapGet(sb, v)) arr.append(alloc, v) catch unreachable;
                }
                alloc.free(sb);
                self.* = .{ .array = arr };
            },
        },
        .array => |*sa| switch (other.*) {
            .bitmap => |ob| {
                // array & bitmap -> filter array by bitmap
                var wi: usize = 0;
                for (sa.items) |v| {
                    if (bitmapGet(ob, v)) {
                        sa.items[wi] = v;
                        wi += 1;
                    }
                }
                sa.shrinkRetainingCapacity(wi);
            },
            .array => |*oa| {
                // array & array -> sorted intersection
                var wi: usize = 0;
                var si: usize = 0;
                var oi: usize = 0;
                while (si < sa.items.len and oi < oa.items.len) {
                    if (sa.items[si] < oa.items[oi]) {
                        si += 1;
                    } else if (sa.items[si] > oa.items[oi]) {
                        oi += 1;
                    } else {
                        sa.items[wi] = sa.items[si];
                        wi += 1;
                        si += 1;
                        oi += 1;
                    }
                }
                sa.shrinkRetainingCapacity(wi);
            },
        },
    }
}

fn orContainers(alloc: Allocator, self: *Container, other: *const Container) !void {
    switch (self.*) {
        .bitmap => |sb| switch (other.*) {
            .bitmap => |ob| bitmapOrSimd(sb, ob),
            .array => |*oa| {
                for (oa.items) |v| bitmapSet(sb, v);
            },
        },
        .array => |*sa| switch (other.*) {
            .bitmap => |ob| {
                // Convert self to bitmap, then OR
                const bm = try alloc.alloc(u64, bitmap_words);
                @memset(bm, 0);
                for (sa.items) |v| bitmapSet(bm, v);
                bitmapOrSimd(bm, ob);
                sa.deinit(alloc);
                self.* = .{ .bitmap = bm };
            },
            .array => |*oa| {
                for (oa.items) |v| {
                    const pos = arraySearchPos(sa.items, v);
                    if (pos >= sa.items.len or sa.items[pos] != v) {
                        try sa.insert(alloc, pos, v);
                    }
                }
                // Check if should convert to bitmap
                if (sa.items.len > array_max) {
                    const bm = try alloc.alloc(u64, bitmap_words);
                    @memset(bm, 0);
                    for (sa.items) |v| bitmapSet(bm, v);
                    sa.deinit(alloc);
                    self.* = .{ .bitmap = bm };
                }
            },
        },
    }
}

fn andNotContainers(_: Allocator, self: *Container, other: *const Container) void {
    switch (self.*) {
        .bitmap => |sb| switch (other.*) {
            .bitmap => |ob| bitmapAndNotSimd(sb, ob),
            .array => |*oa| {
                for (oa.items) |v| bitmapUnset(sb, v);
            },
        },
        .array => |*sa| switch (other.*) {
            .bitmap => |ob| {
                var wi: usize = 0;
                for (sa.items) |v| {
                    if (!bitmapGet(ob, v)) {
                        sa.items[wi] = v;
                        wi += 1;
                    }
                }
                sa.shrinkRetainingCapacity(wi);
            },
            .array => |*oa| {
                var wi: usize = 0;
                var oi: usize = 0;
                for (sa.items) |v| {
                    while (oi < oa.items.len and oa.items[oi] < v) oi += 1;
                    if (oi >= oa.items.len or oa.items[oi] != v) {
                        sa.items[wi] = v;
                        wi += 1;
                    }
                }
                sa.shrinkRetainingCapacity(wi);
            },
        },
    }
}

fn cloneContainer(alloc: Allocator, src: *const Container) !Container {
    return switch (src.*) {
        .array => |*a| .{ .array = .{ .items = try alloc.dupe(u16, a.items), .capacity = a.items.len, .pointer_stability = .{} } },
        .bitmap => |b| .{ .bitmap = try alloc.dupe(u64, b) },
    };
}

// ============================================================================
// Roaring Bitmap
// ============================================================================

/// Roaring bitmap: compressed bitmap for uint32 values.
/// Partitions 32-bit space into 16-bit chunks, each stored as an array or bitmap.
pub const RoaringBitmap = struct {
    alloc: Allocator,
    keys: std.ArrayListUnmanaged(u16),
    containers: std.ArrayListUnmanaged(Container),
    /// Cumulative cardinality: `cumulative_cards[i]` is the sum of
    /// cardinalities of `containers[0..i]` (so the value at the last index is
    /// the cardinality of all-but-last container). Precomputed by
    /// `prepareRead` so `rank()` can find a target's container in
    /// O(log K) and avoid recomputing per-container popcounts on every call.
    /// Invalidated by mutations (`add`, `remove`, etc.) and freed in
    /// `deinit`. `null` while the cache is unbuilt or stale.
    cumulative_cards: ?[]usize = null,
    read_rank: ?*FrozenRankIndex = null,

    pub fn init(alloc: Allocator) RoaringBitmap {
        return .{ .alloc = alloc, .keys = .empty, .containers = .empty };
    }

    pub fn deinit(self: *RoaringBitmap) void {
        self.clearReadRank();
        for (self.containers.items) |*c| c.deinit(self.alloc);
        self.keys.deinit(self.alloc);
        self.containers.deinit(self.alloc);
        if (self.cumulative_cards) |c| self.alloc.free(c);
        self.* = undefined;
    }

    /// Prepare immutable-read navigation, including per-word rank prefixes.
    /// Idempotent; mutations invalidate both word and container metadata.
    pub fn prepareRead(self: *RoaringBitmap) !void {
        if (self.read_rank == null) {
            const index = try self.alloc.create(FrozenRankIndex);
            errdefer self.alloc.destroy(index);
            index.* = try FrozenRankIndex.init(self.alloc, self.*);
            self.read_rank = index;
        }
        if (self.cumulative_cards != null) return;
        const cards = try self.alloc.alloc(usize, self.containers.items.len);
        var sum: usize = 0;
        for (self.containers.items, 0..) |*c, i| {
            cards[i] = sum;
            sum += c.cardinality();
        }
        self.cumulative_cards = cards;
    }

    fn clearReadRank(self: *RoaringBitmap) void {
        if (self.read_rank) |index| {
            index.deinit();
            self.alloc.destroy(index);
            self.read_rank = null;
        }
    }
    fn invalidateRankCache(self: *RoaringBitmap) void {
        self.clearReadRank();
        if (self.cumulative_cards) |c| {
            self.alloc.free(c);
            self.cumulative_cards = null;
        }
    }

    fn lowerChunk(self: *const RoaringBitmap, key: u16) usize {
        var lo: usize = 0;
        var hi = self.keys.items.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (self.keys.items[mid] < key) lo = mid + 1 else hi = mid;
        }
        return lo;
    }
    fn findChunk(self: *const RoaringBitmap, key: u16) ?usize {
        const index = self.lowerChunk(key);
        return if (index < self.keys.items.len and self.keys.items[index] == key) index else null;
    }
    fn getOrCreateChunk(self: *RoaringBitmap, key: u16) !*Container {
        const index = self.lowerChunk(key);
        if (index < self.keys.items.len and self.keys.items[index] == key) return &self.containers.items[index];
        try self.keys.ensureUnusedCapacity(self.alloc, 1);
        try self.containers.ensureUnusedCapacity(self.alloc, 1);
        self.keys.insertAssumeCapacity(index, key);
        self.containers.insertAssumeCapacity(index, .{ .array = .empty });
        return &self.containers.items[index];
    }

    pub fn add(self: *RoaringBitmap, val: u32) !void {
        self.invalidateRankCache();
        const key: u16 = @intCast(val >> 16);
        const low: u16 = @truncate(val);
        const container = try self.getOrCreateChunk(key);
        try container.add(self.alloc, low);
    }

    /// Union a half-open interval using word kernels for dense containers.
    /// The u64 upper endpoint permits including maxInt(u32) without wrapping.
    pub fn addRange(self: *RoaringBitmap, lower: u32, upper: u64) !void {
        if (upper > @as(u64, 1) << 32 or upper < lower) return error.InvalidRange;
        if (upper == lower) return;
        self.invalidateRankCache();
        var position: u64 = lower;
        while (position < upper) {
            const key: u16 = @intCast(position >> 16);
            const end = @min(upper, ((position >> 16) + 1) << 16);
            const begin_low: u32 = @intCast(position & 0xffff);
            const end_low: u32 = @intCast(end - (@as(u64, key) << 16));
            const container = try self.getOrCreateChunk(key);
            if (container.* == .array and container.array.items.len + end_low - begin_low <= array_max) {
                for (begin_low..end_low) |low| try container.add(self.alloc, @intCast(low));
            } else {
                if (container.* == .array) {
                    const bitmap = try self.alloc.alloc(u64, bitmap_words);
                    @memset(bitmap, 0);
                    for (container.array.items) |low| bitmapSet(bitmap, low);
                    container.array.deinit(self.alloc);
                    container.* = .{ .bitmap = bitmap };
                }
                const first = begin_low / 64;
                const last = (end_low - 1) / 64;
                for (first..last + 1) |word| {
                    const start_bit: u6 = if (word == first) @intCast(begin_low % 64) else 0;
                    const end_bits = if (word == last) (end_low - 1) % 64 + 1 else 64;
                    const mask = (@as(u64, std.math.maxInt(u64)) << start_bit) & (if (end_bits == 64) std.math.maxInt(u64) else (@as(u64, 1) << @as(u6, @intCast(end_bits))) - 1);
                    container.bitmap[word] |= mask;
                }
            }
            position = end;
        }
    }

    /// Bulk-add a strictly-ascending slice of u32 values. The caller asserts
    /// that `vals` is sorted ascending and free of duplicates within the slice;
    /// values may still collide with existing bitmap members (those are deduped).
    /// Avoids per-value linear chunk lookup and per-value binary search inside
    /// array containers — both become O(1) amortized when input is monotonic.
    pub fn addSortedAscending(self: *RoaringBitmap, vals: []const u32) !void {
        if (vals.len == 0) return;
        self.invalidateRankCache();
        var i: usize = 0;
        while (i < vals.len) {
            const key: u16 = @intCast(vals[i] >> 16);
            // Locate the run end: all consecutive vals sharing this key.
            var j: usize = i + 1;
            while (j < vals.len and @as(u16, @intCast(vals[j] >> 16)) == key) : (j += 1) {}

            const container = try self.getOrCreateChunk(key);
            try container.appendSortedAscending(self.alloc, vals[i..j]);
            i = j;
        }
    }

    pub fn remove(self: *RoaringBitmap, val: u32) !void {
        self.invalidateRankCache();
        const key: u16 = @intCast(val >> 16);
        const low: u16 = @truncate(val);
        if (self.findChunk(key)) |idx| {
            try self.containers.items[idx].remove(self.alloc, low);
        }
    }

    /// Remove without changing the container representation. This is used by
    /// rollback paths that must not allocate while undoing a failed mutation.
    pub fn removeRetainingStorage(self: *RoaringBitmap, val: u32) void {
        self.invalidateRankCache();
        const key: u16 = @intCast(val >> 16);
        const low: u16 = @truncate(val);
        const idx = self.findChunk(key) orelse return;
        switch (self.containers.items[idx]) {
            .array => |*values| {
                const pos = arraySearchPos(values.items, low);
                if (pos < values.items.len and values.items[pos] == low) _ = values.orderedRemove(pos);
            },
            .bitmap => |bitmap| bitmapUnset(bitmap, low),
        }
    }

    pub fn contains(self: *const RoaringBitmap, val: u32) bool {
        const key: u16 = @intCast(val >> 16);
        const low: u16 = @truncate(val);
        if (self.findChunk(key)) |idx| {
            return self.containers.items[idx].contains(low);
        }
        return false;
    }

    pub fn cardinality(self: *const RoaringBitmap) usize {
        if (self.read_rank) |index| return index.count;
        var total: usize = 0;
        for (self.containers.items) |*c| total += c.cardinality();
        return total;
    }

    /// Number of bitmap members strictly less than `value`. Used by
    /// `PostingsIterator.advanceTo` to compute the chunked freq/norm
    /// decoder's offset-within-chunk after a bitmap seek.
    ///
    /// When the cumulative-cardinality cache is built (every read-path
    /// bitmap from `fromBytes`), this is O(log K + log card) where K is
    /// the container count: a binary search over the keys to find the
    /// target's container plus a `rankBelow` inside it. When the cache is
    /// absent (build-time bitmaps mid-construction), falls back to a linear
    /// per-container walk that re-popcounts on demand.
    pub fn rank(self: *const RoaringBitmap, value: u32) usize {
        if (self.read_rank) |index| return index.rank(value);
        const target_high: u16 = @intCast(value >> 16);
        const target_low: u16 = @truncate(value);

        if (self.cumulative_cards) |cards| {
            // Binary search: find the leftmost container with key >= target_high.
            var lo: usize = 0;
            var hi: usize = self.keys.items.len;
            while (lo < hi) {
                const mid = lo + (hi - lo) / 2;
                if (self.keys.items[mid] < target_high) {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            if (lo == self.keys.items.len) {
                // All container keys < target_high: count is total cardinality.
                if (lo == 0) return 0;
                return cards[lo - 1] + self.containers.items[lo - 1].cardinality();
            }
            if (self.keys.items[lo] == target_high) {
                return cards[lo] + self.containers.items[lo].rankBelow(target_low);
            }
            // No container with this exact key — return cumulative up to here.
            return cards[lo];
        }

        // Slow path (cache invalidated by a recent mutation, or never built).
        var count: usize = 0;
        for (self.keys.items, self.containers.items) |key, *container| {
            if (key < target_high) {
                count += container.cardinality();
            } else if (key == target_high) {
                count += container.rankBelow(target_low);
                return count;
            } else {
                return count;
            }
        }
        return count;
    }

    pub fn isEmpty(self: *const RoaringBitmap) bool {
        return self.keys.items.len == 0;
    }

    pub fn iterator(self: *const RoaringBitmap) Iterator {
        return Iterator.init(self);
    }

    pub fn clone(self: *const RoaringBitmap, alloc: Allocator) !RoaringBitmap {
        var copied = RoaringBitmap.init(alloc);
        errdefer copied.deinit();

        try copied.keys.ensureTotalCapacity(alloc, self.keys.items.len);
        try copied.containers.ensureTotalCapacity(alloc, self.containers.items.len);

        for (self.keys.items, self.containers.items) |key, container| {
            copied.keys.appendAssumeCapacity(key);
            switch (container) {
                .array => |a| {
                    var arr = std.ArrayListUnmanaged(u16).empty;
                    errdefer arr.deinit(alloc);
                    try arr.appendSlice(alloc, a.items);
                    copied.containers.appendAssumeCapacity(.{ .array = arr });
                },
                .bitmap => |words| {
                    copied.containers.appendAssumeCapacity(.{ .bitmap = try alloc.dupe(u64, words) });
                },
            }
        }

        return copied;
    }

    pub fn eql(self: *const RoaringBitmap, other: *const RoaringBitmap) bool {
        if (!std.mem.eql(u16, self.keys.items, other.keys.items)) return false;
        if (self.containers.items.len != other.containers.items.len) return false;

        for (self.containers.items, other.containers.items) |lhs, rhs| {
            switch (lhs) {
                .array => |lhs_array| switch (rhs) {
                    .array => |rhs_array| {
                        if (!std.mem.eql(u16, lhs_array.items, rhs_array.items)) return false;
                    },
                    .bitmap => return false,
                },
                .bitmap => |lhs_bitmap| switch (rhs) {
                    .bitmap => |rhs_bitmap| {
                        if (!std.mem.eql(u64, lhs_bitmap, rhs_bitmap)) return false;
                    },
                    .array => return false,
                },
            }
        }

        return true;
    }

    // ========================================================================
    // Serialization
    // ========================================================================

    /// Serialize to bytes. Format:
    ///   [num_containers: u16 LE]
    ///   [keys: num_containers x u16 LE]
    ///   [cardinalities: num_containers x u16 LE]  (cardinality - 1)
    ///   [container data: ...]
    ///     array: sorted u16 LE values
    ///     bitmap: 1024 x u64 LE words
    pub fn toBytes(self: *const RoaringBitmap, alloc: Allocator) ![]u8 {
        const n: usize = self.keys.items.len;
        if (n > std.math.maxInt(u16)) return error.BitmapTooLarge;
        // Calculate size
        var size: usize = 2;
        size = try std.math.add(usize, size, try std.math.mul(usize, n, 2));
        size = try std.math.add(usize, size, try std.math.mul(usize, n, 2));
        for (self.containers.items) |*c| {
            switch (c.*) {
                .array => |*a| size = try std.math.add(usize, size, try std.math.mul(usize, a.items.len, 2)),
                .bitmap => size = try std.math.add(usize, size, bitmap_words * 8),
            }
        }

        var buf = try alloc.alloc(u8, size);
        var pos: usize = 0;

        // Num containers
        buf[pos..][0..2].* = @bitCast(@as(u16, @as(u16, @intCast(n))));
        pos += 2;

        // Keys
        for (self.keys.items) |k| {
            buf[pos..][0..2].* = @bitCast(@as(u16, k));
            pos += 2;
        }

        // Cardinalities (stored as card - 1)
        for (self.containers.items) |*c| {
            const card: u16 = @intCast(c.cardinality() - 1);
            buf[pos..][0..2].* = @bitCast(@as(u16, card));
            pos += 2;
        }

        // Container data
        for (self.containers.items) |*c| {
            switch (c.*) {
                .array => |*a| {
                    for (a.items) |v| {
                        buf[pos..][0..2].* = @bitCast(@as(u16, v));
                        pos += 2;
                    }
                },
                .bitmap => |b| {
                    for (b) |word| {
                        buf[pos..][0..8].* = @bitCast(@as(u64, word));
                        pos += 8;
                    }
                },
            }
        }

        return buf;
    }

    /// Deserialize from bytes.
    pub fn fromBytes(alloc: Allocator, data: []const u8) !RoaringBitmap {
        if (data.len < 2) return RoaringBitmap.init(alloc);

        const n = std.mem.readInt(u16, data[0..2], .little);
        var pos: usize = 2;
        const header_len = pos + @as(usize, n) * 4;
        if (data.len < header_len) return error.InvalidRoaringBitmap;

        var bm = RoaringBitmap.init(alloc);
        errdefer bm.deinit();

        // Read keys
        try bm.keys.ensureTotalCapacity(alloc, n);
        for (0..n) |_| {
            const k = std.mem.readInt(u16, data[pos..][0..2], .little);
            pos += 2;
            bm.keys.appendAssumeCapacity(k);
        }

        // Read cardinalities
        var cards = try alloc.alloc(u16, n);
        defer alloc.free(cards);
        for (0..n) |i| {
            cards[i] = std.mem.readInt(u16, data[pos..][0..2], .little);
            pos += 2;
        }

        // Read containers
        try bm.containers.ensureTotalCapacity(alloc, n);
        // Build the cumulative cardinality cache while we're already walking
        // every container. The wire format carries per-container cardinality
        // up front, so this is zero extra IO and saves the lazy popcount-walk
        // that `prepareRead` would otherwise do on first `rank()` call.
        const cumulative = try alloc.alloc(usize, n);
        errdefer alloc.free(cumulative);
        var running: usize = 0;
        for (0..n) |i| {
            const card = @as(usize, cards[i]) + 1;
            cumulative[i] = running;
            running += card;
            if (card > array_max) {
                // Bitmap container
                if (data.len - pos < bitmap_words * @sizeOf(u64)) return error.InvalidRoaringBitmap;
                const words = try alloc.alloc(u64, bitmap_words);
                for (0..bitmap_words) |w| {
                    words[w] = std.mem.readInt(u64, data[pos..][0..8], .little);
                    pos += 8;
                }
                bm.containers.appendAssumeCapacity(.{ .bitmap = words });
            } else {
                // Array container
                if (data.len - pos < card * @sizeOf(u16)) return error.InvalidRoaringBitmap;
                var arr = std.ArrayListUnmanaged(u16).empty;
                try arr.ensureTotalCapacity(alloc, card);
                for (0..card) |_| {
                    const v = std.mem.readInt(u16, data[pos..][0..2], .little);
                    pos += 2;
                    arr.appendAssumeCapacity(v);
                }
                bm.containers.appendAssumeCapacity(.{ .array = arr });
            }
        }
        bm.cumulative_cards = cumulative;

        return bm;
    }

    // ========================================================================
    // SIMD-accelerated bulk operations
    // ========================================================================

    pub fn andWith(self: *RoaringBitmap, other: *const RoaringBitmap) void {
        self.invalidateRankCache();
        var wi: usize = 0;
        var si: usize = 0;
        var oi: usize = 0;

        while (si < self.keys.items.len and oi < other.keys.items.len) {
            const sk = self.keys.items[si];
            const ok = other.keys.items[oi];

            if (sk < ok) {
                self.containers.items[si].deinit(self.alloc);
                si += 1;
            } else if (sk > ok) {
                oi += 1;
            } else {
                andContainers(self.alloc, &self.containers.items[si], &other.containers.items[oi]);
                if (wi != si) {
                    self.keys.items[wi] = sk;
                    self.containers.items[wi] = self.containers.items[si];
                }
                wi += 1;
                si += 1;
                oi += 1;
            }
        }
        while (si < self.keys.items.len) : (si += 1) {
            self.containers.items[si].deinit(self.alloc);
        }
        self.keys.shrinkRetainingCapacity(wi);
        self.containers.shrinkRetainingCapacity(wi);
    }

    pub fn orWith(self: *RoaringBitmap, other: *const RoaringBitmap) !void {
        self.invalidateRankCache();
        for (other.keys.items, other.containers.items) |ok, *oc| {
            if (self.findChunk(ok)) |idx| {
                try orContainers(self.alloc, &self.containers.items[idx], oc);
            } else {
                var container = try cloneContainer(self.alloc, oc);
                errdefer container.deinit(self.alloc);
                const insert_idx = blk: {
                    for (self.keys.items, 0..) |sk, i| {
                        if (sk > ok) break :blk i;
                    }
                    break :blk self.keys.items.len;
                };
                try self.keys.ensureUnusedCapacity(self.alloc, 1);
                try self.containers.ensureUnusedCapacity(self.alloc, 1);
                self.keys.insertAssumeCapacity(insert_idx, ok);
                self.containers.insertAssumeCapacity(insert_idx, container);
            }
        }
    }

    pub fn andNotWith(self: *RoaringBitmap, other: *const RoaringBitmap) void {
        self.invalidateRankCache();
        var si: usize = 0;
        var oi: usize = 0;

        while (si < self.keys.items.len and oi < other.keys.items.len) {
            const sk = self.keys.items[si];
            const ok = other.keys.items[oi];

            if (sk < ok) {
                si += 1;
            } else if (sk > ok) {
                oi += 1;
            } else {
                andNotContainers(self.alloc, &self.containers.items[si], &other.containers.items[oi]);
                si += 1;
                oi += 1;
            }
        }
    }

    /// Membership bits in the aligned 64-document word containing doc.
    /// Dense containers use one load; sparse containers inspect only this word.
    pub fn wordMask(self: *const RoaringBitmap, doc: u32) u64 {
        const key: u16 = @truncate(doc >> 16);
        const pos = arraySearchPos(self.keys.items, key);
        if (pos == self.keys.items.len or self.keys.items[pos] != key) return 0;
        const low: u16 = @truncate(doc & ~@as(u32, 63));
        return switch (self.containers.items[pos]) {
            .bitmap => |words| words[low / 64],
            .array => |values| blk: {
                var result: u64 = 0;
                var i = arraySearchPos(values.items, low);
                while (i < values.items.len and @as(u32, values.items[i]) < @as(u32, low) + 64) : (i += 1)
                    result |= @as(u64, 1) << @as(u6, @truncate(values.items[i]));
                break :blk result;
            },
        };
    }

    /// First non-member at or after lower, or 2^32 when the suffix is full.
    /// Navigate containers/words directly rather than binary-searching rank.
    pub fn nextAbsent(self: *const RoaringBitmap, lower: u32) u64 {
        var target: u64 = lower;
        while (target < 0x1_0000_0000) {
            const word = ~self.wordMask(@intCast(target)) & (@as(u64, std.math.maxInt(u64)) << @as(u6, @truncate(target)));
            if (word != 0) return (target & ~@as(u64, 63)) + @ctz(word);
            target = (target & ~@as(u64, 63)) + 64;
        }
        return target;
    }

    /// Find an intersection-minus-union member in a bounded half-open window.
    /// No bitmap copies or per-document navigation, even for overlapping masks.
    pub fn nextMatching(lower: u32, upper: u64, includes: []const *const RoaringBitmap, excludes: []const *const RoaringBitmap) ?u32 {
        std.debug.assert(upper <= 0x1_0000_0000);
        var target: u64 = lower;
        while (target < upper) {
            var word = @as(u64, std.math.maxInt(u64)) << @as(u6, @truncate(target));
            for (includes) |bitmap| word &= bitmap.wordMask(@intCast(target));
            for (excludes) |bitmap| word &= ~bitmap.wordMask(@intCast(target));
            if (word != 0) {
                const candidate = (target & ~@as(u64, 63)) + @ctz(word);
                return if (candidate < upper) @intCast(candidate) else null;
            }
            target = (target & ~@as(u64, 63)) + 64;
        }
        return null;
    }

    /// Conservative membership seek for posting scorers. Sparse includes jump
    /// directly to their next member; overlapping masks inspect at most 64 words
    /// before yielding a lower bound back to the posting/cancellation loop.
    /// Admission must still check exact membership when this returns a bound.
    pub fn candidateLowerBound(lower: u32, upper: u64, includes: []const *const RoaringBitmap, excludes: []const *const RoaringBitmap) u64 {
        var target: u64 = lower;
        for (0..64) |_| {
            if (target >= upper) return upper;
            for (includes) |bitmap| {
                var navigation = bitmap.iterator();
                target = navigation.seekTo(@intCast(target)) orelse return upper;
                if (target >= upper) return upper;
            }
            const end = @min(upper, (target & ~@as(u64, 63)) + 64);
            if (nextMatching(@intCast(target), end, includes, excludes)) |candidate| return candidate;
            target = end;
        }
        return target;
    }

    /// Count a half-open range without materializing or enumerating members.
    pub fn rangeCardinality(self: *const RoaringBitmap, lower: u32, upper: u64) usize {
        std.debug.assert(upper <= 0x1_0000_0000 and upper >= lower);
        const end = if (upper == 0x1_0000_0000) self.cardinality() else self.rank(@intCast(upper));
        return end - self.rank(lower);
    }

    /// Copy only intersecting containers, mask boundary words and rebase to zero.
    /// Dense selections cost words/containers rather than selected documents.
    pub fn sliceRebased(self: *const RoaringBitmap, alloc: Allocator, lower: u32, upper: u64) !RoaringBitmap {
        std.debug.assert(upper <= 0x1_0000_0000 and upper >= lower);
        var clipped = RoaringBitmap.init(alloc);
        defer clipped.deinit();
        const first_container = self.lowerChunk(@intCast(lower >> 16));
        for (self.keys.items[first_container..], self.containers.items[first_container..]) |key, *container| {
            const base = @as(u64, key) << 16;
            if (base >= upper) break;
            if (base + 65536 <= lower) continue;
            const lo: u32 = @intCast(@max(base, lower) - base);
            const hi: u32 = @intCast(@min(base + 65536, upper) - base);
            if (lo == hi) continue;
            var copy = try cloneContainer(alloc, container);
            errdefer copy.deinit(alloc);
            switch (copy) {
                .array => |*array| {
                    const begin = if (lo == 65536) array.items.len else arraySearchPos(array.items, @intCast(lo));
                    const end = if (hi == 65536) array.items.len else arraySearchPos(array.items, @intCast(hi));
                    std.mem.copyForwards(u16, array.items[0 .. end - begin], array.items[begin..end]);
                    array.shrinkRetainingCapacity(end - begin);
                },
                .bitmap => |words| {
                    for (words, 0..) |*word, i| {
                        const start: u32 = @intCast(i * 64);
                        if (start + 64 <= lo or start >= hi) {
                            word.* = 0;
                        } else {
                            if (lo > start) word.* &= @as(u64, std.math.maxInt(u64)) << @as(u6, @intCast(lo - start));
                            if (hi < start + 64) word.* &= @as(u64, std.math.maxInt(u64)) >> @as(u6, @intCast(start + 64 - hi));
                        }
                    }
                },
            }
            if (copy.cardinality() == 0) {
                copy.deinit(alloc);
                continue;
            }
            try clipped.keys.ensureUnusedCapacity(alloc, 1);
            try clipped.containers.ensureUnusedCapacity(alloc, 1);
            clipped.keys.appendAssumeCapacity(key);
            clipped.containers.appendAssumeCapacity(copy);
        }
        return clipped.addOffset(0 -% lower);
    }

    /// Returns a new bitmap with all values shifted by offset.
    /// Used during merge to renumber doc IDs across segments.
    pub fn addOffset(self: *const RoaringBitmap, offset: u32) !RoaringBitmap {
        var result = RoaringBitmap.init(self.alloc);
        errdefer result.deinit();
        try result.keys.ensureTotalCapacity(self.alloc, self.keys.items.len * 2);
        try result.containers.ensureTotalCapacity(self.alloc, self.containers.items.len * 2);

        if (offset == 0) {
            for (self.keys.items, self.containers.items) |key, *container| {
                try insertShiftedContainer(&result, key, try cloneContainer(self.alloc, container));
            }
            return result;
        }

        const chunk_offset: u16 = @intCast(offset >> 16);
        const low_offset: u16 = @truncate(offset);

        for (self.keys.items, self.containers.items) |key, *container| {
            switch (container.*) {
                .array => |*a| {
                    const shifted_key = key +% chunk_offset;
                    var shifted = try shiftArrayContainer(self.alloc, a.items, low_offset);
                    errdefer {
                        if (shifted.current) |*c| c.deinit(self.alloc);
                        if (shifted.next) |*c| c.deinit(self.alloc);
                    }

                    if (shifted.current) |current| {
                        try insertShiftedContainer(&result, shifted_key, current);
                        shifted.current = null;
                    }
                    if (shifted.next) |next| {
                        try insertShiftedContainer(&result, shifted_key +% 1, next);
                        shifted.next = null;
                    }
                },
                .bitmap => |b| {
                    const shifted_key = key +% chunk_offset;
                    var shifted = try shiftBitmapContainer(self.alloc, b, low_offset);
                    errdefer {
                        if (shifted.current) |*c| c.deinit(self.alloc);
                        if (shifted.next) |*c| c.deinit(self.alloc);
                    }

                    if (shifted.current) |current| {
                        try insertShiftedContainer(&result, shifted_key, current);
                        shifted.current = null;
                    }
                    if (shifted.next) |next| {
                        try insertShiftedContainer(&result, shifted_key +% 1, next);
                        shifted.next = null;
                    }
                },
            }
        }
        return result;
    }
};

const ShiftedContainers = struct {
    current: ?Container = null,
    next: ?Container = null,
};

fn shiftArrayContainer(alloc: Allocator, items: []const u16, low_offset: u16) !ShiftedContainers {
    if (items.len == 0) return .{};
    if (low_offset == 0) {
        return .{
            .current = .{
                .array = .{
                    .items = try alloc.dupe(u16, items),
                    .capacity = items.len,
                    .pointer_stability = .{},
                },
            },
        };
    }

    const max_current = std.math.maxInt(u16) - low_offset;
    const split_idx = blk: {
        for (items, 0..) |item, idx| {
            if (item > max_current) break :blk idx;
        }
        break :blk items.len;
    };

    var shifted: ShiftedContainers = .{};
    errdefer {
        if (shifted.current) |*c| c.deinit(alloc);
        if (shifted.next) |*c| c.deinit(alloc);
    }

    if (split_idx > 0) {
        var arr = std.ArrayListUnmanaged(u16).empty;
        try arr.ensureTotalCapacity(alloc, split_idx);
        for (items[0..split_idx]) |item| {
            arr.appendAssumeCapacity(item + low_offset);
        }
        shifted.current = .{ .array = arr };
    }

    if (split_idx < items.len) {
        var arr = std.ArrayListUnmanaged(u16).empty;
        const spill_len = items.len - split_idx;
        try arr.ensureTotalCapacity(alloc, spill_len);
        for (items[split_idx..]) |item| {
            const shifted_low = @as(u32, item) + low_offset - 0x1_0000;
            arr.appendAssumeCapacity(@intCast(shifted_low));
        }
        shifted.next = .{ .array = arr };
    }

    return shifted;
}

fn shiftBitmapContainer(alloc: Allocator, words: []const u64, low_offset: u16) !ShiftedContainers {
    if (low_offset == 0) {
        return .{ .current = .{ .bitmap = try alloc.dupe(u64, words) } };
    }

    const word_shift: usize = low_offset >> 6;
    const bit_shift: u6 = @truncate(low_offset);

    const current_words = try alloc.alloc(u64, bitmap_words);
    var current_owned = true;
    errdefer if (current_owned) alloc.free(current_words);
    @memset(current_words, 0);

    const next_words = try alloc.alloc(u64, bitmap_words);
    var next_owned = true;
    errdefer if (next_owned) alloc.free(next_words);
    @memset(next_words, 0);

    for (words, 0..) |word, idx| {
        if (word == 0) continue;

        const target = idx + word_shift;
        if (target < bitmap_words) {
            current_words[target] |= word << bit_shift;
            if (bit_shift != 0) {
                const carry_shift: u6 = @intCast(@as(u7, 64) - bit_shift);
                const carry = word >> carry_shift;
                if (carry != 0) {
                    if (target + 1 < bitmap_words) {
                        current_words[target + 1] |= carry;
                    } else {
                        next_words[0] |= carry;
                    }
                }
            }
        } else {
            const next_idx = target - bitmap_words;
            next_words[next_idx] |= word << bit_shift;
            if (bit_shift != 0 and next_idx + 1 < bitmap_words) {
                const carry_shift: u6 = @intCast(@as(u7, 64) - bit_shift);
                next_words[next_idx + 1] |= word >> carry_shift;
            }
        }
    }

    const current = try bitmapWordsToContainer(alloc, current_words);
    current_owned = false;
    errdefer if (current) |*c| {
        var mc = c.*;
        mc.deinit(alloc);
    };
    const next = try bitmapWordsToContainer(alloc, next_words);
    next_owned = false;

    return .{
        .current = current,
        .next = next,
    };
}

fn bitmapWordsToContainer(alloc: Allocator, words: []u64) !?Container {
    const card = bitmapPopcount(words);
    if (card == 0) {
        alloc.free(words);
        return null;
    }
    if (card > array_max) {
        return Container{ .bitmap = words };
    }

    var arr = std.ArrayListUnmanaged(u16).empty;
    errdefer arr.deinit(alloc);
    try arr.ensureTotalCapacity(alloc, card);
    var iter = bitmapIterator(words);
    while (iter.next()) |v| {
        arr.appendAssumeCapacity(v);
    }
    alloc.free(words);
    return Container{ .array = arr };
}

fn insertShiftedContainer(result: *RoaringBitmap, key: u16, container: Container) !void {
    const len = result.keys.items.len;
    if (len == 0) {
        result.keys.appendAssumeCapacity(key);
        result.containers.appendAssumeCapacity(container);
        return;
    }

    const last_idx = len - 1;
    const last_key = result.keys.items[last_idx];
    if (last_key == key) {
        var owned = container;
        try orContainers(result.alloc, &result.containers.items[last_idx], &owned);
        owned.deinit(result.alloc);
        return;
    }
    if (last_key < key) {
        result.keys.appendAssumeCapacity(key);
        result.containers.appendAssumeCapacity(container);
        return;
    }

    if (result.findChunk(key)) |idx| {
        var owned = container;
        try orContainers(result.alloc, &result.containers.items[idx], &owned);
        owned.deinit(result.alloc);
        return;
    }

    const insert_idx = blk: {
        for (result.keys.items, 0..) |existing, idx| {
            if (existing > key) break :blk idx;
        }
        break :blk result.keys.items.len;
    };
    try result.keys.ensureUnusedCapacity(result.alloc, 1);
    try result.containers.ensureUnusedCapacity(result.alloc, 1);
    result.keys.insertAssumeCapacity(insert_idx, key);
    result.containers.insertAssumeCapacity(insert_idx, container);
}

// ============================================================================
// Iterator
// ============================================================================

pub const Iterator = struct {
    bitmap: *const RoaringBitmap,
    chunk_idx: usize,
    array_pos: usize,
    bm_iter: ?BitmapIter,

    fn init(bitmap: *const RoaringBitmap) Iterator {
        var self = Iterator{ .bitmap = bitmap, .chunk_idx = 0, .array_pos = 0, .bm_iter = null };
        self.initChunk();
        return self;
    }

    fn initChunk(self: *Iterator) void {
        if (self.chunk_idx >= self.bitmap.containers.items.len) return;
        switch (self.bitmap.containers.items[self.chunk_idx]) {
            .array => self.array_pos = 0,
            .bitmap => |b| self.bm_iter = bitmapIterator(b),
        }
    }

    /// Seek to a chunk containing lower, skipping preceding containers.
    pub fn seek(self: *Iterator, lower: u32) void {
        const high: u16 = @intCast(lower >> 16);
        self.chunk_idx = 0;
        while (self.chunk_idx < self.bitmap.keys.items.len and self.bitmap.keys.items[self.chunk_idx] < high) self.chunk_idx += 1;
        self.bm_iter = null;
        self.initChunk();
        if (self.chunk_idx == self.bitmap.keys.items.len or self.bitmap.keys.items[self.chunk_idx] != high) return;
        const low: u16 = @truncate(lower);
        switch (self.bitmap.containers.items[self.chunk_idx]) {
            .array => |a| {
                while (self.array_pos < a.items.len and a.items[self.array_pos] < low) self.array_pos += 1;
            },
            .bitmap => |words| {
                const word: usize = low / 64;
                self.bm_iter = .{ .words = words, .word_idx = word, .current = words[word] & (@as(u64, std.math.maxInt(u64)) << @as(u6, @truncate(low))) };
            },
        }
    }

    pub fn next(self: *Iterator) ?u32 {
        while (self.chunk_idx < self.bitmap.containers.items.len) {
            const high: u32 = @as(u32, self.bitmap.keys.items[self.chunk_idx]) << 16;
            switch (self.bitmap.containers.items[self.chunk_idx]) {
                .array => |*a| {
                    if (self.array_pos < a.items.len) {
                        const val = high | a.items[self.array_pos];
                        self.array_pos += 1;
                        return val;
                    }
                },
                .bitmap => {
                    if (self.bm_iter) |*bi| {
                        if (bi.next()) |low| return high | low;
                    }
                },
            }
            self.chunk_idx += 1;
            self.initChunk();
        }
        return null;
    }

    /// Advance the iterator to the smallest value >= `target`, returning that
    /// value or null if every remaining value is below it. Skips whole
    /// containers when `target.high16` is past the current container, and
    /// skips array entries / bitmap bits within the matching container.
    /// Equivalent to repeatedly calling `next()` until the returned value is
    /// >= target, but with O(log container) cost inside the matching
    /// container instead of O(target - cursor) sequential steps.
    pub fn seekTo(self: *Iterator, target: u32) ?u32 {
        const target_high: u16 = @intCast(target >> 16);
        const target_low: u16 = @truncate(target);
        const containers = self.bitmap.containers.items;
        const keys = self.bitmap.keys.items;

        // Jump directly to the target container. Fresh lower-bound probes
        // must not walk every preceding container in a broad selection.
        if (self.chunk_idx >= containers.len) return null;
        if (keys[self.chunk_idx] < target_high) {
            self.chunk_idx = self.bitmap.lowerChunk(target_high);
            self.initChunk();
            if (self.chunk_idx >= containers.len) return null;
        }

        // 2) If we landed on a strictly-greater container, return its first
        // remaining value via the regular sequential path.
        if (keys[self.chunk_idx] > target_high) return self.next();

        // 3) We're in target's container. Advance the per-container cursor
        // past `target_low - 1` so the next read returns >= target.
        const high: u32 = @as(u32, keys[self.chunk_idx]) << 16;
        switch (containers[self.chunk_idx]) {
            .array => |*a| {
                // Binary search for first entry >= target_low.
                var lo: usize = self.array_pos;
                var hi: usize = a.items.len;
                while (lo < hi) {
                    const mid = lo + (hi - lo) / 2;
                    if (a.items[mid] < target_low) {
                        lo = mid + 1;
                    } else {
                        hi = mid;
                    }
                }
                self.array_pos = lo;
                if (lo < a.items.len) {
                    const val = high | a.items[lo];
                    self.array_pos = lo + 1;
                    return val;
                }
                // Container exhausted; fall through to next container.
            },
            .bitmap => |b| {
                // Re-init the bitmap iterator at the right word boundary.
                const word_start: usize = target_low >> 6;
                const bit_start: u6 = @truncate(target_low);
                if (word_start < bitmap_words) {
                    // Mask off bits below `bit_start` in the starting word so
                    // popcount-walk only sees bits >= target_low.
                    const mask: u64 = if (bit_start == 0) std.math.maxInt(u64) else (~@as(u64, 0)) << bit_start;
                    self.bm_iter = .{
                        .words = b,
                        .word_idx = word_start,
                        .current = b[word_start] & mask,
                    };
                    if (self.bm_iter.?.next()) |low| return high | low;
                }
                // Container exhausted; fall through to next container.
            },
        }

        self.chunk_idx += 1;
        self.initChunk();
        return self.next();
    }
};

// ============================================================================
// Tests
// ============================================================================

test "basic add and contains" {
    const alloc = std.testing.allocator;
    var bm = RoaringBitmap.init(alloc);
    defer bm.deinit();

    try bm.add(42);
    try bm.add(1000);
    try bm.add(100000);

    try std.testing.expect(bm.contains(42));
    try std.testing.expect(bm.contains(1000));
    try std.testing.expect(bm.contains(100000));
    try std.testing.expect(!bm.contains(43));
    try std.testing.expect(!bm.contains(0));
}

test "array contains SIMD quad matches binary search" {
    var items: [array_max]u16 = undefined;

    var len: usize = 0;
    while (len <= array_max) : (len += 1) {
        for (items[0..len], 0..) |*item, idx| {
            item.* = @intCast(idx * 3 + 1);
        }

        const probes = [_]u16{
            0,
            1,
            if (len == 0) 1 else @intCast((len / 2) * 3 + 1),
            if (len == 0) 2 else @intCast((len / 2) * 3 + 2),
            if (len == 0) 3 else @intCast((len - 1) * 3 + 1),
            if (len == 0) 4 else @intCast((len - 1) * 3 + 2),
            std.math.maxInt(u16),
        };

        for (probes) |probe| {
            try std.testing.expectEqual(
                arrayContainsBinary(items[0..len], probe),
                arrayContainsSimdQuad(items[0..len], probe),
            );
        }
    }
}

test "cardinality" {
    const alloc = std.testing.allocator;
    var bm = RoaringBitmap.init(alloc);
    defer bm.deinit();

    for (0..100) |i| try bm.add(@intCast(i));
    try std.testing.expectEqual(@as(usize, 100), bm.cardinality());
}

test "remove" {
    const alloc = std.testing.allocator;
    var bm = RoaringBitmap.init(alloc);
    defer bm.deinit();

    try bm.add(10);
    try bm.add(20);
    try bm.add(30);

    try bm.remove(20);
    try std.testing.expect(bm.contains(10));
    try std.testing.expect(!bm.contains(20));
    try std.testing.expect(bm.contains(30));
    try std.testing.expectEqual(@as(usize, 2), bm.cardinality());
}

test "iterator sorted order" {
    const alloc = std.testing.allocator;
    var bm = RoaringBitmap.init(alloc);
    defer bm.deinit();

    try bm.add(300);
    try bm.add(100);
    try bm.add(200);

    var iter = bm.iterator();
    try std.testing.expectEqual(@as(?u32, 100), iter.next());
    try std.testing.expectEqual(@as(?u32, 200), iter.next());
    try std.testing.expectEqual(@as(?u32, 300), iter.next());
    try std.testing.expectEqual(@as(?u32, null), iter.next());
}

test "multiple chunks" {
    const alloc = std.testing.allocator;
    var bm = RoaringBitmap.init(alloc);
    defer bm.deinit();

    try bm.add(5); // chunk 0
    try bm.add(70000); // chunk 1
    try bm.add(200000); // chunk 3

    try std.testing.expect(bm.contains(5));
    try std.testing.expect(bm.contains(70000));
    try std.testing.expect(bm.contains(200000));
    try std.testing.expectEqual(@as(usize, 3), bm.cardinality());
}

test "array to bitmap promotion" {
    const alloc = std.testing.allocator;
    var bm = RoaringBitmap.init(alloc);
    defer bm.deinit();

    for (0..5000) |i| try bm.add(@intCast(i));

    try std.testing.expectEqual(@as(usize, 5000), bm.cardinality());
    try std.testing.expect(bm.contains(0));
    try std.testing.expect(bm.contains(4999));
    try std.testing.expect(!bm.contains(5000));
}

test "clone and equality preserve mixed containers" {
    const alloc = std.testing.allocator;
    var bm = RoaringBitmap.init(alloc);
    defer bm.deinit();

    for (0..5000) |i| try bm.add(@intCast(i));
    try bm.add(0x1_0001);
    try bm.add(0x2_0002);

    var cloned = try bm.clone(alloc);
    defer cloned.deinit();

    try std.testing.expect(bm.eql(&cloned));

    try cloned.add(0x2_0003);
    try std.testing.expect(!bm.eql(&cloned));
}

test "AND operation" {
    const alloc = std.testing.allocator;
    var a = RoaringBitmap.init(alloc);
    defer a.deinit();
    var b = RoaringBitmap.init(alloc);
    defer b.deinit();

    for ([_]u32{ 1, 2, 3, 4, 5 }) |v| try a.add(v);
    for ([_]u32{ 3, 4, 5, 6, 7 }) |v| try b.add(v);

    a.andWith(&b);

    try std.testing.expectEqual(@as(usize, 3), a.cardinality());
    try std.testing.expect(a.contains(3));
    try std.testing.expect(a.contains(4));
    try std.testing.expect(a.contains(5));
    try std.testing.expect(!a.contains(1));
    try std.testing.expect(!a.contains(7));
}

test "OR operation" {
    const alloc = std.testing.allocator;
    var a = RoaringBitmap.init(alloc);
    defer a.deinit();
    var b = RoaringBitmap.init(alloc);
    defer b.deinit();

    for ([_]u32{ 1, 3, 5 }) |v| try a.add(v);
    for ([_]u32{ 2, 4, 6 }) |v| try b.add(v);

    try a.orWith(&b);

    try std.testing.expectEqual(@as(usize, 6), a.cardinality());
    for (1..7) |i| try std.testing.expect(a.contains(@intCast(i)));
}

test "AND NOT operation" {
    const alloc = std.testing.allocator;
    var a = RoaringBitmap.init(alloc);
    defer a.deinit();
    var b = RoaringBitmap.init(alloc);
    defer b.deinit();

    for ([_]u32{ 1, 2, 3, 4, 5 }) |v| try a.add(v);
    for ([_]u32{ 2, 4 }) |v| try b.add(v);

    a.andNotWith(&b);

    try std.testing.expectEqual(@as(usize, 3), a.cardinality());
    try std.testing.expect(a.contains(1));
    try std.testing.expect(a.contains(3));
    try std.testing.expect(a.contains(5));
    try std.testing.expect(!a.contains(2));
    try std.testing.expect(!a.contains(4));
}

test "SIMD bitmap AND" {
    const alloc = std.testing.allocator;
    var a = RoaringBitmap.init(alloc);
    defer a.deinit();
    var b = RoaringBitmap.init(alloc);
    defer b.deinit();

    for (0..5000) |i| try a.add(@intCast(i));
    for (2500..7500) |i| try b.add(@intCast(i));

    a.andWith(&b);

    try std.testing.expectEqual(@as(usize, 2500), a.cardinality());
    try std.testing.expect(a.contains(2500));
    try std.testing.expect(a.contains(4999));
    try std.testing.expect(!a.contains(2499));
    try std.testing.expect(!a.contains(5000));
}

test "empty bitmap" {
    const alloc = std.testing.allocator;
    var bm = RoaringBitmap.init(alloc);
    defer bm.deinit();

    try std.testing.expect(bm.isEmpty());
    try std.testing.expectEqual(@as(usize, 0), bm.cardinality());
    var iter = bm.iterator();
    try std.testing.expectEqual(@as(?u32, null), iter.next());
}

test "addOffset shifts all values" {
    const alloc = std.testing.allocator;
    var bm = RoaringBitmap.init(alloc);
    defer bm.deinit();

    try bm.add(10);
    try bm.add(100);
    try bm.add(70000); // crosses chunk boundary

    var shifted = try bm.addOffset(1000);
    defer shifted.deinit();

    try std.testing.expectEqual(@as(usize, 3), shifted.cardinality());
    try std.testing.expect(shifted.contains(1010));
    try std.testing.expect(shifted.contains(1100));
    try std.testing.expect(shifted.contains(71000));
    try std.testing.expect(!shifted.contains(10));
    try std.testing.expect(!shifted.contains(100));
}

test "addOffset shifts sparse array container across chunk boundary" {
    const alloc = std.testing.allocator;
    var bm = RoaringBitmap.init(alloc);
    defer bm.deinit();

    try bm.add(65530);
    try bm.add(65534);
    try bm.add(65535);
    try bm.add(70000);

    var shifted = try bm.addOffset(10);
    defer shifted.deinit();

    try std.testing.expectEqual(@as(usize, 4), shifted.cardinality());
    try std.testing.expect(shifted.contains(65540));
    try std.testing.expect(shifted.contains(65544));
    try std.testing.expect(shifted.contains(65545));
    try std.testing.expect(shifted.contains(70010));
}

test "addOffset shifts dense bitmap container across chunk boundary" {
    const alloc = std.testing.allocator;
    var bm = RoaringBitmap.init(alloc);
    defer bm.deinit();

    for (60000..65000) |i| {
        try bm.add(@intCast(i));
    }

    var shifted = try bm.addOffset(1000);
    defer shifted.deinit();

    try std.testing.expectEqual(@as(usize, 5000), shifted.cardinality());
    try std.testing.expect(shifted.contains(61000));
    try std.testing.expect(shifted.contains(65535));
    try std.testing.expect(shifted.contains(65999));
    try std.testing.expect(!shifted.contains(60000));
}

test "iterator seekTo: array container, in-bounds and across-bounds targets" {
    const alloc = std.testing.allocator;
    var bm = RoaringBitmap.init(alloc);
    defer bm.deinit();
    // Sparse, single array container: values 10, 100, 1000, 5000.
    for ([_]u32{ 10, 100, 1000, 5000 }) |v| try bm.add(v);

    {
        var it = bm.iterator();
        try std.testing.expectEqual(@as(?u32, 100), it.seekTo(50));
        try std.testing.expectEqual(@as(?u32, 1000), it.next());
    }
    {
        var it = bm.iterator();
        // Exact-match seek returns the matching value.
        try std.testing.expectEqual(@as(?u32, 100), it.seekTo(100));
    }
    {
        var it = bm.iterator();
        // Seek past everything → null.
        try std.testing.expectEqual(@as(?u32, null), it.seekTo(10_000));
    }
    {
        var it = bm.iterator();
        // Seek before everything → first element.
        try std.testing.expectEqual(@as(?u32, 10), it.seekTo(0));
    }
}

test "iterator seekTo: bitmap container, word-level skip" {
    const alloc = std.testing.allocator;
    var bm = RoaringBitmap.init(alloc);
    defer bm.deinit();
    // Force a bitmap container by adding > array_max densely.
    var v: u32 = 0;
    while (v < array_max + 100) : (v += 1) try bm.add(v);

    var it = bm.iterator();
    try std.testing.expectEqual(@as(?u32, 4096), it.seekTo(4096));
    try std.testing.expectEqual(@as(?u32, 4097), it.next());

    var it2 = bm.iterator();
    // Seek into the middle of a word.
    try std.testing.expectEqual(@as(?u32, 1003), it2.seekTo(1003));
    try std.testing.expectEqual(@as(?u32, 1004), it2.next());

    // Seek past the container's last value.
    var it3 = bm.iterator();
    try std.testing.expectEqual(@as(?u32, null), it3.seekTo(array_max + 100));
}

test "iterator seekTo: across multiple containers" {
    const alloc = std.testing.allocator;
    var bm = RoaringBitmap.init(alloc);
    defer bm.deinit();
    // Three containers: low (chunk 0), middle (chunk 1), high (chunk 2).
    try bm.add(50);
    try bm.add(60);
    try bm.add(70_000);
    try bm.add(80_000);
    try bm.add(150_000);

    var it = bm.iterator();
    // Skip past chunk 0 entirely, land on first chunk-1 element.
    try std.testing.expectEqual(@as(?u32, 70_000), it.seekTo(65_536));
    try std.testing.expectEqual(@as(?u32, 80_000), it.next());

    // Then jump over chunk 1 to chunk 2.
    try std.testing.expectEqual(@as(?u32, 150_000), it.seekTo(131_072));
    try std.testing.expectEqual(@as(?u32, null), it.next());
}

test "rank counts members below target across array and bitmap containers" {
    const alloc = std.testing.allocator;
    var bm = RoaringBitmap.init(alloc);
    defer bm.deinit();

    // Mix: sparse low (array), dense middle (bitmap), sparse high (array).
    for ([_]u32{ 5, 12, 100 }) |v| try bm.add(v);
    var v: u32 = 70_000;
    while (v < 70_000 + array_max + 50) : (v += 1) try bm.add(v); // bitmap
    for ([_]u32{ 200_000, 300_000 }) |w| try bm.add(w);

    try std.testing.expectEqual(@as(usize, 0), bm.rank(0));
    try std.testing.expectEqual(@as(usize, 0), bm.rank(5));
    try std.testing.expectEqual(@as(usize, 1), bm.rank(6));
    try std.testing.expectEqual(@as(usize, 2), bm.rank(13));
    try std.testing.expectEqual(@as(usize, 3), bm.rank(70_000));
    try std.testing.expectEqual(@as(usize, 4), bm.rank(70_001));
    // Cross into bitmap container, partially.
    try std.testing.expectEqual(@as(usize, 3 + 1234), bm.rank(70_000 + 1234));
    // Exhaust the bitmap container.
    try std.testing.expectEqual(@as(usize, 3 + array_max + 50), bm.rank(150_000));
    try std.testing.expectEqual(@as(usize, 3 + array_max + 50 + 1), bm.rank(200_001));
    try std.testing.expectEqual(@as(usize, 3 + array_max + 50 + 2), bm.rank(400_000));
    try std.testing.expectEqual(bm.cardinality(), bm.rank(std.math.maxInt(u32)));
}

test "rank cached path matches uncached path on round-tripped bitmaps" {
    const alloc = std.testing.allocator;
    var bm = RoaringBitmap.init(alloc);
    defer bm.deinit();

    // Build a mixed-shape bitmap (sparse-low / dense-mid / sparse-high) the
    // same as the rank test, but then round-trip through toBytes/fromBytes
    // so the read-path eagerly populates `cumulative_cards`.
    for ([_]u32{ 5, 12, 100 }) |v| try bm.add(v);
    var v: u32 = 70_000;
    while (v < 70_000 + array_max + 50) : (v += 1) try bm.add(v);
    for ([_]u32{ 200_000, 300_000 }) |w| try bm.add(w);

    const bytes = try bm.toBytes(alloc);
    defer alloc.free(bytes);

    var loaded = try RoaringBitmap.fromBytes(alloc, bytes);
    defer loaded.deinit();
    try std.testing.expect(loaded.cumulative_cards != null);

    // Probe values across the whole range, ensuring both bitmaps agree.
    const probes = [_]u32{ 0, 5, 6, 13, 70_000, 71_234, 150_000, 200_001, 400_000, std.math.maxInt(u32) };
    for (probes) |p| {
        try std.testing.expectEqual(bm.rank(p), loaded.rank(p));
    }
}

test "rank cache is invalidated on mutation" {
    const alloc = std.testing.allocator;
    var bm = RoaringBitmap.init(alloc);
    defer bm.deinit();
    try bm.add(10);
    try bm.add(20);
    try bm.prepareRead();
    try std.testing.expect(bm.cumulative_cards != null);

    // Any mutation drops the cache; the next rank() falls back to the slow
    // per-container walk and gives the right answer.
    try bm.add(30);
    try std.testing.expect(bm.cumulative_cards == null);
    try std.testing.expectEqual(@as(usize, 3), bm.rank(40));
}

test "iterator seekTo: matches sequential next() output" {
    const alloc = std.testing.allocator;
    var bm = RoaringBitmap.init(alloc);
    defer bm.deinit();

    var prng = std.Random.DefaultPrng.init(0xc0ffee);
    const rng = prng.random();
    var i: usize = 0;
    while (i < 20_000) : (i += 1) try bm.add(rng.uintLessThan(u32, 200_000));

    // Build the full sorted-set the slow way for a reference.
    var all = std.ArrayListUnmanaged(u32).empty;
    defer all.deinit(alloc);
    var it_seq = bm.iterator();
    while (it_seq.next()) |v| try all.append(alloc, v);

    // Pick random seek targets and verify seekTo returns the sorted-successor.
    const targets = [_]u32{ 0, 1, 100, 5_000, 50_000, 99_999, 100_000, 199_999, 200_000 };
    for (targets) |target| {
        var found: ?u32 = null;
        for (all.items) |v| {
            if (v >= target) {
                found = v;
                break;
            }
        }
        var it = bm.iterator();
        try std.testing.expectEqual(found, it.seekTo(target));
    }
}

/// Immutable, borrowed bitmap navigation for repeated rank lookups. Dense
/// containers retain one u16 prefix per 64-bit word rather than popcounting
/// hundreds of preceding words for every posting. Mutation invalidates this
/// view; its owner must freeze the bitmap until deinit.
pub const FrozenRankIndex = struct {
    const Entry = struct { before: usize = 0, words: ?[]u16 = null };
    allocator: Allocator,
    bitmap: RoaringBitmap,
    entries: []Entry,
    count: usize,
    pub fn init(allocator: Allocator, bitmap: RoaringBitmap) !@This() {
        const entries = try allocator.alloc(Entry, bitmap.containers.items.len);
        for (entries) |*entry| entry.* = .{};
        errdefer {
            for (entries) |entry| if (entry.words) |words| allocator.free(words);
            allocator.free(entries);
        }
        var count: usize = 0;
        for (bitmap.containers.items, entries) |container, *entry| {
            entry.before = count;
            switch (container) {
                .array => |array| count += array.items.len,
                .bitmap => |bits| {
                    const words = try allocator.alloc(u16, bitmap_words);
                    entry.words = words;
                    var within: usize = 0;
                    for (bits, words) |word, *prefix| {
                        prefix.* = @intCast(within);
                        within += @popCount(word);
                    }
                    count += within;
                },
            }
        }
        return .{ .allocator = allocator, .bitmap = bitmap, .entries = entries, .count = count };
    }
    pub fn deinit(self: *@This()) void {
        for (self.entries) |entry| if (entry.words) |words| self.allocator.free(words);
        self.allocator.free(self.entries);
        self.* = undefined;
    }
    pub fn rank(self: *const @This(), value: u32) usize {
        const high: u16 = @intCast(value >> 16);
        const low: u16 = @truncate(value);
        var lo: usize = 0;
        var hi = self.entries.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (self.bitmap.keys.items[mid] < high) lo = mid + 1 else hi = mid;
        }
        if (lo == self.entries.len) return self.count;
        const entry = self.entries[lo];
        if (self.bitmap.keys.items[lo] != high) return entry.before;
        if (entry.words) |words| {
            const bit: u6 = @truncate(low);
            const mask = (@as(u64, 1) << bit) - 1;
            return entry.before + words[low / 64] + @as(usize, @popCount(self.bitmap.containers.items[lo].bitmap[low / 64] & mask));
        }
        return entry.before + self.bitmap.containers.items[lo].rankBelow(low);
    }
    /// Zero-based ordinal selection over the same immutable word prefixes.
    pub fn select(self: *const @This(), ordinal: usize) ?u32 {
        if (ordinal >= self.count) return null;
        var lo: usize = 0;
        var hi = self.entries.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            const end = if (mid + 1 < self.entries.len) self.entries[mid + 1].before else self.count;
            if (end <= ordinal) lo = mid + 1 else hi = mid;
        }
        const entry = self.entries[lo];
        const within_rank = ordinal - entry.before;
        const base = @as(u32, self.bitmap.keys.items[lo]) << 16;
        if (entry.words) |words| {
            var lower: usize = 0;
            var upper = words.len;
            while (lower < upper) {
                const mid = lower + (upper - lower) / 2;
                const end: usize = if (mid + 1 < words.len) words[mid + 1] else (if (lo + 1 < self.entries.len) self.entries[lo + 1].before else self.count) - entry.before;
                if (end <= within_rank) lower = mid + 1 else upper = mid;
            }
            var bits = self.bitmap.containers.items[lo].bitmap[lower];
            var skip = within_rank - words[lower];
            while (skip != 0) : (skip -= 1) bits &= bits - 1;
            return base | @as(u32, @intCast(lower * 64 + @ctz(bits)));
        }
        return base | self.bitmap.containers.items[lo].array.items[within_rank];
    }
    pub fn retainedBytes(self: *const @This()) usize {
        var bytes = self.entries.len * @sizeOf(Entry);
        for (self.entries) |entry| if (entry.words) |words| {
            bytes += words.len * 2;
        };
        return bytes;
    }
};

test "frozen rank navigation matches sparse dense and boundary ranks with bounded prefixes" {
    var bitmap = RoaringBitmap.init(std.testing.allocator);
    defer bitmap.deinit();
    for (0..200_000) |i| if (i % 3 != 0) {
        try bitmap.add(@intCast(i));
    };
    try bitmap.add(300_000);
    var index = try FrozenRankIndex.init(std.testing.allocator, bitmap);
    defer index.deinit();
    for (0..200_002) |i| {
        const expected = i - (i + 2) / 3;
        try std.testing.expectEqual(@min(@as(usize, 133_333), expected), index.rank(@intCast(i)));
    }
    try std.testing.expectEqual(bitmap.rank(300_001), index.rank(300_001));
    try std.testing.expect(index.retainedBytes() < 10 * 1024);
    const Harness = struct {
        fn run(allocator: Allocator, source: RoaringBitmap) !void {
            var navigation = try FrozenRankIndex.init(allocator, source);
            defer navigation.deinit();
            try std.testing.expectEqual(source.rank(170_123), navigation.rank(170_123));
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{bitmap});
}

test "external lake bitmap seeks skip containers and preserve bit and array boundaries" {
    var bitmap = RoaringBitmap.init(std.testing.allocator);
    defer bitmap.deinit();
    for (0..100001) |value| try bitmap.add(@intCast(value));
    try bitmap.add(200003);
    var it = bitmap.iterator();
    it.seek(65535);
    try std.testing.expectEqual(@as(?u32, 65535), it.next());
    try std.testing.expectEqual(@as(?u32, 65536), it.next());
    it.seek(100002);
    try std.testing.expectEqual(@as(?u32, 200003), it.next());
    it.seek(200004);
    try std.testing.expectEqual(@as(?u32, null), it.next());
}

test "external lake bitmap interval kernels preserve overlaps chunk boundaries and u32 endpoints" {
    var bitmap = RoaringBitmap.init(std.testing.allocator);
    defer bitmap.deinit();
    try bitmap.add(1);
    try bitmap.addRange(65534, 65540);
    try bitmap.addRange(65536, 70000);
    try bitmap.addRange(std.math.maxInt(u32), @as(u64, 1) << 32);
    try std.testing.expectEqual(@as(usize, 4468), bitmap.cardinality());
    try std.testing.expect(bitmap.contains(1));
    try std.testing.expect(!bitmap.contains(65533));
    try std.testing.expect(bitmap.contains(65534));
    try std.testing.expect(bitmap.contains(69999));
    try std.testing.expect(!bitmap.contains(70000));
    try std.testing.expect(bitmap.contains(std.math.maxInt(u32)));
    const bytes = try bitmap.toBytes(std.testing.allocator);
    defer std.testing.allocator.free(bytes);
    var decoded = try RoaringBitmap.fromBytes(std.testing.allocator, bytes);
    defer decoded.deinit();
    try std.testing.expect(bitmap.eql(&decoded));
}

test "range slices rebase sparse dense and u32 boundary selections" {
    const a = std.testing.allocator;
    var source = RoaringBitmap.init(a);
    defer source.deinit();
    try source.addRange(65000, 140000);
    try source.add(7);
    try source.add(std.math.maxInt(u32));
    for ([_][2]u64{ .{ 0, 0 }, .{ 3, 10 }, .{ 65033, 131077 }, .{ 90000, 90031 }, .{ 0xfffffff0, 0x1_0000_0000 } }) |bounds| {
        const lower: u32 = @intCast(bounds[0]);
        var actual = try source.sliceRebased(a, lower, bounds[1]);
        defer actual.deinit();
        var expected = RoaringBitmap.init(a);
        defer expected.deinit();
        var it = source.iterator();
        while (it.next()) |value| if (value >= lower and value < bounds[1]) {
            try expected.add(value - lower);
        };
        try std.testing.expect(actual.eql(&expected));
        try std.testing.expectEqual(expected.cardinality(), source.rangeCardinality(lower, bounds[1]));
    }
}

test "range slice owns partial containers on every allocation failure" {
    const a = std.testing.allocator;
    var source = RoaringBitmap.init(a);
    defer source.deinit();
    try source.addRange(1, 170000);
    try source.add(190000);
    const Probe = struct {
        fn run(failing: Allocator, bitmap: *const RoaringBitmap) !void {
            var sliced = try bitmap.sliceRebased(failing, 65533, 190001);
            defer sliced.deinit();
            try std.testing.expectEqual(bitmap.rangeCardinality(65533, 190001), sliced.cardinality());
        }
    };
    try std.testing.checkAllAllocationFailures(a, Probe.run, .{&source});
}

test "external lake prepared bitmap navigation invalidates on mutations and survives OOM" {
    const a = std.testing.allocator;
    const Probe = struct {
        fn run(allocator: Allocator) !void {
            var bitmap = RoaringBitmap.init(allocator);
            defer bitmap.deinit();
            try bitmap.addRange(100, 150000);
            try bitmap.prepareRead();
            try std.testing.expectEqual(@as(usize, 149900), bitmap.rangeCardinality(0, 150000));
            try bitmap.add(200000);
            try std.testing.expect(bitmap.read_rank == null);
            try bitmap.prepareRead();
            try std.testing.expectEqual(@as(usize, 149901), bitmap.cardinality());
            try bitmap.remove(101);
            try std.testing.expect(bitmap.read_rank == null);
            try std.testing.expectEqual(@as(usize, 1), bitmap.rangeCardinality(100, 102));
        }
    };
    try Probe.run(a);
    try std.testing.checkAllAllocationFailures(a, Probe.run, .{});
}

test "frozen rank select skips dense words holes and empty containers" {
    const a = std.testing.allocator;
    var bitmap = RoaringBitmap.init(a);
    defer bitmap.deinit();
    try bitmap.addRange(0, 131072);
    for (0..131072) |i| if (i % 3 == 0) try bitmap.remove(@intCast(i));
    try bitmap.add(200000);
    try bitmap.remove(200000); // Retain an empty container between live ones.
    try bitmap.add(std.math.maxInt(u32));
    try bitmap.prepareRead();
    var iterator = bitmap.iterator();
    var ordinal: usize = 0;
    while (iterator.next()) |value| : (ordinal += 1) {
        try std.testing.expectEqual(value, bitmap.read_rank.?.select(ordinal).?);
        try std.testing.expectEqual(ordinal, bitmap.rank(value));
    }
    try std.testing.expect(bitmap.read_rank.?.select(ordinal) == null);
}

test "fresh bitmap seeks jump dense containers and never rewind exhausted iterators" {
    var bitmap = RoaringBitmap.init(std.testing.allocator);
    defer bitmap.deinit();
    try bitmap.addRange(0, 50_000_000);
    try bitmap.remove(49_123_456);
    try bitmap.add(std.math.maxInt(u32));
    for ([_]u32{ 0, 65_535, 65_536, 49_123_456, 49_999_999, 50_000_000, std.math.maxInt(u32) }) |lower| {
        var fresh = bitmap.iterator();
        const expected: u32 = if (lower >= 50_000_000) std.math.maxInt(u32) else if (lower == 49_123_456) lower + 1 else lower;
        try std.testing.expectEqual(@as(?u32, expected), fresh.seekTo(lower));
    }
    var forward = bitmap.iterator();
    try std.testing.expectEqual(@as(?u32, 49_999_999), forward.seekTo(49_999_999));
    try std.testing.expectEqual(@as(?u32, std.math.maxInt(u32)), forward.seekTo(0));
    try std.testing.expectEqual(@as(?u32, null), forward.seekTo(0));
    try std.testing.expectEqual(@as(?u32, null), forward.seekTo(std.math.maxInt(u32)));
}

test "bitmap absent seeks skip dense runs holes and the u32 endpoint" {
    var bitmap = RoaringBitmap.init(std.testing.allocator);
    defer bitmap.deinit();
    try bitmap.addRange(65000, 200000);
    try bitmap.addRange(4294836224, @as(u64, std.math.maxInt(u32)) + 1);
    try bitmap.prepareRead();
    try std.testing.expectEqual(@as(u64, 42), bitmap.nextAbsent(42));
    try std.testing.expectEqual(@as(u64, 200000), bitmap.nextAbsent(65000));
    try std.testing.expectEqual(@as(u64, 200000), bitmap.nextAbsent(65535));
    try std.testing.expectEqual(@as(u64, 4294967296), bitmap.nextAbsent(4294836224));
}

test "word mask navigation intersects exclusions across sparse dense and u32 boundaries" {
    const a = std.testing.allocator;
    var include = RoaringBitmap.init(a);
    defer include.deinit();
    var exclude = RoaringBitmap.init(a);
    defer exclude.deinit();
    try include.addRange(65500, 140000);
    try include.add(std.math.maxInt(u32));
    for (65500..140000) |i| if (i % 2 == 0) {
        try exclude.add(@intCast(i));
    };
    try exclude.add(std.math.maxInt(u32));
    for ([_]u32{ 65500, 65535, 65536, 131071, 139998 }) |lower| {
        try std.testing.expectEqual(@as(?u32, if (lower % 2 == 0) lower + 1 else lower), RoaringBitmap.nextMatching(lower, 140000, &.{&include}, &.{&exclude}));
        try std.testing.expectEqual(@as(u64, if (lower % 2 == 0) lower + 1 else lower), exclude.nextAbsent(lower));
    }
    try std.testing.expect(RoaringBitmap.nextMatching(65500, 140000, &.{&include}, &.{&include}) == null);
    try std.testing.expect(RoaringBitmap.nextMatching(std.math.maxInt(u32), 0x1_0000_0000, &.{&include}, &.{&exclude}) == null);
    try std.testing.expectEqual(@as(u64, 0x1_0000_0000), exclude.nextAbsent(std.math.maxInt(u32)));
    try std.testing.expectEqual(@as(?u32, 139999), RoaringBitmap.nextMatching(139999, 140000, &.{&include}, &.{}));
    try std.testing.expectEqual(@as(u64, std.math.maxInt(u32)), RoaringBitmap.candidateLowerBound(140000, 0x1_0000_0000, &.{&include}, &.{}));
}
