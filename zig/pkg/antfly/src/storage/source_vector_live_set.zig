// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Rebuildable GC reachability, never ownership authority. A source snapshot
//! pins immutable segment identities for every bitmap bit. Duplicate physical
//! occurrences select the newest segment deterministically; WAL-only identities
//! use a bounded map. No digest or vector encoding is shortened.
const std = @import("std");
const native = @import("vector_block_store.zig");
const block = @import("antfly_vectorindex").vector_block;
const wal = @import("vector_wal_view.zig");
const Digest = [32]u8;
const Map = std.AutoHashMap(Digest, u32);

pub const LiveSet = struct {
    allocator: std.mem.Allocator,
    map: Map,
    source: ?native.Opened = null,
    offsets: []usize = &.{},
    bits: []u64 = &.{},
    marked: u32 = 0,

    pub fn init(alloc: std.mem.Allocator) LiveSet {
        return .{ .allocator = alloc, .map = Map.init(alloc) };
    }

    fn walCount(node: ?*wal.Node) usize {
        const n = node orelse return 0;
        return @intFromBool(n.record.kind == .upsert) + walCount(n.left) + walCount(n.right);
    }

    fn fallbackBound(source: *const native.Opened) usize {
        return if (source.wal_tree_initialized) walCount(source.wal_tree) else source.wal.records.items.len;
    }

    pub fn workspaceBytes(source: *const native.Opened) !usize {
        var entries: usize = 0;
        for (source.readers) |reader| entries = try std.math.add(usize, entries, reader.count);
        const bitmap = try std.math.mul(usize, (try std.math.add(usize, entries, 63)) / 64, 8);
        const slots = try std.math.ceilPowerOfTwo(usize, @max(8, try std.math.add(usize, (try std.math.mul(usize, fallbackBound(source), 5)) / 4, 1)));
        return std.math.add(usize, bitmap, try std.math.add(usize, try std.math.mul(usize, source.readers.len + 1, @sizeOf(usize)), try std.math.mul(usize, slots, 40)));
    }

    pub fn enableBitmaps(self: *LiveSet, source: *const native.Opened) !void {
        std.debug.assert(self.count() == 0 and self.source == null);
        var lease = try source.clone(self.allocator);
        errdefer lease.deinit();
        const offsets = try self.allocator.alloc(usize, lease.readers.len + 1);
        errdefer self.allocator.free(offsets);
        offsets[0] = 0;
        for (lease.readers, 0..) |reader, i| offsets[i + 1] = try std.math.add(usize, offsets[i], reader.count);
        const bits = try self.allocator.alloc(u64, (try std.math.add(usize, offsets[offsets.len - 1], 63)) / 64);
        errdefer self.allocator.free(bits);
        @memset(bits, 0);
        try self.map.ensureTotalCapacity(std.math.cast(u32, fallbackBound(&lease)) orelse return error.ResourceBudgetExceeded);
        self.source = lease;
        self.offsets = offsets;
        self.bits = bits;
    }

    const Location = struct { bit: usize, dims: u32 };
    fn locate(self: *const LiveSet, digest: Digest) ?Location {
        const source = if (self.source) |*s| s else return null;
        const hash = block.keyHash(&digest);
        const shard: usize = @intCast(hash & (source.store.manifest.?.shard_count - 1));
        const start = source.shard_offsets[shard];
        var pos = source.shard_offsets[shard + 1];
        while (pos > start) {
            pos -= 1;
            const index = source.reader_order[pos];
            const reader = source.readers[index];
            if (reader.sourceIndex(&digest, hash)) |row| {
                return .{ .bit = self.offsets[index] + row, .dims = reader.sourceIdentityAt(row).dims };
            }
        }
        return null;
    }

    fn isSet(self: *const LiveSet, bit: usize) bool {
        return self.bits[bit / 64] & (@as(u64, 1) << @as(u6, @intCast(bit % 64))) != 0;
    }

    pub fn put(self: *LiveSet, digest: Digest, dims: u32) !void {
        if (self.source) |*source| {
            if (self.locate(digest)) |location| {
                if (location.dims != dims) return error.VectorReferenceIdentityMismatch;
                if (!self.isSet(location.bit)) {
                    self.bits[location.bit / 64] |= @as(u64, 1) << @as(u6, @intCast(location.bit % 64));
                    self.marked = try std.math.add(u32, self.marked, 1);
                }
                return;
            }
            // A missing/corrupt owner cannot grow an unbounded fallback map.
            // Only payloads present in the admitted snapshot's WAL qualify.
            const found = try source.get(&digest, std.math.maxInt(u64), 1);
            if (found != .vector) return error.MissingCommittedVectorPayload;
            if (found.vector.dims != dims) return error.VectorReferenceIdentityMismatch;
        }
        if (self.map.get(digest)) |old| if (old != dims) return error.VectorReferenceIdentityMismatch;
        try self.map.put(digest, dims);
    }

    pub fn get(self: *const LiveSet, digest: Digest) ?u32 {
        if (self.map.get(digest)) |dims| return dims;
        const location = self.locate(digest) orelse return null;
        return if (self.isSet(location.bit)) location.dims else null;
    }
    pub fn contains(self: *const LiveSet, digest: Digest) bool {
        return self.get(digest) != null;
    }
    pub fn count(self: *const LiveSet) u32 {
        return self.marked + self.map.count();
    }
    pub fn bitmapBytes(self: *const LiveSet) usize {
        return self.bits.len * @sizeOf(u64) + self.offsets.len * @sizeOf(usize);
    }
    pub fn ensureTotalCapacity(self: *LiveSet, capacity: u32) !void {
        try self.map.ensureTotalCapacity(capacity);
    }
    pub fn clearRetainingCapacity(self: *LiveSet) void {
        if (self.source) |*source| source.deinit();
        self.source = null;
        self.allocator.free(self.bits);
        self.allocator.free(self.offsets);
        self.bits = &.{};
        self.offsets = &.{};
        self.marked = 0;
        self.map.clearRetainingCapacity();
    }
    pub fn deinit(self: *LiveSet) void {
        self.clearRetainingCapacity();
        self.map.deinit();
    }

    pub const Iterator = struct {
        set: *const LiveSet,
        map: Map.Iterator,
        reader: usize = 0,
        row: usize = 0,
        digest: Digest = undefined,
        dims: u32 = undefined,

        pub fn next(self: *Iterator) ?struct { key_ptr: *const Digest, value_ptr: *const u32 } {
            if (self.map.next()) |entry| return .{ .key_ptr = entry.key_ptr, .value_ptr = entry.value_ptr };
            const source = if (self.set.source) |*s| s else return null;
            while (self.reader < source.readers.len) {
                const reader = source.readers[self.reader];
                while (self.row < reader.count) {
                    const row = self.row;
                    self.row += 1;
                    if (!self.set.isSet(self.set.offsets[self.reader] + row)) continue;
                    const identity = reader.sourceIdentityAt(row);
                    std.debug.assert(identity.vector and identity.key.len == 32);
                    self.digest = identity.key[0..32].*;
                    self.dims = identity.dims;
                    return .{ .key_ptr = &self.digest, .value_ptr = &self.dims };
                }
                self.row = 0;
                self.reader += 1;
            }
            return null;
        }
    };
    pub fn iterator(self: *const LiveSet) Iterator {
        return .{ .set = self, .map = self.map.iterator() };
    }
    pub const ValueIterator = struct {
        entries: Iterator,
        pub fn next(self: *ValueIterator) ?*const u32 {
            return if (self.entries.next()) |entry| entry.value_ptr else null;
        }
    };
    pub fn valueIterator(self: *const LiveSet) ValueIterator {
        return .{ .entries = self.iterator() };
    }
};
