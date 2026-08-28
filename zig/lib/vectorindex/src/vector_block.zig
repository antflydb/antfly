// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//! Immutable, mmap-friendly exact-vector blocks.
//!
//! Blocks are table-level projections keyed by the authoritative embedding
//! artifact key, rather than by an index-local vector id. A block may retain
//! multiple source revisions for a key so a query holding an older HBC
//! generation lease cannot accidentally rerank with a newer vector. Primary
//! document/artifact storage remains authoritative; a missing revision is a
//! signal to fall back to that store.

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;

const magic: [8]u8 = .{ 'A', 'F', 'V', 'B', 'L', 'K', 0, 0 };
const legacy_version: u16 = 1;
const unscaled_encoding_version: u16 = 2;
const version: u16 = 3;
const header_size: usize = 40;
const legacy_index_entry_size: usize = 60;
const index_entry_size: usize = 64;
const footer_size: usize = 60;
const tombstone_flag: u32 = 1;

pub const Encoding = enum(u16) {
    float32 = 0,
    float16 = 1,

    pub fn componentBytes(self: Encoding) usize {
        return switch (self) {
            .float32 => @sizeOf(f32),
            .float16 => @sizeOf(f16),
        };
    }

    fn alignment(self: Encoding) usize {
        return switch (self) {
            .float32 => @alignOf(f32),
            .float16 => @alignOf(f16),
        };
    }
};

pub fn encodedVectorBytesLen(encoding: Encoding, dims: usize) !usize {
    if (dims == 0) return error.InvalidVectorDimensions;
    return std.math.mul(usize, dims, encoding.componentBytes()) catch error.VectorBlockTooLarge;
}

/// Encodes one exact-vector payload in the same representation stored by a
/// block. The returned scale must be persisted beside `out`; float16 blocks
/// use it to retain vectors whose components exceed the native f16 range.
pub fn encodeVectorInto(encoding: Encoding, vector: []const f32, out: []u8) !f32 {
    const expected = try encodedVectorBytesLen(encoding, vector.len);
    if (out.len != expected) return error.InvalidEncodedVectorLength;

    var scale: f32 = 1;
    if (encoding == .float16) {
        var max_abs: f32 = 0;
        // Validate while computing the scale so float16 encoding needs only
        // two source traversals (scale and conversion), not three.
        for (vector) |value| {
            if (!std.math.isFinite(value)) return error.InvalidVectorComponent;
            max_abs = @max(max_abs, @abs(value));
        }
        // Leave conversion headroom below the largest finite f16 so f32
        // division rounding cannot turn the extremum into inf.
        if (max_abs > 65_000.0) scale = max_abs / 65_000.0;
    }
    var pos: usize = 0;
    for (vector) |value| switch (encoding) {
        .float32 => {
            // float32 needs only this single validation/encoding traversal.
            if (!std.math.isFinite(value)) return error.InvalidVectorComponent;
            std.mem.writeInt(u32, out[pos..][0..4], @bitCast(value), .little);
            pos += 4;
        },
        .float16 => {
            const encoded: f16 = @floatCast(value / scale);
            std.mem.writeInt(u16, out[pos..][0..2], @bitCast(encoded), .little);
            pos += 2;
        },
    };
    return scale;
}

fn validateEncodedVector(encoding: Encoding, dims: usize, bytes: []const u8, scale: f32) !void {
    if (bytes.len != try encodedVectorBytesLen(encoding, dims)) return error.InvalidEncodedVectorLength;
    if (!std.math.isFinite(scale) or scale <= 0) return error.InvalidVectorScale;
    var pos: usize = 0;
    for (0..dims) |_| switch (encoding) {
        .float32 => {
            const value: f32 = @bitCast(std.mem.readInt(u32, bytes[pos..][0..4], .little));
            if (!std.math.isFinite(value)) return error.InvalidVectorComponent;
            pos += 4;
        },
        .float16 => {
            const value: f16 = @bitCast(std.mem.readInt(u16, bytes[pos..][0..2], .little));
            if (!std.math.isFinite(value)) return error.InvalidVectorComponent;
            pos += 2;
        },
    };
}

pub fn keyHash(key: []const u8) u64 {
    return std.hash.XxHash64.hash(0, key);
}

pub fn shardForKey(key: []const u8, shard_count: u32) !u32 {
    if (!validShardCount(shard_count)) return error.InvalidVectorBlockShardCount;
    return @intCast(keyHash(key) & (@as(u64, shard_count) - 1));
}

pub const Writer = struct {
    alloc: Allocator,
    // Keep keys and vector payloads in separate physical regions. Reader
    // admission validates every key to prove hash-shard membership and total
    // ordering before binary search is allowed. Interleaving each key with a
    // multi-KiB vector made that compact validation fault nearly the entire
    // mmap into RSS. New blocks retain keys in `data` and stage vectors here;
    // build() fixes their relative offsets after publishing the aligned vector
    // arena. The on-disk reader contract remains compatible with older blocks.
    vector_data: std.ArrayListUnmanaged(u8) = .empty,
    data: std.ArrayListUnmanaged(u8) = .empty,
    index: std.ArrayListUnmanaged(u8) = .empty,
    generation: u64,
    shard_id: u32,
    shard_count: u32,
    covered_source_sequence: u64,
    encoding: Encoding,
    count: u64 = 0,
    previous: ?OrderingKey = null,
    finished: bool = false,

    const OrderingKey = struct {
        hash: u64,
        key: []const u8,
        source_sequence: u64,
        revision: u64,
    };

    const VectorPayload = union(enum) {
        decoded: []const f32,
        encoded: struct {
            bytes: []const u8,
            scale: f32,
        },
    };

    pub fn init(
        alloc: Allocator,
        generation: u64,
        shard_id: u32,
        shard_count: u32,
        covered_source_sequence: u64,
    ) !Writer {
        return try initWithEncoding(alloc, generation, shard_id, shard_count, covered_source_sequence, .float32);
    }

    pub fn initWithEncoding(
        alloc: Allocator,
        generation: u64,
        shard_id: u32,
        shard_count: u32,
        covered_source_sequence: u64,
        encoding: Encoding,
    ) !Writer {
        if (!validShardCount(shard_count)) return error.InvalidVectorBlockShardCount;
        if (shard_id >= shard_count) return error.InvalidVectorBlockShard;
        var self: Writer = .{
            .alloc = alloc,
            .generation = generation,
            .shard_id = shard_id,
            .shard_count = shard_count,
            .covered_source_sequence = covered_source_sequence,
            .encoding = encoding,
        };
        errdefer self.deinit();
        try self.data.resize(alloc, header_size);
        @memcpy(self.data.items[0..magic.len], &magic);
        writeU16(self.data.items[8..10], version);
        writeU16(self.data.items[10..12], @intFromEnum(encoding));
        writeU64(self.data.items[12..20], generation);
        writeU32(self.data.items[20..24], shard_id);
        writeU32(self.data.items[24..28], shard_count);
        writeU64(self.data.items[28..36], covered_source_sequence);
        writeU32(self.data.items[36..40], std.hash.Crc32.hash(self.data.items[0..36]));
        return self;
    }

    pub fn deinit(self: *Writer) void {
        self.vector_data.deinit(self.alloc);
        self.data.deinit(self.alloc);
        self.index.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn appendVector(
        self: *Writer,
        key: []const u8,
        source_sequence: u64,
        revision: u64,
        vector: []const f32,
    ) !void {
        if (vector.len == 0) return error.InvalidVectorDimensions;
        for (vector) |value| if (!std.math.isFinite(value)) return error.InvalidVectorComponent;
        const dims = std.math.cast(u32, vector.len) orelse return error.VectorBlockTooLarge;
        try self.append(key, source_sequence, revision, dims, .{ .decoded = vector }, false);
    }

    /// Appends bytes produced by `encodeVectorInto` without decoding and
    /// encoding them again. This is used by bounded bulk builders that spool
    /// the target representation directly.
    pub fn appendEncodedVector(
        self: *Writer,
        key: []const u8,
        source_sequence: u64,
        revision: u64,
        dims: u32,
        bytes: []const u8,
        scale: f32,
    ) !void {
        try validateEncodedVector(self.encoding, dims, bytes, scale);
        try self.append(key, source_sequence, revision, dims, .{ .encoded = .{
            .bytes = bytes,
            .scale = scale,
        } }, false);
    }

    pub fn appendTombstone(self: *Writer, key: []const u8, source_sequence: u64, revision: u64) !void {
        try self.append(key, source_sequence, revision, 0, .{ .encoded = .{
            .bytes = &.{},
            .scale = 0,
        } }, true);
    }

    fn append(
        self: *Writer,
        key: []const u8,
        source_sequence: u64,
        revision: u64,
        dims: u32,
        payload: VectorPayload,
        tombstone: bool,
    ) !void {
        if (self.finished) return error.VectorBlockWriterFinished;
        if (key.len == 0) return error.InvalidVectorBlockKey;
        if (source_sequence > self.covered_source_sequence) return error.VectorBlockSequenceBeyondCoverage;
        const key_len = std.math.cast(u32, key.len) orelse return error.VectorBlockTooLarge;
        const hash = keyHash(key);
        if ((hash & (@as(u64, self.shard_count) - 1)) != self.shard_id) return error.WrongVectorBlockShard;
        const ordering: OrderingKey = .{
            .hash = hash,
            .key = key,
            .source_sequence = source_sequence,
            .revision = revision,
        };
        if (self.previous) |previous| {
            if (compareOrdering(previous, ordering) != .lt) return error.OutOfOrderVectorBlockEntry;
        }

        const key_offset = self.data.items.len;
        try self.data.appendSlice(self.alloc, key);
        var vector_offset: usize = 0;
        var vector_checksum: u32 = 0;
        var vector_scale: f32 = 1;
        if (!tombstone) {
            const padding = std.mem.alignForward(usize, self.vector_data.items.len, self.encoding.alignment()) - self.vector_data.items.len;
            try self.vector_data.appendNTimes(self.alloc, 0, padding);
            // This is relative to the vector arena until build() knows its
            // aligned absolute offset.
            vector_offset = self.vector_data.items.len;
            const vector_bytes = try encodedVectorBytesLen(self.encoding, dims);
            const old_len = self.vector_data.items.len;
            try self.vector_data.resize(self.alloc, std.math.add(usize, old_len, vector_bytes) catch return error.VectorBlockTooLarge);
            const destination = self.vector_data.items[old_len..][0..vector_bytes];
            switch (payload) {
                .decoded => |vector| vector_scale = try encodeVectorInto(self.encoding, vector, destination),
                .encoded => |encoded| {
                    try validateEncodedVector(self.encoding, dims, encoded.bytes, encoded.scale);
                    @memcpy(destination, encoded.bytes);
                    vector_scale = encoded.scale;
                },
            }
            vector_checksum = std.hash.Crc32.hash(destination);
        }

        try appendU64(self.alloc, &self.index, hash);
        try appendU64(self.alloc, &self.index, @intCast(key_offset));
        try appendU32(self.alloc, &self.index, key_len);
        try appendU32(self.alloc, &self.index, std.hash.Crc32.hash(key));
        try appendU64(self.alloc, &self.index, source_sequence);
        try appendU64(self.alloc, &self.index, revision);
        try appendU64(self.alloc, &self.index, @intCast(vector_offset));
        try appendU32(self.alloc, &self.index, dims);
        try appendU32(self.alloc, &self.index, if (tombstone) tombstone_flag else 0);
        try appendU32(self.alloc, &self.index, vector_checksum);
        try appendU32(self.alloc, &self.index, if (tombstone) 0 else @bitCast(vector_scale));
        self.previous = ordering;
        self.count += 1;
    }

    pub fn build(self: *Writer) ![]u8 {
        if (self.finished) return error.VectorBlockWriterFinished;
        self.finished = true;

        const vector_arena_offset = std.mem.alignForward(usize, self.data.items.len, self.encoding.alignment());
        try self.data.appendNTimes(self.alloc, 0, vector_arena_offset - self.data.items.len);
        try self.data.appendSlice(self.alloc, self.vector_data.items);

        // Index vector offsets were recorded relative to vector_data so keys
        // could remain contiguous. Convert only live entries; tombstones keep
        // the required all-zero vector tuple.
        var entry_offset: usize = 0;
        while (entry_offset < self.index.items.len) : (entry_offset += index_entry_size) {
            const flags = readU32(self.index.items[entry_offset + 52 ..][0..4]);
            if ((flags & tombstone_flag) != 0) continue;
            const relative = std.math.cast(usize, readU64(self.index.items[entry_offset + 40 ..][0..8])) orelse return error.VectorBlockTooLarge;
            const absolute = std.math.add(usize, vector_arena_offset, relative) catch return error.VectorBlockTooLarge;
            writeU64(self.index.items[entry_offset + 40 ..][0..8], absolute);
        }

        const index_offset = self.data.items.len;
        try self.data.appendSlice(self.alloc, self.index.items);
        const footer_start = self.data.items.len;
        try appendU64(self.alloc, &self.data, @intCast(index_offset));
        try appendU64(self.alloc, &self.data, self.count);
        try appendU64(self.alloc, &self.data, self.covered_source_sequence);
        try appendU64(self.alloc, &self.data, self.generation);
        try appendU32(self.alloc, &self.data, std.hash.Crc32.hash(self.index.items));
        try appendU16(self.alloc, &self.data, version);
        try appendU16(self.alloc, &self.data, @intFromEnum(self.encoding));
        try appendU32(self.alloc, &self.data, self.shard_id);
        try appendU32(self.alloc, &self.data, self.shard_count);
        try appendU32(self.alloc, &self.data, std.hash.Crc32.hash(self.data.items[footer_start..][0..48]));
        try self.data.appendSlice(self.alloc, &magic);
        self.vector_data.clearAndFree(self.alloc);
        self.index.clearAndFree(self.alloc);
        return try self.data.toOwnedSlice(self.alloc);
    }
};

pub const Value = struct {
    source_sequence: u64,
    revision: u64,
    dims: u32,
    bytes: []const u8,
    encoding: Encoding = .float32,
    scale: f32 = 1,

    pub fn vectorView(self: Value) ?[]const f32 {
        if (self.encoding != .float32 or self.scale != 1) return null;
        if (builtin.target.cpu.arch.endian() != .little) return null;
        if (@intFromPtr(self.bytes.ptr) % @alignOf(f32) != 0) return null;
        const aligned: []align(@alignOf(f32)) const u8 = @alignCast(self.bytes);
        return std.mem.bytesAsSlice(f32, aligned);
    }

    pub fn decodeInto(self: Value, out: []f32) ![]const f32 {
        if (self.dims > out.len) return error.BufferTooSmall;
        if (self.vectorView()) |view| {
            @memcpy(out[0..self.dims], view);
            return out[0..self.dims];
        }
        var pos: usize = 0;
        for (out[0..self.dims]) |*value| {
            switch (self.encoding) {
                .float32 => {
                    value.* = @bitCast(std.mem.readInt(u32, self.bytes[pos..][0..4], .little));
                    pos += 4;
                },
                .float16 => {
                    const encoded: f16 = @bitCast(std.mem.readInt(u16, self.bytes[pos..][0..2], .little));
                    value.* = @as(f32, @floatCast(encoded)) * self.scale;
                    pos += 2;
                },
            }
        }
        return out[0..self.dims];
    }
};

pub const Tombstone = struct {
    source_sequence: u64,
    revision: u64,
};

pub const Lookup = union(enum) {
    missing,
    tombstone: Tombstone,
    vector: Value,
};

/// Borrowed, checksum-verified view used by bounded generation compaction.
/// The key and vector bytes remain owned by the reader's mmap lease.
pub const EntryView = struct {
    key: []const u8,
    value: Lookup,
};

pub const Reader = struct {
    data: []const u8,
    index_offset: usize,
    count: usize,
    generation: u64,
    shard_id: u32,
    shard_count: u32,
    covered_source_sequence: u64,
    encoding: Encoding,
    index_entry_size: usize,

    const Entry = struct {
        hash: u64,
        key_offset: usize,
        key_len: usize,
        key_checksum: u32,
        source_sequence: u64,
        revision: u64,
        vector_offset: usize,
        dims: u32,
        flags: u32,
        vector_checksum: u32,
        vector_scale: f32,
    };

    pub fn init(data: []const u8) !Reader {
        if (data.len < header_size + footer_size) return error.CorruptedVectorBlock;
        if (!std.mem.eql(u8, data[0..8], &magic)) return error.CorruptedVectorBlock;
        const block_version = readU16(data[8..10]);
        const encoding: Encoding = switch (block_version) {
            legacy_version => if (readU16(data[10..12]) == 0) .float32 else return error.UnsupportedVectorBlockVersion,
            unscaled_encoding_version, version => switch (readU16(data[10..12])) {
                @intFromEnum(Encoding.float32) => .float32,
                @intFromEnum(Encoding.float16) => .float16,
                else => return error.UnsupportedVectorBlockEncoding,
            },
            else => return error.UnsupportedVectorBlockVersion,
        };
        const entry_size = if (block_version == version) index_entry_size else legacy_index_entry_size;
        if (readU32(data[36..40]) != std.hash.Crc32.hash(data[0..36])) return error.VectorBlockHeaderChecksumMismatch;

        const generation = readU64(data[12..20]);
        const shard_id = readU32(data[20..24]);
        const shard_count = readU32(data[24..28]);
        const covered_source_sequence = readU64(data[28..36]);
        if (!validShardCount(shard_count) or shard_id >= shard_count) return error.CorruptedVectorBlock;
        const footer = data[data.len - footer_size ..];
        if (!std.mem.eql(u8, footer[52..60], &magic)) return error.CorruptedVectorBlock;
        if (readU16(footer[36..38]) != block_version or readU16(footer[38..40]) != @intFromEnum(encoding)) return error.UnsupportedVectorBlockVersion;
        if (readU32(footer[48..52]) != std.hash.Crc32.hash(footer[0..48])) return error.VectorBlockFooterChecksumMismatch;
        if (readU64(footer[24..32]) != generation) return error.CorruptedVectorBlock;
        if (readU32(footer[40..44]) != shard_id or readU32(footer[44..48]) != shard_count) return error.CorruptedVectorBlock;
        if (readU64(footer[16..24]) != covered_source_sequence) return error.CorruptedVectorBlock;
        const index_offset = std.math.cast(usize, readU64(footer[0..8])) orelse return error.CorruptedVectorBlock;
        const count = std.math.cast(usize, readU64(footer[8..16])) orelse return error.CorruptedVectorBlock;
        const index_len = std.math.mul(usize, count, entry_size) catch return error.CorruptedVectorBlock;
        if (index_offset < header_size or index_offset + index_len != data.len - footer_size) return error.CorruptedVectorBlock;
        if (readU32(footer[32..36]) != std.hash.Crc32.hash(data[index_offset..][0..index_len])) return error.VectorBlockIndexChecksumMismatch;
        const reader: Reader = .{
            .data = data,
            .index_offset = index_offset,
            .count = count,
            .generation = generation,
            .shard_id = shard_id,
            .shard_count = shard_count,
            .covered_source_sequence = covered_source_sequence,
            .encoding = encoding,
            .index_entry_size = entry_size,
        };
        try reader.validate();
        return reader;
    }

    /// Cheap identity checksum for a manifest descriptor. Entry payloads keep
    /// their own lazy checksums; this binds the validated header, footer, and
    /// index checksum without rereading the potentially multi-GiB vector data.
    pub fn admissionChecksum(self: Reader) u32 {
        var identity: [12]u8 = undefined;
        writeU32(identity[0..4], readU32(self.data[36..40]));
        writeU32(identity[4..8], readU32(self.data[self.data.len - footer_size + 32 ..][0..4]));
        writeU32(identity[8..12], readU32(self.data[self.data.len - footer_size + 48 ..][0..4]));
        return std.hash.Crc32.hash(&identity);
    }

    pub fn get(self: Reader, key: []const u8, max_source_sequence: u64, expected_revision: ?u64) !Lookup {
        const hash = keyHash(key);
        return self.getHashed(key, hash, max_source_sequence, expected_revision);
    }

    /// Point lookup using a caller-provided hash. Reader admission has already
    /// verified the complete immutable index, every entry invariant, ordering,
    /// and every key checksum. Query lookups can therefore trust those regions
    /// for the lifetime of the mmap lease while payload CRCs remain lazy.
    pub fn getHashed(self: Reader, key: []const u8, hash: u64, max_source_sequence: u64, expected_revision: ?u64) !Lookup {
        if ((hash & (@as(u64, self.shard_count) - 1)) != self.shard_id) return .missing;
        var lo: usize = 0;
        var hi = self.count;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            const found = self.entryAssumeValidated(mid);
            if (self.compareEntryKeyAssumeValidated(found, hash, key) == .lt) lo = mid + 1 else hi = mid;
        }

        var selected: ?Entry = null;
        var pos = lo;
        while (pos < self.count) : (pos += 1) {
            const candidate = self.entryAssumeValidated(pos);
            if (self.compareEntryKeyAssumeValidated(candidate, hash, key) != .eq) break;
            if (candidate.source_sequence > max_source_sequence) break;
            selected = candidate;
        }
        const found = selected orelse return .missing;
        if (expected_revision) |expected| {
            if (found.revision != expected) return error.VectorBlockRevisionMismatch;
        }
        return try self.lookupFromEntry(found);
    }

    /// Returns one physical entry in block sort order. This deliberately does
    /// not expose the private index representation: callers receive the same
    /// lazy key/payload checksum guarantees as point lookup while retaining a
    /// zero-copy mmap view for merge compaction.
    pub fn entryAt(self: Reader, index: usize) !EntryView {
        const found = try self.entry(index);
        return .{
            .key = try self.entryKey(found),
            .value = try self.lookupFromEntry(found),
        };
    }

    fn lookupFromEntry(self: Reader, found: Entry) !Lookup {
        if ((found.flags & tombstone_flag) != 0) return .{ .tombstone = .{
            .source_sequence = found.source_sequence,
            .revision = found.revision,
        } };
        const byte_len = std.math.mul(usize, found.dims, self.encoding.componentBytes()) catch return error.CorruptedVectorBlock;
        const bytes = self.data[found.vector_offset..][0..byte_len];
        if (std.hash.Crc32.hash(bytes) != found.vector_checksum) return error.VectorBlockPayloadChecksumMismatch;
        return .{ .vector = .{
            .source_sequence = found.source_sequence,
            .revision = found.revision,
            .dims = found.dims,
            .bytes = bytes,
            .encoding = self.encoding,
            .scale = found.vector_scale,
        } };
    }

    fn entry(self: Reader, index: usize) !Entry {
        if (index >= self.count) return error.CorruptedVectorBlock;
        const raw = self.data[self.index_offset + index * self.index_entry_size ..][0..self.index_entry_size];
        const entry_value: Entry = .{
            .hash = readU64(raw[0..8]),
            .key_offset = std.math.cast(usize, readU64(raw[8..16])) orelse return error.CorruptedVectorBlock,
            .key_len = readU32(raw[16..20]),
            .key_checksum = readU32(raw[20..24]),
            .source_sequence = readU64(raw[24..32]),
            .revision = readU64(raw[32..40]),
            .vector_offset = std.math.cast(usize, readU64(raw[40..48])) orelse return error.CorruptedVectorBlock,
            .dims = readU32(raw[48..52]),
            .flags = readU32(raw[52..56]),
            .vector_checksum = readU32(raw[56..60]),
            .vector_scale = if (self.index_entry_size == index_entry_size)
                @bitCast(readU32(raw[60..64]))
            else if ((readU32(raw[52..56]) & tombstone_flag) != 0)
                0
            else
                1,
        };
        if (entry_value.key_offset < header_size or entry_value.key_offset > self.index_offset or entry_value.key_len > self.index_offset - entry_value.key_offset) return error.CorruptedVectorBlock;
        if (entry_value.source_sequence > self.covered_source_sequence) return error.CorruptedVectorBlock;
        if ((entry_value.flags & ~tombstone_flag) != 0) return error.CorruptedVectorBlock;
        const tombstone = (entry_value.flags & tombstone_flag) != 0;
        if (tombstone) {
            if (entry_value.vector_offset != 0 or entry_value.dims != 0 or entry_value.vector_checksum != 0 or entry_value.vector_scale != 0) return error.CorruptedVectorBlock;
        } else {
            if (!std.math.isFinite(entry_value.vector_scale) or entry_value.vector_scale <= 0) return error.CorruptedVectorBlock;
            if (entry_value.dims == 0 or entry_value.vector_offset < header_size or entry_value.vector_offset % self.encoding.alignment() != 0) return error.CorruptedVectorBlock;
            const byte_len = std.math.mul(usize, entry_value.dims, self.encoding.componentBytes()) catch return error.CorruptedVectorBlock;
            if (entry_value.vector_offset > self.index_offset or byte_len > self.index_offset - entry_value.vector_offset) return error.CorruptedVectorBlock;
        }
        return entry_value;
    }

    fn entryAssumeValidated(self: Reader, index: usize) Entry {
        std.debug.assert(index < self.count);
        const raw = self.data[self.index_offset + index * self.index_entry_size ..][0..self.index_entry_size];
        return .{
            .hash = readU64(raw[0..8]),
            .key_offset = @intCast(readU64(raw[8..16])),
            .key_len = readU32(raw[16..20]),
            .key_checksum = readU32(raw[20..24]),
            .source_sequence = readU64(raw[24..32]),
            .revision = readU64(raw[32..40]),
            .vector_offset = @intCast(readU64(raw[40..48])),
            .dims = readU32(raw[48..52]),
            .flags = readU32(raw[52..56]),
            .vector_checksum = readU32(raw[56..60]),
            .vector_scale = if (self.index_entry_size == index_entry_size)
                @bitCast(readU32(raw[60..64]))
            else if ((readU32(raw[52..56]) & tombstone_flag) != 0)
                0
            else
                1,
        };
    }

    fn entryKey(self: Reader, entry_value: Entry) ![]const u8 {
        const bytes = self.data[entry_value.key_offset..][0..entry_value.key_len];
        if (std.hash.Crc32.hash(bytes) != entry_value.key_checksum) return error.VectorBlockKeyChecksumMismatch;
        return bytes;
    }

    fn compareEntryKeyAssumeValidated(self: Reader, entry_value: Entry, hash: u64, key_value: []const u8) std.math.Order {
        if (entry_value.hash != hash) return std.math.order(entry_value.hash, hash);
        const key = self.data[entry_value.key_offset..][0..entry_value.key_len];
        return std.mem.order(u8, key, key_value);
    }

    fn validate(self: Reader) !void {
        var previous: ?Writer.OrderingKey = null;
        for (0..self.count) |i| {
            const current = try self.entry(i);
            const current_key = try self.entryKey(current);
            if (keyHash(current_key) != current.hash) return error.CorruptedVectorBlock;
            if ((current.hash & (@as(u64, self.shard_count) - 1)) != self.shard_id) return error.CorruptedVectorBlock;
            const ordering: Writer.OrderingKey = .{
                .hash = current.hash,
                .key = current_key,
                .source_sequence = current.source_sequence,
                .revision = current.revision,
            };
            if (previous) |old| if (compareOrdering(old, ordering) != .lt) return error.CorruptedVectorBlock;
            previous = ordering;
        }
    }
};

fn validShardCount(shard_count: u32) bool {
    return shard_count != 0 and std.math.isPowerOfTwo(shard_count);
}

fn compareOrdering(lhs: Writer.OrderingKey, rhs: Writer.OrderingKey) std.math.Order {
    if (lhs.hash != rhs.hash) return std.math.order(lhs.hash, rhs.hash);
    const key_order = std.mem.order(u8, lhs.key, rhs.key);
    if (key_order != .eq) return key_order;
    if (lhs.source_sequence != rhs.source_sequence) return std.math.order(lhs.source_sequence, rhs.source_sequence);
    return std.math.order(lhs.revision, rhs.revision);
}

fn appendU16(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: u16) !void {
    var buf: [2]u8 = undefined;
    writeU16(&buf, value);
    try out.appendSlice(alloc, &buf);
}

fn appendU32(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: u32) !void {
    var buf: [4]u8 = undefined;
    writeU32(&buf, value);
    try out.appendSlice(alloc, &buf);
}

fn appendU64(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: u64) !void {
    var buf: [8]u8 = undefined;
    writeU64(&buf, value);
    try out.appendSlice(alloc, &buf);
}

fn writeU16(out: []u8, value: u16) void {
    std.mem.writeInt(u16, out[0..2], value, .big);
}
fn writeU32(out: []u8, value: u32) void {
    std.mem.writeInt(u32, out[0..4], value, .big);
}
fn writeU64(out: []u8, value: u64) void {
    std.mem.writeInt(u64, out[0..8], value, .big);
}
fn readU16(bytes: []const u8) u16 {
    return std.mem.readInt(u16, bytes[0..2], .big);
}
fn readU32(bytes: []const u8) u32 {
    return std.mem.readInt(u32, bytes[0..4], .big);
}
fn readU64(bytes: []const u8) u64 {
    return std.mem.readInt(u64, bytes[0..8], .big);
}

test "vector block preserves generation revisions and tombstones" {
    const alloc = std.testing.allocator;
    const shard_count: u32 = 1;
    var writer = try Writer.init(alloc, 7, 0, shard_count, 30);
    defer writer.deinit();
    try writer.appendVector("artifact-a", 10, 1, &.{ 1.0, 2.0, 3.0 });
    try writer.appendVector("artifact-a", 20, 2, &.{ 4.0, 5.0, 6.0 });
    try writer.appendTombstone("artifact-a", 30, 3);
    const encoded = try writer.build();
    defer alloc.free(encoded);

    const reader = try Reader.init(encoded);
    try std.testing.expectEqual(@as(u64, 7), reader.generation);
    try std.testing.expectEqual(@as(u64, 30), reader.covered_source_sequence);
    const first_entry = try reader.entry(0);
    const second_entry = try reader.entry(1);
    const tombstone_entry = try reader.entry(2);
    // Admission walks every key. Keep that compact region wholly before the
    // aligned vector arena so validation cannot fault vector payload pages in.
    try std.testing.expect(first_entry.key_offset + first_entry.key_len <= second_entry.key_offset);
    try std.testing.expect(second_entry.key_offset + second_entry.key_len <= tombstone_entry.key_offset);
    try std.testing.expect(tombstone_entry.key_offset + tombstone_entry.key_len <= first_entry.vector_offset);
    try std.testing.expectEqual(first_entry.vector_offset + 3 * @sizeOf(f32), second_entry.vector_offset);
    try std.testing.expect(second_entry.vector_offset + 3 * @sizeOf(f32) <= reader.index_offset);
    const old = try reader.get("artifact-a", 15, 1);
    try std.testing.expectEqual(@as(u64, 10), old.vector.source_sequence);
    try std.testing.expectEqualSlices(f32, &.{ 1.0, 2.0, 3.0 }, old.vector.vectorView().?);
    const old_hashed = try reader.getHashed("artifact-a", keyHash("artifact-a"), 15, 1);
    try std.testing.expectEqual(@as(u64, 10), old_hashed.vector.source_sequence);
    try std.testing.expectEqualSlices(f32, &.{ 1.0, 2.0, 3.0 }, old_hashed.vector.vectorView().?);
    try std.testing.expectEqual(@as(u64, 2), (try reader.get("artifact-a", 25, null)).vector.revision);
    try std.testing.expectEqual(@as(u64, 3), (try reader.get("artifact-a", 30, null)).tombstone.revision);
    try std.testing.expectError(error.VectorBlockRevisionMismatch, reader.get("artifact-a", 25, 1));
    try std.testing.expect((try reader.get("missing", 30, null)) == .missing);
}

test "vector block float16 projection round trips through explicit encoding" {
    const alloc = std.testing.allocator;
    var writer = try Writer.initWithEncoding(alloc, 9, 0, 1, 1, .float16);
    defer writer.deinit();
    try writer.appendVector("artifact-a", 1, 7, &.{ 0.125, -0.33325, 4.5 });
    const encoded = try writer.build();
    defer alloc.free(encoded);

    const reader = try Reader.init(encoded);
    try std.testing.expectEqual(Encoding.float16, reader.encoding);
    const value = (try reader.get("artifact-a", 1, 7)).vector;
    try std.testing.expect(value.vectorView() == null);
    try std.testing.expectEqual(@as(usize, 3 * @sizeOf(f16)), value.bytes.len);
    var decoded: [3]f32 = undefined;
    const view = try value.decodeInto(&decoded);
    try std.testing.expectApproxEqAbs(@as(f32, 0.125), view[0], 0.0005);
    try std.testing.expectApproxEqAbs(@as(f32, -0.33325), view[1], 0.0005);
    try std.testing.expectApproxEqAbs(@as(f32, 4.5), view[2], 0.0005);
}

test "vector block float16 projection scales values outside its native finite domain" {
    const alloc = std.testing.allocator;
    var writer = try Writer.initWithEncoding(alloc, 1, 0, 1, 1, .float16);
    defer writer.deinit();
    try writer.appendVector("artifact-a", 1, 1, &.{ 100_000.0, -250_000.0 });
    const encoded = try writer.build();
    defer alloc.free(encoded);
    const reader = try Reader.init(encoded);
    const value = (try reader.get("artifact-a", 1, 1)).vector;
    try std.testing.expect(value.scale > 1);
    var decoded: [2]f32 = undefined;
    const view = try value.decodeInto(&decoded);
    try std.testing.expectApproxEqRel(@as(f32, 100_000.0), view[0], 0.001);
    try std.testing.expectApproxEqRel(@as(f32, -250_000.0), view[1], 0.001);
}

test "vector block accepts validated pre-encoded payload without changing representation" {
    const alloc = std.testing.allocator;
    const source = [_]f32{ 100_000.0, -250_000.0, 6.0 };
    var payload: [source.len * @sizeOf(f16)]u8 = undefined;
    const scale = try encodeVectorInto(.float16, &source, &payload);

    var writer = try Writer.initWithEncoding(alloc, 1, 0, 1, 1, .float16);
    defer writer.deinit();
    try writer.appendEncodedVector("artifact-a", 1, 1, source.len, &payload, scale);
    const block = try writer.build();
    defer alloc.free(block);

    const value = (try (try Reader.init(block)).get("artifact-a", 1, 1)).vector;
    try std.testing.expectEqual(scale, value.scale);
    try std.testing.expectEqualSlices(u8, &payload, value.bytes);
    var decoded: [source.len]f32 = undefined;
    const actual = try value.decodeInto(&decoded);
    for (source, actual) |expected, found| try std.testing.expectApproxEqRel(expected, found, 0.001);
}

test "vector block rejects malformed pre-encoded payload before writer mutation" {
    const alloc = std.testing.allocator;
    var writer = try Writer.initWithEncoding(alloc, 1, 0, 1, 1, .float16);
    defer writer.deinit();
    try std.testing.expectError(
        error.InvalidEncodedVectorLength,
        writer.appendEncodedVector("artifact-a", 1, 1, 2, &.{0}, 1),
    );
    var non_finite: [2]u8 = undefined;
    std.mem.writeInt(u16, &non_finite, @bitCast(std.math.inf(f16)), .little);
    try std.testing.expectError(
        error.InvalidVectorComponent,
        writer.appendEncodedVector("artifact-a", 1, 1, 1, &non_finite, 1),
    );
    try writer.appendVector("artifact-a", 1, 1, &.{1.0});
    const block = try writer.build();
    defer alloc.free(block);
    _ = try Reader.init(block);
}

test "vector block rejects non-finite components before mutating writer state" {
    const alloc = std.testing.allocator;
    var writer = try Writer.initWithEncoding(alloc, 1, 0, 1, 1, .float16);
    defer writer.deinit();
    try std.testing.expectError(error.InvalidVectorComponent, writer.appendVector("artifact-a", 1, 1, &.{std.math.nan(f32)}));
    try writer.appendVector("artifact-a", 1, 1, &.{1.0});
    const encoded = try writer.build();
    defer alloc.free(encoded);
    _ = try Reader.init(encoded);
}

test "vector block validates shards and entry ordering" {
    const alloc = std.testing.allocator;
    const key = "artifact-for-shard";
    const shard_count: u32 = 8;
    const shard_id = try shardForKey(key, shard_count);
    var writer = try Writer.init(alloc, 1, shard_id, shard_count, 5);
    defer writer.deinit();
    try writer.appendVector(key, 5, 1, &.{ 0.25, -0.5 });
    try std.testing.expectError(error.OutOfOrderVectorBlockEntry, writer.appendVector(key, 5, 1, &.{1.0}));
    try std.testing.expectError(error.VectorBlockSequenceBeyondCoverage, writer.appendVector(key, 6, 2, &.{1.0}));

    const wrong_shard = (shard_id + 1) % shard_count;
    var wrong = try Writer.init(alloc, 1, wrong_shard, shard_count, 5);
    defer wrong.deinit();
    try std.testing.expectError(error.WrongVectorBlockShard, wrong.appendVector(key, 5, 1, &.{1.0}));
}

test "vector block detects lazy vector corruption" {
    const alloc = std.testing.allocator;
    var writer = try Writer.init(alloc, 1, 0, 1, 1);
    defer writer.deinit();
    try writer.appendVector("artifact-a", 1, 1, &.{ 1.0, 2.0 });
    const encoded = try writer.build();
    defer alloc.free(encoded);
    const pristine = try Reader.init(encoded);
    const value = (try pristine.get("artifact-a", 1, null)).vector;
    encoded[@intFromPtr(value.bytes.ptr) - @intFromPtr(encoded.ptr)] ^= 0xff;
    const reader = try Reader.init(encoded);
    try std.testing.expectError(error.VectorBlockPayloadChecksumMismatch, reader.get("artifact-a", 1, null));
}
