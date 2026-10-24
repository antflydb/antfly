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

const std = @import("std");
const read_admission = @import("../../common/read_admission.zig");
const Crc32 = @import("antfly_hash").Crc32;
const bloom = @import("bloom");
const byte_copy = @import("../../common/byte_copy.zig");
const snappy = @import("../../encoding/snappy.zig");

pub const magic = "ALSMTBL3";
pub const footer_magic = "ALSMIDX3";
pub const header_len = magic.len + 12;
pub const footer_len = footer_magic.len + @sizeOf(u32) * 6 + @sizeOf(u64) * 2;
pub const default_block_size: usize = 32 * 1024;
pub const version: u32 = 11;
pub const max_entry_data_len: usize = std.math.maxInt(u32);
pub const max_entry_count: usize = std.math.maxInt(u32);
pub const default_filter_config: bloom.Config = .{ .bits_per_key = 14 };
const block_filter_config: bloom.Config = default_filter_config;
const min_compress_block_bytes: usize = 1024;
const compression_savings_denominator: usize = 8;
const snappy_rejection_streak_limit: usize = 4;
const snappy_reprobe_interval: usize = 32;
const prefix_block_magic = "ALSMPFX1";
const prefix_restart_interval: usize = 16;
/// Footer metadata starts with an explicit marker because footer-only index
/// reads deliberately avoid fetching the table header.
const packed_offsets_magic = "ALSMOFF1";

pub const PrefixExtractor = enum(u32) {
    none = 0,
    first_separator = 1,
};

pub const default_prefix_extractor: PrefixExtractor = .first_separator;

pub const BlockCompression = enum(u32) {
    none = 0,
    snappy = 1,
    prefix = 2,
    prefix_snappy = 3,
};

pub const CompressionPolicy = enum(u8) {
    none,
    snappy_adaptive,
};

pub const CompressionStats = struct {
    logical_entry_bytes: u64 = 0,
    physical_entry_bytes: u64 = 0,
    raw_blocks: u64 = 0,
    compressed_blocks: u64 = 0,
    compression_codec_mask: u64 = 0,

    pub fn compressedBytesSaved(self: CompressionStats) u64 {
        return if (self.logical_entry_bytes > self.physical_entry_bytes)
            self.logical_entry_bytes - self.physical_entry_bytes
        else
            0;
    }

    pub fn compressionRatioBps(self: CompressionStats) u64 {
        if (self.logical_entry_bytes == 0) return 10_000;
        return (self.physical_entry_bytes * 10_000) / self.logical_entry_bytes;
    }

    pub fn add(self: *CompressionStats, other: CompressionStats) void {
        self.logical_entry_bytes +|= other.logical_entry_bytes;
        self.physical_entry_bytes +|= other.physical_entry_bytes;
        self.raw_blocks +|= other.raw_blocks;
        self.compressed_blocks +|= other.compressed_blocks;
        self.compression_codec_mask |= other.compression_codec_mask;
    }
};

pub fn blockCompressionCodecMask(codec: BlockCompression) u64 {
    return @as(u64, 1) << @intCast(@backingInt(codec));
}

pub const EncodeOptions = struct {
    block_compression: CompressionPolicy = .snappy_adaptive,
    prefix_extractor: PrefixExtractor = default_prefix_extractor,
    compression_stats: ?*CompressionStats = null,
};

pub const Entry = struct {
    namespace_name: ?[]const u8 = null,
    key: []const u8,
    value: []const u8,
    tombstone: bool = false,
};

pub const OwnedEntry = struct {
    namespace_name: ?[]u8 = null,
    key: []u8,
    value: []u8,
    tombstone: bool = false,

    pub fn deinit(self: *OwnedEntry, allocator: std.mem.Allocator) void {
        if (self.namespace_name) |name| allocator.free(name);
        allocator.free(self.key);
        allocator.free(self.value);
        self.* = undefined;
    }
};

pub const Decoded = struct {
    entries: []OwnedEntry,
    filter: bloom.OwnedFilter,

    pub fn deinit(self: *Decoded, allocator: std.mem.Allocator) void {
        for (self.entries) |*entry| entry.deinit(allocator);
        allocator.free(self.entries);
        self.filter.deinit(allocator);
        self.* = undefined;
    }
};

pub const TableIndex = struct {
    pub const BlockMeta = struct {
        relative_offset: u32,
        len: u32,
        physical_relative_offset: u32 = 0,
        physical_len: u32 = 0,
        compression: BlockCompression = .none,
        checksum: u32 = 0,
        first_entry_index: u32,
        entry_count: u32,
        smallest_namespace_name: ?[]u8 = null,
        smallest_key: ?[]u8 = null,
        largest_namespace_name: ?[]u8 = null,
        largest_key: []u8,
        filter: ?bloom.OwnedFilter = null,
        prefix_filter: ?bloom.OwnedFilter = null,
        hash_slots: []u32 = &.{},

        pub fn deinit(self: *BlockMeta, allocator: std.mem.Allocator) void {
            if (self.smallest_namespace_name) |name| allocator.free(name);
            if (self.smallest_key) |key| allocator.free(key);
            if (self.largest_namespace_name) |name| allocator.free(name);
            allocator.free(self.largest_key);
            if (self.filter) |*filter| filter.deinit(allocator);
            if (self.prefix_filter) |*filter| filter.deinit(allocator);
            allocator.free(self.hash_slots);
            self.* = undefined;
        }

        pub fn clone(self: *const BlockMeta, allocator: std.mem.Allocator) !BlockMeta {
            const smallest_namespace_name = if (self.smallest_namespace_name) |name| try allocator.dupe(u8, name) else null;
            errdefer if (smallest_namespace_name) |name| allocator.free(name);
            const smallest_key = if (self.smallest_key) |key| try allocator.dupe(u8, key) else null;
            errdefer if (smallest_key) |key| allocator.free(key);
            const largest_namespace_name = if (self.largest_namespace_name) |name| try allocator.dupe(u8, name) else null;
            errdefer if (largest_namespace_name) |name| allocator.free(name);
            const largest_key = try allocator.dupe(u8, self.largest_key);
            errdefer allocator.free(largest_key);
            const filter = if (self.filter) |owned_filter| try owned_filter.clone(allocator) else null;
            errdefer if (filter) |*owned_filter| owned_filter.deinit(allocator);
            const prefix_filter = if (self.prefix_filter) |owned_filter| try owned_filter.clone(allocator) else null;
            errdefer if (prefix_filter) |*owned_filter| owned_filter.deinit(allocator);
            const hash_slots = try allocator.dupe(u32, self.hash_slots);
            errdefer allocator.free(hash_slots);
            return .{
                .relative_offset = self.relative_offset,
                .len = self.len,
                .physical_relative_offset = self.physicalRelativeOffset(),
                .physical_len = self.physicalLen(),
                .compression = self.compression,
                .checksum = self.checksum,
                .first_entry_index = self.first_entry_index,
                .entry_count = self.entry_count,
                .smallest_namespace_name = smallest_namespace_name,
                .smallest_key = smallest_key,
                .largest_namespace_name = largest_namespace_name,
                .largest_key = largest_key,
                .filter = filter,
                .prefix_filter = prefix_filter,
                .hash_slots = hash_slots,
            };
        }

        pub fn lastEntryIndex(self: *const BlockMeta) usize {
            return self.first_entry_index + self.entry_count - 1;
        }

        pub fn maybeContains(self: *const BlockMeta, namespace_name: ?[]const u8, key: []const u8) bool {
            if (self.filter) |filter| {
                const hashes = entryHashes(namespace_name, key);
                return filter.maybeContainsHashes(hashes[0], hashes[1]);
            }
            return true;
        }

        pub fn maybeContainsPrefix(self: *const BlockMeta, namespace_name: ?[]const u8, prefix: []const u8) bool {
            if (self.prefix_filter) |filter| {
                const hashes = prefixHashes(namespace_name, prefix);
                return filter.maybeContainsHashes(hashes[0], hashes[1]);
            }
            return true;
        }

        pub fn mayContainKeyByBounds(self: *const BlockMeta, namespace_name: ?[]const u8, key: []const u8) bool {
            if (compareKeyBound(self.largest_namespace_name, self.largest_key, namespace_name, key) == .lt) return false;
            if (self.smallest_key) |smallest_key| {
                if (compareKeyBound(self.smallest_namespace_name, smallest_key, namespace_name, key) == .gt) return false;
            }
            return true;
        }

        pub fn mayContainAtOrAfter(self: *const BlockMeta, namespace_name: ?[]const u8, key: []const u8) bool {
            return compareKeyBound(self.largest_namespace_name, self.largest_key, namespace_name, key) != .lt;
        }

        pub fn rangeMayOverlap(self: *const BlockMeta, lower_namespace_name: ?[]const u8, lower_key: []const u8, upper_namespace_name: ?[]const u8, upper_key: []const u8) bool {
            if (compareKeyBound(self.largest_namespace_name, self.largest_key, lower_namespace_name, lower_key) == .lt) return false;
            if (self.smallest_key) |smallest_key| {
                if (compareKeyBound(self.smallest_namespace_name, smallest_key, upper_namespace_name, upper_key) == .gt) return false;
            }
            return true;
        }

        pub fn physicalRelativeOffset(self: *const BlockMeta) u32 {
            return if (self.physical_len == 0 and self.compression == .none) self.relative_offset else self.physical_relative_offset;
        }

        pub fn physicalLen(self: *const BlockMeta) u32 {
            return if (self.physical_len == 0 and self.compression == .none) self.len else self.physical_len;
        }
    };

    // Tables persist offsets as u16 values local to each block.
    entry_offsets: []u32 = &.{},
    block_entry_offsets: []u16 = &.{},
    entry_data_start: usize,
    entry_data_len: usize,
    filter: bloom.OwnedFilter,
    prefix_extractor: PrefixExtractor = .none,
    prefix_filter: ?bloom.OwnedFilter = null,
    blocks: []BlockMeta = &.{},

    pub fn deinit(self: *TableIndex, allocator: std.mem.Allocator) void {
        allocator.free(self.entry_offsets);
        allocator.free(self.block_entry_offsets);
        for (self.blocks) |*block| block.deinit(allocator);
        allocator.free(self.blocks);
        self.filter.deinit(allocator);
        if (self.prefix_filter) |*filter| filter.deinit(allocator);
        self.* = undefined;
    }

    pub fn clone(self: *const TableIndex, allocator: std.mem.Allocator) !TableIndex {
        const blocks = try allocator.alloc(BlockMeta, self.blocks.len);
        errdefer allocator.free(blocks);
        var block_count: usize = 0;
        errdefer {
            for (blocks[0..block_count]) |*block| block.deinit(allocator);
        }
        for (self.blocks, 0..) |block, i| {
            blocks[i] = try block.clone(allocator);
            block_count += 1;
        }
        const entry_offsets = try allocator.dupe(u32, self.entry_offsets);
        errdefer allocator.free(entry_offsets);
        const block_entry_offsets = try allocator.dupe(u16, self.block_entry_offsets);
        errdefer allocator.free(block_entry_offsets);
        const filter = try self.filter.clone(allocator);
        errdefer {
            var cleanup = filter;
            cleanup.deinit(allocator);
        }
        const prefix_filter = if (self.prefix_filter) |owned_filter| try owned_filter.clone(allocator) else null;
        errdefer if (prefix_filter) |owned_filter| {
            var cleanup = owned_filter;
            cleanup.deinit(allocator);
        };
        return .{
            .entry_offsets = entry_offsets,
            .block_entry_offsets = block_entry_offsets,
            .entry_data_start = self.entry_data_start,
            .entry_data_len = self.entry_data_len,
            .filter = filter,
            .prefix_extractor = self.prefix_extractor,
            .prefix_filter = prefix_filter,
            .blocks = blocks,
        };
    }

    pub fn borrowFilter(self: *const TableIndex) bloom.BorrowedFilter {
        return .{
            .bytes = self.filter.bytes,
            .bit_count = self.filter.bit_count,
            .hash_count = self.filter.hash_count,
        };
    }

    pub fn maybeContainsPrefix(self: *const TableIndex, namespace_name: ?[]const u8, prefix: []const u8) bool {
        if (self.prefix_filter) |filter| {
            const hashes = prefixHashes(namespace_name, prefix);
            return filter.maybeContainsHashes(hashes[0], hashes[1]);
        }
        return true;
    }

    pub fn entryCount(self: *const TableIndex) usize {
        return @max(self.entry_offsets.len, self.block_entry_offsets.len);
    }

    pub fn blockCount(self: *const TableIndex) usize {
        return self.blocks.len;
    }

    pub fn findBlockIndex(self: *const TableIndex, namespace_name: ?[]const u8, key: []const u8) ?usize {
        if (self.blocks.len == 0) return null;
        var lo: usize = 0;
        var hi: usize = self.blocks.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (compareKeyBound(self.blocks[mid].largest_namespace_name, self.blocks[mid].largest_key, namespace_name, key) == .lt) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if (lo >= self.blocks.len) return null;
        return lo;
    }

    pub fn findBlockLowerBound(self: *const TableIndex, namespace_name: ?[]const u8, key: []const u8) ?usize {
        if (self.blocks.len == 0) return null;
        var lo: usize = 0;
        var hi: usize = self.blocks.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (compareKeyBound(self.blocks[mid].largest_namespace_name, self.blocks[mid].largest_key, namespace_name, key) == .lt) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if (lo >= self.blocks.len) return null;
        return lo;
    }

    pub fn findBlockIndexForEntry(self: *const TableIndex, entry_index: usize) ?usize {
        if (self.blocks.len == 0) return null;
        var lo: usize = 0;
        var hi: usize = self.blocks.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            const block = self.blocks[mid];
            if (entry_index < block.first_entry_index) {
                hi = mid;
            } else if (entry_index > block.lastEntryIndex()) {
                lo = mid + 1;
            } else {
                return mid;
            }
        }
        return null;
    }

    pub fn blockWindow(self: *const TableIndex, block_index: usize) EntryDataWindow {
        const block = self.blocks[block_index];
        return .{
            .relative_offset = block.relative_offset,
            .len = block.len,
            .physical_relative_offset = block.physicalRelativeOffset(),
            .physical_len = block.physicalLen(),
            .compression = block.compression,
            .checksum = block.checksum,
        };
    }

    pub fn entryStart(self: *const TableIndex, index: usize) u32 {
        if (self.block_entry_offsets.len == 0) return self.entry_offsets[index];
        const block_index = self.findBlockIndexForEntry(index) orelse unreachable;
        return self.entryStartInBlock(index, block_index);
    }

    pub fn entryStartInBlock(self: *const TableIndex, index: usize, block_index: usize) u32 {
        if (self.block_entry_offsets.len == 0) return self.entry_offsets[index];
        return self.blocks[block_index].relative_offset + self.block_entry_offsets[index];
    }

    pub fn entryEnd(self: *const TableIndex, index: usize) u32 {
        if (self.block_entry_offsets.len == 0) {
            if (index + 1 < self.entry_offsets.len) return self.entry_offsets[index + 1];
            return @intCast(self.entry_data_len);
        }
        const block_index = self.findBlockIndexForEntry(index) orelse unreachable;
        const block = self.blocks[block_index];
        if (index + 1 < block.first_entry_index + block.entry_count) {
            return block.relative_offset + self.block_entry_offsets[index + 1];
        }
        return block.relative_offset + block.len;
    }
};

pub const Header = struct {
    version: u32,
    entry_count: usize,
    entry_data_len: usize,
    entry_offsets_start: usize,
    entry_data_start: usize,
};

pub const Footer = struct {
    metadata_offset: usize,
    metadata_len: usize,
    entry_count: usize,
    entry_data_len: usize,
    metadata_checksum: u32,
};

pub const EntryDataWindow = struct {
    relative_offset: u32,
    len: u32,
    physical_relative_offset: u32 = 0,
    physical_len: u32 = 0,
    compression: BlockCompression = .none,
    checksum: u32 = 0,

    pub fn physicalRelativeOffset(self: EntryDataWindow) u32 {
        return if (self.physical_len == 0 and self.compression == .none) self.relative_offset else self.physical_relative_offset;
    }

    pub fn physicalLen(self: EntryDataWindow) u32 {
        return if (self.physical_len == 0 and self.compression == .none) self.len else self.physical_len;
    }
};

/// The metadata required by a forward-only table scan. Compaction never does
/// point lookups, so retaining entry offsets, Bloom filters, per-block hash
/// tables, and key bounds for every input run only multiplies memory by the
/// merge fan-in.
pub const SequentialTableIndex = struct {
    pub const Block = struct {
        window: EntryDataWindow,
        entry_count: usize,
    };

    entry_data_start: usize,
    entry_count: usize,
    blocks: []Block,

    pub fn deinit(self: *SequentialTableIndex, allocator: std.mem.Allocator) void {
        allocator.free(self.blocks);
        self.* = undefined;
    }
};

pub const BorrowedDecoded = struct {
    pub const PositionedEntry = struct {
        index: usize,
        entry: Entry,
    };

    raw: []u8,
    owns_raw: bool = true,
    entry_offsets: []const u32,
    owns_entry_offsets: bool = true,
    block_entry_offsets: []const u16 = &.{},
    owns_block_entry_offsets: bool = true,
    entry_data_start: usize,
    filter: bloom.BorrowedFilter,
    owned_filter: ?bloom.OwnedFilter = null,
    blocks: []const TableIndex.BlockMeta = &.{},
    owns_blocks: bool = true,

    pub fn deinit(self: *BorrowedDecoded, allocator: std.mem.Allocator) void {
        if (self.owned_filter) |*filter| filter.deinit(allocator);
        if (self.owns_blocks) {
            for (@constCast(self.blocks)) |*block| block.deinit(allocator);
            allocator.free(@constCast(self.blocks));
        }
        if (self.owns_entry_offsets) allocator.free(@constCast(self.entry_offsets));
        if (self.owns_block_entry_offsets) allocator.free(@constCast(self.block_entry_offsets));
        if (self.owns_raw) allocator.free(self.raw);
        self.* = undefined;
    }

    pub fn entryCount(self: *const BorrowedDecoded) usize {
        return @max(self.entry_offsets.len, self.block_entry_offsets.len);
    }

    fn entryStart(self: *const BorrowedDecoded, index: usize) u32 {
        if (self.block_entry_offsets.len == 0) return self.entry_offsets[index];
        const block_index = findBlockIndexForEntryInMetas(self.blocks, index) orelse unreachable;
        return self.blocks[block_index].relative_offset + self.block_entry_offsets[index];
    }

    pub fn entryAt(self: *const BorrowedDecoded, index: usize) !Entry {
        return try parseEntryAt(self.raw, self.entry_data_start + self.entryStart(index));
    }

    pub fn lowerBound(self: *const BorrowedDecoded, namespace_name: ?[]const u8, key: []const u8) !usize {
        if (self.blocks.len > 0) {
            const block_index = findBlockLowerBoundInMetas(self.blocks, namespace_name, key) orelse return self.entryCount();
            const block = self.blocks[block_index];
            const raw_block = self.raw[self.entry_data_start + block.relative_offset .. self.entry_data_start + block.relative_offset + block.len];
            var lo: usize = block.first_entry_index;
            var hi: usize = block.first_entry_index + block.entry_count;
            while (lo < hi) {
                const mid = lo + (hi - lo) / 2;
                const relative_offset: usize = @intCast(self.entryStart(mid) - block.relative_offset);
                const entry = try parseEntryAt(raw_block, relative_offset);
                if (compareEntryTo(entry, namespace_name, key) == .lt) lo = mid + 1 else hi = mid;
            }
            return lo;
        }
        var lo: usize = 0;
        var hi: usize = self.entryCount();
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            const entry = try self.entryAt(mid);
            const ord = compareEntryTo(entry, namespace_name, key);
            if (ord == .lt) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    pub fn findIndex(self: *const BorrowedDecoded, namespace_name: ?[]const u8, key: []const u8) !?usize {
        const idx = try self.lowerBound(namespace_name, key);
        if (idx >= self.entryCount()) return null;
        const entry = try self.entryAt(idx);
        if (compareEntryTo(entry, namespace_name, key) != .eq) return null;
        return idx;
    }

    pub fn lowerBoundPosition(self: *const BorrowedDecoded, namespace_name: ?[]const u8, key: []const u8, inclusive: bool) !?PositionedEntry {
        var idx = try self.lowerBound(namespace_name, key);
        while (idx < self.entryCount()) : (idx += 1) {
            const entry = try self.entryAt(idx);
            if (compareNamespace(entry.namespace_name, namespace_name) != .eq) return null;
            if (!inclusive and std.mem.eql(u8, entry.key, key)) continue;
            return .{
                .index = idx,
                .entry = entry,
            };
        }
        return null;
    }

    pub fn nextPositionInNamespace(self: *const BorrowedDecoded, namespace_name: ?[]const u8, current: usize) !?PositionedEntry {
        var idx = current + 1;
        while (idx < self.entryCount()) : (idx += 1) {
            const entry = try self.entryAt(idx);
            const order = compareNamespace(entry.namespace_name, namespace_name);
            if (order == .eq) {
                return .{
                    .index = idx,
                    .entry = entry,
                };
            }
            if (order == .gt) return null;
        }
        return null;
    }

    pub fn prevPositionInNamespace(self: *const BorrowedDecoded, namespace_name: ?[]const u8, current: usize) !?PositionedEntry {
        if (current == 0) return null;
        var idx = current - 1;
        while (true) {
            const entry = try self.entryAt(idx);
            const order = compareNamespace(entry.namespace_name, namespace_name);
            if (order == .eq) {
                return .{
                    .index = idx,
                    .entry = entry,
                };
            }
            if (order == .lt or idx == 0) return null;
            idx -= 1;
        }
    }

    pub fn seekAtOrBefore(self: *const BorrowedDecoded, namespace_name: ?[]const u8, key: []const u8, inclusive: bool) !?PositionedEntry {
        const idx = try self.lowerBound(namespace_name, key);
        if (idx < self.entryCount()) {
            const entry = try self.entryAt(idx);
            if (compareNamespace(entry.namespace_name, namespace_name) == .eq and compareEntryTo(entry, namespace_name, key) == .eq and inclusive) {
                return .{
                    .index = idx,
                    .entry = entry,
                };
            }
        }

        var probe = if (idx == 0) return null else idx - 1;
        while (true) {
            const entry = try self.entryAt(probe);
            const order = compareNamespace(entry.namespace_name, namespace_name);
            if (order == .eq) {
                return .{
                    .index = probe,
                    .entry = entry,
                };
            }
            if (order == .lt or probe == 0) return null;
            probe -= 1;
        }
    }

    pub fn lastPositionInNamespace(self: *const BorrowedDecoded, namespace_name: ?[]const u8) !?PositionedEntry {
        if (self.entryCount() == 0) return null;
        var idx = self.entryCount();
        while (idx > 0) {
            idx -= 1;
            const entry = try self.entryAt(idx);
            const order = compareNamespace(entry.namespace_name, namespace_name);
            if (order == .eq) {
                return .{
                    .index = idx,
                    .entry = entry,
                };
            }
            if (order == .lt) return null;
        }
        return null;
    }

    pub fn seekAtOrAfterFromIndex(self: *const BorrowedDecoded, namespace_name: ?[]const u8, key: []const u8, start: usize) !?PositionedEntry {
        var idx = @max(start, try self.lowerBound(namespace_name, key));
        while (idx < self.entryCount()) : (idx += 1) {
            const entry = try self.entryAt(idx);
            const order = compareEntryTo(entry, namespace_name, key);
            if (order == .lt) continue;
            if (compareNamespace(entry.namespace_name, namespace_name) != .eq) return null;
            return .{
                .index = idx,
                .entry = entry,
            };
        }
        return null;
    }
};

pub const OwnedPositionedEntry = struct {
    index: usize,
    entry: Entry,
    bytes: []u8,
};

pub fn borrowDecoded(raw: []u8, index: *const TableIndex) BorrowedDecoded {
    std.debug.assert(!indexHasCompressedBlocks(index));
    return .{
        .raw = raw,
        .owns_raw = false,
        .entry_offsets = index.entry_offsets,
        .owns_entry_offsets = false,
        .block_entry_offsets = index.block_entry_offsets,
        .owns_block_entry_offsets = false,
        .entry_data_start = index.entry_data_start,
        .filter = index.borrowFilter(),
        .blocks = index.blocks,
        .owns_blocks = false,
    };
}

const EncodedBlockMeta = struct {
    relative_offset: u32,
    len: u32,
    physical_relative_offset: u32,
    physical_len: u32,
    compression: BlockCompression,
    checksum: u32,
    first_entry_index: u32,
    entry_count: u32,
    smallest_namespace_name: ?[]const u8,
    smallest_key: []const u8,
    largest_namespace_name: ?[]const u8,
    largest_key: []const u8,
    filter: bloom.OwnedFilter,
    prefix_filter: ?bloom.OwnedFilter,
    hash_slots: []u32,
};

const OwnedEncodedBlockMeta = struct {
    relative_offset: u32,
    len: u32,
    physical_relative_offset: u32,
    physical_len: u32,
    compression: BlockCompression,
    checksum: u32,
    first_entry_index: u32,
    entry_count: u32,
    smallest_namespace_name: ?[]u8 = null,
    smallest_key: []u8,
    largest_namespace_name: ?[]u8 = null,
    largest_key: []u8,
    filter: bloom.OwnedFilter,
    prefix_filter: ?bloom.OwnedFilter = null,
    hash_slots: []u32,

    pub fn deinit(self: *OwnedEncodedBlockMeta, allocator: std.mem.Allocator) void {
        if (self.smallest_namespace_name) |name| allocator.free(name);
        allocator.free(self.smallest_key);
        if (self.largest_namespace_name) |name| allocator.free(name);
        allocator.free(self.largest_key);
        self.filter.deinit(allocator);
        if (self.prefix_filter) |*filter| filter.deinit(allocator);
        allocator.free(self.hash_slots);
        self.* = undefined;
    }
};

pub const TableSink = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        len: *const fn (*anyopaque) usize,
        append_slice: *const fn (*anyopaque, []const u8) anyerror!void,
        append_byte: *const fn (*anyopaque, u8) anyerror!void,
        write_at: *const fn (*anyopaque, usize, []const u8) anyerror!void,
    };

    pub fn len(self: *TableSink) usize {
        return self.vtable.len(self.ptr);
    }

    pub fn appendSlice(self: *TableSink, bytes: []const u8) !void {
        try self.vtable.append_slice(self.ptr, bytes);
    }

    pub fn appendByte(self: *TableSink, byte: u8) !void {
        try self.vtable.append_byte(self.ptr, byte);
    }

    pub fn writeAt(self: *TableSink, offset: usize, bytes: []const u8) !void {
        try self.vtable.write_at(self.ptr, offset, bytes);
    }
};

pub const MemoryTableSink = struct {
    allocator: std.mem.Allocator,
    out: std.ArrayListUnmanaged(u8) = .empty,

    pub fn init(allocator: std.mem.Allocator) MemoryTableSink {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *MemoryTableSink) void {
        self.out.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn sink(self: *MemoryTableSink) TableSink {
        return .{
            .ptr = self,
            .vtable = &memory_table_sink_vtable,
        };
    }

    pub fn finishOwned(self: *MemoryTableSink) ![]u8 {
        return try self.out.toOwnedSlice(self.allocator);
    }

    fn len(ptr: *anyopaque) usize {
        const self: *MemoryTableSink = @ptrCast(@alignCast(ptr));
        return self.out.items.len;
    }

    fn appendSlice(ptr: *anyopaque, bytes: []const u8) !void {
        const self: *MemoryTableSink = @ptrCast(@alignCast(ptr));
        try byte_copy.appendSlicePossiblyAliased(&self.out, self.allocator, bytes);
    }

    fn appendByte(ptr: *anyopaque, byte: u8) !void {
        const self: *MemoryTableSink = @ptrCast(@alignCast(ptr));
        try self.out.append(self.allocator, byte);
    }

    fn writeAt(ptr: *anyopaque, offset: usize, bytes: []const u8) !void {
        const self: *MemoryTableSink = @ptrCast(@alignCast(ptr));
        if (offset > self.out.items.len or bytes.len > self.out.items.len - offset) return error.InvalidTableFile;
        byte_copy.copyPossiblyAliased(self.out.items[offset..][0..bytes.len], bytes);
    }
};

const memory_table_sink_vtable = TableSink.VTable{
    .len = MemoryTableSink.len,
    .append_slice = MemoryTableSink.appendSlice,
    .append_byte = MemoryTableSink.appendByte,
    .write_at = MemoryTableSink.writeAt,
};

const ChecksummedTableSink = struct {
    parent: *TableSink,
    crc: Crc32 = .init(),

    fn sink(self: *ChecksummedTableSink) TableSink {
        return .{
            .ptr = self,
            .vtable = &checksummed_table_sink_vtable,
        };
    }

    fn checksum(self: *const ChecksummedTableSink) u32 {
        return self.crc.final();
    }

    fn len(ptr: *anyopaque) usize {
        const self: *ChecksummedTableSink = @ptrCast(@alignCast(ptr));
        return self.parent.len();
    }

    fn appendSlice(ptr: *anyopaque, bytes: []const u8) !void {
        const self: *ChecksummedTableSink = @ptrCast(@alignCast(ptr));
        try self.parent.appendSlice(bytes);
        self.crc.update(bytes);
    }

    fn appendByte(ptr: *anyopaque, byte: u8) !void {
        const self: *ChecksummedTableSink = @ptrCast(@alignCast(ptr));
        try self.parent.appendByte(byte);
        self.crc.update(&.{byte});
    }

    fn writeAt(_: *anyopaque, _: usize, _: []const u8) !void {
        return error.InvalidTableFile;
    }
};

const checksummed_table_sink_vtable = TableSink.VTable{
    .len = ChecksummedTableSink.len,
    .append_slice = ChecksummedTableSink.appendSlice,
    .append_byte = ChecksummedTableSink.appendByte,
    .write_at = ChecksummedTableSink.writeAt,
};

test "memory table sink supports overlapping writes and appends" {
    var sink_impl = MemoryTableSink.init(std.testing.allocator);
    defer sink_impl.deinit();
    var sink = sink_impl.sink();

    try sink.appendSlice("abcdef");
    try sink.writeAt(2, sink_impl.out.items[0..4]);
    try std.testing.expectEqualStrings("ababcd", sink_impl.out.items);

    try sink.appendSlice(sink_impl.out.items[1..5]);
    try std.testing.expectEqualStrings("ababcdbabc", sink_impl.out.items);
}

pub fn encodeAlloc(allocator: std.mem.Allocator, entries: []const Entry) ![]u8 {
    var filter = try buildFilterAlloc(allocator, entries, default_filter_config);
    defer filter.deinit(allocator);
    return try encodeWithFilterAlloc(allocator, entries, filter);
}

pub fn encodeWithFilterAlloc(
    allocator: std.mem.Allocator,
    entries: []const Entry,
    filter: bloom.OwnedFilter,
) ![]u8 {
    return try encodeWithFilterAllocOptions(allocator, entries, filter, .{});
}

pub fn encodeWithFilterAllocOptions(
    allocator: std.mem.Allocator,
    entries: []const Entry,
    filter: bloom.OwnedFilter,
    options: EncodeOptions,
) ![]u8 {
    var sink_impl = MemoryTableSink.init(allocator);
    errdefer sink_impl.deinit();
    var sink = sink_impl.sink();
    _ = try encodeWithFilterToSinkOptions(allocator, &sink, entries, filter, options);
    return try sink_impl.finishOwned();
}

pub fn encodeWithFilterToSink(
    allocator: std.mem.Allocator,
    sink: *TableSink,
    entries: []const Entry,
    filter: bloom.OwnedFilter,
) !usize {
    return try encodeWithFilterToSinkOptions(allocator, sink, entries, filter, .{});
}

pub fn encodeWithFilterToSinkOptions(
    allocator: std.mem.Allocator,
    sink: *TableSink,
    entries: []const Entry,
    filter: bloom.OwnedFilter,
    options: EncodeOptions,
) !usize {
    if (entries.len > max_entry_count) return error.TableFileTooLarge;

    var encoded_filter_bytes = std.ArrayListUnmanaged(u8).empty;
    defer encoded_filter_bytes.deinit(allocator);
    const encoded_filter = try filter.encodeInto(allocator, &encoded_filter_bytes);
    var prefix_filter = try buildPrefixFilterAlloc(allocator, entries, options.prefix_extractor, default_filter_config);
    defer prefix_filter.deinit(allocator);
    const encoded_prefix_filter = try prefix_filter.encodeAlloc(allocator);
    defer allocator.free(encoded_prefix_filter);

    var entry_offsets = std.ArrayListUnmanaged(u16).empty;
    defer entry_offsets.deinit(allocator);
    try entry_offsets.ensureTotalCapacity(allocator, entries.len);

    var blocks = std.ArrayListUnmanaged(EncodedBlockMeta).empty;
    defer {
        for (blocks.items) |*block| {
            block.filter.deinit(allocator);
            if (block.prefix_filter) |*prefix_filter_ptr| prefix_filter_ptr.deinit(allocator);
            allocator.free(block.hash_slots);
        }
        blocks.deinit(allocator);
    }

    try sink.appendSlice(magic);
    try sinkAppendU32(sink, version);
    try sinkAppendU32(sink, try checkedU32(entries.len));
    const entry_data_len_offset = sink.len();
    try sinkAppendU32(sink, 0);
    const entry_data_start = sink.len();

    var block_bytes = std.ArrayListUnmanaged(u8).empty;
    defer block_bytes.deinit(allocator);
    var compression_bytes = std.ArrayListUnmanaged(u8).empty;
    defer compression_bytes.deinit(allocator);

    var logical_entry_data_len: usize = 0;
    var block_start: ?u32 = null;
    var block_first_entry_index: usize = 0;
    var block_entry_count: usize = 0;
    var block_smallest_namespace_name: ?[]const u8 = null;
    var block_smallest_key: []const u8 = &.{};
    var block_largest_namespace_name: ?[]const u8 = null;
    var block_largest_key: []const u8 = &.{};

    for (entries, 0..) |entry, entry_index| {
        const entry_start_usize = logical_entry_data_len;
        const entry_start = try checkedU32(entry_start_usize);
        const entry_len = try tableEntryEncodedLen(entry);
        if (entry_start_usize > max_entry_data_len or entry_len > max_entry_data_len - entry_start_usize) {
            return error.TableFileTooLarge;
        }
        if (block_start == null) {
            block_start = entry_start;
            block_first_entry_index = entry_index;
            block_smallest_namespace_name = entry.namespace_name;
            block_smallest_key = entry.key;
        } else if (block_entry_count > 0 and block_bytes.items.len + entry_len > default_block_size) {
            try flushEncodedBlock(
                allocator,
                sink,
                &blocks,
                entries,
                &block_bytes,
                entry_data_start,
                block_start.?,
                block_first_entry_index,
                block_entry_count,
                block_smallest_namespace_name,
                block_smallest_key,
                block_largest_namespace_name,
                block_largest_key,
                options.block_compression,
                options.prefix_extractor,
                &compression_bytes,
            );
            block_start = entry_start;
            block_first_entry_index = entry_index;
            block_smallest_namespace_name = entry.namespace_name;
            block_smallest_key = entry.key;
            block_entry_count = 0;
        }

        // Blocks are capped at 32 KiB. An oversized entry occupies a block by
        // itself and therefore still has local offset zero.
        try entry_offsets.append(allocator, try checkedU16(block_bytes.items.len));
        try appendEntryBytesToList(allocator, &block_bytes, entry);
        logical_entry_data_len += entry_len;

        block_entry_count += 1;
        block_largest_namespace_name = entry.namespace_name;
        block_largest_key = entry.key;
    }

    const entry_data_len_u32 = try checkedU32(logical_entry_data_len);

    if (block_start) |start| {
        try flushEncodedBlock(
            allocator,
            sink,
            &blocks,
            entries,
            &block_bytes,
            entry_data_start,
            start,
            block_first_entry_index,
            block_entry_count,
            block_smallest_namespace_name,
            block_smallest_key,
            block_largest_namespace_name,
            block_largest_key,
            options.block_compression,
            options.prefix_extractor,
            &compression_bytes,
        );
    }

    const physical_entry_data_len = sink.len() - entry_data_start;
    const physical_entry_data_len_u32 = try checkedU32(physical_entry_data_len);
    try sink.writeAt(entry_data_len_offset, &@as([4]u8, @bitCast(@as(u32, physical_entry_data_len_u32))));

    const metadata_offset = sink.len();
    var metadata_sink_impl = ChecksummedTableSink{ .parent = sink };
    var metadata_sink = metadata_sink_impl.sink();
    try metadata_sink.appendSlice(packed_offsets_magic);
    for (entry_offsets.items) |offset| try sinkAppendU16(&metadata_sink, offset);
    try sinkAppendU32(&metadata_sink, try checkedU32(encoded_filter.len));
    try metadata_sink.appendSlice(encoded_filter);
    try sinkAppendU32(&metadata_sink, try checkedU32(blocks.items.len));
    for (blocks.items) |block| {
        try sinkAppendU32(&metadata_sink, block.relative_offset);
        try sinkAppendU32(&metadata_sink, block.len);
        try sinkAppendU32(&metadata_sink, block.first_entry_index);
        try sinkAppendU32(&metadata_sink, block.entry_count);
        try sinkAppendU32(&metadata_sink, if (block.largest_namespace_name) |name| try checkedU32(name.len) else std.math.maxInt(u32));
        try sinkAppendU32(&metadata_sink, try checkedU32(block.largest_key.len));
        if (block.largest_namespace_name) |name| try metadata_sink.appendSlice(name);
        try metadata_sink.appendSlice(block.largest_key);
    }
    try sinkAppendU32(&metadata_sink, try checkedU32(blocks.items.len));
    for (blocks.items) |block| {
        const encoded_block_filter = try block.filter.encodeInto(allocator, &encoded_filter_bytes);
        try sinkAppendU32(&metadata_sink, try checkedU32(encoded_block_filter.len));
        try metadata_sink.appendSlice(encoded_block_filter);
    }
    try sinkAppendU32(&metadata_sink, try checkedU32(blocks.items.len));
    for (blocks.items) |block| {
        try sinkAppendU32(&metadata_sink, try checkedU32(block.hash_slots.len));
        for (block.hash_slots) |slot| try sinkAppendU32(&metadata_sink, slot);
    }
    try sinkAppendU32(&metadata_sink, try checkedU32(blocks.items.len));
    for (blocks.items) |block| {
        try sinkAppendU32(&metadata_sink, block.physical_relative_offset);
        try sinkAppendU32(&metadata_sink, block.physical_len);
        try sinkAppendU32(&metadata_sink, @backingInt(block.compression));
        try sinkAppendU32(&metadata_sink, block.checksum);
    }
    try sinkAppendU32(&metadata_sink, try checkedU32(blocks.items.len));
    for (blocks.items) |block| {
        try sinkAppendU32(&metadata_sink, if (block.smallest_namespace_name) |name| try checkedU32(name.len) else std.math.maxInt(u32));
        try sinkAppendU32(&metadata_sink, try checkedU32(block.smallest_key.len));
        if (block.smallest_namespace_name) |name| try metadata_sink.appendSlice(name);
        try metadata_sink.appendSlice(block.smallest_key);
    }
    try sinkAppendU32(&metadata_sink, @backingInt(options.prefix_extractor));
    try sinkAppendU32(&metadata_sink, try checkedU32(encoded_prefix_filter.len));
    try metadata_sink.appendSlice(encoded_prefix_filter);
    try sinkAppendU32(&metadata_sink, try checkedU32(blocks.items.len));
    for (blocks.items) |block| {
        const encoded_block_prefix_filter = if (block.prefix_filter) |block_prefix_filter|
            try block_prefix_filter.encodeInto(allocator, &encoded_filter_bytes)
        else
            "";
        try sinkAppendU32(&metadata_sink, try checkedU32(encoded_block_prefix_filter.len));
        try metadata_sink.appendSlice(encoded_block_prefix_filter);
    }
    const metadata_len = sink.len() - metadata_offset;
    try appendFooter(
        sink,
        metadata_offset,
        metadata_len,
        entries.len,
        entry_data_len_u32,
        metadata_sink_impl.checksum(),
    );

    if (options.compression_stats) |stats| {
        stats.* = summarizeCompressionStats(logical_entry_data_len, physical_entry_data_len, blocks.items);
    }

    return sink.len();
}

pub const StreamingEncoderOptions = struct {
    block_compression: CompressionPolicy = .snappy_adaptive,
    bloom_config: bloom.Config = default_filter_config,
    prefix_extractor: PrefixExtractor = default_prefix_extractor,
    compression_stats: ?*CompressionStats = null,
};

pub const StreamingEncoderResult = struct {
    size_bytes: usize,
    entry_count: usize,
    filter: bloom.OwnedFilter,
    compression_stats: CompressionStats,
};

pub const StreamingEncoder = struct {
    allocator: std.mem.Allocator,
    sink: *TableSink,
    compression_policy: CompressionPolicy,
    bloom_config: bloom.Config,
    prefix_extractor: PrefixExtractor,
    compression_stats_out: ?*CompressionStats,
    filter_builder: bloom.Builder,
    filter_builder_active: bool = true,
    /// Prefixes are leading key ranges, so equal prefixes are consecutive in
    /// the sorted table stream. Retain one hash pair per distinct prefix rather
    /// than sizing a second Bloom filter for every entry in the run.
    prefix_hashes: std.ArrayListUnmanaged([2]u64) = .empty,
    entry_offsets: std.ArrayListUnmanaged(u16) = .empty,
    blocks: std.ArrayListUnmanaged(OwnedEncodedBlockMeta) = .empty,
    block_bytes: std.ArrayListUnmanaged(u8) = .empty,
    compression_bytes: std.ArrayListUnmanaged(u8) = .empty,
    /// Dense embeddings and encrypted/compressed documents often contain long
    /// runs of blocks for which Snappy cannot meet the adaptive savings floor.
    /// Probe periodically after a rejection streak instead of recompressing
    /// every block only to discard the result. Prefix compression remains
    /// active for every block and a successful probe immediately re-enables
    /// Snappy.
    snappy_rejection_streak: usize = 0,
    snappy_reprobe_countdown: usize = 0,
    snappy_attempted_blocks: usize = 0,
    encoded_filter_bytes: std.ArrayListUnmanaged(u8) = .empty,
    block_hashes: std.ArrayListUnmanaged([2]u64) = .empty,
    block_prefix_hashes: std.ArrayListUnmanaged([2]u64) = .empty,
    /// Exact encoded metadata contribution of completed blocks. Maintaining it
    /// at flush time keeps physical-size admission O(1) per appended entry.
    completed_blocks_metadata_bytes: usize = 0,
    /// Heap owned by completed block metadata. Keep this incrementally so
    /// resource accounting remains O(1) as a streaming table grows.
    blocks_owned_heap_bytes: u64 = 0,
    entry_count_offset: usize,
    entry_data_len_offset: usize,
    entry_data_start: usize,
    entry_count: usize = 0,
    logical_entry_data_len: usize = 0,
    block_start: ?u32 = null,
    block_first_entry_index: usize = 0,
    block_entry_count: usize = 0,
    block_smallest_namespace_name: ?[]u8 = null,
    block_smallest_key: []u8 = &.{},
    block_largest_namespace_name: ?[]u8 = null,
    block_largest_key: []u8 = &.{},
    block_largest_namespace_buffer: std.ArrayListUnmanaged(u8) = .empty,
    block_largest_key_buffer: std.ArrayListUnmanaged(u8) = .empty,
    finished: bool = false,
    block_active: bool = false,
    entry_active: bool = false,
    entry_namespace_spare: std.ArrayListUnmanaged(u8) = .empty,
    entry_key_spare: std.ArrayListUnmanaged(u8) = .empty,
    physical_output_len: usize,
    legacy_sink_allowed: bool = true,
    metadata_active: bool = false,
    retiring: bool = false,
    retired: bool = false,

    /// Pure CPU construction with a retained header command. Caller prepays
    /// memoryBound(), retains allocator/input ownership and serializes methods.
    /// The header remains private until acknowledged after owning IO completes.
    pub const Initialization = struct {
        const Phase = enum { allocate, zero, header, complete, transferred, retired };
        pub const Command = struct { offset: usize, bytes: []const u8 };
        allocator: std.mem.Allocator,
        expected_entries: usize,
        options: StreamingEncoderOptions,
        output_offset: usize,
        header: [header_len]u8,
        builder: ?bloom.Builder = null,
        phase: Phase = .allocate,
        offset: usize = 0,
        retiring: bool = false,

        pub fn memoryBound(expected_entries: usize, options: StreamingEncoderOptions) !usize {
            if (expected_entries > max_entry_count) return error.TableFileTooLarge;
            // Validate both the encoded bit count and the hash-count arithmetic
            // before provider allocation, including empty/hostile estimates.
            const hash_estimate = std.math.mul(usize, options.bloom_config.bits_per_key, 69) catch return error.TableFileTooLarge;
            _ = std.math.add(usize, hash_estimate, 50) catch return error.TableFileTooLarge;
            const bytes = (try bloomEncodedLenForKeyCount(expected_entries, options.bloom_config)) - bloom_encoded_header_len;
            return checkedAddUsize(@sizeOf(Initialization) + @sizeOf(StreamingEncoder), bytes);
        }

        pub fn init(a: std.mem.Allocator, expected_entries: usize, options: StreamingEncoderOptions, output_offset: usize) !Initialization {
            _ = try memoryBound(expected_entries, options);
            _ = try checkedAddUsize(output_offset, header_len);
            var result: Initialization = .{ .allocator = a, .expected_entries = expected_entries, .options = options, .output_offset = output_offset, .header = undefined };
            @memcpy(result.header[0..8], magic);
            std.mem.writeInt(u32, result.header[8..12], version, .little);
            @memset(result.header[12..], 0);
            return result;
        }

        pub fn step(self: *Initialization, original: ?read_admission.Port) !bool {
            if (self.retiring or self.phase == .retired or self.phase == .transferred) return error.InvalidTableFile;
            if (original) |control| try control.checkpoint();
            switch (self.phase) {
                .allocate => {
                    self.builder = try bloom.Builder.initUninitialized(self.allocator, self.expected_entries, self.options.bloom_config);
                    self.phase = .zero;
                },
                .zero => {
                    const bytes = self.builder.?.bytes;
                    const n = @min(bytes.len - self.offset, 16 * 1024);
                    @memset(bytes[self.offset..][0..n], 0);
                    self.offset += n;
                    if (self.offset == bytes.len) {
                        self.offset = 0;
                        self.phase = .header;
                    }
                },
                .header, .complete => return true,
                .transferred, .retired => unreachable,
            }
            return self.phase == .header;
        }

        pub fn next(self: *Initialization, original: ?read_admission.Port) !?Command {
            while (!(try self.step(original))) {}
            return if (self.phase == .complete) null else .{ .offset = self.output_offset, .bytes = &self.header };
        }

        pub fn acknowledge(self: *Initialization) void {
            std.debug.assert(!self.retiring and self.phase == .header);
            self.phase = .complete;
        }

        pub fn writeNextWithAdmission(self: *Initialization, writer: anytype, original: read_admission.Port) !bool {
            const command = (try self.next(original)) orelse return false;
            try writer.writeWithAdmission(command.offset, command.bytes, original);
            self.acknowledge();
            return true;
        }

        /// Moves the still-funded filter and borrows a stable caller-owned sink
        /// solely as a legacy boundary. Owned construction disables synchronous
        /// append/finish APIs so they cannot silently fall back to sink IO.
        pub fn takeEncoder(self: *Initialization, sink: *TableSink) !StreamingEncoder {
            if (self.retiring or self.phase != .complete or self.builder == null) return error.InvalidTableFile;
            const builder = self.builder.?;
            self.builder = null;
            self.phase = .transferred;
            return .{
                .allocator = self.allocator,
                .sink = sink,
                .compression_policy = self.options.block_compression,
                .bloom_config = self.options.bloom_config,
                .prefix_extractor = self.options.prefix_extractor,
                .compression_stats_out = self.options.compression_stats,
                .filter_builder = builder,
                .entry_count_offset = self.output_offset + 12,
                .entry_data_len_offset = self.output_offset + 16,
                .entry_data_start = self.output_offset + header_len,
                .physical_output_len = self.output_offset + header_len,
                .legacy_sink_allowed = false,
            };
        }

        /// Settle accepted header IO before retiring this borrowed command.
        pub fn retireWithAdmission(self: *Initialization, original: ?read_admission.Port) !void {
            if (self.phase == .retired) return;
            if (!self.retiring) {
                self.retiring = true;
                self.offset = 0;
            }
            try retirePrivate(u8, self.allocator, if (self.builder) |b| b.bytes else &.{}, &self.offset, original);
            self.builder = null;
            self.phase = .retired;
        }
        pub fn deinit(self: *Initialization) void {
            self.retireWithAdmission(null) catch unreachable;
        }
    };

    pub fn init(
        allocator: std.mem.Allocator,
        sink: *TableSink,
        expected_entries: usize,
        options: StreamingEncoderOptions,
    ) !StreamingEncoder {
        var construction = try Initialization.init(allocator, expected_entries, options, sink.len());
        defer construction.deinit();
        const command = (try construction.next(null)).?;
        try sink.appendSlice(command.bytes);
        construction.acknowledge();
        var encoder = try construction.takeEncoder(sink);
        encoder.legacy_sink_allowed = true;
        return encoder;
    }

    pub fn prepareRetirement(self: *StreamingEncoder) !Retirement {
        if (self.block_active or self.entry_active or self.metadata_active or self.retiring or self.retired) return error.InvalidTableFile;
        self.finished = true;
        self.retiring = true;
        return .{ .encoder = self };
    }

    /// Borrows the frozen encoder, not a control. One call poisons at most
    /// 16 KiB or skips one absent allocation. Credit is released only with the
    /// final chunk's raw allocator callback; the descriptor array retires last.
    pub const Retirement = struct {
        encoder: *StreamingEncoder,
        slot: usize = 0,
        block_index: usize = 0,
        block_field: usize = 0,
        offset: usize = 0,
        done: bool = false,

        fn list(self: *Retirement, comptime T: type, value: *std.ArrayListUnmanaged(T)) bool {
            if (!retirePrivateChunk(T, self.encoder.allocator, value.allocatedSlice(), &self.offset)) return false;
            value.* = .empty;
            return true;
        }

        fn blockField(self: *Retirement) bool {
            const b = &self.encoder.blocks.items[self.block_index];
            const a = self.encoder.allocator;
            const bytes: usize = switch (self.block_field) {
                0 => if (b.smallest_namespace_name) |name| name.len else 0,
                1 => b.smallest_key.len,
                2 => if (b.largest_namespace_name) |name| name.len else 0,
                3 => b.largest_key.len,
                4 => b.filter.bytes.len,
                5 => if (b.prefix_filter) |f| f.bytes.len else 0,
                6 => b.hash_slots.len * @sizeOf(u32),
                else => unreachable,
            };
            const freed = switch (self.block_field) {
                0 => retirePrivateChunk(u8, a, b.smallest_namespace_name orelse &.{}, &self.offset),
                1 => retirePrivateChunk(u8, a, b.smallest_key, &self.offset),
                2 => retirePrivateChunk(u8, a, b.largest_namespace_name orelse &.{}, &self.offset),
                3 => retirePrivateChunk(u8, a, b.largest_key, &self.offset),
                4 => retirePrivateChunk(u8, a, b.filter.bytes, &self.offset),
                5 => retirePrivateChunk(u8, a, if (b.prefix_filter) |f| f.bytes else &.{}, &self.offset),
                6 => retirePrivateChunk(u32, a, b.hash_slots, &self.offset),
                else => unreachable,
            };
            if (!freed) return false;
            switch (self.block_field) {
                0 => b.smallest_namespace_name = null,
                1 => b.smallest_key = &.{},
                2 => b.largest_namespace_name = null,
                3 => b.largest_key = &.{},
                4 => b.filter.bytes = &.{},
                5 => b.prefix_filter = null,
                6 => b.hash_slots = &.{},
                else => unreachable,
            }
            self.encoder.blocks_owned_heap_bytes -|= bytes;
            self.block_field += 1;
            if (self.block_field == 7) {
                self.block_field = 0;
                self.block_index += 1;
            }
            return true;
        }

        pub fn step(self: *Retirement, original: ?read_admission.Port) !bool {
            if (self.done) return true;
            if (original) |control| try control.checkpoint();
            const e = self.encoder;
            const complete = switch (self.slot) {
                0 => blk: {
                    if (e.filter_builder_active) {
                        if (!retirePrivateChunk(u8, e.allocator, e.filter_builder.bytes, &self.offset)) break :blk false;
                        e.filter_builder.bytes = &.{};
                        e.filter_builder_active = false;
                    }
                    break :blk true;
                },
                1 => self.list([2]u64, &e.prefix_hashes),
                2 => self.list(u16, &e.entry_offsets),
                3 => blk: {
                    if (self.block_index < e.blocks.items.len) {
                        _ = self.blockField();
                        break :blk false;
                    }
                    break :blk true;
                },
                4 => self.list(OwnedEncodedBlockMeta, &e.blocks),
                5 => self.list(u8, &e.block_bytes),
                6 => self.list(u8, &e.compression_bytes),
                7 => self.list(u8, &e.encoded_filter_bytes),
                8 => self.list([2]u64, &e.block_hashes),
                9 => self.list([2]u64, &e.block_prefix_hashes),
                10 => blk: {
                    if (!retirePrivateChunk(u8, e.allocator, e.block_smallest_namespace_name orelse &.{}, &self.offset)) break :blk false;
                    e.block_smallest_namespace_name = null;
                    break :blk true;
                },
                11 => blk: {
                    if (!retirePrivateChunk(u8, e.allocator, e.block_smallest_key, &self.offset)) break :blk false;
                    e.block_smallest_key = &.{};
                    break :blk true;
                },
                12 => blk: {
                    const done = self.list(u8, &e.block_largest_namespace_buffer);
                    if (done) e.block_largest_namespace_name = null;
                    break :blk done;
                },
                13 => blk: {
                    const done = self.list(u8, &e.block_largest_key_buffer);
                    if (done) e.block_largest_key = &.{};
                    break :blk done;
                },
                14 => self.list(u8, &e.entry_namespace_spare),
                15 => self.list(u8, &e.entry_key_spare),
                else => unreachable,
            };
            if (complete) self.slot += 1;
            if (self.slot == 16) {
                e.block_largest_namespace_name = null;
                e.block_largest_key = &.{};
                e.retiring = false;
                e.retired = true;
                self.done = true;
            }
            return self.done;
        }
        pub fn deinit(self: *Retirement) void {
            while (!(self.step(null) catch unreachable)) {}
        }
    };

    pub fn deinit(self: *StreamingEncoder) void {
        if (!self.retired) {
            var retirement = self.prepareRetirement() catch unreachable;
            retirement.deinit();
        }
        self.* = undefined;
    }

    pub fn workingSetBytes(self: *const StreamingEncoder) u64 {
        var bytes: u64 = 0;
        if (self.filter_builder_active) bytes +|= self.filter_builder.bytes.len;
        bytes +|= capacityBytes([2]u64, self.prefix_hashes.capacity);
        bytes +|= capacityBytes(u16, self.entry_offsets.capacity);
        bytes +|= capacityBytes(OwnedEncodedBlockMeta, self.blocks.capacity);
        bytes +|= capacityBytes(u8, self.block_bytes.capacity);
        bytes +|= capacityBytes(u8, self.compression_bytes.capacity);
        bytes +|= capacityBytes(u8, self.encoded_filter_bytes.capacity);
        bytes +|= capacityBytes([2]u64, self.block_hashes.capacity);
        bytes +|= capacityBytes([2]u64, self.block_prefix_hashes.capacity);
        bytes +|= self.blocks_owned_heap_bytes;
        if (self.block_smallest_namespace_name) |name| bytes +|= name.len;
        bytes +|= self.block_smallest_key.len;
        bytes +|= capacityBytes(u8, self.block_largest_namespace_buffer.capacity);
        bytes +|= capacityBytes(u8, self.block_largest_key_buffer.capacity);
        bytes +|= capacityBytes(u8, self.entry_namespace_spare.capacity);
        bytes +|= capacityBytes(u8, self.entry_key_spare.capacity);
        return bytes;
    }

    /// Returns a conservative upper bound for the complete encoded table if
    /// `entry` is appended and the encoder is finished immediately afterward.
    /// Block compression is deliberately ignored because every adaptive codec
    /// falls back to the raw payload when compression would grow the block.
    /// This lets streaming compaction split before an append without buffering
    /// or rolling back an arbitrarily large entry.
    pub fn encodedSizeUpperBoundAfterEntry(self: *const StreamingEncoder, entry: Entry) !usize {
        if (self.block_active or self.entry_active) return error.InvalidTableFile;
        if (self.finished or self.entry_count >= max_entry_count) return error.TableFileTooLarge;
        const entry_len = try tableEntryEncodedLen(entry);
        if (self.logical_entry_data_len > max_entry_data_len or
            entry_len > max_entry_data_len - self.logical_entry_data_len)
        {
            return error.TableFileTooLarge;
        }

        const may_prefix = self.prefix_extractor != .none and entry.key.len != 0;
        const split_pending_block = self.block_entry_count > 0 and
            self.block_bytes.items.len +| entry_len > default_block_size;
        const projected_entry_count = try checkedAddUsize(self.entry_count, 1);

        var total = self.physical_output_len;
        total = try checkedAddUsize(total, self.block_bytes.items.len);
        total = try checkedAddUsize(total, entry_len);

        // Metadata section headers and the packed local entry offsets.
        total = try checkedAddUsize(total, packed_offsets_magic.len);
        total = try checkedAddUsize(
            total,
            std.math.mul(usize, projected_entry_count, @sizeOf(u16)) catch return error.TableFileTooLarge,
        );
        total = try checkedAddUsize(total, @sizeOf(u32));
        total = try checkedAddUsize(total, bloomEncodedLenForBytes(self.filter_builder.bytes.len));
        // Six block-index sections, each prefixed by a u32 block count.
        total = try checkedAddUsize(total, 6 * @sizeOf(u32));
        total = try checkedAddUsize(total, self.completed_blocks_metadata_bytes);

        if (split_pending_block) {
            total = try checkedAddUsize(total, try encodedBlockMetadataLen(
                self.block_smallest_namespace_name,
                self.block_smallest_key,
                self.block_largest_namespace_name,
                self.block_largest_key,
                try bloomEncodedLenForKeyCount(self.block_hashes.items.len, block_filter_config),
                try bloomEncodedLenForKeyCount(self.block_prefix_hashes.items.len, block_filter_config),
            ));
            total = try checkedAddUsize(total, try encodedBlockMetadataLen(
                entry.namespace_name,
                entry.key,
                entry.namespace_name,
                entry.key,
                try bloomEncodedLenForKeyCount(1, block_filter_config),
                try bloomEncodedLenForKeyCount(if (may_prefix) 1 else 0, block_filter_config),
            ));
        } else {
            const smallest_namespace_name = if (self.block_entry_count == 0) entry.namespace_name else self.block_smallest_namespace_name;
            const smallest_key = if (self.block_entry_count == 0) entry.key else self.block_smallest_key;
            const block_prefix_count = self.block_prefix_hashes.items.len + @intFromBool(may_prefix);
            total = try checkedAddUsize(total, try encodedBlockMetadataLen(
                smallest_namespace_name,
                smallest_key,
                entry.namespace_name,
                entry.key,
                try bloomEncodedLenForKeyCount(self.block_hashes.items.len + 1, block_filter_config),
                try bloomEncodedLenForKeyCount(block_prefix_count, block_filter_config),
            ));
        }

        // Prefix extractor, run-level prefix Bloom filter, and footer.
        // Assume a distinct prefix when one is possible, without scanning
        // or hashing the candidate key merely to compute an upper bound.
        const prefix_count = self.prefix_hashes.items.len + @intFromBool(may_prefix);
        total = try checkedAddUsize(total, 2 * @sizeOf(u32));
        total = try checkedAddUsize(total, try bloomEncodedLenForKeyCount(prefix_count, self.bloom_config));
        total = try checkedAddUsize(total, footer_len);
        return total;
    }

    // Raw private buffers are initialized only by bounded stages before reads.
    fn allocatePrivate(comptime T: type, a: std.mem.Allocator, count: usize) ![]T {
        if (count == 0) return &.{};
        const size = std.math.mul(usize, count, @sizeOf(T)) catch return error.OutOfMemory;
        const raw = a.rawAlloc(size, .of(T), @returnAddress()) orelse return error.OutOfMemory;
        const ptr: [*]T = @ptrCast(@alignCast(raw));
        return ptr[0..count];
    }

    fn retirePrivateChunk(comptime T: type, allocator: std.mem.Allocator, memory: []T, offset: *usize) bool {
        const bytes = std.mem.sliceAsBytes(memory);
        if (bytes.len == 0) return true;
        const n = @min(bytes.len - offset.*, 16 * 1024);
        @memset(bytes[offset.*..][0..n], undefined);
        offset.* += n;
        if (offset.* != bytes.len) return false;
        allocator.rawFree(bytes, .of(T), @returnAddress());
        offset.* = 0;
        return true;
    }

    fn retirePrivate(comptime T: type, allocator: std.mem.Allocator, memory: []T, offset: *usize, original: ?read_admission.Port) !void {
        while (true) {
            if (original) |control| try control.checkpoint();
            if (retirePrivateChunk(T, allocator, memory, offset)) return;
        }
    }

    /// Length-only planning. Admitted callers publish/retire the pending block
    /// explicitly before preparing an entry that would cross its target.
    pub fn needsBlockBeforeEntry(self: *const StreamingEncoder, entry: Entry) !bool {
        if (self.finished or self.block_active or self.entry_active) return error.InvalidTableFile;
        if (self.entry_count >= max_entry_count) return error.TableFileTooLarge;
        const len = try tableEntryEncodedLen(entry);
        _ = try checkedU32(if (entry.namespace_name) |name| name.len else 0);
        _ = try checkedU32(entry.key.len);
        _ = try checkedU32(entry.value.len);
        if (self.logical_entry_data_len > max_entry_data_len or len > max_entry_data_len - self.logical_entry_data_len) return error.TableFileTooLarge;
        return self.block_entry_count > 0 and self.block_bytes.items.len + len > default_block_size;
    }

    /// Borrow immutable input through adoption. The caller retains encoder and
    /// input credits, prepays EntryPreparation.memoryBound(), keeps the encoder
    /// address stable and serializes methods until retirement finishes. This
    /// owner performs no sink IO or implicit block flush and is itself movable.
    pub fn prepareEntry(self: *StreamingEncoder, entry: Entry) !EntryPreparation {
        if (try self.needsBlockBeforeEntry(entry)) return error.InvalidTableFile;
        _ = try EntryPreparation.memoryBound(self, entry);
        _ = try checkedU16(self.block_bytes.items.len);
        const len = try tableEntryEncodedLen(entry);
        self.entry_active = true;
        var result: EntryPreparation = .{ .encoder = self, .input = entry, .entry_len = len };
        result.header[0] = @intFromBool(entry.tombstone);
        std.mem.writeInt(u32, result.header[1..5], @intCast(if (entry.namespace_name) |name| name.len else 0), .little);
        std.mem.writeInt(u32, result.header[5..9], @intCast(entry.key.len), .little);
        std.mem.writeInt(u32, result.header[9..13], @intCast(entry.value.len), .little);
        return result;
    }

    pub const EntryPreparation = struct {
        const Pair = [2]u64;
        const Phase = enum { prefix_scan, hash_init, hash_namespace, hash_key_header, hash_key, hash_prefix, hash_finish, growth, smallest_allocate, row_header, row_namespace, row_key, row_value, bounds_namespace, bounds_key, ready, adopted, retired };
        encoder: *StreamingEncoder,
        input: Entry,
        entry_len: usize,
        phase: Phase = .prefix_scan,
        offset: usize = 0,
        prefix_len: ?usize = null,
        hashers: [4]std.hash.Wyhash = undefined,
        hashes: Pair = undefined,
        prefix_hashes: ?Pair = null,
        add_run_prefix: bool = false,
        add_block_prefix: bool = false,
        header: [13]u8 = undefined,
        growth_index: usize = 0,
        data: Growth(u8) = .{},
        offsets: Growth(u16) = .{},
        block_hashes: Growth(Pair) = .{},
        run_prefixes: Growth(Pair) = .{},
        block_prefixes: Growth(Pair) = .{},
        scratch_namespace: Growth(u8) = .{},
        scratch_key: Growth(u8) = .{},
        smallest_namespace: ?[]u8 = null,
        smallest_key: ?[]u8 = null,
        retiring: bool = false,
        retire_slot: usize = 0,
        retire_offset: usize = 0,

        fn capacity(current: usize, needed: usize) !usize {
            return @max(needed, try checkedAddUsize(try checkedAddUsize(current, current / 2), 8));
        }

        fn Growth(comptime T: type) type {
            return struct {
                new: ?[]T = null,
                old: ?[]T = null,
                copied: usize = 0,
                fn bound(list: std.ArrayListUnmanaged(T), needed: usize) !usize {
                    if (needed <= list.capacity) return 0;
                    return std.math.mul(usize, try capacity(list.capacity, needed), @sizeOf(T)) catch error.OutOfMemory;
                }
                fn prepare(self: *@This(), a: std.mem.Allocator, list: std.ArrayListUnmanaged(T), needed: usize, copy_existing: bool) !bool {
                    if (needed <= list.capacity) return true;
                    if (self.new == null) self.new = try allocatePrivate(T, a, try capacity(list.capacity, needed));
                    if (!copy_existing) return true;
                    const take = @min(list.items.len - self.copied, @max(1, 16 * 1024 / @sizeOf(T)));
                    @memcpy(self.new.?[self.copied..][0..take], list.items[self.copied..][0..take]);
                    self.copied += take;
                    return self.copied == list.items.len;
                }
                fn storage(self: *@This(), list: *std.ArrayListUnmanaged(T)) []T {
                    return self.new orelse list.allocatedSlice();
                }
                fn adopt(self: *@This(), list: *std.ArrayListUnmanaged(T)) void {
                    if (self.new) |memory| {
                        const len = list.items.len;
                        self.old = list.allocatedSlice();
                        list.* = .fromOwnedSlice(memory);
                        list.items.len = len;
                        self.new = null;
                    }
                }
                fn retiredMemory(self: *@This()) []T {
                    return self.new orelse self.old orelse &.{};
                }
                fn clear(self: *@This()) void {
                    self.new = null;
                    self.old = null;
                }
            };
        }

        /// Additional peak bytes beyond the current encoder and borrowed input.
        /// Original allocations remain charged through descriptor adoption and
        /// bounded retirement. Reusable spare bounds avoid per-row allocation.
        pub fn memoryBound(e: *const StreamingEncoder, entry: Entry) !usize {
            if (try e.needsBlockBeforeEntry(entry)) return error.InvalidTableFile;
            const len = try tableEntryEncodedLen(entry);
            const ns = if (entry.namespace_name) |name| name.len else 0;
            const may_prefix = e.prefix_extractor != .none and entry.key.len != 0;
            var bytes: usize = @sizeOf(EntryPreparation);
            bytes = try checkedAddUsize(bytes, try Growth(u8).bound(e.block_bytes, e.block_bytes.items.len + len));
            bytes = try checkedAddUsize(bytes, try Growth(u16).bound(e.entry_offsets, e.entry_offsets.items.len + 1));
            bytes = try checkedAddUsize(bytes, try Growth(Pair).bound(e.block_hashes, e.block_hashes.items.len + 1));
            bytes = try checkedAddUsize(bytes, try Growth(Pair).bound(e.prefix_hashes, e.prefix_hashes.items.len + @intFromBool(may_prefix)));
            bytes = try checkedAddUsize(bytes, try Growth(Pair).bound(e.block_prefix_hashes, e.block_prefix_hashes.items.len + @intFromBool(may_prefix)));
            bytes = try checkedAddUsize(bytes, try Growth(u8).bound(e.entry_namespace_spare, ns));
            bytes = try checkedAddUsize(bytes, try Growth(u8).bound(e.entry_key_spare, entry.key.len));
            if (e.block_entry_count == 0) bytes = try checkedAddUsize(bytes, try checkedAddUsize(ns, entry.key.len));
            return bytes;
        }

        fn namespace(self: *const EntryPreparation) []const u8 {
            return self.input.namespace_name orelse &.{};
        }
        fn appendedPrefix(list: []const Pair, pair: Pair) bool {
            return list.len == 0 or !std.meta.eql(list[list.len - 1], pair);
        }

        fn rowCopy(self: *EntryPreparation, bytes: []const u8, row_offset: usize, next: Phase) void {
            const take = @min(bytes.len - self.offset, 16 * 1024);
            const dest = self.data.storage(&self.encoder.block_bytes);
            @memcpy(dest[self.encoder.block_bytes.items.len + row_offset + self.offset ..][0..take], bytes[self.offset..][0..take]);
            self.offset += take;
            if (self.offset == bytes.len) {
                self.offset = 0;
                self.phase = next;
            }
        }

        fn boundsCopy(self: *EntryPreparation, bytes: []const u8, comptime is_namespace: bool, next: Phase) void {
            const first = self.encoder.block_entry_count == 0;
            const take = @min(bytes.len - self.offset, if (first) @as(usize, 8 * 1024) else 16 * 1024);
            const dest = if (is_namespace) self.scratch_namespace.storage(&self.encoder.entry_namespace_spare) else self.scratch_key.storage(&self.encoder.entry_key_spare);
            @memcpy(dest[self.offset..][0..take], bytes[self.offset..][0..take]);
            if (first) {
                const smallest: []u8 = if (is_namespace) self.smallest_namespace orelse &.{} else self.smallest_key.?;
                @memcpy(smallest[self.offset..][0..take], bytes[self.offset..][0..take]);
            }
            self.offset += take;
            if (self.offset == bytes.len) {
                self.offset = 0;
                self.phase = next;
            }
        }

        /// One bounded CPU stage. Rejection preserves input, hash state,
        /// speculative buffers and all logical encoder state. No borrow escapes.
        pub fn step(self: *EntryPreparation, original: ?read_admission.Port) !bool {
            if (self.retiring or self.phase == .retired) return error.InvalidTableFile;
            if (original) |control| try control.checkpoint();
            const e = self.encoder;
            switch (self.phase) {
                .prefix_scan => {
                    if (e.prefix_extractor == .none) {
                        self.phase = .hash_init;
                        return false;
                    }
                    const take = @min(self.input.key.len - self.offset, 16 * 1024);
                    if (std.mem.indexOfAny(u8, self.input.key[self.offset..][0..take], ":/")) |index| {
                        self.prefix_len = self.offset + index + 1;
                        self.offset = 0;
                        self.phase = .hash_init;
                    } else {
                        self.offset += take;
                        if (self.offset == self.input.key.len) {
                            self.offset = 0;
                            self.phase = .hash_init;
                        }
                    }
                },
                .hash_init => {
                    for ([_]u64{ 0x243f6a8885a308d3, 0x13198a2e03707344, 0x452821e638d01377, 0xbe5466cf34e90c6c }, 0..) |seed, i| self.hashers[i] = .init(seed);
                    var raw: [4]u8 = undefined;
                    std.mem.writeInt(u32, &raw, if (self.input.namespace_name) |name| @intCast(name.len) else std.math.maxInt(u32), .little);
                    for (self.hashers[0..if (self.prefix_len != null) @as(usize, 4) else 2]) |*h| h.update(&raw);
                    self.phase = .hash_namespace;
                },
                .hash_namespace => {
                    const bytes = self.namespace();
                    const take = @min(bytes.len - self.offset, 4096);
                    for (self.hashers[0..if (self.prefix_len != null) @as(usize, 4) else 2]) |*h| h.update(bytes[self.offset..][0..take]);
                    self.offset += take;
                    if (self.offset == bytes.len) {
                        self.offset = 0;
                        self.phase = .hash_key_header;
                    }
                },
                .hash_key_header => {
                    var raw: [4]u8 = undefined;
                    std.mem.writeInt(u32, &raw, @intCast(self.input.key.len), .little);
                    for (self.hashers[0..2]) |*h| h.update(&raw);
                    if (self.prefix_len) |len| {
                        std.mem.writeInt(u32, &raw, @intCast(len), .little);
                        for (self.hashers[2..4]) |*h| h.update(&raw);
                    }
                    self.phase = .hash_key;
                },
                .hash_key => {
                    const take = @min(self.input.key.len - self.offset, 4096);
                    for (self.hashers[0..2]) |*h| h.update(self.input.key[self.offset..][0..take]);
                    self.offset += take;
                    if (self.offset == self.input.key.len) {
                        self.offset = 0;
                        self.phase = .hash_prefix;
                    }
                },
                .hash_prefix => {
                    const len = self.prefix_len orelse 0;
                    const take = @min(len - self.offset, 4096);
                    if (self.prefix_len != null) for (self.hashers[2..4]) |*h| h.update(self.input.key[self.offset..][0..take]);
                    self.offset += take;
                    if (self.offset == len) {
                        self.offset = 0;
                        self.phase = .hash_finish;
                    }
                },
                .hash_finish => {
                    self.hashes = .{ self.hashers[0].final(), self.hashers[1].final() };
                    if (self.prefix_len != null) {
                        const hashes: Pair = .{ self.hashers[2].final(), self.hashers[3].final() };
                        self.prefix_hashes = hashes;
                        self.add_run_prefix = appendedPrefix(e.prefix_hashes.items, hashes);
                        self.add_block_prefix = appendedPrefix(e.block_prefix_hashes.items, hashes);
                    }
                    self.phase = .growth;
                },
                .growth => {
                    const done = switch (self.growth_index) {
                        0 => try self.data.prepare(e.allocator, e.block_bytes, e.block_bytes.items.len + self.entry_len, true),
                        1 => try self.offsets.prepare(e.allocator, e.entry_offsets, e.entry_offsets.items.len + 1, true),
                        2 => try self.block_hashes.prepare(e.allocator, e.block_hashes, e.block_hashes.items.len + 1, true),
                        3 => try self.run_prefixes.prepare(e.allocator, e.prefix_hashes, e.prefix_hashes.items.len + @intFromBool(self.add_run_prefix), true),
                        4 => try self.block_prefixes.prepare(e.allocator, e.block_prefix_hashes, e.block_prefix_hashes.items.len + @intFromBool(self.add_block_prefix), true),
                        5 => try self.scratch_namespace.prepare(e.allocator, e.entry_namespace_spare, self.namespace().len, false),
                        6 => try self.scratch_key.prepare(e.allocator, e.entry_key_spare, self.input.key.len, false),
                        else => unreachable,
                    };
                    if (done) self.growth_index += 1;
                    if (self.growth_index == 7) self.phase = .smallest_allocate;
                },
                .smallest_allocate => {
                    if (e.block_entry_count == 0) {
                        if (self.smallest_namespace == null) if (self.input.namespace_name) |name| {
                            self.smallest_namespace = try allocatePrivate(u8, e.allocator, name.len);
                        };
                        if (self.smallest_key == null) self.smallest_key = try allocatePrivate(u8, e.allocator, self.input.key.len);
                    }
                    self.phase = .row_header;
                },
                .row_header => {
                    @memcpy(self.data.storage(&e.block_bytes)[e.block_bytes.items.len..][0..13], &self.header);
                    self.phase = .row_namespace;
                },
                .row_namespace => self.rowCopy(self.namespace(), 13, .row_key),
                .row_key => self.rowCopy(self.input.key, 13 + self.namespace().len, .row_value),
                .row_value => self.rowCopy(self.input.value, 13 + self.namespace().len + self.input.key.len, .bounds_namespace),
                .bounds_namespace => self.boundsCopy(self.namespace(), true, .bounds_key),
                .bounds_key => self.boundsCopy(self.input.key, false, .ready),
                .ready, .adopted => return true,
                .retired => unreachable,
            }
            return self.phase == .ready or self.phase == .adopted;
        }

        /// Fixed CPU receipt. Every allocation, hash and byte copy is complete.
        /// Move input ownership/counters before another checkpoint may reject.
        pub fn adopt(self: *EntryPreparation) void {
            std.debug.assert(!self.retiring and self.phase == .ready);
            const e = self.encoder;
            const start = e.block_bytes.items.len;
            self.data.adopt(&e.block_bytes);
            self.offsets.adopt(&e.entry_offsets);
            self.block_hashes.adopt(&e.block_hashes);
            self.run_prefixes.adopt(&e.prefix_hashes);
            self.block_prefixes.adopt(&e.block_prefix_hashes);
            self.scratch_namespace.adopt(&e.entry_namespace_spare);
            self.scratch_key.adopt(&e.entry_key_spare);
            e.block_bytes.items.len = start + self.entry_len;
            e.entry_offsets.appendAssumeCapacity(@intCast(start));
            e.block_hashes.appendAssumeCapacity(self.hashes);
            if (self.add_run_prefix) e.prefix_hashes.appendAssumeCapacity(self.prefix_hashes.?);
            if (self.add_block_prefix) e.block_prefix_hashes.appendAssumeCapacity(self.prefix_hashes.?);
            e.filter_builder.addHashes(self.hashes[0], self.hashes[1]);
            if (e.block_entry_count == 0) {
                e.block_start = @intCast(e.logical_entry_data_len);
                e.block_first_entry_index = e.entry_count;
                e.block_smallest_namespace_name = self.smallest_namespace;
                self.smallest_namespace = null;
                e.block_smallest_key = self.smallest_key.?;
                self.smallest_key = null;
            }
            e.entry_namespace_spare.items.len = self.namespace().len;
            e.entry_key_spare.items.len = self.input.key.len;
            std.mem.swap(std.ArrayListUnmanaged(u8), &e.entry_namespace_spare, &e.block_largest_namespace_buffer);
            std.mem.swap(std.ArrayListUnmanaged(u8), &e.entry_key_spare, &e.block_largest_key_buffer);
            e.block_largest_namespace_name = if (self.input.namespace_name != null) e.block_largest_namespace_buffer.items else null;
            e.block_largest_key = e.block_largest_key_buffer.items;
            e.logical_entry_data_len += self.entry_len;
            e.entry_count += 1;
            e.block_entry_count += 1;
            self.phase = .adopted;
        }

        fn retireGrowth(self: *EntryPreparation, comptime T: type, growth: *Growth(T), original: ?read_admission.Port) !void {
            try retirePrivate(T, self.encoder.allocator, growth.retiredMemory(), &self.retire_offset, original);
            growth.clear();
        }

        pub fn retireWithAdmission(self: *EntryPreparation, original: ?read_admission.Port) !void {
            if (self.phase == .retired) return;
            self.retiring = true;
            while (self.retire_slot < 9) {
                switch (self.retire_slot) {
                    0 => try self.retireGrowth(u8, &self.data, original),
                    1 => try self.retireGrowth(u16, &self.offsets, original),
                    2 => try self.retireGrowth(Pair, &self.block_hashes, original),
                    3 => try self.retireGrowth(Pair, &self.run_prefixes, original),
                    4 => try self.retireGrowth(Pair, &self.block_prefixes, original),
                    5 => try self.retireGrowth(u8, &self.scratch_namespace, original),
                    6 => try self.retireGrowth(u8, &self.scratch_key, original),
                    7 => {
                        try retirePrivate(u8, self.encoder.allocator, self.smallest_namespace orelse &.{}, &self.retire_offset, original);
                        self.smallest_namespace = null;
                    },
                    8 => {
                        try retirePrivate(u8, self.encoder.allocator, self.smallest_key orelse &.{}, &self.retire_offset, original);
                        self.smallest_key = null;
                    },
                    else => unreachable,
                }
                self.retire_slot += 1;
            }
            self.encoder.entry_active = false;
            self.phase = .retired;
        }
        pub fn deinit(self: *EntryPreparation) void {
            self.retireWithAdmission(null) catch unreachable;
        }
    };

    /// Legacy synchronous wrapper. Admitted callers retain EntryPreparation and
    /// Block owners explicitly rather than invoking an implicit sink flush.
    pub fn appendEntry(self: *StreamingEncoder, entry: Entry) !void {
        if (!self.legacy_sink_allowed) return error.ExecutionWaitUnsupported;
        if (try self.needsBlockBeforeEntry(entry)) try self.flushBlock();
        var preparation = try self.prepareEntry(entry);
        defer preparation.deinit();
        while (!(try preparation.step(null))) {}
        preparation.adopt();
    }

    /// Legacy synchronous final-block boundary. This can invoke sink IO on
    /// caller CPU; admitted writers still need a retained block producer.
    /// A partial output failure makes the encoder terminal, never replayable.
    pub fn finishBlocks(self: *StreamingEncoder) !void {
        if (!self.legacy_sink_allowed) return error.ExecutionWaitUnsupported;
        if (self.finished or self.block_active or self.entry_active) return error.InvalidTableFile;
        self.flushBlock() catch |cause| {
            self.finished = true;
            return cause;
        };
    }

    /// The final block must already have been published by its owning writer.
    /// Metadata owns the final filters and borrows this frozen encoder until
    /// result transfer or destruction. It issues no filesystem operations.
    pub fn prepareMetadata(self: *StreamingEncoder) !Metadata {
        if (self.finished or self.block_active or self.entry_active) return error.InvalidTableFile;
        return self.metadataAfterFinalBlock();
    }

    fn metadataAfterFinalBlock(self: *StreamingEncoder) !Metadata {
        if (self.block_active or self.entry_active or self.block_entry_count != 0) return error.InvalidTableFile;
        const physical_len = self.physical_output_len - self.entry_data_start;
        _ = try checkedU32(self.entry_count);
        _ = try checkedU32(self.logical_entry_data_len);
        _ = try checkedU32(physical_len);
        self.finished = true;
        self.metadata_active = true;
        const filter = self.filter_builder.finish();
        self.filter_builder_active = false;
        return .{ .encoder = self, .table_filter = filter, .metadata_offset = self.physical_output_len, .stats = .{
            .logical_entry_bytes = @intCast(self.logical_entry_data_len),
            .physical_entry_bytes = @intCast(physical_len),
        } };
    }

    pub fn finish(self: *StreamingEncoder) !StreamingEncoderResult {
        if (!self.legacy_sink_allowed) return error.ExecutionWaitUnsupported;
        if (self.finished or self.block_active or self.entry_active) return error.InvalidTableFile;
        // This legacy API has no retained physical-write receipt. A failed
        // final block may have mutated the sink and must never be replayed.
        self.finished = true;
        try self.flushBlock();
        var metadata = try self.metadataAfterFinalBlock();
        defer metadata.deinit();
        while (try metadata.next(null)) |command| {
            if (command.append) {
                std.debug.assert(self.sink.len() == command.offset);
                try self.sink.appendSlice(command.bytes);
            } else try self.sink.writeAt(command.offset, command.bytes);
            metadata.acknowledge();
        }
        return metadata.takeResult();
    }

    /// Retained pure-CPU metadata producer. The caller retains the encoder's
    /// existing credits (including the moved table filter), prepays the extra
    /// memoryBound(), freezes the encoder and serializes methods. Each command remains byte
    /// identical until acknowledged after physical completion. IO ownership and
    /// partial-write recovery belong to the writer; this value never dispatches
    /// work, stores an admission borrow or treats a rejected waiter as a receipt.
    pub const Metadata = struct {
        pub const Command = struct { offset: usize, append: bool, bytes: []const u8 };
        const Phase = enum {
            entry_count_patch,
            data_len_patch,
            prefix_allocate,
            prefix_zero,
            prefix_hashes,
            offsets_magic,
            offsets,
            table_filter,
            largest_count,
            largest,
            filters_count,
            filters,
            slots_count,
            slots,
            physical_count,
            physical,
            smallest_count,
            smallest,
            extractor,
            prefix_filter,
            block_prefix_count,
            block_prefix,
            footer,
            complete,
        };
        encoder: *StreamingEncoder,
        table_filter: ?bloom.OwnedFilter,
        prefix_builder: ?bloom.Builder = null,
        prefix_filter: ?bloom.OwnedFilter = null,
        phase: Phase = .entry_count_patch,
        index: usize = 0,
        field: usize = 0,
        slot: usize = 0,
        offset: usize = 0,
        metadata_offset: usize,
        metadata_len: usize = 0,
        crc: Crc32 = .init(),
        stats: CompressionStats,
        buffer: [16 * 1024]u8 = undefined,
        len: usize = 0,
        output_offset: usize = 0,
        append: bool = true,
        pending: bool = false,
        retiring: bool = false,
        retired: bool = false,
        retire_slot: usize = 0,
        retire_offset: usize = 0,

        pub fn memoryBound(encoder: *const StreamingEncoder) !usize {
            const prefix_bytes = (try bloomEncodedLenForKeyCount(encoder.prefix_hashes.items.len, encoder.bloom_config)) - bloom_encoded_header_len;
            return checkedAddUsize(@sizeOf(@This()), prefix_bytes);
        }

        /// Settle accepted file work before retiring its borrowed commands.
        pub fn retireWithAdmission(self: *@This(), original: ?read_admission.Port) !void {
            if (self.retired) return;
            self.retiring = true;
            while (self.retire_slot < 3) {
                const bytes: []u8 = switch (self.retire_slot) {
                    0 => if (self.table_filter) |f| f.bytes else &.{},
                    1 => if (self.prefix_builder) |b| b.bytes else &.{},
                    2 => if (self.prefix_filter) |f| f.bytes else &.{},
                    else => unreachable,
                };
                try retirePrivate(u8, self.encoder.allocator, bytes, &self.retire_offset, original);
                switch (self.retire_slot) {
                    0 => self.table_filter = null,
                    1 => self.prefix_builder = null,
                    2 => self.prefix_filter = null,
                    else => unreachable,
                }
                self.retire_slot += 1;
            }
            self.encoder.metadata_active = false;
            self.retired = true;
        }
        pub fn deinit(self: *@This()) void {
            self.retireWithAdmission(null) catch unreachable;
        }

        fn advance(self: *@This(), phase: Phase) void {
            self.phase = phase;
            self.index = 0;
            self.field = 0;
            self.slot = 0;
            self.offset = 0;
        }

        fn command(self: *@This()) Command {
            std.debug.assert(self.len != 0);
            self.pending = true;
            return .{ .offset = self.output_offset, .append = self.append, .bytes = self.buffer[0..self.len] };
        }

        /// Fixed receipt only. The caller must acknowledge once, after its
        /// accepted write is known complete, before asking for another command.
        pub fn acknowledge(self: *@This()) void {
            std.debug.assert(self.pending);
            self.pending = false;
            self.len = 0;
        }

        fn beginBytes(self: *@This()) void {
            if (self.len == 0) {
                self.output_offset = self.metadata_offset + self.metadata_len;
                self.append = true;
            }
        }

        fn emit(self: *@This(), bytes: []const u8) !bool {
            self.beginBytes();
            const n = @min(bytes.len - self.offset, self.buffer.len - self.len);
            const new_len = try checkedAddUsize(self.metadata_len, n);
            const out = self.buffer[self.len..][0..n];
            @memcpy(out, bytes[self.offset..][0..n]);
            self.crc.update(out);
            self.metadata_len = new_len;
            self.len += n;
            self.offset += n;
            if (self.offset != bytes.len) return false;
            self.offset = 0;
            return true;
        }

        fn number(self: *@This(), comptime T: type, value: T) !bool {
            var raw: [@sizeOf(T)]u8 = undefined;
            std.mem.writeInt(T, &raw, value, .little);
            return self.emit(&raw);
        }

        fn emitFilter(self: *@This(), value: ?bloom.OwnedFilter) !bool {
            if (self.field == 0) {
                if (try self.number(u32, try checkedU32(if (value) |v| v.encodedSize() else 0))) self.field = 1;
                return false;
            }
            if (value) |v| {
                self.beginBytes();
                const n = v.encodeRange(self.offset, self.buffer[self.len..]);
                const new_len = try checkedAddUsize(self.metadata_len, n);
                self.crc.update(self.buffer[self.len..][0..n]);
                self.metadata_len = new_len;
                self.len += n;
                self.offset += n;
                if (self.offset != v.encodedSize()) return false;
            }
            self.field = 0;
            self.offset = 0;
            return true;
        }

        pub fn next(self: *@This(), original: ?read_admission.Port) !?Command {
            if (self.retiring or self.retired) return error.InvalidTableFile;
            if (original) |control| try control.checkpoint();
            if (self.pending) return self.command();
            const e = self.encoder;
            while (self.phase != .complete) {
                if (original) |control| try control.checkpoint();
                if (self.len == self.buffer.len) return self.command();
                switch (self.phase) {
                    .entry_count_patch, .data_len_patch => {
                        const count = self.phase == .entry_count_patch;
                        std.debug.assert(self.len == 0);
                        std.mem.writeInt(u32, self.buffer[0..4], try checkedU32(if (count) e.entry_count else @as(usize, @intCast(self.stats.physical_entry_bytes))), .little);
                        self.len = 4;
                        self.output_offset = if (count) e.entry_count_offset else e.entry_data_len_offset;
                        self.append = false;
                        self.advance(if (count) .data_len_patch else .prefix_allocate);
                        return self.command();
                    },
                    .prefix_allocate => {
                        // Validate the size before entering the library's
                        // uninitialized allocation; no hash sees dirty bytes.
                        _ = try memoryBound(e);
                        self.prefix_builder = try bloom.Builder.initUninitialized(e.allocator, e.prefix_hashes.items.len, e.bloom_config);
                        self.advance(.prefix_zero);
                    },
                    .prefix_zero => {
                        const bytes = self.prefix_builder.?.bytes;
                        const n = @min(bytes.len - self.offset, 16 * 1024);
                        @memset(bytes[self.offset..][0..n], 0);
                        self.offset += n;
                        if (self.offset == bytes.len) self.advance(.prefix_hashes);
                    },
                    .prefix_hashes => {
                        const end = self.index + @min(e.prefix_hashes.items.len - self.index, 64);
                        while (self.index < end) : (self.index += 1) {
                            const hashes = e.prefix_hashes.items[self.index];
                            self.prefix_builder.?.addHashes(hashes[0], hashes[1]);
                        }
                        if (self.index == e.prefix_hashes.items.len) {
                            self.prefix_filter = self.prefix_builder.?.finish();
                            self.prefix_builder = null;
                            self.advance(.offsets_magic);
                        }
                    },
                    .offsets_magic => if (try self.emit(packed_offsets_magic)) {
                        self.advance(.offsets);
                    },
                    .offsets => {
                        if (self.index == e.entry_offsets.items.len) self.advance(.table_filter) else if (try self.number(u16, e.entry_offsets.items[self.index])) {
                            self.index += 1;
                        }
                    },
                    .table_filter => if (try self.emitFilter(self.table_filter)) {
                        self.advance(.largest_count);
                    },
                    .largest_count, .filters_count, .slots_count, .physical_count, .smallest_count, .block_prefix_count => {
                        if (try self.number(u32, try checkedU32(e.blocks.items.len))) self.advance(switch (self.phase) {
                            .largest_count => .largest,
                            .filters_count => .filters,
                            .slots_count => .slots,
                            .physical_count => .physical,
                            .smallest_count => .smallest,
                            .block_prefix_count => .block_prefix,
                            else => unreachable,
                        });
                    },
                    .largest, .smallest => {
                        const largest = self.phase == .largest;
                        if (self.index == e.blocks.items.len) {
                            self.advance(if (largest) .filters_count else .extractor);
                            continue;
                        }
                        const block = e.blocks.items[self.index];
                        const name = if (largest) block.largest_namespace_name else block.smallest_namespace_name;
                        const key = if (largest) block.largest_key else block.smallest_key;
                        const base: usize = if (largest) 4 else 0;
                        const done = if (self.field < base) try self.number(u32, switch (self.field) {
                            0 => block.relative_offset,
                            1 => block.len,
                            2 => block.first_entry_index,
                            3 => block.entry_count,
                            else => unreachable,
                        }) else switch (self.field - base) {
                            0 => try self.number(u32, if (name) |n| try checkedU32(n.len) else std.math.maxInt(u32)),
                            1 => try self.number(u32, try checkedU32(key.len)),
                            2 => try self.emit(if (name) |n| n else ""),
                            3 => try self.emit(key),
                            else => unreachable,
                        };
                        if (done) {
                            self.field += 1;
                            if (self.field == base + 4) {
                                self.field = 0;
                                self.index += 1;
                            }
                        }
                    },
                    .filters, .block_prefix => {
                        const prefix = self.phase == .block_prefix;
                        if (self.index == e.blocks.items.len) self.advance(if (prefix) .footer else .slots_count) else if (try self.emitFilter(if (prefix) e.blocks.items[self.index].prefix_filter else e.blocks.items[self.index].filter)) {
                            self.index += 1;
                        }
                    },
                    .slots => {
                        if (self.index == e.blocks.items.len) {
                            self.advance(.physical_count);
                            continue;
                        }
                        const slots = e.blocks.items[self.index].hash_slots;
                        if (self.field == 0) {
                            if (try self.number(u32, try checkedU32(slots.len))) self.field = 1;
                        } else if (self.slot == slots.len) {
                            self.index += 1;
                            self.field = 0;
                            self.slot = 0;
                        } else if (try self.number(u32, slots[self.slot])) {
                            self.slot += 1;
                        }
                    },
                    .physical => {
                        if (self.index == e.blocks.items.len) {
                            self.advance(.smallest_count);
                            continue;
                        }
                        const block = e.blocks.items[self.index];
                        if (try self.number(u32, switch (self.field) {
                            0 => block.physical_relative_offset,
                            1 => block.physical_len,
                            2 => @backingInt(block.compression),
                            3 => block.checksum,
                            else => unreachable,
                        })) {
                            self.field += 1;
                            if (self.field == 4) {
                                switch (block.compression) {
                                    .none => self.stats.raw_blocks += 1,
                                    .snappy, .prefix, .prefix_snappy => {
                                        self.stats.compressed_blocks += 1;
                                        self.stats.compression_codec_mask |= blockCompressionCodecMask(block.compression);
                                    },
                                }
                                self.field = 0;
                                self.index += 1;
                            }
                        }
                    },
                    .extractor => if (try self.number(u32, @backingInt(e.prefix_extractor))) {
                        self.advance(.prefix_filter);
                    },
                    .prefix_filter => if (try self.emitFilter(self.prefix_filter)) {
                        self.advance(.block_prefix_count);
                    },
                    .footer => {
                        // Keep the checksummed metadata command separate from
                        // the footer; rejection cannot hash either one twice.
                        if (self.len != 0) return self.command();
                        const raw = try footerBytes(self.metadata_offset, self.metadata_len, e.entry_count, e.logical_entry_data_len, self.crc.final());
                        @memcpy(self.buffer[0..raw.len], &raw);
                        self.len = raw.len;
                        self.output_offset = self.metadata_offset + self.metadata_len;
                        self.append = true;
                        self.advance(.complete);
                        return self.command();
                    },
                    .complete => unreachable,
                }
            }
            return null;
        }

        /// Coupled receipt: after accepted file IO completes, acknowledge the
        /// frozen input cursor before another original checkpoint can reject.
        pub fn writeNextWithAdmission(self: *@This(), writer: anytype, original: read_admission.Port) !bool {
            const output = (try self.next(original)) orelse return false;
            try writer.writeWithAdmission(output.offset, output.bytes, original);
            self.acknowledge();
            return true;
        }

        pub fn takeResult(self: *@This()) !StreamingEncoderResult {
            if (self.retiring or self.retired or self.phase != .complete or self.pending or self.table_filter == null) return error.InvalidTableFile;
            const owned_filter = self.table_filter.?;
            self.table_filter = null;
            if (self.encoder.compression_stats_out) |out| out.* = self.stats;
            return .{ .size_bytes = self.metadata_offset + self.metadata_len + footer_len, .entry_count = self.encoder.entry_count, .filter = owned_filter, .compression_stats = self.stats };
        }
    };

    fn setBlockSmallest(self: *StreamingEncoder, entry: Entry) !void {
        const namespace = if (entry.namespace_name) |name| try self.allocator.dupe(u8, name) else null;
        errdefer if (namespace) |name| self.allocator.free(name);
        const key = try self.allocator.dupe(u8, entry.key);
        self.clearBlockSmallest();
        self.block_smallest_namespace_name = namespace;
        self.block_smallest_key = key;
    }

    fn clearBlockSmallest(self: *StreamingEncoder) void {
        if (self.block_smallest_namespace_name) |name| self.allocator.free(name);
        self.block_smallest_namespace_name = null;
        if (self.block_smallest_key.len > 0) self.allocator.free(self.block_smallest_key);
        self.block_smallest_key = &.{};
    }

    fn reserveBlockLargest(self: *StreamingEncoder, namespace_len: usize, key_len: usize) !void {
        defer {
            if (self.block_largest_namespace_name != null) self.block_largest_namespace_name = self.block_largest_namespace_buffer.items;
            self.block_largest_key = self.block_largest_key_buffer.items;
        }
        try self.block_largest_namespace_buffer.ensureTotalCapacity(self.allocator, namespace_len);
        try self.block_largest_key_buffer.ensureTotalCapacity(self.allocator, key_len);
    }

    fn setBlockLargest(self: *StreamingEncoder, entry: Entry) !void {
        try self.reserveBlockLargest(if (entry.namespace_name) |name| name.len else 0, entry.key.len);
        self.clearBlockLargest();
        if (entry.namespace_name) |name| self.block_largest_namespace_buffer.appendSliceAssumeCapacity(name);
        self.block_largest_key_buffer.appendSliceAssumeCapacity(entry.key);
        self.block_largest_namespace_name = if (entry.namespace_name != null) self.block_largest_namespace_buffer.items else null;
        self.block_largest_key = self.block_largest_key_buffer.items;
    }

    fn clearBlockLargest(self: *StreamingEncoder) void {
        // Length-only discard: safety-mode ArrayList.clear poisons all bytes.
        self.block_largest_namespace_buffer.items.len = 0;
        self.block_largest_key_buffer.items.len = 0;
        self.block_largest_namespace_name = null;
        self.block_largest_key = &.{};
    }

    /// Freeze the current block. The caller retains encoder/input credits and
    /// prepays Block.memoryBound() until retirement. Retire accepted file IO
    /// before abandoning this owner; its payload is borrowed by that IO.
    pub fn prepareBlock(self: *StreamingEncoder) !Block {
        if (self.finished or self.block_active or self.entry_active) return error.InvalidTableFile;
        return Block.init(self);
    }

    pub const Block = struct {
        pub const Command = struct { offset: usize, bytes: []const u8 };
        const Phase = enum { prefix_allocate, prefix_encode, snappy_allocate, snappy_encode, checksum, filter_allocate, filter_zero, filter_hashes, bounds_allocate, bounds_copy, metadata_allocate, metadata_copy, validate, ready, complete, retired };
        encoder: *StreamingEncoder,
        phase: Phase = .prefix_allocate,
        prefix: ?[]u8 = null,
        prefix_cursor: ?PrefixEncodeCursor = null,
        prefix_len: usize = 0,
        workspace: ?[]u32 = null,
        snappy_cursor: ?snappy.EncodeCursor = null,
        compression: BlockCompression = .none,
        snappy_attempted: bool = false,
        crc: Crc32 = .init(),
        cpu_offset: usize = 0,
        filters: [2]?bloom.Builder = .{ null, null },
        filter_index: usize = 0,
        hash_index: usize = 0,
        largest_namespace: ?[]u8 = null,
        largest_key: ?[]u8 = null,
        new_blocks: ?[]OwnedEncodedBlockMeta = null,
        old_blocks: ?[]OwnedEncodedBlockMeta = null,
        old_compression: ?[]u8 = null,
        copied_blocks: usize = 0,
        meta: OwnedEncodedBlockMeta = undefined,
        completed_metadata_bytes: usize = 0,
        output_offset: usize = 0,
        pending_len: usize = 0,
        io_attempted: bool = false,
        published: bool = false,
        retire_slot: usize = 0,
        retire_offset: usize = 0,
        retiring: bool = false,

        fn metadataCapacity(e: *const StreamingEncoder) !usize {
            return checkedAddUsize(try checkedAddUsize(e.blocks.items.len, e.blocks.items.len / 2), 8);
        }

        /// Additional peak bytes beyond the encoder's current working set.
        /// Includes speculative metadata growth while its old array survives,
        /// private compression, filters/bounds, and the reusable buffer growth.
        pub fn memoryBound(e: *const StreamingEncoder) !usize {
            var total: usize = @sizeOf(Block);
            if (e.compression_policy != .none) total = try checkedAddUsize(total, try PrefixEncodeCursor.outputBound(e.block_bytes.items.len));
            if (e.compression_policy == .snappy_adaptive and e.snappy_reprobe_countdown == 0 and e.block_bytes.items.len >= min_compress_block_bytes) {
                total = try checkedAddUsize(total, snappy.EncodeCursor.table_entries * @sizeOf(u32));
                const bound = try snappy.EncodeCursor.outputBound(e.block_bytes.items.len);
                if (e.compression_bytes.capacity < bound) total = try checkedAddUsize(total, bound);
            }
            total = try checkedAddUsize(total, e.block_largest_key.len);
            if (e.block_largest_namespace_name) |name| total = try checkedAddUsize(total, name.len);
            total = try checkedAddUsize(total, try bloomEncodedLenForKeyCount(e.block_hashes.items.len, block_filter_config));
            total = try checkedAddUsize(total, try bloomEncodedLenForKeyCount(e.block_prefix_hashes.items.len, block_filter_config));
            if (e.blocks.items.len == e.blocks.capacity) total = try checkedAddUsize(total, try std.math.mul(usize, try metadataCapacity(e), @sizeOf(OwnedEncodedBlockMeta)));
            return total;
        }

        fn init(e: *StreamingEncoder) !Block {
            if (e.block_active or e.entry_active or e.block_entry_count == 0) return error.InvalidTableFile;
            _ = try memoryBound(e);
            e.block_active = true;
            return .{ .encoder = e };
        }

        fn preSnappyBytes(self: *const Block) []const u8 {
            return if (self.prefix_len != 0) self.prefix.?[0..self.prefix_len] else self.encoder.block_bytes.items;
        }

        fn payload(self: *const Block) []const u8 {
            return if (self.compression == .snappy or self.compression == .prefix_snappy) self.encoder.compression_bytes.items else self.preSnappyBytes();
        }

        /// One bounded CPU stage per call. The original execution checks before
        /// every stage; rejection never loses a completed stage or frozen write.
        pub fn step(self: *Block, original: ?read_admission.Port) !bool {
            if (self.phase == .retired or self.retiring) return error.InvalidTableFile;
            if (original) |control| try control.checkpoint();
            const e = self.encoder;
            const a = e.allocator;
            switch (self.phase) {
                .prefix_allocate => {
                    if (e.compression_policy == .none) {
                        self.phase = .checksum;
                        return false;
                    }
                    self.prefix = try allocatePrivate(u8, a, try PrefixEncodeCursor.outputBound(e.block_bytes.items.len));
                    self.prefix_cursor = try PrefixEncodeCursor.init(e.block_bytes.items.len);
                    self.phase = .prefix_encode;
                },
                .prefix_encode => {
                    if ((try self.prefix_cursor.?.step(e.block_bytes.items, self.prefix.?, 4096)).done) {
                        if (self.prefix_cursor.?.output_offset < e.block_bytes.items.len) {
                            self.prefix_len = self.prefix_cursor.?.output_offset;
                            self.compression = .prefix;
                        }
                        self.phase = .snappy_allocate;
                    }
                },
                .snappy_allocate => {
                    const src = self.preSnappyBytes();
                    if (e.compression_policy != .snappy_adaptive or e.snappy_reprobe_countdown != 0 or src.len < min_compress_block_bytes) {
                        self.phase = .checksum;
                        return false;
                    }
                    // Old contents are dead. Capacity growth must not copy a
                    // previous oversized block under one execution checkpoint.
                    const bound = try snappy.EncodeCursor.outputBound(src.len);
                    if (e.compression_bytes.capacity < bound) {
                        const memory = try allocatePrivate(u8, a, bound);
                        // Keep the old allocation charged until bounded
                        // retirement instead of copying/poisoning it here.
                        self.old_compression = e.compression_bytes.allocatedSlice();
                        e.compression_bytes = .fromOwnedSlice(memory);
                    }
                    e.compression_bytes.items.len = 0;
                    if (self.workspace == null) self.workspace = try allocatePrivate(u32, a, snappy.EncodeCursor.table_entries);
                    self.snappy_cursor = snappy.EncodeCursor.init(src.len);
                    self.snappy_attempted = true;
                    self.phase = .snappy_encode;
                },
                .snappy_encode => {
                    if ((try self.snappy_cursor.?.step(self.preSnappyBytes(), e.compression_bytes.allocatedSlice(), self.workspace.?, 4096)).done) {
                        e.compression_bytes.items.len = self.snappy_cursor.?.output_offset;
                        if (e.compression_bytes.items.len < self.preSnappyBytes().len - self.preSnappyBytes().len / compression_savings_denominator) {
                            self.compression = if (self.prefix_len != 0) .prefix_snappy else .snappy;
                        }
                        self.phase = .checksum;
                    }
                },
                .checksum => {
                    const src = self.payload();
                    const n = @min(src.len - self.cpu_offset, 16 * 1024);
                    self.crc.update(src[self.cpu_offset..][0..n]);
                    self.cpu_offset += n;
                    if (self.cpu_offset == src.len) {
                        self.cpu_offset = 0;
                        self.phase = .filter_allocate;
                    }
                },
                .filter_allocate => {
                    const hashes = if (self.filter_index == 0) e.block_hashes.items else e.block_prefix_hashes.items;
                    _ = try bloomEncodedLenForKeyCount(hashes.len, block_filter_config);
                    self.filters[self.filter_index] = try bloom.Builder.initUninitialized(a, hashes.len, block_filter_config);
                    self.phase = .filter_zero;
                },
                .filter_zero => {
                    const bytes = self.filters[self.filter_index].?.bytes;
                    const n = @min(bytes.len - self.cpu_offset, 16 * 1024);
                    @memset(bytes[self.cpu_offset..][0..n], 0);
                    self.cpu_offset += n;
                    if (self.cpu_offset == bytes.len) {
                        self.cpu_offset = 0;
                        self.phase = .filter_hashes;
                    }
                },
                .filter_hashes => {
                    const hashes = if (self.filter_index == 0) e.block_hashes.items else e.block_prefix_hashes.items;
                    const end = self.hash_index + @min(hashes.len - self.hash_index, 64);
                    for (hashes[self.hash_index..end]) |hash| self.filters[self.filter_index].?.addHashes(hash[0], hash[1]);
                    self.hash_index = end;
                    if (end == hashes.len) {
                        self.hash_index = 0;
                        if (self.filter_index == 0) {
                            self.filter_index = 1;
                            self.phase = .filter_allocate;
                        } else self.phase = .bounds_allocate;
                    }
                },
                .bounds_allocate => {
                    if (self.largest_namespace == null) {
                        if (e.block_largest_namespace_name) |name| self.largest_namespace = try allocatePrivate(u8, a, name.len);
                    }
                    self.largest_key = try allocatePrivate(u8, a, e.block_largest_key.len);
                    self.phase = .bounds_copy;
                },
                .bounds_copy => {
                    const ns = e.block_largest_namespace_name orelse &.{};
                    if (self.cpu_offset < ns.len) {
                        const n = @min(ns.len - self.cpu_offset, 16 * 1024);
                        @memcpy(self.largest_namespace.?[self.cpu_offset..][0..n], ns[self.cpu_offset..][0..n]);
                        self.cpu_offset += n;
                    } else {
                        const offset = self.cpu_offset - ns.len;
                        const n = @min(e.block_largest_key.len - offset, 16 * 1024);
                        @memcpy(self.largest_key.?[offset..][0..n], e.block_largest_key[offset..][0..n]);
                        self.cpu_offset += n;
                        if (offset + n == e.block_largest_key.len) {
                            self.cpu_offset = 0;
                            self.phase = .metadata_allocate;
                        }
                    }
                },
                .metadata_allocate => {
                    if (e.blocks.items.len == e.blocks.capacity) {
                        self.new_blocks = try allocatePrivate(OwnedEncodedBlockMeta, a, try metadataCapacity(e));
                        self.phase = .metadata_copy;
                    } else self.phase = .validate;
                },
                .metadata_copy => {
                    const n = @min(e.blocks.items.len - self.copied_blocks, 64);
                    @memcpy(self.new_blocks.?[self.copied_blocks..][0..n], e.blocks.items[self.copied_blocks..][0..n]);
                    self.copied_blocks += n;
                    if (self.copied_blocks == e.blocks.items.len) self.phase = .validate;
                },
                .validate => {
                    const bytes = self.payload();
                    const physical_end = try checkedAddUsize(e.physical_output_len, bytes.len);
                    _ = try checkedU32(physical_end - e.entry_data_start);
                    self.completed_metadata_bytes = try checkedAddUsize(e.completed_blocks_metadata_bytes, try encodedBlockMetadataLen(
                        e.block_smallest_namespace_name,
                        e.block_smallest_key,
                        self.largest_namespace,
                        self.largest_key.?,
                        bloomEncodedLenForBytes(self.filters[0].?.bytes.len),
                        bloomEncodedLenForBytes(self.filters[1].?.bytes.len),
                    ));
                    self.meta = .{
                        .relative_offset = e.block_start.?,
                        .len = try checkedU32(e.block_bytes.items.len),
                        .physical_relative_offset = try checkedU32(e.physical_output_len - e.entry_data_start),
                        .physical_len = try checkedU32(bytes.len),
                        .compression = self.compression,
                        .checksum = self.crc.final(),
                        .first_entry_index = try checkedU32(e.block_first_entry_index),
                        .entry_count = try checkedU32(e.block_entry_count),
                        .smallest_namespace_name = e.block_smallest_namespace_name,
                        .smallest_key = e.block_smallest_key,
                        .largest_namespace_name = self.largest_namespace,
                        .largest_key = self.largest_key.?,
                        .filter = undefined,
                        .hash_slots = &.{},
                    };
                    self.phase = .ready;
                },
                .ready, .complete => return true,
                .retired => unreachable,
            }
            return self.phase == .ready or self.phase == .complete;
        }

        /// Commands stay byte-identical until acknowledge(). No CPU stage or
        /// allocation follows the first output command. Physical failures and
        /// accepted IO belong to the owning file writer, which must outlive all
        /// borrows of these bytes even when the observer is rejected.
        pub fn next(self: *Block, original: ?read_admission.Port) !?Command {
            while (!(try self.step(original))) {}
            if (self.phase == .complete) return null;
            if (self.pending_len == 0) self.pending_len = @min(self.payload().len - self.output_offset, 16 * 1024);
            return .{ .offset = self.encoder.physical_output_len + self.output_offset, .bytes = self.payload()[self.output_offset..][0..self.pending_len] };
        }

        pub fn writeNextWithAdmission(self: *Block, writer: anytype, original: read_admission.Port) !bool {
            const command = (try self.next(original)) orelse return false;
            self.io_attempted = true;
            try writer.writeWithAdmission(command.offset, command.bytes, original);
            self.acknowledge();
            return true;
        }

        /// Infallible receipt after known physical completion. The final ack
        /// adopts all prepared metadata before any subsequent checkpoint.
        pub fn acknowledge(self: *Block) void {
            std.debug.assert(!self.retiring and self.phase == .ready and self.pending_len != 0);
            self.output_offset += self.pending_len;
            self.pending_len = 0;
            if (self.output_offset != self.payload().len) return;
            const e = self.encoder;
            if (self.new_blocks) |new| {
                self.old_blocks = e.blocks.allocatedSlice();
                const count = e.blocks.items.len;
                e.blocks = .fromOwnedSlice(new);
                e.blocks.items.len = count;
                self.new_blocks = null;
            }
            self.meta.filter = self.filters[0].?.finish();
            self.filters[0] = null;
            self.meta.prefix_filter = self.filters[1].?.finish();
            self.filters[1] = null;
            e.blocks.appendAssumeCapacity(self.meta);
            self.largest_namespace = null;
            self.largest_key = null;
            e.block_smallest_namespace_name = null;
            e.block_smallest_key = &.{};
            e.completed_blocks_metadata_bytes = self.completed_metadata_bytes;
            e.blocks_owned_heap_bytes +|= ownedEncodedBlockMetaHeapBytes(self.meta);
            e.physical_output_len += self.output_offset;
            if (e.snappy_reprobe_countdown > 0) e.snappy_reprobe_countdown -= 1;
            if (self.snappy_attempted) {
                e.snappy_attempted_blocks += 1;
                if (self.compression == .snappy or self.compression == .prefix_snappy) {
                    e.snappy_rejection_streak = 0;
                    e.snappy_reprobe_countdown = 0;
                } else {
                    e.snappy_rejection_streak +|= 1;
                    if (e.snappy_rejection_streak >= snappy_rejection_streak_limit) e.snappy_reprobe_countdown = snappy_reprobe_interval - 1;
                }
            }
            // Receipt publication is scalar-only, including in Debug mode.
            e.block_bytes.items.len = 0;
            e.block_hashes.items.len = 0;
            e.block_prefix_hashes.items.len = 0;
            e.clearBlockLargest();
            e.block_start = null;
            e.block_entry_count = 0;
            self.published = true;
            self.phase = .complete;
        }

        // std.Allocator.free poisons the entire allocation in safety builds.
        // Poison under bounded original checkpoints, then call the allocator's
        // ordinary raw release so its accounting still runs exactly once.
        fn retireMemory(self: *Block, comptime T: type, memory: []T, original: ?read_admission.Port) !void {
            return retirePrivate(T, self.encoder.allocator, memory, &self.retire_offset, original);
        }

        /// Poison at most 16 KiB per original checkpoint, then release each
        /// allocation once. Abandonment after attempted IO or any ack makes the
        /// encoder terminal. Accepted file work must be joined before retiring.
        pub fn retireWithAdmission(self: *Block, original: ?read_admission.Port) !void {
            if (self.phase == .retired) return;
            self.retiring = true;
            if (!self.published and (self.io_attempted or self.output_offset != 0)) self.encoder.finished = true;
            while (self.retire_slot < 9) {
                switch (self.retire_slot) {
                    0 => {
                        try self.retireMemory(u8, self.prefix orelse &.{}, original);
                        self.prefix = null;
                    },
                    1 => {
                        try self.retireMemory(u32, self.workspace orelse &.{}, original);
                        self.workspace = null;
                    },
                    2, 3 => {
                        const i = self.retire_slot - 2;
                        try self.retireMemory(u8, if (self.filters[i]) |b| b.bytes else &.{}, original);
                        self.filters[i] = null;
                    },
                    4 => {
                        try self.retireMemory(u8, self.largest_namespace orelse &.{}, original);
                        self.largest_namespace = null;
                    },
                    5 => {
                        try self.retireMemory(u8, self.largest_key orelse &.{}, original);
                        self.largest_key = null;
                    },
                    6 => {
                        try self.retireMemory(OwnedEncodedBlockMeta, self.new_blocks orelse &.{}, original);
                        self.new_blocks = null;
                    },
                    7 => {
                        try self.retireMemory(OwnedEncodedBlockMeta, self.old_blocks orelse &.{}, original);
                        self.old_blocks = null;
                    },
                    8 => {
                        try self.retireMemory(u8, self.old_compression orelse &.{}, original);
                        self.old_compression = null;
                    },
                    else => unreachable,
                }
                self.retire_slot += 1;
            }
            self.encoder.block_active = false;
            self.phase = .retired;
        }

        pub fn deinit(self: *Block) void {
            self.retireWithAdmission(null) catch unreachable;
        }
    };

    fn flushBlock(self: *StreamingEncoder) !void {
        if (self.block_entry_count == 0) return;
        var block = try Block.init(self);
        defer block.deinit();
        while (try block.next(null)) |command| {
            std.debug.assert(self.sink.len() == command.offset);
            block.io_attempted = true;
            self.sink.appendSlice(command.bytes) catch |cause| {
                self.finished = true;
                return cause;
            };
            block.acknowledge();
        }
    }
};

const bloom_encoded_header_len = bloom.magic.len + @sizeOf(u32) * 3 + @sizeOf(u8);

fn bloomEncodedLenForBytes(byte_len: usize) usize {
    return bloom_encoded_header_len +| byte_len;
}

fn bloomEncodedLenForKeyCount(key_count: usize, config: bloom.Config) !usize {
    if (key_count == 0) return bloomEncodedLenForBytes(0);
    const requested_bits = std.math.mul(usize, key_count, config.bits_per_key) catch return error.TableFileTooLarge;
    const raw_bits = @max(config.min_bits, requested_bits);
    const bit_count = std.math.ceilPowerOfTwo(usize, raw_bits) catch raw_bits;
    if (bit_count > std.math.maxInt(u32)) return error.TableFileTooLarge;
    const byte_len = try checkedAddUsize(bit_count, 7) / 8;
    return try checkedAddUsize(bloom_encoded_header_len, byte_len);
}

fn encodedBlockMetadataLen(
    smallest_namespace_name: ?[]const u8,
    smallest_key: []const u8,
    largest_namespace_name: ?[]const u8,
    largest_key: []const u8,
    encoded_filter_len: usize,
    encoded_prefix_filter_len: usize,
) !usize {
    // Largest bound: six u32 fields. Bloom and prefix Bloom: a u32 length
    // each. Empty hash slots: one u32 length. Physical block metadata: four
    // u32 fields. Smallest bound: two u32 fields.
    var total: usize = 15 * @sizeOf(u32);
    if (smallest_namespace_name) |name| total = try checkedAddUsize(total, name.len);
    total = try checkedAddUsize(total, smallest_key.len);
    if (largest_namespace_name) |name| total = try checkedAddUsize(total, name.len);
    total = try checkedAddUsize(total, largest_key.len);
    total = try checkedAddUsize(total, encoded_filter_len);
    total = try checkedAddUsize(total, encoded_prefix_filter_len);
    return total;
}

fn capacityBytes(comptime T: type, capacity: usize) u64 {
    return @as(u64, @intCast(capacity)) *| @as(u64, @intCast(@sizeOf(T)));
}

fn ownedEncodedBlockMetaHeapBytes(block: OwnedEncodedBlockMeta) u64 {
    var bytes: u64 = 0;
    if (block.smallest_namespace_name) |name| bytes +|= name.len;
    bytes +|= block.smallest_key.len;
    if (block.largest_namespace_name) |name| bytes +|= name.len;
    bytes +|= block.largest_key.len;
    bytes +|= block.filter.bytes.len;
    if (block.prefix_filter) |filter| bytes +|= filter.bytes.len;
    bytes +|= @as(u64, @intCast(block.hash_slots.len)) *| @sizeOf(u32);
    return bytes;
}

fn summarizeCompressionStats(logical_entry_data_len: usize, physical_entry_data_len: usize, blocks: []const EncodedBlockMeta) CompressionStats {
    var stats = CompressionStats{
        .logical_entry_bytes = @intCast(logical_entry_data_len),
        .physical_entry_bytes = @intCast(physical_entry_data_len),
    };
    for (blocks) |block| {
        switch (block.compression) {
            .none => stats.raw_blocks += 1,
            .snappy, .prefix, .prefix_snappy => {
                stats.compressed_blocks += 1;
                stats.compression_codec_mask |= blockCompressionCodecMask(block.compression);
            },
        }
    }
    return stats;
}

fn appendEntryBytesToList(allocator: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), entry: Entry) !void {
    try out.append(allocator, @intFromBool(entry.tombstone));
    try appendU32(allocator, out, if (entry.namespace_name) |name| try checkedU32(name.len) else 0);
    try appendU32(allocator, out, try checkedU32(entry.key.len));
    try appendU32(allocator, out, try checkedU32(entry.value.len));
    if (entry.namespace_name) |name| try out.appendSlice(allocator, name);
    try out.appendSlice(allocator, entry.key);
    try out.appendSlice(allocator, entry.value);
}

fn flushEncodedBlock(
    allocator: std.mem.Allocator,
    sink: *TableSink,
    blocks: *std.ArrayListUnmanaged(EncodedBlockMeta),
    entries: []const Entry,
    block_bytes: *std.ArrayListUnmanaged(u8),
    entry_data_start: usize,
    block_start: u32,
    block_first_entry_index: usize,
    block_entry_count: usize,
    block_smallest_namespace_name: ?[]const u8,
    block_smallest_key: []const u8,
    block_largest_namespace_name: ?[]const u8,
    block_largest_key: []const u8,
    compression_policy: CompressionPolicy,
    prefix_extractor: PrefixExtractor,
    compression_bytes: *std.ArrayListUnmanaged(u8),
) !void {
    if (block_entry_count == 0) return;

    var encoded_payload = try encodeBlockPayloadAlloc(allocator, block_bytes.items, compression_policy, true, compression_bytes);
    defer encoded_payload.deinit(allocator);

    const physical_relative_offset = try checkedU32(sink.len() - entry_data_start);
    try sink.appendSlice(encoded_payload.payload);

    var block_filter = try buildFilterAlloc(allocator, entries[block_first_entry_index .. block_first_entry_index + block_entry_count], block_filter_config);
    errdefer block_filter.deinit(allocator);
    var block_prefix_filter = try buildPrefixFilterAlloc(allocator, entries[block_first_entry_index .. block_first_entry_index + block_entry_count], prefix_extractor, block_filter_config);
    errdefer block_prefix_filter.deinit(allocator);
    const hash_slots = try allocator.alloc(u32, 0);
    errdefer allocator.free(hash_slots);
    try blocks.append(allocator, .{
        .relative_offset = block_start,
        .len = try checkedU32(block_bytes.items.len),
        .physical_relative_offset = physical_relative_offset,
        .physical_len = try checkedU32(encoded_payload.payload.len),
        .compression = encoded_payload.compression,
        .checksum = Crc32.hash(encoded_payload.payload),
        .first_entry_index = try checkedU32(block_first_entry_index),
        .entry_count = try checkedU32(block_entry_count),
        .smallest_namespace_name = block_smallest_namespace_name,
        .smallest_key = block_smallest_key,
        .largest_namespace_name = block_largest_namespace_name,
        .largest_key = block_largest_key,
        .filter = block_filter,
        .prefix_filter = block_prefix_filter,
        .hash_slots = hash_slots,
    });
    block_bytes.clearRetainingCapacity();
}

fn tableEntryEncodedLen(entry: Entry) !usize {
    var total: usize = 1 + @sizeOf(u32) * 3;
    if (entry.namespace_name) |name| total = checkedAddUsize(total, name.len) catch return error.TableFileTooLarge;
    total = checkedAddUsize(total, entry.key.len) catch return error.TableFileTooLarge;
    total = checkedAddUsize(total, entry.value.len) catch return error.TableFileTooLarge;
    return total;
}

fn checkedAddUsize(lhs: usize, rhs: usize) !usize {
    return std.math.add(usize, lhs, rhs) catch error.TableFileTooLarge;
}

fn checkedU32(value: usize) !u32 {
    if (value > std.math.maxInt(u32)) return error.TableFileTooLarge;
    return @intCast(value);
}

fn checkedU16(value: usize) !u16 {
    if (value > std.math.maxInt(u16)) return error.TableFileTooLarge;
    return @intCast(value);
}

pub fn buildFilterAlloc(
    allocator: std.mem.Allocator,
    entries: []const Entry,
    config: bloom.Config,
) !bloom.OwnedFilter {
    var builder = try bloom.Builder.init(allocator, entries.len, config);
    errdefer builder.deinit();
    for (entries) |entry| {
        const hashes = entryHashes(entry.namespace_name, entry.key);
        builder.addHashes(hashes[0], hashes[1]);
    }
    return builder.finish();
}

pub fn buildPrefixFilterAlloc(
    allocator: std.mem.Allocator,
    entries: []const Entry,
    extractor: PrefixExtractor,
    config: bloom.Config,
) !bloom.OwnedFilter {
    var prefix_hashes = std.ArrayListUnmanaged([2]u64).empty;
    defer prefix_hashes.deinit(allocator);
    for (entries) |entry| {
        if (entryPrefixHashes(extractor, entry.namespace_name, entry.key)) |hashes| {
            try appendUniqueConsecutiveHash(allocator, &prefix_hashes, hashes);
        }
    }
    return buildFilterFromHashesAlloc(allocator, prefix_hashes.items, config);
}

fn appendUniqueConsecutiveHash(
    allocator: std.mem.Allocator,
    hashes: *std.ArrayListUnmanaged([2]u64),
    value: [2]u64,
) !void {
    if (hashes.items.len > 0 and std.meta.eql(hashes.items[hashes.items.len - 1], value)) return;
    try hashes.append(allocator, value);
}

fn buildFilterFromHashesAlloc(
    allocator: std.mem.Allocator,
    hashes: []const [2]u64,
    config: bloom.Config,
) !bloom.OwnedFilter {
    var builder = try bloom.Builder.init(allocator, hashes.len, config);
    errdefer builder.deinit();
    for (hashes) |entry_hashes| builder.addHashes(entry_hashes[0], entry_hashes[1]);
    return builder.finish();
}

pub fn extractKeyPrefix(extractor: PrefixExtractor, key: []const u8) ?[]const u8 {
    return switch (extractor) {
        .none => null,
        .first_separator => firstSeparatorPrefix(key),
    };
}

fn firstSeparatorPrefix(key: []const u8) ?[]const u8 {
    for (key, 0..) |byte, i| {
        if (byte == ':' or byte == '/') return key[0 .. i + 1];
    }
    return null;
}

pub fn upperBoundWithinPrefix(prefix: []const u8, upper: []const u8) bool {
    if (std.mem.startsWith(u8, upper, prefix)) return true;
    var pivot = prefix.len;
    while (pivot > 0) {
        pivot -= 1;
        if (prefix[pivot] == 0xff) continue;
        return compareBytesToPrefixSuccessor(upper, prefix, pivot) != .gt;
    }
    return false;
}

fn compareBytesToPrefixSuccessor(bytes: []const u8, prefix: []const u8, pivot: usize) std.math.Order {
    var i: usize = 0;
    while (i < bytes.len and i <= pivot) : (i += 1) {
        const rhs = if (i == pivot) prefix[i] + 1 else prefix[i];
        if (bytes[i] < rhs) return .lt;
        if (bytes[i] > rhs) return .gt;
    }
    if (bytes.len <= pivot) return .lt;
    return .eq;
}

pub fn decodeAlloc(allocator: std.mem.Allocator, raw: []const u8) !Decoded {
    var borrowed = transfer: {
        const owned_raw = try allocator.dupe(u8, raw);
        errdefer allocator.free(owned_raw);
        break :transfer try decodeBorrowedOwnedAlloc(allocator, owned_raw);
    };
    defer borrowed.deinit(allocator);

    const entries = try allocator.alloc(OwnedEntry, borrowed.entryCount());
    errdefer allocator.free(entries);

    var initialized: usize = 0;
    errdefer for (entries[0..initialized]) |*entry| entry.deinit(allocator);

    while (initialized < entries.len) : (initialized += 1) {
        const entry = try borrowed.entryAt(initialized);
        const namespace = if (entry.namespace_name) |name| try allocator.dupe(u8, name) else null;
        errdefer if (namespace) |name| allocator.free(name);
        const key = try allocator.dupe(u8, entry.key);
        errdefer allocator.free(key);
        const value = try allocator.dupe(u8, entry.value);
        entries[initialized] = .{ .namespace_name = namespace, .key = key, .value = value, .tombstone = entry.tombstone };
    }

    return .{
        .entries = entries,
        .filter = try borrowed.filter.clone(allocator),
    };
}

pub fn decodeBorrowedOwnedAlloc(allocator: std.mem.Allocator, raw: []u8) !BorrowedDecoded {
    return decodeBorrowedOwnedAllocWithAdmission(allocator, raw, null);
}

pub fn decodeBorrowedOwnedAllocWithAdmission(allocator: std.mem.Allocator, raw: []u8, control: ?read_admission.Port) !BorrowedDecoded {
    var index = try decodeIndexAllocWithAdmission(allocator, raw, control);
    errdefer index.deinit(allocator);
    if (index.prefix_filter) |*filter| {
        filter.deinit(allocator);
        index.prefix_filter = null;
    }

    if (indexHasCompressedBlocks(&index)) {
        const logical_raw = try materializeLogicalEntryDataRawAlloc(allocator, raw, &index, control);
        allocator.free(raw);
        return .{
            .raw = logical_raw,
            .owns_raw = true,
            .entry_offsets = index.entry_offsets,
            .owns_entry_offsets = true,
            .block_entry_offsets = index.block_entry_offsets,
            .owns_block_entry_offsets = true,
            .entry_data_start = index.entry_data_start,
            .filter = index.borrowFilter(),
            .owned_filter = index.filter,
            .blocks = index.blocks,
            .owns_blocks = true,
        };
    }

    return .{
        .raw = raw,
        .owns_raw = true,
        .entry_offsets = index.entry_offsets,
        .owns_entry_offsets = true,
        .block_entry_offsets = index.block_entry_offsets,
        .owns_block_entry_offsets = true,
        .entry_data_start = index.entry_data_start,
        .filter = index.borrowFilter(),
        .owned_filter = index.filter,
        .blocks = index.blocks,
        .owns_blocks = true,
    };
}

pub fn indexHasCompressedBlocks(index: *const TableIndex) bool {
    for (index.blocks) |block| {
        if (block.compression != .none) return true;
    }
    return false;
}

const EncodedBlockPayload = struct {
    payload: []const u8,
    compression: BlockCompression,
    owned_prefix: ?[]u8 = null,
    snappy_attempted: bool = false,

    pub fn deinit(self: *EncodedBlockPayload, allocator: std.mem.Allocator) void {
        if (self.owned_prefix) |bytes| allocator.free(bytes);
        self.* = undefined;
    }
};

fn encodeBlockPayloadAlloc(
    allocator: std.mem.Allocator,
    block_bytes: []const u8,
    compression_policy: CompressionPolicy,
    attempt_snappy: bool,
    compression_bytes: *std.ArrayListUnmanaged(u8),
) !EncodedBlockPayload {
    var payload: EncodedBlockPayload = .{
        .payload = block_bytes,
        .compression = .none,
    };

    if (compression_policy != .none) {
        const prefix_payload = try encodePrefixCompressedBlockAlloc(allocator, block_bytes);
        if (prefix_payload.len < block_bytes.len) {
            payload = .{
                .payload = prefix_payload,
                .compression = .prefix,
                .owned_prefix = prefix_payload,
            };
        } else {
            allocator.free(prefix_payload);
        }
    }

    if (compression_policy == .snappy_adaptive and attempt_snappy and payload.payload.len >= min_compress_block_bytes) {
        payload.snappy_attempted = true;
        const compressed = try snappy.encodeInto(allocator, compression_bytes, payload.payload);
        if (compressed.len < payload.payload.len - (payload.payload.len / compression_savings_denominator)) {
            payload.payload = compressed;
            payload.compression = switch (payload.compression) {
                .none => .snappy,
                .prefix => .prefix_snappy,
                .snappy, .prefix_snappy => unreachable,
            };
        }
    }

    return payload;
}

/// Pure CPU prefix encoder. The caller retains immutable raw entries and a
/// private output allocation of outputBound() bytes until done. All state is
/// scalar, so moving this cursor never invalidates its own borrows. No step
/// allocates, calls a sink, or publishes partially encoded output.
pub const PrefixEncodeCursor = struct {
    pub const Progress = struct { done: bool, work: usize, output_bytes: usize };
    const Phase = enum { entry, restart, shared, header, namespace, key, value, trailer, restarts, done, failed };
    input_len: usize,
    phase: Phase = .entry,
    input_offset: usize = 0,
    output_offset: usize = 24,
    entry_count: usize = 0,
    restart_count: usize = 0,
    restart_base: usize,
    previous_key_offset: usize = 0,
    previous_key_len: usize = 0,
    namespace_offset: usize = 0,
    namespace_len: usize = 0,
    key_offset: usize = 0,
    key_len: usize = 0,
    value_offset: usize = 0,
    value_len: usize = 0,
    shared_len: usize = 0,
    copied: usize = 0,
    header: [24]u8 = undefined,
    header_offset: usize = 0,
    restart_copied: usize = 0,
    restart_header: [4]u8 = undefined,
    restart_header_offset: usize = 0,

    /// Raw rows use a 13-byte header; prefix rows add four bytes per
    /// entry and one four-byte restart per sixteen entries. This includes
    /// temporary tail storage used for restarts before their final placement.
    pub fn outputBound(input_len: usize) !usize {
        _ = try checkedU32(input_len);
        const entries = input_len / 13;
        const base = try checkedAddUsize(24, try checkedAddUsize(input_len, try std.math.mul(usize, entries, 4)));
        return checkedAddUsize(base, try std.math.mul(usize, (entries + 15) / 16, 4));
    }

    pub fn init(input_len: usize) !PrefixEncodeCursor {
        _ = try outputBound(input_len);
        return .{ .input_len = input_len, .restart_base = 24 + input_len + (input_len / 13) * 4 };
    }

    fn put(out: []u8, offset: usize, value: usize) !void {
        std.mem.writeInt(u32, out[offset..][0..4], try checkedU32(value), .little);
    }

    /// A work unit parses one fixed row header, compares one shared-key
    /// byte, writes/copies one byte, or records one fixed-width restart.
    /// At most quantum units and quantum output bytes are touched per call.
    pub fn step(self: *PrefixEncodeCursor, src: []const u8, dst: []u8, quantum: usize) !Progress {
        if (quantum == 0 or src.len != self.input_len or dst.len < try outputBound(self.input_len)) return error.InvalidArgument;
        return self.advance(src, dst, quantum) catch |err| {
            self.phase = .failed;
            return err;
        };
    }

    fn advance(self: *PrefixEncodeCursor, src: []const u8, dst: []u8, quantum: usize) !Progress {
        var left = quantum;
        var touched: usize = 0;
        while (left != 0) {
            switch (self.phase) {
                .failed => return error.InvalidTableFile,
                .done => break,
                .entry => {
                    if (self.input_offset == src.len) {
                        @memcpy(self.header[0..8], prefix_block_magic);
                        try put(&self.header, 8, self.entry_count);
                        try put(&self.header, 12, prefix_restart_interval);
                        try put(&self.header, 16, self.restart_count);
                        try put(&self.header, 20, self.output_offset - 24);
                        self.header_offset = 0;
                        self.phase = .trailer;
                        left -= 1;
                        continue;
                    }
                    const e = try parseEntryAt(src, self.input_offset);
                    const row_len = try tableEntryEncodedLen(e);
                    self.namespace_offset = self.input_offset + 13;
                    self.namespace_len = if (e.namespace_name) |name| name.len else 0;
                    self.key_offset = self.namespace_offset + self.namespace_len;
                    self.key_len = e.key.len;
                    self.value_offset = self.key_offset + self.key_len;
                    self.value_len = e.value.len;
                    self.input_offset += row_len;
                    self.shared_len = 0;
                    self.header[0] = if (e.tombstone) 1 else 0;
                    if (self.entry_count % 16 == 0) {
                        try put(&self.restart_header, 0, self.output_offset - 24);
                        self.restart_header_offset = 0;
                        self.phase = .restart;
                    } else self.phase = .shared;
                    left -= 1;
                    self.header_offset = 0;
                },
                .restart => {
                    const n = @min(4 - self.restart_header_offset, @min(left, quantum - touched));
                    if (n == 0) break;
                    @memcpy(dst[self.restart_base + self.restart_count * 4 + self.restart_header_offset ..][0..n], self.restart_header[self.restart_header_offset..][0..n]);
                    self.restart_header_offset += n;
                    left -= n;
                    touched += n;
                    if (self.restart_header_offset == 4) {
                        self.restart_count += 1;
                        self.phase = .header;
                    }
                },
                .shared => {
                    const limit = @min(self.previous_key_len, self.key_len);
                    while (left != 0 and self.shared_len < limit) {
                        left -= 1;
                        if (src[self.previous_key_offset + self.shared_len] != src[self.key_offset + self.shared_len]) break;
                        self.shared_len += 1;
                    } else {
                        if (self.shared_len != limit) continue;
                    }
                    self.phase = .header;
                },
                .header => {
                    if (self.header_offset == 0) {
                        try put(&self.header, 1, self.namespace_len);
                        try put(&self.header, 5, self.shared_len);
                        try put(&self.header, 9, self.key_len - self.shared_len);
                        try put(&self.header, 13, self.value_len);
                    }
                    const n = @min(17 - self.header_offset, @min(left, quantum - touched));
                    if (n == 0) break;
                    @memcpy(dst[self.output_offset..][0..n], self.header[self.header_offset..][0..n]);
                    self.header_offset += n;
                    self.output_offset += n;
                    touched += n;
                    left -= n;
                    if (self.header_offset == 17) {
                        self.phase = .namespace;
                        self.copied = 0;
                    }
                },
                .namespace, .key, .value => {
                    const start = switch (self.phase) {
                        .namespace => self.namespace_offset,
                        .key => self.key_offset + self.shared_len,
                        else => self.value_offset,
                    };
                    const len = switch (self.phase) {
                        .namespace => self.namespace_len,
                        .key => self.key_len - self.shared_len,
                        else => self.value_len,
                    };
                    const n = @min(len - self.copied, @min(left, quantum - touched));
                    if (n == 0 and self.copied != len) break;
                    @memcpy(dst[self.output_offset..][0..n], src[start + self.copied ..][0..n]);
                    self.output_offset += n;
                    self.copied += n;
                    left -= n;
                    touched += n;
                    if (self.copied == len) {
                        self.copied = 0;
                        self.phase = switch (self.phase) {
                            .namespace => .key,
                            .key => .value,
                            else => .entry,
                        };
                        if (self.phase == .entry) {
                            self.previous_key_offset = self.key_offset;
                            self.previous_key_len = self.key_len;
                            self.entry_count += 1;
                        }
                    }
                },
                .trailer => {
                    const n = @min(24 - self.header_offset, @min(left, quantum - touched));
                    if (n == 0) break;
                    @memcpy(dst[self.header_offset..][0..n], self.header[self.header_offset..][0..n]);
                    self.header_offset += n;
                    left -= n;
                    touched += n;
                    if (self.header_offset == 24) self.phase = .restarts;
                },
                .restarts => {
                    const n = @min(self.restart_count * 4 - self.restart_copied, @min(left, quantum - touched));
                    if (n == 0 and self.restart_copied != self.restart_count * 4) break;
                    // Destination is at/before source; forward copies remain
                    // correct across overlap and arbitrary step boundaries.
                    std.mem.copyForwards(u8, dst[self.output_offset + self.restart_copied ..][0..n], dst[self.restart_base + self.restart_copied ..][0..n]);
                    self.restart_copied += n;
                    left -= n;
                    touched += n;
                    if (self.restart_copied == self.restart_count * 4) {
                        self.output_offset += self.restart_copied;
                        self.phase = .done;
                    }
                },
            }
        }
        return .{ .done = self.phase == .done, .work = quantum - left, .output_bytes = touched };
    }
};

fn encodePrefixCompressedBlockAlloc(allocator: std.mem.Allocator, block_bytes: []const u8) ![]u8 {
    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(allocator);
    const bound = try PrefixEncodeCursor.outputBound(block_bytes.len);
    try out.ensureTotalCapacity(allocator, bound);
    var cursor = try PrefixEncodeCursor.init(block_bytes.len);
    while (!(try cursor.step(block_bytes, out.allocatedSlice()[0..bound], 4096)).done) {}
    out.items.len = cursor.output_offset;
    return out.toOwnedSlice(allocator);
}

fn decodePrefixCompressedBlockAlloc(
    allocator: std.mem.Allocator,
    scratch: std.mem.Allocator,
    payload: []const u8,
    expected_len: usize,
) ![]u8 {
    return decodePrefixCompressedBlockAllocWithAdmission(allocator, scratch, payload, expected_len, null);
}

fn appendSliceWithAdmission(allocator: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), bytes: []const u8, control: ?read_admission.Port) !void {
    if (control == null) return out.appendSlice(allocator, bytes);
    var cursor: usize = 0;
    while (cursor < bytes.len) {
        try control.?.checkpoint();
        const end = cursor + @min(bytes.len - cursor, 64 * 1024);
        try out.appendSlice(allocator, bytes[cursor..end]);
        cursor = end;
    }
}

fn copyWithAdmission(dst: []u8, src: []const u8, control: ?read_admission.Port) !void {
    if (control == null) {
        @memcpy(dst, src);
        return;
    }
    var cursor: usize = 0;
    while (cursor < src.len) {
        try control.?.checkpoint();
        const end = cursor + @min(src.len - cursor, 64 * 1024);
        @memcpy(dst[cursor..end], src[cursor..end]);
        cursor = end;
    }
}

fn appendEntryBytesWithAdmission(allocator: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), entry: Entry, control: ?read_admission.Port) !void {
    if (control == null) return appendEntryBytesToList(allocator, out, entry);
    try control.?.checkpoint();
    try out.append(allocator, @intFromBool(entry.tombstone));
    try appendU32(allocator, out, if (entry.namespace_name) |name| try checkedU32(name.len) else 0);
    try appendU32(allocator, out, try checkedU32(entry.key.len));
    try appendU32(allocator, out, try checkedU32(entry.value.len));
    if (entry.namespace_name) |name| try appendSliceWithAdmission(allocator, out, name, control);
    try appendSliceWithAdmission(allocator, out, entry.key, control);
    try appendSliceWithAdmission(allocator, out, entry.value, control);
}

fn decodePrefixCompressedBlockAllocWithAdmission(
    allocator: std.mem.Allocator,
    scratch: std.mem.Allocator,
    payload: []const u8,
    expected_len: usize,
    control: ?read_admission.Port,
) ![]u8 {
    if (control) |budget| try budget.checkpoint();
    const view = try parsePrefixBlockPayload(payload);

    // The final buffer never moves. Each key shares its prefix directly from
    // the preceding output key instead of maintaining two growing key arrays.
    _ = scratch;
    var previous_key: []const u8 = &.{};
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);
    try out.ensureTotalCapacityPrecise(allocator, expected_len);

    var entries_cursor: usize = 0;
    for (0..view.entry_count) |entry_index| {
        if (control) |budget| try budget.checkpoint();
        const restart = entry_index % view.restart_interval == 0;
        if (restart) {
            const expected_restart = try view.restartOffset(entry_index / view.restart_interval);
            if (expected_restart != entries_cursor) return error.InvalidTableFile;
        }
        const tombstone = try readByte(view.encoded_entries, &entries_cursor);
        if (tombstone > 1) return error.InvalidTableFile;
        const namespace_len: usize = try readU32(view.encoded_entries, &entries_cursor);
        const shared_len: usize = try readU32(view.encoded_entries, &entries_cursor);
        const suffix_len: usize = try readU32(view.encoded_entries, &entries_cursor);
        const value_len: usize = try readU32(view.encoded_entries, &entries_cursor);
        if (shared_len > previous_key.len or (restart and shared_len != 0)) return error.InvalidTableFile;
        const key_len = std.math.add(usize, shared_len, suffix_len) catch return error.InvalidTableFile;
        if (key_len > std.math.maxInt(u32)) return error.InvalidTableFile;
        var entry_len = std.math.add(usize, 13, namespace_len) catch return error.InvalidTableFile;
        entry_len = std.math.add(usize, entry_len, key_len) catch return error.InvalidTableFile;
        entry_len = std.math.add(usize, entry_len, value_len) catch return error.InvalidTableFile;
        if (entry_len > expected_len -| out.items.len) return error.InvalidTableFile;
        const namespace = try readSlice(view.encoded_entries, &entries_cursor, namespace_len);
        const suffix = try readSlice(view.encoded_entries, &entries_cursor, suffix_len);
        const value = try readSlice(view.encoded_entries, &entries_cursor, value_len);
        out.appendAssumeCapacity(tombstone);
        try appendU32(allocator, &out, @intCast(namespace_len));
        try appendU32(allocator, &out, @intCast(key_len));
        try appendU32(allocator, &out, @intCast(value_len));
        try appendSliceWithAdmission(allocator, &out, namespace, control);
        const key_start = out.items.len;
        try appendSliceWithAdmission(allocator, &out, previous_key[0..shared_len], control);
        try appendSliceWithAdmission(allocator, &out, suffix, control);
        previous_key = out.items[key_start..];
        try appendSliceWithAdmission(allocator, &out, value, control);
    }
    if (entries_cursor != view.encoded_entries.len) return error.InvalidTableFile;
    if (out.items.len != expected_len) return error.InvalidTableFile;
    if (control) |budget| try budget.checkpoint();
    return out.toOwnedSliceAssert();
}

/// Movable prefix-block expansion. Every call reads or writes at most quantum
/// bytes and advances at most one fixed metadata/copy phase. All input/output
/// spans are caller-owned, immutable/stable for this operation, and borrowed
/// only during step. Restart offsets and preceding keys are scalar positions.
pub const PrefixDecodeCursor = struct {
    pub const Status = enum { pending, needs_output, done };
    pub const Progress = struct { status: Status, input_bytes: usize = 0, output_bytes: usize = 0 };
    const Phase = enum { header, output, restart, entry, raw_header, namespace, shared, suffix, value, finish, done, failed };
    input_len: usize,
    output_len: usize,
    phase: Phase = .header,
    failure: ?anyerror = null,
    header: [24]u8 = undefined,
    entry: [17]u8 = undefined,
    raw_header: [13]u8 = undefined,
    restart: [4]u8 = undefined,
    partial: usize = 0,
    input_offset: usize = 24,
    output_offset: usize = 0,
    entries_end: usize = 0,
    entry_count: usize = 0,
    restart_interval: usize = 1,
    entry_index: usize = 0,
    namespace_len: usize = 0,
    shared_len: usize = 0,
    suffix_len: usize = 0,
    value_len: usize = 0,
    previous_key_offset: usize = 0,
    previous_key_len: usize = 0,
    key_offset: usize = 0,

    pub fn init(input_len: usize, output_len: usize) @This() {
        return .{ .input_len = input_len, .output_len = output_len };
    }
    pub fn step(self: *@This(), input: []const u8, output: ?[]u8, quantum: usize) !Progress {
        if (quantum == 0 or quantum > 64 * 1024 or input.len != self.input_len) return error.InvalidArgument;
        if (output) |bytes| if (bytes.len != self.output_len) return error.InvalidArgument;
        return self.advance(input, output, quantum) catch |err| {
            self.failure = err;
            self.phase = .failed;
            return err;
        };
    }
    fn advance(self: *@This(), input: []const u8, output: ?[]u8, quantum: usize) !Progress {
        switch (self.phase) {
            .failed => return self.failure.?,
            .done => return .{ .status = .done },
            .header => {
                if (input.len < self.header.len) return error.InvalidTableFile;
                const take = @min(quantum, self.header.len - self.partial);
                @memcpy(self.header[self.partial..][0..take], input[self.partial..][0..take]);
                self.partial += take;
                if (self.partial == self.header.len) {
                    if (!std.mem.eql(u8, self.header[0..8], prefix_block_magic)) return error.InvalidTableFile;
                    self.entry_count = std.mem.readInt(u32, self.header[8..12], .little);
                    self.restart_interval = std.mem.readInt(u32, self.header[12..16], .little);
                    if (self.restart_interval == 0) return error.InvalidTableFile;
                    const restarts: usize = std.mem.readInt(u32, self.header[16..20], .little);
                    const entries_len: usize = std.mem.readInt(u32, self.header[20..24], .little);
                    const expected_restarts = if (self.entry_count == 0) 0 else 1 + (self.entry_count - 1) / self.restart_interval;
                    if (restarts != expected_restarts) return error.InvalidTableFile;
                    self.entries_end = std.math.add(usize, self.header.len, entries_len) catch return error.InvalidTableFile;
                    const restart_bytes = std.math.mul(usize, restarts, 4) catch return error.InvalidTableFile;
                    const total = std.math.add(usize, self.entries_end, restart_bytes) catch return error.InvalidTableFile;
                    if (total != input.len) return error.InvalidTableFile;
                    self.partial = 0;
                    self.phase = .output;
                }
                return .{ .status = if (self.phase == .output and output == null) .needs_output else .pending, .input_bytes = take };
            },
            .output => {
                if (output == null) return .{ .status = .needs_output };
                self.phase = if (self.entry_count == 0) .finish else .restart;
                return .{ .status = .pending };
            },
            .restart => {
                const base = self.entries_end + (self.entry_index / self.restart_interval) * 4;
                const take = @min(quantum, self.restart.len - self.partial);
                @memcpy(self.restart[self.partial..][0..take], input[base + self.partial ..][0..take]);
                self.partial += take;
                if (self.partial == self.restart.len) {
                    if (std.mem.readInt(u32, &self.restart, .little) != self.input_offset - self.header.len) return error.InvalidTableFile;
                    self.partial = 0;
                    self.phase = .entry;
                }
                return .{ .status = .pending, .input_bytes = take };
            },
            .entry => {
                if (self.entry.len > self.entries_end -| self.input_offset) return error.InvalidTableFile;
                const take = @min(quantum, self.entry.len - self.partial);
                @memcpy(self.entry[self.partial..][0..take], input[self.input_offset + self.partial ..][0..take]);
                self.partial += take;
                if (self.partial == self.entry.len) {
                    if (self.entry[0] > 1) return error.InvalidTableFile;
                    self.namespace_len = std.mem.readInt(u32, self.entry[1..5], .little);
                    self.shared_len = std.mem.readInt(u32, self.entry[5..9], .little);
                    self.suffix_len = std.mem.readInt(u32, self.entry[9..13], .little);
                    self.value_len = std.mem.readInt(u32, self.entry[13..17], .little);
                    if (self.shared_len > self.previous_key_len or (self.entry_index % self.restart_interval == 0 and self.shared_len != 0)) return error.InvalidTableFile;
                    const key_len = std.math.add(usize, self.shared_len, self.suffix_len) catch return error.InvalidTableFile;
                    if (key_len > std.math.maxInt(u32)) return error.InvalidTableFile;
                    var encoded_len = std.math.add(usize, self.namespace_len, self.suffix_len) catch return error.InvalidTableFile;
                    encoded_len = std.math.add(usize, encoded_len, self.value_len) catch return error.InvalidTableFile;
                    self.input_offset += self.entry.len;
                    if (encoded_len > self.entries_end -| self.input_offset) return error.InvalidTableFile;
                    var raw_len = std.math.add(usize, self.raw_header.len, self.namespace_len) catch return error.InvalidTableFile;
                    raw_len = std.math.add(usize, raw_len, key_len) catch return error.InvalidTableFile;
                    raw_len = std.math.add(usize, raw_len, self.value_len) catch return error.InvalidTableFile;
                    if (raw_len > self.output_len -| self.output_offset) return error.InvalidTableFile;
                    self.raw_header[0] = self.entry[0];
                    std.mem.writeInt(u32, self.raw_header[1..5], @intCast(self.namespace_len), .little);
                    std.mem.writeInt(u32, self.raw_header[5..9], @intCast(key_len), .little);
                    std.mem.writeInt(u32, self.raw_header[9..13], @intCast(self.value_len), .little);
                    self.partial = 0;
                    self.phase = .raw_header;
                }
                return .{ .status = .pending, .input_bytes = take };
            },
            .raw_header => {
                const out = output orelse return .{ .status = .needs_output };
                const take = @min(quantum, self.raw_header.len - self.partial);
                @memcpy(out[self.output_offset..][0..take], self.raw_header[self.partial..][0..take]);
                self.output_offset += take;
                self.partial += take;
                if (self.partial == self.raw_header.len) {
                    self.partial = 0;
                    self.phase = .namespace;
                }
                return .{ .status = .pending, .output_bytes = take };
            },
            .namespace, .suffix, .value => {
                const out = output orelse return .{ .status = .needs_output };
                const len = switch (self.phase) {
                    .namespace => self.namespace_len,
                    .suffix => self.suffix_len,
                    .value => self.value_len,
                    else => unreachable,
                };
                const take = @min(quantum, len - self.partial);
                @memcpy(out[self.output_offset..][0..take], input[self.input_offset..][0..take]);
                self.input_offset += take;
                self.output_offset += take;
                self.partial += take;
                if (self.partial == len) {
                    self.partial = 0;
                    switch (self.phase) {
                        .namespace => {
                            self.key_offset = self.output_offset;
                            self.phase = .shared;
                        },
                        .suffix => {
                            self.previous_key_offset = self.key_offset;
                            self.previous_key_len = self.shared_len + self.suffix_len;
                            self.phase = .value;
                        },
                        .value => {
                            self.entry_index += 1;
                            self.phase = if (self.entry_index == self.entry_count) .finish else if (self.entry_index % self.restart_interval == 0) .restart else .entry;
                        },
                        else => unreachable,
                    }
                }
                return .{ .status = .pending, .input_bytes = take, .output_bytes = take };
            },
            .shared => {
                const out = output orelse return .{ .status = .needs_output };
                const take = @min(quantum, self.shared_len - self.partial);
                @memcpy(out[self.output_offset..][0..take], out[self.previous_key_offset + self.partial ..][0..take]);
                self.output_offset += take;
                self.partial += take;
                if (self.partial == self.shared_len) {
                    self.partial = 0;
                    self.phase = .suffix;
                }
                return .{ .status = .pending, .output_bytes = take };
            },
            .finish => {
                if (self.input_offset != self.entries_end or self.output_offset != self.output_len) return error.InvalidTableFile;
                self.phase = .done;
                return .{ .status = .done };
            },
        }
    }
};

const PrefixBlockView = struct {
    entry_count: usize,
    restart_interval: usize,
    restart_count: usize,
    encoded_entries: []const u8,
    restart_bytes: []const u8,

    fn restartOffset(self: PrefixBlockView, index: usize) !usize {
        if (index >= self.restart_count) return error.InvalidTableFile;
        var cursor = index * @sizeOf(u32);
        const offset: usize = @intCast(try readU32(self.restart_bytes, &cursor));
        if (offset > self.encoded_entries.len) return error.InvalidTableFile;
        return offset;
    }
};

fn parsePrefixBlockPayload(payload: []const u8) !PrefixBlockView {
    var cursor: usize = 0;
    if (!std.mem.eql(u8, try readSlice(payload, &cursor, prefix_block_magic.len), prefix_block_magic)) return error.InvalidTableFile;
    const entry_count: usize = @intCast(try readU32(payload, &cursor));
    const restart_interval: usize = @intCast(try readU32(payload, &cursor));
    if (restart_interval == 0) return error.InvalidTableFile;
    const restart_count: usize = @intCast(try readU32(payload, &cursor));
    const encoded_entries_len: usize = @intCast(try readU32(payload, &cursor));
    const encoded_entries = try readSlice(payload, &cursor, encoded_entries_len);
    const restart_bytes_len = std.math.mul(usize, restart_count, @sizeOf(u32)) catch return error.InvalidTableFile;
    const restart_bytes = try readSlice(payload, &cursor, restart_bytes_len);
    if (cursor != payload.len) return error.InvalidTableFile;
    if (entry_count == 0 and restart_count != 0) return error.InvalidTableFile;
    if (entry_count > 0 and restart_count != (1 + (entry_count - 1) / restart_interval)) return error.InvalidTableFile;
    return .{
        .entry_count = entry_count,
        .restart_interval = restart_interval,
        .restart_count = restart_count,
        .encoded_entries = encoded_entries,
        .restart_bytes = restart_bytes,
    };
}

// Reconstruct keys in place: the shared prefix is already in the buffer.
// Growing the allocation preserves it; only the encoded suffix is appended.
fn readPrefixBlockEntryInPlace(
    allocator: std.mem.Allocator,
    encoded_entries: []const u8,
    cursor: *usize,
    key: *std.ArrayListUnmanaged(u8),
    max_key_bytes: usize,
) !Entry {
    return readPrefixBlockEntryInPlaceWithAdmission(allocator, encoded_entries, cursor, key, max_key_bytes, null);
}

fn readPrefixBlockEntryInPlaceWithAdmission(
    allocator: std.mem.Allocator,
    encoded_entries: []const u8,
    cursor: *usize,
    key: *std.ArrayListUnmanaged(u8),
    max_key_bytes: usize,
    control: ?read_admission.Port,
) !Entry {
    if (control) |budget| try budget.checkpoint();
    const tombstone = switch (try readByte(encoded_entries, cursor)) {
        0 => false,
        1 => true,
        else => return error.InvalidTableFile,
    };
    const namespace_len: usize = try readU32(encoded_entries, cursor);
    const shared_len: usize = try readU32(encoded_entries, cursor);
    const suffix_len: usize = try readU32(encoded_entries, cursor);
    const value_len: usize = try readU32(encoded_entries, cursor);
    if (shared_len > key.items.len) return error.InvalidTableFile;
    const key_len = std.math.add(usize, shared_len, suffix_len) catch return error.InvalidTableFile;
    if (key_len > max_key_bytes or key_len > std.math.maxInt(u32)) return error.InvalidTableFile;
    const namespace = try readSlice(encoded_entries, cursor, namespace_len);
    const suffix = try readSlice(encoded_entries, cursor, suffix_len);
    const value = try readSlice(encoded_entries, cursor, value_len);
    if (control) |budget| try budget.checkpoint();
    try key.ensureTotalCapacity(allocator, key_len);
    key.items.len = shared_len;
    try appendSliceWithAdmission(allocator, key, suffix, control);
    return .{ .namespace_name = if (namespace_len > 0) namespace else null, .key = key.items, .value = value, .tombstone = tombstone };
}

/// Restart keys have no shared prefix and can borrow the encoded payload.
fn prefixRestartEntry(view: PrefixBlockView, restart_index: usize) !Entry {
    var cursor = try view.restartOffset(restart_index);
    const tombstone = switch (try readByte(view.encoded_entries, &cursor)) {
        0 => false,
        1 => true,
        else => return error.InvalidTableFile,
    };
    const namespace_len: usize = try readU32(view.encoded_entries, &cursor);
    if (try readU32(view.encoded_entries, &cursor) != 0) return error.InvalidTableFile;
    const key_len: usize = try readU32(view.encoded_entries, &cursor);
    const value_len: usize = try readU32(view.encoded_entries, &cursor);
    const namespace = try readSlice(view.encoded_entries, &cursor, namespace_len);
    const key = try readSlice(view.encoded_entries, &cursor, key_len);
    const value = try readSlice(view.encoded_entries, &cursor, value_len);
    return .{ .namespace_name = if (namespace_len > 0) namespace else null, .key = key, .value = value, .tombstone = tombstone };
}

/// A borrowed prefix-block reader. Payload bytes must remain pinned until
/// deinit; returned keys remain valid only until the next find or deinit.
/// Monotone lookups reuse their current restart interval, while sparse jumps
/// and backwards lookups seek through the restart index instead of replaying
/// the whole block. All key storage uses the caller's scratch allocator.
pub const PrefixPointReader = struct {
    view: PrefixBlockView,
    first_entry_index: usize,
    max_result_bytes: usize,
    key_bytes: std.ArrayListUnmanaged(u8) = .empty,
    current: ?BorrowedDecoded.PositionedEntry = null,
    cursor: usize = 0,
    next_entry_index: usize = 0,
    interval_end_index: usize = 0,
    interval_end_cursor: usize = 0,
    upper_restart: ?Entry = null,

    pub fn init(payload: []const u8, first_entry_index: usize, max_result_bytes: usize) !@This() {
        const view = try parsePrefixBlockPayload(payload);
        if (view.entry_count == 0 and view.encoded_entries.len != 0) return error.InvalidTableFile;
        _ = std.math.add(usize, first_entry_index, view.entry_count) catch return error.InvalidTableFile;
        return .{ .view = view, .first_entry_index = first_entry_index, .max_result_bytes = max_result_bytes };
    }

    pub fn entryCount(self: *const @This()) usize {
        return self.view.entry_count;
    }

    pub fn retainedBytes(self: *const @This()) usize {
        return self.key_bytes.capacity;
    }

    pub fn deinit(self: *@This(), scratch: std.mem.Allocator) void {
        self.key_bytes.deinit(scratch);
        self.* = undefined;
    }

    fn seek(self: *@This(), namespace_name: ?[]const u8, key: []const u8, control: ?read_admission.Port) !void {
        var lo: usize = 0;
        var hi = self.view.restart_count;
        while (lo < hi) {
            if (control) |budget| try budget.checkpoint();
            const mid = lo + (hi - lo) / 2;
            if (compareEntryTo(try prefixRestartEntry(self.view, mid), namespace_name, key) != .gt) lo = mid + 1 else hi = mid;
        }
        const restart = if (lo == 0) 0 else lo - 1;
        self.current = null;
        self.key_bytes.clearRetainingCapacity();
        self.cursor = try self.view.restartOffset(restart);
        self.next_entry_index = restart * self.view.restart_interval;
        self.interval_end_index = self.next_entry_index + @min(self.view.restart_interval, self.view.entry_count - self.next_entry_index);
        self.interval_end_cursor = if (restart + 1 < self.view.restart_count) try self.view.restartOffset(restart + 1) else self.view.encoded_entries.len;
        self.upper_restart = if (restart + 1 < self.view.restart_count) try prefixRestartEntry(self.view, restart + 1) else null;
        if (self.cursor > self.interval_end_cursor) return error.InvalidTableFile;
    }

    pub fn find(self: *@This(), scratch: std.mem.Allocator, namespace_name: ?[]const u8, key: []const u8) !?BorrowedDecoded.PositionedEntry {
        return self.findWithAdmission(scratch, namespace_name, key, null);
    }

    /// Admission is borrowed only for this lookup, including restart search
    /// and bounded key reconstruction. A stopped reader can retry from a restart.
    pub fn findWithAdmission(self: *@This(), scratch: std.mem.Allocator, namespace_name: ?[]const u8, key: []const u8, control: ?read_admission.Port) !?BorrowedDecoded.PositionedEntry {
        // A failed allocation may leave the byte cursor advanced. Retry from
        // the restart index, preserving only the reusable key capacity.
        errdefer self.current = null;
        if (control) |budget| try budget.checkpoint();
        if (self.view.entry_count == 0) return null;
        if (self.current) |current| {
            switch (compareEntryTo(current.entry, namespace_name, key)) {
                .eq => {
                    if (try tableEntryEncodedLen(current.entry) > self.max_result_bytes) return error.InvalidTableFile;
                    if (control) |budget| try budget.checkpoint();
                    return current;
                },
                .gt => try self.seek(namespace_name, key, control),
                .lt => if (self.upper_restart) |upper| {
                    if (compareEntryTo(upper, namespace_name, key) != .gt) try self.seek(namespace_name, key, control);
                },
            }
        } else try self.seek(namespace_name, key, control);
        while (self.next_entry_index < self.interval_end_index) {
            const entry = try readPrefixBlockEntryInPlaceWithAdmission(scratch, self.view.encoded_entries, &self.cursor, &self.key_bytes, self.max_result_bytes, control);
            self.next_entry_index += 1;
            if (self.cursor > self.interval_end_cursor or (self.next_entry_index == self.interval_end_index and self.cursor != self.interval_end_cursor)) return error.InvalidTableFile;
            const current: BorrowedDecoded.PositionedEntry = .{ .index = self.first_entry_index + self.next_entry_index - 1, .entry = entry };
            self.current = current;
            switch (compareEntryTo(entry, namespace_name, key)) {
                .eq => {
                    if (try tableEntryEncodedLen(entry) > self.max_result_bytes) return error.InvalidTableFile;
                    if (control) |budget| try budget.checkpoint();
                    return current;
                },
                .gt => return null,
                .lt => {},
            }
        }
        return null;
    }
};

// The physical checksum must be validated by the caller. The reader checks
// prefix structure and declared result bounds before publishing a borrowed row.
pub fn findExactEntryInPrefixPayloadAlloc(
    allocator: std.mem.Allocator,
    scratch: std.mem.Allocator,
    max_result_bytes: usize,
    payload: []const u8,
    first_entry_index: usize,
    namespace_name: ?[]const u8,
    key: []const u8,
) !?OwnedPositionedEntry {
    var reader = try PrefixPointReader.init(payload, first_entry_index, max_result_bytes);
    defer reader.deinit(scratch);
    const positioned = try reader.find(scratch, namespace_name, key) orelse return null;
    const entry = positioned.entry;
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);
    try out.ensureTotalCapacityPrecise(allocator, try tableEntryEncodedLen(entry));
    try appendEntryBytesToList(allocator, &out, entry);
    const bytes = out.toOwnedSliceAssert();
    errdefer allocator.free(bytes);
    return .{ .index = positioned.index, .entry = try parseEntryAt(bytes, 0), .bytes = bytes };
}

pub fn findExactEntryInCompressedBlockPayloadAlloc(
    allocator: std.mem.Allocator,
    compression: BlockCompression,
    payload: []const u8,
    expected_checksum: u32,
    first_entry_index: usize,
    namespace_name: ?[]const u8,
    key: []const u8,
) !?OwnedPositionedEntry {
    return findExactEntryInCompressedBlockPayloadWithScratchAlloc(allocator, allocator, compression, payload, expected_checksum, first_entry_index, namespace_name, key, std.math.maxInt(usize));
}

pub fn findExactEntryInCompressedBlockPayloadWithScratchAlloc(
    allocator: std.mem.Allocator,
    scratch: std.mem.Allocator,
    compression: BlockCompression,
    payload: []const u8,
    expected_checksum: u32,
    first_entry_index: usize,
    namespace_name: ?[]const u8,
    key: []const u8,
    max_result_bytes: usize,
) !?OwnedPositionedEntry {
    try validateBlockPayload(payload, expected_checksum);
    return switch (compression) {
        .prefix => try findExactEntryInPrefixPayloadAlloc(allocator, scratch, max_result_bytes, payload, first_entry_index, namespace_name, key),
        .prefix_snappy => blk: {
            if (max_result_bytes != std.math.maxInt(usize)) try validatePrefixDecodedSize(payload, max_result_bytes);
            const prefix_payload = try snappy.decode(scratch, payload);
            defer scratch.free(prefix_payload);
            break :blk try findExactEntryInPrefixPayloadAlloc(allocator, scratch, max_result_bytes, prefix_payload, first_entry_index, namespace_name, key);
        },
        .none, .snappy => null,
    };
}

pub fn validatePrefixDecodedSize(payload: []const u8, logical_len: usize) !void {
    // Raw entries have a 13-byte header; prefix entries add four bytes and
    // at most one four-byte restart per entry. Shared keys only shrink the
    // payload. Thus even restart_interval=1 fits 2 * logical_len + 32 bytes
    // (including the 24-byte block header), without assuming compressibility.
    const ceiling = std.math.add(usize, std.math.mul(usize, logical_len, 2) catch return error.InvalidTableFile, 32) catch return error.InvalidTableFile;
    if (try snappy.decodedLen(payload) > ceiling) return error.InvalidTableFile;
}

fn commonPrefixLen(lhs: []const u8, rhs: []const u8) usize {
    const limit = @min(lhs.len, rhs.len);
    var index: usize = 0;
    while (index < limit and lhs[index] == rhs[index]) : (index += 1) {}
    return index;
}

/// Expand previously checksum-validated prefix records into immutable raw rows.
/// The caller owns admission and the compressed source's integrity check.
pub fn expandValidatedPrefixRecordsAlloc(allocator: std.mem.Allocator, records: []const u8, expected_len: usize) ![]u8 {
    return expandValidatedPrefixRecordsAllocWithAdmission(allocator, records, expected_len, null);
}

pub fn expandValidatedPrefixRecordsAllocWithAdmission(allocator: std.mem.Allocator, records: []const u8, expected_len: usize, control: ?read_admission.Port) ![]u8 {
    return decodePrefixCompressedBlockAllocWithAdmission(allocator, allocator, records, expected_len, control);
}

pub fn decodeBlockPayloadAlloc(
    allocator: std.mem.Allocator,
    compression: BlockCompression,
    payload: []const u8,
    expected_len: usize,
    expected_checksum: u32,
) ![]u8 {
    return decodeBlockPayloadAllocWithAdmission(allocator, compression, payload, expected_len, expected_checksum, null);
}

pub fn decodeBlockPayloadWithScratchAlloc(
    allocator: std.mem.Allocator,
    scratch: std.mem.Allocator,
    compression: BlockCompression,
    payload: []const u8,
    expected_len: usize,
    expected_checksum: u32,
) ![]u8 {
    return decodeBlockPayloadWithScratchAllocWithAdmission(allocator, scratch, compression, payload, expected_len, expected_checksum, null);
}

pub fn decodeBlockPayloadAllocWithAdmission(
    allocator: std.mem.Allocator,
    compression: BlockCompression,
    payload: []const u8,
    expected_len: usize,
    expected_checksum: u32,
    control: ?read_admission.Port,
) ![]u8 {
    return decodeBlockPayloadWithScratchAllocWithAdmission(allocator, allocator, compression, payload, expected_len, expected_checksum, control);
}

/// Scalar continuation for raw or Snappy block verification/materialization.
/// Input stays immutable; output (once supplied) preserves its decoded prefix.
/// Both buffers and controls remain caller-owned. The checksum and Snappy
/// preamble are verified before needs_output authorizes output allocation.
/// Prefix block reconstruction still uses its existing decoder separately.
pub const BlockDecodeCursor = struct {
    pub const Status = enum { pending, needs_output, done };
    pub const Progress = struct { status: Status, checksum_bytes: usize = 0, input_bytes: usize = 0, output_bytes: usize = 0, tags: usize = 0 };
    const Phase = enum { checksum, header, output, done, failed };
    compression: BlockCompression,
    input_len: usize,
    output_len: usize,
    expected_checksum: u32,
    checksum_offset: usize = 0,
    output_offset: usize = 0,
    crc: Crc32 = .init(),
    decoder: snappy.DecodeCursor,
    phase: Phase = .checksum,
    failure: ?anyerror = null,

    pub fn init(compression: BlockCompression, input_len: usize, output_len: usize, expected_checksum: u32) !BlockDecodeCursor {
        if (compression != .none and compression != .snappy) return error.InvalidArgument;
        return .{ .compression = compression, .input_len = input_len, .output_len = output_len, .expected_checksum = expected_checksum, .decoder = snappy.DecodeCursor.init(input_len, output_len) };
    }

    pub fn step(self: *BlockDecodeCursor, src: []const u8, dst: ?[]u8, quantum: usize) !Progress {
        if (quantum == 0 or src.len != self.input_len) return error.InvalidArgument;
        if (dst) |out| if (out.len != self.output_len) return error.InvalidArgument;
        return self.stepFromReader(SliceReader{ .bytes = src, .length = src.len }, dst, quantum);
    }

    const SliceReader = struct {
        bytes: []const u8,
        length: usize,
        pub fn window(self: @This(), offset: usize, max_len: usize) ![]const u8 {
            return self.bytes[offset..][0..@min(max_len, self.bytes.len - offset)];
        }
        pub fn readInto(self: @This(), offset: u64, out: []u8) !void {
            @memcpy(out, self.bytes[@intCast(offset)..][0..out.len]);
        }
    };

    /// The reader owns immutable source binding and funded input windows.
    /// A failed read terminates this operation; discard unpublished output.
    pub fn stepFromReader(self: *BlockDecodeCursor, reader: anytype, dst: ?[]u8, quantum: usize) !Progress {
        if (quantum == 0 or reader.length != self.input_len) return error.InvalidArgument;
        if (dst) |out| if (out.len != self.output_len) return error.InvalidArgument;
        return self.advance(reader, dst, quantum) catch |err| {
            self.phase = .failed;
            self.failure = err;
            return err;
        };
    }

    fn advance(self: *BlockDecodeCursor, reader: anytype, dst: ?[]u8, quantum: usize) !Progress {
        switch (self.phase) {
            .failed => return self.failure.?,
            .done => return .{ .status = .done },
            .checksum => {
                const take = @min(quantum, self.input_len - self.checksum_offset);
                const bytes = try reader.window(self.checksum_offset, take);
                if (bytes.len > take or (bytes.len == 0 and take != 0)) return error.CorruptInput;
                self.crc.update(bytes);
                self.checksum_offset += bytes.len;
                if (self.checksum_offset == self.input_len) {
                    if (self.crc.final() != self.expected_checksum) return error.TableBlockChecksumMismatch;
                    self.phase = .header;
                }
                // A separate phase prevents checksum plus decode from spending
                // two input quanta in one invocation.
                return .{ .status = .pending, .checksum_bytes = bytes.len };
            },
            .header => {
                if (self.compression == .none) {
                    if (self.input_len != self.output_len) return error.InvalidTableFile;
                    self.phase = .output;
                    return .{ .status = if (dst == null) .needs_output else .pending };
                }
                if (self.input_len == 0 and self.output_len != 0) return error.InvalidTableFile;
                const progress = self.decoder.stepFromReader(reader, null, quantum) catch |err| {
                    if (self.decoder.declared_length) |length| if (length != self.output_len) return error.InvalidTableFile;
                    return err;
                };
                if (progress.needs_output or progress.done) self.phase = .output;
                return .{ .status = if (self.phase == .output and dst == null) .needs_output else .pending, .input_bytes = progress.input_bytes, .tags = progress.tags };
            },
            .output => {
                const out = dst orelse return .{ .status = .needs_output };
                if (self.compression == .none) {
                    const take = @min(quantum, out.len - self.output_offset);
                    try reader.readInto(@intCast(self.output_offset), out[self.output_offset..][0..take]);
                    self.output_offset += take;
                    if (self.output_offset == out.len) self.phase = .done;
                    return .{ .status = if (self.phase == .done) .done else .pending, .input_bytes = take, .output_bytes = take };
                }
                const progress = try self.decoder.stepFromReader(reader, out, quantum);
                self.output_offset = self.decoder.output_offset;
                if (progress.done) self.phase = .done;
                return .{ .status = if (progress.done) .done else .pending, .input_bytes = progress.input_bytes, .output_bytes = progress.output_bytes, .tags = progress.tags };
            },
        }
    }
};

fn decodeRawOrSnappyBlockAlloc(allocator: std.mem.Allocator, compression: BlockCompression, payload: []const u8, expected_len: usize, expected_checksum: u32, control: ?read_admission.Port) ![]u8 {
    var cursor = try BlockDecodeCursor.init(compression, payload.len, expected_len, expected_checksum);
    var output: ?[]u8 = null;
    errdefer if (output) |bytes| allocator.free(bytes);
    while (true) {
        if (control) |port| try port.checkpoint();
        const progress = cursor.step(payload, output, 64 * 1024) catch |err| {
            // The final original lifetime check precedes reporting corruption,
            // just as it did in the synchronous checksum/decoder path.
            if (control) |port| try port.checkpoint();
            return err;
        };
        if (control) |port| try port.checkpoint();
        switch (progress.status) {
            .pending => {},
            .needs_output => {
                std.debug.assert(output == null);
                output = try allocator.alloc(u8, expected_len);
            },
            .done => return output.?,
        }
    }
}

pub fn decodeBlockPayloadWithScratchAllocWithAdmission(
    allocator: std.mem.Allocator,
    scratch: std.mem.Allocator,
    compression: BlockCompression,
    payload: []const u8,
    expected_len: usize,
    expected_checksum: u32,
    control: ?read_admission.Port,
) ![]u8 {
    if (compression == .none or compression == .snappy)
        return decodeRawOrSnappyBlockAlloc(allocator, compression, payload, expected_len, expected_checksum, control);
    if (try checksumWithAdmission(payload, control) != expected_checksum) return error.TableBlockChecksumMismatch;
    const decoded = switch (compression) {
        .none, .snappy => unreachable,
        .prefix => try decodePrefixCompressedBlockAllocWithAdmission(allocator, scratch, payload, expected_len, control),
        .prefix_snappy => blk: {
            try validatePrefixDecodedSize(payload, expected_len);
            const prefix_payload = try snappy.decodeWithAdmission(scratch, payload, control);
            defer scratch.free(prefix_payload);
            break :blk try decodePrefixCompressedBlockAllocWithAdmission(allocator, scratch, prefix_payload, expected_len, control);
        },
    };
    errdefer allocator.free(decoded);
    if (control) |budget| try budget.checkpoint();
    return decoded;
}

pub fn decodeWindowFromRawAlloc(
    allocator: std.mem.Allocator,
    raw: []const u8,
    index: *const TableIndex,
    window: EntryDataWindow,
) ![]u8 {
    return decodeWindowFromRawAllocWithAdmission(allocator, raw, index, window, null);
}

pub fn decodeWindowFromRawAllocWithAdmission(
    allocator: std.mem.Allocator,
    raw: []const u8,
    index: *const TableIndex,
    window: EntryDataWindow,
    control: ?read_admission.Port,
) ![]u8 {
    const physical_start = index.entry_data_start + @as(usize, window.physicalRelativeOffset());
    const physical_len: usize = @intCast(window.physicalLen());
    if (physical_start > raw.len or physical_len > raw.len - physical_start) return error.InvalidTableFile;
    return try decodeBlockPayloadAllocWithAdmission(
        allocator,
        window.compression,
        raw[physical_start..][0..physical_len],
        window.len,
        window.checksum,
        control,
    );
}

pub fn validateBlockPayload(payload: []const u8, expected_checksum: u32) !void {
    return validateBlockPayloadWithAdmission(payload, expected_checksum, null);
}

pub fn validateBlockPayloadWithAdmission(payload: []const u8, expected_checksum: u32, control: ?read_admission.Port) !void {
    if (try checksumWithAdmission(payload, control) != expected_checksum) return error.TableBlockChecksumMismatch;
}

fn validateRawBlockChecksums(raw: []const u8, index: *const TableIndex, control: ?read_admission.Port) !void {
    for (0..index.blocks.len) |block_index| {
        const window = index.blockWindow(block_index);
        const physical_start = index.entry_data_start + @as(usize, window.physicalRelativeOffset());
        const physical_len: usize = @intCast(window.physicalLen());
        if (physical_start > raw.len or physical_len > raw.len - physical_start) return error.InvalidTableFile;
        if (try checksumWithAdmission(raw[physical_start..][0..physical_len], control) != window.checksum) return error.TableBlockChecksumMismatch;
    }
}

fn materializeLogicalEntryDataRawAlloc(
    allocator: std.mem.Allocator,
    raw: []const u8,
    index: *const TableIndex,
    control: ?read_admission.Port,
) ![]u8 {
    if (control) |budget| try budget.checkpoint();
    const logical_len = index.entry_data_start + index.entry_data_len;
    const logical_raw = try allocator.alloc(u8, logical_len);
    errdefer allocator.free(logical_raw);
    if (index.entry_data_start > raw.len) return error.InvalidTableFile;
    try copyWithAdmission(logical_raw[0..index.entry_data_start], raw[0..index.entry_data_start], control);
    for (index.blocks, 0..) |_, block_index| {
        if (control) |budget| try budget.checkpoint();
        const window = index.blockWindow(block_index);
        const decoded = try decodeWindowFromRawAllocWithAdmission(allocator, raw, index, window, control);
        defer allocator.free(decoded);
        const logical_start = index.entry_data_start + @as(usize, window.relative_offset);
        if (logical_start > logical_raw.len or decoded.len > logical_raw.len - logical_start) return error.InvalidTableFile;
        if (control) |budget| try budget.checkpoint();
        try copyWithAdmission(logical_raw[logical_start..][0..decoded.len], decoded, control);
    }
    if (control) |budget| try budget.checkpoint();
    return logical_raw;
}

pub fn decodeIndexAlloc(allocator: std.mem.Allocator, raw: []const u8) !TableIndex {
    return decodeIndexAllocWithAdmission(allocator, raw, null);
}

pub fn decodeIndexAllocWithAdmission(allocator: std.mem.Allocator, raw: []const u8, control: ?read_admission.Port) !TableIndex {
    if (control) |budget| try budget.checkpoint();
    var cursor: usize = 0;
    const header = try decodeHeader(raw, &cursor);
    return switch (header.version) {
        version => decodeVersionedIndexAlloc(allocator, raw, header, control),
        else => error.UnsupportedVersion,
    };
}

// Check cancellation between finite checksum chunks, without changing on-disk
// framing or keeping a borrow beyond the synchronous decode operation.
fn checksumWithAdmission(bytes: []const u8, control: ?read_admission.Port) !u32 {
    if (control == null) return Crc32.hash(bytes);
    var crc: Crc32 = .init();
    var offset: usize = 0;
    while (offset < bytes.len) {
        try control.?.checkpoint();
        const end = offset + @min(bytes.len - offset, 64 * 1024);
        crc.update(bytes[offset..end]);
        offset = end;
    }
    try control.?.checkpoint();
    return crc.final();
}

pub fn decodeHeader(raw: []const u8, cursor: *usize) !Header {
    if (raw.len < header_len) return error.InvalidTableFile;
    if (!std.mem.eql(u8, raw[0..magic.len], magic)) return error.InvalidTableFile;
    cursor.* = magic.len;

    const found_version = try readU32(raw, cursor);
    const entry_count: usize = @intCast(try readU32(raw, cursor));
    return switch (found_version) {
        version => blk: {
            const entry_data_len: usize = @intCast(try readU32(raw, cursor));
            const entry_data_start = cursor.*;
            break :blk .{
                .version = found_version,
                .entry_count = entry_count,
                .entry_data_len = entry_data_len,
                .entry_offsets_start = entry_data_start + entry_data_len,
                .entry_data_start = entry_data_start,
            };
        },
        else => return error.UnsupportedVersion,
    };
}

pub fn hasFooterMagic(raw: []const u8) bool {
    return raw.len >= footer_magic.len and std.mem.eql(u8, raw[0..footer_magic.len], footer_magic);
}

pub fn decodeFooter(raw: []const u8) !Footer {
    if (raw.len < footer_len) return error.InvalidTableFile;
    return try decodeFooterBytes(raw[raw.len - footer_len ..]);
}

pub fn decodeFooterBytes(raw: []const u8) !Footer {
    if (raw.len != footer_len) return error.InvalidTableFile;
    if (!hasFooterMagic(raw)) return error.InvalidTableFile;
    const stored_footer_checksum = std.mem.readInt(u32, raw[44..48], .little);
    if (Crc32.hash(raw[0..44]) != stored_footer_checksum) return error.InvalidTableFile;

    var cursor: usize = footer_magic.len;
    if (try readU32(raw, &cursor) != version) return error.UnsupportedVersion;
    if (try readU32(raw, &cursor) != 0) return error.InvalidTableFile;
    const metadata_offset: usize = @intCast(try readU64(raw, &cursor));
    const metadata_len: usize = @intCast(try readU64(raw, &cursor));
    const entry_count: usize = @intCast(try readU32(raw, &cursor));
    const entry_data_len: usize = @intCast(try readU32(raw, &cursor));
    const metadata_checksum = try readU32(raw, &cursor);
    _ = try readU32(raw, &cursor);
    if (cursor != raw.len) return error.InvalidTableFile;
    if (metadata_offset < header_len or metadata_len < packed_offsets_magic.len) return error.InvalidTableFile;

    return .{
        .metadata_offset = metadata_offset,
        .metadata_len = metadata_len,
        .entry_count = entry_count,
        .entry_data_len = entry_data_len,
        .metadata_checksum = metadata_checksum,
    };
}

/// A read-only index whose variable spans belong to one retained metadata
/// allocation. Use view() for access and deinit() for retirement; TableIndex's
/// ordinary deinit must not free the interior borrowed spans.
pub const RetainedIndex = struct {
    allocator: std.mem.Allocator,
    metadata: []u8,
    value: TableIndex,

    pub fn view(self: *const @This()) *const TableIndex {
        return &self.value;
    }
    pub fn deinit(self: *@This()) void {
        self.allocator.free(self.value.entry_offsets);
        self.allocator.free(self.value.block_entry_offsets);
        self.allocator.free(self.value.blocks);
        self.allocator.free(self.metadata);
        self.* = undefined;
    }
};

/// Takes ownership of metadata immediately. No step retains a control borrow.
/// CRC and offset validation use finite quanta; other phases visit one scalar
/// record. Variable strings and filters borrow the owned buffer without copies.
pub const IndexDecodeCursor = struct {
    pub const Status = enum { pending, ready };
    const Phase = enum { checksum, offsets_allocate, offsets, filter, blocks_allocate, blocks, filter_count, block_filters, hash_count, hashes, physical_count, physical, smallest_count, smallest, compare_namespace, compare_key, prefix, prefix_count, block_prefix, validate_offsets, ready, taken, closed, failed };
    allocator: std.mem.Allocator,
    metadata: ?[]u8,
    footer: Footer,
    phase: Phase = .checksum,
    failure: ?anyerror = null,
    crc: Crc32 = .init(),
    position: usize = 0,
    cursor: usize = 0,
    offsets: []u16 = &.{},
    blocks: []TableIndex.BlockMeta = &.{},
    filter: ?bloom.OwnedFilter = null,
    prefix_filter: ?bloom.OwnedFilter = null,
    prefix_extractor: PrefixExtractor = .none,
    row: usize = 0,
    block: usize = 0,
    expected_entry: usize = 0,
    expected_logical: usize = 0,
    expected_physical: usize = 0,
    previous_local: u16 = 0,
    compare_offset: usize = 0,

    pub fn init(allocator: std.mem.Allocator, footer: Footer, metadata: []u8) @This() {
        return .{ .allocator = allocator, .footer = footer, .metadata = metadata };
    }
    pub fn step(self: *@This(), quantum: usize, control: ?read_admission.Port) !Status {
        if (quantum == 0 or quantum > 65536) return error.InvalidArgument;
        if (self.failure) |err| return err;
        if (self.phase == .closed or self.phase == .taken) return error.TransactionClosed;
        return self.advance(quantum, control) catch |err| {
            const cause = if (control) |port| blk: {
                port.checkpoint() catch |original| break :blk original;
                break :blk err;
            } else err;
            self.deinit();
            self.failure = cause;
            self.phase = .failed;
            return cause;
        };
    }
    fn count(self: *@This(), bytes: []const u8, next: Phase) !void {
        if (try readU32(bytes, &self.cursor) != self.blocks.len) return error.InvalidTableFile;
        self.block = 0;
        self.phase = next;
    }
    fn borrowFilter(self: *@This(), bytes: []const u8, optional: bool) !?bloom.OwnedFilter {
        const len = try readU32(bytes, &self.cursor);
        if (optional and len == 0) return null;
        const filter = try bloom.BorrowedFilter.decode(try readSlice(bytes, &self.cursor, len));
        return .{ .bytes = @constCast(filter.bytes), .bit_count = filter.bit_count, .hash_count = filter.hash_count };
    }
    fn span(self: *@This(), bytes: []const u8, len: usize) ![]u8 {
        return @constCast(try readSlice(bytes, &self.cursor, len));
    }
    fn compare(self: *@This(), lhs: []const u8, rhs: []const u8, quantum: usize) ?std.math.Order {
        const common = @min(lhs.len, rhs.len);
        const end = self.compare_offset + @min(quantum, common - self.compare_offset);
        const order = std.mem.order(u8, lhs[self.compare_offset..end], rhs[self.compare_offset..end]);
        self.compare_offset = end;
        if (order != .eq) return order;
        if (end == common) return std.math.order(lhs.len, rhs.len);
        return null;
    }
    fn compared(self: *@This(), order: std.math.Order, namespace: bool) !void {
        if (order == .gt) return error.InvalidTableFile;
        self.compare_offset = 0;
        if (namespace and order == .eq) self.phase = .compare_key else {
            self.block += 1;
            self.phase = .smallest;
        }
    }
    fn advance(self: *@This(), quantum: usize, control: ?read_admission.Port) !Status {
        if (control) |port| try port.checkpoint();
        const bytes = self.metadata.?;
        switch (self.phase) {
            .ready => return .ready,
            .checksum => {
                if (bytes.len != self.footer.metadata_len or self.footer.metadata_offset < header_len) return error.InvalidTableFile;
                const end = self.position + @min(quantum, bytes.len - self.position);
                self.crc.update(bytes[self.position..end]);
                self.position = end;
                if (end == bytes.len) {
                    if (self.crc.final() != self.footer.metadata_checksum) return error.InvalidTableFile;
                    try consumePackedOffsetsMarker(bytes, &self.cursor);
                    if (self.footer.entry_count > (bytes.len - self.cursor) / 2) return error.InvalidTableFile;
                    self.phase = .offsets_allocate;
                }
            },
            .offsets_allocate => {
                self.offsets = try self.allocator.alloc(u16, self.footer.entry_count);
                self.row = 0;
                self.phase = .offsets;
            },
            .offsets => {
                const end = self.row + @min(@max(1, quantum / 2), self.offsets.len - self.row);
                while (self.row < end) : (self.row += 1) self.offsets[self.row] = try readU16(bytes, &self.cursor);
                if (self.row == self.offsets.len) self.phase = .filter;
            },
            .filter => {
                self.filter = (try self.borrowFilter(bytes, false)).?;
                self.phase = .blocks_allocate;
            },
            .blocks_allocate => {
                const n = try readU32(bytes, &self.cursor);
                if (n > self.footer.entry_count or n > (bytes.len - self.cursor) / 24) return error.InvalidTableFile;
                self.blocks = try self.allocator.alloc(TableIndex.BlockMeta, n);
                self.block = 0;
                self.phase = .blocks;
            },
            .blocks => {
                if (self.block == self.blocks.len) {
                    if (self.expected_entry != self.footer.entry_count) return error.InvalidTableFile;
                    self.phase = .filter_count;
                } else {
                    const offset = try readU32(bytes, &self.cursor);
                    const len = try readU32(bytes, &self.cursor);
                    const first = try readU32(bytes, &self.cursor);
                    const n = try readU32(bytes, &self.cursor);
                    const ns_len = try readU32(bytes, &self.cursor);
                    const key_len = try readU32(bytes, &self.cursor);
                    const ns = if (ns_len == std.math.maxInt(u32)) null else try self.span(bytes, ns_len);
                    const key = try self.span(bytes, key_len);
                    if (n == 0 or first != self.expected_entry or offset != self.expected_logical or
                        @as(usize, first) + n > self.footer.entry_count or @as(usize, offset) + len > self.footer.entry_data_len or
                        (self.block + 1 == self.blocks.len and @as(usize, offset) + len != self.footer.entry_data_len)) return error.InvalidTableFile;
                    self.blocks[self.block] = .{ .relative_offset = offset, .len = len, .physical_relative_offset = offset, .physical_len = len, .first_entry_index = first, .entry_count = n, .largest_namespace_name = ns, .largest_key = key };
                    self.expected_entry += n;
                    self.expected_logical += len;
                    self.block += 1;
                }
            },
            .filter_count => try self.count(bytes, .block_filters),
            .block_filters => {
                if (self.block == self.blocks.len) self.phase = .hash_count else {
                    self.blocks[self.block].filter = try self.borrowFilter(bytes, false);
                    self.block += 1;
                }
            },
            .hash_count => try self.count(bytes, .hashes),
            .hashes => {
                if (self.block == self.blocks.len) self.phase = .physical_count else {
                    const n = try readU32(bytes, &self.cursor);
                    if (n > (bytes.len - self.cursor) / 4) return error.InvalidTableFile;
                    _ = try readSlice(bytes, &self.cursor, @as(usize, n) * 4);
                    self.block += 1;
                }
            },
            .physical_count => try self.count(bytes, .physical),
            .physical => {
                if (self.block == self.blocks.len) {
                    if (self.expected_physical != self.footer.metadata_offset - header_len) return error.InvalidTableFile;
                    self.phase = .smallest_count;
                } else {
                    const offset = try readU32(bytes, &self.cursor);
                    const len = try readU32(bytes, &self.cursor);
                    const compression: BlockCompression = switch (try readU32(bytes, &self.cursor)) {
                        0 => .none,
                        1 => .snappy,
                        2 => .prefix,
                        3 => .prefix_snappy,
                        else => return error.InvalidTableFile,
                    };
                    const checksum = try readU32(bytes, &self.cursor);
                    if (len == 0 or offset != self.expected_physical) return error.InvalidTableFile;
                    const block = &self.blocks[self.block];
                    block.physical_relative_offset = offset;
                    block.physical_len = len;
                    block.compression = compression;
                    block.checksum = checksum;
                    self.expected_physical += len;
                    self.block += 1;
                }
            },
            .smallest_count => try self.count(bytes, .smallest),
            .smallest => {
                if (self.block == self.blocks.len) self.phase = .prefix else {
                    const ns_len = try readU32(bytes, &self.cursor);
                    const key_len = try readU32(bytes, &self.cursor);
                    const block = &self.blocks[self.block];
                    block.smallest_namespace_name = if (ns_len == std.math.maxInt(u32)) null else try self.span(bytes, ns_len);
                    block.smallest_key = try self.span(bytes, key_len);
                    self.compare_offset = 0;
                    self.phase = .compare_namespace;
                }
            },
            .compare_namespace => {
                const block = self.blocks[self.block];
                const lhs = block.smallest_namespace_name;
                const rhs = block.largest_namespace_name;
                if (lhs == null or rhs == null) try self.compared(if (lhs == null and rhs == null) .eq else if (lhs == null) .lt else .gt, true) else if (self.compare(lhs.?, rhs.?, quantum)) |order| try self.compared(order, true);
            },
            .compare_key => {
                const block = self.blocks[self.block];
                if (self.compare(block.smallest_key.?, block.largest_key, quantum)) |order| try self.compared(order, false);
            },
            .prefix => {
                self.prefix_extractor = try decodePrefixExtractor(try readU32(bytes, &self.cursor));
                self.prefix_filter = try self.borrowFilter(bytes, true);
                self.phase = .prefix_count;
            },
            .prefix_count => try self.count(bytes, .block_prefix),
            .block_prefix => {
                if (self.block == self.blocks.len) {
                    if (self.cursor != bytes.len) return error.InvalidTableFile;
                    if (self.offsets.len == 0) {
                        if (self.blocks.len != 0 or self.footer.entry_data_len != 0) return error.InvalidTableFile;
                        self.phase = .ready;
                    } else {
                        if (self.blocks.len == 0) return error.InvalidTableFile;
                        self.block = 0;
                        self.row = 0;
                        self.previous_local = 0;
                        self.phase = .validate_offsets;
                    }
                } else {
                    self.blocks[self.block].prefix_filter = try self.borrowFilter(bytes, true);
                    self.block += 1;
                }
            },
            .validate_offsets => {
                const block = self.blocks[self.block];
                const block_end = @as(usize, block.first_entry_index) + block.entry_count;
                const end = self.row + @min(@max(1, quantum / 2), block_end - self.row);
                while (self.row < end) : (self.row += 1) {
                    const local = self.offsets[self.row];
                    if ((self.row == block.first_entry_index and local != 0) or (self.row > block.first_entry_index and local <= self.previous_local) or local >= block.len) return error.InvalidTableFile;
                    self.previous_local = local;
                }
                if (self.row == block_end) {
                    self.block += 1;
                    self.previous_local = 0;
                    if (self.block == self.blocks.len) {
                        if (@as(usize, block.relative_offset) + self.offsets[self.offsets.len - 1] >= self.footer.entry_data_len) return error.InvalidTableFile;
                        self.phase = .ready;
                    }
                }
            },
            else => unreachable,
        }
        if (control) |port| try port.checkpoint();
        return if (self.phase == .ready) .ready else .pending;
    }
    pub fn takeOutput(self: *@This()) !RetainedIndex {
        if (self.failure) |err| return err;
        if (self.phase != .ready) return error.InvalidArgument;
        const result: RetainedIndex = .{ .allocator = self.allocator, .metadata = self.metadata.?, .value = .{ .block_entry_offsets = self.offsets, .entry_data_start = header_len, .entry_data_len = self.footer.entry_data_len, .filter = self.filter.?, .prefix_extractor = self.prefix_extractor, .prefix_filter = self.prefix_filter, .blocks = self.blocks } };
        self.metadata = null;
        self.offsets = &.{};
        self.blocks = &.{};
        self.phase = .taken;
        return result;
    }
    pub fn deinit(self: *@This()) void {
        // Every variable span is inside metadata, even on partial parse. No
        // per-block cleanup loop or access to uninitialized block slots.
        self.allocator.free(self.blocks);
        self.blocks = &.{};
        self.allocator.free(self.offsets);
        self.offsets = &.{};
        if (self.metadata) |bytes| self.allocator.free(bytes);
        self.metadata = null;
        self.phase = .closed;
    }
};

pub fn decodeIndexFromFooterAlloc(allocator: std.mem.Allocator, footer: Footer, metadata: []const u8) !TableIndex {
    return decodeIndexFromFooterAllocWithAdmission(allocator, footer, metadata, null);
}

pub fn decodeIndexFromFooterAllocWithAdmission(allocator: std.mem.Allocator, footer: Footer, metadata: []const u8, control: ?read_admission.Port) !TableIndex {
    if (control) |budget| try budget.checkpoint();
    if (metadata.len != footer.metadata_len) return error.InvalidTableFile;
    if (try checksumWithAdmission(metadata, control) != footer.metadata_checksum) return error.InvalidTableFile;
    var index = try decodeFooterMetadataAlloc(allocator, metadata, header_len, footer.entry_count, footer.entry_data_len, footer.metadata_offset - header_len, null, control);
    errdefer index.deinit(allocator);
    if (control) |budget| try budget.checkpoint();
    return index;
}

pub fn decodeSequentialIndexFromFooterAlloc(
    allocator: std.mem.Allocator,
    footer: Footer,
    metadata: []const u8,
) !SequentialTableIndex {
    if (metadata.len != footer.metadata_len) return error.InvalidTableFile;
    if (Crc32.hash(metadata) != footer.metadata_checksum) return error.InvalidTableFile;
    var cursor: usize = 0;

    // Entry offsets and all lookup accelerators are deliberately skipped.
    try consumePackedOffsetsMarker(metadata, &cursor);
    if (footer.entry_count > (metadata.len - cursor) / @sizeOf(u16)) return error.InvalidTableFile;
    _ = try readSlice(metadata, &cursor, footer.entry_count * @sizeOf(u16));
    const bloom_len: usize = @intCast(try readU32(metadata, &cursor));
    _ = try readSlice(metadata, &cursor, bloom_len);

    const block_count: usize = @intCast(try readU32(metadata, &cursor));
    const minimum_block_metadata_bytes = @sizeOf(u32) * 6;
    if (block_count > footer.entry_count or
        block_count > (metadata.len - cursor) / minimum_block_metadata_bytes)
    {
        return error.InvalidTableFile;
    }
    const blocks = try allocator.alloc(SequentialTableIndex.Block, block_count);
    errdefer allocator.free(blocks);
    var expected_entry_index: usize = 0;
    var expected_logical_offset: usize = 0;
    for (blocks) |*block| {
        const relative_offset = try readU32(metadata, &cursor);
        const logical_len = try readU32(metadata, &cursor);
        const first_entry_index = try readU32(metadata, &cursor);
        const entry_count = try readU32(metadata, &cursor);
        const largest_namespace_len = try readU32(metadata, &cursor);
        const largest_key_len: usize = @intCast(try readU32(metadata, &cursor));
        if (largest_namespace_len != std.math.maxInt(u32)) {
            _ = try readSlice(metadata, &cursor, largest_namespace_len);
        }
        _ = try readSlice(metadata, &cursor, largest_key_len);
        if (entry_count == 0 or first_entry_index != expected_entry_index or relative_offset != expected_logical_offset) {
            return error.InvalidTableFile;
        }
        if (@as(usize, relative_offset) + logical_len > footer.entry_data_len) return error.InvalidTableFile;
        block.* = .{
            .window = .{
                .relative_offset = relative_offset,
                .len = logical_len,
                .physical_relative_offset = relative_offset,
                .physical_len = logical_len,
                .compression = .none,
            },
            .entry_count = entry_count,
        };
        expected_entry_index += entry_count;
        expected_logical_offset += logical_len;
    }
    if (expected_entry_index != footer.entry_count or expected_logical_offset != footer.entry_data_len) return error.InvalidTableFile;

    // Per-block Bloom filters.
    const block_filter_count: usize = @intCast(try readU32(metadata, &cursor));
    if (block_filter_count != blocks.len) return error.InvalidTableFile;
    for (blocks) |_| {
        const filter_len: usize = @intCast(try readU32(metadata, &cursor));
        _ = try readSlice(metadata, &cursor, filter_len);
    }

    // Per-block point-lookup hash slots.
    const hash_block_count: usize = @intCast(try readU32(metadata, &cursor));
    if (hash_block_count != blocks.len) return error.InvalidTableFile;
    for (blocks) |_| {
        const slot_count: usize = @intCast(try readU32(metadata, &cursor));
        if (slot_count > (metadata.len - cursor) / @sizeOf(u32)) return error.InvalidTableFile;
        _ = try readSlice(metadata, &cursor, slot_count * @sizeOf(u32));
    }

    // Physical block windows are the only optional lookup-era section the
    // sequential cursor retains.
    const physical_count: usize = @intCast(try readU32(metadata, &cursor));
    if (physical_count != blocks.len) return error.InvalidTableFile;
    var expected_physical_offset: usize = 0;
    for (blocks) |*block| {
        const physical_relative_offset = try readU32(metadata, &cursor);
        const physical_len = try readU32(metadata, &cursor);
        const compression = switch (try readU32(metadata, &cursor)) {
            @backingInt(BlockCompression.none) => BlockCompression.none,
            @backingInt(BlockCompression.snappy) => BlockCompression.snappy,
            @backingInt(BlockCompression.prefix) => BlockCompression.prefix,
            @backingInt(BlockCompression.prefix_snappy) => BlockCompression.prefix_snappy,
            else => return error.InvalidTableFile,
        };
        const checksum = try readU32(metadata, &cursor);
        if (physical_len == 0 or physical_relative_offset != expected_physical_offset) return error.InvalidTableFile;
        block.window.physical_relative_offset = physical_relative_offset;
        block.window.physical_len = physical_len;
        block.window.compression = compression;
        block.window.checksum = checksum;
        expected_physical_offset += physical_len;
    }
    if (expected_physical_offset != footer.metadata_offset - header_len) return error.InvalidTableFile;

    // Smallest key bounds.
    const smallest_count: usize = @intCast(try readU32(metadata, &cursor));
    if (smallest_count != blocks.len) return error.InvalidTableFile;
    for (blocks) |_| {
        const namespace_len = try readU32(metadata, &cursor);
        const key_len: usize = @intCast(try readU32(metadata, &cursor));
        if (namespace_len != std.math.maxInt(u32)) {
            _ = try readSlice(metadata, &cursor, namespace_len);
        }
        _ = try readSlice(metadata, &cursor, key_len);
    }

    _ = try decodePrefixExtractor(try readU32(metadata, &cursor));
    const prefix_filter_len: usize = @intCast(try readU32(metadata, &cursor));
    _ = try readSlice(metadata, &cursor, prefix_filter_len);
    const block_prefix_filter_count: usize = @intCast(try readU32(metadata, &cursor));
    if (block_prefix_filter_count != blocks.len) return error.InvalidTableFile;
    for (blocks) |_| {
        const filter_len: usize = @intCast(try readU32(metadata, &cursor));
        _ = try readSlice(metadata, &cursor, filter_len);
    }
    if (cursor != metadata.len) return error.InvalidTableFile;

    return .{
        .entry_data_start = header_len,
        .entry_count = footer.entry_count,
        .blocks = blocks,
    };
}

pub fn maybeContains(filter: anytype, namespace_name: ?[]const u8, key: []const u8) bool {
    const hashes = entryHashes(namespace_name, key);
    return filter.maybeContainsHashes(hashes[0], hashes[1]);
}

fn findBlockLowerBoundInMetas(blocks: []const TableIndex.BlockMeta, namespace_name: ?[]const u8, key: []const u8) ?usize {
    if (blocks.len == 0) return null;
    var lo: usize = 0;
    var hi: usize = blocks.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (compareKeyBound(blocks[mid].largest_namespace_name, blocks[mid].largest_key, namespace_name, key) == .lt) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    if (lo >= blocks.len) return null;
    return lo;
}

fn findBlockIndexForEntryInMetas(blocks: []const TableIndex.BlockMeta, entry_index: usize) ?usize {
    var lo: usize = 0;
    var hi: usize = blocks.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        const block = blocks[mid];
        if (entry_index < block.first_entry_index) {
            hi = mid;
        } else if (entry_index > block.lastEntryIndex()) {
            lo = mid + 1;
        } else {
            return mid;
        }
    }
    return null;
}

fn lowerBoundInBlockRaw(
    raw_block: []const u8,
    entry_offsets: []const u32,
    block: TableIndex.BlockMeta,
    namespace_name: ?[]const u8,
    key: []const u8,
) !usize {
    var lo: usize = block.first_entry_index;
    var hi: usize = block.first_entry_index + block.entry_count;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        const relative_offset: usize = @intCast(entry_offsets[mid] - block.relative_offset);
        const entry = try parseEntryAt(raw_block, relative_offset);
        const ord = compareEntryTo(entry, namespace_name, key);
        if (ord == .lt) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    return lo;
}

fn lowerBoundInIndexedBlockRaw(
    index: *const TableIndex,
    raw_block: []const u8,
    block_index: usize,
    namespace_name: ?[]const u8,
    key: []const u8,
) !usize {
    const block = index.blocks[block_index];
    var lo: usize = block.first_entry_index;
    var hi: usize = block.first_entry_index + block.entry_count;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        const relative_offset: usize = @intCast(index.entryStartInBlock(mid, block_index) - block.relative_offset);
        const entry = try parseEntryAt(raw_block, relative_offset);
        if (compareEntryTo(entry, namespace_name, key) == .lt) lo = mid + 1 else hi = mid;
    }
    return lo;
}

pub fn lowerBoundPositionInBlock(
    index: *const TableIndex,
    raw_block: []const u8,
    block_index: usize,
    namespace_name: ?[]const u8,
    key: []const u8,
    inclusive: bool,
) !?BorrowedDecoded.PositionedEntry {
    const block = index.blocks[block_index];
    var idx = try lowerBoundInIndexedBlockRaw(index, raw_block, block_index, namespace_name, key);
    while (idx < block.first_entry_index + block.entry_count) : (idx += 1) {
        const relative_offset: usize = @intCast(index.entryStartInBlock(idx, block_index) - block.relative_offset);
        const entry = try parseEntryAt(raw_block, relative_offset);
        if (compareNamespace(entry.namespace_name, namespace_name) != .eq) return null;
        if (!inclusive and std.mem.eql(u8, entry.key, key)) continue;
        return .{
            .index = idx,
            .entry = entry,
        };
    }
    return null;
}

fn appendU32(allocator: std.mem.Allocator, bytes: *std.ArrayListUnmanaged(u8), value: u32) !void {
    var buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &buf, value, .little);
    try bytes.appendSlice(allocator, &buf);
}

fn sinkAppendU16(sink: *TableSink, value: u16) !void {
    var buf: [2]u8 = undefined;
    std.mem.writeInt(u16, &buf, value, .little);
    try sink.appendSlice(&buf);
}

fn sinkAppendU32(sink: *TableSink, value: u32) !void {
    var buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &buf, value, .little);
    try sink.appendSlice(&buf);
}

fn appendU64(allocator: std.mem.Allocator, bytes: *std.ArrayListUnmanaged(u8), value: usize) !void {
    var buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &buf, @intCast(value), .little);
    try bytes.appendSlice(allocator, &buf);
}

fn sinkAppendU64(sink: *TableSink, value: usize) !void {
    var buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &buf, @intCast(value), .little);
    try sink.appendSlice(&buf);
}

fn appendFooter(
    sink: *TableSink,
    metadata_offset: usize,
    metadata_len: usize,
    entry_count: usize,
    entry_data_len: usize,
    metadata_checksum: u32,
) !void {
    const raw = try footerBytes(metadata_offset, metadata_len, entry_count, entry_data_len, metadata_checksum);
    try sink.appendSlice(&raw);
}

fn footerBytes(metadata_offset: usize, metadata_len: usize, entry_count: usize, entry_data_len: usize, metadata_checksum: u32) ![footer_len]u8 {
    var raw: [footer_len]u8 = undefined;
    @memcpy(raw[0..footer_magic.len], footer_magic);
    std.mem.writeInt(u32, raw[8..12], version, .little);
    std.mem.writeInt(u32, raw[12..16], 0, .little);
    std.mem.writeInt(u64, raw[16..24], @intCast(metadata_offset), .little);
    std.mem.writeInt(u64, raw[24..32], @intCast(metadata_len), .little);
    std.mem.writeInt(u32, raw[32..36], try checkedU32(entry_count), .little);
    std.mem.writeInt(u32, raw[36..40], try checkedU32(entry_data_len), .little);
    std.mem.writeInt(u32, raw[40..44], metadata_checksum, .little);
    std.mem.writeInt(u32, raw[44..48], Crc32.hash(raw[0..44]), .little);
    return raw;
}

fn readByte(raw: []const u8, cursor: *usize) !u8 {
    if (cursor.* >= raw.len) return error.InvalidTableFile;
    const out = raw[cursor.*];
    cursor.* += 1;
    return out;
}

fn readU16(raw: []const u8, cursor: *usize) !u16 {
    const bytes = try readSlice(raw, cursor, 2);
    return std.mem.readInt(u16, bytes[0..2], .little);
}

fn readU32(raw: []const u8, cursor: *usize) !u32 {
    const bytes = try readSlice(raw, cursor, 4);
    return std.mem.readInt(u32, bytes[0..4], .little);
}

fn readU64(raw: []const u8, cursor: *usize) !u64 {
    const bytes = try readSlice(raw, cursor, 8);
    return std.mem.readInt(u64, bytes[0..8], .little);
}

fn readSlice(raw: []const u8, cursor: *usize, len: usize) ![]const u8 {
    if (cursor.* + len > raw.len) return error.InvalidTableFile;
    const out = raw[cursor.* .. cursor.* + len];
    cursor.* += len;
    return out;
}

fn consumePackedOffsetsMarker(metadata: []const u8, cursor: *usize) !void {
    if (metadata.len -| cursor.* < packed_offsets_magic.len) return error.InvalidTableFile;
    if (!std.mem.eql(u8, metadata[cursor.*..][0..packed_offsets_magic.len], packed_offsets_magic))
        return error.InvalidTableFile;
    cursor.* += packed_offsets_magic.len;
}

fn decodeVersionedIndexAlloc(
    allocator: std.mem.Allocator,
    raw: []const u8,
    header: Header,
    control: ?read_admission.Port,
) !TableIndex {
    const footer = try decodeFooter(raw);
    if (footer.entry_count != header.entry_count) return error.InvalidTableFile;
    if (footer.metadata_offset != header.entry_data_start + header.entry_data_len) return error.InvalidTableFile;
    const footer_offset = raw.len - footer_len;
    if (footer.metadata_offset > footer_offset or footer.metadata_len != footer_offset - footer.metadata_offset)
        return error.InvalidTableFile;
    const metadata = raw[footer.metadata_offset..footer_offset];
    if (try checksumWithAdmission(metadata, control) != footer.metadata_checksum) return error.InvalidTableFile;
    var index = try decodeFooterMetadataAlloc(
        allocator,
        metadata,
        header.entry_data_start,
        footer.entry_count,
        footer.entry_data_len,
        footer.metadata_offset - header.entry_data_start,
        header.version,
        control,
    );
    errdefer index.deinit(allocator);
    try validateRawBlockChecksums(raw, &index, control);
    if (control) |budget| try budget.checkpoint();
    return index;
}

fn decodeFooterMetadataAlloc(
    allocator: std.mem.Allocator,
    metadata: []const u8,
    entry_data_start: usize,
    entry_count: usize,
    entry_data_len: usize,
    physical_entry_data_len: usize,
    expected_version: ?u32,
    control: ?read_admission.Port,
) !TableIndex {
    if (control) |budget| try budget.checkpoint();
    var cursor: usize = 0;
    try consumePackedOffsetsMarker(metadata, &cursor);
    if (expected_version) |expected| if (expected != version) return error.UnsupportedVersion;
    const offsets: []u32 = &.{};
    var block_offsets: []u16 = &.{};
    errdefer allocator.free(block_offsets);
    if (entry_count > (metadata.len - cursor) / @sizeOf(u16))
        return error.InvalidTableFile;
    block_offsets = try allocator.alloc(u16, entry_count);
    for (block_offsets, 0..) |*offset, i| {
        if (i % 256 == 0) if (control) |budget| try budget.checkpoint();
        offset.* = try readU16(metadata, &cursor);
    }

    const bloom_len: usize = @intCast(try readU32(metadata, &cursor));
    const encoded_filter = try readSlice(metadata, &cursor, bloom_len);
    var filter = try bloom.OwnedFilter.decodeAlloc(allocator, encoded_filter);
    errdefer filter.deinit(allocator);
    const blocks = try decodeBlockMetasAlloc(allocator, metadata, &cursor, entry_count, entry_data_len, control);
    errdefer {
        for (blocks) |*block| block.deinit(allocator);
        allocator.free(blocks);
    }
    try decodeBlockFiltersAlloc(allocator, metadata, &cursor, blocks, control);
    try skipBlockHashSlots(metadata, &cursor, blocks, control);
    try decodeBlockPhysicalMetas(metadata, &cursor, blocks, control);
    try validateBlockPhysicalCoverage(blocks, physical_entry_data_len);
    try decodeBlockSmallestKeysAlloc(allocator, metadata, &cursor, blocks, control);
    var prefix_extractor: PrefixExtractor = .none;
    var prefix_filter: ?bloom.OwnedFilter = null;
    errdefer if (prefix_filter) |*filter_ptr| filter_ptr.deinit(allocator);
    prefix_extractor = try decodePrefixExtractor(try readU32(metadata, &cursor));
    const prefix_filter_len: usize = @intCast(try readU32(metadata, &cursor));
    if (prefix_filter_len > 0) {
        prefix_filter = try bloom.OwnedFilter.decodeAlloc(allocator, try readSlice(metadata, &cursor, prefix_filter_len));
    }
    try decodeBlockPrefixFiltersAlloc(allocator, metadata, &cursor, blocks, control);
    if (cursor != metadata.len) return error.InvalidTableFile;

    try validateEntryOffsets(offsets, block_offsets, blocks, entry_data_len, control);

    return .{
        .entry_offsets = offsets,
        .block_entry_offsets = block_offsets,
        .entry_data_start = entry_data_start,
        .entry_data_len = entry_data_len,
        .filter = filter,
        .prefix_extractor = prefix_extractor,
        .prefix_filter = prefix_filter,
        .blocks = blocks,
    };
}

fn validateEntryOffsets(
    offsets: []const u32,
    block_offsets: []const u16,
    blocks: []const TableIndex.BlockMeta,
    entry_data_len: usize,
    control: ?read_admission.Port,
) !void {
    const entry_count = @max(offsets.len, block_offsets.len);
    if (offsets.len != 0 and block_offsets.len != 0) return error.InvalidTableFile;
    if (entry_count == 0) {
        if (blocks.len != 0 or entry_data_len != 0) return error.InvalidTableFile;
        return;
    }
    if (blocks.len == 0) return error.InvalidTableFile;

    var expected_entry_index: usize = 0;
    for (blocks) |block| {
        if (control) |budget| try budget.checkpoint();
        if (@as(usize, block.first_entry_index) != expected_entry_index) return error.InvalidTableFile;
        const block_end = @as(usize, block.first_entry_index) + block.entry_count;
        if (block_end > entry_count) return error.InvalidTableFile;
        var previous_local: u32 = 0;
        for (block.first_entry_index..block_end, 0..) |entry_index, local_index| {
            if (local_index % 256 == 0) if (control) |budget| try budget.checkpoint();
            const local = if (offsets.len != 0) blk: {
                if (offsets[entry_index] < block.relative_offset) return error.InvalidTableFile;
                break :blk offsets[entry_index] - block.relative_offset;
            } else @as(u32, block_offsets[entry_index]);
            if (local_index == 0 and local != 0) return error.InvalidTableFile;
            if (local_index > 0 and local <= previous_local) return error.InvalidTableFile;
            if (local >= block.len) return error.InvalidTableFile;
            previous_local = local;
        }
        expected_entry_index = block_end;
    }
    if (expected_entry_index != entry_count) return error.InvalidTableFile;
    const last_block = blocks[blocks.len - 1];
    const last_local: usize = if (offsets.len != 0)
        offsets[offsets.len - 1] - last_block.relative_offset
    else
        block_offsets[block_offsets.len - 1];
    if (@as(usize, last_block.relative_offset) + last_local >= entry_data_len) return error.InvalidTableFile;
}

fn decodeBlockMetasAlloc(
    allocator: std.mem.Allocator,
    metadata: []const u8,
    cursor: *usize,
    entry_count: usize,
    entry_data_len: usize,
    control: ?read_admission.Port,
) ![]TableIndex.BlockMeta {
    const block_count: usize = @intCast(try readU32(metadata, cursor));
    const minimum_block_metadata_bytes = @sizeOf(u32) * 6;
    if (block_count > entry_count or
        block_count > (metadata.len - cursor.*) / minimum_block_metadata_bytes)
    {
        return error.InvalidTableFile;
    }
    const blocks = try allocator.alloc(TableIndex.BlockMeta, block_count);
    errdefer allocator.free(blocks);

    var initialized: usize = 0;
    errdefer {
        for (blocks[0..initialized]) |*block| block.deinit(allocator);
    }

    var expected_entry_index: usize = 0;
    var expected_offset: usize = 0;
    for (blocks, 0..) |*block, block_index| {
        if (control) |budget| try budget.checkpoint();
        const relative_offset = try readU32(metadata, cursor);
        const len = try readU32(metadata, cursor);
        const first_entry_index = try readU32(metadata, cursor);
        const block_entry_count = try readU32(metadata, cursor);
        const largest_namespace_len = try readU32(metadata, cursor);
        const largest_key_len: usize = @intCast(try readU32(metadata, cursor));
        const largest_namespace_name = if (largest_namespace_len == std.math.maxInt(u32))
            null
        else
            try allocator.dupe(u8, try readSlice(metadata, cursor, largest_namespace_len));
        errdefer if (largest_namespace_name) |name| allocator.free(name);
        const largest_key = try allocator.dupe(u8, try readSlice(metadata, cursor, largest_key_len));
        errdefer allocator.free(largest_key);

        if (block_entry_count == 0) return error.InvalidTableFile;
        if (@as(usize, first_entry_index) != expected_entry_index) return error.InvalidTableFile;
        if (@as(usize, relative_offset) != expected_offset) return error.InvalidTableFile;
        if (@as(usize, first_entry_index) + block_entry_count > entry_count) return error.InvalidTableFile;
        if (@as(usize, relative_offset) + len > entry_data_len) return error.InvalidTableFile;
        if (block_index + 1 == block_count and @as(usize, relative_offset) + len != entry_data_len) return error.InvalidTableFile;
        block.* = .{
            .relative_offset = relative_offset,
            .len = len,
            .physical_relative_offset = relative_offset,
            .physical_len = len,
            .compression = .none,
            .first_entry_index = first_entry_index,
            .entry_count = block_entry_count,
            .smallest_namespace_name = null,
            .smallest_key = null,
            .largest_namespace_name = largest_namespace_name,
            .largest_key = largest_key,
            .hash_slots = &.{},
        };
        initialized += 1;
        expected_entry_index += block_entry_count;
        expected_offset += len;
    }

    if (expected_entry_index != entry_count) return error.InvalidTableFile;
    return blocks;
}

fn decodeBlockFiltersAlloc(
    allocator: std.mem.Allocator,
    metadata: []const u8,
    cursor: *usize,
    blocks: []TableIndex.BlockMeta,
    control: ?read_admission.Port,
) !void {
    const filter_count: usize = @intCast(try readU32(metadata, cursor));
    if (filter_count != blocks.len) return error.InvalidTableFile;
    for (blocks) |*block| {
        if (control) |budget| try budget.checkpoint();
        const filter_len: usize = @intCast(try readU32(metadata, cursor));
        const encoded_filter = try readSlice(metadata, cursor, filter_len);
        block.filter = try bloom.OwnedFilter.decodeAlloc(allocator, encoded_filter);
    }
}

fn skipBlockHashSlots(
    metadata: []const u8,
    cursor: *usize,
    blocks: []TableIndex.BlockMeta,
    control: ?read_admission.Port,
) !void {
    const hash_count: usize = @intCast(try readU32(metadata, cursor));
    if (hash_count != blocks.len) return error.InvalidTableFile;
    for (blocks) |_| {
        if (control) |budget| try budget.checkpoint();
        const slot_count: usize = @intCast(try readU32(metadata, cursor));
        // v9 readers never consulted this open-addressed lookup structure;
        // exact reads binary-search entry_offsets. Preserve old-run framing
        // validation without materializing hundreds of MiB of dead u32 slots
        // during DB.open.
        if (slot_count > (metadata.len - cursor.*) / @sizeOf(u32)) return error.InvalidTableFile;
        _ = try readSlice(metadata, cursor, slot_count * @sizeOf(u32));
    }
}

fn decodeBlockPhysicalMetas(
    metadata: []const u8,
    cursor: *usize,
    blocks: []TableIndex.BlockMeta,
    control: ?read_admission.Port,
) !void {
    const physical_count: usize = @intCast(try readU32(metadata, cursor));
    if (physical_count != blocks.len) return error.InvalidTableFile;
    var expected_physical_offset: usize = 0;
    for (blocks) |*block| {
        if (control) |budget| try budget.checkpoint();
        const physical_relative_offset = try readU32(metadata, cursor);
        const physical_len = try readU32(metadata, cursor);
        const compression_raw = try readU32(metadata, cursor);
        const checksum = try readU32(metadata, cursor);
        const compression: BlockCompression = switch (compression_raw) {
            @backingInt(BlockCompression.none) => .none,
            @backingInt(BlockCompression.snappy) => .snappy,
            @backingInt(BlockCompression.prefix) => .prefix,
            @backingInt(BlockCompression.prefix_snappy) => .prefix_snappy,
            else => return error.InvalidTableFile,
        };
        if (physical_len == 0) return error.InvalidTableFile;
        if (@as(usize, physical_relative_offset) != expected_physical_offset) return error.InvalidTableFile;
        block.physical_relative_offset = physical_relative_offset;
        block.physical_len = physical_len;
        block.compression = compression;
        block.checksum = checksum;
        expected_physical_offset += physical_len;
    }
}

fn validateBlockPhysicalCoverage(blocks: []const TableIndex.BlockMeta, physical_entry_data_len: usize) !void {
    if (blocks.len == 0) {
        if (physical_entry_data_len != 0) return error.InvalidTableFile;
        return;
    }
    const last = blocks[blocks.len - 1];
    if (@as(usize, last.physical_relative_offset) + last.physical_len != physical_entry_data_len)
        return error.InvalidTableFile;
}

fn decodeBlockSmallestKeysAlloc(
    allocator: std.mem.Allocator,
    metadata: []const u8,
    cursor: *usize,
    blocks: []TableIndex.BlockMeta,
    control: ?read_admission.Port,
) !void {
    const smallest_count: usize = @intCast(try readU32(metadata, cursor));
    if (smallest_count != blocks.len) return error.InvalidTableFile;
    for (blocks) |*block| {
        if (control) |budget| try budget.checkpoint();
        const smallest_namespace_len = try readU32(metadata, cursor);
        const smallest_key_len: usize = @intCast(try readU32(metadata, cursor));
        const smallest_namespace_name = if (smallest_namespace_len == std.math.maxInt(u32))
            null
        else
            try allocator.dupe(u8, try readSlice(metadata, cursor, smallest_namespace_len));
        errdefer if (smallest_namespace_name) |name| allocator.free(name);
        const smallest_key = try allocator.dupe(u8, try readSlice(metadata, cursor, smallest_key_len));
        errdefer allocator.free(smallest_key);
        if (compareKeyBound(smallest_namespace_name, smallest_key, block.largest_namespace_name, block.largest_key) == .gt) {
            return error.InvalidTableFile;
        }
        block.smallest_namespace_name = smallest_namespace_name;
        block.smallest_key = smallest_key;
    }
}

fn decodePrefixExtractor(raw: u32) !PrefixExtractor {
    return switch (raw) {
        @backingInt(PrefixExtractor.none) => .none,
        @backingInt(PrefixExtractor.first_separator) => .first_separator,
        else => error.InvalidTableFile,
    };
}

fn decodeBlockPrefixFiltersAlloc(
    allocator: std.mem.Allocator,
    metadata: []const u8,
    cursor: *usize,
    blocks: []TableIndex.BlockMeta,
    control: ?read_admission.Port,
) !void {
    const filter_count: usize = @intCast(try readU32(metadata, cursor));
    if (filter_count != blocks.len) return error.InvalidTableFile;
    for (blocks) |*block| {
        if (control) |budget| try budget.checkpoint();
        const filter_len: usize = @intCast(try readU32(metadata, cursor));
        if (filter_len == 0) {
            block.prefix_filter = null;
            continue;
        }
        block.prefix_filter = try bloom.OwnedFilter.decodeAlloc(allocator, try readSlice(metadata, cursor, filter_len));
    }
}

pub fn parseEntryAt(raw: []const u8, absolute_offset: usize) !Entry {
    var cursor = absolute_offset;
    const tombstone = switch (try readByte(raw, &cursor)) {
        0 => false,
        1 => true,
        else => return error.InvalidTableFile,
    };
    const namespace_len: usize = @intCast(try readU32(raw, &cursor));
    const key_len: usize = @intCast(try readU32(raw, &cursor));
    const value_len: usize = @intCast(try readU32(raw, &cursor));
    return .{
        .namespace_name = if (namespace_len > 0) try readSlice(raw, &cursor, namespace_len) else null,
        .key = try readSlice(raw, &cursor, key_len),
        .value = try readSlice(raw, &cursor, value_len),
        .tombstone = tombstone,
    };
}

/// Bound to one immutable indexed raw block. Nearby forward probes gallop
/// within eight rows; a sparse jump disables probing until density returns.
/// The caller resets this reader whenever its block owner changes.
pub const IndexedPointReader = struct {
    current: ?BorrowedDecoded.PositionedEntry = null,
    probe_limit: u8 = 1,
    test_parsed_rows: if (@import("builtin").is_test) usize else void = if (@import("builtin").is_test) 0 else {},

    fn at(self: *@This(), index: *const TableIndex, raw: []const u8, block_index: usize, row: usize) !BorrowedDecoded.PositionedEntry {
        if (@import("builtin").is_test) self.test_parsed_rows += 1;
        const offset = index.entryStartInBlock(row, block_index) - index.blocks[block_index].relative_offset;
        return .{ .index = row, .entry = try parseEntryAt(raw, offset) };
    }

    fn remember(self: *@This(), previous: ?BorrowedDecoded.PositionedEntry, row: BorrowedDecoded.PositionedEntry) void {
        self.probe_limit = if (previous) |prev| (if (row.index >= prev.index and row.index - prev.index <= 8) @as(u8, 8) else 0) else 1;
        self.current = row;
    }

    pub fn find(self: *@This(), index: *const TableIndex, raw: []const u8, block_index: usize, namespace_name: ?[]const u8, key: []const u8) !?BorrowedDecoded.PositionedEntry {
        errdefer {
            self.current = null;
            self.probe_limit = 1;
        }
        const block = index.blocks[block_index];
        const previous = self.current;
        var upper: ?BorrowedDecoded.PositionedEntry = null;
        var lo: usize = block.first_entry_index;
        var hi: usize = lo + block.entry_count;
        if (previous) |current| {
            switch (compareEntryTo(current.entry, namespace_name, key)) {
                .eq => return current,
                .gt => {
                    hi = current.index;
                    upper = current;
                },
                .lt => {
                    lo = current.index + 1;
                    if (self.probe_limit != 0) {
                        var step: usize = 1;
                        while (step <= self.probe_limit and step < hi - current.index) : (step *= 2) {
                            const row = try self.at(index, raw, block_index, current.index + step);
                            if (compareEntryTo(row.entry, namespace_name, key) != .lt) {
                                hi = row.index;
                                upper = row;
                                break;
                            }
                            lo = row.index + 1;
                        }
                    }
                },
            }
        }
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            const row = try self.at(index, raw, block_index, mid);
            if (compareEntryTo(row.entry, namespace_name, key) == .lt) lo = mid + 1 else hi = mid;
        }
        if (lo == block.first_entry_index + block.entry_count) {
            if (block.entry_count != 0) self.remember(previous, try self.at(index, raw, block_index, lo - 1));
            return null;
        }
        const row = if (upper != null and upper.?.index == lo) upper.? else try self.at(index, raw, block_index, lo);
        self.remember(previous, row);
        return if (compareEntryTo(row.entry, namespace_name, key) == .eq) row else null;
    }
};

pub fn findExactEntryInBlock(
    index: *const TableIndex,
    raw_block: []const u8,
    block_index: usize,
    namespace_name: ?[]const u8,
    key: []const u8,
) !?BorrowedDecoded.PositionedEntry {
    const positioned = try lowerBoundPositionInBlock(index, raw_block, block_index, namespace_name, key, true) orelse return null;
    if (compareEntryTo(positioned.entry, namespace_name, key) != .eq) return null;
    return positioned;
}

fn compareEntryTo(entry: Entry, namespace_name: ?[]const u8, key: []const u8) std.math.Order {
    const namespace_order = compareNamespace(entry.namespace_name, namespace_name);
    if (namespace_order != .eq) return namespace_order;
    return std.mem.order(u8, entry.key, key);
}

fn compareKeyBound(lhs_namespace_name: ?[]const u8, lhs_key: []const u8, rhs_namespace_name: ?[]const u8, rhs_key: []const u8) std.math.Order {
    const namespace_order = compareNamespace(lhs_namespace_name, rhs_namespace_name);
    if (namespace_order != .eq) return namespace_order;
    return std.mem.order(u8, lhs_key, rhs_key);
}

fn compareNamespace(lhs: ?[]const u8, rhs: ?[]const u8) std.math.Order {
    if (lhs == null and rhs == null) return .eq;
    if (lhs == null) return .lt;
    if (rhs == null) return .gt;
    return std.mem.order(u8, lhs.?, rhs.?);
}

fn entryHashes(namespace_name: ?[]const u8, key: []const u8) [2]u64 {
    return .{
        hashEntryWithSeed(0x243f6a8885a308d3, namespace_name, key),
        hashEntryWithSeed(0x13198a2e03707344, namespace_name, key),
    };
}

fn entryPrefixHashes(extractor: PrefixExtractor, namespace_name: ?[]const u8, key: []const u8) ?[2]u64 {
    const prefix = extractKeyPrefix(extractor, key) orelse return null;
    return prefixHashes(namespace_name, prefix);
}

fn prefixHashes(namespace_name: ?[]const u8, prefix: []const u8) [2]u64 {
    return .{
        hashEntryWithSeed(0x452821e638d01377, namespace_name, prefix),
        hashEntryWithSeed(0xbe5466cf34e90c6c, namespace_name, prefix),
    };
}

fn hashEntryWithSeed(seed: u64, namespace_name: ?[]const u8, key: []const u8) u64 {
    var hasher = std.hash.Wyhash.init(seed);
    const namespace_len: u32 = if (namespace_name) |name| @intCast(name.len) else std.math.maxInt(u32);
    const key_len: u32 = @intCast(key.len);
    var buf: [4]u8 = undefined;

    std.mem.writeInt(u32, &buf, namespace_len, .little);
    hasher.update(&buf);
    if (namespace_name) |name| hasher.update(name);

    std.mem.writeInt(u32, &buf, key_len, .little);
    hasher.update(&buf);
    hasher.update(key);
    return hasher.final();
}

fn encodeV3ForTest(allocator: std.mem.Allocator, entries: []const Entry) ![]u8 {
    var filter = try buildFilterAlloc(allocator, entries, .{});
    defer filter.deinit(allocator);
    const encoded_filter = try filter.encodeAlloc(allocator);
    defer allocator.free(encoded_filter);

    var entry_offsets = std.ArrayListUnmanaged(u32).empty;
    defer entry_offsets.deinit(allocator);
    try entry_offsets.ensureTotalCapacity(allocator, entries.len);

    var entry_bytes = std.ArrayListUnmanaged(u8).empty;
    defer entry_bytes.deinit(allocator);

    for (entries) |entry| {
        try entry_offsets.append(allocator, @intCast(entry_bytes.items.len));
        try entry_bytes.append(allocator, @intFromBool(entry.tombstone));
        try appendU32(allocator, &entry_bytes, if (entry.namespace_name) |name| @intCast(name.len) else 0);
        try appendU32(allocator, &entry_bytes, @intCast(entry.key.len));
        try appendU32(allocator, &entry_bytes, @intCast(entry.value.len));
        if (entry.namespace_name) |name| try entry_bytes.appendSlice(allocator, name);
        try entry_bytes.appendSlice(allocator, entry.key);
        try entry_bytes.appendSlice(allocator, entry.value);
    }

    var bytes = std.ArrayListUnmanaged(u8).empty;
    errdefer bytes.deinit(allocator);

    try bytes.appendSlice(allocator, magic);
    try appendU32(allocator, &bytes, 3);
    try appendU32(allocator, &bytes, @intCast(entries.len));
    try appendU32(allocator, &bytes, @intCast(entry_bytes.items.len));
    for (entry_offsets.items) |offset| try appendU32(allocator, &bytes, offset);
    try bytes.appendSlice(allocator, entry_bytes.items);
    try appendU32(allocator, &bytes, @intCast(encoded_filter.len));
    try bytes.appendSlice(allocator, encoded_filter);

    return try bytes.toOwnedSlice(allocator);
}

test "table file codec round trips namespaced entries" {
    const entries = [_]Entry{
        .{ .key = "a", .value = "1" },
        .{ .namespace_name = "docs", .key = "doc:a", .value = "A" },
        .{ .namespace_name = "docs", .key = "doc:b", .value = "", .tombstone = true },
    };

    const encoded = try encodeAlloc(std.testing.allocator, &entries);
    defer std.testing.allocator.free(encoded);

    var decoded = try decodeAlloc(std.testing.allocator, encoded);
    defer decoded.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, entries.len), decoded.entries.len);
    try std.testing.expectEqual(@as(?[]const u8, null), decoded.entries[0].namespace_name);
    try std.testing.expectEqualStrings("a", decoded.entries[0].key);
    try std.testing.expectEqualStrings("1", decoded.entries[0].value);
    try std.testing.expectEqualStrings("docs", decoded.entries[1].namespace_name.?);
    try std.testing.expectEqualStrings("doc:a", decoded.entries[1].key);
    try std.testing.expectEqualStrings("A", decoded.entries[1].value);
    try std.testing.expect(decoded.entries[2].tombstone);
    try std.testing.expect(maybeContains(decoded.filter, null, "a"));
    try std.testing.expect(maybeContains(decoded.filter, "docs", "doc:a"));
    try std.testing.expect(maybeContains(decoded.filter, "docs", "doc:b"));
}

test "table file rejects corrupt footer metadata before using bloom filters" {
    const entries = [_]Entry{
        .{ .namespace_name = "docs", .key = "doc:a", .value = "A" },
        .{ .namespace_name = "docs", .key = "doc:b", .value = "B" },
    };
    const encoded = try encodeAlloc(std.testing.allocator, &entries);
    defer std.testing.allocator.free(encoded);
    const footer = try decodeFooter(encoded);
    encoded[footer.metadata_offset + packed_offsets_magic.len] ^= 0x40;

    try std.testing.expectError(
        error.InvalidTableFile,
        decodeIndexAlloc(std.testing.allocator, encoded),
    );
    try std.testing.expectError(
        error.InvalidTableFile,
        decodeIndexFromFooterAlloc(
            std.testing.allocator,
            footer,
            encoded[footer.metadata_offset .. footer.metadata_offset + footer.metadata_len],
        ),
    );
}

test "table file rejects forged footer entry count before allocating offsets" {
    const entries = [_]Entry{
        .{ .namespace_name = "docs", .key = "doc:a", .value = "A" },
    };
    const encoded = try encodeAlloc(std.testing.allocator, &entries);
    defer std.testing.allocator.free(encoded);

    const footer_bytes = encoded[encoded.len - footer_len ..];
    std.mem.writeInt(u32, footer_bytes[32..36], std.math.maxInt(u32), .little);
    std.mem.writeInt(u32, footer_bytes[44..48], Crc32.hash(footer_bytes[0..44]), .little);
    const footer = try decodeFooterBytes(footer_bytes);
    const metadata = encoded[footer.metadata_offset .. footer.metadata_offset + footer.metadata_len];

    try std.testing.expectError(
        error.InvalidTableFile,
        decodeIndexFromFooterAlloc(std.testing.allocator, footer, metadata),
    );
}

test "table file rejects corrupt physical blocks before decoding or caching" {
    const entries = [_]Entry{
        .{ .namespace_name = "docs", .key = "doc:a", .value = "A" },
        .{ .namespace_name = "docs", .key = "doc:b", .value = "B" },
    };
    const encoded = try encodeAlloc(std.testing.allocator, &entries);
    defer std.testing.allocator.free(encoded);
    const footer = try decodeFooter(encoded);
    var footer_index = try decodeIndexFromFooterAlloc(
        std.testing.allocator,
        footer,
        encoded[footer.metadata_offset .. footer.metadata_offset + footer.metadata_len],
    );
    defer footer_index.deinit(std.testing.allocator);
    const window = footer_index.blockWindow(0);
    const physical_offset = footer_index.entry_data_start + window.physicalRelativeOffset();
    encoded[physical_offset] ^= 0x20;

    try std.testing.expectError(
        error.TableBlockChecksumMismatch,
        decodeIndexAlloc(std.testing.allocator, encoded),
    );
    try std.testing.expectError(
        error.TableBlockChecksumMismatch,
        decodeWindowFromRawAlloc(std.testing.allocator, encoded, &footer_index, window),
    );
}

test "table file v11 checksummed footer metadata decodes through footer" {
    const entries = [_]Entry{
        .{ .namespace_name = "docs", .key = "doc:a", .value = "A" },
        .{ .namespace_name = "docs", .key = "doc:b", .value = "B" },
    };

    const encoded = try encodeAlloc(std.testing.allocator, &entries);
    defer std.testing.allocator.free(encoded);

    const footer = try decodeFooter(encoded);
    try std.testing.expectEqual(@as(usize, entries.len), footer.entry_count);
    try std.testing.expectEqual(@as(usize, header_len), footer.metadata_offset - footer.entry_data_len);

    const footer_bytes = encoded[encoded.len - footer_len ..];
    try std.testing.expect(hasFooterMagic(footer_bytes));

    var index = try decodeIndexFromFooterAlloc(
        std.testing.allocator,
        footer,
        encoded[footer.metadata_offset .. footer.metadata_offset + footer.metadata_len],
    );
    defer index.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, entries.len), index.entryCount());
    try std.testing.expectEqual(@as(usize, entries.len), index.block_entry_offsets.len);
    try std.testing.expect(maybeContains(index.borrowFilter(), "docs", "doc:a"));
    try std.testing.expect(maybeContains(index.borrowFilter(), "docs", "doc:b"));

    var sequential = try decodeSequentialIndexFromFooterAlloc(
        std.testing.allocator,
        footer,
        encoded[footer.metadata_offset .. footer.metadata_offset + footer.metadata_len],
    );
    defer sequential.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, entries.len), sequential.entry_count);
    try std.testing.expectEqual(index.blocks.len, sequential.blocks.len);
    try std.testing.expectEqual(index.blocks[0].entry_count, sequential.blocks[0].entry_count);
    try std.testing.expectEqual(index.blockWindow(0), sequential.blocks[0].window);
}

test "table file footer metadata includes block bounds for point reads" {
    const allocator = std.testing.allocator;
    const block_value = try allocator.alloc(u8, default_block_size / 4);
    defer allocator.free(block_value);
    @memset(block_value, 'v');

    const entries = try allocator.alloc(Entry, 6);
    defer allocator.free(entries);
    var owned_keys = try allocator.alloc([]u8, entries.len);
    defer {
        for (owned_keys) |key| allocator.free(key);
        allocator.free(owned_keys);
    }

    for (entries, 0..) |*entry, i| {
        const key = try std.fmt.allocPrint(allocator, "doc:{d:0>3}", .{i});
        owned_keys[i] = key;
        entry.* = .{
            .namespace_name = "docs",
            .key = key,
            .value = block_value,
        };
    }

    const encoded = try encodeAlloc(allocator, entries);
    defer allocator.free(encoded);

    var index = try decodeIndexAlloc(allocator, encoded);
    defer index.deinit(allocator);

    try std.testing.expect(index.blockCount() > 1);
    const target_block = index.findBlockIndex("docs", "doc:005") orelse return error.TestUnexpectedResult;
    try std.testing.expect(target_block > 0);
    try std.testing.expectEqual(@as(u32, 3), index.blocks[target_block].first_entry_index);
    try std.testing.expectEqual(@as(u32, 3), index.blocks[target_block].entry_count);
    try std.testing.expectEqualStrings("doc:003", index.blocks[target_block].smallest_key.?);
    try std.testing.expectEqualStrings("doc:005", index.blocks[target_block].largest_key);
    try std.testing.expect(index.blocks[target_block].mayContainKeyByBounds("docs", "doc:004"));
    try std.testing.expect(!index.blocks[target_block].mayContainKeyByBounds("docs", "doc:002"));
    try std.testing.expect(index.blocks[target_block].rangeMayOverlap("docs", "doc:004", "docs", "doc:004"));
    try std.testing.expect(!index.blocks[target_block].rangeMayOverlap("docs", "doc:000", "docs", "doc:002"));
    try std.testing.expect(index.blocks[target_block].filter != null);
    try std.testing.expectEqual(@as(usize, 0), index.blocks[target_block].hash_slots.len);
    try std.testing.expect(index.blocks[target_block].maybeContains("docs", "doc:005"));

    const raw_block = try decodeWindowFromRawAlloc(allocator, encoded, &index, index.blockWindow(target_block));
    defer allocator.free(raw_block);
    const found = try findExactEntryInBlock(&index, raw_block, target_block, "docs", "doc:005");
    try std.testing.expect(found != null);
    try std.testing.expectEqual(@as(usize, 5), found.?.index);
    try std.testing.expectEqualStrings("doc:005", found.?.entry.key);

    const borrowed_raw = try allocator.dupe(u8, encoded);
    var borrowed = try decodeBorrowedOwnedAlloc(allocator, borrowed_raw);
    defer borrowed.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 4), try borrowed.lowerBound("docs", "doc:004"));
    const positioned = try borrowed.seekAtOrAfterFromIndex("docs", "doc:004", 0);
    try std.testing.expect(positioned != null);
    try std.testing.expectEqual(@as(usize, 4), positioned.?.index);
    try std.testing.expectEqualStrings("doc:004", positioned.?.entry.key);
}

test "table file footer metadata includes prefix bloom filters" {
    const allocator = std.testing.allocator;
    var entries = [_]Entry{
        .{ .namespace_name = "docs", .key = "tenant-a:001", .value = "a" },
        .{ .namespace_name = "docs", .key = "tenant-a:002", .value = "b" },
        .{ .namespace_name = "docs", .key = "tenant-c:001", .value = "c" },
    };

    const encoded = try encodeAlloc(allocator, &entries);
    defer allocator.free(encoded);

    var index = try decodeIndexAlloc(allocator, encoded);
    defer index.deinit(allocator);

    var header_cursor: usize = 0;
    try std.testing.expectEqual(@as(u32, version), (try decodeHeader(encoded, &header_cursor)).version);
    try std.testing.expectEqual(PrefixExtractor.first_separator, index.prefix_extractor);
    try std.testing.expect(index.prefix_filter != null);
    try std.testing.expect(index.maybeContainsPrefix("docs", "tenant-a:"));
    try std.testing.expect(index.maybeContainsPrefix("docs", "tenant-c:"));
    try std.testing.expect(!index.maybeContainsPrefix("docs", "tenant-b:"));
    try std.testing.expect(index.blocks.len > 0);
    try std.testing.expect(index.blocks[0].prefix_filter != null);
    try std.testing.expect(index.blocks[0].maybeContainsPrefix("docs", "tenant-a:"));
    try std.testing.expect(!index.blocks[0].maybeContainsPrefix("docs", "tenant-b:"));
    try std.testing.expectEqualStrings("tenant-a:", extractKeyPrefix(default_prefix_extractor, "tenant-a:001").?);
    try std.testing.expect(upperBoundWithinPrefix("tenant-a:", "tenant-a;"));
}

test "table file prefix blooms scale with distinct prefixes rather than entries" {
    const allocator = std.testing.allocator;
    const entries = @as([128]Entry, @splat(.{ .namespace_name = "docs", .key = "doc:repeated", .value = "value" }));

    var filter = try buildPrefixFilterAlloc(allocator, &entries, default_prefix_extractor, default_filter_config);
    defer filter.deinit(allocator);

    // One distinct prefix uses the Bloom implementation's 64-bit minimum.
    // Sizing by all 128 entries would instead allocate 2,048 bits.
    try std.testing.expectEqual(@as(usize, 8), filter.bytes.len);
    const hashes = prefixHashes("docs", "doc:");
    try std.testing.expect(filter.maybeContainsHashes(hashes[0], hashes[1]));

    const binary_entries = [_]Entry{
        .{ .key = "a", .value = "1" },
        .{ .key = "b", .value = "2" },
    };
    var binary_filter = try buildPrefixFilterAlloc(allocator, &binary_entries, default_prefix_extractor, default_filter_config);
    defer binary_filter.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 0), binary_filter.bytes.len);
}

test "streaming table prefix blooms ignore expected entry overestimates" {
    const allocator = std.testing.allocator;
    var sink_impl = MemoryTableSink.init(allocator);
    defer sink_impl.deinit();
    var sink = sink_impl.sink();

    var encoder = try StreamingEncoder.init(allocator, &sink, 1_000_000, .{});
    defer encoder.deinit();
    try encoder.appendEntry(.{ .namespace_name = "docs", .key = "doc:001", .value = "a" });
    try encoder.appendEntry(.{ .namespace_name = "docs", .key = "doc:002", .value = "b" });
    var result = try encoder.finish();
    defer result.filter.deinit(allocator);

    const encoded = try sink_impl.finishOwned();
    defer allocator.free(encoded);
    var index = try decodeIndexAlloc(allocator, encoded);
    defer index.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 8), index.prefix_filter.?.bytes.len);
    try std.testing.expectEqual(@as(usize, 8), index.blocks[0].prefix_filter.?.bytes.len);
    try std.testing.expect(index.maybeContainsPrefix("docs", "doc:"));
}

test "streaming table backs off rejected snappy probes without disabling prefix encoding" {
    const allocator = std.testing.allocator;
    const count = 160;
    const value_len = 2048;
    const entries = try allocator.alloc(Entry, count);
    defer allocator.free(entries);
    const keys = try allocator.alloc([24]u8, count);
    defer allocator.free(keys);
    const values = try allocator.alloc(u8, count * value_len);
    defer allocator.free(values);

    // High-entropy values model dense float payloads without depending on the
    // host PRNG or architecture. Each entry gets a distinct slice so Snappy
    // cannot win by referring to a repeated value from a neighboring entry.
    var random_state: u64 = 0x9e3779b97f4a7c15;
    for (values) |*byte| {
        random_state ^= random_state << 13;
        random_state ^= random_state >> 7;
        random_state ^= random_state << 17;
        byte.* = @truncate(random_state >> 24);
    }
    for (entries, 0..) |*entry, i| {
        const key = try std.fmt.bufPrint(keys[i][0..], "doc:{d:0>6}", .{i});
        entry.* = .{
            .namespace_name = "docs",
            .key = key,
            .value = values[i * value_len ..][0..value_len],
        };
    }

    var sink_impl = MemoryTableSink.init(allocator);
    defer sink_impl.deinit();
    var sink = sink_impl.sink();
    var encoder = try StreamingEncoder.init(allocator, &sink, entries.len, .{});
    defer encoder.deinit();
    for (entries) |entry| try encoder.appendEntry(entry);
    var result = try encoder.finish();
    defer result.filter.deinit(allocator);

    try std.testing.expect(encoder.blocks.items.len > snappy_rejection_streak_limit);
    try std.testing.expect(encoder.snappy_attempted_blocks >= snappy_rejection_streak_limit);
    try std.testing.expect(encoder.snappy_attempted_blocks < encoder.blocks.items.len);

    const encoded = try sink_impl.finishOwned();
    defer allocator.free(encoded);
    var decoded = try decodeAlloc(allocator, encoded);
    defer decoded.deinit(allocator);
    try std.testing.expectEqual(entries.len, decoded.entries.len);
    for (decoded.entries, entries) |actual, expected| {
        try std.testing.expectEqualStrings(expected.key, actual.key);
        try std.testing.expectEqualSlices(u8, expected.value, actual.value);
    }
}

test "table file adaptive snappy compression round trips repetitive blocks" {
    const allocator = std.testing.allocator;
    const repeated_value = try allocator.alloc(u8, 8192);
    defer allocator.free(repeated_value);
    @memset(repeated_value, 'x');

    var entries = [_]Entry{
        .{ .namespace_name = "docs", .key = "doc:000", .value = repeated_value },
        .{ .namespace_name = "docs", .key = "doc:001", .value = repeated_value },
        .{ .namespace_name = "docs", .key = "doc:002", .value = repeated_value },
        .{ .namespace_name = "docs", .key = "doc:003", .value = repeated_value },
    };

    const encoded = try encodeAlloc(allocator, &entries);
    defer allocator.free(encoded);

    var index = try decodeIndexAlloc(allocator, encoded);
    defer index.deinit(allocator);

    var compressed_blocks: usize = 0;
    for (index.blocks) |block| {
        if (block.compression == .snappy or block.compression == .prefix_snappy) {
            compressed_blocks += 1;
            try std.testing.expect(block.physicalLen() < block.len);
        }
    }
    try std.testing.expect(compressed_blocks > 0);

    var decoded = try decodeAlloc(allocator, encoded);
    defer decoded.deinit(allocator);
    try std.testing.expectEqual(entries.len, decoded.entries.len);
    for (decoded.entries, 0..) |entry, i| {
        try std.testing.expectEqualStrings(entries[i].key, entry.key);
        try std.testing.expectEqualStrings(repeated_value, entry.value);
    }

    var borrowed = try decodeBorrowedOwnedAlloc(allocator, try allocator.dupe(u8, encoded));
    defer borrowed.deinit(allocator);
    const positioned = try borrowed.seekAtOrAfterFromIndex("docs", "doc:002", 0);
    try std.testing.expect(positioned != null);
    try std.testing.expectEqual(@as(usize, 2), positioned.?.index);
    try std.testing.expectEqualStrings("doc:002", positioned.?.entry.key);
}

test "table file prefix-compressed blocks round trip long shared keys" {
    const allocator = std.testing.allocator;
    const count = 96;
    const entries = try allocator.alloc(Entry, count);
    defer allocator.free(entries);
    var keys = try allocator.alloc([]u8, count);
    defer {
        for (keys) |key| allocator.free(key);
        allocator.free(keys);
    }

    for (entries, 0..) |*entry, i| {
        const key = try std.fmt.allocPrint(
            allocator,
            "tenant:docs:collection:very-long-shared-prefix:segment:{d:0>6}:field:dense-vector",
            .{i},
        );
        keys[i] = key;
        entry.* = .{
            .namespace_name = "docs",
            .key = key,
            .value = "v",
        };
    }

    const encoded = try encodeAlloc(allocator, entries);
    defer allocator.free(encoded);

    var index = try decodeIndexAlloc(allocator, encoded);
    defer index.deinit(allocator);

    const footer = try decodeFooter(encoded);
    var sequential = try decodeSequentialIndexFromFooterAlloc(
        allocator,
        footer,
        encoded[footer.metadata_offset .. footer.metadata_offset + footer.metadata_len],
    );
    defer sequential.deinit(allocator);
    try std.testing.expectEqual(entries.len, sequential.entry_count);
    try std.testing.expectEqual(index.blocks.len, sequential.blocks.len);
    for (index.blocks, sequential.blocks) |block, sequential_block| {
        try std.testing.expectEqual(block.entry_count, sequential_block.entry_count);
        try std.testing.expectEqual(block.relative_offset, sequential_block.window.relative_offset);
        try std.testing.expectEqual(block.len, sequential_block.window.len);
        try std.testing.expectEqual(block.physicalRelativeOffset(), sequential_block.window.physicalRelativeOffset());
        try std.testing.expectEqual(block.physicalLen(), sequential_block.window.physicalLen());
        try std.testing.expectEqual(block.compression, sequential_block.window.compression);
    }

    var prefix_blocks: usize = 0;
    for (index.blocks) |block| {
        if (block.compression == .prefix or block.compression == .prefix_snappy) {
            prefix_blocks += 1;
            try std.testing.expect(block.physicalLen() < block.len);
        }
    }
    try std.testing.expect(prefix_blocks > 0);

    var decoded = try decodeAlloc(allocator, encoded);
    defer decoded.deinit(allocator);
    try std.testing.expectEqual(entries.len, decoded.entries.len);
    for (decoded.entries, 0..) |entry, i| {
        try std.testing.expectEqualStrings(entries[i].key, entry.key);
        try std.testing.expectEqualStrings(entries[i].value, entry.value);
    }

    const target_key = keys[37];
    const target_block = index.findBlockIndex("docs", target_key) orelse return error.TestUnexpectedResult;
    const block = index.blocks[target_block];
    const physical_start = index.entry_data_start + block.physicalRelativeOffset();
    const payload = encoded[physical_start..][0..block.physicalLen()];
    const direct = try findExactEntryInCompressedBlockPayloadAlloc(
        allocator,
        block.compression,
        payload,
        block.checksum,
        block.first_entry_index,
        "docs",
        target_key,
    );
    defer if (direct) |found| allocator.free(found.bytes);
    try std.testing.expect(direct != null);
    try std.testing.expectEqual(@as(usize, 37), direct.?.index);
    try std.testing.expectEqualStrings(target_key, direct.?.entry.key);
    try std.testing.expectEqualStrings("v", direct.?.entry.value);

    const absent = try findExactEntryInCompressedBlockPayloadAlloc(
        allocator,
        block.compression,
        payload,
        block.checksum,
        block.first_entry_index,
        "docs",
        "tenant:docs:collection:very-long-shared-prefix:segment:000037:field:absent",
    );
    try std.testing.expect(absent == null);
}

test "table file exact block lookup matches lower-bound lookup for path-like keys" {
    const allocator = std.testing.allocator;

    var entries = [_]Entry{
        .{ .namespace_name = "docs", .key = "docs/configuration.md", .value = "{\"title\":\"Configuration\"}" },
        .{ .namespace_name = "docs", .key = "docs/getting-started.md", .value = "{\"title\":\"Getting Started\"}" },
        .{ .namespace_name = "docs", .key = "docs/installation.md", .value = "{\"title\":\"Installation\"}" },
        .{ .namespace_name = "docs", .key = "docs/reference/api.md", .value = "{\"title\":\"API\"}" },
    };

    const encoded = try encodeAlloc(allocator, &entries);
    defer allocator.free(encoded);

    var index = try decodeIndexAlloc(allocator, encoded);
    defer index.deinit(allocator);

    const key = "docs/getting-started.md";
    const target_block = index.findBlockIndex("docs", key) orelse return error.TestUnexpectedResult;
    const raw_block = try decodeWindowFromRawAlloc(allocator, encoded, &index, index.blockWindow(target_block));
    defer allocator.free(raw_block);

    const exact = try findExactEntryInBlock(&index, raw_block, target_block, "docs", key);
    try std.testing.expect(exact != null);
    try std.testing.expectEqualStrings(key, exact.?.entry.key);

    const positioned = try lowerBoundPositionInBlock(&index, raw_block, target_block, "docs", key, true);
    try std.testing.expect(positioned != null);
    try std.testing.expectEqualStrings(key, positioned.?.entry.key);
    try std.testing.expectEqual(exact.?.index, positioned.?.index);
}

test "table file compression can be disabled per encode options" {
    const allocator = std.testing.allocator;
    const repeated_value = try allocator.alloc(u8, 8192);
    defer allocator.free(repeated_value);
    @memset(repeated_value, 'z');

    var entries = [_]Entry{
        .{ .namespace_name = "docs", .key = "doc:000", .value = repeated_value },
        .{ .namespace_name = "docs", .key = "doc:001", .value = repeated_value },
    };

    var filter = try buildFilterAlloc(allocator, &entries, .{});
    defer filter.deinit(allocator);
    const encoded = try encodeWithFilterAllocOptions(allocator, &entries, filter, .{
        .block_compression = .none,
    });
    defer allocator.free(encoded);

    var index = try decodeIndexAlloc(allocator, encoded);
    defer index.deinit(allocator);
    for (index.blocks) |block| {
        try std.testing.expectEqual(BlockCompression.none, block.compression);
        try std.testing.expectEqual(block.relative_offset, block.physicalRelativeOffset());
        try std.testing.expectEqual(block.len, block.physicalLen());
    }
}

test "table file legacy v3 index decoder is rejected" {
    const entries = [_]Entry{
        .{ .key = "a", .value = "1" },
        .{ .namespace_name = "docs", .key = "doc:a", .value = "A" },
    };

    const encoded = try encodeV3ForTest(std.testing.allocator, &entries);
    defer std.testing.allocator.free(encoded);

    try std.testing.expectError(error.UnsupportedVersion, decodeIndexAlloc(std.testing.allocator, encoded));
}

test "table file codec rejects invalid header" {
    try std.testing.expectError(error.InvalidTableFile, decodeAlloc(std.testing.allocator, "bad"));
}

test "streaming table bounds avoid per-entry allocations" {
    const allocator = std.testing.allocator;
    var counting = std.testing.FailingAllocator.init(allocator, .{});
    var sink_impl = MemoryTableSink.init(allocator);
    defer sink_impl.deinit();
    var sink = sink_impl.sink();
    const entry_count = 4096;
    var encoder = try StreamingEncoder.init(counting.allocator(), &sink, entry_count, .{
        .block_compression = .none,
        .prefix_extractor = .none,
    });
    defer encoder.deinit();

    const allocations_before = counting.alloc_index;
    for (0..entry_count) |i| {
        var key: [8]u8 = undefined;
        std.mem.writeInt(u64, &key, @intCast(i), .big);
        try encoder.appendEntry(.{ .key = &key, .value = "", .tombstone = true });
    }
    var result = try encoder.finish();
    defer result.filter.deinit(counting.allocator());

    try std.testing.expectEqual(@as(usize, entry_count), result.entry_count);
    try std.testing.expect(encoder.blocks.items.len > 1);
    std.debug.print("\nSTREAMING_ALLOCATION_BOUND observed={d} exclusive_limit={d}\n", .{ counting.alloc_index - allocations_before, entry_count / 8 });
    try std.testing.expect(counting.alloc_index - allocations_before < entry_count / 8);
}

test "streaming table bounds own mutable inputs across blocks" {
    const allocator = std.testing.allocator;
    const namespaces = [_]?[]const u8{ null, "", "docs", "namespace-long" };
    const keys = [_][]const u8{ "", "aaaaaaaaaaaaaaaaaaaa", "b", "cc" };
    const value = @as([(default_block_size / 4)]u8, @splat('v'));
    var entries: [namespaces.len * keys.len]Entry = undefined;
    for (&entries, 0..) |*entry, i| {
        const tombstone = i % 3 == 1;
        entry.* = .{
            .namespace_name = namespaces[i / keys.len],
            .key = keys[i % keys.len],
            .value = if (tombstone) "" else &value,
            .tombstone = tombstone,
        };
    }

    for ([_]CompressionPolicy{ .none, .snappy_adaptive }) |policy| {
        var sink_impl = MemoryTableSink.init(allocator);
        defer sink_impl.deinit();
        var sink = sink_impl.sink();
        var encoder = try StreamingEncoder.init(allocator, &sink, entries.len, .{ .block_compression = policy });
        defer encoder.deinit();

        var namespace_buf: [32]u8 = undefined;
        var key_buf: [32]u8 = undefined;
        var value_buf: [value.len]u8 = undefined;
        for (entries) |entry| {
            const namespace_name = if (entry.namespace_name) |name| blk: {
                @memcpy(namespace_buf[0..name.len], name);
                break :blk namespace_buf[0..name.len];
            } else null;
            @memcpy(key_buf[0..entry.key.len], entry.key);
            @memcpy(value_buf[0..entry.value.len], entry.value);
            try encoder.appendEntry(.{
                .namespace_name = namespace_name,
                .key = key_buf[0..entry.key.len],
                .value = value_buf[0..entry.value.len],
                .tombstone = entry.tombstone,
            });
            @memset(&namespace_buf, 0xcc);
            @memset(&key_buf, 0xcc);
            @memset(&value_buf, 0xcc);
        }
        var result = try encoder.finish();
        defer result.filter.deinit(allocator);
        const encoded = try sink_impl.finishOwned();
        defer allocator.free(encoded);

        var filter = try buildFilterAlloc(allocator, &entries, default_filter_config);
        defer filter.deinit(allocator);
        const reference = try encodeWithFilterAllocOptions(allocator, &entries, filter, .{ .block_compression = policy });
        defer allocator.free(reference);
        try std.testing.expectEqualSlices(u8, reference, encoded);

        var decoded = try decodeAlloc(allocator, encoded);
        defer decoded.deinit(allocator);
        try std.testing.expectEqual(entries.len, decoded.entries.len);
        for (decoded.entries, entries) |actual, expected| {
            const expected_namespace: ?[]const u8 = if (expected.namespace_name) |name| (if (name.len == 0) null else name) else null;
            try std.testing.expectEqual(expected_namespace == null, actual.namespace_name == null);
            if (expected_namespace) |name| try std.testing.expectEqualStrings(name, actual.namespace_name.?);
            try std.testing.expectEqualStrings(expected.key, actual.key);
            try std.testing.expectEqualSlices(u8, expected.value, actual.value);
            try std.testing.expectEqual(expected.tombstone, actual.tombstone);
        }

        var index = try decodeIndexAlloc(allocator, encoded);
        defer index.deinit(allocator);
        try std.testing.expect(index.blockCount() > 1);
        for (index.blocks) |block| {
            const first = entries[block.first_entry_index];
            const last = entries[block.lastEntryIndex()];
            try std.testing.expectEqual(first.namespace_name == null, block.smallest_namespace_name == null);
            if (first.namespace_name) |name| try std.testing.expectEqualStrings(name, block.smallest_namespace_name.?);
            try std.testing.expectEqualStrings(first.key, block.smallest_key.?);
            try std.testing.expectEqual(last.namespace_name == null, block.largest_namespace_name == null);
            if (last.namespace_name) |name| try std.testing.expectEqualStrings(name, block.largest_namespace_name.?);
            try std.testing.expectEqualStrings(last.key, block.largest_key);
        }
    }
}

test "streaming table largest bounds clean up allocation failures" {
    const Fixture = struct {
        fn run(allocator: std.mem.Allocator) !void {
            var sink_impl = MemoryTableSink.init(std.testing.allocator);
            defer sink_impl.deinit();
            var sink = sink_impl.sink();
            var encoder = try StreamingEncoder.init(allocator, &sink, 4, .{});
            defer encoder.deinit();

            try encoder.setBlockLargest(.{ .key = "", .value = "" });
            try encoder.setBlockLargest(.{ .namespace_name = "", .key = "a", .value = "" });
            try encoder.setBlockLargest(.{ .namespace_name = "docs", .key = "long-key-before-growth", .value = "" });
            try encoder.setBlockLargest(.{ .namespace_name = "namespace-after-growth", .key = "b", .value = "" });
            try encoder.setBlockLargest(.{ .key = "c", .value = "" });
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "streaming table unnamed block publication cleans up allocation failures" {
    const Fixture = struct {
        fn run(allocator: std.mem.Allocator) !void {
            var sink_impl = MemoryTableSink.init(std.testing.allocator);
            defer sink_impl.deinit();
            var sink = sink_impl.sink();
            var encoder = try StreamingEncoder.init(allocator, &sink, 4, .{ .block_compression = .none });
            defer encoder.deinit();
            const value = @as([(default_block_size / 2)]u8, @splat('v'));
            for ([_][]const u8{ "", "aaaaaaaaaaaaaaaaaaaa", "b", "cc" }) |key| {
                try encoder.appendEntry(.{ .key = key, .value = &value });
            }
            var result = try encoder.finish();
            defer result.filter.deinit(allocator);
            try std.testing.expectEqual(@as(usize, 4), result.entry_count);
            try std.testing.expectEqual(@as(usize, 4), encoder.blocks.items.len);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "table file prefix full decoder reconstructs into output without key scratch" {
    const a = std.testing.allocator;
    var raw = std.ArrayListUnmanaged(u8).empty;
    defer raw.deinit(a);
    var key: [4096]u8 = @splat('k');
    for (0..40) |i| {
        key[key.len - 1] = @intCast(i);
        try appendEntryBytesToList(a, &raw, .{
            .namespace_name = if (i % 2 == 0) "docs" else null,
            .key = &key,
            .value = if (i % 3 == 0) "" else "value",
            .tombstone = i % 3 == 0,
        });
    }
    const encoded = try encodePrefixCompressedBlockAlloc(a, raw.items);
    defer a.free(encoded);
    var rejected = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    const decoded = try decodePrefixCompressedBlockAlloc(a, rejected.allocator(), encoded, raw.items.len);
    defer a.free(decoded);
    try std.testing.expectEqualSlices(u8, raw.items, decoded);
    try std.testing.expectEqual(@as(usize, 0), rejected.alloc_index);
    try std.testing.expectError(error.InvalidTableFile, decodePrefixCompressedBlockAlloc(a, rejected.allocator(), encoded, raw.items.len - 1));
    try std.testing.expectError(error.InvalidTableFile, decodePrefixCompressedBlockAlloc(a, rejected.allocator(), encoded[0 .. encoded.len - 1], raw.items.len));
    std.debug.print("prefix full decoder: entries=40 key_bytes=4096 scratch_allocations=0\n", .{});
}

test "table file prefix snappy expansion bound allows worst restart overhead" {
    const a = std.testing.allocator;
    const count = 96;
    var payload = std.ArrayListUnmanaged(u8).empty;
    defer payload.deinit(a);
    try payload.appendSlice(a, prefix_block_magic);
    try appendU32(a, &payload, count);
    try appendU32(a, &payload, 1);
    try appendU32(a, &payload, count);
    try appendU32(a, &payload, 17 * count);
    for (0..count) |_| {
        try payload.append(a, 1);
        for (0..4) |_| try appendU32(a, &payload, 0);
    }
    for (0..count) |i| try appendU32(a, &payload, @intCast(i * 17));
    const encoded = try snappy.encode(a, payload.items);
    defer a.free(encoded);
    try validatePrefixDecodedSize(encoded, 13 * count);
    const decoded = try decodeBlockPayloadWithScratchAlloc(a, a, .prefix_snappy, encoded, 13 * count, Crc32.hash(encoded));
    defer a.free(decoded);
    try std.testing.expectEqual(@as(usize, 13 * count), decoded.len);
    for (0..count) |i| {
        try std.testing.expectEqual(@as(u8, 1), decoded[i * 13]);
        for (decoded[i * 13 + 1 .. (i + 1) * 13]) |byte| try std.testing.expectEqual(@as(u8, 0), byte);
    }
    try std.testing.expectError(error.InvalidTableFile, validatePrefixDecodedSize(encoded, 1));
}

test "table file prefix point lookup borrows restart keys and reconstructs in place" {
    const a = std.testing.allocator;
    const Budget = @import("../lite/test_allocator.zig").BudgetAllocator;
    var raw = std.ArrayListUnmanaged(u8).empty;
    defer raw.deinit(a);
    var key: [4096]u8 = @splat('k');
    for (0..40) |i| {
        key[key.len - 1] = @intCast(i * 2);
        try appendEntryBytesToList(a, &raw, .{ .namespace_name = "docs", .key = &key, .value = if (i % 3 == 0) "" else "value", .tombstone = i % 3 == 0 });
    }
    const payload = try encodePrefixCompressedBlockAlloc(a, raw.items);
    defer a.free(payload);
    const view = try parsePrefixBlockPayload(payload);
    try std.testing.expectEqual(@as(usize, 4096), (try prefixRestartEntry(view, 1)).key.len);
    for (0..40) |i| {
        key[key.len - 1] = @intCast(i * 2);
        var scratch = Budget{ .backing = a };
        const located = (try findExactEntryInPrefixPayloadAlloc(a, scratch.allocator(), raw.items.len, payload, 0, "docs", &key)).?;
        defer a.free(located.bytes);
        try std.testing.expectEqual(i, located.index);
        try std.testing.expectEqualSlices(u8, &key, located.entry.key);
        try std.testing.expectEqual(i % 3 == 0, located.entry.tombstone);
        try std.testing.expectEqualStrings(if (i % 3 == 0) "" else "value", located.entry.value);
        try std.testing.expectEqual(@as(usize, 1), scratch.alloc_calls);
        try std.testing.expectEqual(@as(usize, 0), scratch.live);
        if (i == 15) std.debug.print("prefix point: key_bytes=4096 scratch_allocations={d} scratch_peak={d}\n", .{ scratch.alloc_calls, scratch.peak });
        key[key.len - 1] += 1;
        try std.testing.expect((try findExactEntryInPrefixPayloadAlloc(a, scratch.allocator(), raw.items.len, payload, 0, "docs", &key)) == null);
    }
    // Key truncation and regrowth must retain only the current shared prefix.
    var encoded = std.ArrayListUnmanaged(u8).empty;
    defer encoded.deinit(a);
    for ([_][]const u8{ "abcdef", "abcq", "abcqrstuvwxyz" }, 0..) |suffix, i| {
        try encoded.append(a, 0);
        try appendU32(a, &encoded, 0);
        const shared: usize = if (i == 0) 0 else 3;
        try appendU32(a, &encoded, @intCast(shared));
        try appendU32(a, &encoded, @intCast(suffix.len - shared));
        try appendU32(a, &encoded, 0);
        try encoded.appendSlice(a, suffix[shared..]);
    }
    var buffer = std.ArrayListUnmanaged(u8).empty;
    defer buffer.deinit(a);
    var offset: usize = 0;
    for ([_][]const u8{ "abcdef", "abcq", "abcqrstuvwxyz" }) |expected| {
        const entry = try readPrefixBlockEntryInPlace(a, encoded.items, &offset, &buffer, 64);
        try std.testing.expectEqualStrings(expected, entry.key);
    }
}

test "prefix point reader reuses intervals and handles namespace misses duplicates and backwards seeks" {
    const a = std.testing.allocator;
    var raw: std.ArrayListUnmanaged(u8) = .empty;
    defer raw.deinit(a);
    var entries: [96]Entry = undefined;
    var keys: [96][40]u8 = undefined;
    for (&entries, 0..) |*entry, i| {
        entry.* = .{ .namespace_name = if (i < 48) "docs" else "graph", .key = try std.fmt.bufPrint(&keys[i], "shared-key:{d:0>4}", .{i % 48}), .value = if (i % 7 == 0) "" else "payload", .tombstone = i % 7 == 0 };
        try appendEntryBytesToList(a, &raw, entry.*);
    }
    const payload = try encodePrefixCompressedBlockAlloc(a, raw.items);
    defer a.free(payload);
    const Budget = @import("../lite/test_allocator.zig").BudgetAllocator;
    var backing = Budget{ .backing = a };
    var reader = try PrefixPointReader.init(payload, 700, raw.items.len);
    defer reader.deinit(backing.allocator());
    for (entries, 0..) |entry, i| {
        const found = (try reader.find(backing.allocator(), entry.namespace_name, entry.key)).?;
        try std.testing.expectEqual(700 + i, found.index);
        try std.testing.expectEqualStrings(entry.key, found.entry.key);
        try std.testing.expectEqualStrings(entry.value, found.entry.value);
        try std.testing.expectEqual(entry.tombstone, found.entry.tombstone);
        const calls = backing.alloc_calls;
        const duplicate = (try reader.find(backing.allocator(), entry.namespace_name, entry.key)).?;
        try std.testing.expectEqual(found.index, duplicate.index);
        try std.testing.expectEqual(calls, backing.alloc_calls);
    }
    try std.testing.expectEqual(@as(usize, 1), backing.alloc_calls);
    for ([_]usize{ 95, 0, 83, 5, 48, 16, 47 }) |i| {
        const found = (try reader.find(backing.allocator(), entries[i].namespace_name, entries[i].key)).?;
        try std.testing.expectEqual(700 + i, found.index);
    }
    try std.testing.expect(try reader.find(backing.allocator(), "docs", "shared-key:0005~") == null);
    try std.testing.expectEqual(@as(usize, 706), (try reader.find(backing.allocator(), "docs", entries[6].key)).?.index);
    try std.testing.expect(try reader.find(backing.allocator(), "aaa", "shared-key:0001") == null);
    try std.testing.expect(try reader.find(backing.allocator(), "zzz", "shared-key:0001") == null);
    try std.testing.expectEqual(@as(usize, 795), (try reader.find(backing.allocator(), "graph", entries[95].key)).?.index);
}

test "prefix point reader retries failed key growth and unwinds every allocation failure" {
    const a = std.testing.allocator;
    const large_key: [128 * 1024]u8 = @splat('z');
    var raw: std.ArrayListUnmanaged(u8) = .empty;
    defer raw.deinit(a);
    try appendEntryBytesToList(a, &raw, .{ .key = "a", .value = "small" });
    try appendEntryBytesToList(a, &raw, .{ .key = &large_key, .value = "large" });
    const payload = try encodePrefixCompressedBlockAlloc(a, raw.items);
    defer a.free(payload);
    const Budget = @import("../lite/test_allocator.zig").BudgetAllocator;
    var backing = Budget{ .backing = a, .limit = 1024 };
    var reader = try PrefixPointReader.init(payload, 0, raw.items.len);
    defer reader.deinit(backing.allocator());
    try std.testing.expectError(error.OutOfMemory, reader.find(backing.allocator(), null, &large_key));
    try std.testing.expect(reader.current == null);
    backing.limit = std.math.maxInt(usize);
    const found = (try reader.find(backing.allocator(), null, &large_key)).?;
    try std.testing.expectEqualStrings("large", found.entry.value);
    try std.testing.expectEqual(@as(usize, 1), found.index);
    const Fixture = struct {
        fn run(scratch: std.mem.Allocator, prefix: []const u8, max_bytes: usize, target: []const u8) !void {
            var r = try PrefixPointReader.init(prefix, 0, max_bytes);
            defer r.deinit(scratch);
            _ = (try r.find(scratch, null, "a")).?;
            _ = (try r.find(scratch, null, target)).?;
            _ = (try r.find(scratch, null, "a")).?;
        }
    };
    try std.testing.checkAllAllocationFailures(a, Fixture.run, .{ payload, raw.items.len, &large_key });
}

test "prefix point reader rejects malformed restart boundaries and index overflow" {
    const a = std.testing.allocator;
    var raw: std.ArrayListUnmanaged(u8) = .empty;
    defer raw.deinit(a);
    try appendEntryBytesToList(a, &raw, .{ .key = "a", .value = "value" });
    const payload = try encodePrefixCompressedBlockAlloc(a, raw.items);
    defer a.free(payload);
    try std.testing.expectError(error.InvalidTableFile, PrefixPointReader.init(payload, std.math.maxInt(usize), raw.items.len));
    var bounded = try PrefixPointReader.init(payload, 0, 1);
    defer bounded.deinit(a);
    try std.testing.expectError(error.InvalidTableFile, bounded.find(a, null, "a"));
    const corrupted = try a.dupe(u8, payload);
    defer a.free(corrupted);
    // A trailing byte inside the final interval contradicts its entry count.
    std.mem.writeInt(u32, corrupted[8..12], 2, .little);
    var invalid = try PrefixPointReader.init(corrupted, 0, raw.items.len);
    defer invalid.deinit(a);
    try std.testing.expectError(error.InvalidTableFile, invalid.find(a, null, "z"));
}

test "prefix point reader dense and sparse allocation measurement" {
    const a = std.testing.allocator;
    var raw: std.ArrayListUnmanaged(u8) = .empty;
    defer raw.deinit(a);
    var entries: [256]Entry = undefined;
    var keys: [256][80]u8 = undefined;
    for (&entries, 0..) |*entry, i| {
        entry.* = .{ .key = try std.fmt.bufPrint(&keys[i], "long-shared-document-key-prefix-for-compression:{d:0>4}", .{i}), .value = "v" };
        try appendEntryBytesToList(a, &raw, entry.*);
    }
    const payload = try encodePrefixCompressedBlockAlloc(a, raw.items);
    defer a.free(payload);
    const Budget = @import("../lite/test_allocator.zig").BudgetAllocator;
    // Use the native backing allocator so test allocator stack tracking does
    // not dominate decoder timing. Bytes/counts are deterministic; time is not.
    const native = if (@import("builtin").link_libc) std.heap.c_allocator else a;
    for ([_]usize{ 2, 16, 256 }) |requested| {
        var independent = Budget{ .backing = native };
        var reused = Budget{ .backing = native };
        const iterations = 1000;
        const before = @import("antfly_platform").time.monotonicNs();
        for (0..iterations) |_| for (0..requested) |n| {
            const i = if (requested == 2 and n == 1) 255 else n;
            const found = (try findExactEntryInPrefixPayloadAlloc(independent.allocator(), independent.allocator(), raw.items.len, payload, 0, null, entries[i].key)).?;
            try std.testing.expectEqualStrings("v", found.entry.value);
            independent.allocator().free(found.bytes);
        };
        const independent_ns = @import("antfly_platform").time.monotonicNs() - before;
        const after = @import("antfly_platform").time.monotonicNs();
        for (0..iterations) |_| {
            var reader = try PrefixPointReader.init(payload, 0, raw.items.len);
            defer reader.deinit(reused.allocator());
            for (0..requested) |n| {
                const i = if (requested == 2 and n == 1) 255 else n;
                const found = (try reader.find(reused.allocator(), null, entries[i].key)).?;
                const value = try reused.allocator().dupe(u8, found.entry.value);
                try std.testing.expectEqualStrings("v", value);
                reused.allocator().free(value);
            }
        }
        const reused_ns = @import("antfly_platform").time.monotonicNs() - after;
        try std.testing.expectEqual(@as(usize, 0), independent.live);
        try std.testing.expectEqual(@as(usize, 0), reused.live);
        try std.testing.expectEqual(iterations * (requested + 1), reused.alloc_calls);
        try std.testing.expectEqual(iterations * requested * 2, independent.alloc_calls);
        std.debug.print("prefix reader production: rows=256 keys={d} batches=1000 independent_ns={d} reused_ns={d} independent_allocations={d} reused_allocations={d} independent_peak={d} reused_peak={d}\n", .{ requested, independent_ns, reused_ns, independent.alloc_calls, reused.alloc_calls, independent.peak, reused.peak });
    }
}

test "prefix point reader bounds selected results without rejecting skipped values" {
    const a = std.testing.allocator;
    const large_value: [4096]u8 = @splat('v');
    var raw: std.ArrayListUnmanaged(u8) = .empty;
    defer raw.deinit(a);
    try appendEntryBytesToList(a, &raw, .{ .key = "a", .value = &large_value });
    try appendEntryBytesToList(a, &raw, .{ .key = "b", .value = "small" });
    const payload = try encodePrefixCompressedBlockAlloc(a, raw.items);
    defer a.free(payload);
    var reader = try PrefixPointReader.init(payload, 0, 128);
    defer reader.deinit(a);
    const found = (try reader.find(a, null, "b")).?;
    try std.testing.expectEqualStrings("small", found.entry.value);
    try std.testing.expectError(error.InvalidTableFile, reader.find(a, null, "a"));
    try std.testing.expectEqualStrings("small", (try reader.find(a, null, "b")).?.entry.value);
}

test "indexed point reader adapts dense sparse backwards repeated and missing probes" {
    const a = std.testing.allocator;
    var keys: [512][8]u8 = undefined;
    var entries: [512]Entry = undefined;
    for (&entries, 0..) |*entry, i| {
        const key = try std.fmt.bufPrint(&keys[i], "k{d:0>6}", .{i});
        entry.* = .{ .namespace_name = "docs", .key = key, .value = "v", .tombstone = i % 17 == 0 };
    }
    var filter = try buildFilterAlloc(a, &entries, .{});
    defer filter.deinit(a);
    const encoded = try encodeWithFilterAllocOptions(a, &entries, filter, .{ .block_compression = .none });
    defer a.free(encoded);
    var index = try decodeIndexAlloc(a, encoded);
    defer index.deinit(a);
    try std.testing.expectEqual(@as(usize, 1), index.blockCount());
    const window = index.blockWindow(0);
    const raw = encoded[index.entry_data_start + window.relative_offset ..][0..window.len];
    var reader: IndexedPointReader = .{};
    var baseline: IndexedPointReader = .{};
    for (entries) |entry| {
        baseline.current = null;
        const expected = (try baseline.find(&index, raw, 0, "docs", entry.key)).?;
        const found = (try reader.find(&index, raw, 0, "docs", entry.key)).?;
        try std.testing.expectEqual(expected.index, found.index);
        try std.testing.expectEqual(entry.tombstone, found.entry.tombstone);
    }
    try std.testing.expect(reader.test_parsed_rows <= entries.len + 16);
    std.debug.print("\nindexed dense points: keys=512 parsed_rows={d}->{d}\n", .{ baseline.test_parsed_rows, reader.test_parsed_rows });
    for ([_]usize{ 16, 32, 64, 128, 256 }) |stride| {
        var sparse: IndexedPointReader = .{};
        var binary: IndexedPointReader = .{};
        var row_index: usize = 0;
        while (row_index < entries.len) : (row_index += stride) {
            binary.current = null;
            const expected = (try binary.find(&index, raw, 0, "docs", entries[row_index].key)).?;
            const actual = (try sparse.find(&index, raw, 0, "docs", entries[row_index].key)).?;
            try std.testing.expectEqual(expected.index, actual.index);
        }
        // The initial density probe costs at most one additional row; wide
        // jumps then disable it until a nearby hit establishes density again.
        try std.testing.expect(sparse.test_parsed_rows <= binary.test_parsed_rows + 1);
        std.debug.print("\nadaptive sparse points: stride={d} keys={d} parsed_rows={d}->{d}\n", .{ stride, entries.len / stride, binary.test_parsed_rows, sparse.test_parsed_rows });
    }
    for ([_][]const u8{ "k000255", "k000000", "k000511", "k000511", "k000256x", "k000256", "k999999", "a", "k000007" }) |key| {
        const oracle = try findExactEntryInBlock(&index, raw, 0, "docs", key);
        const found = try reader.find(&index, raw, 0, "docs", key);
        try std.testing.expectEqual(oracle == null, found == null);
        if (oracle) |row| try std.testing.expectEqual(row.index, found.?.index);
    }
    try std.testing.expect(try reader.find(&index, raw, 0, "other", "k000000") == null);
    try std.testing.expectEqual(@as(usize, 1), (try reader.find(&index, raw, 0, "docs", entries[1].key)).?.index);
    var rng = std.Random.DefaultPrng.init(42);
    for (0..2048) |_| {
        var key_buf: [8]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "k{d:0>6}", .{rng.random().uintLessThan(usize, 600)});
        const name = if (rng.random().uintLessThan(usize, 5) == 0) "other" else "docs";
        const expected = try findExactEntryInBlock(&index, raw, 0, name, key);
        const found = try reader.find(&index, raw, 0, name, key);
        try std.testing.expectEqual(expected == null, found == null);
        if (expected) |row| {
            try std.testing.expectEqual(row.index, found.?.index);
            try std.testing.expectEqual(row.entry.tombstone, found.?.entry.tombstone);
        }
    }
}

pub fn qualifyControlledPrefixPointReader(control: read_admission.Port) !void {
    if (!@import("builtin").is_test) @compileError("test fixture only");
    const a = std.testing.allocator;
    var raw: std.ArrayListUnmanaged(u8) = .empty;
    defer raw.deinit(a);
    var entries: [97]Entry = undefined;
    var key_bytes: [96][32]u8 = undefined;
    for (entries[0..96], 0..) |*entry, i| {
        entry.* = .{ .namespace_name = if (i < 64) "docs" else "graph", .key = try std.fmt.bufPrint(&key_bytes[i], "shared-key:{d:0>4}", .{i}), .value = "exact", .tombstone = i % 13 == 0 };
        try appendEntryBytesToList(a, &raw, entry.*);
    }
    const wide_key: [128 * 1024]u8 = @splat('z');
    entries[96] = .{ .namespace_name = "graph", .key = &wide_key, .value = "wide" };
    try appendEntryBytesToList(a, &raw, entries[96]);
    const payload = try encodePrefixCompressedBlockAlloc(a, raw.items);
    defer a.free(payload);
    var reader = try PrefixPointReader.init(payload, 700, raw.items.len);
    defer reader.deinit(a);
    for ([_]usize{ 0, 1, 1, 15, 16, 47, 48, 63, 64, 95, 96, 0, 83, 5, 96 }) |i| {
        const entry = entries[i];
        const result = reader.findWithAdmission(a, entry.namespace_name, entry.key, control);
        const found = result catch |err| {
            // Rejection invalidates the interrupted cursor. A fresh call can
            // restart from the same retained payload without a dangling key.
            try std.testing.expect(reader.current == null);
            const retried = (try reader.find(a, entry.namespace_name, entry.key)).?;
            try std.testing.expectEqual(700 + i, retried.index);
            try std.testing.expectEqualStrings(entry.key, retried.entry.key);
            try std.testing.expectEqualStrings(entry.value, retried.entry.value);
            return err;
        };
        try std.testing.expectEqual(700 + i, found.?.index);
        try std.testing.expectEqualStrings(entry.key, found.?.entry.key);
        try std.testing.expectEqualStrings(entry.value, found.?.entry.value);
        try std.testing.expectEqual(entry.tombstone, found.?.entry.tombstone);
    }
    try std.testing.expect(try reader.findWithAdmission(a, "aaa", "shared-key:0001", control) == null);
    try std.testing.expect(try reader.findWithAdmission(a, "zzz", "shared-key:0001", control) == null);
    try std.testing.expect(try reader.findWithAdmission(a, "docs", "shared-key:0005~", control) == null);
}

test "table block decode continuation bounds checksum and output without replaying prefixes" {
    const a = std.testing.allocator;
    const expected = try a.alloc(u8, 128 * 1024 + 31);
    defer a.free(expected);
    for (expected, 0..) |*byte, i| byte.* = @truncate(i *% 7);
    for ([_]BlockCompression{ .none, .snappy }) |compression| {
        const payload = if (compression == .snappy) try snappy.encode(a, expected) else expected;
        defer if (compression == .snappy) a.free(payload);
        var cursor = try BlockDecodeCursor.init(compression, payload.len, expected.len, Crc32.hash(payload));
        var output: ?[]u8 = null;
        defer if (output) |bytes| a.free(bytes);
        var checksum_bytes: usize = 0;
        var input_bytes: usize = 0;
        var output_bytes: usize = 0;
        var allocations: usize = 0;
        var steps: usize = 0;
        while (true) {
            const progress = try cursor.step(payload, output, 257);
            try std.testing.expect(progress.checksum_bytes + progress.input_bytes <= 257);
            try std.testing.expect(progress.output_bytes <= 257 and progress.tags <= 256);
            checksum_bytes += progress.checksum_bytes;
            input_bytes += progress.input_bytes;
            output_bytes += progress.output_bytes;
            steps += 1;
            try std.testing.expect(steps < expected.len + payload.len + 8);
            switch (progress.status) {
                .pending => {},
                .needs_output => {
                    try std.testing.expect(output == null);
                    try std.testing.expectEqual(payload.len, checksum_bytes);
                    output = try a.alloc(u8, expected.len);
                    allocations += 1;
                },
                .done => break,
            }
        }
        try std.testing.expectEqual(@as(usize, 1), allocations);
        try std.testing.expectEqual(payload.len, checksum_bytes);
        try std.testing.expectEqual(payload.len, input_bytes);
        try std.testing.expectEqual(expected.len, output_bytes);
        try std.testing.expectEqualSlices(u8, expected, output.?);
        try std.testing.expect(@sizeOf(BlockDecodeCursor) <= 256);
    }
}

test "table block decode continuation verifies checksum and declared size before output allocation" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    const payload = [_]u8{ 17, 0 };
    try std.testing.expectError(error.TableBlockChecksumMismatch, decodeBlockPayloadAlloc(failing.allocator(), .snappy, &payload, 4, Crc32.hash(&payload) ^ 1));
    try std.testing.expectError(error.InvalidTableFile, decodeBlockPayloadAlloc(failing.allocator(), .snappy, &payload, 4, Crc32.hash(&payload)));
    try std.testing.expectError(error.InvalidTableFile, decodeBlockPayloadAlloc(failing.allocator(), .snappy, "", 4, Crc32.hash("")));
    try std.testing.expectEqual(@as(usize, 0), failing.alloc_index);
    try std.testing.expect(!failing.has_induced_failure);
    var cursor = try BlockDecodeCursor.init(.none, payload.len, payload.len, Crc32.hash(&payload) ^ 1);
    try std.testing.expectError(error.TableBlockChecksumMismatch, cursor.step(&payload, null, 4));
    try std.testing.expectError(error.TableBlockChecksumMismatch, cursor.step(&payload, null, 4));
}

test "table block decode continuation preserves original rejection and retires partial output" {
    const a = std.testing.allocator;
    const Control = struct {
        calls: usize = 0,
        fail_at: usize = 0,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (self.calls == self.fail_at) return error.OriginalBlockDeadline;
        }
    };
    const bytes = try a.alloc(u8, 128 * 1024 + 17);
    defer a.free(bytes);
    @memset(bytes, 51);
    for ([_]BlockCompression{ .none, .snappy }) |compression| {
        const payload = if (compression == .snappy) try snappy.encode(a, bytes) else bytes;
        defer if (compression == .snappy) a.free(payload);
        var state: Control = .{};
        const owned: read_admission.Payload = .{ .ptr = &state, .vtable = &.{ .checkpoint = Control.check, .check_lifetime = Control.check } };
        const control = read_admission.Port.init(&owned);
        var tracking = std.testing.FailingAllocator.init(a, .{});
        const result = try decodeBlockPayloadAllocWithAdmission(tracking.allocator(), compression, payload, bytes.len, Crc32.hash(payload), control);
        tracking.allocator().free(result);
        const checks = state.calls;
        try std.testing.expect(checks > 6);
        for (1..checks + 1) |index| {
            state = .{ .fail_at = index };
            try std.testing.expectError(error.OriginalBlockDeadline, decodeBlockPayloadAllocWithAdmission(tracking.allocator(), compression, payload, bytes.len, Crc32.hash(payload), control));
            try std.testing.expectEqual(index, state.calls);
            try std.testing.expectEqual(tracking.allocated_bytes, tracking.freed_bytes);
        }
        state = .{ .fail_at = 2 };
        try std.testing.expectError(error.OriginalBlockDeadline, decodeBlockPayloadAllocWithAdmission(tracking.allocator(), compression, payload[0..1], bytes.len, 0, control));
        try std.testing.expectEqual(tracking.allocated_bytes, tracking.freed_bytes);
    }
}

test "table file prefix continuation bounds shared keys headers and restart validation" {
    const a = std.testing.allocator;
    var raw = std.ArrayListUnmanaged(u8).empty;
    defer raw.deinit(a);
    const key = try a.alloc(u8, 8192);
    defer a.free(key);
    @memset(key, 'k');
    for (0..40) |i| {
        key[key.len - 1] = @intCast(i);
        try appendEntryBytesToList(a, &raw, .{ .namespace_name = if (i % 2 == 0) "docs" else null, .key = key, .value = if (i % 3 == 0) "" else "value", .tombstone = i % 3 == 0 });
    }
    const encoded = try encodePrefixCompressedBlockAlloc(a, raw.items);
    defer a.free(encoded);
    for ([_]usize{ 1, 97, 4093 }) |quantum| {
        var cursor = PrefixDecodeCursor.init(encoded.len, raw.items.len);
        var output: ?[]u8 = null;
        defer if (output) |bytes| a.free(bytes);
        var steps: usize = 0;
        while (true) {
            const progress = try cursor.step(encoded, output, quantum);
            try std.testing.expect(progress.input_bytes <= quantum and progress.output_bytes <= quantum);
            steps += 1;
            try std.testing.expect(steps < 1000000);
            switch (progress.status) {
                .pending => {},
                .needs_output => {
                    try std.testing.expect(output == null);
                    output = try a.alloc(u8, raw.items.len);
                },
                .done => break,
            }
        }
        try std.testing.expectEqualSlices(u8, raw.items, output.?);
    }
    for (0..4) |variant| {
        const corrupt = try a.dupe(u8, encoded);
        defer a.free(corrupt);
        switch (variant) {
            0 => corrupt[0] ^= 1,
            1 => std.mem.writeInt(u32, corrupt[12..16], 0, .little),
            2 => std.mem.writeInt(u32, corrupt[29..33], 1, .little), // shared prefix on first restart
            3 => corrupt[corrupt.len - 4] ^= 1,
            else => unreachable,
        }
        var cursor = PrefixDecodeCursor.init(corrupt.len, raw.items.len);
        const output = try a.alloc(u8, raw.items.len);
        defer a.free(output);
        var failed = false;
        for (0..10000) |_| {
            const result = cursor.step(corrupt, output, 4093) catch |err| {
                try std.testing.expectEqual(error.InvalidTableFile, err);
                try std.testing.expectError(err, cursor.step(encoded, output, 4093));
                failed = true;
                break;
            };
            if (result.status == .done) break;
        }
        try std.testing.expect(failed);
    }
}

fn expectRetainedIndexForTest(expected: *const TableIndex, actual: *const TableIndex) !void {
    try std.testing.expectEqual(expected.entry_data_start, actual.entry_data_start);
    try std.testing.expectEqual(expected.entry_data_len, actual.entry_data_len);
    try std.testing.expectEqualSlices(u16, expected.block_entry_offsets, actual.block_entry_offsets);
    try std.testing.expectEqualDeep(expected.filter, actual.filter);
    try std.testing.expectEqualDeep(expected.prefix_filter, actual.prefix_filter);
    try std.testing.expectEqual(expected.prefix_extractor, actual.prefix_extractor);
    try std.testing.expectEqual(expected.blocks.len, actual.blocks.len);
    for (expected.blocks, actual.blocks) |lhs, rhs| try std.testing.expectEqualDeep(lhs, rhs);
}

fn finishIndexCursorForTest(cursor: *IndexDecodeCursor, quantum: usize) !RetainedIndex {
    var polls: usize = 0;
    while (try cursor.step(quantum, null) != .ready) : (polls += 1) try std.testing.expect(polls < 100000);
    return cursor.takeOutput();
}

test "table file retained index continuation matches allocating decoder across large metadata and movable ownership" {
    const a = std.testing.allocator;
    const keys = try a.alloc(u8, 32 * 4096);
    defer a.free(keys);
    const namespace = try a.alloc(u8, 1024);
    defer a.free(namespace);
    @memset(namespace, 'n');
    const value: [default_block_size]u8 = @splat('v');
    var entries: [32]Entry = undefined;
    for (&entries, 0..) |*entry, i| {
        const key = keys[i * 4096 ..][0..4096];
        @memset(key, 'k');
        key[key.len - 1] = @intCast(i);
        entry.* = .{ .namespace_name = namespace, .key = key, .value = &value };
    }
    const encoded = try encodeAlloc(a, &entries);
    defer a.free(encoded);
    const footer = try decodeFooter(encoded);
    const metadata = encoded[footer.metadata_offset..][0..footer.metadata_len];
    try std.testing.expect(metadata.len > 65536);
    var baseline = try decodeIndexFromFooterAlloc(a, footer, metadata);
    defer baseline.deinit(a);
    for ([_]usize{ 127, 4093, 65536 }) |quantum| {
        var decoder = IndexDecodeCursor.init(a, footer, try a.dupe(u8, metadata));
        defer decoder.deinit();
        _ = try decoder.step(quantum, null);
        var moved = decoder;
        decoder = .{ .allocator = a, .footer = footer, .metadata = null, .phase = .closed };
        defer moved.deinit();
        var index = try finishIndexCursorForTest(&moved, quantum);
        defer index.deinit();
        try expectRetainedIndexForTest(&baseline, index.view());
        for (index.view().blocks) |block| {
            try std.testing.expect(@intFromPtr(block.largest_key.ptr) >= @intFromPtr(index.metadata.ptr));
            try std.testing.expect(@intFromPtr(block.largest_key.ptr) + block.largest_key.len <= @intFromPtr(index.metadata.ptr) + index.metadata.len);
        }
        try std.testing.expectError(error.TransactionClosed, moved.step(1, null));
    }
}

test "table file retained index continuation retires every allocation failure and original rejection phase" {
    const a = std.testing.allocator;
    const entries = [_]Entry{ .{ .namespace_name = "docs", .key = "key0", .value = "payload" }, .{ .namespace_name = "docs", .key = "key1", .value = "payload" } };
    const encoded = try encodeAlloc(a, &entries);
    defer a.free(encoded);
    const footer = try decodeFooter(encoded);
    const metadata = encoded[footer.metadata_offset..][0..footer.metadata_len];
    const Probe = struct {
        fn run(root: std.mem.Allocator, f: Footer, raw: []const u8) !void {
            var decoder = IndexDecodeCursor.init(root, f, try root.dupe(u8, raw));
            defer decoder.deinit();
            var index = try finishIndexCursorForTest(&decoder, 17);
            defer index.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(a, Probe.run, .{ footer, metadata });
    const Budget = struct {
        pub fn checkpoint(_: *@This()) !void {
            return error.TestIndexOriginalRefusal;
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedIndexWait;
        }
    };
    var budget: Budget = .{};
    var adapter: read_admission.Adapter(*Budget) = .{ .budget = &budget };
    const payload = adapter.payload();
    inline for (@typeInfo(IndexDecodeCursor.Phase).@"enum".field_values) |field_value| {
        const phase: IndexDecodeCursor.Phase = @fromBackingInt(field_value);
        if (phase == .taken or phase == .closed or phase == .failed) continue;
        var decoder = IndexDecodeCursor.init(a, footer, try a.dupe(u8, metadata));
        defer decoder.deinit();
        var polls: usize = 0;
        while (decoder.phase != phase and decoder.phase != .ready) : (polls += 1) {
            try std.testing.expect(polls < 10000);
            _ = try decoder.step(17, null);
        }
        try std.testing.expectEqual(phase, decoder.phase);
        try std.testing.expectError(error.TestIndexOriginalRefusal, decoder.step(17, .init(&payload)));
        try std.testing.expect(decoder.metadata == null);
        try std.testing.expectEqual(@as(usize, 0), decoder.blocks.len);
        try std.testing.expectEqual(@as(usize, 0), decoder.offsets.len);
        try std.testing.expectError(error.TestIndexOriginalRefusal, decoder.step(17, null));
    }
}

test "table file retained index continuation preserves malformed metadata validation" {
    const a = std.testing.allocator;
    const entries = [_]Entry{ .{ .namespace_name = "docs", .key = "key0", .value = "payload" }, .{ .namespace_name = "docs", .key = "key1", .value = "payload" } };
    const encoded = try encodeAlloc(a, &entries);
    defer a.free(encoded);
    var footer = try decodeFooter(encoded);
    const metadata = encoded[footer.metadata_offset..][0..footer.metadata_len];
    var rejected: usize = 0;
    var accepted: usize = 0;
    for (0..metadata.len) |position| {
        const changed = try a.dupe(u8, metadata);
        defer a.free(changed);
        changed[position] ^= 0x80;
        footer.metadata_checksum = Crc32.hash(changed);
        const expected = decodeIndexFromFooterAlloc(a, footer, changed);
        var cursor = IndexDecodeCursor.init(a, footer, try a.dupe(u8, changed));
        defer cursor.deinit();
        const actual = finishIndexCursorForTest(&cursor, 17);
        if (expected) |value| {
            var baseline = value;
            defer baseline.deinit(a);
            var index = try actual;
            defer index.deinit();
            try expectRetainedIndexForTest(&baseline, index.view());
            accepted += 1;
        } else |err| {
            try std.testing.expectError(err, actual);
            rejected += 1;
        }
    }
    try std.testing.expect(rejected > 0 and accepted > 0);
}

test "controlled physical sync table metadata commands retain original rejection and match the existing wire encoder" {
    const Control = struct {
        calls: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        failure: anyerror,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const call = self.calls;
            self.calls += 1;
            if (call == self.fail_at) return self.failure;
        }
    };
    const a = std.testing.allocator;
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.FileNotFound }) |failure| {
        for ([_]usize{ 2, 17 }) |stride| {
            var sink_impl = MemoryTableSink.init(a);
            defer sink_impl.deinit();
            var sink = sink_impl.sink();
            var encoder = try StreamingEncoder.init(a, &sink, 1_000_000, .{});
            defer encoder.deinit();
            const keys = try a.alloc(u8, 24 * 36 * 1024);
            defer a.free(keys);
            var entries: [24]Entry = undefined;
            for (&entries, 0..) |*entry, i| {
                const key = keys[i * 36 * 1024 ..][0 .. 36 * 1024];
                @memset(key, 'x');
                _ = try std.fmt.bufPrint(key[0..16], "doc{d:0>8}/", .{i});
                entry.* = .{ .namespace_name = "docs", .key = key, .value = "value", .tombstone = i % 3 == 0 };
                try encoder.appendEntry(entry.*);
            }
            // The unchanged whole-table producer is an independent wire
            // oracle. Supply the same deliberately oversized table Bloom.
            const borrowed_filter: bloom.OwnedFilter = .{ .bytes = encoder.filter_builder.bytes, .bit_count = encoder.filter_builder.bit_count, .hash_count = encoder.filter_builder.hash_count };
            const expected = try encodeWithFilterAllocOptions(a, &entries, borrowed_filter, .{});
            defer a.free(expected);
            try encoder.flushBlock();
            var metadata = try encoder.prepareMetadata();
            defer metadata.deinit();
            var control: Control = .{ .failure = failure };
            const payload: read_admission.Payload = .{ .ptr = &control, .vtable = &.{ .checkpoint = Control.check } };
            const original = read_admission.Port.init(&payload);
            var rejections: usize = 0;
            var commands: usize = 0;
            var image: [16 * 1024]u8 = undefined;
            while (true) {
                control.fail_at = control.calls + stride;
                const command = (metadata.next(original) catch |cause| {
                    try std.testing.expectEqual(failure, cause);
                    rejections += 1;
                    continue;
                }) orelse break;
                commands += 1;
                try std.testing.expect(command.bytes.len <= image.len);
                @memcpy(image[0..command.bytes.len], command.bytes);
                // Rejected observation cannot mutate a pending command, even
                // when the caller has already physically applied its bytes.
                if (command.append) {
                    try std.testing.expectEqual(sink.len(), command.offset);
                    try sink.appendSlice(command.bytes);
                } else try sink.writeAt(command.offset, command.bytes);
                control.fail_at = control.calls;
                try std.testing.expectError(failure, metadata.next(original));
                control.fail_at = std.math.maxInt(usize);
                const same = (try metadata.next(original)).?;
                try std.testing.expectEqual(command.offset, same.offset);
                try std.testing.expectEqual(command.append, same.append);
                try std.testing.expectEqualSlices(u8, image[0..command.bytes.len], same.bytes);
                try std.testing.expectError(error.InvalidTableFile, metadata.takeResult());
                metadata.acknowledge();
            }
            var result = try metadata.takeResult();
            defer result.filter.deinit(a);
            try std.testing.expectError(error.InvalidTableFile, metadata.takeResult());
            try std.testing.expect(commands > 100 and rejections > 10);
            try std.testing.expectEqual(expected.len, result.size_bytes);
            try std.testing.expectEqualSlices(u8, expected, sink_impl.out.items);
            var decoded = try decodeIndexAlloc(a, sink_impl.out.items);
            defer decoded.deinit(a);
            try std.testing.expectEqual(@as(usize, entries.len), decoded.entryCount());
            try std.testing.expect(decoded.maybeContainsPrefix("docs", "doc00000000/"));
        }
    }
}

test "controlled physical sync table metadata prefix preparation yields CPU and retires partial allocation ownership" {
    const cpu = @import("../../common/runnable_scheduler.zig");
    const now = @import("antfly_platform").time.monotonicNs;
    const Control = struct {
        execution: *cpu.Execution,
        scheduler: *cpu.Scheduler,
        calls: usize = 0,
        turns: usize = 0,
        metadata: ?*StreamingEncoder.Metadata = null,
        stop: enum { none, zero, hashes } = .none,
        rejected: bool = false,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.execution.checkLifetime();
            try std.testing.expectEqual(cpu.State.running, self.scheduler.state(&self.execution.entry.?));
            self.calls += 1;
            if (self.metadata) |m| {
                const reject = switch (self.stop) {
                    .none => false,
                    .zero => m.phase == .prefix_zero and m.offset != 0,
                    .hashes => m.phase == .prefix_hashes and m.index != 0,
                };
                if (reject) {
                    self.rejected = true;
                    return error.DeadlineExceeded;
                }
            }
            if (self.calls % 32 == 0) {
                try self.execution.park(.resource_blocked);
                var control: cpu.Execution = .{};
                defer control.finish();
                try control.start(self.scheduler, .{ .io = std.testing.io, .native_now_ns = @import("antfly_platform").time.monotonicNs }, .control);
                control.finish();
                self.turns += 1;
                try self.execution.enter(.write);
            }
        }
    };
    const a = std.testing.allocator;
    var slots = try cpu.Scheduler.init(.{ .slots = 3, .control_slots = 1, .cleanup_slots = 1 });
    defer slots.close();
    var execution: cpu.Execution = .{};
    defer execution.finish();
    try execution.reserveCleanupRoot(&slots, .{ .io = std.testing.io, .native_now_ns = now, .allow_wait = true }, 10 * std.time.ns_per_s);
    try execution.activateCleanup();
    var query: cpu.Execution = .{};
    defer query.finish();
    try query.start(&slots, .{ .io = std.testing.io, .native_now_ns = now }, .query);
    var control: Control = .{ .execution = &execution, .scheduler = &slots };
    const payload: read_admission.Payload = .{ .ptr = &control, .vtable = &.{ .checkpoint = Control.check } };
    const original = read_admission.Port.init(&payload);
    var failures: usize = 0;
    // Failed prefix allocation keeps the moved table filter alive. Retrying
    // or abandoning partial zero/hash preparation must free each owner once.
    for ([_]@FieldType(Control, "stop"){ .none, .zero, .hashes }) |stop| {
        const abandon = stop != .none;
        var failing = std.testing.FailingAllocator.init(a, .{});
        const allocator = failing.allocator();
        var sink_impl = MemoryTableSink.init(a);
        defer sink_impl.deinit();
        var sink = sink_impl.sink();
        var encoder = try StreamingEncoder.init(allocator, &sink, 8192, .{ .bloom_config = .{ .min_bits = 1024 * 1024 } });
        defer encoder.deinit();
        for (0..192) |i| {
            var key: [24]u8 = undefined;
            try encoder.appendEntry(.{ .namespace_name = "docs", .key = try std.fmt.bufPrint(&key, "{d:0>8}/key", .{i}), .value = "value" });
        }
        try encoder.flushBlock();
        var metadata = try encoder.prepareMetadata();
        defer metadata.deinit();
        control.metadata = &metadata;
        control.stop = stop;
        control.rejected = false;
        var allocated = false;
        while (true) {
            if (metadata.phase == .prefix_allocate and !allocated) {
                const index = failing.alloc_index;
                failing.fail_index = index;
                try std.testing.expectError(error.OutOfMemory, metadata.next(original));
                try std.testing.expect(metadata.table_filter != null and metadata.prefix_builder == null);
                failures += 1;
                failing.fail_index = std.math.maxInt(usize);
                allocated = true;
            }
            const command = (metadata.next(original) catch |cause| {
                try std.testing.expect(abandon and control.rejected);
                try std.testing.expectEqual(error.DeadlineExceeded, cause);
                try std.testing.expect(metadata.table_filter != null and metadata.prefix_builder != null and metadata.prefix_filter == null);
                break;
            }) orelse break;
            if (command.append) try sink.appendSlice(command.bytes) else try sink.writeAt(command.offset, command.bytes);
            metadata.acknowledge();
        }
        if (!abandon) {
            var result = try metadata.takeResult();
            defer result.filter.deinit(allocator);
            var decoded = try decodeIndexAlloc(a, sink_impl.out.items);
            defer decoded.deinit(a);
            try std.testing.expectEqual(@as(usize, 128 * 1024), decoded.prefix_filter.?.bytes.len);
            try std.testing.expect(decoded.maybeContainsPrefix("docs", "00000001/"));
        }
    }
    try std.testing.expect(failures == 3 and control.turns > 0);
    try std.testing.expectEqual(cpu.State.running, slots.state(&query.entry.?));
}

test "controlled physical sync legacy table finish rejects replay after partial final block output" {
    const Fixture = struct {
        memory: MemoryTableSink,
        fail: bool = false,
        writes: usize = 0,
        fn len(raw: *anyopaque) usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.memory.out.items.len;
        }
        fn append(raw: *anyopaque, bytes: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.writes += 1;
            if (self.fail) {
                try self.memory.out.appendSlice(self.memory.allocator, bytes[0 .. bytes.len / 2]);
                return error.InjectedWriteFault;
            }
            try self.memory.out.appendSlice(self.memory.allocator, bytes);
        }
        fn byte(raw: *anyopaque, value: u8) !void {
            return append(raw, &.{value});
        }
        fn write(raw: *anyopaque, offset: usize, bytes: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            var sink = self.memory.sink();
            try sink.writeAt(offset, bytes);
        }
    };
    const a = std.testing.allocator;
    var fixture: Fixture = .{ .memory = .init(a) };
    defer fixture.memory.deinit();
    var sink: TableSink = .{ .ptr = &fixture, .vtable = &.{ .len = Fixture.len, .append_slice = Fixture.append, .append_byte = Fixture.byte, .write_at = Fixture.write } };
    var encoder = try StreamingEncoder.init(a, &sink, 1, .{});
    defer encoder.deinit();
    try encoder.appendEntry(.{ .namespace_name = "docs", .key = "doc:key", .value = "value" });
    fixture.fail = true;
    const before = fixture.memory.out.items.len;
    try std.testing.expectError(error.InjectedWriteFault, encoder.finish());
    const after = fixture.memory.out.items.len;
    const writes = fixture.writes;
    try std.testing.expect(after > before);
    try std.testing.expectError(error.InvalidTableFile, encoder.finish());
    try std.testing.expectEqual(after, fixture.memory.out.items.len);
    try std.testing.expectEqual(writes, fixture.writes);
}

// Frozen pre-cursor prefix encoder used as an independent wire oracle.
fn encodePrefixReferenceForTest(allocator: std.mem.Allocator, block_bytes: []const u8) ![]u8 {
    var encoded_entries = std.ArrayListUnmanaged(u8).empty;
    defer encoded_entries.deinit(allocator);
    var restart_offsets = std.ArrayListUnmanaged(u32).empty;
    defer restart_offsets.deinit(allocator);
    var previous_key: []const u8 = &.{};

    var cursor: usize = 0;
    var entry_count: usize = 0;
    while (cursor < block_bytes.len) : (entry_count += 1) {
        const entry_start = cursor;
        const entry = try parseEntryAt(block_bytes, entry_start);
        const entry_len = try tableEntryEncodedLen(entry);
        if (entry_len > block_bytes.len - cursor) return error.InvalidTableFile;
        cursor += entry_len;

        var shared_key_len: usize = 0;
        if (entry_count % prefix_restart_interval == 0) {
            try restart_offsets.append(allocator, try checkedU32(encoded_entries.items.len));
        } else {
            shared_key_len = commonPrefixLen(previous_key, entry.key);
        }
        const unshared_key = entry.key[shared_key_len..];

        try encoded_entries.append(allocator, if (entry.tombstone) 1 else 0);
        try appendU32(allocator, &encoded_entries, if (entry.namespace_name) |name| try checkedU32(name.len) else 0);
        try appendU32(allocator, &encoded_entries, try checkedU32(shared_key_len));
        try appendU32(allocator, &encoded_entries, try checkedU32(unshared_key.len));
        try appendU32(allocator, &encoded_entries, try checkedU32(entry.value.len));
        if (entry.namespace_name) |name| try encoded_entries.appendSlice(allocator, name);
        try encoded_entries.appendSlice(allocator, unshared_key);
        try encoded_entries.appendSlice(allocator, entry.value);

        previous_key = entry.key;
    }
    if (cursor != block_bytes.len) return error.InvalidTableFile;

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, prefix_block_magic);
    try appendU32(allocator, &out, try checkedU32(entry_count));
    try appendU32(allocator, &out, try checkedU32(prefix_restart_interval));
    try appendU32(allocator, &out, try checkedU32(restart_offsets.items.len));
    try appendU32(allocator, &out, try checkedU32(encoded_entries.items.len));
    try out.appendSlice(allocator, encoded_entries.items);
    for (restart_offsets.items) |offset| try appendU32(allocator, &out, offset);
    return try out.toOwnedSlice(allocator);
}

test "table file prefix encoder cursor bounds huge keys values and restart relocation" {
    const a = std.testing.allocator;
    var raw: std.ArrayListUnmanaged(u8) = .empty;
    defer raw.deinit(a);
    const key = try a.alloc(u8, 32769);
    defer a.free(key);
    @memset(key, 'k');
    const value = try a.alloc(u8, 65537);
    defer a.free(value);
    @memset(value, 'v');
    var table: std.ArrayListUnmanaged(u8) = .empty;
    defer table.deinit(a);
    for ([_]usize{ 0, 1, 16, 17, 33 }) |count| {
        raw.clearRetainingCapacity();
        for (0..count) |i| {
            key[key.len - 1] = @intCast(i);
            try appendEntryBytesToList(a, &raw, .{
                .namespace_name = if (i % 2 == 0) "docs" else null,
                .key = if (i % 7 == 0) "" else key,
                .value = if (i % 3 == 0) value else "",
                .tombstone = i % 3 == 1,
            });
        }
        const expected = try encodePrefixReferenceForTest(a, raw.items);
        defer a.free(expected);
        const bound = try PrefixEncodeCursor.outputBound(raw.items.len);
        try table.resize(a, bound + 16);
        for ([_]usize{ 1, 7, 4096 }) |quantum| {
            @memset(table.items, 0xcc);
            var cursor = try PrefixEncodeCursor.init(raw.items.len);
            while (true) {
                var moved = cursor;
                const progress = try moved.step(raw.items, table.items[0..bound], quantum);
                try std.testing.expect(progress.work <= quantum and progress.output_bytes <= quantum);
                try std.testing.expect(progress.done or progress.work != 0);
                cursor = moved;
                if (progress.done) break;
            }
            try std.testing.expectEqualSlices(u8, expected, table.items[0..cursor.output_offset]);
            for (table.items[bound..]) |b| try std.testing.expectEqual(@as(u8, 0xcc), b);
            const done = try cursor.step(raw.items, table.items[0..bound], quantum);
            try std.testing.expect(done.done and done.work == 0 and done.output_bytes == 0);
            if (count != 0) {
                const decoded = try decodePrefixCompressedBlockAlloc(a, a, table.items[0..cursor.output_offset], raw.items.len);
                defer a.free(decoded);
                try std.testing.expectEqualSlices(u8, raw.items, decoded);
            }
        }
        const ordinary = try encodePrefixCompressedBlockAlloc(a, raw.items);
        defer a.free(ordinary);
        try std.testing.expectEqualSlices(u8, expected, ordinary);
    }
}

test "table file prefix encoder malformed input is terminal and invalid borrows preserve progress" {
    var cursor = try PrefixEncodeCursor.init(13);
    var output: [128]u8 = @splat(0xcc);
    var invalid: [13]u8 = @splat(0);
    invalid[0] = 2;
    try std.testing.expectError(error.InvalidArgument, cursor.step(&invalid, &output, 0));
    try std.testing.expectError(error.InvalidArgument, cursor.step(&invalid, output[0..1], 1));
    try std.testing.expectError(error.InvalidArgument, cursor.step(invalid[0..1], &output, 1));
    try std.testing.expectEqual(@as(usize, 0), cursor.input_offset);
    for (output) |b| try std.testing.expectEqual(@as(u8, 0xcc), b);
    try std.testing.expectError(error.InvalidTableFile, cursor.step(&invalid, &output, 1));
    try std.testing.expectError(error.InvalidTableFile, cursor.step(&invalid, &output, 1));
    const Fixture = struct {
        fn run(a: std.mem.Allocator) !void {
            const raw: [13]u8 = @splat(0);
            const bytes = try encodePrefixCompressedBlockAlloc(a, &raw);
            defer a.free(bytes);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "table file prefix encoder relocates overlapping restart tail in one byte steps" {
    const a = std.testing.allocator;
    var raw: std.ArrayListUnmanaged(u8) = .empty;
    defer raw.deinit(a);
    // Exactly one shared byte makes the final restart destination overlap
    // its staging range, exercising memmove across separate step calls.
    for (0..64) |i| {
        const key = [_]u8{ if (i == 2) 1 else @intCast(i), @intCast(i) };
        try appendEntryBytesToList(a, &raw, .{ .key = if (i == 1 or i == 2) &key else "", .value = "" });
    }
    const expected = try encodePrefixReferenceForTest(a, raw.items);
    defer a.free(expected);
    const output = try a.alloc(u8, try PrefixEncodeCursor.outputBound(raw.items.len));
    defer a.free(output);
    var cursor = try PrefixEncodeCursor.init(raw.items.len);
    while (cursor.phase != .restarts) {
        const p = try cursor.step(raw.items, output, 1);
        try std.testing.expect(p.work <= 1 and p.output_bytes <= 1 and !p.done);
    }
    try std.testing.expect(cursor.output_offset < cursor.restart_base);
    try std.testing.expect(cursor.output_offset + cursor.restart_count * 4 > cursor.restart_base);
    while (!(try cursor.step(raw.items, output, 1)).done) {}
    try std.testing.expectEqualSlices(u8, expected, output[0..cursor.output_offset]);
}

test "controlled physical sync retained table block freezes CPU stages and adopts only final acknowledged output" {
    const Control = struct {
        reject: bool = false,
        failure: anyerror,
        calls: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const call = self.calls;
            self.calls += 1;
            if (self.reject or call == self.fail_at) return self.failure;
        }
    };
    const a = std.testing.allocator;
    const value = try a.alloc(u8, 96 * 1024 + 7);
    defer a.free(value);
    @memset(value, 'v');
    for ([_]CompressionPolicy{ .none, .snappy_adaptive }) |policy| {
        for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.FileNotFound }) |failure| {
            var memory = MemoryTableSink.init(a);
            defer memory.deinit();
            var sink = memory.sink();
            var e = try StreamingEncoder.init(a, &sink, 96, .{ .block_compression = policy });
            defer e.deinit();
            var c: Control = .{ .failure = failure };
            const payload_control: read_admission.Payload = .{ .ptr = &c, .vtable = &.{ .checkpoint = Control.check } };
            const original = read_admission.Port.init(&payload_control);
            for (0..96) |i| {
                var key: [8]u8 = undefined;
                std.mem.writeInt(u64, &key, @intCast(i), .big);
                try e.appendEntry(.{ .namespace_name = "docs", .key = &key, .value = value });
                _ = try StreamingEncoder.Block.memoryBound(&e);
                var block = try e.prepareBlock();
                defer block.deinit();
                try std.testing.expectError(error.InvalidTableFile, e.appendEntry(.{ .key = "bad", .value = "" }));
                try std.testing.expectError(error.InvalidTableFile, e.prepareMetadata());
                try std.testing.expectError(error.InvalidTableFile, e.finishBlocks());
                try std.testing.expectError(error.InvalidTableFile, e.prepareBlock());
                const before = sink.len();
                while (true) {
                    const phase = block.phase;
                    const offset = block.cpu_offset;
                    c.reject = true;
                    try std.testing.expectError(failure, block.step(original));
                    try std.testing.expectEqual(phase, block.phase);
                    try std.testing.expectEqual(offset, block.cpu_offset);
                    try std.testing.expectEqual(before, sink.len());
                    try std.testing.expectEqual(i, e.blocks.items.len);
                    c.reject = false;
                    // The owner is movable; no self pointer survives this call.
                    var moved = block;
                    const ready = try moved.step(original);
                    block = moved;
                    if (ready) break;
                }
                var bytes_written: usize = 0;
                while (try block.next(original)) |command| {
                    try std.testing.expect(command.bytes.len > 0 and command.bytes.len <= 16 * 1024);
                    try std.testing.expectEqual(sink.len(), command.offset);
                    const again = (try block.next(original)).?;
                    try std.testing.expect(command.bytes.ptr == again.bytes.ptr and command.bytes.len == again.bytes.len);
                    c.reject = true;
                    try std.testing.expectError(failure, block.next(original));
                    try std.testing.expectEqual(i, e.blocks.items.len);
                    c.reject = false;
                    try sink.appendSlice(command.bytes);
                    bytes_written += command.bytes.len;
                    const raw_len = e.block_bytes.items.len;
                    const last_byte = e.block_bytes.items[raw_len - 1];
                    block.acknowledge();
                    if (block.published) {
                        try std.testing.expectEqual(i + 1, e.blocks.items.len);
                        try std.testing.expectEqual(last_byte, e.block_bytes.allocatedSlice()[raw_len - 1]);
                    }
                }
                try std.testing.expect(block.published);
                try std.testing.expectEqual(before + bytes_written, e.physical_output_len);
                try std.testing.expectEqual(Crc32.hash(memory.out.items[before..]), e.blocks.items[i].checksum);
                // Rejected retirement keeps the encoder frozen and fences all
                // preparation/output methods until every allocation retires.
                c.reject = true;
                try std.testing.expectError(failure, block.retireWithAdmission(original));
                try std.testing.expect(e.block_active);
                try std.testing.expectError(error.InvalidTableFile, block.next(null));
                c.reject = false;
                while (block.phase != .retired) {
                    // Advance one bounded retirement stage, then reject and
                    // resume without releasing any allocation twice.
                    c.fail_at = c.calls + 1;
                    block.retireWithAdmission(original) catch |cause| {
                        try std.testing.expectEqual(failure, cause);
                        try std.testing.expect(e.block_active);
                        continue;
                    };
                }
                c.fail_at = std.math.maxInt(usize);
                try block.retireWithAdmission(original);
                try std.testing.expect(!e.block_active);
            }
            var result = try e.finish();
            defer result.filter.deinit(a);
            var decoded = try decodeAlloc(a, memory.out.items);
            defer decoded.deinit(a);
            try std.testing.expectEqual(@as(usize, 96), decoded.entries.len);
            for (decoded.entries, 0..) |entry, i| {
                var key: [8]u8 = undefined;
                std.mem.writeInt(u64, &key, @intCast(i), .big);
                try std.testing.expectEqualStrings("docs", entry.namespace_name.?);
                try std.testing.expectEqualSlices(u8, &key, entry.key);
                try std.testing.expectEqualSlices(u8, value, entry.value);
            }
        }
    }
}

test "controlled physical sync retained table block abandonment and allocation failures retain input ownership" {
    const Fixture = struct {
        fn noResize(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) bool {
            return false;
        }
        fn noRemap(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
            return null;
        }
        fn run(backing: std.mem.Allocator) !void {
            // Production prepaid segments also refuse remapping. Fix the
            // allocation schedule so every fault index is deterministic even
            // when an optimized backing allocator can resize in place.
            var vtable = backing.vtable.*;
            vtable.resize = noResize;
            vtable.remap = noRemap;
            const a: std.mem.Allocator = .{ .ptr = backing.ptr, .vtable = &vtable };
            var memory = MemoryTableSink.init(a);
            defer memory.deinit();
            var sink = memory.sink();
            var e = try StreamingEncoder.init(a, &sink, 1, .{});
            defer e.deinit();
            var key: [4097]u8 = @splat('k');
            key[key.len - 1] = 'z';
            try e.appendEntry(.{ .namespace_name = "docs", .key = &key, .value = "value" });
            const count = e.block_entry_count;
            const smallest = e.block_smallest_key;
            {
                var block = try e.prepareBlock();
                defer block.deinit();
                _ = try block.next(null);
                try std.testing.expectEqual(@as(usize, header_len), sink.len());
                try std.testing.expectEqual(count, e.block_entry_count);
                try std.testing.expect(e.block_smallest_key.ptr == smallest.ptr);
            }
            try std.testing.expect(!e.finished and !e.block_active);
            // Abandon a prepared but unwritten block; retry produces one row.
            var result = try e.finish();
            defer result.filter.deinit(a);
            var decoded = try decodeAlloc(a, memory.out.items);
            defer decoded.deinit(a);
            try std.testing.expectEqual(@as(usize, 1), decoded.entries.len);
            try std.testing.expectEqualSlices(u8, &key, decoded.entries[0].key);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
    const a = std.testing.allocator;
    var memory = MemoryTableSink.init(a);
    defer memory.deinit();
    var sink = memory.sink();
    var e = try StreamingEncoder.init(a, &sink, 1, .{ .block_compression = .none });
    defer e.deinit();
    const large: [32769]u8 = @splat('v');
    try e.appendEntry(.{ .key = "key", .value = &large });
    var block = try e.prepareBlock();
    defer block.deinit();
    const first = (try block.next(null)).?;
    try sink.appendSlice(first.bytes);
    block.acknowledge();
    try std.testing.expect(!block.published);
    block.deinit();
    try std.testing.expect(e.finished);
    try std.testing.expectError(error.InvalidTableFile, e.finish());
}

test "controlled physical sync retained block preparation fits prepaid memory bound through metadata growth" {
    const a = std.testing.allocator;
    var memory = MemoryTableSink.init(a);
    defer memory.deinit();
    var sink = memory.sink();
    var e = try StreamingEncoder.init(a, &sink, 16, .{ .block_compression = .none });
    defer e.deinit();
    for (0..8) |i| {
        const key = [_]u8{@intCast(i)};
        try e.appendEntry(.{ .key = &key, .value = "" });
        try e.finishBlocks();
    }
    try std.testing.expectEqual(e.blocks.capacity, e.blocks.items.len);
    const key: [8193]u8 = @splat('k');
    try e.appendEntry(.{ .namespace_name = "docs", .key = &key, .value = &key });
    e.compression_policy = .snappy_adaptive;
    const bound = try StreamingEncoder.Block.memoryBound(&e);
    const prepaid = try a.alloc(u8, bound);
    defer a.free(prepaid);
    var fixed = std.heap.FixedBufferAllocator.init(prepaid);
    e.allocator = fixed.allocator();
    defer {
        e.compression_bytes.deinit(fixed.allocator());
        e.compression_bytes = .empty;
        e.allocator = a;
    }
    var block = try e.prepareBlock();
    defer block.deinit();
    _ = try block.next(null);
    try std.testing.expect(fixed.end_index <= bound);
    try std.testing.expect(block.new_blocks != null and block.copied_blocks == 8);
    try std.testing.expectEqual(@as(usize, 8), e.blocks.items.len);
    try std.testing.expectEqual(@as(usize, 1), e.block_entry_count);
    try std.testing.expectEqualSlices(u8, &key, e.block_smallest_key);
}

test "controlled physical sync table decode allocation failures retire transferred input and partial row copies once" {
    const Fixture = struct {
        fn run(a: std.mem.Allocator, raw: []const u8) !void {
            var decoded = try decodeAlloc(a, raw);
            defer decoded.deinit(a);
            try std.testing.expectEqual(@as(usize, 2), decoded.entries.len);
            try std.testing.expectEqualStrings("docs", decoded.entries[0].namespace_name.?);
        }
    };
    const a = std.testing.allocator;
    const value: [16385]u8 = @splat('v');
    const entries = [_]Entry{
        .{ .namespace_name = "docs", .key = "doc:1", .value = &value },
        .{ .namespace_name = "docs", .key = "doc:2", .value = "short" },
    };
    var filter = try buildFilterAlloc(a, &entries, .{});
    defer filter.deinit(a);
    for ([_]CompressionPolicy{ .none, .snappy_adaptive }) |policy| {
        const raw = try encodeWithFilterAllocOptions(a, &entries, filter, .{ .block_compression = policy });
        defer a.free(raw);
        try std.testing.checkAllAllocationFailures(a, Fixture.run, .{raw});
    }
}

test "controlled physical sync retained block defers old compression buffer reclamation and skips disabled compression credit" {
    const a = std.testing.allocator;
    var memory = MemoryTableSink.init(a);
    defer memory.deinit();
    var sink = memory.sink();
    var e = try StreamingEncoder.init(a, &sink, 1, .{ .block_compression = .none });
    defer e.deinit();
    const value = try a.alloc(u8, 128 * 1024 + 7);
    defer a.free(value);
    @memset(value, 'v');
    try e.appendEntry(.{ .key = "key", .value = value });
    const uncompressed_bound = try StreamingEncoder.Block.memoryBound(&e);
    try std.testing.expect(uncompressed_bound < value.len);
    e.compression_policy = .snappy_adaptive;
    try e.compression_bytes.resize(a, 65537);
    @memset(e.compression_bytes.items, 0x5a);
    const old = e.compression_bytes.allocatedSlice();
    var block = try e.prepareBlock();
    defer block.deinit();
    _ = try block.next(null);
    try std.testing.expect(block.old_compression.?.ptr == old.ptr);
    try std.testing.expect(e.compression_bytes.items.ptr != old.ptr);
    for (old[0..65537]) |byte| try std.testing.expectEqual(@as(u8, 0x5a), byte);
    block.deinit();
    try std.testing.expect(!e.block_active and !e.finished);
    try std.testing.expectEqual(@as(usize, 1), e.block_entry_count);
}

test "controlled physical sync retained block allocations preserve provider bytes until bounded initialization" {
    const Painted = struct {
        backing: std.mem.Allocator,
        fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const ptr = self.backing.rawAlloc(len, alignment, ra) orelse return null;
            @memset(ptr[0..len], 0x5a);
            return ptr;
        }
        fn resize(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) bool {
            return false;
        }
        fn remap(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
            return null;
        }
        fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.backing.rawFree(bytes, alignment, ra);
        }
    };
    const a = std.testing.allocator;
    var memory = MemoryTableSink.init(a);
    defer memory.deinit();
    var sink = memory.sink();
    var e = try StreamingEncoder.init(a, &sink, 1, .{});
    defer e.deinit();
    var key: [8193]u8 = @splat('k');
    key[3] = ':';
    try e.appendEntry(.{ .namespace_name = "docs", .key = &key, .value = &key });
    var painted: Painted = .{ .backing = a };
    e.allocator = .{ .ptr = &painted, .vtable = &.{ .alloc = Painted.alloc, .resize = Painted.resize, .remap = Painted.remap, .free = Painted.free } };
    defer e.allocator = a;
    var block = try e.prepareBlock();
    defer block.deinit();
    var prefix_seen = false;
    var workspace_seen = false;
    var filter_seen = false;
    var bounds_seen = false;
    var metadata_seen = false;
    while (!(try block.step(null))) {
        switch (block.phase) {
            .prefix_encode => if (!prefix_seen) {
                try std.testing.expectEqual(@as(u8, 0x5a), block.prefix.?[0]);
                prefix_seen = true;
            },
            .snappy_encode => if (!workspace_seen) {
                try std.testing.expectEqual(@as(u32, 0x5a5a5a5a), block.workspace.?[0]);
                try std.testing.expectEqual(@as(u8, 0x5a), e.compression_bytes.allocatedSlice()[0]);
                workspace_seen = true;
            },
            .filter_zero => if (!filter_seen) {
                try std.testing.expectEqual(@as(u8, 0x5a), block.filters[block.filter_index].?.bytes[0]);
                filter_seen = true;
            },
            .bounds_copy => if (!bounds_seen) {
                try std.testing.expectEqual(@as(u8, 0x5a), block.largest_namespace.?[0]);
                try std.testing.expectEqual(@as(u8, 0x5a), block.largest_key.?[0]);
                bounds_seen = true;
            },
            .metadata_copy => if (!metadata_seen) {
                try std.testing.expectEqual(@as(u8, 0x5a), std.mem.sliceAsBytes(block.new_blocks.?)[0]);
                metadata_seen = true;
            },
            else => {},
        }
    }
    try std.testing.expect(prefix_seen and workspace_seen and filter_seen and bounds_seen and metadata_seen);
}

test "controlled physical sync retained entry stages preserve original state through rejection adoption and bounded retirement" {
    const Control = struct {
        reject: bool = false,
        calls: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        failure: anyerror,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const call = self.calls;
            self.calls += 1;
            if (self.reject or call == self.fail_at) return self.failure;
        }
    };
    const a = std.testing.allocator;
    const ns = try a.alloc(u8, 16385);
    defer a.free(ns);
    const key = try a.alloc(u8, 32769);
    defer a.free(key);
    const value = try a.alloc(u8, 65537);
    defer a.free(value);
    for ([_]CompressionPolicy{ .none, .snappy_adaptive }) |policy| {
        for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.FileNotFound }) |failure| {
            @memset(ns, 'n');
            @memset(key, 'k');
            @memset(value, 'v');
            key[30000] = ':';
            const input: Entry = .{ .namespace_name = ns, .key = key, .value = value };
            var memory = MemoryTableSink.init(a);
            defer memory.deinit();
            var sink = memory.sink();
            var e = try StreamingEncoder.init(a, &sink, 1, .{ .block_compression = policy });
            defer e.deinit();
            const baseline = e.workingSetBytes();
            const filter_crc = Crc32.hash(e.filter_builder.bytes);
            var control: Control = .{ .failure = failure };
            const payload: read_admission.Payload = .{ .ptr = &control, .vtable = &.{ .checkpoint = Control.check } };
            const original = read_admission.Port.init(&payload);
            for (0..2) |attempt| {
                var entry = try e.prepareEntry(input);
                defer entry.deinit();
                try std.testing.expectError(error.InvalidTableFile, e.prepareBlock());
                try std.testing.expectError(error.InvalidTableFile, e.prepareMetadata());
                try std.testing.expectError(error.InvalidTableFile, e.appendEntry(input));
                try std.testing.expectError(error.InvalidTableFile, e.finish());
                while (true) {
                    const phase = entry.phase;
                    const offset = entry.offset;
                    control.reject = true;
                    try std.testing.expectError(failure, entry.step(original));
                    try std.testing.expectEqual(phase, entry.phase);
                    try std.testing.expectEqual(offset, entry.offset);
                    try std.testing.expectEqual(@as(usize, 0), e.entry_count);
                    try std.testing.expectEqual(@as(usize, 0), e.block_bytes.items.len);
                    try std.testing.expectEqual(filter_crc, Crc32.hash(e.filter_builder.bytes));
                    try std.testing.expectEqual(@as(usize, header_len), sink.len());
                    control.reject = false;
                    var moved = entry;
                    const ready = try moved.step(original);
                    entry = moved;
                    if (ready) break;
                }
                try std.testing.expectEqual(entryHashes(ns, key), entry.hashes);
                try std.testing.expectEqual(entryPrefixHashes(e.prefix_extractor, ns, key).?, entry.prefix_hashes.?);
                if (attempt == 1) {
                    const calls = control.calls;
                    entry.adopt();
                    try std.testing.expectEqual(calls, control.calls);
                    try std.testing.expectEqual(@as(usize, 1), e.entry_count);
                    try std.testing.expectEqual(@as(usize, 1), e.prefix_hashes.items.len);
                    try std.testing.expectEqualStrings(key, e.block_smallest_key);
                    try std.testing.expectEqualStrings(key, e.block_largest_key);
                    @memset(ns, 0xcc);
                    @memset(key, 0xcc);
                    @memset(value, 0xcc);
                }
                control.reject = true;
                try std.testing.expectError(failure, entry.retireWithAdmission(original));
                try std.testing.expectError(error.InvalidTableFile, entry.step(null));
                control.reject = false;
                while (entry.phase != .retired) {
                    control.fail_at = control.calls + 1;
                    entry.retireWithAdmission(original) catch |cause| {
                        try std.testing.expectEqual(failure, cause);
                        try std.testing.expect(e.entry_active);
                        continue;
                    };
                }
                control.fail_at = std.math.maxInt(usize);
                try std.testing.expect(!e.entry_active and !e.finished);
                if (attempt == 0) {
                    try std.testing.expectEqual(baseline, e.workingSetBytes());
                    try std.testing.expectEqual(filter_crc, Crc32.hash(e.filter_builder.bytes));
                }
            }
            var result = try e.finish();
            defer result.filter.deinit(a);
            var decoded = try decodeAlloc(a, memory.out.items);
            defer decoded.deinit(a);
            try std.testing.expectEqual(@as(usize, 1), decoded.entries.len);
            for (decoded.entries[0].namespace_name.?) |byte| try std.testing.expectEqual(@as(u8, 'n'), byte);
            try std.testing.expectEqual(@as(u8, ':'), decoded.entries[0].key[30000]);
            try std.testing.expectEqual(@as(u8, 'k'), decoded.entries[0].key[0]);
            for (decoded.entries[0].value) |byte| try std.testing.expectEqual(@as(u8, 'v'), byte);
        }
    }
}

test "controlled physical sync retained entry vector growth fits prepaid memory and copies bounded existing ranges" {
    const a = std.testing.allocator;
    var memory = MemoryTableSink.init(a);
    defer memory.deinit();
    var sink = memory.sink();
    var e = try StreamingEncoder.init(a, &sink, 10000, .{ .block_compression = .none });
    defer e.deinit();
    for (0..9000) |i| {
        var key: [32]u8 = undefined;
        try e.appendEntry(.{ .namespace_name = "docs", .key = try std.fmt.bufPrint(&key, "{d:0>8}:key", .{i}), .value = "" });
    }
    const Exact = struct {
        fn set(comptime T: type, alloc: std.mem.Allocator, list: *std.ArrayListUnmanaged(T)) !void {
            const copy = try alloc.dupe(T, list.items);
            list.deinit(alloc);
            list.* = .fromOwnedSlice(copy);
        }
    };
    try Exact.set(u8, a, &e.block_bytes);
    try Exact.set(u16, a, &e.entry_offsets);
    try Exact.set([2]u64, a, &e.prefix_hashes);
    try Exact.set([2]u64, a, &e.block_hashes);
    try Exact.set([2]u64, a, &e.block_prefix_hashes);
    const input: Entry = .{ .namespace_name = "docs", .key = "zzzz:new", .value = "v" };
    try std.testing.expect(!try e.needsBlockBeforeEntry(input));
    const baseline = e.workingSetBytes();
    const bound = try StreamingEncoder.EntryPreparation.memoryBound(&e, input);
    const bytes = try a.alloc(u8, bound);
    defer a.free(bytes);
    var fixed = std.heap.FixedBufferAllocator.init(bytes);
    e.allocator = fixed.allocator();
    defer e.allocator = a;
    var entry = try e.prepareEntry(input);
    defer entry.deinit();
    var copied: usize = 0;
    while (true) {
        const ready = try entry.step(null);
        const total = entry.data.copied + entry.offsets.copied * 2 + (entry.run_prefixes.copied + entry.block_hashes.copied + entry.block_prefixes.copied) * 16;
        try std.testing.expect(total - copied <= 16 * 1024);
        copied = total;
        if (ready) break;
    }
    try std.testing.expect(entry.data.copied > 16 * 1024);
    try std.testing.expectEqual(@as(usize, 9000), entry.offsets.copied);
    try std.testing.expectEqual(@as(usize, 9000), entry.run_prefixes.copied);
    try std.testing.expect(fixed.end_index <= bound);
    try std.testing.expectEqual(baseline, e.workingSetBytes());
    try std.testing.expectEqual(@as(usize, 9000), e.entry_count);
    entry.deinit();
    try std.testing.expect(!e.entry_active);
}

test "controlled physical sync retained entry allocation failures leave a retryable encoder and no adopted hash bits" {
    const Fixture = struct {
        fn noResize(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) bool {
            return false;
        }
        fn noRemap(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
            return null;
        }
        fn run(backing: std.mem.Allocator) !void {
            var vtable = backing.vtable.*;
            vtable.resize = noResize;
            vtable.remap = noRemap;
            const a: std.mem.Allocator = .{ .ptr = backing.ptr, .vtable = &vtable };
            var memory = MemoryTableSink.init(a);
            defer memory.deinit();
            var sink = memory.sink();
            var e = try StreamingEncoder.init(a, &sink, 1, .{});
            defer e.deinit();
            const key: [4097]u8 = @splat('k');
            const input: Entry = .{ .namespace_name = "docs", .key = &key, .value = "value" };
            {
                var entry = try e.prepareEntry(input);
                defer entry.deinit();
                while (!(try entry.step(null))) {}
                try std.testing.expectEqual(@as(usize, 0), e.entry_count);
            }
            try std.testing.expect(!e.entry_active and !e.finished);
            try e.appendEntry(input);
            var result = try e.finish();
            defer result.filter.deinit(a);
            var decoded = try decodeAlloc(a, memory.out.items);
            defer decoded.deinit(a);
            try std.testing.expectEqual(@as(usize, 1), decoded.entries.len);
            try std.testing.expectEqualSlices(u8, &key, decoded.entries[0].key);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "controlled physical sync encoder initialization prepays and zeros bounded ranges before stable owning header" {
    const Control = struct {
        reject: bool = false,
        calls: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        failure: anyerror,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const call = self.calls;
            self.calls += 1;
            if (self.reject or call == self.fail_at) return self.failure;
        }
    };
    const a = std.testing.allocator;
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.FileNotFound }) |failure| {
        const bound = try StreamingEncoder.Initialization.memoryBound(1_000_000, .{});
        const prepaid = try a.alloc(u8, bound);
        defer a.free(prepaid);
        @memset(prepaid, 0x5a);
        var fixed = std.heap.FixedBufferAllocator.init(prepaid);
        var construction = try StreamingEncoder.Initialization.init(fixed.allocator(), 1_000_000, .{}, 0);
        defer construction.deinit();
        var c: Control = .{ .failure = failure };
        const payload: read_admission.Payload = .{ .ptr = &c, .vtable = &.{ .checkpoint = Control.check } };
        const original = read_admission.Port.init(&payload);
        while (true) {
            const phase = construction.phase;
            const offset = construction.offset;
            c.reject = true;
            try std.testing.expectError(failure, construction.step(original));
            try std.testing.expectEqual(phase, construction.phase);
            try std.testing.expectEqual(offset, construction.offset);
            c.reject = false;
            var moved = construction;
            const done = try moved.step(original);
            if (phase == .zero and !done) try std.testing.expect(moved.offset - offset <= 16 * 1024);
            if (phase == .allocate) try std.testing.expectEqual(@as(u8, 0x5a), moved.builder.?.bytes[0]);
            construction = moved;
            if (done) break;
        }
        for (construction.builder.?.bytes) |byte| try std.testing.expectEqual(@as(u8, 0), byte);
        try std.testing.expect(fixed.end_index <= bound);
        const command = (try construction.next(original)).?;
        try std.testing.expectEqual(@as(usize, header_len), command.bytes.len);
        try std.testing.expectEqualSlices(u8, magic, command.bytes[0..8]);
        c.reject = true;
        try std.testing.expectError(failure, construction.next(original));
        c.reject = false;
        const again = (try construction.next(original)).?;
        try std.testing.expect(command.bytes.ptr == again.bytes.ptr);
        construction.acknowledge();
        var memory = MemoryTableSink.init(a);
        defer memory.deinit();
        var sink = memory.sink();
        var e = try construction.takeEncoder(&sink);
        defer e.deinit();
        try std.testing.expectError(error.InvalidTableFile, construction.takeEncoder(&sink));
        try std.testing.expectError(error.ExecutionWaitUnsupported, e.appendEntry(.{ .key = "key", .value = "" }));
        try std.testing.expectError(error.ExecutionWaitUnsupported, e.finish());
        try std.testing.expectEqual(@as(usize, 0), memory.out.items.len);
        var retirement = try e.prepareRetirement();
        defer retirement.deinit();
        var partial = false;
        while (true) {
            c.reject = true;
            const slot = retirement.slot;
            const offset = retirement.offset;
            const before = e.workingSetBytes();
            try std.testing.expectError(failure, retirement.step(original));
            try std.testing.expectEqual(slot, retirement.slot);
            try std.testing.expectEqual(offset, retirement.offset);
            try std.testing.expectEqual(before, e.workingSetBytes());
            c.reject = false;
            var moved = retirement;
            const done = try moved.step(original);
            retirement = moved;
            if (retirement.slot == 0 and retirement.offset != 0) {
                try std.testing.expectEqual(before, e.workingSetBytes());
                partial = true;
            }
            if (done) break;
        }
        try std.testing.expect(partial);
        try std.testing.expectEqual(@as(u64, 0), e.workingSetBytes());
        try std.testing.expectError(error.InvalidTableFile, e.prepareRetirement());
    }
    var fail = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    try std.testing.expectError(error.TableFileTooLarge, StreamingEncoder.Initialization.init(fail.allocator(), 1, .{ .bloom_config = .{ .min_bits = std.math.maxInt(usize) } }, 0));
    try std.testing.expectError(error.TableFileTooLarge, StreamingEncoder.Initialization.init(fail.allocator(), 0, .{ .bloom_config = .{ .bits_per_key = std.math.maxInt(usize) } }, 0));
    try std.testing.expectError(error.TableFileTooLarge, StreamingEncoder.Initialization.init(fail.allocator(), 1, .{}, std.math.maxInt(usize)));
    try std.testing.expectEqual(@as(usize, 0), fail.alloc_index);
}

test "controlled physical sync metadata and encoder retirement preserve each original rejection and exact live accounting" {
    const Control = struct {
        calls: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        stop_prefix: bool = true,
        metadata: *StreamingEncoder.Metadata,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const call = self.calls;
            self.calls += 1;
            if (call == self.fail_at or (self.stop_prefix and self.metadata.phase == .prefix_zero and self.metadata.offset >= 16 * 1024)) return error.Canceled;
        }
    };
    const a = std.testing.allocator;
    var counting = std.testing.FailingAllocator.init(a, .{});
    var memory = MemoryTableSink.init(a);
    defer memory.deinit();
    var sink = memory.sink();
    var e = try StreamingEncoder.init(counting.allocator(), &sink, 64, .{ .bloom_config = .{ .min_bits = 1 << 24 } });
    defer e.deinit();
    const value: [8193]u8 = @splat('v');
    var key: [4097]u8 = @splat('k');
    key[2] = ':';
    for (0..64) |i| {
        key[0] = @intCast(i);
        try e.appendEntry(.{ .namespace_name = "docs", .key = &key, .value = &value });
    }
    try e.finishBlocks();
    var metadata = try e.prepareMetadata();
    defer metadata.deinit();
    try std.testing.expectError(error.InvalidTableFile, e.prepareRetirement());
    var c: Control = .{ .metadata = &metadata };
    const payload: read_admission.Payload = .{ .ptr = &c, .vtable = &.{ .checkpoint = Control.check } };
    const original = read_admission.Port.init(&payload);
    var caught = false;
    while (true) {
        const command = metadata.next(original) catch |cause| {
            try std.testing.expectEqual(error.Canceled, cause);
            caught = true;
            break;
        } orelse break;
        if (command.append) try sink.appendSlice(command.bytes) else try sink.writeAt(command.offset, command.bytes);
        metadata.acknowledge();
    }
    try std.testing.expect(caught and metadata.prefix_builder != null);
    c.stop_prefix = false;
    while (!metadata.retired) {
        c.fail_at = c.calls + 1;
        metadata.retireWithAdmission(original) catch |cause| {
            try std.testing.expectEqual(error.Canceled, cause);
            try std.testing.expect(e.metadata_active);
            try std.testing.expectError(error.InvalidTableFile, metadata.next(null));
            continue;
        };
    }
    c.fail_at = std.math.maxInt(usize);
    try std.testing.expect(!e.metadata_active);
    var retirement = try e.prepareRetirement();
    defer retirement.deinit();
    var interrupted = false;
    while (true) {
        const used = counting.allocated_bytes - counting.freed_bytes;
        const slot = retirement.slot;
        const block = retirement.block_index;
        const field = retirement.block_field;
        const offset = retirement.offset;
        c.fail_at = c.calls;
        try std.testing.expectError(error.Canceled, retirement.step(original));
        try std.testing.expectEqual(used, counting.allocated_bytes - counting.freed_bytes);
        try std.testing.expectEqual(slot, retirement.slot);
        try std.testing.expectEqual(block, retirement.block_index);
        try std.testing.expectEqual(field, retirement.block_field);
        try std.testing.expectEqual(offset, retirement.offset);
        c.fail_at = std.math.maxInt(usize);
        const done = try retirement.step(original);
        if (retirement.slot == slot and retirement.offset != 0) {
            try std.testing.expectEqual(used, counting.allocated_bytes - counting.freed_bytes);
            interrupted = true;
        }
        if (done) break;
    }
    try std.testing.expect(interrupted);
    try std.testing.expectEqual(counting.allocated_bytes, counting.freed_bytes);
    try std.testing.expectEqual(@as(u64, 0), e.workingSetBytes());
}

test "controlled physical sync encoder construction allocation failure retires partial filter and header ownership" {
    const Fixture = struct {
        fn noResize(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) bool {
            return false;
        }
        fn noRemap(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
            return null;
        }
        fn run(backing: std.mem.Allocator) !void {
            var vtable = backing.vtable.*;
            vtable.resize = noResize;
            vtable.remap = noRemap;
            const a: std.mem.Allocator = .{ .ptr = backing.ptr, .vtable = &vtable };
            var construction = try StreamingEncoder.Initialization.init(a, 8192, .{}, 0);
            defer construction.deinit();
            var memory = MemoryTableSink.init(a);
            defer memory.deinit();
            var sink = memory.sink();
            const command = (try construction.next(null)).?;
            try sink.appendSlice(command.bytes);
            construction.acknowledge();
            var e = try construction.takeEncoder(&sink);
            defer e.deinit();
            var retirement = try e.prepareRetirement();
            defer retirement.deinit();
            while (!(try retirement.step(null))) {}
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}
