// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const bloom = @import("bloom");
const snappy = @import("../../encoding/snappy.zig");

pub const magic = "ALSMTBL2";
pub const footer_magic = "ALSMIDX2";
pub const header_len = magic.len + 12;
pub const footer_len = footer_magic.len + @sizeOf(u64) * 2 + @sizeOf(u32) * 2;
pub const default_block_size: usize = 32 * 1024;
pub const version: u32 = 9;
pub const max_entry_data_len: usize = std.math.maxInt(u32);
pub const max_entry_count: usize = std.math.maxInt(u32);
pub const default_filter_config: bloom.Config = .{ .bits_per_key = 14 };
const block_filter_config: bloom.Config = default_filter_config;
const min_compress_block_bytes: usize = 1024;
const compression_savings_denominator: usize = 8;
const prefix_block_magic = "ALSMPFX1";
const prefix_restart_interval: usize = 16;

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
    return @as(u64, 1) << @intCast(@intFromEnum(codec));
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

    entry_offsets: []u32,
    entry_data_start: usize,
    entry_data_len: usize,
    filter: bloom.OwnedFilter,
    prefix_extractor: PrefixExtractor = .none,
    prefix_filter: ?bloom.OwnedFilter = null,
    blocks: []BlockMeta = &.{},

    pub fn deinit(self: *TableIndex, allocator: std.mem.Allocator) void {
        allocator.free(self.entry_offsets);
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
        return .{
            .entry_offsets = try allocator.dupe(u32, self.entry_offsets),
            .entry_data_start = self.entry_data_start,
            .entry_data_len = self.entry_data_len,
            .filter = try self.filter.clone(allocator),
            .prefix_extractor = self.prefix_extractor,
            .prefix_filter = if (self.prefix_filter) |filter| try filter.clone(allocator) else null,
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
        return self.entry_offsets.len;
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
        };
    }

    pub fn entryStart(self: *const TableIndex, index: usize) u32 {
        return self.entry_offsets[index];
    }

    pub fn entryEnd(self: *const TableIndex, index: usize) u32 {
        if (index + 1 < self.entry_offsets.len) return self.entry_offsets[index + 1];
        return @intCast(self.entry_data_len);
    }

    pub fn entryDataWindow(self: *const TableIndex, entry_index: usize, block_size: usize) EntryDataWindow {
        if (self.findBlockIndexForEntry(entry_index)) |block_index| return self.blockWindow(block_index);
        const entry_start: usize = self.entryStart(entry_index);
        const entry_end: usize = self.entryEnd(entry_index);
        const block_start = (entry_start / block_size) * block_size;
        const min_block_end = block_start + block_size;
        const block_end = @min(@max(min_block_end, entry_end), self.entry_data_len);
        return .{
            .relative_offset = @intCast(block_start),
            .len = @intCast(block_end - block_start),
            .physical_relative_offset = @intCast(block_start),
            .physical_len = @intCast(block_end - block_start),
            .compression = .none,
        };
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
};

pub const EntryDataWindow = struct {
    relative_offset: u32,
    len: u32,
    physical_relative_offset: u32 = 0,
    physical_len: u32 = 0,
    compression: BlockCompression = .none,

    pub fn physicalRelativeOffset(self: EntryDataWindow) u32 {
        return if (self.physical_len == 0 and self.compression == .none) self.relative_offset else self.physical_relative_offset;
    }

    pub fn physicalLen(self: EntryDataWindow) u32 {
        return if (self.physical_len == 0 and self.compression == .none) self.len else self.physical_len;
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
        if (self.owns_raw) allocator.free(self.raw);
        self.* = undefined;
    }

    pub fn entryCount(self: *const BorrowedDecoded) usize {
        return self.entry_offsets.len;
    }

    pub fn entryAt(self: *const BorrowedDecoded, index: usize) !Entry {
        return try parseEntryAt(self.raw, self.entry_data_start + self.entry_offsets[index]);
    }

    pub fn lowerBound(self: *const BorrowedDecoded, namespace_name: ?[]const u8, key: []const u8) !usize {
        if (self.blocks.len > 0) {
            const block_index = findBlockLowerBoundInMetas(self.blocks, namespace_name, key) orelse return self.entry_offsets.len;
            return try lowerBoundInBlockRaw(
                self.raw[self.entry_data_start + self.blocks[block_index].relative_offset .. self.entry_data_start + self.blocks[block_index].relative_offset + self.blocks[block_index].len],
                self.entry_offsets,
                self.blocks[block_index],
                namespace_name,
                key,
            );
        }
        var lo: usize = 0;
        var hi: usize = self.entry_offsets.len;
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
        if (idx >= self.entry_offsets.len) return null;
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
    first_entry_index: u32,
    entry_count: u32,
    smallest_namespace_name: ?[]u8 = null,
    smallest_key: []u8,
    largest_namespace_name: ?[]u8 = null,
    largest_key: []u8,
    filter: bloom.OwnedFilter,
    prefix_filter: ?bloom.OwnedFilter = null,
    hash_slots: []u32,

    fn deinit(self: *OwnedEncodedBlockMeta, allocator: std.mem.Allocator) void {
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
        try self.out.appendSlice(self.allocator, bytes);
    }

    fn appendByte(ptr: *anyopaque, byte: u8) !void {
        const self: *MemoryTableSink = @ptrCast(@alignCast(ptr));
        try self.out.append(self.allocator, byte);
    }

    fn writeAt(ptr: *anyopaque, offset: usize, bytes: []const u8) !void {
        const self: *MemoryTableSink = @ptrCast(@alignCast(ptr));
        if (offset > self.out.items.len or bytes.len > self.out.items.len - offset) return error.InvalidTableFile;
        @memcpy(self.out.items[offset..][0..bytes.len], bytes);
    }
};

const memory_table_sink_vtable = TableSink.VTable{
    .len = MemoryTableSink.len,
    .append_slice = MemoryTableSink.appendSlice,
    .append_byte = MemoryTableSink.appendByte,
    .write_at = MemoryTableSink.writeAt,
};

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

    var entry_offsets = std.ArrayListUnmanaged(u32).empty;
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

        try entry_offsets.append(allocator, entry_start);
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
    try sink.writeAt(entry_data_len_offset, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, physical_entry_data_len_u32))));

    const metadata_offset = sink.len();
    for (entry_offsets.items) |offset| try sinkAppendU32(sink, offset);
    try sinkAppendU32(sink, try checkedU32(encoded_filter.len));
    try sink.appendSlice(encoded_filter);
    try sinkAppendU32(sink, try checkedU32(blocks.items.len));
    for (blocks.items) |block| {
        try sinkAppendU32(sink, block.relative_offset);
        try sinkAppendU32(sink, block.len);
        try sinkAppendU32(sink, block.first_entry_index);
        try sinkAppendU32(sink, block.entry_count);
        try sinkAppendU32(sink, if (block.largest_namespace_name) |name| try checkedU32(name.len) else std.math.maxInt(u32));
        try sinkAppendU32(sink, try checkedU32(block.largest_key.len));
        if (block.largest_namespace_name) |name| try sink.appendSlice(name);
        try sink.appendSlice(block.largest_key);
    }
    try sinkAppendU32(sink, try checkedU32(blocks.items.len));
    for (blocks.items) |block| {
        const encoded_block_filter = try block.filter.encodeInto(allocator, &encoded_filter_bytes);
        try sinkAppendU32(sink, try checkedU32(encoded_block_filter.len));
        try sink.appendSlice(encoded_block_filter);
    }
    try sinkAppendU32(sink, try checkedU32(blocks.items.len));
    for (blocks.items) |block| {
        try sinkAppendU32(sink, try checkedU32(block.hash_slots.len));
        for (block.hash_slots) |slot| try sinkAppendU32(sink, slot);
    }
    try sinkAppendU32(sink, try checkedU32(blocks.items.len));
    for (blocks.items) |block| {
        try sinkAppendU32(sink, block.physical_relative_offset);
        try sinkAppendU32(sink, block.physical_len);
        try sinkAppendU32(sink, @intFromEnum(block.compression));
    }
    try sinkAppendU32(sink, try checkedU32(blocks.items.len));
    for (blocks.items) |block| {
        try sinkAppendU32(sink, if (block.smallest_namespace_name) |name| try checkedU32(name.len) else std.math.maxInt(u32));
        try sinkAppendU32(sink, try checkedU32(block.smallest_key.len));
        if (block.smallest_namespace_name) |name| try sink.appendSlice(name);
        try sink.appendSlice(block.smallest_key);
    }
    try sinkAppendU32(sink, @intFromEnum(options.prefix_extractor));
    try sinkAppendU32(sink, try checkedU32(encoded_prefix_filter.len));
    try sink.appendSlice(encoded_prefix_filter);
    try sinkAppendU32(sink, try checkedU32(blocks.items.len));
    for (blocks.items) |block| {
        const encoded_block_prefix_filter = if (block.prefix_filter) |block_prefix_filter|
            try block_prefix_filter.encodeInto(allocator, &encoded_filter_bytes)
        else
            "";
        try sinkAppendU32(sink, try checkedU32(encoded_block_prefix_filter.len));
        try sink.appendSlice(encoded_block_prefix_filter);
    }
    const metadata_len = sink.len() - metadata_offset;

    try sink.appendSlice(footer_magic);
    try sinkAppendU64(sink, metadata_offset);
    try sinkAppendU64(sink, metadata_len);
    try sinkAppendU32(sink, try checkedU32(entries.len));
    try sinkAppendU32(sink, entry_data_len_u32);

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
    prefix_extractor: PrefixExtractor,
    compression_stats_out: ?*CompressionStats,
    filter_builder: bloom.Builder,
    filter_builder_active: bool = true,
    prefix_filter_builder: bloom.Builder,
    prefix_filter_builder_active: bool = true,
    entry_offsets: std.ArrayListUnmanaged(u32) = .empty,
    blocks: std.ArrayListUnmanaged(OwnedEncodedBlockMeta) = .empty,
    block_bytes: std.ArrayListUnmanaged(u8) = .empty,
    compression_bytes: std.ArrayListUnmanaged(u8) = .empty,
    encoded_filter_bytes: std.ArrayListUnmanaged(u8) = .empty,
    block_hashes: std.ArrayListUnmanaged([2]u64) = .empty,
    block_prefix_hashes: std.ArrayListUnmanaged([2]u64) = .empty,
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
    finished: bool = false,

    pub fn init(
        allocator: std.mem.Allocator,
        sink: *TableSink,
        expected_entries: usize,
        options: StreamingEncoderOptions,
    ) !StreamingEncoder {
        if (expected_entries > max_entry_count) return error.TableFileTooLarge;
        var filter_builder = try bloom.Builder.init(allocator, expected_entries, options.bloom_config);
        errdefer filter_builder.deinit();
        var prefix_filter_builder = try bloom.Builder.init(allocator, expected_entries, options.bloom_config);
        errdefer prefix_filter_builder.deinit();

        try sink.appendSlice(magic);
        try sinkAppendU32(sink, version);
        const entry_count_offset = sink.len();
        try sinkAppendU32(sink, 0);
        const entry_data_len_offset = sink.len();
        try sinkAppendU32(sink, 0);
        const entry_data_start = sink.len();

        return .{
            .allocator = allocator,
            .sink = sink,
            .compression_policy = options.block_compression,
            .prefix_extractor = options.prefix_extractor,
            .compression_stats_out = options.compression_stats,
            .filter_builder = filter_builder,
            .prefix_filter_builder = prefix_filter_builder,
            .entry_count_offset = entry_count_offset,
            .entry_data_len_offset = entry_data_len_offset,
            .entry_data_start = entry_data_start,
        };
    }

    pub fn deinit(self: *StreamingEncoder) void {
        if (self.filter_builder_active) self.filter_builder.deinit();
        if (self.prefix_filter_builder_active) self.prefix_filter_builder.deinit();
        self.entry_offsets.deinit(self.allocator);
        for (self.blocks.items) |*block| block.deinit(self.allocator);
        self.blocks.deinit(self.allocator);
        self.block_bytes.deinit(self.allocator);
        self.compression_bytes.deinit(self.allocator);
        self.encoded_filter_bytes.deinit(self.allocator);
        self.block_hashes.deinit(self.allocator);
        self.block_prefix_hashes.deinit(self.allocator);
        self.clearBlockSmallest();
        self.clearBlockLargest();
        self.* = undefined;
    }

    pub fn appendEntry(self: *StreamingEncoder, entry: Entry) !void {
        if (self.finished) return error.InvalidTableFile;
        if (self.entry_count >= max_entry_count) return error.TableFileTooLarge;

        const entry_start_usize = self.logical_entry_data_len;
        const entry_start = try checkedU32(entry_start_usize);
        const entry_len = try tableEntryEncodedLen(entry);
        if (entry_start_usize > max_entry_data_len or entry_len > max_entry_data_len - entry_start_usize) {
            return error.TableFileTooLarge;
        }

        if (self.block_start == null) {
            self.block_start = entry_start;
            self.block_first_entry_index = self.entry_count;
            try self.setBlockSmallest(entry);
        } else if (self.block_entry_count > 0 and self.block_bytes.items.len + entry_len > default_block_size) {
            try self.flushBlock();
            self.block_start = entry_start;
            self.block_first_entry_index = self.entry_count;
            try self.setBlockSmallest(entry);
        }

        try self.entry_offsets.append(self.allocator, entry_start);
        try appendEntryBytesToList(self.allocator, &self.block_bytes, entry);
        self.logical_entry_data_len += entry_len;
        self.entry_count += 1;
        self.block_entry_count += 1;

        const hashes = entryHashes(entry.namespace_name, entry.key);
        self.filter_builder.addHashes(hashes[0], hashes[1]);
        try self.block_hashes.append(self.allocator, hashes);
        if (entryPrefixHashes(self.prefix_extractor, entry.namespace_name, entry.key)) |prefix_hashes| {
            self.prefix_filter_builder.addHashes(prefix_hashes[0], prefix_hashes[1]);
            try self.block_prefix_hashes.append(self.allocator, prefix_hashes);
        }
        try self.setBlockLargest(entry);
    }

    pub fn finish(self: *StreamingEncoder) !StreamingEncoderResult {
        if (self.finished) return error.InvalidTableFile;
        self.finished = true;
        try self.flushBlock();

        const entry_data_len_u32 = try checkedU32(self.logical_entry_data_len);
        const physical_entry_data_len = self.sink.len() - self.entry_data_start;
        const physical_entry_data_len_u32 = try checkedU32(physical_entry_data_len);
        try self.sink.writeAt(self.entry_count_offset, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, try checkedU32(self.entry_count)))));
        try self.sink.writeAt(self.entry_data_len_offset, &@as([4]u8, @bitCast(std.mem.nativeToLittle(u32, physical_entry_data_len_u32))));

        var filter = self.filter_builder.finish();
        self.filter_builder_active = false;
        errdefer filter.deinit(self.allocator);
        const encoded_filter = try filter.encodeInto(self.allocator, &self.encoded_filter_bytes);
        var prefix_filter = self.prefix_filter_builder.finish();
        self.prefix_filter_builder_active = false;
        defer prefix_filter.deinit(self.allocator);
        const encoded_prefix_filter = try prefix_filter.encodeAlloc(self.allocator);
        defer self.allocator.free(encoded_prefix_filter);

        const metadata_offset = self.sink.len();
        for (self.entry_offsets.items) |offset| try sinkAppendU32(self.sink, offset);
        try sinkAppendU32(self.sink, try checkedU32(encoded_filter.len));
        try self.sink.appendSlice(encoded_filter);
        try sinkAppendU32(self.sink, try checkedU32(self.blocks.items.len));
        for (self.blocks.items) |block| {
            try sinkAppendU32(self.sink, block.relative_offset);
            try sinkAppendU32(self.sink, block.len);
            try sinkAppendU32(self.sink, block.first_entry_index);
            try sinkAppendU32(self.sink, block.entry_count);
            try sinkAppendU32(self.sink, if (block.largest_namespace_name) |name| try checkedU32(name.len) else std.math.maxInt(u32));
            try sinkAppendU32(self.sink, try checkedU32(block.largest_key.len));
            if (block.largest_namespace_name) |name| try self.sink.appendSlice(name);
            try self.sink.appendSlice(block.largest_key);
        }
        try sinkAppendU32(self.sink, try checkedU32(self.blocks.items.len));
        for (self.blocks.items) |block| {
            const encoded_block_filter = try block.filter.encodeInto(self.allocator, &self.encoded_filter_bytes);
            try sinkAppendU32(self.sink, try checkedU32(encoded_block_filter.len));
            try self.sink.appendSlice(encoded_block_filter);
        }
        try sinkAppendU32(self.sink, try checkedU32(self.blocks.items.len));
        for (self.blocks.items) |block| {
            try sinkAppendU32(self.sink, try checkedU32(block.hash_slots.len));
            for (block.hash_slots) |slot| try sinkAppendU32(self.sink, slot);
        }
        try sinkAppendU32(self.sink, try checkedU32(self.blocks.items.len));
        for (self.blocks.items) |block| {
            try sinkAppendU32(self.sink, block.physical_relative_offset);
            try sinkAppendU32(self.sink, block.physical_len);
            try sinkAppendU32(self.sink, @intFromEnum(block.compression));
        }
        try sinkAppendU32(self.sink, try checkedU32(self.blocks.items.len));
        for (self.blocks.items) |block| {
            try sinkAppendU32(self.sink, if (block.smallest_namespace_name) |name| try checkedU32(name.len) else std.math.maxInt(u32));
            try sinkAppendU32(self.sink, try checkedU32(block.smallest_key.len));
            if (block.smallest_namespace_name) |name| try self.sink.appendSlice(name);
            try self.sink.appendSlice(block.smallest_key);
        }
        try sinkAppendU32(self.sink, @intFromEnum(self.prefix_extractor));
        try sinkAppendU32(self.sink, try checkedU32(encoded_prefix_filter.len));
        try self.sink.appendSlice(encoded_prefix_filter);
        try sinkAppendU32(self.sink, try checkedU32(self.blocks.items.len));
        for (self.blocks.items) |block| {
            const encoded_block_prefix_filter = if (block.prefix_filter) |block_prefix_filter|
                try block_prefix_filter.encodeInto(self.allocator, &self.encoded_filter_bytes)
            else
                "";
            try sinkAppendU32(self.sink, try checkedU32(encoded_block_prefix_filter.len));
            try self.sink.appendSlice(encoded_block_prefix_filter);
        }
        const metadata_len = self.sink.len() - metadata_offset;

        try self.sink.appendSlice(footer_magic);
        try sinkAppendU64(self.sink, metadata_offset);
        try sinkAppendU64(self.sink, metadata_len);
        try sinkAppendU32(self.sink, try checkedU32(self.entry_count));
        try sinkAppendU32(self.sink, entry_data_len_u32);

        const compression_stats = summarizeOwnedCompressionStats(self.logical_entry_data_len, physical_entry_data_len, self.blocks.items);
        if (self.compression_stats_out) |stats| stats.* = compression_stats;
        return .{
            .size_bytes = self.sink.len(),
            .entry_count = self.entry_count,
            .filter = filter,
            .compression_stats = compression_stats,
        };
    }

    fn setBlockSmallest(self: *StreamingEncoder, entry: Entry) !void {
        self.clearBlockSmallest();
        self.block_smallest_namespace_name = if (entry.namespace_name) |name| try self.allocator.dupe(u8, name) else null;
        errdefer if (self.block_smallest_namespace_name) |name| self.allocator.free(name);
        self.block_smallest_key = try self.allocator.dupe(u8, entry.key);
    }

    fn clearBlockSmallest(self: *StreamingEncoder) void {
        if (self.block_smallest_namespace_name) |name| self.allocator.free(name);
        self.block_smallest_namespace_name = null;
        if (self.block_smallest_key.len > 0) self.allocator.free(self.block_smallest_key);
        self.block_smallest_key = &.{};
    }

    fn setBlockLargest(self: *StreamingEncoder, entry: Entry) !void {
        self.clearBlockLargest();
        self.block_largest_namespace_name = if (entry.namespace_name) |name| try self.allocator.dupe(u8, name) else null;
        errdefer if (self.block_largest_namespace_name) |name| self.allocator.free(name);
        self.block_largest_key = try self.allocator.dupe(u8, entry.key);
    }

    fn clearBlockLargest(self: *StreamingEncoder) void {
        if (self.block_largest_namespace_name) |name| self.allocator.free(name);
        self.block_largest_namespace_name = null;
        if (self.block_largest_key.len > 0) self.allocator.free(self.block_largest_key);
        self.block_largest_key = &.{};
    }

    fn flushBlock(self: *StreamingEncoder) !void {
        if (self.block_entry_count == 0) return;

        var encoded_payload = try encodeBlockPayloadAlloc(self.allocator, self.block_bytes.items, self.compression_policy, &self.compression_bytes);
        defer encoded_payload.deinit(self.allocator);

        const physical_relative_offset = try checkedU32(self.sink.len() - self.entry_data_start);
        try self.sink.appendSlice(encoded_payload.payload);

        var block_filter = try buildFilterFromHashesAlloc(self.allocator, self.block_hashes.items, block_filter_config);
        errdefer block_filter.deinit(self.allocator);
        var block_prefix_filter = try buildFilterFromHashesAlloc(self.allocator, self.block_prefix_hashes.items, block_filter_config);
        errdefer block_prefix_filter.deinit(self.allocator);
        const hash_slots = try buildBlockHashSlotsFromHashesAlloc(self.allocator, self.block_hashes.items, self.block_first_entry_index);
        errdefer self.allocator.free(hash_slots);

        const smallest_namespace_name = self.block_smallest_namespace_name;
        self.block_smallest_namespace_name = null;
        const smallest_key = self.block_smallest_key;
        self.block_smallest_key = &.{};
        const largest_namespace_name = self.block_largest_namespace_name;
        self.block_largest_namespace_name = null;
        const largest_key = self.block_largest_key;
        self.block_largest_key = &.{};
        errdefer {
            if (smallest_namespace_name) |name| self.allocator.free(name);
            self.allocator.free(smallest_key);
            if (largest_namespace_name) |name| self.allocator.free(name);
            self.allocator.free(largest_key);
        }

        try self.blocks.append(self.allocator, .{
            .relative_offset = self.block_start.?,
            .len = try checkedU32(self.block_bytes.items.len),
            .physical_relative_offset = physical_relative_offset,
            .physical_len = try checkedU32(encoded_payload.payload.len),
            .compression = encoded_payload.compression,
            .first_entry_index = try checkedU32(self.block_first_entry_index),
            .entry_count = try checkedU32(self.block_entry_count),
            .smallest_namespace_name = smallest_namespace_name,
            .smallest_key = smallest_key,
            .largest_namespace_name = largest_namespace_name,
            .largest_key = largest_key,
            .filter = block_filter,
            .prefix_filter = block_prefix_filter,
            .hash_slots = hash_slots,
        });

        self.block_bytes.clearRetainingCapacity();
        self.block_hashes.clearRetainingCapacity();
        self.block_prefix_hashes.clearRetainingCapacity();
        self.block_start = null;
        self.block_entry_count = 0;
    }
};

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

fn summarizeOwnedCompressionStats(logical_entry_data_len: usize, physical_entry_data_len: usize, blocks: []const OwnedEncodedBlockMeta) CompressionStats {
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

    var encoded_payload = try encodeBlockPayloadAlloc(allocator, block_bytes.items, compression_policy, compression_bytes);
    defer encoded_payload.deinit(allocator);

    const physical_relative_offset = try checkedU32(sink.len() - entry_data_start);
    try sink.appendSlice(encoded_payload.payload);

    var block_filter = try buildFilterAlloc(allocator, entries[block_first_entry_index .. block_first_entry_index + block_entry_count], block_filter_config);
    errdefer block_filter.deinit(allocator);
    var block_prefix_filter = try buildPrefixFilterAlloc(allocator, entries[block_first_entry_index .. block_first_entry_index + block_entry_count], prefix_extractor, block_filter_config);
    errdefer block_prefix_filter.deinit(allocator);
    const hash_slots = try buildBlockHashSlotsAlloc(allocator, entries[block_first_entry_index .. block_first_entry_index + block_entry_count], block_first_entry_index);
    errdefer allocator.free(hash_slots);
    try blocks.append(allocator, .{
        .relative_offset = block_start,
        .len = try checkedU32(block_bytes.items.len),
        .physical_relative_offset = physical_relative_offset,
        .physical_len = try checkedU32(encoded_payload.payload.len),
        .compression = encoded_payload.compression,
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
    var builder = try bloom.Builder.init(allocator, entries.len, config);
    errdefer builder.deinit();
    for (entries) |entry| {
        if (entryPrefixHashes(extractor, entry.namespace_name, entry.key)) |hashes| {
            builder.addHashes(hashes[0], hashes[1]);
        }
    }
    return builder.finish();
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

fn buildBlockHashSlotsAlloc(
    allocator: std.mem.Allocator,
    entries: []const Entry,
    first_entry_index: usize,
) ![]u32 {
    if (entries.len == 0) return try allocator.alloc(u32, 0);
    const min_slots = @max(2, entries.len * 2);
    const slot_count = std.math.ceilPowerOfTwo(usize, min_slots) catch min_slots;
    const slots = try allocator.alloc(u32, slot_count);
    @memset(slots, 0);
    for (entries, 0..) |entry, local_index| {
        const hashes = entryHashes(entry.namespace_name, entry.key);
        var slot_index: usize = @intCast(hashes[0] % slots.len);
        while (slots[slot_index] != 0) {
            slot_index +%= 1;
            if (slot_index >= slots.len) slot_index = 0;
        }
        slots[slot_index] = @intCast(first_entry_index + local_index + 1);
    }
    return slots;
}

fn buildBlockHashSlotsFromHashesAlloc(
    allocator: std.mem.Allocator,
    hashes: []const [2]u64,
    first_entry_index: usize,
) ![]u32 {
    if (hashes.len == 0) return try allocator.alloc(u32, 0);
    const min_slots = @max(2, hashes.len * 2);
    const slot_count = std.math.ceilPowerOfTwo(usize, min_slots) catch min_slots;
    const slots = try allocator.alloc(u32, slot_count);
    @memset(slots, 0);
    for (hashes, 0..) |entry_hashes, local_index| {
        var slot_index: usize = @intCast(entry_hashes[0] % slots.len);
        while (slots[slot_index] != 0) {
            slot_index +%= 1;
            if (slot_index >= slots.len) slot_index = 0;
        }
        slots[slot_index] = @intCast(first_entry_index + local_index + 1);
    }
    return slots;
}

pub fn decodeAlloc(allocator: std.mem.Allocator, raw: []const u8) !Decoded {
    const owned_raw = try allocator.dupe(u8, raw);
    errdefer allocator.free(owned_raw);
    var borrowed = try decodeBorrowedOwnedAlloc(allocator, owned_raw);
    defer borrowed.deinit(allocator);

    const entries = try allocator.alloc(OwnedEntry, borrowed.entryCount());
    errdefer allocator.free(entries);

    var initialized: usize = 0;
    errdefer for (entries[0..initialized]) |*entry| entry.deinit(allocator);

    while (initialized < entries.len) : (initialized += 1) {
        const entry = try borrowed.entryAt(initialized);
        entries[initialized] = .{
            .namespace_name = if (entry.namespace_name) |name| try allocator.dupe(u8, name) else null,
            .key = try allocator.dupe(u8, entry.key),
            .value = try allocator.dupe(u8, entry.value),
            .tombstone = entry.tombstone,
        };
    }

    return .{
        .entries = entries,
        .filter = try borrowed.filter.clone(allocator),
    };
}

pub fn decodeBorrowedOwnedAlloc(allocator: std.mem.Allocator, raw: []u8) !BorrowedDecoded {
    var index = try decodeIndexAlloc(allocator, raw);
    errdefer index.deinit(allocator);
    if (index.prefix_filter) |*filter| {
        filter.deinit(allocator);
        index.prefix_filter = null;
    }

    if (indexHasCompressedBlocks(&index)) {
        const logical_raw = try materializeLogicalEntryDataRawAlloc(allocator, raw, &index);
        allocator.free(raw);
        return .{
            .raw = logical_raw,
            .owns_raw = true,
            .entry_offsets = index.entry_offsets,
            .owns_entry_offsets = true,
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

    fn deinit(self: *EncodedBlockPayload, allocator: std.mem.Allocator) void {
        if (self.owned_prefix) |bytes| allocator.free(bytes);
        self.* = undefined;
    }
};

fn encodeBlockPayloadAlloc(
    allocator: std.mem.Allocator,
    block_bytes: []const u8,
    compression_policy: CompressionPolicy,
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

    if (compression_policy == .snappy_adaptive and payload.payload.len >= min_compress_block_bytes) {
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

fn encodePrefixCompressedBlockAlloc(allocator: std.mem.Allocator, block_bytes: []const u8) ![]u8 {
    var encoded_entries = std.ArrayListUnmanaged(u8).empty;
    defer encoded_entries.deinit(allocator);
    var restart_offsets = std.ArrayListUnmanaged(u32).empty;
    defer restart_offsets.deinit(allocator);
    var previous_key = std.ArrayListUnmanaged(u8).empty;
    defer previous_key.deinit(allocator);

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
            shared_key_len = commonPrefixLen(previous_key.items, entry.key);
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

        previous_key.clearRetainingCapacity();
        try previous_key.appendSlice(allocator, entry.key);
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

fn decodePrefixCompressedBlockAlloc(
    allocator: std.mem.Allocator,
    payload: []const u8,
    expected_len: usize,
) ![]u8 {
    const view = try parsePrefixBlockPayload(payload);

    var previous_key = std.ArrayListUnmanaged(u8).empty;
    defer previous_key.deinit(allocator);
    var current_key = std.ArrayListUnmanaged(u8).empty;
    defer current_key.deinit(allocator);
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);
    try out.ensureTotalCapacity(allocator, expected_len);

    var entries_cursor: usize = 0;
    for (0..view.entry_count) |entry_index| {
        if (entry_index % view.restart_interval == 0) {
            const expected_restart = try view.restartOffset(entry_index / view.restart_interval);
            if (expected_restart != entries_cursor) return error.InvalidTableFile;
        }
        const entry = try readPrefixBlockEntry(allocator, view.encoded_entries, &entries_cursor, previous_key.items, &current_key);
        try appendEntryBytesToList(allocator, &out, .{
            .namespace_name = entry.namespace_name,
            .key = entry.key,
            .value = entry.value,
            .tombstone = entry.tombstone,
        });

        previous_key.clearRetainingCapacity();
        try previous_key.ensureTotalCapacity(allocator, entry.key.len);
        previous_key.appendSliceAssumeCapacity(entry.key);
    }
    if (entries_cursor != view.encoded_entries.len) return error.InvalidTableFile;
    if (out.items.len != expected_len) return error.InvalidTableFile;
    return try out.toOwnedSlice(allocator);
}

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
    const restart_bytes = try readSlice(payload, &cursor, restart_count * @sizeOf(u32));
    if (cursor != payload.len) return error.InvalidTableFile;
    if (entry_count == 0 and restart_count != 0) return error.InvalidTableFile;
    if (entry_count > 0 and restart_count != ((entry_count + restart_interval - 1) / restart_interval)) return error.InvalidTableFile;
    return .{
        .entry_count = entry_count,
        .restart_interval = restart_interval,
        .restart_count = restart_count,
        .encoded_entries = encoded_entries,
        .restart_bytes = restart_bytes,
    };
}

fn readPrefixBlockEntry(
    allocator: std.mem.Allocator,
    encoded_entries: []const u8,
    cursor: *usize,
    previous_key: []const u8,
    current_key: *std.ArrayListUnmanaged(u8),
) !Entry {
    const tombstone = switch (try readByte(encoded_entries, cursor)) {
        0 => false,
        1 => true,
        else => return error.InvalidTableFile,
    };
    const namespace_len: usize = @intCast(try readU32(encoded_entries, cursor));
    const shared_key_len: usize = @intCast(try readU32(encoded_entries, cursor));
    const unshared_key_len: usize = @intCast(try readU32(encoded_entries, cursor));
    const value_len: usize = @intCast(try readU32(encoded_entries, cursor));
    if (shared_key_len > previous_key.len) return error.InvalidTableFile;
    const namespace_name = try readSlice(encoded_entries, cursor, namespace_len);
    const unshared_key = try readSlice(encoded_entries, cursor, unshared_key_len);
    const value = try readSlice(encoded_entries, cursor, value_len);

    current_key.clearRetainingCapacity();
    try current_key.ensureTotalCapacity(allocator, shared_key_len + unshared_key.len);
    current_key.appendSliceAssumeCapacity(previous_key[0..shared_key_len]);
    current_key.appendSliceAssumeCapacity(unshared_key);
    return .{
        .namespace_name = if (namespace_len > 0) namespace_name else null,
        .key = current_key.items,
        .value = value,
        .tombstone = tombstone,
    };
}

fn prefixRestartEntry(
    allocator: std.mem.Allocator,
    view: PrefixBlockView,
    restart_index: usize,
    scratch: *std.ArrayListUnmanaged(u8),
) !Entry {
    var cursor = try view.restartOffset(restart_index);
    return try readPrefixBlockEntry(allocator, view.encoded_entries, &cursor, &.{}, scratch);
}

fn findExactEntryInPrefixPayloadAlloc(
    allocator: std.mem.Allocator,
    payload: []const u8,
    first_entry_index: usize,
    namespace_name: ?[]const u8,
    key: []const u8,
) !?OwnedPositionedEntry {
    const view = try parsePrefixBlockPayload(payload);
    if (view.entry_count == 0) return null;

    var restart_key = std.ArrayListUnmanaged(u8).empty;
    defer restart_key.deinit(allocator);
    var lo: usize = 0;
    var hi: usize = view.restart_count;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        const entry = try prefixRestartEntry(allocator, view, mid, &restart_key);
        if (compareEntryTo(entry, namespace_name, key) != .gt) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }

    const restart_index = if (lo == 0) 0 else lo - 1;
    var entries_cursor = try view.restartOffset(restart_index);
    const end_cursor = if (restart_index + 1 < view.restart_count)
        try view.restartOffset(restart_index + 1)
    else
        view.encoded_entries.len;

    var previous_key = std.ArrayListUnmanaged(u8).empty;
    defer previous_key.deinit(allocator);
    var current_key = std.ArrayListUnmanaged(u8).empty;
    defer current_key.deinit(allocator);
    var entry_index = first_entry_index + restart_index * view.restart_interval;
    while (entries_cursor < end_cursor and entry_index < first_entry_index + view.entry_count) : (entry_index += 1) {
        const entry = try readPrefixBlockEntry(allocator, view.encoded_entries, &entries_cursor, previous_key.items, &current_key);
        const order = compareEntryTo(entry, namespace_name, key);
        if (order == .eq) {
            var out = std.ArrayListUnmanaged(u8).empty;
            errdefer out.deinit(allocator);
            try appendEntryBytesToList(allocator, &out, entry);
            const bytes = try out.toOwnedSlice(allocator);
            errdefer allocator.free(bytes);
            return .{
                .index = entry_index,
                .entry = try parseEntryAt(bytes, 0),
                .bytes = bytes,
            };
        }
        if (order == .gt) return null;

        previous_key.clearRetainingCapacity();
        try previous_key.ensureTotalCapacity(allocator, entry.key.len);
        previous_key.appendSliceAssumeCapacity(entry.key);
    }
    if (entries_cursor != end_cursor) return error.InvalidTableFile;
    return null;
}

pub fn findExactEntryInCompressedBlockPayloadAlloc(
    allocator: std.mem.Allocator,
    compression: BlockCompression,
    payload: []const u8,
    first_entry_index: usize,
    namespace_name: ?[]const u8,
    key: []const u8,
) !?OwnedPositionedEntry {
    return switch (compression) {
        .prefix => try findExactEntryInPrefixPayloadAlloc(allocator, payload, first_entry_index, namespace_name, key),
        .prefix_snappy => blk: {
            const prefix_payload = try snappy.decode(allocator, payload);
            defer allocator.free(prefix_payload);
            break :blk try findExactEntryInPrefixPayloadAlloc(allocator, prefix_payload, first_entry_index, namespace_name, key);
        },
        .none, .snappy => null,
    };
}

fn commonPrefixLen(lhs: []const u8, rhs: []const u8) usize {
    const limit = @min(lhs.len, rhs.len);
    var index: usize = 0;
    while (index < limit and lhs[index] == rhs[index]) : (index += 1) {}
    return index;
}

pub fn decodeBlockPayloadAlloc(
    allocator: std.mem.Allocator,
    compression: BlockCompression,
    payload: []const u8,
    expected_len: usize,
) ![]u8 {
    return switch (compression) {
        .none => blk: {
            if (payload.len != expected_len) return error.InvalidTableFile;
            break :blk try allocator.dupe(u8, payload);
        },
        .snappy => blk: {
            const decoded = try snappy.decode(allocator, payload);
            errdefer allocator.free(decoded);
            if (decoded.len != expected_len) return error.InvalidTableFile;
            break :blk decoded;
        },
        .prefix => try decodePrefixCompressedBlockAlloc(allocator, payload, expected_len),
        .prefix_snappy => blk: {
            const prefix_payload = try snappy.decode(allocator, payload);
            defer allocator.free(prefix_payload);
            break :blk try decodePrefixCompressedBlockAlloc(allocator, prefix_payload, expected_len);
        },
    };
}

pub fn decodeWindowFromRawAlloc(
    allocator: std.mem.Allocator,
    raw: []const u8,
    index: *const TableIndex,
    window: EntryDataWindow,
) ![]u8 {
    const physical_start = index.entry_data_start + @as(usize, window.physicalRelativeOffset());
    const physical_len: usize = @intCast(window.physicalLen());
    if (physical_start > raw.len or physical_len > raw.len - physical_start) return error.InvalidTableFile;
    return try decodeBlockPayloadAlloc(
        allocator,
        window.compression,
        raw[physical_start..][0..physical_len],
        window.len,
    );
}

fn materializeLogicalEntryDataRawAlloc(
    allocator: std.mem.Allocator,
    raw: []const u8,
    index: *const TableIndex,
) ![]u8 {
    const logical_len = index.entry_data_start + index.entry_data_len;
    const logical_raw = try allocator.alloc(u8, logical_len);
    errdefer allocator.free(logical_raw);
    if (index.entry_data_start > raw.len) return error.InvalidTableFile;
    @memcpy(logical_raw[0..index.entry_data_start], raw[0..index.entry_data_start]);
    for (index.blocks, 0..) |_, block_index| {
        const window = index.blockWindow(block_index);
        const decoded = try decodeWindowFromRawAlloc(allocator, raw, index, window);
        defer allocator.free(decoded);
        const logical_start = index.entry_data_start + @as(usize, window.relative_offset);
        if (logical_start > logical_raw.len or decoded.len > logical_raw.len - logical_start) return error.InvalidTableFile;
        @memcpy(logical_raw[logical_start..][0..decoded.len], decoded);
    }
    return logical_raw;
}

pub fn decodeIndexAlloc(allocator: std.mem.Allocator, raw: []const u8) !TableIndex {
    var cursor: usize = 0;
    const header = try decodeHeader(raw, &cursor);
    return switch (header.version) {
        version => decodeV9IndexAlloc(allocator, raw, header),
        else => error.UnsupportedVersion,
    };
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

    var cursor: usize = footer_magic.len;
    const metadata_offset: usize = @intCast(try readU64(raw, &cursor));
    const metadata_len: usize = @intCast(try readU64(raw, &cursor));
    const entry_count: usize = @intCast(try readU32(raw, &cursor));
    const entry_data_len: usize = @intCast(try readU32(raw, &cursor));
    if (cursor != raw.len) return error.InvalidTableFile;
    if (metadata_len < entry_count * @sizeOf(u32) + @sizeOf(u32)) return error.InvalidTableFile;

    return .{
        .metadata_offset = metadata_offset,
        .metadata_len = metadata_len,
        .entry_count = entry_count,
        .entry_data_len = entry_data_len,
    };
}

pub fn decodeIndexFromFooterAlloc(
    allocator: std.mem.Allocator,
    footer: Footer,
    metadata: []const u8,
) !TableIndex {
    if (metadata.len != footer.metadata_len) return error.InvalidTableFile;
    return try decodeFooterMetadataAlloc(allocator, metadata, header_len, footer.entry_count, footer.entry_data_len);
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

pub fn lowerBoundPositionInBlock(
    index: *const TableIndex,
    raw_block: []const u8,
    block_index: usize,
    namespace_name: ?[]const u8,
    key: []const u8,
    inclusive: bool,
) !?BorrowedDecoded.PositionedEntry {
    const block = index.blocks[block_index];
    var idx = try lowerBoundInBlockRaw(raw_block, index.entry_offsets, block, namespace_name, key);
    while (idx < block.first_entry_index + block.entry_count) : (idx += 1) {
        const relative_offset: usize = @intCast(index.entryStart(idx) - block.relative_offset);
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

fn readByte(raw: []const u8, cursor: *usize) !u8 {
    if (cursor.* >= raw.len) return error.InvalidTableFile;
    const out = raw[cursor.*];
    cursor.* += 1;
    return out;
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

fn decodeV9IndexAlloc(
    allocator: std.mem.Allocator,
    raw: []const u8,
    header: Header,
) !TableIndex {
    const footer = try decodeFooter(raw);
    if (footer.entry_count != header.entry_count) return error.InvalidTableFile;
    if (footer.metadata_offset != header.entry_data_start + header.entry_data_len) return error.InvalidTableFile;
    const footer_offset = raw.len - footer_len;
    if (footer.metadata_offset + footer.metadata_len != footer_offset) return error.InvalidTableFile;
    const metadata = raw[footer.metadata_offset..footer_offset];
    return try decodeFooterMetadataAlloc(allocator, metadata, header.entry_data_start, footer.entry_count, footer.entry_data_len);
}

fn decodeFooterMetadataAlloc(
    allocator: std.mem.Allocator,
    metadata: []const u8,
    entry_data_start: usize,
    entry_count: usize,
    entry_data_len: usize,
) !TableIndex {
    var cursor: usize = 0;
    const offsets = try allocator.alloc(u32, entry_count);
    errdefer allocator.free(offsets);
    for (offsets) |*offset| offset.* = try readU32(metadata, &cursor);

    const bloom_len: usize = @intCast(try readU32(metadata, &cursor));
    const encoded_filter = try readSlice(metadata, &cursor, bloom_len);
    var filter = try bloom.OwnedFilter.decodeAlloc(allocator, encoded_filter);
    errdefer filter.deinit(allocator);
    const blocks = if (cursor == metadata.len)
        try allocator.alloc(TableIndex.BlockMeta, 0)
    else
        try decodeBlockMetasAlloc(allocator, metadata, &cursor, entry_count, entry_data_len);
    errdefer {
        for (blocks) |*block| block.deinit(allocator);
        allocator.free(blocks);
    }
    if (cursor < metadata.len) {
        try decodeBlockFiltersAlloc(allocator, metadata, &cursor, blocks);
    }
    if (cursor < metadata.len) {
        try decodeBlockHashSlotsAlloc(allocator, metadata, &cursor, blocks);
    }
    if (cursor < metadata.len) {
        try decodeBlockPhysicalMetas(metadata, &cursor, blocks);
    }
    if (cursor < metadata.len) {
        try decodeBlockSmallestKeysAlloc(allocator, metadata, &cursor, blocks);
    }
    var prefix_extractor: PrefixExtractor = .none;
    var prefix_filter: ?bloom.OwnedFilter = null;
    errdefer if (prefix_filter) |*filter_ptr| filter_ptr.deinit(allocator);
    if (cursor < metadata.len) {
        prefix_extractor = try decodePrefixExtractor(try readU32(metadata, &cursor));
        const prefix_filter_len: usize = @intCast(try readU32(metadata, &cursor));
        if (prefix_filter_len > 0) {
            prefix_filter = try bloom.OwnedFilter.decodeAlloc(allocator, try readSlice(metadata, &cursor, prefix_filter_len));
        }
        try decodeBlockPrefixFiltersAlloc(allocator, metadata, &cursor, blocks);
    }
    if (cursor != metadata.len) return error.InvalidTableFile;

    return .{
        .entry_offsets = offsets,
        .entry_data_start = entry_data_start,
        .entry_data_len = entry_data_len,
        .filter = filter,
        .prefix_extractor = prefix_extractor,
        .prefix_filter = prefix_filter,
        .blocks = blocks,
    };
}

fn decodeBlockMetasAlloc(
    allocator: std.mem.Allocator,
    metadata: []const u8,
    cursor: *usize,
    entry_count: usize,
    entry_data_len: usize,
) ![]TableIndex.BlockMeta {
    const block_count: usize = @intCast(try readU32(metadata, cursor));
    const blocks = try allocator.alloc(TableIndex.BlockMeta, block_count);
    errdefer allocator.free(blocks);

    var initialized: usize = 0;
    errdefer {
        for (blocks[0..initialized]) |*block| block.deinit(allocator);
    }

    var expected_entry_index: usize = 0;
    var expected_offset: usize = 0;
    for (blocks, 0..) |*block, block_index| {
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
) !void {
    const filter_count: usize = @intCast(try readU32(metadata, cursor));
    if (filter_count != blocks.len) return error.InvalidTableFile;
    for (blocks) |*block| {
        const filter_len: usize = @intCast(try readU32(metadata, cursor));
        const encoded_filter = try readSlice(metadata, cursor, filter_len);
        block.filter = try bloom.OwnedFilter.decodeAlloc(allocator, encoded_filter);
    }
}

fn decodeBlockHashSlotsAlloc(
    allocator: std.mem.Allocator,
    metadata: []const u8,
    cursor: *usize,
    blocks: []TableIndex.BlockMeta,
) !void {
    const hash_count: usize = @intCast(try readU32(metadata, cursor));
    if (hash_count != blocks.len) return error.InvalidTableFile;
    for (blocks) |*block| {
        const slot_count: usize = @intCast(try readU32(metadata, cursor));
        const slots = try allocator.alloc(u32, slot_count);
        errdefer allocator.free(slots);
        for (slots) |*slot| {
            slot.* = try readU32(metadata, cursor);
            if (slot.* != 0) {
                const entry_index = slot.* - 1;
                if (entry_index < block.first_entry_index or entry_index > block.lastEntryIndex()) return error.InvalidTableFile;
            }
        }
        block.hash_slots = slots;
    }
}

fn decodeBlockPhysicalMetas(
    metadata: []const u8,
    cursor: *usize,
    blocks: []TableIndex.BlockMeta,
) !void {
    const physical_count: usize = @intCast(try readU32(metadata, cursor));
    if (physical_count != blocks.len) return error.InvalidTableFile;
    var expected_physical_offset: usize = 0;
    for (blocks) |*block| {
        const physical_relative_offset = try readU32(metadata, cursor);
        const physical_len = try readU32(metadata, cursor);
        const compression_raw = try readU32(metadata, cursor);
        const compression: BlockCompression = switch (compression_raw) {
            @intFromEnum(BlockCompression.none) => .none,
            @intFromEnum(BlockCompression.snappy) => .snappy,
            @intFromEnum(BlockCompression.prefix) => .prefix,
            @intFromEnum(BlockCompression.prefix_snappy) => .prefix_snappy,
            else => return error.InvalidTableFile,
        };
        if (physical_len == 0) return error.InvalidTableFile;
        if (@as(usize, physical_relative_offset) != expected_physical_offset) return error.InvalidTableFile;
        block.physical_relative_offset = physical_relative_offset;
        block.physical_len = physical_len;
        block.compression = compression;
        expected_physical_offset += physical_len;
    }
}

fn decodeBlockSmallestKeysAlloc(
    allocator: std.mem.Allocator,
    metadata: []const u8,
    cursor: *usize,
    blocks: []TableIndex.BlockMeta,
) !void {
    const smallest_count: usize = @intCast(try readU32(metadata, cursor));
    if (smallest_count != blocks.len) return error.InvalidTableFile;
    for (blocks) |*block| {
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
        @intFromEnum(PrefixExtractor.none) => .none,
        @intFromEnum(PrefixExtractor.first_separator) => .first_separator,
        else => error.InvalidTableFile,
    };
}

fn decodeBlockPrefixFiltersAlloc(
    allocator: std.mem.Allocator,
    metadata: []const u8,
    cursor: *usize,
    blocks: []TableIndex.BlockMeta,
) !void {
    const filter_count: usize = @intCast(try readU32(metadata, cursor));
    if (filter_count != blocks.len) return error.InvalidTableFile;
    for (blocks) |*block| {
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

test "table file v9 footer metadata decodes through footer" {
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

    try std.testing.expectEqual(@as(usize, entries.len), index.entry_offsets.len);
    try std.testing.expect(maybeContains(index.borrowFilter(), "docs", "doc:a"));
    try std.testing.expect(maybeContains(index.borrowFilter(), "docs", "doc:b"));
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
    try std.testing.expect(index.blocks[target_block].hash_slots.len > 0);
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
