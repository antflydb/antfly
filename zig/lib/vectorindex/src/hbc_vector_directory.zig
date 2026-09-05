// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//! Block-oriented immutable vector-to-leaf and vector-metadata directory.
//!
//! V2 groups the union of vector IDs into independently checksummed row
//! blocks. Each ID is encoded once; presence bitmaps select the required
//! fixed-width leaf plane and optional metadata plane. Delta-coded IDs carry
//! restart points so point lookup has bounded decode work. A compact root is
//! validated at open while block indexes and values are validated on demand.

const std = @import("std");
const Allocator = std.mem.Allocator;
const checked_region = @import("checked_region.zig");

const magic: [4]u8 = "AFVD".*;
const version: u16 = 2;
const header_size: usize = 8;
const block_entry_limit: usize = 256;
const block_data_target: usize = 64 * 1024;
const id_restart_interval: usize = 16;
const descriptor_size: usize = 68;
const footer_size: usize = 40;

pub const Kind = enum(u8) { leaf = 1, metadata = 2 };
const IdEncoding = enum(u8) { raw = 0, restart_varint = 1 };
const MetadataEndEncoding = enum(u8) { u16 = 0, u32 = 1 };

const Descriptor = struct {
    encoding: IdEncoding,
    metadata_end_encoding: MetadataEndEncoding,
    count: usize,
    first_id: u64,
    last_id: u64,
    data_offset: usize,
    data_len: usize,
    index_offset: usize,
    index_len: usize,
    data_checksum: u32,
    index_checksum: u32,
    metadata_count: usize,
    metadata_data_checksum: u32,
};

const BufferSink = struct {
    alloc: Allocator,
    out: std.ArrayListUnmanaged(u8) = .empty,

    fn len(self: *const BufferSink) usize {
        return self.out.items.len;
    }
    fn appendSlice(self: *BufferSink, bytes: []const u8) !void {
        try self.out.appendSlice(self.alloc, bytes);
    }
};

pub const Writer = struct {
    alloc: Allocator,
    sink: BufferSink,
    streaming: StreamingWriter,

    pub fn init(alloc: Allocator) !Writer {
        var sink: BufferSink = .{ .alloc = alloc };
        errdefer sink.out.deinit(alloc);
        const streaming = try StreamingWriter.init(alloc, &sink);
        return .{ .alloc = alloc, .sink = sink, .streaming = streaming };
    }
    pub fn deinit(self: *Writer) void {
        self.streaming.deinit();
        self.sink.out.deinit(self.alloc);
        self.* = undefined;
    }
    pub fn appendRow(self: *Writer, id: u64, leaf: ?[]const u8, metadata: ?[]const u8) !void {
        try self.streaming.appendRow(&self.sink, id, leaf, metadata);
    }
    pub fn build(self: *Writer) ![]u8 {
        const finish = try self.streaming.finish(&self.sink);
        if (finish.offset != 0 or finish.len != self.sink.out.items.len) return error.CorruptedVectorDirectory;
        return try self.sink.out.toOwnedSlice(self.alloc);
    }
};

/// File-backed writer retaining only one block plus compact root descriptors.
pub const StreamingWriter = struct {
    alloc: Allocator,
    base_offset: usize,
    ids: std.ArrayListUnmanaged(u64) = .empty,
    leaf_data_scratch: std.ArrayListUnmanaged(u8) = .empty,
    metadata_data_scratch: std.ArrayListUnmanaged(u8) = .empty,
    metadata_ends: std.ArrayListUnmanaged(u32) = .empty,
    index_scratch: std.ArrayListUnmanaged(u8) = .empty,
    descriptors: std.ArrayListUnmanaged(Descriptor) = .empty,
    active_data_offset: usize = 0,
    active_data_len: usize = 0,
    leaf_presence: [block_entry_limit / 8]u8 = @splat(0),
    metadata_presence: [block_entry_limit / 8]u8 = @splat(0),
    entry_count: u64 = 0,
    leaf_entry_count: u64 = 0,
    metadata_entry_count: u64 = 0,
    previous_id: ?u64 = null,
    finished: bool = false,

    pub const Finish = struct {
        offset: usize,
        len: usize,
        entry_count: u64,
        leaf_entry_count: u64,
        metadata_entry_count: u64,
        block_count: usize,
        value_bytes: u64,
        index_bytes: u64,
        root_bytes: usize,
        /// The nested format checksums its root and lazily validates
        /// blocks, so the outer segment need not reread the staged range.
        outer_checksum: u32 = 0,
    };

    pub fn init(alloc: Allocator, sink: anytype) !StreamingWriter {
        const base_offset = sink.len();
        var header: [header_size]u8 = @splat(0);
        @memcpy(header[0..4], &magic);
        writeU16(&header, 4, version);
        try sink.appendSlice(&header);
        var self: StreamingWriter = .{ .alloc = alloc, .base_offset = base_offset };
        errdefer self.deinit();
        try self.ids.ensureTotalCapacity(alloc, block_entry_limit);
        try self.metadata_ends.ensureTotalCapacity(alloc, block_entry_limit);
        return self;
    }
    pub fn deinit(self: *StreamingWriter) void {
        self.ids.deinit(self.alloc);
        self.leaf_data_scratch.deinit(self.alloc);
        self.metadata_data_scratch.deinit(self.alloc);
        self.metadata_ends.deinit(self.alloc);
        self.index_scratch.deinit(self.alloc);
        self.descriptors.deinit(self.alloc);
        self.* = undefined;
    }
    pub fn reserveEntries(self: *StreamingWriter, count: usize) !void {
        if (self.finished) return error.VectorDirectoryWriterFinished;
        const blocks = std.math.divCeil(usize, count, block_entry_limit) catch return error.VectorDirectoryTooLarge;
        try self.descriptors.ensureTotalCapacity(self.alloc, blocks);
    }
    pub fn appendRow(self: *StreamingWriter, sink: anytype, id: u64, leaf: ?[]const u8, metadata: ?[]const u8) !void {
        if (self.finished) return error.VectorDirectoryWriterFinished;
        if (leaf == null and metadata == null) return error.EmptyVectorDirectoryRow;
        if (self.previous_id) |previous| if (id <= previous) return error.OutOfOrderVectorDirectoryEntry;
        if (leaf) |value| if (value.len != @sizeOf(u64)) return error.InvalidVectorLeafValue;
        const metadata_len = if (metadata) |value| value.len else 0;
        _ = std.math.cast(u32, metadata_len) orelse return error.VectorDirectoryTooLarge;
        const value_len = std.math.add(usize, if (leaf != null) @sizeOf(u64) else 0, metadata_len) catch
            return error.VectorDirectoryTooLarge;
        if (self.ids.items.len == block_entry_limit or
            (self.ids.items.len != 0 and
                (self.active_data_len >= block_data_target or value_len > block_data_target - self.active_data_len)))
        {
            try self.flushBlock(sink);
        }
        if (self.ids.items.len == 0) {
            self.active_data_offset = sink.len() - self.base_offset;
            self.active_data_len = 0;
        }
        const row = self.ids.items.len;
        try self.ids.append(self.alloc, id);
        if (leaf) |value| {
            setPresence(&self.leaf_presence, row);
            try self.leaf_data_scratch.appendSlice(self.alloc, value);
            self.leaf_entry_count = std.math.add(u64, self.leaf_entry_count, 1) catch return error.VectorDirectoryTooLarge;
        }
        if (metadata) |value| {
            setPresence(&self.metadata_presence, row);
            try self.metadata_data_scratch.appendSlice(self.alloc, value);
            self.metadata_ends.appendAssumeCapacity(std.math.cast(u32, self.metadata_data_scratch.items.len) orelse
                return error.VectorDirectoryTooLarge);
            self.metadata_entry_count = std.math.add(u64, self.metadata_entry_count, 1) catch return error.VectorDirectoryTooLarge;
        }
        self.active_data_len = std.math.add(usize, self.active_data_len, value_len) catch return error.VectorDirectoryTooLarge;
        self.entry_count = std.math.add(u64, self.entry_count, 1) catch return error.VectorDirectoryTooLarge;
        self.previous_id = id;
    }
    pub fn finish(self: *StreamingWriter, sink: anytype) !Finish {
        if (self.finished) return error.VectorDirectoryWriterFinished;
        self.finished = true;
        try self.flushBlock(sink);
        const root_offset = sink.len() - self.base_offset;
        self.index_scratch.clearRetainingCapacity();
        for (self.descriptors.items) |descriptor_value| {
            const encoded = encodeDescriptor(descriptor_value);
            try self.index_scratch.appendSlice(self.alloc, &encoded);
        }
        try sink.appendSlice(self.index_scratch.items);
        var footer: [footer_size]u8 = @splat(0);
        writeU64(&footer, 0, @intCast(root_offset));
        writeU64(&footer, 8, @intCast(self.descriptors.items.len));
        writeU64(&footer, 16, self.entry_count);
        writeU32(&footer, 24, std.hash.Crc32.hash(self.index_scratch.items));
        writeU16(&footer, 28, version);
        writeU32(&footer, 32, std.hash.Crc32.hash(footer[0..32]));
        @memcpy(footer[36..40], &magic);
        try sink.appendSlice(&footer);
        var value_bytes: u64 = 0;
        var index_bytes: u64 = 0;
        for (self.descriptors.items) |descriptor_value| {
            value_bytes = std.math.add(u64, value_bytes, descriptor_value.data_len) catch
                return error.VectorDirectoryTooLarge;
            index_bytes = std.math.add(u64, index_bytes, descriptor_value.index_len) catch
                return error.VectorDirectoryTooLarge;
        }
        return .{
            .offset = self.base_offset,
            .len = sink.len() - self.base_offset,
            .entry_count = self.entry_count,
            .leaf_entry_count = self.leaf_entry_count,
            .metadata_entry_count = self.metadata_entry_count,
            .block_count = self.descriptors.items.len,
            .value_bytes = value_bytes,
            .index_bytes = index_bytes,
            .root_bytes = self.descriptors.items.len * descriptor_size,
        };
    }
    fn flushBlock(self: *StreamingWriter, sink: anytype) !void {
        if (self.ids.items.len == 0) return;
        if (self.leaf_data_scratch.items.len + self.metadata_data_scratch.items.len != self.active_data_len)
            return error.CorruptedVectorDirectory;

        // One block write preserves leaf and metadata planes contiguously while
        // retaining only bounded scratch. The production sink is pwrite-like,
        // so this is deliberately not one append per row.
        self.index_scratch.clearRetainingCapacity();
        try self.index_scratch.appendSlice(self.alloc, self.leaf_data_scratch.items);
        try self.index_scratch.appendSlice(self.alloc, self.metadata_data_scratch.items);
        try sink.appendSlice(self.index_scratch.items);
        const leaf_data_checksum = std.hash.Crc32.hash(self.leaf_data_scratch.items);
        const metadata_data_checksum = std.hash.Crc32.hash(self.metadata_data_scratch.items);

        self.index_scratch.clearRetainingCapacity();
        const raw_len = std.math.mul(usize, self.ids.items.len, @sizeOf(u64)) catch return error.VectorDirectoryTooLarge;
        const restart_count = std.math.divCeil(usize, self.ids.items.len, id_restart_interval) catch
            return error.VectorDirectoryTooLarge;
        var restart_len = std.math.mul(usize, restart_count, 12) catch return error.VectorDirectoryTooLarge;
        for (self.ids.items, 0..) |current, row| {
            if (row % id_restart_interval == 0) continue;
            restart_len = std.math.add(usize, restart_len, varintLength(current - self.ids.items[row - 1])) catch
                return error.VectorDirectoryTooLarge;
        }
        const encoding: IdEncoding = if (restart_len < raw_len) .restart_varint else .raw;
        switch (encoding) {
            .raw => for (self.ids.items) |id| try appendU64(self.alloc, &self.index_scratch, id),
            .restart_varint => {
                var delta_offset: usize = 0;
                var restart_row: usize = 0;
                while (restart_row < self.ids.items.len) : (restart_row += id_restart_interval) {
                    try appendU64(self.alloc, &self.index_scratch, self.ids.items[restart_row]);
                    try appendU32(self.alloc, &self.index_scratch, @intCast(delta_offset));
                    const group_end = @min(restart_row + id_restart_interval, self.ids.items.len);
                    for (self.ids.items[restart_row + 1 .. group_end], self.ids.items[restart_row .. group_end - 1]) |current, previous| {
                        delta_offset = std.math.add(usize, delta_offset, varintLength(current - previous)) catch
                            return error.VectorDirectoryTooLarge;
                    }
                }
                for (self.ids.items, 0..) |current, row| {
                    if (row % id_restart_interval != 0)
                        try appendVarint(self.alloc, &self.index_scratch, current - self.ids.items[row - 1]);
                }
            },
        }
        const bitmap_len = presenceBytes(self.ids.items.len);
        try self.index_scratch.appendSlice(self.alloc, self.leaf_presence[0..bitmap_len]);
        try self.index_scratch.appendSlice(self.alloc, self.metadata_presence[0..bitmap_len]);
        const metadata_end_encoding: MetadataEndEncoding = if (self.metadata_data_scratch.items.len <= std.math.maxInt(u16))
            .u16
        else
            .u32;
        for (self.metadata_ends.items) |end| switch (metadata_end_encoding) {
            .u16 => try appendU16(self.alloc, &self.index_scratch, @intCast(end)),
            .u32 => try appendU32(self.alloc, &self.index_scratch, end),
        };
        const index_offset = sink.len() - self.base_offset;
        try sink.appendSlice(self.index_scratch.items);
        try self.descriptors.append(self.alloc, .{
            .encoding = encoding,
            .metadata_end_encoding = metadata_end_encoding,
            .count = self.ids.items.len,
            .first_id = self.ids.items[0],
            .last_id = self.ids.items[self.ids.items.len - 1],
            .data_offset = self.active_data_offset,
            .data_len = self.active_data_len,
            .index_offset = index_offset,
            .index_len = self.index_scratch.items.len,
            .data_checksum = leaf_data_checksum,
            .index_checksum = std.hash.Crc32.hash(self.index_scratch.items),
            .metadata_count = self.metadata_ends.items.len,
            .metadata_data_checksum = metadata_data_checksum,
        });
        self.ids.clearRetainingCapacity();
        self.leaf_data_scratch.clearRetainingCapacity();
        self.metadata_data_scratch.clearRetainingCapacity();
        self.metadata_ends.clearRetainingCapacity();
        @memset(&self.leaf_presence, 0);
        @memset(&self.metadata_presence, 0);
        self.active_data_len = 0;
    }
};

pub const Reader = struct {
    alloc: Allocator,
    data: []const u8,
    root_offset: usize,
    block_count: usize,
    entry_count: usize,
    verified_indexes: []std.atomic.Value(u64),
    verified_leaf_data: []std.atomic.Value(u64),
    verified_metadata_data: []std.atomic.Value(u64),

    pub fn init(alloc: Allocator, data: []const u8) !Reader {
        if (data.len < header_size + footer_size or !std.mem.eql(u8, data[0..4], &magic)) return error.CorruptedVectorDirectory;
        if (readU16(data, 4) != version or readU16(data, 6) != 0) return error.UnsupportedVectorDirectoryVersion;
        const footer = data[data.len - footer_size ..];
        if (!std.mem.eql(u8, footer[36..40], &magic)) return error.CorruptedVectorDirectory;
        if (readU16(footer, 28) != version or readU16(footer, 30) != 0) return error.UnsupportedVectorDirectoryVersion;
        if (readU32(footer, 32) != std.hash.Crc32.hash(footer[0..32])) return error.VectorDirectoryChecksumMismatch;
        const root = checked_region.exactTail(data.len, footer_size, header_size, readU64(footer, 0), readU64(footer, 8), descriptor_size) catch
            return error.CorruptedVectorDirectory;
        if (readU32(footer, 24) != std.hash.Crc32.hash(root.slice(data))) return error.VectorDirectoryChecksumMismatch;
        const block_count = std.math.cast(usize, readU64(footer, 8)) orelse return error.CorruptedVectorDirectory;
        const verification_words = std.math.divCeil(usize, block_count, @bitSizeOf(u64)) catch
            return error.CorruptedVectorDirectory;
        const verified_indexes = try alloc.alloc(std.atomic.Value(u64), verification_words);
        errdefer alloc.free(verified_indexes);
        const verified_leaf_data = try alloc.alloc(std.atomic.Value(u64), verification_words);
        errdefer alloc.free(verified_leaf_data);
        const verified_metadata_data = try alloc.alloc(std.atomic.Value(u64), verification_words);
        errdefer alloc.free(verified_metadata_data);
        for (verified_indexes) |*word| word.* = std.atomic.Value(u64).init(0);
        for (verified_leaf_data) |*word| word.* = std.atomic.Value(u64).init(0);
        for (verified_metadata_data) |*word| word.* = std.atomic.Value(u64).init(0);
        var reader: Reader = .{
            .alloc = alloc,
            .data = data,
            .root_offset = root.offset,
            .block_count = block_count,
            .entry_count = std.math.cast(usize, readU64(footer, 16)) orelse return error.CorruptedVectorDirectory,
            .verified_indexes = verified_indexes,
            .verified_leaf_data = verified_leaf_data,
            .verified_metadata_data = verified_metadata_data,
        };
        try reader.validateRoot();
        return reader;
    }
    pub fn deinit(self: *Reader) void {
        self.alloc.free(self.verified_indexes);
        self.alloc.free(self.verified_leaf_data);
        self.alloc.free(self.verified_metadata_data);
        self.* = undefined;
    }
    pub fn get(self: Reader, kind: Kind, id: u64) !?[]const u8 {
        const block_index = self.findBlock(id) orelse return null;
        const descriptor_value = try self.descriptor(block_index);
        try self.validateBlock(block_index, descriptor_value, kind);
        const row = (try self.findRow(descriptor_value, id)) orelse return null;
        return try self.valueAt(descriptor_value, kind, row);
    }
    pub fn contains(self: Reader, kind: Kind, id: u64) !bool {
        const block_index = self.findBlock(id) orelse return false;
        const descriptor_value = try self.descriptor(block_index);
        try self.validateBlock(block_index, descriptor_value, null);
        const row = (try self.findRow(descriptor_value, id)) orelse return false;
        return self.present(descriptor_value, kind, row);
    }

    fn findBlock(self: Reader, id: u64) ?usize {
        var lo: usize = 0;
        var hi = self.block_count;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            const descriptor_value = self.descriptor(mid) catch return null;
            if (descriptor_value.last_id < id) lo = mid + 1 else hi = mid;
        }
        if (lo >= self.block_count) return null;
        const descriptor_value = self.descriptor(lo) catch return null;
        if (id < descriptor_value.first_id or id > descriptor_value.last_id) return null;
        return lo;
    }

    pub const Item = struct { kind: Kind, id: u64, value: []const u8 };
    pub const Iterator = struct {
        reader: Reader,
        kind: Kind = .leaf,
        all_kinds: bool = false,
        block_index: usize = 0,
        row: usize = 0,
        descriptor_value: ?Descriptor = null,
        delta_cursor: usize = 0,
        current_id: u64 = 0,

        pub fn next(self: *Iterator) !?Item {
            while (true) {
                while (self.descriptor_value == null or self.row >= self.descriptor_value.?.count) {
                    if (self.block_index >= self.reader.block_count) {
                        if (self.all_kinds and self.kind == .leaf) {
                            self.kind = .metadata;
                            self.block_index = 0;
                            self.descriptor_value = null;
                            continue;
                        }
                        return null;
                    }
                    const current_block = self.block_index;
                    const descriptor_value = try self.reader.descriptor(current_block);
                    try self.reader.validateBlock(current_block, descriptor_value, self.kind);
                    self.block_index += 1;
                    self.row = 0;
                    self.delta_cursor = 0;
                    self.current_id = descriptor_value.first_id;
                    self.descriptor_value = descriptor_value;
                }
                const descriptor_value = self.descriptor_value.?;
                const id = try self.decodeNextId(descriptor_value);
                const row = self.row;
                self.row += 1;
                if (try self.reader.valueAt(descriptor_value, self.kind, row)) |value|
                    return .{ .kind = self.kind, .id = id, .value = value };
            }
        }

        fn decodeNextId(self: *Iterator, descriptor_value: Descriptor) !u64 {
            return switch (descriptor_value.encoding) {
                .raw => readU64(self.reader.idBytes(descriptor_value), self.row * @sizeOf(u64)),
                .restart_varint => blk: {
                    const row_in_group = self.row % id_restart_interval;
                    if (row_in_group == 0) {
                        const restart = self.reader.restartAt(descriptor_value, self.row / id_restart_interval);
                        self.current_id = restart.id;
                        self.delta_cursor = restart.delta_offset;
                    } else {
                        const decoded = try readVarint(self.reader.deltaBytes(descriptor_value), &self.delta_cursor);
                        self.current_id = std.math.add(u64, self.current_id, decoded) catch return error.CorruptedVectorDirectory;
                    }
                    break :blk self.current_id;
                },
            };
        }
    };
    pub fn iterator(self: Reader) Iterator {
        return .{ .reader = self, .all_kinds = true };
    }
    pub fn kindIterator(self: Reader, kind: Kind) Iterator {
        return .{ .reader = self, .kind = kind };
    }
    pub fn entryCount(self: Reader) usize {
        return self.entry_count;
    }

    fn descriptor(self: Reader, index: usize) !Descriptor {
        if (index >= self.block_count) return error.CorruptedVectorDirectory;
        const offset = self.root_offset + index * descriptor_size;
        return decodeDescriptor(self.data[offset..][0..descriptor_size]);
    }
    fn validateRoot(self: Reader) !void {
        var previous: ?Descriptor = null;
        var previous_end = header_size;
        var observed_entries: usize = 0;
        for (0..self.block_count) |index| {
            const current = try self.descriptor(index);
            if (current.count == 0 or current.count > block_entry_limit or current.first_id > current.last_id) return error.CorruptedVectorDirectory;
            if (current.data_offset != previous_end or current.data_offset > current.index_offset) return error.CorruptedVectorDirectory;
            if (current.data_len != current.index_offset - current.data_offset) return error.CorruptedVectorDirectory;
            if (current.index_offset > self.root_offset or current.index_len > self.root_offset - current.index_offset) return error.CorruptedVectorDirectory;
            if (current.metadata_count > current.count) return error.CorruptedVectorDirectory;
            const bitmap_bytes = std.math.mul(usize, presenceBytes(current.count), 2) catch return error.CorruptedVectorDirectory;
            const lengths_len = std.math.mul(usize, current.metadata_count, metadataEndWidth(current)) catch return error.CorruptedVectorDirectory;
            if (current.index_len < bitmap_bytes + lengths_len) return error.CorruptedVectorDirectory;
            if (current.encoding == .raw) {
                const ids_len = std.math.mul(usize, current.count, @sizeOf(u64)) catch return error.CorruptedVectorDirectory;
                if (current.index_len - bitmap_bytes - lengths_len != ids_len) return error.CorruptedVectorDirectory;
            } else {
                const restart_count = std.math.divCeil(usize, current.count, id_restart_interval) catch
                    return error.CorruptedVectorDirectory;
                const minimum_ids_len = std.math.mul(usize, restart_count, 12) catch return error.CorruptedVectorDirectory;
                if (current.index_len - bitmap_bytes - lengths_len < minimum_ids_len) return error.CorruptedVectorDirectory;
            }
            if (previous) |old| if (current.first_id <= old.last_id) return error.CorruptedVectorDirectory;
            observed_entries = std.math.add(usize, observed_entries, current.count) catch return error.CorruptedVectorDirectory;
            previous_end = current.index_offset + current.index_len;
            previous = current;
        }
        if (observed_entries != self.entry_count) return error.CorruptedVectorDirectory;
        if (previous_end != self.root_offset) return error.CorruptedVectorDirectory;
    }
    fn validateBlock(self: Reader, block_index: usize, descriptor_value: Descriptor, verify_data: ?Kind) !void {
        if (!isVerified(self.verified_indexes, block_index)) {
            const index = self.indexBytes(descriptor_value);
            if (std.hash.Crc32.hash(index) != descriptor_value.index_checksum) return error.VectorDirectoryChecksumMismatch;
            switch (descriptor_value.encoding) {
                .raw => if (readU64(index, 0) != descriptor_value.first_id or
                    readU64(index, (descriptor_value.count - 1) * @sizeOf(u64)) != descriptor_value.last_id) return error.CorruptedVectorDirectory,
                .restart_varint => {
                    const restart_count = self.restartCount(descriptor_value);
                    const deltas = self.deltaBytes(descriptor_value);
                    var previous_id: ?u64 = null;
                    for (0..restart_count) |restart_index| {
                        const restart = self.restartAt(descriptor_value, restart_index);
                        if (restart.delta_offset > deltas.len) return error.CorruptedVectorDirectory;
                        if (previous_id) |old| if (restart.id <= old) return error.CorruptedVectorDirectory;
                        var cursor = restart.delta_offset;
                        var id = restart.id;
                        const first_row = restart_index * id_restart_interval;
                        const end_row = @min(first_row + id_restart_interval, descriptor_value.count);
                        var row = first_row + 1;
                        while (row < end_row) : (row += 1) {
                            const delta = try readVarint(deltas, &cursor);
                            if (delta == 0) return error.CorruptedVectorDirectory;
                            id = std.math.add(u64, id, delta) catch return error.CorruptedVectorDirectory;
                        }
                        const expected_end = if (restart_index + 1 < restart_count)
                            self.restartAt(descriptor_value, restart_index + 1).delta_offset
                        else
                            deltas.len;
                        if (cursor != expected_end) return error.CorruptedVectorDirectory;
                        previous_id = id;
                    }
                    if (self.restartAt(descriptor_value, 0).id != descriptor_value.first_id or
                        previous_id.? != descriptor_value.last_id) return error.CorruptedVectorDirectory;
                },
            }
            if (countPresence(self.leafBitmap(descriptor_value), descriptor_value.count) +
                countPresence(self.metadataBitmap(descriptor_value), descriptor_value.count) == 0)
                return error.CorruptedVectorDirectory;
            for (0..descriptor_value.count) |row| {
                if (!self.present(descriptor_value, .leaf, row) and !self.present(descriptor_value, .metadata, row))
                    return error.CorruptedVectorDirectory;
            }
            const bitmap_len = presenceBytes(descriptor_value.count);
            const used_tail_bits: u3 = @intCast(descriptor_value.count % 8);
            if (used_tail_bits != 0) {
                const unused_mask = ~((@as(u8, 1) << used_tail_bits) - 1);
                if (self.leafBitmap(descriptor_value)[bitmap_len - 1] & unused_mask != 0 or
                    self.metadataBitmap(descriptor_value)[bitmap_len - 1] & unused_mask != 0)
                    return error.CorruptedVectorDirectory;
            }
            if (countPresence(self.metadataBitmap(descriptor_value), descriptor_value.count) != descriptor_value.metadata_count)
                return error.CorruptedVectorDirectory;
            const leaf_bytes = countPresence(self.leafBitmap(descriptor_value), descriptor_value.count) * @sizeOf(u64);
            if (leaf_bytes > descriptor_value.data_len) return error.CorruptedVectorDirectory;
            var previous_end: usize = 0;
            for (0..descriptor_value.metadata_count) |ordinal| {
                const end = try self.metadataEnd(descriptor_value, ordinal);
                if (end < previous_end or end > descriptor_value.data_len - leaf_bytes) return error.CorruptedVectorDirectory;
                previous_end = end;
            }
            if (leaf_bytes + previous_end != descriptor_value.data_len) return error.CorruptedVectorDirectory;
            markVerified(self.verified_indexes, block_index);
        }
        if (verify_data) |kind| {
            switch (kind) {
                .leaf => if (!isVerified(self.verified_leaf_data, block_index)) {
                    if (std.hash.Crc32.hash(self.leafDataBytes(descriptor_value)) != descriptor_value.data_checksum)
                        return error.VectorDirectoryChecksumMismatch;
                    markVerified(self.verified_leaf_data, block_index);
                },
                .metadata => if (!isVerified(self.verified_metadata_data, block_index)) {
                    if (std.hash.Crc32.hash(self.metadataDataBytes(descriptor_value)) != descriptor_value.metadata_data_checksum)
                        return error.VectorDirectoryChecksumMismatch;
                    markVerified(self.verified_metadata_data, block_index);
                },
            }
        }
    }
    fn findRow(self: Reader, descriptor_value: Descriptor, id: u64) !?usize {
        const ids = self.idBytes(descriptor_value);
        return switch (descriptor_value.encoding) {
            .raw => blk: {
                var lo: usize = 0;
                var hi = descriptor_value.count;
                while (lo < hi) {
                    const mid = lo + (hi - lo) / 2;
                    if (readU64(ids, mid * @sizeOf(u64)) < id) lo = mid + 1 else hi = mid;
                }
                if (lo < descriptor_value.count and readU64(ids, lo * @sizeOf(u64)) == id) break :blk lo;
                break :blk null;
            },
            .restart_varint => blk: {
                const restart_count = self.restartCount(descriptor_value);
                var lo: usize = 0;
                var hi = restart_count;
                while (lo < hi) {
                    const mid = lo + (hi - lo) / 2;
                    if (self.restartAt(descriptor_value, mid).id <= id) lo = mid + 1 else hi = mid;
                }
                if (lo == 0) break :blk null;
                const restart_index = lo - 1;
                const restart = self.restartAt(descriptor_value, restart_index);
                var cursor = restart.delta_offset;
                var found = restart.id;
                var row = restart_index * id_restart_interval;
                if (found == id) break :blk row;
                const end_row = @min(row + id_restart_interval, descriptor_value.count);
                row += 1;
                while (row < end_row) : (row += 1) {
                    found = std.math.add(u64, found, try readVarint(self.deltaBytes(descriptor_value), &cursor)) catch return error.CorruptedVectorDirectory;
                    if (found == id) break :blk row;
                    if (found > id) break;
                }
                break :blk null;
            },
        };
    }
    fn valueAt(self: Reader, descriptor_value: Descriptor, kind: Kind, row: usize) !?[]const u8 {
        if (row >= descriptor_value.count) return error.CorruptedVectorDirectory;
        if (!self.present(descriptor_value, kind, row)) return null;
        return switch (kind) {
            .leaf => blk: {
                const ordinal = presenceRank(self.leafBitmap(descriptor_value), row);
                break :blk self.data[descriptor_value.data_offset + ordinal * @sizeOf(u64) ..][0..@sizeOf(u64)];
            },
            .metadata => blk: {
                const ordinal = presenceRank(self.metadataBitmap(descriptor_value), row);
                const offset = try self.metadataOffset(descriptor_value, ordinal);
                const len = try self.metadataLength(descriptor_value, ordinal);
                const leaf_bytes = countPresence(self.leafBitmap(descriptor_value), descriptor_value.count) * @sizeOf(u64);
                break :blk self.data[descriptor_value.data_offset + leaf_bytes + offset ..][0..len];
            },
        };
    }
    fn indexBytes(self: Reader, descriptor_value: Descriptor) []const u8 {
        return self.data[descriptor_value.index_offset..][0..descriptor_value.index_len];
    }
    fn idBytes(self: Reader, descriptor_value: Descriptor) []const u8 {
        const suffix_len = presenceBytes(descriptor_value.count) * 2 + descriptor_value.metadata_count * metadataEndWidth(descriptor_value);
        return self.indexBytes(descriptor_value)[0 .. descriptor_value.index_len - suffix_len];
    }
    fn restartCount(_: Reader, descriptor_value: Descriptor) usize {
        return std.math.divCeil(usize, descriptor_value.count, id_restart_interval) catch unreachable;
    }
    const Restart = struct { id: u64, delta_offset: usize };
    fn restartAt(self: Reader, descriptor_value: Descriptor, index: usize) Restart {
        const ids = self.idBytes(descriptor_value);
        const offset = index * 12;
        return .{ .id = readU64(ids, offset), .delta_offset = readU32(ids, offset + 8) };
    }
    fn deltaBytes(self: Reader, descriptor_value: Descriptor) []const u8 {
        const ids = self.idBytes(descriptor_value);
        return ids[self.restartCount(descriptor_value) * 12 ..];
    }
    fn leafBitmap(self: Reader, descriptor_value: Descriptor) []const u8 {
        const ids_len = self.idBytes(descriptor_value).len;
        return self.indexBytes(descriptor_value)[ids_len..][0..presenceBytes(descriptor_value.count)];
    }
    fn metadataBitmap(self: Reader, descriptor_value: Descriptor) []const u8 {
        const ids_len = self.idBytes(descriptor_value).len;
        const bitmap_len = presenceBytes(descriptor_value.count);
        return self.indexBytes(descriptor_value)[ids_len + bitmap_len ..][0..bitmap_len];
    }
    fn present(self: Reader, descriptor_value: Descriptor, kind: Kind, row: usize) bool {
        const bitmap = switch (kind) {
            .leaf => self.leafBitmap(descriptor_value),
            .metadata => self.metadataBitmap(descriptor_value),
        };
        return hasPresence(bitmap, row);
    }
    fn dataBytes(self: Reader, descriptor_value: Descriptor) []const u8 {
        return self.data[descriptor_value.data_offset..][0..descriptor_value.data_len];
    }
    fn leafDataBytes(self: Reader, descriptor_value: Descriptor) []const u8 {
        const len = countPresence(self.leafBitmap(descriptor_value), descriptor_value.count) * @sizeOf(u64);
        return self.data[descriptor_value.data_offset..][0..len];
    }
    fn metadataDataBytes(self: Reader, descriptor_value: Descriptor) []const u8 {
        const leaf_len = self.leafDataBytes(descriptor_value).len;
        return self.data[descriptor_value.data_offset + leaf_len ..][0 .. descriptor_value.data_len - leaf_len];
    }
    fn metadataLength(self: Reader, descriptor_value: Descriptor, ordinal: usize) !usize {
        if (ordinal >= descriptor_value.metadata_count) return error.CorruptedVectorDirectory;
        return (try self.metadataEnd(descriptor_value, ordinal)) - try self.metadataOffset(descriptor_value, ordinal);
    }
    fn metadataOffset(self: Reader, descriptor_value: Descriptor, ordinal: usize) !usize {
        if (ordinal >= descriptor_value.metadata_count) return error.CorruptedVectorDirectory;
        return if (ordinal == 0) 0 else try self.metadataEnd(descriptor_value, ordinal - 1);
    }
    fn metadataEnd(self: Reader, descriptor_value: Descriptor, ordinal: usize) !usize {
        if (ordinal >= descriptor_value.metadata_count) return error.CorruptedVectorDirectory;
        const index = self.indexBytes(descriptor_value);
        const width = metadataEndWidth(descriptor_value);
        const offset = index.len - descriptor_value.metadata_count * width + ordinal * width;
        return switch (descriptor_value.metadata_end_encoding) {
            .u16 => readU16(index, offset),
            .u32 => readU32(index, offset),
        };
    }
};

fn metadataEndWidth(descriptor_value: Descriptor) usize {
    return switch (descriptor_value.metadata_end_encoding) {
        .u16 => @sizeOf(u16),
        .u32 => @sizeOf(u32),
    };
}

fn presenceBytes(count: usize) usize {
    return std.math.divCeil(usize, count, 8) catch unreachable;
}
fn setPresence(bitmap: []u8, row: usize) void {
    bitmap[row / 8] |= @as(u8, 1) << @intCast(row % 8);
}
fn hasPresence(bitmap: []const u8, row: usize) bool {
    return bitmap[row / 8] & (@as(u8, 1) << @intCast(row % 8)) != 0;
}
fn presenceRank(bitmap: []const u8, row: usize) usize {
    var total: usize = 0;
    const whole = row / 8;
    for (bitmap[0..whole]) |byte| total += @popCount(byte);
    if (row % 8 != 0) total += @popCount(bitmap[whole] & ((@as(u8, 1) << @intCast(row % 8)) - 1));
    return total;
}
fn countPresence(bitmap: []const u8, count: usize) usize {
    var total: usize = 0;
    for (bitmap) |byte| total += @popCount(byte);
    const unused = bitmap.len * 8 - count;
    if (unused != 0) total -= @popCount(bitmap[bitmap.len - 1] >> @intCast(8 - unused));
    return total;
}

fn isVerified(words: []std.atomic.Value(u64), block_index: usize) bool {
    const word = block_index / @bitSizeOf(u64);
    const mask = @as(u64, 1) << @intCast(block_index % @bitSizeOf(u64));
    return words[word].load(.acquire) & mask != 0;
}

fn markVerified(words: []std.atomic.Value(u64), block_index: usize) void {
    const word = block_index / @bitSizeOf(u64);
    const mask = @as(u64, 1) << @intCast(block_index % @bitSizeOf(u64));
    _ = words[word].fetchOr(mask, .release);
}

fn encodeDescriptor(value: Descriptor) [descriptor_size]u8 {
    var out: [descriptor_size]u8 = @splat(0);
    out[0] = @intFromEnum(value.encoding);
    out[1] = @intFromEnum(value.metadata_end_encoding);
    writeU32(&out, 4, @intCast(value.count));
    writeU64(&out, 8, value.first_id);
    writeU64(&out, 16, value.last_id);
    writeU64(&out, 24, @intCast(value.data_offset));
    writeU64(&out, 32, @intCast(value.data_len));
    writeU64(&out, 40, @intCast(value.index_offset));
    writeU32(&out, 48, @intCast(value.index_len));
    writeU32(&out, 52, value.data_checksum);
    writeU32(&out, 56, value.index_checksum);
    writeU32(&out, 60, @intCast(value.metadata_count));
    writeU32(&out, 64, value.metadata_data_checksum);
    return out;
}
fn decodeDescriptor(bytes: []const u8) !Descriptor {
    if (bytes.len != descriptor_size or readU16(bytes, 2) != 0) return error.CorruptedVectorDirectory;
    const encoding: IdEncoding = switch (bytes[0]) {
        @intFromEnum(IdEncoding.raw) => .raw,
        @intFromEnum(IdEncoding.restart_varint) => .restart_varint,
        else => return error.CorruptedVectorDirectory,
    };
    const metadata_end_encoding: MetadataEndEncoding = switch (bytes[1]) {
        @intFromEnum(MetadataEndEncoding.u16) => .u16,
        @intFromEnum(MetadataEndEncoding.u32) => .u32,
        else => return error.CorruptedVectorDirectory,
    };
    return .{
        .encoding = encoding,
        .metadata_end_encoding = metadata_end_encoding,
        .count = readU32(bytes, 4),
        .first_id = readU64(bytes, 8),
        .last_id = readU64(bytes, 16),
        .data_offset = std.math.cast(usize, readU64(bytes, 24)) orelse return error.CorruptedVectorDirectory,
        .data_len = std.math.cast(usize, readU64(bytes, 32)) orelse return error.CorruptedVectorDirectory,
        .index_offset = std.math.cast(usize, readU64(bytes, 40)) orelse return error.CorruptedVectorDirectory,
        .index_len = readU32(bytes, 48),
        .data_checksum = readU32(bytes, 52),
        .index_checksum = readU32(bytes, 56),
        .metadata_count = readU32(bytes, 60),
        .metadata_data_checksum = readU32(bytes, 64),
    };
}

fn varintLength(value: u64) usize {
    var remaining = value;
    var len: usize = 1;
    while (remaining >= 0x80) : (len += 1) remaining >>= 7;
    return len;
}
fn appendVarint(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: u64) !void {
    var remaining = value;
    while (remaining >= 0x80) {
        try out.append(alloc, @as(u8, @truncate(remaining)) | 0x80);
        remaining >>= 7;
    }
    try out.append(alloc, @truncate(remaining));
}
fn readVarint(bytes: []const u8, cursor: *usize) !u64 {
    var value: u64 = 0;
    var shift: u6 = 0;
    var count: usize = 0;
    while (cursor.* < bytes.len and count < 10) : (count += 1) {
        const byte = bytes[cursor.*];
        cursor.* += 1;
        if (count == 9 and byte > 1) return error.CorruptedVectorDirectory;
        value |= @as(u64, byte & 0x7f) << shift;
        if (byte & 0x80 == 0) return value;
        shift += 7;
    }
    return error.CorruptedVectorDirectory;
}
fn appendU32(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: u32) !void {
    var buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &buf, value, .big);
    try out.appendSlice(alloc, &buf);
}
fn appendU16(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: u16) !void {
    var buf: [2]u8 = undefined;
    std.mem.writeInt(u16, &buf, value, .big);
    try out.appendSlice(alloc, &buf);
}
fn appendU64(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: u64) !void {
    var buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &buf, value, .big);
    try out.appendSlice(alloc, &buf);
}
fn writeU16(bytes: []u8, offset: usize, value: u16) void {
    std.mem.writeInt(u16, bytes[offset..][0..2], value, .big);
}
fn writeU32(bytes: []u8, offset: usize, value: u32) void {
    std.mem.writeInt(u32, bytes[offset..][0..4], value, .big);
}
fn writeU64(bytes: []u8, offset: usize, value: u64) void {
    std.mem.writeInt(u64, bytes[offset..][0..8], value, .big);
}
fn readU16(bytes: []const u8, offset: usize) u16 {
    return std.mem.readInt(u16, bytes[offset..][0..2], .big);
}
fn readU32(bytes: []const u8, offset: usize) u32 {
    return std.mem.readInt(u32, bytes[offset..][0..4], .big);
}
fn readU64(bytes: []const u8, offset: usize) u64 {
    return std.mem.readInt(u64, bytes[offset..][0..8], .big);
}

test "HBC vector directory round trips union row blocks" {
    const alloc = std.testing.allocator;
    var writer = try Writer.init(alloc);
    defer writer.deinit();
    try writer.appendRow(7, "leaf-007", "doc:7");
    try writer.appendRow(42, "leaf-042", null);
    const bytes = try writer.build();
    defer alloc.free(bytes);
    var reader = try Reader.init(alloc, bytes);
    defer reader.deinit();
    try std.testing.expectEqualStrings("leaf-042", (try reader.get(.leaf, 42)).?);
    try std.testing.expectEqualStrings("doc:7", (try reader.get(.metadata, 7)).?);
    try std.testing.expectEqual(@as(?[]const u8, null), try reader.get(.metadata, 42));
    var it = reader.iterator();
    const first = (try it.next()).?;
    try std.testing.expectEqual(Kind.leaf, first.kind);
    try std.testing.expectEqual(@as(u64, 7), first.id);
    try std.testing.expectEqualStrings("leaf-007", first.value);
    try std.testing.expect((try it.next()) != null);
    try std.testing.expect((try it.next()) != null);
    try std.testing.expect((try it.next()) == null);
}

test "HBC vector directory streaming writer is byte-compatible" {
    const alloc = std.testing.allocator;
    const Sink = struct {
        alloc: Allocator,
        out: std.ArrayListUnmanaged(u8) = .empty,
        fn len(self: *const @This()) usize {
            return self.out.items.len;
        }
        fn appendSlice(self: *@This(), bytes: []const u8) !void {
            try self.out.appendSlice(self.alloc, bytes);
        }
    };
    var expected_writer = try Writer.init(alloc);
    defer expected_writer.deinit();
    try expected_writer.appendRow(7, "leaf-007", "doc:7");
    try expected_writer.appendRow(42, "leaf-042", null);
    const expected = try expected_writer.build();
    defer alloc.free(expected);
    var sink: Sink = .{ .alloc = alloc };
    defer sink.out.deinit(alloc);
    try sink.appendSlice("prefix");
    var streaming = try StreamingWriter.init(alloc, &sink);
    defer streaming.deinit();
    try streaming.appendRow(&sink, 7, "leaf-007", "doc:7");
    try streaming.appendRow(&sink, 42, "leaf-042", null);
    const finish = try streaming.finish(&sink);
    try std.testing.expectEqual(@as(usize, "prefix".len), finish.offset);
    try std.testing.expectEqual(expected.len, finish.len);
    try std.testing.expectEqual(@as(u64, 2), finish.entry_count);
    try std.testing.expectEqual(@as(u64, 2), finish.leaf_entry_count);
    try std.testing.expectEqual(@as(u64, 1), finish.metadata_entry_count);
    try std.testing.expectEqual(@as(usize, 1), finish.block_count);
    try std.testing.expectEqual(@as(u64, 21), finish.value_bytes);
    try std.testing.expect(finish.index_bytes < 32);
    try std.testing.expectEqual(@as(usize, descriptor_size), finish.root_bytes);
    try std.testing.expectEqualSlices(u8, expected, sink.out.items[finish.offset..][0..finish.len]);
    var reader = try Reader.init(alloc, sink.out.items[finish.offset..][0..finish.len]);
    defer reader.deinit();
    try std.testing.expect(try reader.contains(.leaf, 42));
    try std.testing.expect(!try reader.contains(.metadata, 42));
    try std.testing.expectEqualStrings("leaf-042", (try reader.get(.leaf, 42)).?);
}

test "HBC vector directory streaming writer batches physical appends by block" {
    const alloc = std.testing.allocator;
    const Sink = struct {
        alloc: Allocator,
        out: std.ArrayListUnmanaged(u8) = .empty,
        append_calls: usize = 0,
        fn len(self: *const @This()) usize {
            return self.out.items.len;
        }
        fn appendSlice(self: *@This(), bytes: []const u8) !void {
            self.append_calls += 1;
            try self.out.appendSlice(self.alloc, bytes);
        }
    };
    const count = block_entry_limit * 4;
    var sink: Sink = .{ .alloc = alloc };
    defer sink.out.deinit(alloc);
    var writer = try StreamingWriter.init(alloc, &sink);
    defer writer.deinit();
    try writer.reserveEntries(count * 2);
    var leaf: [8]u8 = undefined;
    for (0..count) |index| {
        std.mem.writeInt(u64, &leaf, @intCast(index + 1), .little);
        try writer.appendRow(&sink, @intCast(index * 3 + 1), &leaf, "doc");
    }
    const finish = try writer.finish(&sink);
    const expected_blocks: usize = 4;
    try std.testing.expectEqual(expected_blocks, finish.block_count);
    // Header + two writes per block (data/index) + root + footer. The count is
    // independent of entry cardinality within a block.
    try std.testing.expectEqual(@as(usize, 1 + expected_blocks * 2 + 1 + 1), sink.append_calls);
}

test "HBC vector directory validates blocks lazily" {
    const alloc = std.testing.allocator;
    var writer = try Writer.init(alloc);
    defer writer.deinit();
    try writer.appendRow(7, "leaf-007", "doc:7");
    const bytes = try writer.build();
    defer alloc.free(bytes);
    var clean = try Reader.init(alloc, bytes);
    defer clean.deinit();
    const leaf = try clean.descriptor(0);
    bytes[leaf.data_offset] ^= 1;
    var reader = try Reader.init(alloc, bytes);
    defer reader.deinit();
    try std.testing.expectEqualStrings("doc:7", (try reader.get(.metadata, 7)).?);
    try std.testing.expectError(error.VectorDirectoryChecksumMismatch, reader.get(.leaf, 7));
}

test "HBC vector directory supports sparse IDs across block boundaries" {
    const alloc = std.testing.allocator;
    var writer = try Writer.init(alloc);
    defer writer.deinit();
    var leaf: [8]u8 = undefined;
    for (0..block_entry_limit + 3) |index| {
        std.mem.writeInt(u64, &leaf, @intCast(index + 1000), .little);
        const id = @as(u64, @intCast(index)) * 1009 + 7;
        try writer.appendRow(id, &leaf, if (index == 0) "first" else null);
    }
    try writer.appendRow(999_999_937, null, "last");
    const bytes = try writer.build();
    defer alloc.free(bytes);
    var reader = try Reader.init(alloc, bytes);
    defer reader.deinit();
    try std.testing.expectEqual(@as(usize, block_entry_limit + 4), reader.entryCount());
    try std.testing.expectEqualStrings("first", (try reader.get(.metadata, 7)).?);
    try std.testing.expectEqualStrings("last", (try reader.get(.metadata, 999_999_937)).?);
    try std.testing.expect(try reader.contains(.leaf, @as(u64, block_entry_limit + 2) * 1009 + 7));
    try std.testing.expect(!try reader.contains(.leaf, 8));
}

test "HBC vector directory row-block overhead stays bounded" {
    const alloc = std.testing.allocator;
    const count = 4096;
    var writer = try Writer.init(alloc);
    defer writer.deinit();
    var leaf: [8]u8 = undefined;
    for (0..count) |index| {
        std.mem.writeInt(u64, &leaf, @intCast(index % 127 + 1), .little);
        try writer.appendRow(@intCast(index * 3 + 1), &leaf, "");
    }
    const bytes = try writer.build();
    defer alloc.free(bytes);

    // Empty metadata isolates structural overhead. V1 used 58 bytes of index
    // per logical vector before leaf values; V2 remains below 16 bytes total.
    try std.testing.expect(bytes.len < count * 16);
    var reader = try Reader.init(alloc, bytes);
    defer reader.deinit();
    try std.testing.expectEqual(@as(usize, count), reader.entryCount());
    try std.testing.expect(try reader.contains(.metadata, (count - 1) * 3 + 1));
    try std.testing.expectEqual(@as(usize, 0), (try reader.get(.metadata, (count - 1) * 3 + 1)).?.len);
}

test "HBC vector directory widens metadata ends only for oversized blocks" {
    const alloc = std.testing.allocator;
    var writer = try Writer.init(alloc);
    defer writer.deinit();
    var leaf: [8]u8 = @splat(0);
    try writer.appendRow(1, &leaf, "small");
    const oversized = try alloc.alloc(u8, std.math.maxInt(u16) + 1);
    defer alloc.free(oversized);
    @memset(oversized, 0xa5);
    try writer.appendRow(2, &leaf, oversized);
    const bytes = try writer.build();
    defer alloc.free(bytes);

    var reader = try Reader.init(alloc, bytes);
    defer reader.deinit();
    try std.testing.expectEqual(MetadataEndEncoding.u16, (try reader.descriptor(0)).metadata_end_encoding);
    try std.testing.expectEqual(MetadataEndEncoding.u32, (try reader.descriptor(1)).metadata_end_encoding);
    try std.testing.expectEqualStrings("small", (try reader.get(.metadata, 1)).?);
    try std.testing.expectEqual(oversized.len, (try reader.get(.metadata, 2)).?.len);
}

test "HBC vector directory rejects wrapped root regions before slicing" {
    const alloc = std.testing.allocator;
    var writer = try Writer.init(alloc);
    defer writer.deinit();
    try writer.appendRow(7, "leaf-007", null);
    const bytes = try writer.build();
    defer alloc.free(bytes);
    const footer = bytes[bytes.len - footer_size ..];
    writeU64(footer, 0, std.math.maxInt(u64) - 7);
    writeU64(footer, 8, 1);
    writeU32(footer, 32, std.hash.Crc32.hash(footer[0..32]));
    try std.testing.expectError(error.CorruptedVectorDirectory, Reader.init(alloc, bytes));
}

test "HBC vector directory rejects unchecksummed bytes before root" {
    const alloc = std.testing.allocator;
    var writer = try Writer.init(alloc);
    defer writer.deinit();
    try writer.appendRow(7, "leaf-007", null);
    try writer.appendRow(8, "leaf-008", null);
    const bytes = try writer.build();
    defer alloc.free(bytes);

    const footer = bytes[bytes.len - footer_size ..];
    const root_offset = std.math.cast(usize, readU64(footer, 0)) orelse unreachable;
    const root = bytes[root_offset .. bytes.len - footer_size];
    const old_index_len = readU32(root, 48);
    try std.testing.expect(old_index_len > 0);
    writeU32(root, 48, old_index_len - 1);
    writeU32(footer, 24, std.hash.Crc32.hash(root));
    writeU32(footer, 32, std.hash.Crc32.hash(footer[0..32]));

    try std.testing.expectError(error.CorruptedVectorDirectory, Reader.init(alloc, bytes));
}
