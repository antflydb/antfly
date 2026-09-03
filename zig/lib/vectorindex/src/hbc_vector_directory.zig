// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//! Compact immutable vector-to-leaf and vector-metadata directory.
//!
//! Entries arrive in HBC namespace key order (`l:` then `m:`), so the writer
//! emits the binary-search index directly without allocating or sorting one
//! object per vector. Values occupy one contiguous block and retain individual
//! checksums. Raw embedding vectors intentionally use the source store or a
//! separately sharded vector-block format.

const std = @import("std");
const Allocator = std.mem.Allocator;
const checked_region = @import("checked_region.zig");

const magic: [4]u8 = "AFVD".*;
const version: u16 = 1;
const header_size: usize = 8;
const index_entry_size: usize = 1 + 8 + 8 + 8 + 4;
const footer_size: usize = 8 + 8 + 4 + 2 + 2 + 4 + 4;

pub const Kind = enum(u8) {
    leaf = 1,
    metadata = 2,
};

pub const Writer = struct {
    alloc: Allocator,
    data: std.ArrayListUnmanaged(u8) = .empty,
    index: std.ArrayListUnmanaged(u8) = .empty,
    count: u64 = 0,
    previous_kind: ?Kind = null,
    previous_id: u64 = 0,
    finished: bool = false,

    pub fn init(alloc: Allocator) !Writer {
        var self: Writer = .{ .alloc = alloc };
        try self.data.appendSlice(alloc, &magic);
        try appendU16(alloc, &self.data, version);
        try appendU16(alloc, &self.data, 0);
        return self;
    }

    pub fn deinit(self: *Writer) void {
        self.data.deinit(self.alloc);
        self.index.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn append(self: *Writer, kind: Kind, id: u64, value: []const u8) !void {
        if (self.finished) return error.VectorDirectoryWriterFinished;
        if (self.previous_kind) |previous_kind| {
            if (@intFromEnum(kind) < @intFromEnum(previous_kind) or
                (kind == previous_kind and id <= self.previous_id))
            {
                return error.OutOfOrderVectorDirectoryEntry;
            }
        }
        const offset = self.data.items.len;
        try self.data.appendSlice(self.alloc, value);
        try self.index.append(self.alloc, @intFromEnum(kind));
        try appendU64(self.alloc, &self.index, id);
        try appendU64(self.alloc, &self.index, @intCast(offset));
        try appendU64(self.alloc, &self.index, @intCast(value.len));
        try appendU32(self.alloc, &self.index, std.hash.Crc32.hash(value));
        self.count += 1;
        self.previous_kind = kind;
        self.previous_id = id;
    }

    pub fn build(self: *Writer) ![]u8 {
        if (self.finished) return error.VectorDirectoryWriterFinished;
        self.finished = true;
        const index_offset = self.data.items.len;
        try self.data.appendSlice(self.alloc, self.index.items);
        try appendU64(self.alloc, &self.data, @intCast(index_offset));
        try appendU64(self.alloc, &self.data, self.count);
        try appendU32(self.alloc, &self.data, std.hash.Crc32.hash(self.index.items));
        try appendU16(self.alloc, &self.data, version);
        try appendU16(self.alloc, &self.data, 0);
        const footer_without_checksum = self.data.items[self.data.items.len - 24 ..];
        try appendU32(self.alloc, &self.data, std.hash.Crc32.hash(footer_without_checksum));
        try self.data.appendSlice(self.alloc, &magic);
        self.index.clearAndFree(self.alloc);
        return try self.data.toOwnedSlice(self.alloc);
    }
};

/// File-backed counterpart to `Writer`. Values are copied directly into the
/// enclosing atomic generation; only the fixed-width binary-search index is
/// retained until publication. This preserves the V1 byte format while
/// avoiding a second corpus-sized value buffer and the final joined copy.
pub const StreamingWriter = struct {
    alloc: Allocator,
    base_offset: usize,
    index: std.ArrayListUnmanaged(u8) = .empty,
    count: u64 = 0,
    previous_kind: ?Kind = null,
    previous_id: u64 = 0,
    finished: bool = false,

    pub const Finish = struct {
        offset: usize,
        len: usize,
        /// The nested reader validates its index eagerly and each value lazily,
        /// so the outer segment intentionally avoids rereading the completed
        /// staged range merely to calculate a redundant checksum.
        outer_checksum: u32 = 0,
    };

    pub fn init(alloc: Allocator, sink: anytype) !StreamingWriter {
        const base_offset = sink.len();
        var header: [header_size]u8 = undefined;
        @memcpy(header[0..4], &magic);
        std.mem.writeInt(u16, header[4..6], version, .big);
        std.mem.writeInt(u16, header[6..8], 0, .big);
        try sink.appendSlice(&header);
        return .{ .alloc = alloc, .base_offset = base_offset };
    }

    pub fn deinit(self: *StreamingWriter) void {
        self.index.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn reserveEntries(self: *StreamingWriter, count: usize) !void {
        if (self.finished) return error.VectorDirectoryWriterFinished;
        const bytes = std.math.mul(usize, count, index_entry_size) catch
            return error.VectorDirectoryTooLarge;
        try self.index.ensureTotalCapacity(self.alloc, bytes);
    }

    pub fn append(self: *StreamingWriter, sink: anytype, kind: Kind, id: u64, value: []const u8) !void {
        if (self.finished) return error.VectorDirectoryWriterFinished;
        if (self.previous_kind) |previous_kind| {
            if (@intFromEnum(kind) < @intFromEnum(previous_kind) or
                (kind == previous_kind and id <= self.previous_id))
            {
                return error.OutOfOrderVectorDirectoryEntry;
            }
        }
        const offset = sink.len() - self.base_offset;
        try sink.appendSlice(value);
        var entry: [index_entry_size]u8 = undefined;
        entry[0] = @intFromEnum(kind);
        std.mem.writeInt(u64, entry[1..9], id, .big);
        std.mem.writeInt(u64, entry[9..17], @intCast(offset), .big);
        std.mem.writeInt(u64, entry[17..25], @intCast(value.len), .big);
        std.mem.writeInt(u32, entry[25..29], std.hash.Crc32.hash(value), .big);
        try self.index.appendSlice(self.alloc, &entry);
        self.count += 1;
        self.previous_kind = kind;
        self.previous_id = id;
    }

    pub fn finish(self: *StreamingWriter, sink: anytype) !Finish {
        if (self.finished) return error.VectorDirectoryWriterFinished;
        self.finished = true;
        const index_offset = sink.len() - self.base_offset;
        try sink.appendSlice(self.index.items);
        var footer: [footer_size]u8 = undefined;
        std.mem.writeInt(u64, footer[0..8], @intCast(index_offset), .big);
        std.mem.writeInt(u64, footer[8..16], self.count, .big);
        std.mem.writeInt(u32, footer[16..20], std.hash.Crc32.hash(self.index.items), .big);
        std.mem.writeInt(u16, footer[20..22], version, .big);
        std.mem.writeInt(u16, footer[22..24], 0, .big);
        std.mem.writeInt(u32, footer[24..28], std.hash.Crc32.hash(footer[0..24]), .big);
        @memcpy(footer[28..32], &magic);
        try sink.appendSlice(&footer);
        return .{
            .offset = self.base_offset,
            .len = sink.len() - self.base_offset,
        };
    }
};

pub const Reader = struct {
    data: []const u8,
    index_offset: usize,
    count: usize,

    pub fn init(data: []const u8) !Reader {
        if (data.len < header_size + footer_size or !std.mem.eql(u8, data[0..4], &magic)) return error.CorruptedVectorDirectory;
        if (readU16(data[4..6]) != version or readU16(data[6..8]) != 0) return error.UnsupportedVectorDirectoryVersion;
        const footer = data[data.len - footer_size ..];
        if (!std.mem.eql(u8, footer[footer_size - 4 ..], &magic)) return error.CorruptedVectorDirectory;
        if (readU16(footer[20..22]) != version or readU16(footer[22..24]) != 0) return error.UnsupportedVectorDirectoryVersion;
        if (readU32(footer[24..28]) != std.hash.Crc32.hash(footer[0..24])) return error.VectorDirectoryChecksumMismatch;
        const count_raw = readU64(footer[8..16]);
        const index_region = checked_region.exactTail(
            data.len,
            footer_size,
            header_size,
            readU64(footer[0..8]),
            count_raw,
            index_entry_size,
        ) catch return error.CorruptedVectorDirectory;
        const count = std.math.cast(usize, count_raw) orelse return error.CorruptedVectorDirectory;
        if (readU32(footer[16..20]) != std.hash.Crc32.hash(index_region.slice(data))) return error.VectorDirectoryChecksumMismatch;
        const reader: Reader = .{ .data = data, .index_offset = index_region.offset, .count = count };
        try reader.validate();
        return reader;
    }

    pub fn get(self: Reader, kind: Kind, id: u64) !?[]const u8 {
        const index = (try self.findIndex(kind, id)) orelse return null;
        return try self.checkedValue(try self.entry(index));
    }

    /// Metadata-only membership probe used by streaming compaction to reserve
    /// the exact destination index size without touching the value plane.
    pub fn contains(self: Reader, kind: Kind, id: u64) !bool {
        return (try self.findIndex(kind, id)) != null;
    }

    fn findIndex(self: Reader, kind: Kind, id: u64) !?usize {
        var lo: usize = 0;
        var hi = self.count;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            const found = try self.entry(mid);
            switch (compare(found.kind, found.id, kind, id)) {
                .lt => lo = mid + 1,
                .gt => hi = mid,
                .eq => return mid,
            }
        }
        return null;
    }

    pub const Item = struct {
        kind: Kind,
        id: u64,
        value: []const u8,
    };

    pub const Iterator = struct {
        reader: Reader,
        index: usize = 0,

        pub fn next(self: *Iterator) !?Item {
            if (self.index >= self.reader.count) return null;
            const found = try self.reader.entry(self.index);
            self.index += 1;
            return .{
                .kind = found.kind,
                .id = found.id,
                .value = try self.reader.checkedValue(found),
            };
        }
    };

    pub fn iterator(self: Reader) Iterator {
        return .{ .reader = self };
    }

    pub fn entryCount(self: Reader) usize {
        return self.count;
    }

    const Entry = struct { kind: Kind, id: u64, offset: usize, len: usize, checksum: u32 };

    fn entry(self: Reader, index: usize) !Entry {
        if (index >= self.count) return error.CorruptedVectorDirectory;
        const raw = self.data[self.index_offset + index * index_entry_size ..][0..index_entry_size];
        const kind: Kind = switch (raw[0]) {
            @intFromEnum(Kind.leaf) => .leaf,
            @intFromEnum(Kind.metadata) => .metadata,
            else => return error.CorruptedVectorDirectory,
        };
        const offset = std.math.cast(usize, readU64(raw[9..17])) orelse return error.CorruptedVectorDirectory;
        const len = std.math.cast(usize, readU64(raw[17..25])) orelse return error.CorruptedVectorDirectory;
        if (offset < header_size or offset > self.index_offset or len > self.index_offset - offset) return error.CorruptedVectorDirectory;
        return .{ .kind = kind, .id = readU64(raw[1..9]), .offset = offset, .len = len, .checksum = readU32(raw[25..29]) };
    }

    fn checkedValue(self: Reader, entry_value: Entry) ![]const u8 {
        const value = self.data[entry_value.offset..][0..entry_value.len];
        if (std.hash.Crc32.hash(value) != entry_value.checksum) return error.VectorDirectoryChecksumMismatch;
        return value;
    }

    fn validate(self: Reader) !void {
        var previous: ?Entry = null;
        for (0..self.count) |i| {
            const current = try self.entry(i);
            if (previous) |old| if (compare(old.kind, old.id, current.kind, current.id) != .lt) return error.CorruptedVectorDirectory;
            previous = current;
        }
    }
};

fn compare(a_kind: Kind, a_id: u64, b_kind: Kind, b_id: u64) std.math.Order {
    if (@intFromEnum(a_kind) < @intFromEnum(b_kind)) return .lt;
    if (@intFromEnum(a_kind) > @intFromEnum(b_kind)) return .gt;
    return std.math.order(a_id, b_id);
}

fn appendU16(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: u16) !void {
    var buf: [2]u8 = undefined;
    std.mem.writeInt(u16, &buf, value, .big);
    try out.appendSlice(alloc, &buf);
}
fn appendU32(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: u32) !void {
    var buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &buf, value, .big);
    try out.appendSlice(alloc, &buf);
}
fn appendU64(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: u64) !void {
    var buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &buf, value, .big);
    try out.appendSlice(alloc, &buf);
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

test "HBC vector directory round trips compact ordered values" {
    const alloc = std.testing.allocator;
    var writer = try Writer.init(alloc);
    defer writer.deinit();
    try writer.append(.leaf, 7, "leaf-7");
    try writer.append(.leaf, 42, "leaf-42");
    try writer.append(.metadata, 7, "doc:7");
    const bytes = try writer.build();
    defer alloc.free(bytes);
    const reader = try Reader.init(bytes);
    try std.testing.expectEqualStrings("leaf-42", (try reader.get(.leaf, 42)).?);
    try std.testing.expectEqualStrings("doc:7", (try reader.get(.metadata, 7)).?);
    try std.testing.expectEqual(@as(?[]const u8, null), try reader.get(.metadata, 42));

    var it = reader.iterator();
    const first = (try it.next()).?;
    try std.testing.expectEqual(Kind.leaf, first.kind);
    try std.testing.expectEqual(@as(u64, 7), first.id);
    try std.testing.expectEqualStrings("leaf-7", first.value);
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
    try expected_writer.append(.leaf, 7, "leaf-7");
    try expected_writer.append(.leaf, 42, "leaf-42");
    try expected_writer.append(.metadata, 7, "doc:7");
    const expected = try expected_writer.build();
    defer alloc.free(expected);

    var sink: Sink = .{ .alloc = alloc };
    defer sink.out.deinit(alloc);
    try sink.appendSlice("prefix");
    var streaming = try StreamingWriter.init(alloc, &sink);
    defer streaming.deinit();
    try streaming.append(&sink, .leaf, 7, "leaf-7");
    try streaming.append(&sink, .leaf, 42, "leaf-42");
    try streaming.append(&sink, .metadata, 7, "doc:7");
    const finish = try streaming.finish(&sink);
    try std.testing.expectEqual(@as(usize, "prefix".len), finish.offset);
    try std.testing.expectEqual(expected.len, finish.len);
    try std.testing.expectEqualSlices(u8, expected, sink.out.items[finish.offset..][0..finish.len]);

    const reader = try Reader.init(sink.out.items[finish.offset..][0..finish.len]);
    try std.testing.expect(try reader.contains(.leaf, 42));
    try std.testing.expect(!try reader.contains(.metadata, 42));
    try std.testing.expectEqualStrings("leaf-42", (try reader.get(.leaf, 42)).?);
}

test "HBC vector directory verifies values lazily" {
    const alloc = std.testing.allocator;
    var writer = try Writer.init(alloc);
    defer writer.deinit();
    try writer.append(.leaf, 7, "leaf-7");
    try writer.append(.metadata, 7, "doc:7");
    const bytes = try writer.build();
    defer alloc.free(bytes);

    // Payload corruption does not prevent opening or unrelated lookup. It is
    // reported precisely when the corrupt value is requested.
    bytes[header_size] ^= 1;
    const reader = try Reader.init(bytes);
    try std.testing.expectEqualStrings("doc:7", (try reader.get(.metadata, 7)).?);
    try std.testing.expectError(error.VectorDirectoryChecksumMismatch, reader.get(.leaf, 7));
}

test "HBC vector directory rejects wrapped index regions before slicing" {
    const alloc = std.testing.allocator;
    var writer = try Writer.init(alloc);
    defer writer.deinit();
    try writer.append(.leaf, 7, "leaf-7");
    const bytes = try writer.build();
    defer alloc.free(bytes);

    const footer = bytes[bytes.len - footer_size ..];
    std.mem.writeInt(u64, footer[0..8], std.math.maxInt(u64) - 7, .big);
    std.mem.writeInt(u64, footer[8..16], 1, .big);
    std.mem.writeInt(u32, footer[24..28], std.hash.Crc32.hash(footer[0..24]), .big);
    try std.testing.expectError(error.CorruptedVectorDirectory, Reader.init(bytes));
}
