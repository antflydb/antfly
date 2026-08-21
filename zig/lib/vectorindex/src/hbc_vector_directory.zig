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
        const index_offset = std.math.cast(usize, readU64(footer[0..8])) orelse return error.CorruptedVectorDirectory;
        const count = std.math.cast(usize, readU64(footer[8..16])) orelse return error.CorruptedVectorDirectory;
        const index_len = std.math.mul(usize, count, index_entry_size) catch return error.CorruptedVectorDirectory;
        if (index_offset < header_size or index_offset + index_len != data.len - footer_size) return error.CorruptedVectorDirectory;
        if (readU32(footer[16..20]) != std.hash.Crc32.hash(data[index_offset .. index_offset + index_len])) return error.VectorDirectoryChecksumMismatch;
        const reader: Reader = .{ .data = data, .index_offset = index_offset, .count = count };
        try reader.validate();
        return reader;
    }

    pub fn get(self: Reader, kind: Kind, id: u64) !?[]const u8 {
        var lo: usize = 0;
        var hi = self.count;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            const found = try self.entry(mid);
            switch (compare(found.kind, found.id, kind, id)) {
                .lt => lo = mid + 1,
                .gt => hi = mid,
                .eq => {
                    const value = self.data[found.offset..][0..found.len];
                    if (std.hash.Crc32.hash(value) != found.checksum) return error.VectorDirectoryChecksumMismatch;
                    return value;
                },
            }
        }
        return null;
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
