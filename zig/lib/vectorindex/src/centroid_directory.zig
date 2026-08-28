// Copyright 2026 Antfly, Inc.
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

//! Immutable, mmap-friendly exact-centroid directory.
//!
//! The outer posting segment supplies durability, generation publication, and
//! a checksum. This codec keeps routing data columnar inside independently
//! aligned blocks so a query can SIMD-scan centroid vectors without decoding
//! or retaining a second heap copy.

const std = @import("std");

const Allocator = std.mem.Allocator;
const magic: [4]u8 = "AFCD".*;
const version: u16 = 1;
const header_size: usize = 32;
const block_header_size: usize = 16;
const block_alignment: usize = 64;

fn appendZeros(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), count: usize) !void {
    try out.appendNTimes(alloc, 0, count);
}

fn alignOutput(alloc: Allocator, out: *std.ArrayListUnmanaged(u8)) !void {
    const aligned = std.mem.alignForward(usize, out.items.len, block_alignment);
    try appendZeros(alloc, out, aligned - out.items.len);
}

fn writeU16(bytes: []u8, offset: usize, value: u16) void {
    std.mem.writeInt(u16, bytes[offset..][0..2], value, .little);
}

fn writeU32(bytes: []u8, offset: usize, value: u32) void {
    std.mem.writeInt(u32, bytes[offset..][0..4], value, .little);
}

fn writeU64(bytes: []u8, offset: usize, value: u64) void {
    std.mem.writeInt(u64, bytes[offset..][0..8], value, .little);
}

fn readU16(bytes: []const u8, offset: usize) u16 {
    return std.mem.readInt(u16, bytes[offset..][0..2], .little);
}

fn readU32(bytes: []const u8, offset: usize) u32 {
    return std.mem.readInt(u32, bytes[offset..][0..4], .little);
}

fn readU64(bytes: []const u8, offset: usize) u64 {
    return std.mem.readInt(u64, bytes[offset..][0..8], .little);
}

pub const Writer = struct {
    alloc: Allocator,
    dims: usize,
    metric: u8,
    block_size: usize,
    out: std.ArrayListUnmanaged(u8) = .empty,
    posting_ids: std.ArrayListUnmanaged(u64) = .empty,
    covering_radii: std.ArrayListUnmanaged(f32) = .empty,
    measures: std.ArrayListUnmanaged(f32) = .empty,
    vectors: std.ArrayListUnmanaged(f32) = .empty,
    posting_count: u64 = 0,
    block_count: u32 = 0,
    finished: bool = false,

    pub fn init(alloc: Allocator, dims: usize, metric: u8, block_size: usize) !Writer {
        if (dims == 0 or dims > std.math.maxInt(u32) or block_size == 0 or block_size > std.math.maxInt(u32)) {
            return error.InvalidCentroidDirectoryConfig;
        }
        var self: Writer = .{
            .alloc = alloc,
            .dims = dims,
            .metric = metric,
            .block_size = block_size,
        };
        errdefer self.deinit();
        try appendZeros(alloc, &self.out, header_size);
        return self;
    }

    pub fn deinit(self: *Writer) void {
        self.out.deinit(self.alloc);
        self.posting_ids.deinit(self.alloc);
        self.covering_radii.deinit(self.alloc);
        self.measures.deinit(self.alloc);
        self.vectors.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn append(self: *Writer, posting_id: u64, covering_radius: f32, centroid: []const f32, measure: f32) !void {
        if (self.finished) return error.CentroidDirectoryWriterFinished;
        if (centroid.len != self.dims) return error.InvalidCentroidDimensions;
        try self.posting_ids.append(self.alloc, posting_id);
        errdefer _ = self.posting_ids.pop();
        try self.covering_radii.append(self.alloc, covering_radius);
        errdefer _ = self.covering_radii.pop();
        try self.measures.append(self.alloc, measure);
        errdefer _ = self.measures.pop();
        try self.vectors.appendSlice(self.alloc, centroid);
        self.posting_count = std.math.add(u64, self.posting_count, 1) catch return error.CentroidDirectoryTooLarge;
        if (self.posting_ids.items.len == self.block_size) try self.flushBlock();
    }

    fn flushBlock(self: *Writer) !void {
        const count = self.posting_ids.items.len;
        if (count == 0) return;
        std.debug.assert(self.covering_radii.items.len == count);
        std.debug.assert(self.measures.items.len == count);
        std.debug.assert(self.vectors.items.len == count * self.dims);

        try alignOutput(self.alloc, &self.out);
        const header_offset = self.out.items.len;
        try appendZeros(self.alloc, &self.out, block_header_size);
        writeU32(self.out.items, header_offset, @intCast(count));
        try self.out.appendSlice(self.alloc, std.mem.sliceAsBytes(self.posting_ids.items));
        try self.out.appendSlice(self.alloc, std.mem.sliceAsBytes(self.covering_radii.items));
        try self.out.appendSlice(self.alloc, std.mem.sliceAsBytes(self.measures.items));
        try self.out.appendSlice(self.alloc, std.mem.sliceAsBytes(self.vectors.items));
        self.block_count = std.math.add(u32, self.block_count, 1) catch return error.CentroidDirectoryTooLarge;
        self.posting_ids.clearRetainingCapacity();
        self.covering_radii.clearRetainingCapacity();
        self.measures.clearRetainingCapacity();
        self.vectors.clearRetainingCapacity();
    }

    pub fn build(self: *Writer) ![]u8 {
        if (self.finished) return error.CentroidDirectoryWriterFinished;
        try self.flushBlock();
        self.finished = true;
        @memcpy(self.out.items[0..4], &magic);
        writeU16(self.out.items, 4, version);
        self.out.items[6] = self.metric;
        writeU32(self.out.items, 8, @intCast(self.dims));
        writeU32(self.out.items, 12, @intCast(self.block_size));
        writeU64(self.out.items, 16, self.posting_count);
        writeU32(self.out.items, 24, self.block_count);
        const owned = try self.out.toOwnedSlice(self.alloc);
        self.out = .empty;
        return owned;
    }
};

pub const Block = struct {
    posting_ids: []const u64,
    covering_radii: []const f32,
    measures: []const f32,
    vectors: []const f32,
};

pub const Reader = struct {
    data: []const u8,
    dims: usize,
    metric: u8,
    block_size: usize,
    posting_count: usize,
    block_count: usize,

    pub fn init(data: []const u8) !Reader {
        if (data.len < header_size or !std.mem.eql(u8, data[0..4], &magic)) return error.InvalidCentroidDirectory;
        if (@intFromPtr(data.ptr) % @alignOf(u64) != 0) return error.MisalignedCentroidDirectory;
        if (readU16(data, 4) != version) return error.UnsupportedCentroidDirectoryVersion;
        const dims: usize = readU32(data, 8);
        const block_size: usize = readU32(data, 12);
        const posting_count = std.math.cast(usize, readU64(data, 16)) orelse return error.CentroidDirectoryTooLarge;
        const block_count: usize = readU32(data, 24);
        if (dims == 0 or block_size == 0) return error.InvalidCentroidDirectory;
        const reader: Reader = .{
            .data = data,
            .dims = dims,
            .metric = data[6],
            .block_size = block_size,
            .posting_count = posting_count,
            .block_count = block_count,
        };
        var iter = reader.blocks();
        var seen_blocks: usize = 0;
        var seen_postings: usize = 0;
        while (try iter.next()) |block| {
            seen_blocks += 1;
            seen_postings = std.math.add(usize, seen_postings, block.posting_ids.len) catch return error.InvalidCentroidDirectory;
        }
        if (seen_blocks != block_count or seen_postings != posting_count or iter.offset != data.len) return error.InvalidCentroidDirectory;
        return reader;
    }

    pub fn blocks(self: Reader) BlockIterator {
        return .{ .reader = self, .offset = header_size };
    }
};

pub const BlockIterator = struct {
    reader: Reader,
    offset: usize,
    index: usize = 0,

    pub fn next(self: *BlockIterator) !?Block {
        if (self.index == self.reader.block_count) return null;
        self.offset = std.mem.alignForward(usize, self.offset, block_alignment);
        if (self.offset > self.reader.data.len or self.reader.data.len - self.offset < block_header_size) return error.InvalidCentroidDirectory;
        const count: usize = readU32(self.reader.data, self.offset);
        if (count == 0 or count > self.reader.block_size) return error.InvalidCentroidDirectory;
        var cursor = self.offset + block_header_size;
        const ids_bytes = std.math.mul(usize, count, @sizeOf(u64)) catch return error.InvalidCentroidDirectory;
        const scalar_bytes = std.math.mul(usize, count, @sizeOf(f32)) catch return error.InvalidCentroidDirectory;
        const vector_count = std.math.mul(usize, count, self.reader.dims) catch return error.InvalidCentroidDirectory;
        const vector_bytes = std.math.mul(usize, vector_count, @sizeOf(f32)) catch return error.InvalidCentroidDirectory;
        const total = ids_bytes + scalar_bytes * 2 + vector_bytes;
        if (cursor > self.reader.data.len or total > self.reader.data.len - cursor) return error.InvalidCentroidDirectory;

        const ids_raw: []align(@alignOf(u64)) const u8 = @alignCast(self.reader.data[cursor .. cursor + ids_bytes]);
        const posting_ids = std.mem.bytesAsSlice(u64, ids_raw);
        cursor += ids_bytes;
        const radii_raw: []align(@alignOf(f32)) const u8 = @alignCast(self.reader.data[cursor .. cursor + scalar_bytes]);
        const covering_radii = std.mem.bytesAsSlice(f32, radii_raw);
        cursor += scalar_bytes;
        const measures_raw: []align(@alignOf(f32)) const u8 = @alignCast(self.reader.data[cursor .. cursor + scalar_bytes]);
        const measures = std.mem.bytesAsSlice(f32, measures_raw);
        cursor += scalar_bytes;
        const vectors_raw: []align(@alignOf(f32)) const u8 = @alignCast(self.reader.data[cursor .. cursor + vector_bytes]);
        const vectors = std.mem.bytesAsSlice(f32, vectors_raw);
        cursor += vector_bytes;

        self.offset = cursor;
        self.index += 1;
        return .{
            .posting_ids = posting_ids,
            .covering_radii = covering_radii,
            .measures = measures,
            .vectors = vectors,
        };
    }
};

test "exact centroid directory round trips aligned blocks" {
    const alloc = std.testing.allocator;
    var writer = try Writer.init(alloc, 3, 1, 2);
    defer writer.deinit();
    try writer.append(7, 0.25, &.{ 1, 2, 3 }, 3.75);
    try writer.append(9, 0.5, &.{ 4, 5, 6 }, 8.25);
    try writer.append(11, 0.75, &.{ 7, 8, 9 }, 12.5);
    const encoded = try writer.build();
    defer alloc.free(encoded);

    const reader = try Reader.init(encoded);
    try std.testing.expectEqual(@as(usize, 3), reader.dims);
    try std.testing.expectEqual(@as(usize, 3), reader.posting_count);
    try std.testing.expectEqual(@as(usize, 2), reader.block_count);
    var blocks = reader.blocks();
    const first = (try blocks.next()).?;
    try std.testing.expectEqualSlices(u64, &.{ 7, 9 }, first.posting_ids);
    try std.testing.expectEqualSlices(f32, &.{ 0.25, 0.5 }, first.covering_radii);
    try std.testing.expectEqualSlices(f32, &.{ 3.75, 8.25 }, first.measures);
    try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3, 4, 5, 6 }, first.vectors);
    const second = (try blocks.next()).?;
    try std.testing.expectEqualSlices(u64, &.{11}, second.posting_ids);
    try std.testing.expectEqualSlices(f32, &.{ 7, 8, 9 }, second.vectors);
    try std.testing.expect(try blocks.next() == null);
}

test "exact centroid directory rejects truncation" {
    const alloc = std.testing.allocator;
    var writer = try Writer.init(alloc, 2, 0, 4);
    defer writer.deinit();
    try writer.append(1, 1, &.{ 1, 2 }, 2);
    const encoded = try writer.build();
    defer alloc.free(encoded);
    try std.testing.expectError(error.InvalidCentroidDirectory, Reader.init(encoded[0 .. encoded.len - 1]));
}
