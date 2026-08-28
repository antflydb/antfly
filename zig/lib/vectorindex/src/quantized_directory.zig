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

//! Immutable, mmap-friendly RaBitQ posting payload directory.
//!
//! The outer posting segment owns durability, checksum validation, and the
//! generation lease. This codec keeps the hot fixed-width arrays naturally
//! aligned and exposes borrowed slices, avoiding a second decoded heap copy
//! for every immutable posting visited by a query.

const std = @import("std");
const proto = @import("antfly_vector").proto;

const Allocator = std.mem.Allocator;
const magic: [4]u8 = "AFQD".*;
const version: u16 = 1;
const header_size: usize = 32;
const entry_header_size: usize = 32;
const index_entry_size: usize = 20;
const entry_alignment: usize = 64;

fn appendZeros(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), count: usize) !void {
    try out.appendNTimes(alloc, 0, count);
}

fn alignOutput(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), alignment: usize) !void {
    const aligned = std.mem.alignForward(usize, out.items.len, alignment);
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

fn writeF32(bytes: []u8, offset: usize, value: f32) void {
    writeU32(bytes, offset, @bitCast(value));
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

fn readF32(bytes: []const u8, offset: usize) f32 {
    return @bitCast(readU32(bytes, offset));
}

pub const Writer = struct {
    alloc: Allocator,
    dims: usize,
    metric: u8,
    out: std.ArrayListUnmanaged(u8) = .empty,
    posting_ids: std.ArrayListUnmanaged(u64) = .empty,
    offsets: std.ArrayListUnmanaged(u64) = .empty,
    finished: bool = false,

    pub fn init(alloc: Allocator, dims: usize, metric: u8) !Writer {
        if (dims == 0 or dims > std.math.maxInt(u32)) return error.InvalidQuantizedDirectoryConfig;
        var self: Writer = .{ .alloc = alloc, .dims = dims, .metric = metric };
        errdefer self.deinit();
        try appendZeros(alloc, &self.out, header_size);
        return self;
    }

    pub fn deinit(self: *Writer) void {
        self.out.deinit(self.alloc);
        self.posting_ids.deinit(self.alloc);
        self.offsets.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn append(self: *Writer, posting_id: u64, set: *const proto.RaBitQuantizedVectorSet) !void {
        if (self.finished) return error.QuantizedDirectoryWriterFinished;
        if (posting_id == 0 or set.centroid.len != self.dims) return error.InvalidQuantizedDirectoryEntry;
        const count = set.getCount();
        const width = std.math.cast(usize, set.codes.width) orelse return error.InvalidQuantizedDirectoryEntry;
        const encoded_count = std.math.cast(i64, count) orelse return error.InvalidQuantizedDirectoryEntry;
        const code_values = std.math.mul(usize, count, width) catch return error.InvalidQuantizedDirectoryEntry;
        if (count == 0 or width == 0 or
            set.codes.count != encoded_count or
            set.codes.data.len != code_values or
            set.centroid_distances.len != count or
            set.quantized_dot_products.len != count or
            set.centroid_dot_products.len != count)
        {
            return error.InvalidQuantizedDirectoryEntry;
        }
        if (self.posting_ids.getLastOrNull()) |previous| {
            if (posting_id <= previous) return error.UnsortedQuantizedDirectory;
        }

        try alignOutput(self.alloc, &self.out, entry_alignment);
        const entry_offset = self.out.items.len;
        try self.posting_ids.append(self.alloc, posting_id);
        errdefer _ = self.posting_ids.pop();
        try self.offsets.append(self.alloc, @intCast(entry_offset));
        errdefer _ = self.offsets.pop();
        try appendZeros(self.alloc, &self.out, entry_header_size);
        writeU64(self.out.items, entry_offset, posting_id);
        writeU32(self.out.items, entry_offset + 8, @intCast(count));
        writeU32(self.out.items, entry_offset + 12, @intCast(width));
        writeF32(self.out.items, entry_offset + 16, set.centroid_norm);
        try self.out.appendSlice(self.alloc, std.mem.sliceAsBytes(set.centroid));
        try alignOutput(self.alloc, &self.out, @alignOf(u64));
        try self.out.appendSlice(self.alloc, std.mem.sliceAsBytes(set.codes.data));
        try self.out.appendSlice(self.alloc, std.mem.sliceAsBytes(set.code_counts));
        try self.out.appendSlice(self.alloc, std.mem.sliceAsBytes(set.centroid_distances));
        try self.out.appendSlice(self.alloc, std.mem.sliceAsBytes(set.quantized_dot_products));
        try self.out.appendSlice(self.alloc, std.mem.sliceAsBytes(set.centroid_dot_products));
    }

    pub fn build(self: *Writer) ![]u8 {
        if (self.finished) return error.QuantizedDirectoryWriterFinished;
        self.finished = true;
        try alignOutput(self.alloc, &self.out, entry_alignment);
        const index_offset = self.out.items.len;
        for (self.posting_ids.items, self.offsets.items, 0..) |posting_id, offset, index| {
            const end = if (index + 1 < self.offsets.items.len) self.offsets.items[index + 1] else @as(u64, @intCast(index_offset));
            const start_usize: usize = @intCast(offset);
            const end_usize: usize = @intCast(end);
            const start = self.out.items.len;
            try appendZeros(self.alloc, &self.out, index_entry_size);
            writeU64(self.out.items, start, posting_id);
            writeU64(self.out.items, start + 8, offset);
            writeU32(self.out.items, start + 16, std.hash.Crc32.hash(self.out.items[start_usize..end_usize]));
        }
        @memcpy(self.out.items[0..4], &magic);
        writeU16(self.out.items, 4, version);
        self.out.items[6] = self.metric;
        writeU32(self.out.items, 8, @intCast(self.dims));
        writeU32(self.out.items, 12, std.hash.Crc32.hash(self.out.items[index_offset..]));
        writeU64(self.out.items, 16, @intCast(self.posting_ids.items.len));
        writeU64(self.out.items, 24, @intCast(index_offset));
        const owned = try self.out.toOwnedSlice(self.alloc);
        self.out = .empty;
        return owned;
    }
};

pub const View = struct {
    metric: u8,
    centroid: []const f32,
    codes: []const u64,
    code_counts: []const u32,
    centroid_distances: []const f32,
    quantized_dot_products: []const f32,
    centroid_dot_products: []const f32,
    centroid_norm: f32,
    count: usize,
    width: usize,

    pub fn asProto(self: View) proto.RaBitQuantizedVectorSet {
        return .{
            .metric = @enumFromInt(self.metric),
            .centroid = @constCast(self.centroid),
            .codes = .{
                .count = @intCast(self.count),
                .width = @intCast(self.width),
                .data = @constCast(self.codes),
            },
            .code_counts = @constCast(self.code_counts),
            .centroid_distances = @constCast(self.centroid_distances),
            .quantized_dot_products = @constCast(self.quantized_dot_products),
            .centroid_dot_products = @constCast(self.centroid_dot_products),
            .centroid_norm = self.centroid_norm,
        };
    }
};

pub const Reader = struct {
    data: []const u8,
    dims: usize,
    metric: u8,
    posting_count: usize,
    index_offset: usize,

    pub fn init(data: []const u8) !Reader {
        if (data.len < header_size or !std.mem.eql(u8, data[0..4], &magic)) return error.InvalidQuantizedDirectory;
        if (@intFromPtr(data.ptr) % @alignOf(u64) != 0) return error.MisalignedQuantizedDirectory;
        if (readU16(data, 4) != version) return error.UnsupportedQuantizedDirectoryVersion;
        const dims: usize = readU32(data, 8);
        const posting_count = std.math.cast(usize, readU64(data, 16)) orelse return error.QuantizedDirectoryTooLarge;
        const index_offset = std.math.cast(usize, readU64(data, 24)) orelse return error.QuantizedDirectoryTooLarge;
        const index_bytes = std.math.mul(usize, posting_count, index_entry_size) catch return error.InvalidQuantizedDirectory;
        if (dims == 0 or index_offset < header_size or index_offset > data.len or index_bytes != data.len - index_offset) {
            return error.InvalidQuantizedDirectory;
        }
        if (std.hash.Crc32.hash(data[index_offset..]) != readU32(data, 12)) {
            return error.QuantizedDirectoryIndexChecksumMismatch;
        }
        var previous_id: u64 = 0;
        var previous_offset: usize = 0;
        for (0..posting_count) |index| {
            const cursor = index_offset + index * index_entry_size;
            const posting_id = readU64(data, cursor);
            const offset = std.math.cast(usize, readU64(data, cursor + 8)) orelse return error.InvalidQuantizedDirectory;
            if (posting_id == 0 or (index != 0 and posting_id <= previous_id) or
                offset < header_size or offset % entry_alignment != 0 or
                offset > index_offset -| entry_header_size or (index != 0 and offset <= previous_offset))
            {
                return error.InvalidQuantizedDirectory;
            }
            previous_id = posting_id;
            previous_offset = offset;
        }
        return .{ .data = data, .dims = dims, .metric = data[6], .posting_count = posting_count, .index_offset = index_offset };
    }

    fn indexId(self: Reader, index: usize) u64 {
        return readU64(self.data, self.index_offset + index * index_entry_size);
    }

    fn indexOffset(self: Reader, index: usize) usize {
        return @intCast(readU64(self.data, self.index_offset + index * index_entry_size + 8));
    }

    fn indexChecksum(self: Reader, index: usize) u32 {
        return readU32(self.data, self.index_offset + index * index_entry_size + 16);
    }

    fn findIndex(self: Reader, posting_id: u64) ?usize {
        var low: usize = 0;
        var high = self.posting_count;
        while (low < high) {
            const mid = low + (high - low) / 2;
            const id = self.indexId(mid);
            if (id < posting_id) low = mid + 1 else high = mid;
        }
        if (low == self.posting_count or self.indexId(low) != posting_id) return null;
        return low;
    }

    fn entryBytes(self: Reader, index: usize) ![]const u8 {
        if (index >= self.posting_count) return error.InvalidQuantizedDirectory;
        const start = self.indexOffset(index);
        const end = if (index + 1 < self.posting_count) self.indexOffset(index + 1) else self.index_offset;
        if (start > end or end > self.index_offset) return error.InvalidQuantizedDirectory;
        return self.data[start..end];
    }

    pub fn get(self: Reader, posting_id: u64) !?View {
        const index = self.findIndex(posting_id) orelse return null;
        return try self.viewAt(index);
    }

    fn viewAt(self: Reader, index: usize) !View {
        const entry = try self.entryBytes(index);
        const start = @intFromPtr(entry.ptr) - @intFromPtr(self.data.ptr);
        const end = start + entry.len;
        const posting_id = self.indexId(index);
        if (end - start < entry_header_size or readU64(self.data, start) != posting_id) {
            return error.InvalidQuantizedDirectory;
        }
        const count: usize = readU32(self.data, start + 8);
        const width: usize = readU32(self.data, start + 12);
        if (count == 0 or width == 0) return error.InvalidQuantizedDirectory;
        var cursor = start + entry_header_size;
        const centroid_bytes = std.math.mul(usize, self.dims, @sizeOf(f32)) catch return error.InvalidQuantizedDirectory;
        const code_values = std.math.mul(usize, count, width) catch return error.InvalidQuantizedDirectory;
        const code_bytes = std.math.mul(usize, code_values, @sizeOf(u64)) catch return error.InvalidQuantizedDirectory;
        const scalar_bytes = std.math.mul(usize, count, @sizeOf(f32)) catch return error.InvalidQuantizedDirectory;
        const centroid_end = std.math.add(usize, cursor, centroid_bytes) catch return error.InvalidQuantizedDirectory;
        if (centroid_end > end) return error.InvalidQuantizedDirectory;
        const centroid_raw: []align(@alignOf(f32)) const u8 = @alignCast(self.data[cursor..centroid_end]);
        const centroid = std.mem.bytesAsSlice(f32, centroid_raw);
        cursor = std.mem.alignForward(usize, centroid_end, @alignOf(u64));
        const codes_end = std.math.add(usize, cursor, code_bytes) catch return error.InvalidQuantizedDirectory;
        if (codes_end > end) return error.InvalidQuantizedDirectory;
        const codes_raw: []align(@alignOf(u64)) const u8 = @alignCast(self.data[cursor..codes_end]);
        const codes = std.mem.bytesAsSlice(u64, codes_raw);
        cursor = codes_end;
        const code_counts_end = std.math.add(usize, cursor, scalar_bytes) catch return error.InvalidQuantizedDirectory;
        const distances_end = std.math.add(usize, code_counts_end, scalar_bytes) catch return error.InvalidQuantizedDirectory;
        const dots_end = std.math.add(usize, distances_end, scalar_bytes) catch return error.InvalidQuantizedDirectory;
        const centroid_dots_end = std.math.add(usize, dots_end, scalar_bytes) catch return error.InvalidQuantizedDirectory;
        if (centroid_dots_end > end) return error.InvalidQuantizedDirectory;
        const code_counts_raw: []align(@alignOf(u32)) const u8 = @alignCast(self.data[cursor..code_counts_end]);
        const distances_raw: []align(@alignOf(f32)) const u8 = @alignCast(self.data[code_counts_end..distances_end]);
        const dots_raw: []align(@alignOf(f32)) const u8 = @alignCast(self.data[distances_end..dots_end]);
        const centroid_dots_raw: []align(@alignOf(f32)) const u8 = @alignCast(self.data[dots_end..centroid_dots_end]);
        return View{
            .metric = self.metric,
            .centroid = centroid,
            .codes = codes,
            .code_counts = std.mem.bytesAsSlice(u32, code_counts_raw),
            .centroid_distances = std.mem.bytesAsSlice(f32, distances_raw),
            .quantized_dot_products = std.mem.bytesAsSlice(f32, dots_raw),
            .centroid_dot_products = std.mem.bytesAsSlice(f32, centroid_dots_raw),
            .centroid_norm = readF32(self.data, start + 16),
            .count = count,
            .width = width,
        };
    }
};

/// Lazily authenticates each independently indexed posting exactly once. This
/// preserves mmap-lazy restart while ensuring a corrupted payload can never be
/// consumed merely because the compact directory index itself was valid.
pub const VerifiedReader = struct {
    const unknown: u8 = 0;
    const valid: u8 = 1;
    const corrupt: u8 = 2;

    alloc: Allocator,
    reader: Reader,
    verification: []std.atomic.Value(u8),

    pub fn init(alloc: Allocator, data: []const u8) !VerifiedReader {
        const reader = try Reader.init(data);
        const verification = try alloc.alloc(std.atomic.Value(u8), reader.posting_count);
        for (verification) |*state| state.* = .init(unknown);
        return .{ .alloc = alloc, .reader = reader, .verification = verification };
    }

    pub fn deinit(self: *VerifiedReader) void {
        self.alloc.free(self.verification);
        self.* = undefined;
    }

    pub fn get(self: *VerifiedReader, posting_id: u64) !?View {
        const index = self.reader.findIndex(posting_id) orelse return null;
        const state = self.verification[index].load(.acquire);
        if (state == corrupt) return error.QuantizedDirectoryChecksumMismatch;
        if (state == unknown) {
            const entry = try self.reader.entryBytes(index);
            if (std.hash.Crc32.hash(entry) != self.reader.indexChecksum(index)) {
                self.verification[index].store(corrupt, .release);
                return error.QuantizedDirectoryChecksumMismatch;
            }
            self.verification[index].store(valid, .release);
        }
        return try self.reader.viewAt(index);
    }
};

test "quantized directory round trips borrowed aligned views" {
    const alloc = std.testing.allocator;
    var writer = try Writer.init(alloc, 3, 2);
    defer writer.deinit();
    const first = proto.RaBitQuantizedVectorSet{
        .metric = .cosine,
        .centroid = @constCast(&[_]f32{ 1, 2, 3 }),
        .codes = .{ .count = 2, .width = 1, .data = @constCast(&[_]u64{ 7, 9 }) },
        .code_counts = @constCast(&[_]u32{ 2, 3 }),
        .centroid_distances = @constCast(&[_]f32{ 0.25, 0.5 }),
        .quantized_dot_products = @constCast(&[_]f32{ 1.25, 1.5 }),
        .centroid_dot_products = @constCast(&[_]f32{ 2.25, 2.5 }),
        .centroid_norm = 3.5,
    };
    try writer.append(7, &first);
    try writer.append(11, &first);
    const encoded = try writer.build();
    defer alloc.free(encoded);
    const reader = try Reader.init(encoded);
    try std.testing.expect((try reader.get(8)) == null);
    const view = (try reader.get(11)).?;
    try std.testing.expectEqualSlices(f32, first.centroid, view.centroid);
    try std.testing.expectEqualSlices(u64, first.codes.data, view.codes);
    try std.testing.expectEqualSlices(u32, first.code_counts, view.code_counts);
    try std.testing.expectEqual(first.centroid_norm, view.centroid_norm);
    const borrowed = view.asProto();
    try std.testing.expectEqualSlices(u64, first.codes.data, borrowed.codes.data);
}

test "quantized directory rejects truncation" {
    const alloc = std.testing.allocator;
    var writer = try Writer.init(alloc, 1, 0);
    defer writer.deinit();
    const set = proto.RaBitQuantizedVectorSet{
        .centroid = @constCast(&[_]f32{1}),
        .codes = .{ .count = 1, .width = 1, .data = @constCast(&[_]u64{1}) },
        .code_counts = @constCast(&[_]u32{1}),
        .centroid_distances = @constCast(&[_]f32{1}),
        .quantized_dot_products = @constCast(&[_]f32{1}),
        .centroid_dot_products = @constCast(&[_]f32{1}),
    };
    try writer.append(1, &set);
    const encoded = try writer.build();
    defer alloc.free(encoded);
    try std.testing.expectError(error.InvalidQuantizedDirectory, Reader.init(encoded[0 .. encoded.len - 1]));
}

test "quantized directory authenticates its compact index eagerly" {
    const alloc = std.testing.allocator;
    var writer = try Writer.init(alloc, 1, 0);
    defer writer.deinit();
    const set = proto.RaBitQuantizedVectorSet{
        .centroid = @constCast(&[_]f32{1}),
        .codes = .{ .count = 1, .width = 1, .data = @constCast(&[_]u64{1}) },
        .code_counts = @constCast(&[_]u32{1}),
        .centroid_distances = @constCast(&[_]f32{1}),
        .quantized_dot_products = @constCast(&[_]f32{1}),
        .centroid_dot_products = @constCast(&[_]f32{1}),
    };
    try writer.append(1, &set);
    const encoded = try writer.build();
    defer alloc.free(encoded);
    encoded[encoded.len - index_entry_size] ^= 1;
    try std.testing.expectError(error.QuantizedDirectoryIndexChecksumMismatch, Reader.init(encoded));
}

test "quantized directory verifies posting payload lazily" {
    const alloc = std.testing.allocator;
    var writer = try Writer.init(alloc, 1, 0);
    defer writer.deinit();
    const set = proto.RaBitQuantizedVectorSet{
        .centroid = @constCast(&[_]f32{1}),
        .codes = .{ .count = 1, .width = 1, .data = @constCast(&[_]u64{1}) },
        .code_counts = @constCast(&[_]u32{1}),
        .centroid_distances = @constCast(&[_]f32{1}),
        .quantized_dot_products = @constCast(&[_]f32{1}),
        .centroid_dot_products = @constCast(&[_]f32{1}),
    };
    try writer.append(1, &set);
    const encoded = try writer.build();
    defer alloc.free(encoded);
    encoded[entry_alignment + entry_header_size] ^= 0x80;
    var reader = try VerifiedReader.init(alloc, encoded);
    defer reader.deinit();
    try std.testing.expectError(error.QuantizedDirectoryChecksumMismatch, reader.get(1));
    try std.testing.expectError(error.QuantizedDirectoryChecksumMismatch, reader.get(1));
}
