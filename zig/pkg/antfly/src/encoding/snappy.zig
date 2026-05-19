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

//! Snappy block compression/decompression.
//!
//! Wire-compatible with Go's `github.com/golang/snappy` block format.
//! This is the raw block format (no framing), used by zapx for stored fields
//! and doc values compression.

const std = @import("std");
const Allocator = std.mem.Allocator;

// Tag types (lower 2 bits of tag byte)
const tag_literal: u8 = 0;
const tag_copy1: u8 = 1;
const tag_copy2: u8 = 2;
const tag_copy4: u8 = 3;

// Max encoded length overhead constants
const max_block_size: usize = 1 << 16; // 65536
const hash_table_bits: u6 = 14;
const hash_table_size: usize = 1 << hash_table_bits; // 16384

/// Decode Snappy-compressed block data.
/// Returns decompressed bytes owned by caller.
pub fn decode(alloc: Allocator, src: []const u8) ![]u8 {
    if (src.len == 0) return try alloc.alloc(u8, 0);

    // Read uncompressed length (varint preamble)
    var pos: usize = 0;
    const uncompressed_len = try readVarint(src, &pos);

    var dst = try alloc.alloc(u8, uncompressed_len);
    errdefer alloc.free(dst);

    var d: usize = 0; // destination offset

    while (pos < src.len) {
        const tag = src[pos];
        pos += 1;
        const element_type: u2 = @truncate(tag);

        switch (element_type) {
            tag_literal => {
                var length: usize = @as(usize, tag >> 2) + 1;
                if (length <= 60) {
                    // Length is inline in tag
                } else {
                    // Length follows in 1-4 bytes
                    const extra_bytes = length - 60;
                    if (pos + extra_bytes > src.len) return error.CorruptInput;
                    length = 1;
                    for (0..extra_bytes) |i| {
                        length += @as(usize, src[pos + i]) << @intCast(i * 8);
                    }
                    pos += extra_bytes;
                }
                if (pos + length > src.len) return error.CorruptInput;
                if (d + length > dst.len) return error.CorruptInput;
                @memcpy(dst[d..][0..length], src[pos..][0..length]);
                d += length;
                pos += length;
            },
            tag_copy1 => {
                // 2-byte element: tag + 1 offset byte
                if (pos >= src.len) return error.CorruptInput;
                const length: usize = @as(usize, (tag >> 2) & 0x07) + 4;
                const offset: usize = (@as(usize, tag & 0xe0) << 3) | @as(usize, src[pos]);
                pos += 1;
                if (offset == 0 or offset > d) return error.CorruptInput;
                if (d + length > dst.len) return error.CorruptInput;
                copyOverlapping(dst, d, offset, length);
                d += length;
            },
            tag_copy2 => {
                // 3-byte element: tag + 2 offset bytes
                if (pos + 2 > src.len) return error.CorruptInput;
                const length: usize = @as(usize, tag >> 2) + 1;
                const offset: usize = @as(usize, src[pos]) | (@as(usize, src[pos + 1]) << 8);
                pos += 2;
                if (offset == 0 or offset > d) return error.CorruptInput;
                if (d + length > dst.len) return error.CorruptInput;
                copyOverlapping(dst, d, offset, length);
                d += length;
            },
            tag_copy4 => {
                // 5-byte element: tag + 4 offset bytes
                if (pos + 4 > src.len) return error.CorruptInput;
                const length: usize = @as(usize, tag >> 2) + 1;
                const offset: usize = @as(usize, src[pos]) |
                    (@as(usize, src[pos + 1]) << 8) |
                    (@as(usize, src[pos + 2]) << 16) |
                    (@as(usize, src[pos + 3]) << 24);
                pos += 4;
                if (offset == 0 or offset > d) return error.CorruptInput;
                if (d + length > dst.len) return error.CorruptInput;
                copyOverlapping(dst, d, offset, length);
                d += length;
            },
        }
    }

    if (d != uncompressed_len) return error.CorruptInput;
    return dst;
}

/// Encode data using Snappy block compression.
/// Returns compressed bytes owned by caller.
pub fn encode(alloc: Allocator, src: []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    _ = try encodeInto(alloc, &out, src);
    return try out.toOwnedSlice(alloc);
}

/// Encode data using Snappy block compression into a reusable output buffer.
/// Returns the written slice, backed by `out.items`.
pub fn encodeInto(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), src: []const u8) ![]const u8 {
    out.clearRetainingCapacity();
    try out.ensureTotalCapacity(alloc, maxEncodedLen(src.len));

    // Write uncompressed length as varint preamble
    try writeVarint(alloc, out, src.len);

    if (src.len == 0) {
        return out.items;
    }

    // Hash table: maps 4-byte sequences to source positions (1-indexed, 0 = empty)
    var table: [hash_table_size]u32 = undefined;
    @memset(&table, 0);

    var s: usize = 0; // source position
    var lit_start: usize = 0; // start of current literal run

    // Process input
    while (s + 4 <= src.len) {
        // Hash current 4-byte sequence
        const h = hash4(src[s..][0..4]);

        // Check for match (table stores 1-indexed positions)
        const table_val = table[h];
        table[h] = @intCast(s + 1); // store 1-indexed

        if (table_val > 0 and blk: {
            const candidate = table_val - 1; // convert to 0-indexed
            break :blk s - candidate <= 65535 and
                std.mem.eql(u8, src[candidate..][0..4], src[s..][0..4]);
        }) {
            const candidate = table_val - 1;
            // Found match - emit pending literals first
            if (lit_start < s) {
                try emitLiteral(alloc, out, src[lit_start..s]);
            }

            // Extend match forward
            var match_len: usize = 4;
            while (s + match_len < src.len and
                src[s + match_len] == src[candidate + match_len])
            {
                match_len += 1;
            }

            const offset = s - candidate;
            try emitCopy(alloc, out, offset, match_len);
            s += match_len;
            lit_start = s;
        } else {
            s += 1;
        }
    }

    // Emit remaining literals
    if (lit_start < src.len) {
        try emitLiteral(alloc, out, src[lit_start..]);
    }

    return out.items;
}

// -- Internal helpers --

fn hash4(data: *const [4]u8) usize {
    const v = std.mem.readInt(u32, data, .little);
    return @intCast((v *% 0x1e35a7bd) >> (32 - hash_table_bits));
}

fn maxEncodedLen(src_len: usize) usize {
    if (src_len == 0) return 1;
    return src_len + (src_len / 6) + 32;
}

fn emitLiteral(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), lit: []const u8) !void {
    const n = lit.len;
    if (n == 0) return;

    if (n <= 60) {
        try out.append(alloc, @intCast((@as(u8, @intCast(n - 1)) << 2) | tag_literal));
    } else if (n <= 256) {
        try out.append(alloc, (60 << 2) | tag_literal);
        try out.append(alloc, @intCast(n - 1));
    } else if (n <= 65536) {
        try out.append(alloc, (61 << 2) | tag_literal);
        try out.append(alloc, @truncate(n - 1));
        try out.append(alloc, @truncate((n - 1) >> 8));
    } else if (n <= 16777216) {
        try out.append(alloc, (62 << 2) | tag_literal);
        try out.append(alloc, @truncate(n - 1));
        try out.append(alloc, @truncate((n - 1) >> 8));
        try out.append(alloc, @truncate((n - 1) >> 16));
    } else {
        try out.append(alloc, (63 << 2) | tag_literal);
        try out.append(alloc, @truncate(n - 1));
        try out.append(alloc, @truncate((n - 1) >> 8));
        try out.append(alloc, @truncate((n - 1) >> 16));
        try out.append(alloc, @truncate((n - 1) >> 24));
    }
    try out.appendSlice(alloc, lit);
}

fn emitCopy(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), offset: usize, length: usize) !void {
    var remaining = length;
    // Emit in chunks - copy elements can encode at most 64 bytes
    while (remaining > 0) {
        if (remaining >= 4 and remaining <= 11 and offset <= 2047) {
            // Copy1: 2-byte tag, length 4-11, offset 0-2047
            const len3: u8 = @intCast(remaining - 4);
            const off_hi: u8 = @truncate(offset >> 8);
            try out.append(alloc, (off_hi << 5) | (len3 << 2) | tag_copy1);
            try out.append(alloc, @truncate(offset));
            return;
        } else if (offset <= 65535) {
            // Copy2: 3-byte tag, length 1-64
            const chunk = @min(remaining, 64);
            const len6: u8 = @intCast(chunk - 1);
            try out.append(alloc, (len6 << 2) | tag_copy2);
            try out.append(alloc, @truncate(offset));
            try out.append(alloc, @truncate(offset >> 8));
            remaining -= chunk;
        } else {
            // Copy4: 5-byte tag, length 1-64
            const chunk = @min(remaining, 64);
            const len6: u8 = @intCast(chunk - 1);
            try out.append(alloc, (len6 << 2) | tag_copy4);
            try out.append(alloc, @truncate(offset));
            try out.append(alloc, @truncate(offset >> 8));
            try out.append(alloc, @truncate(offset >> 16));
            try out.append(alloc, @truncate(offset >> 24));
            remaining -= chunk;
        }
    }
}

fn copyOverlapping(dst: []u8, d: usize, offset: usize, length: usize) void {
    var src_pos = d - offset;
    for (0..length) |i| {
        dst[d + i] = dst[src_pos];
        src_pos += 1;
    }
}

fn readVarint(data: []const u8, pos: *usize) !usize {
    var result: usize = 0;
    var shift: std.math.Log2Int(usize) = 0;
    while (pos.* < data.len) {
        const b = data[pos.*];
        pos.* += 1;
        result |= @as(usize, b & 0x7f) << shift;
        if (b & 0x80 == 0) return result;
        shift += 7;
        if (shift >= @bitSizeOf(usize)) return error.CorruptInput;
    }
    return error.CorruptInput;
}

fn writeVarint(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: usize) !void {
    var v = value;
    while (v >= 0x80) {
        try out.append(alloc, @as(u8, @truncate(v)) | 0x80);
        v >>= 7;
    }
    try out.append(alloc, @truncate(v));
}

// ============================================================================
// Tests
// ============================================================================

test "round-trip empty" {
    const alloc = std.testing.allocator;
    const compressed = try encode(alloc, &.{});
    defer alloc.free(compressed);
    const decompressed = try decode(alloc, compressed);
    defer alloc.free(decompressed);
    try std.testing.expectEqual(@as(usize, 0), decompressed.len);
}

test "round-trip small literal" {
    const alloc = std.testing.allocator;
    const input = "Hello, Snappy!";
    const compressed = try encode(alloc, input);
    defer alloc.free(compressed);
    const decompressed = try decode(alloc, compressed);
    defer alloc.free(decompressed);
    try std.testing.expectEqualStrings(input, decompressed);
}

test "round-trip with repetition" {
    const alloc = std.testing.allocator;
    // Repetitive data should compress well
    var input: [1024]u8 = undefined;
    for (&input, 0..) |*b, i| {
        b.* = @truncate(i % 10);
    }
    const compressed = try encode(alloc, &input);
    defer alloc.free(compressed);

    // Should actually compress
    try std.testing.expect(compressed.len < input.len);

    const decompressed = try decode(alloc, compressed);
    defer alloc.free(decompressed);
    try std.testing.expectEqualSlices(u8, &input, decompressed);
}

test "round-trip larger data" {
    const alloc = std.testing.allocator;
    // Generate data with some structure
    var input: [8192]u8 = undefined;
    for (&input, 0..) |*b, i| {
        b.* = @truncate(i *% 7 +% (i / 256));
    }
    const compressed = try encode(alloc, &input);
    defer alloc.free(compressed);
    const decompressed = try decode(alloc, compressed);
    defer alloc.free(decompressed);
    try std.testing.expectEqualSlices(u8, &input, decompressed);
}

test "encodeInto reuses output buffer across calls" {
    const alloc = std.testing.allocator;
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);

    const first = try encodeInto(alloc, &out, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    try std.testing.expect(first.len > 0);
    const capacity_after_first = out.capacity;

    const second = try encodeInto(alloc, &out, "bbbbbbbbbbbbbbbb");
    try std.testing.expect(second.len > 0);
    try std.testing.expect(out.capacity == capacity_after_first);

    const decoded = try decode(alloc, second);
    defer alloc.free(decoded);
    try std.testing.expectEqualStrings("bbbbbbbbbbbbbbbb", decoded);
}

test "round-trip all zeros" {
    const alloc = std.testing.allocator;
    const input = [_]u8{0} ** 4096;
    const compressed = try encode(alloc, &input);
    defer alloc.free(compressed);
    // Should compress significantly (4096 -> under 300 bytes)
    try std.testing.expect(compressed.len < 300);
    const decompressed = try decode(alloc, compressed);
    defer alloc.free(decompressed);
    try std.testing.expectEqualSlices(u8, &input, decompressed);
}

test "decode corrupt input" {
    const alloc = std.testing.allocator;
    // Varint says 100 bytes but no data follows
    const bad = [_]u8{ 100, 0 };
    try std.testing.expectError(error.CorruptInput, decode(alloc, &bad));
}

test "decode Go snappy output" {
    const alloc = std.testing.allocator;

    // test1: "Hello, Snappy!" encoded by Go snappy
    const go_encoded1 = &[_]u8{ 0x0e, 0x34, 0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x2c, 0x20, 0x53, 0x6e, 0x61, 0x70, 0x70, 0x79, 0x21 };
    const decoded1 = try decode(alloc, go_encoded1);
    defer alloc.free(decoded1);
    try std.testing.expectEqualStrings("Hello, Snappy!", decoded1);

    // test2: 100 bytes of [0,1,2,...,9,0,1,...] encoded by Go snappy
    const go_encoded2 = &[_]u8{ 0x64, 0x24, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0xfe, 0x0a, 0x00, 0x66, 0x0a, 0x00 };
    const decoded2 = try decode(alloc, go_encoded2);
    defer alloc.free(decoded2);
    try std.testing.expectEqual(@as(usize, 100), decoded2.len);
    for (decoded2, 0..) |b, i| {
        try std.testing.expectEqual(@as(u8, @intCast(i % 10)), b);
    }

    // test3: 64 zeros encoded by Go snappy
    const go_encoded3 = &[_]u8{ 0x40, 0x00, 0x00, 0xfa, 0x01, 0x00 };
    const decoded3 = try decode(alloc, go_encoded3);
    defer alloc.free(decoded3);
    try std.testing.expectEqual(@as(usize, 64), decoded3.len);
    for (decoded3) |b| {
        try std.testing.expectEqual(@as(u8, 0), b);
    }
}

test "literal lengths > 60" {
    const alloc = std.testing.allocator;
    // 200 bytes of non-repeating data (no matches possible)
    var input: [200]u8 = undefined;
    for (&input, 0..) |*b, i| {
        b.* = @truncate(i *% 251 +% 37);
    }
    const compressed = try encode(alloc, &input);
    defer alloc.free(compressed);
    const decompressed = try decode(alloc, compressed);
    defer alloc.free(decompressed);
    try std.testing.expectEqualSlices(u8, &input, decompressed);
}
