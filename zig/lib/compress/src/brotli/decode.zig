// Native Zig decoder adapted from akeating/zig-parquet, commit
// 38dec45c665037f2092db1464676afb8406a90d4. Decoder only; no encoder or C backend.
// Antfly changes: Zig 0.17 API, bounded output, malformed block validation.
// SPDX-License-Identifier: MIT
// MIT License
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//
// Copyright (c) 2009, 2010, 2013-2016 by the Brotli Authors.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

//! Native Brotli decompression (pure Zig)
//!
//! Implements RFC 7932 Brotli compressed data format.
//! Native streaming Brotli with bounded history and shared raw dictionaries.
//! Reference: https://www.rfc-editor.org/rfc/rfc7932

const std = @import("std");
const shared = @import("shared_dictionary.zig");
pub const SharedDictionary = shared.Dictionary;
pub const DictionaryFormat = shared.Format;

pub const Error = shared.Error || error{
    CompressionError,
    DecompressionError,
    OutOfMemory,
    InvalidSize,
    ReadFailed,
    WriteFailed,
    WindowTooLarge,
};

// =========================================================================
// Bit Reader (for decoder) — LSB-first
// =========================================================================

const BitReader = struct {
    input: *std.Io.Reader,
    bits: u128 = 0,
    available: u8 = 0,
    pos: usize = 0,
    failure: ?Error = null,
    fn ensure(self: *BitReader, n: u8) Error!void {
        if (self.failure) |err| return err;
        while (self.available < n) {
            const byte = self.input.takeByte() catch |err| {
                if (err == error.EndOfStream) return error.DecompressionError;
                self.failure = error.ReadFailed;
                return error.ReadFailed;
            };
            self.bits |= @as(u128, byte) << @intCast(self.available);
            self.available += 8;
        }
    }
    fn bitsAvailable(self: *BitReader) usize {
        self.ensure(16) catch {};
        return self.available;
    }
    fn readBits(self: *BitReader, n: u5) Error!u32 {
        const value = try self.peekBits(n);
        self.dropBits(n);
        return value;
    }
    fn readBitsWide(self: *BitReader, n: u7) Error!u32 {
        if (n > 32) return error.DecompressionError;
        return @intCast(try self.readBits64(@intCast(n)));
    }
    fn readBits64(self: *BitReader, n: u6) Error!u64 {
        try self.ensure(n);
        const mask = (@as(u128, 1) << n) - 1;
        const value: u64 = @intCast(self.bits & mask);
        self.dropBits(n);
        return value;
    }
    fn peekBits(self: *BitReader, n: u5) Error!u32 {
        try self.ensure(n);
        return @intCast(self.bits & ((@as(u128, 1) << n) - 1));
    }
    fn dropBits(self: *BitReader, n: u6) void {
        std.debug.assert(n <= self.available);
        self.bits >>= n;
        self.available -= n;
        self.pos += n;
    }
    fn alignToByte(self: *BitReader) Error!void {
        const n: u3 = @intCast((8 - self.pos % 8) % 8);
        try self.ensure(n);
        self.dropBits(n);
    }
    fn readByte(self: *BitReader) Error!u8 {
        try self.alignToByte();
        return @intCast(try self.readBits(8));
    }
};

// =========================================================================
// Huffman Decoding Tables
// =========================================================================

const HuffmanEntry = struct {
    bits: u8,
    value: u16,
};

const PRIMARY_TABLE_BITS: u4 = 8;
const PRIMARY_TABLE_SIZE: usize = 1 << PRIMARY_TABLE_BITS;
const MAX_HUFFMAN_TABLE_SIZE: usize = PRIMARY_TABLE_SIZE + (1 << 15); // worst case

const HuffmanTable = struct {
    entries: []HuffmanEntry,
    allocator: std.mem.Allocator,

    fn deinit(self: *HuffmanTable) void {
        self.allocator.free(self.entries);
    }

    fn lookup(self: *const HuffmanTable, reader: *BitReader) Error!u16 {
        // Peek up to 8 bits for primary table index
        const available = reader.bitsAvailable();
        const peek_bits: u5 = if (available >= PRIMARY_TABLE_BITS) PRIMARY_TABLE_BITS else @intCast(available);
        const idx = if (peek_bits > 0) try reader.peekBits(peek_bits) else 0;
        const primary_idx = if (peek_bits < PRIMARY_TABLE_BITS) idx else idx;
        if (primary_idx >= self.entries.len) return error.DecompressionError;

        const entry = self.entries[primary_idx];

        if (entry.bits <= PRIMARY_TABLE_BITS) {
            if (entry.bits <= peek_bits) {
                reader.dropBits(@intCast(entry.bits));
            } else {
                return error.DecompressionError;
            }
            return entry.value;
        }

        // Secondary table lookup: entry.bits = secondary_table_bits + PRIMARY_TABLE_BITS,
        // entry.value = absolute offset to secondary table start.
        reader.dropBits(PRIMARY_TABLE_BITS);
        const secondary_table_bits: u5 = @intCast(entry.bits - PRIMARY_TABLE_BITS);
        const secondary_idx = try reader.peekBits(secondary_table_bits);
        const table_offset = entry.value;
        const final_idx = table_offset + secondary_idx;
        if (final_idx >= self.entries.len) return error.DecompressionError;
        const secondary_entry = self.entries[final_idx];
        // Drop only the actual code length's secondary bits, not the full table width
        reader.dropBits(@intCast(secondary_entry.bits));
        return secondary_entry.value;
    }
};

fn buildHuffmanTable(allocator: std.mem.Allocator, code_lengths: []const u8, alphabet_size: u16) Error!HuffmanTable {
    // Count code lengths
    var bl_count: [16]u16 = @splat(0);
    for (code_lengths) |cl| {
        if (cl > 15) return error.DecompressionError;
        bl_count[cl] += 1;
    }
    bl_count[0] = 0;

    // Compute next_code
    var next_code: [16]u32 = @splat(0);
    var code: u32 = 0;
    for (1..16) |bits| {
        code = (code + bl_count[bits - 1]) << 1;
        next_code[bits] = code;
    }

    // Determine max code length
    var max_bits: u8 = 0;
    for (code_lengths) |cl| {
        if (cl > max_bits) max_bits = cl;
    }
    if (max_bits == 0) {
        // All zero code lengths — single symbol tables
        const entries = allocator.alloc(HuffmanEntry, PRIMARY_TABLE_SIZE) catch return error.OutOfMemory;
        for (entries) |*e| {
            e.* = .{ .bits = 0, .value = 0 };
        }
        return .{ .entries = entries, .allocator = allocator };
    }

    // Count non-zero code lengths
    var num_nonzero: u16 = 0;
    var single_sym: u16 = 0;
    for (code_lengths, 0..) |cl, sym| {
        if (cl > 0) {
            num_nonzero += 1;
            single_sym = @intCast(sym);
        }
    }

    if (num_nonzero == 1) {
        // Single symbol — fill entire table with it, consuming 0 bits
        const entries = allocator.alloc(HuffmanEntry, PRIMARY_TABLE_SIZE) catch return error.OutOfMemory;
        for (entries) |*e| {
            e.* = .{ .bits = 0, .value = single_sym };
        }
        return .{ .entries = entries, .allocator = allocator };
    }

    // Compute per-primary-key secondary table bit widths
    var secondary_table_bits: [PRIMARY_TABLE_SIZE]u8 = @splat(0);
    var table_size: usize = PRIMARY_TABLE_SIZE;
    if (max_bits > PRIMARY_TABLE_BITS) {
        var temp_codes: [16]u32 = next_code;
        for (code_lengths) |cl| {
            if (cl > PRIMARY_TABLE_BITS) {
                const c2 = temp_codes[cl];
                temp_codes[cl] += 1;
                const reversed = reverseBits(c2, cl);
                const primary = reversed & (PRIMARY_TABLE_SIZE - 1);
                const sec_bits: u8 = @intCast(cl - PRIMARY_TABLE_BITS);
                if (sec_bits > secondary_table_bits[primary]) {
                    secondary_table_bits[primary] = sec_bits;
                }
            }
        }
        for (secondary_table_bits) |sb| {
            if (sb > 0) table_size += @as(usize, 1) << @intCast(sb);
        }
    }

    const entries = allocator.alloc(HuffmanEntry, table_size) catch return error.OutOfMemory;
    errdefer allocator.free(entries);
    for (entries) |*e| {
        e.* = .{ .bits = 0, .value = 0 };
    }

    // Build secondary table offset map
    var secondary_offsets: [PRIMARY_TABLE_SIZE]u16 = @splat(0);
    if (max_bits > PRIMARY_TABLE_BITS) {
        var offset: u16 = @intCast(PRIMARY_TABLE_SIZE);
        for (0..PRIMARY_TABLE_SIZE) |i| {
            secondary_offsets[i] = offset;
            if (secondary_table_bits[i] > 0) {
                offset += @as(u16, 1) << @intCast(secondary_table_bits[i]);
            }
        }
    }

    // Determine effective table bits (reduced if max_bits < PRIMARY_TABLE_BITS)
    const effective_table_bits: u8 = @intCast(@min(max_bits, PRIMARY_TABLE_BITS));
    const effective_table_size: usize = @as(usize, 1) << @intCast(effective_table_bits);

    // Fill primary table entries
    for (code_lengths, 0..) |cl, sym| {
        if (cl == 0) continue;
        const c2 = next_code[cl];
        next_code[cl] += 1;
        const reversed = reverseBits(c2, cl);

        if (cl <= PRIMARY_TABLE_BITS) {
            // Fill all aliased entries in the effective table
            const step = @as(usize, 1) << @intCast(cl);
            var idx: usize = reversed;
            while (idx < effective_table_size) : (idx += step) {
                entries[idx] = .{ .bits = @intCast(cl), .value = @intCast(sym) };
            }
        } else {
            // Secondary table entry
            const primary = reversed & (PRIMARY_TABLE_SIZE - 1);
            const stb = secondary_table_bits[primary];
            // Primary entry: bits = secondary table width + PRIMARY_TABLE_BITS,
            // value = absolute offset to secondary table start
            entries[primary] = .{ .bits = @intCast(stb + PRIMARY_TABLE_BITS), .value = secondary_offsets[primary] };

            // Replicate this symbol across the secondary table
            const sym_sec_bits: u8 = @intCast(cl - PRIMARY_TABLE_BITS);
            const secondary_idx = reversed >> PRIMARY_TABLE_BITS;
            const step = @as(usize, 1) << @intCast(sym_sec_bits);
            const base = secondary_offsets[primary];
            const sec_table_size = @as(usize, 1) << @intCast(stb);
            var idx: usize = secondary_idx;
            while (idx < sec_table_size) : (idx += step) {
                const final_idx = base + idx;
                if (final_idx < entries.len) {
                    entries[final_idx] = .{ .bits = @intCast(sym_sec_bits), .value = @intCast(sym) };
                }
            }
        }
    }

    // Replicate reduced table to fill the full PRIMARY_TABLE_SIZE
    if (effective_table_size < PRIMARY_TABLE_SIZE) {
        var size = effective_table_size;
        while (size < PRIMARY_TABLE_SIZE) {
            @memcpy(entries[size..][0..size], entries[0..size]);
            size <<= 1;
        }
    }

    _ = alphabet_size;

    return .{ .entries = entries, .allocator = allocator };
}

fn reverseBits(val: u32, nbits: anytype) usize {
    const n: u5 = @intCast(nbits);
    var result: usize = 0;
    var v = val;
    for (0..n) |_| {
        result = (result << 1) | @as(usize, v & 1);
        v >>= 1;
    }
    return result;
}

fn buildSimpleHuffmanTable(allocator: std.mem.Allocator, symbols: []const u16, nsym: u8) Error!HuffmanTable {
    const entries = allocator.alloc(HuffmanEntry, PRIMARY_TABLE_SIZE) catch return error.OutOfMemory;
    errdefer allocator.free(entries);
    for (entries) |*e| {
        e.* = .{ .bits = 0, .value = 0 };
    }

    switch (nsym) {
        1 => {
            for (entries) |*e| {
                e.* = .{ .bits = 0, .value = symbols[0] };
            }
        },
        2 => {
            // 1-bit code: smaller symbol gets bit 0, larger gets bit 1 (matching C)
            const s0 = if (symbols[0] < symbols[1]) symbols[0] else symbols[1];
            const s1 = if (symbols[0] < symbols[1]) symbols[1] else symbols[0];
            var i: usize = 0;
            while (i < PRIMARY_TABLE_SIZE) : (i += 1) {
                if (i & 1 == 0) {
                    entries[i] = .{ .bits = 1, .value = s0 };
                } else {
                    entries[i] = .{ .bits = 1, .value = s1 };
                }
            }
        },
        3 => {
            // symbols[0] has 1-bit code, symbols[1] and symbols[2] have 2-bit codes
            // Sort symbols[1] and symbols[2] so smaller gets first 2-bit code (matching C)
            const s1 = if (symbols[1] < symbols[2]) symbols[1] else symbols[2];
            const s2 = if (symbols[1] < symbols[2]) symbols[2] else symbols[1];
            var i: usize = 0;
            while (i < PRIMARY_TABLE_SIZE) : (i += 1) {
                if (i & 1 == 0) {
                    entries[i] = .{ .bits = 1, .value = symbols[0] };
                } else if (i & 3 == 1) {
                    entries[i] = .{ .bits = 2, .value = s1 };
                } else {
                    entries[i] = .{ .bits = 2, .value = s2 };
                }
            }
        },
        4 => {
            // 4 symbols, all 2-bit codes (tree_select=0)
            // Canonical codes: 00→sym[0], 01→sym[1], 10→sym[2], 11→sym[3]
            // LSB-first reversed: 00→sym[0], 10→sym[1], 01→sym[2], 11→sym[3]
            // So table indices: 0→sym[0], 1→sym[2], 2→sym[1], 3→sym[3]
            var i: usize = 0;
            while (i < PRIMARY_TABLE_SIZE) : (i += 1) {
                const idx2 = i & 3;
                const sym_idx: usize = switch (idx2) {
                    0 => 0,
                    1 => 2,
                    2 => 1,
                    3 => 3,
                    else => unreachable,
                };
                entries[i] = .{ .bits = 2, .value = symbols[sym_idx] };
            }
        },
        else => return error.DecompressionError,
    }

    return .{ .entries = entries, .allocator = allocator };
}

// =========================================================================
// Context Lookup Table (RFC 7932 Section 7.1)
// =========================================================================

// Context modes
const CONTEXT_LSB6: u2 = 0;
const CONTEXT_MSB6: u2 = 1;
const CONTEXT_UTF8: u2 = 2;
const CONTEXT_SIGNED: u2 = 3;

// Context lookup table — exact copy of _kBrotliContextLookupTable from the C reference.
// 4 modes x 512 entries (256 for p1, 256 for p2). Context ID = table[mode*512 + p1] | table[mode*512 + 256 + p2].
const kContextLookup = [2048]u8{
    // CONTEXT_LSB6, p1 part
    0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15,
    16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
    32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47,
    48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63,
    0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15,
    16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
    32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47,
    48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63,
    0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15,
    16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
    32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47,
    48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63,
    0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15,
    16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
    32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47,
    48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63,
    // CONTEXT_LSB6, p2 part (all zeros)
    0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
    0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
    0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
    0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
    0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
    0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
    0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
    0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
    0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
    0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
    0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
    0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
    0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
    0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
    0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
    0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
    // CONTEXT_MSB6, p1 part
    0,  0,  0,  0,  1,  1,  1,  1,  2,  2,  2,  2,  3,  3,  3,  3,
    4,  4,  4,  4,  5,  5,  5,  5,  6,  6,  6,  6,  7,  7,  7,  7,
    8,  8,  8,  8,  9,  9,  9,  9,  10, 10, 10, 10, 11, 11, 11, 11,
    12, 12, 12, 12, 13, 13, 13, 13, 14, 14, 14, 14, 15, 15, 15, 15,
    16, 16, 16, 16, 17, 17, 17, 17, 18, 18, 18, 18, 19, 19, 19, 19,
    20, 20, 20, 20, 21, 21, 21, 21, 22, 22, 22, 22, 23, 23, 23, 23,
    24, 24, 24, 24, 25, 25, 25, 25, 26, 26, 26, 26, 27, 27, 27, 27,
    28, 28, 28, 28, 29, 29, 29, 29, 30, 30, 30, 30, 31, 31, 31, 31,
    32, 32, 32, 32, 33, 33, 33, 33, 34, 34, 34, 34, 35, 35, 35, 35,
    36, 36, 36, 36, 37, 37, 37, 37, 38, 38, 38, 38, 39, 39, 39, 39,
    40, 40, 40, 40, 41, 41, 41, 41, 42, 42, 42, 42, 43, 43, 43, 43,
    44, 44, 44, 44, 45, 45, 45, 45, 46, 46, 46, 46, 47, 47, 47, 47,
    48, 48, 48, 48, 49, 49, 49, 49, 50, 50, 50, 50, 51, 51, 51, 51,
    52, 52, 52, 52, 53, 53, 53, 53, 54, 54, 54, 54, 55, 55, 55, 55,
    56, 56, 56, 56, 57, 57, 57, 57, 58, 58, 58, 58, 59, 59, 59, 59,
    60, 60, 60, 60, 61, 61, 61, 61, 62, 62, 62, 62, 63, 63, 63, 63,
    // CONTEXT_MSB6, p2 part (all zeros)
    0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
    0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
    0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
    0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
    0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
    0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
    0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
    0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
    0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
    0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
    0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
    0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
    0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
    0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
    0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
    0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
    // CONTEXT_UTF8, p1 part
    0,  0,  0,  0,  0,  0,  0,  0,  0,  4,  4,  0,  0,  4,  0,  0,
    0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
    8,  12, 16, 12, 12, 20, 12, 16, 24, 28, 12, 12, 32, 12, 36, 12,
    44, 44, 44, 44, 44, 44, 44, 44, 44, 44, 32, 32, 24, 40, 28, 12,
    12, 48, 52, 52, 52, 48, 52, 52, 52, 48, 52, 52, 52, 52, 52, 48,
    52, 52, 52, 52, 52, 48, 52, 52, 52, 52, 52, 24, 12, 28, 12, 12,
    12, 56, 60, 60, 60, 56, 60, 60, 60, 56, 60, 60, 60, 60, 60, 56,
    60, 60, 60, 60, 60, 56, 60, 60, 60, 60, 60, 24, 12, 28, 12, 0,
    // UTF8 continuation byte range
    0,  1,  0,  1,  0,  1,  0,  1,  0,  1,  0,  1,  0,  1,  0,  1,
    0,  1,  0,  1,  0,  1,  0,  1,  0,  1,  0,  1,  0,  1,  0,  1,
    0,  1,  0,  1,  0,  1,  0,  1,  0,  1,  0,  1,  0,  1,  0,  1,
    0,  1,  0,  1,  0,  1,  0,  1,  0,  1,  0,  1,  0,  1,  0,  1,
    // UTF8 lead byte range
    2,  3,  2,  3,  2,  3,  2,  3,  2,  3,  2,  3,  2,  3,  2,  3,
    2,  3,  2,  3,  2,  3,  2,  3,  2,  3,  2,  3,  2,  3,  2,  3,
    2,  3,  2,  3,  2,  3,  2,  3,  2,  3,  2,  3,  2,  3,  2,  3,
    2,  3,  2,  3,  2,  3,  2,  3,  2,  3,  2,  3,  2,  3,  2,  3,
    // CONTEXT_UTF8, p2 part
    // ASCII range
    0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
    0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
    0,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,
    2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  1,  1,  1,  1,  1,  1,
    1,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,
    2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  1,  1,  1,  1,  1,
    1,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,
    3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  1,  1,  1,  1,  0,
    // UTF8 continuation byte range
    0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
    0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
    0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
    0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
    0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
    // UTF8 lead byte range
    0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
    2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,
    2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,
    // CONTEXT_SIGNED, p1 part
    0,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,
    16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16,
    16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16,
    16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16,
    24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24,
    24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24,
    24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24,
    24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24,
    32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32,
    32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32,
    32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32,
    32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32,
    40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40,
    40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40,
    40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40,
    48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 56,
    // CONTEXT_SIGNED, p2 part
    0,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,
    2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,
    2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,
    2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,
    3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,
    3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,
    3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,
    3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,
    4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,
    4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,
    4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,
    4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,
    5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,
    5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,
    5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,
    6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  7,
};

fn getContextId(mode: u2, p1: u8, p2: u8) u8 {
    const base: usize = @as(usize, mode) * 512;
    const v1 = kContextLookup[base + p1];
    const v2 = kContextLookup[base + 256 + p2];
    return v1 | v2;
}

// =========================================================================
// Command Lookup Table (comptime)
// =========================================================================

const CmdLutElement = struct {
    insert_len_extra_bits: u8,
    copy_len_extra_bits: u8,
    distance_code: i8,
    context: u8,
    insert_len_offset: u16,
    copy_len_offset: u16,
};

const kInsertLengthExtraBits = [24]u8{ 0, 0, 0, 0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 7, 8, 9, 10, 12, 14, 24 };
const kCopyLengthExtraBits = [24]u8{ 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 7, 8, 9, 10, 24 };

const kInsertLengthOffsets = blk: {
    var offsets: [24]u16 = undefined;
    offsets[0] = 0;
    for (1..24) |i| {
        offsets[i] = offsets[i - 1] + (@as(u16, 1) << @intCast(kInsertLengthExtraBits[i - 1]));
    }
    break :blk offsets;
};

const kCopyLengthOffsets = blk: {
    var offsets: [24]u16 = undefined;
    offsets[0] = 2;
    for (1..24) |i| {
        offsets[i] = offsets[i - 1] + (@as(u16, 1) << @intCast(kCopyLengthExtraBits[i - 1]));
    }
    break :blk offsets;
};

const kCellPos = [11]u8{ 0, 1, 0, 1, 8, 9, 2, 16, 10, 17, 18 };

const kCmdLut: [704]CmdLutElement = blk: {
    var lut: [704]CmdLutElement = undefined;
    for (0..704) |symbol| {
        const cell_idx = symbol >> 6;
        const cell_pos = kCellPos[cell_idx];
        const copy_code: u8 = ((@as(u8, cell_pos) << 3) & 0x18) | @as(u8, @intCast(symbol & 7));
        const insert_code: u8 = (@as(u8, cell_pos) & 0x18) | @as(u8, @intCast((symbol >> 3) & 7));
        lut[symbol] = .{
            .insert_len_extra_bits = kInsertLengthExtraBits[insert_code],
            .copy_len_extra_bits = kCopyLengthExtraBits[copy_code],
            .distance_code = if (cell_idx >= 2) -1 else 0,
            .context = if (kCopyLengthOffsets[copy_code] > 4) 3 else @intCast(kCopyLengthOffsets[copy_code] - 2),
            .insert_len_offset = kInsertLengthOffsets[insert_code],
            .copy_len_offset = kCopyLengthOffsets[copy_code],
        };
    }
    break :blk lut;
};

// =========================================================================
// Block Length Prefix Code
// =========================================================================

const BlockLengthPrefixEntry = struct {
    offset: u32,
    nbits: u8,
};

const kBlockLengthPrefixCode = [26]BlockLengthPrefixEntry{
    .{ .offset = 1, .nbits = 2 },     .{ .offset = 5, .nbits = 2 },      .{ .offset = 9, .nbits = 2 },
    .{ .offset = 13, .nbits = 2 },    .{ .offset = 17, .nbits = 3 },     .{ .offset = 25, .nbits = 3 },
    .{ .offset = 33, .nbits = 3 },    .{ .offset = 41, .nbits = 3 },     .{ .offset = 49, .nbits = 4 },
    .{ .offset = 65, .nbits = 4 },    .{ .offset = 81, .nbits = 4 },     .{ .offset = 97, .nbits = 4 },
    .{ .offset = 113, .nbits = 5 },   .{ .offset = 145, .nbits = 5 },    .{ .offset = 177, .nbits = 5 },
    .{ .offset = 209, .nbits = 5 },   .{ .offset = 241, .nbits = 6 },    .{ .offset = 305, .nbits = 6 },
    .{ .offset = 369, .nbits = 7 },   .{ .offset = 497, .nbits = 8 },    .{ .offset = 753, .nbits = 9 },
    .{ .offset = 1265, .nbits = 10 }, .{ .offset = 2289, .nbits = 11 },  .{ .offset = 4337, .nbits = 12 },
    .{ .offset = 8433, .nbits = 13 }, .{ .offset = 16625, .nbits = 24 },
};

fn readBlockLength(reader: *BitReader, table: *const HuffmanTable) Error!u32 {
    const code = try table.lookup(reader);
    if (code >= 26) return error.DecompressionError;
    const entry = kBlockLengthPrefixCode[code];
    if (entry.nbits == 0) return entry.offset;
    const extra = try reader.readBitsWide(@intCast(entry.nbits));
    return entry.offset + extra;
}

// =========================================================================
// Distance Short Codes
// =========================================================================

// Distance short code lookup table, matching C TakeDistanceFromRingBuffer.
// For codes 0-3: index into dist_rb relative to dist_rb_idx.
// For codes 4-15: index_delta and value delta packed from C's 0x605142 magic.
// These are computed at comptime to match the C reference exactly.
const DistShortCode = struct { index_offset: i8, delta: i8 };
const dist_short_codes: [16]DistShortCode = blk: {
    var codes: [16]DistShortCode = undefined;
    // Codes 0-3: access dist_rb[(dist_rb_idx + offset) & 3] with offset derived from C
    // Code 0: offset = 3 (last distance)
    // Code 1: offset = 2 (second-to-last)
    // Code 2: offset = 1 (third-to-last)
    // Code 3: offset = 0 (fourth-to-last)
    codes[0] = .{ .index_offset = 3, .delta = 0 };
    codes[1] = .{ .index_offset = 2, .delta = 0 };
    codes[2] = .{ .index_offset = 1, .delta = 0 };
    codes[3] = .{ .index_offset = 0, .delta = 0 };
    // Codes 4-9: index_delta=3, delta from 0x605142 nibbles
    // C: base = code - 4, delta = ((0x605142 >> (4*base)) & 0xF) - 3
    for (4..10) |c| {
        const base = c - 4;
        const nibble: i8 = @intCast((0x605142 >> @intCast(4 * base)) & 0xF);
        codes[c] = .{ .index_offset = 3, .delta = nibble - 3 };
    }
    // Codes 10-15: index_delta=2, delta from 0x605142 nibbles
    // C: base = code - 10, delta = ((0x605142 >> (4*base)) & 0xF) - 3
    for (10..16) |c| {
        const base = c - 10;
        const nibble: i8 = @intCast((0x605142 >> @intCast(4 * base)) & 0xF);
        codes[c] = .{ .index_offset = 2, .delta = nibble - 3 };
    }
    break :blk codes;
};

// =========================================================================
// Static Dictionary
// =========================================================================

const brotli_dictionary = @embedFile("brotli_dictionary.bin");

const kSizeBitsByLength = [32]u8{
    0, 0, 0, 0, 10, 10, 11, 11, 10, 10, 10, 10, 10, 9, 9, 8,
    7, 7, 8, 7, 7,  6,  5,  5,  5,  0,  0,  0,  0,  0, 0, 0,
};

const kOffsetsByLength = [32]u32{
    0,      0,      0,      0,      0,      4096,   9216,   21504,
    35840,  44032,  53248,  63488,  74752,  87040,  93696,  100864,
    104704, 106752, 108928, 113536, 115968, 118528, 119872, 121280,
    122016, 122784, 122784, 122784, 122784, 122784, 122784, 122784,
};

// =========================================================================
// Transforms
// =========================================================================

const NUM_TRANSFORMS: usize = 121;

const kTransformsData = [NUM_TRANSFORMS * 3]u8{
    49, 0,  49, 49, 0,  0,  0,  0,  0,  49, 12, 49, 49, 10, 0,
    49, 0,  47, 0,  0,  49, 4,  0,  0,  49, 0,  3,  49, 10, 49,
    49, 0,  6,  49, 13, 49, 49, 1,  49, 1,  0,  0,  49, 0,  1,
    0,  10, 0,  49, 0,  7,  49, 0,  9,  48, 0,  0,  49, 0,  8,
    49, 0,  5,  49, 0,  10, 49, 0,  11, 49, 3,  49, 49, 0,  13,
    49, 0,  14, 49, 14, 49, 49, 2,  49, 49, 0,  15, 49, 0,  16,
    0,  10, 49, 49, 0,  12, 5,  0,  49, 0,  0,  1,  49, 15, 49,
    49, 0,  18, 49, 0,  17, 49, 0,  19, 49, 0,  20, 49, 16, 49,
    49, 17, 49, 47, 0,  49, 49, 4,  49, 49, 0,  22, 49, 11, 49,
    49, 0,  23, 49, 0,  24, 49, 0,  25, 49, 7,  49, 49, 1,  26,
    49, 0,  27, 49, 0,  28, 0,  0,  12, 49, 0,  29, 49, 20, 49,
    49, 18, 49, 49, 6,  49, 49, 0,  21, 49, 10, 1,  49, 8,  49,
    49, 0,  31, 49, 0,  32, 47, 0,  3,  49, 5,  49, 49, 9,  49,
    0,  10, 1,  49, 10, 8,  5,  0,  21, 49, 11, 0,  49, 10, 10,
    49, 0,  30, 0,  0,  5,  35, 0,  49, 47, 0,  2,  49, 10, 17,
    49, 0,  36, 49, 0,  33, 5,  0,  0,  49, 10, 21, 49, 10, 5,
    49, 0,  37, 0,  0,  30, 49, 0,  38, 0,  11, 0,  49, 0,  39,
    0,  11, 49, 49, 0,  34, 49, 11, 8,  49, 10, 12, 0,  0,  21,
    49, 0,  40, 0,  10, 12, 49, 0,  41, 49, 0,  42, 49, 11, 17,
    49, 0,  43, 0,  10, 5,  49, 11, 10, 0,  0,  34, 49, 10, 33,
    49, 0,  44, 49, 11, 5,  45, 0,  49, 0,  0,  33, 49, 10, 30,
    49, 11, 30, 49, 0,  46, 49, 11, 1,  49, 10, 34, 0,  10, 33,
    0,  11, 30, 0,  11, 1,  49, 11, 33, 49, 11, 21, 49, 11, 12,
    0,  11, 5,  49, 11, 34, 0,  11, 12, 0,  10, 30, 0,  11, 34,
    0,  10, 34,
};

// Prefix/suffix string table (length-prefixed format)
const kPrefixSuffix = "\x01 \x02, \x10 of the \x04 of \x02s \x01.\x05 and \x04 in \x01\"\x04 to \x02\">\x01\n\x02. \x01]\x05 for \x03 a \x06 that \x01\'\x06 with \x06 from \x04 by \x01(\x06. The \x04 on \x04 as \x04 is \x04ing \x02\n\t\x01:\x03ed \x02=\"\x04 at \x03ly \x01,\x02=\'\x05.com/\x07. This \x05 not \x03er \x03al \x04ful \x04ive \x05less \x04est \x04ize \x02\xc2\xa0\x04ous \x05 the \x02e \x00";

const kPrefixSuffixMap = [50]u8{
    0x00, 0x02, 0x05, 0x0E, 0x13, 0x16, 0x18, 0x1E, 0x23, 0x25,
    0x2A, 0x2D, 0x2F, 0x32, 0x34, 0x3A, 0x3E, 0x45, 0x47, 0x4E,
    0x55, 0x5A, 0x5C, 0x63, 0x68, 0x6D, 0x72, 0x77, 0x7A, 0x7C,
    0x80, 0x83, 0x88, 0x8C, 0x8E, 0x91, 0x97, 0x9F, 0xA5, 0xA9,
    0xAD, 0xB2, 0xB7, 0xBD, 0xC2, 0xC7, 0xCA, 0xCF, 0xD5, 0xD8,
};

const builtin_words: shared.WordList = .{
    .data = brotli_dictionary,
    .bits = kSizeBitsByLength,
    .offsets = blk: {
        var offsets: [32]usize = undefined;
        for (kOffsetsByLength, 0..) |offset, i| offsets[i] = offset;
        break :blk offsets;
    },
};
const builtin_string_offsets = blk: {
    var offsets: [kPrefixSuffixMap.len]u16 = undefined;
    for (kPrefixSuffixMap, 0..) |offset, i| offsets[i] = offset;
    break :blk offsets;
};
const builtin_transforms: shared.TransformList = .{
    .strings = kPrefixSuffix,
    .offsets = &builtin_string_offsets,
    .triplets = &kTransformsData,
    .google_order = true,
};

fn decodeVarLenUint8(reader: *BitReader) Error!u32 {
    const bit = try reader.readBits(1);
    if (bit == 0) return 0;
    const n = try reader.readBits(3);
    if (n == 0) return 1;
    const nbits: u5 = @intCast(n);
    const val = try reader.readBits(nbits);
    return (@as(u32, 1) << nbits) + val;
}

fn decodeContextMap(allocator: std.mem.Allocator, reader: *BitReader, context_map_size: usize, num_htrees_out: *u32) Error![]u8 {
    const num_htrees = (try decodeVarLenUint8(reader)) + 1;
    num_htrees_out.* = num_htrees;

    const context_map = allocator.alloc(u8, context_map_size) catch return error.OutOfMemory;
    errdefer allocator.free(context_map);

    if (num_htrees == 1) {
        @memset(context_map, 0);
        return context_map;
    }

    const use_rle = try reader.readBits(1);
    var max_rle_prefix: u32 = 0;
    if (use_rle != 0) {
        max_rle_prefix = (try reader.readBits(4)) + 1;
    }

    const alphabet_size: u16 = @intCast(num_htrees + max_rle_prefix);
    var table = try readHuffmanCode(allocator, reader, alphabet_size);
    defer table.deinit();

    var i: usize = 0;
    while (i < context_map_size) {
        const code = try table.lookup(reader);
        if (code == 0) {
            context_map[i] = 0;
            i += 1;
        } else if (code <= max_rle_prefix) {
            // RLE of zeros: base is (1 << code), plus `code` extra bits
            const rle_bits: u5 = @intCast(code);
            const rle_extra = try reader.readBits(rle_bits);
            const rle_len = (@as(u32, 1) << rle_bits) + rle_extra;
            if (rle_len > context_map_size - i) return error.DecompressionError;
            var j: u32 = 0;
            while (j < rle_len and i < context_map_size) : (j += 1) {
                context_map[i] = 0;
                i += 1;
            }
        } else {
            const htree_val = code - max_rle_prefix;
            if (htree_val > std.math.maxInt(u8)) return error.DecompressionError;
            context_map[i] = @intCast(htree_val);
            i += 1;
        }
    }

    // Inverse move-to-front
    const imtf_bit = try reader.readBits(1);
    if (imtf_bit != 0) {
        inverseMoveToFront(context_map);
    }

    return context_map;
}

fn inverseMoveToFront(context_map: []u8) void {
    var mtf: [256]u8 = undefined;
    for (0..256) |i| {
        mtf[i] = @intCast(i);
    }
    for (context_map) |*v| {
        const idx = v.*;
        const val = mtf[idx];
        v.* = val;
        // Move val to front
        var j: usize = idx;
        while (j > 0) : (j -= 1) {
            mtf[j] = mtf[j - 1];
        }
        mtf[0] = val;
    }
}

// =========================================================================
// Huffman Code Reading
// =========================================================================

const kCodeLengthCodeOrder = [18]u8{ 1, 2, 3, 4, 0, 5, 17, 6, 16, 7, 8, 9, 10, 11, 12, 13, 14, 15 };

fn readHuffmanCode(allocator: std.mem.Allocator, reader: *BitReader, alphabet_size: u16) Error!HuffmanTable {
    const simple = try reader.readBits(2);
    if (simple == 1) {
        // Simple prefix code
        const nsym_minus1 = try reader.readBits(2);
        const nsym: u8 = @intCast(nsym_minus1 + 1);
        var symbols: [4]u16 = @splat(0);

        // Number of bits per symbol: Log2Floor(alphabet_size - 1), matching C reference
        // C's Log2Floor counts right-shifts until 0, i.e. ceil(log2(x+1)) = number of bits needed
        const sym_bits: u5 = blk: {
            var n: u16 = alphabet_size - 1;
            var bits: u5 = 0;
            while (n != 0) : (n >>= 1) {
                bits += 1;
            }
            break :blk bits;
        };

        for (0..nsym) |i| {
            if (sym_bits <= 25) {
                symbols[i] = @intCast(try reader.readBits(@intCast(sym_bits)));
            } else {
                symbols[i] = @intCast(try reader.readBitsWide(@intCast(sym_bits)));
            }
        }

        for (symbols[0..nsym], 0..) |symbol, i| {
            if (symbol >= alphabet_size) return error.DecompressionError;
            for (symbols[0..i]) |earlier| if (earlier == symbol) return error.DecompressionError;
        }
        if (nsym == 4) {
            // Read tree-select bit
            const tree_select = try reader.readBits(1);
            if (tree_select != 0) {
                // Shape 1,2,3,3: only sort the two 3-bit symbols (indices 2,3).
                // Symbols 0 and 1 keep their stream order (matching C case 4).
                if (symbols[2] > symbols[3]) {
                    const tmp = symbols[2];
                    symbols[2] = symbols[3];
                    symbols[3] = tmp;
                }
                return buildSimple4SymbolHuffmanTableDeep(allocator, &symbols);
            }
            // Shape 2,2,2,2: full sort all 4 symbols (matching C case 3)
            if (symbols[0] > symbols[1]) {
                const tmp = symbols[0];
                symbols[0] = symbols[1];
                symbols[1] = tmp;
            }
            if (symbols[2] > symbols[3]) {
                const tmp = symbols[2];
                symbols[2] = symbols[3];
                symbols[3] = tmp;
            }
            if (symbols[0] > symbols[2]) {
                const tmp = symbols[0];
                symbols[0] = symbols[2];
                symbols[2] = tmp;
            }
            if (symbols[1] > symbols[3]) {
                const tmp = symbols[1];
                symbols[1] = symbols[3];
                symbols[3] = tmp;
            }
            if (symbols[1] > symbols[2]) {
                const tmp = symbols[1];
                symbols[1] = symbols[2];
                symbols[2] = tmp;
            }
        }

        return buildSimpleHuffmanTable(allocator, &symbols, nsym);
    } else {
        // Complex prefix code — `simple` value (0, 2, or 3) is the HSKIP
        return readComplexHuffmanCodeWithSkip(allocator, reader, alphabet_size, simple);
    }
}

fn buildSimple4SymbolHuffmanTableDeep(allocator: std.mem.Allocator, symbols: *const [4]u16) Error!HuffmanTable {
    // Tree shape: symbol[0]=1bit, symbol[1]=2bits, symbol[2]=3bits, symbol[3]=3bits
    const entries = allocator.alloc(HuffmanEntry, PRIMARY_TABLE_SIZE) catch return error.OutOfMemory;
    errdefer allocator.free(entries);

    var i: usize = 0;
    while (i < PRIMARY_TABLE_SIZE) : (i += 1) {
        if (i & 1 == 0) {
            entries[i] = .{ .bits = 1, .value = symbols[0] };
        } else if (i & 3 == 1) {
            entries[i] = .{ .bits = 2, .value = symbols[1] };
        } else if (i & 7 == 3) {
            entries[i] = .{ .bits = 3, .value = symbols[2] };
        } else {
            entries[i] = .{ .bits = 3, .value = symbols[3] };
        }
    }

    return .{ .entries = entries, .allocator = allocator };
}

fn readComplexHuffmanCodeWithSkip(allocator: std.mem.Allocator, reader: *BitReader, alphabet_size: u16, hskip: u32) Error!HuffmanTable {
    // Read code-length code lengths
    var cl_code_lengths: [18]u8 = @splat(0);
    var space: i32 = 32;
    var num_codes: usize = 0;

    var i: usize = hskip;
    while (i < 18) : (i += 1) {
        const cl_order_idx = kCodeLengthCodeOrder[i];

        // Read code length using variable-length prefix (max 5 bits)
        const v = try readCodeLengthCodeLength(reader);
        cl_code_lengths[cl_order_idx] = v;
        if (v != 0) {
            num_codes += 1;
            space -= @as(i32, 32) >> @intCast(v);
        }
        if (space <= 0) break;
    }

    // Build Huffman table for code lengths
    var cl_table = try buildHuffmanTable(allocator, &cl_code_lengths, 18);
    defer cl_table.deinit();

    // Decode the actual code lengths using the same repeat accumulation as C reference
    const code_lengths = allocator.alloc(u8, alphabet_size) catch return error.OutOfMemory;
    defer allocator.free(code_lengths);
    @memset(code_lengths, 0);

    var sym_idx: usize = 0;
    var prev_code_len: u8 = 8;
    var repeat: u32 = 0;
    var repeat_code_len: u8 = 0;
    var code_space: i32 = 32768;
    if (space != 0 and num_codes != 1) return error.DecompressionError;

    while (sym_idx < alphabet_size and code_space > 0) {
        const code = try cl_table.lookup(reader);
        if (code < 16) {
            // Literal code length — reset repeat
            repeat = 0;
            code_lengths[sym_idx] = @intCast(code);
            if (code != 0) {
                prev_code_len = @intCast(code);
                code_space -= @as(i32, 32768) >> @intCast(code);
            }
            sym_idx += 1;
        } else {
            // code == 16: repeat prev_code_len, extra_bits=2
            // code == 17: repeat zero, extra_bits=3
            const new_len: u8 = if (code == 16) prev_code_len else 0;
            const extra_bits: u5 = if (code == 16) 2 else 3;
            const repeat_delta = try reader.readBits(extra_bits);

            // Reset repeat if switching repeat type
            if (repeat_code_len != new_len) {
                repeat = 0;
                repeat_code_len = new_len;
            }

            // Exponential repeat accumulation (matches C ProcessRepeatedCodeLength)
            const old_repeat = repeat;
            if (repeat > 0) {
                repeat = (repeat - 2) << extra_bits;
            }
            repeat += repeat_delta + 3;
            var delta = repeat - old_repeat;

            if (delta > alphabet_size - sym_idx) return error.DecompressionError;

            if (new_len != 0) {
                code_space -= @as(i32, @intCast(delta)) * (@as(i32, 32768) >> @intCast(new_len));
            }

            while (delta > 0 and sym_idx < alphabet_size) : (delta -= 1) {
                code_lengths[sym_idx] = new_len;
                sym_idx += 1;
            }
        }
    }

    if (code_space != 0) return error.DecompressionError;
    return buildHuffmanTable(allocator, code_lengths, alphabet_size);
}

fn readCodeLengthCodeLength(reader: *BitReader) Error!u8 {
    // RFC 7932 section 3.5: static prefix code for code-length code lengths.
    // 4-bit lookup table from the reference implementation:
    //   kCodeLengthPrefixLength = {2,2,2,3,2,2,2,4,2,2,2,3,2,2,2,4}
    //   kCodeLengthPrefixValue  = {0,4,3,2,0,4,3,1,0,4,3,2,0,4,3,5}
    // Peek 4 bits, lookup value and consumed bits.
    const prefix_lengths = [16]u8{ 2, 2, 2, 3, 2, 2, 2, 4, 2, 2, 2, 3, 2, 2, 2, 4 };
    const prefix_values = [16]u8{ 0, 4, 3, 2, 0, 4, 3, 1, 0, 4, 3, 2, 0, 4, 3, 5 };

    // Read only the number of bits required by this prefix. A valid terminal
    // code can occupy fewer than four bits at the physical end of input.
    const lo = try reader.peekBits(2);
    if (lo != 3) {
        reader.dropBits(2);
        return prefix_values[lo];
    }
    const three = try reader.peekBits(3);
    if (three != 7) {
        reader.dropBits(3);
        return prefix_values[three];
    }
    const idx: u4 = @intCast(try reader.peekBits(4));
    reader.dropBits(@intCast(prefix_lengths[idx]));
    return prefix_values[idx];
}

// =========================================================================
// Window bits decoding
// =========================================================================

const Window = struct { bits: u6, large: bool = false };
fn decodeWindowBits(reader: *BitReader) Error!Window {
    if (try reader.readBits(1) == 0) return .{ .bits = 16 };
    const n = try reader.readBits(3);
    if (n != 0) return .{ .bits = @intCast(17 + n) };
    const n2 = try reader.readBits(3);
    if (n2 == 0) return .{ .bits = 17 };
    if (n2 != 1) return .{ .bits = @intCast(8 + n2) };
    if (try reader.readBits(1) != 0) return error.DecompressionError;
    const bits = try reader.readBits(6);
    if (bits < 10 or bits > 62) return error.DecompressionError;
    return .{ .bits = @intCast(bits), .large = true };
}

// =========================================================================
// Decoder
// =========================================================================

pub const Options = struct {
    max_window_bytes: usize = 64 * 1024 * 1024,
    raw_dictionary: []const u8 = &.{},
    dictionary_format: DictionaryFormat = .auto,
    prepared_dictionary: ?*const SharedDictionary = null,
};
pub fn decompress(allocator: std.mem.Allocator, compressed: []const u8, limit: usize) Error![]u8 {
    var input = std.Io.Reader.fixed(compressed);
    var output: std.Io.Writer.Allocating = .init(allocator);
    errdefer output.deinit();
    decodeTo(allocator, &input, &output.writer, limit) catch |err| return if (err == error.WriteFailed) error.OutOfMemory else err;
    return output.toOwnedSlice() catch return error.OutOfMemory;
}
pub fn decodeTo(allocator: std.mem.Allocator, input: *std.Io.Reader, output: *std.Io.Writer, limit: usize) Error!void {
    return decodeToOptions(allocator, input, output, limit, .{});
}
pub fn decodeToOptions(allocator: std.mem.Allocator, input: *std.Io.Reader, output: *std.Io.Writer, limit: usize, options: Options) Error!void {
    var local_dictionary: ?SharedDictionary = null;
    defer if (local_dictionary) |*dictionary| dictionary.deinit();
    const dictionary = options.prepared_dictionary orelse blk: {
        local_dictionary = try SharedDictionary.init(allocator, options.raw_dictionary, options.dictionary_format);
        break :blk &local_dictionary.?;
    };
    var reader = BitReader{ .input = input };
    const window = try decodeWindowBits(&reader);
    if (window.bits >= @bitSizeOf(usize)) return error.WindowTooLarge;
    const window_size = @as(usize, 1) << @intCast(window.bits);
    if (window_size > options.max_window_bytes) return error.WindowTooLarge;
    var ring_buf = allocator.alloc(u8, @min(window_size, 65536)) catch return error.OutOfMemory;
    defer allocator.free(ring_buf);

    var ring_pos: usize = 0;
    var dist_rb = [4]usize{ 16, 15, 11, 4 }; // distance ring buffer, initialized per spec
    var dist_rb_idx: usize = 0; // rolling index, matches C reference

    // Main meta-block loop
    var is_last = false;
    while (!is_last) {
        // Read ISLAST
        is_last = (try reader.readBits(1)) != 0;

        if (is_last) {
            // Check ISEMPTY
            const is_empty = (try reader.readBits(1)) != 0;
            if (is_empty) {
                break; // Done
            }
        }

        // Read MLEN (meta-block length)
        const mlen = try readMetaBlockLength(&reader);
        const meta_block_len = mlen.len;

        if (mlen.is_metadata) {
            // Metadata block: skip
            try reader.alignToByte();
            var skip: usize = 0;
            while (skip < meta_block_len) : (skip += 1) {
                _ = try reader.readByte();
            }
            continue;
        }

        if (meta_block_len > limit - ring_pos) return error.InvalidSize;

        // ISUNCOMPRESSED bit is only present when ISLAST=0
        var is_uncompressed = false;
        if (!is_last) {
            is_uncompressed = (try reader.readBits(1)) != 0;
        }

        if (is_uncompressed) {
            // Uncompressed meta-block
            try reader.alignToByte();
            for (0..meta_block_len) |_| {
                const byte = try reader.readByte();
                try emitByte(allocator, output, &ring_buf, &ring_pos, window_size, byte);
            }
            continue;
        }

        // Compressed meta-block
        // Read block type / count info for 3 categories
        var lit_block_types: u32 = undefined;
        var lit_block_type_table: ?HuffmanTable = null;
        var lit_block_count_table: ?HuffmanTable = null;
        var lit_block_count: u32 = undefined;
        var lit_block_type: u32 = 0;
        var lit_prev_block_type: u32 = 0;
        defer if (lit_block_type_table) |*t| t.deinit();
        defer if (lit_block_count_table) |*t| t.deinit();

        var cmd_block_types: u32 = undefined;
        var cmd_block_type_table: ?HuffmanTable = null;
        var cmd_block_count_table: ?HuffmanTable = null;
        var cmd_block_count: u32 = undefined;
        var cmd_block_type: u32 = 0;
        var cmd_prev_block_type: u32 = 0;
        defer if (cmd_block_type_table) |*t| t.deinit();
        defer if (cmd_block_count_table) |*t| t.deinit();

        var dist_block_types: u32 = undefined;
        var dist_block_type_table: ?HuffmanTable = null;
        var dist_block_count_table: ?HuffmanTable = null;
        var dist_block_count: u32 = undefined;
        var dist_block_type: u32 = 0;
        var dist_prev_block_type: u32 = 0;
        defer if (dist_block_type_table) |*t| t.deinit();
        defer if (dist_block_count_table) |*t| t.deinit();

        // Literal block types
        lit_block_types = (try decodeVarLenUint8(&reader)) + 1;
        if (lit_block_types >= 2) {
            lit_block_type_table = try readHuffmanCode(allocator, &reader, @intCast(lit_block_types + 2));
            lit_block_count_table = try readHuffmanCode(allocator, &reader, 26);
            lit_block_count = try readBlockLength(&reader, &lit_block_count_table.?);
        } else {
            lit_block_count = @intCast(meta_block_len);
        }

        // Command block types
        cmd_block_types = (try decodeVarLenUint8(&reader)) + 1;
        if (cmd_block_types >= 2) {
            cmd_block_type_table = try readHuffmanCode(allocator, &reader, @intCast(cmd_block_types + 2));
            cmd_block_count_table = try readHuffmanCode(allocator, &reader, 26);
            cmd_block_count = try readBlockLength(&reader, &cmd_block_count_table.?);
        } else {
            cmd_block_count = @intCast(meta_block_len);
        }

        // Distance block types
        dist_block_types = (try decodeVarLenUint8(&reader)) + 1;
        if (dist_block_types >= 2) {
            dist_block_type_table = try readHuffmanCode(allocator, &reader, @intCast(dist_block_types + 2));
            dist_block_count_table = try readHuffmanCode(allocator, &reader, 26);
            dist_block_count = try readBlockLength(&reader, &dist_block_count_table.?);
        } else {
            dist_block_count = @intCast(meta_block_len);
        }

        // Read NPOSTFIX and NDIRECT
        const npostfix = try reader.readBits(2);
        const ndirect_raw = try reader.readBits(4);
        const ndirect = ndirect_raw << @intCast(npostfix);

        // Context modes for literal block types
        const context_modes = allocator.alloc(u2, lit_block_types) catch return error.OutOfMemory;
        defer allocator.free(context_modes);
        for (0..lit_block_types) |ci| {
            context_modes[ci] = @intCast(try reader.readBits(2));
        }

        // Context maps
        const lit_context_map_size = lit_block_types * 64;
        var num_lit_htrees: u32 = 0;
        const lit_context_map = try decodeContextMap(allocator, &reader, lit_context_map_size, &num_lit_htrees);
        defer allocator.free(lit_context_map);

        const dist_context_map_size = dist_block_types * 4;
        var num_dist_htrees: u32 = 0;
        const dist_context_map = try decodeContextMap(allocator, &reader, dist_context_map_size, &num_dist_htrees);
        defer allocator.free(dist_context_map);

        // Read Huffman code groups
        // Literal trees
        const lit_htrees = allocator.alloc(HuffmanTable, num_lit_htrees) catch return error.OutOfMemory;
        var lit_htrees_init: usize = 0;
        defer {
            for (0..lit_htrees_init) |hi| {
                lit_htrees[hi].deinit();
            }
            allocator.free(lit_htrees);
        }
        for (0..num_lit_htrees) |hi| {
            lit_htrees[hi] = try readHuffmanCode(allocator, &reader, 256);
            lit_htrees_init = hi + 1;
        }

        // Command trees
        const cmd_htrees = allocator.alloc(HuffmanTable, cmd_block_types) catch return error.OutOfMemory;
        var cmd_htrees_init: usize = 0;
        defer {
            for (0..cmd_htrees_init) |hi| {
                cmd_htrees[hi].deinit();
            }
            allocator.free(cmd_htrees);
        }
        for (0..cmd_block_types) |hi| {
            cmd_htrees[hi] = try readHuffmanCode(allocator, &reader, 704);
            cmd_htrees_init = hi + 1;
        }

        // Distance trees
        const num_dist_codes: u16 = @intCast(16 + ndirect + (@as(u32, if (window.large) 124 else 48) << @as(u5, @intCast(npostfix))));
        const dist_htrees = allocator.alloc(HuffmanTable, num_dist_htrees) catch return error.OutOfMemory;
        var dist_htrees_init: usize = 0;
        defer {
            for (0..dist_htrees_init) |hi| {
                dist_htrees[hi].deinit();
            }
            allocator.free(dist_htrees);
        }
        for (0..num_dist_htrees) |hi| {
            dist_htrees[hi] = try readHuffmanCode(allocator, &reader, num_dist_codes);
            dist_htrees_init = hi + 1;
        }

        // Command loop
        var meta_bytes_remaining: usize = meta_block_len;
        while (meta_bytes_remaining > 0) {
            // Check/switch command block type
            if (cmd_block_count == 0 and cmd_block_types >= 2) {
                const new_type = try cmd_block_type_table.?.lookup(&reader);
                cmd_block_count = try readBlockLength(&reader, &cmd_block_count_table.?);
                if (new_type == 0) {
                    // Repeat previous type
                    const tmp = cmd_block_type;
                    cmd_block_type = cmd_prev_block_type;
                    cmd_prev_block_type = tmp;
                } else if (new_type == 1) {
                    // Next type
                    cmd_prev_block_type = cmd_block_type;
                    cmd_block_type = (cmd_block_type + 1) % cmd_block_types;
                } else {
                    if (new_type - 2 >= cmd_block_types) return error.DecompressionError;
                    cmd_prev_block_type = cmd_block_type;
                    cmd_block_type = new_type - 2;
                }
            }
            cmd_block_count -|= 1;

            // Read command
            const cmd_code = try cmd_htrees[cmd_block_type].lookup(&reader);
            if (cmd_code >= 704) return error.DecompressionError;
            const cmd = kCmdLut[cmd_code];

            // Calculate insert length
            var insert_len = @as(u32, cmd.insert_len_offset);
            if (cmd.insert_len_extra_bits > 0) {
                const extra = try reader.readBitsWide(@intCast(cmd.insert_len_extra_bits));
                insert_len += extra;
            }

            // Calculate copy length
            var copy_len = @as(u32, cmd.copy_len_offset);
            if (cmd.copy_len_extra_bits > 0) {
                const extra = try reader.readBitsWide(@intCast(cmd.copy_len_extra_bits));
                copy_len += extra;
            }

            // Reject command runs that cross the declared meta-block.
            if (insert_len > meta_bytes_remaining) return error.DecompressionError;
            // Emit literals
            for (0..insert_len) |_| {
                // Check/switch literal block type
                if (lit_block_count == 0 and lit_block_types >= 2) {
                    const new_type = try lit_block_type_table.?.lookup(&reader);
                    lit_block_count = try readBlockLength(&reader, &lit_block_count_table.?);
                    if (new_type == 0) {
                        const tmp = lit_block_type;
                        lit_block_type = lit_prev_block_type;
                        lit_prev_block_type = tmp;
                    } else if (new_type == 1) {
                        lit_prev_block_type = lit_block_type;
                        lit_block_type = (lit_block_type + 1) % lit_block_types;
                    } else {
                        if (new_type - 2 >= lit_block_types) return error.DecompressionError;
                        lit_prev_block_type = lit_block_type;
                        lit_block_type = new_type - 2;
                    }
                }
                lit_block_count -|= 1;

                // Context for literal
                const p1: u8 = if (ring_pos > 0) ring_buf[(ring_pos - 1) % window_size] else 0;
                const p2: u8 = if (ring_pos > 1) ring_buf[(ring_pos - 2) % window_size] else 0;
                const context_id = getContextId(context_modes[lit_block_type], p1, p2);
                const cm_idx = lit_block_type * 64 + context_id;
                const htree_idx = if (cm_idx < lit_context_map.len) lit_context_map[cm_idx] else 0;
                if (htree_idx >= num_lit_htrees) return error.DecompressionError;
                const literal = try lit_htrees[htree_idx].lookup(&reader);

                const byte: u8 = @intCast(literal & 0xff);
                try emitByte(allocator, output, &ring_buf, &ring_pos, window_size, byte);
                meta_bytes_remaining -|= 1;
            }

            if (meta_bytes_remaining == 0) break;

            // Resolve distance
            var distance: usize = 0;
            const distance_code = cmd.distance_code;

            // Save dist_rb_idx before distance resolution; dictionary references
            // restore it (C compensates with distance_context instead of updating).
            const saved_dist_rb_idx = dist_rb_idx;

            if (distance_code < 0) {
                // Use distance code from stream
                // Check/switch distance block type
                if (dist_block_count == 0 and dist_block_types >= 2) {
                    const new_type = try dist_block_type_table.?.lookup(&reader);
                    dist_block_count = try readBlockLength(&reader, &dist_block_count_table.?);
                    if (new_type == 0) {
                        const tmp = dist_block_type;
                        dist_block_type = dist_prev_block_type;
                        dist_prev_block_type = tmp;
                    } else if (new_type == 1) {
                        dist_prev_block_type = dist_block_type;
                        dist_block_type = (dist_block_type + 1) % dist_block_types;
                    } else {
                        if (new_type - 2 >= dist_block_types) return error.DecompressionError;
                        dist_prev_block_type = dist_block_type;
                        dist_block_type = new_type - 2;
                    }
                }
                dist_block_count -|= 1;

                // Distance context
                const dist_context = cmd.context;
                const dcm_idx = dist_block_type * 4 + dist_context;
                const dist_htree_idx = if (dcm_idx < dist_context_map.len) dist_context_map[dcm_idx] else 0;
                if (dist_htree_idx >= num_dist_htrees) return error.DecompressionError;
                const dist_code = try dist_htrees[dist_htree_idx].lookup(&reader);

                distance = try resolveDistance(dist_code, &dist_rb, &dist_rb_idx, npostfix, ndirect, &reader);
            } else {
                // Implicit distance: use last distance without reading from stream
                // Match C: --dist_rb_idx then read dist_rb[dist_rb_idx & 3]
                dist_rb_idx -%= 1;
                distance = dist_rb[dist_rb_idx & 3];
            }

            // Copy from ring buffer or dictionary
            const max_distance = @min(ring_pos, window_size - 16);
            if (distance <= max_distance) {
                // Update distance ring buffer (C line 2304-2305, only for normal copies)
                dist_rb[dist_rb_idx & 3] = distance;
                dist_rb_idx +%= 1;
                // Copy from ring buffer
                if (copy_len > meta_bytes_remaining) return error.DecompressionError;
                for (0..copy_len) |_| {
                    const src_pos = (ring_pos -% distance) % window_size;
                    const byte = ring_buf[src_pos];
                    try emitByte(allocator, output, &ring_buf, &ring_pos, window_size, byte);
                    meta_bytes_remaining -|= 1;
                }
            } else if (distance - max_distance <= dictionary.lz77.len) {
                if (copy_len > meta_bytes_remaining) return error.DecompressionError;
                dist_rb[dist_rb_idx & 3] = distance;
                dist_rb_idx +%= 1;
                const address = dictionary.lz77.len - (distance - max_distance);
                const regular_start = ring_pos - max_distance;
                for (0..copy_len) |i| {
                    const source = address + i;
                    const byte = if (source < dictionary.lz77.len) dictionary.lz77[source] else ring_buf[(regular_start + source - dictionary.lz77.len) % window_size];
                    try emitByte(allocator, output, &ring_buf, &ring_pos, window_size, byte);
                    meta_bytes_remaining -= 1;
                }
            } else {
                // Static dictionary reference — restore dist_rb_idx
                // (C compensates via distance_context; no ring buffer write)
                dist_rb_idx = saved_dist_rb_idx;
                const dict_distance = distance - max_distance - dictionary.lz77.len - 1;
                const p1: u8 = if (ring_pos > 0) ring_buf[(ring_pos - 1) % window_size] else 0;
                const p2: u8 = if (ring_pos > 1) ring_buf[(ring_pos - 2) % window_size] else 0;
                const context = getContextId(context_modes[lit_block_type], p1, p2);
                const resolved = try dictionary.resolve(copy_len, dict_distance, @intCast(context), &builtin_words, &builtin_transforms);
                var transformed: [541]u8 = undefined;
                const bytes = try resolved.transform.apply(resolved.word, &transformed);
                if (bytes.len > meta_bytes_remaining) return error.DecompressionError;
                for (bytes) |byte| {
                    try emitByte(allocator, output, &ring_buf, &ring_pos, window_size, byte);
                    meta_bytes_remaining -= 1;
                }
            }
        }
    }

    while (reader.pos % 8 != 0) {
        if (try reader.readBits(1) != 0) return error.DecompressionError;
    }
    if (reader.available != 0) return error.DecompressionError;
    _ = input.peekByte() catch |err| switch (err) {
        error.EndOfStream => return,
        else => return error.ReadFailed,
    };
    return error.DecompressionError;
}
fn emitByte(a: std.mem.Allocator, output: *std.Io.Writer, ring: *[]u8, position: *usize, window: usize, byte: u8) Error!void {
    if (ring.len < window and position.* == ring.len) ring.* = a.realloc(ring.*, @min(window, ring.len * 2)) catch return error.OutOfMemory;
    try output.writeByte(byte);
    ring.*[position.* % window] = byte;
    position.* += 1;
}

fn resolveDistance(dist_code: u16, dist_rb: *[4]usize, dist_rb_idx: *usize, npostfix: u32, ndirect: u32, reader: *BitReader) Error!usize {
    if (dist_code < 16) {
        // Short code — lookup from ring buffer using rolling index
        const sc = dist_short_codes[dist_code];
        const rb_idx = (dist_rb_idx.* +% @as(usize, @intCast(sc.index_offset))) & 3;
        const base = dist_rb[rb_idx];
        const result = @as(i64, @intCast(base)) + @as(i64, sc.delta);
        if (result <= 0) {
            return 0x7FFFFFFF;
        }
        // Match C's TakeDistanceFromRingBuffer: for code 0, decrement dist_rb_idx
        // so the caller's unconditional ring buffer update becomes a no-op.
        // C uses: dist_rb_idx -= (1 >> distance_code), which is 1 for code 0, 0 otherwise.
        if (dist_code == 0) {
            dist_rb_idx.* -%= 1;
        }
        return @intCast(result);
    } else if (dist_code < 16 + ndirect) {
        // Direct distance
        return dist_code - 16 + 1;
    } else {
        // Distance with extra bits
        const code = dist_code - 16 - ndirect;
        const postfix_mask = (@as(u32, 1) << @intCast(npostfix)) - 1;
        const hcode = code >> @intCast(npostfix);
        const lcode = code & postfix_mask;
        const bit_count = 1 + (hcode >> 1);
        if (bit_count > 62 or bit_count + npostfix > 62) return error.DecompressionError;
        const nbits: u6 = @intCast(bit_count);
        const offset2: u128 = ((@as(u128, 2) + (hcode & 1)) << nbits) - 4;
        const extra = try reader.readBits64(nbits);
        const distance = ((offset2 + extra) << @intCast(npostfix)) + lcode + ndirect + 1;
        if (distance > std.math.maxInt(i64) - 3) return error.DecompressionError;
        return std.math.cast(usize, distance) orelse error.DecompressionError;
    }
}

const MetaBlockLength = struct {
    len: usize,
    is_uncompressed: bool,
    is_metadata: bool,
};

fn readMetaBlockLength(reader: *BitReader) Error!MetaBlockLength {
    // Read MNIBBLES
    const mnibbles_raw = try reader.readBits(2);
    if (mnibbles_raw == 3) {
        // MNIBBLES=0 means metadata or empty block
        const reserved = try reader.readBits(1);
        if (reserved != 0) return error.DecompressionError;
        const mskipbytes = try reader.readBits(2);
        if (mskipbytes == 0) {
            return .{ .len = 0, .is_uncompressed = false, .is_metadata = true };
        }
        var metadata_len: usize = 0;
        for (0..mskipbytes) |i| {
            const byte = try reader.readBits(8);
            metadata_len |= @as(usize, byte) << @intCast(i * 8);
        }
        metadata_len += 1;
        return .{ .len = metadata_len, .is_uncompressed = false, .is_metadata = true };
    }

    const mnibbles: usize = mnibbles_raw + 4;
    var mlen: usize = 0;
    for (0..mnibbles) |i| {
        const nibble = try reader.readBits(4);
        mlen |= @as(usize, nibble) << @intCast(i * 4);
    }
    mlen += 1;

    // Check ISUNCOMPRESSED
    // NOTE: ISUNCOMPRESSED is only present for non-last meta-blocks,
    // but the caller handles ISLAST separately. Actually per spec,
    // ISUNCOMPRESSED bit is read after MLEN only if ISLAST is false.
    // We need more context. Let me check: in the main loop, when ISLAST=1
    // we've already handled ISEMPTY. For ISLAST=0, we read ISUNCOMPRESSED.
    // For ISLAST=1, the meta-block is always compressed (no ISUNCOMPRESSED bit).
    // But we don't know ISLAST here... let me restructure.
    // For now, always try to read ISUNCOMPRESSED. The caller must handle this.
    // Actually, we can't decide here. Let me just return the length and
    // let the caller handle the ISUNCOMPRESSED bit.
    return .{ .len = mlen, .is_uncompressed = false, .is_metadata = false };
}

// =========================================================================
// Encoder (quality 0)
// =========================================================================
