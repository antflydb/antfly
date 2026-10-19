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

//! Snappy block compression/decompression.
//!
//! Wire-compatible with Go's `github.com/golang/snappy` block format.
//! This is the raw block format (no framing), used for stored fields and doc
//! values compression.

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
    return decodeWithAdmission(alloc, src, null);
}

// Generic synchronous control keeps this codec independent of runtime ABI
// imports. Null instantiation compiles out the controlled checks/copy loops.
fn checkpoint(control: anytype) !void {
    if (comptime @TypeOf(control) != @TypeOf(null)) {
        if (control) |budget| try budget.checkpoint();
    }
}

pub fn decodeWithAdmission(alloc: Allocator, src: []const u8, control: anytype) ![]u8 {
    try checkpoint(control);
    if (src.len == 0) {
        const empty = try alloc.alloc(u8, 0);
        errdefer alloc.free(empty);
        try checkpoint(control);
        return empty;
    }

    // Read uncompressed length (varint preamble)
    var pos: usize = 0;
    const uncompressed_len = try readVarint(src, &pos);

    const dst = try alloc.alloc(u8, uncompressed_len);
    errdefer alloc.free(dst);

    try decodePayload(src, pos, dst, control);
    return dst;
}

/// Decode into final caller-owned storage without a second output allocation.
pub fn decodeInto(src: []const u8, dst: []u8) !void {
    return decodeIntoWithAdmission(src, dst, null);
}

pub fn decodeIntoWithAdmission(src: []const u8, dst: []u8, control: anytype) !void {
    try checkpoint(control);
    if (src.len == 0) {
        if (dst.len != 0) return error.CorruptInput;
        return;
    }
    var pos: usize = 0;
    const uncompressed_len = try readVarint(src, &pos);
    if (uncompressed_len != dst.len) return error.CorruptInput;
    try decodePayload(src, pos, dst, control);
}

/// Explicit raw-block decode progress. Owns only scalar offsets and pending
/// tag metadata; input, output, reader and controls are borrowed per step.
/// Callers keep one immutable input and its decoded prefix alive across steps.
/// No output may be published before done. A failed provider read can be
/// retried: only successfully read input and written output advance progress.
pub const DecodeCursor = struct {
    pub const Phase = enum { preamble, tag, literal_header, copy_header, literal, copy, done, failed };
    pub const Progress = struct { done: bool, needs_output: bool, input_bytes: usize, output_bytes: usize, tags: usize };
    input_len: usize,
    output_len: usize,
    input_offset: usize = 0,
    output_offset: usize = 0,
    phase: Phase = .preamble,
    header_value: usize = 0,
    header_shift: std.math.Log2Int(usize) = 0,
    header_left: usize = 0,
    remaining: usize = 0,
    copy_offset: usize = 0,
    declared_length: ?usize = null,

    pub fn init(input_len: usize, output_len: usize) DecodeCursor {
        return .{ .input_len = input_len, .output_len = output_len, .phase = if (input_len == 0 and output_len == 0) .done else .preamble };
    }

    const SliceReader = struct {
        bytes: []const u8,
        length: usize,
        fn readInto(self: @This(), offset: u64, out: []u8) !void {
            const start = std.math.cast(usize, offset) orelse return error.CorruptInput;
            if (start > self.bytes.len or out.len > self.bytes.len - start) return error.CorruptInput;
            @memcpy(out, self.bytes[start..][0..out.len]);
        }
    };

    /// Each call requests at most quantum input bytes, writes at most quantum
    /// output bytes and parses at most 256 tags. Zero quantum is invalid.
    pub fn step(self: *DecodeCursor, src: []const u8, dst: ?[]u8, quantum: usize) !Progress {
        return self.stepFromReader(SliceReader{ .bytes = src, .length = src.len }, dst, quantum);
    }

    /// A reader supplies length and readInto(offset, out). It must honor its
    /// own physical admission/window bounds. Neither reader nor output escapes.
    pub fn stepFromReader(self: *DecodeCursor, reader: anytype, dst: ?[]u8, quantum: usize) !Progress {
        if (quantum == 0 or reader.length != self.input_len) return error.InvalidArgument;
        if (dst) |out| if (out.len != self.output_len) return error.InvalidArgument;
        return self.advance(reader, dst, quantum) catch |err| {
            if (err == error.CorruptInput) self.phase = .failed;
            return err;
        };
    }

    fn byte(self: *DecodeCursor, reader: anytype) !u8 {
        if (self.input_offset >= self.input_len) return error.CorruptInput;
        var out: [1]u8 = undefined;
        try reader.readInto(@intCast(self.input_offset), &out);
        self.input_offset += 1;
        return out[0];
    }

    fn literalReady(self: *DecodeCursor) !void {
        if (self.remaining > self.output_len - self.output_offset or self.remaining > self.input_len - self.input_offset) return error.CorruptInput;
        self.phase = .literal;
    }

    fn copyReady(self: *DecodeCursor) !void {
        if (self.copy_offset == 0 or self.copy_offset > self.output_offset or self.remaining > self.output_len - self.output_offset) return error.CorruptInput;
        self.phase = .copy;
    }

    fn advance(self: *DecodeCursor, reader: anytype, dst: ?[]u8, quantum: usize) !Progress {
        const input_start = self.input_offset;
        const output_start = self.output_offset;
        var input_left = quantum;
        var output_left = quantum;
        var tags: usize = 0;
        while (true) {
            if (dst == null and self.phase != .preamble and self.phase != .done and self.phase != .failed) break;
            switch (self.phase) {
                .failed => return error.CorruptInput,
                .done => break,
                .preamble => {
                    if (input_left == 0) break;
                    const value = try self.byte(reader);
                    input_left -= 1;
                    const bits: usize = value & 127;
                    if (bits > @as(usize, std.math.maxInt(usize)) >> self.header_shift) return error.CorruptInput;
                    self.header_value |= bits << self.header_shift;
                    if (value & 128 == 0) {
                        self.declared_length = self.header_value;
                        if (self.header_value != self.output_len) return error.CorruptInput;
                        self.phase = .tag;
                    } else {
                        if (self.header_shift > @bitSizeOf(usize) - 1 - 7) return error.CorruptInput;
                        self.header_shift += 7;
                    }
                },
                .tag => {
                    if (self.input_offset == self.input_len) {
                        if (self.output_offset != self.output_len) return error.CorruptInput;
                        self.phase = .done;
                        break;
                    }
                    if (input_left == 0 or tags == 256) break;
                    const tag = try self.byte(reader);
                    input_left -= 1;
                    tags += 1;
                    const kind: u2 = @truncate(tag);
                    self.header_shift = 0;
                    self.header_value = 0;
                    self.remaining = @as(usize, tag >> 2) + 1;
                    if (kind == tag_literal) {
                        if (self.remaining <= 60) {
                            try self.literalReady();
                        } else {
                            self.header_left = self.remaining - 60;
                            self.phase = .literal_header;
                        }
                    } else {
                        self.header_left = switch (kind) {
                            tag_copy1 => 1,
                            tag_copy2 => 2,
                            tag_copy4 => 4,
                            else => unreachable,
                        };
                        if (kind == tag_copy1) {
                            self.remaining = @as(usize, (tag >> 2) & 7) + 4;
                            self.header_value = @as(usize, tag & 0xe0) << 3;
                        }
                        self.phase = .copy_header;
                    }
                },
                .literal_header, .copy_header => {
                    if (input_left == 0) break;
                    const value = try self.byte(reader);
                    input_left -= 1;
                    self.header_value |= @as(usize, value) << self.header_shift;
                    self.header_left -= 1;
                    if (self.header_left == 0) {
                        if (self.phase == .literal_header) {
                            self.remaining = std.math.add(usize, self.header_value, 1) catch return error.CorruptInput;
                            try self.literalReady();
                        } else {
                            self.copy_offset = self.header_value;
                            try self.copyReady();
                        }
                    } else self.header_shift += 8;
                },
                .literal => {
                    if (input_left == 0 or output_left == 0) break;
                    const take = @min(self.remaining, @min(input_left, output_left));
                    // A provider may write a partial range then fail. Its bytes
                    // are unpublished; retry overwrites that uncommitted range.
                    try reader.readInto(@intCast(self.input_offset), dst.?[self.output_offset..][0..take]);
                    self.input_offset += take;
                    self.output_offset += take;
                    self.remaining -= take;
                    input_left -= take;
                    output_left -= take;
                    if (self.remaining == 0) self.phase = .tag;
                },
                .copy => {
                    if (output_left == 0) break;
                    const take = @min(self.remaining, output_left);
                    copyOverlapping(dst.?, self.output_offset, self.copy_offset, take);
                    self.output_offset += take;
                    self.remaining -= take;
                    output_left -= take;
                    if (self.remaining == 0) self.phase = .tag;
                },
            }
        }
        return .{ .done = self.phase == .done, .needs_output = dst == null and self.phase != .preamble and self.phase != .done, .input_bytes = self.input_offset - input_start, .output_bytes = self.output_offset - output_start, .tags = tags };
    }
};

fn decodePayload(src: []const u8, start: usize, dst: []u8, control: anytype) !void {
    var cursor = DecodeCursor.init(src.len, dst.len);
    cursor.input_offset = start;
    cursor.phase = .tag;
    while (true) {
        try checkpoint(control);
        if ((try cursor.step(src, dst, 64 * 1024)).done) break;
    }
    try checkpoint(control);
}

/// Return the decoded length stored in the Snappy block preamble without
/// materializing the decoded bytes.
pub fn decodedLen(src: []const u8) !usize {
    if (src.len == 0) return 0;
    var pos: usize = 0;
    return try readVarint(src, &pos);
}

/// Decode a legacy block from bounded input windows. The required output is
/// charged to the caller allocator; no compressed-block-sized copy is made.
pub fn decodeFromView(alloc: Allocator, view: @import("../segment_source.zig").View, max_output_bytes: usize) ![]u8 {
    var input = RangeInput{ .view = view };
    const size = try input.decodedSize();
    if (size > max_output_bytes) return error.SegmentReadBudgetExceeded;
    const dst = try alloc.alloc(u8, size);
    errdefer alloc.free(dst);
    try decodeRangeFully(&input, dst);
    return dst;
}

fn decodeRangeFully(input: anytype, dst: []u8) !void {
    const Reader = struct {
        input: @TypeOf(input),
        length: u64,
        pub fn readInto(self: @This(), offset: u64, out: []u8) !void {
            return self.input.readInto(offset, out);
        }
    };
    var cursor = DecodeCursor.init(@intCast(input.view.length), dst.len);
    const reader: Reader = .{ .input = input, .length = input.view.length };
    while (!(try cursor.stepFromReader(reader, dst, 64 * 1024)).done) {}
}

const RangeInput = RangeReader(@import("../segment_source.zig").View);
fn RangeReader(comptime View: type) type {
    return struct {
        view: View,
        pos: u64 = 0,
        start: u64 = 0,
        valid: usize = 0,
        window_bytes: usize = 8192,
        bytes: [8192]u8 = undefined,
        pub fn readInto(self: *@This(), offset: u64, out: []u8) !void {
            if (offset > self.view.length or out.len > self.view.length - offset) return error.CorruptInput;
            self.pos = offset;
            try self.literal(out);
        }
        fn decodedSize(self: *@This()) !usize {
            if (self.view.length == 0) return 0;
            var prefix: [5]u8 = undefined;
            var length: usize = 0;
            while (true) {
                if (length == prefix.len) return error.CorruptInput;
                prefix[length] = try self.byte();
                length += 1;
                if (prefix[length - 1] & 128 == 0) break;
            }
            return try decodedLen(prefix[0..length]);
        }
        fn byte(self: *@This()) !u8 {
            if (self.pos >= self.view.length) return error.CorruptInput;
            if (self.valid == 0 or self.pos < self.start or self.pos - self.start >= self.valid) {
                self.start = self.pos;
                self.valid = @intCast(@min(self.window_bytes, self.view.length - self.pos));
                try self.view.readInto(self.pos, self.bytes[0..self.valid]);
            }
            const value = self.bytes[@intCast(self.pos - self.start)];
            self.pos += 1;
            return value;
        }
        fn integer(self: *@This(), count: usize) !usize {
            var value: usize = 0;
            for (0..count) |i| value |= @as(usize, try self.byte()) << @intCast(i * 8);
            return value;
        }
        fn literal(self: *@This(), out: []u8) !void {
            if (out.len > self.view.length - self.pos) return error.CorruptInput;
            var copied: usize = 0;
            while (copied < out.len) {
                const first = try self.byte();
                out[copied] = first;
                copied += 1;
                const available = self.valid - @as(usize, @intCast(self.pos - self.start));
                const take = @min(available, out.len - copied);
                @memcpy(out[copied..][0..take], self.bytes[@intCast(self.pos - self.start)..][0..take]);
                copied += take;
                self.pos += take;
            }
        }
    };
}

/// Decode into reusable caller-owned output without allocating input or output.
pub fn decodeFromViewInto(view: @import("../segment_source.zig").View, dst: []u8) !void {
    if (view.length == 0) {
        if (dst.len != 0) return error.CorruptInput;
        return;
    }
    var input = RangeInput{ .view = view };
    if (try input.decodedSize() != dst.len) return error.CorruptInput;
    try decodeRangeFully(&input, dst);
}

/// Compare a compressed block with borrowed expected spans. The already
/// verified prefix supplies COPY history, so neither an output buffer nor a
/// second encoding is needed. Equality validates the complete block.
pub fn equalsFromReader(view: anytype, parts: []const []const u8) !bool {
    const Expected = struct {
        parts: []const []const u8,
        fn span(self: @This(), offset: usize) []const u8 {
            var at = offset;
            for (self.parts) |part| {
                if (at < part.len) return part[at..];
                at -= part.len;
            }
            return &.{};
        }
    };
    const expected: Expected = .{ .parts = parts };
    var size: usize = 0;
    for (parts) |part| size = std.math.add(usize, size, part.len) catch return error.CorruptInput;
    var input: RangeReader(@TypeOf(view)) = .{ .view = view };
    if (try input.decodedSize() != size) return false;
    var written: usize = 0;
    var bytes: [4096]u8 = undefined;
    while (input.pos < input.view.length) {
        const tag = try input.byte();
        const kind: u2 = @truncate(tag);
        var count: usize = @as(usize, tag >> 2) + 1;
        var offset: usize = 0;
        switch (kind) {
            tag_literal => if (count > 60) {
                count = std.math.add(usize, 1, try input.integer(count - 60)) catch return error.CorruptInput;
            },
            tag_copy1 => {
                count = @as(usize, (tag >> 2) & 7) + 4;
                offset = (@as(usize, tag & 0xe0) << 3) | try input.byte();
            },
            tag_copy2 => offset = try input.integer(2),
            tag_copy4 => offset = try input.integer(4),
        }
        if (count > size - written) return error.CorruptInput;
        if (kind != tag_literal and (offset == 0 or offset > written)) return error.CorruptInput;
        var done: usize = 0;
        while (done < count) {
            const out = expected.span(written + done);
            if (kind == tag_literal) {
                const take = @min(@min(out.len, bytes.len), count - done);
                try input.literal(bytes[0..take]);
                if (!std.mem.eql(u8, bytes[0..take], out[0..take])) return false;
                done += take;
            } else {
                const prior = expected.span(written + done - offset);
                const take = @min(@min(out.len, prior.len), count - done);
                if (!std.mem.eql(u8, out[0..take], prior[0..take])) return false;
                done += take;
            }
        }
        written += count;
    }
    if (written != size) return error.CorruptInput;
    return true;
}

/// Decode only an admission prefix using a small input window. This does not
/// validate the suffix; callers must still fully decode before exposing data.
/// Returns the declared decoded size, without allocating input or output.
pub fn decodePrefixFromView(view: @import("../segment_source.zig").View, prefix: []u8) !usize {
    var input = RangeInput{ .view = view, .window_bytes = 32 };
    const size = try input.decodedSize();
    if (prefix.len > size) return error.CorruptInput;
    try decodeRangePayload(&input, prefix, size, false);
    return size;
}

fn decodeRangePayload(input: *RangeInput, dst: []u8, expected: usize, comptime complete: bool) !void {
    var written: usize = 0;
    while (input.pos < input.view.length) {
        if (!complete and written == dst.len) return;
        const tag = try input.byte();
        const kind: u2 = @truncate(tag);
        var count: usize = @as(usize, tag >> 2) + 1;
        var offset: usize = 0;
        switch (kind) {
            tag_literal => if (count > 60) {
                count = std.math.add(usize, 1, try input.integer(count - 60)) catch return error.CorruptInput;
            },
            tag_copy1 => {
                count = @as(usize, (tag >> 2) & 7) + 4;
                offset = (@as(usize, tag & 0xe0) << 3) | try input.byte();
            },
            tag_copy2 => offset = try input.integer(2),
            tag_copy4 => offset = try input.integer(4),
        }
        if (count > expected - written) return error.CorruptInput;
        const take = @min(count, dst.len - written);
        if (kind == tag_literal) try input.literal(dst[written..][0..take]) else {
            if (offset == 0 or offset > written) return error.CorruptInput;
            copyOverlapping(dst, written, offset, take);
        }
        written += take;
    }
    if (written != dst.len) return error.CorruptInput;
}

/// Valid Snappy payloads expand by at most 64 output bytes per three
/// encoded bytes (COPY_2). Literal, COPY_1 and COPY_4 tags expand less.
/// Counting the length preamble too makes this a conservative admission
/// bound without reading payload pages or allocating decoder output.
pub fn decodedSizeUpperBound(encoded_bytes: u64) !u64 {
    return try std.math.add(
        u64,
        try std.math.mul(u64, encoded_bytes / 3, 64),
        (encoded_bytes % 3) * 64 / 3,
    );
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
        const value: usize = b & 0x7f;
        if (value > @as(usize, std.math.maxInt(usize)) >> shift) return error.CorruptInput;
        result |= value << shift;
        if (b & 0x80 == 0) return result;
        if (shift > @bitSizeOf(usize) - 1 - 7) return error.CorruptInput;
        shift += 7;
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
    const input = @as([4096]u8, @splat(0));
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

test "decode admission checks long literals and repeated copy tags without changing bytes" {
    const len = 256 * 1024;
    const literal = try std.testing.allocator.alloc(u8, len);
    defer std.testing.allocator.free(literal);
    for (literal, 0..) |*byte, i| byte.* = @truncate(i);
    const Budget = struct {
        calls: usize = 0,
        fail_at: usize = 0,
        pub fn checkpoint(self: *@This()) !void {
            self.calls += 1;
            if (self.calls == self.fail_at) return error.OriginalReadDeadline;
        }
    };
    for ([_]bool{ false, true }) |copies| {
        var payload: std.ArrayListUnmanaged(u8) = .empty;
        defer payload.deinit(std.testing.allocator);
        try writeVarint(std.testing.allocator, &payload, len);
        if (copies) {
            try emitLiteral(std.testing.allocator, &payload, "a");
            try emitCopy(std.testing.allocator, &payload, 1, len - 1);
        } else try emitLiteral(std.testing.allocator, &payload, literal);
        var budget = Budget{};
        const decoded = try decodeWithAdmission(std.testing.allocator, payload.items, @as(?*Budget, &budget));
        defer std.testing.allocator.free(decoded);
        if (copies) {
            for (decoded) |byte| try std.testing.expectEqual(@as(u8, 'a'), byte);
        } else try std.testing.expectEqualSlices(u8, literal, decoded);
        try std.testing.expect(budget.calls >= 6);
        const total_checks = budget.calls;
        for (1..total_checks + 1) |fail_at| {
            budget = .{ .fail_at = fail_at };
            try std.testing.expectError(error.OriginalReadDeadline, decodeWithAdmission(std.testing.allocator, payload.items, @as(?*Budget, &budget)));
            try std.testing.expectEqual(fail_at, budget.calls);
        }
        const legacy = try decode(std.testing.allocator, payload.items);
        defer std.testing.allocator.free(legacy);
        try std.testing.expectEqualSlices(u8, legacy, decoded);
    }
}

test "SQL bounded native delivery Snappy exact span comparison validates literals copies and cross-span history without output" {
    const Input = struct {
        bytes: []const u8,
        length: u64,
        pub fn readInto(self: @This(), at: u64, out: []u8) !void {
            if (at > self.bytes.len or out.len > self.bytes.len - at) return error.EndOfStream;
            @memcpy(out, self.bytes[@intCast(at)..][0..out.len]);
        }
    };
    const a = std.testing.allocator;
    const bytes = try a.alloc(u8, 100_000);
    defer a.free(bytes);
    for (bytes, 0..) |*value, i| value.* = @truncate((i % 1024) *% 37);
    const compressed = try encode(a, bytes);
    defer a.free(compressed);
    const input: Input = .{ .bytes = compressed, .length = compressed.len };
    for ([_]usize{ 0, 1, 63, 4095, 65536, bytes.len }) |split| {
        try std.testing.expect(try equalsFromReader(input, &.{ bytes[0..split], &.{}, bytes[split..] }));
    }
    // The final byte is covered by copy tags. An equal length/hash hint is
    // insufficient: the expected history and every literal must agree.
    bytes[bytes.len - 1] ^= 1;
    try std.testing.expect(!try equalsFromReader(input, &.{bytes}));
    bytes[bytes.len - 1] ^= 1;
    const corrupt = [_]u8{ 4, 1, 0 }; // COPY_1 with zero history/offset.
    try std.testing.expectError(error.CorruptInput, equalsFromReader(Input{ .bytes = &corrupt, .length = corrupt.len }, &.{"abcd"}));
    const trailing = [_]u8{ 1, 0, 'a', 0, 'b' };
    try std.testing.expectError(error.CorruptInput, equalsFromReader(Input{ .bytes = &trailing, .length = trailing.len }, &.{"a"}));
}

test "Snappy continuation byte quanta resume all tag widths with fresh input and output borrows" {
    const a = std.testing.allocator;
    var encoded: std.ArrayListUnmanaged(u8) = .empty;
    defer encoded.deinit(a);
    const expected = "abcdefghabcdefgh" ++ "efgh" ++ "efghefgh" ++ "c";
    try writeVarint(a, &encoded, expected.len);
    try emitLiteral(a, &encoded, "abcdefgh");
    // COPY_1: eight bytes at offset eight; COPY_2: four at offset four;
    // COPY_4: eight at offset four, including overlapping history.
    try encoded.appendSlice(a, &.{ 0x11, 8, 0x0e, 4, 0, 0x1f, 4, 0, 0, 0 });
    // Final single literal supplies the last byte independently.
    try emitLiteral(a, &encoded, "c");
    const length = try decodedLen(encoded.items);
    for ([_]usize{ 1, 2, 3, 7, 64 }) |quantum| {
        const first_input = try a.dupe(u8, encoded.items);
        defer a.free(first_input);
        const second_input = try a.dupe(u8, encoded.items);
        defer a.free(second_input);
        const first_output = try a.alloc(u8, length);
        defer a.free(first_output);
        const second_output = try a.alloc(u8, length);
        defer a.free(second_output);
        var cursor = DecodeCursor.init(encoded.items.len, length);
        var steps: usize = 0;
        var input_bytes: usize = 0;
        var output_bytes: usize = 0;
        var prior = first_output;
        while (true) {
            const src = if (steps % 2 == 0) first_input else second_input;
            const dst = if (steps % 2 == 0) second_output else first_output;
            @memcpy(src, encoded.items);
            @memcpy(dst[0..cursor.output_offset], prior[0..cursor.output_offset]);
            const progress = try cursor.step(src, dst, quantum);
            @memset(src, 0xFF);
            try std.testing.expect(progress.input_bytes <= quantum and progress.output_bytes <= quantum and progress.tags <= 256);
            input_bytes += progress.input_bytes;
            output_bytes += progress.output_bytes;
            steps += 1;
            try std.testing.expect(steps <= encoded.items.len + length + 2);
            if (progress.done) {
                try std.testing.expectEqualStrings(expected, dst);
                break;
            }
            try std.testing.expect(progress.input_bytes + progress.output_bytes > 0);
            prior = dst;
        }
        try std.testing.expectEqual(encoded.items.len, input_bytes);
        try std.testing.expectEqual(length, output_bytes);
    }
}

test "Snappy continuation long literals and copies stay within byte and tag quanta" {
    const a = std.testing.allocator;
    const count = 128 * 1024 + 17;
    const data = try a.alloc(u8, count);
    defer a.free(data);
    for (data, 0..) |*b, i| b.* = @truncate(i);
    for ([_]bool{ false, true }) |copies| {
        var encoded: std.ArrayListUnmanaged(u8) = .empty;
        defer encoded.deinit(a);
        try writeVarint(a, &encoded, count);
        if (copies) {
            try emitLiteral(a, &encoded, "a");
            try emitCopy(a, &encoded, 1, count - 1);
        } else try emitLiteral(a, &encoded, data);
        const out = try a.alloc(u8, count);
        defer a.free(out);
        var cursor = DecodeCursor.init(encoded.items.len, count);
        var total_input: usize = 0;
        var total_output: usize = 0;
        var steps: usize = 0;
        while (true) {
            const progress = try cursor.step(encoded.items, out, 1021);
            try std.testing.expect(progress.input_bytes <= 1021 and progress.output_bytes <= 1021 and progress.tags <= 256);
            total_input += progress.input_bytes;
            total_output += progress.output_bytes;
            steps += 1;
            try std.testing.expect(steps < count);
            if (progress.done) break;
        }
        try std.testing.expect(steps > 100);
        try std.testing.expectEqual(encoded.items.len, total_input);
        try std.testing.expectEqual(count, total_output);
        if (copies) {
            for (out) |b| try std.testing.expectEqual(@as(u8, 'a'), b);
        } else try std.testing.expectEqualSlices(u8, data, out);
    }
}

test "Snappy continuation reader failure preserves committed progress and borrows no reader" {
    const a = std.testing.allocator;
    var encoded: std.ArrayListUnmanaged(u8) = .empty;
    defer encoded.deinit(a);
    try writeVarint(a, &encoded, 200);
    var data: [200]u8 = undefined;
    for (&data, 0..) |*b, i| b.* = @truncate(i);
    try emitLiteral(a, &encoded, &data);
    const Reader = struct {
        bytes: []const u8,
        length: usize,
        fail: bool = true,
        pub fn readInto(self: *@This(), offset: u64, out: []u8) !void {
            if (self.fail and out.len > 1) {
                out[0] = 0xDD;
                return error.TransientInputFault;
            }
            @memcpy(out, self.bytes[@intCast(offset)..][0..out.len]);
        }
    };
    var cursor = DecodeCursor.init(encoded.items.len, data.len);
    var output: [200]u8 = @splat(0xCC);
    {
        var reader: Reader = .{ .bytes = encoded.items, .length = encoded.items.len };
        try std.testing.expectError(error.TransientInputFault, cursor.stepFromReader(&reader, &output, 31));
    }
    try std.testing.expect(cursor.phase == .literal and cursor.output_offset == 0);
    const committed_input = cursor.input_offset;
    try std.testing.expect(committed_input > 0 and committed_input < encoded.items.len);
    var resumed: Reader = .{ .bytes = encoded.items, .length = encoded.items.len, .fail = false };
    var resumed_input: usize = 0;
    while (true) {
        const progress = try cursor.stepFromReader(&resumed, &output, 31);
        resumed_input += progress.input_bytes;
        if (progress.done) break;
    }
    try std.testing.expectEqual(encoded.items.len - committed_input, resumed_input);
    try std.testing.expectEqualSlices(u8, &data, &output);
    try std.testing.expect(@sizeOf(DecodeCursor) <= 128);
}

test "Snappy continuation corruption remains terminal across tag and preamble boundaries" {
    const cases = [_]struct { bytes: []const u8, len: usize }{
        .{ .bytes = &.{128}, .len = 1 },
        .{ .bytes = &.{ 1, 0 }, .len = 1 },
        .{ .bytes = &.{ 2, 0, 'a', 2, 0, 0 }, .len = 2 },
        .{ .bytes = &.{ 2, 0, 'a', 2, 2, 0 }, .len = 2 },
        .{ .bytes = &.{ 1, 4, 'a', 'b' }, .len = 1 },
        .{ .bytes = &.{ 0, 0, 'a' }, .len = 0 },
        .{ .bytes = &.{ 1, 240 }, .len = 1 },
        .{ .bytes = &.{ 1, 252, 255, 255, 255, 255 }, .len = 1 },
    };
    for (cases) |case| {
        var cursor = DecodeCursor.init(case.bytes.len, case.len);
        var output: [2]u8 = undefined;
        var caught = false;
        for (0..32) |_| {
            const progress = cursor.step(case.bytes, output[0..case.len], 1) catch |err| {
                try std.testing.expectEqual(error.CorruptInput, err);
                caught = true;
                break;
            };
            try std.testing.expect(!progress.done);
        }
        try std.testing.expect(caught);
        try std.testing.expectError(error.CorruptInput, cursor.step(case.bytes, output[0..case.len], 1));
    }
    var cursor = DecodeCursor.init(2, 1);
    var output: [1]u8 = undefined;
    try std.testing.expectError(error.InvalidArgument, cursor.step(&.{ 1, 0 }, &output, 0));
}

test "Snappy continuation preserves high COPY offsets across partial headers" {
    const a = std.testing.allocator;
    const count = 65543;
    const literal = try a.alloc(u8, count);
    defer a.free(literal);
    for (literal, 0..) |*byte, i| byte.* = @truncate((i *% 0x9e3779b1) ^ (i >> 16));
    var encoded: std.ArrayListUnmanaged(u8) = .empty;
    defer encoded.deinit(a);
    try writeVarint(a, &encoded, count + 4 + 64);
    try emitLiteral(a, &encoded, literal);
    // COPY_1 count four, offset 300; COPY_4 count 64, offset 65536.
    try encoded.appendSlice(a, &.{ 0x21, 44, 0xff, 0, 0, 1, 0 });
    const output = try a.alloc(u8, count + 4 + 64);
    defer a.free(output);
    var cursor = DecodeCursor.init(encoded.items.len, output.len);
    while (!(try cursor.step(encoded.items, output, 13)).done) {}
    try std.testing.expectEqualSlices(u8, literal, output[0..count]);
    try std.testing.expectEqualSlices(u8, literal[count - 300 ..][0..4], output[count..][0..4]);
    try std.testing.expectEqualSlices(u8, literal[count + 4 - 65536 ..][0..64], output[count + 4 ..]);
}
