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

//! Bzip2 bitstream decoder: canonical Huffman, MTF/RLE, inverse BWT and CRC.
const std = @import("std");
const A = std.mem.Allocator;
const E = error{ InvalidBzip2, StreamTooLong } || A.Error || std.Io.Reader.Error || std.Io.Writer.Error;
const Bits = struct {
    input: *std.Io.Reader,
    remaining: u7 = 0,
    reservoir: u64 = 0,
    fn ensure(self: *Bits, n: usize) E!void {
        while (self.remaining < n) {
            const byte = self.input.takeByte() catch |err| return if (err == error.EndOfStream) error.InvalidBzip2 else err;
            self.reservoir = (self.reservoir << 8) | byte;
            self.remaining += 8;
        }
    }
    fn peek(self: *const Bits, n: usize) u64 {
        return (self.reservoir >> @as(u6, @intCast(self.remaining - n))) & ((@as(u64, 1) << @as(u6, @intCast(n))) - 1);
    }
    inline fn read(self: *Bits, n: usize) E!u64 {
        try self.ensure(n);
        const result = self.peek(n);
        self.remaining -= @intCast(n);
        return result;
    }
    fn more(self: *Bits) E!bool {
        if (self.remaining != 0) return true;
        _ = self.input.peekByte() catch |err| switch (err) {
            error.EndOfStream => return false,
            else => return err,
        };
        return true;
    }
};
const Table = struct {
    primary: [1024]u16 = @splat(0),
    counts: [21]u16 = @splat(0),
    first: [21]u32 = @splat(0),
    offsets: [21]usize = @splat(0),
    symbols: [258]u16 = undefined,
    minimum: usize = 20,
    fn init(lengths: []const u8) E!Table {
        var t = Table{};
        for (lengths) |n| {
            if (n < 1 or n > 20) return error.InvalidBzip2;
            t.counts[n] += 1;
            t.minimum = @min(t.minimum, n);
        }
        var code: u32 = 0;
        var offset: usize = 0;
        for (1..21) |n| {
            code = (code + t.counts[n - 1]) << 1;
            if (code + t.counts[n] > (@as(u32, 1) << @intCast(n))) return error.InvalidBzip2;
            t.first[n] = code;
            t.offsets[n] = offset;
            for (lengths, 0..) |len, i| if (len == n) {
                t.symbols[offset] = @intCast(i);
                if (n <= 10) {
                    const shift: u4 = @intCast(10 - n);
                    const start = (code + offset - t.offsets[n]) << shift;
                    @memset(t.primary[start..][0 .. @as(usize, 1) << shift], @as(u16, @intCast((n << 9) | i)));
                }
                offset += 1;
            };
        }
        return t;
    }
    fn symbol(self: *const Table, bits: *Bits) E!usize {
        // Every valid coded block has a following 80-bit trailer, so bounded
        // lookahead cannot require bytes beyond a valid stream's end.
        // Very short codes already resolve with one cheap canonical probe;
        // reserve the lookup for tables whose decoding pays for lookahead.
        if (self.minimum >= 5) {
            try bits.ensure(10);
            const entry = self.primary[@intCast(bits.peek(10))];
            if (entry != 0) {
                bits.remaining -= @intCast(entry >> 9);
                return entry & 511;
            }
        }
        var code: u32 = @intCast(try bits.read(self.minimum));
        var n = self.minimum;
        while (true) : (n += 1) {
            if (code >= self.first[n] and code - self.first[n] < self.counts[n]) return self.symbols[self.offsets[n] + code - self.first[n]];
            if (n == 20) break;
            code = (code << 1) | @as(u32, @intCast(try bits.read(1)));
        }
        return error.InvalidBzip2;
    }
};
const Symbols = struct {
    bits: *Bits,
    tables: []const Table,
    selectors: []const u8,
    index: usize = 0,
    fn next(self: *Symbols) E!usize {
        if (self.index / 50 >= self.selectors.len) return error.InvalidBzip2;
        const table = self.selectors[self.index / 50];
        self.index += 1;
        return self.tables[table].symbol(self.bits);
    }
};
pub fn decode(a: A, data: []const u8, limit: usize) E![]u8 {
    var input = std.Io.Reader.fixed(data);
    var output: std.Io.Writer.Allocating = .init(a);
    errdefer output.deinit();
    decodeTo(a, &input, &output.writer, limit) catch |err| return if (err == error.WriteFailed) error.OutOfMemory else err;
    return output.toOwnedSlice();
}

/// Decode with bounded BWT workspace and caller-provided backpressure. Neither
/// compressed input nor expanded output is retained beyond one BWT block.
pub fn decodeTo(a: A, input: *std.Io.Reader, output: *std.Io.Writer, limit: usize) E!void {
    var bits = Bits{ .input = input };
    var produced: usize = 0;
    // Reuse bounded block workspaces across blocks and concatenated streams.
    var last: std.ArrayList(u8) = .empty;
    defer last.deinit(a);
    var workspace: std.ArrayList(u32) = .empty;
    defer workspace.deinit(a);
    while (try bits.more()) {
        if (try bits.read(24) != 0x425a68) return error.InvalidBzip2;
        const size = try bits.read(8);
        if (size < '1' or size > '9') return error.InvalidBzip2;
        const block_limit: usize = @intCast((size - '0') * 100000);
        var combined: u32 = 0;
        while (true) {
            const magic = try bits.read(48);
            if (magic == 0x177245385090) {
                if (try bits.read(32) != combined) return error.InvalidBzip2;
                bits.remaining = 0;
                break;
            }
            if (magic != 0x314159265359) return error.InvalidBzip2;
            const checksum: u32 = @intCast(try bits.read(32));
            const randomized = try bits.read(1) != 0;
            const origin: usize = @intCast(try bits.read(24));
            var alphabet: [256]u8 = undefined;
            var n: usize = 0;
            const used = try bits.read(16);
            for (0..16) |group| if (used & (@as(u64, 1) << @intCast(15 - group)) != 0) {
                for (0..16) |j| if (try bits.read(1) != 0) {
                    alphabet[n] = @intCast(group * 16 + j);
                    n += 1;
                };
            };
            if (n == 0) return error.InvalidBzip2;
            const groups: usize = @intCast(try bits.read(3));
            const ns: usize = @intCast(try bits.read(15));
            if (groups < 2 or groups > 6 or ns == 0) return error.InvalidBzip2;
            const selectors = try a.alloc(u8, ns);
            defer a.free(selectors);
            var order: [6]u8 = .{ 0, 1, 2, 3, 4, 5 };
            for (selectors) |*selector| {
                var j: usize = 0;
                while (try bits.read(1) != 0) {
                    j += 1;
                    if (j >= groups) return error.InvalidBzip2;
                }
                selector.* = order[j];
                while (j > 0) : (j -= 1) order[j] = order[j - 1];
                order[0] = selector.*;
            }
            var tables: [6]Table = undefined;
            for (tables[0..groups]) |*table| {
                var lengths: [258]u8 = undefined;
                var length: i32 = @intCast(try bits.read(5));
                for (lengths[0 .. n + 2]) |*value| {
                    while (try bits.read(1) != 0) {
                        length += if (try bits.read(1) == 0) @as(i32, 1) else -1;
                        if (length < 1 or length > 20) return error.InvalidBzip2;
                    }
                    if (length < 1 or length > 20) return error.InvalidBzip2;
                    value.* = @intCast(length);
                }
                table.* = try Table.init(lengths[0 .. n + 2]);
            }
            var symbols = Symbols{ .bits = &bits, .tables = tables[0..groups], .selectors = selectors };
            last.clearRetainingCapacity();
            var sym = try symbols.next();
            while (sym != n + 1) {
                if (sym < 2) {
                    var run: usize = 0;
                    var power: usize = 1;
                    while (sym < 2) {
                        run += (sym + 1) * power;
                        if (run > block_limit or power > block_limit) return error.InvalidBzip2;
                        power *= 2;
                        sym = try symbols.next();
                    }
                    if (run > block_limit - last.items.len) return error.InvalidBzip2;
                    try last.appendNTimes(a, alphabet[0], run);
                } else {
                    if (sym > n or last.items.len == block_limit) return error.InvalidBzip2;
                    var j = sym - 1;
                    const ch = alphabet[j];
                    while (j > 0) : (j -= 1) alphabet[j] = alphabet[j - 1];
                    alphabet[0] = ch;
                    try last.append(a, ch);
                    sym = try symbols.next();
                }
            }
            if (origin >= last.items.len) return error.InvalidBzip2;
            var counts: [256]usize = @splat(0);
            for (last.items) |ch| counts[ch] += 1;
            var total: usize = 0;
            for (&counts) |*c| {
                const tmp = c.*;
                c.* = total;
                total += tmp;
            }
            // A link uses four bytes. Geometric growth would retain up to
            // 50% unnecessary capacity for the dominant decoder workspace.
            try workspace.ensureTotalCapacityPrecise(a, last.items.len);
            workspace.items.len = last.items.len;
            const links = workspace.items;
            for (last.items, 0..) |ch, i| {
                links[counts[ch]] = @intCast(i);
                counts[ch] += 1;
            }
            const block_len = last.items.len;
            const pos: usize = links[origin];
            // One dependent lookup supplies both the byte and its successor.
            // The temporary last-column bytes are no longer needed afterwards.
            for (links, last.items) |*entry, byte| entry.* = (entry.* << 8) | byte;
            const actual_crc = if (randomized)
                try inverse(true, links, pos, block_len, output, &produced, limit)
            else
                try inverse(false, links, pos, block_len, output, &produced, limit);
            if (actual_crc != checksum) return error.InvalidBzip2;
            combined = std.math.rotl(u32, combined, 1) ^ checksum;
        }
    }
}

/// Specializing randomization keeps its legacy counters off the common path.
/// Inverse BWT pointer chasing and RLE output run in separate cache-sized loops,
/// so per-byte output bookkeeping does not lengthen the dependency chain.
fn inverse(comptime randomized: bool, links: []const u32, start: usize, block_len: usize, output: *std.Io.Writer, produced: *usize, limit: usize) E!u32 {
    var pos = start;
    var previous: u8 = 0;
    var repeated: usize = 0;
    var crc = @import("antfly_hash").Crc32Bzip2.init();
    var decoded: [16384]u8 = undefined;
    var decoded_len: usize = 0;
    var random_left: usize = 0;
    var random_index: usize = 0;
    var column: [4096]u8 = undefined;
    var left = block_len;
    while (left > 0) {
        const n = @min(left, column.len);
        for (column[0..n]) |*byte| {
            const entry = links[pos];
            byte.* = @truncate(entry);
            pos = entry >> 8;
        }
        if (randomized) for (column[0..n]) |*ch| {
            if (random_left == 0) {
                random_left = @import("bzip2_random.zig").numbers[random_index];
                random_index = (random_index + 1) % 512;
            }
            random_left -= 1;
            if (random_left == 1) ch.* ^= 1;
        };
        var at: usize = 0;
        while (at < n) {
            const ch = column[at];
            if (repeated == 4) {
                const count: usize = ch;
                if (count > limit - produced.*) return error.StreamTooLong;
                if (count > decoded.len - decoded_len) {
                    try output.writeAll(decoded[0..decoded_len]);
                    crc.update(decoded[0..decoded_len]);
                    decoded_len = 0;
                }
                @memset(decoded[decoded_len..][0..count], previous);
                decoded_len += count;
                produced.* += count;
                repeated = 0;
                at += 1;
                continue;
            }
            // Complete runs crossing BWT chunks before entering the bulk path.
            if (repeated > 0 and previous == ch) {
                if (produced.* == limit) return error.StreamTooLong;
                if (decoded_len == decoded.len) {
                    try output.writeAll(decoded[0..decoded_len]);
                    crc.update(decoded[0..decoded_len]);
                    decoded_len = 0;
                }
                decoded[decoded_len] = ch;
                decoded_len += 1;
                produced.* += 1;
                repeated += 1;
                at += 1;
                continue;
            }
            const run = fourEqual(column[at..n]);
            const count = if (run) |index| index + 4 else n - at;
            if (count > limit - produced.*) return error.StreamTooLong;
            if (count > decoded.len - decoded_len) {
                try output.writeAll(decoded[0..decoded_len]);
                crc.update(decoded[0..decoded_len]);
                decoded_len = 0;
            }
            @memcpy(decoded[decoded_len..][0..count], column[at..][0..count]);
            decoded_len += count;
            produced.* += count;
            at += count;
            previous = column[at - 1];
            repeated = if (run != null) 4 else 1;
            if (run == null) while (repeated < @min(count, 3) and column[at - 1 - repeated] == previous) {
                repeated += 1;
            };
        }
        left -= n;
    }
    if (repeated == 4) return error.InvalidBzip2;
    try output.writeAll(decoded[0..decoded_len]);
    crc.update(decoded[0..decoded_len]);
    return crc.final();
}

/// Locate the next RLE escape using SIMD; ordinary bytes can then be copied,
/// bounded and checksummed in blocks rather than individually.
fn fourEqual(bytes: []const u8) ?usize {
    const width = std.simd.suggestVectorLength(u8) orelse 16;
    var at: usize = 0;
    while (bytes.len - at >= width + 3) : (at += width) {
        const a: @Vector(width, u8) = bytes[at..][0..width].*;
        const b: @Vector(width, u8) = bytes[at + 1 ..][0..width].*;
        const c: @Vector(width, u8) = bytes[at + 2 ..][0..width].*;
        const d: @Vector(width, u8) = bytes[at + 3 ..][0..width].*;
        const mask = (a == b) & (b == c) & (c == d);
        if (@reduce(.Or, mask)) {
            const bits: @Int(.unsigned, width) = @bitCast(mask);
            return at + @ctz(bits);
        }
    }
    while (bytes.len - at >= 4) : (at += 1) {
        if (bytes[at] == bytes[at + 1] and bytes[at] == bytes[at + 2] and bytes[at] == bytes[at + 3]) return at;
    }
    return null;
}

test "primary Huffman lookup and long-code fallback agree with canonical codes" {
    var lengths: [21]u8 = undefined;
    for (lengths[0..19], 1..) |*len, n| len.* = @intCast(n);
    lengths[19] = 20;
    lengths[20] = 20;
    const table = try Table.init(&lengths);
    for (lengths, 0..) |len, expected| {
        const code = table.first[len] + @as(u32, @intCast(expected - table.offsets[len]));
        var encoded: [8]u8 = @splat(0);
        std.mem.writeInt(u64, &encoded, @as(u64, code) << @intCast(64 - len), .big);
        var input = std.Io.Reader.fixed(&encoded);
        var bits = Bits{ .input = &input };
        try std.testing.expectEqual(expected, try table.symbol(&bits));
    }
    const mixed = try Table.init(&.{ 8, 9, 10, 11, 20 });
    for ([_]u8{ 8, 9, 10, 11, 20 }, 0..) |len, expected| {
        var encoded: [8]u8 = @splat(0);
        std.mem.writeInt(u64, &encoded, @as(u64, mixed.first[len]) << @intCast(64 - len), .big);
        var reader = std.Io.Reader.fixed(&encoded);
        var lookahead = Bits{ .input = &reader };
        try std.testing.expectEqual(expected, try mixed.symbol(&lookahead));
    }
    const invalid = try Table.init(&.{ 2, 2 });
    var input = std.Io.Reader.fixed(&.{ 0xff, 0xff, 0xff, 0xff });
    var bits = Bits{ .input = &input };
    try std.testing.expectError(error.InvalidBzip2, invalid.symbol(&bits));
}
