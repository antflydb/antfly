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

//! Compact navigation over Parquet delta streams. Checkpoints retain one
//! predecessor per 32 values, never a complete decoded integer vector.
const std = @import("std");
const A = std.mem.Allocator;
const Reader = struct {
    bytes: []const u8,
    offset: usize = 0,
    fn take(self: *Reader, n: usize) ![]const u8 {
        if (n > self.bytes.len - self.offset) return error.InvalidParquetPage;
        const out = self.bytes[self.offset..][0..n];
        self.offset += n;
        return out;
    }
    fn unsigned(self: *Reader) !u64 {
        var value: u64 = 0;
        var shift: u7 = 0;
        while (shift < 70) : (shift += 7) {
            const byte = (try self.take(1))[0];
            if (shift == 63 and byte > 1) return error.InvalidParquetPage;
            value |= @as(u64, byte & 127) << @as(u6, @intCast(shift));
            if (byte & 128 == 0) return value;
        }
        return error.InvalidParquetPage;
    }
    fn signed(self: *Reader) !i64 {
        const value = try self.unsigned();
        return @as(i64, @bitCast(value >> 1)) ^ -@as(i64, @intCast(value & 1));
    }
};
fn unpack(bytes: []const u8, width: u8, index: usize) u64 {
    if (width == 0) return 0;
    switch (width) {
        8 => return bytes[index],
        16 => return std.mem.readInt(u16, bytes[index * 2 ..][0..2], .little),
        32 => return std.mem.readInt(u32, bytes[index * 4 ..][0..4], .little),
        64 => return std.mem.readInt(u64, bytes[index * 8 ..][0..8], .little),
        else => {},
    }
    const start = index * width;
    const shift: u3 = @intCast(start % 8);
    const length = (@as(usize, width) + shift + 7) / 8;
    var value: u128 = 0;
    for (bytes[start / 8 ..][0..length], 0..) |byte, i| value |= @as(u128, byte) << @as(u7, @intCast(i * 8));
    const lane: u64 = @truncate(value >> shift);
    return if (width == 64) lane else lane & ((@as(u64, 1) << @as(u6, @intCast(width))) - 1);
}
pub const Delta = struct {
    const Checkpoint = struct { base: i64, minimum: i64, bytes: []const u8, offset: usize, width: u8, count: usize };
    first: i64,
    count: usize,
    checkpoints: []const Checkpoint,
    consumed: usize,
    bits: u8,
    cached: ?usize = null,
    values: [32]i64 = undefined,
    fn advance(self: Delta, value: i64, minimum: i64, residual: u64) i64 {
        const result = @as(u64, @bitCast(value)) +% @as(u64, @bitCast(minimum)) +% residual;
        return if (self.bits == 32) @as(i32, @bitCast(@as(u32, @truncate(result)))) else @bitCast(result);
    }
    fn decode(self: Delta, checkpoint: Checkpoint) [32]i64 {
        const V = @Vector(32, u64);
        var residuals: [32]u64 = @splat(0);
        if (checkpoint.width == 0) {
            for (&residuals, 0..) |*value, i| value.* = @as(u64, @bitCast(checkpoint.minimum)) *% (i + 1);
        } else {
            for (0..checkpoint.count) |i| residuals[i] = unpack(checkpoint.bytes, checkpoint.width, checkpoint.offset + i) +% @as(u64, @bitCast(checkpoint.minimum));
            var prefix: V = residuals;
            inline for (.{ 1, 2, 4, 8, 16 }) |distance| {
                const shifts = comptime blk: {
                    var lanes: [32]i32 = undefined;
                    for (&lanes, 0..) |*lane, i| lane.* = if (i >= distance) @intCast(i - distance) else -1;
                    break :blk lanes;
                };
                prefix +%= @shuffle(u64, prefix, @as(V, @splat(0)), shifts);
            }
            residuals = prefix;
        }
        const values: V = @as(V, residuals) +% @as(V, @splat(@as(u64, @bitCast(checkpoint.base))));
        return if (self.bits == 32) @as(@Vector(32, i64), @intCast(@as(@Vector(32, i32), @bitCast(@as(@Vector(32, u32), @truncate(values)))))) else @bitCast(values);
    }
    pub fn init(a: A, bytes: []const u8, count: usize, bits: u8) !Delta {
        if (bits != 32 and bits != 64) return error.InvalidParquetPage;
        if (count == 0 and bytes.len == 0) return .{ .first = 0, .count = 0, .bits = bits, .checkpoints = &.{}, .consumed = 0 };
        var reader: Reader = .{ .bytes = bytes };
        const block = try reader.unsigned();
        const minis = try reader.unsigned();
        const total = try reader.unsigned();
        if (block == 0 or block > 1024 * 1024 or block % 128 != 0 or minis == 0 or minis > block / 32 or block % minis != 0 or (block / minis) % 32 != 0 or total != count) return error.InvalidParquetPage;
        const first = try reader.signed();
        if (bits == 32 and std.math.cast(i32, first) == null) return error.InvalidParquetPage;
        const checkpoints = try a.alloc(Checkpoint, (count -| 1 + 31) / 32);
        errdefer a.free(checkpoints);
        var result: Delta = .{ .first = first, .count = count, .bits = bits, .checkpoints = checkpoints, .consumed = 0 };
        var remaining = count -| 1;
        var predecessor = first;
        var slot: usize = 0;
        while (remaining != 0) {
            const minimum = try reader.signed();
            const widths = try reader.take(@intCast(minis));
            for (widths) |width| {
                // Unused widths have arbitrary byte values and no bodies.
                if (remaining == 0) break;
                if (width > bits) return error.InvalidParquetPage;
                const size: usize = @intCast(block / minis);
                const data = try reader.take(size * width / 8);
                const actual = @min(size, remaining);
                var begin: usize = 0;
                while (begin < actual) : (begin += 32) {
                    const n = @min(32, actual - begin);
                    checkpoints[slot] = .{ .base = predecessor, .minimum = minimum, .bytes = data, .offset = begin, .width = width, .count = n };
                    slot += 1;
                    predecessor = if (width == 0) result.advance(predecessor, 0, @as(u64, @bitCast(minimum)) *% n) else result.decode(checkpoints[slot - 1])[n - 1];
                }
                remaining -= actual;
            }
        }
        if (slot != checkpoints.len) return error.InvalidParquetPage;
        result.consumed = reader.offset;
        return result;
    }
    pub fn at(self: *Delta, index: usize) !i64 {
        if (index >= self.count) return error.InvalidParquetPage;
        if (index == 0) return self.first;
        const slot = (index - 1) / 32;
        const checkpoint = self.checkpoints[slot];
        if (checkpoint.width == 0) return self.advance(checkpoint.base, 0, @as(u64, @bitCast(checkpoint.minimum)) *% ((index - 1) % 32 + 1));
        if (self.cached != slot) {
            self.values = self.decode(checkpoint);
            self.cached = slot;
        }
        return self.values[(index - 1) % 32];
    }
    pub fn read(self: *Delta, begin: usize, output: []i64) !void {
        if (begin > self.count or output.len > self.count - begin) return error.InvalidParquetPage;
        var offset: usize = 0;
        while (offset < output.len) {
            const row = begin + offset;
            if (row == 0) {
                output[0] = self.first;
                offset += 1;
                continue;
            }
            const slot = (row - 1) / 32;
            const lane = (row - 1) % 32;
            const n = @min(output.len - offset, self.checkpoints[slot].count - lane);
            if (self.cached != slot) {
                self.values = self.decode(self.checkpoints[slot]);
                self.cached = slot;
            }
            @memcpy(output[offset..][0..n], self.values[lane..][0..n]);
            offset += n;
        }
    }
    pub fn mask(self: *Delta, begin: usize, keep: []bool, literal: i64, comptime op: anytype) !void {
        if (keep.len > 32) return error.InvalidParquetPage;
        var values: [32]i64 = @splat(0);
        try self.read(begin, values[0..keep.len]);
        const left: @Vector(32, i64) = values;
        const right: @Vector(32, i64) = @splat(literal);
        const matches: [32]bool = if (op == .eq) left == right else if (op == .neq) left != right else if (op == .lt) left < right else if (op == .lte) left <= right else if (op == .gt) left > right else left >= right;
        for (keep, matches[0..keep.len]) |*active, matched| active.* = active.* and matched;
    }
};
/// Sequential full-page decoder. Holds only one decoded physical block and
/// borrows miniblock framing; no random-access checkpoints or second decode.
pub const DeltaStream = struct {
    reader: Reader,
    count: usize,
    bits: u8,
    first: i64 = 0,
    block_size: usize = 0,
    minis: usize = 0,
    widths: []const u8 = &.{},
    mini: usize = 0,
    minimum: i64 = 0,
    data: []const u8 = &.{},
    width: u8 = 0,
    offset: usize = 0,
    remaining: usize = 0,
    emitted: usize = 0,
    predecessor: i64 = 0,
    buffer: [32]i64 = undefined,
    position: usize = 0,
    length: usize = 0,
    pub fn init(bytes: []const u8, count: usize, bits: u8) !DeltaStream {
        if (bits != 32 and bits != 64) return error.InvalidParquetPage;
        var self: DeltaStream = .{ .reader = .{ .bytes = bytes }, .count = count, .bits = bits };
        if (count == 0 and bytes.len == 0) return self;
        const block = try self.reader.unsigned();
        const minis = try self.reader.unsigned();
        const total = try self.reader.unsigned();
        if (block == 0 or block > 1024 * 1024 or block % 128 != 0 or minis == 0 or minis > block / 32 or block % minis != 0 or (block / minis) % 32 != 0 or total != count) return error.InvalidParquetPage;
        self.first = try self.reader.signed();
        if (bits == 32 and std.math.cast(i32, self.first) == null) return error.InvalidParquetPage;
        self.block_size = @intCast(block);
        self.minis = @intCast(minis);
        self.predecessor = self.first;
        return self;
    }
    fn refill(self: *DeltaStream) !bool {
        if (self.emitted == self.count) return false;
        self.position = 0;
        if (self.emitted == 0) {
            self.buffer[0] = self.first;
            self.emitted = 1;
            self.length = 1;
            return true;
        }
        if (self.remaining == 0) {
            if (self.mini == self.widths.len) {
                self.minimum = try self.reader.signed();
                self.widths = try self.reader.take(self.minis);
                self.mini = 0;
            }
            self.width = self.widths[self.mini];
            self.mini += 1;
            if (self.width > self.bits) return error.InvalidParquetPage;
            const size = self.block_size / self.minis;
            self.data = try self.reader.take(size * self.width / 8);
            self.remaining = @min(size, self.count - self.emitted);
            self.offset = 0;
        }
        self.length = @min(32, self.remaining);
        const decoder: Delta = .{ .bits = self.bits, .first = self.first, .count = self.count, .checkpoints = &.{}, .consumed = 0 };
        self.buffer = decoder.decode(.{ .base = self.predecessor, .minimum = self.minimum, .bytes = self.data, .offset = self.offset, .width = self.width, .count = self.length });
        self.predecessor = self.buffer[self.length - 1];
        self.offset += self.length;
        self.remaining -= self.length;
        self.emitted += self.length;
        return true;
    }
    pub fn next(self: *DeltaStream) !i64 {
        if (self.position == self.length and !try self.refill()) return error.InvalidParquetPage;
        const value = self.buffer[self.position];
        self.position += 1;
        return value;
    }
    pub fn finish(self: *const DeltaStream) !void {
        if (self.emitted != self.count or self.position != self.length or self.reader.offset != self.reader.bytes.len) return error.InvalidParquetPage;
    }
    pub fn read(self: *DeltaStream, output: []i64) !void {
        var position: usize = 0;
        while (position < output.len) {
            if (self.position == self.length and !try self.refill()) return error.InvalidParquetPage;
            const n = @min(output.len - position, self.length - self.position);
            @memcpy(output[position..][0..n], self.buffer[self.position..][0..n]);
            position += n;
            self.position += n;
        }
        try self.finish();
    }
};
/// Locate adjacent delta streams by framing alone. Value kernels then decode
/// each length/prefix once, without building random-access checkpoints.
pub fn deltaExtent(bytes: []const u8, count: usize, bits: u8) !usize {
    var stream = try DeltaStream.init(bytes, count, bits);
    var remaining = count -| 1;
    while (remaining != 0) {
        _ = try stream.reader.signed();
        const widths = try stream.reader.take(stream.minis);
        for (widths) |width| {
            if (remaining == 0) break;
            if (width > bits) return error.InvalidParquetPage;
            const size = stream.block_size / stream.minis;
            _ = try stream.reader.take(size * width / 8);
            remaining -= @min(size, remaining);
        }
    }
    return stream.reader.offset;
}

/// Delta lengths borrow suffix bytes. Prefix compression reconstructs only
/// demanded strings and reuses previously demanded immutable prefixes.
/// Metadata and reconstructed values belong to the caller's arena.
pub const Strings = struct {
    lengths: Delta,
    prefixes: ?Delta,
    offsets: []const u32,
    data: []const u8,
    a: A,
    materialized: std.AutoHashMapUnmanaged(usize, []const u8) = .empty,
    scratch: []u8 = &.{},
    scratch_row: ?usize = null,
    scratch_len: usize = 0,
    pub fn init(a: A, bytes: []const u8, count: usize, prefix: bool) !Strings {
        var prefixes: ?Delta = if (prefix) try Delta.init(a, bytes, count, 32) else null;
        const skip = if (prefixes) |p| p.consumed else 0;
        var lengths = try Delta.init(a, bytes[skip..], count, 32);
        const data = bytes[skip + lengths.consumed ..];
        const offsets = try a.alloc(u32, count + 1);
        offsets[0] = 0;
        var previous: u64 = 0;
        for (0..count) |i| {
            const len = try lengths.at(i);
            if (len < 0 or @as(u64, @intCast(len)) > data.len - offsets[i]) return error.InvalidParquetPage;
            offsets[i + 1] = offsets[i] + @as(u32, @intCast(len));
            const p = if (prefixes) |*values| try values.at(i) else 0;
            if (p < 0 or p > previous) return error.InvalidParquetPage;
            previous = @as(u64, @intCast(p + len));
            if (previous > std.math.maxInt(u32)) return error.ParquetPageTooLarge;
        }
        if (offsets[count] != data.len) return error.InvalidParquetPage;
        return .{ .a = a, .lengths = lengths, .prefixes = prefixes, .offsets = offsets, .data = data };
    }
    pub fn at(self: *Strings, index: usize) ![]const u8 {
        if (index >= self.lengths.count) return error.InvalidParquetPage;
        if (self.prefixes == null) return self.data[self.offsets[index]..self.offsets[index + 1]];
        if (self.materialized.get(index)) |value| return value;
        const output = try self.a.dupe(u8, try self.borrow(index));
        errdefer self.a.free(output);
        try self.materialized.put(self.a, index, output);
        return output;
    }
    /// Valid until the next borrow; predicates and complete materializers do
    /// not retain a reconstructed string for every rejected/input row.
    pub fn borrow(self: *Strings, index: usize) ![]const u8 {
        if (index >= self.lengths.count) return error.InvalidParquetPage;
        const prefixes = if (self.prefixes) |*p| p else return self.data[self.offsets[index]..self.offsets[index + 1]];
        if (self.materialized.get(index)) |value| return value;
        if (self.scratch_row == index) return self.scratch[0..self.scratch_len];
        const size = @as(usize, @intCast(try prefixes.at(index))) + self.offsets[index + 1] - self.offsets[index];
        if (size > self.scratch.len) self.scratch = try self.a.realloc(self.scratch, @max(size, self.scratch.len * 2));
        const output = self.scratch[0..size];
        var row = index;
        var remaining = size;
        while (remaining != 0) {
            if (self.scratch_row == row and remaining <= self.scratch_len) break;
            if (self.materialized.get(row)) |value| {
                @memcpy(output[0..remaining], value[0..remaining]);
                break;
            }
            const prefix: usize = @intCast(try prefixes.at(row));
            if (remaining > prefix) {
                const suffix = self.data[self.offsets[row]..self.offsets[row + 1]];
                if (remaining - prefix > suffix.len) return error.InvalidParquetPage;
                @memcpy(output[prefix..remaining], suffix[0 .. remaining - prefix]);
                remaining = prefix;
            }
            if (remaining == 0) break;
            if (row == 0) return error.InvalidParquetPage;
            row -= 1;
        }
        self.scratch_row = index;
        self.scratch_len = size;
        return output;
    }
};

fn appendUnsigned(a: A, out: *std.ArrayList(u8), input: u64) !void {
    var value = input;
    while (value >= 128) {
        try out.append(a, @as(u8, @truncate(value)) | 128);
        value >>= 7;
    }
    try out.append(a, @intCast(value));
}
fn appendSigned(a: A, out: *std.ArrayList(u8), value: i64) !void {
    try appendUnsigned(a, out, (@as(u64, @bitCast(value)) << 1) ^ @as(u64, @bitCast(value >> 63)));
}
/// Small deterministic fixtures; real-writer tests independently exercise framing.
pub fn testDelta(a: A, values: []const i64) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(a);
    try appendUnsigned(a, &out, 128);
    try appendUnsigned(a, &out, 4);
    try appendUnsigned(a, &out, values.len);
    try appendSigned(a, &out, if (values.len == 0) 0 else values[0]);
    var begin: usize = 1;
    while (begin < values.len) : (begin += 128) {
        const end = @min(values.len, begin + 128);
        var deltas: [128]i64 = @splat(0);
        var minimum: i64 = std.math.maxInt(i64);
        for (begin..end) |i| {
            deltas[i - begin] = values[i] -% values[i - 1];
            minimum = @min(minimum, deltas[i - begin]);
        }
        try appendSigned(a, &out, minimum);
        var widths: [4]u8 = @splat(0);
        for (&widths, 0..) |*width, mini| {
            for (mini * 32..@max(mini * 32, @min(128, @min(end - begin, (mini + 1) * 32)))) |i| {
                const residual: u64 = @bitCast(deltas[i] -% minimum);
                if (residual != 0) width.* = @max(width.*, @as(u8, @intCast(64 - @clz(residual))));
            }
        }
        try out.appendSlice(a, &widths);
        for (widths, 0..) |width, mini| {
            const start = out.items.len;
            try out.resize(a, start + 4 * width);
            @memset(out.items[start..], 0);
            for (0..32) |lane| {
                const i = mini * 32 + lane;
                const residual: u64 = if (i < end - begin) @bitCast(deltas[i] -% minimum) else 0;
                for (0..width) |bit| out.items[start + (lane * width + bit) / 8] |= @as(u8, @intCast((residual >> @as(u6, @intCast(bit))) & 1)) << @as(u3, @intCast((lane * width + bit) % 8));
            }
        }
    }
    return out.toOwnedSlice(a);
}

test "parquet compact delta navigation handles miniblocks signed overflow and arbitrary selections" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var values: [300]i64 = undefined;
    values[0] = std.math.maxInt(i64);
    for (1..values.len) |i| values[i] = values[i - 1] +% @as(i64, @intCast(i % 7)) -% 3;
    const bytes = try testDelta(a, &values);
    var index = try Delta.init(a, bytes, values.len, 64);
    try std.testing.expect(index.checkpoints.len < values.len / 16);
    for (0..values.len) |i| try std.testing.expectEqual(values[values.len - i - 1], try index.at(values.len - i - 1));
    try std.testing.expectError(error.InvalidParquetPage, Delta.init(a, bytes[0 .. bytes.len - 1], values.len, 64));
    try std.testing.expectError(error.InvalidParquetPage, Delta.init(a, bytes, values.len - 1, 64));
}

test "parquet compact delta strings reconstruct selected immutable prefixes" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const prefixes = try testDelta(a, &.{ 0, 3, 4, 0 });
    const lengths = try testDelta(a, &.{ 6, 1, 2, 3 });
    const bytes = try std.mem.concat(a, u8, &.{ prefixes, lengths, "abcdefX12new" });
    var strings = try Strings.init(a, bytes, 4, true);
    const second = try strings.at(1);
    try std.testing.expectEqualStrings("abcX", second);
    try std.testing.expectEqualStrings("abcX12", try strings.at(2));
    try std.testing.expectEqualStrings("abcdef", try strings.at(0));
    try std.testing.expectEqualStrings("new", try strings.at(3));
    try std.testing.expectEqualStrings("abcX", second);
    try std.testing.expectEqual(@as(usize, 4), strings.materialized.count());
}

fn compactAllocationScenario(a: A) !void {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const work = arena.allocator();
    const prefix = try testDelta(work, &.{ 0, 3, 4, 0 });
    const length = try testDelta(work, &.{ 6, 1, 2, 3 });
    const bytes = try std.mem.concat(work, u8, &.{ prefix, length, "abcdefX12new" });
    var strings = try Strings.init(work, bytes, 4, true);
    try std.testing.expectEqualStrings("abcX12", try strings.at(2));
    try std.testing.expectEqual(@as(usize, 1), strings.materialized.count());
    try std.testing.expectEqualStrings("abcX", try strings.at(1));
    // Prefix bounds are validated before a selected string can be returned.
    const invalid = try std.mem.concat(work, u8, &.{ try testDelta(work, &.{ 1, 3, 4, 0 }), length, "abcdefX12new" });
    try std.testing.expectError(error.InvalidParquetPage, Strings.init(work, invalid, 4, true));
}
test "parquet compact codec unwinds allocation failures and rejects invalid prefix evidence" {
    try compactAllocationScenario(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, compactAllocationScenario, .{});
}

test "parquet compact zero streams and unused miniblock widths preserve framing" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var values = try Delta.init(a, &.{ 128, 1, 4, 2, 0, 2, 0, 255, 7, 1 }, 2, 64);
    try std.testing.expectEqual(@as(i64, 1), try values.at(1));
    try std.testing.expectEqual(@as(usize, 10), values.consumed);
    const empty = try Delta.init(a, &.{ 128, 2, 4, 0, 0 }, 0, 64);
    try std.testing.expectEqual(@as(usize, 5), empty.consumed);
    const strings = try Strings.init(a, &.{ 128, 1, 4, 0, 0, 128, 1, 4, 0, 0 }, 0, true);
    try std.testing.expectEqual(@as(usize, 0), strings.data.len);
    try std.testing.expectError(error.InvalidParquetPage, Strings.init(a, &.{ 128, 1, 4, 0, 0, 0 }, 0, false));
}

test "parquet compact vector reads and masks agree at signed wrapping and block boundaries" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var expected: [300]i64 = undefined;
    expected[0] = std.math.maxInt(i64);
    for (1..expected.len) |i| expected[i] = expected[i - 1] +% @as(i64, @intCast(i % 7)) -% 3;
    var delta = try Delta.init(a, try testDelta(a, &expected), expected.len, 64);
    var actual: [300]i64 = undefined;
    try delta.read(0, &actual);
    try std.testing.expectEqualSlices(i64, &expected, &actual);
    for (0..270) |begin| {
        var keep: [31]bool = @splat(true);
        keep[2] = false;
        try delta.mask(begin, &keep, expected[begin + 15], .gte);
        for (keep, 0..) |matched, i| try std.testing.expectEqual(i != 2 and expected[begin + i] >= expected[begin + 15], matched);
    }
    const prefixes = try testDelta(a, &.{ 0, 3, 4, 0 });
    const lengths = try testDelta(a, &.{ 6, 1, 2, 3 });
    var strings = try Strings.init(a, try std.mem.concat(a, u8, &.{ prefixes, lengths, "abcdefX12new" }), 4, true);
    for ([_][]const u8{ "abcdef", "abcX", "abcX12", "new" }, 0..) |text, i| try std.testing.expectEqualStrings(text, try strings.borrow(i));
    try std.testing.expectEqual(@as(usize, 0), strings.materialized.count());
    try std.testing.expectEqualStrings("abcX12", try strings.borrow(2));
}

test "parquet compact vector INT32 constant deltas wrap in their physical width" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var delta = try Delta.init(arena.allocator(), &.{ 128, 1, 4, 36, 254, 255, 255, 255, 15, 2, 0, 0, 0, 0 }, 36, 32);
    var values: [36]i64 = undefined;
    try delta.read(0, &values);
    var sequential = try DeltaStream.init(&.{ 128, 1, 4, 36, 254, 255, 255, 255, 15, 2, 0, 0, 0, 0 }, 36, 32);
    var streamed: [36]i64 = undefined;
    try sequential.read(&streamed);
    try std.testing.expectEqualSlices(i64, &values, &streamed);
    for (values, 0..) |value, i| {
        const expected: i32 = @bitCast(@as(u32, std.math.maxInt(i32)) +% @as(u32, @intCast(i)));
        try std.testing.expectEqual(@as(i64, expected), value);
        try std.testing.expectEqual(value, try delta.at(i));
    }
}

test "parquet sequential delta decoding shares kernels without allocating checkpoints" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var values: [577]i64 = undefined;
    values[0] = std.math.maxInt(i64);
    for (1..values.len) |i| values[i] = values[i - 1] +% @as(i64, @intCast(i % 11)) -% 6;
    const bytes = try testDelta(a, &values);
    var stream = try DeltaStream.init(bytes, values.len, 64);
    var actual: [577]i64 = undefined;
    try stream.read(&actual);
    try std.testing.expectEqualSlices(i64, &values, &actual);
    stream = try DeltaStream.init(bytes, values.len, 64);
    for (values) |expected| try std.testing.expectEqual(expected, try stream.next());
    try stream.finish();
    try std.testing.expectError(error.InvalidParquetPage, stream.next());
    stream = try DeltaStream.init(bytes[0 .. bytes.len - 1], values.len, 64);
    try std.testing.expectError(error.InvalidParquetPage, stream.read(&actual));
    stream = try DeltaStream.init(&.{ 128, 1, 4, 2, 0, 2, 0, 255, 7, 1 }, 2, 64);
    try std.testing.expectEqual(@as(i64, 0), try stream.next());
    try std.testing.expectEqual(@as(i64, 1), try stream.next());
    try stream.finish();
    stream = try DeltaStream.init(&.{ 128, 1, 4, 0, 0, 1 }, 0, 64);
    try std.testing.expectError(error.InvalidParquetPage, stream.finish());
}
