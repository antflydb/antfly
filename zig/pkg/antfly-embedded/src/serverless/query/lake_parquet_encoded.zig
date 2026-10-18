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
    pub fn init(a: A, bytes: []const u8, count: usize, bits: u8) !Delta {
        if (bits != 32 and bits != 64) return error.InvalidParquetPage;
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
                if (width > bits) return error.InvalidParquetPage;
                const size: usize = @intCast(block / minis);
                const data = try reader.take(size * width / 8);
                const actual = @min(size, remaining);
                var begin: usize = 0;
                while (begin < actual) : (begin += 32) {
                    const n = @min(32, actual - begin);
                    checkpoints[slot] = .{ .base = predecessor, .minimum = minimum, .bytes = data, .offset = begin, .width = width, .count = n };
                    slot += 1;
                    for (begin..begin + n) |i| predecessor = result.advance(predecessor, minimum, unpack(data, width, i));
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
        if (self.cached != slot) {
            const checkpoint = self.checkpoints[slot];
            var value = checkpoint.base;
            for (0..checkpoint.count) |i| {
                value = self.advance(value, checkpoint.minimum, unpack(checkpoint.bytes, checkpoint.width, checkpoint.offset + i));
                self.values[i] = value;
            }
            self.cached = slot;
        }
        return self.values[(index - 1) % 32];
    }
};
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
        const prefixes = if (self.prefixes) |*p| p else return self.data[self.offsets[index]..self.offsets[index + 1]];
        if (self.materialized.get(index)) |value| return value;
        const size = @as(usize, @intCast(try prefixes.at(index))) + self.offsets[index + 1] - self.offsets[index];
        const output = try self.a.alloc(u8, size);
        errdefer self.a.free(output);
        var row = index;
        var remaining = size;
        while (remaining != 0) {
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
        try self.materialized.put(self.a, index, output);
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
